package origin

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"flow/internal/rtmp"
	"flow/internal/source"
	"flow/internal/stream"
)

// TestEnsureSwitchesUpstreamViaLocator 覆盖 S-2 的核心验收点：
// 边缘节点通过动态 locator 从节点 A 拉流，A 下线后下一轮重连改从 B 拉，
// 本地 stream 不关闭，已有下游订阅者继续收到帧。
func TestEnsureSwitchesUpstreamViaLocator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startUpstream := func() (addr string, stop context.CancelFunc) {
		reg := stream.NewRegistry()
		srv := rtmp.New(reg, nil, nil)
		addr = freeAddr(t)
		upCtx, upCancel := context.WithCancel(context.Background())
		go func() { _ = srv.Serve(upCtx, addr) }()
		waitDial(t, addr)

		st := publishUpstream(t, reg, "src")
		streamFrames(t, st)
		return addr, upCancel
	}
	addrA, stopA := startUpstream()
	addrB, stopB := startUpstream()
	defer stopB()

	var useB atomic.Bool
	locator := func(_ context.Context, name string) []Spec {
		if name != "src" && name != "live/src" {
			return nil
		}
		if useB.Load() {
			return []Spec{{
				Scheme: "rtmp", Host: addrB, Raw: "rtmp://" + addrB + "/live/src",
				Hops: 1, Upstream: "node-b",
			}}
		}
		return []Spec{{
			Scheme: "rtmp", Host: addrA, Raw: "rtmp://" + addrA + "/live/src",
			Hops: 1, Upstream: "node-a",
		}}
	}

	edgeReg := stream.NewRegistry()
	m := newTestManager(ctx, edgeReg, nil, 5*time.Second)
	m.SetLocator(locator)

	st := m.Ensure(ctx, "src")
	checkRelayed(t, edgeReg, "src", st)
	rd, cancelSub := st.SourceFor("video").Subscribe()
	defer cancelSub()
	readIDR(t, rd)

	if hops, up := st.DiscoveryMeta(); hops != 1 || up != "node-a" {
		t.Fatalf("meta before failover = (%d, %q), want (1, node-a)", hops, up)
	}

	// 节点 A 下线；locator 从此返回 B。puller 应在退避后重新解析并切换，
	// 本地流对象和下游订阅都不变。
	useB.Store(true)
	stopA()

	deadline := time.Now().Add(5 * time.Second)
	for {
		hops, up := st.DiscoveryMeta()
		if hops == 1 && up == "node-b" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("puller did not switch upstream: meta = (%d, %q)", hops, up)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 同一个 Reader 继续收帧，说明客户端不需要重连。
	awaitFrame(t, rd, 5*time.Second, "frame after upstream switch", func(f source.Frame) bool {
		return len(f.Data) > 0
	})
	if got := edgeReg.Get("src"); got != st {
		t.Fatalf("local stream was replaced during failover: %p != %p", got, st)
	}
}

// TestLocatorNotConfiguredKeepsOriginBehavior 没有 locator 时行为保持 S-1：
// 未匹配模板的流直接返回 nil，不会启动 puller。
func TestLocatorNotConfiguredKeepsOriginBehavior(t *testing.T) {
	m := NewManager(context.Background(), nil, nil, nil, time.Second)
	if st := m.Ensure(context.Background(), "live/unknown"); st != nil {
		t.Fatalf("Ensure = %p, want nil without origin/discovery", st)
	}
	if activeCount(m) != 0 {
		t.Fatalf("puller started without a configured source")
	}
}

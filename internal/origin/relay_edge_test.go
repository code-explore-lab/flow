package origin

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"flow/internal/rtmp"
	"flow/internal/rtsp"
	"flow/internal/source"
	"flow/internal/stream"
)

// TestEnsureRelaysRTMPWithPlayToken 覆盖 S-1 与 T-3 的组合：上游 RTMP 开了
// 拉流 token，回源模板把 ?token= 透传到 play 的流名上。
func TestEnsureRelaysRTMPWithPlayToken(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	upReg := stream.NewRegistry()
	up := rtmp.New(upReg, nil, nil)
	up.SetAuth(rtmp.NewAuth(rtmp.AuthConfig{PlayTokens: []string{"playsecret"}}))
	addr := freeAddr(t)
	go func() { _ = up.Serve(ctx, addr) }()
	waitDial(t, addr)

	ust := publishUpstream(t, upReg, "src")
	streamFrames(t, ust)

	edgeReg := stream.NewRegistry()
	specs, err := ParseSpecs("rtmp://" + addr + "/live?token=playsecret")
	if err != nil {
		t.Fatalf("ParseSpecs: %v", err)
	}
	m := newTestManager(ctx, edgeReg, specs, 3*time.Second)

	st := m.Ensure(ctx, "live/src")
	checkRelayed(t, edgeReg, "live/src", st)

	rd, cancelSub := st.SourceFor("video").Subscribe()
	defer cancelSub()
	readIDR(t, rd)
}

// TestEnsureUpstreamAuthRejected 错误 token 时上游回 StreamNotFound，回源
// 立即失败（不等超时），也不会在本地登记流。
func TestEnsureUpstreamAuthRejected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	upReg := stream.NewRegistry()
	up := rtmp.New(upReg, nil, nil)
	up.SetAuth(rtmp.NewAuth(rtmp.AuthConfig{PlayTokens: []string{"playsecret"}}))
	addr := freeAddr(t)
	go func() { _ = up.Serve(ctx, addr) }()
	waitDial(t, addr)

	ust := publishUpstream(t, upReg, "src")
	streamFrames(t, ust)

	edgeReg := stream.NewRegistry()
	specs, err := ParseSpecs("rtmp://" + addr + "/live?token=wrong")
	if err != nil {
		t.Fatalf("ParseSpecs: %v", err)
	}
	m := newTestManager(ctx, edgeReg, specs, 5*time.Second)

	start := time.Now()
	got := m.Ensure(ctx, "src")
	if got != nil {
		t.Fatalf("Ensure = %p, want nil for a rejected play token", got)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Ensure took %s; rejected play should fail fast", elapsed)
	}
	if st := edgeReg.Get("src"); st != nil {
		t.Errorf("relayed stream registered despite auth rejection")
	}
	waitGone(t, m)
}

// TestEnsureRelaysRTSPWithAuth 上游 RTSP 开 Basic / Digest 时，回源 URL 的
// user:pass 要能自动完成 401 challenge。
func TestEnsureRelaysRTSPWithAuth(t *testing.T) {
	for _, tc := range []struct {
		name   string
		scheme rtsp.AuthScheme
	}{
		{"basic", rtsp.AuthBasic},
		{"digest", rtsp.AuthDigest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			upReg := stream.NewRegistry()
			up := rtsp.New(nil, nil, true)
			up.SetRegistry(upReg)
			up.SetUsers(map[string]string{"alice": "secret"})
			up.SetAuthScheme(tc.scheme)
			addr := freeAddr(t)
			go func() { _ = up.Serve(ctx, addr) }()
			waitDial(t, addr)

			ust := publishUpstream(t, upReg, "live/src")
			streamFrames(t, ust)

			edgeReg := stream.NewRegistry()
			specs, err := ParseSpecs("rtsp://alice:secret@" + addr + "/live")
			if err != nil {
				t.Fatalf("ParseSpecs: %v", err)
			}
			m := newTestManager(ctx, edgeReg, specs, 3*time.Second)

			st := m.Ensure(ctx, "live/src")
			checkRelayed(t, edgeReg, "live/src", st)

			rd, cancelSub := st.SourceFor("video").Subscribe()
			defer cancelSub()
			readIDR(t, rd)
		})
	}
}

// TestEnsureRetriesAfterUpstreamDisappears 源站重启期间上游会短暂回
// StreamNotFound；已经 Ready 过的回源不能因此永久放弃，必须按退避继续重连，
// 上游恢复后下游继续收帧（不收到流结束）。
func TestEnsureRetriesAfterUpstreamDisappears(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	upReg := stream.NewRegistry()
	up := rtmp.New(upReg, nil, nil)
	addr := freeAddr(t)
	go func() { _ = up.Serve(ctx, addr) }()
	waitDial(t, addr)

	ust := publishUpstream(t, upReg, "src")
	streamFrames(t, ust)

	edgeReg := stream.NewRegistry()
	specs, err := ParseSpecs("rtmp://" + addr + "/live")
	if err != nil {
		t.Fatalf("ParseSpecs: %v", err)
	}
	m := newTestManager(ctx, edgeReg, specs, 3*time.Second)

	st := m.Ensure(ctx, "src")
	checkRelayed(t, edgeReg, "src", st)
	rd, cancelSub := st.SourceFor("video").Subscribe()
	defer cancelSub()
	readIDR(t, rd)

	// 源站下线：摘掉上游流，play 会回 StreamNotFound。
	upReg.Unpublish("src", ust)
	time.Sleep(300 * time.Millisecond)
	if activeCount(m) != 1 {
		t.Fatalf("puller gave up after the upstream stream disappeared")
	}

	// 源站恢复：重新发布并继续写帧。
	ust2 := publishUpstream(t, upReg, "src")
	streamFrames(t, ust2)
	awaitFrame(t, rd, 5*time.Second, "frame after upstream recovery", func(f source.Frame) bool {
		return len(f.Data) > 0
	})
}

// TestRTSPDescribeUsesOriginStreamName 回归：回源流可能先被其它协议按裸名
// （src）拉起来，之后 RTSP 客户端 DESCRIBE /live/src。此时注册表里查不到
// 请求名，DESCRIBE 仍必须用回源流自己的编码参数生成 SDP，而不是退回
// 本地文件源的通用 SDP。
func TestRTSPDescribeUsesOriginStreamName(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	upReg := stream.NewRegistry()
	up := rtmp.New(upReg, nil, nil)
	addr := freeAddr(t)
	go func() { _ = up.Serve(ctx, addr) }()
	waitDial(t, addr)

	ust := publishUpstream(t, upReg, "src")
	streamFrames(t, ust)

	edgeReg := stream.NewRegistry()
	specs, err := ParseSpecs("rtmp://" + addr + "/live")
	if err != nil {
		t.Fatalf("ParseSpecs: %v", err)
	}
	m := newTestManager(ctx, edgeReg, specs, 3*time.Second)

	// 模拟 RTMP / HTTP-FLV 下游先用裸名触发回源。
	if st := m.Ensure(ctx, "src"); st == nil {
		t.Fatal("origin puller did not become ready")
	}
	if edgeReg.Get("live/src") != nil {
		t.Fatal("test premise broken: stream already registered as live/src")
	}

	edge := rtsp.New(nil, nil, true)
	edge.SetRegistry(edgeReg)
	edge.SetOrigin(m)
	edgeAddr := freeAddr(t)
	go func() { _ = edge.Serve(ctx, edgeAddr) }()
	waitDial(t, edgeAddr)

	status, sdp := describeSDP(t, edgeAddr, "rtsp://"+edgeAddr+"/live/src")
	if !strings.Contains(status, "200") {
		t.Fatalf("DESCRIBE status = %q, want 200", status)
	}
	if !strings.Contains(sdp, "sprop-parameter-sets") {
		t.Fatalf("DESCRIBE SDP lost the origin stream's parameter sets:\n%s", sdp)
	}
}

// describeSDP 发一条裸 RTSP DESCRIBE 并返回状态行与 body。
func describeSDP(t *testing.T, addr, uri string) (string, string) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := fmt.Fprintf(c, "DESCRIBE %s RTSP/1.0\r\nCSeq: 1\r\nAccept: application/sdp\r\n\r\n", uri); err != nil {
		t.Fatalf("write DESCRIBE: %v", err)
	}

	br := bufio.NewReader(c)
	status, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read status: %v", err)
	}
	headers := make(map[string]string)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("read header: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if k, v, ok := strings.Cut(line, ":"); ok {
			headers[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
		}
	}
	n, _ := strconv.Atoi(headers["content-length"])
	body := make([]byte, n)
	if n > 0 {
		if _, err := io.ReadFull(br, body); err != nil {
			t.Fatalf("read body: %v", err)
		}
	}
	return strings.TrimSpace(status), string(body)
}

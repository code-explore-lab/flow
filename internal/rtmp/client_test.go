package rtmp

import (
	"bytes"
	"context"
	"testing"
	"time"

	"flow/internal/source"
	"flow/internal/stream"
)

func TestParsePullSpec(t *testing.T) {
	valid := []struct {
		in    string
		addr  string
		app   string
		name  string
		tcURL string
		local string
	}{
		{"rtmp://example.com/live/src", "example.com:1935", "live", "src", "rtmp://example.com/live", "src"},
		{"rtmp://127.0.0.1:1936/live/sub/src", "127.0.0.1:1936", "live/sub", "src",
			"rtmp://127.0.0.1:1936/live/sub", "src"},
		{"dst=rtmp://host/live/src", "host:1935", "live", "src", "rtmp://host/live", "dst"},
		{"rtmp://host/live/src?token=abc", "host:1935", "live", "src?token=abc",
			"rtmp://host/live", "src"},
		{"  rtmp://host/app/name  ", "host:1935", "app", "name", "rtmp://host/app", "name"},
	}
	for _, tc := range valid {
		got, err := parsePullSpec(tc.in)
		if err != nil {
			t.Fatalf("parsePullSpec(%q): %v", tc.in, err)
		}
		if got.addr != tc.addr || got.app != tc.app || got.streamName != tc.name ||
			got.tcURL != tc.tcURL || got.localName != tc.local {
			t.Fatalf("parsePullSpec(%q) = %+v, want addr=%q app=%q name=%q tcURL=%q local=%q",
				tc.in, got, tc.addr, tc.app, tc.name, tc.tcURL, tc.local)
		}
	}

	invalid := []string{
		"",
		"rtmp://host/stream",    // 只有一截，缺 app
		"rtmp://host/live/",     // 缺 stream
		"http://host/live/src",  // 不支持的 scheme
		"=rtmp://host/live/src", // 本地名为空
		"rtmp:///live/src",      // 缺 host
		"not-a-url",
	}
	for _, in := range invalid {
		if _, err := parsePullSpec(in); err == nil {
			t.Fatalf("parsePullSpec(%q) accepted an invalid spec", in)
		}
	}
}

func TestRebaseClockMonotonicAcrossReconnect(t *testing.T) {
	var c rebaseClock
	// 第一条连接：原样从 0 起。
	if got := c.apply(0, 0); got != 0 {
		t.Fatalf("first apply(0) = %d, want 0", got)
	}
	if got := c.apply(3600, 0); got != 3600 {
		t.Fatalf("first apply(3600) = %d, want 3600", got)
	}

	// 重连后上游从 0 重新计时：平移到最后值 + 1 之后。
	c.newConn()
	if got := c.apply(0, 0); got != 3601 {
		t.Fatalf("reconnect apply(0) = %d, want 3601", got)
	}
	if got := c.apply(1800, 0); got != 5401 {
		t.Fatalf("reconnect apply(1800) = %d, want 5401", got)
	}

	// 再次重连仍继续保持单调：首帧 PTS 直接落在上一最大值 +1（与上游
	// 这一帧原本的 90ms 无关）。
	c.newConn()
	if got := c.apply(90, 0); got != 5402 {
		t.Fatalf("second reconnect apply(90) = %d, want 5402", got)
	}
}

// TestRebaseClockAccountsForCTS 回归 S-1 重连后观察到的 non monotonic dts：
// 上一连接最后 DTS=3600、CTS=40，PTS 已经到 3640；重连后首帧 CTS=0。若只
// 按 DTS 平移（base=3601）新 PTS=3601 就落到 3640 之前。这里断言基准按
// PTS 计算：新 DTS=3641，PTS=3641。
func TestRebaseClockAccountsForCTS(t *testing.T) {
	var c rebaseClock
	c.apply(3600, 40) // PTS=3640
	c.newConn()
	if got := c.apply(0, 0); got != 3641 {
		t.Fatalf("reconnect apply(0,0) DTS = %d, want 3641", got)
	}
	if c.last != 3641 {
		t.Fatalf("last PTS = %d, want 3641", c.last)
	}

	// 同一连接内音频（无 CTS）照常跟随基准，不会落到视频之前。
	if got := c.apply(0, 0); got != 3641 {
		t.Fatalf("audio apply(0,0) = %d, want 3641", got)
	}
}

// TestRebaseClockNegativeCTS：B 帧 CTS 为负时，重连基准要留出同样的重排
// 窗口，保证新连接里带负 CTS 的帧 PTS 也不低于上一连接的最大值 +1。
func TestRebaseClockNegativeCTS(t *testing.T) {
	var c rebaseClock
	c.apply(3600, 40)  // PTS=3640
	c.apply(3700, -40) // PTS=3660，minCTS=-40
	c.newConn()

	// 首帧（如序列头）DTS=0、CTS=0：base = 3660+1-0-(-40) = 3701。
	if got := c.apply(0, 0); got != 3701 {
		t.Fatalf("reconnect apply(0,0) = %d, want 3701", got)
	}
	// 同连接带负 CTS 的 B 帧：PTS = 3701-40 = 3661 > 上一连接最大值 3660。
	if got := c.apply(0, -40); got != 3701 {
		t.Fatalf("apply(0,-40) DTS = %d, want 3701", got)
	}
	if c.last != 3701 {
		t.Fatalf("last PTS = %d, want 3701", c.last)
	}
}

// TestClientRelayAndReconnect 起一个本地 RTMP 服务器当上游，Client 从它拉流并
// 发布到本地注册表；中途断开上游连接，断言 Client 自动重连、下游持续收到帧
// 且时间戳单调递增；最后取消 ctx，断言本地流注销、下游读到结束。
func TestClientRelayAndReconnect(t *testing.T) {
	sps := []byte{0x67, 0x42, 0x00, 0x1f, 0x96, 0x54, 0x05, 0x01, 0xed}
	pps := []byte{0x68, 0xee, 0x3c, 0x80}
	idr := []byte{0x65, 0x88, 0x84, 0x00}

	// 上游服务器
	up := New(nil, nil, nil)
	addr := freeAddr(t)
	upctx, upcancel := context.WithCancel(context.Background())
	upErrc := make(chan error, 1)
	go func() { upErrc <- up.Serve(upctx, addr) }()
	defer func() {
		upcancel()
		select {
		case <-upErrc:
		case <-time.After(5 * time.Second):
			t.Error("upstream Serve did not return after cancel")
		}
	}()

	// 上游流：直接登记并持续写 IDR，等价于一路已推上来的流。
	ust := &stream.Stream{}
	ust.SetVideoParams("H264", nil, sps, pps)
	ust.EnsureVideo()
	if _, err := up.registry.Publish("src", ust); err != nil {
		t.Fatalf("publish upstream stream: %v", err)
	}
	defer up.registry.Unpublish("src", ust)

	// 回源到第二份注册表
	lreg := stream.NewRegistry()
	cl, err := NewClient("rtmp://"+addr+"/live/src", lreg, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	cl.SetTimeouts(3*time.Second, 3*time.Second)
	cl.SetBackoff(20*time.Millisecond, 100*time.Millisecond)

	cctx, ccancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		cl.Run(cctx)
	}()
	defer func() {
		ccancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("Client.Run did not return after ctx cancel")
		}
	}()

	waitFor(t, 3*time.Second, func() bool { return lreg.Get("src") != nil })
	lst := lreg.Get("src")
	lst.EnsureVideo()
	rd, cancel := lst.SourceFor("video").Subscribe()
	defer cancel()

	// 持续往上游写帧，保证订阅时机不影响观察结果。
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ts := uint32(0)
		for {
			select {
			case <-stop:
				return
			default:
			}
			ust.SourceFor("video").Write(source.Frame{Data: idr, Timestamp: ts, Keyframe: true})
			ts += 3600
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// 先读到至少一帧 IDR，记录时间戳。
	var maxTS uint32
	seen := false
	for !seen {
		f := nextFrame(t, rd)
		if f.Timestamp > maxTS {
			maxTS = f.Timestamp
		}
		if bytes.Equal(f.Data, idr) {
			seen = true
		}
	}

	// 断开上游所有连接，触发 Client 重连。
	if n := up.closeAllConns(); n == 0 {
		t.Fatal("closeAllConns closed no connections")
	}

	// 继续读到时间戳超过中断前最大值的新 IDR，说明重连后帧继续下行；
	// 期间下游 reader 不允许结束（nextFrame 会因流结束而失败）。
	maxBefore := maxTS
	for {
		f := nextFrame(t, rd)
		if f.Timestamp > maxTS {
			maxTS = f.Timestamp
		}
		if bytes.Equal(f.Data, idr) && f.Timestamp > maxBefore {
			break
		}
	}

	// 取消 ctx：本地流应被注销，下游读到结束。
	ccancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Client.Run did not return after ctx cancel")
	}
	waitFor(t, 2*time.Second, func() bool { return lreg.Get("src") == nil })

	doneRead := make(chan struct{})
	defer close(doneRead)
	for {
		if _, ok := rd.Next(doneRead); !ok {
			break
		}
	}
}

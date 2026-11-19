package tsingest

import (
	"context"
	"net"
	"testing"
	"time"

	"flow/internal/stream"
)

func sendDatagrams(t *testing.T, from *net.UDPConn, ts []byte) {
	t.Helper()
	const pkt = 1316
	for len(ts) > 0 {
		n := pkt
		if n > len(ts) {
			n = len(ts)
		}
		if _, err := from.Write(ts[:n]); err != nil {
			t.Fatalf("send datagram: %v", err)
		}
		ts = ts[n:]
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func TestRunUDPPublishIdleRepublish(t *testing.T) {
	reg := stream.NewRegistry()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- runUDPConn(ctx, reg, "live/udp", conn, UDPOptions{Idle: 100 * time.Millisecond})
	}()

	from, err := net.DialUDP("udp4", nil, conn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer from.Close()

	sendDatagrams(t, from, buildTS(t, 3))
	if !waitFor(t, 2*time.Second, func() bool { return reg.Get("live/udp") != nil }) {
		t.Fatal("stream was not published after the first packet")
	}

	st := reg.Get("live/udp")
	if !waitFor(t, 2*time.Second, func() bool { return st.SourceFor("video") != nil }) {
		t.Fatal("video source was not created")
	}
	// 最后一个访问单元要等下一个 PES 或 Flush 才会交给回调，所以这里
	// 只校验已经解出的帧：参数集 + 最近一个 GOP 的开头，时间戳不倒退。
	frames := collectFrames(st.SourceFor("video"), 20, 300*time.Millisecond)
	if len(frames) < 4 {
		t.Fatalf("got %d video frames, want at least 4", len(frames))
	}
	if frames[0].Timestamp != 90000 {
		t.Errorf("first timestamp = %d, want 90000", frames[0].Timestamp)
	}
	for i := 1; i < len(frames); i++ {
		if frames[i].Timestamp < frames[i-1].Timestamp {
			t.Fatalf("timestamp went backwards at frame %d: %d → %d",
				i, frames[i-1].Timestamp, frames[i].Timestamp)
		}
	}

	// 不再发包：空闲超时后应摘流。
	if !waitFor(t, 3*time.Second, func() bool { return reg.Get("live/udp") == nil }) {
		t.Fatal("stream was not unpublished after going idle")
	}

	// 下一包到达时重新发布（同一个接收端不重启）。
	sendDatagrams(t, from, buildTS(t, 1))
	if !waitFor(t, 2*time.Second, func() bool { return reg.Get("live/udp") != nil }) {
		t.Fatal("stream was not republished after new packets")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("RunUDP: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RunUDP did not stop after ctx cancel")
	}
	if reg.Get("live/udp") != nil {
		t.Error("stream still published after shutdown")
	}
}

func TestRunUDPReplacedByAnotherPublisher(t *testing.T) {
	reg := stream.NewRegistry()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- runUDPConn(ctx, reg, "live/udp", conn, UDPOptions{Idle: time.Minute})
	}()

	from, err := net.DialUDP("udp4", nil, conn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer from.Close()

	sendDatagrams(t, from, buildTS(t, 2))
	if !waitFor(t, 2*time.Second, func() bool { return reg.Get("live/udp") != nil }) {
		t.Fatal("stream was not published")
	}

	// 另一个推流端接管同名流：UDP 输入让出名字并退出。
	if _, err := reg.Publish("live/udp", &stream.Stream{}); err != nil {
		t.Fatalf("replace publish: %v", err)
	}
	sendDatagrams(t, from, buildTS(t, 1))

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("RunUDP should report the lost stream name")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RunUDP did not yield the stream name")
	}
}

package rtsp

import (
	"context"
	"strings"
	"testing"

	"flow/internal/stream"
)

// fakeFinder 模拟发现服务：按流名返回固定 Location。
type fakeFinder struct {
	urls  map[string]string
	calls int
}

func (f *fakeFinder) Find(_ context.Context, name string) (string, bool) {
	f.calls++
	u, ok := f.urls[name]
	return u, ok
}

// TestDiscoveryRedirectDescribeAndOptions 验证本地没有的流在 DESCRIBE 和
// OPTIONS 上都回 302 + Location，并且不创建会话。
func TestDiscoveryRedirectDescribeAndOptions(t *testing.T) {
	const loc = "rtsp://edge-a:8554/live/x"
	f := &fakeFinder{urls: map[string]string{"live/x": loc}}

	s := New(nil, nil, false)
	s.SetDiscovery(f)
	br, bw, cleanup := dialTestConn(t, s)
	defer cleanup()

	resp := sendRTSP(t, br, bw,
		"DESCRIBE rtsp://127.0.0.1:8556/live/x RTSP/1.0\r\nCSeq: 1\r\nAccept: application/sdp\r\n\r\n")
	if !strings.HasPrefix(resp, "RTSP/1.0 302 Found\r\n") {
		t.Fatalf("DESCRIBE status = %q", firstLine(resp))
	}
	if got := responseHeader(resp, "Location"); got != loc {
		t.Fatalf("DESCRIBE Location = %q, want %q", got, loc)
	}

	resp = sendRTSP(t, br, bw,
		"OPTIONS rtsp://127.0.0.1:8556/live/x RTSP/1.0\r\nCSeq: 2\r\n\r\n")
	if !strings.HasPrefix(resp, "RTSP/1.0 302 Found\r\n") {
		t.Fatalf("OPTIONS status = %q", firstLine(resp))
	}
	if got := responseHeader(resp, "Location"); got != loc {
		t.Fatalf("OPTIONS Location = %q, want %q", got, loc)
	}

	if n := s.sessions.Len(); n != 0 {
		t.Fatalf("discovery redirect created %d session(s)", n)
	}
	if f.calls != 2 {
		t.Fatalf("finder calls = %d, want 2", f.calls)
	}
}

// TestDiscoveryLocalStreamWins 验证本地已登记的流优先本地，不查询发现服务。
func TestDiscoveryLocalStreamWins(t *testing.T) {
	f := &fakeFinder{urls: map[string]string{"live/local": "rtsp://elsewhere/live/local"}}

	s := New(nil, nil, false)
	s.SetDiscovery(f)
	if _, err := s.registry.Publish("live/local", &stream.Stream{}); err != nil {
		t.Fatal(err)
	}

	br, bw, cleanup := dialTestConn(t, s)
	defer cleanup()
	resp := sendRTSP(t, br, bw,
		"DESCRIBE rtsp://127.0.0.1:8554/live/local RTSP/1.0\r\nCSeq: 1\r\n\r\n")
	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("local DESCRIBE status = %q", firstLine(resp))
	}
	if f.calls != 0 {
		t.Fatalf("finder called %d time(s) for a local stream", f.calls)
	}
}

// TestDiscoveryMissFallsThrough 验证发现未命中时继续原有逻辑（这里 live
// 模式下直接返回本地 SDP）。
func TestDiscoveryMissFallsThrough(t *testing.T) {
	f := &fakeFinder{urls: map[string]string{}}

	s := New(nil, nil, true)
	s.SetDiscovery(f)
	br, bw, cleanup := dialTestConn(t, s)
	defer cleanup()

	resp := sendRTSP(t, br, bw,
		"DESCRIBE rtsp://127.0.0.1:8554/live/missing RTSP/1.0\r\nCSeq: 1\r\n\r\n")
	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("DESCRIBE status = %q", firstLine(resp))
	}
	if f.calls != 1 {
		t.Fatalf("finder calls = %d, want 1", f.calls)
	}
}

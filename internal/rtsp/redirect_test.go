package rtsp

import (
	"strings"
	"testing"
)

// TestRedirectMode 验证重定向模式：OPTIONS/DESCRIBE 回 302 + Location，
// 回包带原请求的 CSeq，且不创建任何会话。
func TestRedirectMode(t *testing.T) {
	const target = "rtsp://127.0.0.1:8555/live"

	s := New(nil, nil, false)
	s.SetRedirect(target)

	br, bw, cleanup := dialTestConn(t, s)
	defer cleanup()

	expectRedirect := func(resp, method string) {
		t.Helper()
		if !strings.HasPrefix(resp, "RTSP/1.0 302 Found\r\n") {
			t.Fatalf("%s: status = %q", method, firstLine(resp))
		}
		if got := responseHeader(resp, "Location"); got != target {
			t.Fatalf("%s: Location = %q, want %q", method, got, target)
		}
	}

	// ffprobe 最先发 OPTIONS，收到 302 就会跟随 Location 重连。
	resp := sendRTSP(t, br, bw,
		"OPTIONS rtsp://127.0.0.1:8554/live RTSP/1.0\r\nCSeq: 1\r\n\r\n")
	expectRedirect(resp, "OPTIONS")
	if got := responseHeader(resp, "CSeq"); got != "1" {
		t.Fatalf("OPTIONS: CSeq = %q, want 1", got)
	}

	// 不做 OPTIONS 的客户端也会在 DESCRIBE 处被重定向。
	resp = sendRTSP(t, br, bw,
		"DESCRIBE rtsp://127.0.0.1:8554/live RTSP/1.0\r\nCSeq: 2\r\nAccept: application/sdp\r\n\r\n")
	expectRedirect(resp, "DESCRIBE")
	if got := responseHeader(resp, "CSeq"); got != "2" {
		t.Fatalf("DESCRIBE: CSeq = %q, want 2", got)
	}

	// 重定向不建立会话，也不该在服务器上留下任何状态。
	if n := s.sessions.Len(); n != 0 {
		t.Fatalf("redirect mode created %d session(s)", n)
	}
}

// TestRedirectNotEchoClientURI 确认 Location 永远来自配置，而不是请求 URI，
// 避免把客户端伪造的地址反射回去。
func TestRedirectNotEchoClientURI(t *testing.T) {
	const target = "rtsp://127.0.0.1:8555/live"

	s := New(nil, nil, false)
	s.SetRedirect(target)
	br, bw, cleanup := dialTestConn(t, s)
	defer cleanup()

	resp := sendRTSP(t, br, bw,
		"DESCRIBE rtsp://evil.example/whatever RTSP/1.0\r\nCSeq: 1\r\n\r\n")
	if got := responseHeader(resp, "Location"); got != target {
		t.Fatalf("Location = %q, want configured %q", got, target)
	}
}

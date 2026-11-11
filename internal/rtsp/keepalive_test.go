package rtsp

import (
	"bufio"
	"context"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"flow/internal/session"
)

// TestHandleKeepalive 覆盖 GET_PARAMETER / SET_PARAMETER 的三种分支：
// 有效会话回 200 并 TouchRTSP、未知会话 454、无 Session 头回 200。
func TestHandleKeepalive(t *testing.T) {
	t.Run("有效会话回 200 并刷新活跃时间", func(t *testing.T) {
		s, conn, finish := newTestPipe(t)
		sess := s.sessions.NewSession("127.0.0.1")
		before := sess.LastActivity()

		time.Sleep(5 * time.Millisecond)
		req := &Request{
			Method:  "GET_PARAMETER",
			URI:     digestTestURI,
			Headers: map[string]string{"Session": sess.ID},
		}
		resp := callAndRead(t, func() error {
			return s.handleKeepalive(conn, req, map[string]string{"CSeq": "2"})
		}, finish)

		if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
			t.Fatalf("GET_PARAMETER response = %q", firstLine(resp))
		}
		if got := responseHeader(resp, "Session"); !strings.HasPrefix(got, sess.ID) {
			t.Fatalf("Session header = %q, want %q", got, sess.ID)
		}
		if after := sess.LastActivity(); !after.After(before) {
			t.Fatalf("LastActivity not refreshed: before=%v after=%v", before, after)
		}
	})

	t.Run("未知会话 454", func(t *testing.T) {
		s, conn, finish := newTestPipe(t)
		req := &Request{
			Method:  "SET_PARAMETER",
			URI:     digestTestURI,
			Headers: map[string]string{"Session": "sess-999"},
		}
		resp := callAndRead(t, func() error {
			return s.handleKeepalive(conn, req, map[string]string{"CSeq": "2"})
		}, finish)

		if !strings.HasPrefix(resp, "RTSP/1.0 454 Session Not Found\r\n") {
			t.Fatalf("unknown-session response = %q", firstLine(resp))
		}
	})

	t.Run("无 Session 头回 200", func(t *testing.T) {
		s, conn, finish := newTestPipe(t)
		req := &Request{Method: "GET_PARAMETER", URI: digestTestURI}
		resp := callAndRead(t, func() error {
			return s.handleKeepalive(conn, req, map[string]string{"CSeq": "2"})
		}, finish)

		if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
			t.Fatalf("sessionless response = %q", firstLine(resp))
		}
	})
}

// TestKeepalivePreventsSessionExpiry 把 60s 超时等比例压缩成 200ms，
// 走真实的 Sweep 路径验证：只要客户端定时发 GET_PARAMETER，会话就不会
// 因没有 RTCP 被回收；停止保活后兜底回收仍然生效。
func TestKeepalivePreventsSessionExpiry(t *testing.T) {
	s := New(nil, nil, false)
	s.sessions = session.NewManagerWithInterval(200*time.Millisecond, 20*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.sessions.Run(ctx)

	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		s.handleConn(&Conn{Conn: serverConn})
		close(done)
	}()
	defer func() {
		clientConn.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("handleConn did not exit after client close")
		}
	}()

	br := bufio.NewReader(clientConn)
	bw := bufio.NewWriter(clientConn)

	resp := sendRTSP(t, br, bw, announceRequest(1, ""))
	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("ANNOUNCE response = %q", firstLine(resp))
	}
	sessID := strings.Split(responseHeader(resp, "Session"), ";")[0]
	if sessID == "" {
		t.Fatal("ANNOUNCE response missing Session header")
	}

	// 每 50ms 保活一次，持续 600ms（3 倍超时时间）。若 GET_PARAMETER 不刷新
	// 活跃时间，会话会在 200ms 后的某次扫描中被回收。
	for i := 0; i < 12; i++ {
		time.Sleep(50 * time.Millisecond)
		raw := "GET_PARAMETER rtsp://127.0.0.1:8554/live RTSP/1.0\r\nCSeq: " +
			strconv.Itoa(i+2) + "\r\nSession: " + sessID + "\r\n\r\n"
		resp := sendRTSP(t, br, bw, raw)
		if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
			t.Fatalf("keepalive #%d response = %q", i+1, firstLine(resp))
		}
	}
	if _, ok := s.sessions.Get(sessID); !ok {
		t.Fatal("session expired despite periodic GET_PARAMETER keepalive")
	}

	// 停止保活，等待超时 + 扫描周期后应被回收。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := s.sessions.Get(sessID); !ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("idle session was not reclaimed after keepalive stopped")
}

package rtsp

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"flow/internal/authhook"
)

// rtspHookCapture 记录 hook 收到的请求。
type rtspHookCapture struct {
	mu   sync.Mutex
	reqs []authhook.Request
}

func (c *rtspHookCapture) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.reqs)
}

func (c *rtspHookCapture) last(t *testing.T) authhook.Request {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.reqs) == 0 {
		t.Fatal("auth hook was not called")
	}
	return c.reqs[len(c.reqs)-1]
}

// attachHook 给 RTSP 服务器挂一个 httptest hook。
func attachHook(t *testing.T, s *Server, status int, body string, cfg authhook.Config) *rtspHookCapture {
	t.Helper()
	cap := &rtspHookCapture{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req authhook.Request
		_ = json.NewDecoder(r.Body).Decode(&req)
		cap.mu.Lock()
		cap.reqs = append(cap.reqs, req)
		cap.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(ts.Close)
	cfg.URL = ts.URL
	hook, err := authhook.New(cfg)
	if err != nil {
		t.Fatalf("authhook.New: %v", err)
	}
	s.SetAuthHook(hook)
	return cap
}

// dialRTSP 用 net.Pipe 直接驱动 handleConn。
func dialRTSP(t *testing.T, s *Server) (*bufio.Reader, *bufio.Writer, func()) {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		s.handleConn(&Conn{Conn: serverConn})
		close(done)
	}()
	cleanup := func() {
		_ = clientConn.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("handleConn did not exit after client close")
		}
	}
	return bufio.NewReader(clientConn), bufio.NewWriter(clientConn), cleanup
}

func hookAnnounceRequest(uri, user string, sdp string) string {
	return fmt.Sprintf("ANNOUNCE %s RTSP/1.0\r\nCSeq: 1\r\n"+
		"Content-Type: application/sdp\r\nContent-Length: %d\r\n"+
		"Authorization: %s\r\n\r\n%s",
		uri, len(sdp), basicAuthHeader(user, "secret"), sdp)
}

func TestRTSPStreamToken(t *testing.T) {
	tests := []struct {
		uri, stream, token string
	}{
		{"rtsp://127.0.0.1:8554/live/cam", "live/cam", ""},
		{"rtsp://127.0.0.1:8554/live/cam?token=t1", "live/cam", "t1"},
		{"/live/cam?key=k1", "live/cam", "k1"},
		{"rtsp://host/live/cam?x=1&token=t2", "live/cam", "t2"},
	}
	for _, c := range tests {
		stream, token := rtspStreamToken(c.uri)
		if stream != c.stream || token != c.token {
			t.Fatalf("rtspStreamToken(%q) = %q/%q, want %q/%q",
				c.uri, stream, token, c.stream, c.token)
		}
	}
}

func TestAuthHookAction(t *testing.T) {
	for _, c := range []struct {
		method, action string
		ok             bool
	}{
		{"ANNOUNCE", "publish", true},
		{"DESCRIBE", "play", true},
		{"OPTIONS", "", false},
		{"SETUP", "", false},
		{"PLAY", "", false},
	} {
		action, ok := authHookAction(c.method)
		if action != c.action || ok != c.ok {
			t.Fatalf("authHookAction(%q) = %q/%v, want %q/%v",
				c.method, action, ok, c.action, c.ok)
		}
	}
}

func TestAuthHookDeniesAnnounce(t *testing.T) {
	s := New(nil, nil, false)
	cap := attachHook(t, s, http.StatusForbidden, "", authhook.Config{})
	br, bw, cleanup := dialRTSP(t, s)
	defer cleanup()

	const uri = "rtsp://127.0.0.1:8554/live/cam?token=tok1"
	resp := sendRTSP(t, br, bw, hookAnnounceRequest(uri, "alice", minVideoSDP("H264")))
	if !strings.HasPrefix(resp, "RTSP/1.0 403 Forbidden\r\n") {
		t.Fatalf("response = %q, want 403", firstLine(resp))
	}
	if st := s.publishedStreamByName("live/cam"); st != nil {
		t.Fatal("denied ANNOUNCE still published the stream")
	}

	got := cap.last(t)
	if got.Action != "publish" || got.Protocol != "rtsp" {
		t.Fatalf("hook request = %+v", got)
	}
	if got.Stream != "live/cam" || got.Token != "tok1" || got.User != "alice" {
		t.Fatalf("stream/token/user = %q/%q/%q", got.Stream, got.Token, got.User)
	}
	if got.IP != "pipe" {
		t.Fatalf("ip = %q, want pipe", got.IP)
	}
}

func TestAuthHookAllowsAnnounce(t *testing.T) {
	s := New(nil, nil, false)
	attachHook(t, s, http.StatusOK, `{"allow":true}`, authhook.Config{})
	br, bw, cleanup := dialRTSP(t, s)
	defer cleanup()

	resp := sendRTSP(t, br, bw, hookAnnounceRequest("rtsp://127.0.0.1:8554/live/cam", "alice", minVideoSDP("H264")))
	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("response = %q, want 200", firstLine(resp))
	}
	if s.publishedStreamByName("live/cam") == nil {
		t.Fatal("allowed ANNOUNCE did not publish the stream")
	}
}

func TestAuthHookDeniesDescribe(t *testing.T) {
	s := New(nil, nil, true)
	cap := attachHook(t, s, http.StatusForbidden, "", authhook.Config{})
	br, bw, cleanup := dialRTSP(t, s)
	defer cleanup()

	const uri = "rtsp://127.0.0.1:8554/live/cam?token=tok2"
	raw := fmt.Sprintf("DESCRIBE %s RTSP/1.0\r\nCSeq: 1\r\nAuthorization: %s\r\nAccept: application/sdp\r\n\r\n",
		uri, basicAuthHeader("bob", "secret"))
	resp := sendRTSP(t, br, bw, raw)
	if !strings.HasPrefix(resp, "RTSP/1.0 403 Forbidden\r\n") {
		t.Fatalf("response = %q, want 403", firstLine(resp))
	}

	got := cap.last(t)
	if got.Action != "play" || got.Stream != "live/cam" || got.Token != "tok2" || got.User != "bob" {
		t.Fatalf("hook request = %+v", got)
	}
}

func TestAuthHookAllowsDescribe(t *testing.T) {
	s := New(nil, nil, true)
	attachHook(t, s, http.StatusOK, `{"allow":true}`, authhook.Config{})
	br, bw, cleanup := dialRTSP(t, s)
	defer cleanup()

	raw := "DESCRIBE rtsp://127.0.0.1:8554/live/cam RTSP/1.0\r\nCSeq: 1\r\nAccept: application/sdp\r\n\r\n"
	resp := sendRTSP(t, br, bw, raw)
	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("response = %q, want 200", firstLine(resp))
	}
}

func TestAuthHookTimeoutUsesDefaultPolicy(t *testing.T) {
	for _, allow := range []bool{false, true} {
		t.Run(fmt.Sprintf("allow=%v", allow), func(t *testing.T) {
			s := New(nil, nil, false)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(200 * time.Millisecond)
				_, _ = w.Write([]byte(`{"allow":true}`))
			}))
			defer ts.Close()
			hook, err := authhook.New(authhook.Config{
				URL: ts.URL, Timeout: 20 * time.Millisecond, DefaultAllow: allow,
			})
			if err != nil {
				t.Fatal(err)
			}
			s.SetAuthHook(hook)

			br, bw, cleanup := dialRTSP(t, s)
			defer cleanup()
			resp := sendRTSP(t, br, bw, hookAnnounceRequest("rtsp://127.0.0.1:8554/live/cam", "alice", minVideoSDP("H264")))

			want := "RTSP/1.0 403 Forbidden\r\n"
			if allow {
				want = "RTSP/1.0 200 OK\r\n"
			}
			if !strings.HasPrefix(resp, want) {
				t.Fatalf("response = %q, want %q", firstLine(resp), strings.TrimSpace(want))
			}
		})
	}
}

func TestAuthHookDecisionCached(t *testing.T) {
	s := New(nil, nil, false)
	cap := attachHook(t, s, http.StatusOK, `{"allow":true}`, authhook.Config{CacheTTL: time.Minute})

	for range 2 {
		br, bw, cleanup := dialRTSP(t, s)
		resp := sendRTSP(t, br, bw, hookAnnounceRequest("rtsp://127.0.0.1:8554/live/cam", "alice", minVideoSDP("H264")))
		cleanup()
		if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
			t.Fatalf("response = %q, want 200", firstLine(resp))
		}
	}
	if n := cap.count(); n != 1 {
		t.Fatalf("hook calls = %d, want 1 (cache hit)", n)
	}
}

package rtsp

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestMultiUserBasicAuth 验证同一进程可以配置多个账号，且各用各的密码。
func TestMultiUserBasicAuth(t *testing.T) {
	s := New(nil, nil, false)
	s.SetUsers(map[string]string{
		"alice": "secret",
		"bob":   "p@ss:word",
	})

	check := func(user, pass string) bool {
		return s.checkRequestAuth(&Request{
			Method:  "DESCRIBE",
			URI:     digestTestURI,
			Headers: map[string]string{"Authorization": basicAuthHeader(user, pass)},
		}).ok
	}

	if !check("alice", "secret") {
		t.Fatal("alice rejected")
	}
	if !check("bob", "p@ss:word") {
		t.Fatal("bob rejected")
	}
	if check("alice", "p@ss:word") {
		t.Fatal("alice accepted bob's password")
	}
	if check("carol", "secret") {
		t.Fatal("unknown user accepted")
	}
}

// TestMultiUserDigestAuth 验证 Digest 也按用户名查各自的密码。
func TestMultiUserDigestAuth(t *testing.T) {
	s := New(nil, nil, false)
	s.SetUsers(map[string]string{
		"alice": "secret",
		"bob":   "p@ss:word",
	})
	s.SetAuthScheme(AuthDigest)

	nonce := s.authChallengeNonce()
	check := func(user, pass string) bool {
		return s.checkRequestAuth(&Request{
			Method:  "DESCRIBE",
			URI:     digestTestURI,
			Headers: map[string]string{"Authorization": digestHeader(user, pass, nonce, "DESCRIBE", digestTestURI, "00000001", "c1")},
		}).ok
	}

	if !check("alice", "secret") {
		t.Fatal("alice rejected")
	}
	if !check("bob", "p@ss:word") {
		t.Fatal("bob rejected")
	}
	if check("alice", "p@ss:word") {
		t.Fatal("alice accepted bob's password")
	}
	if check("carol", "secret") {
		t.Fatal("unknown user accepted")
	}
}

// TestAllMethodsRequireAuth 验证 OPTIONS 放行，而所有其他方法在未认证时
// 都在分发之前返回 401，不会触达会话/流的处理逻辑。
func TestAllMethodsRequireAuth(t *testing.T) {
	s := New(nil, nil, false)
	s.SetAuth("alice", "secret")

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

	resp := sendRTSP(t, br, bw, "OPTIONS rtsp://127.0.0.1:8554/live RTSP/1.0\r\nCSeq: 1\r\n\r\n")
	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("OPTIONS response = %q", firstLine(resp))
	}

	methods := []string{
		"DESCRIBE", "SETUP", "PLAY", "PAUSE", "RECORD", "TEARDOWN",
		"ANNOUNCE", "GET_PARAMETER", "SET_PARAMETER",
	}
	for i, method := range methods {
		raw := method + " rtsp://127.0.0.1:8554/live RTSP/1.0\r\nCSeq: " +
			strconv.Itoa(i+2) + "\r\n\r\n"
		resp := sendRTSP(t, br, bw, raw)
		if !strings.HasPrefix(resp, "RTSP/1.0 401 Unauthorized\r\n") {
			t.Fatalf("%s unauthenticated response = %q, want 401", method, firstLine(resp))
		}
		if !strings.Contains(resp, "WWW-Authenticate: Basic realm=\"flow\"") {
			t.Fatalf("%s 401 missing WWW-Authenticate: %q", method, resp)
		}
	}

	if n := s.sessions.Len(); n != 0 {
		t.Fatalf("sessions after unauthenticated requests = %d, want 0", n)
	}
}

// authChallengeNonce 从服务端 challenge 里取 nonce，供多用户 Digest 测试复用。
func (s *Server) authChallengeNonce() string {
	return parseAuthParams(strings.TrimPrefix(s.authChallenge(false), "Digest "))["nonce"]
}

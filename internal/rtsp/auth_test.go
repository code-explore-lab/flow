package rtsp

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

func basicAuthHeader(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

func TestCheckRequestAuth(t *testing.T) {
	tests := []struct {
		name   string
		user   string
		pass   string
		header string
		// headerName 为空时用 "Authorization"；用于验证头名大小写不敏感。
		headerName string
		want       bool
	}{
		{name: "未配置鉴权且无头", want: true},
		{name: "未配置鉴权忽略非法头", header: "Basic garbage", want: true},
		{name: "缺少 Authorization", user: "alice", pass: "secret", want: false},
		{name: "认证方案不对", user: "alice", pass: "secret", header: "Digest abc", want: false},
		{name: "base64 非法", user: "alice", pass: "secret", header: "Basic !!!", want: false},
		{
			name: "缺少冒号", user: "alice", pass: "secret",
			header: "Basic " + base64.StdEncoding.EncodeToString([]byte("alice")),
			want:   false,
		},
		{name: "密码错误", user: "alice", pass: "secret", header: basicAuthHeader("alice", "wrong"), want: false},
		{name: "用户名错误", user: "alice", pass: "secret", header: basicAuthHeader("bob", "secret"), want: false},
		{name: "认证通过", user: "alice", pass: "secret", header: basicAuthHeader("alice", "secret"), want: true},
		{name: "方案大小写不敏感", user: "alice", pass: "secret", header: "basic " + base64.StdEncoding.EncodeToString([]byte("alice:secret")), want: true},
		{name: "头名大小写不敏感", user: "alice", pass: "secret", header: basicAuthHeader("alice", "secret"), headerName: "authorization", want: true},
		{name: "空密码", user: "alice", header: basicAuthHeader("alice", ""), want: true},
		{name: "密码含冒号", user: "alice", pass: "pa:ss", header: basicAuthHeader("alice", "pa:ss"), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New(nil, nil, false)
			s.SetAuth(tt.user, tt.pass)

			headers := map[string]string{}
			if tt.header != "" {
				name := tt.headerName
				if name == "" {
					name = "Authorization"
				}
				headers[name] = tt.header
			}
			req := &Request{Method: "DESCRIBE", Headers: headers}
			if got := s.checkRequestAuth(req); got != tt.want {
				t.Fatalf("checkRequestAuth() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestHandleConnBasicAuth 走完整 handleConn 循环：OPTIONS 放行，
// 未认证/错误密码返回 401 challenge，正确凭据可以 ANNOUNCE。
func TestHandleConnBasicAuth(t *testing.T) {
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

	// OPTIONS 不需要凭据。
	resp := sendRTSP(t, br, bw, "OPTIONS rtsp://127.0.0.1:8554/live RTSP/1.0\r\nCSeq: 1\r\n\r\n")
	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("OPTIONS response = %q", firstLine(resp))
	}

	// DESCRIBE 未认证：401 + challenge。
	resp = sendRTSP(t, br, bw, "DESCRIBE rtsp://127.0.0.1:8554/live RTSP/1.0\r\nCSeq: 2\r\n\r\n")
	if !strings.HasPrefix(resp, "RTSP/1.0 401 Unauthorized\r\n") {
		t.Fatalf("unauthenticated DESCRIBE response = %q", firstLine(resp))
	}
	if !strings.Contains(resp, `WWW-Authenticate: Basic realm="flow"`) {
		t.Fatalf("401 response missing WWW-Authenticate: %q", resp)
	}

	// 错误密码：401，且不能创建会话。
	resp = sendRTSP(t, br, bw, announceRequest(3, basicAuthHeader("alice", "wrong")))
	if !strings.HasPrefix(resp, "RTSP/1.0 401 Unauthorized\r\n") {
		t.Fatalf("bad-password ANNOUNCE response = %q", firstLine(resp))
	}
	if n := s.sessions.Len(); n != 0 {
		t.Fatalf("sessions after rejected ANNOUNCE = %d, want 0", n)
	}

	// 正确凭据：ANNOUNCE 成功并创建会话。
	resp = sendRTSP(t, br, bw, announceRequest(4, basicAuthHeader("alice", "secret")))
	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("authenticated ANNOUNCE response = %q", firstLine(resp))
	}
	if n := s.sessions.Len(); n != 1 {
		t.Fatalf("sessions after authenticated ANNOUNCE = %d, want 1", n)
	}
}

func announceRequest(cseq int, auth string) string {
	body := minVideoSDP("H264")
	headers := fmt.Sprintf("CSeq: %d\r\n", cseq)
	if auth != "" {
		headers += "Authorization: " + auth + "\r\n"
	}
	return fmt.Sprintf("ANNOUNCE rtsp://127.0.0.1:8554/live RTSP/1.0\r\n"+
		"%sContent-Type: application/sdp\r\nContent-Length: %d\r\n\r\n%s",
		headers, len(body), body)
}

// sendRTSP 写一条请求并读到响应头结束（测试里的响应都没有 body）。
func sendRTSP(t *testing.T, br *bufio.Reader, bw *bufio.Writer, raw string) string {
	t.Helper()
	if _, err := bw.WriteString(raw); err != nil {
		t.Fatalf("write request: %v", err)
	}
	if err := bw.Flush(); err != nil {
		t.Fatalf("flush request: %v", err)
	}
	var sb strings.Builder
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("read response: %v (got %q)", err, sb.String())
		}
		sb.WriteString(line)
		if line == "\r\n" {
			return sb.String()
		}
	}
}

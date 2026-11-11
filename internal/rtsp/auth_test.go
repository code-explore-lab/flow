package rtsp

import (
	"bufio"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
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
			if got := s.checkRequestAuth(req).ok; got != tt.want {
				t.Fatalf("checkRequestAuth().ok = %v, want %v", got, tt.want)
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

const digestTestURI = "rtsp://127.0.0.1:8554/live"

func testMD5(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

// digestHeader 按 RFC 2617（qop=auth）构造客户端会发来的 Digest 头。
func digestHeader(user, pass, nonce, method, uri, nc, cnonce string) string {
	ha1 := testMD5(user + ":" + basicRealm + ":" + pass)
	ha2 := testMD5(method + ":" + uri)
	response := testMD5(ha1 + ":" + nonce + ":" + nc + ":" + cnonce + ":auth:" + ha2)
	return fmt.Sprintf(`Digest username="%s", realm="%s", nonce="%s", uri="%s", response="%s", qop=auth, nc=%s, cnonce="%s", algorithm=MD5`,
		user, basicRealm, nonce, uri, response, nc, cnonce)
}

// digestHeaderNoQop 构造 RFC 2069 旧式摘要（不带 qop/nc/cnonce）。
func digestHeaderNoQop(user, pass, nonce, method, uri string) string {
	ha1 := testMD5(user + ":" + basicRealm + ":" + pass)
	ha2 := testMD5(method + ":" + uri)
	response := testMD5(ha1 + ":" + nonce + ":" + ha2)
	return fmt.Sprintf(`Digest username="%s", realm="%s", nonce="%s", uri="%s", response="%s"`,
		user, basicRealm, nonce, uri, response)
}

func TestParseAuthParams(t *testing.T) {
	got := parseAuthParams(`username="ali,ce", realm="flow", nonce=abc, cnonce="x\"y", qop=auth`)
	if got["username"] != "ali,ce" || got["realm"] != "flow" || got["nonce"] != "abc" ||
		got["cnonce"] != `x"y` || got["qop"] != "auth" {
		t.Fatalf("parseAuthParams() = %#v", got)
	}
}

func TestDigestAuth(t *testing.T) {
	s := New(nil, nil, false)
	s.SetAuth("alice", "secret")
	s.SetAuthScheme(AuthDigest)

	challenge := s.authChallenge(false)
	if !strings.HasPrefix(challenge, "Digest ") {
		t.Fatalf("challenge = %q, want Digest", challenge)
	}
	for _, want := range []string{`realm="flow"`, `qop="auth"`, "algorithm=MD5"} {
		if !strings.Contains(challenge, want) {
			t.Fatalf("challenge = %q, missing %q", challenge, want)
		}
	}
	if strings.Contains(challenge, "stale=true") {
		t.Fatalf("fresh challenge must not be stale: %q", challenge)
	}
	nonce := parseAuthParams(strings.TrimPrefix(challenge, "Digest "))["nonce"]
	if nonce == "" {
		t.Fatalf("challenge missing nonce: %q", challenge)
	}

	check := func(method, uri, header string) authResult {
		return s.checkRequestAuth(&Request{
			Method:  method,
			URI:     uri,
			Headers: map[string]string{"Authorization": header},
		})
	}

	valid := digestHeader("alice", "secret", nonce, "DESCRIBE", digestTestURI, "00000001", "cnonce1")
	if res := check("DESCRIBE", digestTestURI, valid); !res.ok || res.stale {
		t.Fatalf("valid digest rejected: %+v", res)
	}

	invalid := []struct {
		name   string
		method string
		uri    string
		header string
	}{
		{"错误密码", "DESCRIBE", digestTestURI, digestHeader("alice", "wrong", nonce, "DESCRIBE", digestTestURI, "00000001", "cnonce1")},
		{"方法不匹配", "SETUP", digestTestURI, valid},
		{"URI 不匹配", "DESCRIBE", "rtsp://127.0.0.1:8554/other", valid},
		{"伪造 nonce", "DESCRIBE", digestTestURI, digestHeader("alice", "secret", "forged", "DESCRIBE", digestTestURI, "00000001", "cnonce1")},
		{"错误 qop", "DESCRIBE", digestTestURI, strings.Replace(valid, "qop=auth", "qop=auth-int", 1)},
		{"不支持的算法", "DESCRIBE", digestTestURI, strings.Replace(valid, "algorithm=MD5", "algorithm=MD5-sess", 1)},
		{"realm 不匹配", "DESCRIBE", digestTestURI, strings.Replace(valid, `realm="flow"`, `realm="other"`, 1)},
		{"缺少 response", "DESCRIBE", digestTestURI, `Digest username="alice", realm="flow", nonce="` + nonce + `", uri="` + digestTestURI + `"`},
		{"Basic 头", "DESCRIBE", digestTestURI, basicAuthHeader("alice", "secret")},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			if res := check(tt.method, tt.uri, tt.header); res.ok {
				t.Fatalf("invalid digest accepted: %+v", res)
			}
		})
	}

	// RFC 2069 兼容路径：不带 qop 时按 MD5(HA1:nonce:HA2) 校验。
	if res := check("DESCRIBE", digestTestURI, digestHeaderNoQop("alice", "secret", nonce, "DESCRIBE", digestTestURI)); !res.ok {
		t.Fatalf("legacy digest rejected: %+v", res)
	}

	// 签名有效但超过有效期 → stale=true，客户端换 nonce 重试即可。
	expired := s.newNonceAt(time.Now().Add(-digestNonceLifetime - time.Minute))
	res := check("DESCRIBE", digestTestURI, digestHeader("alice", "secret", expired, "DESCRIBE", digestTestURI, "00000001", "c1"))
	if res.ok || !res.stale {
		t.Fatalf("expired nonce result = %+v, want stale", res)
	}

	// 时间戳来自未来（超出 1 分钟容差）按伪造处理。
	future := s.newNonceAt(time.Now().Add(2 * time.Minute))
	if res := check("DESCRIBE", digestTestURI, digestHeader("alice", "secret", future, "DESCRIBE", digestTestURI, "00000001", "c1")); res.ok || res.stale {
		t.Fatalf("future nonce result = %+v, want invalid", res)
	}
}

// TestHandleConnDigestAuth 走完整 handleConn 流程：Digest challenge、
// 伪造 nonce、过期 stale 重试和成功 ANNOUNCE。
func TestHandleConnDigestAuth(t *testing.T) {
	s := New(nil, nil, false)
	s.SetAuth("alice", "secret")
	s.SetAuthScheme(AuthDigest)

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

	// 未认证 → Digest challenge。
	resp = sendRTSP(t, br, bw, announceRequest(2, ""))
	if !strings.HasPrefix(resp, "RTSP/1.0 401 Unauthorized\r\n") {
		t.Fatalf("unauthenticated ANNOUNCE response = %q", firstLine(resp))
	}
	nonce := challengeNonce(t, resp)

	// 伪造 nonce → 401，且不建会话。
	bad := digestHeader("alice", "secret", "forged", "ANNOUNCE", digestTestURI, "00000001", "c1")
	resp = sendRTSP(t, br, bw, announceRequest(3, bad))
	if !strings.HasPrefix(resp, "RTSP/1.0 401 Unauthorized\r\n") {
		t.Fatalf("forged-nonce ANNOUNCE response = %q", firstLine(resp))
	}
	if n := s.sessions.Len(); n != 0 {
		t.Fatalf("sessions after forged nonce = %d, want 0", n)
	}

	// nonce 过期 → 401 + stale=true，客户端应自动换 nonce。
	expired := s.newNonceAt(time.Now().Add(-digestNonceLifetime - time.Minute))
	expiredHeader := digestHeader("alice", "secret", expired, "ANNOUNCE", digestTestURI, "00000001", "c1")
	resp = sendRTSP(t, br, bw, announceRequest(4, expiredHeader))
	if !strings.Contains(resp, "stale=true") {
		t.Fatalf("expired nonce response missing stale=true: %q", resp)
	}
	if n := s.sessions.Len(); n != 0 {
		t.Fatalf("sessions after stale nonce = %d, want 0", n)
	}

	// 正确凭据 → 200，创建会话。
	good := digestHeader("alice", "secret", nonce, "ANNOUNCE", digestTestURI, "00000001", "c1")
	resp = sendRTSP(t, br, bw, announceRequest(5, good))
	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("authenticated ANNOUNCE response = %q (header %q)", firstLine(resp), good)
	}
	if n := s.sessions.Len(); n != 1 {
		t.Fatalf("sessions after authenticated ANNOUNCE = %d, want 1", n)
	}
}

// challengeNonce 从 401 响应里取出 Digest nonce。
func challengeNonce(t *testing.T, resp string) string {
	t.Helper()
	value := responseHeader(resp, "WWW-Authenticate")
	if !strings.HasPrefix(value, "Digest ") {
		t.Fatalf("WWW-Authenticate = %q, want Digest", value)
	}
	nonce := parseAuthParams(strings.TrimPrefix(value, "Digest "))["nonce"]
	if nonce == "" {
		t.Fatalf("challenge missing nonce: %q", value)
	}
	return nonce
}

func responseHeader(resp, name string) string {
	for _, line := range strings.Split(resp, "\r\n") {
		if len(line) > len(name) && strings.EqualFold(line[:len(name)], name) && line[len(name)] == ':' {
			return strings.TrimSpace(line[len(name)+1:])
		}
	}
	return ""
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

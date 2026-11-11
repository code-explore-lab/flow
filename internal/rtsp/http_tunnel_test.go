package rtsp

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// memConn 是测试用的内存 net.Conn：Read 从 r 读，Write 追加到 buf。
type memConn struct {
	r   io.Reader
	buf bytes.Buffer
}

func (c *memConn) Read(p []byte) (int, error)       { return c.r.Read(p) }
func (c *memConn) Write(p []byte) (int, error)      { return c.buf.Write(p) }
func (c *memConn) Close() error                     { return nil }
func (c *memConn) LocalAddr() net.Addr              { return memAddr{} }
func (c *memConn) RemoteAddr() net.Addr             { return memAddr{} }
func (c *memConn) SetDeadline(time.Time) error      { return nil }
func (c *memConn) SetReadDeadline(time.Time) error  { return nil }
func (c *memConn) SetWriteDeadline(time.Time) error { return nil }

type memAddr struct{}

func (memAddr) Network() string { return "mem" }
func (memAddr) String() string  { return "mem" }

// oneByteReader 每次只吐一个字节，模拟 TCP 把 base64 拆成多个分片到达。
type oneByteReader struct {
	r io.Reader
}

func (r *oneByteReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.r.Read(p)
}

func newTestTunnelConn(input []byte) (*tunnelConn, *memConn) {
	get := &memConn{}
	tun := &httpTunnel{cookie: "test", getConn: &Conn{Conn: get}}
	post := &memConn{r: bytes.NewReader(input)}
	return &tunnelConn{tunnel: tun, post: post, br: bufio.NewReader(post)}, get
}

// TestTunnelConnRead 覆盖上行解码：base64 行、多行、原始 interleaved 帧、
// 明文 RTSP 兼容和非法输入。
func TestTunnelConnRead(t *testing.T) {
	t.Run("base64 行解码", func(t *testing.T) {
		msg := "OPTIONS rtsp://127.0.0.1:8554/live RTSP/1.0\r\nCSeq: 1\r\n\r\n"
		line := base64.StdEncoding.EncodeToString([]byte(msg)) + "\r\n"
		tc, get := newTestTunnelConn([]byte(line))

		got, err := io.ReadAll(tc)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if string(got) != msg {
			t.Fatalf("decoded = %q, want %q", got, msg)
		}

		// 下行必须保持明文，不能反向 base64。
		if _, err := tc.Write([]byte("RTSP/1.0 200 OK\r\n\r\n")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if want := "RTSP/1.0 200 OK\r\n\r\n"; get.buf.String() != want {
			t.Fatalf("GET channel = %q, want %q", get.buf.String(), want)
		}
	})

	t.Run("多条 base64 行按序拼接", func(t *testing.T) {
		first := "OPTIONS * RTSP/1.0\r\nCSeq: 1\r\n\r\n"
		second := "PLAY rtsp://127.0.0.1:8554/live RTSP/1.0\r\nCSeq: 2\r\n\r\n"
		input := base64.StdEncoding.EncodeToString([]byte(first)) + "\r\n" +
			base64.StdEncoding.EncodeToString([]byte(second)) + "\r\n"
		tc, _ := newTestTunnelConn([]byte(input))

		got, err := io.ReadAll(tc)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if string(got) != first+second {
			t.Fatalf("decoded = %q, want %q", got, first+second)
		}
	})

	// ffmpeg 写上行数据时不在 base64 后补换行，多个请求的编码首尾相接。
	t.Run("无换行连续 base64", func(t *testing.T) {
		first := "OPTIONS * RTSP/1.0\r\nCSeq: 1\r\nUser-Agent: Lavf62.3.100\r\n\r\n"
		second := "DESCRIBE rtsp://127.0.0.1:8554/live RTSP/1.0\r\nCSeq: 2\r\n\r\n"
		input := base64.StdEncoding.EncodeToString([]byte(first)) +
			base64.StdEncoding.EncodeToString([]byte(second))
		tc, _ := newTestTunnelConn([]byte(input))

		got, err := io.ReadAll(tc)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if string(got) != first+second {
			t.Fatalf("decoded = %q, want %q", got, first+second)
		}
	})

	t.Run("base64 逐字节到达", func(t *testing.T) {
		msg := "SETUP rtsp://127.0.0.1:8554/live/trackID=0 RTSP/1.0\r\nCSeq: 3\r\n\r\n"
		input := []byte(base64.StdEncoding.EncodeToString([]byte(msg)))
		tc, _ := newTestTunnelConn(input)
		tc.br = bufio.NewReader(&oneByteReader{r: bytes.NewReader(input)})

		got, err := io.ReadAll(tc)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if string(got) != msg {
			t.Fatalf("decoded = %q, want %q", got, msg)
		}
	})

	t.Run("原始 interleaved 帧透传", func(t *testing.T) {
		frame := []byte{'$', 1, 0, 3, 0xAA, 0xBB, 0xCC}
		tc, _ := newTestTunnelConn(frame)

		got, err := io.ReadAll(tc)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if !bytes.Equal(got, frame) {
			t.Fatalf("frame = %v, want %v", got, frame)
		}
	})

	t.Run("明文 RTSP 兼容", func(t *testing.T) {
		raw := "OPTIONS * RTSP/1.0\r\nCSeq: 1\r\n\r\n"
		tc, _ := newTestTunnelConn([]byte(raw))

		got, err := io.ReadAll(tc)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if string(got) != raw {
			t.Fatalf("raw = %q, want %q", got, raw)
		}
	})

	t.Run("非法 base64 报错", func(t *testing.T) {
		tc, _ := newTestTunnelConn([]byte("!!!not-base64!!!\r\n"))
		if _, err := io.ReadAll(tc); err == nil {
			t.Fatal("expected decode error")
		}
	})
}

func TestPeekHTTPMethod(t *testing.T) {
	tests := []struct {
		input string
		want  string
		ok    bool
	}{
		{"GET /live HTTP/1.1\r\n", "GET ", true},
		{"POST /live HTTP/1.1\r\n", "POST", true},
		{"OPTIONS * RTSP/1.0\r\n", "", false},
		{"GET_PARAMETER / RTSP/1.0\r\n", "", false},
	}
	for _, tt := range tests {
		br := bufio.NewReader(strings.NewReader(tt.input))
		got, ok := peekHTTPMethod(br)
		if got != tt.want || ok != tt.ok {
			t.Fatalf("peekHTTPMethod(%q) = (%q, %v), want (%q, %v)",
				tt.input, got, ok, tt.want, tt.ok)
		}
	}
}

func TestSetupTunnelForcesInterleaved(t *testing.T) {
	s, conn, finish := newTestPipe(t)
	conn.tunnel = true

	hd := map[string]string{"CSeq": "1"}
	req := &Request{
		Method: "SETUP",
		URI:    "rtsp://127.0.0.1:8554/live/trackID=0",
		// 客户端在隧道里仍写 client_port 的情况：必须忽略并改成 interleaved。
		Headers: map[string]string{"Transport": "RTP/AVP;unicast;client_port=5000-5001"},
	}
	resp := callAndRead(t, func() error { return s.handleSetup(conn, req, hd) }, finish)

	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("status = %q", firstLine(resp))
	}
	if want := "RTP/AVP/TCP;unicast;interleaved=0-1"; hd["Transport"] != want {
		t.Fatalf("Transport = %q, want %q", hd["Transport"], want)
	}
}

// readHTTPHead 读到空行为止，返回完整的 HTTP 响应头。
func readHTTPHead(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	var sb strings.Builder
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("read HTTP head: %v", err)
		}
		sb.WriteString(line)
		if line == "\r\n" {
			return sb.String()
		}
	}
}

// readRTSPMessage 从 GET 通道读一条完整的 RTSP 响应（含 Content-Length 指定的 body）。
func readRTSPMessage(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	var head strings.Builder
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("read RTSP response: %v", err)
		}
		head.WriteString(line)
		if line == "\r\n" {
			break
		}
	}
	out := head.String()
	length := 0
	for _, line := range strings.Split(out, "\r\n") {
		if strings.HasPrefix(strings.ToLower(line), "content-length:") {
			v := strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
			length, _ = strconv.Atoi(v)
		}
	}
	if length > 0 {
		body := make([]byte, length)
		if _, err := io.ReadFull(br, body); err != nil {
			t.Fatalf("read RTSP body: %v", err)
		}
		out += string(body)
	}
	return out
}

func waitClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not exit")
	}
}

// TestHTTPTunnelRoundTrip 走真实的 handleConn：HTTP 嗅探、GET/POST 配对、
// base64 上行、明文下行，以及复用原有 RTSP 协议栈的 OPTIONS/DESCRIBE/
// SETUP/PLAY/TEARDOWN。
func TestHTTPTunnelRoundTrip(t *testing.T) {
	s := New(nil, nil, true)

	getServer, getClient := net.Pipe()
	postServer, postClient := net.Pipe()
	getDone := make(chan struct{})
	postDone := make(chan struct{})
	go func() {
		defer close(getDone)
		s.handleConn(&Conn{Conn: getServer})
	}()
	go func() {
		defer close(postDone)
		s.handleConn(&Conn{Conn: postServer})
	}()
	defer func() {
		getClient.Close()
		postClient.Close()
		waitClosed(t, getDone)
		waitClosed(t, postDone)
	}()

	// 1. 打开 GET 数据通道，先拿到 HTTP 200。
	if _, err := io.WriteString(getClient,
		"GET /live HTTP/1.1\r\nHost: 127.0.0.1:8554\r\n"+
			"x-sessioncookie: testcookie\r\n"+
			"Accept: application/x-rtsp-tunnelled\r\n\r\n"); err != nil {
		t.Fatalf("write GET: %v", err)
	}
	getBr := bufio.NewReader(getClient)
	head := readHTTPHead(t, getBr)
	if !strings.HasPrefix(head, "HTTP/1.0 200 OK\r\n") {
		t.Fatalf("GET response = %q", head)
	}
	if !strings.Contains(head, "Content-Type: "+rtspTunnelContentType) {
		t.Fatalf("GET response missing tunnel content type: %q", head)
	}

	// 2. 打开 POST 命令通道。Content-Length 故意写大值，模拟 ffmpeg 的占位。
	if _, err := io.WriteString(postClient,
		"POST /live HTTP/1.1\r\nHost: 127.0.0.1:8554\r\n"+
			"x-sessioncookie: testcookie\r\n"+
			"Content-Type: application/x-rtsp-tunnelled\r\n"+
			"Content-Length: 32767\r\n\r\n"); err != nil {
		t.Fatalf("write POST: %v", err)
	}

	// 和 ffmpeg 一致：base64 后面不补换行，直接连着发下一条请求。
	send := func(t *testing.T, msg string) {
		t.Helper()
		line := base64.StdEncoding.EncodeToString([]byte(msg))
		if _, err := io.WriteString(postClient, line); err != nil {
			t.Fatalf("write tunnel data: %v", err)
		}
	}
	uri := "rtsp://127.0.0.1:8554/live"

	// OPTIONS
	send(t, "OPTIONS "+uri+" RTSP/1.0\r\nCSeq: 1\r\n\r\n")
	resp := readRTSPMessage(t, getBr)
	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") || !strings.Contains(resp, "Public: ") {
		t.Fatalf("OPTIONS response = %q", resp)
	}

	// DESCRIBE
	send(t, "DESCRIBE "+uri+" RTSP/1.0\r\nCSeq: 2\r\nAccept: application/sdp\r\n\r\n")
	resp = readRTSPMessage(t, getBr)
	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") || !strings.Contains(resp, "m=video") {
		t.Fatalf("DESCRIBE response = %q", resp)
	}

	// SETUP：故意用 UDP Transport，隧道里必须被强制改成 interleaved。
	send(t, "SETUP "+uri+"/trackID=0 RTSP/1.0\r\nCSeq: 3\r\n"+
		"Transport: RTP/AVP;unicast;client_port=5000-5001\r\n\r\n")
	resp = readRTSPMessage(t, getBr)
	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("SETUP response = %q", resp)
	}
	if !strings.Contains(resp, "interleaved=0-1") {
		t.Fatalf("SETUP transport not forced to interleaved: %q", resp)
	}
	session := responseHeader(resp, "Session")
	if session == "" {
		t.Fatalf("SETUP response missing Session: %q", resp)
	}

	// PLAY
	send(t, "PLAY "+uri+" RTSP/1.0\r\nCSeq: 4\r\nSession: "+session+"\r\n\r\n")
	resp = readRTSPMessage(t, getBr)
	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("PLAY response = %q", resp)
	}

	// TEARDOWN
	send(t, "TEARDOWN "+uri+" RTSP/1.0\r\nCSeq: 5\r\nSession: "+session+"\r\n\r\n")
	resp = readRTSPMessage(t, getBr)
	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("TEARDOWN response = %q", resp)
	}
}

// TestTunnelPOSTWithoutGET 验证 POST 找不到配对时短超时后回 404 并退出。
func TestTunnelPOSTWithoutGET(t *testing.T) {
	old := tunnelPairTimeout
	tunnelPairTimeout = 50 * time.Millisecond
	defer func() { tunnelPairTimeout = old }()

	s := New(nil, nil, false)
	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleConn(&Conn{Conn: serverConn})
	}()

	if _, err := io.WriteString(clientConn,
		"POST / HTTP/1.1\r\nHost: 127.0.0.1\r\nx-sessioncookie: missing\r\n\r\n"); err != nil {
		t.Fatalf("write POST: %v", err)
	}

	br := bufio.NewReader(clientConn)
	head := readHTTPHead(t, br)
	if !strings.HasPrefix(head, "HTTP/1.0 404 Not Found\r\n") {
		t.Fatalf("response = %q", head)
	}

	clientConn.Close()
	waitClosed(t, done)
}

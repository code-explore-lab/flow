package rtsp

import (
	"bufio"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

// RTSP over HTTP 隧道（Apple QuickTime 的 de-facto 协议，没有正式 RFC）。
//
// 客户端在 RTSP 端口上打开两条 HTTP 连接，用同一个 x-sessioncookie 关联：
//
//	GET  /  数据通道：服务端 → 客户端，明文 RTSP 响应 + $ interleaved 媒体帧
//	POST /  命令通道：客户端 → 服务端，base64 编码的 RTSP 请求流
//
// 注意方向是不对称的：只有 POST 方向是 base64，GET 方向是明文。ffmpeg
// -rtsp_transport http、live555、GStreamer 都按这个实现；FFmpeg 的
// ff_rtsp_read_reply_internal 逐字节读 GET 流并按明文 RTSP 解析，把下行也
// base64 会让它把编码文本当成 RTSP 响应。
//
// 上行也不能按行读：FFmpeg 直接写 av_base64_encode 的结果且不补换行，多个
// 请求的编码首尾相接（ff_rtsp_send_cmd_with_content_async）。
const (
	// rtspTunnelContentType 是隧道约定的 MIME 类型。
	rtspTunnelContentType = "application/x-rtsp-tunnelled"

	// tunnelPollInterval 是 POST 等待 GET 登记时的轮询间隔。
	tunnelPollInterval = 10 * time.Millisecond
)

// rtspMethodPrefixes 是 RTSP 方法加一个空格的前缀，用于判断明文请求行。
var rtspMethodPrefixes = [...]string{
	"OPTIONS ", "DESCRIBE ", "ANNOUNCE ", "SETUP ", "PLAY ", "PAUSE ",
	"RECORD ", "TEARDOWN ", "GET_PARAMETER ", "SET_PARAMETER ",
}

// tunnelPairTimeout 是 POST 等待同名 GET 的最长时间。正常情况下客户端
// 先打开 GET 再打开 POST，两者几乎同时到达；留出几秒容忍慢速网络。
// 变量形式便于测试缩短等待。
var tunnelPairTimeout = 5 * time.Second

// httpTunnel 是一对通过 x-sessioncookie 关联的 HTTP 连接。
type httpTunnel struct {
	cookie  string
	getConn *Conn // GET 连接，所有下行数据的写入目标

	mu       sync.Mutex
	postConn net.Conn // 配对后的 POST 连接，未配对时为 nil
	closed   bool
}

// close 关闭隧道两侧的连接，幂等。
func (t *httpTunnel) close() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	get, post := t.getConn, t.postConn
	t.mu.Unlock()

	if get != nil {
		get.Close()
	}
	if post != nil {
		post.Close()
	}
}

// attachPost 绑定命令通道。隧道已关闭或已经绑定过时返回 false。
func (t *httpTunnel) attachPost(c net.Conn) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.postConn != nil {
		return false
	}
	t.postConn = c
	return true
}

// addTunnel 登记 GET 通道；同一个 cookie 只允许一条 GET。
func (s *Server) addTunnel(t *httpTunnel) bool {
	s.tunnelsMu.Lock()
	defer s.tunnelsMu.Unlock()
	if _, exists := s.tunnels[t.cookie]; exists {
		return false
	}
	s.tunnels[t.cookie] = t
	return true
}

// removeTunnel 摘掉登记，只删除仍指向 t 的条目（防止误删后来的同 cookie 隧道）。
func (s *Server) removeTunnel(t *httpTunnel) {
	s.tunnelsMu.Lock()
	if s.tunnels[t.cookie] == t {
		delete(s.tunnels, t.cookie)
	}
	s.tunnelsMu.Unlock()
}

// waitTunnel 等 GET 通道登记。POST 可能先于 GET 到达（取决于客户端实现），
// 所以短暂轮询而不是直接 404。
func (s *Server) waitTunnel(cookie string, timeout time.Duration) *httpTunnel {
	deadline := time.Now().Add(timeout)
	for {
		s.tunnelsMu.Lock()
		t := s.tunnels[cookie]
		s.tunnelsMu.Unlock()
		if t != nil {
			return t
		}
		if !time.Now().Before(deadline) {
			return nil
		}
		time.Sleep(tunnelPollInterval)
	}
}

// peekHTTPMethod 判断连接开头是不是 HTTP 隧道请求。
// 返回 "GET " 或 "POST"；不是隧道请求时返回 ("", false)。
func peekHTTPMethod(br *bufio.Reader) (string, bool) {
	b, err := br.Peek(4)
	if err != nil {
		return "", false
	}
	switch string(b) {
	case "GET ", "POST":
		return string(b), true
	}
	return "", false
}

func tunnelLog(format string, args ...any) {
	fmt.Printf("%s http tunnel: %s\n", time.Now().Format("15:04:05.000"), fmt.Sprintf(format, args...))
}

// handleHTTPTunnel 处理一条已经嗅探为 HTTP 的连接。
func (s *Server) handleHTTPTunnel(conn *Conn, br *bufio.Reader, method string) {
	// 只解析 HTTP 头。POST 的 Content-Length 是客户端随手写的占位值
	// （ffmpeg 固定 32767），后面的 base64 流会超过它，所以不能用 req.Body。
	req, err := http.ReadRequest(br)
	if err != nil {
		tunnelLog("read request: %v", err)
		return
	}
	cookie := req.Header.Get("x-sessioncookie")
	if cookie == "" {
		writeHTTPStatus(conn, http.StatusBadRequest, "missing x-sessioncookie")
		return
	}
	tunnelLog("%s %s from %s, cookie=%q", method, req.URL, conn.RemoteAddr(), cookie)

	switch method {
	case "GET ":
		s.handleTunnelGET(conn, br, cookie)
	case "POST":
		s.handleTunnelPOST(conn, br, cookie)
	}
}

// handleTunnelGET 处理数据通道：回 200 后保持连接，只负责承载下行字节。
func (s *Server) handleTunnelGET(conn *Conn, br *bufio.Reader, cookie string) {
	t := &httpTunnel{cookie: cookie, getConn: conn}

	// 先写 200 再登记：POST 只有登记后才能配对开始写 RTSP，
	// 这样能保证 GET 响应体里 HTTP 头永远排在第一条 RTSP 响应之前。
	// 不写 Content-Length——响应体是无限长的 RTSP/RTP 字节流。
	hdr := "HTTP/1.0 200 OK\r\n" +
		"Content-Type: " + rtspTunnelContentType + "\r\n" +
		"Cache-Control: no-cache\r\n" +
		"Pragma: no-cache\r\n" +
		"Expires: Sun, 9 Jan 1972 00:00:00 GMT\r\n" +
		"\r\n"
	if err := conn.write([]byte(hdr)); err != nil {
		return
	}
	if !s.addTunnel(t) {
		tunnelLog("duplicate cookie %q", cookie)
		return
	}
	defer s.removeTunnel(t)
	defer t.close()

	// 数据通道正常情况下只有下行数据；这里读是为了感知客户端关闭
	// （无论是客户端直接关，还是 POST 侧结束时 t.close() 关掉这条连接）。
	// 少数客户端会在这里回 RTCP；隧道模式主要靠 RTSP 请求保活，忽略无妨。
	buf := make([]byte, 1024)
	for {
		if _, err := br.Read(buf); err != nil {
			tunnelLog("GET %q closed: %v", cookie, err)
			return
		}
	}
}

// handleTunnelPOST 处理命令通道：把解码后的 RTSP 请求喂给完整的协议栈。
func (s *Server) handleTunnelPOST(conn *Conn, br *bufio.Reader, cookie string) {
	t := s.waitTunnel(cookie, tunnelPairTimeout)
	if t == nil || !t.attachPost(conn) {
		tunnelLog("POST cookie=%q has no GET pair", cookie)
		writeHTTPStatus(conn, http.StatusNotFound, "no matching tunnel session")
		return
	}
	tunnelLog("paired cookie=%q", cookie)
	// Apple 隧道协议里 POST 通道没有 HTTP 响应：客户端打开后直接开始
	// 写 base64 行，服务端只在 GET 通道上回 RTSP 响应。

	defer s.removeTunnel(t)
	defer t.close()
	// 复用完整的 RTSP 协议栈——鉴权、CSeq、会话、SETUP/PLAY 全部不变，
	// 只是字节来源换成了 POST 的解码结果，写目标换成了 GET。
	s.handleConn(&Conn{Conn: &tunnelConn{tunnel: t, post: conn, br: br}, tunnel: true})
}

// writeHTTPStatus 在隧道协商失败时回一个普通 HTTP 错误并关闭。
func writeHTTPStatus(conn *Conn, code int, msg string) {
	body := msg + "\n"
	hdr := fmt.Sprintf("HTTP/1.0 %d %s\r\nContent-Type: text/plain\r\nContent-Length: %d\r\nConnection: close\r\n\r\n",
		code, http.StatusText(code), len(body))
	_ = conn.write([]byte(hdr + body))
}

// tunnelConn 把隧道两半拼成一个 net.Conn 给 RTSP 协议栈使用：
//   - Read:  从 POST 连接读 base64 流并解码；
//   - Write: 明文写进 GET 连接的响应体。
//
// 上行方向不能按行读：ffmpeg 每次写入的是 av_base64_encode 的裸结果，不补
// 换行，多个请求的编码首尾相接（见 FFmpeg rtsp.c 的
// ff_rtsp_send_cmd_with_content_async）。所以按 4 字符块流式解码；CR/LF/
// 空格直接跳过，兼容按行编码的客户端。编码后的 RTSP 消息长度总是 4 的倍数
// （不足补 '='），块边界不会错位。
//
// 有了它，handleConn 及所有处理方法完全不需要知道 HTTP 隧道的存在。
type tunnelConn struct {
	tunnel *httpTunnel
	post   net.Conn
	br     *bufio.Reader

	pending []byte // 已解码但还没被 Read 取走的字节
	group   [4]byte
	groupN  int  // group 里已填的 base64 字符数
	raw     bool // 客户端在 POST 里直接发明文 RTSP，后续字节全部透传
	checked bool // 是否已经做过明文/ base64 判断
}

func (c *tunnelConn) Read(p []byte) (int, error) {
	if !c.checked {
		c.checked = true
		c.raw = c.startsAsPlainRTSP()
	}
	if c.raw {
		return c.br.Read(p)
	}
	for len(c.pending) == 0 {
		if err := c.decodeBlock(); err != nil {
			return 0, err
		}
	}

	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

// startsAsPlainRTSP 判断 POST 的开头是不是明文 RTSP 请求行。
// 读满最长的方法前缀即可判断；任何真实请求的 base64 都远长于这几个字节，
// 不会因为等待这点数据把正常客户端卡住。
func (c *tunnelConn) startsAsPlainRTSP() bool {
	peek, err := c.br.Peek(9)
	if len(peek) == 0 && err != nil {
		return false
	}
	for _, m := range rtspMethodPrefixes {
		if len(peek) >= len(m) && string(peek[:len(m)]) == m {
			return true
		}
	}
	return false
}

// decodeBlock 读入 base64 字符，凑满 4 个就解码一块放进 pending。
// 空白字符跳过；块边界上的 '$' 说明客户端直接写入了 interleaved 帧（例如
// TCP 模式下的 RTCP），原样向上传递。
func (c *tunnelConn) decodeBlock() error {
	b, err := c.br.ReadByte()
	if err != nil {
		return err
	}
	if b == '\r' || b == '\n' || b == ' ' || b == '\t' {
		return nil
	}
	if b == interleavedMagic && c.groupN == 0 {
		// '$' 不在 base64 字母表里，只可能出现在块边界。
		var hdr [3]byte // channel(1) + length(2, 大端)
		if _, err := io.ReadFull(c.br, hdr[:]); err != nil {
			return err
		}
		length := int(binary.BigEndian.Uint16(hdr[1:3]))
		payload := make([]byte, length)
		if _, err := io.ReadFull(c.br, payload); err != nil {
			return err
		}
		frame := make([]byte, 4+length)
		frame[0] = interleavedMagic
		frame[1] = hdr[0]
		copy(frame[2:4], hdr[1:3])
		copy(frame[4:], payload)
		c.pending = frame
		return nil
	}

	c.group[c.groupN] = b
	c.groupN++
	if c.groupN < 4 {
		return nil
	}
	decoded, derr := base64.StdEncoding.DecodeString(string(c.group[:]))
	c.groupN = 0
	if derr != nil {
		return fmt.Errorf("http tunnel: bad base64 block %q: %w", c.group[:], derr)
	}
	c.pending = decoded
	return nil
}

func (c *tunnelConn) Write(p []byte) (int, error) {
	if err := c.tunnel.getConn.write(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *tunnelConn) Close() error {
	c.tunnel.close()
	return nil
}

func (c *tunnelConn) LocalAddr() net.Addr  { return c.post.LocalAddr() }
func (c *tunnelConn) RemoteAddr() net.Addr { return c.post.RemoteAddr() }

func (c *tunnelConn) SetDeadline(t time.Time) error {
	if err := c.post.SetDeadline(t); err != nil {
		return err
	}
	return c.tunnel.getConn.SetDeadline(t)
}

func (c *tunnelConn) SetReadDeadline(t time.Time) error  { return c.post.SetReadDeadline(t) }
func (c *tunnelConn) SetWriteDeadline(t time.Time) error { return c.tunnel.getConn.SetWriteDeadline(t) }

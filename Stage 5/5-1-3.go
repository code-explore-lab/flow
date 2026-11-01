package Stage5

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"flow/internal/media"
	"flow/internal/rtcp"
	"flow/internal/rtp"
	"flow/internal/session"
	"flow/internal/source"
)

// mediaTrack 一条媒体轨（video/audio）的传输参数。
// 3.2.5：一个 Session 下有两条轨，各自一对端口、各自的目标地址。
type mediaTrack struct {
	kind            string // "video" / "audio"
	isInterleaved   bool
	interleavedCh   string       // 原始文本，如 "0-1"
	interleavedRTP  int          // RTP channel（4.2.3 写 $ 帧时用）
	interleavedRTCP int          // RTCP channel
	conn            *rtspConn    // TCP 模式下共用的 RTSP 连接（UDP 模式为 nil）
	rtpConn         *net.UDPConn // 服务端 RTP socket（用来发）
	rtcpConn        *net.UDPConn // 服务端 RTCP socket（用来收发）
	clientAddr      *net.UDPAddr // 客户端 RTP 目标地址 ip:port
	clientRTCPAddr  *net.UDPAddr // 客户端 RTCP 目标地址 ip:port（= client_port+1）
	serverRtpPort   int          // 偶数
	serverRtcpPort  int          // 奇数 = RTP+1

	// 5.1.3：RTCP SR 的发送者状态。clockRate 是 RTP 时间戳的时钟频率
	// （视频固定 90000，音频 = AAC 采样率），stats 由推流 goroutine 更新、
	// SR goroutine 每 5s 读一次。
	clockRate uint32
	stats     trackStats
}

// trackStats 记录这条轨已经发了多少 RTP，供 RTCP SR 填入 packet/octet count
// 和 NTP↔RTP 映射。推流 goroutine 写、SR goroutine 读，用锁串行化。
type trackStats struct {
	mu       sync.Mutex
	ssrc     uint32
	packets  uint32
	octets   uint32
	lastRTP  uint32
	lastSent time.Time
}

// setSSRC 在推流 goroutine 启动时登记本路的 SSRC，SR 里必须用同一个值。
func (t *mediaTrack) setSSRC(ssrc uint32) {
	t.stats.mu.Lock()
	t.stats.ssrc = ssrc
	t.stats.mu.Unlock()
}

// currentSSRC 返回本路已登记的 SSRC（RTCP SR 和日志用）。
func (t *mediaTrack) currentSSRC() uint32 {
	t.stats.mu.Lock()
	defer t.stats.mu.Unlock()
	return t.stats.ssrc
}

// setClockRate 设置 RTP 时钟频率（音频源解析出采样率后调用）。
func (t *mediaTrack) setClockRate(rate uint32) {
	if rate == 0 {
		return
	}
	t.stats.mu.Lock()
	t.clockRate = rate
	t.stats.mu.Unlock()
}

// recordSent 在一包 RTP 成功写出后更新统计：
// 包数、负载字节数（不含 RTP 头，RFC 3550 的 octet count 定义）、
// 最后一个时间戳及其发送时刻（用来推算 SR 的 RTP timestamp）。
func (t *mediaTrack) recordSent(rtpTS uint32, payloadLen int) {
	t.stats.mu.Lock()
	t.stats.packets++
	t.stats.octets += uint32(payloadLen)
	t.stats.lastRTP = rtpTS
	t.stats.lastSent = time.Now()
	t.stats.mu.Unlock()
}

// rtspConn 包装一条 TCP 连接，串行化所有写操作。
// RTSP 文本响应和 interleaved $ 帧共用同一条字节流，
// 音视频两个推流 goroutine 如果各自直接 conn.Write 会互相穿插，所以统一走这把锁。
type rtspConn struct {
	net.Conn
	mu sync.Mutex

	// 这条连接上建立/复用过的会话 ID。连接断开时（客户端 kill -9、网线拔掉）
	// handleConn 退出，顺手把这些会话回收掉，不用干等 60s 超时。
	sessMu     sync.Mutex
	sessionIDs []string // 记录这条TCP链接上绑定了哪些RTSP Session
}

func (c *rtspConn) write(b []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.Conn.Write(b)
	return err
}

// bindSession 记下这条 TCP 连接用到的会话（一个连接可以带多个会话）。
func (c *rtspConn) bindSession(id string) {
	c.sessMu.Lock()
	defer c.sessMu.Unlock()
	for _, v := range c.sessionIDs {
		if v == id {
			return
		}
	}
	c.sessionIDs = append(c.sessionIDs, id)
}

// boundSessions 返回这条连接上的会话 ID 快照。
func (c *rtspConn) boundSessions() []string {
	c.sessMu.Lock()
	defer c.sessMu.Unlock()
	out := make([]string, len(c.sessionIDs))
	copy(out, c.sessionIDs)
	return out
}

// writeRTP 按这条轨的传输方式发送一个已经序列化好的 RTP 包：
// UDP 模式直接 WriteToUDP，TCP 模式套上 $ 帧再写。
func (t *mediaTrack) writeRTP(pkt []byte) error {
	if t.isInterleaved {
		return t.writeInterleavedChannel(t.interleavedRTP, pkt)
	}
	_, err := t.rtpConn.WriteToUDP(pkt, t.clientAddr)
	return err
}

// writeRTCP 按这条轨的传输方式发送 RTCP 包：
// UDP 模式发给客户端的 RTCP 端口（client_port+1），
// TCP 模式走协商出来的 RTCP channel（ffmpeg 下视频=1、音频=3）。
func (t *mediaTrack) writeRTCP(pkt []byte) error {
	if t.isInterleaved {
		return t.writeInterleavedChannel(t.interleavedRTCP, pkt)
	}
	if t.rtcpConn == nil || t.clientRTCPAddr == nil {
		return fmt.Errorf("rtcp transport is not configured")
	}
	_, err := t.rtcpConn.WriteToUDP(pkt, t.clientRTCPAddr)
	return err
}

// writeInterleavedChannel 把一个 RTCP/RTP 包按 $ 帧格式写到 RTSP 的 TCP 连接：
//
//	$ + channel(1) + length(2, 大端) + packet
//
// channel 用 SETUP 协商出来的 interleavedRTP / interleavedRTCP。
func (t *mediaTrack) writeInterleavedChannel(channel int, pkt []byte) error {
	if len(pkt) > 0xFFFF {
		return fmt.Errorf("packet too large for interleaved: %d bytes", len(pkt))
	}
	if t.conn == nil {
		return fmt.Errorf("interleaved track has no tcp conn")
	}
	frame := make([]byte, 4+len(pkt))
	frame[0] = interleavedMagic // $ 0x24
	frame[1] = byte(channel)    // 网络协议大端序
	binary.BigEndian.PutUint16(frame[2:4], uint16(len(pkt)))
	copy(frame[4:], pkt)
	return t.conn.write(frame)
}

// senderReport 生成当前时刻的 RTCP SR（PT=200）。
// NTP 填 now，RTP timestamp 填“如果现在发一包会用到的值”：
// 上一包的 RTP timestamp + 距上一包发送时刻经过的时间 × 时钟频率。
// 还没发过任何 RTP 时返回 false，避免客户端拿到错误的时间映射
// （尤其是中途加入的客户端，第一包时间戳不是 0）。
func (t *mediaTrack) senderReport(now time.Time) ([]byte, bool) {
	t.stats.mu.Lock()
	ssrc := t.stats.ssrc
	packets := t.stats.packets
	octets := t.stats.octets
	rtp := t.stats.lastRTP
	lastSent := t.stats.lastSent
	clockRate := t.clockRate
	t.stats.mu.Unlock()

	if lastSent.IsZero() {
		return nil, false
	}
	if elapsed := now.Sub(lastSent); elapsed > 0 && clockRate > 0 {
		rtp += uint32(elapsed.Seconds() * float64(clockRate))
	}
	sr := rtcp.SenderReport{
		SSRC:        ssrc,
		NTP:         rtcp.NTPTime(now),
		RTP:         rtp,
		PacketCount: packets,
		OctetCount:  octets,
	}
	return sr.Marshal(), true
}

// sendSenderReports 每 5 秒给这条轨的客户端发一次 SR（5.1.3）。
// 播放器靠 NTP↔RTP 映射做音视频同步；有些播放器收不到 SR 会判定
// 发送方失联而停止播放。会话关闭（TEARDOWN / 超时 / TCP 断开）时
// done 关闭，goroutine 跟着退出。
func sendSenderReports(track *mediaTrack, done <-chan struct{}) {
	const interval = 5 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	fmt.Printf("RTCP SR sender started (%s, %s)\n", track.kind, describeTransport(track))
	for {
		select {
		case <-done:
			fmt.Printf("RTCP SR sender stopped (%s)\n", track.kind)
			return
		case now := <-ticker.C:
			sr, ok := track.senderReport(now)
			if !ok {
				continue // 还没发出任何 RTP，等下一轮
			}
			if err := track.writeRTCP(sr); err != nil {
				fmt.Printf("write rtcp sr (%s): %v\n", track.kind, err)
				return
			}
			fmt.Printf("RTCP SR sent (%s, ssrc=%08x, %d bytes)\n",
				track.kind, track.currentSSRC(), len(sr))
		}
	}
}

func describeTransport(t *mediaTrack) string {
	if t.isInterleaved {
		return fmt.Sprintf("tcp ch=%d", t.interleavedRTCP)
	}
	return fmt.Sprintf("udp %s", t.clientRTCPAddr)
}

// rtspSession 是挂在 session.Session.Data 上的 RTSP 层状态：
// 一个会话下的 video/audio 两条轨。
// internal/session.Manager 只管生命周期（ID、状态、超时），不关心这些传输细节。
type rtspSession struct {
	mu     sync.Mutex
	tracks map[string]*mediaTrack // key: "video" / "audio"

	// 5.1.3：同一个会话只起一组 RTCP SR goroutine，
	// PAUSE 之后再 PLAY 不会重复起（两路各一个）。
	srOnce sync.Once
}

func newRtspSession() *rtspSession {
	return &rtspSession{tracks: make(map[string]*mediaTrack)}
}

// setTrack 登记一条轨，返回被替换掉的旧轨（重复 SETUP 同一条轨时用来关旧端口）。
func (s *rtspSession) setTrack(kind string, t *mediaTrack) *mediaTrack {
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.tracks[kind]
	s.tracks[kind] = t
	return old
}

func (s *rtspSession) track(kind string) *mediaTrack {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tracks[kind]
}

// closeAll 关掉会话下所有轨的 UDP socket（TEARDOWN / 超时回收时由 OnClose 调用）。
func (s *rtspSession) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.tracks {
		if t.rtpConn != nil {
			t.rtpConn.Close()
		}
		if t.rtcpConn != nil {
			t.rtcpConn.Close()
		}
	}
}

// hasRTCPChannel 判断 channel 是不是这个会话某条轨的 RTCP 通道。
// interleaved 模式下客户端发来的 RTCP RR 就走这个 channel。
func (s *rtspSession) hasRTCPChannel(channel int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.tracks {
		if t.isInterleaved && t.interleavedRTCP == channel {
			return true
		}
	}
	return false
}

// sessionMgr 全局会话表：map[string]*Session，由 internal/session.Manager 维护。
// 后台每 30s 扫一次，超过 60s 没有任何 RTCP/RTSP 活动的会话自动 TEARDOWN。
var sessionMgr = session.NewManager(session.DefaultTimeout)

// 5.1.2：单源多消费者。整个进程只有一份视频源/音频源，
// 多个客户端同时 PLAY 时共享同一个源 goroutine，各自订阅一个 channel。
var (
	h264Source = source.NewH264Source("testdata/test.h264")
	aacSource  = source.NewAACSource("testdata/test.aac")
)

// sessionHeaderValue 拼 SETUP/PLAY/PAUSE/TEARDOWN 响应里的 Session 头。
func sessionHeaderValue(sess *session.Session) string {
	return fmt.Sprintf("%s;timeout=%d", sess.ID, int(sessionMgr.Timeout()/time.Second))
}

type RtspRequest struct {
	Method  string
	URI     string
	Version string
	CSeq    int
	Headers map[string]string
}

// 解析header文本块
func parseHeaderBlock(block string) (*RtspRequest, error) {
	lines := strings.Split(block, "\r\n")
	if len(lines) == 0 {
		return nil, fmt.Errorf("empty header block")
	}
	//Method uri rtsp/1.0
	reqLine := strings.Fields(lines[0])
	if len(reqLine) != 3 {
		return nil, fmt.Errorf("invalid request line: %s", lines[0])
	}
	req := &RtspRequest{
		Method:  reqLine[0],
		URI:     reqLine[1],
		Version: reqLine[2],
		Headers: make(map[string]string),
	}
	//parse Header
	for _, rawline := range lines[1:] {
		line := strings.TrimSpace(rawline)
		if line == "" {
			continue
		}
		kv := strings.SplitN(line, ":", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])
		req.Headers[key] = val
	}
	// get CSeq
	if cseq, ok := req.Headers["CSeq"]; ok {
		c, err := strconv.Atoi(cseq)
		if err != nil {
			return nil, fmt.Errorf("invalid CSeq value: %s", cseq)
		}
		req.CSeq = c
	}
	return req, nil
}

// 读到\r\n 返回RtspRequest。
// 4.2.2：reader 由 handleConn 统一持有，不能每次新建，
// 否则 bufio 里已缓冲的字节（可能正好是下一个 $ 帧）会被丢掉。
func readRequest(br *bufio.Reader) (*RtspRequest, error) {
	var buf []byte
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		buf = append(buf, line...)
		if len(buf) >= 4 && string(buf[len(buf)-4:]) == "\r\n\r\n" {
			break
		}
	}
	// 切掉末尾的\r\n\r\n
	headerRaw := string(buf[:len(buf)-4])
	return parseHeaderBlock(headerRaw)
}

// writeResponse
// status: "200 OK" / "501 Not Implemented"
// headers: 响应头map，CSeq在这里传入
// body: 可选，SDP文本等；非空自动填充Content-Length

func writeResponse(conn *rtspConn, status string, headers map[string]string, body []byte) error {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("RTSP/1.0 %s\r\n", status))
	if len(body) > 0 {
		headers["Content-Length"] = strconv.Itoa(len(body))
	}
	for k, v := range headers {
		sb.WriteString(fmt.Sprintf("%s: %s\r\n", k, v))
	}
	sb.WriteString("\r\n")
	out := []byte(sb.String())
	out = append(out, body...)

	// 新增打印
	fmt.Printf("---- RESPONSE ----\n%s\n------------------\n", string(out))

	return conn.write(out)
}

func extractTransportInfo(transportStr string) (isInterleaved bool, interStr string, clientPort string, err error) {
	parts := strings.Split(transportStr, ";")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if strings.HasPrefix(p, "interleaved=") {
			isInterleaved = true
			interStr = strings.TrimPrefix(p, "interleaved=")
		}
		if strings.HasPrefix(p, "client_port=") {
			clientPort = strings.TrimPrefix(p, "client_port=")
		}
	}
	if !isInterleaved && clientPort == "" {
		return false, "", "", fmt.Errorf("transport missing interleaved or client_port")
	}
	return isInterleaved, interStr, clientPort, nil
}

// parseInterleaved 解析 Transport 里的 interleaved=0-1，返回 RTP / RTCP 两个 channel 号。
// 4.2.3 推流时要把 channel 号写进 $ 帧头，所以这里先解析并存好。
func parseInterleaved(s string) (rtpCh, rtcpCh int, err error) {
	parts := strings.Split(s, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid interleaved value %q, want RTP-RTCP", s)
	}
	rtpCh, err = strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return 0, 0, fmt.Errorf("invalid interleaved RTP channel %q", parts[0])
	}
	rtcpCh, err = strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return 0, 0, fmt.Errorf("invalid interleaved RTCP channel %q", parts[1])
	}
	// $ 帧的 channel 只占 1 字节
	if rtpCh < 0 || rtpCh > 255 || rtcpCh < 0 || rtcpCh > 255 {
		return 0, 0, fmt.Errorf("interleaved channel out of range: %d-%d", rtpCh, rtcpCh)
	}
	return rtpCh, rtcpCh, nil
}

// parseClientPorts 解析 Transport 里的 client_port=RTP-RTCP。
// 标准写法两个端口都给；只给一个时按 RFC 3550 约定 RTCP = RTP+1。
func parseClientPorts(s string) (rtpPort, rtcpPort int) {
	parts := strings.Split(s, "-")
	rtpPort, _ = strconv.Atoi(strings.TrimSpace(parts[0]))
	if len(parts) > 1 {
		rtcpPort, _ = strconv.Atoi(strings.TrimSpace(parts[1]))
	}
	if rtcpPort == 0 {
		rtcpPort = rtpPort + 1
	}
	return rtpPort, rtcpPort
}

// trackKindFromURI 从 SETUP 的 URI 判断这次要建哪条轨。
// 我们在 DESCRIBE 里下发了 a=control:trackID=0（video）/ trackID=1（audio）；
// 不认 a=control 的客户端会退化成 .../streamid=0、streamid=1，同样映射。
// 都识别不出来返回空串，由调用方按 SETUP 顺序推断。
func trackKindFromURI(uri string) string {
	switch {
	case strings.Contains(uri, "trackID=0"), strings.Contains(uri, "streamid=0"):
		return "video"
	case strings.Contains(uri, "trackID=1"), strings.Contains(uri, "streamid=1"):
		return "audio"
	}
	return ""
}

// interleavedMagic 是 RTSP interleaved 二进制帧的起始字节 '$'。
const interleavedMagic = '$'

// readInterleavedFrame 从同一个 TCP 字节流里切出一个 $ 帧：
//
//	$ + channel(1) + length(2, 大端) + payload
//
// 注意 length 只算 payload，不含这 4 字节头。
func readInterleavedFrame(br *bufio.Reader) (channel byte, payload []byte, err error) {
	var hdr [4]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return 0, nil, err
	}
	if hdr[0] != interleavedMagic {
		return 0, nil, fmt.Errorf("bad interleaved magic: %#x", hdr[0])
	}
	length := binary.BigEndian.Uint16(hdr[2:4])
	payload = make([]byte, length)
	if _, err := io.ReadFull(br, payload); err != nil {
		return 0, nil, err
	}
	return hdr[1], payload, nil
}

// handleInterleaved 处理客户端通过 TCP 发来的 interleaved 帧。
// 客户端主要用它发 RTCP（RR/SDES 等）：
//   - 5.1.1 用它刷新会话活跃时间，超时回收靠它；
//   - 真正的 RTCP 内容解析（SR/RR 统计）放到 5.1.3。
func handleInterleaved(conn *rtspConn, channel byte, payload []byte) {
	if len(payload) < 2 {
		return
	}
	// RTCP 包类型在第二个字节（第一字节是 V/P/RC），200~204 是 SR/RR/SDES/BYE/APP
	pt := payload[1]
	if pt < 200 || pt > 204 {
		fmt.Printf("interleaved recv: channel=%d len=%d (not rtcp)\n", channel, len(payload))
		return
	}
	for _, id := range conn.boundSessions() {
		sess, ok := sessionMgr.Get(id)
		if !ok {
			continue
		}
		st := sess.Data.(*rtspSession)
		if st.hasRTCPChannel(int(channel)) {
			sess.TouchRTCP()
			fmt.Printf("RTCP recv: channel=%d len=%d pt=%d (session=%s)\n",
				channel, len(payload), pt, id)
		}
	}
}

func handleConn(conn *rtspConn) {
	defer func() {
		conn.Close()
		// 连接断了说明客户端没了：立刻回收这条连接上的会话，
		// 而不是干等 30s 扫描 + 60s 超时（最坏 90s）。
		for _, id := range conn.boundSessions() {
			if sess, ok := sessionMgr.Get(id); ok {
				sessionMgr.Delete(id)
				sess.Close()
				fmt.Printf("session %s closed with tcp conn\n", id)
			}
		}
	}()
	// 4.2.2：整个连接共用一个 reader，RTSP 文本和 $ 二进制帧都从它读
	br := bufio.NewReader(conn)
	for {
		// 控制连接静默超过「会话超时 + 一次扫描周期」才算连接异常：
		// 会话回收由 session.Manager 负责，这里只是兜底，防止对端半开时 goroutine 永久挂住。
		// 不能像 4.x 那样写死 30s —— 那会在会话 60s 超时之前就把连接掐掉，timeout 形同虚设。
		_ = conn.SetReadDeadline(time.Now().Add(sessionMgr.Timeout() + session.DefaultScanInterval))

		// 先 peek 一个字节做分支：'$' 走 interleaved，字母走 RTSP 文本
		first, err := br.Peek(1)
		if err != nil {
			fmt.Println("read conn err:", err)
			return
		}
		if first[0] == interleavedMagic {
			ch, payload, err := readInterleavedFrame(br)
			if err != nil {
				fmt.Println("read interleaved err:", err)
				return
			}
			handleInterleaved(conn, ch, payload)
			continue // 处理完继续读，可能紧跟一个 RTSP 请求
		}

		req, err := readRequest(br)
		if err != nil {
			fmt.Println("readRequest err:", err)
			return
		}
		fmt.Printf("recv: %s %s, CSeq=%d\n", req.Method, req.URI, req.CSeq)
		hd := make(map[string]string)
		hd["CSeq"] = strconv.Itoa(req.CSeq)
		switch req.Method {
		case "OPTIONS":
			hd["Public"] = "OPTIONS, DESCRIBE, SETUP, PLAY, PAUSE, TEARDOWN"
			err = writeResponse(conn, "200 OK", hd, nil)
		case "DESCRIBE": //SDP：一个 SDP 里两条 m= 行（video + audio）
			hd["Content-Type"] = "application/sdp"
			spspps, errSPS := media.GetSPSPPSBase64("testdata/test.h264")
			if errSPS != nil {
				err = writeResponse(conn, "500 Internal Server Error", hd, nil)
				break
			}
			// 3.2.2/3.2.4：从 test.aac 首帧动态拿采样率、声道数和 AudioSpecificConfig
			aacInfo, errAAC := media.GetAACInfo("testdata/test.aac")
			if errAAC != nil {
				err = writeResponse(conn, "500 Internal Server Error", hd, nil)
				break
			}
			sdp := "v=0\r\n" +
				"o=- 0 0 IN IP4 127.0.0.1\r\n" +
				"s=TestStream\r\n" +
				"c=IN IP4 127.0.0.1\r\n" +
				"t=0 0\r\n" +
				"m=video 0 RTP/AVP 96\r\n" +
				"a=rtpmap:96 H264/90000\r\n" +
				"a=fmtp:96 packetization-mode=1; sprop-parameter-sets=" + spspps + "\r\n" +
				"a=control:trackID=0\r\n" +
				aacInfo.MediaDescription(media.AACPT) +
				"a=control:trackID=1\r\n"
			err = writeResponse(conn, "200 OK", hd, []byte(sdp))
		case "SETUP":
			transReq, ok := req.Headers["Transport"]
			if !ok {
				err = writeResponse(conn, "400 Bad Request", hd, nil)
				break
			}
			isInter, interStr, clientPortStr, errPort := extractTransportInfo(transReq)
			if errPort != nil {
				err = writeResponse(conn, "400 Bad Request", hd, nil)
				break
			}

			// 3.2.5：第二次 SETUP 会带回第一次下发的 SessionID，必须复用它，不能新建会话
			var sess *session.Session
			if sh, ok := req.Headers["Session"]; ok {
				sessID := strings.Split(sh, ";")[0]
				var found bool
				sess, found = sessionMgr.Get(sessID)
				if !found {
					err = writeResponse(conn, "454 Session Not Found", hd, nil)
					break
				}
				sess.TouchRTSP()
			} else {
				ip, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
				sess = sessionMgr.NewSession(ip)
				st := newRtspSession()
				sess.Data = st
				sess.OnClose = st.closeAll
				fmt.Printf("new session %s from %s\n", sess.ID, ip)
			}
			// 记下会话属于哪条 TCP 连接，连接断开时一起回收
			conn.bindSession(sess.ID)
			rtspSess := sess.Data.(*rtspSession)

			// 判断这次 SETUP 建哪条轨：优先看 URI 里的 trackID/streamid，
			// 客户端没带就按“video 先、audio 后”的顺序推断
			kind := trackKindFromURI(req.URI)
			if kind == "" {
				kind = "video"
				if rtspSess.track("video") != nil {
					kind = "audio"
				}
			}

			// 5.1.3：RTCP SR 的 RTP 时钟频率。视频固定 90kHz；
			// 音频是 AAC 采样率，streamAAC 拿到第一帧后会按实际值覆盖。
			clockRate := uint32(90000)
			if kind == "audio" {
				clockRate = 44100
			}
			t := &mediaTrack{
				kind:          kind,
				isInterleaved: isInter,
				interleavedCh: interStr,
				conn:          conn,
				clockRate:     clockRate,
			}
			var respTransport string
			if isInter {
				// 4.2.1：TCP interleaved 模式 —— 不开任何 UDP 端口，只记录 channel 对。
				// 4.2.3 会往同一条 TCP 连接里写 $<channel><len><rtp>。
				rtpCh, rtcpCh, errCh := parseInterleaved(interStr)
				if errCh != nil {
					err = writeResponse(conn, "400 Bad Request", hd, nil)
					break
				}
				t.interleavedRTP = rtpCh
				t.interleavedRTCP = rtcpCh
				respTransport = fmt.Sprintf("RTP/AVP/TCP;unicast;interleaved=%d-%d", rtpCh, rtcpCh)
			} else {
				rtpConn, rtcpConn, errUDP := openUDPPortPair()
				if errUDP != nil {
					err = writeResponse(conn, "500 Internal Server Error", hd, nil)
					break
				}
				// 客户端目标地址：client_port 一般是 "RTP-RTCP"，RTCP 端口 = RTP+1。
				// 5.1.3 要把 SR 发给客户端的 RTCP 端口，所以两个端口都要记下来。
				cRtpPort, cRtcpPort := parseClientPorts(clientPortStr)
				t.rtpConn = rtpConn
				t.rtcpConn = rtcpConn
				t.clientAddr = &net.UDPAddr{IP: net.ParseIP(sess.RemoteIP), Port: cRtpPort}
				t.clientRTCPAddr = &net.UDPAddr{IP: net.ParseIP(sess.RemoteIP), Port: cRtcpPort}
				t.serverRtpPort = rtpConn.LocalAddr().(*net.UDPAddr).Port
				t.serverRtcpPort = rtcpConn.LocalAddr().(*net.UDPAddr).Port
				respTransport = fmt.Sprintf("RTP/AVP;unicast;client_port=%s;server_port=%d-%d",
					clientPortStr, t.serverRtpPort, t.serverRtcpPort)
				// 收 RTCP：5.1.1 用它给会话打活跃点，超时回收靠它；
				// 真正的 RTCP 内容解析（SR/RR 统计）放到 5.1.3。
				go func(kind string, rtcpConn *net.UDPConn, sess *session.Session) {
					buf := make([]byte, 2048)
					for {
						n, _, err := rtcpConn.ReadFromUDP(buf)
						if err != nil {
							return
						}
						sess.TouchRTCP()
						fmt.Printf("RTCP recv %d bytes (%s, session=%s)\n", n, kind, sess.ID)
					}
				}(kind, rtcpConn, sess)
			}

			// 同一个 Session 下按轨登记；重复 SETUP 同一条轨时先关掉旧端口
			if old := rtspSess.setTrack(kind, t); old != nil {
				if old.rtpConn != nil {
					old.rtpConn.Close()
				}
				if old.rtcpConn != nil {
					old.rtcpConn.Close()
				}
			}

			hd["Transport"] = respTransport
			hd["Session"] = sessionHeaderValue(sess)
			err = writeResponse(conn, "200 OK", hd, nil)
		case "PLAY":
			// 1. 读取Session头
			sessHeader, ok := req.Headers["Session"]
			if !ok {
				err = writeResponse(conn, "400 Bad Request", hd, nil)
				break
			}
			// 截断 ;timeout=xx 只拿session id
			sessID := strings.Split(sessHeader, ";")[0]
			// 2. 校验session是否存在
			sess, ok := sessionMgr.Get(sessID)
			if !ok {
				err = writeResponse(conn, "454 Session Not Found", hd, nil)
				break
			}
			sess.TouchRTSP()
			// 状态校验：只有 Init / Paused 允许PLAY
			if st := sess.State(); st != session.StateInit && st != session.StatePaused {
				err = writeResponse(conn, "455 Method Not Valid In This State", hd, nil)
				break
			}

			sess.SetState(session.StatePlaying)
			rtspSess := sess.Data.(*rtspSession)
			conn.bindSession(sess.ID)

			// 3. 返回200 OK，然后起 goroutine 推流（UDP 模式）
			hd["Session"] = sessionHeaderValue(sess)
			err = writeResponse(conn, "200 OK", hd, nil)
			if err == nil {
				video := rtspSess.track("video")
				audio := rtspSess.track("audio")
				// 3.2.6：音视频各起一个 goroutine，各用各的 seq/timestamp/SSRC；
				// 4.2.3：UDP 和 TCP interleaved 两种传输都从这里起。
				// 5.1.1：把会话的 Done 通道传进去，会话被回收时推流能及时退出。
				if video != nil {
					go streamH264(video, sess.Done())
				}
				if audio != nil {
					go streamAAC(audio, sess.Done())
				}
				// 5.1.3：每 5s 发一次 RTCP SR，带 NTP↔RTP 映射。
				// 每条轨独立 SSRC/时钟，所以各发各的；会话内只启动一次。
				rtspSess.srOnce.Do(func() {
					if video != nil {
						go sendSenderReports(video, sess.Done())
					}
					if audio != nil {
						go sendSenderReports(audio, sess.Done())
					}
				})
			}
		case "TEARDOWN":
			sessHeader, ok := req.Headers["Session"]
			if !ok {
				err = writeResponse(conn, "400 Bad Request", hd, nil)
				break
			}
			sessID := strings.Split(sessHeader, ";")[0]
			if sess, ok := sessionMgr.Get(sessID); ok {
				sess.TouchRTSP()
				sessionMgr.Delete(sessID)
				// Close：停推流 goroutine + 关 UDP socket（OnClose 里做）
				sess.Close()
				fmt.Printf("session %s teardown by client\n", sessID)
			}
			hd["Session"] = fmt.Sprintf("%s;timeout=%d", sessID, int(sessionMgr.Timeout()/time.Second))
			err = writeResponse(conn, "200 OK", hd, nil)
		case "PAUSE":
			sessHeader, ok := req.Headers["Session"]
			if !ok {
				err = writeResponse(conn, "400 Bad Request", hd, nil)
				break
			}
			sessID := strings.Split(sessHeader, ";")[0]
			sess, ok := sessionMgr.Get(sessID)
			if !ok {
				err = writeResponse(conn, "454 Session Not Found", hd, nil)
				break
			}
			sess.TouchRTSP()
			if sess.State() != session.StatePlaying {
				err = writeResponse(conn, "455 Method Not Valid In This State", hd, nil)
				break
			}
			sess.SetState(session.StatePaused)
			hd["Session"] = sessionHeaderValue(sess)
			err = writeResponse(conn, "200 OK", hd, nil)
		default:
			err = writeResponse(conn, "501 Not Implemented", hd, nil)
		}

		if err != nil {
			fmt.Println("write resp err:", err)
			return
		}
	}
}

// openUDPPortPair 开一对 RTP(偶)+RTCP(奇) 的 UDP socket，冲突就换下一对。
func openUDPPortPair() (rtpConn, rtcpConn *net.UDPConn, err error) {
	for base := 50000; base < 60000; base += 2 { // base 一定是偶数
		rtp, err := net.ListenUDP("udp", &net.UDPAddr{Port: base})
		if err != nil {
			continue
		}
		rtcp, err := net.ListenUDP("udp", &net.UDPAddr{Port: base + 1})
		if err != nil {
			rtp.Close()
			continue
		}
		return rtp, rtcp, nil
	}
	return nil, nil, fmt.Errorf("no udp port pair available")
}

// streamH264 每个客户端一条订阅 goroutine：
// 从共享视频源拿 Frame → 封 RTP → 发送。
// 5.1.2：源只读一次，多个客户端各自从自己的 channel 取同一批 NALU；
// seq/SSRC 仍然每个客户端独立，时间戳由源统一打好，保证多路一致。
func streamH264(track *mediaTrack, done <-chan struct{}) {
	frames, cancel := h264Source.Subscribe()
	defer cancel()

	packer := rtp.NewH264Packer(1400)
	var (
		seq  uint16
		ssrc = rand.Uint32() //同步源
	)
	// 5.1.3：RTCP SR 里的 SSRC 必须和 RTP 包一致
	track.setSSRC(ssrc)
	for {
		select {
		case <-done:
			fmt.Println("video stream stopped")
			return
		case f, ok := <-frames:
			if !ok {
				fmt.Println("video source ended")
				return
			}
			pkts, err := packer.Pack(f.Data)
			if err != nil {
				continue
			}
			for _, p := range pkts {
				p.Header.Seq = seq
				seq++
				p.Header.Timestamp = f.Timestamp
				p.Header.SSRC = ssrc
				data, err := p.Header.Marshal(p.Payload)
				if err != nil {
					continue
				}
				if err := track.writeRTP(data); err != nil {
					fmt.Println("write video rtp:", err)
					return
				}
				// 5.1.3：给 SR 累计包数/字节数，并记下最后一个时间戳
				track.recordSent(p.Header.Timestamp, len(p.Payload))
			}
		}
	}
}

// streamAAC 每个客户端一条订阅 goroutine：从共享音频源拿 AU → RFC 3640 封包 → 发送。
// 音频时间戳（采样数，每个 AU +1024）由源统一维护，客户端只负责封包和 seq/SSRC。
func streamAAC(track *mediaTrack, done <-chan struct{}) {
	frames, cancel := aacSource.Subscribe()
	defer cancel()

	packer := rtp.NewAACPacker()
	var (
		seq  uint16
		ssrc = rand.Uint32() // 和视频各自独立的 SSRC
	)
	track.setSSRC(ssrc)
	rateSet := false
	for {
		select {
		case <-done:
			fmt.Println("audio stream stopped")
			return
		case f, ok := <-frames:
			if !ok {
				fmt.Println("aac source ended")
				return
			}
			// 5.1.3：RTCP SR 的 RTP 时钟 = AAC 采样率。
			// 第一帧到达时源已经解析完 ADTS 头，这时取到的值才是准的。
			if !rateSet {
				track.setClockRate(aacSource.SamplingRate())
				rateSet = true
			}
			pkts, err := packer.Pack(f.Data)
			if err != nil {
				continue
			}
			for _, p := range pkts {
				p.Header.Seq = seq
				seq++
				p.Header.Timestamp = f.Timestamp
				p.Header.SSRC = ssrc
				data, err := p.Header.Marshal(p.Payload)
				if err != nil {
					continue
				}
				if err := track.writeRTP(data); err != nil {
					fmt.Println("write audio rtp:", err)
					return
				}
				track.recordSent(p.Header.Timestamp, len(p.Payload))
			}
		}
	}
}

func main() {
	// 5.1.1：会话表交给 internal/session.Manager：
	//   - 每 30s 扫一次（Run 内部 ticker）
	//   - 超过 60s 没有任何 RTCP / RTSP 活动的会话自动 TEARDOWN
	//     （客户端 kill -9 之后两条通道都会安静，所以能被回收）
	sessionMgr.OnExpire = func(s *session.Session) {
		fmt.Printf("session %s auto teardown: no RTCP/RTSP for %s (last RTCP %s)\n",
			s.ID, sessionMgr.Timeout(), s.LastRTCP().Format(time.RFC3339))
	}
	go sessionMgr.Run(context.Background())

	listener, err := net.Listen("tcp", ":8554")
	if err != nil {
		fmt.Printf("listen failed: %v\n", err)
		return
	}
	defer listener.Close()
	fmt.Println("RTSP server listen on :8554")
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("accept err: %v\n", err)
			continue
		}
		fmt.Printf("new client: %s\n", conn.RemoteAddr())
		go handleConn(&rtspConn{Conn: conn}) // 每个连接单独goroutine
	}
}

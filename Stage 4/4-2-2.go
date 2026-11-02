//go:build ignore

package Stage4

import (
	"bufio"
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
	"flow/internal/rtp"
)

type SessionState int

const (
	StateInit     SessionState = iota // 刚创建，SETUP完成，还未PLAY
	StatePlaying                      // PLAY之后
	StatePaused                       // PAUSE之后（当前代码未实现PAUSE）
	StateTeardown                     // 销毁
)

// mediaTrack 一条媒体轨（video/audio）的传输参数。
// 3.2.5：一个 Session 下有两条轨，各自一对端口、各自的目标地址。
type mediaTrack struct {
	kind            string // "video" / "audio"
	isInterleaved   bool
	interleavedCh   string       // 原始文本，如 "0-1"
	interleavedRTP  int          // RTP channel（4.2.3 写 $ 帧时用）
	interleavedRTCP int          // RTCP channel
	rtpConn         *net.UDPConn // 服务端 RTP socket（用来发）
	rtcpConn        *net.UDPConn // 服务端 RTCP socket（用来收）
	clientAddr      *net.UDPAddr // 客户端 RTP 目标地址 ip:port
	serverRtpPort   int          // 偶数
	serverRtcpPort  int          // 奇数 = RTP+1
}

type RtspSession struct {
	sessionID string
	remoteIP  string
	aliveAt   time.Time
	state     SessionState
	tracks    map[string]*mediaTrack // key: "video" / "audio"
}

// closeTracks 关掉会话下所有轨的 UDP socket（TEARDOWN / 超时回收时调用）。
func (s *RtspSession) closeTracks() {
	for _, t := range s.tracks {
		if t.rtpConn != nil {
			t.rtpConn.Close()
		}
		if t.rtcpConn != nil {
			t.rtcpConn.Close()
		}
	}
}

var (
	sessionMu    sync.Mutex
	sessionIDSeq int
	sessionMap   = make(map[string]*RtspSession)
)

func newSession(remoteAddr net.Addr) *RtspSession {
	sessionMu.Lock()
	defer sessionMu.Unlock()
	sessionIDSeq++
	sid := fmt.Sprintf("sess-%d", sessionIDSeq)
	ip, _, _ := net.SplitHostPort(remoteAddr.String())
	s := &RtspSession{
		sessionID: sid,
		remoteIP:  ip,
		aliveAt:   time.Now(),
		state:     StateInit,
		tracks:    make(map[string]*mediaTrack),
	}
	sessionMap[sid] = s
	return s
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

func writeResponse(conn net.Conn, status string, headers map[string]string, body []byte) error {
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

	_, err := conn.Write(out)
	return err
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
// 客户端主要用它发 RTCP（RR/SDES 等），真正的 RTCP 解析放到 5.1.3；
// 这里先按 channel 分发并打点，证明 $ 帧能从 RTSP 文本流里被正确切出来。
func handleInterleaved(channel byte, payload []byte) {
	if len(payload) == 0 {
		return
	}
	// RTCP 包类型在第二个字节（第一字节是 V/P/RC）
	pt := uint8(0)
	if len(payload) >= 2 {
		pt = payload[1]
	}
	fmt.Printf("interleaved recv: channel=%d len=%d rtcp_pt=%d\n", channel, len(payload), pt)
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	// 4.2.2：整个连接共用一个 reader，RTSP 文本和 $ 二进制帧都从它读
	br := bufio.NewReader(conn)
	for {
		// 每次读之前刷新超时
		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))

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
			handleInterleaved(ch, payload)
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
			var sess *RtspSession
			if sh, ok := req.Headers["Session"]; ok {
				sessID := strings.Split(sh, ";")[0]
				sessionMu.Lock()
				sess = sessionMap[sessID]
				sessionMu.Unlock()
				if sess == nil {
					err = writeResponse(conn, "454 Session Not Found", hd, nil)
					break
				}
			} else {
				sess = newSession(conn.RemoteAddr())
			}

			// 判断这次 SETUP 建哪条轨：优先看 URI 里的 trackID/streamid，
			// 客户端没带就按“video 先、audio 后”的顺序推断
			kind := trackKindFromURI(req.URI)
			if kind == "" {
				kind = "video"
				sessionMu.Lock()
				if sess.tracks["video"] != nil {
					kind = "audio"
				}
				sessionMu.Unlock()
			}

			t := &mediaTrack{
				kind:          kind,
				isInterleaved: isInter,
				interleavedCh: interStr,
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
				// 客户端 RTP 目标地址：客户端IP + client_port 里的 RTP 端口
				cRtpPort, _ := strconv.Atoi(strings.Split(clientPortStr, "-")[0])
				t.rtpConn = rtpConn
				t.rtcpConn = rtcpConn
				t.clientAddr = &net.UDPAddr{IP: net.ParseIP(sess.remoteIP), Port: cRtpPort}
				t.serverRtpPort = rtpConn.LocalAddr().(*net.UDPAddr).Port
				t.serverRtcpPort = rtcpConn.LocalAddr().(*net.UDPAddr).Port
				respTransport = fmt.Sprintf("RTP/AVP;unicast;client_port=%s;server_port=%d-%d",
					clientPortStr, t.serverRtpPort, t.serverRtcpPort)
				// 起一个 goroutine 空读 RTCP，证明端口通（真正解析 RTCP 到 5.1.3）
				go func(kind string, rtcpConn *net.UDPConn) {
					buf := make([]byte, 2048)
					for {
						n, _, err := rtcpConn.ReadFromUDP(buf)
						if err != nil {
							return
						}
						fmt.Printf("RTCP recv %d bytes (%s)\n", n, kind)
					}
				}(kind, rtcpConn)
			}

			// 同一个 Session 下按轨登记；重复 SETUP 同一条轨时先关掉旧端口
			sessionMu.Lock()
			if old := sess.tracks[kind]; old != nil {
				if old.rtpConn != nil {
					old.rtpConn.Close()
				}
				if old.rtcpConn != nil {
					old.rtcpConn.Close()
				}
			}
			sess.tracks[kind] = t
			sess.aliveAt = time.Now()
			sessionMu.Unlock()

			hd["Transport"] = respTransport
			hd["Session"] = fmt.Sprintf("%s;timeout=60", sess.sessionID)
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
			sessionMu.Lock()
			sess, ok := sessionMap[sessID]

			if !ok {
				sessionMu.Unlock()
				err = writeResponse(conn, "454 Session Not Found", hd, nil)
				break
			}
			// 状态校验：只有 StateInit / StatePaused 允许PLAY
			if sess.state != StateInit && sess.state != StatePaused {
				sessionMu.Unlock()
				err = writeResponse(conn, "455 Method Not Valid In This State", hd, nil)
				break
			}

			sess.aliveAt = time.Now()
			sess.state = StatePlaying

			sessionMu.Unlock()
			// 3. 返回200 OK，然后起 goroutine 推流（UDP 模式）
			hd["Session"] = fmt.Sprintf("%s;timeout=60", sessID)
			err = writeResponse(conn, "200 OK", hd, nil)
			if err == nil {
				sessionMu.Lock()
				video := sess.tracks["video"]
				audio := sess.tracks["audio"]
				sessionMu.Unlock()
				// 3.2.6：音视频各起一个 goroutine，各用各的端口和 seq/timestamp/SSRC
				if video != nil && !video.isInterleaved {
					go streamH264(video)
				}
				if audio != nil && !audio.isInterleaved {
					go streamAAC(audio)
				}
			}
		case "TEARDOWN":
			sessHeader, ok := req.Headers["Session"]
			if !ok {
				err = writeResponse(conn, "400 Bad Request", hd, nil)
				break
			}
			sessID := strings.Split(sessHeader, ";")[0]
			sessionMu.Lock()
			if sess, ok := sessionMap[sessID]; ok {
				sess.closeTracks()
				delete(sessionMap, sessID)
			}
			sessionMu.Unlock()
			hd["Session"] = fmt.Sprintf("%s;timeout=60", sessID)
			err = writeResponse(conn, "200 OK", hd, nil)
		case "PAUSE":
			sessHeader, ok := req.Headers["Session"]
			if !ok {
				err = writeResponse(conn, "400 Bad Request", hd, nil)
				break
			}
			sessID := strings.Split(sessHeader, ";")[0]
			sessionMu.Lock()
			sess, ok := sessionMap[sessID]
			if !ok {
				sessionMu.Unlock()
				err = writeResponse(conn, "454 Session Not Found", hd, nil)
				break
			}
			if sess.state != StatePlaying {
				sessionMu.Unlock()
				err = writeResponse(conn, "455 Method Not Valid In This State", hd, nil)
				break
			}
			sess.aliveAt = time.Now()
			sess.state = StatePaused
			sessionMu.Unlock()
			hd["Session"] = fmt.Sprintf("%s;timeout=60", sessID)
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

// streamH264 PLAY 后起 goroutine 推流：读 NALU → 封 RTP → WriteToUDP。
// 2.2.5 按 25fps(40ms) 节奏；2.2.6 填 seq/ts/ssrc；2.2.7 SPS/PPS/IDR 立刻发。
func streamH264(track *mediaTrack) {
	demux, err := media.NewH264Demux("testdata/test.h264")
	if err != nil {
		fmt.Println("open h264:", err)
		return
	}
	packer := rtp.NewH264Packer(1400)

	var (
		seq  uint16
		ts   uint32          //时间错
		ssrc = rand.Uint32() //同步源
	)
	ticker := time.NewTicker(40 * time.Millisecond) // 25fps
	defer ticker.Stop()

	first := true
	for {
		nalu, err := demux.NextNALU()
		if err != nil {
			fmt.Println("stream done:", err)
			return
		}
		typ, _ := media.NALType(nalu)

		// 关键帧相关的 SPS/PPS/IDR 立刻发，普通帧等 40ms
		if !first && typ != rtp.NALUTypeSPS && typ != rtp.NALUTypePPS && typ != rtp.NALUTypeIDR {
			<-ticker.C
		}
		first = false

		pkts, err := packer.Pack(nalu)
		if err != nil {
			continue
		}
		for _, p := range pkts {
			p.Header.Seq = seq
			seq++
			p.Header.Timestamp = ts
			p.Header.SSRC = ssrc
			data, err := p.Header.Marshal(p.Payload)
			if err != nil {
				continue
			}
			if _, err := track.rtpConn.WriteToUDP(data, track.clientAddr); err != nil {
				fmt.Println("write udp:", err)
				return
			}
		}
		// 只有 VCL NALU（type 1~5）才代表一帧；SPS/PPS 不推进时间戳，
		// 否则每个关键帧会多算 2 帧，音视频会越走越偏
		if typ >= 1 && typ <= 5 {
			ts += 3600 // 视频 90000/25
		}
	}
}

// streamAAC PLAY 后起 goroutine 推音频：读一个 AU → RFC 3640 封包 → WriteToUDP。
// 音频时间戳单位是采样数，不是 90000：AAC LC 每个 AU 固定 1024 个采样，每帧 +1024。
func streamAAC(track *mediaTrack) {
	demux, err := media.NewAACDemux("testdata/test.aac")
	if err != nil {
		fmt.Println("open aac:", err)
		return
	}

	// 先读一帧，同时从首帧 ADTS 头拿到采样率，用来算发包节奏
	au, err := demux.NextAU()
	if err != nil {
		fmt.Println("read aac au:", err)
		return
	}
	freq := demux.First.SamplingFrequency
	if freq <= 0 {
		freq = 44100
	}
	// 每个 AU 的播放时长 = 1024 samples / sampling_rate（44100 → 约 23.2ms）
	frameDur := time.Duration(float64(time.Second) * float64(rtp.AACFrameSamples) / float64(freq))
	ticker := time.NewTicker(frameDur)
	defer ticker.Stop()

	packer := rtp.NewAACPacker()

	var (
		seq  uint16
		ts   uint32          // 音频时间戳从 0 开始
		ssrc = rand.Uint32() // 和视频各自独立的 SSRC
	)

	first := true
	for {
		if !first {
			<-ticker.C
			au, err = demux.NextAU()
			if err != nil {
				fmt.Println("aac stream done:", err)
				return
			}
		}
		first = false

		if pkts, err := packer.Pack(au); err == nil {
			for _, p := range pkts {
				p.Header.Seq = seq
				seq++
				p.Header.Timestamp = ts
				p.Header.SSRC = ssrc
				data, err := p.Header.Marshal(p.Payload)
				if err != nil {
					continue
				}
				if _, err := track.rtpConn.WriteToUDP(data, track.clientAddr); err != nil {
					fmt.Println("write aac udp:", err)
					return
				}
			}
		}
		ts += rtp.AACFrameSamples
	}
}

func sessionCleaner() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		sessionMu.Lock()
		now := time.Now()
		for sid, s := range sessionMap {
			if now.Sub(s.aliveAt) > 60*time.Second {
				s.closeTracks()
				delete(sessionMap, sid)
				fmt.Printf("session %s timeout cleaned\n", sid)
			}
		}
		sessionMu.Unlock()
	}
}

func main() {
	go sessionCleaner()
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
		go handleConn(conn) // 每个连接单独goroutine
	}
}

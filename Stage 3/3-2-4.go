//go:build ignore

package Stage3

import (
	"bufio"
	"fmt"
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

type RtspSession struct {
	sessionID      string
	remoteIP       string
	clientRtpPort  string
	clientRtcpPort string
	isInterleaved  bool
	interleavedCh  string //0-1
	aliveAt        time.Time
	state          SessionState

	//UDP 模式专用
	rtpConn        *net.UDPConn // 服务端 RTP socket（用来发）
	rtcpConn       *net.UDPConn // 服务端 RTCP socket（用来收）
	clientAddr     *net.UDPAddr // 客户端 RTP 目标地址 ip:port
	serverRtpPort  int          // 偶数
	serverRtcpPort int          // 奇数 = RTP+1

}

var (
	sessionMu    sync.Mutex
	sessionIDSeq int
	sessionMap   = make(map[string]*RtspSession)
)

func newSession(remoteAddr net.Addr, isInter bool, interStr, cliPort string) *RtspSession {
	sessionMu.Lock()
	defer sessionMu.Unlock()
	sessionIDSeq++
	sid := fmt.Sprintf("sess-%d", sessionIDSeq)
	ip, _, _ := net.SplitHostPort(remoteAddr.String())
	s := &RtspSession{
		sessionID:     sid,
		remoteIP:      ip,
		isInterleaved: isInter,
		interleavedCh: interStr,
		aliveAt:       time.Now(),
		state:         StateInit,
	}
	// 拆分 client_port=9000-9001
	if cliPort != "" {
		pr := strings.Split(cliPort, "-")
		if len(pr) == 2 {
			s.clientRtpPort = pr[0]
			s.clientRtcpPort = pr[1]
		}
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

// 读到\r\n 返回RtspRequest
func readRequest(conn net.Conn) (*RtspRequest, error) {
	//setting timeout
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	br := bufio.NewReader(conn)
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

func handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		req, err := readRequest(conn)
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
				aacInfo.MediaDescription(media.AACPT)
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
			sess := newSession(conn.RemoteAddr(), isInter, interStr, clientPortStr)
			var respTransport string
			if isInter {
				respTransport = fmt.Sprintf("RTP/AVP/TCP;unicast;interleaved=%s", interStr)
			} else {
				rtpConn, rtcpConn, errUDP := openUDPPortPair()
				if errUDP != nil {
					err = writeResponse(conn, "500 Internal Server Error", hd, nil)
					break
				}
				// 客户端 RTP 目标地址：客户端IP + client_port 里的 RTP 端口
				cRtpPort, _ := strconv.Atoi(sess.clientRtpPort)
				sess.rtpConn = rtpConn
				sess.rtcpConn = rtcpConn
				sess.clientAddr = &net.UDPAddr{IP: net.ParseIP(sess.remoteIP), Port: cRtpPort}
				sess.serverRtpPort = rtpConn.LocalAddr().(*net.UDPAddr).Port
				sess.serverRtcpPort = rtcpConn.LocalAddr().(*net.UDPAddr).Port
				respTransport = fmt.Sprintf("RTP/AVP;unicast;client_port=%s;server_port=%d-%d",
					clientPortStr, sess.serverRtpPort, sess.serverRtcpPort)
				// 起一个 goroutine 空读 RTCP，证明端口通（真正解析 RTCP 到 5.1.3）
				go func() {
					buf := make([]byte, 2048)
					for {
						n, _, err := rtcpConn.ReadFromUDP(buf)
						if err != nil {
							return
						}
						fmt.Printf("RTCP recv %d bytes\n", n)
					}
				}()
			}
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
			if err == nil && !sess.isInterleaved {
				go streamH264(sess) // 2.2.5
			}
		case "TEARDOWN":
			sessHeader, ok := req.Headers["Session"]
			if !ok {
				err = writeResponse(conn, "400 Bad Request", hd, nil)
				break
			}
			sessID := strings.Split(sessHeader, ";")[0]
			sessionMu.Lock()
			sess, ok := sessionMap[sessID]
			if ok {
				if sess.rtpConn != nil {
					sess.rtpConn.Close()
					sess.rtcpConn.Close()
				}
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
func streamH264(sess *RtspSession) {
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
			if _, err := sess.rtpConn.WriteToUDP(data, sess.clientAddr); err != nil {
				fmt.Println("write udp:", err)
				return
			}
		}
		ts += 3600 // 视频 90000/25
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
				if s.rtpConn != nil {
					s.rtpConn.Close()
					s.rtcpConn.Close()
				}
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

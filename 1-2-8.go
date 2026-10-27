package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RtspSession struct {
	sessionID      string
	remoteIP       string
	clientRtpPort  string
	clientRtcpPort string
	isInterleaved  bool
	interleavedCh  string //0-1
	aliveAt        time.Time
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
		case "DESCRIBE": //SDP
			hd["Content-Type"] = "application/sdp"
			sdp := "v=0\r\n" +
				"o=- 0 0 IN IP4 127.0.0.1\r\n" +
				"s=TestStream\r\n" +
				"c=IN IP4 127.0.0.1\r\n" +
				"t=0 0\r\n" +
				"m=video 9000 RTP/AVP 96\r\n" +
				"a=rtpmap:96 H264/90000\r\n"
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
			var respTransport string
			if isInter {
				respTransport = fmt.Sprintf("RTP/AVP/TCP;unicast;interleaved=%s", interStr)
			} else {
				respTransport = fmt.Sprintf("RTP/AVP;unicast;client_port=%s;server_port=9002-9003", clientPortStr)
			}
			// 创建独立session
			sess := newSession(conn.RemoteAddr(), isInter, interStr, clientPortStr)
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
			if ok {
				sess.aliveAt = time.Now()
			}
			sessionMu.Unlock()
			if !ok {
				err = writeResponse(conn, "454 Session Not Found", hd, nil)
				break
			}
			// 3. 返回200 OK，本阶段不推流
			hd["Session"] = fmt.Sprintf("%s;timeout=60", sessID)
			err = writeResponse(conn, "200 OK", hd, nil)
		case "TEARDOWN":
			sessHeader, ok := req.Headers["Session"]
			if !ok {
				err = writeResponse(conn, "400 Bad Request", hd, nil)
				break
			}
			sessID := strings.Split(sessHeader, ";")[0]
			sessionMu.Lock()
			_, ok = sessionMap[sessID]
			if ok {
				delete(sessionMap, sessID)
			}
			sessionMu.Unlock()
			// TEARDOWN成功返回200，带上Session头（规范）
			hd["Session"] = sessID
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
func sessionCleaner() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		sessionMu.Lock()
		now := time.Now()
		for sid, s := range sessionMap {
			if now.Sub(s.aliveAt) > 60*time.Second {
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

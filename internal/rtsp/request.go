package rtsp

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

// Request 一条解析好的 RTSP 请求。
type Request struct {
	Method  string
	URI     string
	Version string
	CSeq    int
	Headers map[string]string
}

// parseHeaderBlock 解析 RTSP 请求的文本块（不含末尾的 \r\n\r\n）。
func parseHeaderBlock(block string) (*Request, error) {
	lines := strings.Split(block, "\r\n")
	if len(lines) == 0 {
		return nil, fmt.Errorf("empty header block")
	}
	// Method uri rtsp/1.0
	reqLine := strings.Fields(lines[0])
	if len(reqLine) != 3 {
		return nil, fmt.Errorf("invalid request line: %s", lines[0])
	}
	req := &Request{
		Method:  reqLine[0],
		URI:     reqLine[1],
		Version: reqLine[2],
		Headers: make(map[string]string),
	}
	for _, rawline := range lines[1:] {
		line := strings.TrimSpace(rawline)
		if line == "" {
			continue
		}
		kv := strings.SplitN(line, ":", 2)
		if len(kv) != 2 {
			continue
		}
		req.Headers[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
	}
	if cseq, ok := req.Headers["CSeq"]; ok {
		c, err := strconv.Atoi(cseq)
		if err != nil {
			return nil, fmt.Errorf("invalid CSeq value: %s", cseq)
		}
		req.CSeq = c
	}
	return req, nil
}

// readRequest 读到 \r\n\r\n 返回 Request。
// reader 由 handleConn 统一持有，不能每次新建，否则 bufio 里已缓冲的字节
// （可能正好是下一个 $ 帧）会被丢掉。
func readRequest(br *bufio.Reader) (*Request, error) {
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
	headerRaw := string(buf[:len(buf)-4])
	return parseHeaderBlock(headerRaw)
}

// writeResponse 拼一个 RTSP 响应。
// status: "200 OK" / "501 Not Implemented"；headers 里带 CSeq；body 非空自动填 Content-Length。
func writeResponse(conn *Conn, status string, headers map[string]string, body []byte) error {
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

	fmt.Printf("---- RESPONSE ----\n%s\n------------------\n", string(out))
	return conn.write(out)
}

// extractTransportInfo 从 Transport 头里抽出 interleaved= 和 client_port= 两个字段。
func extractTransportInfo(transportStr string) (isInterleaved bool, interStr, clientPort string, err error) {
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
// 推流时要把 channel 号写进 $ 帧头，所以这里先解析并存好。
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
// DESCRIBE 里下发 a=control:trackID=0（video）/ trackID=1（audio）；
// 不认 a=control 的客户端会退化成 streamid=0 / streamid=1，同样映射。
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

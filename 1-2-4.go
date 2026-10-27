package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

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
	// 如果有body，自动补Content-Length（用户传了就覆盖，以用户为准）
	if len(body) > 0 {
		headers["Content-Length"] = strconv.Itoa(len(body))
	}

	// 遍历headers输出
	for k, v := range headers {
		sb.WriteString(fmt.Sprintf("%s: %s\r\n", k, v))
	}
	// header结束分隔
	sb.WriteString("\r\n")

	// 拼接header字节
	out := []byte(sb.String())
	// 追加body
	out = append(out, body...)

	_, err := conn.Write(out)
	return err
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
		case "DESCRIBE":
			hd["Content-Type"] = "application/sdp"
			// 这里就是SDP文本
			sdp := `v=0
o=- 0 0 IN IP4 127.0.0.1
s=TestStream
c=IN IP4 127.0.0.1
t=0 0
m=video 9000 RTP/AVP 96
a=rtpmap:96 H264/90000
`
			err = writeResponse(conn, "200 OK", hd, []byte(sdp))
		case "SETUP":
			hd["Transport"] = "RTP/AVP;unicast;client_port=9000-9001;server_port=9002-9003"
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

func main() {
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

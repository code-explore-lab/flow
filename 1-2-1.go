/*
**1.2.1** 用 `net.Listen("tcp", ":8554")` 起服务，每 accept 一个连接起一个 goroutine。
 */
package main

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

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
func handleConn(conn net.Conn) {
	defer func() {
		fmt.Printf("client close: %s\n", conn.RemoteAddr())
		conn.Close()
	}()
	//setting timeout
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	br := bufio.NewReader(conn)
	for {
		raw, err := br.ReadString('\n')
		if err != nil {
			return
		}
		var buf []byte
		buf = append(buf, raw...)
		for {
			if len(buf) >= 4 && string(buf[len(buf)-4:]) == "\r\n\r\n" {
				break
			}
			chunk, err := br.ReadString('\n')
			if err != nil {
				return
			}
			buf = append(buf, chunk...)
		}
		reqStr := string(buf)
		fmt.Println("===== REQUEST =====")
		fmt.Print(reqStr)

		// 临时固定响应：OPTIONS 200 OK，后面替换成根据Method生成
		resp := "RTSP/1.0 200 OK\r\nCSeq: 6666\r\nPublic: OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN\r\n\r\n"

		// 注意：RTSP 头换行必须 \r\n
		resp = fmt.Sprintf("%s\r\n", resp)
		_, _ = conn.Write([]byte(resp))
	}
}

package rtsp

// E-9 资源回收测试：kill -9（连接直接断开）、断网（静默）、半开连接三种
// 客户端失效方式下，验证会话在超时窗口内被回收、UDP 端口释放，
// 且控制连接的 goroutine 不再等到 30s 读超时兜底。

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"flow/internal/session"
)

// startReclaimServer 起一个真实 TCP 监听 + 压缩超时的会话管理器。
// 与 Serve 不同，这里只跑 accept→handleConn，不掺白名单/限流（E-8 另有测试）。
func startReclaimServer(t *testing.T, timeout time.Duration) (*Server, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := New(nil, nil, false)
	s.sessions = session.NewManagerWithInterval(timeout, 20*time.Millisecond)
	s.installExpireHook()

	ctx, cancel := context.WithCancel(context.Background())
	go s.sessions.Run(ctx)
	t.Cleanup(func() {
		cancel()
		ln.Close()
	})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go s.handleConn(&Conn{Conn: c})
		}
	}()
	return s, ln.Addr().String()
}

// udpPushSession 通过真实 TCP 连接走 ANNOUNCE + SETUP(UDP)，建立一个推流会话。
// 返回客户端连接、会话 ID、以及服务端分配的 RTP 端口。
func udpPushSession(t *testing.T, addr string) (*net.TCPConn, string, int) {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	tcp := c.(*net.TCPConn)
	br := bufio.NewReader(tcp)
	bw := bufio.NewWriter(tcp)

	resp := sendRTSP(t, br, bw, announceRequest(1, ""))
	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK") {
		t.Fatalf("ANNOUNCE response = %q", firstLine(resp))
	}
	sessID := strings.Split(responseHeader(resp, "Session"), ";")[0]
	if sessID == "" {
		t.Fatal("ANNOUNCE response missing Session header")
	}

	setup := fmt.Sprintf("SETUP rtsp://127.0.0.1:8554/live/streamid=0 RTSP/1.0\r\n"+
		"CSeq: 2\r\nSession: %s\r\nTransport: RTP/AVP;unicast;client_port=9000-9001\r\n\r\n", sessID)
	resp = sendRTSP(t, br, bw, setup)
	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK") {
		t.Fatalf("SETUP response = %q", firstLine(resp))
	}
	port := serverRTPPort(t, responseHeader(resp, "Transport"))
	return tcp, sessID, port
}

// serverRTPPort 从 SETUP 响应的 Transport 头里取 server_port 的第一个端口。
func serverRTPPort(t *testing.T, transport string) int {
	t.Helper()
	i := strings.Index(transport, "server_port=")
	if i < 0 {
		t.Fatalf("Transport = %q, want server_port", transport)
	}
	rest := transport[i+len("server_port="):]
	end := strings.IndexAny(rest, "-;")
	if end < 0 {
		end = len(rest)
	}
	port, err := strconv.Atoi(rest[:end])
	if err != nil {
		t.Fatalf("server_port in %q: %v", transport, err)
	}
	return port
}

// assertUDPPortReleased 轮询尝试重新绑定服务端释放的 UDP 端口。
func assertUDPPortReleased(t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: port})
		if err == nil {
			_ = conn.Close()
			return
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("UDP port %d was not released: %v", port, lastErr)
}

// TestReclaimOnClientKill 模拟 kill -9：客户端 TCP 连接直接断开（FIN/RST）。
// 会话应立即回收，不用等会话超时。这里把超时设得远大于等待时间，
// 确保是断开事件触发的回收，而不是超时扫描。
func TestReclaimOnClientKill(t *testing.T) {
	s, addr := startReclaimServer(t, 10*time.Second)
	client, sessID, rtpPort := udpPushSession(t, addr)

	if _, ok := s.sessions.Get(sessID); !ok {
		t.Fatal("session was not registered after SETUP")
	}

	_ = client.Close() // 进程消失：内核发 FIN/RST

	waitFor(t, 3*time.Second, func() bool { return s.sessions.Len() == 0 })
	assertUDPPortReleased(t, rtpPort)
}

// TestReclaimOnHalfOpen 模拟半开连接：客户端只关闭写端（发 FIN），
// 服务端读到 EOF 后应立即回收会话并释放 UDP 端口。
func TestReclaimOnHalfOpen(t *testing.T) {
	s, addr := startReclaimServer(t, 10*time.Second)
	client, sessID, rtpPort := udpPushSession(t, addr)

	if err := client.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}

	waitFor(t, 3*time.Second, func() bool { return s.sessions.Len() == 0 })
	if _, ok := s.sessions.Get(sessID); ok {
		t.Fatal("session still registered after half-close")
	}
	assertUDPPortReleased(t, rtpPort)
}

// TestReclaimOnNetworkDrop 模拟断网：客户端 TCP 连接保持打开但完全静默，
// 服务端收不到 RTSP/RTCP。会话应在超时后被 Sweep 回收、UDP 端口释放，
// 并且控制连接也要随会话关闭（E-9 修复），而不是干等 30s 读超时。
func TestReclaimOnNetworkDrop(t *testing.T) {
	s, addr := startReclaimServer(t, 300*time.Millisecond)
	client, sessID, rtpPort := udpPushSession(t, addr)

	if _, ok := s.sessions.Get(sessID); !ok {
		t.Fatal("session was not registered after SETUP")
	}

	// 不关闭、不发送任何东西：等待超时扫描回收。
	waitFor(t, 3*time.Second, func() bool { return s.sessions.Len() == 0 })
	assertUDPPortReleased(t, rtpPort)

	// 会话回收时控制连接应被关闭：客户端读取应很快拿到 EOF，
	// 而不是阻塞到 30s 读超时（这里用 2s 读超时 + 1s 判定阈值）。
	start := time.Now()
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 256)
	for {
		if _, err := client.Read(buf); err != nil {
			break
		}
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("control conn still open %v after session reclaim; want it closed with the session", d)
	}
}

package rtsp

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"
)

// TestOptionsPublicHeader 校验 OPTIONS 响应的 Public 头与实际实现的
// 方法集合完全一致：既不能漏掉新方法，也不能声明没实现的方法。
func TestOptionsPublicHeader(t *testing.T) {
	s := New(nil, nil, false)

	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		s.handleConn(&Conn{Conn: serverConn})
		close(done)
	}()
	defer func() {
		clientConn.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("handleConn did not exit after client close")
		}
	}()

	br := bufio.NewReader(clientConn)
	bw := bufio.NewWriter(clientConn)
	resp := sendRTSP(t, br, bw, "OPTIONS rtsp://127.0.0.1:8554/live RTSP/1.0\r\nCSeq: 1\r\n\r\n")
	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("OPTIONS response = %q", firstLine(resp))
	}

	got := make(map[string]bool)
	value := responseHeader(resp, "Public")
	for _, method := range strings.Split(value, ",") {
		if method = strings.TrimSpace(method); method != "" {
			got[method] = true
		}
	}

	want := []string{
		"OPTIONS", "DESCRIBE", "ANNOUNCE", "SETUP",
		"PLAY", "PAUSE", "RECORD", "TEARDOWN",
		"GET_PARAMETER", "SET_PARAMETER",
	}
	for _, method := range want {
		if !got[method] {
			t.Errorf("Public = %q, missing %s", value, method)
		}
	}
	if len(got) != len(want) {
		t.Errorf("Public = %q, contains unexpected method(s): %v", value, got)
	}
}

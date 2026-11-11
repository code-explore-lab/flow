package rtsp

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"
)

func TestCSeqGuard(t *testing.T) {
	type step struct {
		cseq    int
		present bool
		want    bool
	}
	tests := []struct {
		name    string
		enabled bool
		maxGap  int
		steps   []step
	}{
		{
			name:    "关闭时全部放行",
			enabled: false,
			steps:   []step{{1, true, true}, {1, true, true}, {0, false, true}},
		},
		{
			name:    "首条不要求从 1 开始",
			enabled: true,
			steps:   []step{{42, true, true}, {43, true, true}},
		},
		{
			name:    "严格递增通过",
			enabled: true,
			steps:   []step{{1, true, true}, {2, true, true}, {100, true, true}},
		},
		{
			name:    "重放相同值拒绝",
			enabled: true,
			steps:   []step{{1, true, true}, {2, true, true}, {2, true, false}, {3, true, true}},
		},
		{
			name:    "回退拒绝",
			enabled: true,
			steps:   []step{{10, true, true}, {9, true, false}, {10, true, false}, {11, true, true}},
		},
		{
			name:    "缺少 CSeq 拒绝且不推进",
			enabled: true,
			steps:   []step{{1, true, true}, {0, false, false}, {0, false, false}, {2, true, true}},
		},
		{
			name:    "负数拒绝",
			enabled: true,
			steps:   []step{{-1, true, false}, {0, true, true}},
		},
		{
			name:    "跳号在限制内",
			enabled: true,
			maxGap:  10,
			steps:   []step{{1, true, true}, {11, true, true}, {21, true, true}},
		},
		{
			name:    "跳号超限拒绝且不推进",
			enabled: true,
			maxGap:  10,
			steps:   []step{{1, true, true}, {50, true, false}, {11, true, true}, {22, true, false}, {21, true, true}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := newCSeqGuard(tt.enabled, tt.maxGap)
			for i, s := range tt.steps {
				if got := g.allow(s.cseq, s.present); got != s.want {
					t.Fatalf("step %d allow(%d, %v) = %v, want %v",
						i, s.cseq, s.present, got, s.want)
				}
			}
		})
	}
}

func TestParseHeaderBlockCSeqCaseInsensitive(t *testing.T) {
	req, err := parseHeaderBlock("OPTIONS * RTSP/1.0\r\ncseq: 7")
	if err != nil {
		t.Fatalf("parseHeaderBlock: %v", err)
	}
	if req.CSeq != 7 {
		t.Fatalf("CSeq = %d, want 7", req.CSeq)
	}
}

// dialTestConn 建一条 net.Pipe 连接并起 handleConn，返回客户端读写器和清理函数。
func dialTestConn(t *testing.T, s *Server) (*bufio.Reader, *bufio.Writer, func()) {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		s.handleConn(&Conn{Conn: serverConn})
		close(done)
	}()
	return bufio.NewReader(clientConn), bufio.NewWriter(clientConn), func() {
		clientConn.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("handleConn did not exit after client close")
		}
	}
}

func optionsRequest(cseq string) string {
	raw := "OPTIONS rtsp://127.0.0.1:8554/live RTSP/1.0\r\n"
	if cseq != "" {
		raw += "CSeq: " + cseq + "\r\n"
	}
	return raw + "\r\n"
}

// TestHandleConnCSeq 走完整 handleConn：重放/回退/缺 CSeq 返回 400，
// 后续正常请求不受影响；头名大小写不敏感。
func TestHandleConnCSeq(t *testing.T) {
	s := New(nil, nil, false)
	br, bw, cleanup := dialTestConn(t, s)
	defer cleanup()

	expect := func(raw, prefix, note string) string {
		t.Helper()
		resp := sendRTSP(t, br, bw, raw)
		if !strings.HasPrefix(resp, prefix) {
			t.Fatalf("%s: response = %q, want %s", note, firstLine(resp), prefix)
		}
		return resp
	}

	expect(optionsRequest("1"), "RTSP/1.0 200 OK\r\n", "第一条")

	// 正常递增。
	expect(optionsRequest("2"), "RTSP/1.0 200 OK\r\n", "递增")

	// 重放：回 400，并回显冲突的 CSeq。
	resp := expect(optionsRequest("2"), "RTSP/1.0 400 Bad Request\r\n", "重放")
	if got := responseHeader(resp, "CSeq"); got != "2" {
		t.Fatalf("400 response CSeq = %q, want 2", got)
	}

	// 回退。
	expect(optionsRequest("1"), "RTSP/1.0 400 Bad Request\r\n", "回退")

	// 缺 CSeq。
	expect(optionsRequest(""), "RTSP/1.0 400 Bad Request\r\n", "缺 CSeq")

	// 小写头名也能识别并推进计数。
	expect("OPTIONS rtsp://127.0.0.1:8554/live RTSP/1.0\r\ncseq: 3\r\n\r\n",
		"RTSP/1.0 200 OK\r\n", "小写 cseq")

	// 被拒绝的请求没有推进 last，3 之后 4 应正常。
	expect(optionsRequest("4"), "RTSP/1.0 200 OK\r\n", "重放后继续递增")
}

// TestHandleConnReplayedTeardownNotDispatched 验证重放的 TEARDOWN 不会
// 进入分发逻辑：会话必须还在。
func TestHandleConnReplayedTeardownNotDispatched(t *testing.T) {
	s := New(nil, nil, false)
	br, bw, cleanup := dialTestConn(t, s)
	defer cleanup()

	resp := sendRTSP(t, br, bw, announceRequest(1, ""))
	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("ANNOUNCE response = %q", firstLine(resp))
	}
	sessID := strings.Split(responseHeader(resp, "Session"), ";")[0]
	if sessID == "" {
		t.Fatal("ANNOUNCE response missing Session header")
	}

	raw := "TEARDOWN rtsp://127.0.0.1:8554/live RTSP/1.0\r\n" +
		"CSeq: 1\r\nSession: " + sessID + "\r\n\r\n"
	resp = sendRTSP(t, br, bw, raw)
	if !strings.HasPrefix(resp, "RTSP/1.0 400 Bad Request\r\n") {
		t.Fatalf("replayed TEARDOWN response = %q", firstLine(resp))
	}
	if _, ok := s.sessions.Get(sessID); !ok {
		t.Fatal("replayed TEARDOWN was dispatched and removed the session")
	}
}

// TestCSeqResetsPerConnection 每条连接独立计数：新连接从 1 开始不应被拒绝。
func TestCSeqResetsPerConnection(t *testing.T) {
	s := New(nil, nil, false)
	for i := 0; i < 2; i++ {
		br, bw, cleanup := dialTestConn(t, s)
		resp := sendRTSP(t, br, bw, optionsRequest("1"))
		cleanup()
		if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
			t.Fatalf("connection %d: response = %q", i+1, firstLine(resp))
		}
	}
}

// TestCSeqCheckDisabled 关闭校验后重复/回退的 CSeq 都正常处理。
func TestCSeqCheckDisabled(t *testing.T) {
	s := New(nil, nil, false)
	s.SetCSeqCheck(false, 0)
	br, bw, cleanup := dialTestConn(t, s)
	defer cleanup()

	for _, cseq := range []string{"5", "5", "1"} {
		resp := sendRTSP(t, br, bw, optionsRequest(cseq))
		if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
			t.Fatalf("CSeq %s: response = %q", cseq, firstLine(resp))
		}
	}
}

// TestCSeqMaxGap 验证 maxGap 配置在 handler 层生效。
func TestCSeqMaxGap(t *testing.T) {
	s := New(nil, nil, false)
	s.SetCSeqCheck(true, 10)
	br, bw, cleanup := dialTestConn(t, s)
	defer cleanup()

	if resp := sendRTSP(t, br, bw, optionsRequest("1")); !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("first response = %q", firstLine(resp))
	}
	if resp := sendRTSP(t, br, bw, optionsRequest("12")); !strings.HasPrefix(resp, "RTSP/1.0 400 Bad Request\r\n") {
		t.Fatalf("gap 11 response = %q, want 400", firstLine(resp))
	}
	if resp := sendRTSP(t, br, bw, optionsRequest("11")); !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("gap 10 response = %q, want 200", firstLine(resp))
	}
}

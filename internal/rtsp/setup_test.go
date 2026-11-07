package rtsp

import (
	"bytes"
	"net"
	"strings"
	"testing"
	"time"

	"flow/internal/rtp"
	"flow/internal/session"
)

// newTestPipe 构造一个 net.Pipe 上的 Server + Conn，并起一个 goroutine
// 收集服务端写回的所有字节。finish 关闭客户端并返回已收到的响应文本。
func newTestPipe(t *testing.T) (s *Server, conn *Conn, finish func() string) {
	t.Helper()
	s = New(nil, nil, false)
	serverConn, clientConn := net.Pipe()

	respCh := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		tmp := make([]byte, 8192)
		for {
			n, err := clientConn.Read(tmp)
			if n > 0 {
				buf.Write(tmp[:n])
			}
			if err != nil {
				respCh <- buf.String()
				return
			}
		}
	}()

	return s, &Conn{Conn: serverConn}, func() string {
		clientConn.Close()
		resp := <-respCh
		serverConn.Close()
		return resp
	}
}

// callAndRead 在 goroutine 里执行处理函数（写响应对 net.Pipe 是阻塞的），
// 同时关闭客户端读取响应。
func callAndRead(t *testing.T, fn func() error, finish func() string) string {
	t.Helper()
	errCh := make(chan error, 1)
	go func() { errCh <- fn() }()
	// 先等 handler 写完返回，再关客户端取响应；反过来会在 write 中途
	// close net.Pipe，让 handler 拿到 "closed pipe" 错误。
	if err := <-errCh; err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	return finish()
}

// newPushSession 直接建一个 ANNOUNCE 之后的推流会话，避免测试里重复走 ANNOUNCE。
func newPushSession(t *testing.T, s *Server, conn *Conn) *session.Session {
	t.Helper()
	tracks, err := parseAnnounceSDP([]byte(ffmpegAnnounceSDP))
	if err != nil {
		t.Fatalf("parseAnnounceSDP: %v", err)
	}
	sess := s.newClientSession(conn)
	sess.Data.(*Session).setAnnounced(tracks)
	conn.bindSession(sess.ID)
	return sess
}

func TestSetupPushInterleaved(t *testing.T) {
	s, conn, finish := newTestPipe(t)
	sess := newPushSession(t, s, conn)

	hd := map[string]string{"CSeq": "3"}
	req := &Request{
		Method: "SETUP",
		URI:    "rtsp://127.0.0.1:8554/live/streamid=0",
		Headers: map[string]string{
			"Transport": "RTP/AVP/TCP;unicast;interleaved=0-1",
			"Session":   sess.ID,
		},
	}
	resp := callAndRead(t, func() error { return s.handleSetup(conn, req, hd) }, finish)

	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("status = %q", firstLine(resp))
	}
	wantTransport := "RTP/AVP/TCP;unicast;interleaved=0-1;mode=record"
	if hd["Transport"] != wantTransport {
		t.Fatalf("Transport = %q, want %q", hd["Transport"], wantTransport)
	}

	rs := sess.Data.(*Session)
	track := rs.track("video")
	if track == nil {
		t.Fatal("video track was not registered")
	}
	if !track.isPush || !track.isInterleaved {
		t.Fatalf("track push/interleaved = %v/%v, want true/true", track.isPush, track.isInterleaved)
	}
	if track.interleavedRTP != 0 || track.interleavedRTCP != 1 {
		t.Fatalf("interleaved channels = %d-%d, want 0-1", track.interleavedRTP, track.interleavedRTCP)
	}
	if track.rtpConn != nil || track.rtcpConn != nil {
		t.Fatal("interleaved push SETUP must not open UDP sockets")
	}
	if track.payloadType != 96 || track.encoding != "H264" || track.clockRate != 90000 {
		t.Fatalf("track codec = pt%d %s clock%d", track.payloadType, track.encoding, track.clockRate)
	}
}

func TestSetupPushUDPReceivesRTP(t *testing.T) {
	s, conn, finish := newTestPipe(t)
	sess := newPushSession(t, s, conn)

	hd := map[string]string{"CSeq": "3"}
	req := &Request{
		Method: "SETUP",
		URI:    "rtsp://127.0.0.1:8554/live/streamid=0",
		Headers: map[string]string{
			"Transport": "RTP/AVP;unicast;client_port=9000-9001",
			"Session":   sess.ID,
		},
	}
	resp := callAndRead(t, func() error { return s.handleSetup(conn, req, hd) }, finish)

	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("status = %q", firstLine(resp))
	}
	if !strings.Contains(hd["Transport"], "server_port=") || !strings.Contains(hd["Transport"], "mode=record") {
		t.Fatalf("Transport = %q, want server_port + mode=record", hd["Transport"])
	}

	track := sess.Data.(*Session).track("video")
	if track == nil || track.rtpConn == nil || track.serverRtpPort == 0 {
		t.Fatalf("track UDP sockets not configured: %+v", track)
	}

	// 服务端分配的 server_port 必须真的能收到 RTP（推流方向的反向端口）。
	header := rtp.RTPHeader{Version: 2, PT: 96, Seq: 7, Timestamp: 3600, SSRC: 0x11223344}
	pkt, err := header.Marshal([]byte{0x65, 0x01, 0x02, 0x03})
	if err != nil {
		t.Fatalf("marshal RTP: %v", err)
	}
	dst := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: track.serverRtpPort}
	client, err := net.DialUDP("udp", nil, dst)
	if err != nil {
		t.Fatalf("dial UDP: %v", err)
	}
	defer client.Close()
	if _, err := client.Write(pkt); err != nil {
		t.Fatalf("send RTP: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		packets, bytes, seq, ssrc, pt := track.recvSnapshot()
		if packets >= 1 {
			if seq != 7 || ssrc != 0x11223344 || pt != 96 {
				t.Fatalf("received seq=%d ssrc=%08x pt=%d", seq, ssrc, pt)
			}
			if bytes != uint64(len(pkt)) {
				t.Fatalf("received bytes = %d, want %d", bytes, len(pkt))
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server did not receive RTP on its UDP server_port")
		}
		time.Sleep(10 * time.Millisecond)
	}

	sess.Close() // 关 UDP socket，读 goroutine 退出
}

func TestHandleInterleavedRoutesPushRTP(t *testing.T) {
	s, conn, finish := newTestPipe(t)
	sess := newPushSession(t, s, conn)

	hd := map[string]string{"CSeq": "3"}
	req := &Request{
		Method: "SETUP",
		URI:    "rtsp://127.0.0.1:8554/live/streamid=0",
		Headers: map[string]string{
			"Transport": "RTP/AVP/TCP;unicast;interleaved=0-1",
			"Session":   sess.ID,
		},
	}
	callAndRead(t, func() error { return s.handleSetup(conn, req, hd) }, finish)
	track := sess.Data.(*Session).track("video")

	header := rtp.RTPHeader{Version: 2, PT: 96, Seq: 100, Timestamp: 9000, SSRC: 0xaabbccdd, Marker: true}
	pkt, err := header.Marshal([]byte{0x41, 0x9a, 0x00})
	if err != nil {
		t.Fatalf("marshal RTP: %v", err)
	}

	// RTP channel 0：应该按推流轨接住。
	s.handleInterleaved(conn, 0, pkt)
	if packets, _, seq, ssrc, pt := track.recvSnapshot(); packets != 1 || seq != 100 || ssrc != 0xaabbccdd || pt != 96 {
		t.Fatalf("RTP not routed: packets=%d seq=%d ssrc=%08x pt=%d", packets, seq, ssrc, pt)
	}

	// RTCP channel 1：不能计入 RTP 统计。
	s.handleInterleaved(conn, 1, []byte{0x80, 0xc9, 0x00, 0x01})
	if packets, _, _, _, _ := track.recvSnapshot(); packets != 1 {
		t.Fatalf("RTCP frame was counted as RTP: packets=%d", packets)
	}
}

func TestHandleRecord(t *testing.T) {
	t.Run("推流会话 SETUP 后返回 200", func(t *testing.T) {
		s, conn, finish := newTestPipe(t)
		sess := newPushSession(t, s, conn)

		hd := map[string]string{"CSeq": "3"}
		setupReq := &Request{
			Method: "SETUP",
			URI:    "rtsp://127.0.0.1:8554/live/streamid=0",
			Headers: map[string]string{
				"Transport": "RTP/AVP/TCP;unicast;interleaved=0-1",
				"Session":   sess.ID,
			},
		}
		callAndRead(t, func() error { return s.handleSetup(conn, setupReq, hd) }, finish)

		s2, conn2, finish2 := newTestPipe(t)
		_ = s2
		hd2 := map[string]string{"CSeq": "4"}
		recordReq := &Request{
			Method:  "RECORD",
			URI:     "rtsp://127.0.0.1:8554/live",
			Headers: map[string]string{"Session": sess.ID},
		}
		resp := callAndRead(t, func() error { return s.handleRecord(conn2, recordReq, hd2) }, finish2)
		if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
			t.Fatalf("status = %q", firstLine(resp))
		}
		if sess.State() != session.StateRecording {
			t.Fatalf("state = %v, want Recording", sess.State())
		}
	})

	t.Run("非推流会话返回 455", func(t *testing.T) {
		s, conn, finish := newTestPipe(t)
		sess := s.newClientSession(conn)

		hd := map[string]string{"CSeq": "3"}
		req := &Request{
			Method:  "RECORD",
			URI:     "rtsp://127.0.0.1:8554/live",
			Headers: map[string]string{"Session": sess.ID},
		}
		resp := callAndRead(t, func() error { return s.handleRecord(conn, req, hd) }, finish)
		if !strings.HasPrefix(resp, "RTSP/1.0 455 ") {
			t.Fatalf("status = %q, want 455", firstLine(resp))
		}
	})

	t.Run("未知会话返回 454", func(t *testing.T) {
		s, conn, finish := newTestPipe(t)
		hd := map[string]string{"CSeq": "3"}
		req := &Request{
			Method:  "RECORD",
			URI:     "rtsp://127.0.0.1:8554/live",
			Headers: map[string]string{"Session": "sess-999"},
		}
		resp := callAndRead(t, func() error { return s.handleRecord(conn, req, hd) }, finish)
		if !strings.HasPrefix(resp, "RTSP/1.0 454 ") {
			t.Fatalf("status = %q, want 454", firstLine(resp))
		}
	})
}

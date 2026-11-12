package rtsp

import (
	"strings"
	"testing"
)

func TestHasMulticastTransport(t *testing.T) {
	tests := []struct {
		transport string
		want      bool
	}{
		{"RTP/AVP;multicast", true},
		{"RTP/AVP;Multicast", true},
		{"RTP/AVP;unicast;client_port=5000-5001", false},
		{"RTP/AVP/TCP;unicast;interleaved=0-1", false},
		{"RTP/AVP;multicast;destination=239.255.0.1;port=5000-5001", true},
	}
	for _, tt := range tests {
		if got := hasMulticastTransport(tt.transport); got != tt.want {
			t.Fatalf("hasMulticastTransport(%q) = %v, want %v", tt.transport, got, tt.want)
		}
	}
}

func TestMulticastPortsFor(t *testing.T) {
	if rtp, rtcp := multicastPortsFor("video"); rtp != 5000 || rtcp != 5001 {
		t.Fatalf("video ports = %d-%d, want 5000-5001", rtp, rtcp)
	}
	if rtp, rtcp := multicastPortsFor("audio"); rtp != 5002 || rtcp != 5003 {
		t.Fatalf("audio ports = %d-%d, want 5002-5003", rtp, rtcp)
	}
}

// TestSetupMulticast 验证组播 SETUP：回 200，Transport 里带 destination/port/ttl，
// 轨道走组播且只开一个发送 socket、不按客户端开 UDP 端口对。
func TestSetupMulticast(t *testing.T) {
	s, conn, finish := newTestPipe(t)

	hd := map[string]string{"CSeq": "3"}
	req := &Request{
		Method: "SETUP",
		URI:    "rtsp://127.0.0.1:8554/live/trackID=0",
		// ffplay -rtsp_transport udp_multicast 只写 multicast，不带参数。
		Headers: map[string]string{"Transport": "RTP/AVP;multicast"},
	}
	resp := callAndRead(t, func() error { return s.handleSetup(conn, req, hd) }, finish)

	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("status = %q", firstLine(resp))
	}
	want := "RTP/AVP;multicast;destination=239.255.0.1;port=5000-5001;ttl=1"
	if hd["Transport"] != want {
		t.Fatalf("Transport = %q, want %q", hd["Transport"], want)
	}

	// 会话里应该有一条 video 轨，走组播。
	ids := conn.boundSessions()
	if len(ids) != 1 {
		t.Fatalf("bound sessions = %v, want 1", ids)
	}
	sess, ok := s.sessions.Get(ids[0])
	if !ok {
		t.Fatalf("session %s not found", ids[0])
	}
	rs := sess.Data.(*Session)
	track := rs.track("video")
	if track == nil {
		t.Fatal("video track was not registered")
	}
	if !track.isMulticast {
		t.Fatal("track should be multicast")
	}
	if track.mcAddr == nil || track.mcAddr.Port != 5000 || track.mcRTCPAddr == nil || track.mcRTCPAddr.Port != 5001 {
		t.Fatalf("multicast addrs = %v / %v, want :5000 / :5001", track.mcAddr, track.mcRTCPAddr)
	}
	if track.rtpConn == nil {
		t.Fatal("multicast track should own a sender socket")
	}
	// 组播不维护每客户端 UDP 端口对：clientAddr/rtcpConn 都应该是空。
	if track.clientAddr != nil || track.rtcpConn != nil {
		t.Fatal("multicast must not allocate per-client UDP pair")
	}
}

// TestBuildSDPMulticastAttrs 验证 DESCRIBE 的 SDP 带 a=destination / a=ttl，
// 关闭组播时不再带。
func TestBuildSDPMulticastAttrs(t *testing.T) {
	s := New(nil, nil, true) // live 模式不读本地文件，直接拼 SDP
	sdp, err := s.buildSDP("rtsp://127.0.0.1:8554/live")
	if err != nil {
		t.Fatalf("buildSDP: %v", err)
	}
	for _, want := range []string{"a=destination:239.255.0.1", "a=ttl:1"} {
		if !strings.Contains(sdp, want) {
			t.Fatalf("SDP missing %q:\n%s", want, sdp)
		}
	}

	// 关闭组播后不再出现。
	s.SetMulticast("", 1)
	sdp, err = s.buildSDP("rtsp://127.0.0.1:8554/live")
	if err != nil {
		t.Fatalf("buildSDP: %v", err)
	}
	if strings.Contains(sdp, "a=destination") || strings.Contains(sdp, "a=ttl") {
		t.Fatalf("disabled multicast still advertises group:\n%s", sdp)
	}
}

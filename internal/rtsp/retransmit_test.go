package rtsp

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"

	"flow/internal/rtcp"
	"flow/internal/rtp"
	"flow/internal/session"
)

// newRTXTestTrack 造一条「已 SETUP、能发包」的拉流轨：
// 服务端一个 UDP socket，客户端一个，发送缓存可预先塞包。
func newRTXTestTrack(t *testing.T) (*Server, *session.Session, *mediaTrack, *net.UDPConn) {
	t.Helper()
	s := New(nil, nil, false)
	sess := s.sessions.NewSession("127.0.0.1")
	t.Cleanup(sess.Close)

	serverConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen server UDP: %v", err)
	}
	client, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		serverConn.Close()
		t.Fatalf("listen client UDP: %v", err)
	}
	t.Cleanup(func() {
		client.Close()
		serverConn.Close()
	})

	track := &mediaTrack{
		kind:       "video",
		rtpConn:    serverConn,
		clientAddr: client.LocalAddr().(*net.UDPAddr),
		sendCache:  rtp.NewSendCache(16, 1<<16),
		clockRate:  90000,
	}
	track.ensureSSRC()
	return s, sess, track, client
}

// recvUDP 在 wait 内收一个 UDP 包；超时返回 ok=false。
func recvUDP(t *testing.T, conn *net.UDPConn, wait time.Duration) ([]byte, bool) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(wait)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	buf := make([]byte, 2048)
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return nil, false
		}
		t.Fatalf("read UDP: %v", err)
	}
	return append([]byte(nil), buf[:n]...), true
}

func TestHandleIncomingNACKRetransmits(t *testing.T) {
	s, sess, track, client := newRTXTestTrack(t)

	cached := make(map[uint16][]byte)
	for seq := uint16(100); seq <= 102; seq++ {
		pkt := makeRTP(96, seq, uint32(seq)*3000, track.currentSSRC(),
			[]byte{byte(seq), 0xAA, 0xBB}, false)
		cached[seq] = pkt
		track.cacheSent(seq, pkt)
	}

	// 复合包 RR + NACK：101 重复、999 未发送，都只应产生一次/零次发送。
	rr := []byte{0x80, 201, 0, 1, 0, 0, 0, 1}
	nack := rtcp.NACK(0x11112222, track.currentSSRC(), []uint16{101, 101, 999})
	s.handleIncomingRTCP(track, sess, append(rr, nack...))

	got, ok := recvUDP(t, client, time.Second)
	if !ok {
		t.Fatal("no retransmission received")
	}
	if !bytes.Equal(got, cached[101]) {
		t.Fatalf("retransmitted packet = %x, want original %x", got, cached[101])
	}
	if _, ok := recvUDP(t, client, 150*time.Millisecond); ok {
		t.Fatal("duplicate/unknown seq produced an extra packet")
	}

	// 重传不是新媒体：缓存大小不变。
	if n := track.sendCache.Len(); n != len(cached) {
		t.Fatalf("send cache len = %d, want %d", n, len(cached))
	}
}

func TestHandleIncomingNACKIgnoresOtherSSRC(t *testing.T) {
	s, sess, track, client := newRTXTestTrack(t)
	pkt := makeRTP(96, 7, 9000, track.currentSSRC(), []byte{1, 2, 3}, false)
	track.cacheSent(7, pkt)

	nack := rtcp.NACK(1, track.currentSSRC()+1, []uint16{7})
	s.handleIncomingRTCP(track, sess, nack)
	if _, ok := recvUDP(t, client, 150*time.Millisecond); ok {
		t.Fatal("NACK for another media SSRC was retransmitted")
	}
}

func TestHandleIncomingNACKWithoutCache(t *testing.T) {
	s, sess, track, client := newRTXTestTrack(t)
	track.sendCache = nil // 推流/组播轨的形态
	nack := rtcp.NACK(1, track.currentSSRC(), []uint16{7})
	s.handleIncomingRTCP(track, sess, nack) // 不应 panic
	if _, ok := recvUDP(t, client, 150*time.Millisecond); ok {
		t.Fatal("track without send cache retransmitted a packet")
	}
}

func TestTakeRTXTokens(t *testing.T) {
	track := &mediaTrack{}
	now := time.Unix(0, 0)

	if n := track.takeRTXTokens(now, 1000); n != int(rtxBurst) {
		t.Fatalf("initial burst = %d, want %d", n, int(rtxBurst))
	}
	if n := track.takeRTXTokens(now, 1); n != 0 {
		t.Fatalf("same-instant second take = %d, want 0", n)
	}
	if n := track.takeRTXTokens(now.Add(100*time.Millisecond), 1000); n != 20 {
		t.Fatalf("100ms refill = %d, want 20", n)
	}
	// 长时间空闲后只补到 burst，不会无限累积。
	if n := track.takeRTXTokens(now.Add(time.Hour), 1000); n != int(rtxBurst) {
		t.Fatalf("capped refill = %d, want %d", n, int(rtxBurst))
	}
}

func TestDedupeSeqs(t *testing.T) {
	got := dedupeSeqs([]uint16{3, 1, 3, 2, 1})
	want := []uint16{3, 1, 2}
	if len(got) != len(want) {
		t.Fatalf("dedupe = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("dedupe = %v, want %v", got, want)
		}
	}
}

package rtsp

import (
	"io"
	"net"
	"testing"

	"flow/internal/rtp"
)

// TestStreamH264CachesSentPackets 发送循环把成功写出的 RTP 包存进
// mediaTrack.sendCache，R-7 才能按 seq 重传。
func TestStreamH264CachesSentPackets(t *testing.T) {
	s := New(nil, nil, false)

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	go func() { _, _ = io.Copy(io.Discard, clientConn) }()

	track := &mediaTrack{
		kind:           "video",
		isInterleaved:  true,
		interleavedRTP: 0,
		conn:           &Conn{Conn: serverConn},
		sendCache:      rtp.NewSendCache(8, 1<<20),
	}
	done := make(chan struct{})
	defer close(done)

	if ended := s.streamH264(track, &fakeSeekSource{}, done, 0, 1); !ended {
		t.Fatal("streamH264 should report source ended")
	}

	pkt, ok := track.cachedPacket(0)
	if !ok {
		t.Fatal("first sent packet (seq 0) missing from send cache")
	}
	if len(pkt) < 12 {
		t.Fatalf("cached packet too short: %d bytes", len(pkt))
	}
	// seq 已经用掉 0，不应再有第二包。
	if _, ok := track.cachedPacket(1); ok {
		t.Fatal("unexpected packet cached for seq 1")
	}
}

// TestCachedPacketWithoutCache 没有缓存的轨（推流/组播）查询安全返回 false。
func TestCachedPacketWithoutCache(t *testing.T) {
	track := &mediaTrack{kind: "video"}
	if _, ok := track.cachedPacket(0); ok {
		t.Fatal("track without send cache should not return packets")
	}
	track.cacheSent(0, []byte{1, 2, 3}) // 不应 panic
}

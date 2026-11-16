package rtsp

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"flow/internal/plc"
	"flow/internal/rtp"
	"flow/internal/source"
)

// testPkt 造一个属于该轨的 RTP 包，payload 首字节用来区分内容。
func testPkt(track *mediaTrack, seq uint16, payload byte) []byte {
	return makeRTP(96, seq, 9000, track.currentSSRC(), []byte{payload, 0x11, 0x22}, false)
}

func TestHandleIncomingPLIReplaysKeyframe(t *testing.T) {
	s, sess, track, client := newRTXTestTrack(t)

	params := testPkt(track, 10, 0x07) // 单独一帧的 SPS/PPS
	idr1 := testPkt(track, 11, 0x05)
	idr2 := testPkt(track, 12, 0x05)
	track.storeParamSets([][]byte{params})
	track.storeKeyframe([][]byte{idr1, idr2}, false)

	rr := []byte{0x80, 201, 0, 1, 0, 0, 0, 1}
	s.handleIncomingRTCP(track, sess, append(rr, plc.PLI(0x1111, track.currentSSRC())...))

	want := [][]byte{params, idr1, idr2}
	for i, w := range want {
		got, ok := recvUDP(t, client, time.Second)
		if !ok {
			t.Fatalf("packet %d not replayed", i)
		}
		if !bytes.Equal(got, w) {
			t.Fatalf("packet %d = %x, want %x", i, got, w)
		}
	}
	if _, ok := recvUDP(t, client, 150*time.Millisecond); ok {
		t.Fatal("unexpected extra packet after keyframe replay")
	}
}

func TestHandleIncomingPLISkipsSeparateParamsWhenIncluded(t *testing.T) {
	s, sess, track, client := newRTXTestTrack(t)

	oldParams := testPkt(track, 20, 0x07)
	stap := testPkt(track, 21, 0x18) // STAP-A（含 SPS/PPS）
	idr := testPkt(track, 22, 0x05)
	track.storeParamSets([][]byte{oldParams})
	track.storeKeyframe([][]byte{stap, idr}, true)

	s.handleIncomingRTCP(track, sess, plc.PLI(1, track.currentSSRC()))

	for i, w := range [][]byte{stap, idr} {
		got, ok := recvUDP(t, client, time.Second)
		if !ok {
			t.Fatalf("packet %d not replayed", i)
		}
		if !bytes.Equal(got, w) {
			t.Fatalf("packet %d = %x, want %x", i, got, w)
		}
	}
	if _, ok := recvUDP(t, client, 150*time.Millisecond); ok {
		t.Fatal("keyframe replay also re-sent separate parameter sets")
	}
}

func TestHandleIncomingFIRReplaysKeyframe(t *testing.T) {
	s, sess, track, client := newRTXTestTrack(t)
	idr := testPkt(track, 30, 0x05)
	track.storeKeyframe([][]byte{idr}, false)

	s.handleIncomingRTCP(track, sess, plc.FIR(1, track.currentSSRC(), 3))
	got, ok := recvUDP(t, client, time.Second)
	if !ok {
		t.Fatal("FIR did not trigger keyframe replay")
	}
	if !bytes.Equal(got, idr) {
		t.Fatalf("replayed = %x, want %x", got, idr)
	}
}

func TestPLIReplayRateLimited(t *testing.T) {
	s, sess, track, client := newRTXTestTrack(t)
	idr := testPkt(track, 40, 0x05)
	track.storeKeyframe([][]byte{idr}, false)

	s.handleIncomingRTCP(track, sess, plc.PLI(1, track.currentSSRC()))
	if _, ok := recvUDP(t, client, time.Second); !ok {
		t.Fatal("first PLI did not replay")
	}

	// 1 秒内的第二次 PLI 应被限频。
	s.handleIncomingRTCP(track, sess, plc.PLI(1, track.currentSSRC()))
	if _, ok := recvUDP(t, client, 150*time.Millisecond); ok {
		t.Fatal("rate-limited PLI still replayed")
	}

	// 把上次时间拨远，下一次应恢复。
	track.kfMu.Lock()
	track.lastKFReply = time.Now().Add(-2 * keyframeReplayInterval)
	track.kfMu.Unlock()
	s.handleIncomingRTCP(track, sess, plc.PLI(1, track.currentSSRC()))
	if _, ok := recvUDP(t, client, time.Second); !ok {
		t.Fatal("PLI after rate-limit window did not replay")
	}
}

func TestPLIAndFIRIgnoreOtherSSRC(t *testing.T) {
	s, sess, track, client := newRTXTestTrack(t)
	track.storeKeyframe([][]byte{testPkt(track, 50, 0x05)}, false)

	s.handleIncomingRTCP(track, sess, plc.PLI(1, track.currentSSRC()+1))
	if _, ok := recvUDP(t, client, 150*time.Millisecond); ok {
		t.Fatal("PLI for another SSRC replayed")
	}

	s.handleIncomingRTCP(track, sess, plc.FIR(1, track.currentSSRC()+1, 1))
	if _, ok := recvUDP(t, client, 150*time.Millisecond); ok {
		t.Fatal("FIR for another SSRC replayed")
	}
}

func TestPLIWithoutKeyframeDoesNotPanic(t *testing.T) {
	s, sess, track, client := newRTXTestTrack(t)
	s.handleIncomingRTCP(track, sess, plc.PLI(1, track.currentSSRC()))
	if _, ok := recvUDP(t, client, 150*time.Millisecond); ok {
		t.Fatal("PLI with empty keyframe cache sent a packet")
	}
}

// 复合包里的 PLI 也要能被识别（客户端常把 RR 和反馈包放在一起）。
func TestCompoundPLI(t *testing.T) {
	s, sess, track, client := newRTXTestTrack(t)
	idr := testPkt(track, 60, 0x05)
	track.storeKeyframe([][]byte{idr}, false)

	rr := []byte{0x80, 201, 0, 1, 0, 0, 0, 1}
	compound := append(rr, plc.PLI(1, track.currentSSRC())...)
	s.handleIncomingRTCP(track, sess, compound)
	if got, ok := recvUDP(t, client, time.Second); !ok || !bytes.Equal(got, idr) {
		t.Fatal("PLI in a compound packet was not handled")
	}
}

// TestStreamH264CachesKeyframeAndParams 验证发送循环把参数集单独帧和
// IDR 帧分别缓存；二者时间戳不同，PLI 时应按“参数集 + IDR”顺序重放。
func TestStreamH264CachesKeyframeAndParams(t *testing.T) {
	s := New(nil, nil, false)

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	go func() { _, _ = io.Copy(io.Discard, clientConn) }()

	src := &framesSource{frames: []source.Frame{
		{Data: []byte{0x67, 0x42, 0xC0, 0x1E}, Timestamp: 0},
		{Data: []byte{0x68, 0xCE, 0x3C, 0x80}, Timestamp: 0},
		{Data: []byte{0x65, 0x88, 0x84}, Timestamp: 3000},
	}}
	track := &mediaTrack{
		kind:           "video",
		isInterleaved:  true,
		interleavedRTP: 0,
		conn:           &Conn{Conn: serverConn},
		sendCache:      rtp.NewSendCache(8, 1<<20),
	}
	done := make(chan struct{})
	defer close(done)

	if ended := s.streamH264(track, src, done, 0, 1); !ended {
		t.Fatal("streamH264 should report source ended")
	}

	track.kfMu.Lock()
	params := len(track.lastParamSets)
	keyframe := len(track.lastKeyframe)
	hasParams := track.kfHasParams
	track.kfMu.Unlock()

	if params != 1 {
		t.Fatalf("cached param-set packets = %d, want 1", params)
	}
	if keyframe != 1 {
		t.Fatalf("cached keyframe packets = %d, want 1", keyframe)
	}
	if hasParams {
		t.Fatal("keyframe should not claim to carry parameter sets (they were a separate AU)")
	}
}

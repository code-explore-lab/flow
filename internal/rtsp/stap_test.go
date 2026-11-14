package rtsp

import (
	"bufio"
	"net"
	"testing"
	"time"

	"flow/internal/rtp"
	"flow/internal/source"
)

// framesSource 按顺序吐出一组预置帧，用来验证 PLAY 侧的封包行为。
type framesSource struct {
	frames []source.Frame
}

func (f *framesSource) Subscribe() (*source.Reader[source.Frame], func()) {
	ring := source.NewRingBuffer[source.Frame](len(f.frames) + 1)
	rd := ring.Subscribe(f.frames...)
	ring.Close()
	return rd, rd.Close
}

func (f *framesSource) SubscribeFrom(time.Duration, float64) (*source.Reader[source.Frame], func(), error) {
	rd, cancel := f.Subscribe()
	return rd, cancel, nil
}

// runVideoStream 在 net.Pipe 上跑 streamH264，读回 want 个 interleaved RTP 包。
func runVideoStream(t *testing.T, src videoFrameSource, want int) []*rtp.RTPPacket {
	t.Helper()
	s := New(nil, nil, false)

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	track := &mediaTrack{
		kind:           "video",
		isInterleaved:  true,
		interleavedRTP: 0,
		conn:           &Conn{Conn: serverConn},
	}
	done := make(chan struct{})
	defer close(done)

	streamDone := make(chan struct{})
	go func() {
		s.streamH264(track, src, done, 0, 1)
		close(streamDone)
	}()

	br := bufio.NewReader(clientConn)
	pkts := make([]*rtp.RTPPacket, 0, want)
	for i := 0; i < want; i++ {
		ch, payload, err := readInterleavedFrame(br)
		if err != nil {
			t.Fatalf("read packet %d: %v", i, err)
		}
		if ch != 0 {
			t.Fatalf("packet %d on channel %d, want 0", i, ch)
		}
		pkt, err := rtp.Unmarshal(payload)
		if err != nil {
			t.Fatalf("unmarshal packet %d: %v", i, err)
		}
		pkts = append(pkts, pkt)
	}
	<-streamDone
	return pkts
}

// parseSTAPANALUs 拆开 STAP-A 载荷，返回其中的 NALU。
func parseSTAPANALUs(t *testing.T, payload []byte) [][]byte {
	t.Helper()
	if len(payload) == 0 || payload[0]&0x1F != rtp.NALUTypeSTAPA {
		t.Fatalf("not a STAP-A packet: % x", payload)
	}
	rest := payload[1:]
	var nalus [][]byte
	for len(rest) > 0 {
		if len(rest) < 2 {
			t.Fatalf("truncated STAP-A size field")
		}
		n := int(rest[0])<<8 | int(rest[1])
		rest = rest[2:]
		if n > len(rest) {
			t.Fatalf("STAP-A NALU length %d exceeds remaining %d", n, len(rest))
		}
		nalus = append(nalus, rest[:n])
		rest = rest[n:]
	}
	return nalus
}

// TestStreamH264AggregatesFrameNALUs 同一时间戳的 SPS/PPS/小 IDR
// 应聚合为一个 STAP-A 包发出。
func TestStreamH264AggregatesFrameNALUs(t *testing.T) {
	sps := []byte{0x67, 0x42, 0xC0, 0x1E}
	pps := []byte{0x68, 0xCE, 0x3C, 0x80}
	idr := []byte{0x65, 0x88, 0x84, 0x00}
	src := &framesSource{frames: []source.Frame{
		{Data: sps},
		{Data: pps},
		{Data: idr, Keyframe: true},
	}}

	pkts := runVideoStream(t, src, 1)
	pkt := pkts[0]
	if got := pkt.Payload[0] & 0x1F; got != rtp.NALUTypeSTAPA {
		t.Fatalf("payload type = %d, want STAP-A(24)", got)
	}
	if !pkt.Header.Marker {
		t.Fatal("single frame packet should set marker")
	}
	nalus := parseSTAPANALUs(t, pkt.Payload)
	if len(nalus) != 3 {
		t.Fatalf("STAP-A contains %d NALUs, want 3", len(nalus))
	}
	for i, want := range [][]byte{sps, pps, idr} {
		if string(nalus[i]) != string(want) {
			t.Fatalf("NALU %d = % x, want % x", i, nalus[i], want)
		}
	}
}

// TestStreamH264DoesNotAggregateAcrossFrames 时间戳变化时必须先结算
// 参数集，SPS/PPS 不能和下一帧的 slice 混在一个 STAP-A 里。
func TestStreamH264DoesNotAggregateAcrossFrames(t *testing.T) {
	sps := []byte{0x67, 0x42, 0xC0, 0x1E}
	pps := []byte{0x68, 0xCE, 0x3C, 0x80}
	idr := []byte{0x65, 0x88, 0x84, 0x00}
	src := &framesSource{frames: []source.Frame{
		{Data: sps},
		{Data: pps},
		{Data: idr, Timestamp: 3600, Keyframe: true},
	}}

	pkts := runVideoStream(t, src, 2)
	if got := pkts[0].Payload[0] & 0x1F; got != rtp.NALUTypeSTAPA {
		t.Fatalf("first packet type = %d, want STAP-A(24)", got)
	}
	if nalus := parseSTAPANALUs(t, pkts[0].Payload); len(nalus) != 2 {
		t.Fatalf("STAP-A contains %d NALUs, want SPS+PPS only", len(nalus))
	}
	if got := pkts[1].Payload[0] & 0x1F; got != rtp.NALUTypeIDR {
		t.Fatalf("second packet type = %d, want IDR(5)", got)
	}
	if !pkts[0].Header.Marker || !pkts[1].Header.Marker {
		t.Fatal("each frame should end with marker on its last packet")
	}
}

// TestStreamH264MarkerOnLastPacket 大 IDR 回退 FU-A 后，整帧只有
// 最后一个分片带 marker。
func TestStreamH264MarkerOnLastPacket(t *testing.T) {
	sps := []byte{0x67, 0x42, 0xC0, 0x1E}
	pps := []byte{0x68, 0xCE, 0x3C, 0x80}
	idr := make([]byte, 1500)
	idr[0] = 0x65
	src := &framesSource{frames: []source.Frame{
		{Data: sps},
		{Data: pps},
		{Data: idr, Keyframe: true},
	}}

	// 1 个 STAP-A(SPS,PPS) + IDR 的 FU-A 分片数（MTU 1400，每片 1398 字节数据）。
	frags := (len(idr) - 1 + 1398 - 1) / 1398
	pkts := runVideoStream(t, src, 1+frags)

	if got := pkts[0].Payload[0] & 0x1F; got != rtp.NALUTypeSTAPA {
		t.Fatalf("first packet type = %d, want STAP-A(24)", got)
	}
	for i, pkt := range pkts[1:] {
		if got := pkt.Payload[0] & 0x1F; got != rtp.NALUTypeFUA {
			t.Fatalf("packet %d type = %d, want FU-A(28)", i+1, got)
		}
	}
	for i, pkt := range pkts {
		if want := i == len(pkts)-1; pkt.Header.Marker != want {
			t.Fatalf("packet %d marker = %v, want %v", i, pkt.Header.Marker, want)
		}
	}
}

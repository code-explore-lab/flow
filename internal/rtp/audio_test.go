package rtp

import (
	"bytes"
	"testing"
)

func TestG711PackerSplit(t *testing.T) {
	data := bytes.Repeat([]byte{0x55}, 2500)
	p := NewPCMAPacker()
	p.MTU = 1200
	pkts, err := p.Pack(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(pkts) != 3 {
		t.Fatalf("packets = %d, want 3", len(pkts))
	}
	sizes := []int{1200, 1200, 100}
	for i, pkt := range pkts {
		if pkt.Header.PT != PCMAPayloadType {
			t.Fatalf("packet %d PT = %d", i, pkt.Header.PT)
		}
		if len(pkt.Payload) != sizes[i] {
			t.Fatalf("packet %d size = %d, want %d", i, len(pkt.Payload), sizes[i])
		}
		if got := pkt.Header.Marker; got != (i == len(pkts)-1) {
			t.Fatalf("packet %d marker = %v", i, got)
		}
	}
	if !bytes.Equal(append(append(append([]byte{}, pkts[0].Payload...), pkts[1].Payload...), pkts[2].Payload...), data) {
		t.Fatal("payload not preserved across packets")
	}
}

func TestOpusPacker(t *testing.T) {
	p := NewOpusPacker()
	pkts, err := p.Pack([]byte{0x78, 0x01})
	if err != nil {
		t.Fatal(err)
	}
	if len(pkts) != 1 || !pkts[0].Header.Marker || pkts[0].Header.PT != OpusDefaultPayloadType {
		t.Fatalf("packets = %+v", pkts)
	}
}

// mp3TestFrame 生成 MPEG-1 Layer III 44100Hz 128kbps 立体声帧：长度 417。
func mp3TestFrame(fill byte) []byte {
	f := make([]byte, 417)
	f[0], f[1], f[2], f[3] = 0xFF, 0xFB, 0x90, 0x00
	for i := 4; i < len(f); i++ {
		f[i] = fill
	}
	return f
}

func TestMPAPackerRoundTrip(t *testing.T) {
	// 3 帧：1251 字节，按 MTU 1200 会切成两包。
	frame := append(append(mp3TestFrame(0x11), mp3TestFrame(0x22)...), mp3TestFrame(0x33)...)
	p := NewMPAPacker()
	p.MTU = 1200
	pkts, err := p.Pack(frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(pkts) != 2 {
		t.Fatalf("packets = %d, want 2", len(pkts))
	}

	d := NewMPADepacketizer()
	var got []MPAFrame
	for i, pkt := range pkts {
		pkt.Header.Timestamp = 9000
		frames, ok := d.Depacketize(pkt)
		got = append(got, frames...)
		if i == 0 && !ok {
			t.Fatalf("first packet should yield complete frames, got %d", len(frames))
		}
	}
	if len(got) != 3 {
		t.Fatalf("frames = %d, want 3", len(got))
	}
	for i, want := range [][]byte{mp3TestFrame(0x11), mp3TestFrame(0x22), mp3TestFrame(0x33)} {
		if !bytes.Equal(got[i].Data, want) {
			t.Fatalf("frame %d differs", i)
		}
	}
	// 每帧 1152 采样 @44100Hz、90kHz 时钟：+2351。
	wantTS := []uint32{9000, 9000 + 2351, 9000 + 4702}
	for i, want := range wantTS {
		if got[i].Timestamp != want {
			t.Fatalf("frame %d ts = %d, want %d", i, got[i].Timestamp, want)
		}
	}
}

// TestMPADepacketizerMultipleFramesPerPacket 模拟 ffmpeg 的封装方式：
// 一个 RTP 包放多个完整帧、marker 不置位。
func TestMPADepacketizerMultipleFramesPerPacket(t *testing.T) {
	body := append(mp3TestFrame(0x11), mp3TestFrame(0x22)...)
	payload := make([]byte, MPAHeaderSize+len(body))
	copy(payload[MPAHeaderSize:], body)
	pkt := newPacket(MPAPayloadType, payload, false)

	d := NewMPADepacketizer()
	frames, ok := d.Depacketize(pkt)
	if !ok || len(frames) != 2 {
		t.Fatalf("frames = %d ok = %v", len(frames), ok)
	}
	if !bytes.Equal(frames[0].Data, mp3TestFrame(0x11)) ||
		!bytes.Equal(frames[1].Data, mp3TestFrame(0x22)) {
		t.Fatal("frame content differs")
	}
}

func TestMPADepacketizerDropsOutOfOrder(t *testing.T) {
	d := NewMPADepacketizer()
	first := newPacket(MPAPayloadType, []byte{0, 0, 0, 0, 0xFF, 0xFB}, false)
	second := newPacket(MPAPayloadType, []byte{0, 0, 0, 2, 0x90, 0x00}, false)

	// 先到第二片：没有第一片，丢弃。
	if _, ok := d.Depacketize(second); ok {
		t.Fatal("out-of-order fragment completed")
	}
	// 再补第一片和完整第二片：4 个字节还不是完整帧，不应产出。
	if _, ok := d.Depacketize(first); ok {
		t.Fatal("incomplete frame produced")
	}
	if _, ok := d.Depacketize(second); ok {
		t.Fatal("incomplete frame produced")
	}
}

func TestMPADepacketizerHeaderless(t *testing.T) {
	d := NewMPADepacketizer()
	pkt := newPacket(MPAPayloadType, mp3TestFrame(0x44), true)
	frames, ok := d.Depacketize(pkt)
	if !ok || len(frames) != 1 || frames[0].Timestamp != 0 ||
		!bytes.Equal(frames[0].Data, pkt.Payload) {
		t.Fatalf("frames = %+v ok = %v", frames, ok)
	}
}

// TestMPADepacketizerResync 校验分片中途出现垃圾字节时能重新找到同步字。
func TestMPADepacketizerResync(t *testing.T) {
	d := NewMPADepacketizer()
	body := append([]byte{0x00, 0x01, 0x02}, mp3TestFrame(0x55)...)
	payload := make([]byte, MPAHeaderSize+len(body))
	copy(payload[MPAHeaderSize:], body)
	frames, ok := d.Depacketize(newPacket(MPAPayloadType, payload, true))
	if !ok || len(frames) != 1 || !bytes.Equal(frames[0].Data, mp3TestFrame(0x55)) {
		t.Fatalf("frames = %d ok = %v", len(frames), ok)
	}
}

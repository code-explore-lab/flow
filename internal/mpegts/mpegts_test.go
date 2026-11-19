package mpegts

import (
	"bytes"
	"testing"
	"time"

	"flow/internal/media"
)

var (
	testSPS = []byte{0x67, 0x42, 0x00, 0x1E, 0x95, 0xA8, 0x28, 0x0F, 0x00, 0x44, 0xFC, 0xB8, 0x08, 0x80}
	testPPS = []byte{0x68, 0xCE, 0x06, 0xE2}
	testASC = []byte{0x12, 0x10} // AAC LC / 44100 / stereo

	testIDR   = []byte{0x65, 0x80, 0x11, 0x22, 0x33}
	testSlice = []byte{0x65, 0x40, 0x44, 0x55} // 同一帧的后续 slice（first_mb != 0）
	testP     = []byte{0x41, 0x80, 0x66, 0x77}
)

func newTestMuxer(t *testing.T, buf *bytes.Buffer) *Muxer {
	t.Helper()
	m := NewMuxer(buf)
	m.SetVideoParams(testSPS, testPPS)
	if err := m.SetAudioParams(testASC); err != nil {
		t.Fatalf("SetAudioParams: %v", err)
	}
	return m
}

func demuxAll(t *testing.T, data []byte) ([]VideoAU, []AudioAU) {
	t.Helper()
	d := NewDemuxer()
	var videos []VideoAU
	var audios []AudioAU
	d.OnVideo = func(au VideoAU) {
		cp := VideoAU{PTS: au.PTS, DTS: au.DTS, Keyframe: au.Keyframe, Codec: au.Codec}
		for _, n := range au.NALUs {
			cp.NALUs = append(cp.NALUs, append([]byte(nil), n...))
		}
		videos = append(videos, cp)
	}
	d.OnAudio = func(au AudioAU) {
		au.AU = append([]byte(nil), au.AU...)
		au.ASC = append([]byte(nil), au.ASC...)
		audios = append(audios, au)
	}
	d.OnError = func(err error) { t.Logf("demux warning: %v", err) }
	d.Write(data)
	d.Flush()
	return videos, audios
}

func TestMuxDemuxRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	m := newTestMuxer(t, &buf)

	// 关键帧不再手动带 SPS/PPS，验证 muxer 会自动注入这两个参数集。
	if err := m.WriteVideo([][]byte{testIDR, testSlice}, 90000, 81000); err != nil {
		t.Fatalf("WriteVideo: %v", err)
	}
	if err := m.WriteAudio(bytes.Repeat([]byte{0xAA}, 128), 0); err != nil {
		t.Fatalf("WriteAudio: %v", err)
	}
	if err := m.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if buf.Len()%PacketSize != 0 {
		t.Fatalf("output length %d is not a multiple of %d", buf.Len(), PacketSize)
	}

	videos, audios := demuxAll(t, buf.Bytes())
	if len(videos) != 1 {
		t.Fatalf("got %d video AUs, want 1", len(videos))
	}
	v := videos[0]
	if !v.Keyframe {
		t.Error("IDR AU was not marked as keyframe")
	}
	if v.PTS != 90000 || v.DTS != 81000 {
		t.Errorf("PTS/DTS = %d/%d, want 90000/81000", v.PTS, v.DTS)
	}
	if v.CompositionTime() != 9000 {
		t.Errorf("composition time = %d, want 9000", v.CompositionTime())
	}
	var haveSPS, havePPS bool
	for _, n := range v.NALUs {
		typ, err := media.NALType(n)
		if err != nil {
			continue
		}
		switch typ {
		case 7:
			haveSPS = true
		case 8:
			havePPS = true
		}
	}
	if !haveSPS || !havePPS {
		t.Errorf("keyframe AU missing injected parameter sets: sps=%v pps=%v", haveSPS, havePPS)
	}

	if len(audios) != 1 {
		t.Fatalf("got %d audio AUs, want 1", len(audios))
	}
	a := audios[0]
	if !bytes.Equal(a.ASC, testASC) {
		t.Errorf("ASC = %x, want %x", a.ASC, testASC)
	}
	if a.SampleRate != 44100 || a.Channels != 2 {
		t.Errorf("audio params = %d Hz / %d ch, want 44100 / 2", a.SampleRate, a.Channels)
	}
	if len(a.AU) != 128 {
		t.Errorf("audio AU length = %d, want 128", len(a.AU))
	}
}

func TestMuxDemuxMultipleFramesAndTimestamps(t *testing.T) {
	var buf bytes.Buffer
	m := newTestMuxer(t, &buf)

	pts := []uint32{0, 3000, 6000, 9000}
	for _, ts := range pts {
		nalus := [][]byte{testP}
		if ts == 0 {
			nalus = [][]byte{testIDR}
		}
		if err := m.WriteVideo(nalus, ts, ts); err != nil {
			t.Fatalf("WriteVideo: %v", err)
		}
	}

	videos, _ := demuxAll(t, buf.Bytes())
	if len(videos) != len(pts) {
		t.Fatalf("got %d video AUs, want %d", len(videos), len(pts))
	}
	for i, v := range videos {
		if v.PTS != pts[i] || v.DTS != pts[i] {
			t.Errorf("AU %d PTS/DTS = %d/%d, want %d", i, v.PTS, v.DTS, pts[i])
		}
		if (i == 0) != v.Keyframe {
			t.Errorf("AU %d keyframe = %v", i, v.Keyframe)
		}
	}
}

func TestDemuxChunkedInput(t *testing.T) {
	var buf bytes.Buffer
	m := newTestMuxer(t, &buf)
	if err := m.WriteVideo([][]byte{testIDR}, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := m.WriteAudio(bytes.Repeat([]byte{1, 2, 3, 4}, 40), 0); err != nil {
		t.Fatal(err)
	}

	d := NewDemuxer()
	videos, audios := 0, 0
	d.OnVideo = func(VideoAU) { videos++ }
	d.OnAudio = func(AudioAU) { audios++ }
	data := buf.Bytes()
	// 故意用不能整除 188 的块大小喂入，覆盖跨包拼接。
	for off := 0; off < len(data); off += 37 {
		end := min(off+37, len(data))
		d.Write(data[off:end])
	}
	d.Flush()

	if videos != 1 || audios != 1 {
		t.Fatalf("got %d video / %d audio AUs, want 1 / 1", videos, audios)
	}
}

func TestGroupAUsSlices(t *testing.T) {
	aud := []byte{0x09, 0xF0}
	// 关键帧的第二个 slice 属于同一帧；P 帧开启新 AU。
	groups := groupAUs([][]byte{aud, testSPS, testPPS, testIDR, testSlice, testP}, "H264")
	if len(groups) != 2 {
		t.Fatalf("got %d groups, want 2", len(groups))
	}
	if len(groups[0]) != 5 || len(groups[1]) != 1 {
		t.Errorf("group sizes = %d/%d, want 5/1", len(groups[0]), len(groups[1]))
	}
}

func TestPCRPresentOnVideoPackets(t *testing.T) {
	var buf bytes.Buffer
	m := newTestMuxer(t, &buf)
	if err := m.WriteVideo([][]byte{testIDR}, 1000, 1000); err != nil {
		t.Fatal(err)
	}

	data := buf.Bytes()
	found := false
	for off := 0; off < len(data); off += PacketSize {
		pkt := data[off : off+PacketSize]
		pid := uint16(pkt[1]&0x1F)<<8 | uint16(pkt[2])
		afc := (pkt[3] >> 4) & 0x03
		if pid == DefaultVideoPID && afc == 0x03 && pkt[4] >= 7 && pkt[5]&0x10 != 0 {
			found = true
		}
	}
	if !found {
		t.Fatal("no video TS packet with PCR")
	}
}

func TestMuxerPSIRefresh(t *testing.T) {
	var buf bytes.Buffer
	m := newTestMuxer(t, &buf)
	if err := m.WriteVideo([][]byte{testP}, 0, 0); err != nil {
		t.Fatal(err)
	}
	first := buf.Len()
	if err := m.WriteVideo([][]byte{testP}, 3000, 3000); err != nil {
		t.Fatal(err)
	}
	// 500ms 内不重复发送 PSI，第二帧只应增加 PES 包。
	second := buf.Len() - first
	if second >= first {
		t.Errorf("PSI was retransmitted within %s (%d bytes vs first frame %d)", psiInterval, second, first)
	}

	time.Sleep(psiInterval + 20*time.Millisecond)
	before := buf.Len()
	if err := m.WriteVideo([][]byte{testP}, 6000, 6000); err != nil {
		t.Fatal(err)
	}
	if buf.Len()-before <= second {
		t.Error("PSI was not refreshed after the interval")
	}
}

var (
	hevcVPS = []byte{0x40, 0x01, 0x0C, 0x01, 0xFF, 0xFF}
	hevcSPS = []byte{0x42, 0x01, 0x01, 0x01, 0x60, 0x00}
	hevcPPS = []byte{0x44, 0x01, 0xC0, 0x73}
	// IDR_W_RADL（类型 19），first_slice_segment_in_pic_flag = 1。
	hevcIDR = []byte{0x26, 0x01, 0x80, 0xAA, 0xBB}
	// TRAIL_R（类型 1），新的 slice。
	hevcP = []byte{0x02, 0x01, 0x80, 0xCC}
)

// TestMuxDemuxRoundTripHEVC 验证 H.265 的 PMT stream_type、AUD 和 VPS/SPS/PPS
// 注入，demux 侧能按 HEVC 语法重新识别出关键帧。
func TestMuxDemuxRoundTripHEVC(t *testing.T) {
	var buf bytes.Buffer
	m := NewMuxer(&buf)
	m.SetVideoParamsFor("H265", hevcVPS, hevcSPS, hevcPPS)

	if err := m.WriteVideo([][]byte{hevcIDR}, 90000, 90000); err != nil {
		t.Fatalf("WriteVideo: %v", err)
	}
	if err := m.WriteVideo([][]byte{hevcP}, 93000, 93000); err != nil {
		t.Fatalf("WriteVideo: %v", err)
	}

	videos, _ := demuxAll(t, buf.Bytes())
	if len(videos) != 2 {
		t.Fatalf("got %d video AUs, want 2", len(videos))
	}
	v := videos[0]
	if v.Codec != "H265" {
		t.Errorf("AU codec = %q, want H265", v.Codec)
	}
	if !v.Keyframe {
		t.Error("IDR AU was not marked as keyframe")
	}
	var haveVPS, haveSPS, havePPS bool
	for _, n := range v.NALUs {
		typ, err := media.VideoNALType("H265", n)
		if err != nil {
			continue
		}
		switch typ {
		case media.HEVCNALTypeVPS:
			haveVPS = true
		case media.HEVCNALTypeSPS:
			haveSPS = true
		case media.HEVCNALTypePPS:
			havePPS = true
		}
	}
	if !haveVPS || !haveSPS || !havePPS {
		t.Errorf("keyframe AU missing injected parameter sets: vps=%v sps=%v pps=%v", haveVPS, haveSPS, havePPS)
	}
	if videos[1].Keyframe {
		t.Error("P AU was marked as keyframe")
	}
}

func TestGroupAUsHEVC(t *testing.T) {
	aud := []byte{0x46, 0x01}
	groups := groupAUs([][]byte{aud, hevcVPS, hevcSPS, hevcPPS, hevcIDR, hevcP}, "H265")
	if len(groups) != 2 {
		t.Fatalf("got %d groups, want 2", len(groups))
	}
	if len(groups[0]) != 5 || len(groups[1]) != 1 {
		t.Errorf("group sizes = %d/%d, want 5/1", len(groups[0]), len(groups[1]))
	}
}

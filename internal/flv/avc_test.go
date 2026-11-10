package flv

import (
	"bytes"
	"testing"
)

// 常见的 SPS/PPS（裸 NALU，无起始码）。
var (
	testSPS = []byte{0x67, 0x42, 0x00, 0x1E, 0x95, 0xA8, 0x14, 0x01, 0x6E, 0x40}
	testPPS = []byte{0x68, 0xCE, 0x3C, 0x80}
)

func TestAVCDecoderConfigRoundTrip(t *testing.T) {
	config := BuildAVCDecoderConfig(testSPS, testPPS)
	sps, pps, err := ParseAVCDecoderConfig(config)
	if err != nil {
		t.Fatalf("ParseAVCDecoderConfig: %v", err)
	}
	if !bytes.Equal(sps, testSPS) || !bytes.Equal(pps, testPPS) {
		t.Fatalf("sps=%x pps=%x, want %x / %x", sps, pps, testSPS, testPPS)
	}
}

func TestAVCDecoderConfigFields(t *testing.T) {
	config := BuildAVCDecoderConfig(testSPS, testPPS)

	if config[0] != 0x01 {
		t.Fatalf("configurationVersion = %#x, want 1", config[0])
	}
	// profile / compat / level 来自 SPS 第 2~4 字节。
	if config[1] != testSPS[1] || config[2] != testSPS[2] || config[3] != testSPS[3] {
		t.Fatalf("profile/compat/level = % x, want % x", config[1:4], testSPS[1:4])
	}
	if config[4] != 0xFF { // lengthSizeMinusOne=3
		t.Fatalf("byte4 = %#x, want 0xFF", config[4])
	}
	if config[5] != 0xE1 { // numOfSPS=1
		t.Fatalf("byte5 = %#x, want 0xE1", config[5])
	}
}

func TestParseAVCDecoderConfigErrors(t *testing.T) {
	if _, _, err := ParseAVCDecoderConfig(nil); err == nil {
		t.Fatal("accepted empty config")
	}
	if _, _, err := ParseAVCDecoderConfig([]byte{0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}); err == nil {
		t.Fatal("accepted wrong configurationVersion")
	}
	// 长度声明超出实际数据。
	bad := BuildAVCDecoderConfig(testSPS, testPPS)
	if _, _, err := ParseAVCDecoderConfig(bad[:len(bad)-1]); err == nil {
		t.Fatal("accepted truncated config")
	}
}

func TestVideoSequenceHeader(t *testing.T) {
	body := BuildVideoSequenceHeader(testSPS, testPPS)

	if body[0] != 0x17 {
		t.Fatalf("frame byte = %#x, want 0x17 (keyframe+AVC)", body[0])
	}
	if body[1] != AVCSeqHeader {
		t.Fatalf("packet type = %d, want 0", body[1])
	}
	if cts := getCTS24(body[2:5]); cts != 0 {
		t.Fatalf("cts = %d, want 0", cts)
	}

	vt, err := ParseVideoTag(body)
	if err != nil {
		t.Fatalf("ParseVideoTag: %v", err)
	}
	if !vt.Keyframe || vt.PacketType != AVCSeqHeader {
		t.Fatalf("video tag = %+v", vt)
	}
	sps, pps, err := ParseAVCDecoderConfig(vt.Config)
	if err != nil {
		t.Fatalf("parse config from tag: %v", err)
	}
	if !bytes.Equal(sps, testSPS) || !bytes.Equal(pps, testPPS) {
		t.Fatalf("sps/pps from tag = %x / %x", sps, pps)
	}
}

func TestVideoTagKeyAndInter(t *testing.T) {
	nalu := []byte{0x65, 0x88, 0x84, 0x00, 0x0A} // IDR
	keyBody := BuildVideoTag(nalu, true, 0)
	interBody := BuildVideoTag(nalu, false, 0)

	if keyBody[0] != 0x17 || interBody[0] != 0x27 {
		t.Fatalf("frame bytes = %#x / %#x, want 0x17 / 0x27", keyBody[0], interBody[0])
	}

	kt, err := ParseVideoTag(keyBody)
	if err != nil {
		t.Fatal(err)
	}
	if !kt.Keyframe || len(kt.NALUs) != 1 || !bytes.Equal(kt.NALUs[0], nalu) {
		t.Fatalf("key tag = %+v", kt)
	}
	it, err := ParseVideoTag(interBody)
	if err != nil {
		t.Fatal(err)
	}
	if it.Keyframe {
		t.Fatal("inter tag parsed as keyframe")
	}
}

func TestVideoTagCTS(t *testing.T) {
	for _, cts := range []int32{0, 1, -1, 40, -80, 0x7FFFFF, -0x800000} {
		body := BuildVideoTag([]byte{0x41}, false, cts)
		// 原始 3 字节往返。
		if got := getCTS24(body[2:5]); got != cts {
			t.Fatalf("raw cts(%d) = %d", cts, got)
		}
		vt, err := ParseVideoTag(body)
		if err != nil {
			t.Fatalf("ParseVideoTag(cts=%d): %v", cts, err)
		}
		if vt.CTS != cts {
			t.Fatalf("parsed cts = %d, want %d", vt.CTS, cts)
		}
	}
}

func TestVideoTagMultiNALU(t *testing.T) {
	n1 := []byte{0x65, 0x01, 0x02}
	n2 := []byte{0x41, 0xAA, 0xBB, 0xCC}
	// 手动拼两个 NALU（4 字节长度前缀）。
	body := make([]byte, 5+4+len(n1)+4+len(n2))
	body[0] = 0x17
	body[1] = AVCNALU
	off := 5
	putLen := func(n []byte) {
		body[off] = byte(len(n) >> 24)
		body[off+1] = byte(len(n) >> 16)
		body[off+2] = byte(len(n) >> 8)
		body[off+3] = byte(len(n))
		off += 4
		copy(body[off:], n)
		off += len(n)
	}
	putLen(n1)
	putLen(n2)

	vt, err := ParseVideoTag(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(vt.NALUs) != 2 || !bytes.Equal(vt.NALUs[0], n1) || !bytes.Equal(vt.NALUs[1], n2) {
		t.Fatalf("nalus = %x / %x", vt.NALUs, [][]byte{n1, n2})
	}
}

func TestParseVideoTagErrors(t *testing.T) {
	if _, err := ParseVideoTag([]byte{0x17}); err == nil {
		t.Fatal("accepted too-short tag")
	}
	if _, err := ParseVideoTag([]byte{0x10, 0x01, 0, 0, 0}); err == nil {
		t.Fatal("accepted non-AVC codec id")
	}
	// 截断的 NALU 长度。
	if _, err := ParseVideoTag([]byte{0x17, 0x01, 0, 0, 0, 0xFF}); err == nil {
		t.Fatal("accepted truncated nalu length")
	}
}

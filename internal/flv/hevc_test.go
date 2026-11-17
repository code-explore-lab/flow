package flv

import (
	"bytes"
	"testing"
)

// 合成的最小 HEVC 参数集：SPS 的 RBSP 里带一段合法的 profile_tier_level，
// 便于验证配置记录头里的 profile/level 字段。
var (
	testVPS     = []byte{0x40, 0x01, 0x0C, 0x01, 0xFF, 0xFF}
	testHEVCSPS = []byte{
		0x42, 0x01, // NAL header（type 33）
		0x01,                   // vps_id=0, max_sub_layers_minus1=0, nesting=1
		0x01,                   // profile_space=0, tier=0, profile_idc=1 (Main)
		0x60, 0x00, 0x00, 0x00, // general_profile_compatibility_flags
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // general_constraint_indicator_flags
		0x5D, // general_level_idc = 93 (Level 3.1)
	}
	testHEVCPPS = []byte{0x44, 0x01, 0xC0, 0x73}
)

func TestHEVCDecoderConfigRoundTrip(t *testing.T) {
	config := BuildHEVCDecoderConfigRecord(testVPS, testHEVCSPS, testHEVCPPS)
	vps, sps, pps, err := ParseHEVCDecoderConfigRecord(config)
	if err != nil {
		t.Fatalf("ParseHEVCDecoderConfigRecord: %v", err)
	}
	if !bytes.Equal(vps, testVPS) || !bytes.Equal(sps, testHEVCSPS) || !bytes.Equal(pps, testHEVCPPS) {
		t.Fatalf("round trip mismatch: vps=%x sps=%x pps=%x", vps, sps, pps)
	}
}

func TestHEVCDecoderConfigFields(t *testing.T) {
	config := BuildHEVCDecoderConfigRecord(testVPS, testHEVCSPS, testHEVCPPS)

	if config[0] != 0x01 {
		t.Fatalf("configurationVersion = %#x, want 1", config[0])
	}
	if config[1] != 0x01 { // profile_space=0 | tier=0 | profile_idc=1
		t.Fatalf("profile byte = %#x, want 0x01", config[1])
	}
	if !bytes.Equal(config[2:6], []byte{0x60, 0x00, 0x00, 0x00}) {
		t.Fatalf("compatibility flags = %x", config[2:6])
	}
	if config[12] != 0x5D {
		t.Fatalf("general_level_idc = %#x, want 0x5D", config[12])
	}
	if config[21]&0x03 != 0x03 { // lengthSizeMinusOne = 3
		t.Fatalf("lengthSizeMinusOne = %d, want 3", config[21]&0x03)
	}
	if config[22] != 3 { // numOfArrays: VPS + SPS + PPS
		t.Fatalf("numOfArrays = %d, want 3", config[22])
	}
}

func TestParseHEVCDecoderConfigErrors(t *testing.T) {
	if _, _, _, err := ParseHEVCDecoderConfigRecord(nil); err == nil {
		t.Fatal("accepted empty config")
	}
	if _, _, _, err := ParseHEVCDecoderConfigRecord([]byte{0x02, 0x01, 0x60}); err == nil {
		t.Fatal("accepted wrong configurationVersion")
	}
	// 头和 numOfArrays 都在，但没有参数集。
	config := make([]byte, 23)
	config[0] = 0x01
	if _, _, _, err := ParseHEVCDecoderConfigRecord(config); err == nil {
		t.Fatal("accepted config without parameter sets")
	}
}

func TestHEVCSequenceHeaderRoundTrip(t *testing.T) {
	header := BuildHEVCVideoSequenceHeader(testVPS, testHEVCSPS, testHEVCPPS)
	vt, err := ParseVideoTag(header)
	if err != nil {
		t.Fatalf("ParseVideoTag: %v", err)
	}
	if vt.Codec != "H265" {
		t.Fatalf("Codec = %q, want H265", vt.Codec)
	}
	if !vt.Keyframe {
		t.Fatal("sequence header should be marked keyframe")
	}
	if vt.PacketType != AVCSeqHeader {
		t.Fatalf("PacketType = %d, want %d", vt.PacketType, AVCSeqHeader)
	}
	vps, sps, pps, err := ParseHEVCDecoderConfigRecord(vt.Config)
	if err != nil {
		t.Fatalf("config parse: %v", err)
	}
	if !bytes.Equal(vps, testVPS) || !bytes.Equal(sps, testHEVCSPS) || !bytes.Equal(pps, testHEVCPPS) {
		t.Fatalf("config round trip mismatch")
	}
}

func TestHEVCVideoTagRoundTrip(t *testing.T) {
	idr := []byte{0x26, 0x01, 0xAF, 0x12}
	tag := BuildHEVCVideoTag(idr, true, 40)
	vt, err := ParseVideoTag(tag)
	if err != nil {
		t.Fatalf("ParseVideoTag: %v", err)
	}
	if vt.Codec != "H265" || !vt.Keyframe {
		t.Fatalf("Codec/Keyframe = %q/%v", vt.Codec, vt.Keyframe)
	}
	if vt.PacketType != AVCNALU {
		t.Fatalf("PacketType = %d, want %d", vt.PacketType, AVCNALU)
	}
	if vt.CTS != 40 {
		t.Fatalf("CTS = %d, want 40", vt.CTS)
	}
	if len(vt.NALUs) != 1 || !bytes.Equal(vt.NALUs[0], idr) {
		t.Fatalf("NALUs = %x, want %x", vt.NALUs, idr)
	}
}

// TestHEVCVideoTagMultiNALUWithCTS Enhanced RTMP 的 HEVC tag 也支持多 NALU
// 和负 CTS（B 帧 PTS 早于 DTS）。
func TestHEVCVideoTagMultiNALUWithCTS(t *testing.T) {
	sei := []byte{0x4E, 0x01, 0x05}
	idr := []byte{0x26, 0x01, 0xAF, 0x12}
	for _, cts := range []int32{80, -80} {
		tag := BuildHEVCVideoTagNALUs([][]byte{sei, idr}, true, cts)
		vt, err := ParseVideoTag(tag)
		if err != nil {
			t.Fatalf("cts=%d: %v", cts, err)
		}
		if vt.Codec != "H265" || !vt.Keyframe || vt.CTS != cts {
			t.Fatalf("cts=%d: codec=%q key=%v got=%d", cts, vt.Codec, vt.Keyframe, vt.CTS)
		}
		if len(vt.NALUs) != 2 || !bytes.Equal(vt.NALUs[0], sei) || !bytes.Equal(vt.NALUs[1], idr) {
			t.Fatalf("cts=%d: nalus = %x", cts, vt.NALUs)
		}
	}
}

func TestParseVideoTagCodecDispatch(t *testing.T) {
	// 传统 AVC 标签仍按 H264 解析。
	avc := BuildVideoTag([]byte{0x65, 0x88}, true, 0)
	vt, err := ParseVideoTag(avc)
	if err != nil {
		t.Fatal(err)
	}
	if vt.Codec != "H264" {
		t.Fatalf("legacy Codec = %q, want H264", vt.Codec)
	}

	// 未知 FourCC 应报错而不是当成 AVC。
	bad := append([]byte{exHeaderFlag | frameTypeKey<<4, 'v', 'p', '0', '9'}, 0)
	if _, err := ParseVideoTag(bad); err == nil {
		t.Fatal("accepted unsupported fourcc")
	}
}

func TestParseHEVCMetadataPacketIgnored(t *testing.T) {
	// ffmpeg 在 HEVC 流开头发的 Enhanced RTMP 元数据包（PacketType=4）。
	tag := []byte{exHeaderFlag | frameTypeKey<<4 | exPacketTypeMetadata}
	tag = append(tag, 'h', 'v', 'c', '1')
	tag = append(tag, 0x02, 0x00, 0x0A) // 任意 AMF 负载，内容不解析

	vt, err := ParseVideoTag(tag)
	if err != nil {
		t.Fatalf("metadata packet rejected: %v", err)
	}
	if vt.PacketType != AVCMetadata {
		t.Fatalf("PacketType = %d, want %d", vt.PacketType, AVCMetadata)
	}
	if vt.Codec != "H265" || len(vt.NALUs) != 0 {
		t.Fatalf("codec/nalus = %q/%x, want H265/none", vt.Codec, vt.NALUs)
	}
}

package transcode

import (
	"bytes"
	"testing"

	"flow/internal/stream"
)

func TestTSUnwrapperWrap(t *testing.T) {
	var u tsUnwrapper
	// 从接近 2^32 的位置开始，跨过回绕点。
	values := []uint32{0xFFFFF000, 0xFFFFF800, 0x00000000, 0x00000800}
	var prev int64
	for i, raw := range values {
		got := u.unwrap(raw)
		if i > 0 && got <= prev {
			t.Fatalf("unwrap(%#x) = %d, want > %d", raw, got, prev)
		}
		prev = got
	}
	// 累计了 0x1800 个 tick，跨过 2^32 回绕点。
	if prev != 0x100000800 {
		t.Errorf("final timestamp = %#x, want 0x100000800", prev)
	}
}

func TestTSUnwrapperBackwards(t *testing.T) {
	var u tsUnwrapper
	u.unwrap(3600)
	if got := u.unwrap(0); got != 0 {
		t.Errorf("unwrap(0) after 3600 = %d, want 0", got)
	}
}

func TestParamFingerprint(t *testing.T) {
	a := paramFingerprint(nil, []byte{1, 2}, []byte{3})
	b := paramFingerprint(nil, []byte{1, 2}, []byte{3})
	if !bytes.Equal(a, b) {
		t.Error("identical parameter sets should have equal fingerprints")
	}
	c := paramFingerprint(nil, []byte{1, 2}, []byte{4})
	if bytes.Equal(a, c) {
		t.Error("different PPS should have different fingerprints")
	}
	// VPS 与 SPS 的边界不能混淆。
	d := paramFingerprint([]byte{9}, []byte{1, 2}, []byte{3})
	if bytes.Equal(a, d) {
		t.Error("VPS presence should change the fingerprint")
	}
}

func TestIsAUD(t *testing.T) {
	cases := []struct {
		codec string
		nalu  []byte
		want  bool
	}{
		{"H264", []byte{0x09, 0xF0}, true},
		{"H264", []byte{0x65, 0x88}, false},
		{"H265", []byte{0x46, 0x01}, true},
		{"H265", []byte{0x26, 0x01}, false},
	}
	for _, tc := range cases {
		if got := isAUD(tc.codec, tc.nalu); got != tc.want {
			t.Errorf("isAUD(%s, %x) = %v, want %v", tc.codec, tc.nalu, got, tc.want)
		}
	}
}

func TestBoundedBufferTruncates(t *testing.T) {
	b := newBoundedBuffer(5)
	b.Write([]byte("abc"))
	b.Write([]byte("defg"))
	if got := b.String(); got != "cdefg" {
		t.Errorf("buffer = %q, want cdefg", got)
	}
}

func TestAudioFingerprint(t *testing.T) {
	a := audioFingerprint(&stream.AudioCodec{
		Codec: "AAC", Config: []byte{0x12, 0x10}, SampleRate: 44100, Channels: 2,
	})
	b := audioFingerprint(&stream.AudioCodec{
		Codec: "AAC", Config: []byte{0x12, 0x10}, SampleRate: 44100, Channels: 2,
	})
	if !bytes.Equal(a, b) {
		t.Error("identical audio params should have equal fingerprints")
	}
	c := audioFingerprint(&stream.AudioCodec{
		Codec: "AAC", Config: []byte{0x12, 0x10}, SampleRate: 48000, Channels: 2,
	})
	if bytes.Equal(a, c) {
		t.Error("different sample rate should change the fingerprint")
	}
}

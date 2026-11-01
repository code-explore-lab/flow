package media

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// buildADTSFrame 构造一帧 ADTS：MPEG-4、AAC LC(profile=1)、44100(sfi=4)、
// 立体声(chCfg=2)、无 CRC、VBR(fullness=0x7FF)、单个 raw_data_block。
func buildADTSFrame(payload []byte) []byte {
	frameLen := 7 + len(payload)
	const (
		profile    = 1
		sfi        = 4
		channelCfg = 2
		fullness   = 0x7FF
	)
	h := make([]byte, 7)
	h[0] = 0xFF
	h[1] = 0xF1                            // 1111 ID=0 layer=00 protection_absent=1
	h[2] = byte(profile<<6) | byte(sfi<<2) // chCfg 最高位=0，private=0
	h[3] = byte(channelCfg&0x03)<<6 | byte((frameLen>>11)&0x03)
	h[4] = byte((frameLen >> 3) & 0xFF)
	h[5] = byte((frameLen&0x07)<<5) | byte((fullness>>6)&0x1F)
	h[6] = byte((fullness & 0x3F) << 2) // blocks=0
	return append(h, payload...)
}

func TestParseADTSHeader(t *testing.T) {
	payload := []byte{0xAA, 0xBB, 0xCC, 0xDD}
	h, err := ParseADTSHeader(buildADTSFrame(payload))
	if err != nil {
		t.Fatalf("ParseADTSHeader err: %v", err)
	}
	if h.ID != 0 {
		t.Fatalf("ID = %d, want 0 (MPEG-4)", h.ID)
	}
	if !h.ProtectionAbsent {
		t.Fatal("ProtectionAbsent should be true (no CRC)")
	}
	if h.Profile != 1 || h.AudioObjectType != 2 {
		t.Fatalf("profile=%d AOT=%d, want 1/2 (AAC LC)", h.Profile, h.AudioObjectType)
	}
	if h.SamplingFrequencyIndex != 4 || h.SamplingFrequency != 44100 {
		t.Fatalf("sfi=%d freq=%d, want 4/44100", h.SamplingFrequencyIndex, h.SamplingFrequency)
	}
	if h.ChannelConfig != 2 {
		t.Fatalf("ChannelConfig = %d, want 2", h.ChannelConfig)
	}
	if h.FrameLength != 7+len(payload) {
		t.Fatalf("FrameLength = %d, want %d", h.FrameLength, 7+len(payload))
	}
	if h.HeaderLength != 7 {
		t.Fatalf("HeaderLength = %d, want 7", h.HeaderLength)
	}
}

func TestNextAUSplitByFrameLength(t *testing.T) {
	payloads := [][]byte{
		{0x01, 0x02, 0x03, 0x04},
		{0xAA, 0xBB},
		{0x11, 0x22, 0x33, 0x44, 0x55, 0x66},
	}
	var blob []byte
	for _, p := range payloads {
		blob = append(blob, buildADTSFrame(p)...)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "test.aac")
	if err := os.WriteFile(path, blob, 0o644); err != nil {
		t.Fatal(err)
	}

	demux, err := NewAACDemux(path)
	if err != nil {
		t.Fatal(err)
	}

	var got [][]byte
	for {
		au, err := demux.NextAU()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, au)
	}

	if len(got) != len(payloads) {
		t.Fatalf("got %d AUs, want %d", len(got), len(payloads))
	}
	for i := range payloads {
		if !bytes.Equal(got[i], payloads[i]) {
			t.Fatalf("AU[%d] mismatch: got %x want %x", i, got[i], payloads[i])
		}
	}
	if demux.First == nil {
		t.Fatal("First header should be recorded")
	}
	if demux.First.SamplingFrequency != 44100 || demux.First.ChannelConfig != 2 {
		t.Fatalf("First header wrong: %+v", demux.First)
	}
}

func TestNextAUSkipsJunkPrefix(t *testing.T) {
	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	blob := append([]byte{0x00, 0x00, 0x01, 0xFF, 0xFE}, buildADTSFrame(payload)...)

	dir := t.TempDir()
	path := filepath.Join(dir, "junk.aac")
	if err := os.WriteFile(path, blob, 0o644); err != nil {
		t.Fatal(err)
	}

	demux, err := NewAACDemux(path)
	if err != nil {
		t.Fatal(err)
	}
	au, err := demux.NextAU()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(au, payload) {
		t.Fatalf("AU mismatch: got %x want %x", au, payload)
	}
}

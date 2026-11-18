package media

import (
	"encoding/hex"
	"testing"
)

// 来自 testdata/test.h264 的真实 SPS/PPS（H.264 High 3.0，640x480，含 emulation prevention）。
const (
	testSPSHex = "6764001eacd940a03db011000003000100000300320f162d9600"
	testPPSHex = "68ebecb22c"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("decode hex: %v", err)
	}
	return b
}

func TestParseSPSReal(t *testing.T) {
	info, err := ParseSPS(mustHex(t, testSPSHex))
	if err != nil {
		t.Fatalf("ParseSPS: %v", err)
	}
	if info.ProfileIDC != 100 {
		t.Errorf("ProfileIDC = %d, want 100 (High)", info.ProfileIDC)
	}
	if info.ConstraintFlags != 0 {
		t.Errorf("ConstraintFlags = %#x, want 0", info.ConstraintFlags)
	}
	if info.LevelIDC != 30 {
		t.Errorf("LevelIDC = %d, want 30", info.LevelIDC)
	}
	if info.ChromaFormatIDC != 1 {
		t.Errorf("ChromaFormatIDC = %d, want 1 (4:2:0)", info.ChromaFormatIDC)
	}
	if info.Width != 640 || info.Height != 480 {
		t.Errorf("size = %dx%d, want 640x480", info.Width, info.Height)
	}
	if !info.FrameMBSOnly {
		t.Error("FrameMBSOnly = false, want true")
	}
	if got := info.CodecString(); got != "avc1.64001e" {
		t.Errorf("CodecString = %q, want avc1.64001e", got)
	}
}

func TestParseSPSInvalid(t *testing.T) {
	sps := mustHex(t, testSPSHex)
	tests := []struct {
		name string
		in   []byte
	}{
		{"empty", nil},
		{"too short", []byte{0x67, 0x64, 0x00}},
		{"wrong nal type", append([]byte{0x68}, sps[1:]...)},
		{"forbidden bit", append([]byte{0xE7}, sps[1:]...)},
		{"truncated rbsp", sps[:8]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseSPS(tt.in); err == nil {
				t.Fatal("ParseSPS returned nil error")
			}
		})
	}
}

func TestUnescapeRBSP(t *testing.T) {
	in := []byte{0x00, 0x00, 0x03, 0x01, 0x00, 0x00, 0x03, 0x00, 0x02}
	got := unescapeRBSP(in)
	want := []byte{0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d (% x)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("[%d] = %#x, want %#x", i, got[i], want[i])
		}
	}
}

package rtcp

import (
	"encoding/binary"
	"testing"
)

func TestBYEMarshal(t *testing.T) {
	const ssrc = 0xDEADBEEF
	b := BYE(ssrc)

	if len(b) != 8 {
		t.Fatalf("len = %d, want 8", len(b))
	}
	if b[0] != 0x81 { // V=2, P=0, SC=1
		t.Fatalf("first byte = %#x, want 0x81", b[0])
	}
	if b[1] != ByeType {
		t.Fatalf("PT = %d, want %d", b[1], ByeType)
	}
	if got := binary.BigEndian.Uint16(b[2:4]); got != 1 {
		t.Fatalf("length = %d, want 1", got)
	}
	if got := binary.BigEndian.Uint32(b[4:8]); got != ssrc {
		t.Fatalf("SSRC = %#x, want %#x", got, ssrc)
	}

	ssrcs, ok := ParseBYE(b)
	if !ok || len(ssrcs) != 1 || ssrcs[0] != ssrc {
		t.Fatalf("ParseBYE = (%v, %v), want ([%#x], true)", ssrcs, ok, ssrc)
	}
}

func TestBYEMultipleSSRC(t *testing.T) {
	b := BYE(1, 2, 3)

	if len(b) != 16 {
		t.Fatalf("len = %d, want 16", len(b))
	}
	if b[0] != 0x83 {
		t.Fatalf("first byte = %#x, want 0x83 (SC=3)", b[0])
	}
	if got := binary.BigEndian.Uint16(b[2:4]); got != 3 {
		t.Fatalf("length = %d, want 3", got)
	}

	ssrcs, ok := ParseBYE(b)
	if !ok {
		t.Fatal("ParseBYE failed")
	}
	want := []uint32{1, 2, 3}
	if len(ssrcs) != len(want) {
		t.Fatalf("ssrc count = %d, want %d", len(ssrcs), len(want))
	}
	for i := range want {
		if ssrcs[i] != want[i] {
			t.Fatalf("ssrc[%d] = %d, want %d", i, ssrcs[i], want[i])
		}
	}
}

func TestParseBYERejectsInvalid(t *testing.T) {
	valid := BYE(0x01020304)

	if _, ok := ParseBYE(valid[:7]); ok {
		t.Fatal("short packet should be rejected")
	}

	wrongPT := append([]byte(nil), valid...)
	wrongPT[1] = SenderReportType
	if _, ok := ParseBYE(wrongPT); ok {
		t.Fatal("non-BYE packet should be rejected")
	}

	wrongVersion := append([]byte(nil), valid...)
	wrongVersion[0] = 0x41 // V=1
	if _, ok := ParseBYE(wrongVersion); ok {
		t.Fatal("wrong version should be rejected")
	}

	badLength := append([]byte(nil), valid...)
	binary.BigEndian.PutUint16(badLength[2:4], 0xFFFF)
	if _, ok := ParseBYE(badLength); ok {
		t.Fatal("length past the buffer should be rejected")
	}

	// SC 声称有 2 个 SSRC，但包体只有一个。
	scTooLarge := append([]byte(nil), valid...)
	scTooLarge[0] = 0x82
	if _, ok := ParseBYE(scTooLarge); ok {
		t.Fatal("SC larger than the SSRC list should be rejected")
	}
}

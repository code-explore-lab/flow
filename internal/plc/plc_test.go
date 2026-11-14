package plc

import (
	"bytes"
	"testing"
)

func TestPLIRoundTrip(t *testing.T) {
	b := PLI(0x11223344, 0xAABBCCDD)
	if len(b) != PLISize {
		t.Fatalf("len = %d, want %d", len(b), PLISize)
	}
	if b[0] != 0x81 || b[1] != PSFBType {
		t.Fatalf("header = %02x %02x, want 81 %d", b[0], b[1], PSFBType)
	}
	sender, media, ok := ParsePLI(b)
	if !ok || sender != 0x11223344 || media != 0xAABBCCDD {
		t.Fatalf("parsed sender=%08x media=%08x ok=%v", sender, media, ok)
	}
	if _, _, ok := ParsePLI(b[:8]); ok {
		t.Fatal("truncated PLI accepted")
	}
	bad := append([]byte(nil), b...)
	bad[1] = 200
	if _, _, ok := ParsePLI(bad); ok {
		t.Fatal("SR accepted as PLI")
	}
}

func TestSilenceAU(t *testing.T) {
	mono, ok := SilenceAU(1)
	if !ok || !bytes.Equal(mono, []byte{0x01, 0x18, 0x20, 0x07}) {
		t.Fatalf("mono silence = % x ok=%v", mono, ok)
	}
	stereo, ok := SilenceAU(2)
	if !ok || !bytes.Equal(stereo, []byte{0x21, 0x10, 0x04, 0x60, 0x8C, 0x1C}) {
		t.Fatalf("stereo silence = % x ok=%v", stereo, ok)
	}
	for _, ch := range []int{0, 3, 8, -1} {
		if _, ok := SilenceAU(ch); ok {
			t.Fatalf("channels=%d has silence AU", ch)
		}
	}
}

func TestAACLCChannels(t *testing.T) {
	cases := []struct {
		name string
		asc  []byte
		want int
		ok   bool
	}{
		{"stereo LC", []byte{0x12, 0x10}, 2, true},
		{"mono LC", []byte{0x12, 0x08}, 1, true},
		{"HE-AAC", []byte{0x2A, 0x10}, 0, false},
		{"3 channels", []byte{0x12, 0x18}, 0, false},
		{"short", []byte{0x12}, 0, false},
		{"empty", nil, 0, false},
	}
	for _, c := range cases {
		got, ok := AACLCChannels(c.asc)
		if got != c.want || ok != c.ok {
			t.Fatalf("%s: got (%d,%v), want (%d,%v)", c.name, got, ok, c.want, c.ok)
		}
	}
}

func TestMissingAUs(t *testing.T) {
	cases := []struct {
		name              string
		prevEnd, ts, step uint32
		want              int
	}{
		{"contiguous", 2048, 3072, 1024, 1},
		{"none", 2048, 2048, 1024, 0},
		{"backwards", 3072, 2048, 1024, 0},
		{"two", 1024, 3072, 1024, 2},
		{"zero step", 1024, 4096, 0, 0},
		{"capped", 0, 1024 * 1000, 1024, MaxConcealAUs},
	}
	for _, c := range cases {
		if got := MissingAUs(c.prevEnd, c.ts, c.step); got != c.want {
			t.Fatalf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}

func TestVideoConceal(t *testing.T) {
	v := NewVideo(2)
	if _, ok := v.Conceal(); ok {
		t.Fatal("Conceal without Observe succeeded")
	}
	au := [][]byte{{0x41, 1, 2}, {0x41, 3}}
	v.Observe(au)
	for i := range 2 {
		rep, ok := v.Conceal()
		if !ok {
			t.Fatalf("Conceal %d rejected", i)
		}
		if len(rep) != 2 || !bytes.Equal(rep[0], au[0]) || !bytes.Equal(rep[1], au[1]) {
			t.Fatalf("repeat = %x", rep)
		}
	}
	if _, ok := v.Conceal(); ok {
		t.Fatal("Conceal beyond cap succeeded")
	}
	// 新的完整 AU 重置连续重复计数。
	v.Observe(au)
	if _, ok := v.Conceal(); !ok {
		t.Fatal("Conceal after new Observe rejected")
	}
}

func TestVideoObserveCopiesData(t *testing.T) {
	v := NewVideo(1)
	au := []byte{0x41, 0xAA}
	v.Observe([][]byte{au})
	au[1] = 0xFF
	rep, ok := v.Conceal()
	if !ok || rep[0][1] != 0xAA {
		t.Fatalf("cached frame mutated: %x", rep)
	}
}

func TestVideoDefaults(t *testing.T) {
	v := NewVideo(0)
	if v.maxRepeat != DefaultMaxRepeat {
		t.Fatalf("maxRepeat = %d, want %d", v.maxRepeat, DefaultMaxRepeat)
	}
}

func TestVideoReset(t *testing.T) {
	v := NewVideo(1)
	v.Observe([][]byte{{0x41}})
	v.Reset()
	if _, ok := v.Conceal(); ok {
		t.Fatal("Conceal after Reset succeeded")
	}
}

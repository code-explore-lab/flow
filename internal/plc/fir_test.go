package plc

import "testing"

func TestFIRRoundTrip(t *testing.T) {
	b := FIR(0x11223344, 0xAABBCCDD, 7)
	if len(b) != FIRSize {
		t.Fatalf("len = %d, want %d", len(b), FIRSize)
	}
	if b[0] != 0x84 {
		t.Fatalf("first byte = %#x, want 0x84 (V=2, FMT=4)", b[0])
	}
	if b[1] != PSFBType {
		t.Fatalf("PT = %d, want %d", b[1], PSFBType)
	}

	sender, target, seq, ok := ParseFIR(b)
	if !ok {
		t.Fatal("ParseFIR failed")
	}
	if sender != 0x11223344 || target != 0xAABBCCDD || seq != 7 {
		t.Fatalf("parsed sender=%08x target=%08x seq=%d", sender, target, seq)
	}
}

func TestFIRRejectsInvalid(t *testing.T) {
	good := FIR(1, 2, 0)
	cases := []struct {
		name string
		b    []byte
	}{
		{"empty", nil},
		{"truncated", good[:FIRSize-1]},
		{"bad version", mutate(good, 0, 0x44)},
		{"bad pt", mutate(good, 1, 205)},
		{"bad fmt", mutate(good, 0, 0x81)}, // FMT=1 是 PLI
	}
	for _, c := range cases {
		if _, _, _, ok := ParseFIR(c.b); ok {
			t.Fatalf("%s accepted", c.name)
		}
	}
}

// mutate 复制一份并修改指定字节，避免共享底层数组。
func mutate(b []byte, i int, v byte) []byte {
	out := append([]byte(nil), b...)
	out[i] = v
	return out
}

package rtcp

import "testing"

// compound 把若干 RTCP 子包拼成一个复合包。
func compound(pkts ...[]byte) []byte {
	var b []byte
	for _, p := range pkts {
		b = append(b, p...)
	}
	return b
}

func TestParseNACKsCompound(t *testing.T) {
	rr := []byte{0x80, 201, 0, 1, 0, 0, 0, 1} // RR，length=1
	nack := NACK(0x11223344, 0xAABBCCDD, []uint16{8, 9, 12})
	got := ParseNACKs(compound(rr, nack, rr))
	if len(got) != 1 {
		t.Fatalf("ParseNACKs returned %d feedbacks, want 1", len(got))
	}
	if got[0].SenderSSRC != 0x11223344 || got[0].MediaSSRC != 0xAABBCCDD {
		t.Fatalf("feedback = %+v", got[0])
	}
	if !sameSeqSet(got[0].Seqs, []uint16{8, 9, 12}) {
		t.Fatalf("seqs = %v, want [8 9 12]", got[0].Seqs)
	}
}

func TestParseNACKsMultiple(t *testing.T) {
	a := NACK(1, 0xAAA, []uint16{1})
	b := NACK(2, 0xBBB, []uint16{2})
	got := ParseNACKs(compound(a, b))
	if len(got) != 2 {
		t.Fatalf("ParseNACKs returned %d feedbacks, want 2", len(got))
	}
	if got[0].MediaSSRC != 0xAAA || got[1].MediaSSRC != 0xBBB {
		t.Fatalf("feedbacks = %+v", got)
	}
}

func TestParseNACKsIgnoresNonNACK(t *testing.T) {
	sr := SenderReport{SSRC: 1}.Marshal()
	rr := []byte{0x80, 201, 0, 1, 0, 0, 0, 1}
	if got := ParseNACKs(compound(sr, rr)); len(got) != 0 {
		t.Fatalf("non-NACK packets produced %+v", got)
	}
	if got := ParseNACKs(nil); len(got) != 0 {
		t.Fatalf("empty input produced %+v", got)
	}
}

func TestParseNACKsBadFMT(t *testing.T) {
	b := NACK(1, 2, []uint16{5})
	b[0] = 0x82 // FMT=2，不是 Generic NACK
	if got := ParseNACKs(b); len(got) != 0 {
		t.Fatalf("FMT=2 NACK accepted: %+v", got)
	}
}

func TestWalkStopsOnBadLength(t *testing.T) {
	bad := []byte{0x80, 201, 0xFF, 0xFF} // length 远超实际数据
	called := false
	Walk(bad, func(uint8, []byte) bool {
		called = true
		return true
	})
	if called {
		t.Fatal("Walk called fn for an invalid packet")
	}

	// 第二个子包被截断：只应访问第一个。
	rr := []byte{0x80, 201, 0, 1, 0, 0, 0, 1}
	nack := NACK(1, 2, []uint16{3})
	var count int
	Walk(append(rr, nack[:len(nack)-2]...), func(uint8, []byte) bool {
		count++
		return true
	})
	if count != 1 {
		t.Fatalf("Walk visited %d packets, want 1", count)
	}
}

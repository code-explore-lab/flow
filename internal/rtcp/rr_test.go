package rtcp

import (
	"encoding/binary"
	"testing"
)

// testRR 手工构造一个 RR 包，用来验证解析；生产代码只需要解析方向。
func testRR(reporter uint32, blocks ...ReportBlock) []byte {
	b := make([]byte, 8+ReportBlockSize*len(blocks))
	b[0] = 0x80 | byte(len(blocks))
	b[1] = ReceiverReportType
	binary.BigEndian.PutUint16(b[2:4], uint16(len(b)/4-1))
	binary.BigEndian.PutUint32(b[4:8], reporter)
	for i, blk := range blocks {
		off := 8 + i*ReportBlockSize
		binary.BigEndian.PutUint32(b[off:], blk.SSRC)
		b[off+4] = blk.FractionLost
		lost := uint32(blk.CumulativeLost) & 0xFFFFFF
		b[off+5] = byte(lost >> 16)
		b[off+6] = byte(lost >> 8)
		b[off+7] = byte(lost)
		binary.BigEndian.PutUint32(b[off+8:], blk.ExtendedHighestSeq)
		binary.BigEndian.PutUint32(b[off+12:], blk.Jitter)
		binary.BigEndian.PutUint32(b[off+16:], blk.LastSR)
		binary.BigEndian.PutUint32(b[off+20:], blk.DelaySinceLastSR)
	}
	return b
}

func TestParseRRSingleBlock(t *testing.T) {
	want := ReportBlock{
		SSRC:               0xDEADBEEF,
		FractionLost:       7,
		CumulativeLost:     1234,
		ExtendedHighestSeq: 0x0001ABCD,
		Jitter:             4500,
		LastSR:             0x11223344,
		DelaySinceLastSR:   65536,
	}
	b := testRR(0x01020304, want)

	rr, ok := ParseRR(b)
	if !ok {
		t.Fatal("ParseRR failed")
	}
	if rr.SSRC != 0x01020304 {
		t.Fatalf("reporter SSRC = %#x, want 0x01020304", rr.SSRC)
	}
	if len(rr.Blocks) != 1 || rr.Blocks[0] != want {
		t.Fatalf("block = %+v, want %+v", rr.Blocks, want)
	}
}

func TestParseRRNegativeCumulativeLost(t *testing.T) {
	for _, want := range []int32{-1, -12, -0x800000, 0, 0x7FFFFF} {
		b := testRR(1, ReportBlock{SSRC: 2, CumulativeLost: want})
		rr, ok := ParseRR(b)
		if !ok {
			t.Fatalf("lost %d: ParseRR failed", want)
		}
		if got := rr.Blocks[0].CumulativeLost; got != want {
			t.Fatalf("cumulative lost = %d, want %d", got, want)
		}
	}
}

func TestParseRRMultipleBlocks(t *testing.T) {
	first := ReportBlock{SSRC: 0x11111111, FractionLost: 1}
	second := ReportBlock{SSRC: 0x22222222, FractionLost: 2, CumulativeLost: -5}
	b := testRR(0x33333333, first, second)

	rr, ok := ParseRR(b)
	if !ok {
		t.Fatal("ParseRR failed")
	}
	if len(rr.Blocks) != 2 {
		t.Fatalf("block count = %d, want 2", len(rr.Blocks))
	}
	if rr.Blocks[0] != first || rr.Blocks[1] != second {
		t.Fatalf("blocks = %+v, want [%+v %+v]", rr.Blocks, first, second)
	}
}

func TestParseRREmpty(t *testing.T) {
	rr, ok := ParseRR(testRR(0xAABBCCDD))
	if !ok {
		t.Fatal("empty RR should parse")
	}
	if rr.SSRC != 0xAABBCCDD || len(rr.Blocks) != 0 {
		t.Fatalf("empty RR = %+v", rr)
	}
}

func TestParseRRRejectsInvalid(t *testing.T) {
	valid := testRR(1, ReportBlock{SSRC: 2})

	if _, ok := ParseRR(valid[:7]); ok {
		t.Fatal("short packet should be rejected")
	}

	wrongPT := append([]byte(nil), valid...)
	wrongPT[1] = SenderReportType
	if _, ok := ParseRR(wrongPT); ok {
		t.Fatal("non-RR packet should be rejected")
	}

	wrongVersion := append([]byte(nil), valid...)
	wrongVersion[0] = 0x41 // V=1
	if _, ok := ParseRR(wrongVersion); ok {
		t.Fatal("wrong version should be rejected")
	}

	// RC 声称有 2 个块，但长度只够 1 个。
	rcTooLarge := append([]byte(nil), valid...)
	rcTooLarge[0] = 0x82
	if _, ok := ParseRR(rcTooLarge); ok {
		t.Fatal("RC beyond the packet length should be rejected")
	}

	badLength := append([]byte(nil), valid...)
	binary.BigEndian.PutUint16(badLength[2:4], 0xFFFF)
	if _, ok := ParseRR(badLength); ok {
		t.Fatal("length past the buffer should be rejected")
	}
}

func TestParseRRsCompound(t *testing.T) {
	const reporter = 0x01020304
	blk := ReportBlock{SSRC: 0xDEADBEEF, FractionLost: 3, CumulativeLost: 9}
	pkt := Compound(testRR(reporter, blk), CNAME(reporter, "flow@host"))

	got := ParseRRs(pkt)
	if len(got) != 1 {
		t.Fatalf("RR count = %d, want 1", len(got))
	}
	if got[0].SSRC != reporter || len(got[0].Blocks) != 1 || got[0].Blocks[0] != blk {
		t.Fatalf("parsed = %+v, want reporter %#x block %+v", got, reporter, blk)
	}
}

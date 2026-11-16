package rtcp

import (
	"encoding/binary"
	"strings"
	"testing"
	"time"
)

func TestCNAMEMarshal(t *testing.T) {
	const ssrc = 0xDEADBEEF
	const name = "flow@host"

	b := CNAME(ssrc, name)

	if len(b) != 20 { // 4 头 + 4 SSRC + 2 item 头 + 9 CNAME + 1 END
		t.Fatalf("len = %d, want 20", len(b))
	}
	if b[0] != 0x81 { // V=2, SC=1
		t.Fatalf("first byte = %#x, want 0x81", b[0])
	}
	if b[1] != SDESType {
		t.Fatalf("PT = %d, want %d", b[1], SDESType)
	}
	if got := binary.BigEndian.Uint16(b[2:4]); got != 4 {
		t.Fatalf("length = %d, want 4", got)
	}
	if got := binary.BigEndian.Uint32(b[4:8]); got != ssrc {
		t.Fatalf("SSRC = %#x, want %#x", got, ssrc)
	}
	if b[8] != CNAMEItem {
		t.Fatalf("item type = %d, want %d", b[8], CNAMEItem)
	}
	if got := int(b[9]); got != len(name) {
		t.Fatalf("item length = %d, want %d", got, len(name))
	}
	if got := string(b[10 : 10+len(name)]); got != name {
		t.Fatalf("CNAME = %q, want %q", got, name)
	}
	if b[10+len(name)] != 0 {
		t.Fatal("CNAME chunk not terminated by END item")
	}

	gotSSRC, gotName, ok := ParseCNAME(b)
	if !ok {
		t.Fatal("ParseCNAME failed")
	}
	if gotSSRC != ssrc || gotName != name {
		t.Fatalf("round trip = (%#x, %q), want (%#x, %q)", gotSSRC, gotName, ssrc, name)
	}
}

func TestCNAMEIsPaddedToWordBoundary(t *testing.T) {
	for n := 1; n <= 12; n++ {
		name := strings.Repeat("a", n)
		b := CNAME(1, name)

		if len(b)%4 != 0 {
			t.Fatalf("cname len %d: packet len %d is not a multiple of 4", n, len(b))
		}
		if got, want := int(binary.BigEndian.Uint16(b[2:4])), len(b)/4-1; got != want {
			t.Fatalf("cname len %d: length field = %d, want %d", n, got, want)
		}
		if b[10+n] != 0 {
			t.Fatalf("cname len %d: missing END item", n)
		}
		for off := 11 + n; off < len(b); off++ {
			if b[off] != 0 {
				t.Fatalf("cname len %d: padding byte %d = %#x, want 0", n, off, b[off])
			}
		}
		if gotSSRC, gotName, ok := ParseCNAME(b); !ok || gotSSRC != 1 || gotName != name {
			t.Fatalf("cname len %d: round trip = (%#x, %q, %v)", n, gotSSRC, gotName, ok)
		}
	}
}

func TestCNAMEParseRejectsInvalid(t *testing.T) {
	valid := CNAME(1, "flow@host")

	short := append([]byte(nil), valid[:9]...)
	if _, _, ok := ParseCNAME(short); ok {
		t.Fatal("short packet should be rejected")
	}

	wrongPT := append([]byte(nil), valid...)
	wrongPT[1] = SenderReportType
	if _, _, ok := ParseCNAME(wrongPT); ok {
		t.Fatal("non-SDES packet should be rejected")
	}

	noChunks := append([]byte(nil), valid...)
	noChunks[0] = 0x80 // SC=0
	if _, _, ok := ParseCNAME(noChunks); ok {
		t.Fatal("packet without chunks should be rejected")
	}

	badLength := append([]byte(nil), valid...)
	binary.BigEndian.PutUint16(badLength[2:4], 0xFFFF)
	if _, _, ok := ParseCNAME(badLength); ok {
		t.Fatal("length field past the buffer should be rejected")
	}

	truncatedItem := append([]byte(nil), valid...)
	truncatedItem[9] = 0xFF // CNAME 长度超出剩余字节
	if _, _, ok := ParseCNAME(truncatedItem); ok {
		t.Fatal("item length past the chunk should be rejected")
	}

	// 只有 END、没有 CNAME。
	noCNAME := make([]byte, 12)
	noCNAME[0] = 0x81
	noCNAME[1] = SDESType
	binary.BigEndian.PutUint16(noCNAME[2:4], 2)
	binary.BigEndian.PutUint32(noCNAME[4:8], 1)
	if _, _, ok := ParseCNAME(noCNAME); ok {
		t.Fatal("chunk without CNAME should be rejected")
	}

	// 未知 item 在前，跳过后仍能找到 CNAME。
	withOtherItem := append([]byte(nil), valid...)
	withOtherItem[8] = 2 // NAME item，内容与 CNAME 相同，解析时跳过
	if _, name, ok := ParseCNAME(withOtherItem); ok {
		t.Fatalf("unknown item must not be reported as CNAME (got %q)", name)
	}
}

func TestCompoundSRAndCNAME(t *testing.T) {
	const ssrc = 0x01020304
	sr := SenderReport{SSRC: ssrc, NTP: NTPTime(time.Now())}.Marshal()
	pkt := Compound(sr, CNAME(ssrc, "flow@host"))

	if !IsRTCP(pkt) {
		t.Fatal("compound SR+SDES should look like RTCP")
	}

	var pts []uint8
	var cname string
	var nameSSRC uint32
	Walk(pkt, func(pt uint8, sub []byte) bool {
		pts = append(pts, pt)
		if pt == SDESType {
			nameSSRC, cname, _ = ParseCNAME(sub)
		}
		return true
	})
	if len(pts) != 2 || pts[0] != SenderReportType || pts[1] != SDESType {
		t.Fatalf("sub-packet types = %v, want [%d %d]", pts, SenderReportType, SDESType)
	}
	if nameSSRC != ssrc || cname != "flow@host" {
		t.Fatalf("SDES = (%#x, %q), want (%#x, %q)", nameSSRC, cname, ssrc, "flow@host")
	}
}

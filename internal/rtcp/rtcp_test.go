package rtcp

import (
	"encoding/binary"
	"testing"
	"time"
)

func TestNTPTime(t *testing.T) {
	cases := []struct {
		name string
		t    time.Time
		sec  uint64
		frac uint64
	}{
		{
			name: "unix epoch",
			t:    time.Unix(0, 0),
			sec:  ntpEpochOffset,
			frac: 0,
		},
		{
			name: "half second",
			t:    time.Unix(0, 500_000_000),
			sec:  ntpEpochOffset,
			frac: 1 << 31,
		},
		{
			name: "one nanosecond",
			t:    time.Unix(0, 1),
			sec:  ntpEpochOffset,
			frac: (1 << 32) / 1_000_000_000,
		},
		{
			name: "2026-11-01T00:00:00Z",
			t:    time.Unix(1793491200, 0),
			sec:  uint64(1793491200 + ntpEpochOffset),
			frac: 0,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := NTPTime(c.t)
			if got>>32 != c.sec {
				t.Fatalf("seconds = %d, want %d", got>>32, c.sec)
			}
			if got&0xFFFFFFFF != c.frac {
				t.Fatalf("fraction = %d, want %d", got&0xFFFFFFFF, c.frac)
			}
		})
	}
}

func TestNTPTimeFractionsIncrease(t *testing.T) {
	base := time.Unix(1793491200, 0)
	if NTPTime(base.Add(250*time.Millisecond)) <= NTPTime(base) {
		t.Fatal("later time must have larger NTP timestamp")
	}
}

func TestSenderReportMarshal(t *testing.T) {
	sr := SenderReport{
		SSRC:        0xDEADBEEF,
		NTP:         NTPTime(time.Unix(1793491200, 0)),
		RTP:         450000,
		PacketCount: 1234,
		OctetCount:  567890,
	}
	b := sr.Marshal()

	if len(b) != SenderReportSize {
		t.Fatalf("len = %d, want %d", len(b), SenderReportSize)
	}
	if b[0] != 0x80 {
		t.Fatalf("first byte = %#x, want 0x80 (V=2, P=0, RC=0)", b[0])
	}
	if b[1] != 200 {
		t.Fatalf("PT = %d, want 200", b[1])
	}
	if got := binary.BigEndian.Uint16(b[2:4]); got != 6 {
		t.Fatalf("length = %d, want 6", got)
	}

	got, ok := ParseSenderReport(b)
	if !ok {
		t.Fatal("ParseSenderReport failed")
	}
	if got.SSRC != sr.SSRC || got.NTP != sr.NTP || got.RTP != sr.RTP ||
		got.PacketCount != sr.PacketCount || got.OctetCount != sr.OctetCount {
		t.Fatalf("round trip mismatch: got %+v, want %+v", got, sr)
	}
}

func TestParseSenderReportRejectsOtherPackets(t *testing.T) {
	sr := SenderReport{SSRC: 1, NTP: NTPTime(time.Unix(0, 0))}
	b := sr.Marshal()

	// 太短
	if _, ok := ParseSenderReport(b[:SenderReportSize-1]); ok {
		t.Fatal("short packet should be rejected")
	}

	// 版本号不对
	bad := append([]byte(nil), b...)
	bad[0] = 0x40
	if _, ok := ParseSenderReport(bad); ok {
		t.Fatal("V=1 packet should be rejected")
	}

	// packet type 不对（比如 RR=201）
	bad = append([]byte(nil), b...)
	bad[1] = 201
	if _, ok := ParseSenderReport(bad); ok {
		t.Fatal("RR should be rejected")
	}
}

func TestIsRTCP(t *testing.T) {
	// RTP：V=2，PT=96，marker 置位后第二字节 224，必须判成 RTP。
	rtp := make([]byte, 12+4)
	rtp[0] = 0x80
	rtp[1] = 0x80 | 96
	binary.BigEndian.PutUint16(rtp[2:4], 8)
	binary.BigEndian.PutUint32(rtp[4:8], 1234)

	sr := SenderReport{SSRC: 1, NTP: NTPTime(time.Unix(0, 0))}.Marshal()
	rr := []byte{0x80, 201, 0, 1, 0, 0, 0, 1}
	nack := NACK(1, 2, []uint16{7})
	pli := []byte{0x80, 206, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2}

	cases := []struct {
		name string
		pkt  []byte
		want bool
	}{
		{"RTP PT=96 with marker", rtp, false},
		{"RTP PT=96 without marker", append([]byte{0x80, 96}, rtp[2:]...), false},
		{"SR", sr, true},
		{"RR", rr, true},
		{"NACK RTPFB", nack, true},
		{"PLI PSFB", pli, true},
		{"truncated SR", sr[:7], false},
		{"V=1", append([]byte{0x40}, sr[1:]...), false},
		{"unknown PT 120", append([]byte{0x80, 120}, make([]byte, 10)...), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsRTCP(c.pkt); got != c.want {
				t.Fatalf("IsRTCP(%x) = %v, want %v", c.pkt[:min(4, len(c.pkt))], got, c.want)
			}
		})
	}
}

func TestSenderReportLengthFieldIgnoresReportBlocks(t *testing.T) {
	// 带 1 个 report block 的 SR：长度 28+24=52 字节，length 字段应为 12。
	sr := SenderReport{SSRC: 0x01020304, RTP: 1, PacketCount: 2, OctetCount: 3}
	b := append(sr.Marshal(), make([]byte, 24)...)
	binary.BigEndian.PutUint16(b[2:4], 12)
	b[0] = 0x81 // RC=1

	got, ok := ParseSenderReport(b)
	if !ok {
		t.Fatal("SR with report block should still parse")
	}
	if got.SSRC != sr.SSRC || got.PacketCount != 2 {
		t.Fatalf("got %+v, want %+v", got, sr)
	}
}

package rtsp

import (
	"encoding/binary"
	"testing"
	"time"

	"flow/internal/rtcp"
)

// makeRRPkt 手工构造 RR 包（生产代码只会解析 RR，不会发送）。
func makeRRPkt(reporter uint32, blocks ...rtcp.ReportBlock) []byte {
	b := make([]byte, 8+rtcp.ReportBlockSize*len(blocks))
	b[0] = 0x80 | byte(len(blocks))
	b[1] = rtcp.ReceiverReportType
	binary.BigEndian.PutUint16(b[2:4], uint16(len(b)/4-1))
	binary.BigEndian.PutUint32(b[4:8], reporter)
	for i, blk := range blocks {
		off := 8 + i*rtcp.ReportBlockSize
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

func TestHandleIncomingRRSavesReport(t *testing.T) {
	s, sess, track, _ := newRTXTestTrack(t)
	blk := rtcp.ReportBlock{
		SSRC:               track.currentSSRC(),
		FractionLost:       7,
		CumulativeLost:     -3,
		ExtendedHighestSeq: 12345,
		Jitter:             90,
		LastSR:             0xAABBCCDD,
		DelaySinceLastSR:   6553,
	}
	// 客户端通常把 RR 和 SDES 放在同一个复合包里。
	pkt := rtcp.Compound(makeRRPkt(0x01020304, blk), rtcp.CNAME(0x01020304, "client@host"))
	before := time.Now()

	s.handleIncomingRTCP(track, sess, pkt)

	sender, got, at, ok := track.latestReceiverReport()
	if !ok {
		t.Fatal("RR report block was not stored")
	}
	if sender != 0x01020304 {
		t.Fatalf("reporter SSRC = %#x, want 0x01020304", sender)
	}
	if got != blk {
		t.Fatalf("report block = %+v, want %+v", got, blk)
	}
	if at.Before(before) || time.Since(at) > time.Minute {
		t.Fatalf("report arrival time = %v, want around %v", at, before)
	}
}

func TestHandleIncomingRRIgnoresOtherSSRC(t *testing.T) {
	s, sess, track, _ := newRTXTestTrack(t)
	blk := rtcp.ReportBlock{SSRC: track.currentSSRC() + 1, FractionLost: 1}

	s.handleIncomingRTCP(track, sess, makeRRPkt(0x01020304, blk))

	if _, _, _, ok := track.latestReceiverReport(); ok {
		t.Fatal("report block for another SSRC should be ignored")
	}
}

package rtp

import (
	"bytes"
	"testing"
)

// hevcRTP 构造一个测试用 RTP 包（只填解包器用到的字段）。
func hevcRTP(seq uint16, ts, ssrc uint32, payload []byte) *RTPPacket {
	pkt := newPacket(96, payload, false)
	pkt.Header.Seq = seq
	pkt.Header.Timestamp = ts
	pkt.Header.SSRC = ssrc
	return pkt
}

func TestH265DepacketizeSingleNALU(t *testing.T) {
	d := &H265Depacketizer{}
	nalu := []byte{0x42, 0x01, 0xAA, 0xBB}
	nalus, err := d.Depacketize(hevcRTP(1, 100, 7, nalu))
	if err != nil {
		t.Fatal(err)
	}
	if len(nalus) != 1 || !bytes.Equal(nalus[0], nalu) {
		t.Fatalf("got %x, want %x", nalus, nalu)
	}
}

func TestH265DepacketizeAP(t *testing.T) {
	vps := []byte{0x40, 0x01, 0x0C}
	sps := []byte{0x42, 0x01, 0x01}
	payload := packHEVCAP([][]byte{vps, sps})

	d := &H265Depacketizer{}
	nalus, err := d.Depacketize(hevcRTP(1, 100, 7, payload))
	if err != nil {
		t.Fatal(err)
	}
	if len(nalus) != 2 || !bytes.Equal(nalus[0], vps) || !bytes.Equal(nalus[1], sps) {
		t.Fatalf("AP split = %x", nalus)
	}
}

func TestH265FUARoundTrip(t *testing.T) {
	p := NewH265Packer(20)
	nalu := make([]byte, 60)
	nalu[0] = 0x26 // IDR_W_RADL
	nalu[1] = 0x01
	for i := 2; i < len(nalu); i++ {
		nalu[i] = byte(i)
	}
	pkts, err := p.Pack(nalu)
	if err != nil {
		t.Fatal(err)
	}

	d := &H265Depacketizer{}
	var out [][]byte
	for i, pkt := range pkts {
		pkt.Header.Seq = uint16(100 + i)
		pkt.Header.Timestamp = 9000
		pkt.Header.SSRC = 0x1234
		nalus, err := d.Depacketize(pkt)
		if err != nil {
			t.Fatalf("packet %d: %v", i, err)
		}
		out = append(out, nalus...)
	}
	if len(out) != 1 {
		t.Fatalf("got %d nalus, want 1", len(out))
	}
	if !bytes.Equal(out[0], nalu) {
		t.Fatalf("round trip mismatch:\n got %x\nwant %x", out[0], nalu)
	}
	if d.DroppedCount() != 0 {
		t.Fatalf("Dropped = %d, want 0", d.DroppedCount())
	}
}

func TestH265FUDropOnGap(t *testing.T) {
	p := NewH265Packer(10)
	nalu := make([]byte, 40)
	nalu[0] = 0x26
	nalu[1] = 0x01
	pkts, err := p.Pack(nalu)
	if err != nil {
		t.Fatal(err)
	}

	d := &H265Depacketizer{}
	// 首片正常，中间片缺失，尾片到达 → 半成品被丢弃。
	pkts[0].Header.Seq, pkts[0].Header.Timestamp, pkts[0].Header.SSRC = 1, 100, 7
	if _, err := d.Depacketize(pkts[0]); err != nil {
		t.Fatal(err)
	}
	last := pkts[len(pkts)-1]
	last.Header.Seq = uint16(1 + len(pkts)) // 跳过中间片
	last.Header.Timestamp, last.Header.SSRC = 100, 7
	nalus, err := d.Depacketize(last)
	if err != nil {
		t.Fatal(err)
	}
	if len(nalus) != 0 {
		t.Fatalf("got %x, want no NALU", nalus)
	}
	if d.DroppedCount() == 0 {
		t.Fatal("dropped half-assembled FU was not counted")
	}
}

func TestH265DepacketizeErrors(t *testing.T) {
	d := &H265Depacketizer{}
	if _, err := d.Depacketize(&RTPPacket{}); err == nil {
		t.Fatal("accepted empty payload")
	}
	if _, err := d.Depacketize(hevcRTP(1, 0, 0, []byte{0x42})); err == nil {
		t.Fatal("accepted 1-byte payload")
	}
	// AP 长度字段越界
	badAP := []byte{H265TypeAP << 1, 0x01, 0x00, 0xFF, 0x42, 0x01}
	if _, err := d.Depacketize(hevcRTP(1, 0, 0, badAP)); err == nil {
		t.Fatal("accepted AP with out-of-range NALU length")
	}
}

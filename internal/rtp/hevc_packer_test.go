package rtp

import (
	"bytes"
	"testing"
)

func TestH265PackSingleNALU(t *testing.T) {
	p := NewH265Packer(1400)
	nalu := []byte{0x42, 0x01, 0x00, 0x02} // type 33 (SPS)
	pkts, err := p.Pack(nalu)
	if err != nil {
		t.Fatalf("Pack err: %v", err)
	}
	if len(pkts) != 1 {
		t.Fatalf("want 1 packet, got %d", len(pkts))
	}
	if !bytes.Equal(pkts[0].Payload, nalu) {
		t.Fatalf("payload = %x, want %x", pkts[0].Payload, nalu)
	}
	if !pkts[0].Header.Marker || pkts[0].Header.PT != 96 {
		t.Fatalf("marker/PT = %v/%d", pkts[0].Header.Marker, pkts[0].Header.PT)
	}
}

func TestH265PackFUBoundary(t *testing.T) {
	mtu := 10
	p := NewH265Packer(mtu)

	if _, err := p.Pack([]byte{0x42}); err == nil {
		t.Fatal("accepted NALU shorter than 2-byte header")
	}

	// len <= MTU → 单包
	nalu := make([]byte, mtu)
	nalu[0] = 0x26 // type 19 (IDR_W_RADL)
	pkts, err := p.Pack(nalu)
	if err != nil {
		t.Fatal(err)
	}
	if len(pkts) != 1 {
		t.Fatalf("len==mtu should be single NALU, got %d packets", len(pkts))
	}

	// 多 1 字节 → FU 分片
	nalu = append(nalu, 0xFF)
	pkts, err = p.Pack(nalu)
	if err != nil {
		t.Fatal(err)
	}
	if len(pkts) < 2 {
		t.Fatalf("len==mtu+1 should fragment, got %d packets", len(pkts))
	}
	// 第一片带 S、最后一片带 E，类型字段都是 49。
	first, last := pkts[0], pkts[len(pkts)-1]
	if got := (first.Payload[0] >> 1) & 0x3F; got != H265TypeFU {
		t.Fatalf("first payload type = %d, want %d", got, H265TypeFU)
	}
	if first.Payload[2]&0x80 == 0 {
		t.Fatal("first FU missing S bit")
	}
	if last.Payload[2]&0x40 == 0 {
		t.Fatal("last FU missing E bit")
	}
	if got := int(last.Payload[2] & 0x3F); got != 19 {
		t.Fatalf("FU type = %d, want 19", got)
	}
	if !last.Header.Marker {
		t.Fatal("last FU should set marker")
	}
	if first.Header.Marker {
		t.Fatal("non-last FU should not set marker")
	}
}

func TestH265PackFrameAP(t *testing.T) {
	p := NewH265Packer(1400)
	vps := []byte{0x40, 0x01, 0x0C}
	sps := []byte{0x42, 0x01, 0x01}
	pps := []byte{0x44, 0x01, 0xC0}
	pkts, err := p.PackFrame([][]byte{vps, sps, pps})
	if err != nil {
		t.Fatal(err)
	}
	if len(pkts) != 1 {
		t.Fatalf("small parameter sets should aggregate, got %d packets", len(pkts))
	}
	payload := pkts[0].Payload
	if got := (payload[0] >> 1) & 0x3F; got != H265TypeAP {
		t.Fatalf("payload type = %d, want %d", got, H265TypeAP)
	}
	if !pkts[0].Header.Marker {
		t.Fatal("single aggregated packet should set marker")
	}
	nalus, err := splitHEVCAP(payload)
	if err != nil {
		t.Fatalf("splitHEVCAP: %v", err)
	}
	want := [][]byte{vps, sps, pps}
	if len(nalus) != len(want) {
		t.Fatalf("got %d nalus, want %d", len(nalus), len(want))
	}
	for i := range want {
		if !bytes.Equal(nalus[i], want[i]) {
			t.Fatalf("nalu %d = %x, want %x", i, nalus[i], want[i])
		}
	}
}

func TestH265PackFrameMarker(t *testing.T) {
	p := NewH265Packer(40)
	big := make([]byte, 100)
	big[0] = 0x26
	big[1] = 0x01
	pkts, err := p.PackFrame([][]byte{big, big})
	if err != nil {
		t.Fatal(err)
	}
	for i, pkt := range pkts {
		last := i == len(pkts)-1
		if pkt.Header.Marker != last {
			t.Fatalf("packet %d marker = %v, want %v", i, pkt.Header.Marker, last)
		}
	}
}

func TestH265PackFrameRejectsEmpty(t *testing.T) {
	p := NewH265Packer(1400)
	if _, err := p.PackFrame([][]byte{{0x01}}); err == nil {
		t.Fatal("accepted frame with no valid NALU")
	}
}

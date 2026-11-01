package rtp

import (
	"bytes"
	"testing"
)

func TestPackSingleNALU(t *testing.T) {
	p := NewH264Packer(1400)
	// 5 字节小 NALU（含 1 字节 NAL header），type=5(IDR) NRI=3
	nalu := []byte{0x65, 0x00, 0x01, 0x02, 0x03}
	pkts, err := p.Pack(nalu)
	if err != nil {
		t.Fatalf("Pack err: %v", err)
	}
	if len(pkts) != 1 {
		t.Fatalf("want 1 packet, got %d", len(pkts))
	}
	pkt := pkts[0]
	if !bytes.Equal(pkt.Payload, nalu) {
		t.Fatalf("single NALU payload mismatch: got %x want %x", pkt.Payload, nalu)
	}
	if !pkt.Header.Marker {
		t.Fatal("single NALU should set marker")
	}
	if pkt.Header.Version != 2 {
		t.Fatalf("version = %d, want 2", pkt.Header.Version)
	}
	if pkt.Header.PT != 96 {
		t.Fatalf("PT = %d, want 96", pkt.Header.PT)
	}
}

func TestPackFUABoundary(t *testing.T) {
	mtu := 10
	p := NewH264Packer(mtu)

	// 长度恰好等于 MTU → 单包
	nalu := make([]byte, mtu)
	nalu[0] = 0x65
	pkts, err := p.Pack(nalu)
	if err != nil {
		t.Fatal(err)
	}
	if len(pkts) != 1 {
		t.Fatalf("len==mtu should be single NALU, got %d packets", len(pkts))
	}

	// 多 1 字节 → 拆成 2 个 FU-A
	nalu2 := make([]byte, mtu+1)
	nalu2[0] = 0x65
	pkts2, err := p.Pack(nalu2)
	if err != nil {
		t.Fatal(err)
	}
	if len(pkts2) != 2 {
		t.Fatalf("len==mtu+1 should split into 2 FU-A, got %d", len(pkts2))
	}
}

func TestPackFUA(t *testing.T) {
	// MTU=5 → FU-A 数据部分每片 3 字节
	p := NewH264Packer(5)
	nalHeader := byte(0x65) // F=0, NRI=3, type=5
	data := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22}
	nalu := append([]byte{nalHeader}, data...)

	pkts, err := p.Pack(nalu)
	if err != nil {
		t.Fatal(err)
	}
	// 8 字节数据，每片 3 字节 → ceil(8/3)=3 片
	if len(pkts) != 3 {
		t.Fatalf("want 3 FU-A packets, got %d", len(pkts))
	}

	var gotData []byte
	for i, pkt := range pkts {
		pl := pkt.Payload
		if len(pl) < 2 {
			t.Fatalf("frag %d too short: len=%d", i, len(pl))
		}
		fuIndicator := pl[0]
		fuHeader := pl[1]

		if fuIndicator&0x1F != NALUTypeFUA {
			t.Fatalf("frag %d indicator type=%d, want 28", i, fuIndicator&0x1F)
		}
		if fuIndicator&0xE0 != nalHeader&0xE0 {
			t.Fatalf("frag %d F/NRI mismatch", i)
		}
		if fuHeader&0x1F != nalHeader&0x1F {
			t.Fatalf("frag %d type=%d, want %d", i, fuHeader&0x1F, nalHeader&0x1F)
		}

		s := fuHeader & 0x80
		e := fuHeader & 0x40
		last := i == len(pkts)-1
		if i == 0 && s == 0 {
			t.Fatal("first frag S should be 1")
		}
		if i > 0 && s != 0 {
			t.Fatal("non-first frag S should be 0")
		}
		if last && e == 0 {
			t.Fatal("last frag E should be 1")
		}
		if !last && e != 0 {
			t.Fatal("non-last frag E should be 0")
		}
		if last != pkt.Header.Marker {
			t.Fatalf("frag %d marker mismatch", i)
		}

		gotData = append(gotData, pl[2:]...)
	}

	if !bytes.Equal(gotData, data) {
		t.Fatalf("reassembled data mismatch: got %x want %x", gotData, data)
	}
}

func TestPackEmpty(t *testing.T) {
	p := NewH264Packer(1400)
	if _, err := p.Pack(nil); err == nil {
		t.Fatal("empty nalu should error")
	}
}

func TestPackMTUTooSmall(t *testing.T) {
	p := NewH264Packer(2) // FU-A 至少需要 2 字节头 + 1 字节数据
	nalu := make([]byte, 10)
	nalu[0] = 0x65
	if _, err := p.Pack(nalu); err == nil {
		t.Fatal("mtu too small should error")
	}
}

func TestPackMarshalRoundTrip(t *testing.T) {
	p := NewH264Packer(1400)
	nalu := []byte{0x65, 0x00, 0x01}
	pkts, err := p.Pack(nalu)
	if err != nil {
		t.Fatal(err)
	}
	pkts[0].Header.Seq = 1
	pkts[0].Header.Timestamp = 3600
	pkts[0].Header.SSRC = 0xDEADBEEF

	raw, err := pkts[0].Header.Marshal(pkts[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Unmarshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Header.Seq != 1 || got.Header.Timestamp != 3600 || got.Header.SSRC != 0xDEADBEEF {
		t.Fatalf("round trip mismatch: %+v", got.Header)
	}
	if !got.Header.Marker {
		t.Fatal("marker lost after round trip")
	}
}

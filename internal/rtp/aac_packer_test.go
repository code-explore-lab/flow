package rtp

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestPackAACHeader(t *testing.T) {
	p := NewAACPacker()
	au := []byte{0x21, 0x10, 0x05, 0x03, 0xFE} // 任意 5 字节裸 AAC 帧

	pkts, err := p.Pack(au)
	if err != nil {
		t.Fatalf("Pack err: %v", err)
	}
	if len(pkts) != 1 {
		t.Fatalf("want 1 packet, got %d", len(pkts))
	}

	pkt := pkts[0]
	if pkt.Header.Version != 2 {
		t.Fatalf("version = %d, want 2", pkt.Header.Version)
	}
	if pkt.Header.PT != 97 {
		t.Fatalf("PT = %d, want 97", pkt.Header.PT)
	}
	if !pkt.Header.Marker {
		t.Fatal("single AU packet should set marker")
	}

	pl := pkt.Payload
	if len(pl) != 4+len(au) {
		t.Fatalf("payload len = %d, want %d", len(pl), 4+len(au))
	}
	// AU-headers-length 是 16（bit），不是字节数
	if got := binary.BigEndian.Uint16(pl[0:2]); got != 16 {
		t.Fatalf("AU-headers-length = %d, want 16", got)
	}
	// AU-header：高 13 位 size，低 3 位 index=0
	if got := binary.BigEndian.Uint16(pl[2:4]); got != uint16(len(au))<<3 {
		t.Fatalf("AU-header = %#x, want %#x", got, uint16(len(au))<<3)
	}
	if !bytes.Equal(pl[4:], au) {
		t.Fatalf("AU data mismatch: got %x want %x", pl[4:], au)
	}
}

func TestPackAACCustomPT(t *testing.T) {
	p := NewAACPacker()
	p.PT = 98

	pkts, err := p.Pack([]byte{0x01, 0x02})
	if err != nil {
		t.Fatal(err)
	}
	if pkts[0].Header.PT != 98 {
		t.Fatalf("PT = %d, want 98", pkts[0].Header.PT)
	}
}

func TestPackAACEmpty(t *testing.T) {
	p := NewAACPacker()
	if _, err := p.Pack(nil); err == nil {
		t.Fatal("empty au should error")
	}
}

func TestPackAACSizeLimit(t *testing.T) {
	p := NewAACPacker()

	// 恰好 8191 字节：13 bit 能表达的上限，应该成功
	au := make([]byte, AACMaxAUSize)
	pkts, err := p.Pack(au)
	if err != nil {
		t.Fatalf("len=%d should succeed: %v", len(au), err)
	}
	got := binary.BigEndian.Uint16(pkts[0].Payload[2:4]) >> AACIndexLength
	if got != AACMaxAUSize {
		t.Fatalf("AU size field = %d, want %d", got, AACMaxAUSize)
	}

	// 8192 字节：超出 13 bit，必须报错而不是截断
	if _, err := p.Pack(make([]byte, AACMaxAUSize+1)); err == nil {
		t.Fatal("au larger than 13-bit size should error")
	}
}

func TestPackAACMarshalRoundTrip(t *testing.T) {
	p := NewAACPacker()
	au := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	pkts, err := p.Pack(au)
	if err != nil {
		t.Fatal(err)
	}
	pkts[0].Header.Seq = 7
	pkts[0].Header.Timestamp = AACFrameSamples
	pkts[0].Header.SSRC = 0xCAFEBABE

	raw, err := pkts[0].Header.Marshal(pkts[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Unmarshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Header.PT != 97 || got.Header.Seq != 7 ||
		got.Header.Timestamp != AACFrameSamples || got.Header.SSRC != 0xCAFEBABE {
		t.Fatalf("round trip mismatch: %+v", got.Header)
	}
	if !bytes.Equal(got.Payload, pkts[0].Payload) {
		t.Fatalf("payload mismatch: got %x", got.Payload)
	}
}

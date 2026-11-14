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

// parseSTAPA 拆开 STAP-A 载荷，返回其中的 NALU 列表。
func parseSTAPA(t *testing.T, payload []byte) [][]byte {
	t.Helper()
	if len(payload) == 0 || payload[0]&0x1F != NALUTypeSTAPA {
		t.Fatalf("not a STAP-A packet: % x", payload)
	}
	rest := payload[1:]
	var nalus [][]byte
	for len(rest) > 0 {
		if len(rest) < 2 {
			t.Fatalf("truncated STAP-A size field: % x", rest)
		}
		n := int(rest[0])<<8 | int(rest[1])
		rest = rest[2:]
		if n > len(rest) {
			t.Fatalf("STAP-A NALU length %d exceeds remaining %d", n, len(rest))
		}
		nalus = append(nalus, rest[:n])
		rest = rest[n:]
	}
	return nalus
}

// TestPackFrameSTAPA 同一帧的 SPS/PPS/小 IDR 能塞进 MTU，聚合成一个 STAP-A。
func TestPackFrameSTAPA(t *testing.T) {
	p := NewH264Packer(1400)
	sps := []byte{0x67, 0x42, 0xC0, 0x1E}
	pps := []byte{0x68, 0xCE, 0x3C, 0x80}
	idr := []byte{0x65, 0x88, 0x84, 0x00}

	pkts, err := p.PackFrame([][]byte{sps, pps, idr})
	if err != nil {
		t.Fatalf("PackFrame: %v", err)
	}
	if len(pkts) != 1 {
		t.Fatalf("want 1 STAP-A packet, got %d", len(pkts))
	}
	pkt := pkts[0]
	if !pkt.Header.Marker {
		t.Fatal("single packet should carry the frame marker")
	}
	if got := pkt.Payload[0] & 0x1F; got != NALUTypeSTAPA {
		t.Fatalf("payload type = %d, want STAP-A(24)", got)
	}
	// SPS/PPS/IDR 的 NRI 都是 3，STAP-A 头应保留最大值。
	if got := pkt.Payload[0] & 0x60; got != 0x60 {
		t.Fatalf("STAP-A NRI = %#x, want 0x60", got)
	}

	got := parseSTAPA(t, pkt.Payload)
	if len(got) != 3 {
		t.Fatalf("STAP-A contains %d NALUs, want 3", len(got))
	}
	for i, want := range [][]byte{sps, pps, idr} {
		if !bytes.Equal(got[i], want) {
			t.Fatalf("NALU %d = % x, want % x", i, got[i], want)
		}
	}
}

// TestPackFrameFallback 参数集先聚合成 STAP-A，大 IDR 退回 FU-A；
// marker 只出现在整帧最后一个分片。
func TestPackFrameFallback(t *testing.T) {
	p := NewH264Packer(20)
	sps := []byte{0x67, 0x42, 0xC0, 0x1E}
	pps := []byte{0x68, 0xCE, 0x3C, 0x80}
	idr := make([]byte, 60)
	idr[0] = 0x65

	pkts, err := p.PackFrame([][]byte{sps, pps, idr})
	if err != nil {
		t.Fatalf("PackFrame: %v", err)
	}
	if len(pkts) < 3 {
		t.Fatalf("want STAP-A + several FU-A packets, got %d", len(pkts))
	}
	if got := pkts[0].Payload[0] & 0x1F; got != NALUTypeSTAPA {
		t.Fatalf("first packet type = %d, want STAP-A(24)", got)
	}
	if params := parseSTAPA(t, pkts[0].Payload); len(params) != 2 ||
		!bytes.Equal(params[0], sps) || !bytes.Equal(params[1], pps) {
		t.Fatalf("STAP-A should carry SPS+PPS: % x", pkts[0].Payload)
	}
	for i, pkt := range pkts[1:] {
		if got := pkt.Payload[0] & 0x1F; got != NALUTypeFUA {
			t.Fatalf("packet %d type = %d, want FU-A(28)", i+1, got)
		}
	}
	for i, pkt := range pkts {
		if want := i == len(pkts)-1; pkt.Header.Marker != want {
			t.Fatalf("packet %d marker = %v, want %v", i, pkt.Header.Marker, want)
		}
	}
}

// TestPackFrameSplitsWhenAggregateExceedsMTU 两个小 NALU 合起来放不下时
// 退回两个单 NALU 包。
func TestPackFrameSplitsWhenAggregateExceedsMTU(t *testing.T) {
	p := NewH264Packer(10)
	a := []byte{0x41, 1, 2, 3, 4, 5}
	b := []byte{0x41, 6, 7, 8, 9, 10}

	pkts, err := p.PackFrame([][]byte{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if len(pkts) != 2 {
		t.Fatalf("want 2 packets, got %d", len(pkts))
	}
	if !bytes.Equal(pkts[0].Payload, a) || !bytes.Equal(pkts[1].Payload, b) {
		t.Fatalf("payloads = % x / % x", pkts[0].Payload, pkts[1].Payload)
	}
	if pkts[0].Header.Marker || !pkts[1].Header.Marker {
		t.Fatalf("markers = %v/%v, want false/true",
			pkts[0].Header.Marker, pkts[1].Header.Marker)
	}
}

// TestPackFrameSTAPABoundary 聚合后正好等于 MTU 时仍然走 STAP-A。
func TestPackFrameSTAPABoundary(t *testing.T) {
	p := NewH264Packer(10)
	a := []byte{0x41, 1}
	b := []byte{0x41, 3, 4}
	total := 1 + (2 + len(a)) + (2 + len(b))
	if total != p.MTU {
		t.Fatalf("test setup: total=%d, mtu=%d", total, p.MTU)
	}
	pkts, err := p.PackFrame([][]byte{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if len(pkts) != 1 || pkts[0].Payload[0]&0x1F != NALUTypeSTAPA {
		t.Fatalf("want one STAP-A packet, got %d", len(pkts))
	}
}

// TestPackFrameSingleNALU 单个 NALU 不应被包成 STAP-A。
func TestPackFrameSingleNALU(t *testing.T) {
	p := NewH264Packer(1400)
	nalu := []byte{0x65, 0x00, 0x01}
	pkts, err := p.PackFrame([][]byte{nalu})
	if err != nil {
		t.Fatal(err)
	}
	if len(pkts) != 1 || !bytes.Equal(pkts[0].Payload, nalu) {
		t.Fatalf("want single NALU packet, got %+v", pkts)
	}
	if !pkts[0].Header.Marker {
		t.Fatal("single-NALU frame should set marker")
	}
}

// TestPackFrameEmpty 空输入（全部 NALU 为空）返回错误。
func TestPackFrameEmpty(t *testing.T) {
	p := NewH264Packer(1400)
	if _, err := p.PackFrame(nil); err == nil {
		t.Fatal("nil frame should error")
	}
	if _, err := p.PackFrame([][]byte{{}, nil}); err == nil {
		t.Fatal("all-empty frame should error")
	}
}

// TestPackFrameSkipsEmptyNALU 空 NALU 被跳过，不影响其余聚合。
func TestPackFrameSkipsEmptyNALU(t *testing.T) {
	p := NewH264Packer(1400)
	sps := []byte{0x67, 0x42, 0xC0, 0x1E}
	pps := []byte{0x68, 0xCE, 0x3C, 0x80}
	pkts, err := p.PackFrame([][]byte{nil, sps, {}, pps})
	if err != nil {
		t.Fatal(err)
	}
	if len(pkts) != 1 {
		t.Fatalf("want 1 STAP-A packet, got %d", len(pkts))
	}
	if got := parseSTAPA(t, pkts[0].Payload); len(got) != 2 {
		t.Fatalf("STAP-A contains %d NALUs, want 2", len(got))
	}
}

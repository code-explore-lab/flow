package rtp

import (
	"bytes"
	"testing"
)

// effectiveMTU 返回 NewH264Packer/Pack 实际使用的 MTU（非正数回退 1400）。
func effectiveMTU(mtu uint16) int {
	if mtu == 0 {
		return 1400
	}
	return int(mtu)
}

// checkPackets 校验封包结果的公共不变量：载荷不超 MTU、版本/PT 正确、
// marker 只在整帧最后一个包上。
func checkPackets(t *testing.T, pkts []*RTPPacket, mtu, inputBytes int) {
	t.Helper()
	if len(pkts) == 0 {
		t.Fatal("no packets produced")
	}
	// 每个包至少带 1 字节载荷，包数不可能超过输入总字节数。
	if inputBytes > 0 && len(pkts) > inputBytes {
		t.Fatalf("%d packets for %d input bytes, looks like a runaway loop", len(pkts), inputBytes)
	}
	for i, pkt := range pkts {
		if len(pkt.Payload) == 0 {
			t.Fatalf("packet %d has empty payload", i)
		}
		if len(pkt.Payload) > mtu {
			t.Fatalf("packet %d payload %d bytes exceeds MTU %d", i, len(pkt.Payload), mtu)
		}
		if pkt.Header.Version != 2 {
			t.Fatalf("packet %d version = %d, want 2", i, pkt.Header.Version)
		}
		if pkt.Header.PT != 96 {
			t.Fatalf("packet %d PT = %d, want 96", i, pkt.Header.PT)
		}
		if want := i == len(pkts)-1; pkt.Header.Marker != want {
			t.Fatalf("packet %d marker = %v, want %v", i, pkt.Header.Marker, want)
		}
	}
}

// reassemblePack 把 Pack 的输出拼回原始 NALU：单包直接返回，
// 多包必须是合法的 FU-A 序列。
func reassemblePack(t *testing.T, pkts []*RTPPacket) []byte {
	t.Helper()
	if len(pkts) == 1 {
		return pkts[0].Payload
	}
	var (
		nalu   []byte
		header byte
	)
	for i, pkt := range pkts {
		pl := pkt.Payload
		if len(pl) < 3 {
			t.Fatalf("fragment %d too short: %d bytes", i, len(pl))
		}
		if pl[0]&0x1F != NALUTypeFUA {
			t.Fatalf("fragment %d type = %d, want FU-A(28)", i, pl[0]&0x1F)
		}
		start := pl[1]&0x80 != 0
		end := pl[1]&0x40 != 0
		if pl[1]&0x20 != 0 {
			t.Fatalf("fragment %d has reserved bit set", i)
		}
		if i == 0 {
			if !start {
				t.Fatalf("first fragment missing S bit")
			}
			header = pl[0]&0xE0 | pl[1]&0x1F
		} else if start {
			t.Fatalf("fragment %d repeats S bit", i)
		}
		if i == len(pkts)-1 {
			if !end {
				t.Fatalf("last fragment missing E bit")
			}
		} else if end {
			t.Fatalf("fragment %d has early E bit", i)
		}
		nalu = append(nalu, pl[2:]...)
	}
	return append([]byte{header}, nalu...)
}

// flattenPackets 把 PackFrame 的输出还原成 NALU 列表：单 NALU 包原样收集，
// STAP-A 按长度字段拆开，FU-A 跨包拼接。输入 NALU 都规范成 type 1，
// 因此独立包不会与 STAP-A/FU-A 混淆。
func flattenPackets(t *testing.T, pkts []*RTPPacket) [][]byte {
	t.Helper()
	var (
		out  [][]byte
		frag []byte
	)
	for i, pkt := range pkts {
		pl := pkt.Payload
		if len(pl) == 0 {
			t.Fatalf("packet %d has empty payload", i)
		}
		switch pl[0] & 0x1F {
		case NALUTypeSTAPA:
			if frag != nil {
				t.Fatalf("STAP-A inside unterminated FU-A")
			}
			rest := pl[1:]
			for len(rest) > 0 {
				if len(rest) < 2 {
					t.Fatalf("packet %d: truncated STAP-A size", i)
				}
				n := int(rest[0])<<8 | int(rest[1])
				rest = rest[2:]
				if n == 0 || n > len(rest) {
					t.Fatalf("packet %d: bad STAP-A NALU length %d (remaining %d)", i, n, len(rest))
				}
				out = append(out, append([]byte(nil), rest[:n]...))
				rest = rest[n:]
			}
		case NALUTypeFUA:
			if len(pl) < 3 {
				t.Fatalf("packet %d: FU-A too short", i)
			}
			fh := pl[1]
			if fh&0x80 != 0 {
				if frag != nil {
					t.Fatalf("packet %d: nested FU-A start", i)
				}
				frag = []byte{pl[0]&0xE0 | fh&0x1F}
			} else if frag == nil {
				t.Fatalf("packet %d: FU-A continuation without start", i)
			}
			frag = append(frag, pl[2:]...)
			if fh&0x40 != 0 {
				out = append(out, frag)
				frag = nil
			}
		default:
			if frag != nil {
				t.Fatalf("packet %d: raw NALU inside unterminated FU-A", i)
			}
			out = append(out, append([]byte(nil), pl...))
		}
	}
	if frag != nil {
		t.Fatalf("unterminated FU-A at end")
	}
	return out
}

// splitFuzzNALUs 按 chunk 切分 fuzz 输入；chunk<=0 表示整段作为一个 NALU。
// 首字节规范成 type 1（保留 F/NRI），避免随机字节命中 STAP-A/FU-A 类型
// 让测试侧的还原逻辑产生歧义。
func splitFuzzNALUs(data []byte, chunk uint16) [][]byte {
	if len(data) == 0 {
		return nil
	}
	size := int(chunk)
	if size <= 0 {
		size = len(data)
	}
	var out [][]byte
	for start := 0; start < len(data); start += size {
		end := min(start+size, len(data))
		nalu := append([]byte(nil), data[start:end]...)
		nalu[0] = nalu[0]&0xE0 | NALUTypeNonIDR
		out = append(out, nalu)
	}
	return out
}

// FuzzPack 单 NALU 封包：任何输入都不能 panic，输出必须能无损还原。
func FuzzPack(f *testing.F) {
	seeds := []struct {
		mtu  uint16
		nalu []byte
	}{
		{1400, nil},
		{1, []byte{0x65}},
		{2, []byte{0x65, 0x01}},
		{3, []byte{0x65, 0x01, 0x02}},
		{5, []byte{0x65, 1, 2, 3, 4, 5, 6}},
		{10, bytes.Repeat([]byte{0x41}, 19)},
		{10, bytes.Repeat([]byte{0x41}, 20)},
		{10, bytes.Repeat([]byte{0x41}, 21)},
		{0, bytes.Repeat([]byte{0x65}, 1401)},
		{65535, []byte{0x67, 0x42, 0xC0, 0x1E}},
	}
	for _, s := range seeds {
		f.Add(s.nalu, s.mtu)
	}

	f.Fuzz(func(t *testing.T, nalu []byte, mtu uint16) {
		p := NewH264Packer(int(mtu))
		pkts, err := p.Pack(nalu)
		if err != nil {
			return // 空 NALU / MTU 太小：合法的拒绝
		}
		checkPackets(t, pkts, effectiveMTU(mtu), len(nalu))
		if got := reassemblePack(t, pkts); !bytes.Equal(got, nalu) {
			t.Fatalf("reassembled % x, want % x", got, nalu)
		}
	})
}

// FuzzPackFrame 整帧封包：聚合/回退后的包序列必须能还原出输入的 NALU 列表。
func FuzzPackFrame(f *testing.F) {
	seeds := []struct {
		data  []byte
		mtu   uint16
		chunk uint16
	}{
		{[]byte{0x67, 0x42, 0xC0, 0x1E, 0x68, 0xCE, 0x3C, 0x80, 0x65, 1, 2, 3}, 1400, 4},
		{bytes.Repeat([]byte{0x65}, 1500), 1400, 0},
		{bytes.Repeat([]byte{0x41}, 30), 10, 3},
		{bytes.Repeat([]byte{0x41}, 8), 5, 1},
		{nil, 1400, 0},
		{[]byte{0x41}, 2, 1},
		{[]byte{0x41, 0x42}, 2, 1},
		{bytes.Repeat([]byte{0x41}, 16), 65535, 8},
	}
	for _, s := range seeds {
		f.Add(s.data, s.mtu, s.chunk)
	}

	f.Fuzz(func(t *testing.T, data []byte, mtu, chunk uint16) {
		nalus := splitFuzzNALUs(data, chunk)
		p := NewH264Packer(int(mtu))
		pkts, err := p.PackFrame(nalus)
		if err != nil {
			return // 空帧 / MTU 太小：合法的拒绝
		}
		checkPackets(t, pkts, effectiveMTU(mtu), len(data))
		got := flattenPackets(t, pkts)
		if len(got) != len(nalus) {
			t.Fatalf("got %d NALUs, want %d", len(got), len(nalus))
		}
		for i := range nalus {
			if !bytes.Equal(got[i], nalus[i]) {
				t.Fatalf("NALU %d = % x, want % x", i, got[i], nalus[i])
			}
		}
	})
}

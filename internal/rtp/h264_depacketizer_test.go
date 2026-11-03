package rtp

import (
	"bytes"
	"errors"
	"strconv"
	"testing"
)

func testPacket(seq uint16, ts, ssrc uint32, payload []byte) *RTPPacket {
	return &RTPPacket{
		Header: RTPHeader{
			Version: 2, PT: 96,
			Seq: seq, Timestamp: ts, SSRC: ssrc,
		},
		Payload: payload,
	}
}

// fuaPayload 拼一个 FU-A 分片：FU indicator + FU header + 数据。
func fuaPayload(indicator, nalType byte, start, end bool, data []byte) []byte {
	out := make([]byte, 0, fuaHeaderLen+len(data))
	out = append(out, indicator)
	fuHeader := nalType & 0x1F
	if start {
		fuHeader |= 0x80
	}
	if end {
		fuHeader |= 0x40
	}
	out = append(out, fuHeader)
	return append(out, data...)
}

func stapAPayload(nalus ...[]byte) []byte {
	out := []byte{NALUTypeSTAPA}
	for _, nalu := range nalus {
		out = append(out, byte(len(nalu)>>8), byte(len(nalu)))
		out = append(out, nalu...)
	}
	return out
}

func TestH264DepacketizeSingleNALU(t *testing.T) {
	// 1~23 都属于单 NALU 包，其中 5=IDR、7=SPS、8=PPS、23 是边界。
	types := []byte{NALUTypeNonIDR, NALUTypeIDR, NALUTypeSPS, NALUTypePPS, 23}
	for _, typ := range types {
		t.Run("type="+strconv.Itoa(int(typ)), func(t *testing.T) {
			payload := []byte{0x60 | typ, 0xAA, 0xBB, 0xCC}
			d := &H264Depacketizer{}
			got, err := d.Depacketize(testPacket(1, 3600, 0x11223344, payload))
			if err != nil {
				t.Fatalf("Depacketize: %v", err)
			}
			if len(got) != 1 || !bytes.Equal(got[0], payload) {
				t.Fatalf("got %x, want [%x]", got, payload)
			}
			if d.Dropped != 0 {
				t.Fatalf("unexpected dropped count: %d", d.Dropped)
			}
		})
	}
}

func TestH264DepacketizeFUA(t *testing.T) {
	const (
		ts   = 90000
		ssrc = 0xCAFEBABE
	)
	// 原始 NALU：header 0x65（NRI=3, type=5）+ 8 字节数据，拆成 3 片。
	naluHeader := byte(0x65)
	wantNALU := append([]byte{naluHeader}, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}...)
	indicator := (naluHeader & 0xE0) | NALUTypeFUA

	d := &H264Depacketizer{}
	steps := []struct {
		seq   uint16
		start bool
		end   bool
		data  []byte
	}{
		{10, true, false, wantNALU[1:4]},
		{11, false, false, wantNALU[4:7]},
		{12, false, true, wantNALU[7:]},
	}

	for i, s := range steps {
		got, err := d.Depacketize(testPacket(s.seq, ts, ssrc, fuaPayload(indicator, naluHeader, s.start, s.end, s.data)))
		if err != nil {
			t.Fatalf("frag %d: %v", i, err)
		}
		if i < len(steps)-1 {
			if len(got) != 0 {
				t.Fatalf("frag %d returned early: %x", i, got)
			}
			continue
		}
		if len(got) != 1 || !bytes.Equal(got[0], wantNALU) {
			t.Fatalf("reassembled = %x, want %x", got, wantNALU)
		}
	}
	if d.Dropped != 0 {
		t.Fatalf("unexpected dropped count: %d", d.Dropped)
	}
}

// TestH264DepacketizeFUASinglePacket 覆盖 S=1 且 E=1 的宽容处理。
func TestH264DepacketizeFUASinglePacket(t *testing.T) {
	naluHeader := byte(0x41)
	indicator := (naluHeader & 0xE0) | NALUTypeFUA
	want := []byte{naluHeader, 0x11, 0x22}

	d := &H264Depacketizer{}
	got, err := d.Depacketize(testPacket(1, 0, 0, fuaPayload(indicator, naluHeader, true, true, want[1:])))
	if err != nil {
		t.Fatalf("Depacketize: %v", err)
	}
	if len(got) != 1 || !bytes.Equal(got[0], want) {
		t.Fatalf("got %x, want %x", got, want)
	}
}

// TestH264DepacketizeFUAMissingFirst 覆盖缺首片：中间的片直接丢，后续完整 NALU 能恢复。
func TestH264DepacketizeFUAMissingFirst(t *testing.T) {
	const (
		ts   = 90000
		ssrc = 0x01020304
	)
	naluHeader := byte(0x65)
	indicator := (naluHeader & 0xE0) | NALUTypeFUA
	d := &H264Depacketizer{}

	// seq 11 是中间片、seq 12 是尾片，但首片 seq 10 丢了。
	for _, seq := range []uint16{11, 12} {
		got, err := d.Depacketize(testPacket(seq, ts, ssrc, fuaPayload(indicator, naluHeader, false, seq == 12, []byte{0xAA})))
		if err != nil {
			t.Fatalf("seq %d: %v", seq, err)
		}
		if len(got) != 0 {
			t.Fatalf("seq %d should be dropped, got %x", seq, got)
		}
	}
	if d.Dropped != 2 {
		t.Fatalf("Dropped = %d, want 2", d.Dropped)
	}

	// 下一个 NALU 完整到达，仍能正常重组。
	want := []byte{naluHeader, 0x01, 0x02}
	d.Dropped = 0
	for i, s := range []struct {
		seq   uint16
		start bool
		end   bool
		data  []byte
	}{
		{20, true, false, want[1:2]},
		{21, false, true, want[2:]},
	} {
		got, err := d.Depacketize(testPacket(s.seq, ts, ssrc, fuaPayload(indicator, naluHeader, s.start, s.end, s.data)))
		if err != nil {
			t.Fatalf("recovery frag %d: %v", i, err)
		}
		if i == 0 && len(got) != 0 {
			t.Fatalf("recovery frag 0 returned early: %x", got)
		}
		if i == 1 && (len(got) != 1 || !bytes.Equal(got[0], want)) {
			t.Fatalf("recovery got %x, want %x", got, want)
		}
	}
}

// TestH264DepacketizeFUAMissingLast 覆盖缺尾片：下一个 FU-A 开始时丢弃半成品。
func TestH264DepacketizeFUAMissingLast(t *testing.T) {
	const (
		ts   = 90000
		ssrc = 0xAAAAAAAA
	)
	naluHeader := byte(0x65)
	indicator := (naluHeader & 0xE0) | NALUTypeFUA
	d := &H264Depacketizer{}

	// 第一个 NALU：start + middle，end 丢了。
	for _, s := range []struct {
		seq   uint16
		start bool
		data  []byte
	}{
		{30, true, []byte{0x01}},
		{31, false, []byte{0x02}},
	} {
		got, err := d.Depacketize(testPacket(s.seq, ts, ssrc, fuaPayload(indicator, naluHeader, s.start, false, s.data)))
		if err != nil {
			t.Fatalf("seq %d: %v", s.seq, err)
		}
		if len(got) != 0 {
			t.Fatalf("seq %d returned early: %x", s.seq, got)
		}
	}

	// 下一个 NALU 的 start 片到来，旧半成品应被丢弃，而不是拼到新 NALU 上。
	want := []byte{naluHeader, 0x10, 0x20}
	got, err := d.Depacketize(testPacket(40, ts, ssrc, fuaPayload(indicator, naluHeader, true, false, want[1:2])))
	if err != nil {
		t.Fatalf("new start: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("new start returned early: %x", got)
	}
	if d.Dropped != 1 {
		t.Fatalf("Dropped = %d, want 1 (old half NALU)", d.Dropped)
	}

	got, err = d.Depacketize(testPacket(41, ts, ssrc, fuaPayload(indicator, naluHeader, false, true, want[2:])))
	if err != nil {
		t.Fatalf("new end: %v", err)
	}
	if len(got) != 1 || !bytes.Equal(got[0], want) {
		t.Fatalf("got %x, want %x", got, want)
	}
}

// TestH264DepacketizeFUAMissingMiddle 覆盖中间缺片（seq 跳变）导致的重组失败与恢复。
func TestH264DepacketizeFUAMissingMiddle(t *testing.T) {
	const (
		ts   = 180000
		ssrc = 0xBBBBBBBB
	)
	naluHeader := byte(0x65)
	indicator := (naluHeader & 0xE0) | NALUTypeFUA
	d := &H264Depacketizer{}

	if got, err := d.Depacketize(testPacket(50, ts, ssrc, fuaPayload(indicator, naluHeader, true, false, []byte{0x01}))); err != nil || len(got) != 0 {
		t.Fatalf("start: got=%x err=%v", got, err)
	}
	// seq 51 丢了，直接收到 seq 52 的尾片。
	got, err := d.Depacketize(testPacket(52, ts, ssrc, fuaPayload(indicator, naluHeader, false, true, []byte{0x03})))
	if err != nil {
		t.Fatalf("end: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("gap should discard, got %x", got)
	}
	if d.Dropped != 1 {
		t.Fatalf("Dropped = %d, want 1", d.Dropped)
	}

	// 恢复：新的完整 NALU。
	want := []byte{naluHeader, 0x0A, 0x0B}
	for i, s := range []struct {
		seq   uint16
		start bool
		end   bool
		data  []byte
	}{
		{60, true, false, want[1:2]},
		{61, false, true, want[2:]},
	} {
		got, err := d.Depacketize(testPacket(s.seq, ts, ssrc, fuaPayload(indicator, naluHeader, s.start, s.end, s.data)))
		if err != nil {
			t.Fatalf("recovery %d: %v", i, err)
		}
		if i == 1 && (len(got) != 1 || !bytes.Equal(got[0], want)) {
			t.Fatalf("recovery got %x, want %x", got, want)
		}
	}
}

// TestH264DepacketizeFUAOutOfOrder 覆盖分片乱序：先到的尾片无法使用，
// 本轮重组作废，但之后的完整 NALU 不受影响。
func TestH264DepacketizeFUAOutOfOrder(t *testing.T) {
	const (
		ts   = 270000
		ssrc = 0xCCCCCCCC
	)
	naluHeader := byte(0x65)
	indicator := (naluHeader & 0xE0) | NALUTypeFUA
	d := &H264Depacketizer{}

	// 尾片 seq 12 先于 start(10)、middle(11) 到达 → 孤立分片，丢弃。
	if got, err := d.Depacketize(testPacket(12, ts, ssrc, fuaPayload(indicator, naluHeader, false, true, []byte{0x03}))); err != nil || len(got) != 0 {
		t.Fatalf("early end: got=%x err=%v", got, err)
	}
	// start 到达后开始重组，但原尾片已经消费掉了，只能等下一个 NALU。
	if got, err := d.Depacketize(testPacket(10, ts, ssrc, fuaPayload(indicator, naluHeader, true, false, []byte{0x01}))); err != nil || len(got) != 0 {
		t.Fatalf("start: got=%x err=%v", got, err)
	}
	if got, err := d.Depacketize(testPacket(11, ts, ssrc, fuaPayload(indicator, naluHeader, false, false, []byte{0x02}))); err != nil || len(got) != 0 {
		t.Fatalf("middle: got=%x err=%v", got, err)
	}
	// 下一轮正常到达，能重组成功，且没有把上一轮的数据混进来。
	want := []byte{naluHeader, 0x77, 0x88}
	got, err := d.Depacketize(testPacket(20, ts+3600, ssrc, fuaPayload(indicator, naluHeader, true, true, want[1:])))
	if err != nil {
		t.Fatalf("new NALU: %v", err)
	}
	if len(got) != 1 || !bytes.Equal(got[0], want) {
		t.Fatalf("got %x, want %x", got, want)
	}
}

// TestH264DepacketizeFUAResetsOnTimestamp 覆盖 timestamp 变化时丢弃半成品。
func TestH264DepacketizeFUAResetsOnTimestamp(t *testing.T) {
	naluHeader := byte(0x65)
	indicator := (naluHeader & 0xE0) | NALUTypeFUA
	d := &H264Depacketizer{}

	if _, err := d.Depacketize(testPacket(1, 1000, 7, fuaPayload(indicator, naluHeader, true, false, []byte{0x01}))); err != nil {
		t.Fatal(err)
	}
	// 同一 seq 连续性，但 timestamp 变了，不能拼在一起。
	if got, err := d.Depacketize(testPacket(2, 2000, 7, fuaPayload(indicator, naluHeader, false, true, []byte{0x02}))); err != nil || len(got) != 0 {
		t.Fatalf("got=%x err=%v", got, err)
	}
	if d.Dropped != 1 {
		t.Fatalf("Dropped = %d, want 1", d.Dropped)
	}
}

// TestH264DepacketizeFUAResetsOnSSRC 覆盖 SSRC 变化时丢弃半成品。
func TestH264DepacketizeFUAResetsOnSSRC(t *testing.T) {
	naluHeader := byte(0x65)
	indicator := (naluHeader & 0xE0) | NALUTypeFUA
	d := &H264Depacketizer{}

	if _, err := d.Depacketize(testPacket(1, 1000, 7, fuaPayload(indicator, naluHeader, true, false, []byte{0x01}))); err != nil {
		t.Fatal(err)
	}
	if got, err := d.Depacketize(testPacket(2, 1000, 8, fuaPayload(indicator, naluHeader, false, true, []byte{0x02}))); err != nil || len(got) != 0 {
		t.Fatalf("got=%x err=%v", got, err)
	}
	if d.Dropped != 1 {
		t.Fatalf("Dropped = %d, want 1", d.Dropped)
	}
}

// TestH264DepacketizeSingleWhileAssembling 覆盖半成品未完成时来了单 NALU。
func TestH264DepacketizeSingleWhileAssembling(t *testing.T) {
	naluHeader := byte(0x65)
	indicator := (naluHeader & 0xE0) | NALUTypeFUA
	d := &H264Depacketizer{}

	if _, err := d.Depacketize(testPacket(1, 1000, 7, fuaPayload(indicator, naluHeader, true, false, []byte{0x01}))); err != nil {
		t.Fatal(err)
	}
	payload := []byte{0x67, 0x42, 0x00, 0x1F}
	got, err := d.Depacketize(testPacket(2, 2000, 7, payload))
	if err != nil {
		t.Fatalf("single NALU: %v", err)
	}
	if len(got) != 1 || !bytes.Equal(got[0], payload) {
		t.Fatalf("got %x, want %x", got, payload)
	}
	if d.Dropped != 1 {
		t.Fatalf("Dropped = %d, want 1", d.Dropped)
	}
}

func TestH264DepacketizeSTAPA(t *testing.T) {
	sps := []byte{0x67, 0x42, 0xC0, 0x1E}
	pps := []byte{0x68, 0xCE, 0x06, 0xE2}
	idr := []byte{0x65, 0x88, 0x84, 0x00, 0x01}
	payload := stapAPayload(sps, pps, idr)

	d := &H264Depacketizer{}
	got, err := d.Depacketize(testPacket(1, 7200, 0xDDDDDDDD, payload))
	if err != nil {
		t.Fatalf("Depacketize: %v", err)
	}
	want := [][]byte{sps, pps, idr}
	if len(got) != len(want) {
		t.Fatalf("got %d NALUs, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("NALU %d = %x, want %x", i, got[i], want[i])
		}
	}
}

func TestH264DepacketizeSTAPAErrors(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "只有头", payload: []byte{NALUTypeSTAPA}},
		{name: "长度字段截断", payload: []byte{NALUTypeSTAPA, 0x00}},
		{name: "长度为 0", payload: []byte{NALUTypeSTAPA, 0x00, 0x00}},
		{name: "长度超过剩余数据", payload: []byte{NALUTypeSTAPA, 0x00, 0x05, 0x67, 0x42}},
		{name: "第二个 NALU 截断", payload: append(stapAPayload([]byte{0x67, 0x42}), 0x00, 0x03, 0x68)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &H264Depacketizer{}
			if _, err := d.Depacketize(testPacket(1, 0, 0, tt.payload)); !errors.Is(err, ErrBadSTAPA) {
				t.Fatalf("err = %v, want ErrBadSTAPA", err)
			}
		})
	}
}

func TestH264DepacketizeErrors(t *testing.T) {
	t.Run("nil packet", func(t *testing.T) {
		d := &H264Depacketizer{}
		if _, err := d.Depacketize(nil); !errors.Is(err, ErrEmptyH264Payload) {
			t.Fatalf("err = %v, want ErrEmptyH264Payload", err)
		}
	})
	t.Run("empty payload", func(t *testing.T) {
		d := &H264Depacketizer{}
		if _, err := d.Depacketize(testPacket(1, 0, 0, nil)); !errors.Is(err, ErrEmptyH264Payload) {
			t.Fatalf("err = %v, want ErrEmptyH264Payload", err)
		}
	})
	t.Run("FU-A 头截断", func(t *testing.T) {
		d := &H264Depacketizer{}
		if _, err := d.Depacketize(testPacket(1, 0, 0, []byte{NALUTypeFUA})); !errors.Is(err, ErrShortFUAHeader) {
			t.Fatalf("err = %v, want ErrShortFUAHeader", err)
		}
	})
	for _, typ := range []byte{0, 25, 29, 31} {
		t.Run("不支持的类型 "+strconv.Itoa(int(typ)), func(t *testing.T) {
			d := &H264Depacketizer{}
			payload := []byte{typ, 0x01}
			if _, err := d.Depacketize(testPacket(1, 0, 0, payload)); !errors.Is(err, ErrUnsupportedNALUType) {
				t.Fatalf("err = %v, want ErrUnsupportedNALUType", err)
			}
		})
	}
}

// TestH264DepacketizePackRoundTrip 用发送侧封包器验证解包结果和原始 NALU 一致。
func TestH264DepacketizePackRoundTrip(t *testing.T) {
	nalu := append([]byte{0x65}, bytes.Repeat([]byte{0xAB}, 3000)...)
	p := NewH264Packer(1200) // 拆成多片 FU-A
	pkts, err := p.Pack(nalu)
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	if len(pkts) < 2 {
		t.Fatalf("expected FU-A fragments, got %d packet(s)", len(pkts))
	}

	d := &H264Depacketizer{}
	var got [][]byte
	for i, pkt := range pkts {
		pkt.Header.Seq = uint16(100 + i)
		pkt.Header.Timestamp = 3600
		pkt.Header.SSRC = 0x12345678
		nalus, err := d.Depacketize(pkt)
		if err != nil {
			t.Fatalf("frag %d: %v", i, err)
		}
		got = append(got, nalus...)
	}
	if len(got) != 1 || !bytes.Equal(got[0], nalu) {
		t.Fatalf("round trip mismatch: got %d NALU(s), equal=%v", len(got), len(got) == 1 && bytes.Equal(got[0], nalu))
	}
}

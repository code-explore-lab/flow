package media

import (
	"bytes"
	"io"
	"testing"
	"testing/iotest"
)

// buildAnnexB 用给定的起始码长度把 NALU 拼成 Annex-B 流。
// threeByte[i] 决定第 i 个 NALU 前用 3 字节还是 4 字节起始码。
func buildAnnexB(nalus [][]byte, threeByte []bool) []byte {
	var out []byte
	for i, n := range nalus {
		if threeByte[i] {
			out = append(out, 0, 0, 1)
		} else {
			out = append(out, 0, 0, 0, 1)
		}
		out = append(out, n...)
	}
	return out
}

func collectStream(t *testing.T, r io.Reader) [][]byte {
	t.Helper()
	d := NewH264StreamDemux(r)
	var out [][]byte
	for i := 0; i < 1000; i++ {
		nalu, err := d.NextNALU()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatalf("NextNALU: %v", err)
		}
		out = append(out, append([]byte(nil), nalu...))
	}
	t.Fatal("too many NALUs (possible infinite loop)")
	return nil
}

func TestH264StreamDemuxMixedStartCodes(t *testing.T) {
	nalus := [][]byte{
		{0x67, 0x42, 0x00, 0x1E},                   // SPS，含 0
		{0x68, 0xCE, 0x3C, 0x80},                   // PPS
		{0x65, 0x88, 0x00, 0x00, 0x03, 0x00, 0x01}, // IDR，含防竞争字节 00 00 03
		{0x41, 0x9A, 0x00, 0x11},                   // 非 IDR
	}
	blob := buildAnnexB(nalus, []bool{false, true, false, true})

	got := collectStream(t, bytes.NewReader(blob))
	if len(got) != len(nalus) {
		t.Fatalf("got %d NALUs, want %d: %v", len(got), len(nalus), got)
	}
	for i := range nalus {
		if !bytes.Equal(got[i], nalus[i]) {
			t.Fatalf("NALU %d = % X, want % X", i, got[i], nalus[i])
		}
	}
}

func TestH264StreamDemuxLeadingJunkAndOneByteReads(t *testing.T) {
	nalus := [][]byte{{0x67, 0x42, 0xC0, 0x1E}, {0x65, 0x88, 0x84}}
	blob := append([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00}, buildAnnexB(nalus, []bool{false, true})...)

	// OneByteReader 强制每次只返回 1 字节，验证跨 read 的起始码扫描
	got := collectStream(t, iotest.OneByteReader(bytes.NewReader(blob)))
	if len(got) != len(nalus) {
		t.Fatalf("got %v, want %v", got, nalus)
	}
	for i := range nalus {
		if !bytes.Equal(got[i], nalus[i]) {
			t.Fatalf("NALU %d = % X, want % X", i, got[i], nalus[i])
		}
	}
}

func TestH264StreamDemuxLastNALUWithoutStartCode(t *testing.T) {
	blob := append([]byte{0, 0, 0, 1}, []byte{0x65, 0x88, 0x84}...)

	got := collectStream(t, bytes.NewReader(blob))
	if len(got) != 1 || !bytes.Equal(got[0], []byte{0x65, 0x88, 0x84}) {
		t.Fatalf("got %v, want one trailing NALU", got)
	}
}

func TestH264StreamDemuxAdjacentStartCodesYieldEmptyNALU(t *testing.T) {
	blob := []byte{0, 0, 1, 0, 0, 1, 0x65, 0x88}
	got := collectStream(t, bytes.NewReader(blob))
	if len(got) != 2 {
		t.Fatalf("got %v, want empty NALU + data NALU", got)
	}
	if len(got[0]) != 0 {
		t.Fatalf("first NALU = % X, want empty", got[0])
	}
	if !bytes.Equal(got[1], []byte{0x65, 0x88}) {
		t.Fatalf("second NALU = % X", got[1])
	}
}

func TestH264StreamDemuxEOFWithoutStartCode(t *testing.T) {
	d := NewH264StreamDemux(bytes.NewReader([]byte{0xDE, 0xAD, 0xBE, 0xEF}))
	if _, err := d.NextNALU(); err != io.EOF {
		t.Fatalf("err = %v, want io.EOF", err)
	}
}

func TestH264StreamDemuxEmptyStream(t *testing.T) {
	d := NewH264StreamDemux(bytes.NewReader(nil))
	if _, err := d.NextNALU(); err != io.EOF {
		t.Fatalf("err = %v, want io.EOF", err)
	}
}

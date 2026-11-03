package rtp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

// headerBytes 手工拼 12 字节固定头（不走 Marshal，避免用被测代码造测试数据）。
func headerBytes(h RTPHeader) []byte {
	b := make([]byte, rtpFixedHeaderLen)
	b0 := (h.Version&0x03)<<6 | h.CC&0x0F
	if h.Padding {
		b0 |= 1 << 5
	}
	if h.Extension {
		b0 |= 1 << 4
	}
	b[0] = b0
	b[1] = h.PT & 0x7F
	if h.Marker {
		b[1] |= 1 << 7
	}
	binary.BigEndian.PutUint16(b[2:4], h.Seq)
	binary.BigEndian.PutUint32(b[4:8], h.Timestamp)
	binary.BigEndian.PutUint32(b[8:12], h.SSRC)
	return b
}

func csrcBytes(csrc ...uint32) []byte {
	b := make([]byte, 4*len(csrc))
	for i, c := range csrc {
		binary.BigEndian.PutUint32(b[4*i:], c)
	}
	return b
}

// extBytes 拼扩展头：profile(2) + length(2，单位 32 位字) + 数据。
func extBytes(profile uint16, data []byte) []byte {
	b := make([]byte, rtpExtensionHeaderLen+len(data))
	binary.BigEndian.PutUint16(b[0:2], profile)
	binary.BigEndian.PutUint16(b[2:4], uint16(len(data)/4))
	copy(b[4:], data)
	return b
}

func TestParse(t *testing.T) {
	payload := []byte{0x65, 0x88, 0x84, 0x00}

	tests := []struct {
		name    string
		raw     []byte
		want    RTPHeader
		payload []byte
	}{
		{
			name: "普通包",
			raw: append(headerBytes(RTPHeader{
				Version: 2, Marker: true, PT: 96,
				Seq: 0x1234, Timestamp: 0xDEADBEEF, SSRC: 0xCAFEBABE,
			}), payload...),
			want: RTPHeader{
				Version: 2, Marker: true, PT: 96,
				Seq: 0x1234, Timestamp: 0xDEADBEEF, SSRC: 0xCAFEBABE,
			},
			payload: payload,
		},
		{
			name: "带 CSRC",
			raw: func() []byte {
				h := headerBytes(RTPHeader{
					Version: 2, CC: 2, PT: 97,
					Seq: 7, Timestamp: 9000, SSRC: 0x11223344,
				})
				return append(append(h, csrcBytes(0xAAAA0001, 0xBBBB0002)...), payload...)
			}(),
			want: RTPHeader{
				Version: 2, CC: 2, PT: 97,
				Seq: 7, Timestamp: 9000, SSRC: 0x11223344,
				CSRC: []uint32{0xAAAA0001, 0xBBBB0002},
			},
			payload: payload,
		},
		{
			name: "带扩展头",
			raw: func() []byte {
				h := headerBytes(RTPHeader{
					Version: 2, Extension: true, PT: 96,
					Seq: 8, Timestamp: 12345, SSRC: 0x55667788,
				})
				ext := []byte{1, 0, 0, 0, 2, 0, 0, 0} // 两字扩展数据
				return append(append(h, extBytes(0xBEDE, ext)...), payload...)
			}(),
			want: RTPHeader{
				Version: 2, Extension: true, PT: 96,
				Seq: 8, Timestamp: 12345, SSRC: 0x55667788,
				ExtensionProfile: 0xBEDE,
				ExtensionData:    []byte{1, 0, 0, 0, 2, 0, 0, 0},
			},
			payload: payload,
		},
		{
			name: "带 padding",
			raw: func() []byte {
				h := headerBytes(RTPHeader{
					Version: 2, Padding: true, PT: 96,
					Seq: 9, Timestamp: 18000, SSRC: 0x99AABBCC,
				})
				// 3 字节 padding，最后一个字节是总长 3
				return append(append(h, payload...), 0x00, 0x00, 0x03)
			}(),
			want: RTPHeader{
				Version: 2, Padding: true, PT: 96,
				Seq: 9, Timestamp: 18000, SSRC: 0x99AABBCC,
			},
			payload: payload,
		},
		{
			name: "CSRC + 扩展 + padding 组合",
			raw: func() []byte {
				h := headerBytes(RTPHeader{
					Version: 2, CC: 1, Extension: true, Padding: true, Marker: true, PT: 98,
					Seq: 65535, Timestamp: 0xFFFFFFFF, SSRC: 0x01020304,
				})
				out := append(h, csrcBytes(0xFEEDFACE)...)
				out = append(out, extBytes(0x1000, []byte{0xDE, 0xAD, 0xBE, 0xEF})...)
				out = append(out, payload...)
				out = append(out, 0x01) // 1 字节 padding，值 1
				return out
			}(),
			want: RTPHeader{
				Version: 2, CC: 1, Extension: true, Padding: true, Marker: true, PT: 98,
				Seq: 65535, Timestamp: 0xFFFFFFFF, SSRC: 0x01020304,
				CSRC:             []uint32{0xFEEDFACE},
				ExtensionProfile: 0x1000,
				ExtensionData:    []byte{0xDE, 0xAD, 0xBE, 0xEF},
			},
			payload: payload,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.raw)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if !reflect.DeepEqual(got.Header, tt.want) {
				t.Fatalf("header mismatch:\n got %+v\nwant %+v", got.Header, tt.want)
			}
			if !bytes.Equal(got.Payload, tt.payload) {
				t.Fatalf("payload = %x, want %x", got.Payload, tt.payload)
			}
		})
	}
}

func TestParseErrors(t *testing.T) {
	valid := headerBytes(RTPHeader{Version: 2, PT: 96})

	tests := []struct {
		name string
		raw  []byte
		want error
	}{
		{
			name: "少于 12 字节",
			raw:  make([]byte, 11),
			want: ErrPacketTooShort,
		},
		{
			name: "版本不是 2",
			raw: func() []byte {
				b := append([]byte(nil), valid...)
				b[0] = (b[0] & 0x3F) | (1 << 6) // V=1
				return b
			}(),
			want: ErrBadVersion,
		},
		{
			name: "CSRC 截断",
			raw: func() []byte {
				b := append([]byte(nil), valid...)
				b[0] |= 3 // CC=3，需要 12 字节但只给 4 字节
				return append(b, 0, 0, 0, 1)
			}(),
			want: ErrCSRCTruncated,
		},
		{
			name: "扩展头截断（只有 2 字节）",
			raw: func() []byte {
				b := append([]byte(nil), valid...)
				b[0] |= 1 << 4 // X=1
				return append(b, 0xBE, 0xDE)
			}(),
			want: ErrExtensionTrunc,
		},
		{
			name: "扩展数据截断",
			raw: func() []byte {
				b := append([]byte(nil), valid...)
				b[0] |= 1 << 4                        // X=1
				b = append(b, 0xBE, 0xDE, 0x00, 0x02) // 声明 2 个 32 位字
				return append(b, 0, 0, 0, 0)          // 只给 1 个字
			}(),
			want: ErrExtensionTrunc,
		},
		{
			name: "padding 长度为 0",
			raw: func() []byte {
				b := append([]byte(nil), valid...)
				b[0] |= 1 << 5 // P=1
				return append(b, 0xAA, 0x00)
			}(),
			want: ErrBadPadding,
		},
		{
			name: "padding 长度超过 payload",
			raw: func() []byte {
				b := append([]byte(nil), valid...)
				b[0] |= 1 << 5 // P=1
				return append(b, 0xAA, 0x05)
			}(),
			want: ErrBadPadding,
		},
		{
			name: "只有 padding 标志没有数据",
			raw: func() []byte {
				b := append([]byte(nil), valid...)
				b[0] |= 1 << 5 // P=1
				return b
			}(),
			want: ErrBadPadding,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Parse(tt.raw); !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
		})
	}
}

// TestDepacketizerTracksLastPacket 验证状态只在解析成功后更新。
func TestDepacketizerTracksLastPacket(t *testing.T) {
	d := &Depacketizer{}

	raw := append(headerBytes(RTPHeader{
		Version: 2, Marker: true, PT: 96,
		Seq: 101, Timestamp: 7200, SSRC: 0x0BADF00D,
	}), 0x65)

	if _, err := d.Parse(raw); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if d.Seq != 101 || d.Timestamp != 7200 || d.SSRC != 0x0BADF00D || !d.Marker || d.Packets != 1 {
		t.Fatalf("state = {seq:%d ts:%d ssrc:%08x marker:%v packets:%d}", d.Seq, d.Timestamp, d.SSRC, d.Marker, d.Packets)
	}

	// 解析失败不改状态，也不计数。
	if _, err := d.Parse([]byte{0x80}); err == nil {
		t.Fatal("expected error for short packet")
	}
	if d.Seq != 101 || d.Packets != 1 {
		t.Fatalf("state changed after failed parse: seq=%d packets=%d", d.Seq, d.Packets)
	}
}

// TestMarshalParseRoundTripWithExtensionAndPadding 验证写出去再读回来一致。
func TestMarshalParseRoundTripWithExtensionAndPadding(t *testing.T) {
	h := RTPHeader{
		Version: 2, Extension: true, Padding: true, Marker: true, PT: 96,
		Seq: 1, Timestamp: 3600, SSRC: 0xDEADBEEF,
		ExtensionProfile: 0xBEDE,
		ExtensionData:    []byte{1, 2, 3, 4, 5}, // 5 字节，需要补 3 字节对齐
	}
	raw, err := h.Marshal([]byte{0x67, 0x42})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !got.Header.Extension || !got.Header.Padding || got.Header.ExtensionProfile != 0xBEDE {
		t.Fatalf("flags/profile lost: %+v", got.Header)
	}
	// 扩展数据按 32 位字对齐，解析回来会带上 3 字节 0 填充。
	wantExt := []byte{1, 2, 3, 4, 5, 0, 0, 0}
	if !bytes.Equal(got.Header.ExtensionData, wantExt) {
		t.Fatalf("extension data = %x, want %x", got.Header.ExtensionData, wantExt)
	}
	if !bytes.Equal(got.Payload, []byte{0x67, 0x42}) {
		t.Fatalf("payload = %x", got.Payload)
	}
}

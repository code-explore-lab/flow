package rtp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func testAACPacket(payload []byte) *RTPPacket {
	return &RTPPacket{
		Header:  RTPHeader{Version: 2, PT: AACDefaultPayloadType, Seq: 1, Timestamp: AACFrameSamples},
		Payload: payload,
	}
}

// testBitWriter 按 MSB-first 拼 AU headers，避免测试依赖被测的 bitReader。
type testBitWriter struct {
	bits []byte
}

func (w *testBitWriter) write(v uint64, n int) {
	for i := n - 1; i >= 0; i-- {
		w.bits = append(w.bits, byte(v>>uint(i))&1)
	}
}

func (w *testBitWriter) bytes() []byte {
	out := make([]byte, (len(w.bits)+7)/8)
	for i, b := range w.bits {
		if b != 0 {
			out[i/8] |= 1 << uint(7-i%8)
		}
	}
	return out
}

// buildAACPayload 按配置拼出 [AU-headers-length][AU headers][AU data]。
// 第一个 AU 用 firstIndex 作为绝对 index，后续每个 AU 的 delta 由 deltas 给出。
func buildAACPayload(cfg AACConfig, firstIndex int, deltas []int, aus ...[]byte) []byte {
	w := &testBitWriter{}
	for i, au := range aus {
		w.write(uint64(len(au)), cfg.SizeLength)
		if i == 0 {
			w.write(uint64(firstIndex), cfg.IndexLength)
		} else {
			w.write(uint64(deltas[i-1]), cfg.IndexDeltaLength)
		}
	}
	headerBits := len(w.bits)
	header := w.bytes()

	out := make([]byte, 2+len(header))
	binary.BigEndian.PutUint16(out[:2], uint16(headerBits))
	copy(out[2:], header)
	for _, au := range aus {
		out = append(out, au...)
	}
	return out
}

func TestAACDepacketizeOneAU(t *testing.T) {
	d, err := NewAACDepacketizer(DefaultAACConfig())
	if err != nil {
		t.Fatal(err)
	}

	au := []byte{0x21, 0x10, 0x05, 0x03, 0xFE}
	got, err := d.Depacketize(testAACPacket(buildAACPayload(DefaultAACConfig(), 0, nil, au)))
	if err != nil {
		t.Fatalf("Depacketize: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d AU(s), want 1", len(got))
	}
	if !bytes.Equal(got[0].Data, au) || got[0].Size != len(au) || got[0].Index != 0 {
		t.Fatalf("AU = {data:%x size:%d index:%d}, want {data:%x size:%d index:0}",
			got[0].Data, got[0].Size, got[0].Index, au, len(au))
	}
}

// TestAACDepacketizeTwoAUsManualLayout 手写一个符合 RFC 3640 的两 AU payload，
// 不经过 buildAACPayload，确保解析逻辑对真实位布局正确。
func TestAACDepacketizeTwoAUsManualLayout(t *testing.T) {
	au1 := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	au2 := []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17}

	payload := []byte{
		0x00, 0x20, // AU-headers-length = 32 bit
		0x00, 0x28, // AU-size=5(13 bit), AU-Index=0(3 bit)      → 5<<3 | 0
		0x00, 0x3A, // AU-size=7(13 bit), AU-Index-delta=2(3 bit) → 7<<3 | 2
	}
	payload = append(payload, au1...)
	payload = append(payload, au2...)

	d, err := NewAACDepacketizer(DefaultAACConfig())
	if err != nil {
		t.Fatal(err)
	}
	got, err := d.Depacketize(testAACPacket(payload))
	if err != nil {
		t.Fatalf("Depacketize: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d AU(s), want 2", len(got))
	}
	if !bytes.Equal(got[0].Data, au1) || got[0].Size != 5 || got[0].Index != 0 {
		t.Fatalf("AU 0 = {data:%x size:%d index:%d}", got[0].Data, got[0].Size, got[0].Index)
	}
	// 第二个 index = 上一个 index + delta = 0 + 2。
	if !bytes.Equal(got[1].Data, au2) || got[1].Size != 7 || got[1].Index != 2 {
		t.Fatalf("AU 1 = {data:%x size:%d index:%d}", got[1].Data, got[1].Size, got[1].Index)
	}
}

func TestAACDepacketizeTwoAUsWithIndexDelta(t *testing.T) {
	cfg := DefaultAACConfig()
	au1 := []byte{0xAA, 0xBB}
	au2 := []byte{0xCC, 0xDD, 0xEE}
	payload := buildAACPayload(cfg, 1, []int{2}, au1, au2)

	d, err := NewAACDepacketizer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	got, err := d.Depacketize(testAACPacket(payload))
	if err != nil {
		t.Fatalf("Depacketize: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d AU(s), want 2", len(got))
	}
	if got[0].Index != 1 || got[1].Index != 3 {
		t.Fatalf("indexes = [%d %d], want [1 3]", got[0].Index, got[1].Index)
	}
}

// TestAACDepacketizeCustomWidths 覆盖从 fmtp 读到自定义位宽的场景
// （6+2=8 bit/header，正好字节对齐）。
func TestAACDepacketizeCustomWidths(t *testing.T) {
	cfg, err := ParseAACFmtp("mode=AAC-hbr; SizeLength=6; IndexLength=2; IndexDeltaLength=2")
	if err != nil {
		t.Fatalf("ParseAACFmtp: %v", err)
	}
	if cfg != (AACConfig{SizeLength: 6, IndexLength: 2, IndexDeltaLength: 2}) {
		t.Fatalf("config = %+v", cfg)
	}

	au1 := []byte{0x01}
	au2 := []byte{0x02, 0x03}
	payload := buildAACPayload(cfg, 1, []int{3}, au1, au2)

	d, err := NewAACDepacketizer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	got, err := d.Depacketize(testAACPacket(payload))
	if err != nil {
		t.Fatalf("Depacketize: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d AU(s), want 2", len(got))
	}
	if !bytes.Equal(got[0].Data, au1) || got[0].Index != 1 {
		t.Fatalf("AU 0 = {data:%x index:%d}", got[0].Data, got[0].Index)
	}
	// delta 字段只有 2 bit，值 3 仍能表达；index = 1 + 3 = 4。
	if !bytes.Equal(got[1].Data, au2) || got[1].Index != 4 {
		t.Fatalf("AU 1 = {data:%x index:%d}", got[1].Data, got[1].Index)
	}
}

func TestAACDepacketizePackRoundTrip(t *testing.T) {
	au := []byte{0x21, 0x10, 0x05, 0x03, 0xFE}
	pkts, err := NewAACPacker().Pack(au)
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}

	d, err := NewAACDepacketizer(DefaultAACConfig())
	if err != nil {
		t.Fatal(err)
	}
	got, err := d.Depacketize(pkts[0])
	if err != nil {
		t.Fatalf("Depacketize: %v", err)
	}
	if len(got) != 1 || !bytes.Equal(got[0].Data, au) || got[0].Size != len(au) {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}

func TestParseAACFmtp(t *testing.T) {
	tests := []struct {
		name    string
		fmtp    string
		want    AACConfig
		wantErr bool
	}{
		{
			name: "完整 fmtp",
			fmtp: "streamtype=5; profile-level-id=1; mode=AAC-hbr; SizeLength=13; IndexLength=3; IndexDeltaLength=3; config=1210",
			want: DefaultAACConfig(),
		},
		{
			name: "整行 SDP",
			fmtp: "a=fmtp:97 SizeLength=13; IndexLength=3; IndexDeltaLength=3",
			want: DefaultAACConfig(),
		},
		{
			name: "大小写不敏感",
			fmtp: "sizelength=6; indexlength=2; indexdeltalength=2",
			want: AACConfig{SizeLength: 6, IndexLength: 2, IndexDeltaLength: 2},
		},
		{
			name: "未出现的参数用默认值",
			fmtp: "mode=AAC-hbr",
			want: DefaultAACConfig(),
		},
		{
			name: "空字符串",
			fmtp: "",
			want: DefaultAACConfig(),
		},
		{name: "SizeLength 为 0", fmtp: "SizeLength=0", wantErr: true},
		{name: "SizeLength 非数字", fmtp: "SizeLength=abc", wantErr: true},
		{name: "SizeLength 超过 32", fmtp: "SizeLength=33", wantErr: true},
		{name: "IndexLength 为负", fmtp: "IndexLength=-1", wantErr: true},
		{name: "IndexDeltaLength 超过 32", fmtp: "IndexDeltaLength=33", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseAACFmtp(tt.fmtp)
			if tt.wantErr {
				if !errors.Is(err, ErrAACInvalidConfig) {
					t.Fatalf("err = %v, want ErrAACInvalidConfig", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseAACFmtp: %v", err)
			}
			if got != tt.want {
				t.Fatalf("config = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestNewAACDepacketizerInvalidConfig(t *testing.T) {
	tests := []AACConfig{
		{SizeLength: 0, IndexLength: 3, IndexDeltaLength: 3},
		{SizeLength: 33, IndexLength: 3, IndexDeltaLength: 3},
		{SizeLength: 13, IndexLength: -1, IndexDeltaLength: 3},
		{SizeLength: 13, IndexLength: 3, IndexDeltaLength: 33},
	}
	for _, cfg := range tests {
		if _, err := NewAACDepacketizer(cfg); !errors.Is(err, ErrAACInvalidConfig) {
			t.Fatalf("cfg %+v: err = %v, want ErrAACInvalidConfig", cfg, err)
		}
	}
}

func TestAACDepacketizeErrors(t *testing.T) {
	d, err := NewAACDepacketizer(DefaultAACConfig())
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		payload []byte
		want    error
	}{
		{name: "nil packet", payload: nil, want: ErrEmptyAACPayload},
		{name: "少于 2 字节", payload: []byte{0x00}, want: ErrAACHeadersTruncated},
		{name: "AU-headers-length 为 0", payload: []byte{0x00, 0x00, 0xAA}, want: ErrAACHeadersTruncated},
		{name: "头部长度不是整字节", payload: []byte{0x00, 0x0F, 0xAA, 0xBB}, want: ErrAACHeadersTruncated},
		{name: "头部超过 payload", payload: []byte{0x00, 0x10, 0x00}, want: ErrAACHeadersTruncated},
		// 头部只给 8 bit，但默认 SizeLength=13，字段读不全。
		{name: "AU header 字段溢出", payload: []byte{0x00, 0x08, 0x00, 0xAA}, want: ErrAACBadAUHeader},
		// AU-size 声明 5 字节，但 AU data 只有 1 字节。
		{name: "AU size 超过数据", payload: []byte{0x00, 0x10, 0x00, 0x28, 0xAA}, want: ErrAACSizeMismatch},
		// 一个 1 字节 AU，但 AU data 多了 1 字节。
		{name: "有尾随数据", payload: []byte{0x00, 0x10, 0x00, 0x08, 0xAA, 0xBB}, want: ErrAACSizeMismatch},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var pkt *RTPPacket
			if tt.payload != nil {
				pkt = testAACPacket(tt.payload)
			}
			if _, err := d.Depacketize(pkt); !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
		})
	}
}

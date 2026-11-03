package rtp

import (
	"encoding/binary"
	"errors"
)

// RTPPacket 基础RTP包结构
type RTPPacket struct {
	Header  RTPHeader
	Payload []byte
}

// RTPHeader 固定12字节头 + CSRC列表
type RTPHeader struct {
	Version   uint8 // 固定 2
	Padding   bool
	Extension bool
	CC        uint8 // CSRC count 0~15
	Marker    bool
	PT        uint8 // Payload Type
	Seq       uint16
	Timestamp uint32
	SSRC      uint32
	CSRC      []uint32 // 长度等于 CC

	// 扩展头（X=1 时有效），解析时填充，长度按 32 位字对齐。
	ExtensionProfile uint16
	ExtensionData    []byte
}

// Marshal 把 RTPHeader + payload 序列化为 UDP 报文
// 返回：完整RTP包（header + payload）
func (h *RTPHeader) Marshal(payload []byte) ([]byte, error) {
	if h.Version != 2 {
		return nil, errors.New("invalid rtp version, must be 2")
	}
	//cc 4bit
	if h.CC > 15 {
		return nil, errors.New("cc cannot exceed 15")
	}
	if len(h.CSRC) != int(h.CC) {
		return nil, errors.New("csrc slice length mismatch cc")
	}

	// 基础头 12字节 + csrc长度 + 扩展头（4 字节 + 按 32 位字对齐的数据）
	headerLen := 12 + 4*int(h.CC)
	if h.Extension {
		headerLen += 4 + ((len(h.ExtensionData) + 3) / 4 * 4)
	}
	padLen := 0
	if h.Padding {
		padLen = 1 // 写一个填充字节，值 1 表示 padding 总长（含自身）
	}
	out := make([]byte, headerLen+len(payload)+padLen)

	// byte 0: V(2) P(1) X(1) CC(4)
	var b0 byte
	b0 |= (h.Version & 0x03) << 6 // bit6-7: 版本
	if h.Padding {
		b0 |= 1 << 5 // bit5: P
	}
	if h.Extension {
		b0 |= 1 << 4 // bit4: X
	}
	b0 |= h.CC & 0x0F // bit0-3: CC
	out[0] = b0

	// byte 1: M(1) PT(7)
	var b1 byte
	if h.Marker {
		b1 |= 1 << 7 // bit7: M
	}
	b1 |= h.PT & 0x7F // bit0-6: PT（7 bit）
	out[1] = b1

	// Seq (uint16 big-endian)
	binary.BigEndian.PutUint16(out[2:4], h.Seq)
	// Timestamp
	binary.BigEndian.PutUint32(out[4:8], h.Timestamp)
	// SSRC
	binary.BigEndian.PutUint32(out[8:12], h.SSRC)

	// CSRC
	pos := 12
	for _, csrc := range h.CSRC {
		binary.BigEndian.PutUint32(out[pos:pos+4], csrc)
		pos += 4
	}

	// 扩展头：profile(2) + length(2，单位 32 位字) + 数据（补 0 对齐）
	if h.Extension {
		binary.BigEndian.PutUint16(out[pos:pos+2], h.ExtensionProfile)
		binary.BigEndian.PutUint16(out[pos+2:pos+4], uint16(len(h.ExtensionData)/4+boolToInt(len(h.ExtensionData)%4 != 0)))
		pos += 4
		copy(out[pos:], h.ExtensionData) // out 已清零，剩余字节天然是 0 填充
		pos += (len(h.ExtensionData) + 3) / 4 * 4
	}

	// copy payload
	copy(out[pos:], payload)
	pos += len(payload)

	// padding：最后 1 字节是填充长度（含自身）
	if h.Padding {
		out[pos] = 1
	}
	return out, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// Unmarshal 从一个完整RTP报文（udp包）解析成 RTPPacket。
// 具体解析逻辑见 depacketizer.go 的 Parse（CSRC、扩展头、padding 都支持）。
func Unmarshal(pkt []byte) (*RTPPacket, error) {
	return Parse(pkt)
}

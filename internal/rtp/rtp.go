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
}

// Marshal 把 RTPHeader + payload 序列化为 UDP 报文
// 返回：完整RTP包（header + payload）
func (h *RTPHeader) Marshal(payload []byte) ([]byte, error) {
	if h.Version != 2 {
		return nil, errors.New("invalid rtp version, must be 2")
	}
	if h.CC > 15 {
		return nil, errors.New("cc cannot exceed 15")
	}
	if len(h.CSRC) != int(h.CC) {
		return nil, errors.New("csrc slice length mismatch cc")
	}

	// 基础头 12字节 + csrc长度
	headerLen := 12 + 4*int(h.CC)
	out := make([]byte, headerLen+len(payload))

	// byte 0: V(2) P(1) X(1) CC(4)
	var b0 byte
	b0 |= (h.Version & 0x03) << 6
	if h.Padding {
		b0 |= 1 << 5
	}
	if h.Extension {
		b0 |= 1 << 4
	}
	b0 |= h.CC & 0x0F
	out[0] = b0

	// byte 1: M(1) PT(7)
	var b1 byte
	if h.Marker {
		b1 |= 1 << 7
	}
	b1 |= h.PT & 0x7F
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

	// copy payload
	copy(out[pos:], payload)
	return out, nil
}

// Unmarshal 从一个完整RTP报文（udp包）解析成 RTPPacket
func Unmarshal(pkt []byte) (*RTPPacket, error) {
	if len(pkt) < 12 {
		return nil, errors.New("rtp packet too short, min 12 bytes header")
	}

	h := &RTPHeader{}
	b0 := pkt[0]
	h.Version = (b0 >> 6) & 0x03
	if h.Version != 2 {
		return nil, errors.New("invalid rtp version")
	}
	h.Padding = (b0 & (1 << 5)) != 0
	h.Extension = (b0 & (1 << 4)) != 0
	cc := b0 & 0x0F

	b1 := pkt[1]
	h.Marker = (b1 & (1 << 7)) != 0
	h.PT = b1 & 0x7F

	h.Seq = binary.BigEndian.Uint16(pkt[2:4])
	h.Timestamp = binary.BigEndian.Uint32(pkt[4:8])
	h.SSRC = binary.BigEndian.Uint32(pkt[8:12])

	// 读取 CSRC
	csrcLen := int(cc)
	headerEnd := 12 + csrcLen*4
	if len(pkt) < headerEnd {
		return nil, errors.New("packet truncated, csrc not complete")
	}
	h.CC = cc
	h.CSRC = make([]uint32, csrcLen)
	for i := 0; i < csrcLen; i++ {
		off := 12 + i*4
		h.CSRC[i] = binary.BigEndian.Uint32(pkt[off : off+4])
	}

	// 扩展头这里暂时不解析（X位我们demo不用）
	if h.Extension {
		return nil, errors.New("extension header not supported in this demo")
	}

	// 【注意】Padding 标志为true时，payload最后1字节是填充长度，需要截断
	// 你后面做播放器/推流一定要加这段，现在先注释
	// payload := pkt[headerEnd:]
	// if h.Padding && len(payload) > 0 {
	// 	padLen := int(payload[len(payload)-1])
	// 	if padLen > len(payload) {
	// 		return nil, errors.New("invalid padding length")
	// 	}
	// 	payload = payload[:len(payload)-padLen]
	// }
	payload := pkt[headerEnd:]

	return &RTPPacket{
		Header:  *h,
		Payload: payload,
	}, nil
}

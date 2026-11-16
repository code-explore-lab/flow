package rtcp

import "encoding/binary"

// ByeType 是 BYE 的 packet type（RFC 3550 §6.5）。
const ByeType = 203

// BYE 构造一个 BYE 包，携带一组 SSRC/CSRC。
//
//	0                   1                   2                   3
//	0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|V=2|P|    SC   |   PT=BYE=203  |             length            |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                           SSRC/CSRC                           |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//
// 这里不带 reason 字段（RFC 3550 §6.6 中该字段可选）。BYE 仍须放在
// 以 SR/RR 开头的复合包里发送（§6.1），拼接交给 Compound。
func BYE(ssrcs ...uint32) []byte {
	b := make([]byte, 4+4*len(ssrcs))
	b[0] = 0x80 | byte(len(ssrcs)) // V=2, P=0, SC=len(ssrcs)
	b[1] = ByeType
	binary.BigEndian.PutUint16(b[2:4], uint16(len(ssrcs)))
	for i, ssrc := range ssrcs {
		binary.BigEndian.PutUint32(b[4+4*i:], ssrc)
	}
	return b
}

// ParseBYE 解析 BYE 里的 SSRC 列表，供测试和抓包排查使用。
func ParseBYE(b []byte) (ssrcs []uint32, ok bool) {
	if len(b) < 8 || b[0]>>6 != 2 || b[1] != ByeType {
		return nil, false
	}
	sc := int(b[0] & 0x1F)
	n := (int(binary.BigEndian.Uint16(b[2:4])) + 1) * 4
	if n < 4+4*sc || n > len(b) {
		return nil, false
	}
	ssrcs = make([]uint32, sc)
	for i := range ssrcs {
		ssrcs[i] = binary.BigEndian.Uint32(b[4+4*i:])
	}
	return ssrcs, true
}

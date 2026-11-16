package rtcp

import "encoding/binary"

const (
	// SDESType 是 SDES 的 packet type（RFC 3550 §6.5）。
	SDESType = 202

	// CNAMEItem 是 SDES item 类型 CNAME（RFC 3550 §6.5）。
	// CNAME 标识一个 RTP 源，跨所有媒体轨保持稳定；SSRC 才标识单条流。
	CNAMEItem = 1

	// maxCNAMELen 是 SDES item 长度字段的上限（1 字节）。
	maxCNAMELen = 255
)

// CNAME 构造一个只含一个 chunk、一个 CNAME item 的 SDES 包（SC=1）。
//
//	0                   1                   2                   3
//	0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|V=2|P|    SC   |  PT=SDES=202  |             length            |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                          SSRC/CSRC                            |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|   CNAME=1     |     length    |        text ...               |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|  END=0 + null padding，补齐到 32 位边界                        |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//
// END item 只有一个类型字节，没有长度字节；length 字段按 32 位字计算，
// 补零也计入包长度。SDES 必须和 SR/RR 组成 compound packet 发送，
// 不能单独发（RFC 3550 §6.1），拼接交给 Compound。
func CNAME(ssrc uint32, cname string) []byte {
	if len(cname) > maxCNAMELen {
		cname = cname[:maxCNAMELen]
	}
	// chunk = SSRC(4) + item 头(2) + text + END(1)，再补齐到 4 字节。
	chunk := 4 + 2 + len(cname) + 1
	size := 4 + (chunk+3)&^3

	b := make([]byte, size)
	b[0] = 0x80 | 1 // V=2, P=0, SC=1
	b[1] = SDESType
	binary.BigEndian.PutUint16(b[2:4], uint16(size/4-1))
	binary.BigEndian.PutUint32(b[4:8], ssrc)
	b[8] = CNAMEItem
	b[9] = byte(len(cname))
	copy(b[10:], cname)
	// b[10+len(cname)] 起保持 0：END item + 补齐用的 null 字节。
	return b
}

// ParseCNAME 解析 SDES 第一个 chunk 里的 CNAME。
// 只用于测试和抓包排查；服务端自身不需要解析入站 SDES。
func ParseCNAME(b []byte) (ssrc uint32, cname string, ok bool) {
	if len(b) < 10 || b[0]>>6 != 2 || b[1] != SDESType || b[0]&0x1F == 0 {
		return 0, "", false
	}
	n := (int(binary.BigEndian.Uint16(b[2:4])) + 1) * 4
	if n < 10 || n > len(b) {
		return 0, "", false
	}
	b = b[:n]
	ssrc = binary.BigEndian.Uint32(b[4:8])
	for off := 8; off < len(b); {
		typ := b[off]
		if typ == 0 { // END：后面只剩补齐字节
			return 0, "", false
		}
		if off+2 > len(b) {
			return 0, "", false
		}
		l := int(b[off+1])
		if off+2+l > len(b) {
			return 0, "", false
		}
		if typ == CNAMEItem {
			return ssrc, string(b[off+2 : off+2+l]), true
		}
		off += 2 + l
	}
	return 0, "", false
}

// Package plc 实现接收侧丢包隐藏（Packet Loss Concealment）：
// 视频重复上一帧 + RTCP PLI 请求关键帧，音频按时间戳补静音 AU。
//
// 这里只放纯逻辑，接收路径的组装/写源在 internal/rtsp/record.go。
package plc

import "encoding/binary"

const (
	// PSFBType 是 RTCP payload-specific feedback 的 packet type（RFC 4585）。
	PSFBType = 206
	// PLIFMT 是 Picture Loss Indication 的 FMT 值（RFC 4585 §6.3.1）。
	PLIFMT = 1
	// PLISize 是 PLI 的固定长度：4 字节头 + sender SSRC + media SSRC。
	PLISize = 12
)

// PLI 构造一个 RTCP PLI 包，请求对端为 mediaSSRC 对应的视频轨发送关键帧。
// senderSSRC 是本端（接收方）自己的 SSRC。
func PLI(senderSSRC, mediaSSRC uint32) []byte {
	b := make([]byte, PLISize)
	b[0] = 0x80 | PLIFMT // V=2, P=0, FMT=1
	b[1] = PSFBType
	binary.BigEndian.PutUint16(b[2:4], PLISize/4-1)
	binary.BigEndian.PutUint32(b[4:8], senderSSRC)
	binary.BigEndian.PutUint32(b[8:12], mediaSSRC)
	return b
}

// ParsePLI 解析 PLI 的 sender/media SSRC，供测试和抓包排查使用。
func ParsePLI(b []byte) (senderSSRC, mediaSSRC uint32, ok bool) {
	if len(b) < PLISize || b[0]>>6 != 2 || b[0]&0x1F != PLIFMT || b[1] != PSFBType {
		return 0, 0, false
	}
	if length := int(binary.BigEndian.Uint16(b[2:4])); length < PLISize/4-1 {
		return 0, 0, false
	}
	return binary.BigEndian.Uint32(b[4:8]), binary.BigEndian.Uint32(b[8:12]), true
}

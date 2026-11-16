package plc

import "encoding/binary"

const (
	// FIRFMT 是 Full Intra Request 的 FMT（RFC 5104 §4.3.1）。
	FIRFMT = 4
	// FIRSize = 4 字节 RTCP 头 + sender SSRC + media SSRC +
	// FCI（target SSRC 4 + seq 1 + reserved 3）。
	FIRSize = 20
)

// FIR 构造一个 RTCP FIR 包，请求 targetSSRC 立即发送关键帧。
// senderSSRC 是本端 SSRC；media SSRC 按惯例填 0，FCI 里才是目标 SSRC。
// seq 是 FIR 自己的序号，每次请求应递增（接收端可据此去重）。
func FIR(senderSSRC, targetSSRC uint32, seq uint8) []byte {
	b := make([]byte, FIRSize)
	b[0] = 0x80 | FIRFMT // V=2, P=0, FMT=4
	b[1] = PSFBType
	binary.BigEndian.PutUint16(b[2:4], FIRSize/4-1)
	binary.BigEndian.PutUint32(b[4:8], senderSSRC)
	// [8:12] media source SSRC 保持 0。
	binary.BigEndian.PutUint32(b[12:16], targetSSRC)
	b[16] = seq
	return b
}

// ParseFIR 解析 FIR 的 sender、目标 SSRC 和 FIR 序号。
// 只接受 FMT=4、PT=206、长度自洽的包。
func ParseFIR(b []byte) (senderSSRC, targetSSRC uint32, seq uint8, ok bool) {
	if len(b) < FIRSize || b[0]>>6 != 2 || b[0]&0x1F != FIRFMT || b[1] != PSFBType {
		return 0, 0, 0, false
	}
	if int(binary.BigEndian.Uint16(b[2:4])) < FIRSize/4-1 {
		return 0, 0, 0, false
	}
	return binary.BigEndian.Uint32(b[4:8]),
		binary.BigEndian.Uint32(b[12:16]),
		b[16],
		true
}

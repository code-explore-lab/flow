// Package rtcp 实现 RTSP 推流需要的最小 RTCP 状态：
// 发送者报告（SR，PT=200）。客户端回送的 RR 只用来给会话打活跃点，
// 这里不做丢包/抖动统计。
package rtcp

import (
	"encoding/binary"
	"time"
)

const (
	// 迷你 RTCP 类型范围：第二字节落在 [192,223] 时按 RTCP 处理。
	// RFC 5761 §4 规定 mux 时 RTP payload type 不能用 64~95，
	// 否则 marker 位置位后（PT+128）会落进这个区间。
	minPacketType = 192
	maxPacketType = 223

	// SenderReportType 是 SR 的 packet type（RFC 3550 §6.4.1）。
	SenderReportType = 200

	// SenderReportSize 是 RC=0（不带 report block）的 SR 长度：
	// 4 字节头 + 4 字节 SSRC + 20 字节 sender info。
	SenderReportSize = 28

	// ntpEpochOffset 是 1900-01-01 到 1970-01-01 的秒数。
	// RTCP 的 NTP 时间戳以 1900 年为纪元，而 Go 的 time.Unix 以 1970 年为纪元。
	ntpEpochOffset = 2208988800
)

// SenderReport 是 RTCP SR 包的 sender info。
// 字段布局见 RFC 3550 §6.4.1：
//
//	0                   1                   2                   3
//	0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|V=2|P|    RC   |   PT=SR=200   |             length            |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                         SSRC of sender                        |
//	+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+
//	|              NTP timestamp, most significant word             |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|             NTP timestamp, least significant word             |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                         RTP timestamp                         |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                     sender's packet count                     |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                      sender's octet count                     |
//	+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+
type SenderReport struct {
	SSRC        uint32
	NTP         uint64 // 64 位 NTP：高 32 位秒、低 32 位小数
	RTP         uint32
	PacketCount uint32
	OctetCount  uint32
}

// NTPTime 把 Go 的墙上时间转成 64 位 NTP 时间戳：
// 高 32 位是 1900 年起的秒数，低 32 位是把纳秒按 2^32 归一化的小数部分。
func NTPTime(t time.Time) uint64 {
	sec := uint64(t.Unix() + ntpEpochOffset)
	frac := (uint64(t.Nanosecond()) << 32) / 1e9
	return sec<<32 | frac
}

// Marshal 序列化成一个 RC=0 的 SR 包。
// length 字段是“32 位字数减一”，28 字节 = 7 个字，所以填 6。
func (sr SenderReport) Marshal() []byte {
	b := make([]byte, SenderReportSize)
	b[0] = 0x80 // V=2, P=0, RC=0
	b[1] = SenderReportType
	binary.BigEndian.PutUint16(b[2:4], SenderReportSize/4-1)
	binary.BigEndian.PutUint32(b[4:8], sr.SSRC)
	binary.BigEndian.PutUint64(b[8:16], sr.NTP)
	binary.BigEndian.PutUint32(b[16:20], sr.RTP)
	binary.BigEndian.PutUint32(b[20:24], sr.PacketCount)
	binary.BigEndian.PutUint32(b[24:28], sr.OctetCount)
	return b
}

// ParseSenderReport 解析 SR 的头部和 sender info（忽略 report block），
// 给测试和抓包排查用。
func ParseSenderReport(b []byte) (SenderReport, bool) {
	if len(b) < SenderReportSize {
		return SenderReport{}, false
	}
	if b[0]>>6 != 2 || b[1] != SenderReportType {
		return SenderReport{}, false
	}
	return SenderReport{
		SSRC:        binary.BigEndian.Uint32(b[4:8]),
		NTP:         binary.BigEndian.Uint64(b[8:16]),
		RTP:         binary.BigEndian.Uint32(b[16:20]),
		PacketCount: binary.BigEndian.Uint32(b[20:24]),
		OctetCount:  binary.BigEndian.Uint32(b[24:28]),
	}, true
}

// IsRTCP 判断一个 UDP 报文是不是 RTCP，用于 rtcp-mux 单端口下区分
// RTP（RFC 3550 §5.1）和 RTCP（RFC 3550 §6.1、RFC 5761 §4）。
//
// 判定依据（按顺序）：
//   - 版本必须是 2（RTP/RTCP 共用同一版本字段，所以版本本身无法区分）；
//   - 第二字节是 RTCP packet type，取值 192~223；
//   - length 字段（32 位字减一）至少覆盖 RTCP 固定头，且不超过实际报文长度。
//
// 复合 RTCP 包的第一条子包长度通常小于总长度，因此用 <= 而不是 ==。
func IsRTCP(b []byte) bool {
	if len(b) < 8 || b[0]>>6 != 2 {
		return false
	}
	pt := b[1]
	if pt < minPacketType || pt > maxPacketType {
		return false
	}
	n := (int(binary.BigEndian.Uint16(b[2:4])) + 1) * 4
	return n >= 8 && n <= len(b)
}

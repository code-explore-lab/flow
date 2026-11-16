package rtcp

import "encoding/binary"

const (
	// ReceiverReportType 是 RR 的 packet type（RFC 3550 §6.4.2）。
	ReceiverReportType = 201

	// ReportBlockSize 是一个 report block 的固定长度。
	ReportBlockSize = 24
)

// ReportBlock 是 RR 里针对一个媒体源的一条接收统计（RFC 3550 §6.4.2）：
//
//	0                   1                   2                   3
//	0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                 SSRC_1 (SSRC of first source)                 |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	| fraction lost |       cumulative number of packets lost       |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|           extended highest sequence number received           |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                      interarrival jitter                      |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                         last SR (LSR)                         |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                   delay since last SR (DLSR)                  |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
type ReportBlock struct {
	SSRC               uint32 // 被报告的媒体源
	FractionLost       uint8  // 自上次报告以来丢包率，单位 1/256
	CumulativeLost     int32  // 24 位有符号累计丢包数，可能为负（重复包导致）
	ExtendedHighestSeq uint32 // 扩展最高序列号（seq 回绕计数在高 16 位）
	Jitter             uint32 // 到达间隔抖动，按 RTP 时钟计
	LastSR             uint32 // 本端上次 SR 的 NTP 时间戳中间 32 位
	DelaySinceLastSR   uint32 // 收到上次 SR 到发出本 RR 的延迟，单位 1/65536 秒
}

// ReceiverReport 是一个 RR 包：上报方 SSRC + 若干 report block。
type ReceiverReport struct {
	SSRC   uint32 // 上报方（客户端）SSRC
	Blocks []ReportBlock
}

// ParseRR 解析一个 RR 包。
// RC（低 5 位）给出 report block 数量，长度字段必须覆盖这些块；
// 额外的 profile-specific extension 被忽略。
func ParseRR(b []byte) (ReceiverReport, bool) {
	if len(b) < 8 || b[0]>>6 != 2 || b[1] != ReceiverReportType {
		return ReceiverReport{}, false
	}
	rc := int(b[0] & 0x1F)
	n := (int(binary.BigEndian.Uint16(b[2:4])) + 1) * 4
	if n < 8 || n > len(b) || 8+rc*ReportBlockSize > n {
		return ReceiverReport{}, false
	}

	rr := ReceiverReport{
		SSRC:   binary.BigEndian.Uint32(b[4:8]),
		Blocks: make([]ReportBlock, 0, rc),
	}
	for i := 0; i < rc; i++ {
		off := 8 + i*ReportBlockSize
		// cumulative lost 是 24 位有符号数，需要手动符号扩展。
		lost := int32(b[off+5])<<16 | int32(b[off+6])<<8 | int32(b[off+7])
		if lost&0x800000 != 0 {
			lost -= 1 << 24
		}
		rr.Blocks = append(rr.Blocks, ReportBlock{
			SSRC:               binary.BigEndian.Uint32(b[off:]),
			FractionLost:       b[off+4],
			CumulativeLost:     lost,
			ExtendedHighestSeq: binary.BigEndian.Uint32(b[off+8:]),
			Jitter:             binary.BigEndian.Uint32(b[off+12:]),
			LastSR:             binary.BigEndian.Uint32(b[off+16:]),
			DelaySinceLastSR:   binary.BigEndian.Uint32(b[off+20:]),
		})
	}
	return rr, true
}

// ParseRRs 提取复合 RTCP 包里的全部 RR（PT=201）。
// 客户端常把 RR 和 SDES/NACK 放在一起，所以用 Walk 逐个子包处理。
func ParseRRs(b []byte) []ReceiverReport {
	var out []ReceiverReport
	Walk(b, func(pt uint8, pkt []byte) bool {
		if pt != ReceiverReportType {
			return true
		}
		if rr, ok := ParseRR(pkt); ok {
			out = append(out, rr)
		}
		return true
	})
	return out
}

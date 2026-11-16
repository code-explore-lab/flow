package rtcp

import "encoding/binary"

// Walk 遍历复合 RTCP 包中的每个子包（RFC 3550 §6.1）。
// 每个子包的 length 字段是「32 位字数 - 1」，据此切出完整子包。
// fn 返回 false 时提前结束；版本不对或长度越界时直接停止，避免把
// 垃圾数据当成后续子包。
func Walk(b []byte, fn func(pt uint8, pkt []byte) bool) {
	for len(b) >= 4 {
		if b[0]>>6 != 2 {
			return
		}
		n := (int(binary.BigEndian.Uint16(b[2:4])) + 1) * 4
		if n < 4 || n > len(b) {
			return
		}
		if !fn(b[1], b[:n]) {
			return
		}
		b = b[n:]
	}
}

// Compound 按顺序把子包拼成一个复合 RTCP 包（RFC 3550 §6.1）。
// 调用方负责保证第一个子包是 SR 或 RR：RTCP 复合包必须由报告包开头，
// 例如 Compound(sr, CNAME(ssrc, name))。
func Compound(pkts ...[]byte) []byte {
	n := 0
	for _, p := range pkts {
		n += len(p)
	}
	out := make([]byte, 0, n)
	for _, p := range pkts {
		out = append(out, p...)
	}
	return out
}

// NACKFeedback 是复合包中解析出的一条 Generic NACK。
// 一个复合包里可能有多条（不同 media SSRC），因此用切片返回。
type NACKFeedback struct {
	SenderSSRC uint32
	MediaSSRC  uint32
	Seqs       []uint16
}

// ParseNACKs 提取复合 RTCP 包中的全部 Generic NACK（PT=205, FMT=1）。
// 客户端通常把 NACK 和 RR 放在同一个复合包里，所以不能只看第一个子包。
func ParseNACKs(b []byte) []NACKFeedback {
	var out []NACKFeedback
	Walk(b, func(pt uint8, pkt []byte) bool {
		if pt != RTPFBType {
			return true
		}
		sender, media, seqs, ok := ParseNACK(pkt)
		if ok && len(seqs) > 0 {
			out = append(out, NACKFeedback{
				SenderSSRC: sender,
				MediaSSRC:  media,
				Seqs:       seqs,
			})
		}
		return true
	})
	return out
}

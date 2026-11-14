package rtcp

import (
	"encoding/binary"
	"slices"
)

const (
	// RTPFBType 是 RTP 反馈包（RTPFB）的 PT（RFC 4585 §6.1）。
	RTPFBType = 205

	// NACKFMT 是 Generic NACK 的 FMT（RFC 4585 §6.2.1）。
	NACKFMT = 1

	// NACKHeaderSize 是 NACK 固定头：RTCP 头 + sender SSRC + media SSRC。
	NACKHeaderSize = 12

	// maxNACKFCIs 限制单个 NACK 包里的 FCI 数（每 FCI 最多 17 个 seq），
	// 防止高丢包时一个包无限膨胀。
	maxNACKFCIs = 16
)

type nackFCI struct {
	pid uint16
	blp uint16
}

// NACK 序列化一个 Generic NACK（PT=205, FMT=1）：
//
//	0                   1                   2                   3
//	|V=2|P|  FMT=1 |   PT=205    |           length            |
//	|                       sender SSRC                         |
//	|                       media SSRC                          |
//	|             PID             |              BLP            |
//
// 缺失的 seq 会排序并压缩成 PID/BLP：BLP 第 i 位表示 PID+i+1 也缺。
// seqs 为空或全部重复时返回 nil。
func NACK(senderSSRC, mediaSSRC uint32, seqs []uint16) []byte {
	fcis := buildFCIs(seqs, maxNACKFCIs)
	if len(fcis) == 0 {
		return nil
	}
	b := make([]byte, NACKHeaderSize+4*len(fcis))
	b[0] = 0x80 | NACKFMT // V=2, P=0
	b[1] = RTPFBType
	// length 是 32 位字数减一：3 个固定字 + N 个 FCI，所以填 N+2。
	binary.BigEndian.PutUint16(b[2:4], uint16(len(fcis)+2))
	binary.BigEndian.PutUint32(b[4:8], senderSSRC)
	binary.BigEndian.PutUint32(b[8:12], mediaSSRC)
	for i, fci := range fcis {
		off := NACKHeaderSize + 4*i
		binary.BigEndian.PutUint16(b[off:off+2], fci.pid)
		binary.BigEndian.PutUint16(b[off+2:off+4], fci.blp)
	}
	return b
}

// ParseNACK 解析 Generic NACK，返回发送者/媒体 SSRC 和展开后的缺失 seq。
// 只接受 FMT=1、PT=205、长度自洽的包。
func ParseNACK(b []byte) (senderSSRC, mediaSSRC uint32, seqs []uint16, ok bool) {
	if len(b) < NACKHeaderSize {
		return 0, 0, nil, false
	}
	if b[0]>>6 != 2 || b[0]&0x1F != NACKFMT || b[1] != RTPFBType {
		return 0, 0, nil, false
	}
	total := int(binary.BigEndian.Uint16(b[2:4])+1) * 4
	if total < NACKHeaderSize || total > len(b) || (total-NACKHeaderSize)%4 != 0 {
		return 0, 0, nil, false
	}
	senderSSRC = binary.BigEndian.Uint32(b[4:8])
	mediaSSRC = binary.BigEndian.Uint32(b[8:12])
	for off := NACKHeaderSize; off < total; off += 4 {
		pid := binary.BigEndian.Uint16(b[off : off+2])
		blp := binary.BigEndian.Uint16(b[off+2 : off+4])
		seqs = append(seqs, pid)
		for i := range 16 {
			if blp&(1<<uint(i)) != 0 {
				seqs = append(seqs, pid+uint16(i+1))
			}
		}
	}
	return senderSSRC, mediaSSRC, seqs, true
}

// buildFCIs 把缺失 seq 压缩成 PID/BLP，最多返回 max 个 FCI。
// seq 按 uint16 回绕处理：PID+i 溢出后自然回到 0 附近。
func buildFCIs(seqs []uint16, max int) []nackFCI {
	if len(seqs) == 0 || max <= 0 {
		return nil
	}
	set := make(map[uint16]struct{}, len(seqs))
	for _, seq := range seqs {
		set[seq] = struct{}{}
	}
	ordered := make([]uint16, 0, len(set))
	for seq := range set {
		ordered = append(ordered, seq)
	}
	slices.Sort(ordered)

	out := make([]nackFCI, 0, max)
	for _, pid := range ordered {
		if _, ok := set[pid]; !ok {
			// 已被前一个 BLP 覆盖。
			continue
		}
		var blp uint16
		for i := range 16 {
			next := pid + uint16(i+1)
			if _, ok := set[next]; ok {
				blp |= 1 << uint(i)
				delete(set, next) // 避免后续重复作为 PID
			}
		}
		delete(set, pid)
		out = append(out, nackFCI{pid: pid, blp: blp})
		if len(out) == max {
			break
		}
	}
	return out
}

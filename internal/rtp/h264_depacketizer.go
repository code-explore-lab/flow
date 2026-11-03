package rtp

// H.264 接收侧解包（RFC 6184）：
//
//   - type 1~23：单 NALU 包，payload 就是完整 NALU，直接输出；
//   - type 24：STAP-A，一个 RTP 包里聚合了多个 NALU，按长度前缀拆开；
//   - type 28：FU-A，一个 NALU 被拆成多片，按 S/E 位重组。
//
// 接收侧无法请求补发，所以遇到缺片只能丢弃当前半成品，等下一个 NALU 重新开始；
// 丢弃事件计入 Dropped，便于上层统计丢帧。

import (
	"encoding/binary"
	"errors"
	"fmt"
)

/*
	0 1 2 3 4 5 6 7

+-+-+-+-+-+-+-+-+
|F|NRI|  Type   |
+-+-+-+-+-+-+-+-+
*/
const (
	// fuaHeaderLen FU-A 的 FU indicator(1) + FU header(1)。
	fuaHeaderLen = 2
	// stapALenField STAP-A 每个 NALU 前的 2 字节长度字段。
	stapALenField = 2
	// maxNALUType 单 NALU 包允许的最大 NAL unit type（1~23）。
	maxNALUType = 23
)

var (
	ErrEmptyH264Payload    = errors.New("rtp: empty H.264 payload")
	ErrShortFUAHeader      = errors.New("rtp: FU-A payload shorter than 2-byte header")
	ErrBadSTAPA            = errors.New("rtp: malformed STAP-A payload")
	ErrUnsupportedNALUType = errors.New("rtp: unsupported H.264 NALU type")
)

// H264Depacketizer 把 RTP 包还原成完整 NALU。
//
// 每个视频轨（或每个 SSRC）持有一个实例，它不是并发安全的。
// FU-A 重组状态跨包保存：start 片开始累积，end 片输出完整 NALU；
// 中间发现 seq 不连续、timestamp 变化或 SSRC 变化，说明缺片/乱序，
// 立即丢弃半成品，避免把不同 NALU 的数据拼在一起。
//
// 返回值里单 NALU 包和 STAP-A 拆出的 NALU 与 pkt.Payload 共享底层数组，
// 需要在 RTP 缓冲区复用后继续使用时由调用方复制；FU-A 重组结果是新分配的。
type H264Depacketizer struct {
	// frag 是正在重组的 FU-A 数据，首字节已还原成原始 NAL header。
	frag []byte
	// assembling 表示当前是否在收一个 FU-A 的后续分片。
	assembling bool
	lastSeq    uint16 // 上一片的 seq，用来检测缺口和乱序
	ts         uint32 // 当前 NALU 的 RTP 时间戳
	ssrc       uint32 // 当前 NALU 的 SSRC

	// Dropped 是因丢包/乱序而被丢弃的半成品（或孤立分片）数量。
	// 上层可以据此判断是否需要请求关键帧。
	Dropped uint64
}

// Depacketize 消费一个 RTP 包，返回其中已经完整的 NALU（可能为 0 个或多个）。
//
//   - 单 NALU / STAP-A 立即返回；
//   - FU-A 的 start/中间片返回 nil，end 片返回重组好的 NALU；
//   - 缺首片、缺中间片、缺尾片等不可恢复的情况丢弃半成品并返回 nil, nil，
//     不把丢包当成解析错误，便于接收循环继续处理后续包。
//
// 只有 payload 本身非法（为空、FU-A 头截断、STAP-A 长度越界、未知类型）才返回 error。
func (d *H264Depacketizer) Depacketize(pkt *RTPPacket) ([][]byte, error) {
	if pkt == nil || len(pkt.Payload) == 0 {
		return nil, ErrEmptyH264Payload
	}

	typ := pkt.Payload[0] & 0x1F
	switch {
	case typ >= 1 && typ <= maxNALUType:
		// 单 NALU 包：新 NALU 到来意味着之前没拼完的 FU-A 已经丢了。
		d.abandon()
		return [][]byte{pkt.Payload}, nil

	case typ == NALUTypeSTAPA:
		d.abandon()
		return splitSTAPA(pkt.Payload)

	case typ == NALUTypeFUA:
		return d.depacketizeFUA(pkt)

	default:
		// STAP-B / MTAP / FU-B 等本阶段不支持；返回错误让调用方记录并跳过。
		d.abandon()
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedNALUType, typ)
	}
}

// depacketizeFUA 处理一个 FU-A 分片。
func (d *H264Depacketizer) depacketizeFUA(pkt *RTPPacket) ([][]byte, error) {
	payload := pkt.Payload
	if len(payload) < fuaHeaderLen {
		// 头都不完整，当前半成品也无法确认是否属于同一 NALU，直接放弃。
		d.abandon()
		return nil, ErrShortFUAHeader
	}

	indicator := payload[0]
	fuHeader := payload[1]
	start := fuHeader&0x80 != 0 // S 位
	end := fuHeader&0x40 != 0   // E 位
	data := payload[fuaHeaderLen:]

	// 非标准但宽容处理：S=1 且 E=1 表示整个 NALU 就在这一包里。
	if start && end {
		d.abandon()
		nalu := make([]byte, 1+len(data))
		nalu[0] = (indicator & 0xE0) | (fuHeader & 0x1F)
		copy(nalu[1:], data)
		return [][]byte{nalu}, nil
	}

	if start {
		// 新的 NALU 开始：之前还没收到 E 的半成品一定是缺尾片，丢掉。
		d.abandon()
		d.assembling = true
		d.lastSeq = pkt.Header.Seq
		d.ts = pkt.Header.Timestamp
		d.ssrc = pkt.Header.SSRC
		// 还原原始 NAL header：F/NRI 来自 FU indicator，Type 来自 FU header。
		d.frag = append(d.frag[:0], (indicator&0xE0)|(fuHeader&0x1F))
		d.frag = append(d.frag, data...)
		return nil, nil
	}

	if !d.assembling {
		// 没有 start 片（缺首片，或分片乱序先到）：无法知道前面的数据，直接丢。
		d.Dropped++
		return nil, nil
	}

	// 中间片/尾片必须和当前 NALU 属于同一个流、同一时间戳，并且 seq 紧接上一片。
	// 任一条件不满足都说明中间缺包，继续拼只会产出损坏的 NALU。
	if pkt.Header.SSRC != d.ssrc ||
		pkt.Header.Timestamp != d.ts ||
		pkt.Header.Seq != d.lastSeq+1 {
		d.abandon()
		return nil, nil
	}

	d.lastSeq = pkt.Header.Seq
	d.frag = append(d.frag, data...)

	if !end {
		return nil, nil
	}

	// E=1：重组完成。把缓冲区所有权交给调用方，下一次重组重新分配。
	nalu := d.frag
	d.frag = nil
	d.assembling = false
	return [][]byte{nalu}, nil
}

// abandon 丢弃正在重组的半成品并计数。
func (d *H264Depacketizer) abandon() {
	if !d.assembling {
		return
	}
	d.assembling = false
	d.frag = nil
	d.Dropped++
}

// splitSTAPA 拆开 STAP-A：
//
//	 0                   1                   2                   3
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|STAP-A NAL HDR |         NALU 1 Size           | NALU 1 HDR    |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                         NALU 1 Data ...                       |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|         NALU 2 Size           | NALU 2 HDR    | ...
//
// 返回的切片与 payload 共享底层数组。
func splitSTAPA(payload []byte) ([][]byte, error) {
	var nalus [][]byte
	off := 1 // 跳过 STAP-A 的 NAL header
	for off < len(payload) {
		if len(payload)-off < stapALenField {
			return nil, fmt.Errorf("%w: truncated size field", ErrBadSTAPA)
		}
		size := int(binary.BigEndian.Uint16(payload[off:]))
		off += stapALenField
		if size == 0 || size > len(payload)-off {
			return nil, fmt.Errorf("%w: NALU size %d exceeds remaining %d", ErrBadSTAPA, size, len(payload)-off)
		}
		nalus = append(nalus, payload[off:off+size])
		off += size
	}
	if len(nalus) == 0 {
		// RFC 6184：STAP-A 至少要携带一个 NALU。
		return nil, fmt.Errorf("%w: no NALU", ErrBadSTAPA)
	}
	return nalus, nil
}

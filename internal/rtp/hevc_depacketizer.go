package rtp

// H.265 / HEVC 接收侧解包（RFC 7798）：
//
//   - Type 0~47：单 NAL 单元包，payload 就是完整 NALU；
//   - Type 48  ：AP，一个 RTP 包里聚合多个 NALU，按 2 字节长度前缀拆开；
//   - Type 49  ：FU，一个 NALU 被拆成多片，按 S/E 位重组。
//
// 与 H264Depacketizer 语义一致：缺片/乱序时丢弃半成品并计入 Dropped，
// 只有 payload 本身非法才返回 error。
//
// 返回值里单 NALU / AP 拆出的结果与 pkt.Payload 共享底层数组；
// FU 重组结果是新分配的。

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var (
	ErrEmptyH265Payload    = errors.New("rtp: empty H.265 payload")
	ErrShortH265Header     = errors.New("rtp: H.265 payload shorter than 2-byte header")
	ErrBadHEVCAP           = errors.New("rtp: malformed H.265 aggregation packet")
	ErrUnsupportedH265Type = errors.New("rtp: unsupported H.265 NAL type")
)

// H265Depacketizer 把 RTP 包还原成完整 HEVC NALU。不是并发安全的。
type H265Depacketizer struct {
	frag       []byte // 正在重组的 FU 数据，前 2 字节已还原成原 NAL header
	assembling bool
	lastSeq    uint16
	ts         uint32
	ssrc       uint32

	// Dropped 是因丢包/乱序丢弃的半成品数量。
	Dropped uint64
}

// DroppedCount 返回丢弃的半成品数量（供统一的视频解包接口使用）。
func (d *H265Depacketizer) DroppedCount() uint64 { return d.Dropped }

// Depacketize 消费一个 RTP 包，返回其中已经完整的 NALU（可能为 0 个或多个）。
func (d *H265Depacketizer) Depacketize(pkt *RTPPacket) ([][]byte, error) {
	if pkt == nil || len(pkt.Payload) == 0 {
		return nil, ErrEmptyH265Payload
	}
	if len(pkt.Payload) < 2 {
		d.abandon()
		return nil, ErrShortH265Header
	}

	typ := (pkt.Payload[0] >> 1) & 0x3F
	switch {
	case typ <= 47:
		// 单 NALU 包：之前没拼完的 FU 已经丢了。
		d.abandon()
		return [][]byte{pkt.Payload}, nil

	case typ == H265TypeAP:
		d.abandon()
		return splitHEVCAP(pkt.Payload)

	case typ == H265TypeFU:
		return d.depacketizeFU(pkt)

	default:
		// PACI（50/51）等本阶段不支持。
		d.abandon()
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedH265Type, typ)
	}
}

// depacketizeFU 处理一个 FU 分片。
func (d *H265Depacketizer) depacketizeFU(pkt *RTPPacket) ([][]byte, error) {
	const fuHeaderLen = 3 // payload header(2) + FU header(1)
	payload := pkt.Payload
	if len(payload) < fuHeaderLen {
		d.abandon()
		return nil, ErrShortH265Header
	}

	ph0, ph1 := payload[0], payload[1]
	fuHeader := payload[2]
	start := fuHeader&0x80 != 0
	end := fuHeader&0x40 != 0
	fuType := fuHeader & 0x3F
	data := payload[fuHeaderLen:]

	// 还原原始 2 字节 NAL header：F/LayerId 来自 payload header，
	// Type 来自 FU header。
	nal0 := (ph0 & 0x81) | (fuType << 1)
	nal1 := ph1

	if start && end {
		d.abandon()
		nalu := make([]byte, 2+len(data))
		nalu[0], nalu[1] = nal0, nal1
		copy(nalu[2:], data)
		return [][]byte{nalu}, nil
	}

	if start {
		d.abandon()
		d.assembling = true
		d.lastSeq = pkt.Header.Seq
		d.ts = pkt.Header.Timestamp
		d.ssrc = pkt.Header.SSRC
		d.frag = append(d.frag[:0], nal0, nal1)
		d.frag = append(d.frag, data...)
		return nil, nil
	}

	if !d.assembling {
		// 缺首片或乱序先到：无法还原，直接丢。
		d.Dropped++
		return nil, nil
	}

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

	nalu := d.frag
	d.frag = nil
	d.assembling = false
	return [][]byte{nalu}, nil
}

// abandon 丢弃正在重组的半成品并计数。
func (d *H265Depacketizer) abandon() {
	if !d.assembling {
		return
	}
	d.assembling = false
	d.frag = nil
	d.Dropped++
}

// splitHEVCAP 拆开 AP：
//
//	PayloadHdr(2B) | 2B len + NALU | 2B len + NALU ...
//
// 返回的切片与 payload 共享底层数组。
func splitHEVCAP(payload []byte) ([][]byte, error) {
	var nalus [][]byte
	off := 2 // 跳过 2 字节 payload header
	for off < len(payload) {
		if len(payload)-off < 2 {
			return nil, fmt.Errorf("%w: truncated size field", ErrBadHEVCAP)
		}
		size := int(binary.BigEndian.Uint16(payload[off:]))
		off += 2
		if size < 2 || size > len(payload)-off {
			return nil, fmt.Errorf("%w: NALU size %d exceeds remaining %d", ErrBadHEVCAP, size, len(payload)-off)
		}
		nalus = append(nalus, payload[off:off+size])
		off += size
	}
	if len(nalus) == 0 {
		return nil, fmt.Errorf("%w: no NALU", ErrBadHEVCAP)
	}
	return nalus, nil
}

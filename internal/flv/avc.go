package flv

// FLV 视频 tag 的 H.264（AVC）负载封装/解析。
//
// 视频 tag 的 Data（去掉 FLV tag 头后）结构：
//
//	byte 0        高 4 位 FrameType、低 4 位 CodecID（AVC=7）
//	byte 1        AVCPacketType：0=序列头，1=NALU，2=结束
//	byte 2..4     CompositionTime（有符号 24 位，序列头为 0）
//	之后          序列头 → AVCDecoderConfigurationRecord；
//	              NALU    → 若干「4 字节长度前缀 + 裸 NALU」
//
// 这里的「裸 NALU」和 source.Frame.Data 一致（无 Annex-B 起始码）。

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// FrameType（byte 0 高 4 位）。
const (
	frameTypeKey   uint8 = 1
	frameTypeInter uint8 = 2
)

// codecIDAVC FLV 里 H.264 的 CodecID。
const codecIDAVC uint8 = 7

// AVCPacketType。
const (
	AVCSeqHeader uint8 = 0
	AVCNALU      uint8 = 1
	AVCEndSeq    uint8 = 2
)

// VideoTag 解析出的一个视频 tag 负载。
type VideoTag struct {
	Keyframe   bool     // FrameType == 1
	PacketType uint8    // AVCPacketType
	CTS        int32    // 组成时间偏移（毫秒级实际以 1/90kHz 计，仅原样搬运）
	NALUs      [][]byte // PacketType == AVCNALU 时的裸 NALU 列表
	Config     []byte   // PacketType == AVCSeqHeader 时的 AVCDecoderConfigurationRecord
}

// BuildAVCDecoderConfig 由 SPS/PPS 生成 AVCDecoderConfigurationRecord。
// 只支持一个 SPS + 一个 PPS，lengthSizeMinusOne 固定为 3（4 字节长度前缀）。
func BuildAVCDecoderConfig(sps, pps []byte) []byte {
	// 6 + 2 + len(sps) + 1 + 2 + len(pps)
	buf := make([]byte, 0, 11+len(sps)+len(pps))
	buf = append(buf, 0x01) // configurationVersion

	// AVCProfileIndication / profile_compatibility / AVCLevelIndication
	// 直接取 SPS 的第 2~4 字节（NAL 头之后）。
	if len(sps) >= 4 {
		buf = append(buf, sps[1], sps[2], sps[3])
	} else {
		buf = append(buf, 0x42, 0x00, 0x1F) // 兜底：Baseline / level 3.1
	}

	buf = append(buf, 0xFF) // 6 位保留(全 1) + lengthSizeMinusOne=3
	buf = append(buf, 0xE1) // 5 位保留(全 1) + numOfSPS=1
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(sps)))
	buf = append(buf, sps...)
	buf = append(buf, 0x01) // numOfPPS=1
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(pps)))
	buf = append(buf, pps...)
	return buf
}

// ParseAVCDecoderConfig 从 AVCDecoderConfigurationRecord 解出第一个 SPS 和 PPS。
// 返回的切片是复制出来的，调用方可复用 data 缓冲区。
func ParseAVCDecoderConfig(data []byte) (sps, pps []byte, err error) {
	if len(data) < 7 || data[0] != 0x01 {
		return nil, nil, errors.New("avc: invalid decoder config")
	}

	off := 6
	for range int(data[5] & 0x1F) {
		if off+2 > len(data) {
			return nil, nil, errors.New("avc: truncated sps length")
		}
		l := int(binary.BigEndian.Uint16(data[off : off+2]))
		off += 2
		if off+l > len(data) {
			return nil, nil, errors.New("avc: truncated sps")
		}
		if sps == nil {
			sps = append([]byte(nil), data[off:off+l]...)
		}
		off += l
	}

	if off >= len(data) {
		return nil, nil, errors.New("avc: missing pps count")
	}
	numPPS := int(data[off])
	off++
	for range numPPS {
		if off+2 > len(data) {
			return nil, nil, errors.New("avc: truncated pps length")
		}
		l := int(binary.BigEndian.Uint16(data[off : off+2]))
		off += 2
		if off+l > len(data) {
			return nil, nil, errors.New("avc: truncated pps")
		}
		if pps == nil {
			pps = append([]byte(nil), data[off:off+l]...)
		}
		off += l
	}
	if sps == nil || pps == nil {
		return nil, nil, errors.New("avc: config has no sps/pps")
	}
	return sps, pps, nil
}

// BuildVideoSequenceHeader 生成 H.264 序列头 tag 的 Data（AVCPacketType=0）。
func BuildVideoSequenceHeader(sps, pps []byte) []byte {
	config := BuildAVCDecoderConfig(sps, pps)
	body := make([]byte, 5+len(config))
	body[0] = frameTypeKey<<4 | codecIDAVC // 0x17
	body[1] = AVCSeqHeader
	// body[2:5] 已为 0（CTS=0）
	copy(body[5:], config)
	return body
}

// BuildVideoTag 生成单个 NALU 的视频 tag 的 Data（AVCPacketType=1）。
// key 决定 FrameType（关键帧=1，非关键帧=2）；cts 是组成时间偏移。
func BuildVideoTag(nalu []byte, key bool, cts int32) []byte {
	body := make([]byte, 5+4+len(nalu))
	if key {
		body[0] = frameTypeKey<<4 | codecIDAVC // 0x17
	} else {
		body[0] = frameTypeInter<<4 | codecIDAVC // 0x27
	}
	body[1] = AVCNALU
	putCTS24(body[2:5], cts)
	binary.BigEndian.PutUint32(body[5:9], uint32(len(nalu)))
	copy(body[9:], nalu)
	return body
}

// ParseVideoTag 解析一个视频 tag 的 Data。只支持 AVC（CodecID=7）。
func ParseVideoTag(data []byte) (*VideoTag, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("avc: video tag too short: %d", len(data))
	}
	if codecID := data[0] & 0x0F; codecID != codecIDAVC {
		return nil, fmt.Errorf("avc: unsupported codec id %d", codecID)
	}

	vt := &VideoTag{
		Keyframe:   data[0]>>4 == frameTypeKey,
		PacketType: data[1],
		CTS:        getCTS24(data[2:5]),
	}
	payload := data[5:]

	switch vt.PacketType {
	case AVCSeqHeader:
		vt.Config = append([]byte(nil), payload...)
	case AVCNALU:
		// 逐个拆「4 字节长度前缀 + NALU」，多 NALU 一个 tag 内连续排列。
		for off := 0; off < len(payload); {
			if off+4 > len(payload) {
				return nil, errors.New("avc: truncated nalu length")
			}
			l := int(binary.BigEndian.Uint32(payload[off : off+4]))
			off += 4
			if l == 0 || off+l > len(payload) {
				return nil, errors.New("avc: truncated nalu")
			}
			vt.NALUs = append(vt.NALUs, append([]byte(nil), payload[off:off+l]...))
			off += l
		}
	case AVCEndSeq:
		// 无 NALU 负载。
	default:
		return nil, fmt.Errorf("avc: unknown packet type %d", vt.PacketType)
	}
	return vt, nil
}

// putCTS24 写有符号 24 位组成时间偏移（大端）。
func putCTS24(b []byte, cts int32) {
	v := uint32(cts) & 0xFFFFFF
	b[0] = byte(v >> 16)
	b[1] = byte(v >> 8)
	b[2] = byte(v)
}

// getCTS24 读有符号 24 位组成时间偏移（大端）。
func getCTS24(b []byte) int32 {
	v := int32(b[0])<<16 | int32(b[1])<<8 | int32(b[2])
	if v&0x800000 != 0 {
		v |= ^0xFFFFFF // 符号扩展
	}
	return v
}

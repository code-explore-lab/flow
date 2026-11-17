package flv

// FLV 视频 tag 的 H.265 / HEVC（Enhanced RTMP）负载封装/解析。
//
// Enhanced RTMP 把视频 tag 的首字节改造成：
//
//	bit 7      IsExHeader（1 表示 FourCC 模式）
//	bit 6..4   FrameType（1=关键帧，2=非关键帧，…）
//	bit 3..0   PacketType（0=序列头，1=CodedFrames，2=序列结束，3=CodedFramesX）
//
// 之后是 4 字节 FourCC（HEVC 为 "hvc1"），再按 PacketType 接配置记录或媒体数据。
// 老式 AVC tag（首字节 bit7=0、低 4 位 CodecID=7）不受影响，见 avc.go。
//
// 序列头的负载是 ISO/IEC 14496-15 的 HEVCDecoderConfigurationRecord；
// CodedFrames 在 FourCC 之后先有 3 字节 CompositionTime，然后是
// 「4 字节长度前缀 + 裸 NALU」。CodedFramesX 省略这 3 字节。

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Enhanced RTMP 视频 packet type。
const (
	exPacketTypeSequenceStart = 0
	exPacketTypeCodedFrames   = 1
	exPacketTypeSequenceEnd   = 2
	exPacketTypeCodedFramesX  = 3
	exPacketTypeMetadata      = 4
)

// AVCMetadata Enhanced RTMP 元数据包（PacketType=4）：负载是 AMF 编码的
// 带外信息（HDR colorInfo、HDR10+ 等），不承载图像数据，上层应忽略。
const AVCMetadata uint8 = 4

// exHeaderFlag 首字节的 IsExHeader 位。
const exHeaderFlag uint8 = 0x80

// fourCCHVC1 "hvc1" 的大端 32 位值。
const fourCCHVC1 uint32 = 0x68766331

// HEVC 参数集 NAL 类型。
const (
	hevcNALTypeVPS = 32
	hevcNALTypeSPS = 33
	hevcNALTypePPS = 34
)

// BuildHEVCDecoderConfigRecord 由 VPS/SPS/PPS 生成 HEVCDecoderConfigurationRecord。
// 配置记录头里的 profile/level 从 SPS 的 profile_tier_level 解析。
func BuildHEVCDecoderConfigRecord(vps, sps, pps []byte) []byte {
	profileSpace, tier, profileIDC, compat, constraint, level := parseHEVCProfileTierLevel(sps)

	buf := make([]byte, 0, 23+len(vps)+len(sps)+len(pps)+12)
	buf = append(buf, 0x01) // configurationVersion
	buf = append(buf, profileSpace<<6|tier<<5|profileIDC)
	buf = binary.BigEndian.AppendUint32(buf, compat)
	for i := 5; i >= 0; i-- { // general_constraint_indicator_flags (48 bits)
		buf = append(buf, byte(constraint>>(8*uint(i))))
	}
	buf = append(buf, level)      // general_level_idc
	buf = append(buf, 0xF0, 0x00) // min_spatial_segmentation_idc = 0
	buf = append(buf, 0xFC)       // parallelismType = 0
	buf = append(buf, 0xFD)       // chromaFormat = 1 (4:2:0)
	buf = append(buf, 0xF8)       // bitDepthLumaMinus8 = 0
	buf = append(buf, 0xF8)       // bitDepthChromaMinus8 = 0
	buf = append(buf, 0x00, 0x00) // avgFrameRate = 0
	// constantFrameRate=0 | numTemporalLayers=1 | temporalIdNested=1 | lengthSizeMinusOne=3
	buf = append(buf, 0x1F)

	var arrays [][2]any // 先数一遍非空参数集，numOfArrays 才准确
	for _, a := range []struct {
		typ  byte
		data []byte
	}{{hevcNALTypeVPS, vps}, {hevcNALTypeSPS, sps}, {hevcNALTypePPS, pps}} {
		if len(a.data) > 0 {
			arrays = append(arrays, [2]any{a.typ, a.data})
		}
	}
	buf = append(buf, byte(len(arrays)))
	for _, a := range arrays {
		buf = appendHEVCArray(buf, a[0].(byte), a[1].([]byte))
	}
	return buf
}

// appendHEVCArray 写一个参数集数组：array header + numNalus + 每个 NALU。
func appendHEVCArray(buf []byte, nalType byte, nalu []byte) []byte {
	buf = append(buf, 0x80|nalType) // array_completeness=1, reserved=0, NAL_unit_type
	buf = binary.BigEndian.AppendUint16(buf, 1)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(nalu)))
	return append(buf, nalu...)
}

// ParseHEVCDecoderConfigRecord 从 HEVCDecoderConfigurationRecord 解出
// 第一组 VPS/SPS/PPS。返回的切片是复制出来的。
func ParseHEVCDecoderConfigRecord(data []byte) (vps, sps, pps []byte, err error) {
	if len(data) < 23 || data[0] != 0x01 {
		return nil, nil, nil, errors.New("hevc: invalid decoder config")
	}
	numArrays := int(data[22])
	off := 23
	for range numArrays {
		if off+3 > len(data) {
			return nil, nil, nil, errors.New("hevc: truncated array header")
		}
		nalType := data[off] & 0x3F
		numNalus := int(binary.BigEndian.Uint16(data[off+1:]))
		off += 3
		for range numNalus {
			if off+2 > len(data) {
				return nil, nil, nil, errors.New("hevc: truncated nalu length")
			}
			l := int(binary.BigEndian.Uint16(data[off:]))
			off += 2
			if l == 0 || off+l > len(data) {
				return nil, nil, nil, errors.New("hevc: truncated nalu")
			}
			nalu := append([]byte(nil), data[off:off+l]...)
			off += l
			switch nalType {
			case hevcNALTypeVPS:
				if vps == nil {
					vps = nalu
				}
			case hevcNALTypeSPS:
				if sps == nil {
					sps = nalu
				}
			case hevcNALTypePPS:
				if pps == nil {
					pps = nalu
				}
			}
		}
	}
	if len(vps) == 0 || len(sps) == 0 || len(pps) == 0 {
		return nil, nil, nil, errors.New("hevc: config has no vps/sps/pps")
	}
	return vps, sps, pps, nil
}

// BuildHEVCVideoSequenceHeader 生成 HEVC 序列头 tag 的 Data。
func BuildHEVCVideoSequenceHeader(vps, sps, pps []byte) []byte {
	config := BuildHEVCDecoderConfigRecord(vps, sps, pps)
	body := make([]byte, 5+len(config))
	body[0] = exHeaderFlag | frameTypeKey<<4 | exPacketTypeSequenceStart
	binary.BigEndian.PutUint32(body[1:5], fourCCHVC1)
	copy(body[5:], config)
	return body
}

// BuildHEVCVideoTag 生成单个 NALU 的 HEVC 视频 tag（CodedFrames）。
// key 决定 FrameType；cts 是组成时间偏移。
func BuildHEVCVideoTag(nalu []byte, key bool, cts int32) []byte {
	ft := frameTypeInter
	if key {
		ft = frameTypeKey
	}
	body := make([]byte, 5+3+4+len(nalu))
	body[0] = exHeaderFlag | ft<<4 | exPacketTypeCodedFrames
	binary.BigEndian.PutUint32(body[1:5], fourCCHVC1)
	putCTS24(body[5:8], cts)
	binary.BigEndian.PutUint32(body[8:12], uint32(len(nalu)))
	copy(body[12:], nalu)
	return body
}

// parseExVideoTag 解析 FourCC 模式的视频 tag。data[0] 的 IsExHeader 位已置位。
func parseExVideoTag(data []byte) (*VideoTag, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("flv: enhanced video tag too short: %d", len(data))
	}
	frameType := (data[0] >> 4) & 0x07
	packetType := data[0] & 0x0F
	fourcc := string(data[1:5])

	switch fourcc {
	case "hvc1", "hev1":
		return parseHEVCTag(data[5:], frameType, packetType)
	default:
		return nil, fmt.Errorf("flv: unsupported enhanced fourcc %q", fourcc)
	}
}

// parseHEVCTag 解析 hvc1 视频 tag 的负载（FourCC 之后）。
func parseHEVCTag(payload []byte, frameType, packetType uint8) (*VideoTag, error) {
	vt := &VideoTag{Codec: "H265", Keyframe: frameType == frameTypeKey}

	switch packetType {
	case exPacketTypeSequenceStart:
		vt.PacketType = AVCSeqHeader
		vt.Config = append([]byte(nil), payload...)
	case exPacketTypeCodedFrames:
		if len(payload) < 3 {
			return nil, errors.New("flv: hevc coded frames missing composition time")
		}
		vt.PacketType = AVCNALU
		vt.CTS = getCTS24(payload[:3])
		nalus, err := splitLengthPrefixed(payload[3:])
		if err != nil {
			return nil, err
		}
		vt.NALUs = nalus
	case exPacketTypeCodedFramesX:
		vt.PacketType = AVCNALU
		nalus, err := splitLengthPrefixed(payload)
		if err != nil {
			return nil, err
		}
		vt.NALUs = nalus
	case exPacketTypeSequenceEnd:
		vt.PacketType = AVCEndSeq
	case exPacketTypeMetadata:
		// ffmpeg 推 HEVC 时会先发一条元数据包；内容与本实现无关，忽略。
		vt.PacketType = AVCMetadata
	default:
		return nil, fmt.Errorf("flv: unsupported enhanced packet type %d", packetType)
	}
	return vt, nil
}

// splitLengthPrefixed 拆「4 字节长度前缀 + NALU」序列，返回复制后的 NALU。
func splitLengthPrefixed(payload []byte) ([][]byte, error) {
	var nalus [][]byte
	for off := 0; off < len(payload); {
		if off+4 > len(payload) {
			return nil, errors.New("flv: truncated nalu length")
		}
		l := int(binary.BigEndian.Uint32(payload[off : off+4]))
		off += 4
		if l == 0 || off+l > len(payload) {
			return nil, errors.New("flv: truncated nalu")
		}
		nalus = append(nalus, append([]byte(nil), payload[off:off+l]...))
		off += l
	}
	return nalus, nil
}

// parseHEVCProfileTierLevel 从 SPS 里取 profile_tier_level() 的前 96 位。
// 解析失败时返回 Main profile 的兜底值。
func parseHEVCProfileTierLevel(sps []byte) (profileSpace, tier, profileIDC byte, compat uint32, constraint uint64, level byte) {
	profileIDC = 1 // Main
	if len(sps) < 4 {
		return
	}
	// SPS 前 2 字节是 NAL header，之后才是 RBSP。
	rbsp := unescapeRBSP(sps[2:])
	// 第一字节：vps_id(4) + max_sub_layers_minus1(3) + temporal_id_nesting(1)，
	// profile_tier_level 从第二字节开始，共 96 位 = 12 字节。
	if len(rbsp) < 13 {
		return
	}
	p := rbsp[1:]
	profileSpace = p[0] >> 6
	tier = (p[0] >> 5) & 0x01
	profileIDC = p[0] & 0x1F
	compat = binary.BigEndian.Uint32(p[1:5])
	for i := 5; i < 11; i++ {
		constraint = constraint<<8 | uint64(p[i])
	}
	level = p[11]
	return
}

// unescapeRBSP 去掉防竞争字节（0x03）：00 00 03 → 00 00。
func unescapeRBSP(b []byte) []byte {
	out := make([]byte, 0, len(b))
	zeros := 0
	for _, c := range b {
		if zeros >= 2 && c == 0x03 {
			zeros = 0
			continue
		}
		out = append(out, c)
		if c == 0 {
			zeros++
		} else {
			zeros = 0
		}
	}
	return out
}

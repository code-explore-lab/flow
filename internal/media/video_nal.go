package media

import "strings"

// VideoNALClass 一个视频 NAL 单元的分类结果，屏蔽 H.264 / H.265 的类型差异。
//
// 上层（参数集缓存、关键帧判定、RTP 降级）只需要这几类布尔值，
// 不用分别处理两套 NAL 类型编号。
type VideoNALClass struct {
	IsVPS bool // 仅 H.265
	IsSPS bool
	IsPPS bool
	// IsIDR 表示随机接入点：H.264 的 IDR，或 H.265 的 BLA/IDR/CRA。
	IsIDR bool
	// IsVCL 表示承载图像数据（slice）的 NAL；参数集和非 VCL 为 false。
	IsVCL bool
}

// ClassifyVideoNAL 按编码分类一个裸 NALU（不含 Annex-B 起始码）。
// codec 为空或无法识别时按 H.264 处理，保持对现有调用方的兼容。
func ClassifyVideoNAL(codec string, nalu []byte) VideoNALClass {
	if isH265(codec) {
		typ, err := H265NALType(nalu)
		if err != nil {
			return VideoNALClass{}
		}
		return VideoNALClass{
			IsVPS: typ == HEVCNALTypeVPS,
			IsSPS: typ == HEVCNALTypeSPS,
			IsPPS: typ == HEVCNALTypePPS,
			IsIDR: typ >= HEVCNALTypeBLAWLP && typ <= HEVCNALTypeCRA,
			IsVCL: typ <= 31,
		}
	}
	typ, err := NALType(nalu)
	if err != nil {
		return VideoNALClass{}
	}
	return VideoNALClass{
		IsSPS: typ == 7,
		IsPPS: typ == 8,
		IsIDR: typ == 5,
		IsVCL: typ >= 1 && typ <= 5,
	}
}

// VideoNALType 按编码返回 NAL 单元类型编号。codec 为空按 H.264 处理。
func VideoNALType(codec string, nalu []byte) (uint8, error) {
	if isH265(codec) {
		return H265NALType(nalu)
	}
	return NALType(nalu)
}

// IsHEVCCodec 判断编码名是否表示 H.265（SDP 常用 H265，部分客户端写 HEVC）。
func IsHEVCCodec(codec string) bool {
	return strings.EqualFold(codec, "H265") || strings.EqualFold(codec, "HEVC")
}

// StartsVideoAU 判断 nalu 是否是一个新访问单元（一帧）的第一个 NALU。
//
// 编码器开 sliced threads 时，一帧会被切成多个 VCL NALU。只有首 slice
// 的 first_mb_in_slice / first_slice_segment_in_pic_flag 标记为新帧，
// 其余 slice 属于同一帧，必须共用同一个 RTP 时间戳并作为同一个 AU 封包。
//
// H.264：VCL NAL（类型 1/2/5）的 first_mb_in_slice 是 ue(v)，
// 编码值 0 表示首 slice，而 ue(v)=0 的码字就是单个比特 1，所以只需看
// NAL 头之后第一个比特。类型 3/4 是 slice data partition B/C，首元素不是
// first_mb_in_slice，保守地按非首 slice 处理。
//
// H.265：VCL NAL（类型 0..31）的 first_slice_segment_in_pic_flag 是
// 2 字节 NAL 头之后 slice segment header 的第一个比特。
//
// 非 VCL、过短或无法识别的 NALU 返回 false。
func StartsVideoAU(codec string, nalu []byte) bool {
	if isH265(codec) {
		return hevcStartsAU(nalu)
	}
	return h264StartsAU(nalu)
}

func h264StartsAU(nalu []byte) bool {
	if len(nalu) < 2 {
		return false
	}
	switch nalu[0] & 0x1F {
	case 1, 2, 5: // non-IDR / slice partition A / IDR：首个语法元素是 first_mb_in_slice
		return nalu[1]&0x80 != 0
	default: // 3/4 分区 B/C、参数集、SEI 等
		return false
	}
}

func hevcStartsAU(nalu []byte) bool {
	if len(nalu) < 3 {
		return false
	}
	if (nalu[0]>>1)&0x3F > 31 { // 非 VCL（VPS=32 / SPS=33 / PPS=34 等）
		return false
	}
	return nalu[2]&0x80 != 0
}

func isH265(codec string) bool { return IsHEVCCodec(codec) }

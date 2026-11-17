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

func isH265(codec string) bool { return IsHEVCCodec(codec) }

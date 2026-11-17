package media

import "errors"

// H.265 / HEVC NAL 单元类型（ITU-T H.265 Table 7-1）。
const (
	HEVCNALTypeBLAWLP   = 16 // BLA_W_LP：可独立解码的随机接入点
	HEVCNALTypeBLAWRADL = 17 // BLA_W_RADL
	HEVCNALTypeBLANLP   = 18 // BLA_N_LP
	HEVCNALTypeIDRWRADL = 19 // IDR_W_RADL
	HEVCNALTypeIDRNLP   = 20 // IDR_N_LP
	HEVCNALTypeCRA      = 21 // CRA_NUT：Clean Random Access

	HEVCNALTypeVPS = 32 // 视频参数集
	HEVCNALTypeSPS = 33 // 序列参数集
	HEVCNALTypePPS = 34 // 图像参数集
)

// H265NALType 返回 HEVC NAL 单元类型。
//
// HEVC NAL 头固定 2 字节：
//
//	bit 15     forbidden_zero_bit
//	bit 14..9  nal_unit_type (6 bits)
//	bit 8..3   nuh_layer_id (6 bits)
//	bit 2..0   nuh_temporal_id_plus1 (3 bits)
//
// 所以类型是第一个字节右移 1 位后取低 6 位。
func H265NALType(nalu []byte) (uint8, error) {
	if len(nalu) < 2 {
		return 0, errors.New("empty H.265 nalu")
	}
	return (nalu[0] >> 1) & 0x3F, nil
}

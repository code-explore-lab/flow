package media

import (
	"errors"
	"fmt"
)

// SPSInfo 是 H.264 Sequence Parameter Set 里与封装/播放相关的字段。
//
// 只解析到「宽高 + profile/level」为止：fMP4 的 avcC 需要 profile/level，
// master playlist 的 RESOLUTION / CODECS 需要宽高和 codec string。
// 更深的 VUI / HRD 参数不解析。
type SPSInfo struct {
	ProfileIDC      uint8
	ConstraintFlags uint8 // SPS 第 2 字节（constraint_set0..5 + 保留位）
	LevelIDC        uint8
	ChromaFormatIDC uint8 // 0=单色，1=4:2:0，2=4:2:2，3=4:4:4
	Width           int
	Height          int
	FrameMBSOnly    bool
}

// CodecString 返回 RFC 6381 的 AVC codec string，例如 "avc1.64001e"。
func (s *SPSInfo) CodecString() string {
	return fmt.Sprintf("avc1.%02x%02x%02x", s.ProfileIDC, s.ConstraintFlags, s.LevelIDC)
}

// ParseSPS 解析一个 SPS NAL 单元（含 1 字节 NAL 头，type=7）。
//
// 处理 emulation prevention（00 00 03 -> 00 00），并按 profile 跳过
// chroma_format_idc / scaling list / POC 等语法元素，最后算出裁剪后的宽高。
func ParseSPS(nalu []byte) (*SPSInfo, error) {
	if len(nalu) < 5 {
		return nil, errors.New("sps: too short")
	}
	if nalu[0]&0x80 != 0 {
		return nil, errors.New("sps: forbidden_zero_bit set")
	}
	if nalu[0]&0x1F != 7 {
		return nil, fmt.Errorf("sps: wrong NAL type %d (want 7)", nalu[0]&0x1F)
	}

	rbsp := unescapeRBSP(nalu[1:])
	if len(rbsp) < 3 {
		return nil, errors.New("sps: rbsp too short")
	}

	info := &SPSInfo{
		ProfileIDC:      rbsp[0],
		ConstraintFlags: rbsp[1],
		LevelIDC:        rbsp[2],
		ChromaFormatIDC: 1,
	}
	r := &bitReader{data: rbsp[3:]}

	if _, err := r.readUE(); err != nil { // seq_parameter_set_id
		return nil, fmt.Errorf("sps: seq_parameter_set_id: %w", err)
	}

	chromaArrayType := uint8(1)
	if isHighProfile(info.ProfileIDC) {
		chroma, err := r.readUE()
		if err != nil {
			return nil, fmt.Errorf("sps: chroma_format_idc: %w", err)
		}
		if chroma > 3 {
			return nil, fmt.Errorf("sps: invalid chroma_format_idc %d", chroma)
		}
		info.ChromaFormatIDC = uint8(chroma)
		chromaArrayType = uint8(chroma)
		if chroma == 3 {
			separate, err := r.readBit()
			if err != nil {
				return nil, fmt.Errorf("sps: separate_colour_plane_flag: %w", err)
			}
			if separate == 1 {
				chromaArrayType = 0
			}
		}
		if _, err := r.readUE(); err != nil { // bit_depth_luma_minus8
			return nil, fmt.Errorf("sps: bit_depth_luma_minus8: %w", err)
		}
		if _, err := r.readUE(); err != nil { // bit_depth_chroma_minus8
			return nil, fmt.Errorf("sps: bit_depth_chroma_minus8: %w", err)
		}
		if _, err := r.readBit(); err != nil { // qpprime_y_zero_transform_bypass_flag
			return nil, fmt.Errorf("sps: qpprime_y_zero_transform_bypass_flag: %w", err)
		}
		scaling, err := r.readBit()
		if err != nil {
			return nil, fmt.Errorf("sps: seq_scaling_matrix_present_flag: %w", err)
		}
		if scaling == 1 {
			lists := 8
			if chroma == 3 {
				lists = 12
			}
			for i := 0; i < lists; i++ {
				present, err := r.readBit()
				if err != nil {
					return nil, fmt.Errorf("sps: scaling list %d: %w", i, err)
				}
				if present == 0 {
					continue
				}
				size := 16
				if i >= 6 {
					size = 64
				}
				if err := skipScalingList(r, size); err != nil {
					return nil, fmt.Errorf("sps: scaling list %d: %w", i, err)
				}
			}
		}
	}

	if _, err := r.readUE(); err != nil { // log2_max_frame_num_minus4
		return nil, fmt.Errorf("sps: log2_max_frame_num_minus4: %w", err)
	}
	pocType, err := r.readUE()
	if err != nil {
		return nil, fmt.Errorf("sps: pic_order_cnt_type: %w", err)
	}
	switch pocType {
	case 0:
		if _, err := r.readUE(); err != nil { // log2_max_pic_order_cnt_lsb_minus4
			return nil, fmt.Errorf("sps: log2_max_pic_order_cnt_lsb_minus4: %w", err)
		}
	case 1:
		if _, err := r.readBit(); err != nil { // delta_pic_order_always_zero_flag
			return nil, fmt.Errorf("sps: delta_pic_order_always_zero_flag: %w", err)
		}
		if _, err := r.readSE(); err != nil { // offset_for_non_ref_pic
			return nil, fmt.Errorf("sps: offset_for_non_ref_pic: %w", err)
		}
		if _, err := r.readSE(); err != nil { // offset_for_top_to_bottom_field
			return nil, fmt.Errorf("sps: offset_for_top_to_bottom_field: %w", err)
		}
		cycle, err := r.readUE()
		if err != nil {
			return nil, fmt.Errorf("sps: num_ref_frames_in_pic_order_cnt_cycle: %w", err)
		}
		if cycle > 255 {
			return nil, fmt.Errorf("sps: num_ref_frames_in_pic_order_cnt_cycle too large: %d", cycle)
		}
		for i := uint32(0); i < cycle; i++ {
			if _, err := r.readSE(); err != nil {
				return nil, fmt.Errorf("sps: offset_for_ref_frame[%d]: %w", i, err)
			}
		}
	case 2:
		// 无额外语法元素
	default:
		return nil, fmt.Errorf("sps: invalid pic_order_cnt_type %d", pocType)
	}

	if _, err := r.readUE(); err != nil { // max_num_ref_frames
		return nil, fmt.Errorf("sps: max_num_ref_frames: %w", err)
	}
	if _, err := r.readBit(); err != nil { // gaps_in_frame_num_value_allowed_flag
		return nil, fmt.Errorf("sps: gaps_in_frame_num_value_allowed_flag: %w", err)
	}
	widthMB, err := r.readUE() // pic_width_in_mbs_minus1
	if err != nil {
		return nil, fmt.Errorf("sps: pic_width_in_mbs_minus1: %w", err)
	}
	heightMap, err := r.readUE() // pic_height_in_map_units_minus1
	if err != nil {
		return nil, fmt.Errorf("sps: pic_height_in_map_units_minus1: %w", err)
	}
	frameMBSOnly, err := r.readBit()
	if err != nil {
		return nil, fmt.Errorf("sps: frame_mbs_only_flag: %w", err)
	}
	if frameMBSOnly == 0 {
		if _, err := r.readBit(); err != nil { // mb_adaptive_frame_field_flag
			return nil, fmt.Errorf("sps: mb_adaptive_frame_field_flag: %w", err)
		}
	}
	if _, err := r.readBit(); err != nil { // direct_8x8_inference_flag
		return nil, fmt.Errorf("sps: direct_8x8_inference_flag: %w", err)
	}

	cropLeft, cropRight, cropTop, cropBottom := uint32(0), uint32(0), uint32(0), uint32(0)
	cropping, err := r.readBit()
	if err != nil {
		return nil, fmt.Errorf("sps: frame_cropping_flag: %w", err)
	}
	if cropping == 1 {
		if cropLeft, err = r.readUE(); err != nil {
			return nil, fmt.Errorf("sps: frame_crop_left_offset: %w", err)
		}
		if cropRight, err = r.readUE(); err != nil {
			return nil, fmt.Errorf("sps: frame_crop_right_offset: %w", err)
		}
		if cropTop, err = r.readUE(); err != nil {
			return nil, fmt.Errorf("sps: frame_crop_top_offset: %w", err)
		}
		if cropBottom, err = r.readUE(); err != nil {
			return nil, fmt.Errorf("sps: frame_crop_bottom_offset: %w", err)
		}
	}

	if widthMB > 1<<16 || heightMap > 1<<16 {
		return nil, fmt.Errorf("sps: picture size too large (%d x %d MB)", widthMB, heightMap)
	}

	width := (int(widthMB) + 1) * 16
	height := (int(heightMap) + 1) * 16 * (2 - int(frameMBSOnly))

	// 裁剪单位取决于色度采样格式和帧/场模式（H.264 7.4.2.1.1）。
	subWidthC, subHeightC := 1, 1
	switch chromaArrayType {
	case 1: // 4:2:0
		subWidthC, subHeightC = 2, 2
	case 2: // 4:2:2
		subWidthC, subHeightC = 2, 1
	}
	cropUnitX := subWidthC
	cropUnitY := subHeightC * (2 - int(frameMBSOnly))
	width -= cropUnitX * int(cropLeft+cropRight)
	height -= cropUnitY * int(cropTop+cropBottom)
	if width <= 0 || height <= 0 {
		return nil, fmt.Errorf("sps: invalid cropped size %dx%d", width, height)
	}

	info.Width = width
	info.Height = height
	info.FrameMBSOnly = frameMBSOnly == 1
	return info, nil
}

// isHighProfile 判断 profile_idc 是否包含 chroma_format_idc 等扩展语法。
func isHighProfile(profile uint8) bool {
	switch profile {
	case 100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135:
		return true
	}
	return false
}

// skipScalingList 跳过 scaling_list（delta_scale 是 se(v)，最多 64 个）。
func skipScalingList(r *bitReader, size int) error {
	lastScale, nextScale := int32(8), int32(8)
	for i := 0; i < size; i++ {
		if nextScale != 0 {
			delta, err := r.readSE()
			if err != nil {
				return err
			}
			nextScale = int32((int64(lastScale) + int64(delta) + 256) % 256)
		}
		if nextScale != 0 {
			lastScale = nextScale
		}
	}
	return nil
}

// unescapeRBSP 去掉 H.264 的 emulation prevention 字节：
// 连续两个 0x00 之后的 0x03 是防竞争字节，解码时删除。
func unescapeRBSP(in []byte) []byte {
	out := make([]byte, 0, len(in))
	zeros := 0
	for _, b := range in {
		if zeros >= 2 && b == 0x03 {
			zeros = 0
			continue
		}
		out = append(out, b)
		if b == 0 {
			zeros++
		} else {
			zeros = 0
		}
	}
	return out
}

// bitReader 按位读取 RBSP，所有读操作都做边界检查。
type bitReader struct {
	data []byte
	pos  int // 已读位数
}

func (r *bitReader) readBit() (uint8, error) {
	if r.pos >= len(r.data)*8 {
		return 0, errors.New("unexpected end of sps")
	}
	b := r.data[r.pos/8] >> (7 - uint(r.pos%8)) & 1
	r.pos++
	return b, nil
}

func (r *bitReader) readBits(n int) (uint32, error) {
	if n < 0 || n > 32 {
		return 0, fmt.Errorf("invalid bit count %d", n)
	}
	var v uint32
	for i := 0; i < n; i++ {
		b, err := r.readBit()
		if err != nil {
			return 0, err
		}
		v = v<<1 | uint32(b)
	}
	return v, nil
}

// readUE 读无符号 Exp-Golomb（ue(v)）。
func (r *bitReader) readUE() (uint32, error) {
	zeros := 0
	for {
		b, err := r.readBit()
		if err != nil {
			return 0, err
		}
		if b == 1 {
			break
		}
		zeros++
		if zeros > 31 {
			return 0, errors.New("exp-golomb value too large")
		}
	}
	if zeros == 0 {
		return 0, nil
	}
	suffix, err := r.readBits(zeros)
	if err != nil {
		return 0, err
	}
	return (uint32(1) << uint(zeros)) - 1 + suffix, nil
}

// readSE 读有符号 Exp-Golomb（se(v)）。
func (r *bitReader) readSE() (int32, error) {
	v, err := r.readUE()
	if err != nil {
		return 0, err
	}
	if v&1 == 1 {
		return int32((v + 1) / 2), nil
	}
	return -int32(v / 2), nil
}

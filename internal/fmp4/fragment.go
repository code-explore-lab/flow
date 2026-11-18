package fmp4

import (
	"errors"
	"fmt"
	"math"
)

// Sample 一个已编码的媒体样本。
//
// 视频的 Data 是一个访问单元（多个 NALU 各自带 4 字节长度前缀，不含参数集）；
// 音频的 Data 是一个裸 AAC AU。DTS 是轨道时间轴上的解码时间，
// Duration 是样本时长，CTS = PTS − DTS（B 帧可为负）。
type Sample struct {
	Data     []byte
	DTS      int64
	Duration uint32
	CTS      int32
	Keyframe bool
}

// TrackFragment 一个 moof 里的一条轨的样本。
// WriteCTS 为 true 时 trun 写有符号 composition time offset（视频），
// 为 false 时不写 CTS，且要求所有样本 CTS 为 0（音频）。
type TrackFragment struct {
	TrackID  uint32
	Samples  []Sample
	WriteCTS bool
}

// Fragment 生成一个 media fragment：moof(mfhd + traf*) + mdat。
//
// 不包含 styp；父段文件由调用方在最前面拼上 Styp()。
// 所有 fragment 使用 default-base-is-moof，trun 的 data_offset 相对 moof 起点。
func Fragment(seq uint32, frags ...TrackFragment) ([]byte, error) {
	if len(frags) == 0 {
		return nil, errors.New("fmp4: fragment has no tracks")
	}

	seen := make(map[uint32]bool, len(frags))
	total := 0
	for i := range frags {
		f := &frags[i]
		if f.TrackID == 0 {
			return nil, fmt.Errorf("fmp4: fragment %d has track ID 0", i)
		}
		if seen[f.TrackID] {
			return nil, fmt.Errorf("fmp4: duplicate track ID %d", f.TrackID)
		}
		seen[f.TrackID] = true
		if len(f.Samples) == 0 {
			return nil, fmt.Errorf("fmp4: track %d has no samples", f.TrackID)
		}
		for j := range f.Samples {
			s := &f.Samples[j]
			if len(s.Data) == 0 {
				return nil, fmt.Errorf("fmp4: track %d sample %d has no data", f.TrackID, j)
			}
			if s.Duration == 0 {
				return nil, fmt.Errorf("fmp4: track %d sample %d has zero duration", f.TrackID, j)
			}
			if !f.WriteCTS && s.CTS != 0 {
				return nil, fmt.Errorf("fmp4: track %d sample %d has CTS %d but WriteCTS is false", f.TrackID, j, s.CTS)
			}
			if j > 0 {
				prev := f.Samples[j-1]
				if s.DTS != prev.DTS+int64(prev.Duration) {
					return nil, fmt.Errorf("fmp4: track %d sample %d DTS %d is not contiguous (want %d)",
						f.TrackID, j, s.DTS, prev.DTS+int64(prev.Duration))
				}
			}
			if total > math.MaxInt32-8-len(s.Data) {
				return nil, errors.New("fmp4: fragment too large")
			}
			total += len(s.Data)
		}
		if f.Samples[0].DTS < 0 {
			return nil, fmt.Errorf("fmp4: track %d has negative DTS", f.TrackID)
		}
	}

	// 第一遍用 0 占位算 moof 大小（data_offset 固定 4 字节，大小不变），
	// 第二遍写真实偏移：moof 之后是 mdat 的 8 字节头，再按轨顺序排数据。
	buildMoof := func(offsets []int) []byte {
		var mb builder
		mb.box("moof", func() {
			mb.fullBox("mfhd", 0, 0, func() {
				mb.u32(seq)
			})
			for i, f := range frags {
				f := f
				mb.box("traf", func() {
					mb.fullBox("tfhd", 0, 0x020002, func() {
						mb.u32(f.TrackID)
						mb.u32(1) // sample_description_index
					})
					mb.fullBox("tfdt", 1, 0, func() {
						mb.u64(uint64(f.Samples[0].DTS))
					})
					flags := uint32(0x000701) // duration + size + flags + data offset
					version := uint8(0)
					if f.WriteCTS {
						flags |= 0x000800
						version = 1
					}
					mb.fullBox("trun", version, flags, func() {
						mb.u32(uint32(len(f.Samples)))
						mb.u32(uint32(offsets[i]))
						for _, s := range f.Samples {
							mb.u32(s.Duration)
							mb.u32(uint32(len(s.Data)))
							mb.u32(sampleFlags(s))
							if f.WriteCTS {
								// 有符号值转 uint32 后按大端写出即为补码。
								mb.u32(uint32(s.CTS))
							}
						}
					})
				})
			}
		})
		return mb.buf
	}

	empty := make([]int, len(frags))
	moofSize := len(buildMoof(empty))
	payloadStart := moofSize + 8
	offsets := make([]int, len(frags))
	off := payloadStart
	for i, f := range frags {
		offsets[i] = off
		for _, s := range f.Samples {
			off += len(s.Data)
		}
	}
	if off-payloadStart != total {
		return nil, errors.New("fmp4: internal size mismatch")
	}
	if off > math.MaxInt32 {
		return nil, errors.New("fmp4: fragment data offset exceeds int32")
	}

	moof := buildMoof(offsets)
	if len(moof) != moofSize {
		return nil, errors.New("fmp4: internal moof size changed")
	}

	var mb builder
	mb.box("mdat", func() {
		for _, f := range frags {
			for _, s := range f.Samples {
				mb.bytes(s.Data)
			}
		}
	})
	if mb.err != nil {
		return nil, mb.err
	}

	out := make([]byte, 0, len(moof)+len(mb.buf))
	out = append(out, moof...)
	out = append(out, mb.buf...)
	return out, nil
}

// Styp 返回一个 segment type box（major=msdh，compatible=msdh,msix），
// 供父段文件开头使用；part 本身不带 styp。
func Styp() []byte {
	var b builder
	b.box("styp", func() {
		b.bytes([]byte("msdh"))
		b.u32(0)
		b.bytes([]byte("msdh"))
		b.bytes([]byte("msix"))
	})
	return b.buf
}

// sampleFlags 生成 trun 里的 sample_flags：
// 同步样本（IDR/音频）sample_depends_on=2，非同步样本 depends_on=1 且置 non-sync 位。
func sampleFlags(s Sample) uint32 {
	if s.Keyframe {
		return 0x02000000
	}
	return 0x01010000
}

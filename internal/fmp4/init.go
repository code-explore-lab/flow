package fmp4

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	videoTrackID = 1
	audioTrackID = 2
	// movieTimescale 是 mvhd 的时间刻度；live init 的 duration 恒为 0。
	movieTimescale = 1000
)

// VideoConfig 描述一条 H.264 视频轨的初始化参数。
// SPS/PPS 必须是去掉 Annex-B 起始码的裸 NALU（含 NAL 头）。
type VideoConfig struct {
	Timescale uint32 // 通常 90000
	SPS       []byte
	PPS       []byte
	Width     int // 0 表示未知（tkhd 宽高写 0，master 省略 RESOLUTION）
	Height    int
}

// AudioConfig 描述一条 AAC 音频轨的初始化参数。
type AudioConfig struct {
	Timescale  uint32 // 通常等于采样率
	ASC        []byte // AudioSpecificConfig
	SampleRate uint32
	Channels   uint8
	MaxBitrate uint32
	AvgBitrate uint32
}

// Config 一个 init segment 里的轨道集合；至少给一轨。
type Config struct {
	Video *VideoConfig
	Audio *AudioConfig
}

// track 是 Init 内部使用的归一化轨道描述。
type track struct {
	id        uint32
	timescale uint32
	video     *VideoConfig
	audio     *AudioConfig
}

// Init 生成 fMP4（CMAF）的 Media Initialization Section：
// ftyp + moov(mvhd, trak*, mvex(trex*)).
//
// 生成的 moov 是 live 头：mvhd/tkhd duration 为 0，样本表为空，
// 具体样本信息全部放在 fragment 的 moof/traf 里。
func Init(cfg Config) ([]byte, error) {
	if cfg.Video == nil && cfg.Audio == nil {
		return nil, errors.New("fmp4: init needs at least one track")
	}

	var tracks []track
	if v := cfg.Video; v != nil {
		if v.Timescale == 0 {
			return nil, errors.New("fmp4: video timescale is zero")
		}
		if len(v.SPS) < 4 {
			return nil, errors.New("fmp4: video SPS must be at least 4 bytes (NAL header + profile/level)")
		}
		if len(v.SPS) > 0xFFFF || len(v.PPS) == 0 || len(v.PPS) > 0xFFFF {
			return nil, errors.New("fmp4: video SPS/PPS length out of range")
		}
		if v.Width < 0 || v.Width > 0xFFFF || v.Height < 0 || v.Height > 0xFFFF {
			return nil, fmt.Errorf("fmp4: invalid video size %dx%d", v.Width, v.Height)
		}
		tracks = append(tracks, track{id: videoTrackID, timescale: v.Timescale, video: v, audio: nil})
	}
	if a := cfg.Audio; a != nil {
		if a.Timescale == 0 {
			return nil, errors.New("fmp4: audio timescale is zero")
		}
		if len(a.ASC) < 2 {
			return nil, errors.New("fmp4: audio ASC too short")
		}
		if a.Channels == 0 {
			return nil, errors.New("fmp4: audio channels is zero")
		}
		tracks = append(tracks, track{id: audioTrackID, timescale: a.Timescale, audio: a})
	}

	nextTrackID := uint32(1)
	for _, t := range tracks {
		if t.id >= nextTrackID {
			nextTrackID = t.id + 1
		}
	}

	var b builder
	b.box("ftyp", func() {
		b.bytes([]byte("iso6")) // major brand：HLS 要求 iso6 或更高
		b.u32(0)
		b.bytes([]byte("iso6"))
		b.bytes([]byte("mp41"))
		b.bytes([]byte("cmfc"))
	})
	b.box("moov", func() {
		b.mvhd(nextTrackID)
		for _, t := range tracks {
			b.trak(t)
		}
		b.box("mvex", func() {
			for _, t := range tracks {
				b.trex(t)
			}
		})
	})
	if b.err != nil {
		return nil, b.err
	}
	return b.buf, nil
}

func (b *builder) mvhd(nextTrackID uint32) {
	b.fullBox("mvhd", 0, 0, func() {
		b.u32(0) // creation_time
		b.u32(0) // modification_time
		b.u32(movieTimescale)
		b.u32(0) // duration：live
		b.u32(0x00010000)
		b.u16(0x0100)
		b.u16(0) // reserved
		b.u32(0)
		b.u32(0)
		b.matrix()
		for i := 0; i < 6; i++ {
			b.u32(0) // pre_defined
		}
		b.u32(nextTrackID)
	})
}

func (b *builder) trak(t track) {
	b.box("trak", func() {
		b.tkhd(t)
		b.box("mdia", func() {
			b.mdhd(t.timescale)
			b.hdlr(t)
			b.box("minf", func() {
				if t.video != nil {
					b.vmhd()
				} else {
					b.smhd()
				}
				b.dinf()
				b.stbl(t)
			})
		})
	})
}

func (b *builder) tkhd(t track) {
	b.fullBox("tkhd", 0, 3, func() { // flags: enabled | in_movie
		b.u32(0) // creation_time
		b.u32(0) // modification_time
		b.u32(t.id)
		b.u32(0) // reserved
		b.u32(0) // duration：live
		b.u32(0)
		b.u32(0) // reserved
		b.u16(0) // layer
		b.u16(0) // alternate_group
		if t.video != nil {
			b.u16(0)
		} else {
			b.u16(0x0100) // volume 1.0
		}
		b.u16(0) // reserved
		b.matrix()
		if t.video != nil {
			b.u32(uint32(t.video.Width) << 16)
			b.u32(uint32(t.video.Height) << 16)
		} else {
			b.u32(0)
			b.u32(0)
		}
	})
}

func (b *builder) mdhd(timescale uint32) {
	b.fullBox("mdhd", 0, 0, func() {
		b.u32(0) // creation_time
		b.u32(0) // modification_time
		b.u32(timescale)
		b.u32(0) // duration：live
		b.u16(0x55C4)
		b.u16(0) // pre_defined
	})
}

func (b *builder) hdlr(t track) {
	handler := "vide"
	name := "VideoHandler\x00"
	if t.audio != nil {
		handler = "soun"
		name = "SoundHandler\x00"
	}
	b.fullBox("hdlr", 0, 0, func() {
		b.u32(0) // pre_defined
		b.bytes([]byte(handler))
		b.u32(0)
		b.u32(0)
		b.u32(0)
		b.bytes([]byte(name))
	})
}

func (b *builder) vmhd() {
	b.fullBox("vmhd", 0, 1, func() {
		b.u16(0) // graphicsmode
		b.u16(0)
		b.u16(0)
		b.u16(0) // opcolor
	})
}

func (b *builder) smhd() {
	b.fullBox("smhd", 0, 0, func() {
		b.u16(0) // balance
		b.u16(0) // reserved
	})
}

func (b *builder) dinf() {
	b.box("dinf", func() {
		b.fullBox("dref", 0, 0, func() {
			b.u32(1)
			b.fullBox("url ", 0, 1, func() {}) // self-contained
		})
	})
}

func (b *builder) stbl(t track) {
	b.box("stbl", func() {
		b.fullBox("stsd", 0, 0, func() {
			b.u32(1)
			if t.video != nil {
				b.avc1(t.video)
			} else {
				b.mp4a(t.audio)
			}
		})
		// 空样本表：具体样本在 fragment 里。
		b.fullBox("stts", 0, 0, func() { b.u32(0) })
		b.fullBox("stsc", 0, 0, func() { b.u32(0) })
		b.fullBox("stsz", 0, 0, func() {
			b.u32(0) // sample_size
			b.u32(0) // sample_count
		})
		b.fullBox("stco", 0, 0, func() { b.u32(0) })
	})
}

func (b *builder) avc1(v *VideoConfig) {
	b.box("avc1", func() {
		for i := 0; i < 6; i++ {
			b.u8(0) // reserved
		}
		b.u16(1) // data_reference_index
		b.u16(0) // pre_defined
		b.u16(0) // reserved
		b.u32(0)
		b.u32(0)
		b.u32(0)                  // pre_defined[3]
		b.u16(uint16(v.Width))    // width
		b.u16(uint16(v.Height))   // height
		b.u32(0x00480000)         // horizresolution 72 dpi
		b.u32(0x00480000)         // vertresolution
		b.u32(0)                  // reserved
		b.u16(1)                  // frame_count
		b.bytes(make([]byte, 32)) // compressorname
		b.u16(0x0018)             // depth
		b.u16(0xFFFF)             // pre_defined = -1
		b.box("avcC", func() {
			b.u8(1)        // configurationVersion
			b.u8(v.SPS[1]) // AVCProfileIndication
			b.u8(v.SPS[2]) // profile_compatibility
			b.u8(v.SPS[3]) // AVCLevelIndication
			b.u8(0xFF)     // 6 bits reserved + lengthSizeMinusOne=3
			b.u8(0xE1)     // 3 bits reserved + numOfSequenceParameterSets=1
			b.u16(uint16(len(v.SPS)))
			b.bytes(v.SPS)
			b.u8(1) // numOfPictureParameterSets
			b.u16(uint16(len(v.PPS)))
			b.bytes(v.PPS)
		})
	})
}

func (b *builder) mp4a(a *AudioConfig) {
	b.box("mp4a", func() {
		for i := 0; i < 6; i++ {
			b.u8(0) // reserved
		}
		b.u16(1) // data_reference_index
		b.u32(0)
		b.u32(0) // reserved
		b.u16(uint16(a.Channels))
		b.u16(16) // samplesize
		b.u16(0)  // pre_defined
		b.u16(0)  // reserved
		sampleRate := a.SampleRate
		if sampleRate == 0 {
			sampleRate = a.Timescale
		}
		// sample entry 里的 samplerate 是 16.16 定点；超过 65535 的采样率
		// （例如 96kHz）写 0，播放器会用 mdhd 的 timescale。
		if sampleRate > 0xFFFF {
			sampleRate = 0
		}
		b.u32(sampleRate << 16)
		b.fullBox("esds", 0, 0, func() {
			b.bytes(esdsPayload(a))
		})
	})
}

// esdsPayload 拼出 ES_Descriptor(DecoderConfigDescriptor(DecoderSpecificInfo), SLConfig)。
func esdsPayload(a *AudioConfig) []byte {
	decoder := []byte{
		0x40, // objectTypeIndication: Audio ISO/IEC 14496-3
		0x15, // streamType=5 (audio) << 2 | upStream=0 << 1 | reserved=1
		0, 0, 0,
	}
	decoder = binary.BigEndian.AppendUint32(decoder, a.MaxBitrate)
	decoder = binary.BigEndian.AppendUint32(decoder, a.AvgBitrate)
	decoder = append(decoder, descriptor(0x05, a.ASC)...)

	es := []byte{0, 0, 0} // ES_ID(2) + flags(1)
	es = append(es, descriptor(0x04, decoder)...)
	es = append(es, descriptor(0x06, []byte{0x02})...)
	return descriptor(0x03, es)
}

// descriptor 写一个 MPEG-4 descriptor（tag + 长度 + payload）。
func descriptor(tag byte, payload []byte) []byte {
	out := []byte{tag}
	out = appendDescriptorLen(out, len(payload))
	return append(out, payload...)
}

// appendDescriptorLen 按 MPEG-4 的变长长度编码追加长度：
// 小于 0x80 用 1 字节，否则用 0x80|4 + 4 字节大端。
func appendDescriptorLen(dst []byte, n int) []byte {
	if n < 0x80 {
		return append(dst, byte(n))
	}
	return append(dst,
		0x80|0x04,
		byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
}

func (b *builder) trex(t track) {
	b.fullBox("trex", 0, 0, func() {
		b.u32(t.id)
		b.u32(1) // default_sample_description_index
		b.u32(0) // default_sample_duration
		b.u32(0) // default_sample_size
		b.u32(0) // default_sample_flags
	})
}

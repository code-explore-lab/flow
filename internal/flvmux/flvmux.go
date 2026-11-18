// Package flvmux 把 source.Frame 流转成 FLV Tag。
//
// 同一套转换供 HTTP-FLV 使用：视频按访问单元（AU）合并 NALU、处理 B 帧的
// DTS/CTS；音频按编码生成 AAC 序列头或 MP3/G.711 裸 tag；时间戳每轨各自
// 从首帧建立相对基准，音视频时钟互不干扰（RTSP 源的音频/视频 RTP 时钟
// 可能相差上亿）。
//
// 调用方负责写文件头（flv.WriteHeader）和把返回的 Tag 写进传输层。
package flvmux

import (
	"flow/internal/flv"
	"flow/internal/media"
	"flow/internal/source"
	"flow/internal/stream"
)

// Muxer 一路流的 FLV 封装状态。每条轨由单独的 goroutine 调用，二者不共享
// 可变状态（时钟/AU 各自独立），因此无需加锁。
type Muxer struct {
	// 视频：H.264 / H.265，参数集可能在首帧之后才补齐。
	videoCodec      string
	vps, sps, pps   []byte
	videoHeaderSent bool

	// 音频：AAC 需要序列头；MP3/G.711 直接发媒体 tag；Opus 无法进 FLV。
	audioCodec      string
	audioConfig     []byte
	audioRate       uint32
	audioClock      uint32
	audioCh         uint8
	audioEnabled    bool
	audioHeaderSent bool

	vclock, aclock relativeClock
	rebaseVideo    bool
	rebaseAudio    bool

	// 当前正在攒的视频访问单元。
	au    []source.Frame
	auPTS uint32
	auCTS int32
}

// New 创建封装器。video/audio 为 nil 表示没有该轨。
func New(video *stream.VideoCodec, audio *stream.AudioCodec) *Muxer {
	m := &Muxer{videoCodec: "H264"}
	if video != nil {
		if video.Codec != "" {
			m.videoCodec = video.Codec
		}
		m.vps, m.sps, m.pps = video.VPS, video.SPS, video.PPS
	}

	if audio == nil {
		return m
	}
	m.audioCodec = stream.NormalizeAudioCodec(audio.Codec)
	m.audioConfig = audio.Config
	m.audioRate = audio.SampleRate
	m.audioClock = audio.ClockRate
	m.audioCh = audio.Channels
	if m.audioRate == 0 {
		m.audioRate = stream.DefaultAudioSampleRate(m.audioCodec)
	}
	if m.audioClock == 0 {
		m.audioClock = stream.AudioClockRate(m.audioCodec, m.audioRate)
	}
	switch m.audioCodec {
	case stream.AudioAAC, stream.AudioMP3, stream.AudioPCMA, stream.AudioPCMU:
		m.audioEnabled = true
	}
	return m
}

// AudioEnabled 报告当前音频编码能否装进 FLV。Opus 返回 false。
func (m *Muxer) AudioEnabled() bool {
	return m.audioEnabled
}

// SequenceTags 返回当前已知参数能生成的序列头（视频 SPS/PPS、AAC ASC）。
// 参数集后来才到齐时，Video/Audio 也会在媒体 tag 之前补发。
func (m *Muxer) SequenceTags() []flv.Tag {
	var tags []flv.Tag
	if t, ok := m.videoHeaderTag(); ok {
		tags = append(tags, t)
	}
	if t, ok := m.audioHeaderTag(); ok {
		tags = append(tags, t)
	}
	return tags
}

// Video 处理一个视频帧，返回应写出的 tag（可能包含迟到的序列头）。
// 参数集帧只更新配置，不产生独立 tag。
func (m *Muxer) Video(f source.Frame) []flv.Tag {
	class := media.ClassifyVideoNAL(m.videoCodec, f.Data)
	if class.IsVPS || class.IsSPS || class.IsPPS {
		switch {
		case class.IsVPS:
			m.vps = f.Data
		case class.IsSPS:
			m.sps = f.Data
		default:
			m.pps = f.Data
		}
		if t, ok := m.videoHeaderTag(); ok {
			return []flv.Tag{t}
		}
		return nil
	}

	if len(m.au) > 0 && !f.AUStart && f.Timestamp == m.auPTS && f.CompositionTime == m.auCTS {
		m.au = append(m.au, f)
		if f.AUEnd {
			return m.flushAU()
		}
		return nil
	}

	tags := m.flushAU()
	m.auPTS, m.auCTS = f.Timestamp, f.CompositionTime
	m.au = append(m.au, f)
	// AUEnd 明确标了帧尾；没有 AU 边界信息的老源按一帧一个 tag 处理。
	if f.AUEnd || !f.AUStart {
		tags = append(tags, m.flushAU()...)
	}
	return tags
}

// FlushVideo 把未结算的访问单元写出去（源结束时调用）。
func (m *Muxer) FlushVideo() []flv.Tag {
	return m.flushAU()
}

// Audio 处理一个音频帧。不支持的编码（Opus）返回 nil。
func (m *Muxer) Audio(f source.Frame) []flv.Tag {
	if !m.audioEnabled {
		return nil
	}
	var tags []flv.Tag
	if t, ok := m.audioHeaderTag(); ok {
		tags = append(tags, t)
	}
	if m.audioClock == 0 {
		m.audioClock = stream.AudioClockRate(m.audioCodec, m.audioRate)
	}
	ms := uint32(uint64(f.Timestamp) * 1000 / uint64(m.audioClock))
	if m.rebaseAudio {
		m.aclock.rebase(ms)
		m.rebaseAudio = false
	}
	tags = append(tags, flv.Tag{
		Type:      flv.TagAudio,
		Timestamp: m.aclock.relative(ms),
		Data:      flv.BuildAudioTagFor(m.audioCodec, f.Data, m.audioRate, m.audioCh),
	})
	return tags
}

// RebaseVideo/RebaseAudio 在慢客户端丢帧后重设时间基准，避免恢复播放时
// 出现一大段空洞。
func (m *Muxer) RebaseVideo() { m.rebaseVideo = true }
func (m *Muxer) RebaseAudio() { m.rebaseAudio = true }

func (m *Muxer) flushAU() []flv.Tag {
	if len(m.au) == 0 {
		return nil
	}
	var tags []flv.Tag
	if !m.videoHeaderSent {
		t, ok := m.videoHeaderTag()
		if !ok {
			// 参数集还没到：丢弃这一帧，避免发出解码器无法初始化的数据。
			m.au = m.au[:0]
			return nil
		}
		tags = append(tags, t)
	}

	nalus := make([][]byte, 0, len(m.au))
	key := false
	for _, f := range m.au {
		nalus = append(nalus, f.Data)
		key = key || f.Keyframe
	}

	dtsMS := ticksToMillis(int64(m.auPTS) - int64(m.auCTS))
	if dtsMS < 0 {
		dtsMS = 0
	}
	ctsMS := int32(ticksToMillis(int64(m.auCTS)))
	if m.rebaseVideo {
		m.vclock.rebase(uint32(dtsMS))
		m.rebaseVideo = false
	}

	var payload []byte
	if media.IsHEVCCodec(m.videoCodec) {
		payload = flv.BuildHEVCVideoTagNALUs(nalus, key, ctsMS)
	} else {
		payload = flv.BuildVideoTagNALUs(nalus, key, ctsMS)
	}
	m.au = m.au[:0]
	tags = append(tags, flv.Tag{
		Type:      flv.TagVideo,
		Timestamp: m.vclock.relative(uint32(dtsMS)),
		Data:      payload,
	})
	return tags
}

func (m *Muxer) videoHeaderTag() (flv.Tag, bool) {
	if m.videoHeaderSent {
		return flv.Tag{}, false
	}
	if media.IsHEVCCodec(m.videoCodec) {
		if len(m.vps) == 0 || len(m.sps) == 0 || len(m.pps) == 0 {
			return flv.Tag{}, false
		}
		m.videoHeaderSent = true
		return flv.Tag{Type: flv.TagVideo, Data: flv.BuildHEVCVideoSequenceHeader(m.vps, m.sps, m.pps)}, true
	}
	if len(m.sps) == 0 || len(m.pps) == 0 {
		return flv.Tag{}, false
	}
	m.videoHeaderSent = true
	return flv.Tag{Type: flv.TagVideo, Data: flv.BuildVideoSequenceHeader(m.sps, m.pps)}, true
}

func (m *Muxer) audioHeaderTag() (flv.Tag, bool) {
	if !m.audioEnabled || m.audioHeaderSent {
		return flv.Tag{}, false
	}
	if m.audioCodec != stream.AudioAAC {
		// MP3/G.711 没有序列头，标记后不再检查。
		m.audioHeaderSent = true
		return flv.Tag{}, false
	}
	if len(m.audioConfig) == 0 {
		return flv.Tag{}, false
	}
	m.audioHeaderSent = true
	return flv.Tag{
		Type: flv.TagAudio,
		Data: flv.BuildAudioSequenceHeader(m.audioConfig, m.audioRate, m.audioCh),
	}, true
}

// ticksToMillis 把 90kHz 时基换算成毫秒（四舍五入，负数向零取整）。
func ticksToMillis(ticks int64) int64 {
	if ticks >= 0 {
		return (ticks*1000 + 45000) / 90000
	}
	return (ticks*1000 - 45000) / 90000
}

// relativeClock 一条轨的播放时间基准；由该轨的封装调用串行使用，无需加锁。
type relativeClock struct {
	set  bool
	base uint32
	last uint32
}

func (rc *relativeClock) relative(ms uint32) uint32 {
	if !rc.set {
		rc.set = true
		rc.base = ms
		return 0
	}
	if ms < rc.base {
		ms = rc.base
	}
	rel := ms - rc.base
	if rel <= rc.last {
		rel = rc.last + 1
	}
	rc.last = rel
	return rel
}

func (rc *relativeClock) rebase(ms uint32) {
	if !rc.set {
		rc.base = ms
		rc.last = 0
		rc.set = true
		return
	}
	if ms > rc.last+1 {
		rc.base = ms - rc.last - 1
	} else {
		rc.base = 0
	}
}

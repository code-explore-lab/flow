package rtmp

// RTMP 推流 ingest：把 FLV 音视频 tag 解成 source.Frame 写进共享注册表。
//
// 编码参数（AVC 序列头里的 SPS/PPS、AAC 序列头里的 ASC）除了登记到
// stream.Stream，还会作为帧写进 ExternalSource：源内部会缓存 SPS/PPS/IDR，
// 晚加入的 RTSP 拉流端订阅时先补齐参数集再解码，不需要等下一个关键帧。
//
// 时间戳换算：
//
//	视频  ms → 90kHz：ts = ms * 90000 / 1000
//	音频  ms → 采样率：ts = ms * rate / 1000

import (
	"fmt"

	"flow/internal/flv"
	"flow/internal/media"
	"flow/internal/source"
	"flow/internal/stream"
)

// defaultAACRate 音频帧先于序列头到达时使用的采样率。
const defaultAACRate uint32 = 44100

// publishState 一条连接当前的推流状态。只在连接的读循环里读写，不需要锁。
type publishState struct {
	stream     *stream.Stream
	audioRate  uint32 // AAC 采样率（兼容旧字段）
	audioClock uint32 // 当前音频 RTP 时钟：AAC=采样率，MP3=90000，G.711=8000
	audioCodec string // 当前音频编码，用于避免重复登记参数
}

// stopPublish 注销当前推流并关闭帧源（读循环退出、FCUnpublish、
// deleteStream 时调用）。可重复调用。
func (c *Conn) stopPublish() {
	p := c.publish
	if p == nil {
		return
	}
	c.publish = nil
	if c.srv != nil {
		c.srv.metrics.DecPublish()
		c.srv.registry.Unpublish(p.stream.Name, p.stream)
	} else {
		p.stream.Close()
	}
	fmt.Printf("rtmp: unpublished %q\n", p.stream.Name)
}

// handleVideo 处理一条 FLV video tag。
func (c *Conn) handleVideo(m *Message) {
	p := c.publish
	if p == nil {
		return
	}
	vt, err := flv.ParseVideoTag(m.Payload)
	if err != nil {
		fmt.Printf("rtmp: bad video tag: %v\n", err)
		return
	}

	switch vt.PacketType {
	// 序列头：AVC 放 SPS/PPS，HEVC 放 VPS/SPS/PPS
	case flv.AVCSeqHeader:
		switch {
		case media.IsHEVCCodec(vt.Codec):
			vps, sps, pps, err := flv.ParseHEVCDecoderConfigRecord(vt.Config)
			if err != nil {
				fmt.Printf("rtmp: bad hevc config: %v\n", err)
				return
			}
			p.stream.SetVideoParams("H265", vps, sps, pps)
			if src := p.stream.SourceFor("video"); src != nil {
				// 参数集也作为帧写入：ExternalSource 按 NAL 类型缓存参数集，
				// 中途加入的订阅者订阅时会先收到它们。时间戳用序列头消息的
				// 时间戳（通常 0）：回源重连后它已被平移到当前时间线，避免
				// 参数集把下游时间戳拉回 0。
				ts := videoTimestamp(m.Timestamp)
				src.Write(source.Frame{Data: vps, Timestamp: ts})
				src.Write(source.Frame{Data: sps, Timestamp: ts})
				src.Write(source.Frame{Data: pps, Timestamp: ts})
			}
		default:
			sps, pps, err := flv.ParseAVCDecoderConfig(vt.Config)
			if err != nil {
				fmt.Printf("rtmp: bad avc config: %v\n", err)
				return
			}
			p.stream.SetVideoParams("H264", nil, sps, pps)
			if src := p.stream.SourceFor("video"); src != nil {
				// 同 HEVC：参数集时间戳跟随序列头，回源重连后才能保持
				// 下游时间线单调。
				ts := videoTimestamp(m.Timestamp)
				src.Write(source.Frame{Data: sps, Timestamp: ts})
				src.Write(source.Frame{Data: pps, Timestamp: ts})
			}
		}
		// 普通编码帧（H.264 / H.265）
	case flv.AVCNALU:
		src := p.stream.SourceFor("video")
		if src == nil {
			p.stream.EnsureVideo()
			src = p.stream.SourceFor("video")
		}
		// FLV tag 的 timestamp 是 DTS，CompositionTime 是 PTS−DTS（毫秒）。
		// 帧源统一存 PTS（视频 90kHz）和 CTS，拉流端据此还原两者。
		dts := int64(videoTimestamp(m.Timestamp))
		cts := int64(vt.CTS) * 90 // 毫秒 → 90kHz
		pts := dts + cts
		if pts < 0 {
			pts = 0
		}
		for i, nalu := range vt.NALUs {
			src.Write(source.Frame{
				Data:            nalu,
				Timestamp:       uint32(pts),
				CompositionTime: int32(cts),
				Keyframe:        vt.Keyframe,
				AUStart:         i == 0,
				AUEnd:           i == len(vt.NALUs)-1,
			})
		}
	case flv.AVCEndSeq:
		// 流结束标记，忽略；连接断开时由 stopPublish 收尾。
	}
}

// handleAudio 处理一条 FLV audio tag，支持 AAC、MP3 和 G.711。
func (c *Conn) handleAudio(m *Message) {
	p := c.publish
	if p == nil {
		return
	}
	at, err := flv.ParseAudioTag(m.Payload)
	if err != nil {
		fmt.Printf("rtmp: bad audio tag: %v\n", err)
		return
	}

	switch at.SoundFormat {
	case flv.SoundFormatAAC:
		c.handleAACAudio(p, m, at)
	case flv.SoundFormatMP3:
		rate, ch := mp3TagParams(at)
		clock := uint32(90000)
		// 采样率可能晚于首帧才确定；只在切换编码时登记一次。
		if p.audioCodec != stream.AudioMP3 {
			p.audioCodec = stream.AudioMP3
			p.audioClock = clock
			p.stream.SetAudioCodec(stream.AudioMP3, nil, rate, ch, clock)
		}
		p.writeAudioFrame(at.Data, m.Timestamp, clock)
	case flv.SoundFormatALaw, flv.SoundFormatMuLaw:
		codec := stream.AudioPCMA
		if at.SoundFormat == flv.SoundFormatMuLaw {
			codec = stream.AudioPCMU
		}
		ch := uint8(1)
		if at.SoundType == 1 {
			ch = 2
		}
		clock := uint32(8000)
		if p.audioCodec != codec {
			p.audioCodec = codec
			p.audioClock = clock
			p.stream.SetAudioCodec(codec, nil, clock, ch, clock)
		}
		p.writeAudioFrame(at.Data, m.Timestamp, clock)
	}
}

// handleAACAudio 处理 AAC 序列头/裸 AU，保持原有行为。
func (c *Conn) handleAACAudio(p *publishState, m *Message, at *flv.AudioTag) {
	switch at.PacketType {
	// 存放音频编码配置
	case flv.AACSeqHeader:
		rate, ch, err := media.ParseASC(at.Config)
		if err != nil {
			fmt.Printf("rtmp: bad aac config: %v\n", err)
			return
		}
		p.audioRate = uint32(rate)
		p.audioClock = uint32(rate)
		p.audioCodec = stream.AudioAAC
		p.stream.SetAudioParams(at.Config, uint32(rate), ch)
		// AAC原始音频帧
	case flv.AACRaw:
		rate := p.audioRate
		if rate == 0 {
			rate = defaultAACRate
		}
		src := p.stream.SourceFor("audio")
		if src == nil {
			p.stream.EnsureAudio(rate)
			src = p.stream.SourceFor("audio")
		}
		src.Write(source.Frame{
			Data:      at.AU,
			Timestamp: audioTimestamp(m.Timestamp, rate),
			Keyframe:  true,
		})
	}
}

// writeAudioFrame 把非 AAC 音频帧写入共享源，时间戳换算到 RTP 时钟。
func (p *publishState) writeAudioFrame(data []byte, ms, clock uint32) {
	src := p.stream.SourceFor("audio")
	if src == nil {
		p.stream.EnsureAudio(clock)
		src = p.stream.SourceFor("audio")
	}
	src.Write(source.Frame{
		Data:      data,
		Timestamp: audioTimestamp(ms, clock),
		Keyframe:  true,
	})
}

// mp3TagParams 从 MP3 tag 里取采样率/声道：优先解析帧头，
// 解析失败时退回 FLV 的 SoundRate/SoundType 提示值。
func mp3TagParams(at *flv.AudioTag) (uint32, uint8) {
	if info, ok := media.ParseMP3Frame(at.Data); ok {
		return info.SampleRate, info.Channels
	}
	rate := uint32(44100)
	switch at.SoundRate {
	case 0:
		rate = 5512
	case 1:
		rate = 11025
	case 2:
		rate = 22050
	}
	ch := uint8(1)
	if at.SoundType == 1 {
		ch = 2
	}
	return rate, ch
}

// videoTimestamp FLV 毫秒时间戳 → 90kHz RTP 时间戳。
func videoTimestamp(ms uint32) uint32 {
	return uint32(uint64(ms) * 90000 / 1000)
}

// audioTimestamp 毫秒时间戳 → 采样率时钟。
func audioTimestamp(ms, rate uint32) uint32 {
	return uint32(uint64(ms) * uint64(rate) / 1000)
}

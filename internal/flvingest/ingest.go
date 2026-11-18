// Package flvingest 把 FLV 音视频 tag 解成 source.Frame 写进共享流。
//
// RTMP 推流（publish）和回源拉流（RTMP / HTTP-FLV）都复用这套逻辑：
// 输入是 FLV tag 的原始负载和毫秒时间戳，输出是 stream.Stream 上的
// 编码参数与帧。时间戳换算：
//
//	视频  ms → 90kHz：ts = ms * 90000 / 1000
//	音频  ms → 采样率：ts = ms * rate / 1000
//
// 编码参数（AVC 序列头里的 SPS/PPS、AAC 序列头里的 ASC）除了登记到
// stream.Stream，还会作为帧写进 ExternalSource：源内部会缓存 SPS/PPS/IDR，
// 晚加入的 RTSP 拉流端订阅时先补齐参数集再解码，不需要等下一个关键帧。
package flvingest

import (
	"fmt"
	"sync"

	"flow/internal/flv"
	"flow/internal/media"
	"flow/internal/source"
	"flow/internal/stream"
)

// DefaultAACRate 音频帧先于序列头到达时使用的采样率。
const DefaultAACRate uint32 = 44100

// Ingest 一路 FLV 流的注入状态。只在写帧的 goroutine 里读写，不需要锁；
// OnFirstMedia 由内部 sync.Once 保证只触发一次。
type Ingest struct {
	Stream     *stream.Stream
	AudioRate  uint32 // AAC 采样率
	AudioClock uint32 // 当前音频 RTP 时钟：AAC=采样率，MP3=90000，G.711=8000
	AudioCodec string // 当前音频编码，用于避免重复登记参数

	// OnFirstMedia 在第一个成功处理的音视频 tag 写出后调用一次。
	// 回源用它表示「上游已就绪」，此时流上的轨道/参数已经可以查询。
	OnFirstMedia func()

	// Rebase 可选：把上游毫秒时间戳平移到跨重连单调。ctsMs 是视频的
	// PTS−DTS（音频/序列头为 0），调用方据此让重连后的首帧 PTS 落在
	// 上一连接最后一帧之后（B 帧的正 CTS 不会被吞掉）。
	Rebase func(tsMs uint32, ctsMs int32) uint32

	firstOnce sync.Once
}

// NewIngest 为一路流创建注入器。
func NewIngest(st *stream.Stream) *Ingest {
	return &Ingest{Stream: st, AudioRate: DefaultAACRate}
}

// rebase 应用可选的时间戳平移；未设置 Rebase 时原样返回。
func (g *Ingest) rebase(tsMs uint32, ctsMs int32) uint32 {
	if g.Rebase == nil {
		return tsMs
	}
	return g.Rebase(tsMs, ctsMs)
}

// fireFirst 触发首帧回调（只触发一次）。
func (g *Ingest) fireFirst() {
	if g.OnFirstMedia == nil {
		return
	}
	g.firstOnce.Do(g.OnFirstMedia)
}

// Video 处理一条 FLV video tag。
func (g *Ingest) Video(payload []byte, tsMs uint32) {
	if g == nil || g.Stream == nil {
		return
	}
	vt, err := flv.ParseVideoTag(payload)
	if err != nil {
		fmt.Printf("flv ingest: bad video tag: %v\n", err)
		return
	}

	switch vt.PacketType {
	// 序列头：AVC 放 SPS/PPS，HEVC 放 VPS/SPS/PPS
	case flv.AVCSeqHeader:
		switch {
		case media.IsHEVCCodec(vt.Codec):
			vps, sps, pps, err := flv.ParseHEVCDecoderConfigRecord(vt.Config)
			if err != nil {
				fmt.Printf("flv ingest: bad hevc config: %v\n", err)
				return
			}
			g.Stream.SetVideoParams("H265", vps, sps, pps)
			if src := g.Stream.SourceFor("video"); src != nil {
				// 参数集也作为帧写入：ExternalSource 按 NAL 类型缓存参数集，
				// 中途加入的订阅者订阅时会先收到它们。时间戳用序列头消息的
				// 时间戳（通常 0）：回源重连后它已被平移到当前时间线，避免
				// 参数集把下游时间戳拉回 0。
				ts := VideoTimestamp(g.rebase(tsMs, 0))
				src.Write(source.Frame{Data: vps, Timestamp: ts})
				src.Write(source.Frame{Data: sps, Timestamp: ts})
				src.Write(source.Frame{Data: pps, Timestamp: ts})
			}
		default:
			sps, pps, err := flv.ParseAVCDecoderConfig(vt.Config)
			if err != nil {
				fmt.Printf("flv ingest: bad avc config: %v\n", err)
				return
			}
			g.Stream.SetVideoParams("H264", nil, sps, pps)
			if src := g.Stream.SourceFor("video"); src != nil {
				// 同 HEVC：参数集时间戳跟随序列头，回源重连后才能保持
				// 下游时间线单调。
				ts := VideoTimestamp(g.rebase(tsMs, 0))
				src.Write(source.Frame{Data: sps, Timestamp: ts})
				src.Write(source.Frame{Data: pps, Timestamp: ts})
			}
		}
		// 普通编码帧（H.264 / H.265）
	case flv.AVCNALU:
		src := g.Stream.SourceFor("video")
		if src == nil {
			g.Stream.EnsureVideo()
			src = g.Stream.SourceFor("video")
		}
		// FLV tag 的 timestamp 是 DTS，CompositionTime 是 PTS−DTS（毫秒）。
		// 帧源统一存 PTS（视频 90kHz）和 CTS，拉流端据此还原两者。
		// 平移按 PTS 计算，否则带正 CTS 的 B 帧会让重连后的 PTS 回退。
		dts := int64(VideoTimestamp(g.rebase(tsMs, int32(vt.CTS))))
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
		// 流结束标记，忽略；连接断开时由调用方收尾。
		return
	default:
		return
	}
	g.fireFirst()
}

// Audio 处理一条 FLV audio tag，支持 AAC、MP3 和 G.711。
func (g *Ingest) Audio(payload []byte, tsMs uint32) {
	if g == nil || g.Stream == nil {
		return
	}
	at, err := flv.ParseAudioTag(payload)
	if err != nil {
		fmt.Printf("flv ingest: bad audio tag: %v\n", err)
		return
	}

	// 音频没有 CTS，平移量按 PTS=DTS 计算。
	tsMs = g.rebase(tsMs, 0)

	switch at.SoundFormat {
	case flv.SoundFormatAAC:
		if !g.aac(at, tsMs) {
			return
		}
	case flv.SoundFormatMP3:
		rate, ch := mp3TagParams(at)
		clock := uint32(90000)
		// 采样率可能晚于首帧才确定；只在切换编码时登记一次。
		if g.AudioCodec != stream.AudioMP3 {
			g.AudioCodec = stream.AudioMP3
			g.AudioClock = clock
			g.Stream.SetAudioCodec(stream.AudioMP3, nil, rate, ch, clock)
		}
		g.writeAudioFrame(at.Data, tsMs, clock)
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
		if g.AudioCodec != codec {
			g.AudioCodec = codec
			g.AudioClock = clock
			g.Stream.SetAudioCodec(codec, nil, clock, ch, clock)
		}
		g.writeAudioFrame(at.Data, tsMs, clock)
	default:
		return
	}
	g.fireFirst()
}

// aac 处理 AAC 序列头/裸 AU。返回 false 表示 tag 非法、不应触发首帧回调。
func (g *Ingest) aac(at *flv.AudioTag, tsMs uint32) bool {
	switch at.PacketType {
	// 存放音频编码配置
	case flv.AACSeqHeader:
		rate, ch, err := media.ParseASC(at.Config)
		if err != nil {
			fmt.Printf("flv ingest: bad aac config: %v\n", err)
			return false
		}
		g.AudioRate = uint32(rate)
		g.AudioClock = uint32(rate)
		g.AudioCodec = stream.AudioAAC
		g.Stream.SetAudioParams(at.Config, uint32(rate), ch)
		return true
	// AAC原始音频帧
	case flv.AACRaw:
		rate := g.AudioRate
		if rate == 0 {
			rate = DefaultAACRate
		}
		src := g.Stream.SourceFor("audio")
		if src == nil {
			g.Stream.EnsureAudio(rate)
			src = g.Stream.SourceFor("audio")
		}
		if src != nil {
			src.Write(source.Frame{
				Data:      at.AU,
				Timestamp: AudioTimestamp(tsMs, rate),
				Keyframe:  true,
			})
		}
		return true
	}
	return false
}

// writeAudioFrame 把非 AAC 音频帧写入共享源，时间戳换算到 RTP 时钟。
func (g *Ingest) writeAudioFrame(data []byte, ms, clock uint32) {
	src := g.Stream.SourceFor("audio")
	if src == nil {
		g.Stream.EnsureAudio(clock)
		src = g.Stream.SourceFor("audio")
	}
	if src == nil {
		return
	}
	src.Write(source.Frame{
		Data:      data,
		Timestamp: AudioTimestamp(ms, clock),
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

// VideoTimestamp FLV 毫秒时间戳 → 90kHz RTP 时间戳。
func VideoTimestamp(ms uint32) uint32 {
	return uint32(uint64(ms) * 90000 / 1000)
}

// AudioTimestamp 毫秒时间戳 → 采样率时钟。
func AudioTimestamp(ms, rate uint32) uint32 {
	return uint32(uint64(ms) * uint64(rate) / 1000)
}

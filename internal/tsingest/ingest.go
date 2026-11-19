// Package tsingest 把 MPEG-TS 解出的音视频写进注册表里的共享流（P-6）。
//
// SRT 推流（P-5）、UDP TS 和文件 TS 共用这套注入逻辑：输入是
// mpegts.Demuxer 回调给出的访问单元，输出是 stream.Stream 上的编码参数
// 与 source.Frame。参数集（H.264 的 SPS/PPS、H.265 的 VPS/SPS/PPS、AAC
// 的 ASC）除了登记到 stream.Stream，也随帧写进 ExternalSource，晚加入的
// 拉流端订阅时会先收到参数集和最近一个 GOP。
package tsingest

import (
	"bytes"

	"flow/internal/media"
	"flow/internal/mpegts"
	"flow/internal/source"
	"flow/internal/stream"
)

// Ingest 一路 MPEG-TS 的注入状态。只在 demuxer 回调所在 goroutine 里读写，
// 不需要加锁。
type Ingest struct {
	Stream     *stream.Stream
	VideoCodec string

	vps, sps, pps []byte
	haveVideoPS   bool

	asc         []byte
	audioRate   uint32
	audioCh     uint8
	haveAudioPS bool
}

// NewIngest 为一路流创建注入器。
func NewIngest(st *stream.Stream) *Ingest {
	return &Ingest{Stream: st, VideoCodec: "H264"}
}

// OnVideo 处理一个视频访问单元：更新参数集，再把每个 NALU 写进帧源。
func (g *Ingest) OnVideo(au mpegts.VideoAU) {
	if g == nil || g.Stream == nil {
		return
	}
	if au.Codec != "" {
		g.VideoCodec = au.Codec
	}
	g.Stream.EnsureVideo()

	paramsChanged := false
	for _, nalu := range au.NALUs {
		class := media.ClassifyVideoNAL(g.VideoCodec, nalu)
		switch {
		case class.IsVPS && !bytes.Equal(g.vps, nalu):
			g.vps = append([]byte(nil), nalu...)
			paramsChanged = true
		case class.IsSPS && !bytes.Equal(g.sps, nalu):
			g.sps = append([]byte(nil), nalu...)
			paramsChanged = true
		case class.IsPPS && !bytes.Equal(g.pps, nalu):
			g.pps = append([]byte(nil), nalu...)
			paramsChanged = true
		}
	}
	if len(g.sps) > 0 && len(g.pps) > 0 && (paramsChanged || !g.haveVideoPS) {
		g.Stream.SetVideoParams(g.VideoCodec, g.vps, g.sps, g.pps)
		g.haveVideoPS = true
	}

	src := g.Stream.SourceFor("video")
	if src == nil {
		return
	}
	for i, nalu := range au.NALUs {
		src.Write(source.Frame{
			Data:            append([]byte(nil), nalu...),
			Timestamp:       au.PTS,
			CompositionTime: au.CompositionTime(),
			Keyframe:        au.Keyframe,
			AUStart:         i == 0,
			AUEnd:           i == len(au.NALUs)-1,
		})
	}
}

// OnAudio 处理一个 AAC 访问单元。
func (g *Ingest) OnAudio(au mpegts.AudioAU) {
	if g == nil || g.Stream == nil {
		return
	}
	if len(au.ASC) != 2 || au.SampleRate == 0 {
		return
	}
	if !g.haveAudioPS || !bytes.Equal(g.asc, au.ASC) ||
		g.audioRate != au.SampleRate || g.audioCh != au.Channels {
		g.Stream.SetAudioParams(au.ASC, au.SampleRate, au.Channels)
		g.asc = append(g.asc[:0], au.ASC...)
		g.audioRate = au.SampleRate
		g.audioCh = au.Channels
		g.haveAudioPS = true
	}
	src := g.Stream.SourceFor("audio")
	if src == nil {
		return
	}
	src.Write(source.Frame{
		Data:      append([]byte(nil), au.AU...),
		Timestamp: au.Timestamp,
		Keyframe:  true,
	})
}

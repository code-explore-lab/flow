package record

import (
	"bytes"
	"fmt"
	"path/filepath"

	"flow/internal/flv"
	"flow/internal/media"
	"flow/internal/stream"
)

// flvWriter 写标准 FLV 文件：FLV 头 + 序列头 + 音视频 tag。
// FLV 天然 append-only，续录时只需要截断到 .state 记录的字节数再追加。
type flvWriter struct {
	writerBase
	header     []byte
	segTicks   int64
	videoCodec string // 视频编码名，H264/H265
	audioCodec string // 归一化后的音频编码
}

func newFLVWriter(opts Options, name string, video *stream.VideoCodec, audio *stream.AudioCodec, hasVideo, hasAudio bool) (*flvWriter, error) {
	w := &flvWriter{
		writerBase: newWriterBase(opts, name, video, audio, hasVideo, hasAudio),
		segTicks:   segmentTicks(opts.SegmentDuration, hasVideo, audio),
		videoCodec: "H264",
	}
	if hasVideo {
		if video.Codec != "" {
			w.videoCodec = video.Codec
		}
		if len(video.SPS) == 0 || len(video.PPS) == 0 {
			return nil, fmt.Errorf("record: flv video needs SPS/PPS")
		}
		if media.IsHEVCCodec(w.videoCodec) && len(video.VPS) == 0 {
			return nil, fmt.Errorf("record: flv HEVC needs VPS/SPS/PPS")
		}
	}
	if hasAudio {
		w.audioCodec = stream.NormalizeAudioCodec(audio.Codec)
		switch w.audioCodec {
		case stream.AudioAAC, stream.AudioMP3, stream.AudioPCMA, stream.AudioPCMU:
		default:
			return nil, fmt.Errorf("record: flv cannot carry %s audio", audio.Codec)
		}
	}
	header, err := w.buildHeader()
	if err != nil {
		return nil, err
	}
	w.header = header
	if err := w.open(header); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *flvWriter) buildHeader() ([]byte, error) {
	var buf bytes.Buffer
	if err := flv.WriteHeader(&buf, flv.Header{
		HasVideo: w.hasVideo,
		HasAudio: w.hasAudio,
	}); err != nil {
		return nil, fmt.Errorf("record: write flv header: %w", err)
	}
	if w.hasVideo {
		var data []byte
		if media.IsHEVCCodec(w.videoCodec) {
			data = flv.BuildHEVCVideoSequenceHeader(w.video.VPS, w.video.SPS, w.video.PPS)
		} else {
			data = flv.BuildVideoSequenceHeader(w.video.SPS, w.video.PPS)
		}
		if err := flv.WriteTag(&buf, flv.Tag{Type: flv.TagVideo, Data: data}); err != nil {
			return nil, err
		}
	}
	if w.hasAudio && w.audioCodec == stream.AudioAAC {
		rate, channels := audioRateChannels(w.writerBase.audio)
		if err := flv.WriteTag(&buf, flv.Tag{
			Type: flv.TagAudio,
			Data: flv.BuildAudioSequenceHeader(w.writerBase.audio.Config, rate, channels),
		}); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (w *flvWriter) Write(s sample) error {
	switch s.kind {
	case videoTrack:
		if !w.hasVideo {
			return nil
		}
		if s.key && w.vclock.have && w.vclock.peek(s.dts) >= w.segTicks {
			if err := w.rotate(); err != nil {
				return err
			}
		}
		dts := w.vclock.normalize(s.dts)
		w.vclock.advance(dts, s.dur)
		cts := ticksToMS(int64(s.cts), videoTimescale)
		var data []byte
		if media.IsHEVCCodec(w.videoCodec) {
			data = flv.BuildHEVCVideoTagNALUs(s.nalus, s.key, int32(cts))
		} else {
			data = flv.BuildVideoTagNALUs(s.nalus, s.key, int32(cts))
		}
		return w.writeTag(flv.Tag{
			Type:      flv.TagVideo,
			Timestamp: uint32(ticksToMS(dts, videoTimescale)),
			Data:      data,
		})

	case audioTrack:
		if !w.hasAudio {
			return nil
		}
		clock := int64(audioClockRate(w.writerBase.audio))
		if !w.hasVideo && w.aclock.have && w.aclock.peek(s.dts) >= w.segTicks {
			if err := w.rotate(); err != nil {
				return err
			}
		}
		dts := w.aclock.normalize(s.dts)
		w.aclock.advance(dts, s.dur)
		rate, channels := audioRateChannels(w.writerBase.audio)
		return w.writeTag(flv.Tag{
			Type:      flv.TagAudio,
			Timestamp: uint32(ticksToMS(dts, clock)),
			Data:      flv.BuildAudioTagFor(w.audioCodec, s.data, rate, channels),
		})
	}
	return nil
}

func (w *flvWriter) writeTag(tag flv.Tag) error {
	if err := flv.WriteTag(w.file, tag); err != nil {
		return fmt.Errorf("record: write flv tag: %w", err)
	}
	// Tag 头 11 字节 + 数据 + PreviousTagSize(4)。
	w.size += int64(11 + len(tag.Data) + 4)
	return w.maybeState(false)
}

func (w *flvWriter) rotate() error {
	old, oldSize := w.path, w.size
	if err := w.writerBase.rotate(w.header); err != nil {
		return err
	}
	w.opts.logf("record[%s]: segment %s complete (%d bytes)", w.name, filepath.Base(old), oldSize)
	return nil
}

func (w *flvWriter) Close() error { return w.closeFile(true) }

func audioRateChannels(ap *stream.AudioCodec) (uint32, uint8) {
	if ap == nil {
		return 44100, 2
	}
	rate := ap.SampleRate
	if rate == 0 {
		rate = stream.DefaultAudioSampleRate(ap.Codec)
	}
	channels := ap.Channels
	if channels == 0 {
		channels = 2
	}
	return rate, channels
}

// ticksToMS 把任意时间基的 ticks 换算成毫秒（四舍五入，负数向零取整）。
func ticksToMS(ticks, timescale int64) int64 {
	if timescale <= 0 {
		return 0
	}
	if ticks >= 0 {
		return (ticks*1000 + timescale/2) / timescale
	}
	return (ticks*1000 - timescale/2) / timescale
}

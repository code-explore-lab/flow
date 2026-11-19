package record

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"flow/internal/fmp4"
	"flow/internal/media"
	"flow/internal/stream"
)

// mp4Writer 写 fMP4（CMAF）：文件头是 ftyp+moov，之后按关键帧追加
// moof+mdat。每个 fragment 只包含一个 GOP，seek/播放器解析都简单；
// 轮转时直接开一个新文件，每个文件都带完整的 init。
type mp4Writer struct {
	writerBase
	init      []byte
	pv, pa    []fmp4.Sample
	segTicks  int64
	lastFlush int64
}

func newMP4Writer(opts Options, name string, video *stream.VideoCodec, audio *stream.AudioCodec, hasVideo, hasAudio bool) (*mp4Writer, error) {
	if hasVideo {
		codec := video.Codec
		if codec == "" {
			codec = "H264"
		}
		if !strings.EqualFold(codec, "H264") {
			return nil, fmt.Errorf("record: mp4 supports H.264 only (got %s); use -record-format flv for HEVC", codec)
		}
	}
	if hasAudio {
		if c := stream.NormalizeAudioCodec(audio.Codec); c != stream.AudioAAC {
			return nil, fmt.Errorf("record: mp4 supports AAC audio only (got %s)", audio.Codec)
		}
	}
	cfg := fmp4.Config{}
	if hasVideo {
		width, height := spsDimensions(video.SPS)
		cfg.Video = &fmp4.VideoConfig{
			Timescale: videoTimescale,
			SPS:       append([]byte(nil), video.SPS...),
			PPS:       append([]byte(nil), video.PPS...),
			Width:     width,
			Height:    height,
		}
	}
	if hasAudio {
		rate := audio.SampleRate
		if rate == 0 {
			rate = stream.DefaultAudioSampleRate(audio.Codec)
		}
		cfg.Audio = &fmp4.AudioConfig{
			Timescale:  audioClockRate(audio),
			ASC:        append([]byte(nil), audio.Config...),
			SampleRate: rate,
			Channels:   audio.Channels,
		}
	}
	init, err := fmp4.Init(cfg)
	if err != nil {
		return nil, fmt.Errorf("record: build mp4 init: %w", err)
	}
	w := &mp4Writer{
		writerBase: newWriterBase(opts, name, video, audio, hasVideo, hasAudio),
		init:       init,
	}
	w.segTicks = segmentTicks(opts.SegmentDuration, hasVideo, audio)
	if err := w.open(init); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *mp4Writer) Write(s sample) error {
	if s.kind == videoTrack {
		if !w.hasVideo {
			return nil
		}
		// 轮转判断要在 normalize 之前：当前关键帧要进新文件。
		if s.key && w.vclock.have && w.vclock.peek(s.dts) >= w.segTicks {
			if err := w.rotate(); err != nil {
				return err
			}
		}
		// 关键帧开启新 fragment，先把上一个 GOP 连同缓冲的音频写出去。
		if s.key && len(w.pv) > 0 {
			if err := w.flushFragment(); err != nil {
				return err
			}
		}
		dts := w.vclock.normalize(s.dts)
		w.vclock.advance(dts, s.dur)
		w.pv = append(w.pv, fmp4.Sample{
			Data:     buildAVCC(s.nalus),
			DTS:      dts,
			Duration: s.dur,
			CTS:      s.cts,
			Keyframe: s.key,
		})
		return nil
	}

	if !w.hasAudio {
		return nil
	}
	// 纯音频没有关键帧，按时间轮转。
	if !w.hasVideo && w.aclock.have && w.aclock.peek(s.dts) >= w.segTicks {
		if err := w.rotate(); err != nil {
			return err
		}
	}
	dts := w.aclock.normalize(s.dts)
	w.aclock.advance(dts, s.dur)
	w.pa = append(w.pa, fmp4.Sample{
		Data:     s.data,
		DTS:      dts,
		Duration: s.dur,
		Keyframe: true,
	})
	if !w.hasVideo {
		// 音频-only：最多每 2 秒落一个 fragment，崩溃时丢失有界。
		flushEvery := w.segTicks / 2
		if limit := int64(audioClockRate(w.writerBase.audio)) * 2; flushEvery > limit {
			flushEvery = limit
		}
		if flushEvery <= 0 {
			flushEvery = 1
		}
		if dts-w.lastFlush >= flushEvery {
			if err := w.flushFragment(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *mp4Writer) flushFragment() error {
	if len(w.pv) == 0 && len(w.pa) == 0 {
		return nil
	}
	frags := make([]fmp4.TrackFragment, 0, 2)
	if len(w.pv) > 0 {
		frags = append(frags, fmp4.TrackFragment{TrackID: 1, Samples: w.pv, WriteCTS: true})
	}
	if len(w.pa) > 0 {
		frags = append(frags, fmp4.TrackFragment{TrackID: 2, Samples: w.pa, WriteCTS: false})
	}
	data, err := fmp4.Fragment(w.nextFrag, frags...)
	if err != nil {
		return fmt.Errorf("record: build mp4 fragment: %w", err)
	}
	if _, err := w.file.Write(data); err != nil {
		return fmt.Errorf("record: write %s: %w", w.path, err)
	}
	w.size += int64(len(data))
	w.nextFrag++
	w.pv = w.pv[:0]
	w.pa = w.pa[:0]
	if !w.hasVideo {
		w.lastFlush = w.aclock.next
	}
	// MP4 的 fragment 是天然的一致性边界，每个 fragment 都同步状态；
	// 崩溃后最多丢掉正在拼装的那一个 GOP。
	return w.writeState()
}

func (w *mp4Writer) rotate() error {
	if err := w.flushFragment(); err != nil {
		return err
	}
	old, oldSize := w.path, w.size
	if err := w.writerBase.rotate(w.init); err != nil {
		return err
	}
	w.pv = nil
	w.pa = nil
	w.lastFlush = 0
	w.opts.logf("record[%s]: segment %s complete (%d bytes)", w.name, filepath.Base(old), oldSize)
	return nil
}

func (w *mp4Writer) Close() error {
	if err := w.flushFragment(); err != nil {
		w.closeFile(false)
		return err
	}
	return w.closeFile(true)
}

// buildAVCC 把一个访问单元写成 MP4 的样本：AUD + 4 字节长度前缀的 NALU。
func buildAVCC(nalus [][]byte) []byte {
	out := []byte{0, 0, 0, 2, 0x09, 0xF0}
	for _, nalu := range nalus {
		if len(nalu) == 0 {
			continue
		}
		out = append(out,
			byte(len(nalu)>>24), byte(len(nalu)>>16), byte(len(nalu)>>8), byte(len(nalu)))
		out = append(out, nalu...)
	}
	return out
}

func spsDimensions(sps []byte) (int, int) {
	info, err := media.ParseSPS(sps)
	if err != nil {
		return 0, 0
	}
	return info.Width, info.Height
}

// segmentTicks 把轮转时长换算成“用于判断时长的那条轨”的时间基。
func segmentTicks(seg time.Duration, hasVideo bool, audio *stream.AudioCodec) int64 {
	if seg <= 0 {
		seg = 30 * time.Second
	}
	ts := int64(videoTimescale)
	if !hasVideo {
		ts = int64(audioClockRate(audio))
	}
	ticks := int64(seg) * ts / int64(time.Second)
	if ticks <= 0 {
		ticks = 1
	}
	return ticks
}

package record

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"flow/internal/mpegts"
	"flow/internal/stream"
)

// tsWriter 写 MPEG-TS。封装与 PSI 交给 internal/mpegts，这里只负责时间轴、
// 轮转和续录。TS 是 append-only 的字节流，续录时截断到状态文件记录的
// 字节数后继续写即可。
type tsWriter struct {
	writerBase
	mux      *mpegts.Muxer
	segTicks int64
}

func newTSWriter(opts Options, name string, video *stream.VideoCodec, audio *stream.AudioCodec, hasVideo, hasAudio bool) (*tsWriter, error) {
	if hasVideo {
		codec := video.Codec
		if codec == "" {
			codec = "H264"
		}
		if !strings.EqualFold(codec, "H264") {
			return nil, fmt.Errorf("record: ts supports H.264 only (got %s); use -record-format flv for HEVC", codec)
		}
	}
	if hasAudio {
		if c := stream.NormalizeAudioCodec(audio.Codec); c != stream.AudioAAC {
			return nil, fmt.Errorf("record: ts supports AAC audio only (got %s)", audio.Codec)
		}
	}
	w := &tsWriter{
		writerBase: newWriterBase(opts, name, video, audio, hasVideo, hasAudio),
		segTicks:   segmentTicks(opts.SegmentDuration, hasVideo, audio),
	}
	if err := w.open(nil); err != nil {
		return nil, err
	}
	if err := w.newMux(); err != nil {
		w.closeFile(false)
		return nil, err
	}
	return w, nil
}

func (w *tsWriter) newMux() error {
	m := mpegts.NewMuxer(countingWriter{w: w.file, n: &w.size})
	if w.hasVideo {
		m.SetVideoParams(w.video.SPS, w.video.PPS)
	}
	if w.hasAudio {
		if err := m.SetAudioParams(w.audio.Config); err != nil {
			return fmt.Errorf("record: ts audio params: %w", err)
		}
	}
	w.mux = m
	return nil
}

func (w *tsWriter) Write(s sample) error {
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
		// 给文件时钟留 1 秒余量：带 B 帧时首个样本的 PTS 可能小于 DTS，
		// 加偏移后不会出现负的 33 位 PTS。音频用同一个 1 秒偏移保持同步。
		pts := dts + int64(s.cts) + videoTimescale
		if err := w.mux.WriteVideo(s.nalus, uint32(pts), uint32(dts+videoTimescale)); err != nil {
			return fmt.Errorf("record: ts video: %w", err)
		}
		w.vclock.advance(dts, s.dur)
		return w.maybeState(false)

	case audioTrack:
		if !w.hasAudio {
			return nil
		}
		if !w.hasVideo && w.aclock.have && w.aclock.peek(s.dts) >= w.segTicks {
			if err := w.rotate(); err != nil {
				return err
			}
		}
		dts := w.aclock.normalize(s.dts)
		offset := int64(audioClockRate(w.writerBase.audio))
		if err := w.mux.WriteAudio(s.data, uint32(dts+offset)); err != nil {
			return fmt.Errorf("record: ts audio: %w", err)
		}
		w.aclock.advance(dts, s.dur)
		return w.maybeState(false)
	}
	return nil
}

func (w *tsWriter) rotate() error {
	if err := w.mux.Flush(); err != nil {
		return fmt.Errorf("record: ts flush: %w", err)
	}
	old, oldSize := w.path, w.size
	if err := w.writerBase.rotate(nil); err != nil {
		return err
	}
	if err := w.newMux(); err != nil {
		return err
	}
	w.opts.logf("record[%s]: segment %s complete (%d bytes)", w.name, filepath.Base(old), oldSize)
	return nil
}

func (w *tsWriter) Close() error {
	if w.mux != nil {
		if err := w.mux.Flush(); err != nil {
			w.closeFile(false)
			return err
		}
	}
	return w.closeFile(true)
}

// countingWriter 让 mpegts 直接写文件，同时维护 writerBase.size，
// 供 .state 和配额使用。
type countingWriter struct {
	w io.Writer
	n *int64
}

func (c countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	*c.n += int64(n)
	return n, err
}

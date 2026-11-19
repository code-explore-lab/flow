package srt

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"

	gosrt "github.com/datarhei/gosrt"

	"flow/internal/media"
	"flow/internal/mpegts"
	"flow/internal/source"
	"flow/internal/stream"
)

// servePublish 读取 SRT 上的 MPEG-TS，解出 H.264/AAC 帧写进注册表。
func (s *Server) servePublish(ctx context.Context, conn gosrt.Conn, name string) {
	st := &stream.Stream{}
	published, err := s.registry.Publish(name, st)
	if err != nil {
		log.Printf("srt: publish %q rejected: %v", name, err)
		return
	}
	defer s.registry.Unpublish(name, published)

	var (
		vps, sps    []byte
		pps         []byte
		videoCodec  = "H264"
		audioASC    []byte
		audioRate   uint32
		audioCh     uint8
		haveVideoPS bool
		haveAudioPS bool
	)

	d := mpegts.NewDemuxer()
	d.OnError = func(err error) { log.Printf("srt: publish %q: %v", name, err) }

	d.OnVideo = func(au mpegts.VideoAU) {
		if au.Codec != "" {
			videoCodec = au.Codec
		}
		st.EnsureVideo()

		paramsChanged := false
		for _, nalu := range au.NALUs {
			class := media.ClassifyVideoNAL(videoCodec, nalu)
			switch {
			case class.IsVPS && !bytes.Equal(vps, nalu):
				vps = append([]byte(nil), nalu...)
				paramsChanged = true
			case class.IsSPS && !bytes.Equal(sps, nalu):
				sps = append([]byte(nil), nalu...)
				paramsChanged = true
			case class.IsPPS && !bytes.Equal(pps, nalu):
				pps = append([]byte(nil), nalu...)
				paramsChanged = true
			}
		}
		if len(sps) > 0 && len(pps) > 0 && (paramsChanged || !haveVideoPS) {
			st.SetVideoParams(videoCodec, vps, sps, pps)
			haveVideoPS = true
		}

		src := st.SourceFor("video")
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

	d.OnAudio = func(au mpegts.AudioAU) {
		if len(au.ASC) != 2 || au.SampleRate == 0 {
			return
		}
		if !haveAudioPS || !bytes.Equal(audioASC, au.ASC) ||
			audioRate != au.SampleRate || audioCh != au.Channels {
			st.SetAudioParams(au.ASC, au.SampleRate, au.Channels)
			audioASC = append(audioASC[:0], au.ASC...)
			audioRate = au.SampleRate
			audioCh = au.Channels
			haveAudioPS = true
		}
		src := st.SourceFor("audio")
		if src == nil {
			return
		}
		src.Write(source.Frame{
			Data:      append([]byte(nil), au.AU...),
			Timestamp: au.Timestamp,
			Keyframe:  true,
		})
	}

	buf := make([]byte, 1316)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			d.Write(buf[:n])
		}
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, io.EOF) {
				log.Printf("srt: publish %q read: %v", name, err)
			}
			break
		}
	}
	d.Flush()
}

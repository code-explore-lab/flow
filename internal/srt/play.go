package srt

import (
	"context"
	"log"
	"sync"
	"time"

	gosrt "github.com/datarhei/gosrt"

	"flow/internal/media"
	"flow/internal/mpegts"
	"flow/internal/source"
	"flow/internal/stream"
)

// servePlay 从注册表订阅音视频轨，封装成 MPEG-TS 后写到 SRT 连接。
func (s *Server) servePlay(ctx context.Context, conn gosrt.Conn, name string) {
	st := s.registry.Get(name)
	if st == nil {
		return
	}

	video, audio := s.waitPlayParams(ctx, name, st)
	if video == nil && audio == nil {
		log.Printf("srt: read %q: no playable track (need H.264 video or AAC audio)", name)
		return
	}
	if video != nil && media.IsHEVCCodec(video.Codec) {
		log.Printf("srt: read %q: H.265 over SRT is not supported", name)
		return
	}

	mux := mpegts.NewMuxer(conn)
	if video != nil {
		mux.SetVideoParams(video.SPS, video.PPS)
	}
	if audio != nil {
		if err := mux.SetAudioParams(audio.Config); err != nil {
			log.Printf("srt: read %q: audio disabled: %v", name, err)
			audio = nil
		}
	}
	// 先把 PAT/PMT 发给播放器，避免首帧到达前探测超时。
	if err := mux.WritePSI(); err != nil {
		return
	}

	playCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	if video != nil {
		if src := st.SourceFor("video"); src != nil {
			rd, unsubscribe := src.Subscribe()
			defer unsubscribe()
			wg.Add(1)
			go func() {
				defer wg.Done()
				pumpVideo(playCtx, cancel, rd, mux)
			}()
		}
	}
	if audio != nil {
		if src := st.SourceFor("audio"); src != nil {
			rd, unsubscribe := src.Subscribe()
			defer unsubscribe()
			wg.Add(1)
			go func() {
				defer wg.Done()
				pumpAudio(playCtx, cancel, rd, mux)
			}()
		}
	}
	wg.Wait()
	_ = mux.Flush()
}

// waitPlayParams 等待推流端把音视频参数集写进注册表。推流端可能在
// 拉流端之后才完成握手，所以这里做有限等待而不是立即失败。
func (s *Server) waitPlayParams(ctx context.Context, name string, st *stream.Stream) (*stream.VideoCodec, *stream.AudioCodec) {
	deadline := time.Now().Add(waitForTracks)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	for {
		if s.registry.Get(name) != st {
			return nil, nil // 流被替换或摘除
		}

		var video *stream.VideoCodec
		if v, ok := st.VideoParams(); ok {
			video = v
		}
		var audio *stream.AudioCodec
		audioTrack := st.AudioTrack()
		if a, ok := st.AudioParams(); ok {
			if a.Codec == stream.AudioAAC && len(a.Config) >= 2 {
				audio = a
			} else if a.Codec != stream.AudioAAC {
				// 非 AAC 音频无法写进 ADTS，按「已确定、但不可用」处理。
				audioTrack = nil
			}
		}
		videoTrack := st.VideoTrack()

		videoSettled := video != nil || videoTrack == nil
		audioSettled := audio != nil || audioTrack == nil
		if videoSettled && audioSettled && (video != nil || audio != nil) {
			return video, audio
		}
		if time.Now().After(deadline) {
			return nil, nil
		}

		select {
		case <-ctx.Done():
			return nil, nil
		case <-ticker.C:
		}
	}
}

// pumpVideo 把逐 NALU 的帧重新拼成访问单元，再交给 TS 封装器。
func pumpVideo(ctx context.Context, cancel context.CancelFunc, rd *source.Reader[source.Frame], mux *mpegts.Muxer) {
	defer cancel()

	var (
		cur [][]byte
		pts uint32
		cts int32
	)
	flush := func() bool {
		if len(cur) == 0 {
			return true
		}
		dts := uint32(int64(pts) - int64(cts))
		err := mux.WriteVideo(cur, pts, dts)
		cur = nil
		return err == nil
	}

	for {
		f, ok := rd.Next(ctx.Done())
		if !ok {
			flush()
			return
		}
		if len(f.Data) == 0 {
			continue
		}
		if len(cur) > 0 && (f.AUStart || f.Timestamp != pts) {
			if !flush() {
				return
			}
		}
		if len(cur) == 0 {
			pts = f.Timestamp
			cts = f.CompositionTime
		}
		cur = append(cur, f.Data)
		if f.AUEnd {
			if !flush() {
				return
			}
		}
	}
}

// pumpAudio 直接把 AAC AU 交给 TS 封装器。
func pumpAudio(ctx context.Context, cancel context.CancelFunc, rd *source.Reader[source.Frame], mux *mpegts.Muxer) {
	defer cancel()
	for {
		f, ok := rd.Next(ctx.Done())
		if !ok {
			return
		}
		if len(f.Data) == 0 {
			continue
		}
		if err := mux.WriteAudio(f.Data, f.Timestamp); err != nil {
			return
		}
	}
}

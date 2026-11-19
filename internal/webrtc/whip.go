package webrtc

import (
	"bytes"
	"context"
	"log"
	"net/http"

	pion "github.com/pion/webrtc/v4"

	"flow/internal/media"
	"flow/internal/rtp"
	"flow/internal/source"
	"flow/internal/stream"
)

// handleWHIP 接收浏览器/OBS 的 WHIP 推流：SDP offer → answer，
// 然后把解包后的 H.264/Opus 帧写进共享注册表。
func (s *Server) handleWHIP(w http.ResponseWriter, r *http.Request, name string) {
	offer, err := readOffer(r)
	if err != nil {
		http.Error(w, "bad offer: "+err.Error(), http.StatusBadRequest)
		return
	}

	st := &stream.Stream{}
	published, err := s.registry.Publish(name, st)
	if err != nil {
		http.Error(w, "stream already published", http.StatusConflict)
		return
	}

	pc, err := s.api.NewPeerConnection(s.pcConfig())
	if err != nil {
		s.registry.Unpublish(name, published)
		http.Error(w, "create peer connection: "+err.Error(), http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	ok := false
	defer func() {
		// 协商失败或返回前客户端断开：撤销这次发布，避免留下空流。
		if !ok {
			cancel()
			_ = pc.Close()
			s.registry.Unpublish(name, published)
		}
	}()

	pc.OnTrack(func(track *pion.TrackRemote, _ *pion.RTPReceiver) {
		switch track.Kind() {
		case pion.RTPCodecTypeVideo:
			go s.ingestVideo(ctx, published, track)
		case pion.RTPCodecTypeAudio:
			go s.ingestAudio(ctx, published, track)
		}
	})
	watchConnection(pc, cancel, nil)

	answer, err := s.answerSDP(pc, offer)
	if err != nil {
		http.Error(w, "negotiate: "+err.Error(), http.StatusBadRequest)
		return
	}

	sess := &session{
		id:     newSessionID(),
		name:   name,
		kind:   kindWHIP,
		pc:     pc,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go func() {
		<-ctx.Done()
		close(sess.done)
	}()
	s.startSession(sess, func() {
		s.registry.Unpublish(name, published)
	})

	ok = true
	writeAnswer(w, r, location(r, kindWHIP, name, sess.id), sess.id, answer)
	log.Printf("webrtc: whip publish %q session %s accepted", name, sess.id)
}

// ingestVideo 把 H.264 RTP 还原成 NALU，按 RTP 时间戳聚合成访问单元后
// 写入注册表。时间戳以首帧为基准归零，避免浏览器随机的 RTP 起点把
// RTMP/HTTP-FLV 的下游时间戳推到很大的值。
func (s *Server) ingestVideo(ctx context.Context, st *stream.Stream, track *pion.TrackRemote) {
	trackSrc := st.EnsureVideo()
	src := trackSrc.Src
	if src == nil {
		return
	}

	var (
		depack  rtp.H264Depacketizer
		base    uint32
		baseSet bool
		sps     []byte
		pps     []byte
		auTS    uint32
		auOpen  bool
		auKey   bool
		au      [][]byte
	)

	flush := func() {
		if !auOpen {
			return
		}
		for i, nalu := range au {
			src.Write(source.Frame{
				Data:      nalu,
				Timestamp: auTS,
				Keyframe:  auKey,
				AUStart:   i == 0,
				AUEnd:     i == len(au)-1,
			})
		}
		au = au[:0]
		auOpen = false
		auKey = false
	}
	defer flush()

	for {
		if ctx.Err() != nil {
			return
		}
		pkt, _, err := track.ReadRTP()
		if err != nil {
			return
		}
		if !baseSet {
			base = pkt.Timestamp
			baseSet = true
		}
		ts := pkt.Timestamp - base

		nalus, err := depack.Depacketize(toInternalRTP(pkt))
		if err != nil {
			continue
		}
		for _, nalu := range nalus {
			class := media.ClassifyVideoNAL("H264", nalu)
			switch {
			case class.IsSPS && !bytes.Equal(sps, nalu):
				sps = append([]byte(nil), nalu...)
				if len(pps) > 0 {
					st.SetVideoParams("H264", nil, sps, pps)
				}
			case class.IsPPS && !bytes.Equal(pps, nalu):
				pps = append([]byte(nil), nalu...)
				if len(sps) > 0 {
					st.SetVideoParams("H264", nil, sps, pps)
				}
			}
			if class.IsIDR {
				auKey = true
			}

			// 同一个 RTP 时间戳属于一个访问单元；时间戳变化或 marker
			// 置位都表示这一帧结束。
			if auOpen && ts != auTS {
				flush()
			}
			if !auOpen {
				auOpen = true
				auTS = ts
			}
			au = append(au, append([]byte(nil), nalu...))
		}
		if pkt.Marker {
			flush()
		}
	}
}

// ingestAudio 把 Opus RTP 负载直接作为一帧写入注册表。Opus 没有分片，
// 一个 RTP 包就是一个完整的编码帧。
func (s *Server) ingestAudio(ctx context.Context, st *stream.Stream, track *pion.TrackRemote) {
	channels := uint8(track.Codec().Channels)
	if channels == 0 {
		channels = 2
	}
	st.SetAudioCodec(stream.AudioOpus, nil, 48000, channels, 48000)
	src := st.EnsureAudio(48000).Src
	if src == nil {
		return
	}

	var (
		base    uint32
		baseSet bool
	)
	for {
		if ctx.Err() != nil {
			return
		}
		pkt, _, err := track.ReadRTP()
		if err != nil {
			return
		}
		if len(pkt.Payload) == 0 {
			continue
		}
		if !baseSet {
			base = pkt.Timestamp
			baseSet = true
		}
		src.Write(source.Frame{
			Data:      append([]byte(nil), pkt.Payload...),
			Timestamp: pkt.Timestamp - base,
			Keyframe:  true,
			AUStart:   true,
			AUEnd:     true,
		})
	}
}

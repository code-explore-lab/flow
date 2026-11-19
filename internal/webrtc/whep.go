package webrtc

import (
	"context"
	"log"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	pion "github.com/pion/webrtc/v4"

	"flow/internal/rtp"
	"flow/internal/source"
	"flow/internal/stream"
)

const (
	// playConnectTimeout 是等待 ICE/DTLS 建连的上限。建连前写出的包会被
	// 静默丢弃，所以等 connected 再订阅，保证预热的 GOP 能真正送达。
	playConnectTimeout = 10 * time.Second
	// playAudioWait 是等待音频编码参数的时间；超过后按“源没有可用音频”处理。
	playAudioWait = 2 * time.Second
	// playTrackWait 是等待推流端建出视频轨的时间。WHIP 的 OnTrack 可能在
	// ICE/DTLS 完成之后才触发，WHEP 客户端可能先于它连上来。
	playTrackWait = 5 * time.Second
	// webrtcVideoMTU 留出 SRTP/DTLS 开销，避免 IP 分片。
	webrtcVideoMTU = 1200
)

// handleWHEP 向浏览器提供 WHEP 拉流：解析 offer，按注册表里的编码
// 附加 H.264/Opus 发送轨，然后持续把订阅到的帧打成 RTP 发出。
func (s *Server) handleWHEP(w http.ResponseWriter, r *http.Request, name string) {
	offer, err := readOffer(r)
	if err != nil {
		http.Error(w, "bad offer: "+err.Error(), http.StatusBadRequest)
		return
	}

	st := s.registry.Get(name)
	if st == nil && s.origin != nil {
		st = s.origin.Ensure(r.Context(), name)
	}
	if st == nil {
		http.Error(w, "stream not found", http.StatusNotFound)
		return
	}

	hasVideo := offerHasMedia(offer.SDP, "video") && strings.Contains(strings.ToUpper(offer.SDP), "H264")
	hasAudio := offerHasMedia(offer.SDP, "audio") && strings.Contains(strings.ToUpper(offer.SDP), "OPUS")
	if !hasVideo && !hasAudio {
		http.Error(w, "offer has no supported H.264/Opus media", http.StatusUnsupportedMediaType)
		return
	}

	pc, err := s.api.NewPeerConnection(s.pcConfig())
	if err != nil {
		http.Error(w, "create peer connection: "+err.Error(), http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	ok := false
	defer func() {
		if !ok {
			cancel()
			_ = pc.Close()
		}
	}()

	var videoTrack, audioTrack *pion.TrackLocalStaticRTP
	if hasVideo {
		videoTrack, err = pion.NewTrackLocalStaticRTP(pion.RTPCodecCapability{
			MimeType:  pion.MimeTypeH264,
			ClockRate: 90000,
		}, "video", name)
		if err != nil {
			http.Error(w, "create video track: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if _, err := pc.AddTrack(videoTrack); err != nil {
			http.Error(w, "add video track: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if hasAudio {
		audioTrack, err = pion.NewTrackLocalStaticRTP(pion.RTPCodecCapability{
			MimeType:  pion.MimeTypeOpus,
			ClockRate: 48000,
			Channels:  2,
		}, "audio", name)
		if err != nil {
			http.Error(w, "create audio track: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if _, err := pc.AddTrack(audioTrack); err != nil {
			http.Error(w, "add audio track: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	connected := make(chan struct{})
	var connectedOnce sync.Once
	watchConnection(pc, cancel, func() {
		connectedOnce.Do(func() { close(connected) })
	})

	answer, err := s.answerSDP(pc, offer)
	if err != nil {
		http.Error(w, "negotiate: "+err.Error(), http.StatusBadRequest)
		return
	}

	sess := &session{
		id:     newSessionID(),
		name:   name,
		kind:   kindWHEP,
		pc:     pc,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go func() {
		<-ctx.Done()
		close(sess.done)
	}()
	s.startSession(sess, nil)

	writeAnswer(w, r, location(r, kindWHEP, name, sess.id), sess.id, answer)
	log.Printf("webrtc: whep play %q session %s accepted", name, sess.id)

	ok = true
	if videoTrack != nil {
		go s.playVideo(ctx, cancel, connected, st, videoTrack)
	}
	if audioTrack != nil {
		go s.playAudio(ctx, cancel, connected, st, audioTrack)
	}
}

// playVideo 从注册表订阅视频帧，复用 internal/rtp 的 H.264 封包器，
// 以访问单元为单位打 RTP 发给浏览器。
func (s *Server) playVideo(ctx context.Context, cancel context.CancelFunc, connected <-chan struct{}, st *stream.Stream, track *pion.TrackLocalStaticRTP) {
	if !waitConnected(ctx, connected) {
		return
	}
	src := waitVideoSource(ctx, st)
	if src == nil {
		return
	}
	rd, unsubscribe := src.Subscribe()
	defer unsubscribe()

	packer := rtp.NewH264Packer(webrtcVideoMTU)
	seq := uint16(rand.Uint32())

	var (
		au     [][]byte
		auTS   uint32
		auOpen bool
	)
	send := func() bool {
		if !auOpen || len(au) == 0 {
			au = au[:0]
			auOpen = false
			return true
		}
		pkts, err := packer.PackFrame(au)
		if err != nil {
			au = au[:0]
			auOpen = false
			return true
		}
		for _, pkt := range pkts {
			pkt.Header.Seq = seq
			seq++
			pkt.Header.Timestamp = auTS
			if err := track.WriteRTP(toPionRTP(pkt)); err != nil {
				log.Printf("webrtc: whep %q write video: %v", st.Name, err)
				au = au[:0]
				auOpen = false
				return false
			}
		}
		au = au[:0]
		auOpen = false
		return true
	}

	for {
		f, ok := rd.Next(ctx.Done())
		if !ok {
			cancel()
			return
		}
		if auOpen && f.Timestamp != auTS {
			if !send() {
				cancel()
				return
			}
		}
		if !auOpen {
			auOpen = true
			auTS = f.Timestamp
		}
		au = append(au, f.Data)
		if f.AUEnd {
			if !send() {
				cancel()
				return
			}
		}
	}
}

// playAudio 订阅音频帧并通过 Opus 发送。WebRTC 浏览器只协商 Opus；
// 源是 AAC（摄像头 RTSP 常见）时本条腿保持静音，视频不受影响。
func (s *Server) playAudio(ctx context.Context, cancel context.CancelFunc, connected <-chan struct{}, st *stream.Stream, track *pion.TrackLocalStaticRTP) {
	if !waitConnected(ctx, connected) {
		return
	}
	if !waitOpus(ctx, st) {
		return
	}
	src := st.SourceFor("audio")
	if src == nil {
		return
	}
	rd, unsubscribe := src.Subscribe()
	defer unsubscribe()

	packer := rtp.NewOpusPacker()
	seq := uint16(rand.Uint32())
	for {
		f, ok := rd.Next(ctx.Done())
		if !ok {
			cancel()
			return
		}
		pkts, err := packer.Pack(f.Data)
		if err != nil {
			continue
		}
		for _, pkt := range pkts {
			pkt.Header.Seq = seq
			seq++
			pkt.Header.Timestamp = f.Timestamp
			if err := track.WriteRTP(toPionRTP(pkt)); err != nil {
				log.Printf("webrtc: whep %q write audio: %v", st.Name, err)
				cancel()
				return
			}
		}
	}
}

// waitVideoSource 等待推流端把视频轨登记进 Stream。WHIP 的 OnTrack 可能
// 晚于 WHEP 建连，这里做有限等待而不是立即放弃。
func waitVideoSource(ctx context.Context, st *stream.Stream) *source.ExternalSource {
	deadline := time.Now().Add(playTrackWait)
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	for {
		if src := st.SourceFor("video"); src != nil {
			return src
		}
		if time.Now().After(deadline) {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			timer.Reset(50 * time.Millisecond)
		}
	}
}

func waitConnected(ctx context.Context, connected <-chan struct{}) bool {
	select {
	case <-ctx.Done():
		return false
	case <-connected:
		return true
	case <-time.After(playConnectTimeout):
		return false
	}
}

// waitOpus 等到音频编码参数就绪。只有 Opus 能原样送进 WebRTC；
// AAC/PCMA/PCMU/MP3 源返回 false（这条腿不发送）。
func waitOpus(ctx context.Context, st *stream.Stream) bool {
	deadline := time.Now().Add(playAudioWait)
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	for {
		if p, ok := st.AudioParams(); ok {
			return p.Codec == stream.AudioOpus
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			timer.Reset(50 * time.Millisecond)
		}
	}
}

// offerHasMedia 判断 offer 里是否有指定类型的 m= 行。
func offerHasMedia(sdp, kind string) bool {
	prefix := "m=" + kind + " "
	for _, line := range strings.Split(sdp, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), prefix) {
			return true
		}
	}
	return false
}

package webrtc

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pion "github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"

	"flow/internal/rtp"
	"flow/internal/source"
	"flow/internal/stream"
)

var (
	testSPS = []byte{0x67, 0x42, 0x00, 0x1E, 0x95, 0xA8, 0x28, 0x0F, 0x00, 0x44, 0xFC, 0xB8, 0x08, 0x80}
	testPPS = []byte{0x68, 0xCE, 0x06, 0xE2}
	testIDR = []byte{0x65, 0x80, 0x01, 0x02, 0x03}
	testP   = []byte{0x41, 0x80, 0x04, 0x05}
)

func newTestServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	srv := New(stream.NewRegistry())
	srv.SetIncludeLoopback(true)
	srv.SetGatherTimeout(5 * time.Second)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return srv, ts
}

func newTestClient(t *testing.T) *pion.PeerConnection {
	t.Helper()
	se := pion.SettingEngine{}
	se.SetIncludeLoopbackCandidate(true)
	api := pion.NewAPI(pion.WithSettingEngine(se))
	pc, err := api.NewPeerConnection(pion.Configuration{})
	if err != nil {
		t.Fatalf("new client peer connection: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	return pc
}

func waitClientConnected(t *testing.T, pc *pion.PeerConnection) {
	t.Helper()
	ch := make(chan struct{})
	var once bool
	pc.OnConnectionStateChange(func(state pion.PeerConnectionState) {
		if state == pion.PeerConnectionStateConnected && !once {
			once = true
			close(ch)
		}
	})
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("client did not connect (state %s)", pc.ConnectionState())
	}
}

// postOffer 完成 WHIP/WHEP 的 HTTP 往返，返回 answer 和 Location。
func postOffer(t *testing.T, url string, offer string) (answer, location string) {
	t.Helper()
	resp, err := http.Post(url, "application/sdp", strings.NewReader(offer))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST %s -> %d: %s", url, resp.StatusCode, body)
	}
	return string(body), resp.Header.Get("Location")
}

// clientOffer 生成一个带完整候选的 offer。
func clientOffer(t *testing.T, pc *pion.PeerConnection) string {
	t.Helper()
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("create offer: %v", err)
	}
	gather := pion.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatalf("set local description: %v", err)
	}
	<-gather
	return pc.LocalDescription().SDP
}

func annexB(nalus ...[]byte) []byte {
	var out []byte
	for _, n := range nalus {
		out = append(out, 0x00, 0x00, 0x00, 0x01)
		out = append(out, n...)
	}
	return out
}

// TestWHIPIngest 验证浏览器侧 H.264 推流能被解包并写进注册表。
func TestWHIPIngest(t *testing.T) {
	srv, ts := newTestServer(t)
	client := newTestClient(t)

	track, err := pion.NewTrackLocalStaticSample(pion.RTPCodecCapability{
		MimeType: pion.MimeTypeH264, ClockRate: 90000,
	}, "video", "test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.AddTrack(track); err != nil {
		t.Fatal(err)
	}

	answer, loc := postOffer(t, ts.URL+"/whip/live/whip", clientOffer(t, client))
	if err := client.SetRemoteDescription(pion.SessionDescription{Type: pion.SDPTypeAnswer, SDP: answer}); err != nil {
		t.Fatalf("set answer: %v", err)
	}
	waitClientConnected(t, client)

	st := srv.registry.Get("live/whip")
	if st == nil {
		t.Fatal("registry has no published stream")
	}

	// Pion 客户的 offer 不带 a=ssrc，服务端要收到第一个 RTP 包才会触发
	// OnTrack（浏览器推流同样是持续发包）。所以先持续发送，再等视频轨。
	frame := annexB(testSPS, testPPS, testIDR)
	writeCtx := t.Context()
	go func() {
		for writeCtx.Err() == nil {
			if err := track.WriteSample(media.Sample{Data: frame, Duration: 33 * time.Millisecond}); err != nil {
				return
			}
			select {
			case <-writeCtx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()

	var videoSrc *source.ExternalSource
	waitFor(t, func() bool {
		videoSrc = st.SourceFor("video")
		return videoSrc != nil
	})
	rd, cancel := videoSrc.Subscribe()
	defer cancel()

	var got [][]byte
	deadline := time.After(10 * time.Second)
	for len(got) < 3 {
		select {
		case <-deadline:
			t.Fatalf("received %d NALUs, want 3", len(got))
		default:
		}
		f, ok := rd.Next(make(chan struct{}))
		if !ok {
			t.Fatal("source closed")
		}
		got = append(got, f.Data)
	}
	if !bytes.Equal(got[0], testSPS) || !bytes.Equal(got[1], testPPS) || !bytes.Equal(got[2], testIDR) {
		t.Fatalf("unexpected NALUs: %x", got)
	}
	if v, ok := st.VideoParams(); !ok || !bytes.Equal(v.SPS, testSPS) || !bytes.Equal(v.PPS, testPPS) {
		t.Fatalf("video params not recorded: %+v", v)
	}

	resp, err := http.NewRequest(http.MethodDelete, ts.URL+loc, nil)
	if err != nil {
		t.Fatal(err)
	}
	del, err := http.DefaultClient.Do(resp)
	if err != nil {
		t.Fatal(err)
	}
	del.Body.Close()
	if del.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE -> %d", del.StatusCode)
	}
	waitFor(t, func() bool { return srv.registry.Get("live/whip") == nil })
}

// TestWHEPPlayback 验证注册表里的 H.264 流能通过 WHEP 送到浏览器。
func TestWHEPPlayback(t *testing.T) {
	srv, ts := newTestServer(t)

	st := &stream.Stream{}
	if _, err := srv.registry.Publish("live/whep", st); err != nil {
		t.Fatal(err)
	}
	st.SetVideoParams("H264", nil, testSPS, testPPS)
	src := st.EnsureVideo().Src
	for i, nalu := range [][]byte{testSPS, testPPS, testIDR, testP} {
		src.Write(source.Frame{
			Data:      nalu,
			Timestamp: uint32(i) * 3000,
			Keyframe:  i == 2,
			AUStart:   true,
			AUEnd:     true,
		})
	}

	client := newTestClient(t)
	if _, err := client.AddTransceiverFromKind(pion.RTPCodecTypeVideo,
		pion.RTPTransceiverInit{Direction: pion.RTPTransceiverDirectionRecvonly}); err != nil {
		t.Fatal(err)
	}

	got := make(chan []byte, 32)
	client.OnTrack(func(track *pion.TrackRemote, _ *pion.RTPReceiver) {
		if track.Kind() != pion.RTPCodecTypeVideo {
			return
		}
		var depack rtp.H264Depacketizer
		for {
			pkt, _, err := track.ReadRTP()
			if err != nil {
				return
			}
			nalus, err := depack.Depacketize(toInternalRTP(pkt))
			if err != nil {
				continue
			}
			for _, n := range nalus {
				select {
				case got <- append([]byte(nil), n...):
				default:
				}
			}
		}
	})

	answer, _ := postOffer(t, ts.URL+"/whep/live/whep", clientOffer(t, client))
	if err := client.SetRemoteDescription(pion.SessionDescription{Type: pion.SDPTypeAnswer, SDP: answer}); err != nil {
		t.Fatalf("set answer: %v", err)
	}

	deadline := time.After(10 * time.Second)
	seen := map[string]bool{}
	for len(seen) < 3 {
		select {
		case n := <-got:
			switch {
			case bytes.Equal(n, testSPS):
				seen["sps"] = true
			case bytes.Equal(n, testPPS):
				seen["pps"] = true
			case bytes.Equal(n, testIDR):
				seen["idr"] = true
			}
		case <-deadline:
			t.Fatalf("did not receive keyframe NALUs, got %v", seen)
		}
	}
}

// TestWHIPOpusIngest 验证 Opus 推流被原样写进注册表并登记编码参数。
func TestWHIPOpusIngest(t *testing.T) {
	srv, ts := newTestServer(t)
	client := newTestClient(t)

	track, err := pion.NewTrackLocalStaticSample(pion.RTPCodecCapability{
		MimeType: pion.MimeTypeOpus, ClockRate: 48000, Channels: 2,
	}, "audio", "test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.AddTrack(track); err != nil {
		t.Fatal(err)
	}

	answer, _ := postOffer(t, ts.URL+"/whip/live/opus", clientOffer(t, client))
	if err := client.SetRemoteDescription(pion.SessionDescription{Type: pion.SDPTypeAnswer, SDP: answer}); err != nil {
		t.Fatalf("set answer: %v", err)
	}
	waitClientConnected(t, client)

	st := srv.registry.Get("live/opus")
	if st == nil {
		t.Fatal("registry has no published stream")
	}

	payload := bytes.Repeat([]byte{0xF8, 0x00}, 20)
	writeCtx := t.Context()
	go func() {
		for writeCtx.Err() == nil {
			if err := track.WriteSample(media.Sample{Data: payload, Duration: 20 * time.Millisecond}); err != nil {
				return
			}
			select {
			case <-writeCtx.Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()

	var audioSrc *source.ExternalSource
	waitFor(t, func() bool {
		audioSrc = st.SourceFor("audio")
		return audioSrc != nil
	})
	if p, ok := st.AudioParams(); !ok || p.Codec != stream.AudioOpus || p.SampleRate != 48000 {
		t.Fatalf("audio params = %+v ok=%v, want Opus 48000", p, ok)
	}

	rd, cancel := audioSrc.Subscribe()
	defer cancel()
	done := make(chan struct{})
	time.AfterFunc(10*time.Second, func() { close(done) })
	f, ok := rd.Next(done)
	if !ok {
		t.Fatal("source closed before audio frame arrived")
	}
	if !bytes.Equal(f.Data, payload) {
		t.Fatalf("opus payload = %x, want %x", f.Data, payload)
	}
}

// TestWHEPOpusPlayback 验证 Opus 源能通过 WHEP 原样发给浏览器。
func TestWHEPOpusPlayback(t *testing.T) {
	srv, ts := newTestServer(t)

	st := &stream.Stream{}
	if _, err := srv.registry.Publish("live/opus", st); err != nil {
		t.Fatal(err)
	}
	st.SetAudioCodec(stream.AudioOpus, nil, 48000, 2, 48000)
	audioSrc := st.EnsureAudio(48000).Src
	payload := bytes.Repeat([]byte{0xF8, 0x01}, 20)
	writeCtx := t.Context()
	go func() {
		for writeCtx.Err() == nil {
			audioSrc.Write(source.Frame{
				Data:      payload,
				Timestamp: 0,
				Keyframe:  true,
				AUStart:   true,
				AUEnd:     true,
			})
			select {
			case <-writeCtx.Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()

	client := newTestClient(t)
	if _, err := client.AddTransceiverFromKind(pion.RTPCodecTypeAudio,
		pion.RTPTransceiverInit{Direction: pion.RTPTransceiverDirectionRecvonly}); err != nil {
		t.Fatal(err)
	}

	got := make(chan []byte, 32)
	client.OnTrack(func(track *pion.TrackRemote, _ *pion.RTPReceiver) {
		if track.Kind() != pion.RTPCodecTypeAudio {
			return
		}
		for {
			pkt, _, err := track.ReadRTP()
			if err != nil {
				return
			}
			select {
			case got <- append([]byte(nil), pkt.Payload...):
			default:
			}
		}
	})

	answer, _ := postOffer(t, ts.URL+"/whep/live/opus", clientOffer(t, client))
	if err := client.SetRemoteDescription(pion.SessionDescription{Type: pion.SDPTypeAnswer, SDP: answer}); err != nil {
		t.Fatalf("set answer: %v", err)
	}

	deadline := time.After(10 * time.Second)
	for {
		select {
		case frame := <-got:
			if bytes.Equal(frame, payload) {
				return
			}
		case <-deadline:
			t.Fatal("did not receive Opus payload over WHEP")
		}
	}
}

// TestWHEPMissingStream 验证没有发布流时返回 404。
func TestWHEPMissingStream(t *testing.T) {
	_, ts := newTestServer(t)
	client := newTestClient(t)
	if _, err := client.AddTransceiverFromKind(pion.RTPCodecTypeVideo,
		pion.RTPTransceiverInit{Direction: pion.RTPTransceiverDirectionRecvonly}); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(ts.URL+"/whep/live/none", "application/sdp",
		strings.NewReader(clientOffer(t, client)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestParsePath(t *testing.T) {
	tests := []struct {
		path string
		kind sessionKind
		name string
		id   string
		ok   bool
	}{
		{"/whip/live", kindWHIP, "live", "", true},
		{"/whep/live/cam", kindWHEP, "live/cam", "", true},
		{"/whip/live/" + strings.Repeat("a", 32), kindWHIP, "live", strings.Repeat("a", 32), true},
		{"/whep/live/demo/" + strings.Repeat("0", 32), kindWHEP, "live/demo", strings.Repeat("0", 32), true},
		{"/whip/", 0, "", "", false},
		{"/other/live", 0, "", "", false},
	}
	for _, tt := range tests {
		kind, name, id, ok := parsePath(tt.path)
		if ok != tt.ok || kind != tt.kind || name != tt.name || id != tt.id {
			t.Errorf("parsePath(%q) = (%v,%q,%q,%v), want (%v,%q,%q,%v)",
				tt.path, kind, name, id, ok, tt.kind, tt.name, tt.id, tt.ok)
		}
	}
}

func TestTokenGate(t *testing.T) {
	srv := New(stream.NewRegistry())
	srv.SetToken("secret")
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/whep/live/x", "application/sdp", strings.NewReader("v=0"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("no token -> %d, want 403", resp.StatusCode)
	}

	resp, err = http.Post(ts.URL+"/whep/live/x?token=secret", "application/sdp", strings.NewReader("v=0"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusForbidden {
		t.Fatal("valid token rejected")
	}
}

func TestCORS(t *testing.T) {
	srv := New(stream.NewRegistry())
	srv.SetCORS("https://player.example")
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodOptions, ts.URL+"/whep/live/x", nil)
	req.Header.Set("Origin", "https://player.example")
	req.Header.Set("Access-Control-Request-Headers", "Content-Type")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "https://player.example" {
		t.Fatalf("allow-origin = %q", got)
	}
	if !strings.Contains(resp.Header.Get("Access-Control-Allow-Methods"), "POST") {
		t.Fatalf("allow-methods = %q", resp.Header.Get("Access-Control-Allow-Methods"))
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

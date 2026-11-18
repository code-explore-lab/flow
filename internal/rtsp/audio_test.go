package rtsp

import (
	"bytes"
	"testing"

	"flow/internal/rtp"
	"flow/internal/source"
	"flow/internal/stream"
)

// minAudioSDP 生成只含一条音频轨的 ANNOUNCE SDP。
func minAudioSDP(mline, extra string) string {
	return "v=0\r\n" +
		"m=" + mline + "\r\n" +
		extra +
		"a=control:streamid=0\r\n"
}

// TestParseAnnounceSDPStaticAudioPT 校验 static payload type 没有 a=rtpmap 时
// 也能从 m= 行推导出编码（ffmpeg 对 PCMU/PCMA/MPA 经常省略 rtpmap）。
func TestParseAnnounceSDPStaticAudioPT(t *testing.T) {
	cases := []struct {
		mline string
		pt    int
		codec string
		clock int
		ch    int
	}{
		{"audio 0 RTP/AVP 0", 0, "PCMU", 8000, 1},
		{"audio 0 RTP/AVP 8", 8, "PCMA", 8000, 1},
		{"audio 0 RTP/AVP 14", 14, "MPA", 90000, 0},
	}
	for _, c := range cases {
		tracks, err := parseAnnounceSDP([]byte(minAudioSDP(c.mline, "")))
		if err != nil {
			t.Fatalf("%s: %v", c.mline, err)
		}
		if len(tracks) != 1 {
			t.Fatalf("%s: %d track(s)", c.mline, len(tracks))
		}
		tk := tracks[0]
		if tk.Kind != "audio" || tk.PayloadType != c.pt || tk.Encoding != c.codec ||
			tk.ClockRate != c.clock || tk.Channels != c.ch {
			t.Fatalf("%s: track = %+v", c.mline, tk)
		}
	}
}

// TestParseAnnounceSDPAudioCodecs 校验带 rtpmap 的 Opus/AAC/MP3 能通过。
func TestParseAnnounceSDPAudioCodecs(t *testing.T) {
	cases := []struct {
		rtpmap string
		codec  string
		clock  int
		ch     int
	}{
		{"a=rtpmap:97 opus/48000/2\r\n", "opus", 48000, 2},
		{"a=rtpmap:97 MPEG4-GENERIC/44100/2\r\n", "MPEG4-GENERIC", 44100, 2},
		{"a=rtpmap:97 MP3/90000\r\n", "MP3", 90000, 0},
	}
	for _, c := range cases {
		tracks, err := parseAnnounceSDP([]byte(minAudioSDP("audio 0 RTP/AVP 97", c.rtpmap)))
		if err != nil {
			t.Fatalf("%s: %v", c.rtpmap, err)
		}
		tk := tracks[0]
		if tk.Encoding != c.codec || tk.ClockRate != c.clock || tk.Channels != c.ch {
			t.Fatalf("%s: track = %+v", c.rtpmap, tk)
		}
	}
}

// TestAudioParamsFromSDP 校验 SDP 轨道到编码参数的映射：
// MP3 的 RTP 时钟是 90000，采样率 SDP 里拿不到，先按 44100 占位。
func TestAudioParamsFromSDP(t *testing.T) {
	cases := []struct {
		name  string
		track announcedTrack
		codec string
		cfg   []byte
		rate  uint32
		ch    uint8
		clock uint32
	}{
		{
			name:  "PCMU",
			track: announcedTrack{Kind: "audio", Encoding: "PCMU", ClockRate: 8000, Channels: 1},
			codec: stream.AudioPCMU, rate: 8000, ch: 1, clock: 8000,
		},
		{
			name:  "PCMA",
			track: announcedTrack{Kind: "audio", Encoding: "PCMA", ClockRate: 8000, Channels: 1},
			codec: stream.AudioPCMA, rate: 8000, ch: 1, clock: 8000,
		},
		{
			name:  "Opus",
			track: announcedTrack{Kind: "audio", Encoding: "opus", ClockRate: 48000, Channels: 2},
			codec: stream.AudioOpus, rate: 48000, ch: 2, clock: 48000,
		},
		{
			name:  "MP3",
			track: announcedTrack{Kind: "audio", Encoding: "MPA", ClockRate: 90000},
			codec: stream.AudioMP3, rate: 44100, ch: 2, clock: 90000,
		},
		{
			name: "AAC",
			track: announcedTrack{
				Kind: "audio", Encoding: "MPEG4-GENERIC", ClockRate: 44100, Channels: 2,
				FMTP: "mode=AAC-hbr; config=1210",
			},
			codec: stream.AudioAAC, cfg: []byte{0x12, 0x10}, rate: 44100, ch: 2, clock: 44100,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			codec, cfg, rate, ch, clock := audioParamsFromSDP(c.track)
			if codec != c.codec || rate != c.rate || ch != c.ch || clock != c.clock {
				t.Fatalf("params = %s %d/%d/%d, want %s %d/%d/%d",
					codec, rate, ch, clock, c.codec, c.rate, c.ch, c.clock)
			}
			if !bytes.Equal(cfg, c.cfg) {
				t.Fatalf("config = %x, want %x", cfg, c.cfg)
			}
		})
	}
}

// TestSynthesizeTracksAudioCodecs 校验非 RTSP 推入的流按音频编码合成 SDP 轨道。
func TestSynthesizeTracksAudioCodecs(t *testing.T) {
	cases := []struct {
		codec    string
		rate     uint32
		ch       uint8
		clock    uint32
		pt       int
		encoding string
		sdpCh    int
		wantFMTP string
	}{
		{stream.AudioPCMA, 8000, 1, 8000, 8, "PCMA", 1, ""},
		{stream.AudioPCMU, 8000, 1, 8000, 0, "PCMU", 1, ""},
		{stream.AudioOpus, 48000, 2, 48000, 97, "opus", 2, ""},
		{stream.AudioMP3, 44100, 2, 90000, 14, "MPA", 0, ""},
	}
	for _, c := range cases {
		p := &stream.Stream{}
		p.SetAudioCodec(c.codec, nil, c.rate, c.ch, c.clock)
		tracks := synthesizeTracks(p)
		if len(tracks) != 1 {
			t.Fatalf("%s: %d track(s)", c.codec, len(tracks))
		}
		tk := tracks[0]
		if tk.Kind != "audio" || tk.PayloadType != c.pt || tk.Encoding != c.encoding ||
			tk.ClockRate != int(c.clock) || tk.Channels != c.sdpCh {
			t.Fatalf("%s: track = %+v", c.codec, tk)
		}
		if c.wantFMTP != "" && tk.FMTP != c.wantFMTP {
			t.Fatalf("%s: fmtp = %q", c.codec, tk.FMTP)
		}
	}
}

// TestSynthesizeTracksAACConfig 校验 AAC 的 config= 仍写进 fmtp。
func TestSynthesizeTracksAACConfig(t *testing.T) {
	p := &stream.Stream{}
	p.SetAudioCodec(stream.AudioAAC, []byte{0x12, 0x10}, 44100, 2, 44100)
	tracks := synthesizeTracks(p)
	if len(tracks) != 1 {
		t.Fatalf("%d track(s)", len(tracks))
	}
	tk := tracks[0]
	if tk.PayloadType != 97 || tk.Encoding != "MPEG4-GENERIC" || tk.ClockRate != 44100 {
		t.Fatalf("track = %+v", tk)
	}
	if !bytes.Contains([]byte(tk.FMTP), []byte("config=1210")) {
		t.Fatalf("fmtp = %q", tk.FMTP)
	}
}

// TestPushReceiverRawAudio 校验 G.711 / Opus 的 RTP 负载原样写入帧源，
// 时间戳沿用 RTP 时间戳。
func TestPushReceiverRawAudio(t *testing.T) {
	cases := []struct {
		encoding string
		payload  []byte
	}{
		{"PCMA", []byte{0xD5, 0x00, 0x7F}},
		{"PCMU", []byte{0xFF, 0x7F, 0x00}},
		{"OPUS", []byte{0x78, 0x01, 0x02}},
	}
	for _, c := range cases {
		ext := source.NewExternalAudioSource()
		rx, err := newPushReceiver(c.encoding, "", 0, ext)
		if err != nil {
			t.Fatalf("%s: %v", c.encoding, err)
		}
		rd, cancel := ext.Subscribe()
		done := make(chan struct{})
		track := &mediaTrack{kind: "audio"}
		rx.handleRawAudio(track, &rtp.RTPPacket{
			Header:  rtp.RTPHeader{Timestamp: 1234},
			Payload: c.payload,
		})
		f, ok := rd.Next(done)
		close(done)
		cancel()
		if !ok {
			t.Fatalf("%s: no frame written", c.encoding)
		}
		if !bytes.Equal(f.Data, c.payload) || f.Timestamp != 1234 || !f.Keyframe {
			t.Fatalf("%s: frame = %+v", c.encoding, f)
		}
	}
}

// TestPushReceiverMP3 校验 RFC 2250 分片重组和首帧采样率回填。
func TestPushReceiverMP3(t *testing.T) {
	ext := source.NewExternalAudioSource()
	rx, err := newPushReceiver("MPA", "", 0, ext)
	if err != nil {
		t.Fatal(err)
	}
	st := &stream.Stream{}
	track := &mediaTrack{kind: "audio", stream: st}
	rd, cancel := ext.Subscribe()
	defer cancel()

	// MPEG-1 Layer III 44100Hz 128kbps 立体声完整帧（417 字节）。
	frame := append([]byte{0xFF, 0xFB, 0x90, 0x00}, bytes.Repeat([]byte{0x11}, 413)...)
	packer := rtp.NewMPAPacker()
	packer.MTU = 64
	pkts, err := packer.Pack(frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(pkts) < 2 {
		t.Fatalf("got %d packet(s), want multiple fragments", len(pkts))
	}
	for _, p := range pkts {
		p.Header.Timestamp = 9000
		rx.handleRawAudio(track, p)
	}

	done := make(chan struct{})
	defer close(done)
	f, ok := rd.Next(done)
	if !ok {
		t.Fatal("no frame written")
	}
	if !bytes.Equal(f.Data, frame) || f.Timestamp != 9000 || !f.Keyframe {
		t.Fatalf("frame len = %d ts = %d key = %v", len(f.Data), f.Timestamp, f.Keyframe)
	}

	params, ok := st.AudioParams()
	if !ok {
		t.Fatal("AudioParams not set")
	}
	if params.Codec != stream.AudioMP3 || params.SampleRate != 44100 ||
		params.Channels != 2 || params.ClockRate != 90000 {
		t.Fatalf("params = %+v", params)
	}
}

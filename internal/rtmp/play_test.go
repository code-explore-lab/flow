package rtmp

import (
	"bytes"
	"testing"
	"time"

	"flow/internal/flv"
	"flow/internal/source"
	"flow/internal/stream"
)

// readMessageWithin 带超时地读一条消息，避免测试挂死。
func readMessageWithin(t *testing.T, c *Conn) *Message {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	m, err := c.readMessage()
	if err != nil {
		t.Fatalf("readMessage: %v", err)
	}
	return m
}

// readStatusWithin 读下一条 onStatus，返回 code。
func readStatusWithin(t *testing.T, c *Conn) string {
	t.Helper()
	m := readMessageWithin(t, c)
	cmd, err := parseCommand(m.Payload)
	if err != nil {
		t.Fatalf("parseCommand: %v", err)
	}
	if cmd.Name != "onStatus" {
		t.Fatalf("command = %q, want onStatus", cmd.Name)
	}
	return commandCode(cmd)
}

// startPlay 在 server 上发起 play 并读掉 Reset/Start 两条状态。
// 返回时播放 goroutine 已经在运行。
func startPlay(t *testing.T, server, client *Conn, name string) {
	t.Helper()
	cmd, err := parseCommand(buildCommand("play", 0, flv.Null(), flv.Str(name)))
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- server.handlePlay(1, cmd) }()
	if code := readStatusWithin(t, client); code != "NetStream.Play.Reset" {
		t.Fatalf("status code = %q, want NetStream.Play.Reset", code)
	}
	if code := readStatusWithin(t, client); code != "NetStream.Play.Start" {
		t.Fatalf("status code = %q, want NetStream.Play.Start", code)
	}
	if err := <-errc; err != nil {
		t.Fatalf("handlePlay: %v", err)
	}
}

// stopPlayer 先断客户端（解锁阻塞中的写），再停播并等 goroutine 退出。
func stopPlayer(t *testing.T, server, client *Conn) {
	t.Helper()
	_ = client.Close()
	server.stopPlay()
}

func TestRelativeClockBaseline(t *testing.T) {
	var rc relativeClock
	// 首帧建立基准。
	if got := rc.relative(1000); got != 0 {
		t.Fatalf("first = %d, want 0", got)
	}
	if got := rc.relative(1040); got != 40 {
		t.Fatalf("1040 = %d, want 40", got)
	}
	// 早于基准的帧（RTSP 源时间戳回退）贴到上一帧之后，保证 dts 单调。
	if got := rc.relative(900); got != 41 {
		t.Fatalf("900 = %d, want 41", got)
	}
	// 重复时间戳同样 +1，避免 ffmpeg 报 non-monotonic dts。
	if got := rc.relative(1040); got != 42 {
		t.Fatalf("repeat 1040 = %d, want 42", got)
	}
	// 基准不会被后续帧覆盖。
	if got := rc.relative(1200); got != 200 {
		t.Fatalf("1200 = %d, want 200", got)
	}
}

func TestRelativeClockPerTrack(t *testing.T) {
	// 音视频各一个基准：两轨绝对时钟互不相关（RTSP 源），不能互相影响。
	var v, a relativeClock
	if got := v.relative(500_000); got != 0 {
		t.Fatalf("video first = %d, want 0", got)
	}
	if got := a.relative(90_000_000); got != 0 {
		t.Fatalf("audio first = %d, want 0", got)
	}
	if got := a.relative(90_000_023); got != 23 {
		t.Fatalf("audio second = %d, want 23", got)
	}
	if got := v.relative(500_040); got != 40 {
		t.Fatalf("video second = %d, want 40", got)
	}
}

func TestPlayVideoFromRegistry(t *testing.T) {
	srv := New(nil, nil, nil)
	st := srv.registry.Publish("live/test", &stream.Stream{})
	sps := []byte{0x67, 0x42, 0x00, 0x1f, 0x96, 0x54, 0x05, 0x01, 0xed}
	pps := []byte{0x68, 0xee, 0x3c, 0x80}
	st.SetVideoParams(sps, pps)
	st.EnsureVideo()

	server, client := pipeConns(t)
	server.srv = srv
	startPlay(t, server, client, "live/test")
	defer stopPlayer(t, server, client)

	// 订阅已建立，写入的帧会直接进入读者的 ring。
	src := st.SourceFor("video")
	idr := []byte{0x65, 0x88, 0x84, 0x00}
	p := []byte{0x41, 0x9a, 0x02}
	src.Write(source.Frame{Data: idr, Timestamp: 9000, Keyframe: true})
	src.Write(source.Frame{Data: p, Timestamp: 18000})
	src.Write(source.Frame{Data: p, Timestamp: 27000})

	// 1. 先收到 AVC 序列头（参数来自注册表，不用等首帧）。
	m := readMessageWithin(t, client)
	if m.TypeID != msgVideo || m.StreamID != 1 {
		t.Fatalf("message type/stream = %d/%d, want %d/1", m.TypeID, m.StreamID, msgVideo)
	}
	tag, err := flv.ParseVideoTag(m.Payload)
	if err != nil {
		t.Fatalf("ParseVideoTag(seq header): %v", err)
	}
	if tag.PacketType != flv.AVCSeqHeader {
		t.Fatalf("packet type = %d, want sequence header", tag.PacketType)
	}
	gotSPS, gotPPS, err := flv.ParseAVCDecoderConfig(tag.Config)
	if err != nil {
		t.Fatalf("ParseAVCDecoderConfig: %v", err)
	}
	if !bytes.Equal(gotSPS, sps) || !bytes.Equal(gotPPS, pps) {
		t.Fatalf("config = %x/%x, want %x/%x", gotSPS, gotPPS, sps, pps)
	}

	// 2. 逐帧 FLV tag，时间戳换算成相对首帧的毫秒。
	want := []struct {
		ts  uint32
		key bool
	}{
		{0, true},    // 9000/90=100ms，基准
		{100, false}, // 18000/90=200ms
		{200, false}, // 27000/90=300ms
	}
	for i, w := range want {
		m := readMessageWithin(t, client)
		if m.TypeID != msgVideo {
			t.Fatalf("frame %d type = %d, want video", i, m.TypeID)
		}
		tag, err := flv.ParseVideoTag(m.Payload)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if tag.PacketType != flv.AVCNALU || len(tag.NALUs) != 1 {
			t.Fatalf("frame %d packet = %d nalu=%d", i, tag.PacketType, len(tag.NALUs))
		}
		if m.Timestamp != w.ts {
			t.Fatalf("frame %d ts = %d, want %d", i, m.Timestamp, w.ts)
		}
		if tag.Keyframe != w.key {
			t.Fatalf("frame %d keyframe = %v, want %v", i, tag.Keyframe, w.key)
		}
	}
}

func TestPlayCapturesParamSetsFromFrames(t *testing.T) {
	srv := New(nil, nil, nil)
	// 没有 SetVideoParams 的流：模拟本地回退源（SPS/PPS 在首帧里）。
	st := srv.registry.Publish("local", &stream.Stream{})
	st.EnsureVideo()

	server, client := pipeConns(t)
	server.srv = srv
	startPlay(t, server, client, "local")
	defer stopPlayer(t, server, client)

	sps := []byte{0x67, 0x42, 0x00, 0x1f, 0x96, 0x54, 0x05, 0x01, 0xed}
	pps := []byte{0x68, 0xee, 0x3c, 0x80}
	idr := []byte{0x65, 0x88, 0x84, 0x00}
	src := st.SourceFor("video")
	src.Write(source.Frame{Data: sps, Timestamp: 0})
	src.Write(source.Frame{Data: pps, Timestamp: 0})
	src.Write(source.Frame{Data: idr, Timestamp: 9000, Keyframe: true})

	// SPS/PPS 只用来生成序列头，不单独发 tag。
	m := readMessageWithin(t, client)
	tag, err := flv.ParseVideoTag(m.Payload)
	if err != nil {
		t.Fatalf("ParseVideoTag: %v", err)
	}
	if tag.PacketType != flv.AVCSeqHeader {
		t.Fatalf("first packet type = %d, want sequence header", tag.PacketType)
	}
	gotSPS, gotPPS, err := flv.ParseAVCDecoderConfig(tag.Config)
	if err != nil {
		t.Fatalf("ParseAVCDecoderConfig: %v", err)
	}
	if !bytes.Equal(gotSPS, sps) || !bytes.Equal(gotPPS, pps) {
		t.Fatalf("captured config = %x/%x, want %x/%x", gotSPS, gotPPS, sps, pps)
	}

	m = readMessageWithin(t, client)
	tag, err = flv.ParseVideoTag(m.Payload)
	if err != nil {
		t.Fatalf("ParseVideoTag(idr): %v", err)
	}
	if tag.PacketType != flv.AVCNALU || !tag.Keyframe || !bytes.Equal(tag.NALUs[0], idr) {
		t.Fatalf("second message = packet %d key %v nalu %x", tag.PacketType, tag.Keyframe, tag.NALUs)
	}
	if m.Timestamp != 0 {
		t.Fatalf("idr ts = %d, want 0 (relative to first frame)", m.Timestamp)
	}
}

func TestPlayAudio(t *testing.T) {
	srv := New(nil, nil, nil)
	st := srv.registry.Publish("radio", &stream.Stream{})
	asc := []byte{0x12, 0x10} // AAC-LC 44100Hz 立体声
	st.SetAudioParams(asc, 44100, 2)
	st.EnsureAudio(44100)

	server, client := pipeConns(t)
	server.srv = srv
	startPlay(t, server, client, "radio")
	defer stopPlayer(t, server, client)

	au1 := []byte{0x21, 0x10, 0x05}
	au2 := []byte{0x21, 0x20, 0x06}
	src := st.SourceFor("audio")
	src.Write(source.Frame{Data: au1, Timestamp: 1024, Keyframe: true})
	src.Write(source.Frame{Data: au2, Timestamp: 2048, Keyframe: true})

	m := readMessageWithin(t, client)
	tag, err := flv.ParseAudioTag(m.Payload)
	if err != nil {
		t.Fatalf("ParseAudioTag(seq header): %v", err)
	}
	if tag.PacketType != flv.AACSeqHeader || !bytes.Equal(tag.Config, asc) {
		t.Fatalf("seq header = packet %d config %x, want 0/%x", tag.PacketType, tag.Config, asc)
	}

	// 1024 个采样 @44100Hz = 23ms，2048 -> 46ms，相对首帧为 0/23。
	for i, want := range []struct {
		au []byte
		ts uint32
	}{{au1, 0}, {au2, 23}} {
		m := readMessageWithin(t, client)
		tag, err := flv.ParseAudioTag(m.Payload)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if tag.PacketType != flv.AACRaw || !bytes.Equal(tag.AU, want.au) {
			t.Fatalf("frame %d packet = %d au = %x", i, tag.PacketType, tag.AU)
		}
		if m.Timestamp != want.ts {
			t.Fatalf("frame %d ts = %d, want %d", i, m.Timestamp, want.ts)
		}
	}
}

func TestPlayStreamNotFound(t *testing.T) {
	for name, streamName := range map[string]string{
		"missing": "nope",
		"empty":   "/?token=x",
	} {
		t.Run(name, func(t *testing.T) {
			srv := New(nil, nil, nil)
			server, client := pipeConns(t)
			server.srv = srv
			defer stopPlayer(t, server, client)

			cmd, err := parseCommand(buildCommand("play", 0, flv.Null(), flv.Str(streamName)))
			if err != nil {
				t.Fatal(err)
			}
			errc := make(chan error, 1)
			go func() { errc <- server.handlePlay(1, cmd) }()
			if code := readStatusWithin(t, client); code != "NetStream.Play.StreamNotFound" {
				t.Fatalf("status code = %q, want NetStream.Play.StreamNotFound", code)
			}
			if err := <-errc; err != nil {
				t.Fatalf("handlePlay: %v", err)
			}
			if server.play != nil {
				t.Fatal("failed play left state behind")
			}
		})
	}
}

// TestHandleCommandDispatchesPlay 确认命令层把 play 交给 play.go。
func TestHandleCommandDispatchesPlay(t *testing.T) {
	srv := New(nil, nil, nil)
	st := srv.registry.Publish("live/test", &stream.Stream{})
	st.SetVideoParams([]byte{0x67, 0x42, 0x00, 0x1f}, []byte{0x68, 0xee})
	st.EnsureVideo()

	server, client := pipeConns(t)
	server.srv = srv
	defer stopPlayer(t, server, client)

	m := &Message{
		TypeID:   msgCommandAMF0,
		StreamID: 1,
		Payload:  buildCommand("play", 0, flv.Null(), flv.Str("live/test")),
	}
	errc := make(chan error, 1)
	go func() { errc <- server.handleCommand(m) }()
	if code := readStatusWithin(t, client); code != "NetStream.Play.Reset" {
		t.Fatalf("status = %q, want NetStream.Play.Reset", code)
	}
	if code := readStatusWithin(t, client); code != "NetStream.Play.Start" {
		t.Fatalf("status = %q, want NetStream.Play.Start", code)
	}
	if err := <-errc; err != nil {
		t.Fatalf("handleCommand: %v", err)
	}
	if server.play == nil {
		t.Fatal("handleCommand did not start playback")
	}
}

func TestPlayCompleteWhenSourceEnds(t *testing.T) {
	srv := New(nil, nil, nil)
	st := srv.registry.Publish("ending", &stream.Stream{})
	st.SetVideoParams([]byte{0x67, 0x42, 0x00, 0x1f}, []byte{0x68, 0xee})
	st.EnsureVideo()

	server, client := pipeConns(t)
	server.srv = srv
	startPlay(t, server, client, "ending")
	defer stopPlayer(t, server, client)

	src := st.SourceFor("video")
	src.Write(source.Frame{Data: []byte{0x65, 0x88, 0x84, 0x00}, Timestamp: 0, Keyframe: true})
	// 读掉序列头 + 那一帧，确保播放 goroutine 已经消费到源尾。
	readMessageWithin(t, client)
	readMessageWithin(t, client)

	src.Close()

	if code := readStatusWithin(t, client); code != "NetStream.Play.Complete" {
		t.Fatalf("status = %q, want NetStream.Play.Complete", code)
	}

	// 收尾后服务端关闭连接：客户端继续读会出错而不是挂住。
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := client.readMessage(); err == nil {
		t.Fatal("connection still open after Play.Complete")
	}
}

// TestLocalAudioParams 本地回退源的 AAC 序列头来自 ADTS 首帧。
func TestLocalAudioParams(t *testing.T) {
	src := source.NewAACSource("../../testdata/test.aac")
	p := localAudioParams(src)
	if p == nil {
		t.Fatal("localAudioParams returned nil")
	}
	if p.SampleRate != 44100 {
		t.Fatalf("sample rate = %d, want 44100", p.SampleRate)
	}
	if len(p.Config) != 2 {
		t.Fatalf("ASC = %x, want 2 bytes", p.Config)
	}
	if p.Channels == 0 {
		t.Fatal("channel count = 0")
	}

	// 参数缺失的文件只打印日志、返回 nil，不 panic。
	if got := localAudioParams(source.NewAACSource("../../testdata/missing.aac")); got != nil {
		t.Fatalf("missing file returned %+v", got)
	}
}

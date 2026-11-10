package rtmp

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"flow/internal/flv"
	"flow/internal/source"
	"flow/internal/stream"
)

// newPublishConn 构造一条已处于推流状态的 Conn（不经过网络）。
func newPublishConn(t *testing.T) (*Server, *Conn, *stream.Stream) {
	t.Helper()
	srv := New(nil, nil, nil)
	st := srv.registry.Publish("push", &stream.Stream{})
	c := &Conn{
		srv:     srv,
		publish: &publishState{stream: st, audioRate: defaultAACRate},
	}
	return srv, c, st
}

// nextFrame 带超时地从读者取一帧。
func nextFrame(t *testing.T, rd *source.Reader[source.Frame]) source.Frame {
	t.Helper()
	done := make(chan struct{})
	close(done)
	type result struct {
		f  source.Frame
		ok bool
	}
	ch := make(chan result, 1)
	go func() {
		f, ok := rd.Next(done)
		ch <- result{f, ok}
	}()
	select {
	case r := <-ch:
		if !r.ok {
			t.Fatal("source ended before a frame arrived")
		}
		return r.f
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a frame")
		return source.Frame{}
	}
}

func TestVideoSequenceHeader(t *testing.T) {
	_, c, st := newPublishConn(t)
	sps := []byte{0x67, 0x42, 0x00, 0x1f, 0x96, 0x54, 0x05, 0x01, 0xed}
	pps := []byte{0x68, 0xee, 0x3c, 0x80}

	// 推流开始后视频轨由序列头创建；这里先建好再订阅，确认写入的帧。
	st.EnsureVideo()
	rd, cancel := st.SourceFor("video").Subscribe()
	defer cancel()

	c.handleVideo(&Message{TypeID: msgVideo, Payload: flv.BuildVideoSequenceHeader(sps, pps)})

	params, ok := st.VideoParams()
	if !ok {
		t.Fatal("VideoParams not set after sequence header")
	}
	if !bytes.Equal(params.SPS, sps) || !bytes.Equal(params.PPS, pps) {
		t.Fatalf("params = %x/%x, want %x/%x", params.SPS, params.PPS, sps, pps)
	}

	// 序列头里的 SPS/PPS 也作为帧写进了源（供中途加入的订阅者预热）。
	if f := nextFrame(t, rd); !bytes.Equal(f.Data, sps) {
		t.Fatalf("first frame = %x, want sps", f.Data)
	}
	if f := nextFrame(t, rd); !bytes.Equal(f.Data, pps) {
		t.Fatalf("second frame = %x, want pps", f.Data)
	}
}

func TestVideoNALUFrames(t *testing.T) {
	_, c, st := newPublishConn(t)
	st.EnsureVideo()
	rd, cancel := st.SourceFor("video").Subscribe()
	defer cancel()

	idr := []byte{0x65, 0x88, 0x84, 0x00}
	p := []byte{0x41, 0x9a, 0x02}

	c.handleVideo(&Message{TypeID: msgVideo, Timestamp: 40, Payload: flv.BuildVideoTag(idr, true, 0)})
	f := nextFrame(t, rd)
	if !bytes.Equal(f.Data, idr) {
		t.Fatalf("data = %x, want idr", f.Data)
	}
	if f.Timestamp != 3600 {
		t.Fatalf("timestamp = %d, want 3600 (40ms -> 90kHz)", f.Timestamp)
	}
	if !f.Keyframe {
		t.Fatal("IDR frame lost its keyframe flag")
	}

	c.handleVideo(&Message{TypeID: msgVideo, Timestamp: 80, Payload: flv.BuildVideoTag(p, false, 0)})
	f = nextFrame(t, rd)
	if f.Timestamp != 7200 || f.Keyframe {
		t.Fatalf("p-frame = ts %d key %v, want 7200/false", f.Timestamp, f.Keyframe)
	}
}

func TestAudioSequenceHeaderAndFrames(t *testing.T) {
	_, c, st := newPublishConn(t)
	asc := []byte{0x12, 0x10} // AAC-LC 44100Hz 立体声

	c.handleAudio(&Message{TypeID: msgAudio, Payload: flv.BuildAudioSequenceHeader(asc, 44100, 2)})
	params, ok := st.AudioParams()
	if !ok {
		t.Fatal("AudioParams not set after sequence header")
	}
	if !bytes.Equal(params.Config, asc) || params.SampleRate != 44100 || params.Channels != 2 {
		t.Fatalf("params = %x %d/%d, want %x 44100/2",
			params.Config, params.SampleRate, params.Channels, asc)
	}

	rd, cancel := st.SourceFor("audio").Subscribe()
	defer cancel()

	au := []byte{0x21, 0x10, 0x05}
	c.handleAudio(&Message{TypeID: msgAudio, Timestamp: 23, Payload: flv.BuildAudioTag(au, 44100, 2)})
	f := nextFrame(t, rd)
	if !bytes.Equal(f.Data, au) {
		t.Fatalf("data = %x, want au", f.Data)
	}
	// 23ms @ 44100Hz = 1014（整数截断）。
	if f.Timestamp != 1014 {
		t.Fatalf("timestamp = %d, want 1014", f.Timestamp)
	}
	if !f.Keyframe {
		t.Fatal("audio frame should be marked keyframe")
	}
}

func TestAudioBeforeSequenceHeader(t *testing.T) {
	_, c, st := newPublishConn(t)
	c.publish.audioRate = 0 // 还没收到序列头

	rd := st.SourceFor("audio")
	if rd != nil {
		t.Fatal("audio track should not exist yet")
	}

	// 没有序列头时按默认 44100 建轨并写帧。
	c.handleAudio(&Message{TypeID: msgAudio, Timestamp: 0, Payload: flv.BuildAudioTag([]byte{0x01}, 44100, 2)})
	track := st.AudioTrack()
	if track == nil {
		t.Fatal("audio track was not created for a raw AU")
	}

	rdr, cancel := track.Src.Subscribe()
	defer cancel()
	c.handleAudio(&Message{TypeID: msgAudio, Timestamp: 23, Payload: flv.BuildAudioTag([]byte{0x02}, 44100, 2)})
	f := nextFrame(t, rdr)
	// 23ms 按默认 44100Hz 换算 = 1014，说明没等序列头也用了正确时钟。
	if f.Timestamp != 1014 || len(f.Data) != 1 {
		t.Fatalf("frame = %+v", f)
	}
}

func TestStopPublishClosesSources(t *testing.T) {
	srv, c, st := newPublishConn(t)
	st.EnsureVideo()
	rd, cancel := st.SourceFor("video").Subscribe()
	defer cancel()

	c.stopPublish()

	if srv.registry.Get("push") != nil {
		t.Fatal("stream still registered after stopPublish")
	}
	done := make(chan struct{})
	close(done)
	if _, ok := rd.Next(done); ok {
		t.Fatal("source still delivers frames after stopPublish")
	}
	// 重复调用安全。
	c.stopPublish()
}

func TestTimestampConversion(t *testing.T) {
	if got := videoTimestamp(40); got != 3600 {
		t.Fatalf("videoTimestamp(40) = %d, want 3600", got)
	}
	if got := videoTimestamp(0); got != 0 {
		t.Fatalf("videoTimestamp(0) = %d, want 0", got)
	}
	if got := audioTimestamp(1000, 44100); got != 44100 {
		t.Fatalf("audioTimestamp(1000, 44100) = %d, want 44100", got)
	}
	if got := audioTimestamp(20, 48000); got != 960 {
		t.Fatalf("audioTimestamp(20, 48000) = %d, want 960", got)
	}
}

// ---- 端到端：真实 TCP 上完成握手 + connect/createStream/publish + 发 FLV 帧 ----

// clientHandshake 以客户端身份完成简单握手（server 的 handshake 是服务端
// 角色，两端都用会互相等待 C0）。
func clientHandshake(t *testing.T, c *Conn) {
	t.Helper()
	out := make([]byte, 0, 1+handshakeSize)
	out = append(out, handshakeVersion)
	out = append(out, make([]byte, handshakeSize)...)
	if err := c.write(out); err != nil {
		t.Fatalf("write C0/C1: %v", err)
	}
	if _, err := io.ReadFull(c.br, make([]byte, 1+2*handshakeSize)); err != nil {
		t.Fatalf("read S0/S1/S2: %v", err)
	}
	if err := c.write(make([]byte, handshakeSize)); err != nil {
		t.Fatalf("write C2: %v", err)
	}
}

// readUntilCommand 读消息直到出现指定命令；控制消息会被就地应用。
func readUntilCommand(t *testing.T, c *Conn, name string) *Command {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.SetReadDeadline(time.Time{}) }()
	for {
		m, err := c.readMessage()
		if err != nil {
			t.Fatalf("waiting for %s: %v", name, err)
		}
		if _, err := c.handleControl(m); err != nil {
			t.Fatalf("handleControl: %v", err)
		}
		if m.TypeID != msgCommandAMF0 {
			continue
		}
		cmd, err := parseCommand(m.Payload)
		if err != nil {
			t.Fatalf("parseCommand: %v", err)
		}
		if cmd.Name == name {
			return cmd
		}
	}
}

func TestPublishIngestOverTCP(t *testing.T) {
	srv := New(nil, nil, nil)
	addr := freeAddr(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ctx, addr) }()

	var raw net.Conn
	deadline := time.Now().Add(3 * time.Second)
	for {
		var err error
		raw, err = net.Dial("tcp", addr)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial %s: %v", addr, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer raw.Close()
	client := newConn(raw)
	clientHandshake(t, client)

	// connect
	if err := client.writeMessage(&Message{
		TypeID:  msgCommandAMF0,
		Payload: buildCommand("connect", 1, flv.Obj(flv.Elem("app", flv.Str("live")))),
	}, commandCSID); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	res := readUntilCommand(t, client, "_result")
	if code := commandCode(res); code != "NetConnection.Connect.Success" {
		t.Fatalf("connect code = %q", code)
	}

	// createStream
	if err := client.writeMessage(&Message{
		TypeID:  msgCommandAMF0,
		Payload: buildCommand("createStream", 2, flv.Null()),
	}, commandCSID); err != nil {
		t.Fatalf("write createStream: %v", err)
	}
	res = readUntilCommand(t, client, "_result")
	streamID := uint32(lastNumber(res))
	if streamID == 0 {
		t.Fatal("createStream returned stream id 0")
	}

	// publish
	if err := client.writeMessage(&Message{
		TypeID:   msgCommandAMF0,
		StreamID: streamID,
		Payload:  buildCommand("publish", 3, flv.Null(), flv.Str("push"), flv.Str("live")),
	}, commandCSID); err != nil {
		t.Fatalf("write publish: %v", err)
	}
	res = readUntilCommand(t, client, "onStatus")
	if code := commandCode(res); code != "NetStream.Publish.Start" {
		t.Fatalf("publish code = %q", code)
	}

	waitFor(t, 2*time.Second, func() bool { return srv.registry.Get("push") != nil })
	st := srv.registry.Get("push")
	// publish 时还不知道有哪些轨；先建视频轨再订阅，保证能读到随后写入的帧。
	st.EnsureVideo()
	vr, vcancel := st.SourceFor("video").Subscribe()
	defer vcancel()

	sps := []byte{0x67, 0x42, 0x00, 0x1f, 0x96, 0x54, 0x05, 0x01, 0xed}
	pps := []byte{0x68, 0xee, 0x3c, 0x80}
	idr := []byte{0x65, 0x88, 0x84, 0x00}
	for _, m := range []*Message{
		{TypeID: msgVideo, StreamID: streamID, Timestamp: 0, Payload: flv.BuildVideoSequenceHeader(sps, pps)},
		{TypeID: msgVideo, StreamID: streamID, Timestamp: 40, Payload: flv.BuildVideoTag(idr, true, 0)},
	} {
		if err := client.writeMessage(m, videoCSID); err != nil {
			t.Fatalf("write video: %v", err)
		}
	}

	if f := nextFrame(t, vr); !bytes.Equal(f.Data, sps) {
		t.Fatalf("frame 1 = %x, want sps", f.Data)
	}
	if f := nextFrame(t, vr); !bytes.Equal(f.Data, pps) {
		t.Fatalf("frame 2 = %x, want pps", f.Data)
	}
	f := nextFrame(t, vr)
	if !bytes.Equal(f.Data, idr) || !f.Keyframe || f.Timestamp != 3600 {
		t.Fatalf("frame 3 = %x key=%v ts=%d", f.Data, f.Keyframe, f.Timestamp)
	}

	// 断开后流应被注销。
	_ = raw.Close()
	waitFor(t, 3*time.Second, func() bool { return srv.registry.Get("push") == nil })

	cancel()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("Serve returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return after ctx cancel")
	}
}

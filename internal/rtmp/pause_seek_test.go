package rtmp

import (
	"testing"
	"time"

	"flow/internal/flv"
	"flow/internal/source"
	"flow/internal/stream"
)

// sendPlayCommand 在 server 上执行一条播放控制命令并读回下一条 onStatus。
// 调用前必须保证没有媒体消息在途（测试里通过先读掉已写入的帧做到）。
func sendPlayCommand(t *testing.T, server, client *Conn, streamID uint32, payload []byte) string {
	t.Helper()
	errc := make(chan error, 1)
	go func() {
		errc <- server.handleCommand(&Message{
			TypeID:   msgCommandAMF0,
			StreamID: streamID,
			Payload:  payload,
		})
	}()
	code := readStatusWithin(t, client)
	if err := <-errc; err != nil {
		t.Fatalf("handleCommand: %v", err)
	}
	return code
}

// TestPauseResumeKeepsTimestamps 暂停期间不发送、不丢订阅；恢复后按原顺序
// 补发，DTS 连续（无重复、无跳变）。
func TestPauseResumeKeepsTimestamps(t *testing.T) {
	srv := New(nil, nil, nil)
	st := srv.registry.Publish("live/pause", &stream.Stream{})
	st.SetVideoParams("H264", nil,
		[]byte{0x67, 0x42, 0x00, 0x1f}, []byte{0x68, 0xee})
	st.EnsureVideo()

	server, client := pipeConns(t)
	server.srv = srv
	startPlay(t, server, client, "live/pause")
	defer stopPlayer(t, server, client)

	// 读掉序列头。
	if m := readMessageWithin(t, client); m.TypeID != msgVideo {
		t.Fatalf("first type = %d, want video", m.TypeID)
	}

	src := st.SourceFor("video")
	frame := []byte{0x41, 0x9a, 0x02}
	for i, ts := range []uint32{0, 3600, 7200} {
		src.Write(source.Frame{Data: frame, Timestamp: ts})
		m := readMessageWithin(t, client)
		if want := uint32(i * 40); m.Timestamp != want {
			t.Fatalf("frame %d ts = %d, want %d", i, m.Timestamp, want)
		}
	}

	if code := sendPlayCommand(t, server, client, 1,
		buildCommand("pause", 0, flv.Null(), flv.Bool(true), flv.Num(80))); code != "NetStream.Pause.Notify" {
		t.Fatalf("pause status = %q", code)
	}

	// 暂停期间写入的帧不得发送。
	src.Write(source.Frame{Data: frame, Timestamp: 14400})
	src.Write(source.Frame{Data: frame, Timestamp: 18000})
	src.Write(source.Frame{Data: frame, Timestamp: 21600})
	if err := client.SetReadDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if m, err := client.readMessage(); err == nil {
		t.Fatalf("收到暂停期间的帧: type=%d ts=%d", m.TypeID, m.Timestamp)
	}
	if err := client.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}

	if code := sendPlayCommand(t, server, client, 1,
		buildCommand("pause", 0, flv.Null(), flv.Bool(false), flv.Num(80))); code != "NetStream.Unpause.Notify" {
		t.Fatalf("unpause status = %q", code)
	}

	// 暂停期间播放 goroutine 阻塞在 Next 上；真实源会持续出新帧来唤醒它，
	// 这里补一帧。缓存帧按 160/200/240/280 连续补发，不重复也不跳变。
	src.Write(source.Frame{Data: frame, Timestamp: 25200})
	for _, want := range []uint32{160, 200, 240, 280} {
		m := readMessageWithin(t, client)
		if m.TypeID != msgVideo || m.Timestamp != want {
			t.Fatalf("resumed frame type/ts = %d/%d, want video/%d", m.TypeID, m.Timestamp, want)
		}
	}
}

// TestSeekLiveFails 实时推流源没有可回退的缓冲，seek 返回 Seek.Failed 且
// 不影响正在进行的播放。
func TestSeekLiveFails(t *testing.T) {
	srv := New(nil, nil, nil)
	st := srv.registry.Publish("live/seek", &stream.Stream{})
	st.SetVideoParams("H264", nil,
		[]byte{0x67, 0x42, 0x00, 0x1f}, []byte{0x68, 0xee})
	st.EnsureVideo()

	server, client := pipeConns(t)
	server.srv = srv
	startPlay(t, server, client, "live/seek")
	defer stopPlayer(t, server, client)

	readMessageWithin(t, client) // 序列头

	code := sendPlayCommand(t, server, client, 1,
		buildCommand("seek", 0, flv.Null(), flv.Num(3000)))
	if code != "NetStream.Seek.Failed" {
		t.Fatalf("seek status = %q, want NetStream.Seek.Failed", code)
	}
	if server.play == nil {
		t.Fatal("seek 失败后播放状态被清掉")
	}
}

// TestSeekFileStartsAtIDR 文件源 seek 后从目标附近的前置 IDR 重新开始，
// 序列头重发，时间戳从 seek 前最后一帧继续。
func TestSeekFileStartsAtIDR(t *testing.T) {
	video := source.NewH264Source("../../testdata/test.h264")
	srv := New(nil, video, nil)

	server, client := pipeConns(t)
	server.srv = srv
	startPlay(t, server, client, "local")
	defer stopPlayer(t, server, client)

	errc := make(chan error, 1)
	go func() {
		errc <- server.handleCommand(&Message{
			TypeID:   msgCommandAMF0,
			StreamID: 1,
			Payload:  buildCommand("seek", 0, flv.Null(), flv.Num(3000)),
		})
	}()

	// 读到 Seek.Notify 为止，同时记录 seek 前最后一条视频帧的 DTS。
	var (
		lastTS   uint32
		sawMedia bool
	)
	for {
		m := readMessageWithin(t, client)
		if m.TypeID == msgCommandAMF0 {
			cmd, err := parseCommand(m.Payload)
			if err != nil {
				t.Fatalf("parseCommand: %v", err)
			}
			if commandCode(cmd) == "NetStream.Seek.Notify" {
				break
			}
			continue
		}
		if m.TypeID != msgVideo {
			continue
		}
		tag, err := flv.ParseVideoTag(m.Payload)
		if err != nil {
			t.Fatalf("ParseVideoTag: %v", err)
		}
		if tag.PacketType == flv.AVCNALU {
			lastTS = m.Timestamp
			sawMedia = true
		}
	}
	if err := <-errc; err != nil {
		t.Fatalf("handleCommand(seek): %v", err)
	}

	// seek 后先重发序列头，再发 IDR。
	m := readMessageWithin(t, client)
	if m.TypeID != msgVideo {
		t.Fatalf("seek 后首条消息 type = %d, want video", m.TypeID)
	}
	tag, err := flv.ParseVideoTag(m.Payload)
	if err != nil {
		t.Fatalf("ParseVideoTag(seq header): %v", err)
	}
	if tag.PacketType != flv.AVCSeqHeader {
		t.Fatalf("seek 后首包 = %d, want sequence header", tag.PacketType)
	}

	m = readMessageWithin(t, client)
	if m.TypeID != msgVideo {
		t.Fatalf("seek 后首帧 type = %d, want video", m.TypeID)
	}
	tag, err = flv.ParseVideoTag(m.Payload)
	if err != nil {
		t.Fatalf("ParseVideoTag(idr): %v", err)
	}
	if tag.PacketType != flv.AVCNALU || !tag.Keyframe {
		t.Fatalf("seek 后首帧 = packet %d keyframe %v, want IDR", tag.PacketType, tag.Keyframe)
	}
	wantTS := uint32(0)
	if sawMedia {
		wantTS = lastTS + 1
	}
	if m.Timestamp != wantTS {
		t.Fatalf("seek 后 IDR ts = %d, want %d", m.Timestamp, wantTS)
	}
}

// TestRelativeClockRebase 暂停丢帧后重设基准，DTS 继续单调递增。
func TestRelativeClockRebase(t *testing.T) {
	var rc relativeClock
	if got := rc.relative(0); got != 0 {
		t.Fatalf("first = %d, want 0", got)
	}
	if got := rc.relative(40); got != 40 {
		t.Fatalf("second = %d, want 40", got)
	}
	rc.rebase(2040) // 暂停期间源已推进到 2040ms
	if got := rc.relative(2040); got != 41 {
		t.Fatalf("after rebase = %d, want 41", got)
	}
	if got := rc.relative(2080); got != 81 {
		t.Fatalf("after rebase +1 frame = %d, want 81", got)
	}
}

package srt

import (
	"bytes"
	"context"
	"testing"
	"time"

	gosrt "github.com/datarhei/gosrt"

	"flow/internal/mpegts"
	"flow/internal/stream"
)

var (
	testSPS = []byte{0x67, 0x42, 0x00, 0x1E, 0x95, 0xA8, 0x28, 0x0F, 0x00, 0x44, 0xFC, 0xB8, 0x08, 0x80}
	testPPS = []byte{0x68, 0xCE, 0x06, 0xE2}
	testASC = []byte{0x12, 0x10}

	testIDR = []byte{0x65, 0x80, 0x01, 0x02, 0x03}
	testP   = []byte{0x41, 0x80, 0x04, 0x05}
)

func TestParseStreamID(t *testing.T) {
	tests := []struct {
		id      string
		name    string
		publish bool
		wantErr bool
	}{
		{id: "publish:live/demo", name: "live/demo", publish: true},
		{id: "read:live/demo", name: "live/demo"},
		{id: "play:live/demo", name: "live/demo"},
		{id: "live/demo", name: "live/demo"},
		{id: "publish:/live/demo/", name: "live/demo", publish: true},
		{id: "#!::r=live/demo,m=publish", name: "live/demo", publish: true},
		{id: "#!::m=request,r=live/demo", name: "live/demo"},
		{id: "#!::r=live/demo,m=request", name: "live/demo"},
		{id: "  ", wantErr: true},
		{id: "publish:", wantErr: true},
		{id: "#!::m=publish", wantErr: true},
		{id: "#!::r=live/demo,m=file", wantErr: true},
	}
	for _, tt := range tests {
		got, err := parseStreamID(tt.id)
		if tt.wantErr {
			if err == nil {
				t.Errorf("parseStreamID(%q) = %+v, want error", tt.id, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseStreamID(%q): %v", tt.id, err)
			continue
		}
		if got.Name != tt.name || got.Publish != tt.publish {
			t.Errorf("parseStreamID(%q) = %+v, want name=%q publish=%v", tt.id, got, tt.name, tt.publish)
		}
	}
}

func TestCleanStreamName(t *testing.T) {
	if got := cleanStreamName("/live/demo?token=x"); got != "live/demo" {
		t.Errorf("cleanStreamName = %q, want live/demo", got)
	}
	if got := cleanStreamName(" /live/demo/ "); got != "live/demo" {
		t.Errorf("cleanStreamName = %q, want live/demo", got)
	}
}

func startTestServer(t *testing.T, passphrase string) (*Server, *stream.Registry, string) {
	t.Helper()
	reg := stream.NewRegistry()
	srv := New(reg)
	srv.SetLatency(50 * time.Millisecond)
	if passphrase != "" {
		if err := srv.SetPassphrase(passphrase); err != nil {
			t.Fatalf("SetPassphrase: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx, "127.0.0.1:0") }()

	var addr string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if a := srv.Addr(); a != nil {
			addr = a.String()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if addr == "" {
		cancel()
		t.Fatal("server did not start listening")
	}

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("Serve did not return after context cancellation")
		}
	})
	return srv, reg, addr
}

func dialTest(t *testing.T, addr, streamID, passphrase string) gosrt.Conn {
	t.Helper()
	cfg := gosrt.DefaultConfig()
	cfg.Latency = 50 * time.Millisecond
	cfg.StreamId = streamID
	cfg.Passphrase = passphrase
	conn, err := gosrt.Dial("srt", addr, cfg)
	if err != nil {
		t.Fatalf("Dial(%q): %v", streamID, err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func writeBytes(conn gosrt.Conn, data []byte) error {
	for off := 0; off < len(data); off += 1316 {
		end := min(off+1316, len(data))
		if _, err := conn.Write(data[off:end]); err != nil {
			return err
		}
	}
	return nil
}

func writeTS(t *testing.T, conn gosrt.Conn, data []byte) {
	t.Helper()
	if err := writeBytes(conn, data); err != nil {
		t.Fatalf("write TS: %v", err)
	}
}

// buildFrame 生成一帧视频（可选附加一个音频 AU）的 TS 字节。
func buildFrame(keyframe bool, pts, audioTS uint32, withAudio bool) ([]byte, error) {
	var buf bytes.Buffer
	mux := mpegts.NewMuxer(&buf)
	mux.SetVideoParams(testSPS, testPPS)
	if err := mux.SetAudioParams(testASC); err != nil {
		return nil, err
	}
	nalu := testP
	if keyframe {
		nalu = testIDR
	}
	if err := mux.WriteVideo([][]byte{nalu}, pts, pts); err != nil {
		return nil, err
	}
	if withAudio {
		if err := mux.WriteAudio(bytes.Repeat([]byte{0xAA}, 64), audioTS); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func TestPublishPlayRoundTrip(t *testing.T) {
	_, reg, addr := startTestServer(t, "")

	pub := dialTest(t, addr, "publish:live/srt-test", "")

	// 先推一个关键帧和若干 P 帧，等注册表出现参数。
	var initial bytes.Buffer
	mux := mpegts.NewMuxer(&initial)
	mux.SetVideoParams(testSPS, testPPS)
	if err := mux.SetAudioParams(testASC); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		nalu := testP
		if i == 0 {
			nalu = testIDR
		}
		pts := uint32(i) * 3000
		if err := mux.WriteVideo([][]byte{nalu}, pts, pts); err != nil {
			t.Fatal(err)
		}
	}
	if err := mux.WriteAudio(bytes.Repeat([]byte{0xAA}, 64), 0); err != nil {
		t.Fatal(err)
	}
	writeTS(t, pub, initial.Bytes())

	var st *stream.Stream
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s := reg.Get("live/srt-test"); s != nil {
			if _, ok := s.VideoParams(); ok {
				if _, aok := s.AudioParams(); aok {
					st = s
					break
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if st == nil {
		t.Fatal("stream did not appear in registry with audio+video params")
	}
	if v, _ := st.VideoParams(); v.Codec != "H264" || !bytes.Equal(v.SPS, testSPS) || !bytes.Equal(v.PPS, testPPS) {
		t.Fatalf("video params = %+v", v)
	}
	if a, _ := st.AudioParams(); a.Codec != stream.AudioAAC || !bytes.Equal(a.Config, testASC) || a.SampleRate != 44100 || a.Channels != 2 {
		t.Fatalf("audio params = %+v", a)
	}

	// 拉流端连上后继续推帧（音频订阅从当前写位置开始，需要新数据）。
	play := dialTest(t, addr, "read:live/srt-test", "")
	time.Sleep(200 * time.Millisecond)

	stop := make(chan struct{})
	writeErr := make(chan error, 1)
	go func() {
		audioTS := uint32(0)
		for pts := uint32(15000); ; pts += 3000 {
			select {
			case <-stop:
				return
			default:
			}
			data, err := buildFrame(false, pts, audioTS, true)
			if err == nil {
				err = writeBytes(pub, data)
			}
			if err != nil {
				writeErr <- err
				return
			}
			audioTS += 1024
			time.Sleep(40 * time.Millisecond)
		}
	}()
	defer close(stop)

	_ = play.SetReadDeadline(time.Now().Add(5 * time.Second))
	d := mpegts.NewDemuxer()
	var videos, audios int
	var sawKeyframe bool
	d.OnVideo = func(au mpegts.VideoAU) {
		videos++
		if au.Keyframe {
			sawKeyframe = true
		}
	}
	d.OnAudio = func(au mpegts.AudioAU) {
		if len(au.AU) == 64 && au.SampleRate == 44100 {
			audios++
		}
	}

	buf := make([]byte, 1316)
	for videos == 0 || audios == 0 {
		n, err := play.Read(buf)
		if n > 0 {
			d.Write(buf[:n])
		}
		if err != nil {
			t.Fatalf("read TS (video=%d audio=%d): %v", videos, audios, err)
		}
	}
	d.Flush()
	if videos == 0 {
		t.Fatal("no video AU received")
	}
	if !sawKeyframe {
		t.Error("no keyframe received")
	}
	if audios == 0 {
		t.Fatal("no audio AU received")
	}

	// 另一协议通过共享注册表能直接读到帧：这里只验证源里有数据。
	src := st.SourceFor("video")
	if src == nil {
		t.Fatal("video source missing")
	}
}

func TestPublishRequiresEncryption(t *testing.T) {
	_, _, addr := startTestServer(t, "testpass1234")

	cfg := gosrt.DefaultConfig()
	cfg.Latency = 50 * time.Millisecond
	cfg.StreamId = "publish:live/plain"
	conn, err := gosrt.Dial("srt", addr, cfg)
	if err == nil {
		conn.Close()
		t.Fatal("plaintext connection was accepted while a passphrase is configured")
	}
}

func TestPublishWrongPassphrase(t *testing.T) {
	_, _, addr := startTestServer(t, "testpass1234")

	cfg := gosrt.DefaultConfig()
	cfg.Latency = 50 * time.Millisecond
	cfg.StreamId = "publish:live/wrong"
	cfg.Passphrase = "wrongpass123"
	conn, err := gosrt.Dial("srt", addr, cfg)
	if err == nil {
		conn.Close()
		t.Fatal("connection with wrong passphrase was accepted")
	}
}

func TestPublishEncrypted(t *testing.T) {
	_, reg, addr := startTestServer(t, "testpass1234")

	pub := dialTest(t, addr, "publish:live/enc", "testpass1234")

	var payload bytes.Buffer
	mux := mpegts.NewMuxer(&payload)
	mux.SetVideoParams(testSPS, testPPS)
	if err := mux.WriteVideo([][]byte{testIDR}, 0, 0); err != nil {
		t.Fatal(err)
	}
	// 视频 PES 长度未知，要等下一个 PUSI 才能确定 AU 结束；再写一帧
	// 触发上一帧的解析。
	if err := mux.WriteVideo([][]byte{testP}, 3000, 3000); err != nil {
		t.Fatal(err)
	}
	writeTS(t, pub, payload.Bytes())

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s := reg.Get("live/enc"); s != nil {
			if _, ok := s.VideoParams(); ok {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("encrypted publish did not reach the registry")
}

func TestReadUnknownStreamRejected(t *testing.T) {
	_, _, addr := startTestServer(t, "")

	cfg := gosrt.DefaultConfig()
	cfg.Latency = 50 * time.Millisecond
	cfg.StreamId = "read:live/nope"
	conn, err := gosrt.Dial("srt", addr, cfg)
	if err == nil {
		conn.Close()
		t.Fatal("read of an unknown stream was accepted")
	}
}

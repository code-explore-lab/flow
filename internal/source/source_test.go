package source

import (
	"bytes"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// annexB 把若干裸 NALU 拼成 Annex-B 裸流（每个 NALU 前加 4 字节起始码）。
func annexB(nalus ...[]byte) []byte {
	var out []byte
	for _, nalu := range nalus {
		out = append(out, 0, 0, 0, 1)
		out = append(out, nalu...)
	}
	return out
}

// buildH264File 写一个最小的 Annex-B 文件：SPS / PPS / IDR + extra 个非 IDR 帧。
func buildH264File(t *testing.T, extra int) string {
	t.Helper()
	nalus := [][]byte{
		{0x67, 0x42, 0xC0, 0x1E}, // SPS
		{0x68, 0xCE, 0x3C, 0x80}, // PPS
		{0x65, 0x88, 0x84, 0x00}, // IDR
	}
	for i := 0; i < extra; i++ {
		nalus = append(nalus, []byte{0x41, 0x9A, byte(i)}) // 非 IDR slice
	}
	path := filepath.Join(t.TempDir(), "test.h264")
	if err := os.WriteFile(path, annexB(nalus...), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// buildADTSFrame 构造一帧 ADTS（AAC LC / 44100 / 立体声，无 CRC）。
func buildADTSFrame(payload []byte) []byte {
	frameLen := 7 + len(payload)
	const (
		profile    = 1
		sfi        = 4
		channelCfg = 2
		fullness   = 0x7FF
	)
	h := make([]byte, 7)
	h[0] = 0xFF
	h[1] = 0xF1
	h[2] = byte(profile<<6) | byte(sfi<<2)
	h[3] = byte(channelCfg&0x03)<<6 | byte((frameLen>>11)&0x03)
	h[4] = byte((frameLen >> 3) & 0xFF)
	h[5] = byte((frameLen&0x07)<<5) | byte((fullness>>6)&0x1F)
	h[6] = byte((fullness & 0x3F) << 2)
	return append(h, payload...)
}

// collect 通过 Reader 读到源结束（ring buffer 关闭且剩余帧读完）；超时则失败。
func collect(t *testing.T, rd *Reader[Frame]) []Frame {
	t.Helper()
	var frames []Frame
	done := make(chan struct{})
	timer := time.AfterFunc(5*time.Second, func() { close(done) })
	defer timer.Stop()
	for {
		f, ok := rd.Next(done)
		if !ok {
			return frames
		}
		frames = append(frames, f)
	}
}

// nextWithTimeout 读一帧，等待超过 1s 视为失败。
func nextWithTimeout(t *testing.T, rd *Reader[Frame]) (Frame, bool) {
	t.Helper()
	done := make(chan struct{})
	timer := time.AfterFunc(time.Second, func() { close(done) })
	defer timer.Stop()
	return rd.Next(done)
}

func TestH264SourceBroadcastsInOrder(t *testing.T) {
	src := NewH264Source(buildH264File(t, 2))

	ch, cancel := src.Subscribe()
	defer cancel()

	frames := collect(t, ch)
	if len(frames) != 5 {
		t.Fatalf("got %d frames, want 5", len(frames))
	}

	wantTypes := []uint8{7, 8, 5, 1, 1}
	wantTS := []uint32{0, 0, 0, 3600, 7200}
	for i, f := range frames {
		if got := f.Data[0] & 0x1F; got != wantTypes[i] {
			t.Fatalf("frame %d type = %d, want %d", i, got, wantTypes[i])
		}
		if f.Timestamp != wantTS[i] {
			t.Fatalf("frame %d ts = %d, want %d", i, f.Timestamp, wantTS[i])
		}
	}
	if !frames[2].Keyframe {
		t.Fatal("IDR frame should be marked Keyframe")
	}
	if frames[3].Keyframe {
		t.Fatal("non-IDR frame should not be marked Keyframe")
	}
}

func TestH264SourceLateSubscriberGetsHeadersAndLastIDR(t *testing.T) {
	// IDR 之后放 20 个非 IDR（每个 40ms），留出中途加入的时间窗
	src := NewH264Source(buildH264File(t, 20))

	first, cancelFirst := src.Subscribe()
	defer cancelFirst()

	// 等源播完开头的 SPS/PPS/IDR
	time.Sleep(120 * time.Millisecond)

	late, cancelLate := src.Subscribe()
	defer cancelLate()

	// 新订阅者应该先收到缓存的 SPS/PPS/IDR
	for i, want := range []uint8{7, 8, 5} {
		f, ok := nextWithTimeout(t, late)
		if !ok {
			t.Fatalf("timeout waiting for preload frame %d", i)
		}
		if got := f.Data[0] & 0x1F; got != want {
			t.Fatalf("preload frame %d type = %d, want %d", i, got, want)
		}
	}

	// 第一个订阅者也应该持续收到数据（单源同时喂多消费者）
	f, ok := nextWithTimeout(t, first)
	if !ok {
		t.Fatal("first subscriber got nothing")
	}
	if f.Data == nil {
		t.Fatal("empty frame")
	}
}

func TestH264SourceFromReaderLiveTimestamps(t *testing.T) {
	path := buildH264File(t, 2)
	blob, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	src := NewH264SourceFromReader(bytes.NewReader(blob))
	rd, cancel := src.Subscribe()
	defer cancel()

	frames := collect(t, rd)
	if len(frames) != 5 {
		t.Fatalf("got %d frames, want 5", len(frames))
	}
	wantTypes := []uint8{7, 8, 5, 1, 1}
	for i, f := range frames {
		if got := f.Data[0] & 0x1F; got != wantTypes[i] {
			t.Fatalf("frame %d type = %d, want %d", i, got, wantTypes[i])
		}
	}
	// 实时模式时间戳来自墙上时钟：不保证 3600 步进，但必须单调不减
	for i := 1; i < len(frames); i++ {
		if frames[i].Timestamp < frames[i-1].Timestamp {
			t.Fatalf("timestamps not monotonic: %v", frames)
		}
	}
}

// TestH264SourceSPSPPSBase64 实时源：启动前参数集为空，读到 SPS/PPS 后能
// 拿到 base64。用 io.Pipe 保持源不结束（不 close），run 停留在 NextNALU 等待
// 下一帧，cur 不会被 finish 清掉。
func TestH264SourceSPSPPSBase64(t *testing.T) {
	pr, pw := io.Pipe()
	src := NewH264SourceFromReader(pr)
	_, cancel := src.Subscribe()
	defer cancel()

	if got := src.SPSPPSBase64(); got != "" {
		t.Fatalf("before data: SPSPPSBase64 = %q, want empty", got)
	}

	sps := []byte{0x67, 0x42, 0xC0, 0x1E}
	pps := []byte{0x68, 0xCE, 0x3C, 0x80}
	idr := []byte{0x65, 0x88, 0x84, 0x00}
	// 必须写三个 NALU：流式 demux 读到下一个起始码才会吐出上一个 NALU，
	// 只写 SPS/PPS 时 PPS 会一直卡在 demux 内部缓冲区里出不来。
	go func() { _, _ = pw.Write(annexB(sps, pps, idr)) }()

	want := base64.StdEncoding.EncodeToString(sps) + "," + base64.StdEncoding.EncodeToString(pps)
	deadline := time.Now().Add(time.Second)
	for src.SPSPPSBase64() != want {
		if time.Now().After(deadline) {
			pw.Close()
			t.Fatalf("SPSPPSBase64 = %q, want %q", src.SPSPPSBase64(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
	pw.Close()
}

func TestH264SourceRestartsAfterEOF(t *testing.T) {
	src := NewH264Source(buildH264File(t, 0))

	ch1, cancel1 := src.Subscribe()
	collect(t, ch1)
	cancel1()

	// 上一轮已经结束，新订阅应该重新从头播
	ch2, cancel2 := src.Subscribe()
	defer cancel2()
	frames := collect(t, ch2)
	if len(frames) != 3 {
		t.Fatalf("second run got %d frames, want 3", len(frames))
	}
	if frames[0].Data[0]&0x1F != 7 {
		t.Fatal("second run should start from SPS")
	}
}

func TestAACSourceBroadcastsAUs(t *testing.T) {
	payloads := [][]byte{{1, 2, 3}, {4, 5}, {6}}
	var blob []byte
	for _, p := range payloads {
		blob = append(blob, buildADTSFrame(p)...)
	}
	path := filepath.Join(t.TempDir(), "test.aac")
	if err := os.WriteFile(path, blob, 0o644); err != nil {
		t.Fatal(err)
	}

	src := NewAACSource(path)
	ch, cancel := src.Subscribe()
	defer cancel()

	frames := collect(t, ch)
	if len(frames) != len(payloads) {
		t.Fatalf("got %d frames, want %d", len(frames), len(payloads))
	}
	for i, f := range frames {
		if !bytes.Equal(f.Data, payloads[i]) {
			t.Fatalf("frame %d data = %v, want %v", i, f.Data, payloads[i])
		}
		if want := uint32(i * 1024); f.Timestamp != want {
			t.Fatalf("frame %d ts = %d, want %d", i, f.Timestamp, want)
		}
	}
}

package source

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// buildSeekH264 写一个带两个 IDR 的文件：
//
//	帧 0         IDR（前面有 SPS/PPS）
//	帧 1..30     非 IDR
//	帧 31        IDR（前面再放一组 SPS/PPS）
//	帧 32..36    非 IDR
func buildSeekH264(t *testing.T) string {
	t.Helper()
	nalus := [][]byte{
		{0x67, 0x42, 0xC0, 0x1E}, // SPS
		{0x68, 0xCE, 0x3C, 0x80}, // PPS
		{0x65, 0x88, 0x84, 0x00}, // IDR（帧 0）
	}
	for i := 0; i < 30; i++ {
		nalus = append(nalus, []byte{0x41, 0x9A, byte(i)}) // 帧 1..30
	}
	nalus = append(nalus,
		[]byte{0x67, 0x42, 0xC0, 0x1E}, // SPS
		[]byte{0x68, 0xCE, 0x3C, 0x80}, // PPS
		[]byte{0x65, 0x88, 0x84, 0x01}, // IDR（帧 31）
	)
	for i := 0; i < 5; i++ {
		nalus = append(nalus, []byte{0x41, 0x9B, byte(i)}) // 帧 32..36
	}
	path := filepath.Join(t.TempDir(), "seek.h264")
	if err := os.WriteFile(path, annexB(nalus...), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestH264SubscribeFromStartsAtNearestIDR 1.5s 对应帧 37，
// 应回退到帧 31 的 IDR，并先补 SPS/PPS；时间戳按文件时间轴从 31*3600 开始。
func TestH264SubscribeFromStartsAtNearestIDR(t *testing.T) {
	src := NewH264Source(buildSeekH264(t))
	rd, cancel, err := src.SubscribeFrom(1500 * time.Millisecond)
	if err != nil {
		t.Fatalf("SubscribeFrom: %v", err)
	}
	defer cancel()

	var frames []Frame
	for i := 0; i < 5; i++ {
		f, ok := nextWithTimeout(t, rd)
		if !ok {
			t.Fatalf("timeout waiting for frame %d", i)
		}
		frames = append(frames, f)
	}

	wantTypes := []uint8{7, 8, 5, 1, 1}
	for i, f := range frames {
		if got := f.Data[0] & 0x1F; got != wantTypes[i] {
			t.Fatalf("frame %d type = %d, want %d", i, got, wantTypes[i])
		}
	}
	wantTS := uint32(31) * videoTimestampStep
	if frames[2].Timestamp != wantTS {
		t.Fatalf("IDR ts = %d, want %d", frames[2].Timestamp, wantTS)
	}
	if !frames[2].Keyframe {
		t.Fatal("IDR should be marked Keyframe")
	}
	if frames[3].Timestamp != wantTS+videoTimestampStep {
		t.Fatalf("next frame ts = %d, want %d", frames[3].Timestamp, wantTS+videoTimestampStep)
	}
}

// TestH264SubscribeFromPastEOF 目标超出文件末尾时用最后一个 IDR。
func TestH264SubscribeFromPastEOF(t *testing.T) {
	src := NewH264Source(buildSeekH264(t))
	rd, cancel, err := src.SubscribeFrom(time.Hour)
	if err != nil {
		t.Fatalf("SubscribeFrom: %v", err)
	}
	defer cancel()

	f, ok := nextWithTimeout(t, rd)
	if !ok {
		t.Fatal("timeout waiting for first frame")
	}
	if f.Data[0]&0x1F != 7 {
		t.Fatalf("first frame type = %d, want SPS(7)", f.Data[0]&0x1F)
	}
	f, ok = nextWithTimeout(t, rd) // PPS
	if !ok {
		t.Fatal("timeout waiting for PPS")
	}
	f, ok = nextWithTimeout(t, rd)
	if !ok {
		t.Fatal("timeout waiting for IDR")
	}
	if f.Data[0]&0x1F != 5 {
		t.Fatalf("frame type = %d, want IDR(5)", f.Data[0]&0x1F)
	}
	if want := uint32(31) * videoTimestampStep; f.Timestamp != want {
		t.Fatalf("IDR ts = %d, want %d", f.Timestamp, want)
	}
}

// TestH264SubscribeFromLiveRejected 实时源没有文件可定位。
func TestH264SubscribeFromLiveRejected(t *testing.T) {
	src := NewH264SourceFromReader(bytes.NewReader(nil))
	if _, _, err := src.SubscribeFrom(time.Second); err == nil {
		t.Fatal("SubscribeFrom on live source should fail")
	}
}

// TestAACSubscribeFrom 按「每帧 1024 采样」定位到目标 AU，
// 数据和时间戳都应从该帧开始。
func TestAACSubscribeFrom(t *testing.T) {
	payloads := [][]byte{{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}}
	var blob []byte
	for _, p := range payloads {
		blob = append(blob, buildADTSFrame(p)...)
	}
	path := filepath.Join(t.TempDir(), "seek.aac")
	if err := os.WriteFile(path, blob, 0o644); err != nil {
		t.Fatal(err)
	}

	src := NewAACSource(path)
	frameDur := time.Second * 1024 / 44100                 // 44100Hz 下每 AU 的时长（整数纳秒）
	rd, cancel, err := src.SubscribeFrom(frameDur * 5 / 2) // 目标 AU = 2
	if err != nil {
		t.Fatalf("SubscribeFrom: %v", err)
	}
	defer cancel()

	for i, want := range [][]byte{payloads[2], payloads[3]} {
		f, ok := nextWithTimeout(t, rd)
		if !ok {
			t.Fatalf("timeout waiting for frame %d", i)
		}
		if !bytes.Equal(f.Data, want) {
			t.Fatalf("frame %d data = %v, want %v", i, f.Data, want)
		}
		if wantTS := uint32(2+i) * 1024; f.Timestamp != wantTS {
			t.Fatalf("frame %d ts = %d, want %d", i, f.Timestamp, wantTS)
		}
	}
}

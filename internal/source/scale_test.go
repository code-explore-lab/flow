package source

import (
	"os"
	"testing"
	"time"
)

// TestScaledInterval 倍率只改变发送间隔，非法值按 1 处理，间隔下限 1ms。
func TestScaledInterval(t *testing.T) {
	tests := []struct {
		scale float64
		want  time.Duration
	}{
		{2, 20 * time.Millisecond},
		{0.5, 80 * time.Millisecond},
		{1, 40 * time.Millisecond},
		{0, 40 * time.Millisecond},
		{-2, 40 * time.Millisecond},
		{1000, time.Millisecond},
	}
	for _, tt := range tests {
		if got := scaledInterval(40*time.Millisecond, tt.scale); got != tt.want {
			t.Fatalf("scaledInterval(40ms, %v) = %s, want %s", tt.scale, got, tt.want)
		}
	}
}

// TestH264SubscribeFromScale 2x 播放时帧到达间隔减半，但时间戳步进不变。
func TestH264SubscribeFromScale(t *testing.T) {
	src := NewH264Source(buildH264File(t, 10))

	start := time.Now()
	rd, cancel, err := src.SubscribeFrom(0, 2)
	if err != nil {
		t.Fatalf("SubscribeFrom: %v", err)
	}
	defer cancel()

	var frames []Frame
	for i := 0; i < 8; i++ {
		f, ok := nextWithTimeout(t, rd)
		if !ok {
			t.Fatalf("timeout waiting for frame %d", i)
		}
		frames = append(frames, f)
	}
	elapsed := time.Since(start)

	// 1x 时 8 帧需要 7*40ms = 280ms；2x 约 140ms，留出调度余量后仍应明显更短。
	if elapsed > 240*time.Millisecond {
		t.Fatalf("2x playback took %s for 8 frames, want well under 1x (280ms)", elapsed)
	}

	wantTypes := []uint8{7, 8, 5, 1, 1, 1, 1, 1}
	wantTS := []uint32{0, 0, 0, 3600, 7200, 10800, 14400, 18000}
	for i, f := range frames {
		if got := f.Data[0] & 0x1F; got != wantTypes[i] {
			t.Fatalf("frame %d type = %d, want %d", i, got, wantTypes[i])
		}
		if f.Timestamp != wantTS[i] {
			t.Fatalf("frame %d ts = %d, want %d (倍率不应改变时间戳步进)", i, f.Timestamp, wantTS[i])
		}
	}
}

// TestAACSubscribeFromScale 音频时间戳仍按 1024 采样步进。
func TestAACSubscribeFromScale(t *testing.T) {
	payloads := [][]byte{{0}, {1}, {2}, {3}, {4}}
	var blob []byte
	for _, p := range payloads {
		blob = append(blob, buildADTSFrame(p)...)
	}
	path := t.TempDir() + "/scale.aac"
	if err := os.WriteFile(path, blob, 0o644); err != nil {
		t.Fatal(err)
	}

	src := NewAACSource(path)
	rd, cancel, err := src.SubscribeFrom(0, 2)
	if err != nil {
		t.Fatalf("SubscribeFrom: %v", err)
	}
	defer cancel()

	for i := range payloads {
		f, ok := nextWithTimeout(t, rd)
		if !ok {
			t.Fatalf("timeout waiting for frame %d", i)
		}
		if want := uint32(i) * 1024; f.Timestamp != want {
			t.Fatalf("frame %d ts = %d, want %d", i, f.Timestamp, want)
		}
	}
}

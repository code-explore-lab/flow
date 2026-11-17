package source

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// buildH264MultiSliceFile 写一个两帧、每帧两个 slice 的 Annex-B 文件：
//
//	SPS / PPS / IDR slice0 (first_mb=0) / IDR slice1 (first_mb>0)
//	/ P slice0 (first_mb=0) / P slice1 (first_mb>0)
func buildH264MultiSliceFile(t *testing.T) string {
	t.Helper()
	nalus := [][]byte{
		{0x67, 0x42, 0xC0, 0x1E}, // SPS
		{0x68, 0xCE, 0x3C, 0x80}, // PPS
		{0x65, 0x88, 0x84, 0x00}, // IDR 首 slice（first_mb_in_slice=0）
		{0x65, 0x00, 0x84, 0x00}, // IDR 后续 slice
		{0x41, 0x9A, 0x00},       // P 首 slice
		{0x41, 0x00, 0x00},       // P 后续 slice
	}
	path := filepath.Join(t.TempDir(), "multislice.h264")
	if err := os.WriteFile(path, annexB(nalus...), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestH264SourceMultiSliceSharesAUTimestamp 文件模式下同一帧的多个 slice
// 必须共用时间戳，且只有首 slice 标记 AUStart；每帧只推进一次时间戳。
func TestH264SourceMultiSliceSharesAUTimestamp(t *testing.T) {
	src := NewH264Source(buildH264MultiSliceFile(t))
	rd, cancel := src.Subscribe()
	defer cancel()

	frames := collect(t, rd)
	if len(frames) != 6 {
		t.Fatalf("got %d frames, want 6", len(frames))
	}

	wantTypes := []uint8{7, 8, 5, 5, 1, 1}
	wantTS := []uint32{0, 0, 0, 0, videoTimestampStep, videoTimestampStep}
	wantStart := []bool{false, false, true, false, true, false}
	for i, f := range frames {
		if got := f.Data[0] & 0x1F; got != wantTypes[i] {
			t.Fatalf("frame %d type = %d, want %d", i, got, wantTypes[i])
		}
		if f.Timestamp != wantTS[i] {
			t.Fatalf("frame %d ts = %d, want %d", i, f.Timestamp, wantTS[i])
		}
		if f.AUStart != wantStart[i] {
			t.Fatalf("frame %d AUStart = %v, want %v", i, f.AUStart, wantStart[i])
		}
	}
	if !frames[2].Keyframe {
		t.Fatal("IDR should be marked Keyframe")
	}
}

// TestH264SourceLiveMultiSliceSharesAUTimestamp 实时模式同样按首 slice
// 划分访问单元：一帧内的 slice 共享时间戳，不同帧时间戳严格递增。
func TestH264SourceLiveMultiSliceSharesAUTimestamp(t *testing.T) {
	path := buildH264MultiSliceFile(t)
	blob, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	src := NewH264SourceFromReader(bytes.NewReader(blob))
	rd, cancel := src.Subscribe()
	defer cancel()

	frames := collect(t, rd)
	if len(frames) != 6 {
		t.Fatalf("got %d frames, want 6", len(frames))
	}
	if frames[2].Timestamp != frames[3].Timestamp {
		t.Fatalf("IDR slices ts differ: %d vs %d", frames[2].Timestamp, frames[3].Timestamp)
	}
	if frames[4].Timestamp != frames[5].Timestamp {
		t.Fatalf("P slices ts differ: %d vs %d", frames[4].Timestamp, frames[5].Timestamp)
	}
	if frames[4].Timestamp <= frames[2].Timestamp {
		t.Fatalf("next AU ts %d must be > previous AU ts %d",
			frames[4].Timestamp, frames[2].Timestamp)
	}
	if !frames[2].AUStart || frames[3].AUStart || !frames[4].AUStart || frames[5].AUStart {
		t.Fatalf("AUStart pattern = %v %v %v %v, want true false true false",
			frames[2].AUStart, frames[3].AUStart, frames[4].AUStart, frames[5].AUStart)
	}
}

// TestH264SourceLiveBurstKeepsAllNALUs 实时源一次读入大批多 slice NALU
// （模拟 64KB 管道突发）时，ring buffer 必须全部保留，不能因为按 NALU
// 个数计容而整批覆盖。默认切片线程的 x264 每帧 7 个 slice，40 帧就有
// 280 个 NALU，超过旧容量 64。
func TestH264SourceLiveBurstKeepsAllNALUs(t *testing.T) {
	const frameCount, slicesPerFrame = 40, 7
	nalus := [][]byte{
		{0x67, 0x42, 0xC0, 0x1E}, // SPS
		{0x68, 0xCE, 0x3C, 0x80}, // PPS
	}
	for i := 0; i < frameCount; i++ {
		typ := byte(0x41) // P slice
		if i == 0 {
			typ = 0x65 // IDR
		}
		for s := 0; s < slicesPerFrame; s++ {
			firstMB := byte(0x00) // first_mb_in_slice > 0
			if s == 0 {
				firstMB = 0x88 // first_mb_in_slice = 0
			}
			nalus = append(nalus, []byte{typ, firstMB, 0x00})
		}
	}

	src := NewH264SourceFromReader(bytes.NewReader(annexB(nalus...)))
	rd, cancel := src.Subscribe()
	defer cancel()

	want := len(nalus)
	got := 0
	auStart := 0
	done := make(chan struct{})
	for {
		f, ok := rd.Next(done)
		if !ok {
			break
		}
		got++
		if f.AUStart {
			auStart++
		}
	}
	if got != want {
		t.Fatalf("read %d NALUs, want %d", got, want)
	}
	if d := rd.Dropped(); d != 0 {
		t.Fatalf("dropped %d NALUs during burst, want 0", d)
	}
	if auStart != frameCount {
		t.Fatalf("AUStart count = %d, want %d", auStart, frameCount)
	}
}

// TestH264SeekIndexCountsAUsNotSlices 多 slice 文件里 seek 的帧号必须按
// 访问单元计：目标 80ms（第 2 帧）应命中第二个 IDR，时间戳 2*3600，
// 而不是被 slice 数放大到 4*3600。
func TestH264SeekIndexCountsAUsNotSlices(t *testing.T) {
	nalus := [][]byte{
		{0x67, 0x42, 0xC0, 0x1E}, // SPS
		{0x68, 0xCE, 0x3C, 0x80}, // PPS
	}
	appendAU := func(typ, tail byte) {
		nalus = append(nalus,
			[]byte{typ, 0x88, tail}, // 首 slice
			[]byte{typ, 0x00, tail}, // 后续 slice
		)
	}
	appendAU(0x65, 0x00) // AU0: IDR
	appendAU(0x41, 0x01) // AU1: P
	appendAU(0x65, 0x02) // AU2: IDR
	appendAU(0x41, 0x03) // AU3: P

	path := filepath.Join(t.TempDir(), "seek_multislice.h264")
	if err := os.WriteFile(path, annexB(nalus...), 0o644); err != nil {
		t.Fatal(err)
	}

	src := NewH264Source(path)
	rd, cancel, err := src.SubscribeFrom(80*time.Millisecond, 1)
	if err != nil {
		t.Fatalf("SubscribeFrom: %v", err)
	}
	defer cancel()

	wantTS := uint32(2) * videoTimestampStep
	wantTypes := []uint8{7, 8, 5, 5}
	wantStart := []bool{false, false, true, false}
	for i, want := range wantTypes {
		f, ok := nextWithTimeout(t, rd)
		if !ok {
			t.Fatalf("timeout waiting for frame %d", i)
		}
		if got := f.Data[0] & 0x1F; got != want {
			t.Fatalf("frame %d type = %d, want %d", i, got, want)
		}
		if f.Timestamp != wantTS {
			t.Fatalf("frame %d ts = %d, want %d", i, f.Timestamp, wantTS)
		}
		if f.AUStart != wantStart[i] {
			t.Fatalf("frame %d AUStart = %v, want %v", i, f.AUStart, wantStart[i])
		}
	}
}

func TestNextLiveAUTS(t *testing.T) {
	cases := []struct {
		name string
		now  uint32
		last uint32
		seen bool
		want uint32
	}{
		{"first AU uses wallclock", 100, 0, false, 100},
		{"wallclock advances", 200, 100, true, 200},
		{"equal ticks force +1", 100, 100, true, 101},
		{"clock goes backwards force +1", 90, 100, true, 101},
		{"B-frame reordering still monotonic", 0, 9000, true, 9001},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := nextLiveAUTS(tc.now, tc.last, tc.seen); got != tc.want {
				t.Fatalf("nextLiveAUTS(%d, %d, %v) = %d, want %d",
					tc.now, tc.last, tc.seen, got, tc.want)
			}
		})
	}
}

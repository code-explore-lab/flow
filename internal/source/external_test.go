package source

import (
	"testing"
)

// TestExternalSourceTwoReadersSameFrames 两个先订阅的读者收到同一批帧。
func TestExternalSourceTwoReadersSameFrames(t *testing.T) {
	src := NewExternalSource()

	rd1, cancel1 := src.Subscribe()
	defer cancel1()
	rd2, cancel2 := src.Subscribe()
	defer cancel2()

	src.Write(Frame{Data: []byte{0x67, 1}, Timestamp: 0})
	src.Write(Frame{Data: []byte{0x68, 2}, Timestamp: 0})
	src.Write(Frame{Data: []byte{0x65, 3}, Timestamp: 0, Keyframe: true})
	src.Write(Frame{Data: []byte{0x41, 4}, Timestamp: 3600})
	src.Write(Frame{Data: []byte{0x41, 5}, Timestamp: 7200})
	src.Close()

	f1 := collect(t, rd1)
	f2 := collect(t, rd2)

	if len(f1) != 5 || len(f2) != 5 {
		t.Fatalf("got %d and %d frames, want 5 each", len(f1), len(f2))
	}
	for i := range f1 {
		if f1[i].Data[0] != f2[i].Data[0] {
			t.Fatalf("readers diverged at frame %d: %#x vs %#x", i, f1[i].Data[0], f2[i].Data[0])
		}
		if f1[i].Timestamp != f2[i].Timestamp {
			t.Fatalf("readers diverged on ts at frame %d: %d vs %d", i, f1[i].Timestamp, f2[i].Timestamp)
		}
	}
}

// TestExternalSourceLateJoinerGetsPreload 中途加入的订阅者先收到缓存的 SPS/PPS/IDR。
func TestExternalSourceLateJoinerGetsPreload(t *testing.T) {
	src := NewExternalSource()
	src.Write(Frame{Data: []byte{0x67, 1}, Timestamp: 0})                 // SPS
	src.Write(Frame{Data: []byte{0x68, 2}, Timestamp: 0})                 // PPS
	src.Write(Frame{Data: []byte{0x65, 3}, Timestamp: 0, Keyframe: true}) // IDR
	src.Write(Frame{Data: []byte{0x41, 4}, Timestamp: 3600})              // 非 IDR

	rd, cancel := src.Subscribe()
	defer cancel()

	want := []byte{0x67, 0x68, 0x65} // 预加载顺序：SPS、PPS、IDR
	for i, w := range want {
		f, ok := nextWithTimeout(t, rd)
		if !ok {
			t.Fatalf("preload frame %d missing", i)
		}
		if got := f.Data[0]; got != w {
			t.Fatalf("preload frame %d = %#x, want %#x", i, got, w)
		}
	}

	// 预加载读完，读者从当前写位置开始；写一帧新数据它应当立刻收到。
	src.Write(Frame{Data: []byte{0x41, 9}, Timestamp: 10800})
	f, ok := nextWithTimeout(t, rd)
	if !ok {
		t.Fatal("expected a live frame after preload")
	}
	if f.Data[0] != 0x41 || f.Timestamp != 10800 {
		t.Fatalf("live frame = %#x ts=%d, want 0x41 ts=10800", f.Data[0], f.Timestamp)
	}
}

// TestExternalAudioSourceSkipsParamSetPreload 音频 AU 首字节可能恰好是
// SPS/PPS/IDR 的类型值，但不能被当成 NAL 缓存并预加载给新订阅者。
func TestExternalAudioSourceSkipsParamSetPreload(t *testing.T) {
	src := NewExternalAudioSource()
	src.Write(Frame{Data: []byte{0x67, 1}}) // 视频里是 SPS
	src.Write(Frame{Data: []byte{0x68, 2}}) // 视频里是 PPS
	src.Write(Frame{Data: []byte{0x65, 3}}) // 视频里是 IDR

	rd, cancel := src.Subscribe()
	defer cancel()
	src.Write(Frame{Data: []byte{0x21, 4}, Timestamp: 1024})

	f, ok := nextWithTimeout(t, rd)
	if !ok {
		t.Fatal("expected a live audio frame")
	}
	if f.Data[0] != 0x21 {
		t.Fatalf("first frame = %#x, want live AU 0x21 (no preload)", f.Data[0])
	}
}

// TestExternalSourceSlowReaderDrops 慢读者落后时跳过被覆盖的帧，Dropped 记录数量。
func TestExternalSourceSlowReaderDrops(t *testing.T) {
	src := NewExternalSource()
	rd, cancel := src.Subscribe()
	defer cancel()

	const n = 70 // 容量 64，最后只保留最近 64 帧（seq 6..69）
	for i := 0; i < n; i++ {
		src.Write(Frame{Data: []byte{0x41, byte(i)}, Timestamp: uint32(i)})
	}

	f, ok := nextWithTimeout(t, rd)
	if !ok {
		t.Fatal("expected a frame")
	}
	if f.Timestamp != 6 {
		t.Fatalf("first frame ts = %d, want 6 (oldest retained)", f.Timestamp)
	}
	if d := rd.Dropped(); d != 6 {
		t.Fatalf("Dropped = %d, want 6", d)
	}
}

package source

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"flow/internal/media"
)

// 不预加载 SPS/PPS/最近 GOP：订阅后写进来的第一帧就是第一帧。
func TestExternalSourceSubscribeLiveSkipsPreload(t *testing.T) {
	src := NewExternalSource()
	src.Write(Frame{Data: []byte{0x67, 0x42, 0xC0, 0x1E}}) // SPS
	src.Write(Frame{Data: []byte{0x68, 0xCE, 0x3C, 0x80}}) // PPS
	src.Write(Frame{Data: []byte{0x65, 0x88, 0x84, 0x00}, Keyframe: true, AUStart: true})

	rd, cancel := src.SubscribeLive()
	defer cancel()

	live := []byte{0x41, 0x9A, 0x01}
	src.Write(Frame{Data: live, Timestamp: 3600, AUStart: true})

	f, ok := nextWithTimeout(t, rd)
	if !ok {
		t.Fatal("no frame")
	}
	if len(f.Data) != len(live) || f.Data[0] != 0x41 {
		t.Fatalf("first frame = % x, want live non-IDR slice % x", f.Data, live)
	}
}

// 普通 Subscribe 仍然应该预加载参数集，作为对照。
func TestExternalSourceSubscribeStillPreloads(t *testing.T) {
	src := NewExternalSource()
	sps := []byte{0x67, 0x42, 0xC0, 0x1E}
	idr := []byte{0x65, 0x88, 0x84, 0x00}
	src.Write(Frame{Data: sps})
	src.Write(Frame{Data: []byte{0x68, 0xCE, 0x3C, 0x80}})
	src.Write(Frame{Data: idr, Keyframe: true, AUStart: true})

	rd, cancel := src.Subscribe()
	defer cancel()

	f, ok := nextWithTimeout(t, rd)
	if !ok {
		t.Fatal("no frame")
	}
	if f.Data[0]&0x1F != 7 {
		t.Fatalf("first preload frame type = %d, want SPS(7)", f.Data[0]&0x1F)
	}
}

func TestExternalSourceSubscribeLiveClosed(t *testing.T) {
	src := NewExternalSource()
	src.Close()
	rd, cancel := src.SubscribeLive()
	defer cancel()
	if _, ok := nextWithTimeout(t, rd); ok {
		t.Fatal("closed source returned a frame")
	}
}

func TestH264SourceSubscribeLiveStartsAtBeginning(t *testing.T) {
	src := NewH264Source(buildH264File(t, 2))
	rd, cancel := src.SubscribeLive()
	defer cancel()

	frames := collect(t, rd)
	if len(frames) < 3 {
		t.Fatalf("frames = %d, want at least 3", len(frames))
	}
	want := []uint8{7, 8, 5}
	for i, typ := range want {
		got, err := media.NALType(frames[i].Data)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if got != typ {
			t.Errorf("frame %d type = %d, want %d", i, got, typ)
		}
	}
}

// 第二个订阅者用 SubscribeLive 时不会收到预热的旧 SPS/PPS/IDR。
func TestH264SourceSubscribeLiveSkipsPreload(t *testing.T) {
	pr, pw := io.Pipe()
	defer pr.Close()
	src := NewH264SourceFromReader(pr)

	first, cancelFirst := src.Subscribe()
	defer cancelFirst()

	sps := []byte{0x67, 0x42, 0xC0, 0x1E}
	pps := []byte{0x68, 0xCE, 0x3C, 0x80}
	idr := []byte{0x65, 0x88, 0x84, 0x00}
	p1 := []byte{0x41, 0x9A, 0x00}
	p2 := []byte{0x41, 0x9A, 0x01}
	if _, err := pw.Write(annexB(sps, pps, idr, p1, p2)); err != nil {
		t.Fatalf("write: %v", err)
	}

	// 先让源把 SPS/PPS/IDR 写进 ring，并让 run 缓存 idr。
	seen := map[uint8]bool{}
	for i := 0; i < 10 && (!seen[7] || !seen[8] || !seen[5]); i++ {
		f, ok := nextWithTimeout(t, first)
		if !ok {
			t.Fatal("first subscriber ended early")
		}
		if typ, err := media.NALType(f.Data); err == nil {
			seen[typ] = true
		}
	}
	if !seen[7] || !seen[8] || !seen[5] {
		t.Fatalf("first subscriber did not see SPS/PPS/IDR: %v", seen)
	}

	second, cancelSecond := src.SubscribeLive()
	defer cancelSecond()
	p3 := []byte{0x41, 0x9A, 0x02}
	if _, err := pw.Write(annexB(p3)); err != nil {
		t.Fatalf("write p3: %v", err)
	}
	f, ok := nextWithTimeout(t, second)
	if !ok {
		t.Fatal("second subscriber got no frame")
	}
	typ, err := media.NALType(f.Data)
	if err != nil {
		t.Fatalf("NALType: %v", err)
	}
	if typ == 7 || typ == 8 || typ == 5 {
		t.Fatalf("second subscriber got preloaded NALU type %d, want live slice", typ)
	}
}

func TestAACSourceSubscribeLive(t *testing.T) {
	var blob []byte
	for i := 0; i < 3; i++ {
		blob = append(blob, buildADTSFrame([]byte{0x21, byte(i)})...)
	}
	path := filepath.Join(t.TempDir(), "test.aac")
	if err := os.WriteFile(path, blob, 0o644); err != nil {
		t.Fatal(err)
	}
	src := NewAACSource(path)
	rd, cancel := src.SubscribeLive()
	defer cancel()
	frames := collect(t, rd)
	if len(frames) != 3 {
		t.Fatalf("frames = %d, want 3", len(frames))
	}
}

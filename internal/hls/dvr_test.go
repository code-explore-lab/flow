package hls

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"flow/internal/source"
)

func segmentFiles(t *testing.T, dir string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "video", "seg-*.m4s"))
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func TestPackagerDVRWindowOnDisk(t *testing.T) {
	dir := t.TempDir()
	spec, _, vs, as := newStreamSpec(t, "live/dvr")
	p, err := NewPackager(spec, Options{
		SegmentTarget: time.Second,
		PartTarget:    500 * time.Millisecond,
		Window:        3, // DVR 模式下应被忽略
		DVRWindow:     5 * time.Second,
		DVRDir:        dir,
		Logf:          t.Logf,
	})
	if err != nil {
		t.Fatalf("NewPackager: %v", err)
	}
	if p.dvrRoot == "" {
		t.Fatal("dvrRoot is empty")
	}

	writeVideoFrames(vs, 0, 250, 25) // 10s，GOP 1s
	writeAudioFrames(as, 0, 430)     // ≈10s
	vs.Close()
	as.Close()
	waitClosed(t, p)

	snap := p.Video().Snapshot()
	if len(snap.Segments) <= 3 {
		t.Fatalf("segments = %d, DVR window should retain more than -hls-window", len(snap.Segments))
	}
	var total float64
	for _, s := range snap.Segments {
		total += s.Duration
	}
	if total > 5.01 {
		t.Errorf("retained duration = %.3fs, want <= 5s", total)
	}
	if snap.Segments[0].MSN == 0 {
		t.Errorf("oldest segments were not evicted (first MSN = 0)")
	}

	files := segmentFiles(t, p.dvrRoot)
	if len(files) != len(snap.Segments) {
		t.Errorf("disk segments = %d, playlist segments = %d", len(files), len(snap.Segments))
	}
	if data, ok := p.Video().Data(snap.Segments[0].URI); !ok || len(data) == 0 {
		t.Errorf("oldest retained segment not readable from disk: %v", ok)
	}

	root := p.dvrRoot
	p.Close()
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Errorf("dvr dir still exists after Close: %v", err)
	}
}

func TestPackagerDVRMaxBytes(t *testing.T) {
	dir := t.TempDir()
	spec, _, vs, as := newStreamSpec(t, "live/dvr-bytes")
	p, err := NewPackager(spec, Options{
		SegmentTarget: time.Second,
		PartTarget:    500 * time.Millisecond,
		Window:        100, // 让 DVRMaxBytes 触发回收
		DVRWindow:     time.Minute,
		DVRDir:        dir,
		DVRMaxBytes:   20_000,
		Logf:          t.Logf,
	})
	if err != nil {
		t.Fatalf("NewPackager: %v", err)
	}
	defer p.Close()

	payload := bytes.Repeat([]byte{0xAA}, 500)
	for i := 0; i < 75; i++ {
		key := i%25 == 0
		nalu := []byte{0x41, 0x9A, byte(i)}
		if key {
			nalu[0] = 0x65
		}
		nalu = append(nalu, payload...)
		vs.Write(source.Frame{
			Data:      nalu,
			Timestamp: uint32(i) * 3600,
			Keyframe:  key,
			AUStart:   true,
			AUEnd:     true,
		})
	}
	writeAudioFrames(as, 0, 50)
	vs.Close()
	as.Close()
	waitClosed(t, p)

	snap := p.Video().Snapshot()
	if len(snap.Segments) == 0 {
		t.Fatal("no segments")
	}
	if len(snap.Segments) >= 3 {
		t.Errorf("segments = %d, DVRMaxBytes should evict below 3", len(snap.Segments))
	}
	if got := p.Video().StoredBytes(); got <= 0 || got > 30_000 {
		t.Errorf("stored bytes = %d, want (0,30000]", got)
	}
}

func TestManagerKeepsDVRPackagerAlive(t *testing.T) {
	spec, _, vs, as := newStreamSpec(t, "live/dvr-manager")
	m := NewManager(Options{
		SegmentTarget: time.Second,
		PartTarget:    500 * time.Millisecond,
		Window:        3,
		DVRWindow:     10 * time.Second,
		DVRDir:        t.TempDir(),
		Logf:          t.Logf,
	}, time.Second)
	defer m.Close()

	p := m.Get(spec)
	if p == nil {
		t.Fatal("packager is nil")
	}
	writeVideoFrames(vs, 0, 50, 25)
	writeAudioFrames(as, 0, 60)

	// 源仍活跃时，空闲回收不能清掉 DVR 窗口（观众暂停后还要能回看）。
	m.Sweep(time.Now().Add(time.Hour))
	if got := m.Len(); got != 1 {
		t.Fatalf("packagers = %d after idle sweep with live DVR source, want 1", got)
	}

	vs.Close()
	as.Close()
	waitClosed(t, p)

	m.Sweep(time.Now().Add(2 * time.Hour))
	if got := m.Len(); got != 0 {
		t.Fatalf("packagers = %d after source ended, want 0", got)
	}
}

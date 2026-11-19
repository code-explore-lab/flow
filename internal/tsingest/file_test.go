package tsingest

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"flow/internal/stream"
)

func writeTempTS(t *testing.T, frames int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.ts")
	if err := os.WriteFile(path, buildTS(t, frames), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func TestRunFileLoopsWithMonotonicTimestamps(t *testing.T) {
	path := writeTempTS(t, 3)
	reg := stream.NewRegistry()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- RunFile(ctx, reg, "live/file", path, FileOptions{Loop: true, Rate: 50})
	}()

	if !waitFor(t, 2*time.Second, func() bool { return reg.Get("live/file") != nil }) {
		t.Fatal("stream was not published")
	}
	st := reg.Get("live/file")
	if !waitFor(t, 2*time.Second, func() bool { return st.SourceFor("video") != nil }) {
		t.Fatal("video source was not created")
	}

	frames := collectFrames(st.SourceFor("video"), 20, 2*time.Second)
	if len(frames) < 10 {
		t.Fatalf("got %d video frames, want at least 10", len(frames))
	}

	// 一圈的长度 = 最后 PTS - 首 PTS + 一帧步长 = 2*3600 + 3600。
	const loopDur = 3 * testFrameDur
	for i := 1; i < len(frames); i++ {
		if frames[i].Timestamp < frames[i-1].Timestamp {
			t.Fatalf("timestamp went backwards at frame %d: %d → %d",
				i, frames[i-1].Timestamp, frames[i].Timestamp)
		}
	}
	span := frames[len(frames)-1].Timestamp - frames[0].Timestamp
	if span < uint32(2*loopDur) {
		t.Fatalf("timestamp span %d is shorter than two loops (%d)", span, 2*loopDur)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunFile: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RunFile did not stop after ctx cancel")
	}
	if reg.Get("live/file") != nil {
		t.Error("stream still published after shutdown")
	}
}

func TestRunFileStopsAtEOF(t *testing.T) {
	path := writeTempTS(t, 2)
	reg := stream.NewRegistry()

	done := make(chan error, 1)
	go func() {
		done <- RunFile(context.Background(), reg, "live/file", path, FileOptions{Rate: 50})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunFile: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunFile did not stop at EOF")
	}
	if reg.Get("live/file") != nil {
		t.Error("stream still published after EOF")
	}
}

func TestRunFileMissingFile(t *testing.T) {
	reg := stream.NewRegistry()
	err := RunFile(context.Background(), reg, "live/file",
		filepath.Join(t.TempDir(), "missing.ts"), FileOptions{})
	if err == nil {
		t.Fatal("expected an error for a missing file")
	}
	if reg.Get("live/file") != nil {
		t.Error("stream was published despite the file error")
	}
}

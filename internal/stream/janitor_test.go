package stream

import (
	"sync"
	"testing"
	"time"

	"flow/internal/source"
)

// TestJanitorReapsIdleStream 验证超过 idle 没有媒体写入的流会被摘除，
// 并触发 Unpublished 事件。
func TestJanitorReapsIdleStream(t *testing.T) {
	reg := NewRegistry()
	var mu sync.Mutex
	var events []Event
	reg.SetObserver(func(e Event, name string, _ *Stream) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})

	st := &Stream{}
	if _, err := reg.Publish("live/idle", st); err != nil {
		t.Fatal(err)
	}

	const idle = 10 * time.Millisecond
	j := NewJanitor(reg, idle)
	j.logger = func(string, ...any) {}
	time.Sleep(3 * idle)

	reaped := j.SweepOnce()
	if len(reaped) != 1 || reaped[0] != "live/idle" {
		t.Fatalf("reaped = %v, want [live/idle]", reaped)
	}
	if got := reg.Get("live/idle"); got != nil {
		t.Fatal("stream still registered after reap")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 || events[0] != EventPublished || events[1] != EventUnpublished {
		t.Fatalf("events = %v, want [published unpublished]", events)
	}
}

// TestJanitorKeepsActiveStream 验证刚写入过媒体的流不会被误杀。
func TestJanitorKeepsActiveStream(t *testing.T) {
	reg := NewRegistry()
	st := &Stream{}
	if _, err := reg.Publish("live/active", st); err != nil {
		t.Fatal(err)
	}
	track := st.EnsureVideo()
	track.Src.Write(source.Frame{Data: []byte{0x65}, Timestamp: 90000})

	const idle = 20 * time.Millisecond
	j := NewJanitor(reg, idle)
	j.logger = func(string, ...any) {}
	time.Sleep(3 * idle)
	// 重新写一帧，模拟持续推流。
	track.Src.Write(source.Frame{Data: []byte{0x41}, Timestamp: 93000})

	if got := j.SweepOnce(); len(got) != 0 {
		t.Fatalf("reaped active stream: %v", got)
	}
	if reg.Get("live/active") != st {
		t.Fatal("active stream disappeared")
	}
}

// TestJanitorSkipsIdleExemptStream 验证回源/-pull 的流不会被空闲回收。
func TestJanitorSkipsIdleExemptStream(t *testing.T) {
	reg := NewRegistry()
	st := &Stream{}
	st.SetIdleExempt(true)
	if _, err := reg.Publish("live/relay", st); err != nil {
		t.Fatal(err)
	}

	const idle = 10 * time.Millisecond
	j := NewJanitor(reg, idle)
	j.logger = func(string, ...any) {}
	time.Sleep(3 * idle)

	if got := j.SweepOnce(); len(got) != 0 {
		t.Fatalf("reaped exempt stream: %v", got)
	}
	if !st.IdleExempt() || reg.Get("live/relay") != st {
		t.Fatal("exempt stream was removed or flag lost")
	}
}

// TestJanitorClosesSource 验证回收会关闭帧源，下游 reader 读到流结束。
func TestJanitorClosesSource(t *testing.T) {
	reg := NewRegistry()
	st := &Stream{}
	if _, err := reg.Publish("live/ending", st); err != nil {
		t.Fatal(err)
	}
	track := st.EnsureVideo()
	rd, cancel := track.Src.Subscribe()
	defer cancel()

	const idle = 10 * time.Millisecond
	j := NewJanitor(reg, idle)
	j.logger = func(string, ...any) {}
	time.Sleep(3 * idle)
	if got := j.SweepOnce(); len(got) != 1 {
		t.Fatalf("reaped = %v, want one stream", got)
	}
	expectReaderEnded(t, rd)
}

// TestReapIgnoresStalePointer 验证旧流晚到的清理不会摘掉同名新流。
func TestReapIgnoresStalePointer(t *testing.T) {
	reg := NewRegistry()
	old := &Stream{}
	if _, err := reg.Publish("live/x", old); err != nil {
		t.Fatal(err)
	}
	fresh := &Stream{}
	if _, err := reg.Publish("live/x", fresh); err != nil {
		t.Fatal(err)
	}

	const idle = 10 * time.Millisecond
	time.Sleep(3 * idle)
	if reg.Reap("live/x", old, idle) {
		t.Fatal("Reap removed a stale pointer")
	}
	if reg.Get("live/x") != fresh {
		t.Fatal("fresh stream was affected by stale Reap")
	}
}

// TestSnapshotSorted 验证 Snapshot 按流名排序，心跳批次稳定。
func TestSnapshotSorted(t *testing.T) {
	reg := NewRegistry()
	for _, name := range []string{"live/b", "live/a", "live/c"} {
		if _, err := reg.Publish(name, &Stream{}); err != nil {
			t.Fatal(err)
		}
	}
	snap := reg.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("snapshot len = %d, want 3", len(snap))
	}
	for i, want := range []string{"live/a", "live/b", "live/c"} {
		if snap[i].Name != want {
			t.Fatalf("snapshot[%d] = %q, want %q", i, snap[i].Name, want)
		}
	}
}

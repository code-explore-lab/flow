package origin

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"flow/internal/stream"
)

// newTestManager 把重连退避调小，避免测试等满 1s/30s。
func newTestManager(ctx context.Context, reg *stream.Registry, specs []Spec, timeout time.Duration) *Manager {
	m := NewManager(ctx, reg, nil, specs, timeout)
	m.minBackoff = 20 * time.Millisecond
	m.maxBackoff = 50 * time.Millisecond
	return m
}

// deadOrigin 返回一个刚关闭的本地端口，连接会被拒绝。
func deadOrigin(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func activeCount(m *Manager) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.active)
}

// waitGone 等 puller 退出（ctx 取消或上游明确不存在后）。
func waitGone(t *testing.T, m *Manager) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if activeCount(m) == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("puller still active after %s", 2*time.Second)
}

func TestEnsureRegistryHit(t *testing.T) {
	reg := stream.NewRegistry()
	st := &stream.Stream{Name: "live/src"}
	if _, err := reg.Publish("live/src", st); err != nil {
		t.Fatalf("publish: %v", err)
	}
	m := newTestManager(context.Background(), reg, nil, 100*time.Millisecond)

	if got := m.Ensure(context.Background(), "/live/src/"); got != st {
		t.Fatalf("Ensure = %p, want registry stream %p", got, st)
	}
	if n := activeCount(m); n != 0 {
		t.Errorf("started %d puller(s) for a local stream", n)
	}
}

func TestEnsureNoSpecs(t *testing.T) {
	m := newTestManager(context.Background(), nil, nil, 100*time.Millisecond)
	if got := m.Ensure(context.Background(), "live/src"); got != nil {
		t.Fatalf("Ensure = %p, want nil without specs", got)
	}
}

func TestEnsureNilManager(t *testing.T) {
	var m *Manager
	if got := m.Ensure(context.Background(), "live/src"); got != nil {
		t.Fatalf("nil Manager.Ensure = %p, want nil", got)
	}
}

func TestMatchSpecsExplicitFirst(t *testing.T) {
	a := Spec{Scheme: "rtmp", Host: "h:1935", Base: "a"}
	b := Spec{Scheme: "http", Host: "h:80"}
	cam := Spec{Local: "cam", Scheme: "rtsp", Host: "h:554", Base: "x", Raw: "rtsp://h:554/x"}
	other := Spec{Local: "other", Scheme: "rtmp", Host: "h:1935", Base: "y", Raw: "rtmp://h:1935/y"}
	m := NewManager(context.Background(), nil, nil, []Spec{a, other, b, cam}, time.Second)

	got := m.matchSpecs("live/cam")
	want := []Spec{cam, a, b}
	if len(got) != len(want) {
		t.Fatalf("matchSpecs = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("spec #%d = %+v, want %+v", i, got[i], want[i])
		}
	}
	if got := m.matchSpecs("live/other"); len(got) != 3 || got[0] != other {
		t.Errorf("explicit mapping didn't match the leaf: %+v", got)
	}
	if got := m.matchSpecs("live/unknown"); len(got) != 2 {
		t.Errorf("unrelated name should only match wildcards, got %+v", got)
	}
}

func TestEnsureTimesOutThenKeepsRetrying(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	specs, err := ParseSpecs("rtsp://" + deadOrigin(t) + "/live")
	if err != nil {
		t.Fatalf("ParseSpecs: %v", err)
	}
	m := newTestManager(ctx, nil, specs, 200*time.Millisecond)

	start := time.Now()
	got := m.Ensure(ctx, "live/src")
	elapsed := time.Since(start)
	if got != nil {
		t.Fatalf("Ensure = %p, want nil for an unreachable upstream", got)
	}
	if elapsed < 150*time.Millisecond {
		t.Errorf("Ensure returned after %s, want it to wait for the timeout", elapsed)
	}
	// 瞬时故障应当留在后台重试，而不是丢掉 puller。
	if n := activeCount(m); n != 1 {
		t.Fatalf("active pullers = %d, want 1 (retrying)", n)
	}

	cancel()
	waitGone(t, m)
}

func TestEnsureConcurrentAcquireIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	specs, err := ParseSpecs("rtsp://" + deadOrigin(t) + "/live")
	if err != nil {
		t.Fatalf("ParseSpecs: %v", err)
	}
	m := newTestManager(ctx, nil, specs, 100*time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got := m.Ensure(ctx, "live/src"); got != nil {
				t.Errorf("Ensure = %p, want nil", got)
			}
		}()
	}
	wg.Wait()
	if n := activeCount(m); n != 1 {
		t.Errorf("active pullers = %d, want exactly 1 for concurrent requests", n)
	}
	cancel()
	waitGone(t, m)
}

func TestEnsureUpstreamNotFoundFailsFast(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		http.Error(w, "no such stream", http.StatusNotFound)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	specs, err := ParseSpecs("http://" + srv.Listener.Addr().String() + "/live")
	if err != nil {
		t.Fatalf("ParseSpecs: %v", err)
	}
	// 超时给足 5s：404 必须靠「立即失败」返回，而不是等超时。
	m := newTestManager(ctx, nil, specs, 5*time.Second)

	start := time.Now()
	got := m.Ensure(ctx, "live/ghost")
	elapsed := time.Since(start)
	if got != nil {
		t.Fatalf("Ensure = %p, want nil for an upstream 404", got)
	}
	if elapsed > time.Second {
		t.Errorf("Ensure took %s; upstream 404 should fail fast", elapsed)
	}
	waitGone(t, m)
	if n := atomic.LoadInt32(&hits); n != 1 {
		t.Errorf("upstream got %d request(s), want 1 (no retries after 404)", n)
	}
}

func TestEnsureContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	specs, err := ParseSpecs("rtsp://" + deadOrigin(t) + "/live")
	if err != nil {
		t.Fatalf("ParseSpecs: %v", err)
	}
	m := newTestManager(ctx, nil, specs, 5*time.Second)

	cancel()
	start := time.Now()
	if got := m.Ensure(ctx, "live/src"); got != nil {
		t.Fatalf("Ensure = %p, want nil after ctx cancel", got)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Ensure ignored ctx cancellation (%s)", elapsed)
	}
	waitGone(t, m)
}

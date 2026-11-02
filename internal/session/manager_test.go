package session

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewSessionRegistersInMap(t *testing.T) {
	m := NewManager(DefaultTimeout)
	s := m.NewSession("192.168.1.10")

	if s.ID != "sess-1" {
		t.Fatalf("ID = %q, want sess-1", s.ID)
	}
	if s.RemoteIP != "192.168.1.10" {
		t.Fatalf("RemoteIP = %q", s.RemoteIP)
	}
	if s.State() != StateInit {
		t.Fatalf("State = %v, want Init", s.State())
	}
	got, ok := m.Get("sess-1")
	if !ok || got != s {
		t.Fatalf("Get(sess-1) = %v, %v; want the same session", got, ok)
	}
	if m.Len() != 1 {
		t.Fatalf("Len = %d, want 1", m.Len())
	}

	// 第二个会话 ID 递增，且互不覆盖
	s2 := m.NewSession("10.0.0.1")
	if s2.ID != "sess-2" {
		t.Fatalf("second ID = %q, want sess-2", s2.ID)
	}
	if m.Len() != 2 {
		t.Fatalf("Len = %d, want 2", m.Len())
	}
}

func TestDelete(t *testing.T) {
	m := NewManager(DefaultTimeout)
	s := m.NewSession("127.0.0.1")

	if !m.Delete(s.ID) {
		t.Fatal("Delete returned false for existing session")
	}
	if m.Delete(s.ID) {
		t.Fatal("Delete returned true for already removed session")
	}
	if _, ok := m.Get(s.ID); ok {
		t.Fatal("session still visible after Delete")
	}
	if m.Len() != 0 {
		t.Fatalf("Len = %d, want 0", m.Len())
	}
}

func TestSessionContextCancelsOnClose(t *testing.T) {
	m := NewManager(DefaultTimeout)
	s := m.NewSession("127.0.0.1")

	// 模拟 RTSP 层为一次 PLAY 派生的子 context。
	playCtx, cancel := context.WithCancel(s.Context())
	defer cancel()

	s.Close()

	select {
	case <-playCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("child context was not canceled after Session.Close")
	}
	if s.Done() != s.Context().Done() {
		t.Fatal("Done and Context should share the same lifecycle")
	}
}

func TestSweepExpiresIdleSession(t *testing.T) {
	m := NewManager(DefaultTimeout)
	s := m.NewSession("127.0.0.1")

	var closed int32
	s.OnClose = func() { atomic.AddInt32(&closed, 1) }

	expired := m.Sweep(time.Now().Add(DefaultTimeout + time.Second))
	if len(expired) != 1 || expired[0] != s {
		t.Fatalf("Sweep = %v, want exactly this session", expired)
	}
	if m.Len() != 0 {
		t.Fatalf("Len = %d after sweep, want 0", m.Len())
	}
	if got := atomic.LoadInt32(&closed); got != 1 {
		t.Fatalf("OnClose called %d times, want 1", got)
	}
	if s.State() != StateTeardown {
		t.Fatalf("State = %v, want Teardown", s.State())
	}
}

func TestSweepKeepsFreshSession(t *testing.T) {
	m := NewManager(DefaultTimeout)
	s := m.NewSession("127.0.0.1")

	// 刚创建就扫，不应该回收
	if got := m.Sweep(time.Now()); len(got) != 0 {
		t.Fatalf("Sweep reclaimed fresh session: %v", got)
	}
	if m.Len() != 1 {
		t.Fatalf("Len = %d, want 1", m.Len())
	}

	// 但 timeout 之后还是要回收
	if got := m.Sweep(time.Now().Add(DefaultTimeout + time.Second)); len(got) != 1 {
		t.Fatalf("Sweep after timeout = %v, want 1 session", got)
	}
	_ = s
}

func TestTouchRTCPKeepsSessionAlive(t *testing.T) {
	m := NewManager(DefaultTimeout)
	s := m.NewSession("127.0.0.1")

	s.TouchRTCP()

	// 收到 RTCP 后紧接着扫，不能回收
	if got := m.Sweep(time.Now()); len(got) != 0 {
		t.Fatalf("Sweep reclaimed session right after RTCP: %v", got)
	}
	// 从最后一次 RTCP 起算超过 timeout 才回收
	if got := m.Sweep(time.Now().Add(DefaultTimeout + time.Second)); len(got) != 1 {
		t.Fatalf("Sweep after timeout = %v, want 1 session", got)
	}
}

func TestTouchRTSPKeepsSessionAlive(t *testing.T) {
	m := NewManager(DefaultTimeout)
	s := m.NewSession("127.0.0.1")

	s.TouchRTSP()

	if got := m.Sweep(time.Now()); len(got) != 0 {
		t.Fatalf("Sweep reclaimed session right after RTSP request: %v", got)
	}
}

func TestExpiredBoundary(t *testing.T) {
	m := NewManager(DefaultTimeout)
	s := m.NewSession("127.0.0.1")

	base := s.CreatedAt
	if s.Expired(base.Add(DefaultTimeout-time.Second), DefaultTimeout) {
		t.Fatal("Expired = true before timeout")
	}
	if !s.Expired(base.Add(DefaultTimeout+time.Second), DefaultTimeout) {
		t.Fatal("Expired = false after timeout")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	m := NewManager(DefaultTimeout)
	s := m.NewSession("127.0.0.1")

	var closed int32
	s.OnClose = func() { atomic.AddInt32(&closed, 1) }

	s.Close()
	s.Close()

	if got := atomic.LoadInt32(&closed); got != 1 {
		t.Fatalf("OnClose called %d times, want 1", got)
	}
	if s.State() != StateTeardown {
		t.Fatalf("State = %v, want Teardown", s.State())
	}
	select {
	case <-s.Done():
	default:
		t.Fatal("Done() is not closed after Close()")
	}
}

func TestExpireCallbackCanTouchManager(t *testing.T) {
	// Sweep 在释放 Manager 锁之后才回调，OnExpire 里再操作 Manager 不能死锁。
	m := NewManager(DefaultTimeout)
	m.NewSession("127.0.0.1")
	m.OnExpire = func(s *Session) { m.Delete(s.ID) }

	done := make(chan struct{})
	go func() {
		m.Sweep(time.Now().Add(DefaultTimeout + time.Second))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Sweep deadlocked in OnExpire callback")
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	m := NewManagerWithInterval(DefaultTimeout, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())

	stopped := make(chan struct{})
	go func() {
		m.Run(ctx)
		close(stopped)
	}()

	cancel()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}

func TestRunReclaimsIdleSessions(t *testing.T) {
	m := NewManagerWithInterval(20*time.Millisecond, 5*time.Millisecond)
	var expired int32
	m.OnExpire = func(*Session) { atomic.AddInt32(&expired, 1) }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	m.NewSession("127.0.0.1")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if m.Len() == 0 && atomic.LoadInt32(&expired) == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("session not reclaimed: Len=%d expired=%d", m.Len(), atomic.LoadInt32(&expired))
}

func TestCloseAll(t *testing.T) {
	m := NewManager(DefaultTimeout)
	var closed int32
	for i := 0; i < 3; i++ {
		s := m.NewSession("127.0.0.1")
		s.OnClose = func() { atomic.AddInt32(&closed, 1) }
	}

	m.CloseAll()

	if m.Len() != 0 {
		t.Fatalf("Len = %d after CloseAll, want 0", m.Len())
	}
	if got := atomic.LoadInt32(&closed); got != 3 {
		t.Fatalf("OnClose called %d times, want 3", got)
	}
}

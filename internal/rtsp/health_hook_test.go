package rtsp

// O-4：验证会话超时回收会把会话异常上报给健康聚合器。
// 正常的 TEARDOWN 不走 Sweep，因此不会触发这个钩子。

import (
	"testing"
	"time"

	"flow/internal/session"
)

func TestSessionExpireHook(t *testing.T) {
	s := New(nil, nil, false)
	s.sessions = session.NewManagerWithInterval(50*time.Millisecond, 10*time.Millisecond)

	type expiry struct {
		id, remote          string
		created, lastActive time.Time
	}
	got := make(chan expiry, 1)
	s.SetSessionExpireHook(func(id, remote string, created, lastActive time.Time) {
		got <- expiry{id: id, remote: remote, created: created, lastActive: lastActive}
	})
	s.installExpireHook()

	sess := s.sessions.NewSession("10.0.0.9")
	// 直接把 now 推到超时之后，避免测试里 sleep。
	s.sessions.Sweep(time.Now().Add(time.Minute))

	select {
	case e := <-got:
		if e.id != sess.ID || e.remote != "10.0.0.9" {
			t.Fatalf("hook = %+v, want %s/10.0.0.9", e, sess.ID)
		}
		if e.created.IsZero() || e.lastActive.IsZero() {
			t.Fatalf("hook timestamps = %+v", e)
		}
	default:
		t.Fatal("session expire hook was not called")
	}
	if s.sessions.Len() != 0 {
		t.Fatalf("session not removed: len=%d", s.sessions.Len())
	}
}

package main

import (
	"context"
	"testing"
	"time"

	"flow/internal/session"
)

// TestStartPlayIsIdempotent 验证同一个会话不会因为重复 PLAY 起两组推流；
// PAUSE（stopPlay）之后再 PLAY 必须拿到新的生命周期。
func TestStartPlayIsIdempotent(t *testing.T) {
	rs := newRtspSession()
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()

	first, started := rs.startPlay(parent)
	if !started {
		t.Fatal("first startPlay should start a new lifecycle")
	}
	second, startedAgain := rs.startPlay(parent)
	if startedAgain {
		t.Fatal("second startPlay should reuse the running lifecycle")
	}
	if second != first {
		t.Fatal("second startPlay returned a different done channel")
	}

	rs.stopPlay()
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("play context was not canceled by stopPlay")
	}

	third, started := rs.startPlay(parent)
	if !started {
		t.Fatal("startPlay after stopPlay should start a new lifecycle")
	}
	if third == first {
		t.Fatal("startPlay after stopPlay reused the canceled lifecycle")
	}
	rs.stopPlay()
}

// TestPauseStopsPlayButKeepsSession 模拟 PAUSE 处理：取消推流生命周期，
// 但 Session、track（端口/SSRC 挂在上面）和管理器里的登记都保留，PLAY 能恢复。
func TestPauseStopsPlayButKeepsSession(t *testing.T) {
	m := session.NewManager(time.Minute)
	sess := m.NewSession("127.0.0.1")
	sess.SetState(session.StatePlaying)

	rs := newRtspSession()
	track := &mediaTrack{kind: "video", serverRtpPort: 50000}
	rs.setTrack("video", track)

	done, started := rs.startPlay(sess.Context())
	if !started {
		t.Fatal("startPlay failed")
	}

	// PAUSE：停推流，保留会话资源。
	rs.stopPlay()
	sess.SetState(session.StatePaused)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("play context was not canceled by PAUSE")
	}

	if got, ok := m.Get(sess.ID); !ok || got != sess {
		t.Fatal("session was removed from manager by PAUSE")
	}
	if sess.State() != session.StatePaused {
		t.Fatalf("state = %v, want Paused", sess.State())
	}
	if rs.track("video") != track {
		t.Fatal("track was dropped by PAUSE")
	}

	// PAUSE 之后再次 PLAY：会话还在，能拿到新的生命周期。
	next, started := rs.startPlay(sess.Context())
	if !started {
		t.Fatal("startPlay after PAUSE did not start a new lifecycle")
	}
	if next == done {
		t.Fatal("PLAY after PAUSE reused the canceled lifecycle")
	}
	rs.stopPlay()
}

// TestStopPlayCancelsStreamLoop 用和 streamH264/streamAAC 相同的 select 模式，
// 验证 stopPlay 后消费循环能退出。
func TestStopPlayCancelsStreamLoop(t *testing.T) {
	rs := newRtspSession()
	done, started := rs.startPlay(context.Background())
	if !started {
		t.Fatal("startPlay failed")
	}

	exited := make(chan struct{})
	go func() {
		defer close(exited)
		for {
			select {
			case <-done:
				return
			default:
				time.Sleep(time.Millisecond)
			}
		}
	}()

	rs.stopPlay()

	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("stream loop did not exit after stopPlay")
	}
}

// TestSessionCloseCancelsPlay 验证会话被 TEARDOWN/超时回收时，
// PLAY 派生的子 context 会被父 context 级联取消。
func TestSessionCloseCancelsPlay(t *testing.T) {
	m := session.NewManager(time.Minute)
	sess := m.NewSession("127.0.0.1")

	rs := newRtspSession()
	done, started := rs.startPlay(sess.Context())
	if !started {
		t.Fatal("startPlay failed")
	}

	sess.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("play context was not canceled after Session.Close")
	}

	// 父 context 已经取消，stopPlay 仍然可以安全调用。
	rs.stopPlay()
	rs.stopPlay()
}

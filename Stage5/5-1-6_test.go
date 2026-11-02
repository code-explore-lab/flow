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

// TestTrackSSRCAndSeqPersist 验证 P0-4 的核心：同一条轨的 SSRC 和 seq
// 跨 PLAY/PAUSE 连续，不因重新起推流 goroutine 而重置。
func TestTrackSSRCAndSeqPersist(t *testing.T) {
	track := &mediaTrack{kind: "video"}

	ssrc1 := track.ensureSSRC()
	ssrc2 := track.ensureSSRC()
	if ssrc1 != ssrc2 {
		t.Fatalf("ensureSSRC returned %08x then %08x, want stable", ssrc1, ssrc2)
	}
	if ssrc1 == 0 {
		t.Fatal("ensureSSRC should generate a non-zero SSRC")
	}
	if track.currentSSRC() != ssrc1 {
		t.Fatalf("currentSSRC = %08x, want %08x", track.currentSSRC(), ssrc1)
	}

	// seq 严格递增；截断成 uint16 后按 RFC 3550 语义仍单调（不考虑回绕）。
	var last uint32
	for i := 0; i < 100; i++ {
		n := track.nextSeq()
		if i > 0 && n != last+1 {
			t.Fatalf("nextSeq jumped from %d to %d", last, n)
		}
		last = n
	}

	// 模拟 PAUSE→PLAY 之间重建一条轨（数据在同一 mediaTrack 上），seq/ssrc 不重置。
	track2 := track // 真实场景中 setTrack 保留同一个 track 对象
	if track2.ensureSSRC() != ssrc1 {
		t.Fatal("SSRC changed after simulated pause/resume")
	}
	if got := track2.nextSeq(); got != 100 {
		t.Fatalf("seq reset after simulated pause/resume: got %d, want 100", got)
	}
}

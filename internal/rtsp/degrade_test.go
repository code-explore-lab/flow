package rtsp

import (
	"bufio"
	"net"
	"testing"
	"time"

	"flow/internal/rtcp"
	"flow/internal/rtp"
	"flow/internal/source"
)

// TestDegradeStateEnterExit 验证高丢包持续 Sustain 后进入、
// 低丢包持续 Recover 后退出。
func TestDegradeStateEnterExit(t *testing.T) {
	d := newDegradeState(degradeConfig{
		LossThreshold: 0.10,
		Sustain:       3 * time.Second,
		Recover:       5 * time.Second,
	})
	t0 := time.Unix(100, 0)

	if entered, _ := d.observe(0.05, t0); entered {
		t.Fatal("low loss must not degrade")
	}
	// 单次高丢包不够，必须持续 Sustain。
	if entered, _ := d.observe(0.20, t0); entered {
		t.Fatal("single high-loss sample must not degrade immediately")
	}
	if entered, _ := d.observe(0.20, t0.Add(2*time.Second)); entered {
		t.Fatal("degraded before Sustain elapsed")
	}
	if entered, _ := d.observe(0.20, t0.Add(3*time.Second)); !entered {
		t.Fatal("did not degrade after Sustain at high loss")
	}
	if !d.active() {
		t.Fatal("state should be degraded")
	}

	// 低丢包也要持续 Recover 才能恢复。
	if _, exited := d.observe(0.01, t0.Add(4*time.Second)); exited {
		t.Fatal("recovered before Recover elapsed")
	}
	if _, exited := d.observe(0.01, t0.Add(8*time.Second)); exited {
		t.Fatal("recovered after 4s, want >= 5s")
	}
	if _, exited := d.observe(0.01, t0.Add(9*time.Second)); !exited {
		t.Fatal("did not recover after Recover at low loss")
	}
	if d.active() {
		t.Fatal("state should be recovered")
	}
}

// TestDegradeStateResetsOnFlip 中途反向采样会重置窗口，避免抖动导致切换。
func TestDegradeStateResetsOnFlip(t *testing.T) {
	d := newDegradeState(degradeConfig{0.10, 3 * time.Second, 5 * time.Second})
	t0 := time.Unix(200, 0)

	d.observe(0.20, t0)
	d.observe(0.20, t0.Add(2*time.Second))
	// 一次低丢包把高丢包窗口清零；重新计时后 3s 才降级。
	d.observe(0.01, t0.Add(2*time.Second))
	if entered, _ := d.observe(0.20, t0.Add(4*time.Second)); entered {
		t.Fatal("high-loss window was not reset")
	}
	if entered, _ := d.observe(0.20, t0.Add(7*time.Second)); !entered {
		t.Fatal("did not degrade after the reset window elapsed")
	}

	// 恢复窗口同样会被高丢包打断。
	d.observe(0.01, t0.Add(8*time.Second))
	d.observe(0.20, t0.Add(9*time.Second))
	if _, exited := d.observe(0.01, t0.Add(13*time.Second)); exited {
		t.Fatal("recovery window was not reset by high loss")
	}
}

// TestSetDegradePolicy 非法配置保持默认，合法配置生效。
func TestSetDegradePolicy(t *testing.T) {
	s := New(nil, nil, false)
	if s.degrade != defaultDegradeConfig {
		t.Fatalf("default policy = %+v, want %+v", s.degrade, defaultDegradeConfig)
	}
	s.SetDegradePolicy(0, time.Second, time.Second)
	if s.degrade != defaultDegradeConfig {
		t.Fatalf("invalid threshold changed policy to %+v", s.degrade)
	}
	s.SetDegradePolicy(0.25, 2*time.Second, 4*time.Second)
	want := degradeConfig{LossThreshold: 0.25, Sustain: 2 * time.Second, Recover: 4 * time.Second}
	if s.degrade != want {
		t.Fatalf("policy = %+v, want %+v", s.degrade, want)
	}
}

// TestDropNonKeyFrameNoState 没有状态机（推流/组播轨）时永远不丢帧。
func TestDropNonKeyFrameNoState(t *testing.T) {
	track := &mediaTrack{kind: "video"}
	if track.dropNonKeyFrame() {
		t.Fatal("track without degrade state must not drop frames")
	}
	track.degrade = newDegradeState(defaultDegradeConfig)
	if track.dropNonKeyFrame() {
		t.Fatal("inactive degrade state must not drop frames")
	}
	track.degrade.on = true
	if !track.dropNonKeyFrame() {
		t.Fatal("active degrade state should drop non-key frames")
	}
	if got := track.degrade.dropped; got != 1 {
		t.Fatalf("dropped counter = %d, want 1", got)
	}
}

// TestHandleIncomingRTCPDrivesDegrade 验证 RR 处理路径确实驱动状态机。
// 这里把两个窗口设为 0，一条 RR 就足以切换，不需要等待真实时间。
func TestHandleIncomingRTCPDrivesDegrade(t *testing.T) {
	s, sess, track, _ := newRTXTestTrack(t)
	track.degrade = newDegradeState(degradeConfig{LossThreshold: 0.10})

	blk := rtcp.ReportBlock{
		SSRC:               track.currentSSRC(),
		FractionLost:       51, // 约 20%
		ExtendedHighestSeq: 100,
	}
	s.handleIncomingRTCP(track, sess, rtcp.Compound(makeRRPkt(0x01020304, blk)))
	if !track.degrade.active() {
		t.Fatal("high-loss RR did not enter degrade mode")
	}

	blk.FractionLost = 0
	s.handleIncomingRTCP(track, sess, rtcp.Compound(makeRRPkt(0x01020304, blk)))
	if track.degrade.active() {
		t.Fatal("low-loss RR did not leave degrade mode")
	}
}

// TestStreamH264SkipsNonKeyFramesWhenDegraded 降级时只发参数集 + IDR，
// P 帧整个 AU 被跳过，但流本身正常结束、不阻塞。
func TestStreamH264SkipsNonKeyFramesWhenDegraded(t *testing.T) {
	s := New(nil, nil, false)
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	track := &mediaTrack{
		kind:           "video",
		isInterleaved:  true,
		interleavedRTP: 0,
		conn:           &Conn{Conn: serverConn},
		degrade:        newDegradeState(defaultDegradeConfig),
	}
	track.degrade.on = true // 直接置为降级，单测不等待 RR 时间窗口

	sps := []byte{0x67, 0x42, 0xC0, 0x1E}
	pps := []byte{0x68, 0xCE, 0x3C, 0x80}
	idr := []byte{0x65, 0x88, 0x84, 0x00}
	pframe := []byte{0x41, 0x9A, 0x00, 0x01}
	src := &framesSource{frames: []source.Frame{
		{Data: sps, Timestamp: 0},
		{Data: pps, Timestamp: 0},
		{Data: idr, Timestamp: 0, Keyframe: true},
		{Data: pframe, Timestamp: 3600}, // 非关键帧：降级期间丢弃
		{Data: idr, Timestamp: 7200, Keyframe: true},
		{Data: pframe, Timestamp: 10800}, // 丢弃
	}}

	done := make(chan struct{})
	defer close(done)
	streamDone := make(chan struct{})
	go func() {
		s.streamH264(track, src, done, 0, 1)
		close(streamDone)
	}()

	br := bufio.NewReader(clientConn)
	var stamps []uint32
	for i := 0; i < 2; i++ {
		_, payload, err := readInterleavedFrame(br)
		if err != nil {
			t.Fatalf("read packet %d: %v", i, err)
		}
		pkt, err := rtp.Unmarshal(payload)
		if err != nil {
			t.Fatalf("unmarshal packet %d: %v", i, err)
		}
		stamps = append(stamps, pkt.Header.Timestamp)
	}
	<-streamDone

	if len(stamps) != 2 || stamps[0] != 0 || stamps[1] != 7200 {
		t.Fatalf("timestamps = %v, want [0 7200] (P frames skipped)", stamps)
	}
	if got := track.degrade.dropped; got != 2 {
		t.Fatalf("dropped = %d, want 2", got)
	}

	// 流结束后不应再有第三个包：关掉服务端后读取应以错误结束。
	serverConn.Close()
	if _, _, err := readInterleavedFrame(br); err == nil {
		t.Fatal("unexpected packet after stream ended")
	}
}

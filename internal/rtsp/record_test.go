package rtsp

import (
	"bytes"
	"net"
	"testing"
	"time"

	"flow/internal/plc"
	"flow/internal/rtcp"
	"flow/internal/rtp"
	"flow/internal/rtpbuf"
	"flow/internal/session"
	"flow/internal/source"
)

// newPushSessionAt 在指定 Server 上建一个 ANNOUNCE 之后的推流会话。
func newPushSessionAt(t *testing.T, s *Server) *session.Session {
	t.Helper()
	tracks, err := parseAnnounceSDP([]byte(ffmpegAnnounceSDP))
	if err != nil {
		t.Fatalf("parseAnnounceSDP: %v", err)
	}
	sess := s.sessions.NewSession("127.0.0.1")
	st := newSession()
	sess.Data = st
	sess.OnClose = st.closeAll
	st.setAnnounced(tracks)
	return sess
}

// setupTrack 用一条临时连接对已有会话做 SETUP，返回响应头。
func setupTrack(t *testing.T, s *Server, sessID, control, transport string) map[string]string {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	conn := &Conn{Conn: serverConn}
	go func() {
		tmp := make([]byte, 8192)
		for {
			if _, err := clientConn.Read(tmp); err != nil {
				return
			}
		}
	}()

	hd := map[string]string{"CSeq": "9"}
	req := &Request{
		Method: "SETUP",
		URI:    "rtsp://127.0.0.1:8554/live/" + control,
		Headers: map[string]string{
			"Transport": transport,
			"Session":   sessID,
		},
	}
	errCh := make(chan error, 1)
	go func() { errCh <- s.handleSetup(conn, req, hd) }()
	if err := <-errCh; err != nil {
		t.Fatalf("handleSetup: %v", err)
	}
	clientConn.Close()
	serverConn.Close()
	if hd["Transport"] == "" {
		t.Fatalf("SETUP %s returned no Transport", control)
	}
	return hd
}

func makeRTP(pt uint8, seq uint16, ts, ssrc uint32, payload []byte, marker bool) []byte {
	h := rtp.RTPHeader{
		Version:   2,
		Marker:    marker,
		PT:        pt,
		Seq:       seq,
		Timestamp: ts,
		SSRC:      ssrc,
	}
	b, err := h.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return b
}

func TestRecordDepacketizesH264SingleNALU(t *testing.T) {
	s := New(nil, nil, false)
	sess := newPushSessionAt(t, s)
	setupTrack(t, s, sess.ID, "streamid=0", "RTP/AVP/TCP;unicast;interleaved=0-1")

	st := sess.Data.(*Session)
	track := st.track("video")
	rx, err := track.ensurePushReceiver()
	if err != nil {
		t.Fatalf("ensurePushReceiver: %v", err)
	}
	rd, cancel := rx.external.Subscribe()
	defer cancel()
	done := make(chan struct{})
	defer close(done)

	// 普通 P 帧（类型 1）。marker=1 表示这是该 AU 的最后一个包，
	// 接收侧攒齐 AU 后才发布（R-5）。
	s.handlePushedRTP(st, track, makeRTP(96, 10, 9000, 0x11223344, []byte{0x41, 0xAA, 0xBB}, true))
	f, ok := rd.Next(done)
	if !ok {
		t.Fatal("no frame written to ExternalSource")
	}
	if string(f.Data) != string([]byte{0x41, 0xAA, 0xBB}) || f.Timestamp != 9000 || f.Keyframe {
		t.Fatalf("frame = %x ts=%d key=%v", f.Data, f.Timestamp, f.Keyframe)
	}

	// IDR（类型 5）应标记为关键帧。
	s.handlePushedRTP(st, track, makeRTP(96, 11, 18000, 0x11223344, []byte{0x65, 0x01, 0x02}, true))
	f, ok = rd.Next(done)
	if !ok {
		t.Fatal("no IDR frame written")
	}
	if !f.Keyframe || f.Timestamp != 18000 {
		t.Fatalf("IDR frame key=%v ts=%d", f.Keyframe, f.Timestamp)
	}

	packets, _, frames, gaps := track.recvSummary()
	if packets != 2 || frames != 2 || gaps != 0 {
		t.Fatalf("summary packets=%d frames=%d gaps=%d", packets, frames, gaps)
	}
}

func TestRecordReassemblesFUADropsOnSeqGap(t *testing.T) {
	s := New(nil, nil, false)
	sess := newPushSessionAt(t, s)
	setupTrack(t, s, sess.ID, "streamid=0", "RTP/AVP/TCP;unicast;interleaved=0-1")

	st := sess.Data.(*Session)
	track := st.track("video")
	rx, err := track.ensurePushReceiver()
	if err != nil {
		t.Fatalf("ensurePushReceiver: %v", err)
	}
	// 本用例验证解包/丢包后立刻可读取统计，不涉及重排；关闭 playout 延迟。
	rx.jbuf = rtpbuf.New(0)
	rd, cancel := rx.external.Subscribe()
	defer cancel()
	done := make(chan struct{})
	defer close(done)

	// FU-A 首片 seq=20，尾片 seq=22：中间缺 21，必须丢弃半成品且不 panic。
	start := []byte{0x7C, 0x85, 0xAA, 0xBB} // FU indicator(type28), FU header(S=1,type5)
	end := []byte{0x7C, 0x45, 0xCC}         // FU header(E=1,type5)
	s.handlePushedRTP(st, track, makeRTP(96, 20, 36000, 0xabcd, start, false))
	s.handlePushedRTP(st, track, makeRTP(96, 22, 36000, 0xabcd, end, true))

	packets, _, frames, gaps := track.recvSummary()
	if packets != 2 || frames != 0 || gaps != 1 {
		t.Fatalf("summary packets=%d frames=%d gaps=%d, want 2/0/1", packets, frames, gaps)
	}

	// 后续完整包必须能继续处理（解包器没有卡死）。
	s.handlePushedRTP(st, track, makeRTP(96, 23, 45000, 0xabcd, []byte{0x41, 0x01}, true))
	if f, ok := rd.Next(done); !ok || f.Data[0] != 0x41 {
		t.Fatalf("recovery frame = %x ok=%v", f.Data, ok)
	}
}

func TestRecordReassemblesFUA(t *testing.T) {
	s := New(nil, nil, false)
	sess := newPushSessionAt(t, s)
	setupTrack(t, s, sess.ID, "streamid=0", "RTP/AVP/TCP;unicast;interleaved=0-1")

	st := sess.Data.(*Session)
	track := st.track("video")
	rx, err := track.ensurePushReceiver()
	if err != nil {
		t.Fatalf("ensurePushReceiver: %v", err)
	}
	rd, cancel := rx.external.Subscribe()
	defer cancel()
	done := make(chan struct{})
	defer close(done)

	s.handlePushedRTP(st, track, makeRTP(96, 30, 45000, 0x99, []byte{0x7C, 0x85, 0xAA, 0xBB}, false))
	s.handlePushedRTP(st, track, makeRTP(96, 31, 45000, 0x99, []byte{0x7C, 0x45, 0xCC}, true))

	f, ok := rd.Next(done)
	if !ok {
		t.Fatal("FU-A was not reassembled")
	}
	want := []byte{0x65, 0xAA, 0xBB, 0xCC} // 还原原始 NAL header(type5) + 两片数据
	if string(f.Data) != string(want) || !f.Keyframe || f.Timestamp != 45000 {
		t.Fatalf("frame = %x key=%v ts=%d, want %x", f.Data, f.Keyframe, f.Timestamp, want)
	}
	if _, _, frames, _ := track.recvSummary(); frames != 1 {
		t.Fatalf("frames = %d, want 1", frames)
	}
}

func TestRecordDepacketizesAAC(t *testing.T) {
	s := New(nil, nil, false)
	sess := newPushSessionAt(t, s)
	setupTrack(t, s, sess.ID, "streamid=1", "RTP/AVP/TCP;unicast;interleaved=2-3")

	st := sess.Data.(*Session)
	track := st.track("audio")
	if track == nil || track.payloadType != 97 {
		t.Fatalf("audio track = %+v", track)
	}
	rx, err := track.ensurePushReceiver()
	if err != nil {
		t.Fatalf("ensurePushReceiver: %v", err)
	}
	rd, cancel := rx.external.Subscribe()
	defer cancel()
	done := make(chan struct{})
	defer close(done)

	// AU-headers-length=16bit；AU-header: size=4(13bit) + index=0(3bit)。
	payload := []byte{0x00, 0x10, 0x00, 0x20, 0xDE, 0xAD, 0xBE, 0xEF}
	s.handlePushedRTP(st, track, makeRTP(97, 1, 1024, 0x5566, payload, true))

	f, ok := rd.Next(done)
	if !ok {
		t.Fatal("AAC AU was not written")
	}
	if string(f.Data) != string([]byte{0xDE, 0xAD, 0xBE, 0xEF}) || f.Timestamp != 1024 || !f.Keyframe {
		t.Fatalf("AU = %x ts=%d key=%v", f.Data, f.Timestamp, f.Keyframe)
	}
}

// 报文到达 video channel，但 PT 是音频的：应按 PT 改路由到 audio 轨。
func TestRecordRoutesByPayloadType(t *testing.T) {
	s := New(nil, nil, false)
	sess := newPushSessionAt(t, s)
	setupTrack(t, s, sess.ID, "streamid=0", "RTP/AVP/TCP;unicast;interleaved=0-1")
	setupTrack(t, s, sess.ID, "streamid=1", "RTP/AVP/TCP;unicast;interleaved=2-3")

	st := sess.Data.(*Session)
	video := st.track("video")
	audio := st.track("audio")
	vrx, err := video.ensurePushReceiver()
	if err != nil {
		t.Fatal(err)
	}
	arx, err := audio.ensurePushReceiver()
	if err != nil {
		t.Fatal(err)
	}
	vrd, vcancel := vrx.external.Subscribe()
	defer vcancel()
	ard, acancel := arx.external.Subscribe()
	defer acancel()
	done := make(chan struct{})
	defer close(done)

	// hint 是 video，但 PT=97，最终应写到 audio。
	payload := []byte{0x00, 0x10, 0x00, 0x20, 0x01, 0x02, 0x03, 0x04}
	s.handlePushedRTP(st, video, makeRTP(97, 1, 1024, 0x77, payload, true))

	if _, ok := ard.Next(done); !ok {
		t.Fatal("audio track did not receive the AU")
	}
	if _, _, vf, _ := video.recvSummary(); vf != 0 {
		t.Fatalf("video track received %d frame(s) from audio PT", vf)
	}
	if _, _, af, _ := audio.recvSummary(); af != 1 {
		t.Fatalf("audio frames = %d, want 1", af)
	}
	// video 的 reader 不应该有数据：用非阻塞方式确认。
	if _, ok := vrd.Next(closedChan()); ok {
		t.Fatal("video track reader unexpectedly got a frame")
	}
}

func closedChan() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// newPLCVideoTrack 建一条 UDP 推流视频轨，并把 RTCP 目标指向测试 socket，
// 这样可以直接断言服务端发出的 PLI。
func newPLCVideoTrack(t *testing.T) (*Server, *Session, *mediaTrack, *net.UDPConn) {
	t.Helper()
	s := New(nil, nil, false)
	sess := newPushSessionAt(t, s)
	t.Cleanup(sess.Close)
	setupTrack(t, s, sess.ID, "streamid=0", "RTP/AVP;unicast;client_port=40000-40001")

	st := sess.Data.(*Session)
	track := st.track("video")
	if track == nil {
		t.Fatal("video push track not created")
	}
	rtcpTarget, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen RTCP target: %v", err)
	}
	t.Cleanup(func() { rtcpTarget.Close() })
	track.clientRTCPAddr = rtcpTarget.LocalAddr().(*net.UDPAddr)
	return s, st, track, rtcpTarget
}

// readNACK 等待第一条 Generic NACK，跳过同通道上的 PLI/SR。
func readNACK(t *testing.T, c *net.UDPConn) (sender, media uint32, seqs []uint16) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	buf := make([]byte, 128)
	for {
		c.SetReadDeadline(deadline)
		n, _, err := c.ReadFromUDP(buf)
		if err != nil {
			t.Fatalf("read NACK: %v", err)
		}
		if sender, media, seqs, ok := rtcp.ParseNACK(buf[:n]); ok {
			return sender, media, seqs
		}
	}
}

// readPLI 等待第一条 PLI。同一条 RTCP 通道上可能先收到 NACK，跳过即可。
func readPLI(t *testing.T, c *net.UDPConn) (sender, media uint32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	buf := make([]byte, 64)
	for {
		c.SetReadDeadline(deadline)
		n, _, err := c.ReadFromUDP(buf)
		if err != nil {
			t.Fatalf("read PLI: %v", err)
		}
		if sender, media, ok := plc.ParsePLI(buf[:n]); ok {
			return sender, media
		}
	}
}

// TestPushVideoConcealsCorruptAU 一个 AU 内部丢包时：不发布半帧，
// 用上一帧占住该时间戳，并向上游发 PLI 请求关键帧。
func TestPushVideoConcealsCorruptAU(t *testing.T) {
	s, st, track, rtcpTarget := newPLCVideoTrack(t)
	rx, err := track.ensurePushReceiver()
	if err != nil {
		t.Fatal(err)
	}
	rd, cancel := rx.external.Subscribe()
	defer cancel()
	done := make(chan struct{})
	defer close(done)

	// AU1：完整 P 帧 ts=9000。
	s.handlePushedRTP(st, track, makeRTP(96, 10, 9000, 0xabcd, []byte{0x41, 0xAA}, true))
	if f, ok := rd.Next(done); !ok || f.Timestamp != 9000 || f.Data[0] != 0x41 {
		t.Fatalf("AU1 = %x ts=%d ok=%v", f.Data, f.Timestamp, ok)
	}

	// AU2：FU-A 的首片 seq=11，尾片 seq=13（缺 12）→ 整帧损坏。
	s.handlePushedRTP(st, track, makeRTP(96, 11, 18000, 0xabcd, []byte{0x7C, 0x85, 0xAA}, false))
	s.handlePushedRTP(st, track, makeRTP(96, 13, 18000, 0xabcd, []byte{0x7C, 0x45, 0xBB}, true))

	// 输出的是重复的上一帧（ts=18000），不是损坏的 AU2。
	f, ok := rd.Next(done)
	if !ok || f.Timestamp != 18000 || !bytes.Equal(f.Data, []byte{0x41, 0xAA}) {
		t.Fatalf("concealment = %x ts=%d ok=%v", f.Data, f.Timestamp, ok)
	}

	sender, media := readPLI(t, rtcpTarget)
	if media != 0xabcd {
		t.Fatalf("PLI media ssrc = %08x, want 0000abcd", media)
	}
	if sender != track.currentSSRC() {
		t.Fatalf("PLI sender ssrc = %08x, want %08x", sender, track.currentSSRC())
	}
	repeats, dropped, pli, _, _ := rx.plcStats()
	if dropped != 1 || repeats != 1 || pli != 1 {
		t.Fatalf("counters dropped=%d repeat=%d pli=%d, want 1/1/1",
			dropped, repeats, pli)
	}
}

// TestPushVideoBFramesKeepAllAUs 有 B 帧时 RTP 时间戳（PTS）到达顺序不单调
// （I/P 先到、B 后到），接收端不能把它当成缺口或旧包丢弃：所有 AU 都必须
// 按到达顺序发布，且不触发 PLC。
func TestPushVideoBFramesKeepAllAUs(t *testing.T) {
	s, st, track, _ := newPLCVideoTrack(t)
	rx, err := track.ensurePushReceiver()
	if err != nil {
		t.Fatal(err)
	}
	rd, cancel := rx.external.Subscribe()
	defer cancel()
	done := make(chan struct{})
	defer close(done)

	// 编码顺序 I(0) P(3) B(1) B(2) P(6)：第 2 个包的时间戳最大，
	// 后面两个包的时间戳反而更小。
	order := []uint32{0, 3 * 3600, 3600, 2 * 3600, 6 * 3600}
	for i, ts := range order {
		s.handlePushedRTP(st, track,
			makeRTP(96, uint16(i+1), ts, 0x4321, []byte{0x41, byte(i)}, true))
	}

	for i, want := range order {
		f, ok := rd.Next(done)
		if !ok {
			t.Fatalf("frame %d missing", i)
		}
		if f.Timestamp != want || f.Data[1] != byte(i) {
			t.Fatalf("frame %d = %x ts=%d, want ts=%d", i, f.Data, f.Timestamp, want)
		}
	}
	if rx.plcDropped != 0 || rx.plcRepeats != 0 || rx.pliSent != 0 {
		t.Fatalf("false PLC: dropped=%d repeat=%d pli=%d",
			rx.plcDropped, rx.plcRepeats, rx.pliSent)
	}
}

// TestPushAACFillsSilence 音频时间戳出现缺口时补 AAC-LC 静音 AU。
func TestPushAACFillsSilence(t *testing.T) {
	s := New(nil, nil, false)
	sess := newPushSessionAt(t, s)
	setupTrack(t, s, sess.ID, "streamid=1", "RTP/AVP/TCP;unicast;interleaved=2-3")

	st := sess.Data.(*Session)
	track := st.track("audio")
	rx, err := track.ensurePushReceiver()
	if err != nil {
		t.Fatal(err)
	}
	if rx.aacChannels != 2 {
		t.Fatalf("aacChannels = %d, want 2 (config=1210)", rx.aacChannels)
	}
	rd, cancel := rx.external.Subscribe()
	defer cancel()
	done := make(chan struct{})
	defer close(done)

	auPayload := func(b byte) []byte {
		return []byte{0x00, 0x10, 0x00, 0x20, b, b, b, b}
	}
	s.handlePushedRTP(st, track, makeRTP(97, 1, 1024, 0x5566, auPayload(0x11), true))
	// seq=2 丢了；ts 从 1024 跳到 3072，中间缺 ts=2048 的一个 AU。
	s.handlePushedRTP(st, track, makeRTP(97, 3, 3072, 0x5566, auPayload(0x33), true))

	f, ok := rd.Next(done)
	if !ok || f.Timestamp != 1024 || f.Data[0] != 0x11 {
		t.Fatalf("first AU = %x ts=%d ok=%v", f.Data, f.Timestamp, ok)
	}
	silence, _ := plc.SilenceAU(2)
	f, ok = rd.Next(done)
	if !ok || f.Timestamp != 2048 || !bytes.Equal(f.Data, silence) {
		t.Fatalf("silence = %x ts=%d ok=%v, want %x ts=2048", f.Data, f.Timestamp, ok, silence)
	}
	f, ok = rd.Next(done)
	if !ok || f.Timestamp != 3072 || f.Data[0] != 0x33 {
		t.Fatalf("third AU = %x ts=%d ok=%v", f.Data, f.Timestamp, ok)
	}
	if _, _, _, _, silence := rx.plcStats(); silence != 1 {
		t.Fatalf("silenceAUs = %d, want 1", silence)
	}
}

// TestPushPLIRateLimited 丢包密集时 PLI 受最小间隔限制。
func TestPushPLIRateLimited(t *testing.T) {
	s, st, track, rtcpTarget := newPLCVideoTrack(t)
	rx, err := track.ensurePushReceiver()
	if err != nil {
		t.Fatal(err)
	}
	rx.pliInterval = time.Hour // 只允许第一次发送

	corrupt := func(seq uint16, ts uint32) {
		// 单 NALU 包，但 seq 从 10 跳到 seq+2 造成缺口。
		s.handlePushedRTP(st, track, makeRTP(96, seq+2, ts, 0x7777, []byte{0x41, 0x01}, true))
	}
	// 先给一份完整帧作为重复源。
	s.handlePushedRTP(st, track, makeRTP(96, 10, 9000, 0x7777, []byte{0x41, 0x00}, true))
	corrupt(11, 18000)
	readPLI(t, rtcpTarget)
	corrupt(14, 27000)

	// 同一条 RTCP 通道上可能同时有 NACK；只检查有没有第二条 PLI。
	deadline := time.Now().Add(300 * time.Millisecond)
	buf := make([]byte, 64)
	for {
		rtcpTarget.SetReadDeadline(deadline)
		n, _, err := rtcpTarget.ReadFromUDP(buf)
		if err != nil {
			break // 截止时间到，没有第二条 PLI
		}
		if _, _, ok := plc.ParsePLI(buf[:n]); ok {
			t.Fatalf("second PLI sent within rate limit: % x", buf[:n])
		}
	}
	if _, _, pli, _, _ := rx.plcStats(); pli != 1 {
		t.Fatalf("pliSent = %d, want 1", pli)
	}
}

// TestPushNACKRetransmitKeepsOrder 缺一个包时先发 NACK；重传在 jitter buffer
// 的等待窗口内到达后按 seq 排序解包，AU 完整发布，不触发 PLC。
func TestPushNACKRetransmitKeepsOrder(t *testing.T) {
	s, st, track, rtcpTarget := newPLCVideoTrack(t)
	rx, err := track.ensurePushReceiver()
	if err != nil {
		t.Fatal(err)
	}
	rd, cancel := rx.external.Subscribe()
	defer cancel()
	done := make(chan struct{})
	defer close(done)

	// AU1 ts=9000：seq 10(NALU A) / 11(NALU B，丢失) / 12(NALU C，marker)。
	s.handlePushedRTP(st, track, makeRTP(96, 10, 9000, 0xabcd, []byte{0x41, 0x0A}, false))
	s.handlePushedRTP(st, track, makeRTP(96, 12, 9000, 0xabcd, []byte{0x41, 0x0C}, true))
	// AU2 ts=12600：seq 13，作为缺口之后新到达的包。
	s.handlePushedRTP(st, track, makeRTP(96, 13, 12600, 0xabcd, []byte{0x41, 0x0D}, true))

	sender, media, nackSeqs := readNACK(t, rtcpTarget)
	if media != 0xabcd {
		t.Fatalf("NACK media ssrc = %08x, want 0000abcd", media)
	}
	if sender != track.currentSSRC() {
		t.Fatalf("NACK sender ssrc = %08x, want %08x", sender, track.currentSSRC())
	}
	found := false
	for _, seq := range nackSeqs {
		if seq == 11 {
			found = true
		}
	}
	if !found {
		t.Fatalf("NACK missing seqs = %v, want to contain 11", nackSeqs)
	}

	// 重传原包：同一 ts、无 marker。
	s.handlePushedRTP(st, track, makeRTP(96, 11, 9000, 0xabcd, []byte{0x41, 0x0B}, false))

	want := []struct {
		ts uint32
		b  byte
	}{
		{9000, 0x0A}, {9000, 0x0B}, {9000, 0x0C}, {12600, 0x0D},
	}
	for i, w := range want {
		f, ok := rd.Next(done)
		if !ok {
			t.Fatalf("frame %d missing", i)
		}
		if f.Timestamp != w.ts || f.Data[1] != w.b {
			t.Fatalf("frame %d = %x ts=%d, want %02x ts=%d", i, f.Data, f.Timestamp, w.b, w.ts)
		}
	}
	repeats, dropped, _, nack, _ := rx.plcStats()
	if dropped != 0 || repeats != 0 {
		t.Fatalf("PLC triggered for recovered loss: dropped=%d repeat=%d", dropped, repeats)
	}
	if nack == 0 {
		t.Fatal("no NACK sent")
	}
}

// collectFrames 在后台把 ExternalSource 的帧收进 channel，避免 ring buffer
// 容量不够导致慢消费者丢帧。
func collectFrames(rd *source.Reader[source.Frame], stop <-chan struct{}, want int) <-chan source.Frame {
	out := make(chan source.Frame, want+16)
	go func() {
		defer close(out)
		for {
			f, ok := rd.Next(stop)
			if !ok {
				return
			}
			out <- f
		}
	}()
	return out
}

func readFrames(t *testing.T, out <-chan source.Frame, want int) []source.Frame {
	t.Helper()
	got := make([]source.Frame, 0, want)
	deadline := time.After(3 * time.Second)
	for len(got) < want {
		select {
		case f, ok := <-out:
			if !ok {
				t.Fatalf("source ended after %d/%d frames", len(got), want)
			}
			got = append(got, f)
		case <-deadline:
			t.Fatalf("timeout after %d/%d frames", len(got), want)
		}
	}
	return got
}

// TestPushVideoFivePercentLossKeepsTimeline 模拟 5% 丢包：每 20 个 RTP 包丢 1 个。
// 每个 AU 4 个 NALU；丢包 AU 用上一帧替换，输出的时间戳序列必须完整、
// 每个 AU 恰好 4 帧，不出现断流空档。
func TestPushVideoFivePercentLossKeepsTimeline(t *testing.T) {
	s, st, track, _ := newPLCVideoTrack(t)
	rx, err := track.ensurePushReceiver()
	if err != nil {
		t.Fatal(err)
	}
	// 这个用例在一个 goroutine 里瞬间灌完 100 个 AU；关掉 playout 延迟，
	// 让帧随 Push 逐包输出，避免定时器一次冲刷几百帧把 ring buffer 撑爆
	// （ring 满时慢消费者会丢帧，与本用例要验证的时间轴无关）。
	rx.jbuf = rtpbuf.New(0)
	rd, cancel := rx.external.Subscribe()
	defer cancel()

	const aus = 100
	const naluPerAU = 4
	stop := make(chan struct{})
	defer close(stop)
	out := collectFrames(rd, stop, aus*naluPerAU)

	seq := uint16(1)
	packet := 0
	for au := range aus {
		ts := uint32(9000 + au*3600)
		for j := range naluPerAU {
			// 5% 丢包：每 20 个包丢 1 个（相位避开 marker 和最后一个包）。
			drop := (packet+2)%20 == 0
			if !drop {
				payload := []byte{0x41, byte(au), byte(j)}
				s.handlePushedRTP(st, track,
					makeRTP(96, seq, ts, 0x5151, payload, j == naluPerAU-1))
			}
			packet++
			seq++
		}
	}

	got := readFrames(t, out, aus*naluPerAU)
	for i, f := range got {
		wantTS := uint32(9000 + (i/naluPerAU)*3600)
		if f.Timestamp != wantTS {
			t.Fatalf("frame %d ts=%d, want %d", i, f.Timestamp, wantTS)
		}
		if len(f.Data) != 3 || f.Data[0] != 0x41 {
			t.Fatalf("frame %d data = %x", i, f.Data)
		}
		if i > 0 && f.Timestamp < got[i-1].Timestamp {
			t.Fatalf("timestamp went backwards at frame %d", i)
		}
	}
	if repeats, dropped, _, _, _ := rx.plcStats(); dropped == 0 || repeats == 0 {
		t.Fatalf("PLC not exercised: dropped=%d repeat=%d", dropped, repeats)
	}
}

// TestPushAACFivePercentLossKeepsTimeline 音频 5% 丢包：缺失的 AU 用静音补齐，
// 输出时间戳连续、总帧数与发送的 AU 数一致。
func TestPushAACFivePercentLossKeepsTimeline(t *testing.T) {
	s := New(nil, nil, false)
	sess := newPushSessionAt(t, s)
	setupTrack(t, s, sess.ID, "streamid=1", "RTP/AVP/TCP;unicast;interleaved=2-3")

	st := sess.Data.(*Session)
	track := st.track("audio")
	rx, err := track.ensurePushReceiver()
	if err != nil {
		t.Fatal(err)
	}
	// 同视频时间轴用例：瞬间灌完 100 个 AU 时关闭 playout 延迟，
	// 避免一次冲刷把 ring buffer 撑爆导致的测试性丢帧。
	rx.jbuf = rtpbuf.New(0)
	rd, cancel := rx.external.Subscribe()
	defer cancel()

	const aus = 100
	stop := make(chan struct{})
	defer close(stop)
	out := collectFrames(rd, stop, aus)

	silence, _ := plc.SilenceAU(2)
	lost := map[uint32]bool{}
	for k := range aus {
		// 每 20 个包丢 1 个；最后一个包不丢——没有后续包时无法从时间戳
		// 判断它缺失（接收端只能等下一个 AU 到来）。
		if (k+1)%20 == 0 && k+1 < aus {
			lost[uint32(1024*(k+1))] = true
			continue
		}
		payload := []byte{0x00, 0x10, 0x00, 0x20, 0xAA, byte(k), 0xBB, 0xCC}
		s.handlePushedRTP(st, track,
			makeRTP(97, uint16(k+1), uint32(1024*(k+1)), 0x5566, payload, true))
	}

	got := readFrames(t, out, aus)
	for i, f := range got {
		wantTS := uint32(1024 * (i + 1))
		if f.Timestamp != wantTS {
			t.Fatalf("frame %d ts=%d, want %d", i, f.Timestamp, wantTS)
		}
		if lost[wantTS] && !bytes.Equal(f.Data, silence) {
			t.Fatalf("frame %d = %x, want silence for lost AU", i, f.Data)
		}
	}
	if _, _, _, _, silenceCount := rx.plcStats(); silenceCount != uint64(len(lost)) {
		t.Fatalf("silenceAUs = %d, want %d", silenceCount, len(lost))
	}
}

package rtsp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"flow/internal/rtp"
	"flow/internal/source"
)

// TestAnnouncePublishesAndDescribeUsesCodecParams ANNOUNCE 之后：
//   - 流按 URL 路径登记，推/拉两端拿到同一组 ExternalSource；
//   - 拉流 DESCRIBE 的 SDP 使用 ANNOUNCE 的 rtpmap/fmtp（含 sprop-parameter-sets）；
//   - 推流会话关闭时流被摘掉。
func TestAnnouncePublishesAndDescribeUsesCodecParams(t *testing.T) {
	s, conn, finish := newTestPipe(t)

	hd := map[string]string{"CSeq": "2"}
	req := &Request{
		Method:  "ANNOUNCE",
		URI:     "rtsp://127.0.0.1:8554/push",
		Version: "RTSP/1.0",
		CSeq:    2,
		Headers: map[string]string{"Content-Type": "application/sdp"},
		Body:    []byte(ffmpegAnnounceSDP),
	}
	callAndRead(t, func() error { return s.handleAnnounce(conn, req, hd) }, finish)

	pub := s.publishedFromURI("rtsp://127.0.0.1:8554/push")
	if pub == nil {
		t.Fatal("ANNOUNCE did not register a published stream")
	}
	video, audio := pub.VideoTrack(), pub.AudioTrack()
	if video == nil || audio == nil {
		t.Fatalf("published tracks = video%v audio%v, want both", video != nil, audio != nil)
	}
	if got := audio.Src.SamplingRate(); got != 44100 {
		t.Fatalf("audio sampling rate = %d, want 44100", got)
	}
	if vc, ok := pub.VideoParams(); !ok || len(vc.SPS) == 0 || len(vc.PPS) == 0 {
		t.Fatalf("video params = %+v ok=%v, want SPS/PPS parsed from ANNOUNCE", vc, ok)
	}
	if ac, ok := pub.AudioParams(); !ok || len(ac.Config) != 2 || ac.SampleRate != 44100 || ac.Channels != 2 {
		t.Fatalf("audio params = %+v ok=%v, want ASC 44100/2", ac, ok)
	}

	sdp, err := s.buildSDP("rtsp://127.0.0.1:8554/push")
	if err != nil {
		t.Fatalf("buildSDP: %v", err)
	}
	want := []string{
		"m=video 0 RTP/AVP 96",
		"a=rtpmap:96 H264/90000",
		"a=fmtp:96 packetization-mode=1; sprop-parameter-sets=Z0IAH5WoFAFuQA==,aM48gA==",
		"a=control:trackID=0",
		"m=audio 0 RTP/AVP 97",
		"a=rtpmap:97 MPEG4-GENERIC/44100/2",
		"a=fmtp:97 profile-level-id=1; mode=AAC-hbr; sizelength=13; indexlength=3; indexdeltalength=3; config=1210",
		"a=control:trackID=1",
	}
	for _, line := range want {
		if !strings.Contains(sdp, line) {
			t.Fatalf("SDP missing %q:\n%s", line, sdp)
		}
	}

	// 推流会话关闭（TEARDOWN / 超时）后，注册表里不应再留下这条流。
	sessID := strings.Split(hd["Session"], ";")[0]
	sess, ok := s.sessions.Get(sessID)
	if !ok {
		t.Fatalf("session %s not registered", sessID)
	}
	sess.Close()
	if got := s.publishedFromURI("rtsp://127.0.0.1:8554/push"); got != nil {
		t.Fatal("published stream still registered after push session closed")
	}
}

// TestPullSetupUsesPublishedTrack 拉流 SETUP 按 URL 找到已发布流，
// 使用 ANNOUNCE 的编码参数，并把 mediaTrack 指向共享的 ExternalSource。
func TestPullSetupUsesPublishedTrack(t *testing.T) {
	t.Run("video", func(t *testing.T) {
		tracks, err := parseAnnounceSDP([]byte(ffmpegAnnounceSDP))
		if err != nil {
			t.Fatal(err)
		}
		s, conn, finish := newTestPipe(t)
		pub := s.publish("push", tracks)

		hd := map[string]string{"CSeq": "3"}
		req := &Request{
			Method:  "SETUP",
			URI:     "rtsp://127.0.0.1:8554/push/trackID=0",
			Version: "RTSP/1.0",
			CSeq:    3,
			Headers: map[string]string{
				"Transport": "RTP/AVP/TCP;unicast;interleaved=0-1",
			},
		}
		resp := callAndRead(t, func() error { return s.handleSetup(conn, req, hd) }, finish)
		if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
			t.Fatalf("status = %q", firstLine(resp))
		}

		ids := conn.boundSessions()
		if len(ids) != 1 {
			t.Fatalf("bound sessions = %v, want 1", ids)
		}
		sess, ok := s.sessions.Get(ids[0])
		if !ok {
			t.Fatalf("session %s not registered", ids[0])
		}
		track := sess.Data.(*Session).track("video")
		if track == nil {
			t.Fatal("video track not created")
		}
		if track.isPush {
			t.Fatal("pull SETUP created a push track")
		}
		if track.external != pub.VideoTrack().Src {
			t.Fatal("pull video track does not subscribe the published video source")
		}
		if track.payloadType != 96 || track.encoding != "H264" || track.clockRate != 90000 {
			t.Fatalf("track codec = pt%d %s clock%d, want 96 H264 90000",
				track.payloadType, track.encoding, track.clockRate)
		}
	})

	t.Run("audio", func(t *testing.T) {
		tracks, err := parseAnnounceSDP([]byte(ffmpegAnnounceSDP))
		if err != nil {
			t.Fatal(err)
		}
		s, conn, finish := newTestPipe(t)
		pub := s.publish("push", tracks)

		hd := map[string]string{"CSeq": "4"}
		req := &Request{
			Method:  "SETUP",
			URI:     "rtsp://127.0.0.1:8554/push/trackID=1",
			Version: "RTSP/1.0",
			CSeq:    4,
			Headers: map[string]string{
				"Transport": "RTP/AVP;unicast;client_port=9000-9001",
			},
		}
		resp := callAndRead(t, func() error { return s.handleSetup(conn, req, hd) }, finish)
		if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
			t.Fatalf("status = %q", firstLine(resp))
		}

		ids := conn.boundSessions()
		if len(ids) != 1 {
			t.Fatalf("bound sessions = %v, want 1", ids)
		}
		sess, _ := s.sessions.Get(ids[0])
		track := sess.Data.(*Session).track("audio")
		if track == nil {
			t.Fatal("audio track not created")
		}
		if track.external != pub.AudioTrack().Src {
			t.Fatal("pull audio track does not subscribe the published audio source")
		}
		if track.payloadType != 97 || track.encoding != "MPEG4-GENERIC" || track.clockRate != 44100 {
			t.Fatalf("track codec = pt%d %s clock%d, want 97 MPEG4-GENERIC 44100",
				track.payloadType, track.encoding, track.clockRate)
		}
		sess.Close() // 关掉 SETUP 分配的 UDP 端口
	})
}

// TestStreamH264ForwardsPublishedFrames 模拟 PLAY 后的转发循环：
// 从 ExternalSource 读 NALU → 封 RTP → 按 $ 帧写到 RTSP 连接。
// 先写入 SPS/PPS/IDR 让新订阅者拿到预加载，再写一帧实时数据。
func TestStreamH264ForwardsPublishedFrames(t *testing.T) {
	nalus := [][]byte{
		{0x67, 0x42, 0x00, 0x1e},       // SPS
		{0x68, 0xce, 0x3c, 0x80},       // PPS
		{0x65, 0x88, 0x84, 0x00, 0x0a}, // IDR
		{0x41, 0x9a, 0x00, 0x02},       // non-IDR slice
	}
	src := source.NewExternalSource()
	for _, nalu := range nalus[:3] {
		src.Write(source.Frame{Data: nalu, Keyframe: nalu[0] == 0x65})
	}

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	s := New(nil, nil, false)
	track := &mediaTrack{
		kind:           "video",
		isInterleaved:  true,
		interleavedRTP: 0,
		conn:           &Conn{Conn: serverConn},
		payloadType:    96,
	}
	done := make(chan struct{})
	defer close(done)
	go s.streamH264(track, src, done)

	br := bufio.NewReader(clientConn)
	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))

	got := make([][]byte, 0, len(nalus))
	for i := 0; i < 3; i++ {
		got = append(got, readInterleavedPayload(t, br, 0))
	}
	// 订阅已经建立，再写实时帧，验证它不会被漏掉。
	src.Write(source.Frame{Data: nalus[3], Timestamp: 3600})
	got = append(got, readInterleavedPayload(t, br, 0))

	for i, raw := range got {
		pkt, err := rtp.Parse(raw)
		if err != nil {
			t.Fatalf("frame %d: parse RTP: %v", i, err)
		}
		if pkt.Header.PT != 96 {
			t.Fatalf("frame %d: PT = %d, want 96", i, pkt.Header.PT)
		}
		if !bytes.Equal(pkt.Payload, nalus[i]) {
			t.Fatalf("frame %d: payload = % x, want % x", i, pkt.Payload, nalus[i])
		}
	}
}

// TestPushTeardownClosesSourceAndRemovesSession 推流端 TEARDOWN 后：
//   - 会话从管理器删除；
//   - 已发布流从注册表摘掉，ExternalSource 关闭，拉流端读者读到结束；
//   - 之后再来的 RTP 不再产生新帧。
func TestPushTeardownClosesSourceAndRemovesSession(t *testing.T) {
	s := New(nil, nil, false)
	conn, closeConn := newDrainedConn(t)
	defer closeConn()

	hd := map[string]string{"CSeq": "1"}
	req := &Request{
		Method:  "ANNOUNCE",
		URI:     "rtsp://127.0.0.1:8554/push",
		Version: "RTSP/1.0",
		CSeq:    1,
		Headers: map[string]string{"Content-Type": "application/sdp"},
		Body:    []byte(ffmpegAnnounceSDP),
	}
	if err := s.handleAnnounce(conn, req, hd); err != nil {
		t.Fatalf("handleAnnounce: %v", err)
	}
	sessID := strings.Split(hd["Session"], ";")[0]
	pub := s.publishedFromURI("rtsp://127.0.0.1:8554/push")
	if pub == nil {
		t.Fatal("ANNOUNCE did not register a published stream")
	}

	rd, cancel := pub.VideoTrack().Src.Subscribe()
	defer cancel()
	pub.VideoTrack().Src.Write(source.Frame{Data: []byte{0x41, 0x01}, Timestamp: 3600})

	thd := map[string]string{"CSeq": "2"}
	td := &Request{
		Method:  "TEARDOWN",
		URI:     "rtsp://127.0.0.1:8554/push",
		Version: "RTSP/1.0",
		CSeq:    2,
		Headers: map[string]string{"Session": sessID},
	}
	if err := s.handleTeardown(conn, td, thd); err != nil {
		t.Fatalf("handleTeardown: %v", err)
	}

	if _, ok := s.sessions.Get(sessID); ok {
		t.Fatal("push session still registered after TEARDOWN")
	}
	if got := s.publishedFromURI("rtsp://127.0.0.1:8554/push"); got != nil {
		t.Fatal("published stream still registered after TEARDOWN")
	}

	done := make(chan struct{})
	defer close(done)
	if f, ok := rd.Next(done); !ok || f.Data[0] != 0x41 {
		t.Fatalf("pending frame = %x ok=%v, want the frame written before teardown", f.Data, ok)
	}
	if _, ok := rd.Next(done); ok {
		t.Fatal("ExternalSource still open after TEARDOWN; pull readers would block")
	}
}

// TestPushTCPDisconnectClosesSourceAndRemovesSession 模拟推流端 Ctrl-C：
// TCP 连接直接断开（没发 TEARDOWN）时，handleConn 的清理路径必须
// 立刻删除会话、注销已发布流并关闭源，不用等 60s 超时。
func TestPushTCPDisconnectClosesSourceAndRemovesSession(t *testing.T) {
	s := New(nil, nil, false)
	serverConn, clientConn := net.Pipe()
	conn := &Conn{Conn: serverConn}
	go s.handleConn(conn)

	sdp := ffmpegAnnounceSDP
	raw := fmt.Sprintf("ANNOUNCE rtsp://127.0.0.1:8554/push RTSP/1.0\r\n"+
		"CSeq: 1\r\nContent-Type: application/sdp\r\nContent-Length: %d\r\n\r\n%s",
		len(sdp), sdp)
	if _, err := clientConn.Write([]byte(raw)); err != nil {
		t.Fatalf("write ANNOUNCE: %v", err)
	}
	br := bufio.NewReader(clientConn)
	if resp := readResponseHeader(t, br); !strings.HasPrefix(resp, "RTSP/1.0 200 OK") {
		t.Fatalf("ANNOUNCE response = %q", resp)
	}

	pub := s.publishedFromURI("rtsp://127.0.0.1:8554/push")
	if pub == nil {
		t.Fatal("ANNOUNCE did not register a published stream")
	}
	rd, cancel := pub.VideoTrack().Src.Subscribe()
	defer cancel()
	if got := s.sessions.Len(); got != 1 {
		t.Fatalf("sessions = %d, want 1", got)
	}

	clientConn.Close() // 推流端进程消失，TCP 断开

	waitFor(t, 2*time.Second, func() bool {
		return s.publishedFromURI("rtsp://127.0.0.1:8554/push") == nil && s.sessions.Len() == 0
	})
	expectReaderEnded(t, rd)
}

// TestPullSourceEndClosesSessionAndConn 推流端断开后，拉流端源关闭：
// 拉流会话要被立即回收、TCP 连接关闭，客户端读到 EOF 而不是一直等 RTP。
func TestPullSourceEndClosesSessionAndConn(t *testing.T) {
	s := New(nil, nil, false)
	tracks, err := parseAnnounceSDP([]byte(ffmpegAnnounceSDP))
	if err != nil {
		t.Fatal(err)
	}
	pub := s.publish("live", tracks)

	serverConn, clientConn := net.Pipe()
	conn := &Conn{Conn: serverConn}
	sess := s.newClientSession(conn)
	st := sess.Data.(*Session)
	st.setPublished(pub, nil)

	track := &mediaTrack{
		kind:            "video",
		isInterleaved:   true,
		interleavedRTP:  0,
		interleavedRTCP: 1,
		conn:            conn,
		payloadType:     96,
		external:        pub.VideoTrack().Src,
	}
	st.setTrack("video", track)

	drained := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, clientConn)
		close(drained)
	}()

	hd := map[string]string{"CSeq": "3"}
	req := &Request{
		Method:  "PLAY",
		URI:     "rtsp://127.0.0.1:8554/live",
		Version: "RTSP/1.0",
		CSeq:    3,
		Headers: map[string]string{"Session": sess.ID},
	}
	if err := s.handlePlay(conn, req, hd); err != nil {
		t.Fatalf("handlePlay: %v", err)
	}

	// 推流端断开 = 已发布流关闭，拉流端 reader 会读到结束。
	s.unpublish("live", pub)

	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("pull connection was not closed after the source ended")
	}
	if _, ok := s.sessions.Get(sess.ID); ok {
		t.Fatal("pull session still registered after the source ended")
	}
}

// newDrainedConn 建一条 net.Pipe 包装的 Conn，后台丢弃所有写出的响应，
// 让 handler 的同步写不会阻塞。
func newDrainedConn(t *testing.T) (*Conn, func()) {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	go func() { _, _ = io.Copy(io.Discard, clientConn) }()
	return &Conn{Conn: serverConn}, func() {
		clientConn.Close()
		serverConn.Close()
	}
}

// readResponseHeader 读到响应头的空行为止，返回已读文本。
func readResponseHeader(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	var b strings.Builder
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("read response header: %v", err)
		}
		b.WriteString(line)
		if line == "\r\n" {
			return b.String()
		}
	}
}

// waitFor 轮询条件，超时则让测试失败。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met before timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// expectReaderEnded 断言读者在源关闭后返回 false（读不到更多帧）。
func expectReaderEnded(t *testing.T, rd *source.Reader[source.Frame]) {
	t.Helper()
	done := make(chan struct{})
	defer close(done)
	result := make(chan bool, 1)
	go func() {
		_, ok := rd.Next(done)
		result <- ok
	}()
	select {
	case ok := <-result:
		if ok {
			t.Fatal("reader returned a frame, want end of stream")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reader did not observe end of stream")
	}
}

// readInterleavedPayload 从 RTSP TCP 流里读一个 $ 帧，返回 RTP 负载。
func readInterleavedPayload(t *testing.T, br *bufio.Reader, wantChannel byte) []byte {
	t.Helper()
	var hdr [4]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		t.Fatalf("read interleaved header: %v", err)
	}
	if hdr[0] != interleavedMagic {
		t.Fatalf("bad interleaved magic: %#x", hdr[0])
	}
	if hdr[1] != wantChannel {
		t.Fatalf("interleaved channel = %d, want %d", hdr[1], wantChannel)
	}
	n := int(binary.BigEndian.Uint16(hdr[2:4]))
	payload := make([]byte, n)
	if _, err := io.ReadFull(br, payload); err != nil {
		t.Fatalf("read interleaved payload: %v", err)
	}
	return payload
}

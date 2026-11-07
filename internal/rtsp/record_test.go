package rtsp

import (
	"net"
	"testing"

	"flow/internal/rtp"
	"flow/internal/session"
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

	// 普通 P 帧（类型 1）。
	s.handlePushedRTP(st, track, makeRTP(96, 10, 9000, 0x11223344, []byte{0x41, 0xAA, 0xBB}, false))
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
	s.handlePushedRTP(st, track, makeRTP(96, 23, 45000, 0xabcd, []byte{0x41, 0x01}, false))
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

package rtsp

import (
	"io"
	"net"
	"testing"
	"time"

	"flow/internal/admin"
	"flow/internal/session"
	"flow/internal/stream"
)

// setupPublisher 建一条推流会话并登记已发布流，返回订阅好的读者。
func setupPublisher(t *testing.T, name string) (*Server, *Conn, *session.Session, *stream.Stream, func() string) {
	t.Helper()
	tracks, err := parseAnnounceSDP([]byte(ffmpegAnnounceSDP))
	if err != nil {
		t.Fatalf("parse announce: %v", err)
	}
	s, conn, finish := newTestPipe(t)
	pub, err := s.publish(name, tracks)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	sess, err := s.newClientSession(conn)
	if err != nil {
		t.Fatalf("newClientSession: %v", err)
	}
	st := sess.Data.(*Session)
	st.setAnnounced(tracks) // 标记为推流会话
	st.setPublished(pub, func() { s.unpublish(name, pub) })
	conn.bindSession(sess.ID)
	return s, conn, sess, pub, finish
}

func TestAdminSessionsReportsPublisher(t *testing.T) {
	s, _, sess, _, finish := setupPublisher(t, "live/push")
	defer finish()

	list := s.Sessions()
	if len(list) != 1 {
		t.Fatalf("sessions = %d, want 1", len(list))
	}
	got := list[0]
	if got.ID != sess.ID || got.Protocol != "rtsp" {
		t.Fatalf("session = %+v", got)
	}
	if got.Direction != admin.DirectionPublish || got.Stream != "live/push" {
		t.Fatalf("direction/stream = %q/%q", got.Direction, got.Stream)
	}
}

func TestAdminKickStreamClosesPublisherAndSource(t *testing.T) {
	s, _, sess, pub, finish := setupPublisher(t, "live/push")
	defer finish()

	rd, cancel := pub.VideoTrack().Src.Subscribe()
	defer cancel()

	n, err := s.KickStream("live/push")
	if err != nil || n != 1 {
		t.Fatalf("KickStream = %d, %v; want 1, nil", n, err)
	}
	if _, ok := s.sessions.Get(sess.ID); ok {
		t.Fatal("session still registered after admin kick")
	}
	expectReaderEnded(t, rd)
	if got, _ := s.KickStream("live/push"); got != 0 {
		t.Fatalf("second KickStream = %d, want 0", got)
	}
}

func TestAdminKickSessionStopsPull(t *testing.T) {
	s := New(nil, nil, false)
	tracks, err := parseAnnounceSDP([]byte(ffmpegAnnounceSDP))
	if err != nil {
		t.Fatal(err)
	}
	pub, err := s.publish("live", tracks)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	serverConn, clientConn := net.Pipe()
	conn := &Conn{Conn: serverConn}
	sess, err := s.newClientSession(conn)
	if err != nil {
		t.Fatalf("newClientSession: %v", err)
	}
	st := sess.Data.(*Session)
	st.setPublished(pub, nil)
	st.setTrack("video", &mediaTrack{
		kind:            "video",
		isInterleaved:   true,
		interleavedRTP:  0,
		interleavedRTCP: 1,
		conn:            conn,
		payloadType:     96,
		external:        pub.VideoTrack().Src,
	})

	drained := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, clientConn)
		close(drained)
	}()

	if ok, err := s.KickSession(sess.ID); err != nil || !ok {
		t.Fatalf("KickSession = %v, %v", ok, err)
	}
	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("pull connection was not closed after admin kick")
	}
	if _, ok := s.sessions.Get(sess.ID); ok {
		t.Fatal("pull session still registered after admin kick")
	}
	if ok, err := s.KickSession(sess.ID); err != nil || ok {
		t.Fatalf("second KickSession = %v, %v; want false, nil", ok, err)
	}
	// 拉流会话被踢不应该注销已发布流：推流仍可继续服务其他观众。
	if s.publishedStreamByName("live") != pub {
		t.Fatal("published stream was removed by kicking a pull session")
	}
}

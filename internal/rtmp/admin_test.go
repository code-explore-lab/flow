package rtmp

import (
	"net"
	"testing"
	"time"

	"flow/internal/admin"
	"flow/internal/stream"
)

func newAdminTestConn(t *testing.T, s *Server) (*Conn, net.Conn) {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	c := newConn(serverConn)
	t.Cleanup(func() {
		c.Close()
		clientConn.Close()
	})
	s.registerConn(c)
	return c, clientConn
}

func TestAdminSessionsReportsPublish(t *testing.T) {
	s := New(stream.NewRegistry(), nil, nil)
	c, _ := newAdminTestConn(t, s)
	c.setAdminApp("live")
	c.setAdminPublish("live/push", true)

	list := s.Sessions()
	if len(list) != 1 {
		t.Fatalf("sessions = %d, want 1", len(list))
	}
	got := list[0]
	if got.ID != c.id || got.Protocol != "rtmp" {
		t.Fatalf("session = %+v", got)
	}
	if got.Direction != admin.DirectionPublish || got.Stream != "live/push" {
		t.Fatalf("direction/stream = %q/%q", got.Direction, got.Stream)
	}
	if got.LastActive.IsZero() || got.LastActive.Before(got.CreatedAt) {
		t.Fatalf("lastActive = %v, createdAt = %v", got.LastActive, got.CreatedAt)
	}
}

func TestAdminKickStreamClosesConnection(t *testing.T) {
	s := New(stream.NewRegistry(), nil, nil)
	c, clientConn := newAdminTestConn(t, s)
	c.setAdminPublish("live/push", true)

	n, err := s.KickStream("live/push")
	if err != nil || n != 1 {
		t.Fatalf("KickStream = %d, %v; want 1, nil", n, err)
	}

	_ = clientConn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := clientConn.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection still open after KickStream")
	}
	if got, _ := s.KickStream("other"); got != 0 {
		t.Fatalf("KickStream(other) = %d, want 0", got)
	}
}

func TestAdminKickSession(t *testing.T) {
	s := New(stream.NewRegistry(), nil, nil)
	c, clientConn := newAdminTestConn(t, s)
	c.setAdminPlaying("live/play", true)

	if ok, err := s.KickSession(c.id); err != nil || !ok {
		t.Fatalf("KickSession = %v, %v; want true, nil", ok, err)
	}
	_ = clientConn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := clientConn.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection still open after KickSession")
	}
	if ok, err := s.KickSession("rtmp-missing"); err != nil || ok {
		t.Fatalf("KickSession(unknown) = %v, %v; want false, nil", ok, err)
	}
}

func TestAdminPlayingState(t *testing.T) {
	s := New(stream.NewRegistry(), nil, nil)
	c, _ := newAdminTestConn(t, s)
	c.setAdminPlaying("live/play", true)

	list := s.Sessions()
	if len(list) != 1 || list[0].Direction != admin.DirectionPlay || list[0].State != statePlaying {
		t.Fatalf("sessions = %+v", list)
	}
	c.setAdminPlaying("", false)
	list = s.Sessions()
	if len(list) != 1 || list[0].Direction != admin.DirectionIdle || list[0].Stream != "" {
		t.Fatalf("after stop: %+v", list)
	}
}

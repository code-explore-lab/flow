package discovery

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"flow/internal/source"
	"flow/internal/stream"
)

func TestAnnouncerCollect(t *testing.T) {
	reg := stream.NewRegistry()
	c := NewClient("http://example.invalid", "node-a", 5*time.Second)
	a := NewAnnouncer(c, reg, map[string]string{
		"rtsp": "rtsp://10.0.0.1:8554/",
		"rtmp": "rtmp://10.0.0.1:1935",
	}, time.Second, time.Second)

	st := &stream.Stream{}
	if _, err := reg.Publish("live/x", st); err != nil {
		t.Fatal(err)
	}
	st.EnsureVideo().Src.Write(source.Frame{Data: []byte{0x65}, Timestamp: 0})

	got := a.collect()
	if len(got) != 1 || got[0].Name != "live/x" {
		t.Fatalf("collect = %+v", got)
	}
	if got[0].URLs["rtsp"] != "rtsp://10.0.0.1:8554/live/x" {
		t.Fatalf("rtsp url = %q", got[0].URLs["rtsp"])
	}
	if got[0].URLs["rtmp"] != "rtmp://10.0.0.1:1935/live/x" {
		t.Fatalf("rtmp url = %q", got[0].URLs["rtmp"])
	}
}

func TestAnnouncerCollectSkipsInactive(t *testing.T) {
	reg := stream.NewRegistry()
	c := NewClient("http://example.invalid", "node-a", 5*time.Second)
	a := NewAnnouncer(c, reg, map[string]string{"rtsp": "rtsp://10.0.0.1:8554"}, time.Second, 10*time.Millisecond)

	if _, err := reg.Publish("live/stale", &stream.Stream{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if got := a.collect(); len(got) != 0 {
		t.Fatalf("collect advertised inactive stream: %+v", got)
	}
}

func TestAnnouncerSyncsRegistry(t *testing.T) {
	store := NewStore(5 * time.Second)
	srv := httptest.NewServer(NewServer(store).Handler())
	defer srv.Close()

	reg := stream.NewRegistry()
	c := NewClient(srv.URL, "node-a", 5*time.Second)
	a := NewAnnouncer(c, reg, map[string]string{"rtsp": "rtsp://node-a:8554"},
		50*time.Millisecond, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.Run(ctx)

	st := &stream.Stream{}
	if _, err := reg.Publish("live/x", st); err != nil {
		t.Fatal(err)
	}
	st.EnsureVideo().Src.Write(source.Frame{Data: []byte{0x65}, Timestamp: 0})
	waitFor(t, 2*time.Second, func() bool {
		_, ok := store.Lookup("live/x")
		return ok
	}, "announcer did not register live/x")

	reg.Unpublish("live/x", st)
	waitFor(t, 2*time.Second, func() bool {
		_, ok := store.Lookup("live/x")
		return !ok
	}, "announcer did not clear live/x after unpublish")

	// Run 退出时发送空批次，清掉本节点全部租约。
	st2 := &stream.Stream{}
	if _, err := reg.Publish("live/y", st2); err != nil {
		t.Fatal(err)
	}
	st2.EnsureVideo().Src.Write(source.Frame{Data: []byte{0x65}, Timestamp: 0})
	waitFor(t, 2*time.Second, func() bool {
		_, ok := store.Lookup("live/y")
		return ok
	}, "announcer did not register live/y")
	cancel()
	waitFor(t, 2*time.Second, func() bool {
		_, ok := store.Lookup("live/y")
		return !ok
	}, "announcer did not clear leases on shutdown")
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(msg)
}

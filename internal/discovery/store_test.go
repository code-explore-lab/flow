package discovery

import (
	"testing"
	"time"
)

func fixedClock(now *time.Time) func() time.Time {
	return func() time.Time { return *now }
}

func testStreams(names ...string) []Stream {
	out := make([]Stream, 0, len(names))
	for _, n := range names {
		out = append(out, Stream{Name: n, URLs: map[string]string{"rtsp": "rtsp://node/" + n}})
	}
	return out
}

func TestStoreRegisterAndLookup(t *testing.T) {
	now := time.Unix(1000, 0)
	s := NewStore(10 * time.Second)
	s.SetClock(fixedClock(&now))

	res := s.RegisterBatch("node-a", testStreams("live/x"), 10*time.Second)
	if len(res.Registered) != 1 || len(res.Conflicts) != 0 || len(res.Rejected) != 0 {
		t.Fatalf("result = %+v", res)
	}
	e, ok := s.Lookup("live/x")
	if !ok {
		t.Fatal("lookup miss")
	}
	if e.Node != "node-a" || e.URL("rtsp") != "rtsp://node/live/x" {
		t.Fatalf("entry = %+v", e)
	}
	if got := e.TTL(now); got != 10*time.Second {
		t.Fatalf("ttl = %s, want 10s", got)
	}
}

func TestStoreLeaseExpiresAtBoundary(t *testing.T) {
	now := time.Unix(1000, 0)
	s := NewStore(10 * time.Second)
	s.SetClock(fixedClock(&now))
	s.RegisterBatch("node-a", testStreams("live/x"), 10*time.Second)

	now = now.Add(10*time.Second - time.Millisecond)
	if _, ok := s.Lookup("live/x"); !ok {
		t.Fatal("lease expired 1ms early")
	}
	now = now.Add(time.Millisecond)
	if _, ok := s.Lookup("live/x"); ok {
		t.Fatal("lease still valid at expiry boundary")
	}
	if s.Len() != 0 {
		t.Fatalf("expired entry not removed lazily: len=%d", s.Len())
	}
}

func TestStoreBatchReplacesNodeSet(t *testing.T) {
	now := time.Unix(1000, 0)
	s := NewStore(10 * time.Second)
	s.SetClock(fixedClock(&now))

	s.RegisterBatch("node-a", testStreams("live/x", "live/y"), 10*time.Second)
	s.RegisterBatch("node-a", testStreams("live/x"), 10*time.Second)

	if _, ok := s.Lookup("live/x"); !ok {
		t.Fatal("live/x disappeared")
	}
	if _, ok := s.Lookup("live/y"); ok {
		t.Fatal("live/y still registered after being omitted from heartbeat")
	}
}

func TestStoreConflictWithLiveOwner(t *testing.T) {
	now := time.Unix(1000, 0)
	s := NewStore(10 * time.Second)
	s.SetClock(fixedClock(&now))

	s.RegisterBatch("node-a", testStreams("live/x"), 10*time.Second)
	res := s.RegisterBatch("node-b", testStreams("live/x"), 10*time.Second)
	if len(res.Conflicts) != 1 || res.Conflicts[0] != "live/x" {
		t.Fatalf("conflicts = %v, want [live/x]", res.Conflicts)
	}
	e, _ := s.Lookup("live/x")
	if e.Node != "node-a" {
		t.Fatalf("owner = %q, want node-a", e.Node)
	}

	// 旧租约过期后，新节点可以接管。
	now = now.Add(10 * time.Second)
	res = s.RegisterBatch("node-b", testStreams("live/x"), 10*time.Second)
	if len(res.Registered) != 1 {
		t.Fatalf("takeover result = %+v", res)
	}
	e, _ = s.Lookup("live/x")
	if e.Node != "node-b" {
		t.Fatalf("owner after takeover = %q, want node-b", e.Node)
	}
}

func TestStoreUnregisterOwnership(t *testing.T) {
	s := NewStore(time.Second)
	s.RegisterBatch("node-a", testStreams("live/x"), time.Second)

	if s.Unregister("node-b", "live/x") {
		t.Fatal("another node removed the lease")
	}
	if _, ok := s.Lookup("live/x"); !ok {
		t.Fatal("lease disappeared after foreign unregister")
	}
	if !s.Unregister("node-a", "live/x") {
		t.Fatal("owner unregister failed")
	}
	if _, ok := s.Lookup("live/x"); ok {
		t.Fatal("lease survived owner unregister")
	}
}

func TestStoreRejectsInvalidStreams(t *testing.T) {
	s := NewStore(time.Second)
	res := s.RegisterBatch("node-a", []Stream{
		{Name: ""},
		{Name: "no/urls"},
		{Name: "live/x", URLs: map[string]string{"rtsp": "rtsp://node/live/x"}},
	}, time.Second)
	if len(res.Rejected) != 2 {
		t.Fatalf("rejected = %v, want two entries", res.Rejected)
	}
	if len(res.Registered) != 1 {
		t.Fatalf("registered = %v", res.Registered)
	}
}

func TestStoreListAndSweep(t *testing.T) {
	now := time.Unix(1000, 0)
	s := NewStore(10 * time.Second)
	s.SetClock(fixedClock(&now))
	s.RegisterBatch("node-b", testStreams("live/z"), 10*time.Second)
	s.RegisterBatch("node-a", testStreams("live/a"), 10*time.Second)

	list := s.List()
	if len(list) != 2 || list[0].Name != "live/a" || list[1].Name != "live/z" {
		t.Fatalf("list = %+v", list)
	}

	now = now.Add(11 * time.Second)
	if got := s.Sweep(); got != 2 {
		t.Fatalf("sweep = %d, want 2", got)
	}
	if s.Len() != 0 || len(s.List()) != 0 {
		t.Fatalf("store not empty after sweep: len=%d", s.Len())
	}
}

func TestParseAdvertise(t *testing.T) {
	m, err := ParseAdvertise(" rtsp://10.0.0.5:8554/ , rtmp://10.0.0.5:1935")
	if err != nil {
		t.Fatal(err)
	}
	if m["rtsp"] != "rtsp://10.0.0.5:8554" || m["rtmp"] != "rtmp://10.0.0.5:1935" {
		t.Fatalf("advertise = %v", m)
	}
	for _, raw := range []string{"", "ftp://x", "rtsp://", "rtsp://x/?a=1"} {
		if _, err := ParseAdvertise(raw); err == nil {
			t.Fatalf("ParseAdvertise(%q) succeeded, want error", raw)
		}
	}
}

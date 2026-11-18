package discovery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newTestRegistry(t *testing.T) (*Store, *Client, func()) {
	t.Helper()
	store := NewStore(5 * time.Second)
	srv := httptest.NewServer(NewServer(store).Handler())
	c := NewClient(srv.URL, "node-a", 5*time.Second)
	return store, c, srv.Close
}

func TestClientHeartbeatLookupDelete(t *testing.T) {
	_, c, cleanup := newTestRegistry(t)
	defer cleanup()
	ctx := context.Background()

	res, err := c.Heartbeat(ctx, []Stream{
		{Name: "live/x", URLs: map[string]string{"rtsp": "rtsp://a/live/x"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Registered) != 1 || res.Registered[0] != "live/x" {
		t.Fatalf("heartbeat result = %+v", res)
	}

	e, ok, err := c.Lookup(ctx, "live/x")
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	if e.Node != "node-a" || e.URL("rtsp") != "rtsp://a/live/x" {
		t.Fatalf("entry = %+v", e)
	}

	if _, ok, err := c.Lookup(ctx, "live/missing"); err != nil || ok {
		t.Fatalf("missing lookup: ok=%v err=%v", ok, err)
	}

	// 空批次清空本节点租约。
	if _, err := c.Heartbeat(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := c.Lookup(ctx, "live/x"); ok {
		t.Fatal("empty heartbeat did not clear lease")
	}

	// Delete 幂等。
	if err := c.Delete(ctx, "live/x"); err != nil {
		t.Fatal(err)
	}
}

func TestClientDeleteOnOwnedEntry(t *testing.T) {
	store, c, cleanup := newTestRegistry(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := c.Heartbeat(ctx, []Stream{
		{Name: "live/x", URLs: map[string]string{"rtsp": "rtsp://a/live/x"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, "live/x"); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Lookup("live/x"); ok {
		t.Fatal("delete did not remove lease")
	}
}

func TestServerRejectsBadRequests(t *testing.T) {
	store := NewStore(time.Second)
	srv := httptest.NewServer(NewServer(store).Handler())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/streams", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("empty body status = %d, want 400", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/v1/streams/live/x", nil)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("delete without node status = %d, want 400", resp2.StatusCode)
	}
}

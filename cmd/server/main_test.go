package main

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"flow/internal/discovery"
)

func TestLoopbackListenAddr(t *testing.T) {
	tests := []struct {
		in   string
		want string
		ok   bool
	}{
		{":6060", "127.0.0.1:6060", true},
		{"127.0.0.1:6060", "127.0.0.1:6060", true},
		{"127.0.0.2:6060", "127.0.0.2:6060", true},
		{"[::1]:6060", "[::1]:6060", true},
		{"0.0.0.0:6060", "", false},
		{"192.168.1.10:6060", "", false},
		{"localhost:6060", "", false},
		{"6060", "", false},
		{":", "", false},
	}
	for _, tt := range tests {
		got, err := loopbackListenAddr(tt.in)
		if tt.ok {
			if err != nil {
				t.Errorf("loopbackListenAddr(%q) 返回错误: %v", tt.in, err)
				continue
			}
			if got != tt.want {
				t.Errorf("loopbackListenAddr(%q) = %q, want %q", tt.in, got, tt.want)
			}
			continue
		}
		if err == nil {
			t.Errorf("loopbackListenAddr(%q) = %q, 应当拒绝", tt.in, got)
		}
	}
}

// TestDiscoveryFinderSkipsSelf 验证指向本节点的条目不会被重定向回去，
// 否则客户端会在两个地址之间来回跳。
func TestDiscoveryFinderSkipsSelf(t *testing.T) {
	store := discovery.NewStore(time.Minute)
	srv := httptest.NewServer(discovery.NewServer(store).Handler())
	defer srv.Close()

	ctx := context.Background()
	self := discovery.NewClient(srv.URL, "node-a", time.Minute)
	other := discovery.NewClient(srv.URL, "node-b", time.Minute)
	if _, err := self.Heartbeat(ctx, []discovery.Stream{
		{Name: "live/self", URLs: map[string]string{"rtsp": "rtsp://node-a/live/self"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := other.Heartbeat(ctx, []discovery.Stream{
		{Name: "live/other", URLs: map[string]string{"rtsp": "rtsp://node-b/live/other"}},
	}); err != nil {
		t.Fatal(err)
	}

	f := discoveryFinder{client: self}
	if _, ok := f.Find(ctx, "live/self"); ok {
		t.Fatal("finder redirected to the local node")
	}
	if u, ok := f.Find(ctx, "live/other"); !ok || u != "rtsp://node-b/live/other" {
		t.Fatalf("other node lookup = %q, ok=%v", u, ok)
	}
}

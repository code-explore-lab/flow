package main

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"flow/internal/discovery"
	"flow/internal/transcode"
)

func TestValidateHLSOptions(t *testing.T) {
	tests := []struct {
		name    string
		segment time.Duration
		part    time.Duration
		window  int
		idle    time.Duration
		max     int64
		ok      bool
	}{
		{"defaults", 4 * time.Second, 500 * time.Millisecond, 6, 60 * time.Second, 64 << 20, true},
		{"classic", 4 * time.Second, 0, 3, 0, 0, true},
		{"zero segment", 0, 0, 6, time.Minute, 0, false},
		{"part equals segment", 4 * time.Second, 4 * time.Second, 6, time.Minute, 0, false},
		{"negative part", 4 * time.Second, -time.Millisecond, 6, time.Minute, 0, false},
		{"window too small", 4 * time.Second, time.Second, 2, time.Minute, 0, false},
		{"negative idle", 4 * time.Second, time.Second, 6, -time.Second, 0, false},
		{"negative max bytes", 4 * time.Second, time.Second, 6, time.Minute, -1, false},
	}
	for _, tt := range tests {
		err := validateHLSOptions(tt.segment, tt.part, tt.window, tt.idle, tt.max)
		if tt.ok && err != nil {
			t.Errorf("%s: unexpected error %v", tt.name, err)
		}
		if !tt.ok && err == nil {
			t.Errorf("%s: expected error", tt.name)
		}
	}
}

func TestValidateDVROptions(t *testing.T) {
	tests := []struct {
		name    string
		window  time.Duration
		max     int64
		wantErr bool
	}{
		{"disabled", 0, 0, false},
		{"five minutes", 5 * time.Minute, 0, false},
		{"negative window", -time.Second, 0, true},
		{"negative bytes", time.Minute, -1, true},
	}
	for _, tt := range tests {
		err := validateDVROptions(tt.window, tt.max)
		if tt.wantErr && err == nil {
			t.Errorf("%s: expected error", tt.name)
		}
		if !tt.wantErr && err != nil {
			t.Errorf("%s: unexpected error %v", tt.name, err)
		}
	}
}

func TestABRGroupsFromProfiles(t *testing.T) {
	profiles, err := transcode.ParseProfiles(
		"live/a=w=1280,h=720,bitrate=3m;live/a=w=640,h=360,bitrate=800k;" +
			"live/b=w=640,h=360,bitrate=600k;live/c=size=320x180")
	if err != nil {
		t.Fatalf("ParseProfiles: %v", err)
	}
	groups := abrGroups(profiles)
	if len(groups) != 1 {
		t.Fatalf("groups = %d, want 1 (only live/a has >=2 variants)", len(groups))
	}
	g := groups[0]
	if g.Source != "live/a" || len(g.Variants) != 2 {
		t.Fatalf("group = %+v", g)
	}
	if g.Variants[0].Stream != "live/a_720p" || g.Variants[0].Width != 1280 ||
		g.Variants[0].Bitrate != 3_000_000 || g.Variants[0].Codec != "H264" {
		t.Errorf("variant[0] = %+v", g.Variants[0])
	}
	if g.Variants[1].Stream != "live/a_360p" || g.Variants[1].Height != 360 {
		t.Errorf("variant[1] = %+v", g.Variants[1])
	}
}

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

	f := discoveryFinder{client: self, self: "node-a", maxHops: 1}
	if _, ok := f.Find(ctx, "live/self"); ok {
		t.Fatal("finder redirected to the local node")
	}
	if u, ok := f.Find(ctx, "live/other"); !ok || u != "rtsp://node-b/live/other" {
		t.Fatalf("other node lookup = %q, ok=%v", u, ok)
	}
}

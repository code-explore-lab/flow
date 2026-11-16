package main

import "testing"

func TestPprofListenAddr(t *testing.T) {
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
		got, err := pprofListenAddr(tt.in)
		if tt.ok {
			if err != nil {
				t.Errorf("pprofListenAddr(%q) 返回错误: %v", tt.in, err)
				continue
			}
			if got != tt.want {
				t.Errorf("pprofListenAddr(%q) = %q, want %q", tt.in, got, tt.want)
			}
			continue
		}
		if err == nil {
			t.Errorf("pprofListenAddr(%q) = %q, 应当拒绝", tt.in, got)
		}
	}
}

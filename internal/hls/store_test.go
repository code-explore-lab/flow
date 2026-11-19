package hls

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMemStore(t *testing.T) {
	s := newMemStore()
	if err := s.Put("video/seg-0.m4s", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if got, ok := s.Get("video/seg-0.m4s"); !ok || string(got) != "abc" {
		t.Fatalf("get = %q, %v", got, ok)
	}
	if s.Size("video/seg-0.m4s") != 3 || s.Bytes() != 3 {
		t.Fatalf("size/bytes = %d/%d", s.Size("video/seg-0.m4s"), s.Bytes())
	}
	// 覆盖写要正确调整计数。
	_ = s.Put("video/seg-0.m4s", []byte("abcde"))
	if s.Bytes() != 5 {
		t.Fatalf("bytes after overwrite = %d", s.Bytes())
	}
	s.Delete("video/seg-0.m4s")
	if _, ok := s.Get("video/seg-0.m4s"); ok {
		t.Fatal("deleted uri still readable")
	}
	if s.Bytes() != 0 {
		t.Fatalf("bytes after delete = %d", s.Bytes())
	}
	s.Close() // 空操作
}

func TestDiskStore(t *testing.T) {
	root := filepath.Join(t.TempDir(), "stream-1")
	s, err := newDiskStore(root, "video")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("video/seg-0.m4s", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	// 段必须真的落到 root/video/seg-0.m4s。
	p := filepath.Join(root, "video", "seg-0.m4s")
	if b, err := os.ReadFile(p); err != nil || string(b) != "hello" {
		t.Fatalf("disk file = %q, %v", b, err)
	}
	if got, ok := s.Get("video/seg-0.m4s"); !ok || string(got) != "hello" {
		t.Fatalf("get = %q, %v", got, ok)
	}
	if s.Size("video/seg-0.m4s") != 5 || s.Bytes() != 5 {
		t.Fatalf("size/bytes = %d/%d", s.Size("video/seg-0.m4s"), s.Bytes())
	}
	s.Delete("video/seg-0.m4s")
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatalf("file still exists: %v", err)
	}
	if s.Bytes() != 0 {
		t.Fatalf("bytes after delete = %d", s.Bytes())
	}

	// 非法 URI 不能逃出 DVR 目录。
	for _, uri := range []string{"audio/seg-0.m4s", "../x.m4s", "video/../../x.m4s", "/abs.m4s", ""} {
		if err := s.Put(uri, []byte("x")); err == nil {
			t.Errorf("Put(%q) accepted", uri)
		}
	}

	if err := s.Put("video/seg-1.m4s", []byte("x")); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if _, err := os.Stat(filepath.Join(root, "video")); !os.IsNotExist(err) {
		t.Fatalf("kind dir still exists after Close: %v", err)
	}
	// 另一条轨的目录不应被本轨 Close 删除。
	if err := os.MkdirAll(filepath.Join(root, "audio"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "audio")); err != nil {
		t.Fatalf("audio dir removed by video store Close: %v", err)
	}
}

func TestPrepareDVR(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "dvr")
	if err := os.MkdirAll(filepath.Join(dir, "old-stream"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "old-stream", "stale.m4s"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := PrepareDVR(dir); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("PrepareDVR left %d entries", len(entries))
	}
	if err := PrepareDVR(""); err == nil {
		t.Fatal("empty dir should be rejected")
	}
}

func TestSanitizeDVRName(t *testing.T) {
	cases := []struct {
		name string
		want string
	}{
		{"live/test", "live_test"},
		{"../../etc/passwd", ".._.._etc_passwd"},
		{"", "stream"},
	}
	for _, tc := range cases {
		got := sanitizeDVRName(tc.name)
		if !strings.HasPrefix(got, tc.want+"-") {
			t.Errorf("sanitizeDVRName(%q) = %q, want prefix %q", tc.name, got, tc.want+"-")
		}
		if strings.ContainsAny(got, `/\`) {
			t.Errorf("sanitizeDVRName(%q) = %q contains a separator", tc.name, got)
		}
	}
}

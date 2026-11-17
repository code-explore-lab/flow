package access

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cidrs.txt")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp whitelist: %v", err)
	}
	return path
}

func TestWhitelistParseAndAllow(t *testing.T) {
	path := writeTempFile(t, `
# loopback and private
127.0.0.0/8
10.0.0.0/8

::1/128
`)
	w, err := ParseWhitelistFile(path)
	if err != nil {
		t.Fatalf("ParseWhitelistFile: %v", err)
	}
	if !w.Enabled() {
		t.Fatal("whitelist should be enabled")
	}

	allow := []string{"127.0.0.1", "10.1.2.3", "::1", "::ffff:10.0.0.1"}
	for _, ip := range allow {
		if !w.Allows(ip) {
			t.Errorf("Allows(%q) = false, want true", ip)
		}
	}
	deny := []string{"8.8.8.8", "192.168.1.1", "2001:db8::1", "not-an-ip"}
	for _, ip := range deny {
		if w.Allows(ip) {
			t.Errorf("Allows(%q) = true, want false", ip)
		}
	}
}

func TestWhitelistEmptyDisables(t *testing.T) {
	w, err := ParseWhitelistFile("")
	if err != nil {
		t.Fatalf("ParseWhitelistFile(\"\"): %v", err)
	}
	if w.Enabled() {
		t.Fatal("empty path should disable whitelist")
	}
	if !w.Allows("8.8.8.8") {
		t.Fatal("disabled whitelist must allow everything")
	}

	// 只有注释/空行的文件同样视为未启用。
	w, err = ParseWhitelistFile(writeTempFile(t, "# nothing here\n\n"))
	if err != nil {
		t.Fatalf("ParseWhitelistFile(comments): %v", err)
	}
	if w.Enabled() {
		t.Fatal("comment-only file should disable whitelist")
	}
}

func TestWhitelistBadLine(t *testing.T) {
	_, err := ParseWhitelistFile(writeTempFile(t, "127.0.0.0/8\nnot-a-cidr\n"))
	if err == nil {
		t.Fatal("expected error for malformed CIDR")
	}
}

func TestWhitelistNil(t *testing.T) {
	var w *Whitelist
	if !w.Allows("1.2.3.4") {
		t.Fatal("nil whitelist must allow everything")
	}
	if w.Enabled() {
		t.Fatal("nil whitelist must not be enabled")
	}
}

func TestRateLimiterBurstAndRefill(t *testing.T) {
	// 每秒 100 个令牌、桶容量 2：前两次放行，第三次拒绝；等 20ms 补满后可再放行。
	r := NewRateLimiter(100, 2)
	if !r.Allow("1.2.3.4") {
		t.Fatal("first burst token should be allowed")
	}
	if !r.Allow("1.2.3.4") {
		t.Fatal("second burst token should be allowed")
	}
	if r.Allow("1.2.3.4") {
		t.Fatal("bucket should be empty after burst")
	}
	time.Sleep(25 * time.Millisecond)
	if !r.Allow("1.2.3.4") {
		t.Fatal("tokens should refill over time")
	}
	// 不同 IP 各有独立桶。
	if !r.Allow("5.6.7.8") {
		t.Fatal("other IP should have its own bucket")
	}
}

func TestRateLimiterDisabled(t *testing.T) {
	for _, r := range []*RateLimiter{
		nil,
		NewRateLimiter(0, 0),
		NewRateLimiter(-1, 5),
		NewRateLimiter(5, 0),
	} {
		for range 100 {
			if !r.Allow("1.2.3.4") {
				t.Fatalf("disabled limiter %v should allow everything", r)
			}
		}
	}
}

func TestBackoffThresholdAndGrowth(t *testing.T) {
	b := NewBackoff(2, 10*time.Millisecond, 40*time.Millisecond)
	ip := "1.2.3.4"
	want := []time.Duration{
		0, 0, // 前两次手滑不罚
		10 * time.Millisecond, // 第 3 次
		20 * time.Millisecond, // 第 4 次
		40 * time.Millisecond, // 第 5 次
		40 * time.Millisecond, // 封顶
	}
	for i, w := range want {
		if got := b.Fail(ip); got != w {
			t.Fatalf("Fail #%d = %v, want %v", i+1, got, w)
		}
	}
	if got := b.Delay(ip); got != 40*time.Millisecond {
		t.Fatalf("Delay = %v, want 40ms", got)
	}

	b.Success(ip)
	if got := b.Delay(ip); got != 0 {
		t.Fatalf("Delay after Success = %v, want 0", got)
	}
	if got := b.Fail(ip); got != 0 {
		t.Fatalf("Fail after Success = %v, want 0 (counter reset)", got)
	}
}

func TestBackoffDisabledAndNil(t *testing.T) {
	for _, b := range []*Backoff{
		nil,
		NewBackoff(0, time.Second, 5*time.Second),
		NewBackoff(5, 0, 5*time.Second),
	} {
		for range 100 {
			if got := b.Fail("1.2.3.4"); got != 0 {
				t.Fatalf("disabled backoff %v returned %v", b, got)
			}
		}
		b.Success("1.2.3.4") // 不应 panic
	}
}

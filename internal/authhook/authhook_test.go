package authhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// hookServer 起一个记录调用次数的 hook 服务。
func hookServer(t *testing.T, status int, body string) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(ts.Close)
	return ts, &calls
}

func mustClient(t *testing.T, cfg Config) *Client {
	t.Helper()
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestAllowAndDeny(t *testing.T) {
	allowSrv, _ := hookServer(t, http.StatusOK, `{"allow":true}`)
	denySrv, _ := hookServer(t, http.StatusForbidden, "")

	c := mustClient(t, Config{URL: allowSrv.URL})
	in := Request{Action: "play", Protocol: "rtsp", Stream: "live/cam", IP: "1.2.3.4", Token: "t"}
	if got, err := c.Authorize(context.Background(), in); !got || err != nil {
		t.Fatalf("allow: got %v, %v", got, err)
	}

	c = mustClient(t, Config{URL: denySrv.URL})
	if got, err := c.Authorize(context.Background(), in); got || err != nil {
		t.Fatalf("deny: got %v, %v", got, err)
	}
}

func TestBodyAndSignature(t *testing.T) {
	var (
		mu   sync.Mutex
		got  Request
		sig  string
		body []byte
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var req Request
		_ = json.Unmarshal(b, &req)
		mu.Lock()
		got, sig, body = req, r.Header.Get("X-Flow-Signature"), b
		mu.Unlock()
		_, _ = io.WriteString(w, `{"allow":true}`)
	}))
	defer ts.Close()

	c := mustClient(t, Config{URL: ts.URL, Secret: "s3cret"})
	in := Request{
		Action: "publish", Protocol: "rtmp", App: "live", Stream: "cam",
		IP: "10.0.0.1", User: "alice", Token: "tok", Method: "publish", URI: "rtmp://x/live/cam",
	}
	if _, err := c.Authorize(context.Background(), in); err != nil {
		t.Fatalf("Authorize: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if got != in {
		t.Fatalf("hook body = %+v, want %+v", got, in)
	}
	mac := hmac.New(sha256.New, []byte("s3cret"))
	mac.Write(body)
	if want := hex.EncodeToString(mac.Sum(nil)); sig != want {
		t.Fatalf("signature = %q, want %q", sig, want)
	}
}

func TestFallbackOnServerError(t *testing.T) {
	const status = http.StatusInternalServerError
	for _, allow := range []bool{true, false} {
		ts, _ := hookServer(t, status, "")
		c := mustClient(t, Config{URL: ts.URL, DefaultAllow: allow})
		got, err := c.Authorize(context.Background(), Request{Action: "play", Stream: "x"})
		if err == nil {
			t.Fatal("expected an error for 500")
		}
		if got != allow {
			t.Fatalf("fallback = %v, want %v", got, allow)
		}
	}
}

func TestFallbackOnTimeout(t *testing.T) {
	var calls atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		time.Sleep(200 * time.Millisecond)
		_, _ = io.WriteString(w, `{"allow":true}`)
	}))
	defer ts.Close()

	c := mustClient(t, Config{URL: ts.URL, Timeout: 20 * time.Millisecond, DefaultAllow: false})
	start := time.Now()
	got, err := c.Authorize(context.Background(), Request{Action: "play", Stream: "x"})
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if got {
		t.Fatal("timeout should fall back to deny")
	}
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("timeout took %v, want bounded by configured timeout", elapsed)
	}
	// 失败结果不缓存：服务恢复后应该重新调用。
	_, _ = c.Authorize(context.Background(), Request{Action: "play", Stream: "x"})
	if calls.Load() != 2 {
		t.Fatalf("hook calls = %d, want 2 (failures must not be cached)", calls.Load())
	}
}

func TestCacheHitAndKeyScope(t *testing.T) {
	ts, calls := hookServer(t, http.StatusOK, `{"allow":true}`)
	c := mustClient(t, Config{URL: ts.URL, CacheTTL: time.Minute})

	base := Request{Action: "play", Protocol: "rtsp", Stream: "live/cam", IP: "1.2.3.4", Token: "t"}
	for range 3 {
		if _, err := c.Authorize(context.Background(), base); err != nil {
			t.Fatal(err)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("hook calls = %d, want 1 (cache hit)", n)
	}

	// token 不同必须单独判定，避免一个合法 token 放行其它 token。
	other := base
	other.Token = "other"
	if _, err := c.Authorize(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("hook calls = %d, want 2 after a different token", n)
	}
}

func TestCacheExpiry(t *testing.T) {
	ts, calls := hookServer(t, http.StatusOK, `{"allow":true}`)
	c := mustClient(t, Config{URL: ts.URL, CacheTTL: 30 * time.Millisecond})

	r := Request{Action: "play", Stream: "x"}
	if _, err := c.Authorize(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Authorize(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("calls = %d, want 1 before expiry", n)
	}
	time.Sleep(60 * time.Millisecond)
	if _, err := c.Authorize(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("calls = %d, want 2 after expiry", n)
	}
}

func TestCacheDisabled(t *testing.T) {
	ts, calls := hookServer(t, http.StatusOK, `{"allow":true}`)
	c := mustClient(t, Config{URL: ts.URL, CacheTTL: 0})
	r := Request{Action: "play", Stream: "x"}
	for range 2 {
		if _, err := c.Authorize(context.Background(), r); err != nil {
			t.Fatal(err)
		}
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("calls = %d, want 2 with cache disabled", n)
	}
}

func TestConcurrentDedup(t *testing.T) {
	var calls atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		_, _ = io.WriteString(w, `{"allow":true}`)
	}))
	defer ts.Close()

	c := mustClient(t, Config{URL: ts.URL, CacheTTL: time.Minute})
	r := Request{Action: "play", Stream: "x"}

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if _, err := c.Authorize(context.Background(), r); err != nil {
				t.Errorf("Authorize: %v", err)
			}
		})
	}
	wg.Wait()
	if n := calls.Load(); n != 1 {
		t.Fatalf("concurrent calls = %d, want 1", n)
	}
}

// 同 key 并发时，等待方也必须拿到默认策略，而不是 fetch 的零值。
func TestConcurrentFallbackUsesDefault(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	c := mustClient(t, Config{URL: ts.URL, DefaultAllow: true})
	r := Request{Action: "play", Stream: "x"}

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			got, err := c.Authorize(context.Background(), r)
			if err == nil {
				t.Error("expected fallback error")
			}
			if !got {
				t.Error("concurrent fallback did not use DefaultAllow")
			}
		})
	}
	wg.Wait()
}

func TestResponseForms(t *testing.T) {
	cases := []struct {
		desc string
		body string
		want bool
	}{
		{"empty body", "", true},
		{"explicit allow", `{"allow":true}`, true},
		{"explicit deny", `{"allow":false}`, false},
		{"allow omitted", `{}`, true},
		{"not json", `not-json`, true},
	}
	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			ts, _ := hookServer(t, http.StatusOK, c.body)
			client := mustClient(t, Config{URL: ts.URL})
			got, err := client.Authorize(context.Background(), Request{Action: "play", Stream: "x"})
			if err != nil {
				t.Fatalf("Authorize: %v", err)
			}
			if got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}

func TestResponseTTLOverride(t *testing.T) {
	ts, calls := hookServer(t, http.StatusOK, `{"allow":true,"ttl":"1h"}`)
	c := mustClient(t, Config{URL: ts.URL, CacheTTL: 0})
	r := Request{Action: "play", Stream: "x"}
	for range 2 {
		if _, err := c.Authorize(context.Background(), r); err != nil {
			t.Fatal(err)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("calls = %d, want 1 (response ttl should enable caching)", n)
	}
}

func TestNilAndDisabledClient(t *testing.T) {
	var nilClient *Client
	if nilClient.Enabled() {
		t.Fatal("nil client should be disabled")
	}
	if ok, err := nilClient.Authorize(context.Background(), Request{}); !ok || err != nil {
		t.Fatalf("nil client = %v, %v; want allow", ok, err)
	}

	disabled := mustClient(t, Config{})
	if disabled.Enabled() {
		t.Fatal("empty URL should be disabled")
	}
	if ok, err := disabled.Authorize(context.Background(), Request{}); !ok || err != nil {
		t.Fatalf("disabled client = %v, %v; want allow", ok, err)
	}
}

func TestNewValidation(t *testing.T) {
	for _, u := range []string{"ftp://host/hook", "http://", "://bad"} {
		if _, err := New(Config{URL: u}); err == nil {
			t.Fatalf("New(%q) should fail", u)
		}
	}
}

package dash

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"flow/internal/hls"
)

type fakeProvider struct{ p *hls.Packager }

func (f fakeProvider) PackagerFor(context.Context, string) *hls.Packager { return f.p }

func newTestServer(f *fixture) *Server {
	s := New(fakeProvider{p: f.packager})
	s.SetManifestOptions(Options{MinimumUpdatePeriod: time.Second, SegmentTarget: time.Second})
	return s
}

func do(s *Server, method, path string, mod func(*http.Request)) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	req.RemoteAddr = "127.0.0.1:12345"
	if mod != nil {
		mod(req)
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func TestServerManifestAndBinary(t *testing.T) {
	f := newFixture(t)
	feedBaseline(t, f)
	s := newTestServer(f)

	rec := do(s, http.MethodGet, "/dash/live/demo/manifest.mpd", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("manifest status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != manifestContentType {
		t.Errorf("content-type = %q", got)
	}
	m := parseManifest(t, rec.Body.Bytes())
	if v := findAdaptation(m, "video"); v == nil {
		t.Fatal("no video AdaptationSet")
	}

	tr := f.packager.Video()
	snap := tr.Snapshot()
	seg := snap.Segments[0]
	rec = do(s, http.MethodGet, "/dash/live/demo/"+seg.URI, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("segment status = %d", rec.Code)
	}
	want, _ := tr.Data(seg.URI)
	if rec.Body.String() != string(want) {
		t.Errorf("segment bytes mismatch: got %d want %d", rec.Body.Len(), len(want))
	}

	rec = do(s, http.MethodGet, "/dash/live/demo/"+seg.MapURI, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("init status = %d", rec.Code)
	}
	wantInit, _ := tr.Data(seg.MapURI)
	if rec.Body.String() != string(wantInit) {
		t.Errorf("init bytes mismatch")
	}

	rec = do(s, http.MethodHead, "/dash/live/demo/manifest.mpd", nil)
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		t.Errorf("HEAD manifest = %d, %d bytes", rec.Code, rec.Body.Len())
	}

	rec = do(s, http.MethodGet, "/dash/live/demo/utc", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("utc status = %d", rec.Code)
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000Z", rec.Body.String()); err != nil {
		t.Errorf("utc body %q: %v", rec.Body.String(), err)
	}
}

func TestServerRouting(t *testing.T) {
	f := newFixture(t)
	s := newTestServer(f)
	for _, p := range []string{
		"/hls/live/demo/manifest.mpd",
		"/dash/live/demo/manifest.mpd/extra",
		"/dash/live/demo/video/part-0-0.m4s",
		"/dash/live/demo/video/seg-x.m4s",
		"/dash/live/demo/video/../seg-0.m4s",
		"/dash//manifest.mpd",
	} {
		if rec := do(s, http.MethodGet, p, nil); rec.Code != http.StatusNotFound {
			t.Errorf("%s status = %d, want 404", p, rec.Code)
		}
	}
}

func TestServerUnknownStream(t *testing.T) {
	s := New(fakeProvider{})
	if rec := do(s, http.MethodGet, "/dash/other/manifest.mpd", nil); rec.Code != http.StatusNotFound {
		t.Errorf("unknown manifest status = %d", rec.Code)
	}
	if rec := do(s, http.MethodGet, "/dash/other/video/seg-0.m4s", nil); rec.Code != http.StatusNotFound {
		t.Errorf("unknown segment status = %d", rec.Code)
	}
}

func TestServerMethodAndPreflight(t *testing.T) {
	f := newFixture(t)
	s := newTestServer(f)
	if rec := do(s, http.MethodPost, "/dash/live/demo/manifest.mpd", nil); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST = %d", rec.Code)
	}
	rec := do(s, http.MethodOptions, "/dash/live/demo/manifest.mpd", nil)
	if rec.Code != http.StatusNoContent {
		t.Errorf("OPTIONS = %d", rec.Code)
	}
}

func TestServerToken(t *testing.T) {
	f := newFixture(t)
	feedBaseline(t, f)
	s := newTestServer(f)
	s.SetToken("abc")

	if rec := do(s, http.MethodGet, "/dash/live/demo/manifest.mpd", nil); rec.Code != http.StatusForbidden {
		t.Fatalf("no token status = %d", rec.Code)
	}
	rec := do(s, http.MethodGet, "/dash/live/demo/manifest.mpd?token=abc", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("token status = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "?token=abc") {
		t.Errorf("manifest does not embed token:\n%s", rec.Body.String())
	}
	rec = do(s, http.MethodGet, "/dash/live/demo/manifest.mpd", func(r *http.Request) {
		r.Header.Set("X-Flow-Token", "abc")
	})
	if rec.Code != http.StatusOK {
		t.Errorf("header token status = %d", rec.Code)
	}
}

func TestServerReferer(t *testing.T) {
	f := newFixture(t)
	feedBaseline(t, f)
	s := newTestServer(f)
	s.SetReferer("example.com,*.cdn.example.com", true)

	if rec := do(s, http.MethodGet, "/dash/live/demo/manifest.mpd", nil); rec.Code != http.StatusForbidden {
		t.Errorf("missing referer = %d", rec.Code)
	}
	if rec := do(s, http.MethodGet, "/dash/live/demo/manifest.mpd", func(r *http.Request) {
		r.Header.Set("Referer", "https://evil.com/example.com")
	}); rec.Code != http.StatusForbidden {
		t.Errorf("evil referer = %d", rec.Code)
	}
	if rec := do(s, http.MethodGet, "/dash/live/demo/manifest.mpd", func(r *http.Request) {
		r.Header.Set("Referer", "https://a.cdn.example.com/watch")
	}); rec.Code != http.StatusOK {
		t.Errorf("allowed referer = %d", rec.Code)
	}
}

func TestServerCORS(t *testing.T) {
	f := newFixture(t)
	s := newTestServer(f)
	s.SetCORS("*")

	rec := do(s, http.MethodOptions, "/dash/live/demo/manifest.mpd", func(r *http.Request) {
		r.Header.Set("Origin", "https://player.example")
		r.Header.Set("Access-Control-Request-Headers", "Range")
	})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("preflight = %d", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("allow-origin = %q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Headers"); got != "Range" {
		t.Errorf("allow-headers = %q", got)
	}
}

// 首个 MPD 请求应阻塞等待首个完整段，而不是立刻返回空 timeline。
func TestServerWaitsForFirstSegment(t *testing.T) {
	f := newFixture(t)
	s := newTestServer(f)

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- do(s, http.MethodGet, "/dash/live/demo/manifest.mpd", nil)
	}()

	select {
	case rec := <-done:
		t.Fatalf("manifest returned before any segment: %d", rec.Code)
	case <-time.After(100 * time.Millisecond):
	}

	go func() {
		writeVideo(f.video, 0, 60)
		writeAudio(f.audio, 0, 60)
	}()

	select {
	case rec := <-done:
		if rec.Code != http.StatusOK {
			t.Fatalf("manifest status = %d, body=%s", rec.Code, rec.Body.String())
		}
	case <-time.After(8 * time.Second):
		t.Fatal("manifest did not return after segments appeared")
	}
}

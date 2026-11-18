package hls

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"flow/internal/access"
	"flow/internal/metrics"
	"flow/internal/source"
	"flow/internal/stream"
)

func newTestHLSServer(t *testing.T, opts Options, idle time.Duration) (*Server, *httptest.Server, *stream.Stream, *source.ExternalSource, *source.ExternalSource) {
	t.Helper()
	if opts.Logf == nil {
		opts.Logf = t.Logf
	}
	_, st, vs, as := newStreamSpec(t, "live/test")
	reg := stream.NewRegistry()
	if _, err := reg.Publish("live/test", st); err != nil {
		t.Fatalf("publish: %v", err)
	}
	srv := New(reg, nil, nil)
	srv.SetPackaging(opts, idle)
	ts := httptest.NewServer(srv)
	t.Cleanup(func() {
		ts.Close()
		srv.Close()
		vs.Close()
		as.Close()
	})
	return srv, ts, st, vs, as
}

func testHLSBase(ts *httptest.Server) string {
	return ts.URL + "/hls/live/test"
}

type httpResult struct {
	resp *http.Response
	body []byte
	err  error
}

func asyncGet(url string, timeout time.Duration) <-chan httpResult {
	ch := make(chan httpResult, 1)
	go func() {
		client := &http.Client{Timeout: timeout}
		resp, err := client.Get(url)
		if err != nil {
			ch <- httpResult{err: err}
			return
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		ch <- httpResult{resp: resp, body: body, err: err}
	}()
	return ch
}

func doReq(t *testing.T, method, url string, headers map[string]string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatalf("new request %s: %v", url, err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	return resp, body
}

func getPlaylist(t *testing.T, url string) *parsedMedia {
	t.Helper()
	resp, body := doReq(t, http.MethodGet, url, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d, body %q", url, resp.StatusCode, body)
	}
	return parseMediaPlaylist(t, body)
}

func TestParseHLSPath(t *testing.T) {
	valid := []struct {
		path string
		kind routeKind
		name string
	}{
		{"/hls/live/test/master.m3u8", routeMaster, "live/test"},
		{"/hls/live/test/index.m3u8", routeMaster, "live/test"},
		{"/hls/a/b/c/video.m3u8", routeMediaVideo, "a/b/c"},
		{"/hls/live/test/audio.m3u8", routeMediaAudio, "live/test"},
		{"/hls/live/test/stats.json", routeStats, "live/test"},
		{"/hls/live/test/video/init.mp4", routeBinary, "live/test"},
		{"/hls/live/test/video/init-3.mp4", routeBinary, "live/test"},
		{"/hls/live/test/video/seg-12.m4s", routeBinary, "live/test"},
		{"/hls/live/test/audio/part-3-2.m4s", routeBinary, "live/test"},
	}
	for _, tc := range valid {
		route, ok := parseHLSPath(tc.path)
		if !ok {
			t.Errorf("%s rejected", tc.path)
			continue
		}
		if route.kind != tc.kind || route.name != tc.name {
			t.Errorf("%s = %+v", tc.path, route)
		}
	}

	invalid := []string{
		"/live/test/master.m3u8",
		"/hls//master.m3u8",
		"/hls/live/test/../master.m3u8",
		"/hls/live/test/video/../../x",
		"/hls/live/test/video/init-0.mp4",
		"/hls/live/test/video/part-1.m4s",
		"/hls/live/test/video/part-1-x.m4s",
		"/hls/live/test/video/foo",
		"/hls/live/test/master.m3u8/",
		"/hls/master.m3u8",
	}
	for _, p := range invalid {
		if _, ok := parseHLSPath(p); ok {
			t.Errorf("%s accepted", p)
		}
	}
}

func TestServerMasterMediaAndBinary(t *testing.T) {
	srv, ts, st, vs, as := newTestHLSServer(t, Options{
		SegmentTarget: time.Second, PartTarget: 500 * time.Millisecond, Window: 6,
	}, 0)
	base := testHLSBase(ts)

	resp, body := doReq(t, http.MethodGet, base+"/master.m3u8", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("master = %d, body %q", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); ct != playlistContentType {
		t.Errorf("master Content-Type = %q", ct)
	}
	master := parseMasterPlaylist(t, body)
	if master.version != 9 || !master.independent {
		t.Errorf("master version/independent = %d/%v", master.version, master.independent)
	}
	if master.media["GROUP-ID"] != "aud" || master.media["URI"] != "audio.m3u8" {
		t.Errorf("audio rendition = %+v", master.media)
	}
	if len(master.variants) != 1 {
		t.Fatalf("variants = %d", len(master.variants))
	}
	v := master.variants[0]
	if v.uri != "video.m3u8" || v.attrs["AUDIO"] != "aud" ||
		v.attrs["RESOLUTION"] != "640x480" || !strings.Contains(v.attrs["CODECS"], "avc1.64001e") ||
		!strings.Contains(v.attrs["CODECS"], "mp4a.40.2") {
		t.Errorf("variant = %+v", v)
	}

	writeVideoFrames(vs, 0, 75, 25)
	writeAudioFrames(as, 0, 100)
	p := srv.getManager().Get(SpecFromStream(st))
	if p == nil {
		t.Fatal("packager = nil")
	}
	waitUntil(t, 3*time.Second, "video segments", func() bool {
		return len(p.Video().Snapshot().Segments) > 0 && len(p.Audio().Snapshot().Segments) > 0
	})

	video := getPlaylist(t, base+"/video.m3u8")
	if video.partTarget <= 0 || len(video.entries) == 0 {
		t.Fatalf("video playlist = %+v", video)
	}
	if video.server["CAN-BLOCK-RELOAD"] != "YES" {
		t.Errorf("server control = %+v", video.server)
	}
	if video.endList {
		t.Error("live video playlist marked ENDLIST")
	}
	if video.entries[0].pdt == "" {
		t.Error("video playlist missing PDT")
	}
	segResp, segBody := doReq(t, http.MethodGet, base+"/"+video.entries[0].uri, nil)
	if segResp.StatusCode != http.StatusOK {
		t.Fatalf("segment = %d", segResp.StatusCode)
	}
	if ct := segResp.Header.Get("Content-Type"); ct != mp4ContentType {
		t.Errorf("segment Content-Type = %q", ct)
	}
	if !bytes.Contains(segBody, []byte("moof")) {
		t.Error("segment missing moof")
	}
	initResp, initBody := doReq(t, http.MethodGet, base+"/"+video.entries[0].mapURI, nil)
	if initResp.StatusCode != http.StatusOK || !bytes.Contains(initBody, []byte("ftyp")) {
		t.Errorf("init status=%d body=%d", initResp.StatusCode, len(initBody))
	}

	audio := getPlaylist(t, base+"/audio.m3u8")
	if len(audio.entries) == 0 || audio.partTarget <= 0 {
		t.Fatalf("audio playlist = %+v", audio)
	}
	if resp, body := doReq(t, http.MethodGet, base+"/"+audio.entries[0].uri, nil); resp.StatusCode != http.StatusOK {
		t.Errorf("audio segment = %d, body %q", resp.StatusCode, body[:min(len(body), 64)])
	}

	// Range / HEAD / stats
	req, _ := http.NewRequest(http.MethodGet, base+"/"+video.entries[0].uri, nil)
	req.Header.Set("Range", "bytes=0-3")
	rresp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	rbody, _ := io.ReadAll(rresp.Body)
	rresp.Body.Close()
	if rresp.StatusCode != http.StatusPartialContent || len(rbody) != 4 {
		t.Errorf("range status/body = %d/%d", rresp.StatusCode, len(rbody))
	}
	hresp, hbody := doReq(t, http.MethodHead, base+"/master.m3u8", nil)
	if hresp.StatusCode != http.StatusOK || len(hbody) != 0 {
		t.Errorf("HEAD master = %d/%d bytes", hresp.StatusCode, len(hbody))
	}
	sresp, sbody := doReq(t, http.MethodGet, base+"/stats.json", nil)
	if sresp.StatusCode != http.StatusOK {
		t.Fatalf("stats = %d", sresp.StatusCode)
	}
	var stats hlsStats
	if err := json.Unmarshal(sbody, &stats); err != nil {
		t.Fatalf("stats json: %v", err)
	}
	if stats.Tracks["video"].Kind != "video" || stats.Tracks["audio"].Kind != "audio" {
		t.Errorf("stats tracks = %+v", stats.Tracks)
	}
}

func TestServerBlockingReloadAndPreloadHint(t *testing.T) {
	srv, ts, st, vs, as := newTestHLSServer(t, Options{
		SegmentTarget: time.Second, PartTarget: 500 * time.Millisecond, Window: 6,
	}, 0)
	base := testHLSBase(ts)
	doReq(t, http.MethodGet, base+"/master.m3u8", nil)

	writeVideoFrames(vs, 0, 30, 25)
	p := srv.getManager().Get(SpecFromStream(st))
	if p == nil {
		t.Fatal("packager = nil")
	}
	waitUntil(t, 2*time.Second, "open segment 1", func() bool {
		snap := p.Video().Snapshot()
		return snap.Open != nil && snap.Open.MSN == 1 && len(snap.Open.Parts) == 0
	})

	pl := getPlaylist(t, base+"/video.m3u8")
	if pl.preload == "" {
		t.Fatalf("no preload hint: %+v", pl)
	}
	if len(pl.parts) != 0 {
		t.Fatalf("expected zero completed parts, got %d", len(pl.parts))
	}
	partData, hasPart := p.Video().Data(pl.preload)
	if hasPart {
		t.Fatalf("hint %s already available (%d bytes)", pl.preload, len(partData))
	}

	hintURL := base + "/" + pl.preload
	ch := asyncGet(hintURL, 5*time.Second)
	select {
	case r := <-ch:
		t.Fatalf("preload hint returned before part was generated: %v", r.err)
	case <-time.After(150 * time.Millisecond):
	}
	writeVideoFrames(vs, 30, 15, 25)
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("preload hint: %v", r.err)
		}
		if r.resp.StatusCode != http.StatusOK {
			t.Fatalf("preload hint = %d, body %q", r.resp.StatusCode, r.body)
		}
		if !bytes.Contains(r.body, []byte("moof")) {
			t.Error("hint part missing moof")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("preload hint did not unblock")
	}

	// 现在 part-1-0 已生成，请求 part-1-1 走阻塞重载。
	pl = getPlaylist(t, base+"/video.m3u8")
	if len(pl.parts) != 1 {
		t.Fatalf("parts = %d, want 1", len(pl.parts))
	}
	reloadURL := base + "/video.m3u8?_HLS_msn=1&_HLS_part=1"
	rch := asyncGet(reloadURL, 5*time.Second)
	select {
	case r := <-rch:
		t.Fatalf("blocking reload returned early: %v", r.err)
	case <-time.After(150 * time.Millisecond):
	}
	writeVideoFrames(vs, 45, 20, 25)
	select {
	case r := <-rch:
		if r.err != nil {
			t.Fatalf("blocking reload: %v", r.err)
		}
		if r.resp.StatusCode != http.StatusOK {
			t.Fatalf("blocking reload = %d, body %q", r.resp.StatusCode, r.body)
		}
		got := parseMediaPlaylist(t, r.body)
		if _, ok := p.Video().Data("video/part-1-1.m4s"); !ok {
			t.Error("part-1-1 missing after blocking reload")
		}
		if len(got.parts) < 2 && !bytes.Contains(r.body, []byte("video/seg-1.m4s")) {
			t.Errorf("reload body has neither parts nor seg-1: %q", r.body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocking reload did not unblock")
	}
	_ = as
}

func TestServerBlockingReloadTimeout(t *testing.T) {
	srv, ts, st, vs, _ := newTestHLSServer(t, Options{
		SegmentTarget: time.Second, PartTarget: 500 * time.Millisecond, Window: 6,
	}, 0)
	base := testHLSBase(ts)
	doReq(t, http.MethodGet, base+"/master.m3u8", nil)

	writeVideoFrames(vs, 0, 30, 25)
	p := srv.getManager().Get(SpecFromStream(st))
	if p == nil {
		t.Fatal("packager = nil")
	}
	waitUntil(t, 2*time.Second, "open segment", func() bool {
		snap := p.Video().Snapshot()
		return snap.Open != nil && snap.Open.MSN == 1
	})

	start := time.Now()
	resp, body := doReq(t, http.MethodGet, base+"/video.m3u8?_HLS_msn=1", nil)
	elapsed := time.Since(start)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("timeout reload = %d, body %q", resp.StatusCode, body)
	}
	if elapsed < 700*time.Millisecond {
		t.Errorf("reload returned too early: %s", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Errorf("reload took too long: %s", elapsed)
	}
	if !strings.Contains(string(body), "#EXTM3U") {
		t.Errorf("body not a playlist: %q", body)
	}
}

func TestServerInvalidBlockingParams(t *testing.T) {
	_, ts, _, _, _ := newTestHLSServer(t, Options{
		SegmentTarget: time.Second, PartTarget: 500 * time.Millisecond, Window: 6,
	}, 0)
	base := testHLSBase(ts)
	for _, q := range []string{
		"?_HLS_part=0",
		"?_HLS_msn=abc",
		"?_HLS_msn=1&_HLS_part=-1",
		"?_HLS_msn=1&_HLS_part=abc",
	} {
		resp, _ := doReq(t, http.MethodGet, base+"/video.m3u8"+q, nil)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s = %d, want 400", q, resp.StatusCode)
		}
	}
}

func TestServerEndListAndPartGone(t *testing.T) {
	srv, ts, st, vs, as := newTestHLSServer(t, Options{
		SegmentTarget: time.Second, PartTarget: 500 * time.Millisecond, Window: 6,
	}, 0)
	base := testHLSBase(ts)
	doReq(t, http.MethodGet, base+"/master.m3u8", nil)
	writeVideoFrames(vs, 0, 40, 25)
	writeAudioFrames(as, 0, 60)
	p := srv.getManager().Get(SpecFromStream(st))
	if p == nil {
		t.Fatal("packager = nil")
	}
	waitUntil(t, 2*time.Second, "open segment", func() bool {
		return p.Video().Snapshot().Open != nil
	})
	hint := p.Video().Snapshot().Open.NextPartURI
	vs.Close()
	as.Close()
	waitUntil(t, 3*time.Second, "packager closed", p.Closed)

	pl := getPlaylist(t, base+"/video.m3u8")
	if !pl.endList {
		t.Error("video playlist missing ENDLIST")
	}
	if pl.preload != "" {
		t.Errorf("ENDLIST playlist has preload %q", pl.preload)
	}
	if hint == "" {
		t.Error("no preload hint before end")
	}
	resp, _ := doReq(t, http.MethodGet, base+"/video/part-99-99.m4s", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("part after end = %d, want 404", resp.StatusCode)
	}
}

func TestServerBlockingReloadWindowSlide(t *testing.T) {
	srv, ts, st, vs, _ := newTestHLSServer(t, Options{
		SegmentTarget: time.Second, PartTarget: 500 * time.Millisecond, Window: 6,
	}, 0)
	base := testHLSBase(ts)
	doReq(t, http.MethodGet, base+"/master.m3u8", nil)
	p := srv.getManager().Get(SpecFromStream(st))
	if p == nil {
		t.Fatal("packager = nil")
	}
	writeVideoFrames(vs, 0, 225, 25) // 9s，窗口只保留最近 6 段
	waitUntil(t, 3*time.Second, "window slide", func() bool {
		snap := p.Video().Snapshot()
		return len(snap.Segments) >= 6 && snap.Segments[0].MSN > 0
	})

	start := time.Now()
	resp, body := doReq(t, http.MethodGet, base+"/video.m3u8?_HLS_msn=0", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("slid request = %d, body %q", resp.StatusCode, body)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("window-slid request blocked for %s", elapsed)
	}
	pl := parseMediaPlaylist(t, body)
	if pl.mediaSeq == 0 {
		t.Errorf("media sequence = 0, want slid window")
	}
}

func TestServerSecurity(t *testing.T) {
	t.Run("token", func(t *testing.T) {
		srv, ts, _, _, _ := newTestHLSServer(t, Options{
			SegmentTarget: time.Second, PartTarget: 500 * time.Millisecond, Window: 6,
		}, 0)
		srv.SetToken("secret")
		base := testHLSBase(ts)

		if resp, _ := doReq(t, http.MethodGet, base+"/master.m3u8", nil); resp.StatusCode != http.StatusForbidden {
			t.Errorf("no token = %d", resp.StatusCode)
		}
		resp, body := doReq(t, http.MethodGet, base+"/master.m3u8?token=secret", nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("token = %d, body %q", resp.StatusCode, body)
		}
		master := parseMasterPlaylist(t, body)
		if !strings.Contains(master.variants[0].uri, "token=secret") {
			t.Errorf("variant URI missing token: %q", master.variants[0].uri)
		}
		if !strings.Contains(master.media["URI"], "token=secret") {
			t.Errorf("audio URI missing token: %q", master.media["URI"])
		}
		if resp, _ := doReq(t, http.MethodGet, base+"/master.m3u8", map[string]string{"X-Flow-Token": "secret"}); resp.StatusCode != http.StatusOK {
			t.Errorf("header token = %d", resp.StatusCode)
		}
		// CORS 预检不应要求 token。
		if resp, _ := doReq(t, http.MethodOptions, base+"/master.m3u8", map[string]string{"Origin": "https://example.com"}); resp.StatusCode != http.StatusNoContent {
			t.Errorf("preflight = %d", resp.StatusCode)
		}
	})

	t.Run("cors", func(t *testing.T) {
		srv, ts, _, _, _ := newTestHLSServer(t, Options{
			SegmentTarget: time.Second, PartTarget: 500 * time.Millisecond, Window: 6,
		}, 0)
		srv.SetCORS("https://example.com")
		base := testHLSBase(ts)
		resp, _ := doReq(t, http.MethodOptions, base+"/master.m3u8", map[string]string{
			"Origin":                        "https://example.com",
			"Access-Control-Request-Method": "GET",
		})
		if resp.StatusCode != http.StatusNoContent {
			t.Errorf("preflight = %d", resp.StatusCode)
		}
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "https://example.com" {
			t.Errorf("allow origin = %q", got)
		}
		resp, _ = doReq(t, http.MethodGet, base+"/master.m3u8", map[string]string{"Origin": "https://example.com"})
		if resp.Header.Get("Access-Control-Allow-Origin") != "https://example.com" {
			t.Errorf("GET allow origin = %q", resp.Header.Get("Access-Control-Allow-Origin"))
		}
		resp, _ = doReq(t, http.MethodGet, base+"/master.m3u8", map[string]string{"Origin": "https://evil.com"})
		if resp.Header.Get("Access-Control-Allow-Origin") != "" {
			t.Errorf("evil origin allowed: %q", resp.Header.Get("Access-Control-Allow-Origin"))
		}
	})

	t.Run("referer", func(t *testing.T) {
		srv, ts, _, _, _ := newTestHLSServer(t, Options{
			SegmentTarget: time.Second, PartTarget: 500 * time.Millisecond, Window: 6,
		}, 0)
		srv.SetReferer("example.com", true)
		base := testHLSBase(ts)
		if resp, _ := doReq(t, http.MethodGet, base+"/master.m3u8", nil); resp.StatusCode != http.StatusForbidden {
			t.Errorf("no referer = %d", resp.StatusCode)
		}
		if resp, _ := doReq(t, http.MethodGet, base+"/master.m3u8", map[string]string{"Referer": "https://evil.com/example.com"}); resp.StatusCode != http.StatusForbidden {
			t.Errorf("evil referer = %d", resp.StatusCode)
		}
		if resp, _ := doReq(t, http.MethodGet, base+"/master.m3u8", map[string]string{"Referer": "https://example.com/watch"}); resp.StatusCode != http.StatusOK {
			t.Errorf("good referer = %d", resp.StatusCode)
		}
	})

	t.Run("whitelist", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "allow.txt")
		if err := os.WriteFile(path, []byte("10.0.0.0/8\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		wl, err := access.ParseWhitelistFile(path)
		if err != nil {
			t.Fatal(err)
		}
		srv, ts, _, _, _ := newTestHLSServer(t, Options{
			SegmentTarget: time.Second, PartTarget: 500 * time.Millisecond, Window: 6,
		}, 0)
		srv.SetWhitelist(wl)
		if resp, _ := doReq(t, http.MethodGet, testHLSBase(ts)+"/master.m3u8", nil); resp.StatusCode != http.StatusForbidden {
			t.Errorf("whitelist deny = %d", resp.StatusCode)
		}
	})
}

func TestServerMethodsAndPaths(t *testing.T) {
	_, ts, _, _, _ := newTestHLSServer(t, Options{
		SegmentTarget: time.Second, PartTarget: 500 * time.Millisecond, Window: 6,
	}, 0)
	base := testHLSBase(ts)
	if resp, _ := doReq(t, http.MethodPost, base+"/master.m3u8", nil); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST = %d", resp.StatusCode)
	}
	for _, p := range []string{
		"/hls/live/test/unknown",
		"/hls/live/test/video/unknown.m4s",
		"/hls/missing/master.m3u8",
		"/hls/../master.m3u8",
	} {
		if resp, _ := doReq(t, http.MethodGet, ts.URL+p, nil); resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s = %d, want 404", p, resp.StatusCode)
		}
	}
}

func TestServerLocalFallback(t *testing.T) {
	dir := t.TempDir()
	sps := []byte{0x67, 0x42, 0xC0, 0x1E}
	pps := []byte{0x68, 0xCE, 0x3C, 0x80}
	idr := []byte{0x65, 0x88, 0x84, 0x00}
	nalus := [][]byte{sps, pps, idr}
	for i := 0; i < 6; i++ {
		nalus = append(nalus, []byte{0x41, 0x9A, byte(i)})
	}
	h264Path := filepath.Join(dir, "test.h264")
	if err := os.WriteFile(h264Path, annexB(nalus...), 0o644); err != nil {
		t.Fatal(err)
	}
	var ablob []byte
	for i := 0; i < 6; i++ {
		ablob = append(ablob, adtsFrame([]byte{0x21, 0x10, byte(i)})...)
	}
	aacPath := filepath.Join(dir, "test.aac")
	if err := os.WriteFile(aacPath, ablob, 0o644); err != nil {
		t.Fatal(err)
	}

	srv := New(stream.NewRegistry(), source.NewH264Source(h264Path), source.NewAACSource(aacPath))
	srv.SetPackaging(Options{
		SegmentTarget: time.Second, PartTarget: 500 * time.Millisecond, Window: 6, Logf: t.Logf,
	}, 0)
	ts := httptest.NewServer(srv)
	defer func() {
		ts.Close()
		srv.Close()
	}()
	base := ts.URL + "/hls/local/test"

	waitUntil(t, 4*time.Second, "local ENDLIST", func() bool {
		resp, body := doReq(t, http.MethodGet, base+"/video.m3u8", nil)
		return resp.StatusCode == http.StatusOK && strings.Contains(string(body), "#EXT-X-ENDLIST")
	})
	pl := getPlaylist(t, base+"/video.m3u8")
	if len(pl.entries) == 0 || !pl.endList {
		t.Errorf("local playlist = %+v", pl)
	}
}

type fakeOrigin struct {
	st     *stream.Stream
	called bool
}

func (f *fakeOrigin) Ensure(ctx context.Context, name string) *stream.Stream {
	f.called = true
	return f.st
}

func TestServerOriginResolver(t *testing.T) {
	_, st, _, _ := newStreamSpec(t, "live/origin")
	srv := New(stream.NewRegistry(), nil, nil)
	srv.SetPackaging(Options{
		SegmentTarget: time.Second, PartTarget: 500 * time.Millisecond, Window: 6, Logf: t.Logf,
	}, 0)
	fo := &fakeOrigin{st: st}
	srv.SetOrigin(fo)
	ts := httptest.NewServer(srv)
	defer func() {
		ts.Close()
		srv.Close()
	}()

	resp, body := doReq(t, http.MethodGet, ts.URL+"/hls/live/origin/master.m3u8", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("origin master = %d, body %q", resp.StatusCode, body)
	}
	if !fo.called {
		t.Error("origin resolver not called")
	}
	if len(parseMasterPlaylist(t, body).variants) == 0 {
		t.Error("origin master has no variants")
	}
}

func TestServerClassicPlaylist(t *testing.T) {
	srv, ts, st, vs, _ := newTestHLSServer(t, Options{
		SegmentTarget: time.Second, PartTarget: 0, Window: 6,
	}, 0)
	base := testHLSBase(ts)

	_, body := doReq(t, http.MethodGet, base+"/master.m3u8", nil)
	master := parseMasterPlaylist(t, body)
	if master.version != 7 {
		t.Errorf("classic master version = %d, want 7", master.version)
	}

	writeVideoFrames(vs, 0, 75, 25)
	p := srv.getManager().Get(SpecFromStream(st))
	if p == nil {
		t.Fatal("packager = nil")
	}
	waitUntil(t, 2*time.Second, "classic segments", func() bool {
		return len(p.Video().Snapshot().Segments) > 0
	})
	pl := getPlaylist(t, base+"/video.m3u8")
	if pl.partTarget != 0 || len(pl.parts) != 0 || pl.preload != "" {
		t.Errorf("classic playlist has LL tags: %+v", pl)
	}
	if len(pl.entries) == 0 {
		t.Fatal("classic playlist has no segments")
	}
}

func TestServerAudioOnlyAndVideoOnly(t *testing.T) {
	t.Run("audio-only", func(t *testing.T) {
		st := &stream.Stream{Name: "live/audio"}
		st.SetAudioCodec(stream.AudioAAC, []byte{0x12, 0x10}, 44100, 2, 44100)
		reg := stream.NewRegistry()
		if _, err := reg.Publish("live/audio", st); err != nil {
			t.Fatal(err)
		}
		as := st.SourceFor("audio")
		srv := New(reg, nil, nil)
		srv.SetPackaging(Options{SegmentTarget: time.Second, PartTarget: 500 * time.Millisecond, Window: 6, Logf: t.Logf}, 0)
		ts := httptest.NewServer(srv)
		t.Cleanup(func() {
			ts.Close()
			srv.Close()
			as.Close()
		})
		base := ts.URL + "/hls/live/audio"

		_, body := doReq(t, http.MethodGet, base+"/master.m3u8", nil)
		master := parseMasterPlaylist(t, body)
		if len(master.variants) != 1 || master.variants[0].uri != "audio.m3u8" {
			t.Fatalf("audio-only master = %+v", master)
		}
		if len(master.media) != 0 {
			t.Errorf("audio-only master has EXT-X-MEDIA: %+v", master.media)
		}
		writeAudioFrames(as, 0, 100)
		p := srv.getManager().Get(SpecFromStream(st))
		if p == nil {
			t.Fatal("audio packager = nil")
		}
		waitUntil(t, 2*time.Second, "audio segments", func() bool {
			return len(p.Audio().Snapshot().Segments) > 0
		})
		pl := getPlaylist(t, base+"/audio.m3u8")
		if len(pl.entries) == 0 {
			t.Fatal("no audio segments")
		}
	})

	t.Run("video-only", func(t *testing.T) {
		st := &stream.Stream{Name: "live/video"}
		sps, pps := realH264Params(t)
		st.SetVideoParams("H264", nil, sps, pps)
		reg := stream.NewRegistry()
		if _, err := reg.Publish("live/video", st); err != nil {
			t.Fatal(err)
		}
		vs := st.SourceFor("video")
		srv := New(reg, nil, nil)
		srv.SetPackaging(Options{SegmentTarget: time.Second, PartTarget: 500 * time.Millisecond, Window: 6, Logf: t.Logf}, 0)
		ts := httptest.NewServer(srv)
		t.Cleanup(func() {
			ts.Close()
			srv.Close()
			vs.Close()
		})
		base := ts.URL + "/hls/live/video"

		_, body := doReq(t, http.MethodGet, base+"/master.m3u8", nil)
		master := parseMasterPlaylist(t, body)
		if len(master.variants) != 1 || master.variants[0].uri != "video.m3u8" {
			t.Fatalf("video-only master = %+v", master)
		}
		if master.variants[0].attrs["AUDIO"] != "" {
			t.Errorf("video-only variant has audio group: %+v", master.variants[0].attrs)
		}
		writeVideoFrames(vs, 0, 75, 25)
		p := srv.getManager().Get(SpecFromStream(st))
		if p == nil {
			t.Fatal("video packager = nil")
		}
		waitUntil(t, 2*time.Second, "video segments", func() bool {
			return len(p.Video().Snapshot().Segments) > 0
		})
		pl := getPlaylist(t, base+"/video.m3u8")
		if len(pl.entries) == 0 {
			t.Fatal("no video segments")
		}
	})
}

func TestServerMetrics(t *testing.T) {
	srv, ts, st, vs, as := newTestHLSServer(t, Options{
		SegmentTarget: time.Second, PartTarget: 500 * time.Millisecond, Window: 6,
	}, 0)
	m := metrics.New()
	srv.SetMetrics(m)
	base := testHLSBase(ts)

	doReq(t, http.MethodGet, base+"/master.m3u8", nil)
	writeVideoFrames(vs, 0, 75, 25)
	writeAudioFrames(as, 0, 100)
	p := srv.getManager().Get(SpecFromStream(st))
	if p == nil {
		t.Fatal("packager = nil")
	}
	waitUntil(t, 3*time.Second, "segments", func() bool {
		return len(p.Video().Snapshot().Segments) > 0
	})
	pl := getPlaylist(t, base+"/video.m3u8")
	doReq(t, http.MethodGet, base+"/"+pl.entries[0].mapURI, nil)
	doReq(t, http.MethodGet, base+"/"+pl.entries[0].uri, nil)
	if len(pl.parts) > 0 {
		doReq(t, http.MethodGet, base+"/"+pl.parts[0].uri, nil)
	}
	doReq(t, http.MethodGet, base+"/stats.json", nil)

	var b strings.Builder
	m.WritePrometheus(&b)
	out := b.String()
	for _, want := range []string{
		"flow_hls_packagers 1",
		`flow_hls_requests_total{kind="master"} 1`,
		`flow_hls_requests_total{kind="media"} 1`,
		`flow_hls_requests_total{kind="init"} 1`,
		`flow_hls_requests_total{kind="segment"} 1`,
		`flow_hls_requests_total{kind="part"} 1`,
		`flow_hls_requests_total{kind="stats"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics missing %q\n%s", want, out)
		}
	}
	if !strings.Contains(out, `flow_hls_segments_total{track="video"} `) || strings.Contains(out, `flow_hls_segments_total{track="video"} 0`) {
		t.Errorf("video segment metric not incremented\n%s", out)
	}
	if !strings.Contains(out, `flow_hls_parts_total{track="video"} `) || strings.Contains(out, `flow_hls_parts_total{track="video"} 0`) {
		t.Errorf("video part metric not incremented\n%s", out)
	}
	if !strings.Contains(out, `flow_hls_bytes_served_total{kind="segment"} `) || strings.Contains(out, `flow_hls_bytes_served_total{kind="segment"} 0`) {
		t.Errorf("segment bytes metric not incremented\n%s", out)
	}
	if !strings.Contains(out, `flow_hls_bytes_served_total{kind="part"} `) || strings.Contains(out, `flow_hls_bytes_served_total{kind="part"} 0`) {
		t.Errorf("part bytes metric not incremented\n%s", out)
	}
}

func TestServerConcurrentReloads(t *testing.T) {
	srv, ts, st, vs, as := newTestHLSServer(t, Options{
		SegmentTarget: time.Second, PartTarget: 500 * time.Millisecond, Window: 6,
	}, 0)
	base := testHLSBase(ts)
	doReq(t, http.MethodGet, base+"/master.m3u8", nil)
	p := srv.getManager().Get(SpecFromStream(st))
	if p == nil {
		t.Fatal("packager = nil")
	}
	writeVideoFrames(vs, 0, 75, 25)
	writeAudioFrames(as, 0, 100)
	waitUntil(t, 3*time.Second, "segments", func() bool {
		return len(p.Video().Snapshot().Segments) > 0
	})

	var wg sync.WaitGroup
	errs := make(chan string, 64)
	wg.Add(1)
	go func() {
		defer wg.Done()
		writeVideoFrames(vs, 75, 75, 25) // 与下面的并发 reload 同时生成段
	}()
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				resp, err := http.Get(base + "/video.m3u8")
				if err != nil {
					errs <- err.Error()
					return
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("#EXTM3U")) {
					errs <- resp.Status
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}

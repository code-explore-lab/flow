package hls

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"flow/internal/source"
	"flow/internal/stream"
)

func TestABRMasterPlaylist(t *testing.T) {
	reg := stream.NewRegistry()
	srv := New(reg, nil, nil)
	srv.SetPackaging(Options{
		SegmentTarget: time.Second, PartTarget: 500 * time.Millisecond, Window: 6,
		Logf: t.Logf,
	}, 0)
	ts := httptest.NewServer(srv)
	t.Cleanup(func() {
		ts.Close()
		srv.Close()
	})

	var sources []*source.ExternalSource
	for _, name := range []string{"live/src_720p", "live/src_360p"} {
		_, st, vs, as := newStreamSpec(t, name)
		if _, err := reg.Publish(name, st); err != nil {
			t.Fatalf("publish %s: %v", name, err)
		}
		sources = append(sources, vs, as)
	}
	t.Cleanup(func() {
		for _, src := range sources {
			src.Close()
		}
	})

	// 故意乱序传入，master 应按码率升序输出。
	srv.SetABR([]ABRGroup{{
		Source: "live/src",
		Variants: []ABRVariant{
			{Stream: "live/src_720p", Width: 1280, Height: 720, Bitrate: 2_500_000, FrameRate: 25, Codec: "H264"},
			{Stream: "live/src_360p", Width: 640, Height: 360, Bitrate: 800_000, FrameRate: 25, Codec: "H264"},
		},
	}})

	resp, body := doReq(t, http.MethodGet, ts.URL+"/hls/live/src/master.m3u8", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("master = %d, body %q", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); ct != playlistContentType {
		t.Errorf("Content-Type = %q", ct)
	}
	master := parseMasterPlaylist(t, body)
	if len(master.variants) != 2 {
		t.Fatalf("variants = %d, want 2:\n%s", len(master.variants), body)
	}
	low, high := master.variants[0], master.variants[1]
	if low.uri != "/hls/live/src_360p/video.m3u8" || low.attrs["RESOLUTION"] != "640x360" ||
		low.attrs["AUDIO"] != "aud0" {
		t.Errorf("low variant = %+v", low)
	}
	if high.uri != "/hls/live/src_720p/video.m3u8" || high.attrs["RESOLUTION"] != "1280x720" ||
		high.attrs["AUDIO"] != "aud1" {
		t.Errorf("high variant = %+v", high)
	}
	if atoi(t, low.attrs["BANDWIDTH"]) >= atoi(t, high.attrs["BANDWIDTH"]) {
		t.Errorf("BANDWIDTH not ascending: %s >= %s", low.attrs["BANDWIDTH"], high.attrs["BANDWIDTH"])
	}
	if !strings.Contains(low.attrs["CODECS"], "avc1") || !strings.Contains(low.attrs["CODECS"], "mp4a") {
		t.Errorf("CODECS = %q", low.attrs["CODECS"])
	}
	s := string(body)
	for _, want := range []string{
		`GROUP-ID="aud0"`, `GROUP-ID="aud1"`,
		`URI="/hls/live/src_360p/audio.m3u8"`, `URI="/hls/live/src_720p/audio.m3u8"`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("master missing %q:\n%s", want, s)
		}
	}

	// 两个 variant 的 media playlist 都应可独立访问并出段。
	writeVideoFrames(sources[0], 0, 75, 25) // 720p
	writeAudioFrames(sources[1], 0, 100)
	writeVideoFrames(sources[2], 0, 75, 25) // 360p
	writeAudioFrames(sources[3], 0, 100)
	for _, base := range []string{
		ts.URL + "/hls/live/src_360p",
		ts.URL + "/hls/live/src_720p",
	} {
		pl := getPlaylist(t, base+"/video.m3u8")
		if len(pl.entries) == 0 {
			t.Errorf("%s: video playlist has no segments", base)
		}
		al := getPlaylist(t, base+"/audio.m3u8")
		if len(al.entries) == 0 {
			t.Errorf("%s: audio playlist has no segments", base)
		}
	}
}

func TestABRMasterFallsBackWhenVariantsMissing(t *testing.T) {
	reg := stream.NewRegistry()
	_, st, vs, as := newStreamSpec(t, "live/src")
	if _, err := reg.Publish("live/src", st); err != nil {
		t.Fatalf("publish: %v", err)
	}
	srv := New(reg, nil, nil)
	srv.SetPackaging(Options{SegmentTarget: time.Second, Window: 4, Logf: t.Logf}, 0)
	ts := httptest.NewServer(srv)
	t.Cleanup(func() {
		ts.Close()
		srv.Close()
		vs.Close()
		as.Close()
	})

	srv.SetABR([]ABRGroup{{
		Source:   "live/src",
		Variants: []ABRVariant{{Stream: "live/src_720p", Width: 1280, Height: 720, Bitrate: 2_000_000}},
	}})

	resp, body := doReq(t, http.MethodGet, ts.URL+"/hls/live/src/master.m3u8", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("master = %d, body %q", resp.StatusCode, body)
	}
	master := parseMasterPlaylist(t, body)
	if len(master.variants) != 1 || master.variants[0].uri != "video.m3u8" {
		t.Fatalf("fallback master = %+v\n%s", master.variants, body)
	}
}

// 未发布的档位不能进入 DASH 多 Representation 列表，否则会落到本地文件
// 回退源（测试素材）并声明错误的分辨率/码率。
func TestABRRenditionsSkipsUnpublished(t *testing.T) {
	reg := stream.NewRegistry()
	_, st, vs, as := newStreamSpec(t, "live/src_360p")
	if _, err := reg.Publish("live/src_360p", st); err != nil {
		t.Fatalf("publish: %v", err)
	}
	t.Cleanup(func() {
		vs.Close()
		as.Close()
	})

	srv := New(reg, nil, nil)
	srv.SetPackaging(Options{SegmentTarget: time.Second, Window: 4, Logf: t.Logf}, 0)
	t.Cleanup(srv.Close)
	srv.SetABR([]ABRGroup{{
		Source: "live/src",
		Variants: []ABRVariant{
			{Stream: "live/src_360p", Width: 640, Height: 360, Bitrate: 800_000},
			{Stream: "live/src_720p", Width: 1280, Height: 720, Bitrate: 2_500_000},
		},
	}})

	rends, ok := srv.ABRRenditions(context.Background(), "live/src")
	if !ok {
		t.Fatal("ladder not found")
	}
	if len(rends) != 1 {
		t.Fatalf("renditions = %d, want 1 (720p not published)", len(rends))
	}
	if rends[0].Variant.Stream != "live/src_360p" || rends[0].Packager == nil {
		t.Fatalf("rendition = %+v", rends[0])
	}

	if _, ok := srv.ABRRenditions(context.Background(), "live/other"); ok {
		t.Fatal("unknown source should not resolve a ladder")
	}
}

func TestABRMasterTokenOnVariantURIs(t *testing.T) {
	reg := stream.NewRegistry()
	srv := New(reg, nil, nil)
	srv.SetPackaging(Options{SegmentTarget: time.Second, Window: 4, Logf: t.Logf}, 0)
	srv.SetToken("secret")
	ts := httptest.NewServer(srv)
	t.Cleanup(func() {
		ts.Close()
		srv.Close()
	})
	var sources []*source.ExternalSource
	for _, name := range []string{"live/src_a", "live/src_b"} {
		_, st, vs, as := newStreamSpec(t, name)
		if _, err := reg.Publish(name, st); err != nil {
			t.Fatalf("publish: %v", err)
		}
		sources = append(sources, vs, as)
	}
	t.Cleanup(func() {
		for _, src := range sources {
			src.Close()
		}
	})
	srv.SetABR([]ABRGroup{{
		Source: "live/src",
		Variants: []ABRVariant{
			{Stream: "live/src_a", Width: 640, Height: 360, Bitrate: 500_000},
			{Stream: "live/src_b", Width: 1280, Height: 720, Bitrate: 2_000_000},
		},
	}})

	if resp, _ := doReq(t, http.MethodGet, ts.URL+"/hls/live/src/master.m3u8", nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("master without token = %d, want 403", resp.StatusCode)
	}
	resp, body := doReq(t, http.MethodGet, ts.URL+"/hls/live/src/master.m3u8?token=secret", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("master = %d, body %q", resp.StatusCode, body)
	}
	s := string(body)
	for _, want := range []string{
		"/hls/live/src_a/video.m3u8?token=secret",
		"/hls/live/src_b/video.m3u8?token=secret",
		`URI="/hls/live/src_a/audio.m3u8?token=secret"`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("master missing %q:\n%s", want, s)
		}
	}
}

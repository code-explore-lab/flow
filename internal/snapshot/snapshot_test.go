package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"image/jpeg"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"flow/internal/media"
	"flow/internal/metrics"
	"flow/internal/source"
	"flow/internal/stream"
)

func TestParsePath(t *testing.T) {
	tests := []struct {
		path   string
		name   string
		format string
		ok     bool
	}{
		{"/snapshot/live/test.jpg", "live/test", FormatJPEG, true},
		{"/snapshot/live/test.png", "live/test", FormatPNG, true},
		{"/snapshot/live/test", "live/test", "", true},
		{"/snapshot/live/test.jpeg", "live/test", FormatJPEG, true},
		{"/snapshot/", "", "", false},
		{"/snapshot", "", "", false},
		{"/other/live/test.jpg", "", "", false},
		{"/snapshot/../etc/passwd", "", "", false},
	}
	for _, tt := range tests {
		name, format, ok := parsePath(tt.path)
		if name != tt.name || format != tt.format || ok != tt.ok {
			t.Errorf("parsePath(%q) = %q, %q, %v; want %q, %q, %v",
				tt.path, name, format, ok, tt.name, tt.format, tt.ok)
		}
	}
}

func TestSnapshotJPEGAndPNG(t *testing.T) {
	reg, _ := loadTestStream(t)
	srv := New(reg, nil)
	srv.SetOptions(Options{Timeout: 2 * time.Second, CacheTTL: 0})

	rec := doRequest(srv, http.MethodGet, "/snapshot/live/test.jpg?width=320", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("jpeg status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "image/jpeg" {
		t.Fatalf("content-type = %q", ct)
	}
	img, err := jpeg.Decode(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("decode jpeg: %v", err)
	}
	if b := img.Bounds(); b.Dx() != 320 || b.Dy() != 240 {
		t.Fatalf("jpeg size = %dx%d, want 320x240", b.Dx(), b.Dy())
	}

	rec = doRequest(srv, http.MethodGet, "/snapshot/live/test.png", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("png status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "image/png" {
		t.Fatalf("content-type = %q", ct)
	}
	img, err = png.Decode(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("decode png: %v", err)
	}
	if b := img.Bounds(); b.Dx() != 640 || b.Dy() != 480 {
		t.Fatalf("png size = %dx%d, want 640x480", b.Dx(), b.Dy())
	}
}

func TestSnapshotHead(t *testing.T) {
	reg, _ := loadTestStream(t)
	srv := New(reg, nil)
	srv.SetOptions(Options{Timeout: 2 * time.Second})

	rec := doRequest(srv, http.MethodHead, "/snapshot/live/test.jpg", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("HEAD body = %d bytes", rec.Body.Len())
	}
	if rec.Header().Get("Content-Length") == "" {
		t.Fatal("HEAD missing Content-Length")
	}
}

func TestSnapshotNotFound(t *testing.T) {
	srv := New(stream.NewRegistry(), nil)
	srv.SetOptions(Options{Timeout: 100 * time.Millisecond})
	rec := doRequest(srv, http.MethodGet, "/snapshot/missing.jpg", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestSnapshotNoVideo(t *testing.T) {
	reg := stream.NewRegistry()
	st := &stream.Stream{}
	st.EnsureAudio(44100)
	if _, err := reg.Publish("live/audio", st); err != nil {
		t.Fatal(err)
	}
	srv := New(reg, nil)
	srv.SetOptions(Options{Timeout: 100 * time.Millisecond})
	rec := doRequest(srv, http.MethodGet, "/snapshot/live/audio.jpg", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestSnapshotUnsupportedCodec(t *testing.T) {
	reg := stream.NewRegistry()
	st := &stream.Stream{}
	st.SetVideoParams("H265", []byte{0, 0, 1}, []byte{0, 0, 1}, []byte{0, 0, 1})
	if _, err := reg.Publish("live/hevc", st); err != nil {
		t.Fatal(err)
	}
	srv := New(reg, nil)
	srv.SetOptions(Options{Timeout: 100 * time.Millisecond})
	rec := doRequest(srv, http.MethodGet, "/snapshot/live/hevc.jpg", nil)
	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want 415; body = %s", rec.Code, rec.Body.String())
	}
}

func TestSnapshotTimeout(t *testing.T) {
	reg := stream.NewRegistry()
	st := &stream.Stream{}
	st.SetVideoParams("H264", nil, nil, nil)
	if _, err := reg.Publish("live/idle", st); err != nil {
		t.Fatal(err)
	}
	srv := New(reg, nil)
	srv.SetOptions(Options{Timeout: 100 * time.Millisecond})
	rec := doRequest(srv, http.MethodGet, "/snapshot/live/idle.jpg", nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body = %s", rec.Code, rec.Body.String())
	}
}

func TestSnapshotCacheAndMetrics(t *testing.T) {
	reg, _ := loadTestStream(t)
	met := metrics.New()
	srv := New(reg, nil)
	srv.SetMetrics(met)
	srv.SetOptions(Options{Timeout: 2 * time.Second, CacheTTL: time.Minute})

	first := doRequest(srv, http.MethodGet, "/snapshot/live/test.jpg", nil)
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d", first.Code)
	}
	if first.Header().Get("X-Flow-Snapshot-Cache") != "" {
		t.Fatalf("first request unexpectedly served from cache")
	}
	second := doRequest(srv, http.MethodGet, "/snapshot/live/test.jpg", nil)
	if second.Code != http.StatusOK {
		t.Fatalf("second status = %d", second.Code)
	}
	if second.Header().Get("X-Flow-Snapshot-Cache") != "hit" {
		t.Fatalf("second request cache header = %q", second.Header().Get("X-Flow-Snapshot-Cache"))
	}
	if !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) {
		t.Fatal("cached body differs from first response")
	}

	var buf bytes.Buffer
	met.WritePrometheus(&buf)
	out := buf.String()
	for _, want := range []string{
		`flow_snapshot_requests_total{result="ok"} 2`,
		`flow_snapshot_cache_hits_total 1`,
		`flow_snapshot_decodes_total 1`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("metrics missing %q:\n%s", want, out)
		}
	}
}

func TestSnapshotToken(t *testing.T) {
	reg, _ := loadTestStream(t)
	srv := New(reg, nil)
	srv.SetOptions(Options{Timeout: 2 * time.Second})
	srv.SetToken("secret")

	if rec := doRequest(srv, http.MethodGet, "/snapshot/live/test.jpg", nil); rec.Code != http.StatusForbidden {
		t.Fatalf("no token status = %d, want 403", rec.Code)
	}
	if rec := doRequest(srv, http.MethodGet, "/snapshot/live/test.jpg?token=wrong", nil); rec.Code != http.StatusForbidden {
		t.Fatalf("wrong token status = %d, want 403", rec.Code)
	}
	if rec := doRequest(srv, http.MethodGet, "/snapshot/live/test.jpg?token=secret", nil); rec.Code != http.StatusOK {
		t.Fatalf("valid token status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	rec := doRequest(srv, http.MethodGet, "/snapshot/live/test.jpg", map[string]string{"X-Flow-Token": "secret"})
	if rec.Code != http.StatusOK {
		t.Fatalf("header token status = %d, want 200", rec.Code)
	}
}

func TestSnapshotCORSAndReferer(t *testing.T) {
	reg, _ := loadTestStream(t)
	srv := New(reg, nil)
	srv.SetOptions(Options{Timeout: 2 * time.Second})
	srv.SetCORS("*")
	srv.SetReferer("example.com", true)

	rec := doRequest(srv, http.MethodOptions, "/snapshot/live/test.jpg", map[string]string{
		"Origin": "https://player.example",
	})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("OPTIONS status = %d", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("CORS origin = %q", got)
	}

	if rec := doRequest(srv, http.MethodGet, "/snapshot/live/test.jpg", nil); rec.Code != http.StatusForbidden {
		t.Fatalf("missing referer status = %d, want 403", rec.Code)
	}
	if rec := doRequest(srv, http.MethodGet, "/snapshot/live/test.jpg", map[string]string{
		"Referer": "https://evil.com/example.com",
	}); rec.Code != http.StatusForbidden {
		t.Fatalf("evil referer status = %d, want 403", rec.Code)
	}
	if rec := doRequest(srv, http.MethodGet, "/snapshot/live/test.jpg", map[string]string{
		"Referer": "https://example.com/watch",
	}); rec.Code != http.StatusOK {
		t.Fatalf("good referer status = %d, want 200", rec.Code)
	}
}

func TestSnapshotLocalFallback(t *testing.T) {
	// 本地文件源：请求 live/anything 时注册表未命中，回退 testdata。
	local := source.NewH264Source(filepath.Join("..", "..", "testdata", "test.h264"))
	srv := New(stream.NewRegistry(), local)
	srv.SetOptions(Options{Timeout: 3 * time.Second, CacheTTL: 0})

	rec := doRequest(srv, http.MethodGet, "/snapshot/live/demo.jpg", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if _, err := jpeg.Decode(bytes.NewReader(rec.Body.Bytes())); err != nil {
		t.Fatalf("decode jpeg: %v", err)
	}
}

func TestSnapshotHEVCFFmpegFallback(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg not installed")
	}
	tmp := t.TempDir()
	hevcFile := filepath.Join(tmp, "hevc.h265")
	cmd := exec.Command(ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
		"-f", "lavfi", "-i", "testsrc2=size=320x240:rate=25", "-t", "0.5",
		"-c:v", "libx265", "-x265-params", "keyint=10:min-keyint=10:log-level=error",
		"-pix_fmt", "yuv420p", "-f", "hevc", hevcFile)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("ffmpeg cannot encode HEVC: %v: %s", err, out)
	}

	reg := stream.NewRegistry()
	st := &stream.Stream{}
	st.SetVideoParams("H265", nil, nil, nil)
	src := st.SourceFor("video")
	if src == nil {
		t.Fatal("no video source")
	}

	demux, err := media.NewH264Demux(hevcFile)
	if err != nil {
		t.Fatal(err)
	}
	var ts uint32
	started := false
	var vps, sps, pps []byte
	for {
		nalu, err := demux.NextNALU()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		cls := media.ClassifyVideoNAL("H265", nalu)
		switch {
		case cls.IsVPS:
			vps = nalu
		case cls.IsSPS:
			sps = nalu
		case cls.IsPPS:
			pps = nalu
		}
		if cls.IsVCL && media.StartsVideoAU("H265", nalu) {
			if started {
				ts += 3600
			} else {
				started = true
			}
		}
		src.Write(source.Frame{
			Data:      nalu,
			Timestamp: ts,
			Keyframe:  cls.IsIDR,
			AUStart:   cls.IsVCL && media.StartsVideoAU("H265", nalu),
		})
	}
	st.SetVideoParams("H265", vps, sps, pps)
	if _, err := reg.Publish("live/hevc", st); err != nil {
		t.Fatal(err)
	}

	srv := New(reg, nil)
	srv.SetOptions(Options{Timeout: 2 * time.Second, FFmpegPath: ffmpeg})
	rec := doRequest(srv, http.MethodGet, "/snapshot/live/hevc.jpg?width=160", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	img, err := jpeg.Decode(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("decode jpeg: %v", err)
	}
	if b := img.Bounds(); b.Dx() != 160 || b.Dy() != 120 {
		t.Fatalf("size = %dx%d, want 160x120", b.Dx(), b.Dy())
	}
}

func TestGrabKeyframeFresh(t *testing.T) {
	src := source.NewExternalSource()
	key := testKeyframe()
	src.Write(key)

	// 预热订阅能立刻拿到缓存的 IDR。
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	kf, err := grabKeyframe(ctx, src, "H264", nil, false, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("cached grab: %v", err)
	}
	if kf.pts != 90000 {
		t.Fatalf("pts = %d, want 90000", kf.pts)
	}

	// fresh 模式不预热，必须等新的关键帧写进来。
	go func() {
		time.Sleep(50 * time.Millisecond)
		src.Write(testFrame(180000))
	}()
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	kf, err = grabKeyframe(ctx2, src, "H264", nil, true, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("fresh grab: %v", err)
	}
	if kf.pts != 180000 {
		t.Fatalf("fresh pts = %d, want 180000", kf.pts)
	}
}

// ---- helpers ----

func doRequest(srv *Server, method, target string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec
}

// loadTestStream 把 testdata/test.h264 的全部 NALU 写入一个 ExternalSource，
// 时间戳按访问单元递增，然后登记到注册表。这样预热 GOP 立即就绪，测试
// 不依赖实时读取节奏。
func loadTestStream(t *testing.T) (*stream.Registry, *stream.Stream) {
	t.Helper()
	demux, err := media.NewH264Demux(filepath.Join("..", "..", "testdata", "test.h264"))
	if err != nil {
		t.Fatal(err)
	}

	st := &stream.Stream{}
	st.SetVideoParams("H264", nil, nil, nil)
	src := st.SourceFor("video")
	if src == nil {
		t.Fatal("no video source")
	}

	var ts uint32
	started := false
	var sps, pps []byte
	for {
		nalu, err := demux.NextNALU()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		cls := media.ClassifyVideoNAL("H264", nalu)
		switch {
		case cls.IsSPS:
			sps = nalu
		case cls.IsPPS:
			pps = nalu
		}
		newAU := cls.IsVCL && media.StartsVideoAU("H264", nalu)
		if newAU {
			if started {
				ts += 3600
			} else {
				started = true
			}
		}
		src.Write(source.Frame{
			Data:      nalu,
			Timestamp: ts,
			Keyframe:  cls.IsIDR,
			AUStart:   newAU,
		})
	}
	if sps != nil || pps != nil {
		st.SetVideoParams("H264", nil, sps, pps)
	}

	reg := stream.NewRegistry()
	if _, err := reg.Publish("live/test", st); err != nil {
		t.Fatal(err)
	}
	return reg, st
}

// testKeyframe 返回一个只用于 grab 逻辑测试的最小 IDR AU（解码器不会用到）。
func testKeyframe() source.Frame {
	return testFrame(90000)
}

func testFrame(pts uint32) source.Frame {
	return source.Frame{
		Data:      []byte{0x65, 0x88, 0x84}, // IDR slice 头（仅用于分类）
		Timestamp: pts,
		Keyframe:  true,
		AUStart:   true,
		AUEnd:     true,
	}
}

var _ = fmt.Sprintf
var _ = os.Stat

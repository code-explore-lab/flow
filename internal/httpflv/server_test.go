package httpflv

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"flow/internal/flv"
	"flow/internal/source"
	"flow/internal/stream"

	"flow/internal/access"
)

// newPublishedStream 登记一路带音视频参数的流。
func newPublishedStream(t *testing.T, reg *stream.Registry, name string) *stream.Stream {
	t.Helper()
	st := &stream.Stream{}
	st.SetVideoParams("H264", nil, []byte{0x67, 0x42, 0x00, 0x1e}, []byte{0x68, 0xce})
	st.SetAudioCodec(stream.AudioAAC, []byte{0x12, 0x10}, 44100, 2, 44100)
	if _, err := reg.Publish(name, st); err != nil {
		t.Fatalf("publish %q: %v", name, err)
	}
	return st
}

type readResult struct {
	tags []*flv.Tag
	err  error
}

// TestStreamVideoAndAudio 走完整 HTTP 路径：FLV 头、onMetaData、两个序列头
// 和音视频媒体 tag 都要按顺序出现。
func TestStreamVideoAndAudio(t *testing.T) {
	reg := stream.NewRegistry()
	st := newPublishedStream(t, reg, "live/test")
	meta := []byte{0x02, 0x00, 0x0A, 'o', 'n', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a'}
	st.SetMetaDataPayload(meta)

	srv := httptest.NewServer(New(reg, nil, nil))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/live/test.flv")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "video/x-flv" {
		t.Fatalf("content-type = %q", ct)
	}

	// 订阅发生在响应头写出之后，持续写帧直到客户端收齐。
	stop := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				st.VideoTrack().Src.Write(source.Frame{
					Data: []byte{0x65, 0x01}, Timestamp: 9000,
					Keyframe: true, AUStart: true, AUEnd: true,
				})
				st.AudioTrack().Src.Write(source.Frame{
					Data: []byte{0xAA}, Timestamp: 44100,
				})
			}
		}
	}()
	defer func() {
		close(stop)
		writer.Wait()
	}()

	hdr, err := flv.ReadHeader(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !hdr.HasVideo || !hdr.HasAudio {
		t.Fatalf("header = %+v", hdr)
	}

	ch := make(chan readResult, 1)
	go func() {
		var tags []*flv.Tag
		for {
			tag, err := flv.ReadTag(resp.Body)
			if err != nil {
				ch <- readResult{tags: tags, err: err}
				return
			}
			tags = append(tags, tag)
			if len(tags) >= 5 {
				ch <- readResult{tags: tags}
				return
			}
		}
	}()

	var res readResult
	select {
	case res = <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for FLV tags")
	}
	if res.err != nil && len(res.tags) < 5 {
		t.Fatalf("read tags: %v", res.err)
	}
	tags := res.tags

	if tags[0].Type != flv.TagScript || !bytes.Equal(tags[0].Data, meta) {
		t.Fatalf("first tag = type %d data %x", tags[0].Type, tags[0].Data)
	}
	vseq, err := flv.ParseVideoTag(tags[1].Data)
	if tags[1].Type != flv.TagVideo || err != nil || vseq.PacketType != flv.AVCSeqHeader {
		t.Fatalf("video sequence tag = %+v err = %v", tags[1], err)
	}
	aseq, err := flv.ParseAudioTag(tags[2].Data)
	if tags[2].Type != flv.TagAudio || err != nil || aseq.PacketType != flv.AACSeqHeader {
		t.Fatalf("audio sequence tag = %+v err = %v", tags[2], err)
	}

	sawVideo, sawAudio := false, false
	for _, tag := range tags[3:] {
		switch tag.Type {
		case flv.TagVideo:
			vt, err := flv.ParseVideoTag(tag.Data)
			if err != nil || vt.PacketType != flv.AVCNALU {
				t.Fatalf("video tag = %+v err = %v", tag, err)
			}
			if tag.Timestamp != 0 {
				t.Fatalf("video timestamp = %d, want 0", tag.Timestamp)
			}
			sawVideo = true
		case flv.TagAudio:
			at, err := flv.ParseAudioTag(tag.Data)
			if err != nil || at.PacketType != flv.AACRaw || !bytes.Equal(at.AU, []byte{0xAA}) {
				t.Fatalf("audio tag = %+v err = %v", tag, err)
			}
			if tag.Timestamp != 0 {
				t.Fatalf("audio timestamp = %d, want 0", tag.Timestamp)
			}
			sawAudio = true
		}
	}
	if !sawVideo || !sawAudio {
		t.Fatalf("sawVideo=%v sawAudio=%v tags=%d", sawVideo, sawAudio, len(tags))
	}
}

// TestHeadAndNameResolution 校验 HEAD 不建立流、RTSP 风格的 live/<name>
// 能匹配到 RTMP 的裸流名。
func TestHeadAndNameResolution(t *testing.T) {
	reg := stream.NewRegistry()
	newPublishedStream(t, reg, "cam")
	srv := httptest.NewServer(New(reg, nil, nil))
	defer srv.Close()

	resp, err := http.Head(srv.URL + "/live/cam.flv")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "video/x-flv" {
		t.Fatalf("content-type = %q", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	if len(body) != 0 {
		t.Fatalf("HEAD body has %d bytes", len(body))
	}
}

func TestRoutingAndMethods(t *testing.T) {
	reg := stream.NewRegistry()
	newPublishedStream(t, reg, "live/test")
	srv := httptest.NewServer(New(reg, nil, nil))
	defer srv.Close()

	cases := []struct {
		name   string
		method string
		path   string
		want   int
	}{
		{"unknown stream", http.MethodHead, "/live/missing.flv", http.StatusNotFound},
		{"not flv", http.MethodHead, "/live/test.mp4", http.StatusNotFound},
		{"path traversal", http.MethodHead, "/live/../test.flv", http.StatusNotFound},
		{"post", http.MethodPost, "/live/test.flv", http.StatusMethodNotAllowed},
		{"get", http.MethodHead, "/live/test.flv", http.StatusOK},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req, err := http.NewRequest(c.method, srv.URL+c.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != c.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, c.want)
			}
			if c.want == http.StatusMethodNotAllowed && resp.Header.Get("Allow") == "" {
				t.Fatal("405 without Allow header")
			}
		})
	}
}

func TestCORS(t *testing.T) {
	reg := stream.NewRegistry()
	newPublishedStream(t, reg, "live/test")

	t.Run("wildcard preflight", func(t *testing.T) {
		s := New(reg, nil, nil)
		s.SetCORS("*")
		srv := httptest.NewServer(s)
		defer srv.Close()

		req, _ := http.NewRequest(http.MethodOptions, srv.URL+"/live/test.flv", nil)
		req.Header.Set("Origin", "http://player.example")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "*" {
			t.Fatalf("allow-origin = %q", got)
		}
		if got := resp.Header.Get("Access-Control-Allow-Methods"); got == "" {
			t.Fatal("missing allow-methods")
		}
	})

	t.Run("origin allowlist", func(t *testing.T) {
		s := New(reg, nil, nil)
		s.SetCORS("http://good.example")
		srv := httptest.NewServer(s)
		defer srv.Close()

		req, _ := http.NewRequest(http.MethodHead, srv.URL+"/live/test.flv", nil)
		req.Header.Set("Origin", "http://evil.example")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "" {
			t.Fatalf("disallowed origin got %q", got)
		}

		req, _ = http.NewRequest(http.MethodHead, srv.URL+"/live/test.flv", nil)
		req.Header.Set("Origin", "http://good.example")
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "http://good.example" {
			t.Fatalf("allow-origin = %q", got)
		}
	})
}

func TestRefererAndToken(t *testing.T) {
	reg := stream.NewRegistry()
	newPublishedStream(t, reg, "live/test")

	s := New(reg, nil, nil)
	s.SetReferer("example.com,*.cdn.example.com", true)
	s.SetToken("secret")
	srv := httptest.NewServer(s)
	defer srv.Close()

	do := func(path, referer, token, headerToken string) int {
		req, _ := http.NewRequest(http.MethodHead, srv.URL+path, nil)
		if referer != "" {
			req.Header.Set("Referer", referer)
		}
		if headerToken != "" {
			req.Header.Set("X-Flow-Token", headerToken)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	if got := do("/live/test.flv?token=secret", "", "", ""); got != http.StatusForbidden {
		t.Fatalf("missing referer: status = %d", got)
	}
	if got := do("/live/test.flv?token=secret", "http://evil.example/x", "", ""); got != http.StatusForbidden {
		t.Fatalf("bad referer: status = %d", got)
	}
	if got := do("/live/test.flv", "http://example.com/x", "", ""); got != http.StatusForbidden {
		t.Fatalf("missing token: status = %d", got)
	}
	if got := do("/live/test.flv?token=wrong", "http://example.com/x", "", ""); got != http.StatusForbidden {
		t.Fatalf("wrong token: status = %d", got)
	}
	if got := do("/live/test.flv?token=secret", "http://sub.cdn.example.com/x", "", ""); got != http.StatusOK {
		t.Fatalf("query token: status = %d", got)
	}
	if got := do("/live/test.flv", "http://example.com/x", "", "secret"); got != http.StatusOK {
		t.Fatalf("header token: status = %d", got)
	}
}

func TestWhitelist(t *testing.T) {
	reg := stream.NewRegistry()
	newPublishedStream(t, reg, "live/test")

	write := func(content string) *access.Whitelist {
		path := filepath.Join(t.TempDir(), "whitelist.txt")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		w, err := access.ParseWhitelistFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return w
	}

	denied := New(reg, nil, nil)
	denied.SetWhitelist(write("10.0.0.0/8\n"))
	srv := httptest.NewServer(denied)
	resp, err := http.Head(srv.URL + "/live/test.flv")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	srv.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("denied status = %d", resp.StatusCode)
	}

	allowed := New(reg, nil, nil)
	allowed.SetWhitelist(write("127.0.0.0/8\n::1/128\n"))
	srv = httptest.NewServer(allowed)
	defer srv.Close()
	resp, err = http.Head(srv.URL + "/live/test.flv")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("allowed status = %d", resp.StatusCode)
	}
}

func TestStreamName(t *testing.T) {
	cases := []struct {
		path string
		name string
		ok   bool
	}{
		{"/live/test.flv", "live/test", true},
		{"/test.flv", "test", true},
		{"/live/test.FLV", "live/test", true},
		{"/a/b/c.flv", "a/b/c", true},
		{"/live/test.mp4", "", false},
		{"/live/../x.flv", "", false},
		{"/live//test.flv", "", false},
		{"/.flv", "", false},
		{"/live/", "", false},
	}
	for _, c := range cases {
		name, ok := streamName(c.path)
		if name != c.name || ok != c.ok {
			t.Fatalf("%q = (%q,%v), want (%q,%v)", c.path, name, ok, c.name, c.ok)
		}
	}
}

func TestRefererMatching(t *testing.T) {
	s := New(nil, nil, nil)
	s.SetReferer("example.com,*.cdn.example.com", false)
	cases := []struct {
		ref  string
		want bool
	}{
		{"", true}, // require=false
		{"http://example.com/page", true},
		{"https://m.example.com/page", true},
		{"http://evil.com/example.com", false},
		{"http://cdn.example.com/x", true},
		{"http://a.cdn.example.com/x", true},
		{"http://a.b.cdn.example.com/x", true},
		{"http://cdn.example.com.evil/x", false},
		{"http://notexample.com/x", false},
	}
	for _, c := range cases {
		req := httptest.NewRequest(http.MethodGet, "/live/x.flv", nil)
		if c.ref != "" {
			req.Header.Set("Referer", c.ref)
		}
		if got := s.refererAllowed(req); got != c.want {
			t.Fatalf("referer %q = %v, want %v", c.ref, got, c.want)
		}
	}
}

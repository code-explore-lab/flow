package origin

import (
	"strings"
	"testing"
)

func TestParseSpecsWildcard(t *testing.T) {
	specs, err := ParseSpecs("rtmp://127.0.0.1/live, rtsp://cam.example/app/ ,http://cdn.example")
	if err != nil {
		t.Fatalf("ParseSpecs: %v", err)
	}
	if len(specs) != 3 {
		t.Fatalf("got %d specs, want 3", len(specs))
	}
	want := []Spec{
		{Scheme: "rtmp", Host: "127.0.0.1:1935", Base: "live"},
		{Scheme: "rtsp", Host: "cam.example:554", Base: "app"},
		{Scheme: "http", Host: "cdn.example:80", Base: ""},
	}
	for i := range want {
		if specs[i] != want[i] {
			t.Errorf("spec #%d = %+v, want %+v", i+1, specs[i], want[i])
		}
	}
}

func TestParseSpecsExplicit(t *testing.T) {
	specs, err := ParseSpecs("cam=rtsp://10.0.0.1:8554/app/stream?token=abc")
	if err != nil {
		t.Fatalf("ParseSpecs: %v", err)
	}
	if len(specs) != 1 {
		t.Fatalf("got %d specs, want 1", len(specs))
	}
	s := specs[0]
	if s.Local != "cam" {
		t.Errorf("Local = %q, want cam", s.Local)
	}
	if s.Scheme != "rtsp" || s.Host != "10.0.0.1:8554" || s.Base != "app/stream" {
		t.Errorf("unexpected spec %+v", s)
	}
	if !strings.Contains(s.Raw, "token=abc") {
		t.Errorf("Raw %q lost the query string", s.Raw)
	}
}

// TestParseSpecsWildcardKeepsQueryAndUser 通配模板也要保住 query 与 userinfo：
// RTMP 的 ?token= 是 T-3 拉流鉴权，RTSP 的 user:pass 用于上游 Basic/Digest。
func TestParseSpecsWildcardKeepsQueryAndUser(t *testing.T) {
	specs, err := ParseSpecs("rtmp://h/live?token=abc, rtsp://user:p%40ss@h/live")
	if err != nil {
		t.Fatalf("ParseSpecs: %v", err)
	}
	if len(specs) != 2 {
		t.Fatalf("got %d specs, want 2", len(specs))
	}
	if specs[0].Query != "token=abc" {
		t.Errorf("Query = %q, want token=abc", specs[0].Query)
	}
	if got := specs[0].urlFor("src"); got != "rtmp://h:1935/live/src?token=abc" {
		t.Errorf("rtmp urlFor = %q, want token preserved", got)
	}
	if specs[1].User == nil || specs[1].User.Username() != "user" {
		t.Fatalf("User = %+v, want user", specs[1].User)
	}
	if pass, _ := specs[1].User.Password(); pass != "p@ss" {
		t.Errorf("password = %q, want p@ss", pass)
	}
	if got := specs[1].urlFor("src"); got != "rtsp://user:p%40ss@h:554/live/src" {
		t.Errorf("rtsp urlFor = %q, want userinfo preserved", got)
	}
}

func TestParseSpecsExplicitFillsDefaultPort(t *testing.T) {
	specs, err := ParseSpecs("a=rtmp://h/live/src")
	if err != nil {
		t.Fatalf("ParseSpecs: %v", err)
	}
	if specs[0].Raw != "rtmp://h:1935/live/src" {
		t.Errorf("Raw = %q, want default port added", specs[0].Raw)
	}
}

func TestParseSpecsErrors(t *testing.T) {
	cases := []string{
		"ftp://host/app",          // 不支持的 scheme
		"rtsp:///app",             // 缺 host
		"x=rtmp://host",           // 显式映射没有流路径
		"na me=rtmp://host/live",  // 非法本地名
		"://bad",                  // 解析失败
		"rtsp://host/app,ftp://h", // 第二项非法
	}
	for _, raw := range cases {
		if specs, err := ParseSpecs(raw); err == nil {
			t.Errorf("ParseSpecs(%q) = %+v, want error", raw, specs)
		}
	}
}

func TestParseSpecsEmpty(t *testing.T) {
	for _, raw := range []string{"", "  ", ", ,"} {
		specs, err := ParseSpecs(raw)
		if err != nil {
			t.Errorf("ParseSpecs(%q): %v", raw, err)
			continue
		}
		if len(specs) != 0 {
			t.Errorf("ParseSpecs(%q) = %+v, want none", raw, specs)
		}
	}
}

func TestSpecURLForWildcard(t *testing.T) {
	cases := []struct {
		spec Spec
		name string
		want string
	}{
		// base 的最后一段与本地名前缀相同，去重后只保留一次。
		{Spec{Scheme: "rtmp", Host: "h:1935", Base: "live"}, "live/src", "rtmp://h:1935/live/src"},
		// 本地名是裸名（RTMP/HTTP-FLV 常见），直接拼在 base 后。
		{Spec{Scheme: "rtmp", Host: "h:1935", Base: "live"}, "src", "rtmp://h:1935/live/src"},
		// http 追加 .flv。
		{Spec{Scheme: "http", Host: "h:80", Base: "live"}, "src", "http://h:80/live/src.flv"},
		{Spec{Scheme: "http", Host: "h:80", Base: "live"}, "live/src", "http://h:80/live/src.flv"},
		// 上游地址本身已带 .flv 时不重复追加。
		{Spec{Scheme: "http", Host: "h:80", Base: "live"}, "src.flv", "http://h:80/live/src.flv"},
		// 无 base 时直接用本地名。
		{Spec{Scheme: "rtmp", Host: "h:1935"}, "a/b", "rtmp://h:1935/a/b"},
		{Spec{Scheme: "rtsp", Host: "h:554"}, "live/src", "rtsp://h:554/live/src"},
		// 本地名恰好等于 base 的最后一段。
		{Spec{Scheme: "rtmp", Host: "h:1935", Base: "live"}, "live", "rtmp://h:1935/live/live"},
		// 本地名前缀不同则不做去重。
		{Spec{Scheme: "rtmp", Host: "h:1935", Base: "app"}, "live/src", "rtmp://h:1935/app/live/src"},
	}
	for _, c := range cases {
		if got := c.spec.urlFor(c.name); got != c.want {
			t.Errorf("urlFor(%q) with %+v = %q, want %q", c.name, c.spec, got, c.want)
		}
	}
}

func TestSpecURLForExplicitIgnoresName(t *testing.T) {
	s := Spec{Local: "cam", Raw: "rtsp://10.0.0.1:554/app/stream"}
	if got := s.urlFor("cam"); got != s.Raw {
		t.Errorf("urlFor = %q, want %q", got, s.Raw)
	}
	if got := s.urlFor("something/else"); got != s.Raw {
		t.Errorf("urlFor = %q, want %q", got, s.Raw)
	}
}

func TestStreamLeaf(t *testing.T) {
	cases := map[string]string{
		"live/src": "src",
		"src":      "src",
		"/a/b/c/":  "c",
		"":         "",
	}
	for in, want := range cases {
		if got := streamLeaf(in); got != want {
			t.Errorf("streamLeaf(%q) = %q, want %q", in, got, want)
		}
	}
}

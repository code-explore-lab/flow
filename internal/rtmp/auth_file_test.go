package rtmp

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestParseAuthFile(t *testing.T) {
	const data = `
# comment

publish:pub1
play:play1
all:all1
bare-secret
app:live
app:studio
all:with:colon
PUBLISH:upper
`
	cfg, err := ParseAuthFile(strings.NewReader(data))
	if err != nil {
		t.Fatalf("ParseAuthFile: %v", err)
	}
	if want := []string{"pub1", "upper"}; !slices.Equal(cfg.PublishTokens, want) {
		t.Fatalf("publish tokens = %v, want %v", cfg.PublishTokens, want)
	}
	if want := []string{"play1"}; !slices.Equal(cfg.PlayTokens, want) {
		t.Fatalf("play tokens = %v, want %v", cfg.PlayTokens, want)
	}
	if want := []string{"all1", "bare-secret", "with:colon"}; !slices.Equal(cfg.AllTokens, want) {
		t.Fatalf("all tokens = %v, want %v", cfg.AllTokens, want)
	}
	if want := []string{"live", "studio"}; !slices.Equal(cfg.Apps, want) {
		t.Fatalf("apps = %v, want %v", cfg.Apps, want)
	}

	a := NewAuth(cfg)
	if !a.Allow("push?token=pub1", "", permPublish) || a.Allow("push?token=pub1", "", permPlay) {
		t.Fatal("publish token permissions are wrong")
	}
	if !a.Allow("push?token=with%3Acolon", "", permPlay) {
		t.Fatal("token containing ':' should be accepted")
	}
	if !a.AppAllowed("live") || a.AppAllowed("other") {
		t.Fatal("app whitelist from file is wrong")
	}
}

func TestParseAuthFileErrors(t *testing.T) {
	cases := map[string]string{
		"empty file":         "",
		"comments only":      "# nothing\n\n",
		"empty value":        "publish:\n",
		"unknown permission": "user:pass\n",
	}
	for name, data := range cases {
		if _, err := ParseAuthFile(strings.NewReader(data)); err == nil {
			t.Fatalf("%s: ParseAuthFile accepted invalid input", name)
		}
	}
}

func TestLoadAuthFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rtmp-auth")
	if err := os.WriteFile(path, []byte("publish:pub\napp:live\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadAuthFile(path)
	if err != nil {
		t.Fatalf("LoadAuthFile: %v", err)
	}
	if !slices.Equal(cfg.PublishTokens, []string{"pub"}) || !slices.Equal(cfg.Apps, []string{"live"}) {
		t.Fatalf("cfg = %+v", cfg)
	}

	// 解析错误要带上文件路径，方便启动排查。
	bad := filepath.Join(dir, "bad")
	if err := os.WriteFile(bad, []byte("nope:value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadAuthFile(bad); err == nil || !strings.Contains(err.Error(), bad) {
		t.Fatalf("error = %v, want it to mention %s", err, bad)
	}
}

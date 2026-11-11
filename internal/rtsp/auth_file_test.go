package rtsp

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestParseAuthFile(t *testing.T) {
	in := `
# 注释行和空行忽略
alice:secret

bob:p@ss:word
  carol : spaced
`
	got, err := ParseAuthFile(strings.NewReader(in))
	if err != nil {
		t.Fatalf("ParseAuthFile() error = %v", err)
	}
	want := map[string]string{
		"alice": "secret",
		"bob":   "p@ss:word", // 密码里允许冒号
		"carol": "spaced",    // 用户名/密码两端空白去掉
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseAuthFile() = %#v, want %#v", got, want)
	}
}

func TestParseAuthFileErrors(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{"缺少冒号", "alice"},
		{"空用户名", ":secret"},
		{"重复用户名", "alice:a\nalice:b"},
		{"只有注释和空行", "# nothing here\n\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseAuthFile(strings.NewReader(tt.in)); err == nil {
				t.Fatalf("ParseAuthFile(%q) = nil error, want error", tt.in)
			}
		})
	}
}

func TestLoadAuthFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials")
	if err := os.WriteFile(path, []byte("alice:secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	users, err := LoadAuthFile(path)
	if err != nil {
		t.Fatalf("LoadAuthFile() error = %v", err)
	}
	if !reflect.DeepEqual(users, map[string]string{"alice": "secret"}) {
		t.Fatalf("LoadAuthFile() = %#v", users)
	}

	// 解析错误要带上路径，方便启动时定位。
	badPath := filepath.Join(dir, "bad")
	if err := os.WriteFile(badPath, []byte("no-colon\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadAuthFile(badPath); err == nil || !strings.Contains(err.Error(), badPath) {
		t.Fatalf("LoadAuthFile(bad) error = %v, want path in message", err)
	}

	if _, err := LoadAuthFile(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("LoadAuthFile(missing) = nil error, want error")
	}
}

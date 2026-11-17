package rtmp

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

// ParseAuthFile 解析 RTMP token 文件。
//
// 每行一条规则，空行和 # 开头的注释忽略：
//
//	publish:SECRET   只允许推流
//	play:SECRET      只允许拉流
//	all:SECRET       推拉都允许
//	SECRET           没有前缀时按 all 处理，方便只放一串密钥
//	app:live         app 白名单条目
//
// 权限前缀大小写不敏感，按第一个冒号切分，所以 token 里可以继续包含冒号。
func ParseAuthFile(r io.Reader) (AuthConfig, error) {
	var cfg AuthConfig
	sc := bufio.NewScanner(r)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			cfg.AllTokens = append(cfg.AllTokens, line)
			continue
		}
		value = strings.TrimSpace(value)
		if value == "" {
			return AuthConfig{}, fmt.Errorf("line %d: empty value", lineNo)
		}
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "publish":
			cfg.PublishTokens = append(cfg.PublishTokens, value)
		case "play":
			cfg.PlayTokens = append(cfg.PlayTokens, value)
		case "all":
			cfg.AllTokens = append(cfg.AllTokens, value)
		case "app":
			cfg.Apps = append(cfg.Apps, value)
		default:
			return AuthConfig{}, fmt.Errorf("line %d: unknown permission %q (want publish, play, all or app)", lineNo, key)
		}
	}
	if err := sc.Err(); err != nil {
		return AuthConfig{}, fmt.Errorf("read auth file: %w", err)
	}
	if len(cfg.PublishTokens)+len(cfg.PlayTokens)+len(cfg.AllTokens)+len(cfg.Apps) == 0 {
		return AuthConfig{}, fmt.Errorf("no credentials found")
	}
	return cfg, nil
}

// LoadAuthFile 读取并解析 token 文件。返回的错误带上文件路径，方便启动排查。
func LoadAuthFile(path string) (AuthConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return AuthConfig{}, err
	}
	defer f.Close()

	cfg, err := ParseAuthFile(f)
	if err != nil {
		return AuthConfig{}, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

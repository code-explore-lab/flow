package rtsp

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

// ParseAuthFile 解析 username:password 格式的凭据文件。
//
// 规则：
//   - 空行和 # 开头的注释行忽略；
//   - 按第一个冒号切分，密码里可以继续包含冒号；
//   - 用户名两端空白会被去掉，密码保留冒号后的内容（两端空白也去掉）；
//   - 空用户名、缺少冒号、重复用户名都是错误；
//   - 文件里至少要有一条凭据。
func ParseAuthFile(r io.Reader) (map[string]string, error) {
	users := make(map[string]string)
	sc := bufio.NewScanner(r)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		user, pass, ok := strings.Cut(line, ":")
		if !ok {
			return nil, fmt.Errorf("line %d: missing ':' separator", lineNo)
		}
		user = strings.TrimSpace(user)
		pass = strings.TrimSpace(pass)
		if user == "" {
			return nil, fmt.Errorf("line %d: empty username", lineNo)
		}
		if _, dup := users[user]; dup {
			return nil, fmt.Errorf("line %d: duplicate username %q", lineNo, user)
		}
		users[user] = pass
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read credentials: %w", err)
	}
	if len(users) == 0 {
		return nil, fmt.Errorf("no credentials found")
	}
	return users, nil
}

// LoadAuthFile 读取并解析凭据文件。返回的错误带上文件路径，方便启动排查。
func LoadAuthFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	users, err := ParseAuthFile(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return users, nil
}

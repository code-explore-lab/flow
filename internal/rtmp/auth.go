package rtmp

// RTMP 连接鉴权：token 与 app 白名单。
//
// RTMP 没有 RTSP 那样的 Authorization 头，ffmpeg 等客户端也不会把 URL 里的
// user:pass@ 发给服务端（实测只发 app 和流名），所以凭据统一放在流名的
// 查询串里：
//
//	rtmp://host/app/stream?token=SECRET
//
// 鉴权发生在 sanitizeStreamName 之前，注册表里的流名仍然不含查询串。
// 推拉权限分离：publish token 只能推、play token 只能拉、all token 两者都可；
// 某个方向没有配置任何 token 时该方向不鉴权，便于只保护推流或只保护拉流。

import (
	"crypto/sha256"
	"net/url"
	"strings"
)

// tokenPerm 是 token 允许的操作位。
type tokenPerm uint8

const (
	permPublish tokenPerm = 1 << iota
	permPlay
)

// AuthConfig 是构造 Auth 的原始配置。
type AuthConfig struct {
	PublishTokens []string // 只允许推流
	PlayTokens    []string // 只允许拉流
	AllTokens     []string // 推拉都允许
	Apps          []string // app 白名单；空表示不限制
}

// Add 追加另一组配置并返回合并结果，用于文件 + 环境变量 + 命令行合并。
func (c AuthConfig) Add(other AuthConfig) AuthConfig {
	c.PublishTokens = append(c.PublishTokens, other.PublishTokens...)
	c.PlayTokens = append(c.PlayTokens, other.PlayTokens...)
	c.AllTokens = append(c.AllTokens, other.AllTokens...)
	c.Apps = append(c.Apps, other.Apps...)
	return c
}

// Auth 是构建后的鉴权配置，构造后只读，可被多个连接并发使用。
type Auth struct {
	// token 只保存 SHA-256，避免内存里留有明文；也避免比较时泄露长度。
	tokens map[[sha256.Size]byte]tokenPerm
	apps   map[string]struct{}
	// publish/play 表示该方向是否配置了 token（配置了才强制校验）。
	publish bool
	play    bool
}

// NewAuth 构建鉴权配置。空 token 会被忽略；同一 token 出现在多组里时权限取并集。
// 配置全空时返回的 Auth 不启用任何校验（Enabled 为 false）。
func NewAuth(cfg AuthConfig) *Auth {
	a := &Auth{
		tokens: make(map[[sha256.Size]byte]tokenPerm),
		apps:   make(map[string]struct{}),
	}
	add := func(tokens []string, perm tokenPerm) {
		for _, tok := range tokens {
			tok = strings.TrimSpace(tok)
			if tok == "" {
				continue
			}
			a.tokens[sha256.Sum256([]byte(tok))] |= perm
		}
	}
	add(cfg.PublishTokens, permPublish)
	add(cfg.PlayTokens, permPlay)
	add(cfg.AllTokens, permPublish|permPlay)
	for _, app := range cfg.Apps {
		if app = strings.TrimSpace(app); app != "" {
			// 白名单按去掉查询串后的 app 比较，和 AppAllowed 保持一致。
			a.apps[appBase(app)] = struct{}{}
		}
	}
	for _, perm := range a.tokens {
		if perm&permPublish != 0 {
			a.publish = true
		}
		if perm&permPlay != 0 {
			a.play = true
		}
	}
	return a
}

// Enabled 报告是否配置了任何鉴权规则。nil 表示未启用。
func (a *Auth) Enabled() bool {
	return a != nil && (len(a.tokens) > 0 || len(a.apps) > 0)
}

// AppAllowed 判断 connect 的 app 是否在白名单内。
// 未配置白名单时恒为 true；比较时忽略 app 的查询串。
func (a *Auth) AppAllowed(app string) bool {
	if a == nil || len(a.apps) == 0 {
		return true
	}
	_, ok := a.apps[appBase(app)]
	return ok
}

// Allow 判断一次 publish/play 是否放行。need 是 permPublish 或 permPlay。
// rawStreamName 是命令里原样的流名（可能带 ?token=...）。
// 某方向未配置 token 时不校验该方向。
func (a *Auth) Allow(rawStreamName, app string, need tokenPerm) bool {
	if a == nil {
		return true
	}
	if need == permPublish && !a.publish {
		return true
	}
	if need == permPlay && !a.play {
		return true
	}
	tok := tokenFromQuery(rawStreamName)
	if tok == "" {
		// 少数客户端把 token 放在 app 的查询串里，作为兜底。
		tok = tokenFromQuery(app)
	}
	if tok == "" {
		return false
	}
	perm, ok := a.tokens[sha256.Sum256([]byte(tok))]
	return ok && perm&need != 0
}

// tokenFromQuery 从字符串的查询串里取 token，兼容 key 别名。
// 例如 "push?token=abc"、"push?key=abc&x=1"。
func tokenFromQuery(s string) string {
	i := strings.IndexByte(s, '?')
	if i < 0 || i == len(s)-1 {
		return ""
	}
	q, err := url.ParseQuery(s[i+1:])
	if err != nil {
		return ""
	}
	if tok := q.Get("token"); tok != "" {
		return tok
	}
	return q.Get("key")
}

// appBase 去掉 app 里的查询串。用于白名单匹配和日志，避免把放在 app 上的
// token 打进日志。
func appBase(app string) string {
	base, _, _ := strings.Cut(app, "?")
	return base
}

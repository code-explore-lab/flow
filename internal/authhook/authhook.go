// Package authhook 把推/拉鉴权委托给外部 HTTP 服务（O-3）。
//
// 协议侧只在“建立推流/拉流”这一次调用 Authorize：RTSP 在 ANNOUNCE /
// DESCRIBE，RTMP 在 publish / play。判定结果按请求特征缓存一段时间，
// 避免每个连接都打一次 hook；hook 超时或返回异常时按配置的默认策略
// （默认拒绝）处理，且失败结果不写入缓存，便于服务恢复后立即生效。
//
// HTTP 协议：
//
//	POST <url>
//	Content-Type: application/json
//	X-Flow-Signature: <hex HMAC-SHA256(secret, body)>   // 配置 secret 时
//
//	{"action":"play","protocol":"rtsp","app":"","stream":"live/cam1",
//	 "ip":"1.2.3.4","user":"alice","token":"...","method":"DESCRIBE"}
//
//	200 + {"allow":true}   放行（空 body 也按放行）
//	200 + {"allow":false}  拒绝
//	403 / 401              拒绝
//	其它状态 / 超时 / 故障  按 DefaultAllow
//
// 响应可以带 {"ttl":"30s"} 覆盖客户端配置的缓存时长；"0s" 表示这次判定
// 不缓存。token 只参与哈希，不会写进日志或错误信息。
package authhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultTimeout 是单次 hook 调用的默认超时。
	DefaultTimeout = 2 * time.Second
	// DefaultCacheTTL 是判定结果的默认缓存时长。
	DefaultCacheTTL = 10 * time.Second
	// DefaultMaxEntries 是缓存条目上限，防止陌生 IP/流名把内存撑大。
	DefaultMaxEntries = 10000

	// maxResponseBytes 是 hook 响应体上限。响应只应该是一个很小的 JSON。
	maxResponseBytes = 4096
)

// Request 是一次鉴权请求。字段会被序列化成 JSON 发给 hook。
type Request struct {
	Action   string `json:"action"`           // publish / play
	Protocol string `json:"protocol"`         // rtsp / rtmp
	App      string `json:"app,omitempty"`    // RTMP app；RTSP 为空
	Stream   string `json:"stream"`           // 流名/路径，不含查询串
	IP       string `json:"ip"`               // 客户端来源 IP
	User     string `json:"user,omitempty"`   // RTSP 认证用户名（Digest 只有用户名）
	Token    string `json:"token,omitempty"`  // 流名/URI 查询串里的 token
	Method   string `json:"method,omitempty"` // ANNOUNCE / DESCRIBE / publish / play
	URI      string `json:"uri,omitempty"`    // 原始请求 URI（仅 RTSP）
}

// Config 是构造 Client 的配置。
type Config struct {
	URL          string        // hook 地址；空表示不启用
	Timeout      time.Duration // 单次调用超时；<=0 用 DefaultTimeout
	CacheTTL     time.Duration // 判定缓存时长；0 表示不缓存
	DefaultAllow bool          // 超时/故障时是否放行（false = 拒绝）
	Secret       string        // 非空时给请求体加 HMAC-SHA256 签名
	MaxEntries   int           // 缓存条目上限；<=0 用 DefaultMaxEntries
}

type cacheEntry struct {
	allow   bool
	expires time.Time
}

// flight 表示一个正在进行的 hook 调用，用于合并同 key 的并发请求。
type flight struct {
	done  chan struct{}
	allow bool
	err   error
}

// Client 是并发安全的鉴权 hook 客户端。nil 或 URL 为空时所有请求放行，
// 与未启用该特性完全一致。
type Client struct {
	url          string
	http         *http.Client
	timeout      time.Duration
	ttl          time.Duration
	defaultAllow bool
	secret       []byte
	maxEntries   int

	mu      sync.Mutex
	cache   map[[sha256.Size]byte]*cacheEntry
	flights map[[sha256.Size]byte]*flight
}

// New 构造 hook 客户端。URL 为空返回一个不启用的 Client（不报错）。
func New(cfg Config) (*Client, error) {
	rawURL := strings.TrimSpace(cfg.URL)
	if rawURL == "" {
		return &Client{}, nil
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("auth hook: invalid URL %q: %w", rawURL, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("auth hook: URL %q must use http or https", rawURL)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("auth hook: URL %q has no host", rawURL)
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	ttl := cfg.CacheTTL
	if ttl < 0 {
		ttl = 0
	}
	maxEntries := cfg.MaxEntries
	if maxEntries <= 0 {
		maxEntries = DefaultMaxEntries
	}

	return &Client{
		url:          rawURL,
		http:         &http.Client{Timeout: timeout},
		timeout:      timeout,
		ttl:          ttl,
		defaultAllow: cfg.DefaultAllow,
		secret:       []byte(cfg.Secret),
		maxEntries:   maxEntries,
		cache:        make(map[[sha256.Size]byte]*cacheEntry),
		flights:      make(map[[sha256.Size]byte]*flight),
	}, nil
}

// Enabled 报告 hook 是否配置。nil Client 视为未启用。
func (c *Client) Enabled() bool {
	return c != nil && c.url != ""
}

// Authorize 判断一次推/拉是否放行。
//
// 返回的 bool 已经应用了超时/故障时的默认策略；err 非空表示本次判定
// 来自 fallback（调用方可以据此打日志），此时结果不会被缓存。
func (c *Client) Authorize(ctx context.Context, r Request) (bool, error) {
	if !c.Enabled() {
		return true, nil
	}
	if err := ctx.Err(); err != nil {
		return c.defaultAllow, err
	}

	key := cacheKey(r)
	now := time.Now()

	c.mu.Lock()
	if e := c.cache[key]; e != nil {
		if now.Before(e.expires) {
			c.mu.Unlock()
			return e.allow, nil
		}
		delete(c.cache, key)
	}
	if f := c.flights[key]; f != nil {
		c.mu.Unlock()
		select {
		case <-f.done:
			return f.allow, f.err
		case <-ctx.Done():
			return c.defaultAllow, ctx.Err()
		}
	}
	f := &flight{done: make(chan struct{})}
	c.flights[key] = f
	c.mu.Unlock()

	allow, ttl, err := c.fetch(ctx, r)

	c.mu.Lock()
	delete(c.flights, key)
	if err == nil && ttl > 0 {
		c.storeLocked(key, allow, time.Now().Add(ttl))
	}
	c.mu.Unlock()

	// 等待同一个 key 的调用方必须拿到和发起方一致的最终判定：
	// hook 失败时是默认策略，而不是 fetch 的零值。
	if err != nil {
		f.allow, f.err = c.defaultAllow, err
		close(f.done)
		return c.defaultAllow, err
	}
	f.allow, f.err = allow, nil
	close(f.done)
	return allow, nil
}

// storeLocked 写入缓存；满时先清过期项，仍满则随机淘汰一条。
func (c *Client) storeLocked(key [sha256.Size]byte, allow bool, expires time.Time) {
	if len(c.cache) >= c.maxEntries {
		now := time.Now()
		for k, e := range c.cache {
			if !now.Before(e.expires) {
				delete(c.cache, k)
			}
		}
		if len(c.cache) >= c.maxEntries {
			for k := range c.cache {
				delete(c.cache, k)
				break
			}
		}
	}
	c.cache[key] = &cacheEntry{allow: allow, expires: expires}
}

// fetch 调用一次 hook。返回判定、本次判定的缓存时长和传输错误。
func (c *Client) fetch(ctx context.Context, r Request) (bool, time.Duration, error) {
	body, err := json.Marshal(r)
	if err != nil {
		return false, 0, fmt.Errorf("auth hook: encode request: %w", err)
	}

	callCtx := ctx
	cancel := func() {}
	if c.timeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, c.timeout)
	}
	defer cancel()

	req, err := http.NewRequestWithContext(callCtx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return false, 0, fmt.Errorf("auth hook: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if len(c.secret) > 0 {
		mac := hmac.New(sha256.New, c.secret)
		mac.Write(body)
		req.Header.Set("X-Flow-Signature", hex.EncodeToString(mac.Sum(nil)))
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return false, 0, fmt.Errorf("auth hook: request failed: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		allow, ttl := c.parseBody(raw)
		return allow, ttl, nil
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return false, c.ttl, nil
	default:
		return false, 0, fmt.Errorf("auth hook: unexpected status %d", resp.StatusCode)
	}
}

// parseBody 解析 2xx 响应。空 body 视为放行；allow 缺省时也按放行。
// ttl 非法时忽略，沿用客户端配置。
func (c *Client) parseBody(raw []byte) (bool, time.Duration) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return true, c.ttl
	}
	var out struct {
		Allow *bool  `json:"allow"`
		TTL   string `json:"ttl"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		// 2xx 但 body 不是约定 JSON：按放行处理，避免把 hook 的输出格式
		// 问题变成生产事故。ttl 仍用默认值。
		return true, c.ttl
	}
	ttl := c.ttl
	if out.TTL != "" {
		if d, err := time.ParseDuration(out.TTL); err == nil && d >= 0 {
			ttl = d
		}
	}
	if out.Allow == nil {
		return true, ttl
	}
	return *out.Allow, ttl
}

// cacheKey 把请求特征压成 32 字节。token 只进入哈希，不保留明文。
// 方法/URI 不参与 key，因为同一次推/拉的后续请求应该命中缓存。
func cacheKey(r Request) [sha256.Size]byte {
	h := sha256.New()
	for _, s := range []string{r.Action, r.Protocol, r.App, r.Stream, r.IP, r.User, r.Token} {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	var key [sha256.Size]byte
	copy(key[:], h.Sum(nil))
	return key
}

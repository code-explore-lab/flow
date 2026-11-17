// Package access 提供 E-8 的访问控制与限流原语：
//
//   - Whitelist：按 CIDR 过滤来源 IP（白名单外的连接直接拒绝）；
//   - RateLimiter：每 IP 建连速率的令牌桶；
//   - Backoff：失败认证的指数退避（tarpit），拖慢暴力破解。
//
// 三者都只依赖标准库，所有方法对 nil 接收者安全：没配置访问控制的测试
// 服务器（或未启用某项功能）不会 panic，也不会误拒连接。
package access

import (
	"bufio"
	"fmt"
	"net/netip"
	"os"
	"strings"
	"sync"
	"time"
)

// Whitelist 一组 CIDR 前缀。enabled=false 表示不启用白名单（放行所有地址），
// 因此空文件/未配置时行为与没有这个特性完全一致。
type Whitelist struct {
	prefixes []netip.Prefix
	enabled  bool
}

// ParseWhitelistFile 从文本文件解析白名单：每行一个 CIDR（如 10.0.0.0/8、
// 192.168.1.0/24、::1/128）。以 # 开头的行和空行会被跳过。
// path 为空返回未启用的白名单；文件读失败或某行不是合法 CIDR 时返回错误。
func ParseWhitelistFile(path string) (*Whitelist, error) {
	if strings.TrimSpace(path) == "" {
		return &Whitelist{}, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open whitelist %s: %w", path, err)
	}
	defer f.Close()

	w := &Whitelist{}
	sc := bufio.NewScanner(f)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p, err := netip.ParsePrefix(line)
		if err != nil {
			return nil, fmt.Errorf("whitelist %s line %d: %q is not a CIDR: %w", path, lineNo, line, err)
		}
		w.prefixes = append(w.prefixes, p.Masked())
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read whitelist %s: %w", path, err)
	}
	w.enabled = len(w.prefixes) > 0
	return w, nil
}

// Enabled 报告白名单是否生效（至少有一条 CIDR）。
func (w *Whitelist) Enabled() bool {
	return w != nil && w.enabled
}

// Allows 报告 ipStr 是否在白名单内。未启用时放行所有地址；
// 启用后无法解析的地址按拒绝处理（宁可误拒，不误放）。
func (w *Whitelist) Allows(ipStr string) bool {
	if w == nil || !w.enabled {
		return true
	}
	addr, err := netip.ParseAddr(ipStr)
	if err != nil {
		return false
	}
	// ::ffff:1.2.3.4 归一成 1.2.3.4，才能匹配 IPv4 前缀。
	addr = addr.Unmap()
	for _, p := range w.prefixes {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

// maxIdleAge 是 RateLimiter 中空闲桶被惰性清理的阈值。
const maxIdleAge = 5 * time.Minute

// RateLimiter 每 IP 建连速率的令牌桶。rate 是每秒补充的令牌数（即每秒
// 允许新建的连接数），burst 是桶容量（允许的瞬时突发）。rate<=0 或
// burst<=0 表示不限速。
type RateLimiter struct {
	mu          sync.Mutex
	rate        float64
	burst       int
	buckets     map[string]*tokenBucket
	lastCleanup time.Time
}

type tokenBucket struct {
	tokens float64
	last   time.Time
}

// NewRateLimiter 构造一个令牌桶限速器。rate 或 burst 非正时返回不限速的实例。
func NewRateLimiter(rate float64, burst int) *RateLimiter {
	return &RateLimiter{
		rate:    rate,
		burst:   burst,
		buckets: make(map[string]*tokenBucket),
	}
}

// Allow 消耗一个令牌并返回是否放行。nil / 未配置 / 未启用时全部放行。
func (r *RateLimiter) Allow(ip string) bool {
	if r == nil || r.rate <= 0 || r.burst <= 0 {
		return true
	}
	now := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()

	// 惰性清理：每分钟最多扫一次，删掉长时间空闲的桶，避免陌生 IP 把
	// map 撑大。
	if now.Sub(r.lastCleanup) > time.Minute {
		for k, b := range r.buckets {
			if now.Sub(b.last) > maxIdleAge {
				delete(r.buckets, k)
			}
		}
		r.lastCleanup = now
	}

	b := r.buckets[ip]
	if b == nil {
		b = &tokenBucket{tokens: float64(r.burst), last: now}
		r.buckets[ip] = b
	} else {
		// 按流逝时间补令牌，封顶到 burst。
		if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
			b.tokens += elapsed * r.rate
			if b.tokens > float64(r.burst) {
				b.tokens = float64(r.burst)
			}
		}
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Backoff 失败认证的指数退避。每个 IP 独立计数：连续失败达到 threshold 次后，
// Fail 返回 0、base、2*base、4*base…（封顶 max），调用方在回应前 sleep
// 这段时长即可拖慢暴力破解；认证成功调用 Success 清零。
type Backoff struct {
	mu        sync.Mutex
	threshold int
	base      time.Duration
	max       time.Duration
	fails     map[string]int
	enabled   bool
}

// NewBackoff 构造失败认证退避。threshold<=0 或 base<=0 表示关闭退避；
// max<=0 或 max<base 时以 base 为上限。
func NewBackoff(threshold int, base, max time.Duration) *Backoff {
	if max < base {
		max = base
	}
	return &Backoff{
		threshold: threshold,
		base:      base,
		max:       max,
		fails:     make(map[string]int),
		enabled:   threshold > 0 && base > 0,
	}
}

// Fail 记录一次认证失败，返回本次应退避的时长。未启用、或失败次数未达
// 阈值时返回 0（允许正常手滑几次）。
func (b *Backoff) Fail(ip string) time.Duration {
	if b == nil || !b.enabled {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	b.fails[ip]++
	n := b.fails[ip]
	if n <= b.threshold {
		return 0
	}
	// 第 threshold+1 次失败返回 base，之后每次翻倍，封顶 max。
	d := b.base
	for i := 0; i < n-b.threshold-1; i++ {
		if d >= b.max {
			return b.max
		}
		d *= 2
	}
	if d > b.max {
		return b.max
	}
	return d
}

// Success 清除该 IP 的失败计数（认证成功后调用）。
func (b *Backoff) Success(ip string) {
	if b == nil || !b.enabled {
		return
	}
	b.mu.Lock()
	delete(b.fails, ip)
	b.mu.Unlock()
}

// Delay 返回该 IP 当前应退避的时长（不增加计数），用于观测。
func (b *Backoff) Delay(ip string) time.Duration {
	if b == nil || !b.enabled {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	n := b.fails[ip]
	if n <= b.threshold {
		return 0
	}
	d := b.base
	for i := 0; i < n-b.threshold-1; i++ {
		if d >= b.max {
			return b.max
		}
		d *= 2
	}
	if d > b.max {
		return b.max
	}
	return d
}

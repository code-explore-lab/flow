// Package limiter 提供按并发连接数限流的简单计数器（E-5）。
//
// 每个协议各自持有一个 ConnLimiter，因此 -max-conns / -max-ip-conns
// 对 RTSP 与 RTMP 独立生效。
package limiter

import "sync"

// ConnLimiter 统计当前连接总数与每个来源 IP 的连接数。
// maxTotal / maxPerIP 为 0 表示不限制。
type ConnLimiter struct {
	mu       sync.Mutex
	total    int
	perIP    map[string]int
	maxTotal int
	maxPerIP int
}

// New 构造一个连接限流器。负数按 0（不限制）处理。
func New(maxTotal, maxPerIP int) *ConnLimiter {
	if maxTotal < 0 {
		maxTotal = 0
	}
	if maxPerIP < 0 {
		maxPerIP = 0
	}
	return &ConnLimiter{
		perIP:    make(map[string]int),
		maxTotal: maxTotal,
		maxPerIP: maxPerIP,
	}
}

// Allow 检查是否还能接受来自 ip 的新连接。允许时占用一个名额并返回 true；
// 超限时返回 false 和人类可读的原因。nil 接收者视为不限制。
func (l *ConnLimiter) Allow(ip string) (bool, string) {
	if l == nil {
		return true, ""
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.maxTotal > 0 && l.total >= l.maxTotal {
		return false, "max conns reached"
	}
	if l.maxPerIP > 0 && l.perIP[ip] >= l.maxPerIP {
		return false, "max conns per ip reached"
	}
	l.total++
	l.perIP[ip]++
	return true, ""
}

// Release 释放一个由 Allow 占用的名额。必须与成功的 Allow 成对调用。
func (l *ConnLimiter) Release(ip string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.total > 0 {
		l.total--
	}
	if n := l.perIP[ip]; n > 1 {
		l.perIP[ip] = n - 1
	} else {
		delete(l.perIP, ip)
	}
}

// Count 返回当前占用的总名额。
func (l *ConnLimiter) Count() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.total
}

// PerIP 返回当前某个来源 IP 占用的名额。
func (l *ConnLimiter) PerIP(ip string) int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.perIP[ip]
}

package stream

// S-3：本地注册表的空闲回收。
//
// 推流端正常断开走 TEARDOWN / TCP close，注册表会立刻摘除；但进程被强杀、
// 半开 TCP、网络分区时对端不会发 FIN，协议层的空闲超时也可能还没到。此时
// 流还在注册表里，下游会一直等帧。Janitor 按「最近一次媒体写入」判断死活，
// 超过 idle 没有新帧就 Unpublish（关闭帧源 → 下游 EOF），不再依赖控制连接
// 的状态。
//
// 回源（S-1）和 -pull（T-9）的流通过 SetIdleExempt 豁免：它们断线时本地
// 流是故意保留的，由各自的指数退避重连负责恢复。

import (
	"context"
	"fmt"
	"time"
)

const (
	// DefaultIdleTimeout 是 Janitor 默认的媒体空闲阈值。必须明显大于
	// 常见关键帧间隔（低帧率直播、纯音频流），否则会误杀正常推流。
	DefaultIdleTimeout = 30 * time.Second

	janitorMinInterval = time.Second
	janitorMaxInterval = 5 * time.Second
)

// Janitor 周期性扫描注册表，回收长时间没有媒体写入的流。
type Janitor struct {
	reg      *Registry
	idle     time.Duration
	interval time.Duration
	logger   func(format string, args ...any)
}

// NewJanitor 构造回收器。idle <= 0 时使用 DefaultIdleTimeout。
// 扫描间隔取 idle/3，并限制在 1s~5s：太小会频繁遍历，太大会拖慢回收。
func NewJanitor(reg *Registry, idle time.Duration) *Janitor {
	if idle <= 0 {
		idle = DefaultIdleTimeout
	}
	interval := idle / 3
	if interval < janitorMinInterval {
		interval = janitorMinInterval
	}
	if interval > janitorMaxInterval {
		interval = janitorMaxInterval
	}
	return &Janitor{
		reg:      reg,
		idle:     idle,
		interval: interval,
		logger:   func(format string, args ...any) { fmt.Printf(format, args...) },
	}
}

// Idle 返回生效的空闲阈值（测试/日志用）。
func (j *Janitor) Idle() time.Duration { return j.idle }

// Run 阻塞运行直到 ctx 取消。
func (j *Janitor) Run(ctx context.Context) {
	if j == nil || j.reg == nil {
		return
	}
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			j.SweepOnce()
		}
	}
}

// SweepOnce 扫描一次并返回被回收的流名（按流名顺序）。
// 判断和摘除由 Registry.Reap 在同一把锁内完成，避免与 Publish 竞态。
func (j *Janitor) SweepOnce() []string {
	if j == nil || j.reg == nil {
		return nil
	}
	var reaped []string
	for _, ns := range j.reg.Snapshot() {
		if ns.Stream.IdleExempt() || ns.Stream.Active(j.idle) {
			continue
		}
		if j.reg.Reap(ns.Name, ns.Stream, j.idle) {
			reaped = append(reaped, ns.Name)
			if j.logger != nil {
				j.logger("stream janitor: dropped idle stream %q (no media for %s)", ns.Name, j.idle)
			}
		}
	}
	return reaped
}

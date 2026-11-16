package rtsp

import (
	"fmt"
	"sync"
	"time"
)

// C-6：反馈驱动降级。
//
// 纯转发场景没有编码器，服务端改不了码率、QP 或 GOP。能做的是根据客户端
// RR 里的区间丢包率（fraction lost）在发送侧丢帧：持续高丢包时只发
// 参数集和 IDR，跳过非关键帧，从而降低码率；丢包回落后自动恢复。
//
// 丢 P 帧会破坏参考链，客户端在下一个 IDR 之前可能出现卡顿/花屏，
// 但 RTSP/RTP 本身不断流。这是没有任何转码能力时的固有限制，
// 详见 docs/rtsp.md 的 C-6 说明。
type degradeConfig struct {
	LossThreshold float64       // 区间丢包率阈值，0~1
	Sustain       time.Duration // 连续高丢包多久后降级
	Recover       time.Duration // 连续低丢包多久后恢复
}

var defaultDegradeConfig = degradeConfig{
	LossThreshold: 0.10,
	Sustain:       3 * time.Second,
	Recover:       5 * time.Second,
}

// valid 判断配置是否可用。阈值必须落在 (0,1]，两个窗口都必须为正。
func (c degradeConfig) valid() bool {
	return c.LossThreshold > 0 && c.LossThreshold <= 1 &&
		c.Sustain > 0 && c.Recover > 0
}

// degradeState 是一条轨的降级状态机。
//
// 用 RR 的区间丢包率而不是累计丢包率：累计丢包只增不减，高丢包过去后
// 也永远不会回落。进入和退出都用时间窗口（而不是单次采样），避免一次
// 抖动就切模式；退出阈值通过 Recover 窗口天然形成迟滞。
type degradeState struct {
	mu        sync.Mutex
	cfg       degradeConfig
	highSince time.Time // 本轮连续高丢包的起点
	lowSince  time.Time // 本轮连续低丢包的起点
	on        bool
	dropped   uint64
}

func newDegradeState(cfg degradeConfig) *degradeState {
	return &degradeState{cfg: cfg}
}

// observe 用一条 RR 的区间丢包率更新状态，返回本次是否发生模式切换。
// at 是 RR 到达服务端的时刻，测试可以注入假时间。
func (d *degradeState) observe(loss float64, at time.Time) (entered, exited bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if loss > d.cfg.LossThreshold {
		d.lowSince = time.Time{}
		if d.highSince.IsZero() {
			d.highSince = at
		}
		if !d.on && at.Sub(d.highSince) >= d.cfg.Sustain {
			d.on = true
			return true, false
		}
		return false, false
	}

	d.highSince = time.Time{}
	if d.lowSince.IsZero() {
		d.lowSince = at
	}
	if d.on && at.Sub(d.lowSince) >= d.cfg.Recover {
		d.on = false
		return false, true
	}
	return false, false
}

// active 报告当前是否处于降级状态。nil 状态（推流/组播轨）视为不降级。
func (d *degradeState) active() bool {
	if d == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.on
}

// noteDrop 记录一个被跳过的非关键帧，返回累计丢弃数（用于节流日志）。
func (d *degradeState) noteDrop() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dropped++
	return d.dropped
}

// dropNonKeyFrame 判断当前是否应该跳过这个非关键帧 AU。
// 只在降级状态下返回 true，并计入丢弃统计。
func (t *mediaTrack) dropNonKeyFrame() bool {
	if t.degrade == nil || !t.degrade.active() {
		return false
	}
	n := t.degrade.noteDrop()
	if n == 1 || n%100 == 0 {
		// 每 100 帧打一行，避免 25fps 下刷屏。
		fmt.Printf("degrade (%s): dropped %d non-key frame(s)\n", t.kind, n)
	}
	return true
}

package origin

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"flow/internal/stream"
)

// 重连退避。握手成功过（played）后重置到 min，避免长时间在线后仍沿用大退避。
const (
	defaultMinBackoff = time.Second
	defaultMaxBackoff = 30 * time.Second
)

// Puller 一路按需回源的生命周期。
//
// 一个 Puller 只对应一个本地 *stream.Stream：第一次收到媒体（Ready）时把
// 它登记到注册表，之后上游断开只在后台重连，不关闭也不替换本地流（下游
// 阻塞在 Reader.Next 上，重连后继续收帧）。只有 Manager 的 ctx 取消时才
// 注销并关闭，下游读到流结束。
//
// 所有字段中的 st/ready/failed 在构造后不再替换，protocol 状态时钟也只由
// run 所在的 goroutine 访问，因此只有 ready/failed 的关闭需要 Once。
type Puller struct {
	mgr   *Manager
	name  string // 本地流名
	specs []Spec // 该流适用的模板，按显式 → 通配排序

	st     *stream.Stream
	ready  chan struct{}
	failed chan struct{}

	readyOnce sync.Once
	failOnce  sync.Once

	published bool // 只在 run goroutine 内读写

	// 每协议的时间戳平移器：每条上游连接重置一次，跨重连保持单调。
	rtmpClock tsClock
	httpClock tsClock
	rtspVideo tsClock
	rtspAudio tsClock
}

func newPuller(m *Manager, name string, specs []Spec) *Puller {
	st := &stream.Stream{Name: name}
	// S-3：回源流由 puller 自己的退避重连维护，上游断线期间下游仍要
	// 继续等帧，不能被本地 janitor 当空闲流摘掉。
	st.SetIdleExempt(true)
	return &Puller{
		mgr:    m,
		name:   name,
		specs:  specs,
		st:     st,
		ready:  make(chan struct{}),
		failed: make(chan struct{}),
	}
}

// Stream 返回本地共享流。Ready 之前也非 nil，但此时还没有轨道/参数。
func (p *Puller) Stream() *stream.Stream { return p.st }

// onFirst 在首个媒体帧写出后登记本地流并放行等待者。
func (p *Puller) onFirst() {
	p.readyOnce.Do(func() {
		if _, err := p.mgr.registry.Publish(p.name, p.st); err != nil {
			fmt.Printf("origin %q: publish local stream: %v\n", p.name, err)
		} else {
			p.published = true
			p.mgr.metrics.IncPublish()
			p.mgr.metrics.IncPublishTotal()
			fmt.Printf("origin %q: relaying from upstream\n", p.name)
		}
		close(p.ready)
	})
}

// fail 表示上游明确没有这路流，等待者应回退，puller 也不再重试。
func (p *Puller) fail() {
	p.failOnce.Do(func() { close(p.failed) })
}

// run 是 puller 的主循环，由 Manager.acquire 启动。
func (p *Puller) run(ctx context.Context) {
	defer p.mgr.remove(p)
	defer func() {
		if p.published {
			p.mgr.metrics.DecPublish()
			p.mgr.registry.Unpublish(p.name, p.st)
			fmt.Printf("origin %q: stopped relaying\n", p.name)
			return
		}
		p.st.Close()
		fmt.Printf("origin %q: gave up (never ready)\n", p.name)
	}()

	backoff := p.mgr.minBackoff
	for {
		if ctx.Err() != nil {
			return
		}
		// 本地同名被其它推流按 replace 策略抢占时重新登记回来。仍是我们
		// 自己时不重复 Publish，避免无意义地关闭其它流或刷日志。
		if p.published && p.mgr.registry.Get(p.name) != p.st {
			if _, err := p.mgr.registry.Publish(p.name, p.st); err != nil {
				fmt.Printf("origin %q: re-publish local stream: %v\n", p.name, err)
			}
		}

		played, err := p.tryOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		// 「上游没有这路流」只在首次回源时视为终态：请求方应该立即回退到
		// 404 / 本地文件。已经 Ready 过之后就不一样了——源站重启/换片期间
		// 会短暂地 play 失败，此时下游还在播，必须按退避继续重连，直到
		// 进程 ctx 取消（与 -pull 的透明续传语义一致）。
		if errors.Is(err, ErrNotFound) && !p.published {
			fmt.Printf("origin %q: upstream stream not found; not retrying\n", p.name)
			p.fail()
			return
		}
		if played {
			backoff = p.mgr.minBackoff
		}
		if err != nil {
			fmt.Printf("origin %q: %v (reconnect in %s)\n", p.name, err, backoff)
		} else {
			fmt.Printf("origin %q: upstream ended (reconnect in %s)\n", p.name, backoff)
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < p.mgr.maxBackoff {
			backoff *= 2
			if backoff > p.mgr.maxBackoff {
				backoff = p.mgr.maxBackoff
			}
		}
	}
}

// tryOnce 按模板顺序尝试一次。任一模板成功播过（played）就直接返回；
// 全部失败时返回最后一个错误，"不存在" 信号优先保留。
func (p *Puller) tryOnce(ctx context.Context) (bool, error) {
	var lastErr error
	for _, spec := range p.specs {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		played, err := p.dial(ctx, spec)
		if played || err == nil {
			return played, err
		}
		lastErr = err
	}
	if lastErr == nil {
		return false, ErrNotFound
	}
	return false, lastErr
}

// dial 解析 URL 并按 scheme 拉流。
func (p *Puller) dial(ctx context.Context, spec Spec) (bool, error) {
	raw := spec.urlFor(p.name)
	u, err := url.Parse(raw)
	if err != nil {
		return false, fmt.Errorf("parse %q: %w", raw, err)
	}
	switch spec.Scheme {
	case "rtmp":
		p.rtmpClock.newConn()
		return p.pullRTMP(ctx, u)
	case "rtsp":
		p.rtspVideo.newConn()
		p.rtspAudio.newConn()
		return p.pullRTSP(ctx, u)
	case "http":
		p.httpClock.newConn()
		return p.pullHTTP(ctx, raw)
	}
	return false, fmt.Errorf("unsupported scheme %q", spec.Scheme)
}

// tsClock 把上游时间戳平移成跨重连单调递增（同 rtmp.Client 的 rebaseClock）。
// 只有 pull 所在 goroutine 访问。
type tsClock struct {
	started bool
	base    uint32
	last    uint32 // 已见最大 PTS（RTSP 为 90kHz，RTMP/HTTP 为毫秒）
	minCTS  int32  // 已见最小 CTS（RTMP/HTTP），重连时留出同样的重排窗口
}

// newConn 标记进入新连接：下一帧重新计算基准。
func (c *tsClock) newConn() { c.started = false }

// apply 返回平移后的时间戳（RTSP：PTS 直接以 90kHz 到达，没有 CTS）。
func (c *tsClock) apply(raw uint32) uint32 {
	if !c.started {
		c.started = true
		if c.last > 0 {
			c.base = c.last + 1
		} else {
			c.base = 0
		}
	}
	v := raw + c.base
	if v > c.last {
		c.last = v
	}
	return v
}

// applyDTSCTS 平移 FLV 的 DTS，并按 PTS=DTS+CTS 维护历史最大值。
// 重连基准由首帧 DTS 与已见的最小 CTS 共同决定，保证新连接里任何一帧的
// PTS 都不落到上一连接最大值之前；只按 DTS 平移再加 CTS 会回退。
func (c *tsClock) applyDTSCTS(dts uint32, cts int32) uint32 {
	if cts < c.minCTS {
		c.minCTS = cts
	}
	if !c.started {
		c.started = true
		c.base = 0
		if c.last > 0 {
			// 首帧 DTS 是本连接最小 DTS；再留出 minCTS 的重排窗口。
			if shift := int64(c.last) + 1 - int64(dts) - int64(c.minCTS); shift > 0 {
				c.base = uint32(shift)
			}
		}
	}
	v := int64(dts) + int64(c.base)
	if v < 0 {
		v = 0
	}
	if p := v + int64(cts); p > int64(c.last) {
		c.last = uint32(p)
	}
	return uint32(v)
}

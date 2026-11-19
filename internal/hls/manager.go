package hls

import (
	"sync"
	"time"

	"flow/internal/metrics"
	"flow/internal/stream"
)

// Manager 按流名复用 packager：同名同 Stream 指针复用，指针变化时替换旧实例；
// 无观众访问超过 idle 后回收。
type Manager struct {
	opts Options
	idle time.Duration

	mu sync.Mutex
	m  map[string]*managed

	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

type managed struct {
	p      *Packager
	stream *stream.Stream
	last   time.Time
}

// NewManager 创建管理器。idle<=0 表示不自动回收（仍需调用 Close）。
func NewManager(opts Options, idle time.Duration) *Manager {
	opts = opts.withDefaults()
	m := &Manager{
		opts: opts,
		idle: idle,
		m:    make(map[string]*managed),
		stop: make(chan struct{}),
	}
	if idle > 0 {
		tick := idle / 2
		if tick < time.Second {
			tick = time.Second
		}
		if tick > 10*time.Second {
			tick = 10 * time.Second
		}
		m.wg.Add(1)
		go m.janitor(tick)
	}
	return m
}

// SetMetrics 注入 HLS 指标注册表（nil-safe）。应在创建 packager 前设置。
func (m *Manager) SetMetrics(reg *metrics.Registry) {
	m.mu.Lock()
	m.opts.Metrics = reg
	m.mu.Unlock()
}

func (m *Manager) janitor(tick time.Duration) {
	defer m.wg.Done()
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
			m.Sweep(m.opts.Now())
		}
	}
}

// Get 返回 name 对应的 packager；不存在时按 spec 创建。
// spec.Stream 指针变化表示同名流被重新发布，旧 packager 会被关闭。
func (m *Manager) Get(spec PackagerSpec) *Packager {
	for {
		now := m.opts.Now()
		m.mu.Lock()
		e := m.m[spec.Name]
		if e != nil && e.stream == spec.Stream {
			e.last = now
			p := e.p
			m.mu.Unlock()
			return p
		}
		if e != nil {
			delete(m.m, spec.Name)
			m.opts.Metrics.DecHLSPackager()
			m.mu.Unlock()
			m.opts.logf("hls[%s]: stream replaced, closing old packager", spec.Name)
			e.p.Close()
			continue
		}
		m.mu.Unlock()
		break
	}

	p, err := NewPackager(spec, m.opts)
	if err != nil {
		m.opts.logf("hls[%s]: create packager: %v", spec.Name, err)
		return nil
	}

	now := m.opts.Now()
	m.mu.Lock()
	if e := m.m[spec.Name]; e != nil {
		// 并发的另一个请求已经创建了；保留先创建的实例。
		same := e.stream == spec.Stream
		if same {
			e.last = now
		}
		m.mu.Unlock()
		p.Close()
		if same {
			return e.p
		}
		return m.Get(spec)
	}
	m.m[spec.Name] = &managed{p: p, stream: spec.Stream, last: now}
	m.opts.Metrics.IncHLSPackager()
	m.mu.Unlock()
	return p
}

// Sweep 回收超过 idle 没被访问的 packager。now 由调用方传入，便于测试。
func (m *Manager) Sweep(now time.Time) {
	if m.idle <= 0 {
		return
	}
	var expired []*Packager
	m.mu.Lock()
	for name, e := range m.m {
		// O-6：DVR 需要在没有请求时也持续切片，否则观众暂停几分钟后
		// 回看窗口就被清空了。源结束（Closed）后仍按 idle 回收。
		if m.opts.DVRWindow > 0 && !e.p.Closed() {
			continue
		}
		if now.Sub(e.last) >= m.idle {
			delete(m.m, name)
			m.opts.Metrics.DecHLSPackager()
			expired = append(expired, e.p)
		}
	}
	m.mu.Unlock()
	for _, p := range expired {
		m.opts.logf("hls[%s]: idle, closing packager", p.Name())
		p.Close()
	}
}

// Len 返回当前管理的流数量。
func (m *Manager) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.m)
}

// Close 停止 janitor 并关闭所有 packager。幂等。
func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		close(m.stop)
	})
	m.wg.Wait()

	m.mu.Lock()
	all := make([]*Packager, 0, len(m.m))
	for _, e := range m.m {
		all = append(all, e.p)
		m.opts.Metrics.DecHLSPackager()
	}
	m.m = make(map[string]*managed)
	m.mu.Unlock()
	for _, p := range all {
		p.Close()
	}
}

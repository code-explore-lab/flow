package transcode

import (
	"context"
	"sync"
	"time"

	"flow/internal/stream"
)

// Manager 按流名轮询注册表，为命中的源流维护转码器。
//
// 用轮询而不是注册表观察者的原因和 F-3 录制相同：观察者目前是单槽位，
// 而且 RTMP 的音轨要等首帧才建立，PUBLISH 时刻往往还拿不到 source。
type Manager struct {
	reg      *stream.Registry
	opts     Options
	profiles []Profile

	mu       sync.Mutex
	entries  map[string]*entry
	waiting  map[string]*waitState
	closed   bool
	closeOne sync.Once
}

type entry struct {
	tr     *Transcoder
	parent *stream.Stream
}

type waitState struct {
	parent   *stream.Stream
	since    time.Time
	retryAt  time.Time
	warnedAt time.Time
}

// NewManager 创建转码管理器。profiles 为空时不启用。
func NewManager(reg *stream.Registry, opts Options, profiles []Profile) *Manager {
	return &Manager{
		reg:      reg,
		opts:     opts.withDefaults(),
		profiles: profiles,
		entries:  make(map[string]*entry),
		waiting:  make(map[string]*waitState),
	}
}

// Enabled 报告是否配置了任何转码档位。
func (m *Manager) Enabled() bool { return len(m.profiles) > 0 }

// Run 阻塞轮询注册表，ctx 取消后关闭所有转码器。
func (m *Manager) Run(ctx context.Context) {
	if !m.Enabled() {
		<-ctx.Done()
		return
	}
	ticker := time.NewTicker(m.opts.Interval)
	defer ticker.Stop()
	m.sweep(ctx)
	for {
		select {
		case <-ctx.Done():
			m.Close()
			return
		case <-ticker.C:
			m.sweep(ctx)
		}
	}
}

// Close 停止所有转码器并等待收尾。可重复调用。
func (m *Manager) Close() {
	m.closeOne.Do(func() {
		m.mu.Lock()
		m.closed = true
		trs := make([]*Transcoder, 0, len(m.entries))
		for _, e := range m.entries {
			trs = append(trs, e.tr)
		}
		m.entries = make(map[string]*entry)
		m.waiting = make(map[string]*waitState)
		m.mu.Unlock()
		for _, tr := range trs {
			tr.Close()
		}
	})
}

func (m *Manager) sweep(ctx context.Context) {
	now := m.opts.Now()

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}

	// 上游消失、被替换或视频轨消失：停掉对应转码器。
	var closeNow []*Transcoder
	for out, e := range m.entries {
		if m.reg.Get(profilesSource(m.profiles, out)) != e.parent || e.parent.SourceFor("video") == nil {
			closeNow = append(closeNow, e.tr)
			delete(m.entries, out)
		}
	}

	type pending struct {
		profile Profile
		parent  *stream.Stream
	}
	var start []pending
	for _, p := range m.profiles {
		if _, ok := m.entries[p.Output]; ok {
			continue
		}
		parent := m.reg.Get(p.Source)
		if parent == nil || parent.SourceFor("video") == nil {
			delete(m.waiting, p.Output)
			continue
		}
		// 目标流名已被别的流占用（例如真实推流先到）：不要覆盖。
		if other := m.reg.Get(p.Output); other != nil && other != parent {
			w := m.waiter(p.Output, parent, now)
			if now.Sub(w.warnedAt) >= 30*time.Second {
				w.warnedAt = now
				m.opts.Logf("transcode[%s]: output %q already exists, waiting", p.Label(), p.Output)
			}
			continue
		}

		w := m.waiter(p.Output, parent, now)
		if now.Before(w.retryAt) {
			continue
		}
		// 等音轨出现（RTMP）或 joinDelay 到点，避免派生流只有视频。
		if parent.SourceFor("audio") == nil && now.Sub(w.since) < m.opts.JoinDelay {
			continue
		}
		delete(m.waiting, p.Output)
		start = append(start, pending{profile: p, parent: parent})
	}
	m.mu.Unlock()

	for _, tr := range closeNow {
		tr.Close()
	}

	for _, item := range start {
		p := item.profile
		tr, err := NewTranscoder(ctx, m.reg, item.parent, p, m.opts)
		if err != nil {
			m.mu.Lock()
			if m.reg.Get(p.Source) == item.parent {
				m.waiting[p.Output] = &waitState{
					parent:  item.parent,
					since:   now,
					retryAt: now.Add(5 * time.Second),
				}
			}
			m.mu.Unlock()
			m.opts.Logf("transcode[%s]: create: %v", p.Label(), err)
			continue
		}
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			tr.Close()
			return
		}
		m.entries[p.Output] = &entry{tr: tr, parent: tr.parent}
		m.mu.Unlock()
		tr.Start()
		m.opts.Logf("transcode[%s]: %dx%d %s %d bps", p.Label(), p.Width, p.Height, p.Codec, p.Bitrate)
	}
}

// waiter 返回输出档位的等待状态，父流变了就重新计时。
func (m *Manager) waiter(output string, parent *stream.Stream, now time.Time) *waitState {
	w := m.waiting[output]
	if w == nil || w.parent != parent {
		w = &waitState{parent: parent, since: now}
		m.waiting[output] = w
	}
	return w
}

func profilesSource(profiles []Profile, output string) string {
	for _, p := range profiles {
		if p.Output == output {
			return p.Source
		}
	}
	return ""
}

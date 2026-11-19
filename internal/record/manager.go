package record

import (
	"context"
	"strings"
	"sync"
	"time"

	"flow/internal/stream"
)

// Manager 按名字/通配符监听注册表，为命中的流创建 Recorder。
//
// 用轮询而不是注册表观察者：观察者目前是单槽位（S-3 发现上报在用），
// 而且 RTMP 的轨要等首帧才建立，PUBLISH 时刻往往还拿不到 source。
// 500ms 的轮询同时解决这两个问题，代价可以忽略。
type Manager struct {
	reg  *stream.Registry
	opts Options

	names map[string]bool
	all   bool

	joinDelay time.Duration // 等另一条轨出现的时间
	interval  time.Duration

	mu      sync.Mutex
	recs    map[string]*recEntry
	waiting map[string]*waitEntry
	closed  bool

	closeOnce sync.Once
}

type recEntry struct {
	rec       *Recorder
	st        *stream.Stream
	video, au bool // 创建时已存在的轨，用于发现后到的轨
}

type waitEntry struct {
	since time.Time
	st    *stream.Stream
}

// NewManager 创建录制管理器。names 为空表示不启用；包含 "*" 表示录制全部流。
func NewManager(reg *stream.Registry, opts Options, names []string) *Manager {
	opts = opts.withDefaults()
	m := &Manager{
		reg:       reg,
		opts:      opts,
		names:     make(map[string]bool),
		joinDelay: 500 * time.Millisecond,
		interval:  500 * time.Millisecond,
		recs:      make(map[string]*recEntry),
		waiting:   make(map[string]*waitEntry),
	}
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if name == "*" {
			m.all = true
			continue
		}
		m.names[name] = true
	}
	return m
}

// Enabled 报告是否配置了任何要录制的流。
func (m *Manager) Enabled() bool { return m.all || len(m.names) > 0 }

// Run 阻塞轮询注册表，ctx 取消后关闭所有录制器。
func (m *Manager) Run(ctx context.Context) {
	if !m.Enabled() {
		<-ctx.Done()
		return
	}
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	m.sweep()
	for {
		select {
		case <-ctx.Done():
			m.Close()
			return
		case <-ticker.C:
			m.sweep()
		}
	}
}

func (m *Manager) match(name string) bool {
	return m.all || m.names[name]
}

func (m *Manager) sweep() {
	now := m.opts.Now()
	seen := make(map[string]*stream.Stream)
	for _, ns := range m.reg.Snapshot() {
		if m.match(ns.Name) {
			seen[ns.Name] = ns.Stream
		}
	}

	var closeNow []*Recorder
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	for name, e := range m.recs {
		st, ok := seen[name]
		if !ok || st != e.st {
			closeNow = append(closeNow, e.rec)
			delete(m.recs, name)
			m.opts.logf("record[%s]: stream ended or replaced, finalizing", name)
			continue
		}
		// 录制开始后才出现的新轨（例如 RTMP 音频晚于视频）：换一个文件
		// 把两轨都录进去，旧文件保持可播放。
		hasVideo := st.SourceFor("video") != nil
		hasAudio := st.SourceFor("audio") != nil
		if (hasVideo && !e.video) || (hasAudio && !e.au) {
			closeNow = append(closeNow, e.rec)
			delete(m.recs, name)
			m.opts.logf("record[%s]: new track appeared, rotating file", name)
		}
	}
	for name, w := range m.waiting {
		st, ok := seen[name]
		if !ok || st != w.st {
			delete(m.waiting, name)
		}
	}
	m.mu.Unlock()

	for _, rec := range closeNow {
		rec.Close()
	}

	// 不持锁创建/启动 Recorder，避免 Close 等待一个还没启动的实例。
	for name, st := range seen {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return
		}
		if e := m.recs[name]; e != nil {
			// 录制器已经失败（磁盘满等）：等流重建或下线再重试。
			m.mu.Unlock()
			continue
		}
		hasVideo := st.SourceFor("video") != nil
		hasAudio := st.SourceFor("audio") != nil
		if !hasVideo && !hasAudio {
			m.mu.Unlock()
			continue
		}
		w := m.waiting[name]
		if w == nil || w.st != st {
			w = &waitEntry{since: now, st: st}
			m.waiting[name] = w
		}
		ready := (hasVideo && hasAudio) || now.Sub(w.since) >= m.joinDelay
		if !ready {
			m.mu.Unlock()
			continue
		}
		delete(m.waiting, name)
		m.mu.Unlock()

		if m.reg.Get(name) != st {
			continue
		}
		spec := SpecFromStream(st)
		if spec.Video == nil && spec.Audio == nil {
			continue
		}
		rec, err := NewRecorder(spec, m.opts)
		if err != nil {
			m.opts.logf("record[%s]: create recorder: %v", name, err)
			continue
		}
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			rec.Close()
			return
		}
		m.recs[name] = &recEntry{rec: rec, st: st, video: hasVideo, au: hasAudio}
		m.mu.Unlock()
		rec.Start()
	}
}

// Close 停止所有录制并等待文件收尾。可重复调用。
func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		recs := make([]*Recorder, 0, len(m.recs))
		for _, e := range m.recs {
			recs = append(recs, e.rec)
		}
		m.recs = make(map[string]*recEntry)
		m.waiting = make(map[string]*waitEntry)
		m.mu.Unlock()
		for _, rec := range recs {
			rec.Close()
		}
	})
}

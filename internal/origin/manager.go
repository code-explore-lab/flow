package origin

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"flow/internal/metrics"
	"flow/internal/stream"
)

// Manager 按 -origin 模板管理按需回源的 puller。
//
// 并发模型：Ensure 可以被任意协议的下游 goroutine 调用；同一条本地流
// 只启动一个 puller（按流名最后一段去重），后来者只等待 Ready。
type Manager struct {
	ctx      context.Context
	registry *stream.Registry
	metrics  *metrics.Registry
	specs    []Spec

	startTimeout time.Duration

	// 重连退避（测试里可调小）。
	minBackoff time.Duration
	maxBackoff time.Duration

	mu     sync.Mutex
	active map[string]*Puller // key: 流名最后一段
}

// NewManager 构造管理器。ctx 用于驱动 puller 的整个生命周期（通常是进程
// 的根 context）；startTimeout 是 Ensure 等待首帧的上限。
func NewManager(ctx context.Context, reg *stream.Registry, m *metrics.Registry, specs []Spec, startTimeout time.Duration) *Manager {
	if ctx == nil {
		ctx = context.Background()
	}
	if reg == nil {
		reg = stream.NewRegistry()
	}
	if m == nil {
		m = metrics.New()
	}
	if startTimeout <= 0 {
		startTimeout = 5 * time.Second
	}
	return &Manager{
		ctx:          ctx,
		registry:     reg,
		metrics:      m,
		specs:        specs,
		startTimeout: startTimeout,
		minBackoff:   defaultMinBackoff,
		maxBackoff:   defaultMaxBackoff,
		active:       make(map[string]*Puller),
	}
}

// Ensure 保证本地流 name 可拉：注册表已有就直接返回；匹配到模板则启动
// （或复用）puller，等待首帧到达。返回 nil 表示上游不可用，下游应走原有
// 的 404 / 本地文件回退。
func (m *Manager) Ensure(ctx context.Context, name string) *stream.Stream {
	if m == nil {
		return nil
	}
	name = strings.Trim(name, "/")
	if name == "" {
		return nil
	}
	if st := m.registry.Get(name); st != nil {
		return st
	}

	specs := m.matchSpecs(name)
	if len(specs) == 0 {
		return nil
	}
	p := m.acquire(name, specs)
	if p == nil {
		return nil
	}
	// 已经 Ready 时不必再进 select（ready/failed 同时关闭时避免随机选中 failed）。
	select {
	case <-p.ready:
		return p.Stream()
	default:
	}

	timer := time.NewTimer(m.startTimeout)
	defer timer.Stop()
	select {
	case <-p.ready:
		return p.Stream()
	case <-p.failed:
		return nil
	case <-timer.C:
		fmt.Printf("origin %q: no media within %s; falling back\n", name, m.startTimeout)
		return nil
	case <-ctx.Done():
		return nil
	}
}

// matchSpecs 返回适用于 name 的模板：显式映射优先，通配按配置顺序。
func (m *Manager) matchSpecs(name string) []Spec {
	leaf := streamLeaf(name)
	var explicit, wild []Spec
	for _, s := range m.specs {
		if s.Local != "" {
			if s.Local == name || s.Local == leaf {
				explicit = append(explicit, s)
			}
			continue
		}
		wild = append(wild, s)
	}
	if len(explicit) == 0 && len(wild) == 0 {
		return nil
	}
	return append(explicit, wild...)
}

// acquire 返回 name 对应的 puller，不存在时懒启动。同一个 leaf 只启动一次，
// 这样 RTSP 的 live/src 与 RTMP 的 src 会共用同一条上游拉流。
func (m *Manager) acquire(name string, specs []Spec) *Puller {
	key := streamLeaf(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	if p := m.active[key]; p != nil {
		return p
	}
	p := newPuller(m, name, specs)
	m.active[key] = p
	go p.run(m.ctx)
	return p
}

// remove 在 puller 退出时摘除。只有当前登记的仍是 p 时才删，避免旧 puller
// 的清理把重新创建的新 puller 摘掉。
func (m *Manager) remove(p *Puller) {
	m.mu.Lock()
	key := streamLeaf(p.name)
	if m.active[key] == p {
		delete(m.active, key)
	}
	m.mu.Unlock()
}

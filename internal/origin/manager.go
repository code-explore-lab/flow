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

// Locator 按本地流名查询可用的上游地址（S-2）。返回的 Spec 会排在静态
// -origin 模板之前，每次回源/重连都会重新调用，因此上游节点变化后能自动
// 切换。查不到时返回 nil，回源继续回退到静态模板。
type Locator func(ctx context.Context, name string) []Spec

// Manager 按 -origin 模板管理按需回源的 puller。
//
// 并发模型：Ensure 可以被任意协议的下游 goroutine 调用；同一条本地流
// 只启动一个 puller（按流名最后一段去重），后来者只等待 Ready。
type Manager struct {
	ctx      context.Context
	registry *stream.Registry
	metrics  *metrics.Registry
	specs    []Spec

	// S-2：动态上游解析。非 nil 时每次回源/重连都会调用，用于把发现服务
	// 给出的候选节点追加到静态模板之前；上游宕机后下一轮重连即可切换。
	resolver Locator

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

// SetLocator 注入动态上游解析（S-2）。必须在 Serve 之前调用。
func (m *Manager) SetLocator(l Locator) {
	if m == nil {
		return
	}
	m.resolver = l
}

// Ensure 保证本地流 name 可拉：注册表已有就直接返回；配置了发现服务或
// 匹配到模板则启动（或复用）puller，等待首帧到达。返回 nil 表示上游
// 不可用，下游应走原有的 404 / 本地文件回退。
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

	if !m.hasSources(name) {
		return nil
	}
	p := m.acquire(name)
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

// hasSources 报告 name 是否可能从某处拉到：配置了动态解析，或匹配到
// 任意静态模板。真正的可用性留到 puller 的首次尝试去判断。
func (m *Manager) hasSources(name string) bool {
	return m.resolver != nil || len(m.matchSpecs(name)) > 0
}

// specsFor 返回本次回源要尝试的地址：发现服务的结果在前（负载最低的
// 节点优先），静态 -origin 模板在后兜底。puller 每次重连都会重新调用，
// 所以上游节点变化后能自动切换。
func (m *Manager) specsFor(ctx context.Context, name string) []Spec {
	static := m.matchSpecs(name)
	if m.resolver == nil {
		return static
	}
	dyn := m.resolver(ctx, name)
	if len(dyn) == 0 {
		return static
	}
	return append(dyn, static...)
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
func (m *Manager) acquire(name string) *Puller {
	key := streamLeaf(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	if p := m.active[key]; p != nil {
		return p
	}
	p := newPuller(m, name)
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

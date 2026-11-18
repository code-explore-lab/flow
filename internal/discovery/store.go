// Package discovery 实现 S-3/S-2：动态源注册、发现与边缘分发。
//
// 节点把自己正在出帧的流登记到注册中心（带 TTL 租约），并周期性续约；
// 注册中心在租约过期后自动删除条目，因此源/节点下线后不需要显式清理。
// 边缘节点在本地没有某路流时查询注册中心：S-3 用 D-1 的 302 把客户端
// 指到真正持有该流的节点，S-2 也可以直接用发现结果作为回源上游（代理），
// 从而在上游宕机时由边缘节点自行切换，下游连接不中断。
//
// 同一路流允许有多个 provider：hops=0 的是推流源（同一时刻只允许一个，
// 防止双写），hops>0 的是边缘副本。查询时按负载、跳数排序，节点据此做
// 简单的负载均衡。
//
// 本包只依赖标准库和 internal/stream，不绑定具体注册中心实现：Store 是
// 进程内实现，后续可以替换成 etcd/Redis 后端而不影响客户端和上报逻辑。
package discovery

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// DefaultTTL 是租约默认有效期。心跳间隔通常取 TTL/3，连续丢两次心跳
// 租约才会过期，避免一次抖动就误删。
const DefaultTTL = 15 * time.Second

// Stream 是节点上报的一路流及其各协议的播放地址。
type Stream struct {
	Name string            `json:"name"`
	URLs map[string]string `json:"urls,omitempty"`

	// Hops 是从推流源算起的级联跳数：0 表示本报文来自推流源，
	// 1 表示从源节点拉流的一级边缘，依此类推。
	Hops int `json:"hops,omitempty"`
	// Upstream 是当前拉流的上游节点 ID；推流源为空。
	Upstream string `json:"upstream,omitempty"`
	// Load 是上报节点当前的活跃观看数，供选择器做负载均衡。
	Load int `json:"load,omitempty"`
}

// Entry 是注册中心里一条有效租约。
type Entry struct {
	Name      string            `json:"name"`
	Node      string            `json:"node"`
	URLs      map[string]string `json:"urls,omitempty"`
	ExpiresAt time.Time         `json:"expires_at"`

	Hops     int    `json:"hops,omitempty"`
	Upstream string `json:"upstream,omitempty"`
	Load     int    `json:"load,omitempty"`
}

// TTL 返回租约剩余时间（已过期时返回 0）。
func (e Entry) TTL(now time.Time) time.Duration {
	if !now.Before(e.ExpiresAt) {
		return 0
	}
	return e.ExpiresAt.Sub(now)
}

// URL 返回指定协议的可播放地址。
func (e Entry) URL(scheme string) string {
	return e.URLs[strings.ToLower(scheme)]
}

// BatchResult 是一次上报的处理结果。冲突表示同名流被另一个仍然存活的
// 推流源持有；上报方应停止广告该流，等对方租约过期后再重试。
// 边缘副本（Hops > 0）可以与源共存，不算冲突。
type BatchResult struct {
	Registered []string `json:"registered"`
	Conflicts  []string `json:"conflicts"`
	Rejected   []string `json:"rejected"`
}

type record struct {
	entry     Entry
	expiresAt time.Time
}

// Store 是进程内的租约表：流名 → 节点 ID → 租约。所有方法并发安全。
type Store struct {
	mu  sync.RWMutex
	m   map[string]map[string]*record
	ttl time.Duration
	now func() time.Time
}

// NewStore 创建租约表。ttl <= 0 时使用 DefaultTTL。
func NewStore(ttl time.Duration) *Store {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Store{
		m:   make(map[string]map[string]*record),
		ttl: ttl,
		now: time.Now,
	}
}

// DefaultTTL 返回 Store 的默认租约时长。
func (s *Store) DefaultTTL() time.Duration { return s.ttl }

// SetClock 替换时间源（单测用）。传 nil 恢复 time.Now。
func (s *Store) SetClock(fn func() time.Time) {
	if fn == nil {
		fn = time.Now
	}
	s.mu.Lock()
	s.now = fn
	s.mu.Unlock()
}

// RegisterBatch 处理一次心跳：整批替换 node 名下的租约集合。
//
// 批次里出现的流登记/续约；同一路流若已被另一个仍存活的推流源（双方
// Hops 都是 0）持有则返回冲突，不抢占。边缘副本（Hops > 0）允许与源
// 共存，用于负载均衡和级联分发。node 之前持有但本次批次没有的流立即
// 删除——这比等 TTL 过期更快，源主动下线时能在一次心跳内从注册中心消失。
func (s *Store) RegisterBatch(node string, streams []Stream, ttl time.Duration) BatchResult {
	node = strings.TrimSpace(node)
	res := BatchResult{}
	if node == "" {
		for _, st := range streams {
			res.Rejected = append(res.Rejected, st.Name)
		}
		return res
	}
	if ttl <= 0 {
		ttl = s.ttl
	}

	// 去重：同名以最后一次出现为准。
	byName := make(map[string]Stream, len(streams))
	for _, st := range streams {
		name := strings.Trim(strings.TrimSpace(st.Name), "/")
		if name == "" {
			res.Rejected = append(res.Rejected, st.Name)
			continue
		}
		st.Name = name
		if st.Hops < 0 {
			st.Hops = 0
		}
		st.Upstream = strings.TrimSpace(st.Upstream)
		if st.Load < 0 {
			st.Load = 0
		}
		byName[name] = st
	}

	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)

	now := s.now()
	expires := now.Add(ttl)
	keep := make(map[string]bool, len(names))

	s.mu.Lock()
	for _, name := range names {
		st := byName[name]
		urls := normalizeURLs(st.URLs)
		if len(urls) == 0 {
			res.Rejected = append(res.Rejected, name)
			continue
		}
		byNode := s.m[name]
		if byNode == nil {
			byNode = make(map[string]*record)
			s.m[name] = byNode
		}

		// 先清理过期租约，再判断是否存在仍然存活的推流源冲突。
		conflict := false
		for other, rec := range byNode {
			if other == node {
				continue
			}
			if !now.Before(rec.expiresAt) {
				delete(byNode, other)
				continue
			}
			// 只禁止「源 vs 源」；边缘副本可以和源共存。
			if st.Hops == 0 && rec.entry.Hops == 0 {
				conflict = true
				break
			}
		}
		if conflict {
			res.Conflicts = append(res.Conflicts, name)
			continue
		}

		byNode[node] = &record{
			entry: Entry{
				Name:      name,
				Node:      node,
				URLs:      urls,
				ExpiresAt: expires,
				Hops:      st.Hops,
				Upstream:  st.Upstream,
				Load:      st.Load,
			},
			expiresAt: expires,
		}
		keep[name] = true
		res.Registered = append(res.Registered, name)
	}

	// 整批替换：删除该节点本轮未上报的租约。
	for name, byNode := range s.m {
		if rec := byNode[node]; rec != nil && !keep[name] {
			delete(byNode, node)
		}
		if len(byNode) == 0 {
			delete(s.m, name)
		}
	}
	s.mu.Unlock()

	sort.Strings(res.Rejected)
	sort.Strings(res.Conflicts)
	return res
}

// Unregister 删除 node 持有的 name。只有登记节点本人能删除；
// 返回是否真的删掉了。
func (s *Store) Unregister(node, name string) bool {
	node = strings.TrimSpace(node)
	name = strings.Trim(strings.TrimSpace(name), "/")
	if node == "" || name == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	byNode := s.m[name]
	if byNode == nil || byNode[node] == nil {
		return false
	}
	delete(byNode, node)
	if len(byNode) == 0 {
		delete(s.m, name)
	}
	return true
}

// Lookup 查询一路流，返回选择器视角下的最优 provider（负载最低，其次
// 跳数最少）。过期的条目按未命中处理，并顺手删除。
func (s *Store) Lookup(name string) (Entry, bool) {
	cands := s.Candidates(name)
	if len(cands) == 0 {
		return Entry{}, false
	}
	return cands[0], true
}

// Candidates 返回一路流的全部候选 provider，按负载、跳数、节点名排序。
// 过期的条目会被惰性删除。
func (s *Store) Candidates(name string) []Entry {
	name = strings.Trim(strings.TrimSpace(name), "/")
	if name == "" {
		return nil
	}
	now := s.now()

	s.mu.Lock()
	byNode := s.m[name]
	out := make([]Entry, 0, len(byNode))
	for node, rec := range byNode {
		if !now.Before(rec.expiresAt) {
			delete(byNode, node)
			continue
		}
		out = append(out, copyEntry(rec.entry))
	}
	if len(byNode) == 0 {
		delete(s.m, name)
	}
	s.mu.Unlock()

	sort.Slice(out, func(i, j int) bool { return betterEntry(out[i], out[j]) })
	return out
}

// List 返回所有未过期的条目，按流名、节点名排序。
func (s *Store) List() []Entry {
	now := s.now()
	s.mu.RLock()
	out := make([]Entry, 0, len(s.m))
	for _, byNode := range s.m {
		for _, rec := range byNode {
			if now.Before(rec.expiresAt) {
				out = append(out, copyEntry(rec.entry))
			}
		}
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Node < out[j].Node
	})
	return out
}

// Sweep 删除所有过期租约，返回删除条数。
func (s *Store) Sweep() int {
	now := s.now()
	s.mu.Lock()
	n := 0
	for name, byNode := range s.m {
		for node, rec := range byNode {
			if !now.Before(rec.expiresAt) {
				delete(byNode, node)
				n++
			}
		}
		if len(byNode) == 0 {
			delete(s.m, name)
		}
	}
	s.mu.Unlock()
	return n
}

// Len 返回当前 entry 数（含尚未扫描掉的过期项，测试/运维用）。
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, byNode := range s.m {
		n += len(byNode)
	}
	return n
}

// Pick 从候选里选一个可用于 scheme 的 provider：排除本节点、超过
// maxHops 的副本，以及指向本节点的反向边。maxHops < 0 表示不限制跳数。
// 排序与 Candidates 一致（负载低者优先，其次跳数少、节点名小）。
func Pick(entries []Entry, self, scheme string, maxHops int) (Entry, bool) {
	var best Entry
	found := false
	for _, e := range entries {
		if !usable(e, self, maxHops) || e.URL(scheme) == "" {
			continue
		}
		if !found || betterEntry(e, best) {
			best = e
			found = true
		}
	}
	return best, found
}

// PickAll 返回过滤后的全部候选，保持选择器顺序。
func PickAll(entries []Entry, self string, maxHops int) []Entry {
	out := make([]Entry, 0, len(entries))
	for _, e := range entries {
		if usable(e, self, maxHops) {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return betterEntry(out[i], out[j]) })
	return out
}

func usable(e Entry, self string, maxHops int) bool {
	if self != "" && e.Node == self {
		return false
	}
	if maxHops >= 0 && e.Hops > maxHops {
		return false
	}
	// 直接反向边：本节点是对方的上游，说明再拉回去就成环了。
	if self != "" && e.Upstream == self {
		return false
	}
	return true
}

// betterEntry 定义选择顺序：负载低者优先，其次跳数少者，最后按节点名
// 保证结果稳定。
func betterEntry(a, b Entry) bool {
	if a.Load != b.Load {
		return a.Load < b.Load
	}
	if a.Hops != b.Hops {
		return a.Hops < b.Hops
	}
	return a.Node < b.Node
}

func normalizeURLs(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for scheme, u := range in {
		scheme = strings.ToLower(strings.TrimSpace(scheme))
		u = strings.TrimSpace(u)
		if scheme == "" || u == "" {
			continue
		}
		out[scheme] = u
	}
	return out
}

func copyEntry(e Entry) Entry {
	e.URLs = normalizeURLs(e.URLs)
	return e
}

// ParseAdvertise 解析 -advertise 的逗号分隔基础地址，返回 scheme → base。
//
//	rtsp://10.0.0.5:8554,rtmp://10.0.0.5:1935,http://10.0.0.5:8080
func ParseAdvertise(raw string) (map[string]string, error) {
	out := map[string]string{}
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		u, err := parseBaseURL(item)
		if err != nil {
			return nil, err
		}
		out[strings.ToLower(u.Scheme)] = strings.TrimRight(u.String(), "/")
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("advertise: no URL given")
	}
	return out, nil
}

var advertiseSchemes = map[string]bool{
	"rtsp": true, "rtsps": true,
	"rtmp": true, "rtmps": true,
	"http": true, "https": true,
}

func parseBaseURL(raw string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, fmt.Errorf("advertise %q: %w", raw, err)
	}
	scheme := strings.ToLower(u.Scheme)
	if !advertiseSchemes[scheme] {
		return nil, fmt.Errorf("advertise %q: unsupported scheme %q", raw, u.Scheme)
	}
	u.Scheme = scheme
	if u.Host == "" {
		return nil, fmt.Errorf("advertise %q: missing host", raw)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("advertise %q: query/fragment not allowed in a base URL", raw)
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = ""
	return u, nil
}

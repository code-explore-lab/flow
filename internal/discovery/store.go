// Package discovery 实现 S-3：动态源注册与发现。
//
// 节点把自己正在出帧的流登记到注册中心（带 TTL 租约），并周期性续约；
// 注册中心在租约过期后自动删除条目，因此源/节点下线后不需要显式清理。
// 边缘节点在本地没有某路流时查询注册中心，命中后用 D-1 的 302 把客户端
// 指到真正持有该流的节点。
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
}

// Entry 是注册中心里一条有效租约。
type Entry struct {
	Name      string            `json:"name"`
	Node      string            `json:"node"`
	URLs      map[string]string `json:"urls,omitempty"`
	ExpiresAt time.Time         `json:"expires_at"`
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
// 节点持有；上报方应停止广告该流，等对方租约过期后再重试。
type BatchResult struct {
	Registered []string `json:"registered"`
	Conflicts  []string `json:"conflicts"`
	Rejected   []string `json:"rejected"`
}

type record struct {
	entry     Entry
	expiresAt time.Time
}

// Store 是进程内的租约表。所有方法并发安全。
type Store struct {
	mu  sync.RWMutex
	m   map[string]*record
	ttl time.Duration
	now func() time.Time
}

// NewStore 创建租约表。ttl <= 0 时使用 DefaultTTL。
func NewStore(ttl time.Duration) *Store {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Store{
		m:   make(map[string]*record),
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
// 批次里出现的流登记/续约；同一路流若已被另一个仍存活的节点持有则返回
// 冲突，不抢占；node 之前持有但本次批次没有的流立即删除——这比等 TTL
// 过期更快，源主动下线时能在一次心跳内从注册中心消失。
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
		if rec := s.m[name]; rec != nil && now.Before(rec.expiresAt) && rec.entry.Node != node {
			res.Conflicts = append(res.Conflicts, name)
			continue
		}
		s.m[name] = &record{
			entry: Entry{
				Name:      name,
				Node:      node,
				URLs:      urls,
				ExpiresAt: expires,
			},
			expiresAt: expires,
		}
		keep[name] = true
		res.Registered = append(res.Registered, name)
	}

	// 整批替换：删除该节点本轮未上报的租约。
	for name, rec := range s.m {
		if rec.entry.Node == node && !keep[name] {
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
	rec := s.m[name]
	if rec == nil || rec.entry.Node != node {
		return false
	}
	delete(s.m, name)
	return true
}

// Lookup 查询一路流。过期的条目按未命中处理，并顺手删除。
func (s *Store) Lookup(name string) (Entry, bool) {
	name = strings.Trim(strings.TrimSpace(name), "/")
	if name == "" {
		return Entry{}, false
	}
	now := s.now()

	s.mu.RLock()
	rec := s.m[name]
	s.mu.RUnlock()
	if rec == nil {
		return Entry{}, false
	}
	if !now.Before(rec.expiresAt) {
		s.mu.Lock()
		if cur := s.m[name]; cur == rec {
			delete(s.m, name)
		}
		s.mu.Unlock()
		return Entry{}, false
	}
	return copyEntry(rec.entry), true
}

// List 返回所有未过期的条目，按流名排序。
func (s *Store) List() []Entry {
	now := s.now()
	s.mu.RLock()
	out := make([]Entry, 0, len(s.m))
	for _, rec := range s.m {
		if now.Before(rec.expiresAt) {
			out = append(out, copyEntry(rec.entry))
		}
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Sweep 删除所有过期租约，返回删除条数。
func (s *Store) Sweep() int {
	now := s.now()
	s.mu.Lock()
	n := 0
	for name, rec := range s.m {
		if !now.Before(rec.expiresAt) {
			delete(s.m, name)
			n++
		}
	}
	s.mu.Unlock()
	return n
}

// Len 返回当前条目数（含尚未扫描掉的过期项，测试/运维用）。
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
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

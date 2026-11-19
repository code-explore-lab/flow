package discovery

// S-3：节点侧的上报循环。
//
// 一个后台 goroutine 周期性地把本地「正在出帧」的流整批发给注册中心。
// 批量的语义是「本节点当前持有这些流」：没在批次里的旧条目由注册中心
// 立即删除，不需要逐条 DELETE；节点进程挂掉时则依靠 TTL 过期兜底。
//
// 只广告 Active 的流：回源/-pull 断线时本地流会保留（下游继续等帧），
// 但此时不宜再把别的边缘节点引过来；等上游恢复、重新有帧写入后自动
// 回到批次里。

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"flow/internal/stream"
)

const (
	announcerDebounce = 100 * time.Millisecond
	announcerTimeout  = 2 * time.Second
	announcerLogEvery = 30 * time.Second
)

// Announcer 把本地注册表同步到发现服务。
type Announcer struct {
	client       *Client
	reg          *stream.Registry
	advertise    map[string]string // scheme -> base URL
	interval     time.Duration
	activeWindow time.Duration

	// S-2：心跳里附带节点负载，且只广告不超过 maxHops 的副本。
	load    func() int
	maxHops int

	dirty chan struct{}

	mu        sync.Mutex
	lastErr   string
	lastLogAt time.Time
}

// NewAnnouncer 构造上报器。advertise 是 scheme → 基础地址（来自
// ParseAdvertise），interval <= 0 时用 TTL/3（最少 1s），activeWindow
// <= 0 时用 stream.DefaultActiveWindow。
func NewAnnouncer(client *Client, reg *stream.Registry, advertise map[string]string, interval, activeWindow time.Duration) *Announcer {
	if interval <= 0 {
		interval = 5 * time.Second
		if client != nil {
			interval = client.TTL() / 3
			if interval < time.Second {
				interval = time.Second
			}
		}
	}
	if activeWindow <= 0 {
		activeWindow = stream.DefaultActiveWindow
	}
	cp := make(map[string]string, len(advertise))
	for scheme, base := range advertise {
		scheme = strings.ToLower(strings.TrimSpace(scheme))
		base = strings.TrimRight(strings.TrimSpace(base), "/")
		if scheme != "" && base != "" {
			cp[scheme] = base
		}
	}
	if reg != nil {
		// Observer 必须非阻塞，这里只投递一个信号；真正的收集/发送
		// 在 Run 的 goroutine 里做。
		a := &Announcer{client: client, reg: reg, advertise: cp, interval: interval, activeWindow: activeWindow, maxHops: 1, dirty: make(chan struct{}, 1)}
		// O-4 的健康告警也会订阅注册表变更，这里用 AddObserver 避免覆盖。
		reg.AddObserver(a.onEvent)
		return a
	}
	return &Announcer{client: client, advertise: cp, interval: interval, activeWindow: activeWindow, maxHops: 1, dirty: make(chan struct{}, 1)}
}

// Interval 返回心跳间隔（测试/日志用）。
func (a *Announcer) Interval() time.Duration { return a.interval }

// SetLoad 设置节点负载来源（通常是 /metrics 里的活跃观看数）。nil 表示
// 统一上报 0。可在 Run 之前调用。
func (a *Announcer) SetLoad(fn func() int) { a.load = fn }

// SetMaxHops 设置广告的最大级联跳数：`hops > n` 的副本不再上报，避免
// 拓扑无限加深。n < 0 按 0 处理（只广告推流源）。可在 Run 之前调用。
func (a *Announcer) SetMaxHops(n int) {
	if n < 0 {
		n = 0
	}
	a.maxHops = n
}

func (a *Announcer) onEvent(stream.Event, string, *stream.Stream) {
	select {
	case a.dirty <- struct{}{}:
	default:
	}
}

// Run 阻塞运行直到 ctx 取消。退出前尽力发送一次空批次，让注册中心
// 立刻清掉本节点的租约；发送失败也无所谓，TTL 会兜底。
func (a *Announcer) Run(ctx context.Context) {
	if a == nil || a.client == nil || a.reg == nil {
		return
	}
	if len(a.advertise) == 0 {
		// 没有可播放地址就无法帮别的节点发现，也就不能发空批次
		// （那会清掉本节点已有租约）。
		fmt.Println("discovery: no advertise URL configured; announcer disabled")
		return
	}
	a.flush(ctx)

	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), announcerTimeout)
			_, _ = a.client.Heartbeat(shutdown, nil)
			cancel()
			return
		case <-ticker.C:
			a.flush(ctx)
		case <-a.dirty:
			// 合并短时间内的多次变更，避免每次推流上下线都发一个包。
			timer := time.NewTimer(announcerDebounce)
			select {
			case <-ctx.Done():
				timer.Stop()
				shutdown, cancel := context.WithTimeout(context.Background(), announcerTimeout)
				_, _ = a.client.Heartbeat(shutdown, nil)
				cancel()
				return
			case <-timer.C:
			}
			a.flush(ctx)
		}
	}
}

// flush 收集当前可广告的流并发送一次心跳。
func (a *Announcer) flush(parent context.Context) {
	streams := a.collect()
	ctx, cancel := context.WithTimeout(parent, announcerTimeout)
	defer cancel()
	res, err := a.client.Heartbeat(ctx, streams)
	if err != nil {
		a.logf("discovery: heartbeat failed (node %s): %v", a.client.Node(), err)
		return
	}
	if len(res.Conflicts) > 0 {
		a.logf("discovery: stream(s) held by another live node: %s",
			strings.Join(res.Conflicts, ", "))
	}
}

// collect 把注册表快照转成上报批次。只有 recent 内有媒体写入的流才广告。
func (a *Announcer) collect() []Stream {
	if len(a.advertise) == 0 {
		return nil
	}
	load := 0
	if a.load != nil {
		load = a.load()
	}
	snap := a.reg.Snapshot()
	out := make([]Stream, 0, len(snap))
	for _, ns := range snap {
		if !ns.Stream.Active(a.activeWindow) {
			continue
		}
		hops, upstream := ns.Stream.DiscoveryMeta()
		if hops > a.maxHops {
			// 超过级联深度上限：本机仍有流可播，但不再把别的边缘引过来。
			continue
		}
		urls := make(map[string]string, len(a.advertise))
		for scheme, base := range a.advertise {
			if u := joinStreamURL(base, ns.Name); u != "" {
				urls[scheme] = u
			}
		}
		if len(urls) == 0 {
			continue
		}
		out = append(out, Stream{
			Name:     ns.Name,
			URLs:     urls,
			Hops:     hops,
			Upstream: upstream,
			Load:     load,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// logf 对相同错误限频，注册中心长时间不可用时不会刷爆日志。
func (a *Announcer) logf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	now := time.Now()
	a.mu.Lock()
	if msg == a.lastErr && now.Sub(a.lastLogAt) < announcerLogEvery {
		a.mu.Unlock()
		return
	}
	a.lastErr = msg
	a.lastLogAt = now
	a.mu.Unlock()
	fmt.Println(msg)
}

// joinStreamURL 在基础地址后拼接流名，保留流名里的路径层级。
// HTTP-FLV 的播放地址必须带 .flv 后缀，其余协议原样拼接。
func joinStreamURL(base, name string) string {
	name = strings.Trim(name, "/")
	if name == "" {
		return ""
	}
	u, err := url.Parse(base)
	if err != nil {
		return strings.TrimRight(base, "/") + "/" + name
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/" + name
	if (u.Scheme == "http" || u.Scheme == "https") && !strings.HasSuffix(strings.ToLower(u.Path), ".flv") {
		u.Path += ".flv"
	}
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

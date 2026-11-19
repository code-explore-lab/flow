// Package health 聚合节点健康状态与进程内告警（O-4）。
//
// 两个探针的职责刻意分开：
//
//   - /healthz（liveness）：进程活着就返回 200，不依赖任何流或监听口；
//   - /readyz（readiness）：本节点能否接新的推/拉流 = 所有已启用监听口
//     bind 成功 + 注册表可用。不要求存在活跃流。
//
// 推流端断开、流静默、监听口占用等只产生单条告警（active + history），
// 不会把全局 readiness 打成 503；外发交给 Prometheus/Alertmanager 抓
// flow_ready 与 flow_alerts 指标。
//
// 数据来源尽量复用现有组件：
//   - stream.Registry 的 Snapshot / Active / LastActivity / 观察者；
//   - stream.Janitor 的 SweepOnce 与 Idle；
//   - session.Manager 的 OnExpire 钩子（由 rtsp.Server 转发到
//     RecordSessionExpired）；
//   - rtsp 的丢包指标仍由 /metrics 的原有输出负责。
//
// 本包不依赖任何协议实现，避免 import 环；协议侧通过 main 注入的回调
// 上报事件。观察者回调在 Publish/Unpublish 的调用方 goroutine 上同步
// 执行，因此这里只做加锁后的内存操作，不做网络 IO。
package health

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"syscall"
	"time"

	"flow/internal/stream"
)

// ListenerState 是监听口的 bind 状态。
//
// 监听发生在 Serve 的 goroutine 里：进程刚启动时是 pending；Serve 成功
// 完成 bind 后调用 NotifyListenerReady 变成 up；bind 失败或 Serve 返回
// 错误时变成 down。
type ListenerState string

const (
	ListenerPending ListenerState = "pending"
	ListenerUp      ListenerState = "up"
	ListenerDown    ListenerState = "down"
)

// 告警类型。前三个是持续状态（会 resolve），session_expired 是带 TTL
// 的短期告警，避免一次会话超时永远挂在「当前活跃」里。
const (
	AlertPortBindFailed = "port_bind_failed" // 端口占用 / bind 失败
	AlertStreamDown     = "stream_down"      // 出过帧的流被摘除（推流端断开/被踢/回收）
	AlertStreamStalled  = "stream_stalled"   // 流仍在注册表里但超过窗口没有新媒体
	AlertSessionExpired = "session_expired"  // RTSP 会话超时/断网被 Sweep 回收
)

var alertKinds = []string{
	AlertPortBindFailed,
	AlertStreamDown,
	AlertStreamStalled,
	AlertSessionExpired,
}

// Listener 一个监听口的当前状态。
type Listener struct {
	Name  string        `json:"name"`
	Addr  string        `json:"addr"`
	State ListenerState `json:"state"`
	Error string        `json:"error,omitempty"`
	Since time.Time     `json:"since"`
}

// Alert 一条当前活跃的告警。
type Alert struct {
	Kind      string     `json:"kind"`
	Subject   string     `json:"subject,omitempty"`
	Message   string     `json:"message"`
	Since     time.Time  `json:"since"`
	Updated   time.Time  `json:"updated"`
	Count     int        `json:"count"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
}

// AlertEvent 告警历史里的一条记录（fired / resolved）。
type AlertEvent struct {
	Action  string    `json:"action"`
	Kind    string    `json:"kind"`
	Subject string    `json:"subject,omitempty"`
	Message string    `json:"message"`
	At      time.Time `json:"at"`
}

// StreamStatus 健康快照里的一路流。
type StreamStatus struct {
	Name         string    `json:"name"`
	Active       bool      `json:"active"`
	HasMedia     bool      `json:"hasMedia"`
	LastActivity time.Time `json:"lastActivity"`
}

// Readiness 是 /readyz 的判定结果。
type Readiness struct {
	Ready        bool       `json:"ready"`
	Reasons      []string   `json:"reasons"`
	Registry     string     `json:"registry"`
	Listeners    []Listener `json:"listeners"`
	ActiveAlerts int        `json:"activeAlerts"`
}

// AlertsView 是告警的对外视图。
type AlertsView struct {
	Active   []Alert           `json:"active"`
	History  []AlertEvent      `json:"history"`
	Counters map[string]uint64 `json:"counters"`
}

// Snapshot 是 /api/v1/health 的完整视图。
type Snapshot struct {
	Status        string         `json:"status"` // ok / degraded
	Ready         bool           `json:"ready"`
	UptimeSeconds float64        `json:"uptimeSeconds"`
	Registry      string         `json:"registry"`
	Listeners     []Listener     `json:"listeners"`
	Streams       []StreamStatus `json:"streams"`
	Alerts        AlertsView     `json:"alerts"`
	StartedAt     time.Time      `json:"startedAt"`
}

// Options 构造 Checker 的参数，零值可用。
type Options struct {
	// Registry 共享流注册表；nil 时只做监听口健康检查。
	Registry *stream.Registry
	// Janitor 可选。Scan 会复用它做一次空闲回收，让「静默流」尽快
	// 从注册表消失并产生 stream_down 告警。
	Janitor *stream.Janitor
	// ActiveWindow 判定流是否活跃 / 静默的窗口，默认 stream.DefaultActiveWindow。
	ActiveWindow time.Duration
	// ScanInterval 后台扫描周期，默认 1s。
	ScanInterval time.Duration
	// SessionAlertTTL session_expired 告警在「当前活跃」里保留多久，默认 5m。
	SessionAlertTTL time.Duration
	// HistoryLimit 历史告警条数上限，默认 100（超出丢最旧的）。
	HistoryLimit int
	// Now 便于测试注入时钟；nil 使用 time.Now。
	Now func() time.Time
	// Logf 告警日志输出；nil 使用 fmt.Printf。
	Logf func(format string, args ...any)
}

// Checker 聚合监听口、流和会话告警，可安全并发使用。
type Checker struct {
	opts    Options
	started time.Time

	mu            sync.Mutex
	listeners     map[string]*Listener
	listenerOrder []string
	active        map[string]*Alert
	history       []AlertEvent
	counters      map[string]uint64
}

// New 构造 Checker；Registry 非 nil 时自动注册变更观察者。
func New(opts Options) *Checker {
	if opts.ActiveWindow <= 0 {
		opts.ActiveWindow = stream.DefaultActiveWindow
	}
	if opts.ScanInterval <= 0 {
		opts.ScanInterval = time.Second
	}
	if opts.SessionAlertTTL <= 0 {
		opts.SessionAlertTTL = 5 * time.Minute
	}
	if opts.HistoryLimit <= 0 {
		opts.HistoryLimit = 100
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logf == nil {
		opts.Logf = func(format string, args ...any) {
			fmt.Printf(format+"\n", args...)
		}
	}
	c := &Checker{
		opts:      opts,
		started:   opts.Now(),
		listeners: make(map[string]*Listener),
		active:    make(map[string]*Alert),
		counters:  make(map[string]uint64),
	}
	if opts.Registry != nil {
		// AddObserver 不覆盖 S-3 发现上报注册的观察者。
		opts.Registry.AddObserver(c.onEvent)
	}
	return c
}

// ActiveWindow 返回流活跃/静默判定窗口（测试/日志用）。
func (c *Checker) ActiveWindow() time.Duration {
	if c == nil {
		return 0
	}
	return c.opts.ActiveWindow
}

// RegisterListener 把一个已启用的监听口登记为 pending。重复调用保持
// 原状态（不会把已经 up 的监听口退回 pending）。
func (c *Checker) RegisterListener(name, addr string) {
	if c == nil || name == "" {
		return
	}
	now := c.opts.Now()
	c.mu.Lock()
	l := c.listeners[name]
	if l == nil {
		l = &Listener{Name: name, Since: now}
		c.listeners[name] = l
		c.listenerOrder = append(c.listenerOrder, name)
	}
	l.Addr = addr
	if l.State == "" {
		l.State = ListenerPending
	}
	c.mu.Unlock()
}

// ListenerUp 标记监听口 bind 成功。Serve 完成 bind 后通过
// NotifyListenerReady(ctx) 触发。
func (c *Checker) ListenerUp(name string) {
	if c == nil || name == "" {
		return
	}
	now := c.opts.Now()
	c.mu.Lock()
	l := c.listeners[name]
	if l == nil {
		l = &Listener{Name: name, Since: now}
		c.listeners[name] = l
		c.listenerOrder = append(c.listenerOrder, name)
	}
	changed := l.State != ListenerUp
	l.State = ListenerUp
	l.Error = ""
	if changed {
		l.Since = now
	}
	c.mu.Unlock()
	if changed {
		c.resolve(AlertPortBindFailed, name, "listener bound successfully")
	}
}

// ListenerDown 标记监听口 bind 失败或 Serve 异常退出，并产生
// port_bind_failed 告警（消息里带 err，EADDRINUSE 会原样保留）。
func (c *Checker) ListenerDown(name string, err error) {
	if c == nil || name == "" {
		return
	}
	now := c.opts.Now()
	msg := "listener stopped"
	if err != nil {
		msg = err.Error()
		// Windows 的错误文本是 "Only one usage of each socket address..."，
		// 不带 EADDRINUSE；显式标出来，方便日志/告警检索和跨平台统一。
		// 注意 Go 在 Windows 上的 syscall.EADDRINUSE 是一个虚构的 errno，
		// 与 Winsock 的 WSAEADDRINUSE(10048) 不相等，两个都要判断。
		if errors.Is(err, syscall.EADDRINUSE) || errors.Is(err, syscall.Errno(10048)) {
			msg = "EADDRINUSE: " + msg
		}
	}
	c.mu.Lock()
	l := c.listeners[name]
	if l == nil {
		l = &Listener{Name: name, Since: now}
		c.listeners[name] = l
		c.listenerOrder = append(c.listenerOrder, name)
	}
	l.State = ListenerDown
	l.Error = msg
	l.Since = now
	addr := l.Addr
	c.mu.Unlock()

	subject := name
	message := fmt.Sprintf("listener %s on %s is down: %s", name, addr, msg)
	c.fire(AlertPortBindFailed, subject, message, time.Time{})
}

// RecordSessionExpired 记录一次会话超时回收（O-4 的「会话异常」）。
// lastActive 是会话最后一次被判定为活着的时间，用于判断异常持续了多久。
func (c *Checker) RecordSessionExpired(protocol, id, remote string, lastActive time.Time) {
	if c == nil || id == "" {
		return
	}
	now := c.opts.Now()
	subject := protocol + "/" + id
	message := fmt.Sprintf("%s session %s (%s) expired: no RTCP/RTSP activity since %s",
		protocol, id, remote, lastActive.Format(time.RFC3339))
	c.fire(AlertSessionExpired, subject, message, now.Add(c.opts.SessionAlertTTL))
}

// Ready 返回本节点是否可接新的推/拉流。口径：
//   - 所有已登记监听口都是 up（pending 也不算就绪）；
//   - 注册表可用（非 nil）。
//
// 活跃流数量不参与判定。reasons 是给 /readyz 和日志用的可读原因。
func (c *Checker) Ready() (bool, []string) {
	if c == nil {
		return true, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readyLocked()
}

func (c *Checker) readyLocked() (bool, []string) {
	var reasons []string
	if c.opts.Registry == nil {
		reasons = append(reasons, "stream registry unavailable")
	}
	for _, name := range c.listenerOrder {
		l := c.listeners[name]
		switch l.State {
		case ListenerUp:
		case ListenerPending:
			reasons = append(reasons, fmt.Sprintf("listener %s (%s) is pending", name, l.Addr))
		default:
			reasons = append(reasons, fmt.Sprintf("listener %s (%s) is down: %s", name, l.Addr, l.Error))
		}
	}
	return len(reasons) == 0, reasons
}

// Readiness 返回 /readyz 需要的完整判定结果。
func (c *Checker) Readiness() Readiness {
	if c == nil {
		return Readiness{Ready: true, Reasons: []string{}, Registry: "unavailable", Listeners: []Listener{}}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ready, reasons := c.readyLocked()
	if reasons == nil {
		reasons = []string{}
	}
	return Readiness{
		Ready:        ready,
		Reasons:      reasons,
		Registry:     c.registryStateLocked(),
		Listeners:    c.listenersLocked(),
		ActiveAlerts: len(c.active),
	}
}

// Alerts 返回当前活跃告警、历史与各类型累计触发次数。
func (c *Checker) Alerts() AlertsView {
	if c == nil {
		return AlertsView{Active: []Alert{}, History: []AlertEvent{}, Counters: map[string]uint64{}}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.alertsLocked()
}

func (c *Checker) alertsLocked() AlertsView {
	active := make([]Alert, 0, len(c.active))
	for _, a := range c.active {
		active = append(active, *a)
	}
	sort.Slice(active, func(i, j int) bool {
		if active[i].Kind != active[j].Kind {
			return active[i].Kind < active[j].Kind
		}
		return active[i].Subject < active[j].Subject
	})
	// 用非 nil 的空切片，JSON 输出 [] 而不是 null。
	history := make([]AlertEvent, len(c.history))
	copy(history, c.history)
	counters := make(map[string]uint64, len(c.counters))
	for k, v := range c.counters {
		counters[k] = v
	}
	return AlertsView{Active: active, History: history, Counters: counters}
}

// Snapshot 返回 /api/v1/health 的完整状态。
func (c *Checker) Snapshot() Snapshot {
	if c == nil {
		return Snapshot{
			Status: "ok", Ready: true, Registry: "unavailable",
			Listeners: []Listener{}, Streams: []StreamStatus{},
			Alerts: AlertsView{Active: []Alert{}, History: []AlertEvent{}, Counters: map[string]uint64{}},
		}
	}
	now := c.opts.Now()
	c.mu.Lock()
	ready, _ := c.readyLocked()
	registry := c.registryStateLocked()
	listeners := c.listenersLocked()
	alerts := c.alertsLocked()
	started := c.started
	c.mu.Unlock()

	streams := c.streamStatuses()
	status := "ok"
	if !ready || len(alerts.Active) > 0 {
		status = "degraded"
	}
	return Snapshot{
		Status:        status,
		Ready:         ready,
		UptimeSeconds: now.Sub(started).Seconds(),
		Registry:      registry,
		Listeners:     listeners,
		Streams:       streams,
		Alerts:        alerts,
		StartedAt:     started,
	}
}

func (c *Checker) registryStateLocked() string {
	if c.opts.Registry == nil {
		return "unavailable"
	}
	return "ok"
}

func (c *Checker) listenersLocked() []Listener {
	out := make([]Listener, 0, len(c.listenerOrder))
	for _, name := range c.listenerOrder {
		if l := c.listeners[name]; l != nil {
			out = append(out, *l)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Run 周期扫描，直到 ctx 取消。
func (c *Checker) Run(ctx context.Context) {
	if c == nil {
		return
	}
	ticker := time.NewTicker(c.opts.ScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.Scan()
		}
	}
}

// Scan 扫描一次注册表：
//   - 复用 Janitor.SweepOnce 回收静默流；
//   - 对「仍在注册表里但超过窗口没有媒体」的流产生 stream_stalled；
//   - 流恢复出帧后 resolve；
//   - 清掉过期的 session_expired 告警。
//
// 流的上下线（stream_down）由注册表观察者实时触发，不依赖扫描周期。
func (c *Checker) Scan() {
	if c == nil {
		return
	}
	if c.opts.Janitor != nil {
		// 与 Janitor.Run 并发调用是安全的：Reap 的判断和删除在同一把锁内。
		c.opts.Janitor.SweepOnce()
	}
	now := c.opts.Now()
	var snap []stream.NamedStream
	if c.opts.Registry != nil {
		snap = c.opts.Registry.Snapshot()
	}
	for _, ns := range snap {
		st := ns.Stream
		if st == nil {
			continue
		}
		last := st.LastActivity()
		if st.Active(c.opts.ActiveWindow) {
			c.resolve(AlertStreamStalled, ns.Name, "stream is publishing media again")
			continue
		}
		if now.Sub(last) >= c.opts.ActiveWindow {
			msg := fmt.Sprintf("stream %q has no media for %s (last activity %s)",
				ns.Name, now.Sub(last).Round(time.Second), last.Format(time.RFC3339))
			c.fire(AlertStreamStalled, ns.Name, msg, time.Time{})
		}
	}

	// 已从注册表消失的流由观察者负责产生 stream_down；这里只处理
	// 带 TTL 的短期告警。
	var expired []*Alert
	c.mu.Lock()
	for key, a := range c.active {
		if a.ExpiresAt != nil && !now.Before(*a.ExpiresAt) {
			expired = append(expired, a)
			delete(c.active, key)
			c.appendHistoryLocked(AlertEvent{
				Action: "resolved", Kind: a.Kind, Subject: a.Subject,
				Message: "alert TTL elapsed", At: now,
			})
		}
	}
	c.mu.Unlock()
	for _, a := range expired {
		c.opts.Logf("health: alert resolved kind=%s subject=%q: alert TTL elapsed", a.Kind, a.Subject)
	}
}

// streamStatuses 快照当前注册表里的流，供管理接口展示。
func (c *Checker) streamStatuses() []StreamStatus {
	if c.opts.Registry == nil {
		return []StreamStatus{}
	}
	snap := c.opts.Registry.Snapshot()
	out := make([]StreamStatus, 0, len(snap))
	for _, ns := range snap {
		if ns.Stream == nil {
			continue
		}
		out = append(out, StreamStatus{
			Name:         ns.Name,
			Active:       ns.Stream.Active(c.opts.ActiveWindow),
			HasMedia:     ns.Stream.HasMedia(),
			LastActivity: ns.Stream.LastActivity(),
		})
	}
	return out
}

// onEvent 注册表观察者：出过帧的流被摘除时立即产生 stream_down，
// 重新发布时 resolve。回调必须快速完成。
func (c *Checker) onEvent(ev stream.Event, name string, st *stream.Stream) {
	if c == nil {
		return
	}
	switch ev {
	case stream.EventPublished:
		c.resolve(AlertStreamDown, name, "stream is publishing again")
	case stream.EventUnpublished:
		if st == nil {
			return
		}
		hadMedia := st.HasMedia()
		last := st.LastActivity()
		// 静默流已在 Scan 里报过 stream_stalled，流消失时先结掉它。
		c.resolve(AlertStreamStalled, name, "stream removed")
		if hadMedia {
			msg := fmt.Sprintf("stream %q stopped publishing (last media %s)", name, last.Format(time.RFC3339))
			c.fire(AlertStreamDown, name, msg, time.Time{})
		}
	}
}

// fire 新增或刷新一条持续告警。同一条告警重复触发只更新消息和计数，
// 不重复写历史，避免半开连接每轮扫描都刷一条。
func (c *Checker) fire(kind, subject, message string, expiresAt time.Time) {
	now := c.opts.Now()
	c.mu.Lock()
	key := alertKey(kind, subject)
	a := c.active[key]
	fired := a == nil
	if a == nil {
		a = &Alert{Kind: kind, Subject: subject, Since: now}
		c.active[key] = a
		c.counters[kind]++
	}
	a.Message = message
	a.Updated = now
	a.Count++
	if !expiresAt.IsZero() {
		a.ExpiresAt = &expiresAt
	}
	if fired {
		c.appendHistoryLocked(AlertEvent{
			Action: "fired", Kind: kind, Subject: subject, Message: message, At: now,
		})
	}
	c.mu.Unlock()
	if fired {
		c.opts.Logf("health: alert fired kind=%s subject=%q: %s", kind, subject, message)
	}
}

// resolve 清除一条持续告警；不存在时什么也不做。
func (c *Checker) resolve(kind, subject, reason string) {
	now := c.opts.Now()
	c.mu.Lock()
	key := alertKey(kind, subject)
	a := c.active[key]
	if a == nil {
		c.mu.Unlock()
		return
	}
	delete(c.active, key)
	c.appendHistoryLocked(AlertEvent{
		Action: "resolved", Kind: kind, Subject: subject, Message: reason, At: now,
	})
	c.mu.Unlock()
	c.opts.Logf("health: alert resolved kind=%s subject=%q: %s", kind, subject, reason)
}

func (c *Checker) appendHistoryLocked(ev AlertEvent) {
	c.history = append(c.history, ev)
	if len(c.history) > c.opts.HistoryLimit {
		c.history = c.history[len(c.history)-c.opts.HistoryLimit:]
	}
}

func alertKey(kind, subject string) string {
	return kind + "|" + subject
}

// WritePrometheus 输出 O-4 的健康指标，由 /metrics 端点调用。
//
//	flow_ready                        1/0
//	flow_alerts{kind="..."}           当前活跃告警数
//	flow_alerts_fired_total{kind}     累计触发次数
//	flow_health_listener_up{name,addr} 1/0（pending 也按 0 输出）
func (c *Checker) WritePrometheus(w io.Writer) {
	if c == nil {
		return
	}
	c.mu.Lock()
	ready, _ := c.readyLocked()
	listeners := c.listenersLocked()
	counts := make(map[string]int, len(alertKinds))
	for _, a := range c.active {
		counts[a.Kind]++
	}
	fired := make(map[string]uint64, len(c.counters))
	for k, v := range c.counters {
		fired[k] = v
	}
	c.mu.Unlock()

	readyValue := 0
	if ready {
		readyValue = 1
	}
	fmt.Fprintln(w, "# HELP flow_ready 本节点能否接新的推/拉流（所有监听口 up 且注册表可用）")
	fmt.Fprintln(w, "# TYPE flow_ready gauge")
	fmt.Fprintf(w, "flow_ready %d\n", readyValue)

	fmt.Fprintln(w, "# HELP flow_alerts 当前活跃告警数（按 kind）")
	fmt.Fprintln(w, "# TYPE flow_alerts gauge")
	for _, kind := range alertKinds {
		fmt.Fprintf(w, "flow_alerts{kind=%q} %d\n", kind, counts[kind])
	}

	fmt.Fprintln(w, "# HELP flow_alerts_fired_total 累计触发的告警数（按 kind）")
	fmt.Fprintln(w, "# TYPE flow_alerts_fired_total counter")
	for _, kind := range alertKinds {
		fmt.Fprintf(w, "flow_alerts_fired_total{kind=%q} %d\n", kind, fired[kind])
	}

	fmt.Fprintln(w, "# HELP flow_health_listener_up 监听口是否 bind 成功（pending/down 为 0）")
	fmt.Fprintln(w, "# TYPE flow_health_listener_up gauge")
	for _, l := range listeners {
		up := 0
		if l.State == ListenerUp {
			up = 1
		}
		fmt.Fprintf(w, "flow_health_listener_up{name=%q,addr=%q} %d\n", l.Name, l.Addr, up)
	}
}

// ---- context 通知：Serve 完成 bind 后标记对应的监听口 up ----
//
// cmd/server 为每个 Serve 派生一个带回调的 context；各协议的 Serve 在
// net.Listen/tls.Listen 成功之后调用 NotifyListenerReady(ctx)。这样
// health 不需要 import 任何协议包，也不用靠「延迟多久算启动成功」的
// 时间猜测来区分 pending/up。

type listenerReadyKey struct{}

// WithListenerReady 在 ctx 上挂一个 bind 成功回调。
func WithListenerReady(ctx context.Context, fn func()) context.Context {
	if fn == nil {
		return ctx
	}
	return context.WithValue(ctx, listenerReadyKey{}, fn)
}

// NotifyListenerReady 在监听口 bind 成功后调用；ctx 上没有回调时是空操作，
// 因此不影响现有测试直接调用各 Server.Serve 的场景。
func NotifyListenerReady(ctx context.Context) {
	if fn, ok := ctx.Value(listenerReadyKey{}).(func()); ok && fn != nil {
		fn()
	}
}

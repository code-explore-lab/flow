// Package session 管理 RTSP 会话的生命周期：创建、查找、活跃度打点、超时扫描回收。
//
// 这一层刻意不碰任何 RTSP/RTP 细节：Manager 只认 ID、对端 IP、状态和时间戳；
// 会话到期的具体动作（关 UDP socket、停推流 goroutine）由使用方通过
// Session.OnClose 注册的回调完成，Manager 保持与协议无关。
package session

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	// DefaultTimeout 是 SETUP 响应里下发给客户端的会话超时（Session: sess-x;timeout=60）。
	DefaultTimeout = 60 * time.Second
	// DefaultScanInterval 是后台清理 goroutine 的扫描周期。
	DefaultScanInterval = 30 * time.Second
)

// State 是 RTSP 会话状态机：Init -> Playing <-> Paused -> Teardown。
type State int

const (
	StateInit State = iota // SETUP 完成，还未 PLAY
	StatePlaying
	StatePaused
	StateTeardown // 已销毁
)

func (s State) String() string {
	switch s {
	case StateInit:
		return "Init"
	case StatePlaying:
		return "Playing"
	case StatePaused:
		return "Paused"
	case StateTeardown:
		return "Teardown"
	default:
		return fmt.Sprintf("State(%d)", int(s))
	}
}

// Session 一个 RTSP 会话。字段都在内部加锁，可被多个 goroutine 并发访问
// （RTSP 处理 goroutine、RTCP 接收 goroutine、后台清理 goroutine）。
type Session struct {
	ID        string
	RemoteIP  string
	CreatedAt time.Time

	// OnClose 由使用方注册：会话被 TEARDOWN 或超时回收时调用一次，
	// 用来关闭该会话独占的资源。必须在会话投入使用前设置。
	OnClose func()

	// Data 挂载使用方自己的数据（这里放 video/audio 两条 mediaTrack），
	// 让 Manager 不必知道 RTSP 的传输细节。同样在投入使用前设置一次。
	Data any

	mu        sync.Mutex
	state     State
	lastRTCP  time.Time // 最后一次收到 RTCP 的时间
	lastRTSP  time.Time // 最后一次收到该会话 RTSP 请求的时间
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
}

func newSession(id, remoteIP string, now time.Time) *Session {
	ctx, cancel := context.WithCancel(context.Background())
	return &Session{
		ID:        id,
		RemoteIP:  remoteIP,
		CreatedAt: now,
		state:     StateInit,
		// 创建即算一次活动：刚 SETUP 还没 PLAY 的会话不会立刻被扫掉。
		lastRTCP: now,
		lastRTSP: now,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// State 返回当前状态。
func (s *Session) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// SetState 更新状态机。
func (s *Session) SetState(st State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = st
}

// TouchRTCP 收到一个 RTCP 包时调用，刷新会话活跃时间。
// UDP 模式由 RTCP socket 的读 goroutine 调用，interleaved 模式由 $ 帧分发处调用。
func (s *Session) TouchRTCP() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastRTCP = time.Now()
}

// TouchRTSP 收到该会话的 RTSP 请求（SETUP/PLAY/PAUSE/TEARDOWN）时调用。
// 有些客户端不发 RTCP、改用 RTSP 请求保活，这种也算活着。
func (s *Session) TouchRTSP() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastRTSP = time.Now()
}

// LastRTCP 返回最后一次收到 RTCP 的时间。
func (s *Session) LastRTCP() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastRTCP
}

// LastActivity 返回最后一次被判定为「会话还活着」的时间：
// 取最后一次 RTCP 和最后一次 RTSP 请求中较晚的那个。
func (s *Session) LastActivity() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastActivityLocked()
}

func (s *Session) lastActivityLocked() time.Time {
	if s.lastRTSP.After(s.lastRTCP) {
		return s.lastRTSP
	}
	return s.lastRTCP
}

// Expired 判断在 now 时刻会话是否已超时。
//
// 活跃时间取「最后一次收到 RTCP」和「最后一次收到 RTSP 请求」中较晚的那个：
// PLAY 之后正常客户端会周期性发 RTCP RR；有的客户端则用 RTSP 请求保活，两种都算活着。
// 客户端被 kill -9 后两者都会停，timeout 内必然被回收。
func (s *Session) Expired(now time.Time, timeout time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return now.Sub(s.lastActivityLocked()) > timeout
}

// Done 在会话被关闭后关闭，推流 goroutine select 它来及时退出。
func (s *Session) Done() <-chan struct{} {
	return s.ctx.Done()
}

// Context 返回会话的生命周期 context，Close 时取消。
// 调用方可以据此派生更短的生命周期（例如一次 PLAY 的推流 context），
// 会话关闭时子 context 会级联取消，不必再单独监听 Done。
func (s *Session) Context() context.Context {
	return s.ctx
}

// Close 关闭会话：状态置为 Teardown、取消生命周期 context、调用一次 OnClose。
// 重复调用是安全的（TEARDOWN 与超时清理可能同时发生）。
func (s *Session) Close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.state = StateTeardown
		s.mu.Unlock()
		s.cancel()
		if s.OnClose != nil {
			s.OnClose()
		}
	})
}

// Manager 全局会话表：map[string]*Session + 超时扫描。
type Manager struct {
	mu       sync.RWMutex
	sessions map[string]*Session
	timeout  time.Duration
	interval time.Duration
	seq      uint64

	// OnExpire 可选：Sweep 回收一个超时会话后调用，用来打日志/统计。
	// 必须在 Run 启动前设置。
	OnExpire func(*Session)
}

// NewManager 用默认 30s 扫描周期构造 Manager。
func NewManager(timeout time.Duration) *Manager {
	return NewManagerWithInterval(timeout, DefaultScanInterval)
}

// NewManagerWithInterval 可自定义扫描周期，主要给测试用。
func NewManagerWithInterval(timeout, interval time.Duration) *Manager {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	if interval <= 0 {
		interval = DefaultScanInterval
	}
	return &Manager{
		sessions: make(map[string]*Session),
		timeout:  timeout,
		interval: interval,
	}
}

// Timeout 返回会话超时时间，拼 Session 响应头时要用。
func (m *Manager) Timeout() time.Duration {
	return m.timeout
}

// NewSession 创建并登记一个会话，ID 形如 sess-1。
func (m *Manager) NewSession(remoteIP string) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	s := newSession(fmt.Sprintf("sess-%d", m.seq), remoteIP, time.Now())
	m.sessions[s.ID] = s
	return s
}

// Get 按 SessionID 查找会话。
func (m *Manager) Get(id string) (*Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[id]
	return s, ok
}

// Delete 从会话表里移除一个会话（不负责 Close，调用方按需处理）。
func (m *Manager) Delete(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[id]; !ok {
		return false
	}
	delete(m.sessions, id)
	return true
}

// Len 返回当前会话数。
func (m *Manager) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}

// Sweep 扫一遍会话表，把 now 时刻已超时的会话移除并 Close。
// 返回被回收的会话，方便调用方打日志/写测试。
//
// 注意：Close 和 OnExpire 都在释放 Manager 锁之后调用，
// 否则 OnClose/OnExpire 里再调 m.Delete 会死锁。
func (m *Manager) Sweep(now time.Time) []*Session {
	m.mu.Lock()
	var expired []*Session
	for id, s := range m.sessions {
		if s.Expired(now, m.timeout) {
			delete(m.sessions, id)
			expired = append(expired, s)
		}
	}
	m.mu.Unlock()

	for _, s := range expired {
		s.Close()
		if m.OnExpire != nil {
			m.OnExpire(s)
		}
	}
	return expired
}

// Run 后台循环：每 interval 扫一次，直到 ctx 取消。
func (m *Manager) Run(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			m.Sweep(now)
		}
	}
}

// CloseAll 关闭并移除所有会话（服务退出时用）。
func (m *Manager) CloseAll() {
	m.mu.Lock()
	sessions := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		sessions = append(sessions, s)
	}
	m.sessions = make(map[string]*Session)
	m.mu.Unlock()

	for _, s := range sessions {
		s.Close()
	}
}

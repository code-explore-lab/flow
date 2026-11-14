package rtcp

import (
	"slices"
	"sync"
	"time"
)

const (
	// DefaultNACKDelay 是发现缺口到第一次发 NACK 的宽限期：
	// 给乱序包一点时间自己补上，避免误报。
	DefaultNACKDelay = 20 * time.Millisecond
	// DefaultNACKInterval 是同一轨道两次 NACK 的最小间隔，防止高丢包时
	// 每个包都触发一个反馈包。
	DefaultNACKInterval = 20 * time.Millisecond
	// DefaultNACKAttempts 是单个 seq 最多请求几次重传，之后放弃，
	// 交给 jitter buffer 跳过缺口和 PLC 处理。
	DefaultNACKAttempts = 3

	defaultNACKMissing   = 256 // 最多同时跟踪的缺口 seq 数
	defaultNACKPerPacket = 32  // 单个 NACK 包最多携带的 seq 数
)

type nackMissing struct {
	seen     time.Time // 第一次发现缺口的时间
	attempts int       // 已经请求重传的次数
}

// NACKTracker 根据收到的 RTP seq 检测缺口并生成 NACK 重传请求。
//
// 接收 goroutine 调 Observe，播放/定时 goroutine 调 Pending；内部加锁。
// 时间由调用方传入，单测不需要 sleep。
type NACKTracker struct {
	mu           sync.Mutex
	started      bool
	highest      uint16
	mediaSSRC    uint32
	missing      map[uint16]*nackMissing
	lastSend     time.Time
	delay        time.Duration
	minInterval  time.Duration
	maxAttempts  int
	maxMissing   int
	maxPerPacket int
}

// NewNACKTracker 使用默认参数创建跟踪器。
func NewNACKTracker() *NACKTracker {
	return &NACKTracker{
		missing:      make(map[uint16]*nackMissing),
		delay:        DefaultNACKDelay,
		minInterval:  DefaultNACKInterval,
		maxAttempts:  DefaultNACKAttempts,
		maxMissing:   defaultNACKMissing,
		maxPerPacket: defaultNACKPerPacket,
	}
}

// Observe 记录一个收到的 RTP seq，返回媒体 SSRC 是否变化。
//
// 向前跳变时把中间缺失的 seq 记入待重传集合；收到旧 seq 时如果它在集合里，
// 说明重传已经到达，直接移除。SSRC 变化时清空状态，避免跨流误报。
func (t *NACKTracker) Observe(seq uint16, ssrc uint32, now time.Time) (ssrcChanged bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.started {
		t.started = true
		t.highest = seq
		t.mediaSSRC = ssrc
		return false
	}
	if ssrc != t.mediaSSRC {
		t.highest = seq
		t.mediaSSRC = ssrc
		clear(t.missing)
		t.lastSend = time.Time{}
		return true
	}

	delta := int16(seq - t.highest)
	switch {
	case delta > 0:
		for i := int16(1); i < delta; i++ {
			missing := t.highest + uint16(i)
			if _, ok := t.missing[missing]; ok {
				continue
			}
			if len(t.missing) >= t.maxMissing {
				break
			}
			t.missing[missing] = &nackMissing{seen: now}
		}
		t.highest = seq
	case delta < 0:
		delete(t.missing, seq) // 重传到达；不存在时 delete 是空操作
	}
	return false
}

// Pending 返回当前应该发 NACK 的 seq（已排序）。
//
// 只返回发现超过 delay、且距上次发送超过 minInterval 的缺口；返回时会
// 累加尝试次数并更新 lastSend。达到 maxAttempts 的 seq 直接放弃。
// 没有可发的返回 nil。
func (t *NACKTracker) Pending(now time.Time) []uint16 {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.started || len(t.missing) == 0 {
		return nil
	}
	if !t.lastSend.IsZero() && now.Sub(t.lastSend) < t.minInterval {
		return nil
	}

	var out []uint16
	for seq, m := range t.missing {
		if m.attempts >= t.maxAttempts {
			delete(t.missing, seq) // 超时未补齐，放弃
			continue
		}
		if now.Sub(m.seen) < t.delay {
			continue
		}
		out = append(out, seq)
	}
	if len(out) == 0 {
		return nil
	}
	slices.Sort(out)
	if len(out) > t.maxPerPacket {
		out = out[:t.maxPerPacket]
	}
	for _, seq := range out {
		t.missing[seq].attempts++
	}
	t.lastSend = now
	return out
}

// Delay 返回从发现缺口到允许发第一次 NACK 的宽限期。
func (t *NACKTracker) Delay() time.Duration {
	return t.delay
}

// MediaSSRC 返回当前跟踪的媒体 SSRC（NACK 的 media source 字段）。
func (t *NACKTracker) MediaSSRC() uint32 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mediaSSRC
}

// Outstanding 返回当前未补齐的缺口数（日志/测试用）。
func (t *NACKTracker) Outstanding() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.missing)
}

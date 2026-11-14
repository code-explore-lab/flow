// Package rtpbuf 提供接收侧 jitter buffer：按 RTP seq 重排、固定 playout
// 延迟输出、丢弃重复包、缺口等待超时后跳过。
//
// 目前只作为独立组件存在，RTSP 推流接收路径（internal/rtsp/record.go）
// 仍然收到就解包；后续 R-5（丢包隐藏）和 R-6（缺口检测 → NACK）会把
// Push/Pop 接进去。
//
// 时间完全由调用方传入（time.Time），单测不依赖 sleep；生产侧传 time.Now()。
package rtpbuf

import (
	"slices"
	"sync"
	"time"
)

const (
	// DefaultDelay 默认 playout 延迟：给乱序包留的等待窗口。
	DefaultDelay = 100 * time.Millisecond
	// DefaultCapacity 默认最多缓存的包数，防止乱序/异常 seq 撑爆内存。
	DefaultCapacity = 1024
)

// Packet 是缓存里的一个 RTP 报文。Payload 在 Push 时拷贝，调用方的
// 接收缓冲区可以复用。
type Packet struct {
	Seq       uint16
	Timestamp uint32
	Payload   []byte
}

type entry struct {
	pkt     Packet
	arrival time.Time
}

// Buffer 是单条轨的接收侧 jitter buffer。
//
// seq 用「相对最高已见 seq 的 int16 差值」扩展成 64 位（RFC 3550 的
// 做法），因此 65535 → 0 的回绕不影响排序；超过 ±32767 的跳变按旧包丢弃。
// Push（接收 goroutine）和 Pop（播放/NACK goroutine）可能并发，内部加锁。
type Buffer struct {
	mu         sync.Mutex
	delay      time.Duration
	maxPackets int

	entries    map[int64]*entry
	started    bool
	emitted    bool   // 是否已经输出或跳过过包，此后 next 固定
	highestExt int64  // 已见最大扩展 seq
	highestSeq uint16 // 与 highestExt 对应的原始 seq
	next       int64  // 下一个应输出的扩展 seq，emitted 前随最小包浮动
}

// New 使用默认容量创建 jitter buffer。delay <= 0 表示不额外缓冲。
func New(delay time.Duration) *Buffer {
	return NewWithCapacity(delay, DefaultCapacity)
}

// NewWithCapacity 创建指定容量的 jitter buffer。
// delay < 0 按 0 处理；maxPackets <= 0 使用 DefaultCapacity。
func NewWithCapacity(delay time.Duration, maxPackets int) *Buffer {
	if delay < 0 {
		delay = 0
	}
	if maxPackets <= 0 {
		maxPackets = DefaultCapacity
	}
	return &Buffer{
		delay:      delay,
		maxPackets: maxPackets,
		entries:    make(map[int64]*entry),
	}
}

// Push 放入一个收到的 RTP 报文，返回是否被接受。
//
// 以下情况丢弃并返回 false：seq 已经播放/被跳过（重复或过旧）、同一 seq
// 已在队列里、落后当前最高 seq 超过 maxPackets 的异常跳变。开始输出前
// 更早的乱序包仍会被接收，playout 游标随最小 seq 前移。容量满时先淘汰
// 最小 seq 的包。
func (b *Buffer) Push(seq uint16, ts uint32, payload []byte, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	var ext int64
	if !b.started {
		ext = int64(seq)
		b.started = true
		b.highestExt = ext
		b.highestSeq = seq
		b.next = ext
	} else {
		ext = b.highestExt + int64(int16(seq-b.highestSeq))
		// 已经输出/跳过的包不再接收；尚未开始输出时 next 还会随更早的
		// 乱序包前移，所以只拒绝明显过旧（超出容量窗口）的包。
		if b.emitted && ext < b.next {
			return false
		}
		if ext < b.highestExt-int64(b.maxPackets) {
			return false
		}
		if _, ok := b.entries[ext]; ok {
			return false // 重复包
		}
		if ext > b.highestExt {
			b.highestExt = ext
			b.highestSeq = seq
		}
	}

	if len(b.entries) >= b.maxPackets {
		b.evictOldestLocked()
	}
	b.entries[ext] = &entry{
		pkt: Packet{
			Seq:       seq,
			Timestamp: ts,
			Payload:   append([]byte(nil), payload...),
		},
		arrival: now,
	}
	return true
}

// Pop 返回当前已经可以播放的包，严格按 seq 顺序。
//
// 每个包在到达后至少等待 delay 才会输出。若 next 缺失（乱序/丢包），
// 等缺口之后最早的那个包也满 delay 后直接跳过缺口，避免无限等待；
// 迟到补上的包在跳过前仍会被正常插入。
func (b *Buffer) Pop(now time.Time) []Packet {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.started {
		return nil
	}
	// 还没开始输出时，更早的乱序包可能随后到达，下一包以当前最小为准。
	if !b.emitted {
		if ext, _, ok := b.lowestLocked(); ok {
			b.next = ext
		}
	}

	var out []Packet
	for {
		if e, ok := b.entries[b.next]; ok {
			if now.Before(e.arrival.Add(b.delay)) {
				break
			}
			out = append(out, e.pkt)
			delete(b.entries, b.next)
			b.next++
			b.emitted = true
			continue
		}

		// next 缺失：找到缺口之后最小的包，看它为重排等够 delay 没有。
		ext, e, ok := b.lowestLocked()
		if !ok || ext <= b.next {
			break
		}
		if now.Sub(e.arrival) < b.delay {
			break
		}
		// 缺口等满 delay 仍未补上，跳过它；此后游标固定。
		b.next = ext
		b.emitted = true
	}
	return out
}

// Len 返回当前缓存的包数。
func (b *Buffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.entries)
}

// Delay 返回 playout 延迟（调用方用它安排下一次 Pop）。
func (b *Buffer) Delay() time.Duration {
	return b.delay
}

// Reset 清空缓冲和 seq 游标，用于 SSRC 变化或流重新开始。
// 与 Push/Pop 并发安全。
func (b *Buffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	clear(b.entries)
	b.started = false
	b.emitted = false
	b.highestExt = 0
	b.highestSeq = 0
	b.next = 0
}

// Drain 忽略 playout 延迟，按 seq 顺序返回所有缓存包并清空状态。
// 用于推流结束时冲刷尾包，避免最后一段数据卡在缓冲里。
func (b *Buffer) Drain() []Packet {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.entries) == 0 {
		return nil
	}
	exts := make([]int64, 0, len(b.entries))
	for ext := range b.entries {
		exts = append(exts, ext)
	}
	slices.Sort(exts)
	out := make([]Packet, 0, len(exts))
	for _, ext := range exts {
		out = append(out, b.entries[ext].pkt)
	}
	clear(b.entries)
	b.started = false
	b.emitted = false
	b.highestExt = 0
	b.highestSeq = 0
	b.next = 0
	return out
}

// lowestLocked 返回队列里最小的扩展 seq；调用方持锁。
func (b *Buffer) lowestLocked() (int64, *entry, bool) {
	var (
		ext int64
		e   *entry
		ok  bool
	)
	for k, v := range b.entries {
		if !ok || k < ext {
			ext, e, ok = k, v, true
		}
	}
	return ext, e, ok
}

// evictOldestLocked 容量满时淘汰最旧的包；调用方持锁。
func (b *Buffer) evictOldestLocked() {
	ext, _, ok := b.lowestLocked()
	if ok {
		delete(b.entries, ext)
	}
}

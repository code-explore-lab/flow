package source

import "sync"

// RingBuffer 是单生产者、多消费者的环形缓冲区。
//
// 5.1.4：源 goroutine 只往这里写，永远不阻塞，也不会被慢客户端拖住；
// 每个订阅者（Reader）自己维护读到的序号（seq），慢的读者会发现自己
// 的序号已经被新数据覆盖，于是跳到最老的可用帧并记一次丢帧。
//
// 内存上只保留最近 capacity 帧，Data 是切片头拷贝，底层字节由源
// 一次性分配、所有读者共享（只读），所以多路开销很小。
type RingBuffer[T any] struct {
	mu      sync.Mutex
	buf     []T
	nextSeq uint64 // 下一帧的序号，同时也是累计写入数
	closed  bool
	subs    map[int]*Reader[T]
	nextID  int
}

// NewRingBuffer 创建指定容量的环形缓冲区。容量 <= 0 时用默认值 64。
func NewRingBuffer[T any](capacity int) *RingBuffer[T] {
	if capacity <= 0 {
		capacity = 64
	}
	return &RingBuffer[T]{
		buf:  make([]T, capacity),
		subs: make(map[int]*Reader[T]),
	}
}

// Capacity 返回环形缓冲区能保留的帧数。
func (r *RingBuffer[T]) Capacity() int {
	return len(r.buf)
}

// Write 写入一帧并唤醒所有读者。写满时覆盖最老的帧，
// 生产者不做任何等待——这正是“慢消费者不拖慢整体”的关键。
func (r *RingBuffer[T]) Write(v T) {
	r.mu.Lock()         // ① 加锁
	defer r.mu.Unlock() // ⑤ 函数返回时解锁
	if r.closed {       // ② 已关闭就丢弃
		return
	}
	r.buf[r.nextSeq%uint64(len(r.buf))] = v // ③ 写入槽位
	r.nextSeq++                             // ④ 序号自增
	for _, rd := range r.subs {             // ⑥ 遍历所有读者
		select {
		case rd.notify <- struct{}{}: // ⑦ 尝试发信号
		default: // ⑧ 发不进就跳过
		}
	}
}

// Subscribe 注册一个读者，从当前写位置开始读（不重放旧帧）。
// preload 里的帧会先于实时帧返回，用来给中途加入的客户端补 SPS/PPS/IDR。
// 返回的 Reader.Close 只退订，不会停止源本身。
func (r *RingBuffer[T]) Subscribe(preload ...T) *Reader[T] {
	r.mu.Lock()
	defer r.mu.Unlock()

	rd := &Reader[T]{
		ring:    r,
		id:      r.nextID,
		pos:     r.nextSeq,
		preload: append([]T(nil), preload...),
		notify:  make(chan struct{}, 1),
		cancel:  make(chan struct{}),
	}
	r.nextID++
	if r.closed {
		// 源已经结束：直接让读者读不到数据，避免挂在等待上。
		rd.closed = true
		return rd
	}
	r.subs[rd.id] = rd
	return rd
}

// Close 关闭环形缓冲区：唤醒所有还在等的读者，让它们把剩余帧读完再退出。
// 之后的 Write 会被忽略。
func (r *RingBuffer[T]) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	for _, rd := range r.subs {
		close(rd.notify)
	}
}

// oldestLocked 返回缓冲区里还能读到的最老序号。
func (r *RingBuffer[T]) oldestLocked() uint64 {
	if r.nextSeq > uint64(len(r.buf)) {
		return r.nextSeq - uint64(len(r.buf))
	}
	return 0
}

// Reader 是某个消费者在环形缓冲区上的读游标。
// 一个 Reader 只应被一个 goroutine 调用 Next。
type Reader[T any] struct {
	ring    *RingBuffer[T]
	id      int
	pos     uint64 // 下一个要读的 seq
	preload []T
	notify  chan struct{}
	cancel  chan struct{}
	dropped uint64
	closed  bool // 受 ring.mu 保护
	once    sync.Once
}

// Next 返回下一帧；缓冲区暂时没有新数据时会阻塞等待。
//
//   - done 关闭（会话结束）或 Reader.Close 之后返回 false；
//   - 源结束且剩余帧读完后返回 false；
//   - 如果本读者落后太多、要读的帧已被覆盖，会跳到最老的可用帧，
//     把跳过的数量记到 Dropped()，然后继续返回可读的最新数据。
func (rd *Reader[T]) Next(done <-chan struct{}) (T, bool) {
	var zero T
	r := rd.ring
	for {
		r.mu.Lock()
		if rd.closed {
			r.mu.Unlock()
			return zero, false
		}
		if len(rd.preload) > 0 {
			v := rd.preload[0]
			rd.preload = rd.preload[1:]
			r.mu.Unlock()
			return v, true
		}

		oldest := r.oldestLocked()
		if rd.pos < oldest {
			rd.dropped += oldest - rd.pos
			rd.pos = oldest
		}
		if rd.pos < r.nextSeq {
			v := r.buf[int(rd.pos%uint64(len(r.buf)))]
			rd.pos++
			r.mu.Unlock()
			return v, true
		}
		if r.closed {
			r.mu.Unlock()
			return zero, false
		}
		notify := rd.notify
		r.mu.Unlock()

		select {
		case <-done:
			rd.Close()
			return zero, false
		case <-rd.cancel:
			return zero, false
		case <-notify:
		}
	}
}

// Close 退订。幂等；会唤醒阻塞在 Next 上的调用。
func (rd *Reader[T]) Close() {
	rd.once.Do(func() {
		rd.ring.mu.Lock()
		rd.closed = true
		delete(rd.ring.subs, rd.id)
		rd.ring.mu.Unlock()
		close(rd.cancel)
	})
}

// Dropped 返回这个读者因为落后而被覆盖掉的帧数。
func (rd *Reader[T]) Dropped() uint64 {
	rd.ring.mu.Lock()
	defer rd.ring.mu.Unlock()
	return rd.dropped
}

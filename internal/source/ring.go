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
	oldest  uint64 // 还能读到的最老序号
	closed  bool
	subs    map[int]*Reader[T] // key=reader.id, value = *Reader[T]
	nextID  int

	// 可选的字节预算：weigh 返回一帧的字节数，maxBytes 是缓冲上限。
	// 多 slice 的一帧包含多个 NALU，只按个数限制会让缓冲的时间窗口
	// 随 slice 数成比例缩短；按字节限制才能稳定保留同样的时长。
	weigh    func(T) int
	maxBytes int
	bytes    int
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

// NewRingBufferWithBudget 创建同时受帧数和字节数限制的环形缓冲区。
// capacity 是硬上限（防止极小帧吃掉过多槽位），maxBytes 是主窗口；
// 超过字节预算时从最老的帧开始淘汰，读者会把这部分计入 Dropped。
func NewRingBufferWithBudget[T any](capacity, maxBytes int, weigh func(T) int) *RingBuffer[T] {
	r := NewRingBuffer[T](capacity)
	r.maxBytes = maxBytes
	r.weigh = weigh
	return r
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
	capn := uint64(len(r.buf))
	slot := r.nextSeq % capn
	if r.nextSeq >= capn {
		// 覆盖的槽位如果已经被字节预算淘汰过，就不能再重复扣减。
		if s := r.nextSeq - capn; s >= r.oldest {
			r.releaseLocked(r.buf[slot])
		}
	}
	r.buf[slot] = v // ③ 写入槽位
	r.nextSeq++     // ④ 序号自增
	r.retainLocked(v)
	if r.nextSeq > capn && r.nextSeq-capn > r.oldest {
		r.oldest = r.nextSeq - capn
	}
	// ⑤ 字节预算：从最老的帧开始淘汰，直到回到预算内。
	// 至少保留刚写入的帧：单个超大 IDR 超过预算时也不能立刻丢掉，
	// 否则所有读者都拿不到关键帧。
	for r.bytes > r.maxBytes && r.oldest+1 < r.nextSeq {
		r.releaseLocked(r.buf[r.oldest%capn])
		r.oldest++
	}
	for _, rd := range r.subs { // ⑥ 遍历所有读者
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
	return r.oldest
}

// retainLocked / releaseLocked 维护字节预算计数；没有配置预算时是空操作。
func (r *RingBuffer[T]) retainLocked(v T) {
	if r.weigh != nil {
		r.bytes += r.weigh(v)
	}
}

func (r *RingBuffer[T]) releaseLocked(v T) {
	if r.weigh != nil {
		r.bytes -= r.weigh(v)
	}
	if r.bytes < 0 {
		r.bytes = 0
	}
}

// Reader 是某个消费者在环形缓冲区上的读游标。
// 一个 Reader 只应被一个 goroutine 调用 Next。
type Reader[T any] struct {
	ring    *RingBuffer[T]
	id      int
	pos     uint64 // 下一个要读的 seq
	preload []T    //External: 预加载的帧，优先于实时帧返回
	notify  chan struct{}
	cancel  chan struct{}
	dropped uint64 //追不上生产者，被覆盖而丢掉的帧
	closed  bool   // 受 ring.mu 保护
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
		if rd.pos < oldest { //读者游标落后太多， 帧已经被覆盖
			rd.dropped += oldest - rd.pos
			rd.pos = oldest
		}
		if rd.pos < r.nextSeq { // 取出新帧
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

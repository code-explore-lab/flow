package rtp

import "sync"

// 每条轨默认保留的发送历史。512 个包在 25fps、每帧若干分片时大约覆盖
// 数秒；1 MiB 是 512 个 1400 字节分片的约 1.4 倍，够放满一轮又不会失控。
const (
	DefaultSendCachePackets = 512
	DefaultSendCacheBytes   = 1 << 20
)

// cacheEntry 是一条已发送的完整 RTP 包（含头部），重传时原样发出即可。
type cacheEntry struct {
	seq  uint16
	data []byte
}

// SendCache 保存一条媒体轨最近发送的 RTP 包，按 seq 索引，是 R-7
// （解析客户端 NACK 后重传）的数据来源。
//
// 用固定长度的环形数组而不是 map：容量只有几百，NACK 是低频事件，
// 线性扫描足够快，也省掉了 map 在 seq 回绕时的歧义和额外的淘汰队列。
// 容量同时受包数和字节数限制，先按最旧顺序淘汰，保证内存有上限。
//
// 并发安全：发送 goroutine 调用 Add，RTCP 读循环调用 Get。
type SendCache struct {
	mu         sync.Mutex
	maxPackets int
	maxBytes   int
	bytes      int
	ring       []cacheEntry
	head       int // 最旧元素下标
	count      int // 有效元素个数
}

// NewSendCache 创建缓存。maxPackets/maxBytes 非正时分别使用
// DefaultSendCachePackets/DefaultSendCacheBytes。
func NewSendCache(maxPackets, maxBytes int) *SendCache {
	if maxPackets <= 0 {
		maxPackets = DefaultSendCachePackets
	}
	if maxBytes <= 0 {
		maxBytes = DefaultSendCacheBytes
	}
	return &SendCache{
		maxPackets: maxPackets,
		maxBytes:   maxBytes,
		ring:       make([]cacheEntry, maxPackets),
	}
}

// Add 记录一个已经成功发送的完整 RTP 包。
//
// data 的所有权移交给缓存：调用方不应再修改它（发送路径每包新分配
// 一个切片，之后只读，因此不需要拷贝）。空包和单包超过字节预算的
// 包直接丢弃——后者缓存了也会立刻把其他包全部挤掉。
func (c *SendCache) Add(seq uint16, data []byte) {
	if len(data) == 0 || len(data) > c.maxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.evictLocked(len(data))
	// 同一 seq 重复出现（理论上要走完 65536 个 seq 才会回绕到旧值，
	// 这里只是兜底）时原地替换，保持它在队列里的年龄不变。
	if i, ok := c.findLocked(seq); ok {
		c.bytes += len(data) - len(c.ring[i].data)
		c.ring[i] = cacheEntry{seq: seq, data: data}
		return
	}
	idx := (c.head + c.count) % c.maxPackets
	c.ring[idx] = cacheEntry{seq: seq, data: data}
	c.count++
	c.bytes += len(data)
}

// Get 按 seq 取回已发送的包。返回的切片由缓存持有，调用方只读。
func (c *SendCache) Get(seq uint16) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if i, ok := c.findLocked(seq); ok {
		return c.ring[i].data, true
	}
	return nil, false
}

// Len 返回当前缓存的包数。
func (c *SendCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

// Bytes 返回当前缓存的字节数（含 RTP 头）。
func (c *SendCache) Bytes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bytes
}

// evictLocked 在写入 incoming 字节前从最旧开始淘汰，直到包数和字节数
// 都留得下。incoming 已保证不超过 maxBytes，所以循环一定能结束。
func (c *SendCache) evictLocked(incoming int) {
	for c.count > 0 && (c.count == c.maxPackets || c.bytes+incoming > c.maxBytes) {
		c.bytes -= len(c.ring[c.head].data)
		c.ring[c.head] = cacheEntry{}
		c.head = (c.head + 1) % c.maxPackets
		c.count--
	}
}

// findLocked 从最旧到最新查找 seq。
func (c *SendCache) findLocked(seq uint16) (int, bool) {
	for i := 0; i < c.count; i++ {
		idx := (c.head + i) % c.maxPackets
		if c.ring[idx].seq == seq {
			return idx, true
		}
	}
	return 0, false
}

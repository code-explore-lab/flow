package rtp

import (
	"bytes"
	"sync"
	"testing"
)

func packet(n byte, size int) []byte {
	return bytes.Repeat([]byte{n}, size)
}

// TestSendCacheEvictsOldest 超过包数上限后按最旧优先淘汰，seq 查找仍准确。
func TestSendCacheEvictsOldest(t *testing.T) {
	c := NewSendCache(3, 1<<20)
	for seq := uint16(10); seq <= 13; seq++ {
		c.Add(seq, packet(byte(seq), 10))
	}

	if got := c.Len(); got != 3 {
		t.Fatalf("Len = %d, want 3", got)
	}
	if got := c.Bytes(); got != 30 {
		t.Fatalf("Bytes = %d, want 30", got)
	}
	if _, ok := c.Get(10); ok {
		t.Fatal("seq 10 should be evicted")
	}
	for seq := uint16(11); seq <= 13; seq++ {
		data, ok := c.Get(seq)
		if !ok {
			t.Fatalf("seq %d missing", seq)
		}
		if !bytes.Equal(data, packet(byte(seq), 10)) {
			t.Fatalf("seq %d data = %v", seq, data)
		}
	}

	// 继续追加，淘汰顺序必须严格从旧到新。
	c.Add(14, packet(14, 10))
	if _, ok := c.Get(11); ok {
		t.Fatal("seq 11 should be evicted after adding 14")
	}
	for seq := uint16(12); seq <= 14; seq++ {
		if _, ok := c.Get(seq); !ok {
			t.Fatalf("seq %d missing", seq)
		}
	}
}

// TestSendCacheByteBudget 字节预算比包数先触发时，也要淘汰最旧的包。
func TestSendCacheByteBudget(t *testing.T) {
	c := NewSendCache(10, 100)
	c.Add(1, packet(1, 40))
	c.Add(2, packet(2, 40))
	c.Add(3, packet(3, 40)) // 120 > 100，淘汰 seq 1

	if _, ok := c.Get(1); ok {
		t.Fatal("seq 1 should be evicted by byte budget")
	}
	if _, ok := c.Get(2); !ok {
		t.Fatal("seq 2 missing")
	}
	if _, ok := c.Get(3); !ok {
		t.Fatal("seq 3 missing")
	}
	if got := c.Bytes(); got != 80 {
		t.Fatalf("Bytes = %d, want 80", got)
	}
	if got := c.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2", got)
	}

	// 单包超过整个字节预算时不缓存，也不影响已有内容。
	c.Add(4, packet(4, 200))
	if _, ok := c.Get(4); ok {
		t.Fatal("oversized packet should not be cached")
	}
	if got := c.Len(); got != 2 {
		t.Fatalf("Len = %d after oversized add, want 2", got)
	}
}

// TestSendCacheSeqWraparound seq 从 65535 回绕到 0 后两个包都能取到。
func TestSendCacheSeqWraparound(t *testing.T) {
	c := NewSendCache(4, 1<<20)
	c.Add(65535, packet(1, 8))
	c.Add(0, packet(2, 8))

	for seq, want := range map[uint16][]byte{65535: packet(1, 8), 0: packet(2, 8)} {
		got, ok := c.Get(seq)
		if !ok {
			t.Fatalf("seq %d missing", seq)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("seq %d data = %v, want %v", seq, got, want)
		}
	}
}

// TestSendCacheDuplicateSeq 同一 seq 再次写入时原地替换，不重复占位。
func TestSendCacheDuplicateSeq(t *testing.T) {
	c := NewSendCache(4, 1<<20)
	c.Add(1, packet(1, 10))
	c.Add(2, packet(2, 10))
	c.Add(1, packet(9, 20))

	if got := c.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2", got)
	}
	if got := c.Bytes(); got != 30 {
		t.Fatalf("Bytes = %d, want 30", got)
	}
	data, ok := c.Get(1)
	if !ok || !bytes.Equal(data, packet(9, 20)) {
		t.Fatalf("Get(1) = %v, %v; want replaced payload", data, ok)
	}
}

// TestSendCacheEmpty 空缓存查询失败，空包不缓存。
func TestSendCacheEmpty(t *testing.T) {
	c := NewSendCache(2, 1<<20)
	if _, ok := c.Get(1); ok {
		t.Fatal("empty cache should not return a packet")
	}
	if c.Len() != 0 || c.Bytes() != 0 {
		t.Fatalf("Len/Bytes = %d/%d, want 0/0", c.Len(), c.Bytes())
	}
	c.Add(1, nil)
	if c.Len() != 0 {
		t.Fatalf("Len = %d after empty add, want 0", c.Len())
	}
}

// TestSendCacheDefaults 非正参数回退到默认容量。
func TestSendCacheDefaults(t *testing.T) {
	c := NewSendCache(0, 0)
	if c.maxPackets != DefaultSendCachePackets || c.maxBytes != DefaultSendCacheBytes {
		t.Fatalf("defaults = %d/%d, want %d/%d",
			c.maxPackets, c.maxBytes, DefaultSendCachePackets, DefaultSendCacheBytes)
	}
	if len(c.ring) != DefaultSendCachePackets {
		t.Fatalf("ring len = %d, want %d", len(c.ring), DefaultSendCachePackets)
	}
}

// TestSendCacheConcurrent 发送与重传查询并发执行，-race 下不报错，
// 两个上限始终成立。
func TestSendCacheConcurrent(t *testing.T) {
	const packets = 200
	c := NewSendCache(32, 4096)

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(base uint16) {
			defer wg.Done()
			for i := 0; i < packets; i++ {
				seq := base + uint16(i)
				c.Add(seq, packet(byte(seq), 64))
				c.Get(seq - 5)
				c.Len()
				c.Bytes()
			}
		}(uint16(w * packets))
	}
	wg.Wait()

	if got := c.Len(); got > 32 {
		t.Fatalf("Len = %d, exceeded maxPackets", got)
	}
	if got := c.Bytes(); got > 4096 {
		t.Fatalf("Bytes = %d, exceeded maxBytes", got)
	}
}

package rtpbuf

import (
	"sync"
	"testing"
	"time"
)

var t0 = time.Unix(1000, 0)

func seqs(pkts []Packet) []uint16 {
	out := make([]uint16, len(pkts))
	for i, p := range pkts {
		out[i] = p.Seq
	}
	return out
}

func wantSeqs(t *testing.T, got []Packet, want ...uint16) {
	t.Helper()
	g := seqs(got)
	if len(g) != len(want) {
		t.Fatalf("got seqs %v, want %v", g, want)
	}
	for i := range want {
		if g[i] != want[i] {
			t.Fatalf("got seqs %v, want %v", g, want)
		}
	}
}

// TestPopRespectsDelay 包在 arrival+delay 之前不能输出。
func TestPopRespectsDelay(t *testing.T) {
	b := NewWithCapacity(20*time.Millisecond, 8)
	if !b.Push(10, 100, []byte{1}, t0) {
		t.Fatal("first packet rejected")
	}
	if got := b.Pop(t0.Add(19 * time.Millisecond)); len(got) != 0 {
		t.Fatalf("popped %d packets before delay", len(got))
	}
	got := b.Pop(t0.Add(20 * time.Millisecond))
	wantSeqs(t, got, 10)
	if got[0].Timestamp != 100 {
		t.Fatalf("timestamp = %d, want 100", got[0].Timestamp)
	}
	if b.Len() != 0 {
		t.Fatalf("Len = %d after pop, want 0", b.Len())
	}
}

// TestReorder 乱序到达的包按 seq 输出。
func TestReorder(t *testing.T) {
	b := NewWithCapacity(20*time.Millisecond, 8)
	b.Push(12, 12, nil, t0)
	b.Push(10, 10, nil, t0.Add(1*time.Millisecond))
	b.Push(11, 11, nil, t0.Add(2*time.Millisecond))
	b.Push(13, 13, nil, t0.Add(3*time.Millisecond))

	if got := b.Pop(t0.Add(19 * time.Millisecond)); len(got) != 0 {
		t.Fatalf("popped before the head packet is due: %v", seqs(got))
	}
	wantSeqs(t, b.Pop(t0.Add(30*time.Millisecond)), 10, 11, 12, 13)
}

// TestDuplicateDropped 重复包只接受一次，已播放的 seq 再到达也丢弃。
func TestDuplicateDropped(t *testing.T) {
	b := NewWithCapacity(20*time.Millisecond, 8)
	if !b.Push(5, 5, nil, t0) {
		t.Fatal("first packet rejected")
	}
	if b.Push(5, 5, nil, t0.Add(time.Millisecond)) {
		t.Fatal("duplicate while queued should be rejected")
	}
	wantSeqs(t, b.Pop(t0.Add(20*time.Millisecond)), 5)
	if b.Push(5, 5, nil, t0.Add(30*time.Millisecond)) {
		t.Fatal("duplicate after playout should be rejected")
	}
}

// TestLongGapSkipped 缺口等满一个 delay 后跳过；迟到的包不再插入。
func TestLongGapSkipped(t *testing.T) {
	const delay = 30 * time.Millisecond
	b := NewWithCapacity(delay, 8)
	b.Push(1, 1, nil, t0)
	b.Push(2, 2, nil, t0)
	b.Push(4, 4, nil, t0.Add(25*time.Millisecond))
	b.Push(5, 5, nil, t0.Add(25*time.Millisecond))

	// 1、2 已经可播，但缺口 3 之后的包才等了 5ms，先不跳。
	wantSeqs(t, b.Pop(t0.Add(30*time.Millisecond)), 1, 2)
	// 缺口后第一个包仍未等满一个 delay。
	if got := b.Pop(t0.Add(50 * time.Millisecond)); len(got) != 0 {
		t.Fatalf("skipped gap too early: %v", seqs(got))
	}
	// 等满 delay 后跳过丢失的 3，输出 4、5。
	wantSeqs(t, b.Pop(t0.Add(55*time.Millisecond)), 4, 5)
	// 迟到的 3 已经被跳过，不能再插回。
	if b.Push(3, 3, nil, t0.Add(60*time.Millisecond)) {
		t.Fatal("late packet after gap skip should be rejected")
	}
}

// TestGapFilledBeforeTimeout 缺口在超时前补上时按序输出。
func TestGapFilledBeforeTimeout(t *testing.T) {
	const delay = 30 * time.Millisecond
	b := NewWithCapacity(delay, 8)
	b.Push(1, 1, nil, t0)
	b.Push(2, 2, nil, t0)
	b.Push(4, 4, nil, t0.Add(5*time.Millisecond))
	b.Push(3, 3, nil, t0.Add(10*time.Millisecond))

	wantSeqs(t, b.Pop(t0.Add(40*time.Millisecond)), 1, 2, 3, 4)
}

// TestArrivalJitterPreservesOrder 到达抖动不影响输出顺序和时间戳。
func TestArrivalJitterPreservesOrder(t *testing.T) {
	const delay = 50 * time.Millisecond
	b := NewWithCapacity(delay, 8)
	b.Push(3, 300, []byte{3}, t0.Add(30*time.Millisecond))
	b.Push(1, 100, []byte{1}, t0)
	b.Push(2, 200, []byte{2}, t0.Add(5*time.Millisecond))

	if got := b.Pop(t0.Add(49 * time.Millisecond)); len(got) != 0 {
		t.Fatalf("popped before delay: %v", seqs(got))
	}
	got := b.Pop(t0.Add(100 * time.Millisecond))
	wantSeqs(t, got, 1, 2, 3)
	for i, want := range []uint32{100, 200, 300} {
		if got[i].Timestamp != want {
			t.Fatalf("packet %d ts = %d, want %d", i, got[i].Timestamp, want)
		}
		if got[i].Payload[0] != byte(i+1) {
			t.Fatalf("packet %d payload = %v", i, got[i].Payload)
		}
	}
}

// TestSeqWraparound 65535 → 0 的回绕按连续 seq 处理。
func TestSeqWraparound(t *testing.T) {
	b := NewWithCapacity(20*time.Millisecond, 8)
	b.Push(65534, 1, nil, t0)
	b.Push(0, 2, nil, t0.Add(1*time.Millisecond))
	b.Push(65535, 3, nil, t0.Add(2*time.Millisecond))
	b.Push(1, 4, nil, t0.Add(3*time.Millisecond))

	wantSeqs(t, b.Pop(t0.Add(30*time.Millisecond)), 65534, 65535, 0, 1)
	if b.Push(65533, 0, nil, t0.Add(31*time.Millisecond)) {
		t.Fatal("seq before the playout point should be rejected")
	}
}

// TestCapacityEvictsOldest 容量满时淘汰最小 seq，Pop 会跳过被淘汰的缺口。
func TestCapacityEvictsOldest(t *testing.T) {
	const delay = 20 * time.Millisecond
	b := NewWithCapacity(delay, 2)
	for seq := uint16(1); seq <= 3; seq++ {
		if !b.Push(seq, uint32(seq), nil, t0) {
			t.Fatalf("seq %d rejected", seq)
		}
	}
	if b.Len() != 2 {
		t.Fatalf("Len = %d, want 2", b.Len())
	}
	wantSeqs(t, b.Pop(t0.Add(delay)), 2, 3)
}

// TestEmptyPop 空缓冲不 panic。
func TestEmptyPop(t *testing.T) {
	b := New(0)
	if got := b.Pop(t0); got != nil {
		t.Fatalf("Pop on empty buffer = %v", got)
	}
	if b.Len() != 0 {
		t.Fatalf("Len = %d, want 0", b.Len())
	}
}

// TestZeroDelay 零延迟时包立即可播，缺口也立即跳过。
func TestZeroDelay(t *testing.T) {
	b := New(0)
	b.Push(1, 1, nil, t0)
	b.Push(3, 3, nil, t0)
	wantSeqs(t, b.Pop(t0), 1, 3)
}

// TestDefaults 非法参数回退到默认值。
func TestDefaults(t *testing.T) {
	b := NewWithCapacity(-time.Second, 0)
	if b.delay != 0 {
		t.Fatalf("delay = %s, want 0", b.delay)
	}
	if b.maxPackets != DefaultCapacity {
		t.Fatalf("maxPackets = %d, want %d", b.maxPackets, DefaultCapacity)
	}
}

// TestConcurrentPushPop Push/Pop 并发执行，-race 下不报错且包不丢不重。
func TestConcurrentPushPop(t *testing.T) {
	b := NewWithCapacity(0, 2048)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 500 {
			b.Push(uint16(i), uint32(i), []byte{1}, t0)
		}
	}()
	var total int
	go func() {
		defer wg.Done()
		for range 1000 {
			total += len(b.Pop(t0.Add(time.Second)))
		}
	}()
	wg.Wait()
	total += len(b.Pop(t0.Add(2 * time.Second)))

	if total != 500 {
		t.Fatalf("popped %d packets, want 500", total)
	}
}

func TestDrainReturnsAllInOrder(t *testing.T) {
	b := New(time.Second)
	b.Push(3, 30, []byte{3}, t0)
	b.Push(1, 10, []byte{1}, t0)
	b.Push(2, 20, []byte{2}, t0)
	got := b.Drain()
	wantSeqs(t, got, 1, 2, 3)
	if b.Len() != 0 {
		t.Fatalf("len after drain = %d", b.Len())
	}
	if got := b.Pop(t0.Add(time.Hour)); len(got) != 0 {
		t.Fatalf("pop after drain = %v", seqs(got))
	}
	// Drain 后从头开始，可以不连续地接收新 seq。
	if !b.Push(100, 1, []byte{9}, t0) {
		t.Fatal("push after drain rejected")
	}
	if got := b.Pop(t0.Add(time.Hour)); len(got) != 1 || got[0].Seq != 100 {
		t.Fatalf("pop after reset = %v", seqs(got))
	}
}

func TestReset(t *testing.T) {
	b := New(time.Second)
	b.Push(5, 50, []byte{5}, t0)
	b.Reset()
	if b.Len() != 0 {
		t.Fatalf("len after reset = %d", b.Len())
	}
	if got := b.Pop(t0.Add(time.Hour)); len(got) != 0 {
		t.Fatalf("pop after reset = %v", seqs(got))
	}
	// 游标也重置了：比旧最高 seq 更小的新序列可以重新开始。
	if !b.Push(1, 10, []byte{1}, t0) {
		t.Fatal("push after reset rejected")
	}
	if got := b.Pop(t0.Add(time.Hour)); len(got) != 1 || got[0].Seq != 1 {
		t.Fatalf("pop after reset = %v", seqs(got))
	}
}

func TestDelay(t *testing.T) {
	if got := New(30 * time.Millisecond).Delay(); got != 30*time.Millisecond {
		t.Fatalf("delay = %v", got)
	}
	if got := New(-time.Second).Delay(); got != 0 {
		t.Fatalf("negative delay = %v, want 0", got)
	}
}

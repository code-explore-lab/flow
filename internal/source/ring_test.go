package source

import (
	"sync"
	"testing"
	"time"
)

// drain 从 Reader 读到没有数据为止（源已经 Close）。
func drain[T any](t *testing.T, rd *Reader[T]) []T {
	t.Helper()
	var out []T
	done := make(chan struct{})
	timer := time.AfterFunc(5*time.Second, func() { close(done) })
	defer timer.Stop()
	for {
		v, ok := rd.Next(done)
		if !ok {
			return out
		}
		out = append(out, v)
	}
}

func TestRingBufferWriteReadWrapOrder(t *testing.T) {
	r := NewRingBuffer[int](4)
	rd := r.Subscribe()
	defer rd.Close()

	// 写一帧、读一帧，强制游标绕环多圈
	for i := 0; i < 20; i++ {
		r.Write(i)
		v, ok := rd.Next(nil)
		if !ok {
			t.Fatalf("Next returned false at %d", i)
		}
		if v != i {
			t.Fatalf("got %d, want %d", v, i)
		}
	}
	if d := rd.Dropped(); d != 0 {
		t.Fatalf("dropped = %d, want 0 (reader kept up)", d)
	}
}

// TestRingBufferByteBudgetEvictsOldest 字节预算比帧数先触顶时，从最老的帧
// 开始淘汰；读者只拿到预算内的最后几帧，并把跳过的计入 Dropped。
func TestRingBufferByteBudgetEvictsOldest(t *testing.T) {
	// 每帧 1 字节，预算 4 字节：帧数上限 100 不生效，字节预算生效。
	r := NewRingBufferWithBudget(100, 4, func(int) int { return 1 })
	rd := r.Subscribe()
	defer rd.Close()

	for i := 0; i < 10; i++ {
		r.Write(i)
	}
	r.Close()

	got := drain(t, rd)
	want := []int{6, 7, 8, 9}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	if d := rd.Dropped(); d != 6 {
		t.Fatalf("dropped = %d, want 6", d)
	}
}

// TestRingBufferByteBudgetSizes 用变长帧验证按字节而不是按帧数淘汰：
// 两个 60 字节的帧就该挤掉一个 100 字节的帧。
func TestRingBufferByteBudgetSizes(t *testing.T) {
	r := NewRingBufferWithBudget(100, 200, func(n int) int { return n })
	rd := r.Subscribe()
	defer rd.Close()

	r.Write(100)
	r.Write(60) // 160 <= 200，都还在
	r.Write(60) // 总量 220 > 200，最老的 100 被淘汰
	r.Close()

	got := drain(t, rd)
	want := []int{60, 60}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	if d := rd.Dropped(); d != 1 {
		t.Fatalf("dropped = %d, want 1", d)
	}
}

// TestRingBufferByteBudgetCapacityStillBinds 极小帧先把槽位写满时，
// 帧数上限仍然生效，且字节计数不会因此变成负数。
func TestRingBufferByteBudgetCapacityStillBinds(t *testing.T) {
	r := NewRingBufferWithBudget(4, 1000, func(int) int { return 1 })
	rd := r.Subscribe()
	defer rd.Close()

	for i := 0; i < 10; i++ {
		r.Write(i)
	}
	r.Close()

	got := drain(t, rd)
	want := []int{6, 7, 8, 9}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	if d := rd.Dropped(); d != 6 {
		t.Fatalf("dropped = %d, want 6", d)
	}
}

// TestRingBufferByteBudgetKeepsNewestOversizedFrame 单帧超过预算时仍要
// 保留最新一帧，不能写进去就被自己的预算淘汰。
func TestRingBufferByteBudgetKeepsNewestOversizedFrame(t *testing.T) {
	r := NewRingBufferWithBudget(4, 10, func(int) int { return 1 })
	rd := r.Subscribe()
	defer rd.Close()

	for i := 0; i < 20; i++ {
		r.Write(i) // 每帧 1 字节，预算 10 字节
	}
	// 再写一个 100 字节的“IDR”，远超预算。
	r.Write(100)
	r.Close()

	got := drain(t, rd)
	if len(got) == 0 || got[len(got)-1] != 100 {
		t.Fatalf("got %v, want the oversized frame retained last", got)
	}
}

func TestRingBufferSlowReaderDropsOldFrames(t *testing.T) {
	r := NewRingBuffer[int](4)
	rd := r.Subscribe()

	// 读者完全不读，写 10 帧，只剩最后 4 帧
	for i := 0; i < 10; i++ {
		r.Write(i)
	}
	r.Close()

	got := drain(t, rd)
	want := []int{6, 7, 8, 9}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	if d := rd.Dropped(); d != 6 {
		t.Fatalf("dropped = %d, want 6", d)
	}
}

func TestRingBufferSlowReaderDoesNotBlockWriter(t *testing.T) {
	r := NewRingBuffer[int](4)
	r.Subscribe() // 注册后故意不读

	finished := make(chan struct{})
	go func() {
		for i := 0; i < 10_000; i++ {
			r.Write(i)
		}
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("writer blocked by slow reader")
	}
}

func TestRingBufferMultipleReadersIndependentCursors(t *testing.T) {
	r := NewRingBuffer[int](4)
	fast := r.Subscribe()
	defer fast.Close()
	slow := r.Subscribe()
	defer slow.Close()

	for i := 0; i < 10; i++ {
		r.Write(i)
		v, ok := fast.Next(nil)
		if !ok || v != i {
			t.Fatalf("fast reader got %d (ok=%v), want %d", v, ok, i)
		}
	}
	if d := fast.Dropped(); d != 0 {
		t.Fatalf("fast reader dropped = %d, want 0", d)
	}

	r.Close()
	got := drain(t, slow)
	if len(got) != 4 || got[0] != 6 || got[3] != 9 {
		t.Fatalf("slow reader got %v, want [6 7 8 9]", got)
	}
	if d := slow.Dropped(); d != 6 {
		t.Fatalf("slow reader dropped = %d, want 6", d)
	}
}

func TestRingBufferLateSubscriberStartsAtCurrentPosition(t *testing.T) {
	r := NewRingBuffer[int](8)
	for i := 0; i < 5; i++ {
		r.Write(i)
	}
	late := r.Subscribe()
	defer late.Close()

	r.Write(5)
	r.Close()

	got := drain(t, late)
	if len(got) != 1 || got[0] != 5 {
		t.Fatalf("late reader got %v, want [5]", got)
	}
}

func TestRingBufferPreloadBeforeLiveFrames(t *testing.T) {
	r := NewRingBuffer[int](8)
	rd := r.Subscribe(100, 200)
	defer rd.Close()

	r.Write(1)
	r.Close()

	got := drain(t, rd)
	want := []int{100, 200, 1}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestRingBufferCloseWakesBlockedReader(t *testing.T) {
	r := NewRingBuffer[int](4)
	rd := r.Subscribe()
	defer rd.Close()

	res := make(chan bool, 1)
	go func() {
		_, ok := rd.Next(nil)
		res <- ok
	}()

	time.Sleep(20 * time.Millisecond)
	r.Close()

	select {
	case ok := <-res:
		if ok {
			t.Fatal("Next should return false after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not wake blocked Next")
	}
}

func TestReaderCloseWakesBlockedNext(t *testing.T) {
	r := NewRingBuffer[int](4)
	defer r.Close()
	rd := r.Subscribe()

	res := make(chan bool, 1)
	go func() {
		_, ok := rd.Next(nil)
		res <- ok
	}()

	time.Sleep(20 * time.Millisecond)
	rd.Close()

	select {
	case ok := <-res:
		if ok {
			t.Fatal("Next should return false after Reader.Close")
		}
	case <-time.After(time.Second):
		t.Fatal("Reader.Close did not wake blocked Next")
	}
}

func TestReaderNextStopsOnDone(t *testing.T) {
	r := NewRingBuffer[int](4)
	defer r.Close()
	rd := r.Subscribe()

	done := make(chan struct{})
	go func() {
		time.Sleep(20 * time.Millisecond)
		close(done)
	}()

	if _, ok := rd.Next(done); ok {
		t.Fatal("Next should return false when done is closed")
	}
}

func TestRingBufferSubscribeAfterClose(t *testing.T) {
	r := NewRingBuffer[int](4)
	r.Close()

	rd := r.Subscribe()
	if _, ok := rd.Next(nil); ok {
		t.Fatal("reader of closed ring should get no data")
	}

	// Close 之后写入应被忽略，不能 panic
	r.Write(1)
	if _, ok := rd.Next(nil); ok {
		t.Fatal("write after Close should be ignored")
	}
}

func TestRingBufferConcurrent(t *testing.T) {
	r := NewRingBuffer[int](16)
	const readers = 4
	const total = 2000

	// 先注册读者再启动写者，保证每个读者都能看到至少一帧
	subs := make([]*Reader[int], readers)
	for i := range subs {
		subs[i] = r.Subscribe()
		defer subs[i].Close()
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < total; i++ {
			r.Write(i)
		}
		r.Close()
	}()

	results := make([][]int, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = drain(t, subs[i])
		}(i)
	}
	wg.Wait()

	for i, got := range results {
		if len(got) == 0 {
			t.Fatalf("reader %d got no frames", i)
		}
		for j := 1; j < len(got); j++ {
			if got[j] <= got[j-1] {
				t.Fatalf("reader %d not increasing: %v", i, got[j-1:j+1])
			}
		}
	}
}

func TestNewRingBufferDefaultCapacity(t *testing.T) {
	if got := NewRingBuffer[int](0).Capacity(); got != 64 {
		t.Fatalf("capacity = %d, want 64", got)
	}
}

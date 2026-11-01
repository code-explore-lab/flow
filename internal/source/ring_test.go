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

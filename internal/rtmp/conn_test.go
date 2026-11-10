package rtmp

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewConnDefaults(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	c := newConn(server)

	if c.readChunkSize != defaultChunkSize {
		t.Fatalf("readChunkSize = %d, want %d", c.readChunkSize, defaultChunkSize)
	}
	if c.writeChunkSize != defaultChunkSize {
		t.Fatalf("writeChunkSize = %d, want %d", c.writeChunkSize, defaultChunkSize)
	}
	if c.br == nil || c.chunks == nil {
		t.Fatal("newConn did not initialize reader/map")
	}
}

func TestChunkStatePerCSID(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	c := newConn(server)

	a1 := c.chunkState(3)
	a2 := c.chunkState(3)
	if a1 != a2 {
		t.Fatal("chunkState returned different states for the same csid")
	}
	b := c.chunkState(4)
	if b == a1 {
		t.Fatal("different csids share the same state")
	}

	// 重组缓冲重置后 header 字段保留。
	a1.buf = []byte{1, 2, 3}
	a1.received = 3
	a1.length = 10
	a1.typeID = 20
	a1.reset()
	if a1.buf != nil || a1.received != 0 {
		t.Fatalf("reset left buf=%v received=%d", a1.buf, a1.received)
	}
	if a1.length != 10 || a1.typeID != 20 {
		t.Fatalf("reset cleared header fields: length=%d type=%d", a1.length, a1.typeID)
	}
}

func TestSetChunkSize(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	c := newConn(server)

	if err := c.setReadChunkSize(4096); err != nil {
		t.Fatalf("setReadChunkSize(4096): %v", err)
	}
	if c.readChunkSize != 4096 {
		t.Fatalf("readChunkSize = %d, want 4096", c.readChunkSize)
	}
	if err := c.setWriteChunkSize(60000); err != nil {
		t.Fatalf("setWriteChunkSize(60000): %v", err)
	}
	if c.writeChunkSize != 60000 {
		t.Fatalf("writeChunkSize = %d, want 60000", c.writeChunkSize)
	}

	for _, n := range []uint32{0, maxChunkSize + 1} {
		if err := c.setReadChunkSize(n); err == nil {
			t.Fatalf("setReadChunkSize(%d) accepted invalid size", n)
		}
		if err := c.setWriteChunkSize(n); err == nil {
			t.Fatalf("setWriteChunkSize(%d) accepted invalid size", n)
		}
	}
	if c.readChunkSize != 4096 || c.writeChunkSize != 60000 {
		t.Fatalf("invalid sizes changed state: read=%d write=%d", c.readChunkSize, c.writeChunkSize)
	}
}

// trackingConn 记录 Write 是否发生并发重入（用于验证 Conn.write 的串行化）。
type trackingConn struct {
	net.Conn
	active  atomic.Int32
	overlap atomic.Bool
	writes  atomic.Int32
}

func (t *trackingConn) Write(b []byte) (int, error) {
	if t.active.Add(1) != 1 {
		t.overlap.Store(true)
	}
	time.Sleep(time.Millisecond)
	t.active.Add(-1)
	t.writes.Add(1)
	return len(b), nil
}

func TestWriteIsSerialized(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	tc := &trackingConn{Conn: server}
	c := newConn(tc)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := c.write([]byte{byte(i)}); err != nil {
				t.Errorf("write: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if tc.overlap.Load() {
		t.Fatal("concurrent Write calls overlapped; write lock is not working")
	}
	if got := tc.writes.Load(); got != 20 {
		t.Fatalf("writes = %d, want 20", got)
	}
}

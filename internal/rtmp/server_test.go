package rtmp

import (
	"context"
	"net"
	"testing"
	"time"
)

// freeAddr 借一个空闲端口：先监听 :0 拿到系统分配的真实地址，再关掉。
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// waitFor 轮询条件，超时则让测试失败。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met before timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestNewDefaultsRegistry(t *testing.T) {
	s := New(nil, nil, nil)
	if s.registry == nil {
		t.Fatal("New(nil, ...) did not create a registry")
	}
	if s.registry.Len() != 0 {
		t.Fatalf("registry len = %d, want 0", s.registry.Len())
	}
}

// TestServeAcceptAndShutdown 验证：
//   - Serve 能监听并接受连接；
//   - ctx 取消后 Serve 返回 nil，且已接受的连接被关闭。
func TestServeAcceptAndShutdown(t *testing.T) {
	s := New(nil, nil, nil)
	addr := freeAddr(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- s.Serve(ctx, addr) }()

	// 等 Serve 真正开始监听（端口刚释放，可能有一小段窗口）。
	var client net.Conn
	deadline := time.Now().Add(3 * time.Second)
	for {
		var err error
		client, err = net.Dial("tcp", addr)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial %s: %v", addr, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer client.Close()

	// 等连接被 accept 并登记，避免 cancel 时它还没进 conns。
	waitFor(t, 2*time.Second, func() bool {
		s.connsMu.Lock()
		defer s.connsMu.Unlock()
		return len(s.conns) == 1
	})

	cancel()

	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("Serve returned %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return after ctx cancel")
	}

	// 服务端应主动关闭连接：客户端读到 EOF 或连接重置。
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Fatal("client connection still open after shutdown")
	}
}

// TestCloseIsIdempotent 验证 Conn.Close 可重复调用（shutdown 与
// handleConn 的 defer 会各调用一次）。
func TestCloseIsIdempotent(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	c := newConn(server)
	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

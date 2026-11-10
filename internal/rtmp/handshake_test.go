package rtmp

import (
	"bytes"
	"io"
	"net"
	"testing"
)

// TestHandshakeEcho 字节级验证简单握手：S0=0x03，S1/S2 都回显 C1，
// 读完 C2 后 handshake 正常返回。
func TestHandshakeEcho(t *testing.T) {
	server, client := net.Pipe()
	conn := newConn(server)
	defer client.Close()

	c1 := make([]byte, handshakeSize)
	for i := range c1 {
		c1[i] = byte(i)
	}

	errc := make(chan error, 1)
	go func() { errc <- conn.handshake() }()

	// 客户端发 C0 + C1。
	if _, err := client.Write([]byte{handshakeVersion}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write(c1); err != nil {
		t.Fatal(err)
	}

	// 读 S0 / S1 / S2。
	s0 := make([]byte, 1)
	if _, err := io.ReadFull(client, s0); err != nil {
		t.Fatal(err)
	}
	if s0[0] != handshakeVersion {
		t.Fatalf("S0 = %#x, want %#x", s0[0], handshakeVersion)
	}
	s1 := make([]byte, handshakeSize)
	if _, err := io.ReadFull(client, s1); err != nil {
		t.Fatal(err)
	}
	s2 := make([]byte, handshakeSize)
	if _, err := io.ReadFull(client, s2); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(s1, c1) {
		t.Fatal("S1 is not an echo of C1")
	}
	if !bytes.Equal(s2, c1) {
		t.Fatal("S2 is not an echo of C1")
	}

	// 客户端回显 S1 作为 C2。
	if _, err := client.Write(s1); err != nil {
		t.Fatal(err)
	}

	if err := <-errc; err != nil {
		t.Fatalf("handshake: %v", err)
	}
}

// TestHandshakeBadVersion 验证非法版本号被拒绝。
func TestHandshakeBadVersion(t *testing.T) {
	server, client := net.Pipe()
	conn := newConn(server)
	defer client.Close()

	errc := make(chan error, 1)
	go func() { errc <- conn.handshake() }()

	if _, err := client.Write([]byte{0x00}); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err == nil {
		t.Fatal("handshake accepted unsupported version")
	}
}

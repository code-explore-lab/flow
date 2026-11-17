package rtmp

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
)

// TestHandshakeEcho 字节级验证简单握手：S0=0x03，S1 回显 C1 但版本字段清零，
// S2 回显整个 C1，读完 C2 后 handshake 正常返回。
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
	wantS1 := bytes.Clone(c1)
	clear(wantS1[4:8]) // 版本字段清零，ffmpeg 才会走 simple 握手
	if !bytes.Equal(s1, wantS1) {
		t.Fatal("S1 is not an echo of C1 with a zeroed version field")
	}
	if wantS1[4] != 0 || s1[0] != c1[0] || s1[8] != c1[8] {
		t.Fatalf("S1 time/version/random = %x/%x/%x", s1[:8], s1[8:12], s1[0])
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

// newComplexC1 构造一个带合法 FP 摘要的 C1（模拟 FP9/FP10 客户端）。
func newComplexC1(t *testing.T, base int) ([]byte, int) {
	t.Helper()
	c1 := make([]byte, handshakeSize)
	if _, err := rand.Read(c1); err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint32(c1[:4], 12345)
	copy(c1[4:8], []byte{0x0d, 0x0e, 0x0a, 0x0d}) // 非零版本，模拟 complex 客户端
	pos := digestPosition(c1, base)
	copy(c1[pos:pos+digestSize],
		hmacSHA256(genuineFPKey[:30], c1[:pos], c1[pos+digestSize:]))
	return c1, pos
}

// findDigest 按两种 offset 方案在 1536 字节块里找指定密钥的摘要。
func findDigest(block, key []byte) (int, bool) {
	for _, base := range [2]int{8, 772} {
		pos := digestPosition(block, base)
		want := hmacSHA256(key, block[:pos], block[pos+digestSize:])
		if hmac.Equal(want, block[pos:pos+digestSize]) {
			return pos, true
		}
	}
	return 0, false
}

// TestHandshakeComplex 验证 complex 握手：C1 的两种 offset 方案都能识别，
// S1 带 FMS 摘要且版本字段仍清零，S2 是 C1 摘要的 HMAC，C2 校验通过。
func TestHandshakeComplex(t *testing.T) {
	for _, base := range []int{8, 772} {
		t.Run(fmt.Sprintf("scheme_%d", base), func(t *testing.T) {
			server, client := net.Pipe()
			conn := newConn(server)
			defer client.Close()

			c1, c1Pos := newComplexC1(t, base)
			errc := make(chan error, 1)
			go func() { errc <- conn.handshake() }()

			if _, err := client.Write([]byte{handshakeVersion}); err != nil {
				t.Fatal(err)
			}
			if _, err := client.Write(c1); err != nil {
				t.Fatal(err)
			}

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

			// S1 版本仍清零：ffmpeg 拉流靠这一点留在 simple 握手。
			if s1[4] != 0 || s1[5] != 0 || s1[6] != 0 || s1[7] != 0 {
				t.Fatalf("S1 version = %x, want zero", s1[4:8])
			}
			// 客户端按两种方案找 S1 摘要，应当命中 FMS 前 36 字节。
			s1Pos, ok := findDigest(s1, genuineFMSKey[:36])
			if !ok {
				t.Fatal("S1 has no valid digest")
			}
			// S2 尾部 = HMAC(FMS 完整密钥, C1 摘要)，且不再是 C1 回显。
			wantS2 := hmacSHA256(genuineFMSKey[:], c1[c1Pos:c1Pos+digestSize])
			if !hmac.Equal(wantS2, s2[handshakeSize-digestSize:]) {
				t.Fatal("S2 digest is not HMAC(FMSKey, C1 digest)")
			}
			if bytes.Equal(s2, c1) {
				t.Fatal("S2 is a simple echo of C1")
			}

			// 客户端回 C2：1504 字节随机 + HMAC(FMSKey, S1 摘要)。
			c2 := make([]byte, handshakeSize)
			if _, err := rand.Read(c2); err != nil {
				t.Fatal(err)
			}
			copy(c2[handshakeSize-digestSize:],
				hmacSHA256(genuineFMSKey[:], s1[s1Pos:s1Pos+digestSize]))
			if _, err := client.Write(c2); err != nil {
				t.Fatal(err)
			}
			if err := <-errc; err != nil {
				t.Fatalf("handshake: %v", err)
			}
		})
	}
}

// TestHandshakeComplexBadC2Ignored 验证 C2 摘要错误只记录日志，不拒绝连接。
func TestHandshakeComplexBadC2Ignored(t *testing.T) {
	server, client := net.Pipe()
	conn := newConn(server)
	defer client.Close()

	c1, _ := newComplexC1(t, 8)
	errc := make(chan error, 1)
	go func() { errc <- conn.handshake() }()

	if _, err := client.Write([]byte{handshakeVersion}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write(c1); err != nil {
		t.Fatal(err)
	}
	// 读走 S0/S1/S2。
	if _, err := io.CopyN(io.Discard, client, 1+2*handshakeSize); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write(bytes.Repeat([]byte{0xAA}, handshakeSize)); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("handshake rejected bad C2: %v", err)
	}
}

// TestVerifyC2 直接验证 C2 摘要的判定：正确摘要通过，改一个字节即失败。
func TestVerifyC2(t *testing.T) {
	s1, _ := complexBlocks(make([]byte, handshakeSize), 0)
	pos := digestPosition(s1, 8)
	c2 := make([]byte, handshakeSize)
	if _, err := rand.Read(c2); err != nil {
		t.Fatal(err)
	}
	copy(c2[handshakeSize-digestSize:],
		hmacSHA256(genuineFMSKey[:], s1[pos:pos+digestSize]))
	if !verifyC2(c2, s1) {
		t.Fatal("verifyC2 rejected a valid C2")
	}
	c2[handshakeSize-1] ^= 0xFF
	if verifyC2(c2, s1) {
		t.Fatal("verifyC2 accepted a corrupted C2")
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

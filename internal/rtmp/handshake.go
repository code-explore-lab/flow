package rtmp

// RTMP 握手（simple echo + complex/digest）。
//
// 握手在连接建立后、任何 Chunk Stream 消息之前进行，两端各交换 1536 字节的
// 随机块以确认对方是活的 RTMP 端点：
//
//	C0/C1          客户端 → 服务端：版本 0x03 + 1536 字节随机块
//	S0/S1/S2       服务端 → 客户端：版本 0x03 + S1 + S2
//	C2             客户端 → 服务端：1536 字节
//
// 服务端先按 complex（FP9/FP10 digest）尝试：如果 C1 里带合法的
// HMAC-SHA256 摘要（两种 offset 方案任一命中），就回带摘要的 S1 和
// HMAC 过的 S2，并校验 C2 尾部摘要；否则回退到 simple echo——
// S1 回显 C1、版本字段清零，S2 回显整个 C1，C2 只读不校验。
//
// 两种路径的 S1 版本字段（第 5~8 字节）都清零：ffmpeg 拉流时看到 S1
// 版本 >= 3 会当作复杂握手去校验摘要，失败后直接断开（"Server response
// validating failed"）。清零后 ffmpeg/ffplay 走 simple 握手。
//
// 具体摘要算法见 handshake_digest.go。

import (
	"fmt"
	"io"
	"time"
)

// handshakeVersion RTMP 握手版本。
const handshakeVersion byte = 0x03

// handshakeSize C1/S1/S2/C2 的固定大小。
const handshakeSize = 1536

// handshake 完成一次 RTMP 握手。读走 C2 后返回，后续字节流就是 Chunk Stream。
func (c *Conn) handshake() error {
	// E-5：握手超时，防止慢客户端/半开连接一直占着 goroutine。
	// 握手结束后清掉 deadline，后续由消息循环的读写超时接管。
	if c.handshakeTimeout > 0 {
		if err := c.SetDeadline(time.Now().Add(c.handshakeTimeout)); err != nil {
			return fmt.Errorf("rtmp handshake: set deadline: %w", err)
		}
		defer c.SetDeadline(time.Time{})
	}

	// C0 + C1
	var c0 [1]byte
	if _, err := io.ReadFull(c.br, c0[:]); err != nil {
		return fmt.Errorf("rtmp handshake: read c0: %w", err)
	}
	if c0[0] != handshakeVersion {
		return fmt.Errorf("rtmp handshake: unsupported version %#x", c0[0])
	}
	c1 := make([]byte, handshakeSize)
	if _, err := io.ReadFull(c.br, c1); err != nil {
		return fmt.Errorf("rtmp handshake: read c1: %w", err)
	}

	// complex 握手：C1 带合法摘要时生成带摘要的 S1 和 HMAC 过的 S2；
	// 否则走原来的 simple echo（S1 回显 C1 但版本字段清零，S2 回显整个 C1）。
	s1 := make([]byte, handshakeSize)
	copy(s1, c1)
	clear(s1[4:8])
	s2 := c1
	complex := false
	if pos, ok := locateC1Digest(c1); ok {
		complex = true
		s1, s2 = complexBlocks(c1, pos)
	}

	// S0 + S1 + S2。
	out := make([]byte, 0, 1+2*handshakeSize)
	out = append(out, handshakeVersion) // S0
	out = append(out, s1...)            // S1
	out = append(out, s2...)            // S2
	if err := c.write(out); err != nil {
		return fmt.Errorf("rtmp handshake: write s0s1s2: %w", err)
	}

	// C2：客户端回显 S1（complex 时尾部再带 HMAC 摘要）。
	c2 := make([]byte, handshakeSize)
	if _, err := io.ReadFull(c.br, c2); err != nil {
		return fmt.Errorf("rtmp handshake: read c2: %w", err)
	}
	// C2 摘要不符只记录不拒绝：S0/S1/S2 已经发出，无法回退，而且明文
	// RTMP 的握手摘要本来就不是安全机制。
	if complex && !verifyC2(c2, s1) {
		fmt.Printf("rtmp handshake: c2 digest mismatch from %s (ignored)\n", c.RemoteAddr())
	}
	return nil
}

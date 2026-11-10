package rtmp

// RTMP 握手（simple echo）。
//
// 握手在连接建立后、任何 Chunk Stream 消息之前进行，两端各交换 1536 字节的
// 随机块以确认对方是活的 RTMP 端点。这里采用最简单的回显方案：
//
//	C0/C1          客户端 → 服务端：版本 0x03 + 1536 字节随机块
//	S0/S1/S2       服务端 → 客户端：版本 0x03 + S1(回显 C1) + S2(回显 C1)
//	C2             客户端 → 服务端：回显 S1
//
// ffmpeg/ffplay 默认发带摘要的复杂握手，但发现服务端回的是纯回显后会自动
// 退化成 simple 握手，所以这个最小实现足以兼容它们。

import (
	"fmt"
	"io"
)

// handshakeVersion RTMP 握手版本。
const handshakeVersion byte = 0x03

// handshakeSize C1/S1/S2/C2 的固定大小。
const handshakeSize = 1536

// handshake 完成一次 RTMP 握手。读走 C2 后返回，后续字节流就是 Chunk Stream。
func (c *Conn) handshake() error {
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

	// S0 + S1 + S2，S1/S2 都回显 C1。
	out := make([]byte, 0, 1+2*handshakeSize)
	out = append(out, handshakeVersion) // S0
	out = append(out, c1...)            // S1
	out = append(out, c1...)            // S2
	if err := c.write(out); err != nil {
		return fmt.Errorf("rtmp handshake: write s0s1s2: %w", err)
	}

	// C2：客户端回显 S1。只负责读完，不校验内容。
	if _, err := io.ReadFull(c.br, make([]byte, handshakeSize)); err != nil {
		return fmt.Errorf("rtmp handshake: read c2: %w", err)
	}
	return nil
}

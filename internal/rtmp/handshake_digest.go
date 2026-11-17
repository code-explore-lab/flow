package rtmp

// RTMP 复杂握手（complex/digest handshake，FP9/FP10）的 HMAC-SHA256 部分。
//
// complex 握手的 C1/S1 仍是 1536 字节：time(4) + version(4) + random(1528)。
// random 的 1528 字节分成两个 764 字节块，32 字节摘要藏在其中一个块里：
//
//	scheme 0: offset 字段在 C1[8:12]，摘要位于 12 + sum(C1[8:12])%728
//	scheme 1: offset 字段在 C1[772:776]，摘要位于 776 + sum(C1[772:776])%728
//
// 摘要是「去掉这 32 字节后的 1504 字节」的 HMAC-SHA256。客户端两种方案都
// 可能用，所以校验 C1 时两种都试；服务端生成 S1 时固定用 scheme 0，客户端
// 自己会两种都试。
//
// 摘要之外还有 32 字节的 key block，只用于 RTMPE 加密派生，明文 RTMP 不处理。
//
// 这些密钥是 Adobe 公开硬编码的，只为兼容性协商，不提供任何安全保证。

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"time"
)

const (
	// digestSize 摘要长度。
	digestSize = 32
	// digestMod 摘要偏移取模的基数：764（块大小）- 4（offset 字段）- 32（摘要）。
	digestMod = 728
)

// genuineFMSKey = "Genuine Adobe Flash Media Server 001" + 32 字节固定常量，
// 共 68 字节。校验/生成 S1 取前 36 字节，S2/C2 用完整 68 字节。
var genuineFMSKey = [...]byte{
	0x47, 0x65, 0x6E, 0x75, 0x69, 0x6E, 0x65, 0x20, // Genuine
	0x41, 0x64, 0x6F, 0x62, 0x65, 0x20, // Adobe
	0x46, 0x6C, 0x61, 0x73, 0x68, 0x20, // Flash
	0x4D, 0x65, 0x64, 0x69, 0x61, 0x20, // Media
	0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x20, // Server
	0x30, 0x30, 0x31, // 001
	0xF0, 0xEE, 0xC2, 0x4A, 0x80, 0x68, 0xBE, 0xE8,
	0x2E, 0x00, 0xD0, 0xD1, 0x02, 0x9E, 0x7E, 0x57,
	0x6E, 0xEC, 0x5D, 0x2D, 0x29, 0x80, 0x6F, 0xAB,
	0x93, 0xB8, 0xE6, 0x36, 0xCF, 0xEB, 0x31, 0xAE,
}

// genuineFPKey = "Genuine Adobe Flash Player 001" + 同样的 32 字节常量，
// 共 62 字节。校验 C1 摘要只取前 30 字节。
var genuineFPKey = [...]byte{
	0x47, 0x65, 0x6E, 0x75, 0x69, 0x6E, 0x65, 0x20, // Genuine
	0x41, 0x64, 0x6F, 0x62, 0x65, 0x20, // Adobe
	0x46, 0x6C, 0x61, 0x73, 0x68, 0x20, // Flash
	0x50, 0x6C, 0x61, 0x79, 0x65, 0x72, 0x20, // Player
	0x30, 0x30, 0x31, // 001
	0xF0, 0xEE, 0xC2, 0x4A, 0x80, 0x68, 0xBE, 0xE8,
	0x2E, 0x00, 0xD0, 0xD1, 0x02, 0x9E, 0x7E, 0x57,
	0x6E, 0xEC, 0x5D, 0x2D, 0x29, 0x80, 0x6F, 0xAB,
	0x93, 0xB8, 0xE6, 0x36, 0xCF, 0xEB, 0x31, 0xAE,
}

// digestPosition 计算某个 764 字节块里 32 字节摘要的起始下标。
// base 是该块里 4 字节 offset 字段的起点：scheme 0 为 8，scheme 1 为 772。
func digestPosition(block []byte, base int) int {
	sum := int(block[base]) + int(block[base+1]) +
		int(block[base+2]) + int(block[base+3])
	return base + 4 + sum%digestMod
}

// hmacSHA256 分片计算 HMAC-SHA256，避免为了「去掉摘要」再拷一份 1504 字节。
func hmacSHA256(key []byte, parts ...[]byte) []byte {
	h := hmac.New(sha256.New, key)
	for _, p := range parts {
		h.Write(p)
	}
	return h.Sum(nil)
}

// locateC1Digest 依次尝试两种 offset 方案校验 C1 摘要，返回摘要下标。
// 两种都失败说明客户端走的是 simple 握手。
func locateC1Digest(c1 []byte) (int, bool) {
	for _, base := range [2]int{8, 772} {
		pos := digestPosition(c1, base)
		want := hmacSHA256(genuineFPKey[:30], c1[:pos], c1[pos+digestSize:])
		if hmac.Equal(want, c1[pos:pos+digestSize]) {
			return pos, true
		}
	}
	return 0, false
}

// complexBlocks 生成 complex 握手的 S1/S2。c1DigestPos 是 C1 里摘要的位置。
//
// S1 版本字段清零：ffmpeg 拉流时看到 S1 版本 >= 3 会去校验复杂摘要，
// 清零后无论 simple 还是 complex 客户端都按各自方式处理。
func complexBlocks(c1 []byte, c1DigestPos int) (s1, s2 []byte) {
	s1 = make([]byte, handshakeSize)
	_, _ = rand.Read(s1)
	binary.BigEndian.PutUint32(s1[:4], uint32(time.Now().Unix()))
	clear(s1[4:8])

	pos := digestPosition(s1, 8)
	copy(s1[pos:pos+digestSize],
		hmacSHA256(genuineFMSKey[:36], s1[:pos], s1[pos+digestSize:]))

	s2 = make([]byte, handshakeSize)
	_, _ = rand.Read(s2)
	copy(s2[handshakeSize-digestSize:],
		hmacSHA256(genuineFMSKey[:], c1[c1DigestPos:c1DigestPos+digestSize]))
	return s1, s2
}

// verifyC2 校验客户端 C2 尾部的摘要：HMAC(FMSKey, S1 摘要)。
func verifyC2(c2, s1 []byte) bool {
	pos := digestPosition(s1, 8)
	want := hmacSHA256(genuineFMSKey[:], s1[pos:pos+digestSize])
	return hmac.Equal(want, c2[handshakeSize-digestSize:])
}

// Package fmp4 生成 HLS / DASH 使用的 fragmented MP4（CMAF）数据。
//
// 只做封装：调用方准备好已经编码、已经按访问单元切好的 sample，
// 本包负责 init segment（ftyp + moov）与 media fragment（moof + mdat）。
// 不解析输入、不做时间轴换算，也不依赖第三方库。
package fmp4

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// builder 顺序写 MP4 盒；出错后后续写操作全部跳过，由调用方统一检查。
type builder struct {
	buf []byte
	err error
}

func (b *builder) setErr(err error) {
	if b.err == nil {
		b.err = err
	}
}

func (b *builder) bytes(v []byte) {
	if b.err == nil {
		b.buf = append(b.buf, v...)
	}
}

func (b *builder) u8(v uint8) {
	if b.err == nil {
		b.buf = append(b.buf, v)
	}
}

func (b *builder) u16(v uint16) {
	if b.err == nil {
		b.buf = binary.BigEndian.AppendUint16(b.buf, v)
	}
}

func (b *builder) u24(v uint32) {
	b.u8(uint8(v >> 16))
	b.u8(uint8(v >> 8))
	b.u8(uint8(v))
}

func (b *builder) u32(v uint32) {
	if b.err == nil {
		b.buf = binary.BigEndian.AppendUint32(b.buf, v)
	}
}

func (b *builder) u64(v uint64) {
	if b.err == nil {
		b.buf = binary.BigEndian.AppendUint64(b.buf, v)
	}
}

// matrix 写 MP4 的 unity matrix（3x3，最后一列是 0,0,0x40000000）。
func (b *builder) matrix() {
	for _, v := range [...]uint32{
		0x00010000, 0, 0,
		0, 0x00010000, 0,
		0, 0, 0x40000000,
	} {
		b.u32(v)
	}
}

// box 写一个普通盒：先占位 4 字节大小，写完子内容后回填。
func (b *builder) box(typ string, fn func()) {
	if b.err != nil {
		return
	}
	if len(typ) != 4 {
		b.setErr(fmt.Errorf("fmp4: box type %q is not 4 bytes", typ))
		return
	}
	start := len(b.buf)
	b.u32(0)
	b.bytes([]byte(typ))
	fn()
	if b.err != nil {
		return
	}
	n := len(b.buf) - start
	if n > math.MaxUint32 {
		b.setErr(errors.New("fmp4: box exceeds 4GiB"))
		return
	}
	binary.BigEndian.PutUint32(b.buf[start:start+4], uint32(n))
}

// fullBox 写带 version/flags 的盒。
func (b *builder) fullBox(typ string, version uint8, flags uint32, fn func()) {
	b.box(typ, func() {
		b.u8(version)
		b.u24(flags)
		fn()
	})
}

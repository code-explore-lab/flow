// Package flv 实现 FLV 容器与 AMF0 编解码。
//
// 只覆盖本项目用到的 H.264 + AAC：
//
//	FLV 文件：9 字节文件头 + PreviousTagSize0 + 若干 Tag；
//	Tag：11 字节头（类型、长度、24+8 位时间戳、StreamID）+ 数据 + PreviousTagSize。
//
// RTMP 的媒体消息和 FLV Tag 结构一致，只是不带文件头；RTMP 层直接复用
// 本包的 Tag 读写和 avc.go / aac.go 的负载格式。
package flv

import (
	"errors"
	"fmt"
	"io"
)

// FLV Tag 类型。
const (
	TagAudio  uint8 = 8
	TagVideo  uint8 = 9
	TagScript uint8 = 18
)

// Header 长度、Tag 头长度、Tag 头后 PreviousTagSize 的长度。
const (
	headerSize     = 9
	tagHeaderSize  = 11
	prevTagSizeLen = 4
)

var flvSignature = [3]byte{'F', 'L', 'V'}

// Header FLV 文件头。
type Header struct {
	Version    uint8
	HasAudio   bool
	HasVideo   bool
	DataOffset uint32 // 通常为 9
}

// Tag 一个 FLV Tag。
type Tag struct {
	Type      uint8  // TagAudio / TagVideo / TagScript
	DataSize  uint32 // 数据长度（不含 11 字节 Tag 头）
	Timestamp uint32 // 毫秒，24+8 位（高 8 位在扩展字段）
	StreamID  uint32 // 通常为 0
	Data      []byte
}

// WriteHeader 写 FLV 文件头 + PreviousTagSize0（0）。
/*
0: 'F'
1: 'L'
2: 'V'
3: Version
4: Flags
5~8: DataOffset (uint32 大端)
*/
func WriteHeader(w io.Writer, h Header) error {
	if h.Version == 0 {
		h.Version = 1
	}
	if h.DataOffset == 0 {
		h.DataOffset = headerSize
	}
	flags := uint8(0)
	if h.HasAudio {
		flags |= 0x04
	}
	if h.HasVideo {
		flags |= 0x01
	}

	buf := make([]byte, headerSize)
	copy(buf[0:3], flvSignature[:])
	buf[3] = h.Version
	buf[4] = flags
	putUint32(buf[5:9], h.DataOffset)
	// DataOffset 之后的自定义扩展区用 0 填充，再跟 PreviousTagSize0（固定为 0）。
	if h.DataOffset > headerSize {
		buf = append(buf, make([]byte, h.DataOffset-headerSize)...)
	}
	buf = append(buf, 0, 0, 0, 0)
	_, err := w.Write(buf)
	return err
}

// ReadHeader 读 FLV 文件头，并跳过 PreviousTagSize0。
func ReadHeader(r io.Reader) (Header, error) {
	var buf [headerSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return Header{}, fmt.Errorf("read flv header: %w", err)
	}
	if buf[0] != 'F' || buf[1] != 'L' || buf[2] != 'V' {
		return Header{}, errors.New("invalid flv signature")
	}

	h := Header{
		Version:    buf[3],
		HasAudio:   buf[4]&0x04 != 0,
		HasVideo:   buf[4]&0x01 != 0,
		DataOffset: getUint32(buf[5:9]),
	}
	// DataOffset 之后可能还有自定义数据，跳过；PreviousTagSize0 紧跟其后。
	off := max(int(h.DataOffset), headerSize)
	if skip := make([]byte, off-headerSize); len(skip) > 0 {
		if _, err := io.ReadFull(r, skip); err != nil {
			return Header{}, fmt.Errorf("skip flv header extension: %w", err)
		}
	}
	var prev [prevTagSizeLen]byte
	if _, err := io.ReadFull(r, prev[:]); err != nil {
		return Header{}, fmt.Errorf("read previous tag size 0: %w", err)
	}
	return h, nil
}

// WriteTag 写一个 Tag 及其 PreviousTagSize。
// DataSize 以 len(tag.Data) 为准，避免调用方填错导致文件损坏。
func WriteTag(w io.Writer, tag Tag) error {
	size := len(tag.Data)
	if size > 0xFFFFFF {
		return fmt.Errorf("flv tag too large: %d bytes", size)
	}
	if tag.StreamID > 0xFFFFFF {
		return fmt.Errorf("flv stream id out of range: %d", tag.StreamID)
	}

	var hdr [tagHeaderSize]byte
	hdr[0] = tag.Type
	putUint24(hdr[1:4], uint32(size))
	// 时间戳低 24 位 + 扩展高 8 位。
	putUint24(hdr[4:7], tag.Timestamp&0xFFFFFF)
	hdr[7] = byte(tag.Timestamp >> 24)
	putUint24(hdr[8:11], tag.StreamID)

	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if size > 0 {
		if _, err := w.Write(tag.Data); err != nil {
			return err
		}
	}
	var prev [prevTagSizeLen]byte
	putUint32(prev[:], uint32(tagHeaderSize+size))
	_, err := w.Write(prev[:])
	return err
}

// ReadTag 读一个 Tag。文件/流结束时返回 io.EOF。
func ReadTag(r io.Reader) (*Tag, error) {
	var hdr [tagHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("read flv tag header: %w", err)
	}

	tag := &Tag{
		Type:      hdr[0],
		DataSize:  getUint24(hdr[1:4]),
		StreamID:  getUint24(hdr[8:11]),
		Timestamp: getUint24(hdr[4:7]) | uint32(hdr[7])<<24,
	}
	if tag.DataSize > 0 {
		tag.Data = make([]byte, tag.DataSize)
		if _, err := io.ReadFull(r, tag.Data); err != nil {
			return nil, fmt.Errorf("read flv tag data: %w", err)
		}
	}

	// PreviousTagSize 是完整性校验字段，读掉保持同步；个别封装会写错，
	// 这里不因它中断解析。
	var prev [prevTagSizeLen]byte
	if _, err := io.ReadFull(r, prev[:]); err != nil {
		return nil, fmt.Errorf("read previous tag size: %w", err)
	}
	return tag, nil
}

// putUint24 写 24 位大端整数。
// 大端（Big-Endian）= 高位字节放在低地址。
func putUint24(b []byte, v uint32) {
	b[0] = byte(v >> 16) // 最高字节 → 放到数组下标0（低地址）
	b[1] = byte(v >> 8)  // 中间字节 → 下标1
	b[2] = byte(v)       // 最低字节 → 下标2（高地址）
}

// getUint24 读 24 位大端整数。
func getUint24(b []byte) uint32 {
	return uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
}

// putUint32 写 32 位大端整数。
func putUint32(b []byte, v uint32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}

// getUint32 读 32 位大端整数。
func getUint32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

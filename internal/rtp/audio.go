package rtp

// 非 AAC 音频的 RTP 封装/解包：
//
//	G.711（PCMA/PCMU）：payload 就是裸采样字节，1 字节 = 1 采样；
//	Opus：payload 就是编码器输出，无额外头；
//	MP3：RFC 2250，payload 前加 4 字节头（16 位 MBZ + 16 位 fragment offset）。
//
// 时间戳由调用方按各编码的 RTP 时钟填写；本文件只负责切包和重组。

import (
	"encoding/binary"
	"errors"
	"fmt"

	"flow/internal/media"
)

const (
	// PCMUPayloadType / PCMAPayloadType 是 RFC 3551 的静态 payload type。
	PCMUPayloadType uint8 = 0
	PCMAPayloadType uint8 = 8
	MPAPayloadType  uint8 = 14

	// OpusDefaultPayloadType 是 opus/48000/2 的默认动态 PT。
	OpusDefaultPayloadType uint8 = 97

	// DefaultAudioMTU 音频 RTP 包的默认负载上限。
	DefaultAudioMTU = 1200

	// MPAHeaderSize 是 RFC 2250 MPEG 音频负载头的长度。
	MPAHeaderSize = 4
)

// G711Packer 把一段 G.711 采样切成 RTP 包。
// 同一个源帧可能被拆成多包；每包时间戳应依次增加 payload 字节数。
type G711Packer struct {
	PT  uint8
	MTU int
}

// NewPCMAPacker 创建 A-law 封包器（PT 8）。
func NewPCMAPacker() *G711Packer {
	return &G711Packer{PT: PCMAPayloadType, MTU: DefaultAudioMTU}
}

// NewPCMUPacker 创建 μ-law 封包器（PT 0）。
func NewPCMUPacker() *G711Packer {
	return &G711Packer{PT: PCMUPayloadType, MTU: DefaultAudioMTU}
}

// Pack 把 samples 切成若干 RTP 包，最后一个包的 marker 置位。
func (p *G711Packer) Pack(samples []byte) ([]*RTPPacket, error) {
	if len(samples) == 0 {
		return nil, errors.New("empty g711 payload")
	}
	mtu := p.MTU
	if mtu <= 0 || mtu > len(samples) {
		mtu = DefaultAudioMTU
	}
	var pkts []*RTPPacket
	for off := 0; off < len(samples); {
		n := len(samples) - off
		if n > mtu {
			n = mtu
		}
		chunk := append([]byte(nil), samples[off:off+n]...)
		pkts = append(pkts, newPacket(p.PT, chunk, off+n == len(samples)))
		off += n
	}
	return pkts, nil
}

// OpusPacker 把一个 Opus 包封成一个 RTP 包。Opus 没有 RTP 分片，
// 超过 MTU 时交给 IP 层分片。
type OpusPacker struct {
	PT uint8
}

// NewOpusPacker 创建 Opus 封包器。
func NewOpusPacker() *OpusPacker {
	return &OpusPacker{PT: OpusDefaultPayloadType}
}

// Pack 原样封装一帧 Opus。
func (p *OpusPacker) Pack(frame []byte) ([]*RTPPacket, error) {
	if len(frame) == 0 {
		return nil, errors.New("empty opus payload")
	}
	return []*RTPPacket{newPacket(p.PT, append([]byte(nil), frame...), true)}, nil
}

// MPAPacker 按 RFC 2250 封装 MPEG 音频帧。
type MPAPacker struct {
	PT  uint8
	MTU int
}

// NewMPAPacker 创建 MP3/MPEG 音频封包器（PT 14）。
func NewMPAPacker() *MPAPacker {
	return &MPAPacker{PT: MPAPayloadType, MTU: DefaultAudioMTU}
}

// Pack 把一帧 MPEG 音频切成 RTP 包。4 字节头里的 fragment offset 是
// 相对整帧起点的字节偏移，所有分片共用同一个 RTP 时间戳。
func (p *MPAPacker) Pack(frame []byte) ([]*RTPPacket, error) {
	if len(frame) == 0 {
		return nil, errors.New("empty mpa payload")
	}
	mtu := p.MTU
	if mtu <= MPAHeaderSize {
		mtu = DefaultAudioMTU
	}
	chunk := mtu - MPAHeaderSize
	var pkts []*RTPPacket
	for off := 0; off < len(frame); {
		n := len(frame) - off
		if n > chunk {
			n = chunk
		}
		if off > 0xFFFF {
			return nil, fmt.Errorf("mpeg audio frame too large: %d bytes", len(frame))
		}
		payload := make([]byte, MPAHeaderSize+n)
		// 前 16 位 MBZ 保持 0，后 16 位是 fragment offset。
		binary.BigEndian.PutUint16(payload[2:4], uint16(off))
		copy(payload[MPAHeaderSize:], frame[off:off+n])
		pkts = append(pkts, newPacket(p.PT, payload, off+n == len(frame)))
		off += n
	}
	return pkts, nil
}

// MPAFrame 解包得到的一个完整 MPEG 音频帧。
type MPAFrame struct {
	Data      []byte
	Timestamp uint32 // 90kHz RTP 时钟
}

// MPADepacketizer 按 RFC 2250 重组 MPEG 音频帧。
// 同一个音频轨只需要一个实例；无并发安全。
type MPADepacketizer struct {
	ts      uint32
	buf     []byte
	have    bool
	offset  int
	elapsed int // 已从 d.buf 起始帧算起的采样数
}

// NewMPADepacketizer 创建 MPEG 音频解包器。
func NewMPADepacketizer() *MPADepacketizer {
	return &MPADepacketizer{}
}

// Depacketize 处理一个 RTP 包，返回其中已经完整的 MPEG 音频帧。
//
// RTP 包不一定按帧对齐：ffmpeg 会把多个完整帧塞进一个包，也可能不带
// marker 位。这里按帧头里的帧长切分，完整几帧就返回几帧；剩下的残片
// 留在缓冲区等后续分片。每帧时间戳按累计采样数从包的 RTP 时间戳推算。
//
// 兼容不带 4 字节头的实现：payload 开头不是 MBZ+offset（前两字节非零）
// 时把整个负载当作一个完整帧。代价是这种实现不能跨包分片。
func (d *MPADepacketizer) Depacketize(pkt *RTPPacket) ([]MPAFrame, bool) {
	if pkt == nil || len(pkt.Payload) == 0 {
		return nil, false
	}
	payload := pkt.Payload
	fragmented := len(payload) >= MPAHeaderSize &&
		payload[0] == 0 && payload[1] == 0
	if !fragmented {
		return []MPAFrame{{
			Data:      append([]byte(nil), payload...),
			Timestamp: pkt.Header.Timestamp,
		}}, true
	}

	off := int(binary.BigEndian.Uint16(payload[2:4]))
	payload = payload[MPAHeaderSize:]
	if off == 0 {
		// 新帧/新包开始；上一包没收齐的残片丢弃。
		d.reset()
		d.ts = pkt.Header.Timestamp
		d.have = true
	} else if !d.have || pkt.Header.Timestamp != d.ts || off != d.offset {
		// 分片错位/换帧：丢弃，等下一个 offset=0。
		d.reset()
		return nil, false
	}
	d.buf = append(d.buf, payload...)
	d.offset += len(payload)

	var frames []MPAFrame
	for len(d.buf) >= 4 {
		info, ok := media.ParseMP3Frame(d.buf)
		if !ok {
			// 同步丢失：丢到下一个同步字；可能是 ID3 尾巴或损坏分片。
			idx := nextMP3Sync(d.buf[1:])
			if idx < 0 {
				d.buf = d.buf[:0]
				break
			}
			d.buf = d.buf[idx+1:]
			continue
		}
		if len(d.buf) < info.Length {
			break // 等后续分片
		}
		data := append([]byte(nil), d.buf[:info.Length]...)
		ts := d.ts + uint32(uint64(d.elapsed)*90000/uint64(info.SampleRate))
		frames = append(frames, MPAFrame{Data: data, Timestamp: ts})
		d.elapsed += info.Samples
		d.buf = d.buf[info.Length:]
		if len(d.buf) == 0 {
			d.buf = nil
		}
	}
	return frames, len(frames) > 0
}

// nextMP3Sync 返回 data 中第一个同步字（0xFF + 高 3 位全 1）的下标。
func nextMP3Sync(data []byte) int {
	for i := 0; i+1 < len(data); i++ {
		if data[i] == 0xFF && data[i+1]&0xE0 == 0xE0 {
			return i
		}
	}
	return -1
}

func (d *MPADepacketizer) reset() {
	d.buf = nil
	d.have = false
	d.offset = 0
	d.elapsed = 0
}

package rtp

import (
	"encoding/binary"
	"errors"
)

// 接收侧 RTP 解析：固定头 12 字节 + CSRC 列表 + 扩展头 + padding。
//
// RFC 3550 §5.1 的头部布局：
//
//	 0                   1                   2                   3
//	 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|V=2|P|X|  CC   |M|     PT      |       sequence number         |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                           timestamp                           |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|           synchronization source (SSRC) identifier            |
//	+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+
//	|            contributing source (CSRC) identifiers             |
//	|                             ....                              |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|      defined by profile       |           length              |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                        header extension                       |
//	|                             ....                              |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
const (
	// rtpFixedHeaderLen 固定头长度：V/P/X/CC(1) M/PT(1) seq(2) ts(4) ssrc(4)。
	rtpFixedHeaderLen = 12
	// rtpExtensionHeaderLen 扩展头前 4 字节：16 位 profile + 16 位 length。
	rtpExtensionHeaderLen = 4
)

var (
	ErrPacketTooShort = errors.New("rtp: packet shorter than 12-byte fixed header")
	ErrBadVersion     = errors.New("rtp: version must be 2")
	ErrCSRCTruncated  = errors.New("rtp: packet truncated in CSRC list")
	ErrExtensionTrunc = errors.New("rtp: packet truncated in header extension")
	ErrBadPadding     = errors.New("rtp: padding length is zero or exceeds payload")
)

// Parse 把一个完整的 RTP 报文解析成 RTPPacket。
//
//   - CSRC 按 CC 计数逐个读出；
//   - 有扩展头（X=1）时跳过 4 字节头 + length 个 32 位字，并把 profile/数据暴露出来；
//   - 有 padding（P=1）时按最后一个字节截断，padding 不计入 payload。
//
// Payload 是与 raw 共享底层数组的切片，零拷贝；需要长期保留时调用方自行复制。
func Parse(raw []byte) (*RTPPacket, error) {
	if len(raw) < rtpFixedHeaderLen {
		return nil, ErrPacketTooShort
	}

	b0 := raw[0]
	version := b0 >> 6 // 高 2 bit
	if version != 2 {
		return nil, ErrBadVersion
	}
	padding := b0&(1<<5) != 0   // bit5
	extension := b0&(1<<4) != 0 // bit4
	cc := int(b0 & 0x0F)        // 低 4 bit

	b1 := raw[1]
	marker := b1&(1<<7) != 0 // bit7
	pt := b1 & 0x7F          // 低 7 bit

	h := RTPHeader{
		Version:   version,
		Padding:   padding,
		Extension: extension,
		CC:        uint8(cc),
		Marker:    marker,
		PT:        pt,
		Seq:       binary.BigEndian.Uint16(raw[2:4]),
		Timestamp: binary.BigEndian.Uint32(raw[4:8]),
		SSRC:      binary.BigEndian.Uint32(raw[8:12]),
	}

	offset := rtpFixedHeaderLen

	if cc > 0 {
		if len(raw) < offset+4*cc {
			return nil, ErrCSRCTruncated
		}
		h.CSRC = make([]uint32, cc)
		for i := 0; i < cc; i++ {
			h.CSRC[i] = binary.BigEndian.Uint32(raw[offset+4*i:])
		}
		offset += 4 * cc
	}

	if extension {
		if len(raw) < offset+rtpExtensionHeaderLen {
			return nil, ErrExtensionTrunc
		}
		h.ExtensionProfile = binary.BigEndian.Uint16(raw[offset:])
		// length 的单位是 32 位字，不是字节。
		extLen := int(binary.BigEndian.Uint16(raw[offset+2:])) * 4
		offset += rtpExtensionHeaderLen
		if len(raw) < offset+extLen {
			return nil, ErrExtensionTrunc
		}
		if extLen > 0 {
			h.ExtensionData = raw[offset : offset+extLen]
		}
		offset += extLen
	}

	payload := raw[offset:]
	if padding {
		if len(payload) == 0 {
			return nil, ErrBadPadding
		}
		// 最后一个字节是 padding 总长度（含它自己），最小 1。
		padLen := int(payload[len(payload)-1])
		if padLen == 0 || padLen > len(payload) {
			return nil, ErrBadPadding
		}
		payload = payload[:len(payload)-padLen]
	}

	return &RTPPacket{Header: h, Payload: payload}, nil
}

// Depacketizer 在 Parse 之上记录最近一个包的 seq/timestamp/SSRC/marker，
// 供推流端统计和后续 H.264/AAC 解包使用。
//
// 非并发安全：每个连接/轨道各持一个。
type Depacketizer struct {
	Seq       uint16
	Timestamp uint32
	SSRC      uint32
	Marker    bool

	// Packets 是成功解析的包数，便于统计。
	Packets uint64
}

// Parse 解析一个报文并更新最近状态；解析失败时不改动状态。
func (d *Depacketizer) Parse(raw []byte) (*RTPPacket, error) {
	pkt, err := Parse(raw)
	if err != nil {
		return nil, err
	}
	d.Seq = pkt.Header.Seq
	d.Timestamp = pkt.Header.Timestamp
	d.SSRC = pkt.Header.SSRC
	d.Marker = pkt.Header.Marker
	d.Packets++
	return pkt, nil
}

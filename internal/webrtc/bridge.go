package webrtc

import (
	pionrtp "github.com/pion/rtp"

	"flow/internal/rtp"
)

// toInternalRTP 把 pion 的 RTP 包转换成仓库 internal/rtp 的表示，复用
// 已经写好的 H.264 解包器。只拷贝头部字段，payload 仍指向原缓冲区；
// 调用方需要在下次 ReadRTP 之前把需要的字节复制出去。
func toInternalRTP(p *pionrtp.Packet) *rtp.RTPPacket {
	return &rtp.RTPPacket{
		Header: rtp.RTPHeader{
			Version:   2,
			Marker:    p.Marker,
			PT:        p.PayloadType,
			Seq:       p.SequenceNumber,
			Timestamp: p.Timestamp,
			SSRC:      p.SSRC,
		},
		Payload: p.Payload,
	}
}

// toPionRTP 把仓库封包器的输出转换成 pion 的 RTP 包。
// TrackLocalStaticRTP 会在发送时覆盖 SSRC 和 payload type，seq/timestamp
// 与 marker 位保留调用方的值。
func toPionRTP(p *rtp.RTPPacket) *pionrtp.Packet {
	return &pionrtp.Packet{
		Header: pionrtp.Header{
			Version:        2,
			Marker:         p.Header.Marker,
			PayloadType:    p.Header.PT,
			SequenceNumber: p.Header.Seq,
			Timestamp:      p.Header.Timestamp,
			SSRC:           p.Header.SSRC,
		},
		Payload: p.Payload,
	}
}

package rtp

//按 RFC 3640（mpeg4-generic / AAC-hbr）把一个 AAC AU 封成一个 RTP 包。
import (
	"encoding/binary"
	"errors"
	"fmt"
)

// RFC 3640 的 AAC-hbr 参数（与 SDP 里的 fmtp 保持一致）
const (
	AACSizeLength         = 13 // SizeLength：AU size 占 13 bit
	AACIndexLength        = 3  // IndexLength：AU index 占 3 bit
	AACIndexDeltaLength   = 3  // IndexDeltaLength：多 AU 时后续 index 增量占 3 bit
	AACAUHeaderLength     = AACSizeLength + AACIndexLength
	AACMaxAUSize          = (1 << AACSizeLength) - 1 // 13 bit 能表达的最大 AU 长度
	AACFrameSamples       = 1024                     // AAC LC 一个 AU 固定 1024 个采样，音频 timestamp 每次 +1024
	AACDefaultPayloadType = 97                       // 音频动态 PT，视频用 96
)

// AACPacker 按 RFC 3640 把 AAC AU 封成 RTP 包。
// 本阶段一个 AU 封一个 RTP 包（AAC LC 单帧通常只有几百字节，远小于 MTU）。
//
// RTP payload 布局：
//
//	AU-headers-length (16 bit，单位是 bit) = 16
//	AU-header         (16 bit) = AU size(13) << 3 | AU index(3)=0
//	AU data           (去掉 ADTS 头的裸 AAC 帧)
type AACPacker struct {
	PT uint8 // RTP payload type，默认 97（mpeg4-generic）
}

// NewAACPacker 创建 AAC 封包器。
func NewAACPacker() *AACPacker {
	return &AACPacker{PT: AACDefaultPayloadType}
}

// Pack 输入一个 AU（不含 ADTS 头），输出一个 RTP 包。
// Version/PT/Marker 已填好（Marker=true 表示这是该 AU 的最后一个包，本阶段也是唯一包）；
// Seq / Timestamp / SSRC 留给调用方填。
func (p *AACPacker) Pack(au []byte) ([]*RTPPacket, error) {
	if len(au) == 0 {
		return nil, errors.New("empty aac au")
	}
	// AU size 只有 13 bit，超了就表达不了（改用 RFC 3640 分片，本阶段不实现）
	if len(au) > AACMaxAUSize {
		return nil, fmt.Errorf("aac au too large for SizeLength=%d: %d bytes", AACSizeLength, len(au))
	}
	if p.PT == 0 {
		p.PT = AACDefaultPayloadType
	}

	// 2 字节 AU-headers-length + 2 字节 AU-header + AU 数据
	payload := make([]byte, 4+len(au))

	// AU-headers-length：一个 AU-header 共 16 bit
	binary.BigEndian.PutUint16(payload[0:2], AACAUHeaderLength)

	// AU-header：高 13 位 size，低 3 位 index（单 AU 时 index 固定为 0，左移 3 位即可）
	auHeader := uint16(len(au)) << AACIndexLength
	binary.BigEndian.PutUint16(payload[2:4], auHeader)

	copy(payload[4:], au)

	return []*RTPPacket{newPacket(p.PT, payload, true)}, nil
}

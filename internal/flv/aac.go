package flv

// FLV 音频 tag 的 AAC 负载封装/解析。
//
// 音频 tag 的 Data（去掉 FLV tag 头后）结构：
//
//	byte 0        高 4 位 SoundFormat（AAC=10）、2 位 SoundRate、
//	              1 位 SoundSize、1 位 SoundType
//	byte 1        AACPacketType：0=序列头，1=裸 AAC
//	之后          序列头 → AudioSpecificConfig；
//	              裸 AAC  → 单个 AAC AU（无 ADTS 头）
//
// 对 AAC 而言 byte 0 里的采样率/位深/声道只是提示，真正参数在 ASC 里；
// 但为了兼容按惯例仍填：SoundSize=1（16 位）、SoundType 按声道数。

import "errors"

// soundFormatAAC FLV 里 AAC 的 SoundFormat。
const soundFormatAAC uint8 = 10

var (
	errAudioTooShort = errors.New("aac: audio tag too short")
	errNotAAC        = errors.New("aac: unsupported sound format")
	errAACPacketType = errors.New("aac: unknown packet type")
)

// AACPacketType。
const (
	AACSeqHeader uint8 = 0 //ASC
	AACRaw       uint8 = 1 //ADTS
)

// AudioTag 解析出的一个音频 tag 负载。
type AudioTag struct {
	SoundFormat uint8  // 应为 soundFormatAAC
	SoundRate   uint8  // 2 位索引（提示值）
	SoundSize   uint8  // 0=8 位，1=16 位
	SoundType   uint8  // 0=单声道，1=立体声
	PacketType  uint8  // AACPacketType
	Config      []byte // 序列头的 AudioSpecificConfig
	AU          []byte // 裸 AAC 的单个 AU
}

// SoundRateIndex 把采样率映射成 FLV 的 2 位 SoundRate 索引。
// 只有 4 档，向上取到能覆盖该采样率的档位；AAC 常用 44100/48000 都落到 3。
func SoundRateIndex(rate uint32) uint8 {
	switch {
	case rate < 11025:
		return 0 // 5.5 kHz
	case rate < 22050:
		return 1 // 11 kHz
	case rate < 44100:
		return 2 // 22 kHz
	default:
		return 3 // 44 kHz
	}
}

// audioFlags 拼出音频 tag 的 byte 0。
func audioFlags(rate uint32, ch uint8) byte {
	soundType := uint8(0)
	if ch >= 2 {
		soundType = 1
	}
	// SoundSize=1（16 位）<< 1 | SoundType。
	return soundFormatAAC<<4 | SoundRateIndex(rate)<<2 | 0x02 | soundType
}

// BuildAudioSequenceHeader 生成 AAC 序列头 tag 的 Data（AACPacketType=0）。
func BuildAudioSequenceHeader(asc []byte, rate uint32, ch uint8) []byte {
	body := make([]byte, 2+len(asc))
	body[0] = audioFlags(rate, ch)
	body[1] = AACSeqHeader
	copy(body[2:], asc)
	return body
}

// BuildAudioTag 生成单个裸 AAC AU 的 tag 的 Data（AACPacketType=1）。
func BuildAudioTag(au []byte, rate uint32, ch uint8) []byte {
	body := make([]byte, 2+len(au))
	body[0] = audioFlags(rate, ch)
	body[1] = AACRaw
	copy(body[2:], au)
	return body
}

// ParseAudioTag 解析一个音频 tag 的 Data。只支持 AAC（SoundFormat=10）。
func ParseAudioTag(data []byte) (*AudioTag, error) {
	if len(data) < 2 {
		return nil, errAudioTooShort
	}
	at := &AudioTag{
		SoundFormat: data[0] >> 4,
		SoundRate:   (data[0] >> 2) & 0x03,
		SoundSize:   (data[0] >> 1) & 0x01,
		SoundType:   data[0] & 0x01,
		PacketType:  data[1],
	}
	if at.SoundFormat != soundFormatAAC {
		return nil, errNotAAC
	}
	payload := data[2:]
	switch at.PacketType {
	case AACSeqHeader:
		at.Config = append([]byte(nil), payload...)
	case AACRaw:
		at.AU = append([]byte(nil), payload...)
	default:
		return nil, errAACPacketType
	}
	return at, nil
}

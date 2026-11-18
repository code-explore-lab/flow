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

import (
	"errors"
	"strings"
)

// FLV 音频 SoundFormat（FLV 规范 + Enhanced RTMP 常用值）。
const (
	SoundFormatMP3   uint8 = 2
	SoundFormatALaw  uint8 = 7
	SoundFormatMuLaw uint8 = 8
	SoundFormatAAC   uint8 = 10

	// soundFormatAAC 保留旧名，兼容包内测试。
	soundFormatAAC = SoundFormatAAC
)

var (
	errAudioTooShort = errors.New("aac: audio tag too short")
	errNotAAC        = errors.New("audio: unsupported sound format")
	errAACPacketType = errors.New("aac: unknown packet type")
)

// AACPacketType。
const (
	AACSeqHeader uint8 = 0 //ASC
	AACRaw       uint8 = 1 //ADTS
)

// AudioTag 解析出的一个音频 tag 负载。
type AudioTag struct {
	SoundFormat uint8  // MP3 / A-law / μ-law / AAC
	SoundRate   uint8  // 2 位索引（提示值）
	SoundSize   uint8  // 0=8 位，1=16 位
	SoundType   uint8  // 0=单声道，1=立体声
	PacketType  uint8  // AACPacketType（仅 AAC）
	Config      []byte // 序列头的 AudioSpecificConfig（仅 AAC）
	AU          []byte // 裸 AAC 的单个 AU（仅 AAC）
	Data        []byte // 非 AAC：byte 1 之后的原始负载（MP3/G.711）
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

// audioFlagsFor 拼出音频 tag 的 byte 0。
func audioFlagsFor(format uint8, rate uint32, ch uint8) byte {
	soundType := uint8(0)
	if ch >= 2 {
		soundType = 1
	}
	// SoundSize=1（16 位）<< 1 | SoundType。
	return format<<4 | SoundRateIndex(rate)<<2 | 0x02 | soundType
}

// audioFlags 拼出 AAC tag 的 byte 0。
func audioFlags(rate uint32, ch uint8) byte {
	return audioFlagsFor(SoundFormatAAC, rate, ch)
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

// buildRawAudioTag 生成 MP3/G.711 的 tag 的 Data：byte 0 是格式标志，
// 之后直接跟编码数据，没有 AACPacketType。
func buildRawAudioTag(format uint8, data []byte, rate uint32, ch uint8) []byte {
	body := make([]byte, 1+len(data))
	body[0] = audioFlagsFor(format, rate, ch)
	copy(body[1:], data)
	return body
}

// BuildAudioTagFor 按编码名生成音频 tag 的 Data。
// codec 为 "MP3" / "PCMA" / "PCMU" 时生成对应格式，其余按 AAC 裸 AU。
func BuildAudioTagFor(codec string, data []byte, rate uint32, ch uint8) []byte {
	switch strings.ToUpper(codec) {
	case "MP3":
		return buildRawAudioTag(SoundFormatMP3, data, rate, ch)
	case "PCMA":
		return buildRawAudioTag(SoundFormatALaw, data, rate, ch)
	case "PCMU":
		return buildRawAudioTag(SoundFormatMuLaw, data, rate, ch)
	default:
		return BuildAudioTag(data, rate, ch)
	}
}

// ParseAudioTag 解析一个音频 tag 的 Data，支持 MP3、G.711 A/μ-law 和 AAC。
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
	switch at.SoundFormat {
	case SoundFormatAAC:
		payload := data[2:]
		switch at.PacketType {
		case AACSeqHeader:
			at.Config = append([]byte(nil), payload...)
		case AACRaw:
			at.AU = append([]byte(nil), payload...)
		default:
			return nil, errAACPacketType
		}
	case SoundFormatMP3, SoundFormatALaw, SoundFormatMuLaw:
		at.PacketType = 0
		at.Data = append([]byte(nil), data[1:]...)
	default:
		return nil, errNotAAC
	}
	return at, nil
}

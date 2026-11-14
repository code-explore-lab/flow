package plc

// AAC-LC 静音 AU。
//
// 这两个字节序列由 ffmpeg 的 anullsrc + aac 编码器生成后提取（去掉 ADTS 头），
// 再用 ffmpeg 解码验证为 -91 dB（PCM 全零）。帧内容不携带采样率——
// 采样率来自 SDP 的 AudioSpecificConfig——因此 22.05k/32k/44.1k/48k
// 的静音帧字节完全一致，只需要按声道数分两份：
//
//	mono   : SCE 0x01 0x18 0x20 0x07
//	stereo : CPE 0x21 0x10 0x04 0x60 0x8c 0x1c
var aacSilenceLC = [][]byte{
	nil,
	{0x01, 0x18, 0x20, 0x07},
	{0x21, 0x10, 0x04, 0x60, 0x8c, 0x1c},
}

// SilenceAU 返回指定声道数的 AAC-LC 静音 AU。目前只覆盖单声道/立体声；
// 其他配置返回 false，调用方应跳过填充（宁可少一包，不写非法 AU）。
func SilenceAU(channels int) ([]byte, bool) {
	if channels < 1 || channels >= len(aacSilenceLC) {
		return nil, false
	}
	return aacSilenceLC[channels], true
}

// AACLCChannels 从 AudioSpecificConfig 解出 AAC-LC 的声道数。
// 只支持 AAC-LC（audioObjectType=2）且 channelConfiguration 为 1/2：
// HE-AAC 等配置的静音帧结构不同，不能复用上面的常量。
func AACLCChannels(asc []byte) (int, bool) {
	if len(asc) < 2 {
		return 0, false
	}
	if aot := asc[0] >> 3; aot != 2 {
		return 0, false
	}
	ch := int(asc[1]>>3) & 0x0F
	if ch < 1 || ch > 2 {
		return 0, false
	}
	return ch, true
}

// MaxConcealAUs 限制一次连续填充的 AU 数，避免长时间断流后
// 一口气补出几秒静音、把时间戳推到很远。
const MaxConcealAUs = 30

// MissingAUs 返回从 prevEnd 到 ts 之间缺失的 AU 数。
// prevEnd 是上一包最后一个 AU 之后的时间戳；step 是一个 AU 的采样数。
// 时间戳回退或相等时返回 0；结果以 MaxConcealAUs 封顶。
func MissingAUs(prevEnd, ts, step uint32) int {
	if step == 0 || ts <= prevEnd {
		return 0
	}
	return min(int((ts-prevEnd)/step), MaxConcealAUs)
}

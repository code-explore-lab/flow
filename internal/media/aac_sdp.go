// internal/media/aac_sdp.go
package media

import (
	"encoding/base64"
	"fmt"
	"strings"
)

// AACPT AAC 在 SDP 里用的动态 payload type（约定 97，视频用 96）。
const AACPT = 97

// AACInfo 一条 AAC 轨在 SDP 里需要的参数，全部来自 ADTS 头。
type AACInfo struct {
	AudioObjectType        uint8
	SamplingFrequencyIndex uint8
	SamplingFrequency      int    // Hz，如 44100
	ChannelConfig          uint8  // 声道数
	Config                 []byte // 2 字节 AudioSpecificConfig
}

// GetAACInfo 读 ADTS 文件的第一帧，解析出 SDP 需要的参数。
// DESCRIBE 时调用，替代写死的音频参数。
func GetAACInfo(filePath string) (*AACInfo, error) {
	demux, err := NewAACDemux(filePath)
	if err != nil {
		return nil, err
	}
	// NextAU 会把首帧头信息记到 demux.First
	if _, err := demux.NextAU(); err != nil {
		return nil, fmt.Errorf("read first aac au: %w", err)
	}
	if demux.First == nil {
		return nil, fmt.Errorf("no adts frame found")
	}

	h := demux.First
	// index 13~15 是保留值，查不到采样率
	if h.SamplingFrequencyIndex > 12 {
		return nil, fmt.Errorf("unsupported sampling_frequency_index %d", h.SamplingFrequencyIndex)
	}
	// channel_configuration=0 表示后面跟 PCE，这里不支持
	if h.ChannelConfig == 0 {
		return nil, fmt.Errorf("channel_configuration 0 (PCE) not supported")
	}

	return &AACInfo{
		AudioObjectType:        h.AudioObjectType,
		SamplingFrequencyIndex: h.SamplingFrequencyIndex,
		SamplingFrequency:      h.SamplingFrequency,
		ChannelConfig:          h.ChannelConfig,
		Config:                 h.AudioSpecificConfig(),
	}, nil
}

// ConfigBase64 返回 SDP fmtp 里 config= 的值。
func (i *AACInfo) ConfigBase64() string {
	return base64.StdEncoding.EncodeToString(i.Config)
}

// MediaDescription 生成 SDP 的 m=audio 段（RFC 3640 + RFC 3640 AAC-hbr）。
// 端口写 0：RTSP 的实际端口在 SETUP 阶段协商，SDP 里用 0 是标准做法。
func (i *AACInfo) MediaDescription(pt int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "m=audio 0 RTP/AVP %d\r\n", pt)
	fmt.Fprintf(&b, "a=rtpmap:%d mpeg4-generic/%d/%d\r\n",
		pt, i.SamplingFrequency, i.ChannelConfig)
	fmt.Fprintf(&b, "a=fmtp:%d streamtype=5; profile-level-id=1; mode=AAC-hbr; "+
		"SizeLength=13; IndexLength=3; IndexDeltaLength=3; config=%s\r\n",
		pt, i.ConfigBase64())
	return b.String()
}

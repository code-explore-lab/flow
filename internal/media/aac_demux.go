package media

//从 `test.aac`（ADTS，每帧带 7 字节头）里按 `frame_length` 切出每个 AU。
import (
	"errors"
	"fmt"
	"io"
	"os"
)

// samplingFrequencyTable 采样率索引表（ISO/IEC 14496-3），下标即 sampling_frequency_index。
var samplingFrequencyTable = [...]int{
	96000, 88200, 64000, 48000, 44100, 32000,
	24000, 22050, 16000, 12000, 11025, 8000, 7350,
}

// ADTSHeader 解析后的 ADTS 头字段（无 CRC 时 7 字节，有 CRC 时 9 字节）。
type ADTSHeader struct {
	ID                     uint8 // MPEG 版本：0=MPEG-4，1=MPEG-2
	ProtectionAbsent       bool  // true=无 CRC（头 7 字节），false=有 CRC（头 9 字节）
	Profile                uint8 // ADTS 里的 profile，等于 AudioObjectType - 1
	AudioObjectType        uint8 // profile + 1，AAC LC = 2
	SamplingFrequencyIndex uint8
	SamplingFrequency      int   // 由 index 查表得到（Hz）
	ChannelConfig          uint8 // 声道数：1=单声道，2=立体声
	FrameLength            int   // 整个 ADTS 帧长度（含头），按它切帧
	HeaderLength           int   // 7（无 CRC）或 9（有 CRC）
}

// ParseADTSHeader 解析 ADTS 头，data 至少 7 字节。
func ParseADTSHeader(data []byte) (*ADTSHeader, error) {
	if len(data) < 7 {
		return nil, errors.New("adts header too short, min 7 bytes")
	}
	// syncword 0xFFF + layer==00（0xF6 掩掉 ID 和 protection_absent 两位）
	if data[0] != 0xFF || (data[1]&0xF6) != 0xF0 {
		return nil, errors.New("invalid adts syncword")
	}

	h := &ADTSHeader{}
	h.ID = (data[1] >> 3) & 0x01
	h.ProtectionAbsent = data[1]&0x01 == 1

	h.Profile = (data[2] >> 6) & 0x03
	h.AudioObjectType = h.Profile + 1
	h.SamplingFrequencyIndex = (data[2] >> 2) & 0x0F
	// channel_configuration：byte2 最低 1 位是最高位，byte3 高 2 位是低两位
	h.ChannelConfig = ((data[2] & 0x01) << 2) | ((data[3] >> 6) & 0x03)

	// frame_length 13 位：byte3 低 2 位 + byte4 整字节 + byte5 高 3 位
	h.FrameLength = int(data[3]&0x03)<<11 | int(data[4])<<3 | int(data[5]>>5)&0x07

	h.HeaderLength = 7
	if !h.ProtectionAbsent {
		h.HeaderLength = 9
	}

	if int(h.SamplingFrequencyIndex) < len(samplingFrequencyTable) {
		h.SamplingFrequency = samplingFrequencyTable[h.SamplingFrequencyIndex]
	}
	return h, nil
}

// AudioSpecificConfig 把 ADTS 头换算成 2 字节的 AudioSpecificConfig（ISO/IEC 14496-3），
// 供 SDP 的 fmtp 里 config= 使用。位布局：
//
//	bit 15..11 audioObjectType       ADTS profile + 1（AAC LC = 2）
//	bit 10..7  samplingFrequencyIndex
//	bit  6..3  channelConfiguration
//	bit  2..0  frameLengthFlag / dependsOnCoreCoder / extensionFlag，AAC LC 全为 0
//
// 例：AAC LC / 44100 / 立体声 → 0x12 0x10。
func (h *ADTSHeader) AudioSpecificConfig() []byte {
	asc := uint16(h.AudioObjectType&0x1F)<<11 |
		uint16(h.SamplingFrequencyIndex&0x0F)<<7 |
		uint16(h.ChannelConfig&0x0F)<<3
	return []byte{byte(asc >> 8), byte(asc)}
}

// ParseASC 解析 AudioSpecificConfig（ISO/IEC 14496-3），返回采样率和声道数。
// 位布局与 AudioSpecificConfig 相反：
//
//	bit 15..11 audioObjectType（AAC LC = 2；31 表示后面还有 6 位显式类型，暂不支持）
//	bit 10..7  samplingFrequencyIndex（15 表示后面跟 24 位显式采样率，暂不支持）
//	bit  6..3  channelConfiguration（0 表示 PCE，暂不支持）
//
// RTMP 推流收到 FLV 音频序列头后用它推算 SDP/FLV 需要的参数。
func ParseASC(asc []byte) (sampleRate int, channels uint8, err error) {
	if len(asc) < 2 {
		return 0, 0, errors.New("asc too short, min 2 bytes")
	}
	audioObjectType := asc[0] >> 3
	if audioObjectType == 31 {
		return 0, 0, errors.New("explicit audio object type not supported")
	}
	sfi := (asc[0]&0x07)<<1 | asc[1]>>7
	if sfi == 15 {
		return 0, 0, errors.New("explicit sampling frequency not supported")
	}
	if int(sfi) >= len(samplingFrequencyTable) {
		return 0, 0, fmt.Errorf("reserved sampling frequency index %d", sfi)
	}
	ch := (asc[1] >> 3) & 0x0F
	if ch == 0 {
		return 0, 0, errors.New("channel configuration 0 (PCE) not supported")
	}
	return samplingFrequencyTable[sfi], ch, nil
}

// AACDemux ADTS 音频解封装器：按 ADTS 头里的 frame_length 切出每个 AU。
type AACDemux struct {
	buf []byte
	off int
	// First 记录第一帧解析出的头信息，供后面拼 SDP 的 AudioSpecificConfig 用
	First *ADTSHeader
}

// NewAACDemux 读取整个 ADTS 文件到内存，返回解封装器。
func NewAACDemux(filePath string) (*AACDemux, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	return &AACDemux{buf: buf, off: 0}, nil
}

// NextAU 读取下一个 AU（Access Unit），返回去掉 ADTS 头之后的裸 AAC 帧。
// 没有更多数据时返回 io.EOF。
func (d *AACDemux) NextAU() ([]byte, error) {
	n := len(d.buf)
	if d.off >= n {
		return nil, io.EOF
	}

	start := d.findSyncWord(d.off)
	if start == -1 {
		return nil, io.EOF
	}

	hdr, err := ParseADTSHeader(d.buf[start:])
	if err != nil || hdr.FrameLength < hdr.HeaderLength {
		// 非法头：跳过当前字节继续找
		d.off = start + 1
		return d.NextAU()
	}

	end := start + hdr.FrameLength
	if end > n {
		// 文件被截断，剩余部分直接丢弃
		d.off = n
		return nil, io.EOF
	}

	if d.First == nil {
		d.First = hdr
	}

	au := d.buf[start+hdr.HeaderLength : end]
	d.off = end
	if len(au) == 0 {
		return d.NextAU()
	}
	return au, nil
}

// findSyncWord 从 offset 起找 ADTS syncword（0xFFF + layer==00），找不到返回 -1。
func (d *AACDemux) findSyncWord(offset int) int {
	data := d.buf
	n := len(data)
	for i := offset; i <= n-2; i++ {
		if data[i] == 0xFF && (data[i+1]&0xF6) == 0xF0 {
			return i
		}
	}
	return -1
}

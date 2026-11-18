package media

// MPEG 音频帧头解析（ISO/IEC 11172-3 / 13818-3）。
//
// 只需要拿到 RTMP/RTSP 互转时用到的参数：采样率、声道数、每帧采样数
// 和帧长度。帧头格式：
//
//	AAAAAAAA AAABBCCD EEEEFFGH IIJJKLMM
//	A sync(11)  B version(2)  C layer(2)  D protection(1)
//	E bitrate index(4)  F samplerate index(2)  G padding(1)
//	H private(1)  I channel mode(2)  J..M 其它
//
// 本包不做 CRC 校验，也不处理 ID3v2 标签；调用方保证 data 从帧同步字开始。

// MP3FrameInfo 一个 MPEG 音频帧的头部信息。
type MP3FrameInfo struct {
	SampleRate uint32 // 44100 / 48000 / 32000 / 22050 / ... / 8000
	Channels   uint8  // 1=单声道，2=立体声/联合立体声/双声道
	Samples    int    // 这一帧包含的采样数：Layer I=384，Layer II=1152，Layer III=1152/576
	Length     int    // 整帧字节数（含头）
	Version    int    // 1 / 2 / 25（MPEG 2.5 记 25）
	Layer      int    // 1 / 2 / 3
	Bitrate    int    // kbps
}

var mp3SampleRates = map[int][3]uint32{
	1:  {44100, 48000, 32000},
	2:  {22050, 24000, 16000},
	25: {11025, 12000, 8000},
}

var mp3BitrateTables = map[int]map[int][15]int{
	1: {
		1: {0, 32, 64, 96, 128, 160, 192, 224, 256, 288, 320, 352, 384, 416, 448},
		2: {0, 32, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384},
		3: {0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320},
	},
	2: {
		1: {0, 32, 48, 56, 64, 80, 96, 112, 128, 144, 160, 176, 192, 224, 256},
		2: {0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160},
		3: {0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160},
	},
}

// ParseMP3Frame 解析 data 开头的 MPEG 音频帧头。
// 同步字不匹配、版本/层/采样率/比特率非法时返回 ok=false。
func ParseMP3Frame(data []byte) (info MP3FrameInfo, ok bool) {
	if len(data) < 4 {
		return MP3FrameInfo{}, false
	}
	if data[0] != 0xFF || data[1]&0xE0 != 0xE0 {
		return MP3FrameInfo{}, false
	}

	// 版本：00=MPEG2.5，01 保留，10=MPEG2，11=MPEG1。
	var version int
	switch (data[1] >> 3) & 0x03 {
	case 0:
		version = 25
	case 2:
		version = 2
	case 3:
		version = 1
	default:
		return MP3FrameInfo{}, false
	}

	// 层：00 保留，01=III，10=II，11=I。
	var layer int
	switch (data[1] >> 1) & 0x03 {
	case 1:
		layer = 3
	case 2:
		layer = 2
	case 3:
		layer = 1
	default:
		return MP3FrameInfo{}, false
	}

	srIndex := int(data[2] >> 2 & 0x03)
	if srIndex == 3 {
		return MP3FrameInfo{}, false
	}
	rates, ok := mp3SampleRates[version]
	if !ok {
		return MP3FrameInfo{}, false
	}
	info.SampleRate = rates[srIndex]
	info.Version = version
	info.Layer = layer

	// 比特率索引 0 表示 free format，15 非法；两者都算无法确定帧长。
	brIndex := int(data[2] >> 4 & 0x0F)
	if brIndex == 0 || brIndex == 15 {
		return MP3FrameInfo{}, false
	}
	verKey := 1
	if version != 1 {
		verKey = 2
	}
	table, ok := mp3BitrateTables[verKey]
	if !ok {
		return MP3FrameInfo{}, false
	}
	bitrates, ok := table[layer]
	if !ok {
		return MP3FrameInfo{}, false
	}
	info.Bitrate = bitrates[brIndex]
	if info.Bitrate == 0 {
		return MP3FrameInfo{}, false
	}

	padding := int(data[2] >> 1 & 0x01)
	sr := int(info.SampleRate)
	switch layer {
	case 1:
		info.Samples = 384
		info.Length = (12*info.Bitrate*1000/sr + padding) * 4
	case 2:
		info.Samples = 1152
		info.Length = 144*info.Bitrate*1000/sr + padding
	case 3:
		if version == 1 {
			info.Samples = 1152
			info.Length = 144*info.Bitrate*1000/sr + padding
		} else {
			info.Samples = 576
			info.Length = 72*info.Bitrate*1000/sr + padding
		}
	}
	if info.Length <= 4 {
		return MP3FrameInfo{}, false
	}

	// channel mode 11 为单声道，其余（立体声/联合立体声/双声道）按 2 声道。
	if data[3]>>6 == 0x03 {
		info.Channels = 1
	} else {
		info.Channels = 2
	}
	return info, true
}

// MP3FrameSamples 返回帧头里的每帧采样数；解析失败时按 MPEG-1 Layer III
// 的 1152 兜底。
func MP3FrameSamples(data []byte) int {
	if info, ok := ParseMP3Frame(data); ok {
		return info.Samples
	}
	return 1152
}

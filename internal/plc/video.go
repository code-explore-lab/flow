package plc

// Video 是视频接收侧的帧级丢包隐藏状态：记住最近一个完整访问单元（AU），
// 丢包时用它在当前 AU 的时间戳上重复，直到解码器拿到新的关键帧。
//
// 注意：H.264 的 RTP 时间戳是呈现时间（PTS），有 B 帧时到达顺序不单调，
// 因此这里不做任何基于时间戳步长的缺帧推断——丢包完全由接收路径的 seq
// 缺口检测，时间戳只用来标记重复帧的位置。

const (
	// DefaultMaxRepeat 是连续重复上一帧的上限。超过后停止重复，
	// 避免长时间断流时画面永久冻结在同一帧。
	DefaultMaxRepeat = 5
)

// Video 不是并发安全的；每个推流轨一个实例，只由接收 goroutine 使用。
type Video struct {
	maxRepeat int
	last      [][]byte // 最近一个完整 AU 的 NALU（深拷贝）
	hasLast   bool
	repeats   int
}

// NewVideo 创建视频 PLC。maxRepeat <= 0 时使用 DefaultMaxRepeat。
func NewVideo(maxRepeat int) *Video {
	if maxRepeat <= 0 {
		maxRepeat = DefaultMaxRepeat
	}
	return &Video{maxRepeat: maxRepeat}
}

// Reset 丢弃缓存的上一帧（SSRC 变化/重新开始时调用）。
func (v *Video) Reset() {
	v.last = nil
	v.hasLast = false
	v.repeats = 0
}

// Observe 记录一个完整、可解码的 AU，作为后续丢帧时的重复源。
// 新的完整 AU 也重置连续重复计数。
func (v *Video) Observe(nalus [][]byte) {
	v.last = make([][]byte, len(nalus))
	for i, nalu := range nalus {
		v.last[i] = append([]byte(nil), nalu...)
	}
	v.hasLast = true
	v.repeats = 0
}

// Conceal 返回上一帧的 NALU，用于在当前 AU 的时间戳上重发。
// 返回的切片归 Video 所有，调用方只能读。没有上一帧或连续重复次数
// 已达上限时返回 false。
func (v *Video) Conceal() ([][]byte, bool) {
	if !v.hasLast || v.repeats >= v.maxRepeat {
		return nil, false
	}
	v.repeats++
	return v.last, true
}

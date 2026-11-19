package source

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"flow/internal/media"
)

// ExternalSource 一个由外部写入的媒体源（RTSP 推流端用）。
//
// 与 H264Source 的区别：它没有自己的源 goroutine，帧由调用方（RECORD 处理
// 逻辑把收到的 RTP 解包后）通过 Write 塞进来；拉流客户端通过 Subscribe 订阅。
// 内部仍是同一个 RingBuffer[Frame]，慢消费者照旧只丢自己的老帧、不拖累整体。
type ExternalSource struct {
	mu   sync.Mutex
	ring *RingBuffer[Frame]

	// cacheParamSets 为 true 时（视频轨）按 NAL 类型缓存参数集/IDR，
	// 中途加入的订阅者先补齐再解码。音频帧没有 NAL 头，必须关掉，
	// 否则 AU 首字节会被误判成 SPS/PPS/IDR。
	cacheParamSets bool

	// codec 是视频编码（"H264" / "H265"），决定 NAL 头按哪套语法解析。
	// 音频轨不使用，保持空值。
	codec string

	// 参数集缓存：中途加入的订阅者先补 VPS/SPS/PPS + 最近一个 IDR，收到就能解码。
	// H.264 没有 VPS，vps 为 nil。在 Write 里写、在 Subscribe 里读，都拿 mu 串行化。
	vps, sps, pps, idr *Frame

	// GOP 缓存：最近一个 IDR 起的所有帧。只补 IDR 而不补它后面的帧，
	// 中途加入的订阅者会缺参考帧（解码器报 POC 错误、时间戳乱序），
	// 所以订阅时按顺序把整个 GOP 先发一遍，再接实时帧。
	gop      []Frame
	gopBytes int
	// lastWasIDR 记录上一次写入的是否 IDR slice，用来区分「新关键帧 AU」
	// 和同一个 AU 的后续 slice（后者不能清空 GOP 缓存）。
	lastWasIDR bool

	// useAUFlags 表示写入方会标记 AUStart/AUEnd。只有这样的源才按 slice
	// 拆帧；不标记的老源一帧就是一个完整 AU，预热时不能擅自改边界。
	useAUFlags bool

	// rate 音频 RTP 时钟（= AAC 采样率），从 ANNOUNCE 的 rtpmap 带过来。
	// 拉流端 streamAAC 用它设置 RTCP SR 的 RTP 时钟。
	rate uint32

	// lastWrite 是最近一次 Write 的时间（Unix 纳秒）。T-8 用它判断一条
	// 已发布流是否还在出帧，供 reject-if-active 区分活跃推流和僵尸连接。
	lastWrite atomic.Int64
	// wrote 表示是否至少写入过一帧。LastWrite 在构造时就置为当前时间，
	// 不能拿来区分「刚发布还没出帧」和「已经出过帧」；O-4 的告警需要后者。
	wrote atomic.Bool
}

// NewExternalSource 创建一个空的视频推流源（缓存参数集/IDR 供预热），
// 默认按 H.264 解析 NAL；H.265 流由 SetCodec 切换。
func NewExternalSource() *ExternalSource {
	return newExternalSource(true)
}

// SetCodec 设置视频编码（"H264" / "H265"）。空串按 H.264 处理。
// 一般由 stream.SetVideoParams 在拿到编码参数时调用；必须在写入帧之前设置。
func (s *ExternalSource) SetCodec(codec string) {
	if codec == "" {
		codec = "H264"
	}
	s.mu.Lock()
	s.codec = codec
	s.mu.Unlock()
}

// Codec 返回当前视频编码；未设置时返回 "H264"。
func (s *ExternalSource) Codec() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.codec == "" {
		return "H264"
	}
	return s.codec
}

// NewExternalAudioSource 创建一个空的音频推流源。
// 音频 AU 没有 NAL 头，不做参数集缓存，订阅者直接跟实时帧。
func NewExternalAudioSource() *ExternalSource {
	s := newExternalSource(false)
	s.rate = aacDefaultFrequency
	return s
}

// SetSamplingRate 记录音频采样率（ANNOUNCE rtpmap 的 clock rate）。
func (s *ExternalSource) SetSamplingRate(rate uint32) {
	if rate == 0 {
		return
	}
	s.mu.Lock()
	s.rate = rate
	s.mu.Unlock()
}

// SamplingRate 返回音频采样率；未设置时返回 AAC 默认值。
// 与 AACSource 的同名方法保持同一语义，streamAAC 不区分源的类型。
func (s *ExternalSource) SamplingRate() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rate == 0 {
		return aacDefaultFrequency
	}
	return s.rate
}

func newExternalSource(cacheParamSets bool) *ExternalSource {
	s := &ExternalSource{
		ring:           newVideoRing(),
		cacheParamSets: cacheParamSets,
	}
	// 刚创建的源先算「活跃”：RTSP 在 ANNOUNCE 就登记流，首帧要等 RECORD
	// 之后才到，期间不能被当成僵尸流替换掉。
	s.lastWrite.Store(time.Now().UnixNano())
	return s
}

// LastWrite 返回最近一次写入帧的时间；从未写入时返回源的创建时间。
func (s *ExternalSource) LastWrite() time.Time {
	return time.Unix(0, s.lastWrite.Load())
}

// Wrote 报告这个源是否至少收到过一帧。用于把「推流刚建立、首帧还没到」
// 和「正常出过帧后中断」区分开。
func (s *ExternalSource) Wrote() bool {
	return s.wrote.Load()
}

// GOP 缓存上限：只保留最近一个关键帧起的有限窗口，防止长 GOP 无限增长。
const (
	maxGOPFrames = 256
	maxGOPBytes  = 1 << 20
)

// Write 写入一帧。视频轨按 NAL 类型缓存 SPS/PPS/IDR 和最近一个 GOP，
// 供中途加入的订阅者预热。任意 goroutine 都可以调用；源 Close 之后写入会被忽略。
//
// 缓存更新和 ring 写入在同一把锁里完成，Subscribe 快照到的 GOP 一定已经
// 写进 ring（读者从当前写位置开始读，不会重复收到缓存帧）。
func (s *ExternalSource) Write(f Frame) {
	s.lastWrite.Store(time.Now().UnixNano())
	s.wrote.Store(true)
	if s.cacheParamSets {
		s.mu.Lock()
		defer s.mu.Unlock()
		c := f
		class := media.ClassifyVideoNAL(s.codec, f.Data)
		// 一个关键帧 AU 可能被拆成多个 slice，每个 slice 都是 IDR 类型；
		// 只有新 AU 的首个 slice 才开启新 GOP，否则缓存里只剩最后一个
		// slice，中途加入的订阅者会缺参考数据、首帧解码花屏。
		newGOP := class.IsIDR && (f.AUStart || !s.lastWasIDR)
		switch {
		case class.IsVPS:
			s.vps = &c
		case class.IsSPS:
			s.sps = &c
		case class.IsPPS:
			s.pps = &c
		case class.IsIDR:
			s.idr = &c
			if newGOP {
				s.gop = s.gop[:0]
				s.gopBytes = 0
			}
		}
		s.lastWasIDR = class.IsIDR
		if f.AUStart || f.AUEnd {
			s.useAUFlags = true
		}
		// 关键帧之后的帧都属于当前 GOP；IDR 之前的参数集不入缓存。
		if len(s.gop) > 0 || newGOP {
			if len(s.gop) < maxGOPFrames && s.gopBytes+len(f.Data) <= maxGOPBytes {
				s.gop = append(s.gop, c)
				s.gopBytes += len(f.Data)
			}
		}
	}
	s.ring.Write(f)
}

// Subscribe 订阅帧。中途加入的订阅者会先收到缓存的 SPS/PPS 和最近一个
// GOP（关键帧及其后续帧），再跟实时帧——只补关键帧会让解码器缺参考帧。
// 返回的 cancel 只退订，不会停止源本身。
func (s *ExternalSource) Subscribe() (*Reader[Frame], func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var preload []Frame
	if s.idr != nil {
		if s.vps != nil {
			preload = append(preload, *s.vps)
		}
		if s.sps != nil {
			preload = append(preload, *s.sps)
		}
		if s.pps != nil {
			preload = append(preload, *s.pps)
		}
		// 按 slice 拆帧的源里，GOP 从 IDR slice 开始，AUStart 原本标在
		// AUD 上（不在缓存里）。预热的首个媒体帧必须补上 AUStart，否则
		// 按 AU 合并的封装器（HTTP-FLV 等）会把每个 slice 当成独立一帧。
		if len(s.gop) > 0 {
			gop := make([]Frame, len(s.gop))
			copy(gop, s.gop)
			if s.useAUFlags {
				gop[0].AUStart = true
			}
			preload = append(preload, gop...)
		} else {
			idr := *s.idr
			if s.useAUFlags {
				idr.AUStart = true
			}
			preload = append(preload, idr)
		}
	}
	rd := s.ring.Subscribe(preload...)
	return rd, rd.Close
}

// SubscribeLive 从当前写位置订阅，不预加载 SPS/PPS/最近 GOP。
// HLS 打包器用它，避免旧 GOP 污染 PDT 锚点。
func (s *ExternalSource) SubscribeLive() (*Reader[Frame], func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rd := s.ring.Subscribe()
	return rd, rd.Close
}

// SubscribeFrom 实时推流源没有可定位/调速的文件，不支持 seek 和 Scale。
// RTSP 层在 live/推流模式已经用 457/551 拒掉对应请求，这里只做兜底。
func (s *ExternalSource) SubscribeFrom(time.Duration, float64) (*Reader[Frame], func(), error) {
	return nil, nil, errors.New("external source: live stream is not seekable")
}

// Close 结束这个源：唤醒所有读者，让它们把剩余帧读完再退出。
// 之后的 Write 会被忽略。
func (s *ExternalSource) Close() {
	s.ring.Close()
}

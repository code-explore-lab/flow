package source

import (
	"errors"
	"sync"
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

	// rate 音频 RTP 时钟（= AAC 采样率），从 ANNOUNCE 的 rtpmap 带过来。
	// 拉流端 streamAAC 用它设置 RTCP SR 的 RTP 时钟。
	rate uint32
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
	return &ExternalSource{
		ring:           newVideoRing(),
		cacheParamSets: cacheParamSets,
	}
}

// Write 写入一帧。视频轨按 NAL 类型缓存 SPS/PPS/IDR，供中途加入的订阅者预热。
// 任意 goroutine 都可以调用；源 Close 之后写入会被忽略。
func (s *ExternalSource) Write(f Frame) {
	if s.cacheParamSets {
		c := f
		s.mu.Lock()
		class := media.ClassifyVideoNAL(s.codec, f.Data)
		switch {
		case class.IsVPS:
			s.vps = &c
		case class.IsSPS:
			s.sps = &c
		case class.IsPPS:
			s.pps = &c
		case class.IsIDR:
			s.idr = &c
		}
		s.mu.Unlock()
	}
	s.ring.Write(f)
}

// Subscribe 订阅帧。中途加入的订阅者会先收到缓存的 SPS/PPS/IDR，再跟实时帧。
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
		preload = append(preload, *s.idr)
	}
	rd := s.ring.Subscribe(preload...)
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

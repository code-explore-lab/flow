package source

import (
	"sync"

	"flow/internal/media"
	"flow/internal/rtp"
)

// ExternalSource 一个由外部写入的媒体源（RTSP 推流端用）。
//
// 与 H264Source 的区别：它没有自己的源 goroutine，帧由调用方（RECORD 处理
// 逻辑把收到的 RTP 解包后）通过 Write 塞进来；拉流客户端通过 Subscribe 订阅。
// 内部仍是同一个 RingBuffer[Frame]，慢消费者照旧只丢自己的老帧、不拖累整体。
type ExternalSource struct {
	mu   sync.Mutex
	ring *RingBuffer[Frame]

	// 参数集缓存：中途加入的订阅者先补 SPS/PPS + 最近一个 IDR，收到就能解码。
	// 在 Write 里写、在 Subscribe 里读，都拿 mu 串行化。
	sps, pps, idr *Frame
}

// NewExternalSource 创建一个空的推流源。
func NewExternalSource() *ExternalSource {
	return &ExternalSource{ring: NewRingBuffer[Frame](videoRingCapacity)}
}

// Write 写入一帧。按 NAL 类型缓存 SPS/PPS/IDR，供中途加入的订阅者预热。
// 任意 goroutine 都可以调用；源 Close 之后写入会被忽略。
func (s *ExternalSource) Write(f Frame) {
	if typ, err := media.NALType(f.Data); err == nil {
		c := f
		s.mu.Lock()
		switch typ {
		case rtp.NALUTypeSPS:
			s.sps = &c
		case rtp.NALUTypePPS:
			s.pps = &c
		case rtp.NALUTypeIDR:
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

// Close 结束这个源：唤醒所有读者，让它们把剩余帧读完再退出。
// 之后的 Write 会被忽略。
func (s *ExternalSource) Close() {
	s.ring.Close()
}

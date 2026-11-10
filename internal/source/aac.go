package source

import (
	"fmt"
	"sync"
	"time"

	"flow/internal/media"
	"flow/internal/rtp"
)

const (
	aacDefaultFrequency = 44100
	aacRingCapacity     = 64
)

// AACSource 单源音频：一个 goroutine 读 test.aac，按采样率节奏把 AU 写进 ring buffer。
// 与 H264Source 一样，第一次 Subscribe 时启动，读完自动结束，下次订阅重新开始。
type AACSource struct {
	path string

	mu   sync.Mutex
	cur  *aacRun
	rate uint32 // 最近一轮解析出的采样率，RTCP SR 的 RTP 时钟要用
}

type aacRun struct {
	ring *RingBuffer[Frame]
}

// NewAACSource 创建一个音频源，path 是 ADTS 格式的 AAC 文件。
func NewAACSource(path string) *AACSource {
	return &AACSource{path: path}
}

// Path 返回音频文件路径。RTMP 拉流端用它读取首帧的 ADTS 头，生成 FLV 的
// AAC 序列头（AudioSpecificConfig）。
func (s *AACSource) Path() string {
	return s.path
}

// SamplingRate 返回最近一轮解析出的采样率；还没解析到 ADTS 头时返回默认值。
// RTP 音频时间戳的时钟频率就是采样率，RTCP SR 必须用同一个值。
func (s *AACSource) SamplingRate() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rate == 0 {
		return aacDefaultFrequency
	}
	return s.rate
}

// Subscribe 订阅音频帧。返回的 Reader 在源读完后可以继续把剩余帧读完，
// 再返回 false；cancel 只退订。
func (s *AACSource) Subscribe() (*Reader[Frame], func()) {
	s.mu.Lock()
	if s.cur == nil {
		run := &aacRun{ring: NewRingBuffer[Frame](aacRingCapacity)}
		s.cur = run
		// 先注册再启动，避免漏掉第一帧
		rd := run.ring.Subscribe()
		s.mu.Unlock()
		go s.run(run)
		return rd, rd.Close
	}
	rd := s.cur.ring.Subscribe()
	s.mu.Unlock()
	return rd, rd.Close
}

// run 是唯一的音频源 goroutine。AAC LC 每个 AU 固定 1024 个采样，
// 时间戳每次 +1024；发包节奏 = 1024 / sampling_rate。
func (s *AACSource) run(run *aacRun) {
	demux, err := media.NewAACDemux(s.path)
	if err != nil {
		fmt.Printf("aac source: open %s: %v\n", s.path, err)
		s.finish(run)
		return
	}
	au, err := demux.NextAU()
	if err != nil {
		fmt.Printf("aac source: %v\n", err)
		s.finish(run)
		return
	}
	freq := aacDefaultFrequency
	if demux.First != nil && demux.First.SamplingFrequency > 0 {
		freq = demux.First.SamplingFrequency
	}
	frameDur := time.Duration(float64(time.Second) * float64(rtp.AACFrameSamples) / float64(freq))

	s.mu.Lock()
	s.rate = uint32(freq)
	s.mu.Unlock()

	fmt.Printf("aac source: start (%s, %dHz)\n", s.path, freq)
	ticker := time.NewTicker(frameDur)
	defer ticker.Stop()

	var (
		ts    uint32
		first = true
	)
	for {
		if !first {
			<-ticker.C
		}
		first = false

		run.ring.Write(Frame{Data: au, Timestamp: ts, Keyframe: true})
		ts += rtp.AACFrameSamples

		au, err = demux.NextAU()
		if err != nil {
			fmt.Printf("aac source: %v\n", err)
			break
		}
	}
	s.finish(run)
}

func (s *AACSource) finish(run *aacRun) {
	s.mu.Lock()
	if s.cur == run {
		s.cur = nil
	}
	s.mu.Unlock()
	run.ring.Close()
	fmt.Printf("aac source: stop\n")
}

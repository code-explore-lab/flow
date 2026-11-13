package source

import (
	"fmt"
	"io"
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

	// 文件 seek 索引：每个 ADTS 帧的起始偏移。AAC 每帧固定 1024 个采样，
	// 目标 AU 序号 = start / (1024/采样率)，按序号直接查偏移。
	indexOnce sync.Once
	auOffsets []int
	indexRate uint32
	indexErr  error
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

// SubscribeFrom 从 start 对应的 AU 开始播放，返回独立的 Reader。
// 音频没有关键帧概念，按「每帧 1024 采样」把时间换成 AU 序号即可，
// 这样音视频 seek 到同一时间点后仍能保持内容同步。
func (s *AACSource) SubscribeFrom(start time.Duration, scale float64) (*Reader[Frame], func(), error) {
	if err := s.buildSeekIndex(); err != nil {
		return nil, nil, err
	}
	if len(s.auOffsets) == 0 {
		rd, cancel := s.Subscribe()
		return rd, cancel, nil
	}

	freq := s.indexRate
	frameDur := time.Duration(float64(time.Second) * float64(rtp.AACFrameSamples) / float64(freq))
	target := 0
	if start > 0 {
		target = int(start / frameDur)
		if target >= len(s.auOffsets) {
			target = len(s.auOffsets) - 1
		}
	}

	ring := NewRingBuffer[Frame](aacRingCapacity)
	rd := ring.Subscribe()
	done := make(chan struct{})
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			close(done)
			ring.Close()
		})
		rd.Close()
	}
	go s.runFrom(ring, done, s.auOffsets[target], uint32(target)*rtp.AACFrameSamples,
		scaledInterval(frameDur, scale))
	return rd, cancel, nil
}

// buildSeekIndex 扫描整个 ADTS 文件，记录每帧的字节偏移和采样率。
func (s *AACSource) buildSeekIndex() error {
	s.indexOnce.Do(func() {
		demux, err := media.NewAACDemux(s.path)
		if err != nil {
			s.indexErr = fmt.Errorf("aac source: seek index: %w", err)
			return
		}
		freq := uint32(aacDefaultFrequency)
		for {
			_, off, err := demux.NextAUWithOffset()
			if err != nil {
				if err != io.EOF {
					s.indexErr = fmt.Errorf("aac source: seek index: %w", err)
				}
				break
			}
			if demux.First != nil && demux.First.SamplingFrequency > 0 {
				freq = uint32(demux.First.SamplingFrequency)
			}
			s.auOffsets = append(s.auOffsets, off)
		}
		if freq == 0 {
			freq = aacDefaultFrequency
		}
		s.indexRate = freq
		// 让 RTCP SR 的 RTP 时钟即使没跑过普通 run 也是对的。
		s.mu.Lock()
		s.rate = freq
		s.mu.Unlock()
	})
	return s.indexErr
}

// runFrom 是一次 seek/调速播放的音频源 goroutine：从指定 AU 偏移开始读，
// 时间戳从 ts 起每帧 +1024，ticker 间隔按倍率缩放。
func (s *AACSource) runFrom(ring *RingBuffer[Frame], done <-chan struct{}, offset int, ts uint32, interval time.Duration) {
	defer ring.Close()

	demux, err := media.NewAACDemuxAt(s.path, offset)
	if err != nil {
		fmt.Printf("aac source: seek open %s@%d: %v\n", s.path, offset, err)
		return
	}
	fmt.Printf("aac seek source: start at offset %d (ts=%d, interval=%s)\n", offset, ts, interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	first := true
	for {
		select {
		case <-done:
			return
		default:
		}
		au, err := demux.NextAU()
		if err != nil {
			fmt.Printf("aac seek source: %v\n", err)
			return
		}
		if !first {
			select {
			case <-ticker.C:
			case <-done:
				return
			}
		}
		first = false

		ring.Write(Frame{Data: au, Timestamp: ts, Keyframe: true})
		ts += rtp.AACFrameSamples
	}
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

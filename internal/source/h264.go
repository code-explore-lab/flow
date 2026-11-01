package source

import (
	"fmt"
	"sync"
	"time"

	"flow/internal/media"
	"flow/internal/rtp"
)

// Frame 源写入 ring buffer 的一帧数据。
type Frame struct {
	Data      []byte // H.264：一个 NALU（已去 Annex-B 起始码）；AAC：一个 AU（已去 ADTS 头）
	Timestamp uint32 // 相对流起点的 RTP 时间戳（视频 90kHz，音频按采样率）
	Keyframe  bool   // 视频：IDR；音频恒为 true，方便统一处理
}

const (
	videoFrameInterval = 40 * time.Millisecond // 25fps
	videoTimestampStep = 90000 / 25            // 90000/25 = 3600
	videoRingCapacity  = 64                    // ring buffer 保留最近 64 帧（约 2.5s）
)

// H264Source 单源视频：一个 goroutine 读 test.h264，按 25fps 往 ring buffer 写 NALU。
// 第一次 Subscribe 时启动源；文件读完自动结束，下一次 Subscribe 会重新读一遍。
type H264Source struct {
	path string

	mu  sync.Mutex
	cur *h264Run
}

// h264Run 一次「从文件头念到尾」的运行实例。
type h264Run struct {
	ring *RingBuffer[Frame]

	// 参数集缓存：中途加入的订阅者先补 SPS/PPS + 最近一个 IDR，收到就能解码。
	// 只在 run goroutine 里写、在 Subscribe 里读，两者都拿 H264Source.mu。
	sps, pps, idr *Frame
}

// NewH264Source 创建一个视频源，path 是 Annex-B 裸流文件。
func NewH264Source(path string) *H264Source {
	return &H264Source{path: path}
}

// Subscribe 订阅视频帧。
// 返回的 Reader 在源读完后可以继续把剩余帧读完，再返回 false；cancel 只退订，
// 不会停止源本身。
func (s *H264Source) Subscribe() (*Reader[Frame], func()) {
	s.mu.Lock()
	if s.cur == nil {
		run := &h264Run{ring: NewRingBuffer[Frame](videoRingCapacity)}
		s.cur = run
		// 先注册读者再启动 goroutine，否则源可能先把开头的 SPS/PPS 写进去，新客户端漏掉
		rd := run.ring.Subscribe()
		s.mu.Unlock()
		go s.run(run)
		return rd, rd.Close
	}

	// 中途加入：从当前写位置开始读，先补发参数集和最近一个 IDR
	run := s.cur
	var preload []Frame
	if run.idr != nil {
		if run.sps != nil {
			preload = append(preload, *run.sps)
		}
		if run.pps != nil {
			preload = append(preload, *run.pps)
		}
		preload = append(preload, *run.idr)
	}
	rd := run.ring.Subscribe(preload...)
	s.mu.Unlock()
	return rd, rd.Close
}

// run 是唯一的源 goroutine：读 NALU -> 打时间戳 -> 写 ring buffer。
func (s *H264Source) run(run *h264Run) {
	demux, err := media.NewH264Demux(s.path)
	if err != nil {
		fmt.Printf("h264 source: open %s: %v\n", s.path, err)
		s.finish(run)
		return
	}
	fmt.Printf("h264 source: start (%s)\n", s.path)

	ticker := time.NewTicker(videoFrameInterval)
	defer ticker.Stop()

	var (
		ts       uint32
		firstVCL = true
	)
	for {
		nalu, err := demux.NextNALU()
		if err != nil {
			fmt.Printf("h264 source: %v\n", err)
			break
		}
		typ, err := media.NALType(nalu)
		if err != nil {
			continue
		}
		// VCL NALU（type 1~5）才代表一帧；SPS/PPS 不占节奏、不推进时间戳
		isVCL := typ >= rtp.NALUTypeNonIDR && typ <= rtp.NALUTypeIDR
		if isVCL && !firstVCL {
			<-ticker.C
		}

		f := Frame{Data: nalu, Timestamp: ts, Keyframe: typ == rtp.NALUTypeIDR}

		s.mu.Lock()
		switch typ {
		case rtp.NALUTypeSPS:
			c := f
			run.sps = &c
		case rtp.NALUTypePPS:
			c := f
			run.pps = &c
		case rtp.NALUTypeIDR:
			c := f
			run.idr = &c
		}
		s.mu.Unlock()

		run.ring.Write(f)

		if isVCL {
			ts += videoTimestampStep
			firstVCL = false
		}
	}
	s.finish(run)
}

// finish 结束一次运行：把 cur 置空（下一次订阅开启新一轮）并关闭 ring buffer，
// 读者把剩余帧读完后会得到 false。
func (s *H264Source) finish(run *h264Run) {
	s.mu.Lock()
	if s.cur == run {
		s.cur = nil
	}
	s.mu.Unlock()
	run.ring.Close()
	fmt.Printf("h264 source: stop\n")
}

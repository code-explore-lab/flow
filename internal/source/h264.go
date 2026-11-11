package source

import (
	"fmt"
	"io"
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
	videoClockRate     = 90000                 // H.264 RTP 时钟 90kHz
	videoFrameInterval = 40 * time.Millisecond // 文件模式按 25fps 播放
	videoTimestampStep = videoClockRate / 25   // 90000/25 = 3600
	videoRingCapacity  = 64                    // ring buffer 保留最近 64 帧（约 2.5s）
)

// H264Source 单源视频：一个 goroutine 读 Annex-B H.264，往 ring buffer 写 NALU。
//   - 文件模式：按 25fps 节奏读 test.h264，读完自动结束，下一次 Subscribe 重新读；
//   - 实时模式：从 io.Reader（通常是 stdin 的 ffmpeg pipe）读，帧到达就发，
//     时间戳跟随墙上时钟，适合摄像头等没有尽头的流。
//
// 第一次 Subscribe 时启动源。
type H264Source struct {
	path   string
	reader io.Reader // 非 nil 表示实时流模式

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

// NewH264SourceFromReader 创建一个实时视频源，从 r 持续读取 Annex-B H.264。
// 典型用法是 os.Stdin 接 `ffmpeg ... -f h264 pipe:1`。
func NewH264SourceFromReader(r io.Reader) *H264Source {
	return &H264Source{reader: r}
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

// SPSPPSBase64 返回已缓存参数集的 "base64(SPS),base64(PPS)"，供 SDP 的
// sprop-parameter-sets 用。源还没启动、或还没读到参数集时返回空串。
//
// 实时模式下源要等第一次 Subscribe（PLAY）才启动，所以单个客户端先 DESCRIBE
// 时通常还是空的；等有客户端在播、参数集已缓存后，后来者的 DESCRIBE 才能拿到。
func (s *H264Source) SPSPPSBase64() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur == nil || s.cur.sps == nil || s.cur.pps == nil {
		return ""
	}
	return media.SPSPPSBase64(s.cur.sps.Data, s.cur.pps.Data)
}

// naluReader 统一文件解封装和实时流解封装的接口。
type naluReader interface {
	NextNALU() ([]byte, error)
}

// run 是唯一的源 goroutine：读 NALU -> 打时间戳 -> 写 ring buffer。
func (s *H264Source) run(run *h264Run) {
	live := s.reader != nil
	var demux naluReader
	if live {
		demux = media.NewH264StreamDemux(s.reader)
		fmt.Println("h264 source: start (live stream)")
	} else {
		d, err := media.NewH264Demux(s.path)
		if err != nil {
			fmt.Printf("h264 source: open %s: %v\n", s.path, err)
			s.finish(run)
			return
		}
		demux = d
		fmt.Printf("h264 source: start (%s)\n", s.path)
	}

	ticker := time.NewTicker(videoFrameInterval)
	defer ticker.Stop()
	start := time.Now()

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
		if live {
			if isVCL {
				// 实时流不额外限速：ffmpeg 已经按摄像头节奏产出。
				// 时间戳跟随墙上时钟，避免固定 25fps 步进和实际帧率不一致，
				// 也保证和 RTCP SR 里的 NTP↔RTP 映射一致。
				ts = uint32(time.Since(start).Seconds() * float64(videoClockRate))
			}
		} else if isVCL && !firstVCL {
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

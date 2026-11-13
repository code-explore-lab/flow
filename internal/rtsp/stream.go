package rtsp

import (
	"fmt"
	"time"

	"flow/internal/rtp"
	"flow/internal/source"
)

// videoFrameSource 拉流端订阅的帧源抽象。
// 本地文件/live 模式的 *source.H264Source 和推流场景的 *source.ExternalSource
// 都满足这个接口，streamH264 不需要区分数据从哪来。
//
// SubscribeFrom 由 G-2/G-3 的文件 seek 与调速使用：只有文件源能定位/调速，
// live/推流源返回错误（RTSP 层已经用 457/551 挡掉了对应请求）。
type videoFrameSource interface {
	Subscribe() (*source.Reader[source.Frame], func())
	SubscribeFrom(start time.Duration, scale float64) (*source.Reader[source.Frame], func(), error)
}

// audioFrameSource 音频帧源额外提供采样率，用来设置 RTCP SR 的 RTP 时钟。
type audioFrameSource interface {
	videoFrameSource
	SamplingRate() uint32
}

// videoSourceFor 选择这条轨的帧源：已发布流优先，否则用本地全局源。
// 返回 nil 表示没有可用的源（调用方不要启动转发 goroutine）。
func (s *Server) videoSourceFor(t *mediaTrack) videoFrameSource {
	if t.external != nil {
		return t.external
	}
	if s.video != nil {
		return s.video
	}
	return nil
}

// audioSourceFor 选择音频帧源，规则同 videoSourceFor。
func (s *Server) audioSourceFor(t *mediaTrack) audioFrameSource {
	if t.external != nil {
		return t.external
	}
	if s.audio != nil {
		return s.audio
	}
	return nil
}

// streamH264 每个客户端一条订阅 goroutine：
// 从共享视频源拿 Frame → 封 RTP → 发送。
// 源只读一次，多个客户端各自从自己的 Reader 取同一批 NALU；
// seq/SSRC 仍然每个客户端独立，时间戳由源统一打好，保证多路一致。
//
// start > 0 表示文件模式 seek：从该 npt 位置最近的前置 IDR 开始发，
// 时间戳也切换到对应档位；scale != 1 表示按倍率调速（G-3）。
// live/推流源在 handler 层已经拒绝了非零 Range 和非 1 倍速。
//
// 返回 true 表示帧源已经结束（推流端断开 / 文件读完），false 表示是
// done 关闭或写连接失败导致的退出。调用方据此决定是否回收拉流会话。
func (s *Server) streamH264(track *mediaTrack, src videoFrameSource, done <-chan struct{}, start time.Duration, scale float64) bool {
	// 每个客户端拿一个独立的 Reader，按自己的 seq 从共享 ring buffer 读。
	// 客户端慢只会让自己的游标落后、被新帧覆盖，不会阻塞源和其他客户端。
	// seek/调速时由源内部为这次播放单独开一条 run。
	reader, cancel, err := subscribeVideo(src, start, scale)
	if err != nil {
		fmt.Printf("video seek: %v\n", err)
		return false
	}
	defer cancel()

	packer := rtp.NewH264Packer(1400)
	// 推流场景沿用 ANNOUNCE 的 payload type，保证和 DESCRIBE 一致。
	if track.payloadType > 0 {
		packer.PT = uint8(track.payloadType)
	}
	var dropped uint64
	// SSRC 和 seq 从 track 取，跨 PAUSE/PLAY 连续。
	ssrc := track.ensureSSRC()
	for {
		f, ok := reader.Next(done)
		if !ok {
			if isDone(done) {
				fmt.Println("video stream stopped")
				return false
			}
			fmt.Println("video source ended")
			return true
		}
		if d := reader.Dropped(); d != dropped {
			fmt.Printf("video stream: dropped %d old frames (slow consumer)\n", d-dropped)
			dropped = d
		}
		pkts, err := packer.Pack(f.Data)
		if err != nil {
			continue
		}
		for _, p := range pkts {
			p.Header.Seq = uint16(track.nextSeq())
			p.Header.Timestamp = f.Timestamp
			p.Header.SSRC = ssrc
			data, err := p.Header.Marshal(p.Payload)
			if err != nil {
				continue
			}
			if err := track.writeRTP(data); err != nil {
				fmt.Println("write video rtp:", err)
				return false
			}
			// 给 SR 累计包数/字节数，并记下最后一个时间戳。
			track.recordSent(p.Header.Timestamp, len(p.Payload))
			// 保留最近的包，NACK 重传（R-7）按 seq 取回。
			track.cacheSent(p.Header.Seq, data)
		}
	}
}

// subscribeVideo 按是否需要定位/调速选择订阅方式：seek 或非 1 倍速用
// SubscribeFrom，否则沿用共享源的实时订阅。
func subscribeVideo(src videoFrameSource, start time.Duration, scale float64) (*source.Reader[source.Frame], func(), error) {
	if start > 0 || scale != 1 {
		return src.SubscribeFrom(start, scale)
	}
	rd, cancel := src.Subscribe()
	return rd, cancel, nil
}

// streamAAC 每个客户端一条订阅 goroutine：从共享音频源拿 AU → RFC 3640 封包 → 发送。
// 音频时间戳（采样数，每个 AU +1024）由源统一维护，客户端只负责封包和 seq/SSRC。
// start/scale 语义同 streamH264：文件模式 seek 到同一时间点并按同一倍率播放，
// 保持音视频内容对齐。返回值语义同 streamH264：true 表示音频源结束。
func (s *Server) streamAAC(track *mediaTrack, src audioFrameSource, done <-chan struct{}, start time.Duration, scale float64) bool {
	reader, cancel, err := subscribeVideo(src, start, scale)
	if err != nil {
		fmt.Printf("audio seek: %v\n", err)
		return false
	}
	defer cancel()

	packer := rtp.NewAACPacker()
	if track.payloadType > 0 {
		packer.PT = uint8(track.payloadType)
	}
	var dropped uint64
	ssrc := track.ensureSSRC()
	rateSet := false
	for {
		f, ok := reader.Next(done)
		if !ok {
			if isDone(done) {
				fmt.Println("audio stream stopped")
				return false
			}
			fmt.Println("aac source ended")
			return true
		}
		if d := reader.Dropped(); d != dropped {
			fmt.Printf("audio stream: dropped %d old frames (slow consumer)\n", d-dropped)
			dropped = d
		}
		// RTCP SR 的 RTP 时钟 = AAC 采样率。
		// 文件源在第一帧到达时已经解析完 ADTS 头；推流源来自 ANNOUNCE rtpmap。
		if !rateSet {
			track.setClockRate(src.SamplingRate())
			rateSet = true
		}
		pkts, err := packer.Pack(f.Data)
		if err != nil {
			continue
		}
		for _, p := range pkts {
			p.Header.Seq = uint16(track.nextSeq())
			p.Header.Timestamp = f.Timestamp
			p.Header.SSRC = ssrc
			data, err := p.Header.Marshal(p.Payload)
			if err != nil {
				continue
			}
			if err := track.writeRTP(data); err != nil {
				fmt.Println("write audio rtp:", err)
				return false
			}
			track.recordSent(p.Header.Timestamp, len(p.Payload))
			track.cacheSent(p.Header.Seq, data)
		}
	}
}

// sendSenderReports 每 5 秒给这条轨的客户端发一次 SR。
// 播放器靠 NTP↔RTP 映射做音视频同步；有些播放器收不到 SR 会判定
// 发送方失联而停止播放。会话关闭（TEARDOWN / 超时 / TCP 断开）时
// done 关闭，goroutine 跟着退出。
func sendSenderReports(track *mediaTrack, done <-chan struct{}) {
	const interval = 5 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	fmt.Printf("RTCP SR sender started (%s, %s)\n", track.kind, describeTransport(track))
	for {
		select {
		case <-done:
			fmt.Printf("RTCP SR sender stopped (%s)\n", track.kind)
			return
		case now := <-ticker.C:
			sr, ok := track.senderReport(now)
			if !ok {
				continue // 还没发出任何 RTP，等下一轮
			}
			if err := track.writeRTCP(sr); err != nil {
				fmt.Printf("write rtcp sr (%s): %v\n", track.kind, err)
				return
			}
			fmt.Printf("RTCP SR sent (%s, ssrc=%08x, %d bytes)\n",
				track.kind, track.currentSSRC(), len(sr))
		}
	}
}

func describeTransport(t *mediaTrack) string {
	if t.isInterleaved {
		return fmt.Sprintf("tcp ch=%d", t.interleavedRTCP)
	}
	if t.isMulticast {
		return fmt.Sprintf("multicast %s", t.mcRTCPAddr)
	}
	return fmt.Sprintf("udp %s", t.clientRTCPAddr)
}

// isDone 判断 Reader.Next 返回 false 是因为会话结束（done 关闭），
// 还是因为源放完了（ring buffer 关闭）。只用于日志区分。
func isDone(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

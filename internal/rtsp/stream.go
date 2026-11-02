package rtsp

import (
	"fmt"
	"time"

	"flow/internal/rtp"
)

// streamH264 每个客户端一条订阅 goroutine：
// 从共享视频源拿 Frame → 封 RTP → 发送。
// 源只读一次，多个客户端各自从自己的 Reader 取同一批 NALU；
// seq/SSRC 仍然每个客户端独立，时间戳由源统一打好，保证多路一致。
func (s *Server) streamH264(track *mediaTrack, done <-chan struct{}) {
	// 每个客户端拿一个独立的 Reader，按自己的 seq 从共享 ring buffer 读。
	// 客户端慢只会让自己的游标落后、被新帧覆盖，不会阻塞源和其他客户端。
	reader, cancel := s.video.Subscribe()
	defer cancel()

	packer := rtp.NewH264Packer(1400)
	var dropped uint64
	// SSRC 和 seq 从 track 取，跨 PAUSE/PLAY 连续。
	ssrc := track.ensureSSRC()
	for {
		f, ok := reader.Next(done)
		if !ok {
			if isDone(done) {
				fmt.Println("video stream stopped")
			} else {
				fmt.Println("video source ended")
			}
			return
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
				return
			}
			// 给 SR 累计包数/字节数，并记下最后一个时间戳。
			track.recordSent(p.Header.Timestamp, len(p.Payload))
		}
	}
}

// streamAAC 每个客户端一条订阅 goroutine：从共享音频源拿 AU → RFC 3640 封包 → 发送。
// 音频时间戳（采样数，每个 AU +1024）由源统一维护，客户端只负责封包和 seq/SSRC。
func (s *Server) streamAAC(track *mediaTrack, done <-chan struct{}) {
	reader, cancel := s.audio.Subscribe()
	defer cancel()

	packer := rtp.NewAACPacker()
	var dropped uint64
	ssrc := track.ensureSSRC()
	rateSet := false
	for {
		f, ok := reader.Next(done)
		if !ok {
			if isDone(done) {
				fmt.Println("audio stream stopped")
			} else {
				fmt.Println("aac source ended")
			}
			return
		}
		if d := reader.Dropped(); d != dropped {
			fmt.Printf("audio stream: dropped %d old frames (slow consumer)\n", d-dropped)
			dropped = d
		}
		// RTCP SR 的 RTP 时钟 = AAC 采样率。
		// 第一帧到达时源已经解析完 ADTS 头，这时取到的值才是准的。
		if !rateSet {
			track.setClockRate(s.audio.SamplingRate())
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
				return
			}
			track.recordSent(p.Header.Timestamp, len(p.Payload))
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

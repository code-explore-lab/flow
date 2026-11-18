package rtsp

import (
	"fmt"
	"time"

	"flow/internal/media"
	"flow/internal/rtp"
	"flow/internal/source"
	"flow/internal/stream"
)

// videoPacker 是 H.264/H.265 封包器的共同接口：把一批同帧 NALU
// 封成 RTP 包。两层都只用到 PackFrame。
type videoPacker interface {
	PackFrame(nalus [][]byte) ([]*rtp.RTPPacket, error)
}

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
	// 包含一个接口， 方法集合合并
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
// 从共享视频源拿 Frame → 封 RTP → 发送。函数名沿用历史命名，
// 实际按 track.encoding 选 H.264 或 H.265 的封包器（空编码按 H.264）。
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
	// 初始化打包器， SSRC 缓存变量。按编码选 H.264 / H.265 封包器；
	// 未协商编码（本地文件源/测试）按 H.264 处理。
	codec := track.encoding
	const mtu = 1400
	var packer videoPacker
	if media.IsHEVCCodec(codec) {
		p := rtp.NewH265Packer(mtu)
		if track.payloadType > 0 {
			p.PT = uint8(track.payloadType)
		}
		packer = p
	} else {
		p := rtp.NewH264Packer(mtu)
		if track.payloadType > 0 {
			p.PT = uint8(track.payloadType)
		}
		packer = p
	}
	var dropped uint64
	// SSRC 和 seq 从 track 取，跨 PAUSE/PLAY 连续。
	ssrc := track.ensureSSRC()

	// R-2：同一访问单元（一帧）的所有 NALU 先攒在一起再封包，SPS/PPS
	// 和同一帧的多个 slice 就能一起进 STAP-A，整帧只有一个 marker。
	// 源的帧带 AUStart（本地 H.264 源）时，批次在下一个 AU 首 slice
	// 到达时结算；不带 AUStart 的推流源沿用原来的逐步转发行为。
	var (
		pending   [][]byte
		pendingTS uint32
		auOpen    bool // 正在攒一个可能还有后续 slice 的 AU
	)
	// 发包回调，NALU 类型判断、降码丢弃、RTP 封装、写连接、缓存 PLI 关键帧
	sendFrame := func(nalus [][]byte, ts uint32) bool {
		// R-8：识别这一帧是否带 IDR / 参数集，发送成功后缓存整帧的
		// 完整 RTP 包，供 PLI/FIR 原样重放。
		isIDR, hasParams, hasVCL := false, false, false
		for _, n := range nalus {
			if len(n) == 0 {
				continue
			}
			class := media.ClassifyVideoNAL(codec, n)
			isIDR = isIDR || class.IsIDR
			hasParams = hasParams || class.IsVPS || class.IsSPS || class.IsPPS
			hasVCL = hasVCL || class.IsVCL
		}
		// C-6：降级期间只放行参数集和 IDR，整 AU 丢弃非关键帧。
		// 只丢整个 AU，不拆 NALU，seq/timestamp 不会错乱。
		if hasVCL && !isIDR && !hasParams && track.dropNonKeyFrame() {
			s.metrics.AddDropped(track.kind, 1)
			return true
		}

		pkts, err := packer.PackFrame(nalus)
		if err != nil {
			return true
		}
		au := make([][]byte, 0, len(pkts))
		for _, p := range pkts {
			p.Header.Seq = uint16(track.nextSeq())
			p.Header.Timestamp = ts
			p.Header.SSRC = ssrc
			data, err := p.Header.Marshal(p.Payload)
			if err != nil {
				continue
			}
			if err := track.writeRTP(data); err != nil {
				fmt.Println("write video rtp:", err)
				return false
			}
			au = append(au, data)
			// 给 SR 累计包数/字节数，并记下最后一个时间戳。
			track.recordSent(p.Header.Timestamp, len(p.Payload))
			s.metrics.AddPackets(track.kind, 1)
			s.metrics.AddOctets(track.kind, uint64(len(p.Payload)))
			// 保留最近的包，NACK 重传（R-7）按 seq 取回。
			track.cacheSent(p.Header.Seq, data)
		}
		// 整帧都发成功后再登记，避免把只发了一半的 AU 当作关键帧重放。
		// 只有单播拉流轨会收到客户端 PLI/FIR；用 sendCache 做同样的判定，
		// 避免为组播轨缓存体积较大的 IDR 帧。
		if track.sendCache != nil {
			if isIDR {
				track.storeKeyframe(au, hasParams)
			} else if hasParams {
				track.storeParamSets(au)
			}
		}
		return true
	}
	// 把 pending 里属于同一个 AU 的 NALU 交给 sendFrame 封包发送。
	flush := func() bool {
		if len(pending) == 0 {
			return true
		}
		ok := sendFrame(pending, pendingTS)
		pending = nil
		return ok
	}

	for { // 单客户端视频流的主循环
		f, ok := reader.Next(done)
		if !ok {
			if isDone(done) {
				fmt.Println("video stream stopped")
				return false
			}
			if !flush() {
				return false
			}
			fmt.Println("video source ended")
			return true
		}
		if d := reader.Dropped(); d != dropped {
			s.metrics.AddDropped("video", d-dropped)
			fmt.Printf("video stream: dropped %d old frames (slow consumer)\n", d-dropped)
			dropped = d
		}
		if len(f.Data) == 0 {
			continue
		}

		// 正在攒一个 AU：下一个首 slice 或时间戳变化说明上一帧已经完整。
		if auOpen {
			if f.AUStart || f.Timestamp != pendingTS {
				if !flush() {
					return false
				}
				auOpen = false
			} else {
				// 同一帧的后续 slice，共用首 slice 的时间戳。
				pending = append(pending, f.Data)
				continue
			}
		}

		// 源标注了 AU 首 NALU：开始攒整帧。若 pending 里已有前面攒下的
		// 参数集，它们随本 AU 一起封包（STAP-A 里 SPS/PPS + 首 slice）。
		if f.AUStart {
			auOpen = true
			pendingTS = f.Timestamp
			pending = append(pending, f.Data)
			continue
		}

		// 兼容不带 AUStart 的源（RTMP/RTSP 推流）：沿用原来的行为，
		// 非 VCL 按时间戳攒批，VCL 帧立即封包发送，避免源空闲时
		// 最后一帧一直压在 pending 里。
		if media.ClassifyVideoNAL(codec, f.Data).IsVCL {
			if len(pending) == 0 {
				if !sendFrame([][]byte{f.Data}, f.Timestamp) {
					return false
				}
				continue
			}
			if f.Timestamp != pendingTS {
				if !flush() {
					return false
				}
				if !sendFrame([][]byte{f.Data}, f.Timestamp) {
					return false
				}
				continue
			}
			batch := append(pending, f.Data)
			ts := pendingTS
			pending = nil
			if !sendFrame(batch, ts) {
				return false
			}
			continue
		}

		if len(pending) > 0 && f.Timestamp != pendingTS {
			if !flush() {
				return false
			}
		}
		if len(pending) == 0 {
			pendingTS = f.Timestamp
		}
		pending = append(pending, f.Data)
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

// audioPacker 音频封包器：AAC / G.711 / Opus / MP3 的统一入口。
type audioPacker interface {
	Pack([]byte) ([]*rtp.RTPPacket, error)
}

// streamAAC 每个客户端一条订阅 goroutine：从共享音频源拿帧 → 按编码封包 → 发送。
//
// 帧源里的时间戳已经是该编码的 RTP 时钟：AAC 每 AU +1024，G.711 每字节
// 1 采样，Opus 保留入站包时间戳，MP3 为 90kHz。客户端只负责封包和 seq/SSRC。
// 函数名沿用历史命名，实际按 track.encoding 选择封包器（空编码按 AAC）。
func (s *Server) streamAAC(track *mediaTrack, src audioFrameSource, done <-chan struct{}, start time.Duration, scale float64) bool {
	reader, cancel, err := subscribeVideo(src, start, scale)
	if err != nil {
		fmt.Printf("audio seek: %v\n", err)
		return false
	}
	defer cancel()

	codec := stream.NormalizeAudioCodec(track.encoding)
	var packer audioPacker
	pcm := false
	switch codec {
	case stream.AudioPCMA:
		p := rtp.NewPCMAPacker()
		if track.payloadType > 0 {
			p.PT = uint8(track.payloadType)
		}
		packer, pcm = p, true
	case stream.AudioPCMU:
		p := rtp.NewPCMUPacker()
		if track.payloadType > 0 {
			p.PT = uint8(track.payloadType)
		}
		packer, pcm = p, true
	case stream.AudioOpus:
		p := rtp.NewOpusPacker()
		if track.payloadType > 0 {
			p.PT = uint8(track.payloadType)
		}
		packer = p
	case stream.AudioMP3:
		p := rtp.NewMPAPacker()
		if track.payloadType > 0 {
			p.PT = uint8(track.payloadType)
		}
		packer = p
	default:
		p := rtp.NewAACPacker()
		if track.payloadType > 0 {
			p.PT = uint8(track.payloadType)
		}
		packer = p
	}
	var dropped uint64
	ssrc := track.ensureSSRC()
	// 只设置一次 RTCP 的时钟频率。
	rateSet := false
	for {
		f, ok := reader.Next(done)
		if !ok {
			if isDone(done) {
				fmt.Println("audio stream stopped")
				return false
			}
			fmt.Printf("%s source ended\n", codec)
			return true
		}
		if d := reader.Dropped(); d != dropped {
			s.metrics.AddDropped("audio", d-dropped)
			fmt.Printf("audio stream: dropped %d old frames (slow consumer)\n", d-dropped)
			dropped = d
		}
		// RTCP SR 的 RTP 时钟：文件源首帧解析出采样率，推流源来自
		// ANNOUNCE/RTMP 登记的 ClockRate（MP3=90000）。
		if !rateSet {
			track.setClockRate(src.SamplingRate())
			rateSet = true
		}
		pkts, err := packer.Pack(f.Data)
		if err != nil {
			continue
		}
		ts := f.Timestamp
		for _, p := range pkts {
			p.Header.Seq = uint16(track.nextSeq())
			p.Header.Timestamp = ts
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
			s.metrics.AddPackets(track.kind, 1)
			s.metrics.AddOctets(track.kind, uint64(len(p.Payload)))
			track.cacheSent(p.Header.Seq, data)
			// G.711 一个包里的字节数就是采样数，后续分片时间戳顺延。
			if pcm {
				ts += uint32(len(p.Payload))
			}
		}
	}
}

// sendSenderReports 按 C-5 的共享 RTCP 带宽预算给这条轨的客户端发 SR。
// 间隔由 rtcpScheduler 根据「在发轨数 × 平均包长 / 媒体总码率」计算，
// 多会话时总 SR 流量不会超过媒体带宽的 2.5%；单会话均值仍是 5 秒。
//
// 播放器靠 NTP↔RTP 映射做音视频同步；有些播放器收不到 SR 会判定
// 发送方失联而停止播放。会话关闭（TEARDOWN / 超时 / TCP 断开）时
// done 关闭，goroutine 跟着退出。
func (s *Server) sendSenderReports(track *mediaTrack, done <-chan struct{}) {
	s.rtcp.add(track)
	defer s.rtcp.remove(track)

	fmt.Printf("RTCP SR sender started (%s, %s)\n", track.kind, describeTransport(track))
	for {
		// 每轮重新计算间隔：会话数、码率和 SR 平均长度都可能变化。
		// 首轮没有码率样本，rtcpScheduler 会退回 5 秒基准间隔。
		timer := time.NewTimer(s.rtcp.nextInterval(time.Now()))
		select {
		case <-done: //收到关闭信号，打印日志，return 退出goroutine
			timer.Stop()
			fmt.Printf("RTCP SR sender stopped (%s)\n", track.kind)
			return
		case now := <-timer.C:
			sr, ok := track.senderReport(now)
			if !ok {
				continue // 还没发出任何 RTP，等下一轮
			}
			if err := track.writeRTCP(sr); err != nil {
				fmt.Printf("write rtcp sr (%s): %v\n", track.kind, err)
				return
			}
			s.rtcp.noteSent(len(sr))
			s.metrics.IncSR()
			fmt.Printf("RTCP SR sent (%s, ssrc=%08x, %d bytes)\n",
				track.kind, track.currentSSRC(), len(sr))
		}
	}
}

func describeTransport(t *mediaTrack) string {
	if t.isInterleaved { // RTSP 交织模式
		return fmt.Sprintf("tcp ch=%d", t.interleavedRTCP)
	}
	if t.isMulticast { // UDP 组播
		return fmt.Sprintf("multicast %s", t.mcRTCPAddr)
	}
	if t.rtcpMux { //UDP 单薄
		return fmt.Sprintf("udp %s (rtcp-mux)", t.clientAddr)
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

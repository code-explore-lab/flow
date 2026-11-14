package rtsp

// 推流接收侧（ANNOUNCE / SETUP / RECORD）。
//
// 数据流：
//
//	$ 帧 / UDP → rtp.Parse → 按 PT 选轨 → 每轨解包器 → ExternalSource.Write
//
// H.264 走单 NALU / STAP-A / FU-A 重组，AAC 走 RFC 3640 AU-header；
// 解包后的 NALU/AU 复制一份再写源，因为 RTP 接收缓冲区会被复用。
//
// 丢包/乱序不 panic：H264Depacketizer 自己丢弃不完整 FU-A 半成品，
// 这里只记录 seq 缺口；AAC 一个包内自洽，头部非法就跳过该包。
//
// 接收侧 PLC（R-5）：视频按 AU 攒齐再发布，损坏的 AU 整帧丢弃并重复
// 上一帧，同时用 RTCP PLI 请求推流端发关键帧；音频按时间戳缺口补静音 AU。

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"flow/internal/media"
	"flow/internal/plc"
	"flow/internal/rtcp"
	"flow/internal/rtp"
	"flow/internal/rtpbuf"
	"flow/internal/session"
	"flow/internal/source"
)

// defaultPLIInterval 是两次 PLI 之间的最小间隔。5% 丢包下如果每个
// 损坏 AU 都发一次，会把上行 RTCP 打满，也对端无意义地频繁编 IDR。
const defaultPLIInterval = 500 * time.Millisecond

// pushReceiver 一条推流轨的接收状态：解包器 + 解包后的帧去向。
// 每个 mediaTrack 独占一个，接收路径是单 goroutine（interleaved 由
// handleConn 串行处理，UDP 每轨一个读 goroutine），所以解包器不需要锁。
type pushReceiver struct {
	external *source.ExternalSource
	h264     *rtp.H264Depacketizer
	aac      *rtp.AACDepacketizer

	// 接收侧 jitter buffer 与 NACK 缺口跟踪（R-6）。
	// 收到的包先入 jbuf，按 seq 排序、等满 delay 后才解包；缺口由 nack
	// 在宽限期后生成 RTCP NACK，重传到达则从缺口集合里移除。
	mu         sync.Mutex // 串行化解包/PLC 与关闭时的尾包冲刷
	jbuf       *rtpbuf.Buffer
	nack       *rtcp.NACKTracker
	drainTimer *time.Timer
	drainAt    time.Time // 当前定时器的唤醒时刻
	drainGen   uint64    // 定时器代次，防止 Stop 失败后的旧回调覆盖新定时器
	closed     bool
	nackSent   uint64

	// 视频 PLC 状态与当前正在攒的访问单元。
	videoPLC *plc.Video
	au       auAssembler

	// 音频时间戳连续性：上一包最后一个 AU 之后的时间（采样数）。
	aacChannels int
	hasAudio    bool
	audioEndTS  uint32

	// PLI 限频与 PLC 统计。只在 rx.mu 内读写。
	pliInterval time.Duration
	lastPLI     time.Time
	pliSent     uint64
	plcRepeats  uint64
	plcDropped  uint64
	silenceAUs  uint64
}

// auAssembler 把一个访问单元的 NALU 攒齐再发布。
// H.264 的 marker 位标出 AU 的最后一个包；时间戳变化作为 marker 丢失时的
// 兜底边界。攒齐是为了在包级丢包时整帧丢弃，不把半帧发给解码器。
type auAssembler struct {
	has     bool
	ts      uint32
	corrupt bool
	nalus   [][]byte
}

func (a *auAssembler) reset() {
	a.has = false
	a.corrupt = false
	a.nalus = a.nalus[:0]
}

// newPushReceiver 按 ANNOUNCE 协商出的编码创建解包器。
// external 是已发布流的帧源（拉流端也要订阅同一个）；为空时退回私有源，
// 兼容不经过 ANNOUNCE 注册发布的直接调用（单测）。
func newPushReceiver(encoding, fmtp string, external *source.ExternalSource) (*pushReceiver, error) {
	rx := &pushReceiver{
		external:    external,
		jbuf:        rtpbuf.New(rtpbuf.DefaultDelay),
		nack:        rtcp.NewNACKTracker(),
		videoPLC:    plc.NewVideo(plc.DefaultMaxRepeat),
		pliInterval: defaultPLIInterval,
	}
	switch strings.ToUpper(encoding) {
	case "H264":
		if rx.external == nil {
			rx.external = source.NewExternalSource()
		}
		rx.h264 = &rtp.H264Depacketizer{}
	case "MPEG4-GENERIC":
		if rx.external == nil {
			rx.external = source.NewExternalAudioSource()
		}
		cfg, err := rtp.ParseAACFmtp(fmtp)
		if err != nil {
			return nil, fmt.Errorf("parse AAC fmtp: %w", err)
		}
		d, err := rtp.NewAACDepacketizer(cfg)
		if err != nil {
			return nil, err
		}
		rx.aac = d
		// 静音 AU 只有 AAC-LC 单/立体声两组常量；其他配置不补，
		// 避免写入非法 AU 让播放器报错。
		if asc, ok := parseConfigParam(fmtp); ok {
			if ch, ok := plc.AACLCChannels(asc); ok {
				rx.aacChannels = ch
			}
		}
	default:
		return nil, fmt.Errorf("unsupported push encoding %q", encoding)
	}
	return rx, nil
}

// ensurePushReceiver 返回本轨的接收器，第一次调用时创建。
// 用 recvMu 保护 pushRx，RECORD 和 UDP 读 goroutine 可能同时到达。
func (t *mediaTrack) ensurePushReceiver() (*pushReceiver, error) {
	t.recvMu.Lock()
	defer t.recvMu.Unlock()
	if t.pushRx != nil {
		return t.pushRx, nil
	}
	rx, err := newPushReceiver(t.encoding, t.fmtp, t.external)
	if err != nil {
		return nil, err
	}
	t.pushRx = rx
	return rx, nil
}

// closePushReceiver 关闭 ExternalSource，唤醒拉流端读者。
// 之后到达的 RTP 仍然会被解包，但 Write 会被源忽略。
func (t *mediaTrack) closePushReceiver() {
	t.recvMu.Lock()
	rx := t.pushRx
	t.pushRx = nil
	t.recvMu.Unlock()
	if rx == nil {
		return
	}

	// 忽略 playout 延迟把缓冲里的尾包冲刷掉，再关闭源唤醒拉流端。
	// 定时器可能已经在执行或排队，closed 标记让它拿到锁后直接退出。
	rx.mu.Lock()
	rx.closed = true
	rx.drainGen++
	if rx.drainTimer != nil {
		rx.drainTimer.Stop()
		rx.drainTimer = nil
	}
	for _, bp := range rx.jbuf.Drain() {
		pkt, err := rtp.Parse(bp.Payload)
		if err != nil {
			continue
		}
		rx.deliver(t, pkt, len(bp.Payload))
	}
	rx.mu.Unlock()

	rx.external.Close()
}

// plcStats 返回 PLC/NACK 统计快照。测试用它跨 goroutine 读取计数，
// 避免和定时冲刷 goroutine 竞争。
func (rx *pushReceiver) plcStats() (repeats, dropped, pli, nack, silence uint64) {
	rx.mu.Lock()
	defer rx.mu.Unlock()
	return rx.plcRepeats, rx.plcDropped, rx.pliSent, rx.nackSent, rx.silenceAUs
}

// handlePushedRTP 处理一个推流端发来的 RTP 报文。
// hint 是按 interleaved channel / UDP 端口初步确定的轨，最终以 PT 为准；
// 这样同一条连接上多轨复用或通道号未登记时也能路由。
func (s *Server) handlePushedRTP(st *Session, hint *mediaTrack, raw []byte) {
	pkt, err := rtp.Parse(raw)
	if err != nil {
		fmt.Printf("push RTP parse error: %v\n", err)
		return
	}

	track := hint
	if byPT, ok := st.pushTrackByPT(int(pkt.Header.PT)); ok {
		track = byPT
	}
	if track == nil {
		fmt.Printf("push RTP dropped: no push track for pt=%d\n", pkt.Header.PT)
		return
	}
	if track.payloadType != 0 && int(pkt.Header.PT) != track.payloadType {
		fmt.Printf("push RTP dropped: pt=%d does not match %s track pt=%d\n",
			pkt.Header.PT, track.kind, track.payloadType)
		return
	}

	rx, err := track.ensurePushReceiver()
	if err != nil {
		fmt.Printf("push receiver init (%s): %v\n", track.kind, err)
		return
	}

	now := time.Now()
	rx.mu.Lock()
	defer rx.mu.Unlock()
	if rx.closed {
		return
	}
	if rx.nack.Observe(pkt.Header.Seq, pkt.Header.SSRC, now) {
		// 换了同步源：旧 SSRC 的缓冲包、半成品和 PLC 缓存都不再适用。
		rx.jbuf.Reset()
		rx.au.reset()
		rx.videoPLC.Reset()
		rx.hasAudio = false
	}
	if !rx.jbuf.Push(pkt.Header.Seq, pkt.Header.Timestamp, raw, now) {
		return // 重复包或已播放/跳过的旧包
	}
	rx.drain(track, now)
	rx.sendNACK(track, now)
	rx.scheduleDrainLocked(track)
}

// drain 把 jitter buffer 里已经到期的包按 seq 顺序解包并发布。
// 调用方必须持有 rx.mu；recordReceived 也在这里调用，因此统计和 PLC
// 看到的是“排完序后仍然缺包”的播放顺序，而不是到达顺序。
func (rx *pushReceiver) drain(track *mediaTrack, now time.Time) {
	for _, bp := range rx.jbuf.Pop(now) {
		pkt, err := rtp.Parse(bp.Payload)
		if err != nil {
			fmt.Printf("push RTP parse (buffered, %s): %v\n", track.kind, err)
			continue
		}
		rx.deliver(track, pkt, len(bp.Payload))
	}
}

// deliver 记录一个包的解包统计并交给 H.264/AAC 处理。
// 调用方必须持有 rx.mu。
func (rx *pushReceiver) deliver(track *mediaTrack, pkt *rtp.RTPPacket, rawLen int) {
	ev := track.recordReceived(pkt, rawLen)
	if ev.first {
		fmt.Printf("push RTP recv (%s): first packet seq=%d ts=%d ssrc=%08x pt=%d len=%d\n",
			track.kind, pkt.Header.Seq, pkt.Header.Timestamp, pkt.Header.SSRC, pkt.Header.PT, rawLen)
	} else if ev.ssrcChanged {
		fmt.Printf("push RTP SSRC changed (%s): %08x -> %08x (in-progress FU-A will be dropped)\n",
			track.kind, ev.prevSSRC, pkt.Header.SSRC)
	}
	if ev.gap != 0 {
		gaps := track.gapCount()
		// 丢包密集时限制日志频率，但保留前 10 条和每 100 条一次。
		if gaps <= 10 || gaps%100 == 0 {
			if ev.gap > 0 {
				fmt.Printf("push RTP seq gap (%s): expected %d got %d (lost %d)\n",
					track.kind, ev.expected, pkt.Header.Seq, ev.gap)
			} else {
				fmt.Printf("push RTP seq out of order (%s): expected %d got %d (duplicate/reordered)\n",
					track.kind, ev.expected, pkt.Header.Seq)
			}
		}
	}

	switch {
	case rx.h264 != nil:
		rx.handleH264(track, pkt, ev)

	case rx.aac != nil:
		rx.handleAAC(track, pkt, ev)
	}

	if ev.count%100 == 0 {
		packets, bytes, frames, gaps := track.recvSummary()
		fmt.Printf("push RTP recv (%s): packets=%d frames=%d bytes=%d gaps=%d "+
			"plc_repeat=%d plc_drop=%d pli=%d nack=%d silence=%d\n",
			track.kind, packets, frames, bytes, gaps,
			rx.plcRepeats, rx.plcDropped, rx.pliSent, rx.nackSent, rx.silenceAUs)
	}
}

// sendNACK 把到期仍未补齐的 seq 汇总成一个 RTCP Generic NACK。
// 调用方必须持有 rx.mu；Pending 内部会做宽限、限频和重试上限。
func (rx *pushReceiver) sendNACK(track *mediaTrack, now time.Time) {
	seqs := rx.nack.Pending(now)
	if len(seqs) == 0 {
		return
	}
	mediaSSRC := rx.nack.MediaSSRC()
	if err := track.writeRTCP(rtcp.NACK(track.currentSSRC(), mediaSSRC, seqs)); err != nil {
		fmt.Printf("push NACK (%s): send failed: %v\n", track.kind, err)
		return
	}
	rx.nackSent += uint64(len(seqs))
	fmt.Printf("push NACK (%s): media ssrc=%08x missing=%v\n", track.kind, mediaSSRC, seqs)
}

// scheduleDrainLocked 在缓冲里还有包时安排一次延迟冲刷。
// 没有后续 RTP 到达时（例如推流刚结束），靠这个定时器把到期包送出去，
// 而不是等下一个包；同一时刻只保留一个定时器。
// 调用方必须持有 rx.mu。
func (rx *pushReceiver) scheduleDrainLocked(track *mediaTrack) {
	if rx.closed {
		return
	}
	if rx.jbuf.Len() == 0 && rx.nack.Outstanding() == 0 {
		return
	}
	delay := rx.jbuf.Delay()
	// 有缺口时按 NACK 宽限期更早醒来：必须赶在 jitter buffer 因超时
	// 跳过缺口之前把重传请求发出去，否则重传回来也没地方放了。
	if rx.nack.Outstanding() > 0 && rx.nack.Delay() < delay {
		delay = rx.nack.Delay()
	}
	if delay <= 0 {
		return // 无缓冲：drain 已经同步输出全部包
	}
	desired := time.Now().Add(delay)
	if rx.drainTimer != nil && !rx.drainAt.After(desired) {
		return // 已有更早或同期的唤醒
	}
	if rx.drainTimer != nil {
		rx.drainTimer.Stop()
	}
	// 代次避免 Stop 失败（回调已启动）时，旧回调清掉新定时器的引用。
	rx.drainGen++
	gen := rx.drainGen
	rx.drainAt = desired
	rx.drainTimer = time.AfterFunc(delay, func() {
		rx.mu.Lock()
		defer rx.mu.Unlock()
		if gen != rx.drainGen {
			return
		}
		rx.drainTimer = nil
		rx.drainAt = time.Time{}
		if rx.closed {
			return
		}
		now := time.Now()
		rx.drain(track, now)
		rx.sendNACK(track, now)
		rx.scheduleDrainLocked(track)
	})
}

// handleH264 解包一个视频 RTP 包，按 AU 组装后发布；丢包时做 PLC。
func (rx *pushReceiver) handleH264(track *mediaTrack, pkt *rtp.RTPPacket, ev recvEvent) {
	if ev.ssrcChanged {
		// 换了同步源：旧 SSRC 的半成品和上一帧缓存都不再适用。
		rx.au.reset()
		rx.videoPLC.Reset()
	}

	ts := pkt.Header.Timestamp
	// 注意：有 B 帧时 RTP 时间戳是 PTS，到达顺序不单调，不能按大小判断
	// 新旧。同一 AU 的包共享时间戳，时间戳变化就是 AU 边界。
	if ev.gap > 0 && rx.au.has {
		// 缺口落在正在攒的 AU 内（或上一个 AU 的尾包丢失）。
		rx.au.corrupt = true
	}

	before := rx.h264.Dropped
	nalus, err := rx.h264.Depacketize(pkt)
	if err != nil {
		fmt.Printf("push H.264 depacketize (%s): %v\n", track.kind, err)
		return
	}
	if lost := rx.h264.Dropped - before; lost > 0 {
		// 解包器丢弃了不完整 FU-A：当前 AU 不可解码。
		if rx.au.has {
			rx.au.corrupt = true
		}
		fmt.Printf("push H.264 incomplete NALU dropped (%s): %d\n", track.kind, lost)
	}

	if rx.au.has && rx.au.ts != ts {
		// 没等到 marker 就换时间戳：上一个 AU 的尾包丢了。
		rx.flushAU(track, pkt.Header.SSRC)
	}
	if !rx.au.has {
		rx.au.has = true
		rx.au.ts = ts
		// 本包到达前已发现缺口：当前 AU 缺包。
		rx.au.corrupt = ev.gap > 0
	}
	for _, nalu := range nalus {
		if len(nalu) == 0 {
			continue
		}
		// 单 NALU/STAP-A 的切片指向接收缓冲区，必须复制后长期保存。
		rx.au.nalus = append(rx.au.nalus, append([]byte(nil), nalu...))
	}
	if pkt.Header.Marker {
		rx.flushAU(track, pkt.Header.SSRC)
	}
}

// flushAU 结束当前访问单元：完整且未损坏就发布；否则整帧丢弃并做 PLC。
func (rx *pushReceiver) flushAU(track *mediaTrack, mediaSSRC uint32) {
	if !rx.au.has {
		return
	}
	ts := rx.au.ts
	nalus := rx.au.nalus
	corrupt := rx.au.corrupt
	rx.au.reset()

	if !corrupt && len(nalus) > 0 {
		rx.videoPLC.Observe(nalus)
		rx.writeNALUs(track, nalus, ts)
		return
	}

	// 损坏的 AU 不发布：解码器看到半帧只会花屏。请求关键帧，
	// 再用上一帧占住这个时间戳，避免时间轴出现空档。
	rx.plcDropped++
	rx.requestKeyframe(track, mediaSSRC)
	if rep, ok := rx.videoPLC.Conceal(); ok {
		rx.writeNALUs(track, rep, ts)
		rx.plcRepeats++
	}
}

// writeNALUs 把同一时间戳的一组 NALU 写入共享源。
func (rx *pushReceiver) writeNALUs(track *mediaTrack, nalus [][]byte, ts uint32) {
	for _, nalu := range nalus {
		typ, _ := media.NALType(nalu)
		rx.external.Write(source.Frame{
			Data:      nalu,
			Timestamp: ts,
			Keyframe:  typ == rtp.NALUTypeIDR,
		})
		if frames := track.recordFrame(); frames == 1 {
			fmt.Printf("push frame recv (%s): first frame type=%d ts=%d %d bytes\n",
				track.kind, typ, ts, len(nalu))
		}
	}
}

// requestKeyframe 向推流端发 RTCP PLI。按 pliInterval 限频。
func (rx *pushReceiver) requestKeyframe(track *mediaTrack, mediaSSRC uint32) {
	now := time.Now()
	if !rx.lastPLI.IsZero() && now.Sub(rx.lastPLI) < rx.pliInterval {
		return
	}
	if err := track.writeRTCP(plc.PLI(track.currentSSRC(), mediaSSRC)); err != nil {
		fmt.Printf("push PLC (%s): send PLI failed: %v\n", track.kind, err)
		return
	}
	rx.lastPLI = now
	rx.pliSent++
	fmt.Printf("push PLC (%s): request keyframe (PLI, media ssrc=%08x)\n",
		track.kind, mediaSSRC)
}

// handleAAC 解包音频包，并在时间戳出现缺口时先补静音 AU。
func (rx *pushReceiver) handleAAC(track *mediaTrack, pkt *rtp.RTPPacket, ev recvEvent) {
	if ev.ssrcChanged {
		rx.hasAudio = false
	}
	ts := pkt.Header.Timestamp
	if rx.hasAudio {
		if int32(ts-rx.audioEndTS) < 0 {
			return // 旧 AU 的迟到包
		}
		if missing := plc.MissingAUs(rx.audioEndTS, ts, rtp.AACFrameSamples); missing > 0 {
			rx.fillSilence(track, rx.audioEndTS, missing)
		}
	}

	aus, err := rx.aac.Depacketize(pkt)
	if err != nil {
		fmt.Printf("push AAC depacketize (%s): %v\n", track.kind, err)
		return
	}
	for i, au := range aus {
		// 一个 RTP 包可带多个 AU，时间戳按每 AU 1024 采样递增
		// （RFC 3640：包时间戳对应第一个 AU）。
		auTS := ts + uint32(i)*rtp.AACFrameSamples
		data := append([]byte(nil), au.Data...)
		rx.external.Write(source.Frame{
			Data:      data,
			Timestamp: auTS,
			Keyframe:  true,
		})
		rx.audioEndTS = auTS + rtp.AACFrameSamples
		rx.hasAudio = true
		if frames := track.recordFrame(); frames == 1 {
			fmt.Printf("push frame recv (%s): first AU %d bytes ts=%d\n",
				track.kind, len(data), auTS)
		}
	}
}

// fillSilence 在 [from, from+n*1024) 的时间戳上写 AAC-LC 静音 AU。
// 声道数未知或不是 AAC-LC 时不补，保持时间戳空档即可。
func (rx *pushReceiver) fillSilence(track *mediaTrack, from uint32, n int) {
	au, ok := plc.SilenceAU(rx.aacChannels)
	if !ok {
		return
	}
	for i := range n {
		rx.external.Write(source.Frame{
			Data:      au,
			Timestamp: from + uint32(i)*rtp.AACFrameSamples,
			Keyframe:  true,
		})
		track.recordFrame()
		rx.silenceAUs++
	}
}

// serveUDPRTP 从 SETUP 分配的 UDP RTP 端口收推流数据。
// 端口关闭（teardown / 会话回收 / 重复 SETUP 替换旧轨）时 ReadFromUDP 返回错误，
// goroutine 随之退出。
func (s *Server) serveUDPRTP(track *mediaTrack, sess *session.Session) {
	st := sess.Data.(*Session)
	fmt.Printf("push UDP RTP receiver started (%s, port=%d)\n", track.kind, track.serverRtpPort)
	buf := make([]byte, 64*1024)
	for {
		n, _, err := track.rtpConn.ReadFromUDP(buf)
		if err != nil {
			fmt.Printf("push UDP RTP receiver stopped (%s): %v\n", track.kind, err)
			return
		}
		// 推流端可能不发 RTSP 保活，收到的媒体报文本身就是活跃证据，
		// 否则 60s 会被会话回收误杀。
		sess.TouchRTCP()
		s.handlePushedRTP(st, track, buf[:n])
	}
}

// recvEvent 记录一个 RTP 报文后的变化，供日志和统计使用。
type recvEvent struct {
	count       uint64
	first       bool
	expected    uint16 // 按上一包推算出的期望 seq
	gap         int    // 正数=丢包数，负数=重复/乱序
	prevSSRC    uint32
	ssrcChanged bool
}

// recordReceived 记录一个收到的 RTP 报文并更新统计。
func (t *mediaTrack) recordReceived(pkt *rtp.RTPPacket, rawLen int) recvEvent {
	t.recvMu.Lock()
	defer t.recvMu.Unlock()

	ev := recvEvent{first: t.recvPackets == 0}
	if !ev.first {
		ev.expected = t.recvLastSeq + 1
		if delta := pkt.Header.Seq - ev.expected; delta != 0 {
			if delta < 0x8000 {
				ev.gap = int(delta) // 正整数：中间缺了 gap 个包
			} else {
				ev.gap = int(delta) - 0x10000 // 负数：重复或乱序
			}
			t.recvGaps++
		}
		ev.ssrcChanged = pkt.Header.SSRC != t.recvLastSSRC
		ev.prevSSRC = t.recvLastSSRC
	}

	t.recvPackets++
	t.recvBytes += uint64(rawLen)
	t.recvLastSeq = pkt.Header.Seq
	t.recvLastSSRC = pkt.Header.SSRC
	t.recvLastPT = pkt.Header.PT
	ev.count = t.recvPackets
	return ev
}

// recordFrame 记录一帧写入了 ExternalSource，返回累计帧数。
func (t *mediaTrack) recordFrame() uint64 {
	t.recvMu.Lock()
	defer t.recvMu.Unlock()
	t.recvFrames++
	return t.recvFrames
}

// gapCount 返回累计 seq 缺口次数（日志限频用）。
func (t *mediaTrack) gapCount() uint64 {
	t.recvMu.Lock()
	defer t.recvMu.Unlock()
	return t.recvGaps
}

// recvSummary 返回接收统计终态：包数、字节数、帧数、缺口次数。
func (t *mediaTrack) recvSummary() (packets, bytes, frames, gaps uint64) {
	t.recvMu.Lock()
	defer t.recvMu.Unlock()
	return t.recvPackets, t.recvBytes, t.recvFrames, t.recvGaps
}

// recvSnapshot 返回接收统计快照（测试使用）。
func (t *mediaTrack) recvSnapshot() (packets, bytes uint64, lastSeq uint16, lastSSRC uint32, lastPT uint8) {
	t.recvMu.Lock()
	defer t.recvMu.Unlock()
	return t.recvPackets, t.recvBytes, t.recvLastSeq, t.recvLastSSRC, t.recvLastPT
}

package rtmp

// RTMP 拉流（play）：把注册表里的流（或本地回退源）转成 FLV 音视频 tag。
//
// 时序：
//
//	play(streamName)
//	  → onStatus(NetStream.Play.Reset)
//	  → onStatus(NetStream.Play.Start)
//	  → AVC/AAC 序列头（有编码参数时）
//	  → 逐帧 FLV tag（时间戳换算成以首帧为基准的相对毫秒）
//	  → 源结束后 onStatus(NetStream.Play.Complete)，关闭连接
//
// 播放 goroutine 与命令读循环并发：写由 Conn.mu 串行化；stopPlay 通过关闭
// cancel 让 Reader.Next 立即返回，再等 goroutine 退出。

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"flow/internal/flv"
	"flow/internal/media"
	"flow/internal/source"
	"flow/internal/stream"
)

// frameSource 拉流需要的帧源接口：ExternalSource / H264Source / AACSource
// 都实现了它。
type frameSource interface {
	Subscribe() (*source.Reader[source.Frame], func())
}

// seekSource 可选接口：能从指定时间点重新订阅。文件源（H264Source /
// AACSource）实现它；实时推流源（ExternalSource）没有可回退的 ring，
// 因此不支持 seek。
type seekSource interface {
	SubscribeFrom(start time.Duration, scale float64) (*source.Reader[source.Frame], func(), error)
}

// maxPausedFrames 暂停期间每条轨最多缓存的帧数。暂停时继续消费源（避免
// 环形缓冲覆盖导致丢帧），超过上限才丢弃最老的帧。25fps 下 256 帧约 10s，
// 足够覆盖 P2 要求的两秒暂停，同时给内存一个硬上限。
const maxPausedFrames = 256

// playState 一次播放的生命周期。
type playState struct {
	streamID uint32
	cancel   chan struct{}
	wg       sync.WaitGroup

	// 已解析的帧源与编码参数，seek 时按原样重新订阅。
	video     frameSource
	audio     frameSource
	vparams   *stream.VideoCodec
	aparams   *stream.AudioCodec
	audioRate uint32
	audioCh   uint8
	codec     string

	// 每条轨一个播放时钟；seek 重启 goroutine 后由同一个 playState 延续，
	// 保证 DTS 不回退。只有对应轨的播放 goroutine 访问（重启前已 wg.Wait）。
	vclock relativeClock
	aclock relativeClock

	// tsOffset 在 seek 后叠加到相对时间戳上，使新位置首帧的 DTS 不小于
	// seek 前的最后一帧；回退 seek 也不会让时间戳倒退。
	tsOffset uint32

	// 暂停门闩：paused 为真时播放 goroutine 停止发送，但继续消费源并把
	// 帧缓存到本地，恢复后按原顺序补发。
	pauseMu sync.Mutex
	paused  bool
	resume  chan struct{}
}

// stop 取消播放并等所有轨的 goroutine 退出。只能调用一次。
func (ps *playState) stop() {
	close(ps.cancel)
	ps.wg.Wait()
}

// setPaused 切换暂停状态。恢复时关闭 resume 唤醒所有等待的轨。
func (ps *playState) setPaused(paused bool) {
	ps.pauseMu.Lock()
	defer ps.pauseMu.Unlock()
	if ps.paused == paused {
		return
	}
	if paused {
		// 切到暂停：新建resume通道，播放协程后续会阻塞在这个chan上
		ps.resume = make(chan struct{})
	} else {
		// 切到恢复：关闭通道，唤醒所有等待的音视频协程
		close(ps.resume)
		ps.resume = nil
	}
	ps.paused = paused
}

func (ps *playState) isPaused() bool {
	ps.pauseMu.Lock()
	defer ps.pauseMu.Unlock()
	return ps.paused
}

// waitUnpaused 阻塞到恢复播放；cancel 关闭时返回 false。
func (ps *playState) waitUnpaused(cancel <-chan struct{}) bool {
	for {
		//快照，锁只抓一瞬间
		ps.pauseMu.Lock()
		paused, resume := ps.paused, ps.resume
		ps.pauseMu.Unlock()
		// 已经不在暂停，直接返回成功
		if !paused {
			return true
		}
		select {
		case <-resume:
			// resume被close（setPaused(false)），进入下一轮循环
			// 循环回去再快照一次状态，确认paused=false再return
		case <-cancel:
			// 收到停止信号，直接返回false，播放协程退出
			return false
		}
	}
}

// seekable 判断这次播放的所有轨是否都支持定位。实时源只要有一轨不支持
// 就整体拒绝，避免音视频不同步。
func (ps *playState) seekable() bool {
	if ps.video == nil && ps.audio == nil {
		return false
	}
	if ps.video != nil {
		if _, ok := ps.video.(seekSource); !ok {
			return false
		}
	}
	if ps.audio != nil {
		if _, ok := ps.audio.(seekSource); !ok {
			return false
		}
	}
	return true
}

// seekOffset 返回 seek 后新时间线的起点：两条轨最后已发 DTS 的较大者 + 1。
func (ps *playState) seekOffset() uint32 {
	var last uint32
	set := false
	if ps.vclock.set {
		last, set = ps.vclock.last, true
	}
	if ps.aclock.set && (!set || ps.aclock.last > last) {
		last, set = ps.aclock.last, true
	}
	if !set {
		return 0
	}
	return last + 1
}

// nextPlayFrame 取下一帧交给播放循环。暂停期间继续从源读帧并缓存到
// pending，避免暂停时间超过环形缓冲窗口后丢帧；恢复后按原顺序补发。
// dropped 表示缓存超过上限、丢弃过最老的帧（调用方需要重设时间基准）。
func nextPlayFrame(ps *playState, rd *source.Reader[source.Frame],
	pending *[]source.Frame, dropped *bool) (source.Frame, bool) {
	for {
		if ps.isPaused() {
			f, ok := rd.Next(ps.cancel)
			if !ok {
				// 源在暂停期间结束：有缓存就先等恢复，把缓存发完再收尾。
				if len(*pending) == 0 || !ps.waitUnpaused(ps.cancel) {
					return source.Frame{}, false
				}
				f = (*pending)[0]
				*pending = (*pending)[1:]
				return f, true
			}
			if len(*pending) >= maxPausedFrames {
				*pending = (*pending)[1:]
				*dropped = true
			}
			*pending = append(*pending, f)
			continue
		}
		if len(*pending) > 0 {
			f := (*pending)[0]
			*pending = (*pending)[1:]
			return f, true
		}
		f, ok := rd.Next(ps.cancel)
		if !ok {
			return source.Frame{}, false
		}
		// 等帧期间可能刚好进入暂停：把这一帧也放进缓存，避免
		// Pause.Notify 之后还漏发一帧。
		if ps.isPaused() {
			if len(*pending) >= maxPausedFrames {
				*pending = (*pending)[1:]
				*dropped = true
			}
			*pending = append(*pending, f)
			continue
		}
		return f, true
	}
}

// relativeClock 一条轨的播放时间基准，由该轨的播放 goroutine 独占，无需加锁；
// seek 时在旧 goroutine 退出（wg.Wait）之后读取。
//
// 音视频各用一个：RTSP 源的音频/视频 RTP 时钟互不相关（可能相差上亿），
// 共用一个基准会让其中一轨的相对毫秒远超 0xFFFFFF，触发扩展时间戳并让
// 播放时间线完全错乱。
//
// FLV tag 的 dts 必须单调递增，但 RTSP 推流带 B 帧时 RTP 时间戳按解码顺序
// 到达、会小幅回退，所以回退的帧贴到上一帧之后（+1ms）。
type relativeClock struct {
	set  bool
	base uint32
	last uint32
}

// relative 把绝对毫秒时间戳换算成相对首帧的毫秒数。
func (rc *relativeClock) relative(ms uint32) uint32 {
	if !rc.set {
		rc.set = true
		rc.base = ms
		return 0
	}
	if ms < rc.base {
		ms = rc.base
	}
	rel := ms - rc.base
	if rel <= rc.last {
		rel = rc.last + 1
	}
	rc.last = rel
	return rel
}

// rebase 把 ms 映射成 last+1，抹掉暂停期间因缓存上限丢弃帧造成的时间空洞，
// 让恢复后的 DTS 继续单调递增。未建立基准时等价于普通首帧。
func (rc *relativeClock) rebase(ms uint32) {
	if !rc.set {
		rc.base = ms
		rc.last = 0
		rc.set = true
		return
	}
	if ms > rc.last+1 {
		rc.base = ms - rc.last - 1
	} else {
		rc.base = 0
	}
}

// supportsHEVC 判断客户端能否解 Enhanced RTMP 的 HEVC 标签。
// 没声明 fourCcList 的按新客户端处理；声明了清单就要含 hvc1（或 hev1）。
func (c *Conn) supportsHEVC() bool {
	if c.fourCCs == nil {
		return true
	}
	for _, f := range c.fourCCs {
		if strings.EqualFold(f, "hvc1") || strings.EqualFold(f, "hev1") {
			return true
		}
	}
	return false
}

// handlePlay 处理 play 命令：选定帧源、订阅、回状态并启动播放 goroutine。
func (c *Conn) handlePlay(streamID uint32, cmd *Command) error {
	rawName := cmd.stringArg(0)
	// T-3：token 在查询串里，必须在 sanitizeStreamName 剥掉之前校验。
	// app 白名单在这里再查一次，防止客户端跳过 connect 直接 play。
	// 鉴权失败统一回 StreamNotFound，不暴露流是否存在。
	if c.srv != nil && !c.srv.auth.AppAllowed(c.app) {
		fmt.Printf("rtmp: reject play (app %q) from %s: app not allowed\n",
			appBase(c.app), c.RemoteAddr())
		return c.writeCommand(streamID, "onStatus", 0, flv.Null(),
			statusObj("error", "NetStream.Play.StreamNotFound", "no stream named "+sanitizeStreamName(rawName)))
	}
	if c.srv != nil && !c.srv.auth.Allow(rawName, c.app, permPlay) {
		fmt.Printf("rtmp: reject play (app %q) from %s: invalid or missing token\n",
			appBase(c.app), c.RemoteAddr())
		return c.writeCommand(streamID, "onStatus", 0, flv.Null(),
			statusObj("error", "NetStream.Play.StreamNotFound", "no stream named "+sanitizeStreamName(rawName)))
	}
	name := sanitizeStreamName(rawName)
	if name == "" {
		return c.writeCommand(streamID, "onStatus", 0, flv.Null(),
			statusObj("error", "NetStream.Play.StreamNotFound", "empty stream name"))
	}
	if c.srv == nil {
		return errors.New("rtmp: play without server")
	}

	c.stopPlay()

	var (
		video     frameSource
		audio     frameSource
		vparams   *stream.VideoCodec
		aparams   *stream.AudioCodec
		audioRate uint32
		audioCh   uint8
	)

	var st *stream.Stream
	if st = c.srv.registry.Get(name); st != nil {
		if t := st.VideoTrack(); t != nil && t.Src != nil {
			video = t.Src
		}
		if t := st.AudioTrack(); t != nil && t.Src != nil {
			audio = t.Src
		}
		if p, ok := st.VideoParams(); ok {
			vparams = p
		}
		if p, ok := st.AudioParams(); ok {
			aparams = p
			audioRate, audioCh = p.SampleRate, p.Channels
		}
	} else {
		// 注册表没有这条流：回退本地源（ffplay rtmp://.../live/test 放 testdata）。
		if c.srv.video != nil {
			video = c.srv.video
		}
		if c.srv.audio != nil {
			audio = c.srv.audio
			audioRate = c.srv.audio.SamplingRate()
			if p := localAudioParams(c.srv.audio); p != nil {
				aparams = p
				audioRate, audioCh = p.SampleRate, p.Channels
			}
		}
	}

	if video == nil && audio == nil {
		return c.writeCommand(streamID, "onStatus", 0, flv.Null(),
			statusObj("error", "NetStream.Play.StreamNotFound", "no stream named "+name))
	}

	// HEVC 只能通过 Enhanced RTMP（FourCC 模式）下发。客户端在 connect 的
	// fourCcList 里声明了支持哪些 FourCC；声明了清单却不含 hvc1 的是老客户端，
	// 明确拒绝比发它解不了的标签更友好。没声明清单的按新客户端放行。
	codec := videoCodec(vparams, video)
	if media.IsHEVCCodec(codec) && !c.supportsHEVC() {
		fmt.Printf("rtmp: reject HEVC play for legacy client %s\n", c.RemoteAddr())
		return c.writeCommand(streamID, "onStatus", 0, flv.Null(),
			statusObj("error", "NetStream.Play.Failed", "H.265 stream requires hvc1 support"))
	}

	// 先订阅再回 Play.Start：客户端收到状态时源已挂好，不会漏掉开头几帧。
	var vrd, ard *source.Reader[source.Frame]
	var vcancel, acancel func()
	if video != nil {
		vrd, vcancel = video.Subscribe()
	}
	if audio != nil {
		ard, acancel = audio.Subscribe()
	}

	ps := &playState{
		streamID:  streamID,
		cancel:    make(chan struct{}),
		video:     video,
		audio:     audio,
		vparams:   vparams,
		aparams:   aparams,
		audioRate: audioRate,
		audioCh:   audioCh,
		codec:     codec,
	}
	c.play = ps

	fmt.Printf("rtmp: play %q (app %q) from %s\n", name, appBase(c.app), c.RemoteAddr())

	if err := c.writeCommand(streamID, "onStatus", 0, flv.Null(),
		statusObj("status", "NetStream.Play.Reset", "Playing and resetting "+name)); err != nil {
		vcancelPlay(vcancel, acancel)
		c.play = nil
		return err
	}
	if err := c.writeCommand(streamID, "onStatus", 0, flv.Null(),
		statusObj("status", "NetStream.Play.Start", "Started playing "+name)); err != nil {
		vcancelPlay(vcancel, acancel)
		c.play = nil
		return err
	}

	c.srv.metrics.IncPlay()
	c.srv.metrics.IncPlayTotal()

	// T-4：Play.Start 之后、音视频序列头之前转发 onMetaData。序列头由
	// playVideo/playAudio 的 goroutine 异步发，这里必须在启动 goroutine 前
	// 同步写，保证客户端先拿到元数据。仅注册表流有（本地回退源无）。
	if st != nil {
		if md, ok := st.MetaData().(*metaData); ok && len(md.payload) > 0 {
			if err := c.writeMessage(&Message{
				TypeID:   msgDataAMF0,
				StreamID: streamID,
				Payload:  md.payload,
			}, dataCSID); err != nil {
				vcancelPlay(vcancel, acancel)
				c.play = nil
				return err
			}
		}
	}

	if video != nil {
		ps.wg.Add(1)
		go c.playVideo(ps, vrd, vcancel, vparams, codec)
	}
	if audio != nil {
		ps.wg.Add(1)
		go c.playAudio(ps, ard, acancel, aparams, audioRate, audioCh)
	}
	go c.watchPlay(ps)
	return nil
}

func vcancelPlay(funcs ...func()) {
	for _, f := range funcs {
		if f != nil {
			f()
		}
	}
}

// stopPlay 停止当前播放并等播放 goroutine 退出。可重复调用。
func (c *Conn) stopPlay() {
	ps := c.play
	if ps == nil {
		return
	}
	c.play = nil
	if c.srv != nil {
		c.srv.metrics.DecPlay()
	}
	ps.stop()
}

// handlePause 处理 pause 命令：暂停后停止发送但保留订阅。
// RTMP 的 pause 参数是（命令对象, 布尔暂停标志, 毫秒位置）；位置只用于
// 客户端自己的续播提示，这里按要求忽略，恢复后从暂停处继续。
func (c *Conn) handlePause(streamID uint32, cmd *Command) error {
	ps := c.play
	if ps == nil || ps.streamID != streamID {
		return nil
	}
	paused := cmd.boolArg(0)
	action := "pause"
	code, desc := "NetStream.Pause.Notify", "Paused"
	if !paused {
		action = "unpause"
		code, desc = "NetStream.Unpause.Notify", "Unpaused"
	}
	fmt.Printf("rtmp: %s stream %d from %s\n", action, streamID, c.RemoteAddr())
	status := func() error {
		return c.writeCommand(streamID, "onStatus", 0, flv.Null(),
			statusObj("status", code, desc))
	}
	if !paused {
		// 先回状态再唤醒播放 goroutine，客户端保证先看到 Unpause.Notify。
		if err := status(); err != nil {
			return err
		}
		ps.setPaused(false)
		return nil
	}
	ps.setPaused(true)
	return status()
}

// handleSeek 处理 seek 命令：文件源按 G-2 的索引从目标附近的前置 IDR
// 重新订阅，实时源返回 Seek.Failed。时间戳从 seek 前的最后一帧继续，
// 回退 seek 也不会倒退。
func (c *Conn) handleSeek(streamID uint32, cmd *Command) error {
	ps := c.play
	if ps == nil || ps.streamID != streamID {
		return nil
	}
	ms, ok := cmd.numberArg(0)
	if !ok || ms < 0 {
		ms = 0
	}
	if !ps.seekable() {
		return c.writeCommand(streamID, "onStatus", 0, flv.Null(),
			statusObj("error", "NetStream.Seek.Failed", "stream is not seekable"))
	}
	start := time.Duration(ms * float64(time.Millisecond))

	// 先为新位置建订阅，再停旧播放：订阅失败时旧播放不受影响。
	var (
		vrd, ard         *source.Reader[source.Frame]
		vcancel, acancel func()
	)
	if ps.video != nil {
		var err error
		vrd, vcancel, err = ps.video.(seekSource).SubscribeFrom(start, 1)
		if err != nil {
			return c.seekFailed(streamID, err)
		}
	}
	if ps.audio != nil {
		var err error
		ard, acancel, err = ps.audio.(seekSource).SubscribeFrom(start, 1)
		if err != nil {
			vcancelPlay(vcancel)
			return c.seekFailed(streamID, err)
		}
	}

	ps.stop()

	np := &playState{
		streamID:  streamID,
		cancel:    make(chan struct{}),
		video:     ps.video,
		audio:     ps.audio,
		vparams:   ps.vparams,
		aparams:   ps.aparams,
		audioRate: ps.audioRate,
		audioCh:   ps.audioCh,
		codec:     ps.codec,
		tsOffset:  ps.seekOffset(),
	}
	c.play = np

	// 先回 Seek.Notify 再启动新 goroutine，客户端收到的下一批媒体消息
	// 一定来自 seek 后的新订阅。
	if err := c.writeCommand(streamID, "onStatus", 0, flv.Null(),
		statusObj("status", "NetStream.Seek.Notify",
			fmt.Sprintf("Seeking to %dms", int64(ms)))); err != nil {
		vcancelPlay(vcancel, acancel)
		c.play = nil
		c.srv.metrics.DecPlay()
		return err
	}
	fmt.Printf("rtmp: seek %dms (stream %d) from %s\n", int64(ms), streamID, c.RemoteAddr())

	if np.video != nil {
		np.wg.Add(1)
		go c.playVideo(np, vrd, vcancel, np.vparams, np.codec)
	}
	if np.audio != nil {
		np.wg.Add(1)
		go c.playAudio(np, ard, acancel, np.aparams, np.audioRate, np.audioCh)
	}
	go c.watchPlay(np)
	return nil
}

// seekFailed 记日志并回 NetStream.Seek.Failed。
func (c *Conn) seekFailed(streamID uint32, err error) error {
	fmt.Printf("rtmp: seek stream %d from %s: %v\n", streamID, c.RemoteAddr(), err)
	return c.writeCommand(streamID, "onStatus", 0, flv.Null(),
		statusObj("error", "NetStream.Seek.Failed", err.Error()))
}

// watchPlay 等所有轨播完：自然结束就发 Play.Complete 并关闭连接，
// 让客户端读到 EOF 而不是一直挂着。
func (c *Conn) watchPlay(ps *playState) {
	ps.wg.Wait()
	select {
	case <-ps.cancel:
		return // 被 stopPlay 取消，不是自然结束
	default:
	}
	_ = c.writeCommand(ps.streamID, "onStatus", 0, flv.Null(),
		statusObj("status", "NetStream.Play.Complete", "Playback complete."))
	fmt.Printf("rtmp: play finished for %s\n", c.RemoteAddr())
	c.Close()
}

// videoCodec 推断一条视频轨的编码：注册表参数优先，其次看帧源自己
// 记的编码（推流源在收到序列头时就设置了），最后按 H.264 处理。
func videoCodec(params *stream.VideoCodec, src frameSource) string {
	if params != nil && params.Codec != "" {
		return params.Codec
	}
	if ext, ok := src.(*source.ExternalSource); ok {
		return ext.Codec()
	}
	return "H264"
}

// playVideo 把视频帧转成 FLV video tag 发送。
// 编码参数优先用注册表里的；没有就从前几帧（SPS/PPS，HEVC 含 VPS）捕获，
// 对应本地源。codec 决定序列头/媒体标签用传统 AVC 还是 Enhanced HEVC 格式。
func (c *Conn) playVideo(ps *playState, rd *source.Reader[source.Frame], unsubscribe func(),
	params *stream.VideoCodec, codec string) {
	defer ps.wg.Done()
	defer unsubscribe()

	isHEVC := media.IsHEVCCodec(codec)
	var vps, sps, pps []byte
	if params != nil {
		vps, sps, pps = params.VPS, params.SPS, params.PPS
	}
	haveParams := func() bool {
		if isHEVC {
			return len(vps) > 0 && len(sps) > 0 && len(pps) > 0
		}
		return len(sps) > 0 && len(pps) > 0
	}
	clock := &ps.vclock
	headerSent := false
	sendHeader := func() error {
		if headerSent || !haveParams() {
			return nil
		}
		var payload []byte
		if isHEVC {
			payload = flv.BuildHEVCVideoSequenceHeader(vps, sps, pps)
		} else {
			payload = flv.BuildVideoSequenceHeader(sps, pps)
		}
		if err := c.writeMessage(&Message{
			TypeID:   msgVideo,
			StreamID: ps.streamID,
			Payload:  payload,
		}, videoCSID); err != nil {
			return err
		}
		headerSent = true
		return nil
	}

	if err := sendHeader(); err != nil {
		return
	}

	var (
		pending []source.Frame
		dropped bool
		rebase  bool
	)
	// 同一访问单元的 NALU 合并成一个 FLV tag：一个 tag 只有一个 DTS/CTS。
	// 多 slice / 带 SEI 的帧如果拆成多个 tag，会被解码器当成多帧。
	var (
		au    []source.Frame
		auPTS uint32
		auCTS int32
	)
	sendAU := func() bool {
		if len(au) == 0 {
			return true
		}
		// 参数集可能晚于第一帧：等参数集齐了再发媒体帧。
		if !headerSent {
			if err := sendHeader(); err != nil {
				return false
			}
			if !headerSent {
				au = au[:0]
				return true
			}
		}

		nalus := make([][]byte, 0, len(au))
		key := false
		for _, f := range au {
			nalus = append(nalus, f.Data)
			key = key || f.Keyframe
		}
		// FLV tag 时间戳 = DTS；CompositionTime = PTS−DTS（毫秒）。
		dtsMS := ticksToMillis(int64(auPTS) - int64(auCTS))
		if dtsMS < 0 {
			dtsMS = 0
		}
		ctsMS := int32(ticksToMillis(int64(auCTS)))
		if rebase {
			clock.rebase(uint32(dtsMS))
			rebase = false
		}
		var payload []byte
		if isHEVC {
			payload = flv.BuildHEVCVideoTagNALUs(nalus, key, ctsMS)
		} else {
			payload = flv.BuildVideoTagNALUs(nalus, key, ctsMS)
		}
		au = au[:0]
		return c.writeMessage(&Message{
			TypeID:    msgVideo,
			StreamID:  ps.streamID,
			Timestamp: clock.relative(uint32(dtsMS)) + ps.tsOffset,
			Payload:   payload,
		}, videoCSID) == nil
	}
	send := func(f source.Frame) bool {
		// 参数集只进序列头，不单独发 tag，也不打断当前 AU。
		if class := media.ClassifyVideoNAL(codec, f.Data); class.IsVPS || class.IsSPS || class.IsPPS {
			switch {
			case class.IsVPS:
				vps = f.Data
			case class.IsSPS:
				sps = f.Data
			default:
				pps = f.Data
			}
			return true
		}

		if len(au) > 0 && !f.AUStart && f.Timestamp == auPTS && f.CompositionTime == auCTS {
			au = append(au, f)
			if f.AUEnd {
				return sendAU()
			}
			return true
		}
		if !sendAU() {
			return false
		}
		auPTS, auCTS = f.Timestamp, f.CompositionTime
		au = append(au, f)
		// AUEnd 明确标了帧尾；没带任何 AU 边界信息的老源沿用旧行为：
		// 一帧一个 tag，收到就发，避免凭空多一帧延迟。
		if f.AUEnd || !f.AUStart {
			return sendAU()
		}
		return true
	}

	for {
		f, ok := nextPlayFrame(ps, rd, &pending, &dropped)
		if !ok {
			sendAU()
			return
		}
		if dropped {
			// 暂停超过缓存上限丢了帧：恢复后的首帧重设基准，DTS 保持连续。
			rebase, dropped = true, false
		}
		if !send(f) {
			return
		}
	}
}

// ticksToMillis 把 90kHz 时基换算成毫秒（四舍五入，负数向零取整）。
func ticksToMillis(ticks int64) int64 {
	if ticks >= 0 {
		return (ticks*1000 + 45000) / 90000
	}
	return (ticks*1000 - 45000) / 90000
}

// playAudio 把音频帧转成 FLV audio tag 发送。
func (c *Conn) playAudio(ps *playState, rd *source.Reader[source.Frame], unsubscribe func(),
	params *stream.AudioCodec, rate uint32, ch uint8) {
	defer ps.wg.Done()
	defer unsubscribe()

	if rate == 0 {
		rate = defaultAACRate
	}
	if params != nil && len(params.Config) > 0 {
		if err := c.writeMessage(&Message{
			TypeID:   msgAudio,
			StreamID: ps.streamID,
			Payload:  flv.BuildAudioSequenceHeader(params.Config, rate, ch),
		}, audioCSID); err != nil {
			return
		}
	}

	clock := &ps.aclock
	var (
		pending []source.Frame
		dropped bool
		rebase  bool
	)
	send := func(f source.Frame) bool {
		ms := uint32(uint64(f.Timestamp) * 1000 / uint64(rate))
		if rebase {
			clock.rebase(ms)
			rebase = false
		}
		return c.writeMessage(&Message{
			TypeID:    msgAudio,
			StreamID:  ps.streamID,
			Timestamp: clock.relative(ms) + ps.tsOffset,
			Payload:   flv.BuildAudioTag(f.Data, rate, ch),
		}, audioCSID) == nil
	}

	for {
		f, ok := nextPlayFrame(ps, rd, &pending, &dropped)
		if !ok {
			return
		}
		if dropped {
			rebase, dropped = true, false
		}
		if !send(f) {
			return
		}
	}
}

// localAudioParams 用 ADTS 首帧解析本地音频源的 ASC（序列头参数）。
func localAudioParams(src *source.AACSource) *stream.AudioCodec {
	info, err := media.GetAACInfo(src.Path())
	if err != nil {
		fmt.Printf("rtmp: read aac info from %s: %v\n", src.Path(), err)
		return nil
	}
	return &stream.AudioCodec{
		Config:     info.Config,
		SampleRate: uint32(info.SamplingFrequency),
		Channels:   info.ChannelConfig,
	}
}

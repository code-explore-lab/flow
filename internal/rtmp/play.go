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
	"sync"

	"flow/internal/flv"
	"flow/internal/media"
	"flow/internal/rtp"
	"flow/internal/source"
	"flow/internal/stream"
)

// frameSource 拉流需要的帧源接口：ExternalSource / H264Source / AACSource
// 都实现了它。
type frameSource interface {
	Subscribe() (*source.Reader[source.Frame], func())
}

// playState 一次播放的生命周期。
type playState struct {
	streamID uint32
	cancel   chan struct{}
	wg       sync.WaitGroup
}

// relativeClock 一条轨的播放时间基准，由该轨的播放 goroutine 独占，无需加锁。
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

// handlePlay 处理 play 命令：选定帧源、订阅、回状态并启动播放 goroutine。
func (c *Conn) handlePlay(streamID uint32, cmd *Command) error {
	name := sanitizeStreamName(cmd.stringArg(0))
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

	if st := c.srv.registry.Get(name); st != nil {
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

	// 先订阅再回 Play.Start：客户端收到状态时源已挂好，不会漏掉开头几帧。
	var vrd, ard *source.Reader[source.Frame]
	var vcancel, acancel func()
	if video != nil {
		vrd, vcancel = video.Subscribe()
	}
	if audio != nil {
		ard, acancel = audio.Subscribe()
	}

	ps := &playState{streamID: streamID, cancel: make(chan struct{})}
	c.play = ps

	fmt.Printf("rtmp: play %q (app %q) from %s\n", name, c.app, c.RemoteAddr())

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

	if video != nil {
		ps.wg.Add(1)
		go c.playVideo(ps, vrd, vcancel, vparams)
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
	close(ps.cancel)
	ps.wg.Wait()
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

// playVideo 把视频帧转成 FLV video tag 发送。
// 编码参数优先用注册表里的；没有就从前几帧（SPS/PPS）捕获，对应本地源。
func (c *Conn) playVideo(ps *playState, rd *source.Reader[source.Frame], unsubscribe func(), params *stream.VideoCodec) {
	defer ps.wg.Done()
	defer unsubscribe()

	var sps, pps []byte
	if params != nil {
		sps, pps = params.SPS, params.PPS
	}
	var clock relativeClock
	headerSent := false
	sendHeader := func() error {
		if headerSent || len(sps) == 0 || len(pps) == 0 {
			return nil
		}
		if err := c.writeMessage(&Message{
			TypeID:   msgVideo,
			StreamID: ps.streamID,
			Payload:  flv.BuildVideoSequenceHeader(sps, pps),
		}, videoCSID); err != nil {
			return err
		}
		headerSent = true
		return nil
	}

	if err := sendHeader(); err != nil {
		return
	}

	for {
		f, ok := rd.Next(ps.cancel)
		if !ok {
			return
		}

		// SPS/PPS 只进序列头，不单独发 tag。
		if typ, err := media.NALType(f.Data); err == nil {
			switch typ {
			case rtp.NALUTypeSPS:
				sps = f.Data
				continue
			case rtp.NALUTypePPS:
				pps = f.Data
				continue
			}
		}

		// 参数集可能晚于第一帧：等 SPS/PPS 齐了再发媒体帧。
		if !headerSent {
			if err := sendHeader(); err != nil {
				return
			}
			if !headerSent {
				continue
			}
		}

		ms := uint32(uint64(f.Timestamp) * 1000 / 90000)
		if err := c.writeMessage(&Message{
			TypeID:    msgVideo,
			StreamID:  ps.streamID,
			Timestamp: clock.relative(ms),
			Payload:   flv.BuildVideoTag(f.Data, f.Keyframe, 0),
		}, videoCSID); err != nil {
			return
		}
	}
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

	var clock relativeClock
	for {
		f, ok := rd.Next(ps.cancel)
		if !ok {
			return
		}
		ms := uint32(uint64(f.Timestamp) * 1000 / uint64(rate))
		if err := c.writeMessage(&Message{
			TypeID:    msgAudio,
			StreamID:  ps.streamID,
			Timestamp: clock.relative(ms),
			Payload:   flv.BuildAudioTag(f.Data, rate, ch),
		}, audioCSID); err != nil {
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

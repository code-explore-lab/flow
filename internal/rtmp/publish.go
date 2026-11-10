package rtmp

// RTMP 推流 ingest：把 FLV 音视频 tag 解成 source.Frame 写进共享注册表。
//
// 编码参数（AVC 序列头里的 SPS/PPS、AAC 序列头里的 ASC）除了登记到
// stream.Stream，还会作为帧写进 ExternalSource：源内部会缓存 SPS/PPS/IDR，
// 晚加入的 RTSP 拉流端订阅时先补齐参数集再解码，不需要等下一个关键帧。
//
// 时间戳换算：
//
//	视频  ms → 90kHz：ts = ms * 90000 / 1000
//	音频  ms → 采样率：ts = ms * rate / 1000

import (
	"fmt"

	"flow/internal/flv"
	"flow/internal/media"
	"flow/internal/source"
	"flow/internal/stream"
)

// defaultAACRate 音频帧先于序列头到达时使用的采样率。
const defaultAACRate uint32 = 44100

// publishState 一条连接当前的推流状态。只在连接的读循环里读写，不需要锁。
type publishState struct {
	stream    *stream.Stream
	audioRate uint32
}

// stopPublish 注销当前推流并关闭帧源（读循环退出、FCUnpublish、
// deleteStream 时调用）。可重复调用。
func (c *Conn) stopPublish() {
	p := c.publish
	if p == nil {
		return
	}
	c.publish = nil
	if c.srv != nil {
		c.srv.registry.Unpublish(p.stream.Name, p.stream)
	} else {
		p.stream.Close()
	}
	fmt.Printf("rtmp: unpublished %q\n", p.stream.Name)
}

// handleVideo 处理一条 FLV video tag。
func (c *Conn) handleVideo(m *Message) {
	p := c.publish
	if p == nil {
		return
	}
	vt, err := flv.ParseVideoTag(m.Payload)
	if err != nil {
		fmt.Printf("rtmp: bad video tag: %v\n", err)
		return
	}

	switch vt.PacketType {
	case flv.AVCSeqHeader:
		sps, pps, err := flv.ParseAVCDecoderConfig(vt.Config)
		if err != nil {
			fmt.Printf("rtmp: bad avc config: %v\n", err)
			return
		}
		p.stream.SetVideoParams(sps, pps)
		if src := p.stream.SourceFor("video"); src != nil {
			// 参数集也作为帧写入：ExternalSource 按 NAL 类型缓存 SPS/PPS，
			// 中途加入的订阅者订阅时会先收到它们。
			src.Write(source.Frame{Data: sps, Timestamp: 0})
			src.Write(source.Frame{Data: pps, Timestamp: 0})
		}
	case flv.AVCNALU:
		src := p.stream.SourceFor("video")
		if src == nil {
			p.stream.EnsureVideo()
			src = p.stream.SourceFor("video")
		}
		ts := videoTimestamp(m.Timestamp)
		for _, nalu := range vt.NALUs {
			src.Write(source.Frame{Data: nalu, Timestamp: ts, Keyframe: vt.Keyframe})
		}
	case flv.AVCEndSeq:
		// 流结束标记，忽略；连接断开时由 stopPublish 收尾。
	}
}

// handleAudio 处理一条 FLV audio tag。
func (c *Conn) handleAudio(m *Message) {
	p := c.publish
	if p == nil {
		return
	}
	at, err := flv.ParseAudioTag(m.Payload)
	if err != nil {
		fmt.Printf("rtmp: bad audio tag: %v\n", err)
		return
	}

	switch at.PacketType {
	case flv.AACSeqHeader:
		rate, ch, err := media.ParseASC(at.Config)
		if err != nil {
			fmt.Printf("rtmp: bad aac config: %v\n", err)
			return
		}
		p.audioRate = uint32(rate)
		p.stream.SetAudioParams(at.Config, uint32(rate), ch)
	case flv.AACRaw:
		rate := p.audioRate
		if rate == 0 {
			rate = defaultAACRate
		}
		src := p.stream.SourceFor("audio")
		if src == nil {
			p.stream.EnsureAudio(rate)
			src = p.stream.SourceFor("audio")
		}
		src.Write(source.Frame{
			Data:      at.AU,
			Timestamp: audioTimestamp(m.Timestamp, rate),
			Keyframe:  true,
		})
	}
}

// videoTimestamp FLV 毫秒时间戳 → 90kHz RTP 时间戳。
func videoTimestamp(ms uint32) uint32 {
	return uint32(uint64(ms) * 90000 / 1000)
}

// audioTimestamp 毫秒时间戳 → 采样率时钟。
func audioTimestamp(ms, rate uint32) uint32 {
	return uint32(uint64(ms) * uint64(rate) / 1000)
}

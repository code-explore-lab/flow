package rtmp

// RTMP 推流 ingest：把 FLV 音视频 tag 解成 source.Frame 写进共享注册表。
//
// 实际的 FLV tag → source.Frame 映射在 internal/flvingest 里，RTMP 推流和
// 回源（RTMP / HTTP-FLV 拉流）共用同一套逻辑。本文件只保留连接生命周期
// （publishState 的登记/注销）和消息入口。

import (
	"fmt"

	"flow/internal/flvingest"
)

// publishState 一条连接当前的推流状态。只在连接的读循环里读写，不需要锁。
type publishState = flvingest.Ingest

// defaultAACRate 音频帧先于序列头到达时使用的采样率（测试/兼容用）。
const defaultAACRate = flvingest.DefaultAACRate

// stopPublish 注销当前推流并关闭帧源（读循环退出、FCUnpublish、
// deleteStream 时调用）。可重复调用。
func (c *Conn) stopPublish() {
	p := c.publish
	if p == nil {
		return
	}
	c.publish = nil
	if c.srv != nil {
		c.srv.metrics.DecPublish()
		c.srv.registry.Unpublish(p.Stream.Name, p.Stream)
	} else {
		p.Stream.Close()
	}
	fmt.Printf("rtmp: unpublished %q\n", p.Stream.Name)
}

// handleVideo 处理一条 FLV video tag。
func (c *Conn) handleVideo(m *Message) {
	if c.publish == nil {
		return
	}
	c.publish.Video(m.Payload, m.Timestamp)
}

// handleAudio 处理一条 FLV audio tag，支持 AAC、MP3 和 G.711。
func (c *Conn) handleAudio(m *Message) {
	if c.publish == nil {
		return
	}
	c.publish.Audio(m.Payload, m.Timestamp)
}

// videoTimestamp FLV 毫秒时间戳 → 90kHz RTP 时间戳（测试用）。
func videoTimestamp(ms uint32) uint32 {
	return flvingest.VideoTimestamp(ms)
}

// audioTimestamp 毫秒时间戳 → 采样率时钟（测试用）。
func audioTimestamp(ms, rate uint32) uint32 {
	return flvingest.AudioTimestamp(ms, rate)
}

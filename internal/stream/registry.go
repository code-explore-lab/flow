// Package stream 提供跨协议共享的「已发布流」注册表。
//
// RTSP 推流（ANNOUNCE）和 RTMP 推流（publish）都把解包后的编码参数与帧源
// 登记到同一个 Registry；任一协议的拉流端按流名查找到同一个 Stream，
// 订阅同一组 ExternalSource，从而实现 RTSP ↔ RTMP 互通。
//
// 本包只依赖 internal/source，不依赖 rtsp/rtmp：协议自己的描述（例如
// RTSP 的 SDP 轨道列表）放在 Stream.Meta 里，由各协议自行解释。
package stream

import (
	"sync"

	"flow/internal/source"
)

// VideoCodec 视频编码参数：裸 NALU（无 Annex-B 起始码）。
type VideoCodec struct {
	SPS []byte
	PPS []byte
}

// AudioCodec 音频编码参数。Config 是 AudioSpecificConfig（2 字节）。
type AudioCodec struct {
	Config     []byte
	SampleRate uint32
	Channels   uint8
}

// Track 一条媒体轨：帧源 + 该轨的编码参数。
type Track struct {
	Kind  string // "video" / "audio"
	Src   *source.ExternalSource
	Video *VideoCodec // Kind == "video"
	Audio *AudioCodec // Kind == "audio"
}

// Stream 一路已发布流。Video/Audio 在 Publish 之后不再替换（只更新参数），
// 因此各协议可以直接读取字段判断轨道是否存在。
type Stream struct {
	Name  string
	Video *Track
	Audio *Track
	Meta  any // 协议自定义描述；RTSP 存 *rtspStreamMeta，RTMP 为 nil

	mu sync.Mutex
}

// EnsureVideo 返回视频轨，不存在时创建。可重复调用。
func (s *Stream) EnsureVideo() *Track {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ensureVideoLocked()
}

func (s *Stream) ensureVideoLocked() *Track {
	if s.Video == nil {
		s.Video = &Track{
			Kind:  "video",
			Src:   source.NewExternalSource(),
			Video: &VideoCodec{},
		}
	}
	if s.Video.Video == nil {
		s.Video.Video = &VideoCodec{}
	}
	return s.Video
}

// EnsureAudio 返回音频轨，不存在时创建；rate 是在已有音频轨上也会更新的采样率。
func (s *Stream) EnsureAudio(rate uint32) *Track {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ensureAudioLocked(rate)
}

func (s *Stream) ensureAudioLocked(rate uint32) *Track {
	if s.Audio == nil {
		s.Audio = &Track{
			Kind:  "audio",
			Src:   source.NewExternalAudioSource(),
			Audio: &AudioCodec{},
		}
	}
	if s.Audio.Audio == nil {
		s.Audio.Audio = &AudioCodec{}
	}
	if rate > 0 {
		s.Audio.Src.SetSamplingRate(rate)
		s.Audio.Audio.SampleRate = rate
	}
	return s.Audio
}

// SetVideoParams 记录 SPS/PPS。切片会复制一份，调用方可复用缓冲区。
func (s *Stream) SetVideoParams(sps, pps []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.ensureVideoLocked()
	t.Video = &VideoCodec{
		SPS: append([]byte(nil), sps...),
		PPS: append([]byte(nil), pps...),
	}
}

// SetAudioParams 记录 AAC 的 AudioSpecificConfig、采样率和声道数。
func (s *Stream) SetAudioParams(asc []byte, rate uint32, ch uint8) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.ensureAudioLocked(rate)
	t.Audio.Config = append([]byte(nil), asc...)
	t.Audio.SampleRate = rate
	t.Audio.Channels = ch
}

// VideoParams 返回视频编码参数的副本。还没收到 SPS 时返回 false。
func (s *Stream) VideoParams() (*VideoCodec, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Video == nil || s.Video.Video == nil || len(s.Video.Video.SPS) == 0 {
		return nil, false
	}
	return &VideoCodec{
		SPS: append([]byte(nil), s.Video.Video.SPS...),
		PPS: append([]byte(nil), s.Video.Video.PPS...),
	}, true
}

// AudioParams 返回音频编码参数的副本。还没收到 ASC 时返回 false。
func (s *Stream) AudioParams() (*AudioCodec, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Audio == nil || s.Audio.Audio == nil || len(s.Audio.Audio.Config) == 0 {
		return nil, false
	}
	a := *s.Audio.Audio
	a.Config = append([]byte(nil), a.Config...)
	return &a, true
}

// VideoTrack 返回视频轨，没有时返回 nil。
func (s *Stream) VideoTrack() *Track {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Video
}

// AudioTrack 返回音频轨，没有时返回 nil。
func (s *Stream) AudioTrack() *Track {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Audio
}

// SourceFor 返回指定轨的帧源，没有该轨时返回 nil。
func (s *Stream) SourceFor(kind string) *source.ExternalSource {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch kind {
	case "video":
		if s.Video != nil {
			return s.Video.Src
		}
	case "audio":
		if s.Audio != nil {
			return s.Audio.Src
		}
	}
	return nil
}

// Close 关闭所有帧源，唤醒拉流端 reader。
func (s *Stream) Close() {
	s.mu.Lock()
	video, audio := s.Video, s.Audio
	s.mu.Unlock()
	if video != nil && video.Src != nil {
		video.Src.Close()
	}
	if audio != nil && audio.Src != nil {
		audio.Src.Close()
	}
}

// Registry 流名 → Stream 的并发安全注册表。
type Registry struct {
	mu sync.RWMutex
	m  map[string]*Stream
}

// NewRegistry 创建一个空注册表。
func NewRegistry() *Registry {
	return &Registry{m: make(map[string]*Stream)}
}

// Publish 登记一路流并返回它。同名流已存在时整体替换，并关闭旧源。
func (r *Registry) Publish(name string, s *Stream) *Stream {
	if s == nil {
		return nil
	}
	s.Name = name

	r.mu.Lock()
	old := r.m[name]
	r.m[name] = s
	r.mu.Unlock()

	if old != nil && old != s {
		old.Close()
	}
	return s
}

// Unpublish 从注册表删除。只有当前登记的仍是 s 时才删，避免旧会话晚到的
// 清理把重新发布的新流摘掉；删没删都关闭 s 的源。
func (r *Registry) Unpublish(name string, s *Stream) {
	if s == nil {
		return
	}
	r.mu.Lock()
	if r.m[name] == s {
		delete(r.m, name)
	}
	r.mu.Unlock()
	s.Close()
}

// Get 按流名查找。
func (r *Registry) Get(name string) *Stream {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.m[name]
}

// Len 返回当前登记的流数量（测试/日志用）。
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.m)
}

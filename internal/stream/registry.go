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
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"flow/internal/source"
)

// PublishPolicy 决定同名推流到达时的处理方式。
type PublishPolicy int

const (
	// PublishReplace 同名推流整体替换旧流并关闭旧源（默认，保持原有行为）。
	PublishReplace PublishPolicy = iota
	// PublishReject 只要同名流已存在就拒绝新推流，旧流保持不变。
	PublishReject
	// PublishRejectIfActive 旧流最近仍有媒体写入时拒绝；否则视为僵尸流，
	// 允许新推流接管并关闭旧源。
	PublishRejectIfActive
)

// ErrPublishRejected 表示同名流已存在且当前策略不允许替换。
var ErrPublishRejected = errors.New("stream: duplicate publish rejected")

// DefaultActiveWindow 是 reject-if-active 判定「旧流仍活跃」的时间窗口：
// 旧流任意一轨在该窗口内还有 Write 就算活跃。
const DefaultActiveWindow = 10 * time.Second

// ParsePublishPolicy 解析配置里的策略名。空串按默认的 replace 处理。
func ParsePublishPolicy(value string) (PublishPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "replace":
		return PublishReplace, nil
	case "reject":
		return PublishReject, nil
	case "reject-if-active":
		return PublishRejectIfActive, nil
	}
	return PublishReplace, fmt.Errorf("stream: invalid publish policy %q (want replace, reject or reject-if-active)", value)
}

// String 返回策略的配置名。
func (p PublishPolicy) String() string {
	switch p {
	case PublishReject:
		return "reject"
	case PublishRejectIfActive:
		return "reject-if-active"
	default:
		return "replace"
	}
}

// VideoCodec 视频编码参数：裸 NALU（无 Annex-B 起始码）。
// Codec 为 "H264" / "H265"；VPS 仅 H.265 使用，H.264 时为空。
type VideoCodec struct {
	Codec string
	VPS   []byte
	SPS   []byte
	PPS   []byte
}

// 音频编码名，跨 RTSP/RTMP 统一使用这组常量。
const (
	AudioAAC  = "AAC"
	AudioPCMA = "PCMA" // G.711 A-law
	AudioPCMU = "PCMU" // G.711 μ-law
	AudioOpus = "OPUS"
	AudioMP3  = "MP3"
)

// NormalizeAudioCodec 把协议里的编码别名归一成上面的常量。
// 空串按 AAC 处理，兼容早期只有 AAC 的调用方。
func NormalizeAudioCodec(codec string) string {
	switch strings.ToUpper(strings.TrimSpace(codec)) {
	case "", "AAC", "MPEG4-GENERIC":
		return AudioAAC
	case "PCMA", "G711A", "ALAW", "A-LAW":
		return AudioPCMA
	case "PCMU", "G711U", "ULAW", "MU-LAW":
		return AudioPCMU
	case "OPUS":
		return AudioOpus
	case "MP3", "MPA", "MPEG":
		return AudioMP3
	}
	return strings.ToUpper(strings.TrimSpace(codec))
}

// AudioClockRate 返回编码的 RTP 时钟频率。MP3 按 RFC 2250 固定 90kHz，
// 其余编码使用采样率（PCMA/PCMU 8k、Opus 48k、AAC 随 ASC）。
func AudioClockRate(codec string, sampleRate uint32) uint32 {
	switch NormalizeAudioCodec(codec) {
	case AudioMP3:
		return 90000
	case AudioPCMA, AudioPCMU:
		if sampleRate == 0 {
			return 8000
		}
		return sampleRate
	case AudioOpus:
		if sampleRate == 0 {
			return 48000
		}
		return sampleRate
	default:
		if sampleRate == 0 {
			return 44100
		}
		return sampleRate
	}
}

// DefaultAudioSampleRate 返回编码在缺少显式参数时使用的采样率。
func DefaultAudioSampleRate(codec string) uint32 {
	switch NormalizeAudioCodec(codec) {
	case AudioPCMA, AudioPCMU:
		return 8000
	case AudioOpus:
		return 48000
	case AudioMP3:
		return 44100
	default:
		return 44100
	}
}

// AudioCodec 音频编码参数。
//
// Config 是编码相关配置：AAC 为 AudioSpecificConfig，Opus 为 OpusHead /
// 声道映射，PCMA/PCMU/MP3 为空。
//
// SampleRate 和 ClockRate 必须分开：MP3 的 RTP 时钟固定 90000，而采样率
// 通常是 44100，混用会让 RTMP 的毫秒时间戳偏差约一倍。
type AudioCodec struct {
	Codec      string // "AAC" / "PCMA" / "PCMU" / "OPUS" / "MP3"
	Config     []byte
	SampleRate uint32 // 音频采样率（RTMP/FLV 用）
	ClockRate  uint32 // RTP 时钟频率（RTSP/RTP 时间戳用）
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

	// metaData 是推流端发来的 onMetaData（RTMP）。any 保持本包不依赖
	// internal/flv；具体结构由 rtmp 包解释。带锁，因为它可能晚于拉流到达。
	metaData any

	// createdAt 是流登记到 Registry 的时间，供还没有建轨的流判断活跃度。
	createdAt time.Time

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
			Video: &VideoCodec{Codec: "H264"},
		}
	}
	if s.Video.Video == nil {
		s.Video.Video = &VideoCodec{Codec: "H264"}
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

// SetVideoParams 记录视频编码参数。codec 为 "H264" / "H265"，
// H.264 传空 vps 即可。切片会复制一份，调用方可复用缓冲区。
// 同时把编码同步给帧源，供 ExternalSource 按 NAL 类型缓存参数集。
func (s *Stream) SetVideoParams(codec string, vps, sps, pps []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.ensureVideoLocked()
	t.Video = &VideoCodec{
		Codec: codec,
		VPS:   append([]byte(nil), vps...),
		SPS:   append([]byte(nil), sps...),
		PPS:   append([]byte(nil), pps...),
	}
	if t.Src != nil {
		t.Src.SetCodec(codec)
	}
}

// SetAudioCodec 记录音频编码、配置、采样率、声道数和 RTP 时钟。
// clockRate 为 0 时按编码推导（MP3=90000，其余=采样率）。
func (s *Stream) SetAudioCodec(codec string, config []byte, rate uint32, ch uint8, clockRate uint32) {
	codec = NormalizeAudioCodec(codec)
	if rate == 0 {
		rate = DefaultAudioSampleRate(codec)
	}
	if clockRate == 0 {
		clockRate = AudioClockRate(codec, rate)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.ensureAudioLocked(rate)
	t.Audio.Codec = codec
	t.Audio.Config = append([]byte(nil), config...)
	t.Audio.SampleRate = rate
	t.Audio.ClockRate = clockRate
	t.Audio.Channels = ch
	// 帧源的 SamplingRate 用作 RTP 时钟（RTCP SR / RTSP 拉流）。
	if t.Src != nil {
		t.Src.SetSamplingRate(clockRate)
	}
}

// SetAudioParams 记录 AAC 的 AudioSpecificConfig、采样率和声道数。
// AAC 的 RTP 时钟等于采样率，保留这个入口兼容原调用方。
func (s *Stream) SetAudioParams(asc []byte, rate uint32, ch uint8) {
	s.SetAudioCodec(AudioAAC, asc, rate, ch, rate)
}

// VideoParams 返回视频编码参数的副本。还没收到 SPS 时返回 false。
func (s *Stream) VideoParams() (*VideoCodec, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Video == nil || s.Video.Video == nil || len(s.Video.Video.SPS) == 0 {
		return nil, false
	}
	return &VideoCodec{
		Codec: s.Video.Video.Codec,
		VPS:   append([]byte(nil), s.Video.Video.VPS...),
		SPS:   append([]byte(nil), s.Video.Video.SPS...),
		PPS:   append([]byte(nil), s.Video.Video.PPS...),
	}, true
}

// SetMetaData 记录推流端发来的 onMetaData。后到的覆盖先到的。
func (s *Stream) SetMetaData(v any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metaData = v
}

// MetaData 返回推流端发来的 onMetaData，没有时为 nil。
func (s *Stream) MetaData() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.metaData
}

// AudioParams 返回音频编码参数的副本。还没收到编码参数时返回 false。
// PCMA/PCMU/MP3 没有 Config，靠 Codec 字段判断参数已就绪。
func (s *Stream) AudioParams() (*AudioCodec, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Audio == nil || s.Audio.Audio == nil {
		return nil, false
	}
	a := *s.Audio.Audio
	if a.Codec == "" && len(a.Config) == 0 {
		return nil, false
	}
	if a.Codec == "" {
		a.Codec = AudioAAC
	}
	if a.SampleRate == 0 {
		a.SampleRate = DefaultAudioSampleRate(a.Codec)
	}
	if a.ClockRate == 0 {
		a.ClockRate = AudioClockRate(a.Codec, a.SampleRate)
	}
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

	// policy 是同名推流策略；activeWindow 是 reject-if-active 的活跃窗口。
	policy       PublishPolicy
	activeWindow time.Duration
}

// NewRegistry 创建一个空注册表，默认替换同名流。
func NewRegistry() *Registry {
	return &Registry{
		m:            make(map[string]*Stream),
		activeWindow: DefaultActiveWindow,
	}
}

// SetPublishPolicy 设置同名推流策略。必须在 Serve 之前调用；
// 默认是 PublishReplace。
func (r *Registry) SetPublishPolicy(p PublishPolicy) {
	r.mu.Lock()
	r.policy = p
	r.mu.Unlock()
}

// Active 报告这条流在 window 内是否还有媒体写入。音视频任一轨在窗口内
// 有 Write 即为活跃；还没建轨（RTMP 首帧之前）时按登记时间判断。
func (s *Stream) Active(window time.Duration) bool {
	if window <= 0 {
		window = DefaultActiveWindow
	}
	now := time.Now()

	s.mu.Lock()
	video, audio, created := s.Video, s.Audio, s.createdAt
	s.mu.Unlock()

	if video == nil && audio == nil {
		// 刚登记、首帧还没到：按创建时间算，避免把正常启动的流当僵尸。
		return created.IsZero() || now.Sub(created) <= window
	}
	for _, t := range []*Track{video, audio} {
		if t != nil && t.Src != nil && now.Sub(t.Src.LastWrite()) <= window {
			return true
		}
	}
	return false
}

// Publish 按当前策略登记一路流。成功返回登记的流；同名冲突被拒时返回
// ErrPublishRejected，新流不登记、旧流保持不变，调用方可以直接丢弃 s。
func (r *Registry) Publish(name string, s *Stream) (*Stream, error) {
	if s == nil {
		return nil, nil
	}
	s.Name = name

	now := time.Now()
	s.mu.Lock()
	if s.createdAt.IsZero() {
		s.createdAt = now
	}
	s.mu.Unlock()

	r.mu.Lock()
	old := r.m[name]
	if old != nil && old != s {
		reject := r.policy == PublishReject ||
			(r.policy == PublishRejectIfActive && old.Active(r.activeWindow))
		if reject {
			r.mu.Unlock()
			return nil, ErrPublishRejected
		}
	}
	r.m[name] = s
	r.mu.Unlock()

	if old != nil && old != s {
		old.Close()
	}
	return s, nil
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

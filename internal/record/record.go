// Package record 把注册表里的实时流写到磁盘（F-3）。
//
// 设计要点：
//   - 只做转封装，不解码、不转码；输入是 source.Frame，输出是 MP4(fMP4)/
//     FLV/MPEG-TS。
//   - 视频按访问单元聚合，只有拿到 IDR 才开始写；轮转也只在关键帧处发生，
//     保证每个文件都能独立播放。
//   - 输出文件的时间轴从 0 开始；跨推流重连/时间戳回退时由 dtsClock 修补，
//     不把断点留给播放器。
//   - 进程崩溃后可以续录：每个文件旁边有一个 .state 状态文件，记录已落盘的
//     字节数和下一片段的序号/时间轴；启动时截断半截数据后追加。
//
// 线程模型：每条轨一个 goroutine 负责订阅、AU 聚合与时间轴修补，把完整的
// sample 投递到一个 channel；单独的 writer 循环负责轮转、配额、落盘。
// channel 有界，磁盘慢时会对录制自己形成背压（源 ring 不会被阻塞）。
package record

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"flow/internal/media"
	"flow/internal/source"
	"flow/internal/stream"
)

// Format 录制容器格式。
type Format string

const (
	FormatMP4 Format = "mp4"
	FormatFLV Format = "flv"
	FormatTS  Format = "ts"
)

// ParseFormat 解析 -record-format。
func ParseFormat(value string) (Format, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "mp4":
		return FormatMP4, nil
	case "flv":
		return FormatFLV, nil
	case "ts", "mpegts":
		return FormatTS, nil
	}
	return "", fmt.Errorf("record: invalid format %q (want mp4, flv or ts)", value)
}

// Options 录制参数。零值可用：30s 一段、不限制磁盘、不续录。
type Options struct {
	Dir             string        // 输出目录
	Format          Format        // 容器格式
	SegmentDuration time.Duration // 到第一个关键帧且达到该时长后轮转
	Quota           int64         // 目录内录制总字节预算；0 不限制
	Retention       time.Duration // 删除超过该年龄的完整文件；0 不删除
	Resume          bool          // 启动时续录未完成的文件
	Now             func() time.Time
	Logf            func(format string, args ...any)
}

func (o Options) withDefaults() Options {
	if o.Dir == "" {
		o.Dir = "recordings"
	}
	if o.Format == "" {
		o.Format = FormatMP4
	}
	if o.SegmentDuration <= 0 {
		o.SegmentDuration = 30 * time.Second
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Logf == nil {
		o.Logf = log.Printf
	}
	return o
}

func (o Options) logf(format string, args ...any) { o.Logf(format, args...) }

// TrackSource 一路轨的订阅源与已知编码参数。
// 视频参数可以缺失：带内 SPS/PPS 会在录制过程中补齐；
// 音频没有带内参数，必须由来源（或回调）提供。
type TrackSource struct {
	Subscriber source.Subscriber
	Video      *stream.VideoCodec
	Audio      *stream.AudioCodec
}

// Spec 一路要录制的流。
type Spec struct {
	Name   string
	Stream *stream.Stream
	Video  *TrackSource
	Audio  *TrackSource

	// VideoParams/AudioParams 在写文件时读取最新参数。RTMP 的 AAC 序列头
	// 可能晚于 PUBLISH 到达，用回调避免录制器启动过早拿不到 ASC。
	VideoParams func() (*stream.VideoCodec, bool)
	AudioParams func() (*stream.AudioCodec, bool)
}

// SpecFromStream 从注册表的一路流构造 Spec。
// 某个轨还没建立时对应字段为 nil，调用方可以稍后重试。
func SpecFromStream(st *stream.Stream) Spec {
	spec := Spec{
		Name:        st.Name,
		Stream:      st,
		VideoParams: st.VideoParams,
		AudioParams: st.AudioParams,
	}
	if src := st.SourceFor("video"); src != nil {
		spec.Video = &TrackSource{Subscriber: src}
		if vp, ok := st.VideoParams(); ok {
			spec.Video.Video = vp
		}
	}
	if src := st.SourceFor("audio"); src != nil {
		spec.Audio = &TrackSource{Subscriber: src}
		if ap, ok := st.AudioParams(); ok {
			spec.Audio.Audio = ap
		}
	}
	return spec
}

type trackKind uint8

const (
	videoTrack trackKind = iota + 1
	audioTrack
)

type eventKind uint8

const (
	eventSample eventKind = iota + 1
	eventVideoParams
	eventAudioParams
)

// sample 一条已聚合、带完整时间信息的媒体样本。
// 视频的 nalus 是访问单元内的裸 NALU（已去掉 SPS/PPS/AUD）；
// 音频的 data 是一个裸 AAC/MP3/G.711 帧。
type sample struct {
	kind  trackKind
	nalus [][]byte
	data  []byte
	dts   int64  // 轨道时间轴上的解码时间（已做连续性修补）
	dur   uint32 // 样本时长
	cts   int32  // PTS − DTS（视频 90kHz）
	key   bool
}

type event struct {
	kind   eventKind
	sample sample
	video  *stream.VideoCodec
	audio  *stream.AudioCodec
}

// Recorder 一路流的录制器。Start 之后由内部 goroutine 驱动，Close 会
// 冲刷当前片段、关闭文件并等待所有 goroutine 退出。
type Recorder struct {
	spec Spec
	opts Options
	name string

	events chan event
	cancel chan struct{}
	done   chan struct{}

	cancelOnce sync.Once
	trackWG    sync.WaitGroup

	errMu sync.Mutex
	err   error

	// 只在 writer 循环里读写
	writer            segmentWriter
	vparams           *stream.VideoCodec
	aparams           *stream.AudioCodec
	pendingAudio      []sample
	pendingAudioBytes int
	awaitKey          bool
	warnedVideoParams bool
	warnedAudioParams bool
}

// NewRecorder 校验参数并创建录制器，但不启动。调用 Start 开始工作。
func NewRecorder(spec Spec, opts Options) (*Recorder, error) {
	opts = opts.withDefaults()
	if strings.TrimSpace(spec.Name) == "" {
		return nil, errors.New("record: stream name is empty")
	}
	if spec.Video == nil && spec.Audio == nil {
		return nil, errors.New("record: no video or audio track")
	}
	if spec.Video != nil && spec.Video.Subscriber == nil {
		return nil, errors.New("record: video subscriber is nil")
	}
	if spec.Audio != nil && spec.Audio.Subscriber == nil {
		return nil, errors.New("record: audio subscriber is nil")
	}
	if opts.Format != FormatMP4 && opts.Format != FormatFLV && opts.Format != FormatTS {
		return nil, fmt.Errorf("record: unsupported format %q", opts.Format)
	}
	if opts.SegmentDuration <= 0 {
		return nil, errors.New("record: segment duration must be > 0")
	}
	if opts.Quota < 0 {
		return nil, errors.New("record: quota must be >= 0")
	}
	if opts.Retention < 0 {
		return nil, errors.New("record: retention must be >= 0")
	}
	if err := mkdirAll(opts.Dir); err != nil {
		return nil, err
	}
	r := &Recorder{
		spec:   spec,
		opts:   opts,
		name:   spec.Name,
		events: make(chan event, 512),
		cancel: make(chan struct{}),
		done:   make(chan struct{}),
	}
	if spec.Video != nil {
		r.vparams = spec.Video.Video
	}
	if spec.Audio != nil {
		r.aparams = spec.Audio.Audio
	}
	// 视频参数可能先于音频到达；等第一个关键帧再决定是否连音频一起录。
	r.awaitKey = true
	return r, nil
}

// Start 启动录制 goroutine。
func (r *Recorder) Start() {
	if r.spec.Video != nil {
		r.trackWG.Add(1)
		go r.videoLoop()
	}
	if r.spec.Audio != nil {
		r.trackWG.Add(1)
		go r.audioLoop()
	}
	go func() {
		r.trackWG.Wait()
		close(r.events)
	}()
	go r.run()
}

// Close 停止录制并等待文件收尾。可重复调用。
func (r *Recorder) Close() {
	r.cancelOnce.Do(func() { close(r.cancel) })
	<-r.done
}

// Err 返回导致录制提前结束的错误；正常关闭返回 nil。
func (r *Recorder) Err() error {
	r.errMu.Lock()
	defer r.errMu.Unlock()
	return r.err
}

func (r *Recorder) fail(err error) {
	if err == nil {
		return
	}
	r.errMu.Lock()
	if r.err == nil {
		r.err = err
		r.opts.logf("record[%s]: %v", r.name, err)
	}
	r.errMu.Unlock()
	r.cancelOnce.Do(func() { close(r.cancel) })
}

func (r *Recorder) failed() bool {
	return r.Err() != nil
}

// send 投递事件；录制被取消时返回 false，调用方应立即退出。
func (r *Recorder) send(ev event) bool {
	select {
	case r.events <- ev:
		return true
	case <-r.cancel:
		return false
	}
}

// ---- writer 循环 ----

func (r *Recorder) run() {
	defer close(r.done)
	for ev := range r.events {
		if r.failed() {
			// writer 出错后仍然吸收事件，避免轨 goroutine 卡在 channel 上。
			continue
		}
		if err := r.handle(ev); err != nil {
			r.fail(err)
		}
	}
	r.finish()
}

func (r *Recorder) handle(ev event) error {
	switch ev.kind {
	case eventVideoParams:
		if codecEqual(r.vparams, ev.video) {
			return nil
		}
		r.vparams = ev.video
		if r.writer != nil {
			return r.reopen("video parameters changed")
		}
		return nil
	case eventAudioParams:
		if audioEqual(r.aparams, ev.audio) {
			return nil
		}
		r.aparams = ev.audio
		if r.writer != nil {
			return r.reopen("audio parameters changed")
		}
		return nil
	case eventSample:
		if ev.sample.kind == videoTrack {
			return r.handleVideo(ev.sample)
		}
		return r.handleAudio(ev.sample)
	}
	return nil
}

func (r *Recorder) reopen(reason string) error {
	if r.writer == nil {
		return nil
	}
	r.opts.logf("record[%s]: rotate: %s", r.name, reason)
	err := r.writer.Close()
	r.writer = nil
	// 下一段必须从 IDR 开始；音频先缓存，视频非关键帧丢掉。
	r.awaitKey = true
	return err
}

func (r *Recorder) handleVideo(s sample) error {
	if r.awaitKey && !s.key {
		return nil
	}
	starting := r.awaitKey
	if r.writer == nil {
		if !r.ensureWriter(true) {
			if !r.warnedVideoParams {
				r.warnedVideoParams = true
				r.opts.logf("record[%s]: waiting for SPS/PPS before writing video", r.name)
			}
			return nil
		}
	}
	r.awaitKey = false
	if starting {
		// 文件从关键帧开始；关键帧之前的音频会让音画在文件开头错位。
		r.trimPendingAudio(s.dts)
	}
	if err := r.flushPendingAudio(); err != nil {
		return err
	}
	return r.writer.Write(s)
}

func (r *Recorder) handleAudio(s sample) error {
	if r.writer == nil {
		if r.spec.Video == nil {
			if !r.ensureWriter(false) {
				if !r.warnedAudioParams {
					r.warnedAudioParams = true
					r.opts.logf("record[%s]: waiting for audio parameters before writing", r.name)
				}
				r.bufferAudio(s)
				return nil
			}
		} else {
			// 视频还没起头：先攒音频，等第一个 IDR 一起写入同一个文件。
			r.bufferAudio(s)
			return nil
		}
	}
	// 音频参数晚到导致刚刚才建文件时，把之前攒下的帧先补进去。
	if err := r.flushPendingAudio(); err != nil {
		return err
	}
	return r.writer.Write(s)
}

func (r *Recorder) bufferAudio(s sample) {
	const maxPendingAudio = 8 << 20
	r.pendingAudio = append(r.pendingAudio, s)
	r.pendingAudioBytes += len(s.data)
	for r.pendingAudioBytes > maxPendingAudio && len(r.pendingAudio) > 1 {
		r.pendingAudioBytes -= len(r.pendingAudio[0].data)
		r.pendingAudio = r.pendingAudio[1:]
	}
}

// trimPendingAudio 丢弃第一个 IDR 之前的音频。音频和视频各自把首个样本
// 归一到 0；不剪掉关键帧之前的音频，文件开头会有固定音画偏移（最大一个 GOP）。
func (r *Recorder) trimPendingAudio(videoDTS int64) {
	if len(r.pendingAudio) == 0 {
		return
	}
	ap := r.audioCodec()
	if ap == nil {
		return
	}
	rate := int64(audioClockRate(ap))
	if rate <= 0 {
		return
	}
	keepFrom := videoDTS * rate / videoTimescale
	tol := rate / 50 // 20ms，容忍时间基换算的舍入
	cut := 0
	for cut < len(r.pendingAudio) && r.pendingAudio[cut].dts < keepFrom-tol {
		r.pendingAudioBytes -= len(r.pendingAudio[cut].data)
		cut++
	}
	if cut > 0 {
		r.pendingAudio = append([]sample(nil), r.pendingAudio[cut:]...)
	}
}

func (r *Recorder) flushPendingAudio() error {
	if r.writer == nil || len(r.pendingAudio) == 0 {
		return nil
	}
	pending := r.pendingAudio
	r.pendingAudio = nil
	r.pendingAudioBytes = 0
	for _, s := range pending {
		if err := r.writer.Write(s); err != nil {
			return err
		}
	}
	return nil
}

// ensureWriter 在参数齐全后创建第一个文件。needVideo 表示当前是不是视频
// 触发的（视频必须等 SPS/PPS；音频可以先开始）。
func (r *Recorder) ensureWriter(needVideo bool) bool {
	if r.writer != nil {
		return true
	}
	vp := r.videoCodec()
	ap := r.audioCodec()
	hasVideo := r.spec.Video != nil && vp != nil && len(vp.SPS) > 0 && len(vp.PPS) > 0
	hasAudio := r.spec.Audio != nil && ap != nil && audioUsable(ap, r.opts.Format)
	if needVideo && r.spec.Video != nil && !hasVideo {
		return false
	}
	if !hasVideo && !hasAudio {
		return false
	}
	w, err := newSegmentWriter(r.opts, r.name, vp, ap, hasVideo, hasAudio)
	if err != nil {
		r.fail(err)
		return false
	}
	r.writer = w
	r.opts.logf("record[%s]: start %s (video=%v audio=%v dir=%s)",
		r.name, r.opts.Format, hasVideo, hasAudio, r.opts.Dir)
	return true
}

func (r *Recorder) videoCodec() *stream.VideoCodec {
	if r.spec.VideoParams != nil {
		if vp, ok := r.spec.VideoParams(); ok {
			return vp
		}
	}
	return r.vparams
}

func (r *Recorder) audioCodec() *stream.AudioCodec {
	if ap := r.audioParamsFor(r.spec.Audio); ap != nil {
		return ap
	}
	return r.aparams
}

// audioParamsFor 优先读注册表的最新参数，回退到创建时的快照。
// 只从 writer 循环或轨 goroutine 调用；回退值在轨 goroutine 里用 src 自己
// 的快照，避免和数据面写 r.aparams 发生竞态。
func (r *Recorder) audioParamsFor(src *TrackSource) *stream.AudioCodec {
	if r.spec.AudioParams != nil {
		if ap, ok := r.spec.AudioParams(); ok {
			return ap
		}
	}
	if src != nil && src.Audio != nil {
		return src.Audio
	}
	return nil
}

func (r *Recorder) finish() {
	if r.writer != nil {
		if err := r.writer.Close(); err != nil {
			r.fail(err)
		}
		r.writer = nil
	}
	r.opts.logf("record[%s]: stopped", r.name)
}

// ---- 视频轨：AU 聚合 ----

type auBuilder struct {
	active bool
	pts    uint32
	rawDTS int64
	cts    int32
	nalus  [][]byte
	key    bool
	hasVCL bool
}

func (r *Recorder) videoLoop() {
	defer r.trackWG.Done()
	src := r.spec.Video
	rd, unsubscribe := src.Subscriber.Subscribe()
	defer unsubscribe()

	codec := "H264"
	if src.Video != nil && src.Video.Codec != "" {
		codec = src.Video.Codec
	}
	var (
		au        auBuilder
		pending   *sample
		clock     = newDTSClock(videoTimescale, videoFallbackTicks)
		pts       dtsUnwrapper
		haveKey   bool
		vps, sps  []byte
		pps       []byte
		lastParam []byte // 参数集指纹，避免重复发送
	)

	emitSample := func(s sample) bool {
		if pending != nil {
			dur := s.dts - pending.dts
			if dur <= 0 || dur > clock.timescale*maxDTSJumpSeconds {
				dur = clock.fallback
			}
			pending.dur = clampDuration(dur)
			if !r.send(event{kind: eventSample, sample: *pending}) {
				return false
			}
		}
		pending = &s
		return true
	}

	finishAU := func() bool {
		if !au.active {
			return true
		}
		a := au
		au = auBuilder{}
		if !a.hasVCL || len(a.nalus) == 0 {
			return true
		}
		if !haveKey {
			if !a.key {
				return true
			}
			haveKey = true
		}
		dts := clock.mapDTS(a.rawDTS)
		return emitSample(sample{
			kind:  videoTrack,
			nalus: a.nalus,
			dts:   dts,
			cts:   a.cts,
			key:   a.key,
		})
	}

	for {
		f, ok := rd.Next(r.cancel)
		if !ok {
			break
		}
		select {
		case <-r.cancel:
			return
		default:
		}
		if len(f.Data) == 0 {
			continue
		}
		class := media.ClassifyVideoNAL(codec, f.Data)
		if class.IsVPS || class.IsSPS || class.IsPPS {
			switch {
			case class.IsVPS:
				vps = append([]byte(nil), f.Data...)
			case class.IsSPS:
				sps = append([]byte(nil), f.Data...)
			default:
				pps = append([]byte(nil), f.Data...)
			}
			if len(sps) > 0 && len(pps) > 0 {
				fp := paramFingerprint(vps, sps, pps)
				if !bytesEqual(fp, lastParam) {
					lastParam = fp
					vp := &stream.VideoCodec{Codec: codec, VPS: vps, SPS: sps, PPS: pps}
					if !r.send(event{kind: eventVideoParams, video: vp}) {
						return
					}
				}
			}
			continue
		}
		typ, err := media.VideoNALType(codec, f.Data)
		if err != nil {
			continue
		}
		if media.IsHEVCCodec(codec) {
			if typ == 35 { // AUD_NUT
				continue
			}
		} else if typ == 9 { // AUD
			continue
		}

		rawPTS := pts.unwrap(f.Timestamp)
		rawDTS := rawPTS - int64(f.CompositionTime)
		newAU := f.AUStart || !au.active || (class.IsVCL && f.Timestamp != au.pts)
		if newAU {
			if !finishAU() {
				return
			}
			au = auBuilder{
				active: true,
				pts:    f.Timestamp,
				rawDTS: rawDTS,
				cts:    f.CompositionTime,
			}
		}
		if class.IsIDR {
			au.key = true
		}
		if class.IsVCL {
			au.hasVCL = true
		}
		au.nalus = append(au.nalus, f.Data)
		if f.AUEnd {
			if !finishAU() {
				return
			}
		}
	}
	finishAU()
	if pending != nil {
		pending.dur = clampDuration(clock.fallback)
		r.send(event{kind: eventSample, sample: *pending})
	}
}

// ---- 音频轨 ----

func (r *Recorder) audioLoop() {
	defer r.trackWG.Done()
	src := r.spec.Audio
	rd, unsubscribe := src.Subscriber.Subscribe()
	defer unsubscribe()

	clock := newDTSClock(audioFallbackTicks, audioFallbackTicks)
	if ap := r.audioParamsFor(src); ap != nil {
		clock.timescale = int64(audioClockRate(ap))
	}
	var (
		pending *sample
		pts     dtsUnwrapper
	)
	emit := func(s sample) bool {
		if pending != nil {
			dur := s.dts - pending.dts
			if dur <= 0 || dur > clock.timescale*5 {
				dur = clock.fallback
			}
			pending.dur = clampDuration(dur)
			if !r.send(event{kind: eventSample, sample: *pending}) {
				return false
			}
		}
		pending = &s
		return true
	}
	for {
		f, ok := rd.Next(r.cancel)
		if !ok {
			break
		}
		select {
		case <-r.cancel:
			return
		default:
		}
		if len(f.Data) == 0 {
			continue
		}
		// 采样率可能在首帧后才修正（MP3），这里按最新参数更新。
		if ap := r.audioParamsFor(src); ap != nil {
			if ts := int64(audioClockRate(ap)); ts > 0 && clock.timescale != ts {
				clock.timescale = ts
			}
		}
		raw := pts.unwrap(f.Timestamp)
		s := sample{
			kind: audioTrack,
			data: append([]byte(nil), f.Data...),
			dts:  clock.mapDTS(raw),
			key:  true,
		}
		if !emit(s) {
			return
		}
	}
	if pending != nil {
		pending.dur = clampDuration(clock.fallback)
		r.send(event{kind: eventSample, sample: *pending})
	}
}

// ---- 时间轴工具 ----

const (
	videoTimescale     = 90000
	videoFallbackTicks = 3600 // 25fps
	audioFallbackTicks = 1024
	maxDTSJumpSeconds  = 5
)

// dtsUnwrapper 把 uint32 时间戳展开成单调的 int64，处理 2^32 回绕。
type dtsUnwrapper struct {
	have bool
	last uint32
	val  int64
}

func (u *dtsUnwrapper) unwrap(raw uint32) int64 {
	if !u.have {
		u.have = true
		u.last = raw
		u.val = int64(raw)
		return u.val
	}
	u.val += int64(int32(raw - u.last))
	u.last = raw
	return u.val
}

// dtsClock 在原始 DTS 上做两件事：给写入器提供连续时间轴；时间戳回退或
// 大跳变时把样本接在上一个样本之后，而不是让文件里出现负时长。
type dtsClock struct {
	timescale int64
	fallback  int64
	have      bool
	offset    int64
	last      int64
}

func newDTSClock(timescale, fallback int64) dtsClock {
	if timescale <= 0 {
		timescale = 1
	}
	if fallback <= 0 {
		fallback = timescale / 25
		if fallback <= 0 {
			fallback = 1
		}
	}
	return dtsClock{timescale: timescale, fallback: fallback}
}

func (c *dtsClock) mapDTS(raw int64) int64 {
	if !c.have {
		c.have = true
		c.last = raw
		return raw
	}
	out := raw + c.offset
	if out < c.last || out-c.last > c.timescale*maxDTSJumpSeconds {
		c.offset += c.last + c.fallback - out
		out = raw + c.offset
	}
	c.last = out
	return out
}

// clampDuration 把时长压到 fMP4 的 uint32 范围内；时长 0 视为非法并取 1。
func clampDuration(dur int64) uint32 {
	if dur <= 0 {
		return 1
	}
	if dur > int64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(dur)
}

func audioClockRate(ap *stream.AudioCodec) uint32 {
	if ap == nil {
		return 44100
	}
	clock := ap.ClockRate
	if clock == 0 {
		clock = stream.AudioClockRate(ap.Codec, ap.SampleRate)
	}
	if clock == 0 {
		clock = 44100
	}
	return clock
}

// audioUsable 报告音频编码能否放进目标容器。
func audioUsable(ap *stream.AudioCodec, format Format) bool {
	if ap == nil {
		return false
	}
	codec := stream.NormalizeAudioCodec(ap.Codec)
	switch format {
	case FormatMP4, FormatTS:
		// fMP4 init 与 TS muxer 目前都只实现 AAC。
		return codec == stream.AudioAAC && len(ap.Config) >= 2
	case FormatFLV:
		switch codec {
		case stream.AudioAAC, stream.AudioMP3, stream.AudioPCMA, stream.AudioPCMU:
			return true
		}
	}
	return false
}

func codecEqual(a, b *stream.VideoCodec) bool {
	if a == nil || b == nil {
		return a == b
	}
	return strings.EqualFold(a.Codec, b.Codec) &&
		bytesEqual(a.VPS, b.VPS) && bytesEqual(a.SPS, b.SPS) && bytesEqual(a.PPS, b.PPS)
}

func audioEqual(a, b *stream.AudioCodec) bool {
	if a == nil || b == nil {
		return a == b
	}
	return strings.EqualFold(a.Codec, b.Codec) &&
		bytesEqual(a.Config, b.Config) &&
		a.SampleRate == b.SampleRate && a.Channels == b.Channels && a.ClockRate == b.ClockRate
}

func bytesEqual(a, b []byte) bool { return bytes.Equal(a, b) }

func paramFingerprint(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, byte(len(p)>>24), byte(len(p)>>16), byte(len(p)>>8), byte(len(p)))
		out = append(out, p...)
	}
	return out
}

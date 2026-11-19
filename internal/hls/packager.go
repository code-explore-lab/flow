package hls

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"flow/internal/fmp4"
	"flow/internal/media"
	"flow/internal/metrics"
	"flow/internal/source"
	"flow/internal/stream"
)

// TrackSource 一路轨的帧源与已知编码参数。
//
// Video/Audio 可以为 nil：视频参数可以等带内 SPS/PPS；音频没有带内参数，
// 因此音频必须提供 Audio（本地文件回退时由 media.GetAACInfo 构造）。
type TrackSource struct {
	Subscriber source.Subscriber
	Video      *stream.VideoCodec
	Audio      *stream.AudioCodec
}

// PackagerSpec 一路流的打包输入。Stream 仅用于 manager 判断同名流是否被替换；
// 本地文件回退时为 nil。
type PackagerSpec struct {
	Name   string
	Stream *stream.Stream
	Video  *TrackSource
	Audio  *TrackSource
}

// Options packager 参数。零值可用：segment 4s、part 0（经典）、window 6。
type Options struct {
	SegmentTarget time.Duration
	PartTarget    time.Duration
	Window        int
	MaxBytes      int64 // 每路流 HLS 内存预算；0 表示只按 Window 回收
	// O-6 DVR：DVRWindow > 0 时改为按时间保留窗口，并把段落盘，
	// 使播放器可以回看最近这段媒体。Window 在 DVR 模式下不参与回收。
	DVRWindow   time.Duration
	DVRDir      string // 磁盘环形缓冲根目录；空时用 "dvr"
	DVRMaxBytes int64  // 每路流 DVR 磁盘预算；0 表示只按 DVRWindow 回收
	Metrics     *metrics.Registry
	Now         func() time.Time
	Logf        func(format string, args ...any)
}

func (o Options) withDefaults() Options {
	if o.SegmentTarget <= 0 {
		o.SegmentTarget = 4 * time.Second
	}
	if o.Window <= 0 {
		o.Window = 6
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.DVRWindow > 0 && o.DVRDir == "" {
		o.DVRDir = "dvr"
	}
	return o
}

func (o Options) logf(format string, args ...any) {
	if o.Logf != nil {
		o.Logf(format, args...)
		return
	}
	log.Printf(format, args...)
}

// SpecFromStream 从共享注册表的一路流构造 PackagerSpec。
//
// 视频参数可以缺失（等带内 SPS/PPS）；音频没有带内参数，参数未就绪时不建音频轨，
// 由调用方决定是否等待后重试。
func SpecFromStream(st *stream.Stream) PackagerSpec {
	spec := PackagerSpec{Name: st.Name, Stream: st}
	if src := st.SourceFor("video"); src != nil {
		spec.Video = &TrackSource{Subscriber: src}
		if vp, ok := st.VideoParams(); ok {
			spec.Video.Video = vp
		}
	}
	if src := st.SourceFor("audio"); src != nil {
		if ap, ok := st.AudioParams(); ok {
			spec.Audio = &TrackSource{Subscriber: src, Audio: ap}
		}
	}
	return spec
}

// Packager 把一路流的两条轨打成 HLS；每路流只创建一个，观众共享。
type Packager struct {
	name    string
	stream  *stream.Stream
	opts    Options
	epoch   time.Time
	video   *Track
	audio   *Track
	dvrRoot string // 非空表示本实例的 DVR 目录，Close 时整体删除

	closeOnce sync.Once
}

// NewPackager 创建并启动一路流的打包器。至少需要一条受支持的轨。
func NewPackager(spec PackagerSpec, opts Options) (*Packager, error) {
	opts = opts.withDefaults()
	if strings.TrimSpace(spec.Name) == "" {
		return nil, errors.New("hls: packager name is empty")
	}
	p := &Packager{
		name: spec.Name, stream: spec.Stream, opts: opts, epoch: opts.Now(),
		dvrRoot: newDVDRoot(opts, spec.Name),
	}

	if spec.Video != nil {
		t, err := newVideoTrack(p, spec.Video)
		if err != nil {
			opts.logf("hls[%s]: skip video: %v", spec.Name, err)
		} else {
			p.video = t
		}
	}
	if spec.Audio != nil {
		t, err := newAudioTrack(p, spec.Audio)
		if err != nil {
			opts.logf("hls[%s]: skip audio: %v", spec.Name, err)
		} else {
			p.audio = t
		}
	}
	if p.video == nil && p.audio == nil {
		if p.dvrRoot != "" {
			_ = os.RemoveAll(p.dvrRoot)
		}
		return nil, fmt.Errorf("hls: %s has no supported track (need H.264 and/or AAC)", spec.Name)
	}
	// 字节预算按存储后端选择：DVR 用磁盘预算，否则用内存预算。
	// 预算覆盖整路流，两条轨平分，单轨拿全部。
	budget := opts.MaxBytes
	if opts.DVRWindow > 0 {
		budget = opts.DVRMaxBytes
	}
	if budget > 0 {
		perTrack := budget
		if p.video != nil && p.audio != nil {
			perTrack /= 2
		}
		if perTrack < 1 {
			perTrack = 1
		}
		if p.video != nil {
			p.video.maxBytes = perTrack
		}
		if p.audio != nil {
			p.audio.maxBytes = perTrack
		}
	}

	if p.video != nil {
		p.video.start()
	}
	if p.audio != nil {
		p.audio.start()
	}
	opts.logf("hls[%s]: packager started (video=%v audio=%v)", spec.Name, p.video != nil, p.audio != nil)
	return p, nil
}

func (p *Packager) Name() string           { return p.name }
func (p *Packager) Stream() *stream.Stream { return p.stream }

// Epoch 返回打包时钟原点。每条轨的 SegmentInfo.StartPTS 都是相对它的 ticks，
// P-3 DASH 用它作为 MPD 的 availabilityStartTime。
func (p *Packager) Epoch() time.Time { return p.epoch }
func (p *Packager) Video() *Track    { return p.video }
func (p *Packager) Audio() *Track    { return p.audio }

// Closed 报告所有轨是否都已经结束。
func (p *Packager) Closed() bool {
	if p.video != nil && !p.video.Closed() {
		return false
	}
	if p.audio != nil && !p.audio.Closed() {
		return false
	}
	return true
}

// Close 停止所有轨的源订阅并等待 goroutine 退出。幂等。
func (p *Packager) Close() {
	p.closeOnce.Do(func() {
		if p.video != nil {
			p.video.Close()
		}
		if p.audio != nil {
			p.audio.Close()
		}
		if p.dvrRoot != "" {
			if err := os.RemoveAll(p.dvrRoot); err != nil {
				p.opts.logf("hls[%s]: remove dvr dir: %v", p.name, err)
			}
		}
		p.opts.logf("hls[%s]: packager closed", p.name)
	})
}

// Snapshot playlist 生成所需的只读快照。
type Snapshot struct {
	Segments       []SegmentInfo
	Open           *OpenSegmentInfo
	PartTarget     time.Duration
	TargetDuration int
	Closed         bool
}

type storedSegment struct {
	info SegmentInfo
	size int64
}

// Track 一条轨：源订阅、AU 组装、切片、playlist 元数据与字节 store。
type Track struct {
	name      string
	kind      string
	prefix    string
	opts      Options
	epoch     time.Time
	timescale uint32
	video     bool
	seg       *Segmenter
	sub       source.Subscriber

	// 编码参数 / init（只由 rendtion goroutine 写）
	videoCfg               *fmp4.VideoConfig
	audioCfg               *fmp4.AudioConfig
	sps, pps               []byte
	pendingSPS, pendingPPS []byte
	awaitKey               bool
	initVersion            int

	// 时间轴：按 raw DTS 差值推进的合成 DTS，保证切片器看到的 DTS 连续。
	haveBase  bool
	trackBase int64
	nextDTS   int64
	haveFed   bool
	lastDur   uint32
	haveRaw   bool
	lastRaw   uint32
	unwrapped int64

	// 源 goroutine
	reader      *source.Reader[source.Frame]
	unsubscribe func()
	done        chan struct{}
	closeOnce   sync.Once
	wg          sync.WaitGroup

	// AU 组装与待结算样本
	au      auBuilder
	pending *auSample

	// playlist / store
	mu             sync.Mutex
	segments       []*storedSegment
	open           *OpenSegmentInfo
	openParts      []PartInfo
	store          blobStore
	partURIs       map[uint64][]string
	targetDuration int
	partTarget     time.Duration
	maxBytes       int64
	codec          string
	width, height  int
	closed         bool
	err            error
	notified       chan struct{}
	firstSegment   bool
	firstIDR       bool
	haveLastKey    bool
	lastKeyRaw     int64
	gopWarned      bool
}

type auBuilder struct {
	active  bool
	pts     uint32
	rawPTS  int64
	rawDTS  int64
	cts     int32
	arrival time.Time
	nalus   [][]byte
	key     bool
	hasVCL  bool
}

type auSample struct {
	data    []byte
	rawPTS  int64
	rawDTS  int64
	cts     int32
	key     bool
	arrival time.Time
}

func newVideoTrack(p *Packager, ts *TrackSource) (*Track, error) {
	if ts.Subscriber == nil {
		return nil, errors.New("video source is nil")
	}
	if ts.Video != nil && ts.Video.Codec != "" && !strings.EqualFold(ts.Video.Codec, "H264") {
		return nil, fmt.Errorf("unsupported video codec %q", ts.Video.Codec)
	}
	seg, err := NewSegmenter(Config{
		TrackID:       1,
		Timescale:     90000,
		SegmentTarget: p.opts.SegmentTarget,
		PartTarget:    p.opts.PartTarget,
		Video:         true,
	})
	if err != nil {
		return nil, err
	}
	t := &Track{
		name: p.name, kind: "video", prefix: "video/", opts: p.opts, epoch: p.epoch,
		timescale: 90000, video: true, seg: seg, sub: ts.Subscriber,
		done: make(chan struct{}), store: newTrackStore(p.opts, p.dvrRoot, "video", p.name),
		partURIs: map[uint64][]string{}, notified: make(chan struct{}),
		targetDuration: seg.TargetDuration(), partTarget: seg.PartTarget(),
	}
	if ts.Video != nil && len(ts.Video.SPS) > 0 && len(ts.Video.PPS) > 0 {
		if err := t.applyVideoParams(ts.Video.SPS, ts.Video.PPS, 0); err != nil {
			// 注册表参数不可用时不要直接放弃视频轨；带内 SPS/PPS 可能随后到达。
			t.logf("registry SPS/PPS unusable: %v (waiting for in-band parameters)", err)
		}
	}
	return t, nil
}

func newAudioTrack(p *Packager, ts *TrackSource) (*Track, error) {
	if ts.Subscriber == nil {
		return nil, errors.New("audio source is nil")
	}
	a := ts.Audio
	if a == nil {
		return nil, errors.New("audio parameters are missing")
	}
	if codec := stream.NormalizeAudioCodec(a.Codec); codec != stream.AudioAAC {
		return nil, fmt.Errorf("unsupported audio codec %q (only AAC)", a.Codec)
	}
	if len(a.Config) < 2 {
		return nil, errors.New("AAC AudioSpecificConfig too short")
	}
	rate, ch, err := media.ParseASC(a.Config)
	if err != nil {
		return nil, fmt.Errorf("AAC ASC: %w", err)
	}
	sampleRate := a.SampleRate
	if sampleRate == 0 {
		sampleRate = uint32(rate)
	}
	channels := a.Channels
	if channels == 0 {
		channels = ch
	}
	clock := a.ClockRate
	if clock == 0 {
		clock = sampleRate
	}
	if clock == 0 {
		clock = 44100
	}
	aot := a.Config[0] >> 3
	if aot == 0 || aot == 31 {
		return nil, fmt.Errorf("unsupported AAC audio object type %d", aot)
	}
	cfg := &fmp4.AudioConfig{
		Timescale:  clock,
		ASC:        append([]byte(nil), a.Config...),
		SampleRate: sampleRate,
		Channels:   channels,
	}
	init, err := fmp4.Init(fmp4.Config{Audio: cfg})
	if err != nil {
		return nil, fmt.Errorf("audio init: %w", err)
	}
	seg, err := NewSegmenter(Config{
		TrackID:       2,
		Timescale:     clock,
		SegmentTarget: p.opts.SegmentTarget,
		PartTarget:    p.opts.PartTarget,
		Video:         false,
	})
	if err != nil {
		return nil, err
	}
	t := &Track{
		name: p.name, kind: "audio", prefix: "audio/", opts: p.opts, epoch: p.epoch,
		timescale: clock, video: false, seg: seg, sub: ts.Subscriber, audioCfg: cfg,
		done: make(chan struct{}), store: newTrackStore(p.opts, p.dvrRoot, "audio", p.name),
		partURIs: map[uint64][]string{}, notified: make(chan struct{}),
		targetDuration: seg.TargetDuration(), partTarget: seg.PartTarget(),
		codec: fmt.Sprintf("mp4a.40.%d", aot),
	}
	t.storePut(t.mapURI(0), init)
	return t, nil
}

func (t *Track) start() {
	t.reader, t.unsubscribe = t.subscribe()
	if t.reader == nil {
		t.fail(errors.New("source subscribe returned nil reader"))
		return
	}
	t.wg.Add(1)
	go t.run()
}

func (t *Track) subscribe() (*source.Reader[source.Frame], func()) {
	if live, ok := t.sub.(source.LiveSubscriber); ok {
		return live.SubscribeLive()
	}
	return t.sub.Subscribe()
}

func (t *Track) run() {
	defer t.wg.Done()
	for {
		f, ok := t.reader.Next(t.done)
		if !ok {
			break
		}
		if t.video {
			t.handleVideo(f)
		} else {
			t.handleAudio(f)
		}
	}
	t.finish()
}

// Close 停止订阅并等待 goroutine。幂等。
func (t *Track) Close() {
	t.closeOnce.Do(func() {
		close(t.done)
		if t.unsubscribe != nil {
			t.unsubscribe()
		}
	})
	t.wg.Wait()
	t.mu.Lock()
	t.closed = true
	t.open = nil
	t.openParts = nil
	if t.store != nil {
		t.store.Close()
	}
	t.notifyLocked()
	t.mu.Unlock()
}

// ---- 视频 AU 组装 ----

func (t *Track) handleVideo(f source.Frame) {
	if len(f.Data) == 0 {
		return
	}
	class := media.ClassifyVideoNAL("H264", f.Data)
	if class.IsSPS || class.IsPPS {
		if class.IsSPS {
			t.pendingSPS = append([]byte(nil), f.Data...)
		}
		if class.IsPPS {
			t.pendingPPS = append([]byte(nil), f.Data...)
		}
		return
	}
	typ, err := media.NALType(f.Data)
	if err != nil {
		return
	}
	if typ == 9 { // AUD；打包时统一补一个
		return
	}

	newAU := f.AUStart || !t.au.active || (class.IsVCL && f.Timestamp != t.au.pts)
	if newAU {
		t.beginAU(f, class)
	} else {
		t.appendNALU(f.Data, class)
	}
	if f.AUEnd {
		t.finishAU()
	}
}

func (t *Track) beginAU(f source.Frame, class media.VideoNALClass) {
	t.finishAU()
	t.maybeApplyParams()
	t.startAU(f)
	t.appendNALU(f.Data, class)
}

func (t *Track) startAU(f source.Frame) {
	rawPTS := t.unwrap(f.Timestamp)
	rawDTS := rawPTS - int64(f.CompositionTime)
	t.feedPrevPending(rawDTS)
	t.au = auBuilder{
		active:  true,
		pts:     f.Timestamp,
		rawPTS:  rawPTS,
		rawDTS:  rawDTS,
		cts:     f.CompositionTime,
		arrival: t.opts.Now(),
	}
}

func (t *Track) appendNALU(nalu []byte, class media.VideoNALClass) {
	if class.IsIDR {
		t.au.key = true
	}
	if class.IsVCL {
		t.au.hasVCL = true
	}
	t.au.nalus = append(t.au.nalus, nalu)
}

func (t *Track) finishAU() {
	if !t.au.active {
		return
	}
	au := t.au
	t.au = auBuilder{}
	if !au.hasVCL {
		return
	}
	data := buildVideoSample(au.nalus)
	if len(data) == 0 {
		return
	}
	smp := &auSample{
		data: data, rawPTS: au.rawPTS, rawDTS: au.rawDTS,
		cts: au.cts, key: au.key, arrival: au.arrival,
	}
	// 正常流程 pending 应已在上一个 AU 开始时结算；保险起见先冲刷。
	if t.pending != nil {
		t.feedPending(t.pending, int64(t.fallbackDuration()))
	}
	t.pending = smp
}

func buildVideoSample(nalus [][]byte) []byte {
	out := []byte{0, 0, 0, 2, 0x09, 0xF0} // AUD
	for _, nalu := range nalus {
		if len(nalu) == 0 {
			continue
		}
		typ, err := media.NALType(nalu)
		if err == nil && (typ == 7 || typ == 8 || typ == 9) {
			continue
		}
		out = append(out, byte(len(nalu)>>24), byte(len(nalu)>>16), byte(len(nalu)>>8), byte(len(nalu)))
		out = append(out, nalu...)
	}
	if len(out) == 6 { // 只有 AUD
		return nil
	}
	return out
}

func (t *Track) maybeApplyParams() {
	if len(t.pendingSPS) == 0 || len(t.pendingPPS) == 0 {
		return
	}
	if t.videoCfg != nil && bytes.Equal(t.pendingSPS, t.sps) && bytes.Equal(t.pendingPPS, t.pps) {
		return
	}
	if t.videoCfg == nil {
		if err := t.applyVideoParams(t.pendingSPS, t.pendingPPS, 0); err != nil {
			t.logf("apply initial SPS/PPS: %v", err)
		}
		return
	}
	// 参数集变化：先把旧参数下的帧结算掉，再切 discontinuity + 新 init。
	t.finishAU()
	if t.pending != nil {
		t.feedPending(t.pending, int64(t.fallbackDuration()))
		t.pending = nil
	}
	newVersion := t.mapVersion() + 1
	if ev, err := t.seg.MarkDiscontinuity(newVersion); err == nil {
		t.handleEvents(ev)
	}
	if err := t.applyVideoParams(t.pendingSPS, t.pendingPPS, newVersion); err != nil {
		t.logf("apply new SPS/PPS: %v", err)
		return
	}
	t.awaitKey = true
	t.logf("SPS/PPS changed, init version %d", newVersion)
}

func (t *Track) applyVideoParams(sps, pps []byte, version int) error {
	spsCopy := append([]byte(nil), sps...)
	ppsCopy := append([]byte(nil), pps...)
	info, parseErr := media.ParseSPS(spsCopy)
	width, height, codec := 0, 0, ""
	if parseErr == nil {
		width, height, codec = info.Width, info.Height, info.CodecString()
	} else {
		t.logf("parse SPS: %v (init will omit resolution/codecs)", parseErr)
	}
	cfg := &fmp4.VideoConfig{Timescale: 90000, SPS: spsCopy, PPS: ppsCopy, Width: width, Height: height}
	init, err := fmp4.Init(fmp4.Config{Video: cfg})
	if err != nil {
		return err
	}
	t.sps = spsCopy
	t.pps = ppsCopy
	t.pendingSPS = spsCopy
	t.pendingPPS = ppsCopy
	t.videoCfg = cfg
	t.initVersion = version
	t.mu.Lock()
	t.storePut(t.mapURI(version), init)
	t.codec = codec
	t.width, t.height = width, height
	t.notifyLocked()
	t.mu.Unlock()
	return nil
}

// mapVersion 是当前 init 版本（只由 rendition goroutine 读写）。
func (t *Track) mapVersion() int {
	// 从已存的 init URI 推导版本效率不高，直接维护一个字段更清晰。
	return t.initVersion
}

// ---- 音频 ----

func (t *Track) handleAudio(f source.Frame) {
	if len(f.Data) == 0 || t.audioCfg == nil {
		return
	}
	rawPTS := t.unwrap(f.Timestamp)
	t.feedPrevPending(rawPTS)
	t.pending = &auSample{
		data:    append([]byte(nil), f.Data...),
		rawPTS:  rawPTS,
		rawDTS:  rawPTS,
		key:     true,
		arrival: t.opts.Now(),
	}
}

// ---- 时间轴 / 样本结算 ----

func (t *Track) feedPrevPending(rawDTS int64) {
	if t.pending == nil {
		return
	}
	dur := rawDTS - t.pending.rawDTS
	t.feedPending(t.pending, dur)
	t.pending = nil
}

func (t *Track) feedPending(smp *auSample, dur int64) {
	if smp == nil || len(smp.data) == 0 {
		return
	}
	if t.video && t.videoCfg == nil {
		return
	}
	if !t.video && t.audioCfg == nil {
		return
	}
	// 首个 IDR / discontinuity 之后的下一个 IDR 之前，视频非关键帧无法独立解码。
	if t.video && (t.awaitKey || !t.haveBase) && !smp.key {
		return
	}
	if t.video && t.awaitKey && smp.key {
		t.awaitKey = false
	}

	if !t.haveBase {
		t.haveBase = true
		d := smp.arrival.Sub(t.epoch)
		if d < 0 {
			d = 0
		}
		t.trackBase = int64(d) * int64(t.timescale) / int64(time.Second)
		t.nextDTS = t.trackBase - int64(smp.cts)
		t.logf("first sample: trackBase=%d ticks (%s)", t.trackBase, d)
	}

	if t.haveFed && (dur <= 0 || dur > 5*int64(t.timescale)) {
		reason := "timestamp jump"
		if dur <= 0 {
			reason = "backward timestamp"
		}
		t.discontinuity(reason)
		if t.video && !smp.key {
			return
		}
		if t.video {
			t.awaitKey = false
		}
		dur = int64(t.fallbackDuration())
	}
	if dur <= 0 || dur > math.MaxUint32 {
		dur = int64(t.fallbackDuration())
	}

	if t.video && smp.key {
		if !t.firstIDR {
			t.firstIDR = true
			t.logf("first IDR: rawDTS=%d", smp.rawDTS)
		}
		if t.haveLastKey && !t.gopWarned {
			ticks := smp.rawDTS - t.lastKeyRaw
			if ticks > 0 {
				gop := time.Duration(ticks) * time.Second / time.Duration(t.timescale)
				if gop > 2*t.opts.SegmentTarget {
					t.gopWarned = true
					t.logf("GOP %s is larger than 2x segment target %s; LL-HLS join latency will suffer",
						gop, t.opts.SegmentTarget)
				}
			}
		}
		t.lastKeyRaw = smp.rawDTS
		t.haveLastKey = true
	}

	duration := uint32(dur)
	ev, err := t.seg.Add(Sample{
		Data:     smp.data,
		DTS:      t.nextDTS,
		Duration: duration,
		CTS:      smp.cts,
		Keyframe: smp.key,
	})
	if err != nil {
		t.fail(fmt.Errorf("segment add: %w", err))
		return
	}
	t.nextDTS += int64(duration)
	t.lastDur = duration
	t.haveFed = true
	t.handleEvents(ev)
}

func (t *Track) fallbackDuration() uint32 {
	if t.lastDur > 0 {
		return t.lastDur
	}
	if t.video {
		return 3600 // 25fps
	}
	return 1024
}

func (t *Track) discontinuity(reason string) {
	if ev, err := t.seg.MarkDiscontinuity(t.mapVersion()); err == nil {
		t.handleEvents(ev)
	} else {
		t.logf("mark discontinuity: %v", err)
	}
	t.awaitKey = t.video
	t.logf("discontinuity: %s", reason)
}

func (t *Track) unwrap(raw uint32) int64 {
	if !t.haveRaw {
		t.haveRaw = true
		t.lastRaw = raw
		t.unwrapped = int64(raw)
		return t.unwrapped
	}
	delta := int32(raw - t.lastRaw)
	t.lastRaw = raw
	t.unwrapped += int64(delta)
	return t.unwrapped
}

func (t *Track) finish() {
	t.finishAU()
	if t.pending != nil {
		t.feedPending(t.pending, int64(t.fallbackDuration()))
		t.pending = nil
	}
	if ev, err := t.seg.Finish(); err == nil {
		t.handleEvents(ev)
	} else {
		t.fail(err)
	}
	t.mu.Lock()
	t.closed = true
	t.open = nil
	t.openParts = nil
	t.notifyLocked()
	t.mu.Unlock()
	t.logf("track ended")
}

func (t *Track) fail(err error) {
	t.mu.Lock()
	if t.err == nil {
		t.err = err
	}
	t.closed = true
	t.open = nil
	t.openParts = nil
	t.notifyLocked()
	t.mu.Unlock()
	t.logf("error: %v", err)
}

// ---- 事件 / store ----

func (t *Track) handleEvents(events []Event) {
	t.mu.Lock()
	for _, ev := range events {
		switch ev.Kind {
		case EventPart:
			p := ev.Part
			info := PartInfo{
				MSN:         p.MSN,
				Index:       p.Index,
				Duration:    p.Duration.Seconds(),
				Independent: p.Independent,
				URI:         t.partURI(p.MSN, p.Index),
			}
			t.storePut(info.URI, p.Data)
			t.partURIs[p.MSN] = append(t.partURIs[p.MSN], info.URI)
			t.opts.Metrics.IncHLSPart(t.kind)
			t.syncOpenLocked()
			t.openParts = append(t.openParts, info)
		case EventSegment:
			s := ev.Segment
			info := SegmentInfo{
				MSN:           s.MSN,
				DiscSeq:       s.DiscSeq,
				Discontinuity: s.Discontinuity,
				MapURI:        t.mapURI(s.MapVersion),
				Duration:      s.Duration.Seconds(),
				DurationTicks: s.DurationTicks,
				PDT:           t.pdt(s.StartPTS),
				URI:           t.segmentURI(s.MSN),
				StartDTS:      s.StartDTS,
				StartPTS:      s.StartPTS,
			}
			t.storePut(info.URI, s.Data)
			t.segments = append(t.segments, &storedSegment{info: info, size: int64(len(s.Data))})
			t.opts.Metrics.IncHLSSegment(t.kind)
			t.open = nil
			t.openParts = nil
			// 已关闭父段的 part 不会再出现在 playlist 里；保留刚结束的这一段，
			// 给 late part 请求留一个段的余量，更早的立即删除（DVR 下避免
			// part 和父段各占一份磁盘）。
			for msn, uris := range t.partURIs {
				if msn >= s.MSN {
					continue
				}
				for _, uri := range uris {
					t.storeDelete(uri)
				}
				delete(t.partURIs, msn)
			}
			t.evictLocked()
			if !t.firstSegment {
				t.firstSegment = true
				t.logf("first segment: msn=%d dur=%.3fs parts=%d", s.MSN, s.Duration.Seconds(), s.PartCount)
			}
		case EventEnd:
			t.closed = true
			t.open = nil
			t.openParts = nil
		}
	}
	t.targetDuration = t.seg.TargetDuration()
	t.partTarget = t.seg.PartTarget()
	t.syncOpenLocked()
	t.notifyLocked()
	t.mu.Unlock()
}

// syncOpenLocked 用切片器当前打开的段刷新 playlist 视角的 Open；调用方必须持有 t.mu。
func (t *Track) syncOpenLocked() {
	o, ok := t.seg.Open()
	if !ok {
		t.open = nil
		t.openParts = nil
		return
	}
	if t.open == nil || t.open.MSN != o.MSN {
		t.open = &OpenSegmentInfo{
			MSN:           o.MSN,
			DiscSeq:       o.DiscSeq,
			Discontinuity: o.Discontinuity,
			MapURI:        t.mapURI(o.MapVersion),
			PDT:           t.pdt(o.StartPTS),
		}
		t.openParts = nil
	}
	if t.partTarget > 0 {
		t.open.NextPartURI = t.partURI(o.MSN, o.PartIndex)
	} else {
		t.open.NextPartURI = ""
	}
}

func (t *Track) evictLocked() {
	for len(t.segments) > 1 {
		over := false
		if t.opts.DVRWindow > 0 {
			// DVR：按时间窗口保留，Window（段数）不再参与回收。
			var total float64
			for _, s := range t.segments {
				total += s.info.Duration
			}
			over = total > t.opts.DVRWindow.Seconds()
		} else {
			over = len(t.segments) > t.opts.Window
		}
		if !over && t.maxBytes > 0 && t.store.Bytes() > t.maxBytes {
			over = true
		}
		if !over {
			return
		}
		old := t.segments[0]
		t.segments = t.segments[1:]
		t.storeDelete(old.info.URI)
		for _, uri := range t.partURIs[old.info.MSN] {
			t.storeDelete(uri)
		}
		delete(t.partURIs, old.info.MSN)
	}
}

// storePut / storeDelete 是所有片段字节的统一入口；错误只记日志，
// 不打断切片（内存后端不会失败，磁盘后端失败时播放列表会自然跳过该段）。
func (t *Track) storePut(uri string, data []byte) {
	if t.store == nil {
		return
	}
	if err := t.store.Put(uri, data); err != nil {
		t.logf("store %s: %v", uri, err)
	}
}

func (t *Track) storeDelete(uri string) {
	if t.store != nil {
		t.store.Delete(uri)
	}
}

func (t *Track) notifyLocked() {
	close(t.notified)
	t.notified = make(chan struct{})
}

func (t *Track) segmentURI(msn uint64) string {
	return fmt.Sprintf("%sseg-%d.m4s", t.prefix, msn)
}

func (t *Track) partURI(msn uint64, index int) string {
	return fmt.Sprintf("%spart-%d-%d.m4s", t.prefix, msn, index)
}

func (t *Track) mapURI(version int) string {
	if version == 0 {
		return t.prefix + "init.mp4"
	}
	return fmt.Sprintf("%sinit-%d.mp4", t.prefix, version)
}

func (t *Track) pdt(pts int64) time.Time {
	return t.epoch.Add(ticksToDuration(pts, t.timescale))
}

// ---- 读接口 ----

func (t *Track) Kind() string      { return t.kind }
func (t *Track) Timescale() uint32 { return t.timescale }

func (t *Track) Codec() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.codec
}

func (t *Track) Size() (int, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.width, t.height
}

// Channels 返回音频轨的声道数；视频轨返回 0。
func (t *Track) Channels() uint8 {
	if t.audioCfg == nil {
		return 0
	}
	return t.audioCfg.Channels
}

func (t *Track) Closed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

func (t *Track) Watch() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.notified
}

func (t *Track) Data(uri string) ([]byte, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.store == nil {
		return nil, false
	}
	return t.store.Get(uri)
}

func (t *Track) Snapshot() Snapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	snap := Snapshot{
		PartTarget:     t.partTarget,
		TargetDuration: t.targetDuration,
		Closed:         t.closed,
	}
	for _, s := range t.segments {
		snap.Segments = append(snap.Segments, s.info)
	}
	if t.open != nil {
		cp := *t.open
		cp.Parts = append([]PartInfo(nil), t.openParts...)
		snap.Open = &cp
	}
	return snap
}

// Progress 返回另一条 rendition 写 EXT-X-RENDITION-REPORT 所需的进度。
func (t *Track) Progress() (msn uint64, part int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.open != nil {
		if len(t.openParts) > 0 {
			return t.open.MSN, t.openParts[len(t.openParts)-1].Index
		}
		return t.open.MSN, -1
	}
	if len(t.segments) > 0 {
		return t.segments[len(t.segments)-1].info.MSN, -1
	}
	return 0, -1
}

// StoredBytes 返回该轨当前 store 里的字节数（内存或 DVR 磁盘）。
func (t *Track) StoredBytes() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.store == nil {
		return 0
	}
	return t.store.Bytes()
}

// Bandwidth 用窗口内完整段估算码率（bit/s）。
func (t *Track) Bandwidth() uint32 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var bytes, dur float64
	for _, s := range t.segments {
		bytes += float64(s.size)
		dur += s.info.Duration
	}
	if dur <= 0 {
		return 0
	}
	return uint32(bytes * 8 / dur)
}

func (t *Track) logf(format string, args ...any) {
	t.opts.logf("hls[%s/%s]: "+format, append([]any{t.name, t.kind}, args...)...)
}

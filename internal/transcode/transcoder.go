package transcode

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"time"

	"flow/internal/media"
	"flow/internal/mpegts"
	"flow/internal/source"
	"flow/internal/stream"
)

const (
	videoClockRate = 90000
	readBufSize    = 188 * 64
	stderrLimit    = 8 << 10

	// 输入 PTS → 喂入时刻的映射最多保留这么多项，避免长跑时无限增长。
	maxLatencyEntries = 2048
)

var (
	// errParentEnded 表示上游流已经结束，不需要重启 ffmpeg。
	errParentEnded = errors.New("transcode: parent stream ended")
	// errReaderDropped 表示转码消费太慢，帧源已经覆盖了未读的帧。
	// 解码器此时已经丢了参考帧，只能重新订阅并等下一个 GOP。
	errReaderDropped = errors.New("transcode: source reader dropped frames")
)

// Transcoder 一路「源流 → ffmpeg → 新流」的转码器。
//
// 生命周期：NewTranscoder 创建并立即把输出流登记到注册表，Start 启动
// 后台循环。上游结束、注册表替换或 Close 时，输出流关闭并从注册表摘除。
type Transcoder struct {
	profile Profile
	parent  *stream.Stream
	reg     *stream.Registry
	opts    Options

	derived *stream.Stream
	vSrc    *source.ExternalSource

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	closeOnce sync.Once

	// 输出时间轴跨 ffmpeg 重启保持单调：重启后 ffmpeg 的 PTS 从头开始，
	// 这里把它接到上一次输出的时间之后，并按墙钟补上重启期间的空洞。
	tlMu    sync.Mutex
	haveOut bool
	lastTS  uint32
	lastAt  time.Time

	// 输入 PTS → 喂入时刻，用于统计转码流水线延迟。
	feedMu    sync.Mutex
	feedAt    map[uint32]time.Time
	feedOrder []uint32
}

// NewTranscoder 创建转码器并发布输出流。调用方负责随后调用 Start。
func NewTranscoder(ctx context.Context, reg *stream.Registry, parent *stream.Stream, p Profile, opts Options) (*Transcoder, error) {
	if reg == nil || parent == nil {
		return nil, errors.New("transcode: nil registry or parent stream")
	}
	opts = opts.withDefaults()
	t := &Transcoder{
		profile: p,
		parent:  parent,
		reg:     reg,
		opts:    opts,
		done:    make(chan struct{}),
		feedAt:  make(map[uint32]time.Time),
	}

	derived := &stream.Stream{}
	vt := derived.EnsureVideo()
	vt.Video.Codec = p.Codec
	vt.Src.SetCodec(p.Codec)
	derived.SetIdleExempt(true)
	t.derived = derived
	t.vSrc = vt.Src

	published, err := reg.Publish(p.Output, derived)
	if err != nil {
		derived.Close()
		return nil, err
	}
	if published != derived {
		derived.Close()
		return nil, fmt.Errorf("transcode: output %q is already owned by another stream", p.Output)
	}
	t.ctx, t.cancel = context.WithCancel(ctx)
	return t, nil
}

// Start 启动转码循环。只应调用一次。
func (t *Transcoder) Start() {
	go t.run()
}

// Close 停止转码并等待收尾。可重复调用；必须在 Start 之后调用。
func (t *Transcoder) Close() {
	t.closeOnce.Do(t.cancel)
	<-t.done
}

func (t *Transcoder) logf(format string, args ...any) {
	t.opts.Logf("transcode[%s]: %s", t.profile.Label(), fmt.Sprintf(format, args...))
}

func (t *Transcoder) run() {
	defer close(t.done)
	if m := t.opts.Metrics; m != nil {
		m.IncTranscodeStream()
		defer m.DecTranscodeStream()
	}
	t.logf("start: %dx%d %s %d bps", t.profile.Width, t.profile.Height, t.profile.Codec, t.profile.Bitrate)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.audioLoop()
	}()

	backoff := initialRestartBackoff
	for {
		started := t.opts.Now()
		err := t.videoLoop(t.ctx)
		if t.ctx.Err() != nil || errors.Is(err, errParentEnded) {
			break
		}
		if err != nil {
			t.logf("video loop: %v", err)
		}
		if m := t.opts.Metrics; m != nil {
			m.IncTranscodeRestart()
		}
		if t.opts.Now().Sub(started) > 10*time.Second {
			backoff = initialRestartBackoff
		}
		select {
		case <-t.ctx.Done():
			goto shutdown
		case <-time.After(backoff):
		}
		if backoff < t.opts.MaxRestartBackoff {
			backoff *= 2
			if backoff > t.opts.MaxRestartBackoff {
				backoff = t.opts.MaxRestartBackoff
			}
		}
	}

shutdown:
	t.cancel()
	t.derived.Close()
	t.reg.Unpublish(t.profile.Output, t.derived)
	wg.Wait()
	t.logf("stopped")
}

// ---- 视频：订阅源 → 封 TS → ffmpeg → 解 TS → 写输出源 ----

type auBuilder struct {
	active bool
	pts    uint32
	rawPTS int64
	rawDTS int64
	cts    int32
	nalus  [][]byte
	key    bool
	hasVCL bool
}

func (t *Transcoder) videoLoop(ctx context.Context) error {
	src := t.parent.SourceFor("video")
	if src == nil {
		return errParentEnded
	}
	codec := src.Codec()
	if vp, ok := t.parent.VideoParams(); ok && vp.Codec != "" {
		codec = vp.Codec
	}
	if !media.IsHEVCCodec(codec) {
		codec = "H264"
	}

	rd, unsubscribe := src.Subscribe()
	defer unsubscribe()

	var (
		acc      auBuilder
		unwrap   tsUnwrapper
		baseDTS  int64
		baseSet  bool
		vps, sps []byte
		pps      []byte
		proc     *process
		paramFP  []byte
		dropped  uint64
		fed      uint64
	)
	defer func() {
		if proc != nil {
			proc.stop()
		}
	}()

	if vp, ok := t.parent.VideoParams(); ok && len(vp.SPS) > 0 && len(vp.PPS) > 0 {
		vps = append([]byte(nil), vp.VPS...)
		sps = append([]byte(nil), vp.SPS...)
		pps = append([]byte(nil), vp.PPS...)
	}
	haveParams := func() bool { return len(sps) > 0 && len(pps) > 0 }

	finishAU := func() error {
		if !acc.active {
			return nil
		}
		a := acc
		acc = auBuilder{}
		if !a.hasVCL || len(a.nalus) == 0 {
			return nil
		}

		if proc == nil {
			// 等参数集和第一个关键帧；之前的数据喂给 ffmpeg 也解不出来。
			if !haveParams() || !a.key {
				return nil
			}
			p, err := t.startProcess(codec, vps, sps, pps)
			if err != nil {
				return err
			}
			proc = p
			paramFP = paramFingerprint(vps, sps, pps)
		} else if proc.exited() {
			return fmt.Errorf("ffmpeg exited: %v %s", proc.exitErr(), proc.stderr.String())
		} else if fp := paramFingerprint(vps, sps, pps); !bytes.Equal(fp, paramFP) {
			// 参数集变化（分辨率/编码切换）：更新封装器，下一帧关键帧
			// 会自动带上新的 SPS/PPS。
			proc.mux.SetVideoParamsFor(codec, vps, sps, pps)
			paramFP = fp
			t.logf("video parameters changed")
		}

		if !baseSet {
			baseDTS = a.rawDTS
			baseSet = true
		}
		pts := a.rawPTS - baseDTS
		dts := a.rawDTS - baseDTS
		if pts < 0 {
			pts = 0
		}
		if dts < 0 {
			dts = 0
		}
		if err := proc.mux.WriteVideo(a.nalus, uint32(pts), uint32(dts)); err != nil {
			return fmt.Errorf("feed ffmpeg: %w", err)
		}
		t.recordFeed(uint32(pts), t.opts.Now())
		fed++
		if m := t.opts.Metrics; m != nil {
			m.AddTranscodeInputFrames(1)
		}
		return nil
	}

	for {
		f, ok := rd.Next(ctx.Done())
		if !ok {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return errParentEnded
		}
		if d := rd.Dropped(); d != dropped {
			if m := t.opts.Metrics; m != nil {
				m.AddTranscodeDroppedFrames(d - dropped)
			}
			dropped = d
			if proc != nil {
				return errReaderDropped
			}
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
			if proc != nil {
				if fp := paramFingerprint(vps, sps, pps); !bytes.Equal(fp, paramFP) {
					proc.mux.SetVideoParamsFor(codec, vps, sps, pps)
					paramFP = fp
				}
			}
			continue
		}
		if isAUD(codec, f.Data) {
			continue
		}

		rawPTS := unwrap.unwrap(f.Timestamp)
		rawDTS := rawPTS - int64(f.CompositionTime)
		newAU := f.AUStart || !acc.active || (class.IsVCL && f.Timestamp != acc.pts)
		if newAU {
			if err := finishAU(); err != nil {
				return err
			}
			acc = auBuilder{
				active: true,
				pts:    f.Timestamp,
				rawPTS: rawPTS,
				rawDTS: rawDTS,
				cts:    f.CompositionTime,
			}
		}
		if class.IsIDR {
			acc.key = true
		}
		if class.IsVCL {
			acc.hasVCL = true
		}
		acc.nalus = append(acc.nalus, f.Data)
		if f.AUEnd {
			if err := finishAU(); err != nil {
				return err
			}
		}
	}
}

// ---- 音频：原样复制，不解码 ----

func (t *Transcoder) audioLoop() {
	for {
		if t.ctx.Err() != nil {
			return
		}
		src := t.parent.SourceFor("audio")
		if src == nil {
			// RTMP 音轨可能晚于视频几百毫秒，这里等它出现。
			select {
			case <-t.ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		rd, unsubscribe := src.Subscribe()
		t.copyAudio(rd)
		unsubscribe()
		return
	}
}

func (t *Transcoder) copyAudio(rd *source.Reader[source.Frame]) {
	var (
		ready  bool
		asrc   *source.ExternalSource
		lastFP []byte
	)
	ensure := func() *source.ExternalSource {
		p, ok := t.parent.AudioParams()
		if !ok {
			return asrc
		}
		fp := audioFingerprint(p)
		if ready && bytes.Equal(fp, lastFP) {
			return asrc
		}
		t.derived.EnsureAudio(p.SampleRate)
		t.derived.SetAudioCodec(p.Codec, p.Config, p.SampleRate, p.Channels, p.ClockRate)
		asrc = t.derived.SourceFor("audio")
		lastFP = fp
		ready = true
		return asrc
	}
	asrc = ensure()

	for {
		f, ok := rd.Next(t.ctx.Done())
		if !ok {
			return
		}
		if asrc == nil {
			asrc = ensure()
			if asrc == nil {
				continue // 参数还没到，丢掉这几帧
			}
		}
		asrc.Write(source.Frame{
			Data:      append([]byte(nil), f.Data...),
			Timestamp: f.Timestamp,
			Keyframe:  true,
		})
	}
}

// ---- ffmpeg 子进程 ----

type process struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	mux    *mpegts.Muxer
	stderr *boundedBuffer

	done chan struct{}
	mu   sync.Mutex
	err  error
}

func (t *Transcoder) startProcess(codec string, vps, sps, pps []byte) (*process, error) {
	cmd := exec.Command(t.opts.FFmpegPath, buildArgs(t.profile, t.opts)...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr := newBoundedBuffer(stderrLimit)
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", t.opts.FFmpegPath, err)
	}
	p := &process{
		cmd:    cmd,
		stdin:  stdin,
		mux:    mpegts.NewMuxer(stdin),
		stderr: stderr,
		done:   make(chan struct{}),
	}
	p.mux.SetVideoParamsFor(codec, vps, sps, pps)
	t.logf("ffmpeg started (pid %d)", cmd.Process.Pid)

	// Wait 必须在 stdout 读完之后调用，否则会提前关闭管道丢掉尾部数据。
	go func() {
		t.readOutput(stdout, codec)
		err := cmd.Wait()
		p.mu.Lock()
		p.err = err
		p.mu.Unlock()
		if err != nil {
			// 没有后续输入时 videoLoop 可能不会立刻察觉进程退出，
			// 这里主动把 stderr 打出来，方便定位启动参数/编码器问题。
			t.logf("ffmpeg exited: %v %s", err, stderr.String())
		}
		close(p.done)
	}()
	return p, nil
}

func (p *process) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *process) exitErr() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *process) stop() {
	p.stdin.Close()
	select {
	case <-p.done:
	case <-time.After(2 * time.Second):
		if p.cmd.Process != nil {
			p.cmd.Process.Kill()
		}
		<-p.done
	}
}

func (t *Transcoder) readOutput(stdout io.Reader, codec string) {
	d := mpegts.NewDemuxer()
	d.OnError = func(err error) { t.logf("demux: %v", err) }

	var (
		runBase     int64
		baseSet     bool
		vps, sps    []byte
		pps         []byte
		paramFP     []byte
		latencyHit  uint64
		latencyMiss uint64
	)

	d.OnVideo = func(au mpegts.VideoAU) {
		now := t.opts.Now()
		if !baseSet {
			t.tlMu.Lock()
			if t.haveOut {
				gap := now.Sub(t.lastAt)
				if gap < 0 {
					gap = 0
				}
				runBase = int64(t.lastTS) + int64(gap.Seconds()*float64(videoClockRate))
			}
			t.tlMu.Unlock()
			baseSet = true
		}

		// 参数集：输出流的 SPS/PPS 可能每个关键帧都带，只在变化时更新。
		for _, nalu := range au.NALUs {
			class := media.ClassifyVideoNAL(codec, nalu)
			switch {
			case class.IsVPS:
				vps = append([]byte(nil), nalu...)
			case class.IsSPS:
				sps = append([]byte(nil), nalu...)
			case class.IsPPS:
				pps = append([]byte(nil), nalu...)
			}
		}
		if len(sps) > 0 && len(pps) > 0 {
			if fp := paramFingerprint(vps, sps, pps); !bytes.Equal(fp, paramFP) {
				t.derived.SetVideoParams(codec, vps, sps, pps)
				paramFP = fp
			}
		}

		var nalus [][]byte
		var size int
		for _, nalu := range au.NALUs {
			if isAUD(codec, nalu) {
				continue
			}
			nalus = append(nalus, nalu)
			size += len(nalu)
		}
		if len(nalus) == 0 {
			return
		}

		ts := runBase + int64(au.PTS)
		t.tlMu.Lock()
		if t.haveOut && ts <= int64(t.lastTS) {
			ts = int64(t.lastTS) + 1
		}
		t.lastTS = uint32(ts)
		t.lastAt = now
		t.haveOut = true
		t.tlMu.Unlock()

		if fedAt, ok := t.lookupFeed(au.PTS); ok {
			if m := t.opts.Metrics; m != nil {
				m.AddTranscodeLatency(now.Sub(fedAt))
			}
			latencyHit++
		} else {
			latencyMiss++
			if latencyMiss == 250 {
				t.logf("latency: no matching input timestamp yet (%d hits / %d misses)", latencyHit, latencyMiss)
			}
		}

		cts := au.CompositionTime()
		for i, nalu := range nalus {
			t.vSrc.Write(source.Frame{
				Data:            append([]byte(nil), nalu...),
				Timestamp:       uint32(ts),
				CompositionTime: cts,
				Keyframe:        au.Keyframe,
				AUStart:         i == 0,
				AUEnd:           i == len(nalus)-1,
			})
		}
		if m := t.opts.Metrics; m != nil {
			m.AddTranscodeOutputFrames(1)
			m.AddTranscodeBytes(size)
		}
	}
	d.OnAudio = func(mpegts.AudioAU) {
		// 输入/output 都不带音频；出现说明参数或 ffmpeg 行为异常。
		t.logf("unexpected audio from ffmpeg, ignored")
	}

	buf := make([]byte, readBufSize)
	for {
		n, err := stdout.Read(buf)
		if n > 0 {
			d.Write(buf[:n])
		}
		if err != nil {
			d.Flush()
			return
		}
	}
}

// ---- 时间戳与延迟辅助 ----

type tsUnwrapper struct {
	have bool
	last uint32
	val  int64
}

func (u *tsUnwrapper) unwrap(raw uint32) int64 {
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

func (t *Transcoder) recordFeed(pts uint32, at time.Time) {
	t.feedMu.Lock()
	defer t.feedMu.Unlock()
	if _, exists := t.feedAt[pts]; !exists {
		t.feedOrder = append(t.feedOrder, pts)
	}
	t.feedAt[pts] = at
	for len(t.feedOrder) > maxLatencyEntries {
		old := t.feedOrder[0]
		t.feedOrder = t.feedOrder[1:]
		delete(t.feedAt, old)
	}
}

func (t *Transcoder) lookupFeed(pts uint32) (time.Time, bool) {
	t.feedMu.Lock()
	defer t.feedMu.Unlock()
	at, ok := t.feedAt[pts]
	return at, ok
}

// ---- 小工具 ----

func paramFingerprint(vps, sps, pps []byte) []byte {
	fp := make([]byte, 0, len(vps)+len(sps)+len(pps)+2)
	fp = append(fp, vps...)
	fp = append(fp, 0)
	fp = append(fp, sps...)
	fp = append(fp, 0)
	fp = append(fp, pps...)
	return fp
}

func audioFingerprint(p *stream.AudioCodec) []byte {
	fp := make([]byte, 0, len(p.Codec)+len(p.Config)+8)
	fp = append(fp, p.Codec...)
	fp = append(fp, 0)
	fp = append(fp, p.Config...)
	fp = append(fp, byte(p.SampleRate>>24), byte(p.SampleRate>>16), byte(p.SampleRate), byte(p.SampleRate>>8))
	fp = append(fp, byte(p.Channels), byte(p.ClockRate>>24), byte(p.ClockRate>>16), byte(p.ClockRate>>8), byte(p.ClockRate))
	return fp
}

func isAUD(codec string, nalu []byte) bool {
	typ, err := media.VideoNALType(codec, nalu)
	if err != nil {
		return false
	}
	if media.IsHEVCCodec(codec) {
		return typ == 35
	}
	return typ == 9
}

type boundedBuffer struct {
	mu    sync.Mutex
	buf   []byte
	limit int
}

func newBoundedBuffer(limit int) *boundedBuffer {
	return &boundedBuffer{limit: limit}
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	if len(b.buf) > b.limit {
		b.buf = append(b.buf[:0], b.buf[len(b.buf)-b.limit:]...)
	}
	return len(p), nil
}

func (b *boundedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

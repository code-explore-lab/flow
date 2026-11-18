// Package hls 把共享注册表里的实时帧切成 HLS 的 part / segment。
//
// 本文件只做「切片状态机」：输入已经按访问单元聚合好的 sample，
// 输出 part / segment / end 事件。它不读网络、不管理观众、不生成 m3u8，
// 也不处理 PDT（墙钟由调用方根据 Segment.StartPTS 计算）。
//
// 设计要点：
//   - 视频段只在 IDR 处切；音频没有关键帧，按目标时长切。
//   - 视频 part 在关键帧处切，并在时长达到「按帧长取整后的 part target」时切；
//     这样 25fps + 0.5s part + 1s GOP 也能让 IDR 落在 part 边界上。
//   - 非 independent 的 part 必须 ≥85% part target（RFC 8216bis §4.4.4.9），
//     最后一片和以 IDR 开头的 part 例外。
//   - 不做并发保护：一个 Segmenter 只由一个 goroutine 调用。
package hls

import (
	"errors"
	"fmt"
	"time"

	"flow/internal/fmp4"
)

// ErrFinished 表示 Segmenter 已经 Finish，不能再写样本。
var ErrFinished = errors.New("hls: segmenter already finished")

// Config 一条轨的切片参数。必须用 NewSegmenter 校验。
type Config struct {
	TrackID       uint32        // fMP4 的 track ID（视频 1、音频 2）
	Timescale     uint32        // 90000（视频）或采样率（音频）
	SegmentTarget time.Duration // 目标段时长，例如 4s
	PartTarget    time.Duration // 目标 part 时长，例如 500ms；0 表示关闭 LL part
	Video         bool          // true=视频（有 IDR 概念）
}

// Sample 一个已经编码、已经按访问单元聚好的样本。
//
// 视频的 Data 是「4 字节大端长度 + NALU」拼接的访问单元（不含 SPS/PPS 会更好）；
// 音频的 Data 是一个裸 AAC AU。Data 在 part 关闭前必须保持稳定，调用方不要复用
// 底层数组（切片器会在关闭 part 时把它写进 fMP4 fragment）。
type Sample struct {
	Data     []byte
	DTS      int64  // 轨道时间轴上的解码时间
	Duration uint32 // 样本时长
	CTS      int32  // PTS − DTS（视频 B 帧可为负；音频必须为 0）
	Keyframe bool
}

// EventKind 事件类型。
type EventKind uint8

const (
	EventPart EventKind = iota + 1
	EventSegment
	EventEnd
)

// Event 切片产生的事件。Part/Segment 只在对应 Kind 时非 nil。
type Event struct {
	Kind    EventKind
	Part    *Part
	Segment *Segment
}

// Part 一个已经完成的 partial segment（父段的一段）。
type Part struct {
	MSN           uint64
	Index         int
	Duration      time.Duration
	DurationTicks int64
	StartPTS      int64
	Independent   bool
	Data          []byte // moof + mdat
}

// Segment 一个已经完成的父段。
type Segment struct {
	MSN           uint64
	DiscSeq       uint64
	Discontinuity bool
	MapVersion    int // 该段使用的 init 版本（参数集变化时递增）
	Duration      time.Duration
	DurationTicks int64
	StartPTS      int64
	PartCount     int
	Data          []byte // styp + 各 part 字节
}

// OpenSegment 当前正在生成的父段的快照，供生成 LL-HLS 的 PRELOAD-HINT 等使用。
type OpenSegment struct {
	MSN           uint64
	DiscSeq       uint64
	Discontinuity bool
	MapVersion    int
	StartPTS      int64
	PartIndex     int // 当前正在累积的 part 的 index（也是下一个将发布 part 的 index）
	PartCount     int // 已经发布的 part 数
}

// Segmenter 单轨切片状态机。不可并发使用。
type Segmenter struct {
	cfg       Config
	trackID   uint32
	timescale uint32
	video     bool

	segmentTargetTicks int64
	partTargetTicks    int64 // 配置值
	effPartTicks       int64 // 按首个样本时长向上取整后的实际 part target
	sampleDurTicks     int64 // 首个样本时长，用于取整；参数集变化后重置
	targetDur          int   // EXT-X-TARGETDURATION（秒），单调不减
	nextSeq            uint32

	// 父段状态
	msn          uint64
	started      bool
	segOpen      bool
	segStartDTS  int64
	segStartPTS  int64
	segDisc      bool
	segMap       int
	segParts     []byte
	segPartCount int

	// part 状态
	partOpen        bool
	partStartDTS    int64
	partStartPTS    int64
	partIndex       int
	partIndependent bool
	partSamples     []fmp4.Sample

	// 上一个已接受样本，用于校验 DTS 连续和 Finish 结算
	lastValid bool
	lastDTS   int64
	lastDur   uint32

	// discontinuity
	discSeq     uint64
	currentMap  int
	pendingDisc bool
	pendingMap  int
	awaitKey    bool

	finished bool
}

// NewSegmenter 校验参数并创建一个切片器。
func NewSegmenter(cfg Config) (*Segmenter, error) {
	if cfg.TrackID == 0 {
		return nil, errors.New("hls: track ID is zero")
	}
	if cfg.Timescale == 0 {
		return nil, errors.New("hls: timescale is zero")
	}
	if cfg.SegmentTarget <= 0 {
		return nil, errors.New("hls: segment target must be > 0")
	}
	if cfg.PartTarget < 0 {
		return nil, errors.New("hls: part target must be >= 0")
	}
	if cfg.PartTarget > cfg.SegmentTarget {
		return nil, fmt.Errorf("hls: part target %s > segment target %s", cfg.PartTarget, cfg.SegmentTarget)
	}

	s := &Segmenter{
		cfg:                cfg,
		trackID:            cfg.TrackID,
		timescale:          cfg.Timescale,
		video:              cfg.Video,
		segmentTargetTicks: durationTicks(cfg.SegmentTarget, cfg.Timescale),
		partTargetTicks:    durationTicks(cfg.PartTarget, cfg.Timescale),
		nextSeq:            1,
	}
	if s.segmentTargetTicks <= 0 {
		return nil, errors.New("hls: segment target rounds to zero ticks")
	}
	s.effPartTicks = s.partTargetTicks
	s.targetDur = ceilSeconds(s.segmentTargetTicks, s.timescale)
	return s, nil
}

// TrackID 返回 fMP4 track ID。
func (s *Segmenter) TrackID() uint32 { return s.trackID }

// TargetDuration 返回当前应写入 EXT-X-TARGETDURATION 的秒数（单调不减）。
func (s *Segmenter) TargetDuration() int { return s.targetDur }

// PartTarget 返回实际使用的 part target（按帧长向上取整后的值）。
// 关闭 LL 时返回 0。
func (s *Segmenter) PartTarget() time.Duration {
	if s.effPartTicks <= 0 {
		return 0
	}
	return ticksToDuration(s.effPartTicks, s.timescale)
}

// PartTargetTicks 返回实际 part target 的 ticks（测试/校验用）。
func (s *Segmenter) PartTargetTicks() int64 { return s.effPartTicks }

// SegmentTarget 返回配置的段目标时长。
func (s *Segmenter) SegmentTarget() time.Duration { return s.cfg.SegmentTarget }

// NextMSN 返回下一个要生成的父段序号（当前打开的段就是它）。
func (s *Segmenter) NextMSN() uint64 { return s.msn }

// DiscSeq 返回当前 discontinuity sequence。
func (s *Segmenter) DiscSeq() uint64 { return s.discSeq }

// Open 返回当前正在生成的父段快照；没有打开段时 ok=false。
func (s *Segmenter) Open() (OpenSegment, bool) {
	if !s.segOpen {
		return OpenSegment{}, false
	}
	return OpenSegment{
		MSN:           s.msn,
		DiscSeq:       s.discSeq,
		Discontinuity: s.segDisc,
		MapVersion:    s.segMap,
		StartPTS:      s.segStartPTS,
		PartIndex:     s.partIndex,
		PartCount:     s.segPartCount,
	}, true
}

// Add 写入一个样本，返回因此完成的 part/segment 事件。
//
// 视频在首个 IDR 之前、以及 discontinuity 之后的下一个 IDR 之前的非关键帧
// 会被静默丢弃（返回 nil, nil）——这些帧无法独立解码。
func (s *Segmenter) Add(smp Sample) ([]Event, error) {
	if s.finished {
		return nil, ErrFinished
	}
	if err := s.validateSample(smp); err != nil {
		return nil, err
	}

	// 等关键帧：首个 IDR 之前 / discontinuity 之后。
	if s.video && (!s.started || s.awaitKey) && !smp.Keyframe {
		return nil, nil
	}
	if s.lastValid && smp.DTS != s.lastDTS+int64(s.lastDur) {
		return nil, fmt.Errorf("hls: non-contiguous DTS %d after %d+%d", smp.DTS, s.lastDTS, s.lastDur)
	}
	if s.sampleDurTicks == 0 {
		s.sampleDurTicks = int64(smp.Duration)
		s.updateEffPart()
	}

	var events []Event
	if !s.segOpen {
		s.openSegment(smp)
		s.openPart(smp)
		s.accept(smp)
		return events, nil
	}

	segDur := smp.DTS - s.segStartDTS

	// 父段边界：视频必须等到 IDR；音频按目标时长（不超出）切。
	if s.video {
		if smp.Keyframe && segDur >= s.segmentTargetTicks {
			ev, err := s.closePartAt(smp.DTS)
			if err != nil {
				return nil, err
			}
			events = append(events, ev...)
			ev, err = s.closeSegmentAt(smp.DTS)
			if err != nil {
				return nil, err
			}
			events = append(events, ev...)
			s.openSegment(smp)
			s.openPart(smp)
			s.accept(smp)
			return events, nil
		}
	} else if segDur+int64(smp.Duration) > s.segmentTargetTicks {
		ev, err := s.closePartAt(smp.DTS)
		if err != nil {
			return nil, err
		}
		events = append(events, ev...)
		ev, err = s.closeSegmentAt(smp.DTS)
		if err != nil {
			return nil, err
		}
		events = append(events, ev...)
		s.openSegment(smp)
		s.openPart(smp)
		s.accept(smp)
		return events, nil
	}

	// part 边界。只有开启 LL（partTarget > 0）时才切 part。
	if s.partTargetTicks > 0 {
		partDur := smp.DTS - s.partStartDTS

		// 关键帧处：只有当前 part 已经达到 85% 才切，避免制造过短且非
		// independent 的 part（85% 规则只对 independent/最后一片豁免）。
		if s.video && smp.Keyframe && partDur*100 >= s.effPartTicks*85 {
			ev, err := s.closePartAt(smp.DTS)
			if err != nil {
				return nil, err
			}
			events = append(events, ev...)
		}
		// 普通时长边界：加进去会超过实际 part target 就先收 part。
		if s.partOpen && smp.DTS-s.partStartDTS+int64(smp.Duration) > s.effPartTicks {
			ev, err := s.closePartAt(smp.DTS)
			if err != nil {
				return nil, err
			}
			events = append(events, ev...)
		}
	}

	if s.partOpen {
		s.partSamples = append(s.partSamples, s.toFMP4(smp))
	} else {
		s.openPart(smp)
	}
	s.accept(smp)
	return events, nil
}

// MarkDiscontinuity 在参数集/时间轴发生变化时调用：先结束当前 part/segment，
// 之后丢弃视频帧直到下一个 IDR，再以新的 init 版本开新段。
//
// mapVersion 是新的 init 版本号；仅时间戳跳变、参数集没变时传当前版本即可。
func (s *Segmenter) MarkDiscontinuity(mapVersion int) ([]Event, error) {
	if s.finished {
		return nil, ErrFinished
	}
	var events []Event
	if s.started {
		end := s.endDTS()
		if s.partOpen {
			ev, err := s.closePartAt(end)
			if err != nil {
				return nil, err
			}
			events = append(events, ev...)
		}
		if s.segOpen {
			ev, err := s.closeSegmentAt(end)
			if err != nil {
				return nil, err
			}
			events = append(events, ev...)
		}
	}

	s.lastValid = false
	s.sampleDurTicks = 0
	s.effPartTicks = s.partTargetTicks
	s.awaitKey = s.video
	if s.started {
		s.pendingDisc = true
		s.pendingMap = mapVersion
	} else {
		// 还没出过媒体：不制造一个空的 discontinuity，只更新 init。
		s.currentMap = mapVersion
	}
	s.partOpen = false
	s.partSamples = nil
	return events, nil
}

// Finish 结束输入：冲刷最后一个 part/segment，并返回 EventEnd。
// 幂等；之后再 Add/MarkDiscontinuity 返回 ErrFinished。
func (s *Segmenter) Finish() ([]Event, error) {
	if s.finished {
		return nil, nil
	}
	var events []Event
	if s.segOpen {
		end := s.endDTS()
		if s.partOpen {
			ev, err := s.closePartAt(end)
			if err != nil {
				return nil, err
			}
			events = append(events, ev...)
		}
		ev, err := s.closeSegmentAt(end)
		if err != nil {
			return nil, err
		}
		events = append(events, ev...)
	}
	s.finished = true
	events = append(events, Event{Kind: EventEnd})
	return events, nil
}

func (s *Segmenter) validateSample(smp Sample) error {
	if len(smp.Data) == 0 {
		return errors.New("hls: sample has no data")
	}
	if smp.Duration == 0 {
		return errors.New("hls: sample duration is zero")
	}
	if smp.DTS < 0 {
		return fmt.Errorf("hls: negative sample DTS %d", smp.DTS)
	}
	if !s.video && smp.CTS != 0 {
		return fmt.Errorf("hls: audio sample has CTS %d", smp.CTS)
	}
	return nil
}

// updateEffPart 把 part target 向上取整到首个样本时长的整数倍。
// 例如 25fps（40ms）+ 500ms -> 520ms，13 个整帧；这样
// 500ms 附近的 part 边界不会漂移到 960ms/1440ms，IDR 才能落在 part 起点。
func (s *Segmenter) updateEffPart() {
	if s.partTargetTicks <= 0 {
		s.effPartTicks = 0
		return
	}
	if s.sampleDurTicks <= 0 {
		s.effPartTicks = s.partTargetTicks
		return
	}
	n := (s.partTargetTicks + s.sampleDurTicks - 1) / s.sampleDurTicks
	s.effPartTicks = n * s.sampleDurTicks
}

func (s *Segmenter) openSegment(smp Sample) {
	s.segOpen = true
	s.started = true
	s.segStartDTS = smp.DTS
	s.segStartPTS = smp.DTS + int64(smp.CTS)
	s.segDisc = false
	s.segMap = s.currentMap
	if s.pendingDisc {
		s.discSeq++
		s.segDisc = true
		s.segMap = s.pendingMap
		s.currentMap = s.pendingMap
		s.pendingDisc = false
	}
	s.segParts = nil
	s.segPartCount = 0
	s.partIndex = 0
	s.awaitKey = false
}

func (s *Segmenter) openPart(smp Sample) {
	s.partOpen = true
	s.partStartDTS = smp.DTS
	s.partStartPTS = smp.DTS + int64(smp.CTS)
	s.partSamples = []fmp4.Sample{s.toFMP4(smp)}
	s.partIndependent = s.video && smp.Keyframe
}

func (s *Segmenter) closePartAt(endDTS int64) ([]Event, error) {
	if !s.partOpen {
		return nil, nil
	}
	dur := endDTS - s.partStartDTS
	if dur <= 0 {
		return nil, fmt.Errorf("hls: part duration %d <= 0 (start %d, end %d)", dur, s.partStartDTS, endDTS)
	}
	frag, err := fmp4.Fragment(s.nextSeq, fmp4.TrackFragment{
		TrackID:  s.trackID,
		Samples:  s.partSamples,
		WriteCTS: s.video,
	})
	if err != nil {
		return nil, fmt.Errorf("hls: encode part: %w", err)
	}
	s.nextSeq++

	part := &Part{
		MSN:           s.msn,
		Index:         s.partIndex,
		Duration:      ticksToDuration(dur, s.timescale),
		DurationTicks: dur,
		StartPTS:      s.partStartPTS,
		Independent:   s.partIndependent,
		Data:          frag,
	}
	s.segParts = append(s.segParts, frag...)
	s.segPartCount++
	s.partIndex++
	s.partOpen = false
	s.partSamples = nil
	return []Event{{Kind: EventPart, Part: part}}, nil
}

func (s *Segmenter) closeSegmentAt(endDTS int64) ([]Event, error) {
	if !s.segOpen {
		return nil, nil
	}
	if s.partOpen {
		return nil, errors.New("hls: close segment before closing part")
	}
	dur := endDTS - s.segStartDTS
	if dur <= 0 {
		return nil, fmt.Errorf("hls: segment duration %d <= 0 (start %d, end %d)", dur, s.segStartDTS, endDTS)
	}
	styp := fmp4.Styp()
	data := make([]byte, 0, len(styp)+len(s.segParts))
	data = append(data, styp...)
	data = append(data, s.segParts...)

	seg := &Segment{
		MSN:           s.msn,
		DiscSeq:       s.discSeq,
		Discontinuity: s.segDisc,
		MapVersion:    s.segMap,
		Duration:      ticksToDuration(dur, s.timescale),
		DurationTicks: dur,
		StartPTS:      s.segStartPTS,
		PartCount:     s.segPartCount,
		Data:          data,
	}
	s.updateTargetDuration(dur)
	s.msn++
	s.segOpen = false
	s.segParts = nil
	s.segPartCount = 0
	s.segDisc = false
	return []Event{{Kind: EventSegment, Segment: seg}}, nil
}

func (s *Segmenter) accept(smp Sample) {
	s.lastValid = true
	s.lastDTS = smp.DTS
	s.lastDur = smp.Duration
}

func (s *Segmenter) endDTS() int64 {
	if !s.lastValid {
		return 0
	}
	return s.lastDTS + int64(s.lastDur)
}

func (s *Segmenter) toFMP4(smp Sample) fmp4.Sample {
	return fmp4.Sample{
		Data:     smp.Data,
		DTS:      smp.DTS,
		Duration: smp.Duration,
		CTS:      smp.CTS,
		Keyframe: smp.Keyframe,
	}
}

func (s *Segmenter) updateTargetDuration(durTicks int64) {
	sec := ceilSeconds(durTicks, s.timescale)
	if sec > s.targetDur {
		s.targetDur = sec
	}
}

// durationTicks 把 time.Duration 换成轨道 ticks。截断到 tick 精度。
func durationTicks(d time.Duration, timescale uint32) int64 {
	if d <= 0 || timescale == 0 {
		return 0
	}
	return int64(d) * int64(timescale) / int64(time.Second)
}

// ticksToDuration 把轨道 ticks 换成 time.Duration。
func ticksToDuration(ticks int64, timescale uint32) time.Duration {
	if ticks <= 0 || timescale == 0 {
		return 0
	}
	return time.Duration(ticks * int64(time.Second) / int64(timescale))
}

// ceilSeconds 把 ticks 向上取整成秒，最小 1。
func ceilSeconds(ticks int64, timescale uint32) int {
	if ticks <= 0 || timescale == 0 {
		return 1
	}
	sec := (ticks + int64(timescale) - 1) / int64(timescale)
	if sec < 1 {
		return 1
	}
	return int(sec)
}

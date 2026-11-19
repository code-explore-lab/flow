// Package dash 把 P-2 已经切好的 fMP4 段发布为 MPEG-DASH（P-3）。
//
// 切片本身完全复用 internal/hls 的 packager：本包只读取 hls.Packager 的快照
// （每轨已完成的完整段 + init 版本），生成 dynamic MPD，并像 HLS 分发器一样
// 从 Track.Data 读取字节。设计说明见 done.md 的 P-3。
package dash

import (
	"encoding/xml"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"flow/internal/hls"
)

// ErrNoSegments 表示 packager 还没有任何已完成的段，无法生成可播放的 MPD。
var ErrNoSegments = errors.New("dash: no completed segments yet")

// Options 控制 MPD 生成。
//
// 零值可用：segment 4s、minimumUpdatePeriod 2s、suggestedPresentationDelay
// 取两倍段长（受窗口深度约束）。
type Options struct {
	MinimumUpdatePeriod        time.Duration
	SegmentTarget              time.Duration
	SuggestedPresentationDelay time.Duration
	Token                      string
}

func (o Options) withDefaults() Options {
	if o.MinimumUpdatePeriod <= 0 {
		o.MinimumUpdatePeriod = 2 * time.Second
	}
	if o.SegmentTarget <= 0 {
		o.SegmentTarget = 4 * time.Second
	}
	return o
}

// Ready 报告 packager 是否已有足够的完整段生成 MPD。
//
// 存在的每条轨（video/audio）都必须至少有一个完整段；轨已结束（closed）
// 且没有段时跳过，由 Build 决定是否整体失败。
func Ready(p *hls.Packager) bool {
	if p == nil {
		return false
	}
	any := false
	for _, tr := range []*hls.Track{p.Video(), p.Audio()} {
		if tr == nil {
			continue
		}
		if len(latestSegments(tr.Snapshot())) == 0 {
			if !tr.Closed() {
				return false
			}
			continue
		}
		any = true
	}
	return any
}

// latestSegments 只保留最新 init 版本（MapURI）的段。
//
// 参数集变化会开启新的 MapVersion 和 discontinuity；一个 DASH Representation
// 只能引用一份 initialization，因此在 v1 里窗口只暴露最新版本的段。旧段在
// 变化发生时通常已经播放过，新加入的客户端从新版本开始，时间线出现一次前跳。
func latestSegments(snap hls.Snapshot) []hls.SegmentInfo {
	if len(snap.Segments) == 0 {
		return nil
	}
	target := ""
	for i := len(snap.Segments) - 1; i >= 0; i-- {
		if snap.Segments[i].MapURI != "" {
			target = snap.Segments[i].MapURI
			break
		}
	}
	out := make([]hls.SegmentInfo, 0, len(snap.Segments))
	for _, s := range snap.Segments {
		if target == "" || s.MapURI == target {
			out = append(out, s)
		}
	}
	return out
}

// Build 用当前窗口生成一份 dynamic MPD。now 同时用于 publishTime。
func Build(p *hls.Packager, now time.Time, opts Options) ([]byte, error) {
	if p == nil {
		return nil, errors.New("dash: nil packager")
	}
	opts = opts.withDefaults()

	type rep struct {
		track    *hls.Track
		segments []hls.SegmentInfo
	}
	var reps []rep
	for _, tr := range []*hls.Track{p.Video(), p.Audio()} {
		if tr == nil {
			continue
		}
		segs := latestSegments(tr.Snapshot())
		if len(segs) == 0 {
			continue
		}
		reps = append(reps, rep{track: tr, segments: segs})
	}
	if len(reps) == 0 {
		return nil, ErrNoSegments
	}

	// 窗口实际覆盖的媒体时间，用于 timeShiftBufferDepth 和延迟上限。
	// 与 SegmentTimeline 保持同一条（DTS）时间轴。
	minStart, maxEnd := math.Inf(1), math.Inf(-1)
	for _, r := range reps {
		ts := float64(r.track.Timescale())
		for _, s := range r.segments {
			start := float64(s.StartDTS) / ts
			end := start + float64(segmentDurationTicks(s, r.track.Timescale()))/ts
			if start < minStart {
				minStart = start
			}
			if end > maxEnd {
				maxEnd = end
			}
		}
	}
	depth := maxEnd - minStart
	if depth <= 0 {
		depth = opts.SegmentTarget.Seconds()
	}

	delay := opts.SuggestedPresentationDelay
	if delay <= 0 {
		delay = 2 * opts.SegmentTarget
	}
	if depth > 0 && delay.Seconds() > depth*0.75 {
		delay = time.Duration(depth * 0.5 * float64(time.Second))
	}
	if delay < 0 {
		delay = 0
	}

	period := Period{ID: "0", Start: "PT0S"}
	for _, r := range reps {
		period.AdaptationSets = append(period.AdaptationSets, buildAdaptationSet(r.track, r.segments, opts))
	}

	manifest := MPD{
		Profiles:                   "urn:mpeg:dash:profile:isoff-live:2011",
		Type:                       "dynamic",
		AvailabilityStartTime:      isoTime(p.Epoch()),
		PublishTime:                isoTime(now),
		MinimumUpdatePeriod:        isoDuration(opts.MinimumUpdatePeriod),
		MinBufferTime:              isoDuration(opts.SegmentTarget),
		SuggestedPresentationDelay: isoDuration(delay),
		TimeShiftBufferDepth:       isoDuration(time.Duration(depth * float64(time.Second))),
		UTCTiming:                  UTCTiming{SchemeIDURI: "urn:mpeg:dash:utc:http-iso:2014", Value: "utc"},
		Periods:                    []Period{period},
	}

	body, err := xml.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("dash: marshal MPD: %w", err)
	}
	out := make([]byte, 0, len(body)+64)
	out = append(out, `<?xml version="1.0" encoding="utf-8"?>`+"\n"...)
	out = append(out, body...)
	out = append(out, '\n')
	return out, nil
}

func buildAdaptationSet(tr *hls.Track, segs []hls.SegmentInfo, opts Options) AdaptationSet {
	kind := tr.Kind()
	rep := Representation{
		ID:        kind,
		Codecs:    tr.Codec(),
		Bandwidth: tr.Bandwidth(),
	}
	if rep.Bandwidth == 0 {
		rep.Bandwidth = 1
	}
	if w, h := tr.Size(); w > 0 && h > 0 {
		rep.Width, rep.Height = w, h
	}
	if kind == "audio" {
		rep.AudioSamplingRate = tr.Timescale()
	}
	rep.SegmentTemplate = &SegmentTemplate{
		Timescale:      tr.Timescale(),
		Initialization: withToken(segs[0].MapURI, opts.Token),
		Media:          withToken(kind+"/seg-$Number$.m4s", opts.Token),
		StartNumber:    segs[0].MSN,
		Timeline:       &SegmentTimeline{S: timeline(segs, tr.Timescale())},
	}
	as := AdaptationSet{
		ID:               kind,
		ContentType:      kind,
		MimeType:         kind + "/mp4",
		SegmentAlignment: "true",
		StartWithSAP:     "1",
		Representations:  []Representation{rep},
	}
	return as
}

// timeline 把窗口内完整段转成 SegmentTimeline。
//
// @t 取段首样本的（合成）DTS，与段内 tfdt 是同一条时间轴；@d 用 Segmenter
// 给的 DTS 时长，因此同一连续序列的 段首 DTS 严格等于上一段的 DTS 时长之和。
// 只有首段、discontinuity / init 版本切换、以及时间轴真的跳变时才显式写 @t，
// 其余按 DASH 语义隐式续接。
//
// 不能用 StartPTS：带 B 帧时相邻段首样本的 CTS 不同（PTS = DTS + CTS），
// 用 PTS 差分会在时间线上留下几十毫秒的假间隙，播放器会当成不连续处理。
func timeline(segs []hls.SegmentInfo, timescale uint32) []TimelineEntry {
	out := make([]TimelineEntry, 0, len(segs))
	var prevEnd int64
	first := true
	for i, s := range segs {
		d := segmentDurationTicks(s, timescale)
		if d <= 0 {
			d = 1
		}
		t := s.StartDTS
		entry := TimelineEntry{D: d}
		explicit := first || t != prevEnd ||
			s.Discontinuity || (i > 0 && s.MapURI != segs[i-1].MapURI)
		if explicit {
			tt := t
			entry.T = &tt
		}
		out = append(out, entry)
		prevEnd = t + d
		first = false
	}
	return out
}

func segmentDurationTicks(s hls.SegmentInfo, timescale uint32) int64 {
	if s.DurationTicks > 0 {
		return s.DurationTicks
	}
	if s.Duration > 0 && timescale > 0 {
		return int64(math.Round(s.Duration * float64(timescale)))
	}
	return 0
}

func withToken(uri, token string) string {
	if token == "" {
		return uri
	}
	sep := "?"
	if strings.Contains(uri, "?") {
		sep = "&"
	}
	return uri + sep + "token=" + urlQueryEscape(token)
}

func urlQueryEscape(v string) string {
	// token 通常很短；避免为这一处引入 net/url 的全局行为差异。
	var b strings.Builder
	for i := 0; i < len(v); i++ {
		c := v[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		default:
			b.WriteString("%")
			b.WriteString(strings.ToUpper(strconv.FormatInt(int64(c), 16)))
		}
	}
	return b.String()
}

func isoTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

// isoDuration 格式化为 xs:duration，只输出秒级/毫秒级形式（PT2S、PT0.5S）。
func isoDuration(d time.Duration) string {
	if d <= 0 {
		return "PT0S"
	}
	ms := d.Milliseconds()
	if ms%1000 == 0 {
		return fmt.Sprintf("PT%dS", ms/1000)
	}
	s := strconv.FormatFloat(float64(ms)/1000, 'f', 3, 64)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	return "PT" + s + "S"
}

// ---- MPD XML 结构（仅覆盖 isoff-live 需要的子集）----

// MPD 对应 <MPD>。
type MPD struct {
	XMLName                    xml.Name  `xml:"urn:mpeg:dash:schema:mpd:2011 MPD"`
	Profiles                   string    `xml:"profiles,attr"`
	Type                       string    `xml:"type,attr"`
	AvailabilityStartTime      string    `xml:"availabilityStartTime,attr"`
	PublishTime                string    `xml:"publishTime,attr"`
	MinimumUpdatePeriod        string    `xml:"minimumUpdatePeriod,attr"`
	MinBufferTime              string    `xml:"minBufferTime,attr"`
	SuggestedPresentationDelay string    `xml:"suggestedPresentationDelay,attr"`
	TimeShiftBufferDepth       string    `xml:"timeShiftBufferDepth,attr"`
	UTCTiming                  UTCTiming `xml:"UTCTiming"`
	Periods                    []Period  `xml:"Period"`
}

// UTCTiming 对应 <UTCTiming>。
type UTCTiming struct {
	SchemeIDURI string `xml:"schemeIdUri,attr"`
	Value       string `xml:"value,attr"`
}

// Period 对应 <Period>。
type Period struct {
	ID             string          `xml:"id,attr,omitempty"`
	Start          string          `xml:"start,attr,omitempty"`
	AdaptationSets []AdaptationSet `xml:"AdaptationSet"`
}

// AdaptationSet 对应 <AdaptationSet>。
type AdaptationSet struct {
	ID               string           `xml:"id,attr,omitempty"`
	ContentType      string           `xml:"contentType,attr,omitempty"`
	MimeType         string           `xml:"mimeType,attr"`
	SegmentAlignment string           `xml:"segmentAlignment,attr,omitempty"`
	StartWithSAP     string           `xml:"startWithSAP,attr,omitempty"`
	Representations  []Representation `xml:"Representation"`
}

// Representation 对应 <Representation>。
type Representation struct {
	ID                string           `xml:"id,attr"`
	Codecs            string           `xml:"codecs,attr,omitempty"`
	Bandwidth         uint32           `xml:"bandwidth,attr"`
	Width             int              `xml:"width,attr,omitempty"`
	Height            int              `xml:"height,attr,omitempty"`
	AudioSamplingRate uint32           `xml:"audioSamplingRate,attr,omitempty"`
	SegmentTemplate   *SegmentTemplate `xml:"SegmentTemplate"`
}

// SegmentTemplate 对应 <SegmentTemplate>。
type SegmentTemplate struct {
	Timescale      uint32           `xml:"timescale,attr"`
	Initialization string           `xml:"initialization,attr"`
	Media          string           `xml:"media,attr"`
	StartNumber    uint64           `xml:"startNumber,attr"`
	Timeline       *SegmentTimeline `xml:"SegmentTimeline"`
}

// SegmentTimeline 对应 <SegmentTimeline>。
type SegmentTimeline struct {
	S []TimelineEntry `xml:"S"`
}

// TimelineEntry 对应一个 <S>。
type TimelineEntry struct {
	T *int64 `xml:"t,attr,omitempty"`
	D int64  `xml:"d,attr"`
}

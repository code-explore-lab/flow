package hls

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"strings"
	"time"
)

// SegmentInfo 是 playlist 视角的一个完整父段。
// Duration 单位是秒；PDT 是该段首样本的绝对时间，零值表示不输出。
// Duration 请直接传 time.Duration.Seconds() 的原值，四舍五入交给生成器统一做。
type SegmentInfo struct {
	MSN           uint64
	DiscSeq       uint64
	Discontinuity bool
	MapURI        string
	Duration      float64
	DurationTicks int64 // 段时长（轨道 timescale），DASH SegmentTimeline 使用
	PDT           time.Time
	URI           string
	StartPTS      int64 // 轨道时间轴上的首样本 PTS；playlist 不输出，调试/测延迟用
}

// PartInfo 是当前打开父段里已经完成的一个 part。
// Duration 同样用原值（秒），不要提前四舍五入。
type PartInfo struct {
	MSN         uint64
	Index       int
	Duration    float64
	Independent bool
	URI         string
}

// OpenSegmentInfo 当前正在生成的父段；Parts 可以为空，NextPartURI 用来生成
// EXT-X-PRELOAD-HINT（也可能为空，表示暂不提示）。
type OpenSegmentInfo struct {
	MSN           uint64
	DiscSeq       uint64
	Discontinuity bool
	MapURI        string
	PDT           time.Time
	Parts         []PartInfo
	NextPartURI   string
}

// RenditionReport 另一条 rendition 的进度，对应 EXT-X-RENDITION-REPORT。
// LastPart 为 -1 时不输出 LAST-PART（经典模式没有 part）。
type RenditionReport struct {
	URI      string
	LastMSN  uint64
	LastPart int
}

// MediaPlaylist 一份 media playlist 的输入。
//
// PartTarget 为 0 时生成经典（无 PART）播放列表；大于 0 时生成 LL-HLS。
// CanBlockReload 表示服务端支持 _HLS_msn/_HLS_part 阻塞重载。
type MediaPlaylist struct {
	TargetDuration int
	PartTarget     float64
	Segments       []SegmentInfo
	Open           *OpenSegmentInfo
	EndList        bool
	Token          string
	Rendition      *RenditionReport
	CanBlockReload bool
}

// Generate 生成 m3u8 文本。所有输入先校验，失败返回 error。
func (p *MediaPlaylist) Generate() ([]byte, error) {
	if p.TargetDuration < 1 {
		return nil, fmt.Errorf("hls: target duration %d < 1", p.TargetDuration)
	}
	if p.PartTarget < 0 {
		return nil, fmt.Errorf("hls: negative part target %f", p.PartTarget)
	}
	if p.EndList && p.Open != nil {
		return nil, errors.New("hls: endlist with an open segment")
	}
	if err := p.validate(); err != nil {
		return nil, err
	}

	firstMSN, firstDisc := uint64(0), uint64(0)
	switch {
	case len(p.Segments) > 0:
		firstMSN, firstDisc = p.Segments[0].MSN, p.Segments[0].DiscSeq
	case p.Open != nil:
		firstMSN, firstDisc = p.Open.MSN, p.Open.DiscSeq
	}

	var b strings.Builder
	b.WriteString("#EXTM3U\n")
	version := 7
	if p.PartTarget > 0 || p.CanBlockReload {
		version = 9
	}
	fmt.Fprintf(&b, "#EXT-X-VERSION:%d\n", version)
	fmt.Fprintf(&b, "#EXT-X-TARGETDURATION:%d\n", p.TargetDuration)
	fmt.Fprintf(&b, "#EXT-X-MEDIA-SEQUENCE:%d\n", firstMSN)
	fmt.Fprintf(&b, "#EXT-X-DISCONTINUITY-SEQUENCE:%d\n", firstDisc)

	if p.PartTarget > 0 {
		fmt.Fprintf(&b, "#EXT-X-PART-INF:PART-TARGET=%.3f\n", p.PartTarget)
	}
	if p.PartTarget > 0 || p.CanBlockReload {
		attrs := make([]string, 0, 3)
		if p.CanBlockReload {
			attrs = append(attrs, "CAN-BLOCK-RELOAD=YES")
		}
		attrs = append(attrs, fmt.Sprintf("HOLD-BACK=%.3f", 3*float64(p.TargetDuration)))
		if p.PartTarget > 0 {
			attrs = append(attrs, fmt.Sprintf("PART-HOLD-BACK=%.3f", 3*p.PartTarget))
		}
		fmt.Fprintf(&b, "#EXT-X-SERVER-CONTROL:%s\n", strings.Join(attrs, ","))
	}

	lastMap := ""
	prevDisc := firstDisc
	first := true

	for _, seg := range p.Segments {
		if !first && seg.DiscSeq != prevDisc {
			b.WriteString("#EXT-X-DISCONTINUITY\n")
		}
		if seg.MapURI != "" && seg.MapURI != lastMap {
			fmt.Fprintf(&b, "#EXT-X-MAP:URI=\"%s\"\n", p.uri(seg.MapURI))
			lastMap = seg.MapURI
		}
		if !seg.PDT.IsZero() {
			fmt.Fprintf(&b, "#EXT-X-PROGRAM-DATE-TIME:%s\n", formatPDT(seg.PDT))
		}
		fmt.Fprintf(&b, "#EXTINF:%.3f,\n%s\n", seg.Duration, p.uri(seg.URI))
		prevDisc = seg.DiscSeq
		first = false
	}

	if open := p.Open; open != nil && (len(open.Parts) > 0 || open.NextPartURI != "") {
		if !first && open.DiscSeq != prevDisc {
			b.WriteString("#EXT-X-DISCONTINUITY\n")
		}
		if open.MapURI != "" && open.MapURI != lastMap {
			fmt.Fprintf(&b, "#EXT-X-MAP:URI=\"%s\"\n", p.uri(open.MapURI))
			lastMap = open.MapURI
		}
		if !open.PDT.IsZero() {
			fmt.Fprintf(&b, "#EXT-X-PROGRAM-DATE-TIME:%s\n", formatPDT(open.PDT))
		}
		for _, part := range open.Parts {
			fmt.Fprintf(&b, "#EXT-X-PART:DURATION=%.3f,URI=\"%s\"", part.Duration, p.uri(part.URI))
			if part.Independent {
				b.WriteString(",INDEPENDENT=YES")
			}
			b.WriteString("\n")
		}
		if !p.EndList && open.NextPartURI != "" {
			fmt.Fprintf(&b, "#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"%s\"\n", p.uri(open.NextPartURI))
		}
	}

	if p.Rendition != nil {
		fmt.Fprintf(&b, "#EXT-X-RENDITION-REPORT:URI=\"%s\",LAST-MSN=%d",
			p.uri(p.Rendition.URI), p.Rendition.LastMSN)
		if p.Rendition.LastPart >= 0 {
			fmt.Fprintf(&b, ",LAST-PART=%d", p.Rendition.LastPart)
		}
		b.WriteString("\n")
	}
	if p.EndList {
		b.WriteString("#EXT-X-ENDLIST\n")
	}
	return []byte(b.String()), nil
}

func (p *MediaPlaylist) validate() error {
	prevDisc := uint64(0)
	for i := range p.Segments {
		seg := &p.Segments[i]
		if seg.Duration <= 0 {
			return fmt.Errorf("hls: segment %d duration %f <= 0", seg.MSN, seg.Duration)
		}
		if int(math.Round(seg.Duration)) > p.TargetDuration {
			return fmt.Errorf("hls: segment %d duration %.3f rounds above target %d",
				seg.MSN, seg.Duration, p.TargetDuration)
		}
		if err := checkURI(seg.URI); err != nil {
			return fmt.Errorf("hls: segment %d: %w", seg.MSN, err)
		}
		if seg.MapURI != "" {
			if err := checkURI(seg.MapURI); err != nil {
				return fmt.Errorf("hls: segment %d map: %w", seg.MSN, err)
			}
		}
		if i > 0 {
			prev := p.Segments[i-1]
			if seg.MSN != prev.MSN+1 {
				return fmt.Errorf("hls: segment sequence gap %d -> %d", prev.MSN, seg.MSN)
			}
			if seg.DiscSeq < prevDisc || seg.DiscSeq > prevDisc+1 {
				return fmt.Errorf("hls: discontinuity sequence jump %d -> %d", prevDisc, seg.DiscSeq)
			}
		}
		prevDisc = seg.DiscSeq
	}

	if open := p.Open; open != nil {
		if open.MapURI != "" {
			if err := checkURI(open.MapURI); err != nil {
				return fmt.Errorf("hls: open map: %w", err)
			}
		}
		if p.PartTarget <= 0 && (len(open.Parts) > 0 || open.NextPartURI != "") {
			return errors.New("hls: parts/preload hint require a part target")
		}
		if open.NextPartURI != "" {
			if err := checkURI(open.NextPartURI); err != nil {
				return fmt.Errorf("hls: preload hint: %w", err)
			}
		}
		if len(p.Segments) > 0 {
			last := p.Segments[len(p.Segments)-1]
			if open.MSN != last.MSN+1 {
				return fmt.Errorf("hls: open segment %d does not follow %d", open.MSN, last.MSN)
			}
			if open.DiscSeq < last.DiscSeq || open.DiscSeq > last.DiscSeq+1 {
				return fmt.Errorf("hls: open discontinuity sequence jump %d -> %d", last.DiscSeq, open.DiscSeq)
			}
		}
		for i, part := range open.Parts {
			if part.MSN != open.MSN {
				return fmt.Errorf("hls: part %d MSN %d != open MSN %d", i, part.MSN, open.MSN)
			}
			if part.Index != i {
				return fmt.Errorf("hls: part index %d != position %d", part.Index, i)
			}
			if part.Duration <= 0 {
				return fmt.Errorf("hls: part %d duration %f <= 0", i, part.Duration)
			}
			if err := checkURI(part.URI); err != nil {
				return fmt.Errorf("hls: part %d: %w", i, err)
			}
			if p.PartTarget > 0 {
				if part.Duration > p.PartTarget+1e-9 {
					return fmt.Errorf("hls: part %d duration %.3f > part target %.3f", i, part.Duration, p.PartTarget)
				}
				// 当前打开父段里的 part 都还没最终确认；最后一片可能是即将结束父段的
				// final part，按规范豁免 85% 下限。其余非 independent part 必须 ≥85%。
				last := i == len(open.Parts)-1
				if !part.Independent && !last && part.Duration*100+1e-9 < p.PartTarget*85 {
					return fmt.Errorf("hls: part %d duration %.3f < 85%% of part target %.3f (not independent)",
						i, part.Duration, p.PartTarget)
				}
			}
		}
	}

	if p.Rendition != nil {
		if err := checkURI(p.Rendition.URI); err != nil {
			return fmt.Errorf("hls: rendition report: %w", err)
		}
		if p.Rendition.LastPart < -1 {
			return fmt.Errorf("hls: rendition report last part %d", p.Rendition.LastPart)
		}
	}
	return nil
}

func (p *MediaPlaylist) uri(uri string) string {
	return appendToken(uri, p.Token)
}

// MasterPlaylist 一份 multivariant playlist 的输入。
type MasterPlaylist struct {
	Version             int
	IndependentSegments bool
	Variants            []Variant
	Audio               *AudioRendition
	Token               string
}

// Variant 一个视频（或音频）变体。
type Variant struct {
	URI        string
	Bandwidth  uint32
	Resolution string // "640x480"，空表示省略
	FrameRate  float64
	Codecs     string // "avc1.64001e,mp4a.40.2"，空表示省略
	AudioGroup string // 引用 EXT-X-MEDIA 的 GROUP-ID
}

// AudioRendition 独立的音频 rendition，对应 EXT-X-MEDIA。
type AudioRendition struct {
	GroupID    string
	Name       string
	Language   string
	URI        string
	Channels   uint8
	Default    bool
	Autoselect bool
}

// Generate 生成 master playlist。
func (p *MasterPlaylist) Generate() ([]byte, error) {
	if len(p.Variants) == 0 {
		return nil, errors.New("hls: master playlist needs at least one variant")
	}
	version := p.Version
	if version == 0 {
		version = 9
	}
	if version < 7 {
		return nil, fmt.Errorf("hls: invalid master version %d", version)
	}
	if p.Audio != nil {
		if p.Audio.GroupID == "" || p.Audio.Name == "" || p.Audio.URI == "" {
			return nil, errors.New("hls: audio rendition needs GROUP-ID, NAME and URI")
		}
		for _, field := range []string{p.Audio.GroupID, p.Audio.Name, p.Audio.Language} {
			if err := checkQuoted(field); err != nil {
				return nil, fmt.Errorf("hls: audio rendition: %w", err)
			}
		}
		if err := checkURI(p.Audio.URI); err != nil {
			return nil, fmt.Errorf("hls: audio rendition: %w", err)
		}
		used := false
		for _, v := range p.Variants {
			if v.AudioGroup == p.Audio.GroupID {
				used = true
				break
			}
		}
		if !used {
			return nil, errors.New("hls: no variant references the audio rendition group")
		}
	}
	for _, v := range p.Variants {
		if v.Bandwidth == 0 {
			return nil, fmt.Errorf("hls: variant %q has zero bandwidth", v.URI)
		}
		if err := checkURI(v.URI); err != nil {
			return nil, fmt.Errorf("hls: variant %q: %w", v.URI, err)
		}
		for _, field := range []string{v.Resolution, v.Codecs, v.AudioGroup} {
			if err := checkQuoted(field); err != nil {
				return nil, fmt.Errorf("hls: variant %q: %w", v.URI, err)
			}
		}
		if v.AudioGroup != "" && p.Audio == nil {
			return nil, fmt.Errorf("hls: variant %q references audio group %q but no audio rendition",
				v.URI, v.AudioGroup)
		}
		if v.AudioGroup != "" && p.Audio != nil && v.AudioGroup != p.Audio.GroupID {
			return nil, fmt.Errorf("hls: variant %q references unknown audio group %q",
				v.URI, v.AudioGroup)
		}
	}

	var b strings.Builder
	b.WriteString("#EXTM3U\n")
	fmt.Fprintf(&b, "#EXT-X-VERSION:%d\n", version)
	if p.IndependentSegments {
		b.WriteString("#EXT-X-INDEPENDENT-SEGMENTS\n")
	}
	if p.Audio != nil {
		attrs := []string{
			"TYPE=AUDIO",
			fmt.Sprintf("GROUP-ID=\"%s\"", p.Audio.GroupID),
			fmt.Sprintf("NAME=\"%s\"", p.Audio.Name),
		}
		if p.Audio.Language != "" {
			attrs = append(attrs, fmt.Sprintf("LANGUAGE=\"%s\"", p.Audio.Language))
		}
		def := "NO"
		auto := "NO"
		if p.Audio.Default {
			def = "YES"
		}
		if p.Audio.Default || p.Audio.Autoselect {
			auto = "YES"
		}
		attrs = append(attrs, "DEFAULT="+def, "AUTOSELECT="+auto)
		if p.Audio.Channels > 0 {
			attrs = append(attrs, fmt.Sprintf("CHANNELS=\"%d\"", p.Audio.Channels))
		}
		attrs = append(attrs, fmt.Sprintf("URI=\"%s\"", appendToken(p.Audio.URI, p.Token)))
		fmt.Fprintf(&b, "#EXT-X-MEDIA:%s\n", strings.Join(attrs, ","))
	}
	for _, v := range p.Variants {
		attrs := []string{fmt.Sprintf("BANDWIDTH=%d", v.Bandwidth)}
		if v.Resolution != "" {
			attrs = append(attrs, "RESOLUTION="+v.Resolution)
		}
		if v.FrameRate > 0 {
			attrs = append(attrs, fmt.Sprintf("FRAME-RATE=%.3f", v.FrameRate))
		}
		if v.Codecs != "" {
			attrs = append(attrs, fmt.Sprintf("CODECS=\"%s\"", v.Codecs))
		}
		if v.AudioGroup != "" {
			attrs = append(attrs, fmt.Sprintf("AUDIO=\"%s\"", v.AudioGroup))
		}
		fmt.Fprintf(&b, "#EXT-X-STREAM-INF:%s\n%s\n", strings.Join(attrs, ","), appendToken(v.URI, p.Token))
	}
	return []byte(b.String()), nil
}

// checkURI 拒绝会让 playlist 结构损坏的 URI 字符。
func checkURI(uri string) error {
	if uri == "" {
		return errors.New("empty URI")
	}
	if strings.ContainsAny(uri, "\"\r\n \t") {
		return fmt.Errorf("URI %q contains quote, whitespace or newline", uri)
	}
	return nil
}

// checkQuoted 校验会放进 quoted-string 的字段。
func checkQuoted(s string) error {
	if strings.ContainsAny(s, "\"\r\n") {
		return fmt.Errorf("value %q contains quote or newline", s)
	}
	return nil
}

// appendToken 给 URI 追加播放令牌；已有 query 时用 &。
func appendToken(uri, token string) string {
	if token == "" {
		return uri
	}
	sep := "?"
	if strings.Contains(uri, "?") {
		sep = "&"
	}
	return uri + sep + "token=" + url.QueryEscape(token)
}

// formatPDT 输出 RFC3339 UTC 毫秒时间，例如 2026-11-18T08:30:00.000Z。
func formatPDT(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

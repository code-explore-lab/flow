package hls

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// ABRVariant 是 ABR 阶梯里的一档。配置来自转码 profile（F-4），
// 流名指向转码器发布到注册表的那一路输出。
type ABRVariant struct {
	Stream    string
	Width     int
	Height    int
	Bitrate   uint32 // 视频编码码率，bits/s；BANDWIDTH 还会加上音频
	FrameRate float64
	Codec     string // "H264" / "H265"
}

// ABRGroup 是同一路源的多档输出。master playlist 按 Source 名聚合。
type ABRGroup struct {
	Source   string
	Variants []ABRVariant
}

// SetABR 配置 ABR 阶梯：请求 /hls/<source>/master.m3u8 时，
// 如果该源的各档已经发布，就输出一个多 variant 的 master playlist。
func (s *Server) SetABR(groups []ABRGroup) {
	m := make(map[string]ABRGroup, len(groups))
	for _, g := range groups {
		if g.Source == "" || len(g.Variants) == 0 {
			continue
		}
		vs := make([]ABRVariant, 0, len(g.Variants))
		for _, v := range g.Variants {
			if v.Stream == "" {
				continue
			}
			vs = append(vs, v)
		}
		if len(vs) == 0 {
			continue
		}
		sort.SliceStable(vs, func(i, j int) bool { return vs[i].Bitrate < vs[j].Bitrate })
		m[g.Source] = ABRGroup{Source: g.Source, Variants: vs}
	}
	s.mu.Lock()
	s.abr = m
	s.mu.Unlock()
}

// ABRRendition 是阶梯里已经发布的一档，连同它的 packager。DASH 用它生成
// 多 Representation MPD。
type ABRRendition struct {
	Variant  ABRVariant
	Packager *Packager
}

// ABRRenditions 返回 name 对应阶梯中已在注册表里发布的档位（按码率升序）。
// 只走注册表，不使用本地文件回退，否则阶梯还没起来时会把测试素材当成
// 真实转码档位发布出去。
func (s *Server) ABRRenditions(ctx context.Context, name string) ([]ABRRendition, bool) {
	g, ok := s.abrGroup(name)
	if !ok {
		return nil, false
	}
	out := make([]ABRRendition, 0, len(g.Variants))
	for _, v := range g.Variants {
		if s.registry.Get(v.Stream) == nil {
			continue
		}
		p := s.packagerFor(ctx, v.Stream)
		if p == nil || p.Video() == nil {
			continue
		}
		out = append(out, ABRRendition{Variant: v, Packager: p})
	}
	return out, true
}

// abrGroup 查找请求名对应的阶梯。先精确匹配，再按最后一段唯一匹配，
// 与 registry 的流名解析保持一致。
func (s *Server) abrGroup(name string) (ABRGroup, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if g, ok := s.abr[name]; ok {
		return g, true
	}
	base := name
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		base = name[i+1:]
	}
	var found ABRGroup
	n := 0
	for src, g := range s.abr {
		if src == base || strings.HasSuffix(src, "/"+base) {
			found = g
			n++
		}
	}
	return found, n == 1
}

// serveABRMaster 尝试为一路源生成 ABR master。所有档位都还没发布时返回
// false，让调用方回退到普通的单流 master。
func (s *Server) serveABRMaster(ctx context.Context, name string, g ABRGroup) ([]Variant, []AudioRendition, int, bool) {
	type ready struct {
		variant ABRVariant
		video   *Track
		audio   *Track
	}
	reps := make([]ready, 0, len(g.Variants))
	for _, v := range g.Variants {
		if s.registry.Get(v.Stream) == nil {
			continue
		}
		p := s.packagerFor(ctx, v.Stream)
		if p == nil || p.Video() == nil {
			continue
		}
		reps = append(reps, ready{variant: v, video: p.Video(), audio: p.Audio()})
	}
	if len(reps) == 0 {
		return nil, nil, 0, false
	}

	// 参数集可能还没写到 init，等一会儿拿 Codecs。解析不出就省略，
	// hls.js 会从 init 段自行探测。
	deadline := time.Now().Add(waitForTracks)
	for _, r := range reps {
		for r.video.Codec() == "" && time.Now().Before(deadline) {
			if !sleepCtx(ctx, 50*time.Millisecond) {
				break
			}
		}
	}

	version := 7
	variants := make([]Variant, 0, len(reps))
	audios := make([]AudioRendition, 0, len(reps))
	for i, r := range reps {
		v := r.variant
		if snap := r.video.Snapshot(); snap.PartTarget > 0 {
			version = 9
		}
		if r.audio != nil {
			if snap := r.audio.Snapshot(); snap.PartTarget > 0 {
				version = 9
			}
		}

		bw := v.Bitrate
		if bw == 0 {
			bw = r.video.Bandwidth()
		}
		group := ""
		if r.audio != nil {
			abw := r.audio.Bandwidth()
			if abw == 0 {
				abw = 64_000
			}
			bw += abw
			group = fmt.Sprintf("aud%d", i)
		}
		if bw == 0 {
			bw = 1_000_000
		}

		width, height := v.Width, v.Height
		if width <= 0 || height <= 0 {
			width, height = r.video.Size()
		}
		res := ""
		if width > 0 && height > 0 {
			res = fmt.Sprintf("%dx%d", width, height)
		}

		codecs := r.video.Codec()
		if codecs == "" {
			codecs = genericCodec(v.Codec)
		}
		if r.audio != nil {
			if ac := r.audio.Codec(); ac != "" {
				if codecs != "" {
					codecs += "," + ac
				} else {
					codecs = ac
				}
			}
			audios = append(audios, AudioRendition{
				GroupID:    group,
				Name:       "audio",
				URI:        hlsStreamURI(v.Stream, "audio.m3u8"),
				Channels:   r.audio.Channels(),
				Default:    i == 0,
				Autoselect: true,
			})
		}
		variants = append(variants, Variant{
			URI:        hlsStreamURI(v.Stream, "video.m3u8"),
			Bandwidth:  bw,
			Resolution: res,
			FrameRate:  v.FrameRate,
			Codecs:     codecs,
			AudioGroup: group,
		})
	}
	s.opts.logf("hls[%s]: ABR master with %d variant(s)", name, len(variants))
	return variants, audios, version, true
}

// hlsStreamURI 返回另一路流在 HLS 分发器下的绝对路径。
func hlsStreamURI(stream, resource string) string {
	return "/hls/" + strings.Trim(stream, "/") + "/" + resource
}

// genericCodec 在 packager 还拿不到参数集时给出保守的 codec 字符串。
// 只用标准 Main Profile，避免声明源不支持的具体 profile。
func genericCodec(codec string) string {
	switch strings.ToUpper(codec) {
	case "H265", "HEVC":
		return "hvc1.1.6.L93.B0"
	case "", "H264", "AVC":
		return "avc1.42E01E"
	}
	return ""
}

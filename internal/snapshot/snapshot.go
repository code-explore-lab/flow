// Package snapshot 从实时流的关键帧生成 JPEG/PNG 截图（F-6）。
//
// 数据链路只做「取一个 IDR 访问单元 → 解码成像素 → 编码成图片」：
//
//	registry.Get(name).SourceFor("video").Subscribe()
//	  → 预热的 SPS/PPS + 最近 GOP
//	  → 收齐同一个 Timestamp 的 IDR slice
//	  → hi264.DecodeNALUs（纯 Go，仅 H.264）
//	  → yuv.WriteJPEGCS / WritePNGCS
//
// H.265 与 hi264 不支持的 H.264 码流（POC type=1、隔行等）可选回退到
// 外部 ffmpeg。解码结果按「流指针 + 格式/质量/宽度」做短 TTL 缓存，
// 并用信号量限制并发解码数；这两点是「CPU 开销可接受」的关键。
package snapshot

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"flow/internal/media"
	"flow/internal/metrics"
	"flow/internal/source"
	"flow/internal/stream"
)

// 输出格式。HTTP 层把请求里的 jpg/jpeg 归一成 FormatJPEG。
const (
	FormatJPEG = "jpeg"
	FormatPNG  = "png"
)

// 取流/等关键帧的默认值。
const (
	DefaultTimeout     = 3 * time.Second
	DefaultCacheTTL    = time.Second
	DefaultConcurrency = 2
	DefaultQuality     = 85
	defaultFFmpegTime  = 5 * time.Second

	// 等推流端建视频轨 / 登记编码参数的最长时间，与 HTTP-FLV 保持一致。
	waitForTracks = 2 * time.Second
	waitForParams = time.Second
)

var (
	// ErrNotFound 表示注册表和本地回退都找不到这路流。
	ErrNotFound = errors.New("snapshot: stream not found")
	// ErrNoVideo 表示流存在但没有视频轨。
	ErrNoVideo = errors.New("snapshot: stream has no video track")
	// ErrNoKeyframe 表示在超时时间内没有等到关键帧。
	ErrNoKeyframe = errors.New("snapshot: no keyframe within timeout")
)

// UnsupportedCodecError 表示当前配置无法解码该编码（通常是没配 ffmpeg 的 H.265）。
type UnsupportedCodecError struct {
	Codec string
}

func (e *UnsupportedCodecError) Error() string {
	return fmt.Sprintf("snapshot: codec %s is not supported (configure -snapshot-ffmpeg for H.265)", e.Codec)
}

// Options 截图行为。零值可用。
type Options struct {
	Timeout       time.Duration // 等待关键帧的最长时间
	CacheTTL      time.Duration // 同一路流复用已编码图片的时长；0 关闭缓存
	Concurrency   int           // 同时进行的解码数上限
	Quality       int           // 默认 JPEG 质量 1..100
	MaxWidth      int           // 默认缩略图最大宽度；0 保持原尺寸
	FFmpegPath    string        // 可选：H.265 / 异常 H.264 的兜底解码器
	FFmpegTimeout time.Duration // ffmpeg 单次解码超时
	Logf          func(format string, args ...any)
}

func (o Options) withDefaults() Options {
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	if o.CacheTTL < 0 {
		o.CacheTTL = 0
	}
	if o.Concurrency < 1 {
		o.Concurrency = DefaultConcurrency
	}
	if o.Quality <= 0 {
		o.Quality = DefaultQuality
	}
	if o.Quality > 100 {
		o.Quality = 100
	}
	if o.MaxWidth < 0 {
		o.MaxWidth = 0
	}
	if o.FFmpegTimeout <= 0 {
		o.FFmpegTimeout = defaultFFmpegTime
	}
	if o.Logf == nil {
		o.Logf = log.Printf
	}
	return o
}

// OriginResolver 按需回源，与 HLS/HTTP-FLV 的接口保持一致，避免 import 环。
type OriginResolver interface {
	Ensure(ctx context.Context, name string) *stream.Stream
}

// Request 一次截图请求的图片参数。
type Request struct {
	Format  string // jpeg / png；空按默认
	Quality int    // 0 用 Options.Quality
	Width   int    // 0 用 Options.MaxWidth
	Fresh   bool   // true 时不用预热 GOP，等下一个关键帧
}

// Image 一次成功截图的结果。
type Image struct {
	Body        []byte
	ContentType string
	PTS         uint32        // 关键帧的 RTP 时间戳（90kHz）
	Age         time.Duration // 距该流最近一次写入媒体的大致时间
	Cached      bool
}

type cacheEntry struct {
	stream *stream.Stream
	at     time.Time
	age    time.Duration
	body   []byte
	ctype  string
	pts    uint32
}

// Grabber 负责从注册表取源、解码和缓存。并发安全。
type Grabber struct {
	registry *stream.Registry
	local    *source.H264Source
	origin   OriginResolver
	metrics  *metrics.Registry

	mu    sync.Mutex
	opts  Options
	sem   chan struct{}
	cache map[string]cacheEntry
}

// NewGrabber 创建截图抓取器。local 是注册表未命中时的本地 testdata 回退源，可为 nil。
func NewGrabber(reg *stream.Registry, local *source.H264Source) *Grabber {
	if reg == nil {
		reg = stream.NewRegistry()
	}
	g := &Grabber{
		registry: reg,
		local:    local,
		cache:    make(map[string]cacheEntry),
	}
	g.setOptions(Options{})
	return g
}

// SetOptions 设置截图参数。应在 Serve 之前调用。
func (g *Grabber) SetOptions(opts Options) {
	g.setOptions(opts)
}

func (g *Grabber) setOptions(opts Options) {
	opts = opts.withDefaults()
	g.mu.Lock()
	g.opts = opts
	g.sem = make(chan struct{}, opts.Concurrency)
	g.mu.Unlock()
}

// SetMetrics 注入进程级指标注册表（nil-safe）。
func (g *Grabber) SetMetrics(reg *metrics.Registry) { g.metrics = reg }

// SetOrigin 注入按需回源解析器。
func (g *Grabber) SetOrigin(o OriginResolver) { g.origin = o }

// Snapshot 截取 name 对应的最近关键帧。
func (g *Grabber) Snapshot(ctx context.Context, name string, req Request) (*Image, error) {
	opts := g.options()
	req = normalizeRequest(req, opts)

	st, sub, codec, params, err := g.resolve(ctx, name)
	if err != nil {
		return nil, err
	}

	key := cacheKey(name, req)
	if img := g.lookup(key, st); img != nil {
		return img, nil
	}

	if err := g.acquire(ctx); err != nil {
		return nil, err
	}
	defer g.release()

	// 等锁期间可能已经有别的请求填好了缓存。
	if img := g.lookup(key, st); img != nil {
		return img, nil
	}

	if media.IsHEVCCodec(codec) && opts.FFmpegPath == "" {
		return nil, &UnsupportedCodecError{Codec: codec}
	}

	start := time.Now()
	kf, err := grabKeyframe(ctx, sub, codec, params, req.Fresh, opts.Timeout)
	if err != nil {
		return nil, err
	}

	var body []byte
	var ctype string
	if !media.IsHEVCCodec(codec) {
		body, ctype, err = decodeAndEncode(kf.nalus, req.Format, req.Quality, req.Width)
	}
	if media.IsHEVCCodec(codec) || err != nil {
		if opts.FFmpegPath == "" {
			if err == nil {
				err = &UnsupportedCodecError{Codec: codec}
			}
			return nil, err
		}
		fctx, cancel := context.WithTimeout(ctx, opts.FFmpegTimeout)
		defer cancel()
		body, ctype, err = encodeWithFFmpeg(fctx, opts.FFmpegPath, codec, kf.nalus, req.Format, req.Quality, req.Width)
		if err != nil {
			return nil, err
		}
	}

	img := &Image{Body: body, ContentType: ctype, PTS: kf.pts}
	if st != nil {
		if src := st.SourceFor("video"); src != nil {
			if lw := src.LastWrite(); !lw.IsZero() {
				img.Age = time.Since(lw)
			}
		}
	}

	if g.metrics != nil {
		g.metrics.AddSnapshotDecode(time.Since(start))
	}
	g.store(key, st, img)
	return img, nil
}

func (g *Grabber) options() Options {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.opts
}

func (g *Grabber) acquire(ctx context.Context) error {
	g.mu.Lock()
	sem := g.sem
	g.mu.Unlock()
	select {
	case sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *Grabber) release() {
	g.mu.Lock()
	sem := g.sem
	g.mu.Unlock()
	<-sem
}

// resolve 按流名找视频源：先精确匹配注册表，再按最后一段匹配（RTSP 注册
// 的是 live/<name>，RTMP 是裸名），然后按需回源，最后回退本地 testdata。
func (g *Grabber) resolve(ctx context.Context, name string) (*stream.Stream, videoSource, string, *stream.VideoCodec, error) {
	st := g.registry.Get(name)
	if st == nil {
		if i := strings.LastIndexByte(name, '/'); i >= 0 && i+1 < len(name) {
			st = g.registry.Get(name[i+1:])
		}
	}
	if st == nil && g.origin != nil {
		st = g.origin.Ensure(ctx, name)
	}
	if st == nil {
		if g.local == nil {
			return nil, nil, "", nil, ErrNotFound
		}
		// 与 RTMP/HTTP-FLV 的本地回退一致：没有推流时预览 testdata。
		return nil, g.local, "H264", nil, nil
	}

	deadline := time.Now().Add(waitForTracks)
	for st.SourceFor("video") == nil {
		if time.Now().After(deadline) {
			return nil, nil, "", nil, ErrNoVideo
		}
		if !sleepCtx(ctx, 50*time.Millisecond) {
			return nil, nil, "", nil, ctx.Err()
		}
	}

	src := st.SourceFor("video")
	params, ok := st.VideoParams()
	if !ok {
		// RTMP 的 SPS/PPS 在序列头里，晚于 publish 命令到达。
		paramDeadline := time.Now().Add(waitForParams)
		for !ok && time.Now().Before(paramDeadline) {
			if !sleepCtx(ctx, 20*time.Millisecond) {
				return nil, nil, "", nil, ctx.Err()
			}
			params, ok = st.VideoParams()
		}
	}
	codec := "H264"
	if ok && params != nil && params.Codec != "" {
		codec = params.Codec
	}
	return st, src, codec, params, nil
}

func normalizeRequest(req Request, opts Options) Request {
	switch strings.ToLower(strings.TrimSpace(req.Format)) {
	case FormatPNG:
		req.Format = FormatPNG
	default:
		req.Format = FormatJPEG
	}
	if req.Quality <= 0 {
		req.Quality = opts.Quality
	}
	if req.Quality > 100 {
		req.Quality = 100
	}
	if req.Width <= 0 {
		req.Width = opts.MaxWidth
	}
	return req
}

func cacheKey(name string, req Request) string {
	return fmt.Sprintf("%s|%s|%d|%d|%t", name, req.Format, req.Quality, req.Width, req.Fresh)
}

func (g *Grabber) lookup(key string, st *stream.Stream) *Image {
	opts := g.options()
	if opts.CacheTTL <= 0 {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	e, ok := g.cache[key]
	if !ok || e.stream != st {
		return nil
	}
	age := time.Since(e.at)
	if age >= opts.CacheTTL {
		delete(g.cache, key)
		return nil
	}
	if g.metrics != nil {
		g.metrics.IncSnapshotCacheHit()
	}
	return &Image{
		Body:        e.body,
		ContentType: e.ctype,
		PTS:         e.pts,
		Age:         e.age + age,
		Cached:      true,
	}
}

func (g *Grabber) store(key string, st *stream.Stream, img *Image) {
	opts := g.options()
	if opts.CacheTTL <= 0 {
		return
	}
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	g.cache[key] = cacheEntry{
		stream: st,
		at:     now,
		age:    img.Age,
		body:   img.Body,
		ctype:  img.ContentType,
		pts:    img.PTS,
	}
	if len(g.cache) > 64 {
		for k, e := range g.cache {
			if now.Sub(e.at) > opts.CacheTTL*4 {
				delete(g.cache, k)
			}
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

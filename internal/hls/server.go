// HLS HTTP 分发与 LL-HLS 阻塞重载（P-2-5）。
//
// 打包部分（segmenter/packager/manager）把共享注册表里的一路流拆成独立的
// 视频/音频 rendition；HTTP 部分负责：
//
//	/hls/<name>/master.m3u8
//	/hls/<name>/video.m3u8 | audio.m3u8
//	/hls/<name>/video|audio/init[-N].mp4
//	/hls/<name>/video|audio/seg-<msn>.m4s
//	/hls/<name>/video|audio/part-<msn>-<index>.m4s
//	/hls/<name>/stats.json
//
// 支持 LL-HLS 的阻塞重载（_HLS_msn/_HLS_part）和 preload hint part 阻塞：
// 请求到达时目标 part 还没生成，handler 等待打包器的通知 channel，而不是轮询。
// 安全策略（IP 白名单、CORS、Referer、token）在进入媒体响应之前完成。
package hls

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"flow/internal/access"
	"flow/internal/health"
	"flow/internal/media"
	"flow/internal/metrics"
	"flow/internal/source"
	"flow/internal/stream"
)

const (
	playlistContentType = "application/vnd.apple.mpegurl"
	mp4ContentType      = "video/mp4"

	// waitForTracks 是等待音轨参数（RTMP 序列头/RTSP SDP）的最长时间。
	waitForTracks = 2 * time.Second
	// maxBlockWait 限制单次阻塞重载/part 等待，防止客户端挂住连接。
	maxBlockWait = 10 * time.Second
)

// OriginResolver 按需回源。接口定义在本包，cmd/server 注入 origin.Manager，
// 避免 hls → origin → rtsp/rtmp 的 import 环。
type OriginResolver interface {
	Ensure(ctx context.Context, name string) *stream.Stream
}

// Server HLS HTTP 服务器。配置项必须在 Serve/第一个请求之前设置。
type Server struct {
	registry   *stream.Registry
	localVideo *source.H264Source
	localAudio *source.AACSource
	whitelist  *access.Whitelist
	origin     OriginResolver

	cors           []string
	referers       []string
	requireReferer bool
	token          string

	mu             sync.Mutex
	manager        *Manager
	opts           Options
	idle           time.Duration
	canBlockReload bool
	metrics        *metrics.Registry
	abr            map[string]ABRGroup

	audioOnce  sync.Once
	audioCodec *stream.AudioCodec
}

// New 构造 HLS 服务器。reg 为 nil 时自建；video/audio 是本地文件回退源，可为 nil。
func New(reg *stream.Registry, video *source.H264Source, audio *source.AACSource) *Server {
	if reg == nil {
		reg = stream.NewRegistry()
	}
	return &Server{
		registry:       reg,
		localVideo:     video,
		localAudio:     audio,
		opts:           Options{SegmentTarget: 4 * time.Second, PartTarget: 500 * time.Millisecond, Window: 6, MaxBytes: 64 << 20},
		idle:           60 * time.Second,
		canBlockReload: true,
	}
}

// SetWhitelist 注入 IP 白名单。nil 表示不启用。
func (s *Server) SetWhitelist(w *access.Whitelist) { s.whitelist = w }

// SetOrigin 注入按需回源解析器。
func (s *Server) SetOrigin(o OriginResolver) { s.origin = o }

// SetCORS 设置允许的 Origin，逗号分隔；"*" 表示任意来源。
func (s *Server) SetCORS(origins string) { s.cors = splitList(origins) }

// SetReferer 设置防盗链允许的 Referer 主机名，逗号分隔。
// require 为 true 时，没有 Referer 头的请求也拒绝。
func (s *Server) SetReferer(list string, require bool) {
	s.referers = splitList(list)
	s.requireReferer = require
}

// SetToken 设置播放令牌。非空时请求必须带 ?token=... 或 X-Flow-Token 头；
// playlist 里的 URI 会自动附带 token。
func (s *Server) SetToken(token string) { s.token = token }

// SetCanBlockReload 控制是否接受 _HLS_msn/_HLS_part 阻塞请求。
func (s *Server) SetCanBlockReload(v bool) { s.canBlockReload = v }

// SetMetrics 注入进程级指标注册表（nil-safe）。
func (s *Server) SetMetrics(reg *metrics.Registry) {
	if reg == nil {
		return
	}
	s.mu.Lock()
	s.metrics = reg
	m := s.manager
	s.mu.Unlock()
	if m != nil {
		m.SetMetrics(reg)
	}
}

// SetPackaging 设置切片参数和 idle 回收时间。必须在 Serve/第一个请求之前调用。
func (s *Server) SetPackaging(opts Options, idle time.Duration) {
	opts = opts.withDefaults()
	if idle < 0 {
		idle = 0
	}
	s.mu.Lock()
	if s.manager != nil {
		s.manager.Close()
		s.manager = nil
	}
	s.opts = opts
	s.idle = idle
	s.mu.Unlock()
}

// SetManager 注入外部 manager（例如 cmd/server 想统一管理）。传 nil 时保留内部实例。
func (s *Server) SetManager(m *Manager) {
	if m == nil {
		return
	}
	s.mu.Lock()
	if s.manager != nil && s.manager != m {
		s.manager.Close()
	}
	s.manager = m
	if s.metrics != nil {
		m.SetMetrics(s.metrics)
	}
	s.mu.Unlock()
}

func (s *Server) getManager() *Manager {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.manager == nil {
		s.manager = NewManager(s.opts, s.idle)
		s.manager.SetMetrics(s.metrics)
	}
	return s.manager
}

// Close 关闭内部 manager（幂等）。Serve 返回时也会调用。
func (s *Server) Close() {
	s.mu.Lock()
	m := s.manager
	s.manager = nil
	s.mu.Unlock()
	if m != nil {
		m.Close()
	}
}

// Serve 启动 HTTP 服务，ctx 取消时关闭监听并断开现有连接。
func (s *Server) Serve(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	health.NotifyListenerReady(ctx)
	srv := &http.Server{
		Handler:           s,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	fmt.Printf("hls server listen on %s\n", addr)
	err = srv.Serve(ln)
	s.Close()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// ServeHTTP 路由并处理一个请求。
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.applyCORS(w, r)

	ip := remoteHost(r.RemoteAddr)
	if s.whitelist != nil && s.whitelist.Enabled() && !s.whitelist.Allows(ip) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	if r.Method == http.MethodOptions {
		w.Header().Set("Allow", "GET, HEAD, OPTIONS")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD, OPTIONS")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !s.refererAllowed(r) {
		http.Error(w, "referer not allowed", http.StatusForbidden)
		return
	}
	if !s.tokenAllowed(r) {
		http.Error(w, "invalid token", http.StatusForbidden)
		return
	}

	route, ok := parseHLSPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	switch route.kind {
	case routeMaster:
		s.serveMaster(w, r, route.name)
	case routeMediaVideo:
		s.serveMedia(w, r, route.name, "video")
	case routeMediaAudio:
		s.serveMedia(w, r, route.name, "audio")
	case routeStats:
		s.serveStats(w, r, route.name)
	case routeBinary:
		s.serveBinary(w, r, route)
	default:
		http.NotFound(w, r)
	}
}

// ---- 路由 ----

type routeKind uint8

const (
	routeMaster routeKind = iota
	routeMediaVideo
	routeMediaAudio
	routeStats
	routeBinary
)

type hlsRoute struct {
	kind     routeKind
	name     string
	track    string // video / audio
	resource string // track 下的资源名，如 seg-0.m4s
	msn      uint64
	part     int
	version  int
}

// parseHLSPath 解析 /hls/ 下的路径。流名可以包含 /，从资源后缀反推。
func parseHLSPath(p string) (hlsRoute, bool) {
	const prefix = "/hls/"
	if !strings.HasPrefix(p, prefix) {
		return hlsRoute{}, false
	}
	rest := strings.TrimPrefix(p, prefix)
	if rest == "" || strings.Contains(rest, "..") || strings.Contains(rest, "//") || strings.ContainsRune(rest, '\\') {
		return hlsRoute{}, false
	}

	for _, suffix := range []string{"/master.m3u8", "/index.m3u8"} {
		if strings.HasSuffix(rest, suffix) {
			name := strings.TrimSuffix(rest, suffix)
			if validStreamName(name) {
				return hlsRoute{kind: routeMaster, name: name}, true
			}
			return hlsRoute{}, false
		}
	}
	if strings.HasSuffix(rest, "/stats.json") {
		name := strings.TrimSuffix(rest, "/stats.json")
		if validStreamName(name) {
			return hlsRoute{kind: routeStats, name: name}, true
		}
		return hlsRoute{}, false
	}
	for _, item := range []struct {
		suffix string
		kind   routeKind
	}{
		{"/video.m3u8", routeMediaVideo},
		{"/audio.m3u8", routeMediaAudio},
	} {
		if strings.HasSuffix(rest, item.suffix) {
			name := strings.TrimSuffix(rest, item.suffix)
			if validStreamName(name) {
				return hlsRoute{kind: item.kind, name: name, track: strings.TrimSuffix(strings.TrimPrefix(item.suffix, "/"), ".m3u8")}, true
			}
			return hlsRoute{}, false
		}
	}

	for _, track := range []string{"video", "audio"} {
		marker := "/" + track + "/"
		idx := strings.LastIndex(rest, marker)
		if idx < 0 {
			continue
		}
		name := rest[:idx]
		resource := rest[idx+len(marker):]
		if !validStreamName(name) || resource == "" || strings.Contains(resource, "/") {
			return hlsRoute{}, false
		}
		r := hlsRoute{kind: routeBinary, name: name, track: track, resource: resource, part: -1}
		switch {
		case resource == "init.mp4":
			r.version = 0
		case strings.HasPrefix(resource, "init-") && strings.HasSuffix(resource, ".mp4"):
			n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(resource, "init-"), ".mp4"))
			if err != nil || n <= 0 {
				return hlsRoute{}, false
			}
			r.version = n
		case strings.HasPrefix(resource, "seg-") && strings.HasSuffix(resource, ".m4s"):
			n, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(resource, "seg-"), ".m4s"), 10, 64)
			if err != nil {
				return hlsRoute{}, false
			}
			r.msn = n
		case strings.HasPrefix(resource, "part-") && strings.HasSuffix(resource, ".m4s"):
			body := strings.TrimSuffix(strings.TrimPrefix(resource, "part-"), ".m4s")
			parts := strings.Split(body, "-")
			if len(parts) != 2 {
				return hlsRoute{}, false
			}
			msn, err1 := strconv.ParseUint(parts[0], 10, 64)
			index, err2 := strconv.Atoi(parts[1])
			if err1 != nil || err2 != nil || index < 0 {
				return hlsRoute{}, false
			}
			r.msn = msn
			r.part = index
		default:
			return hlsRoute{}, false
		}
		return r, true
	}
	return hlsRoute{}, false
}

func validStreamName(name string) bool {
	if name == "" || len(name) > 512 || strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") {
		return false
	}
	for _, part := range strings.Split(name, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	for _, c := range name {
		if c < 0x20 || c == 0x7f {
			return false
		}
	}
	return true
}

// ---- 解析 packager ----

// PackagerFor 是 packagerFor 的导出形式。P-3 DASH 与 HLS 共享同一条切片管线：
// DASH 服务器通过它拿到 HLS manager 里已经切好的段，不重复打包。
func (s *Server) PackagerFor(ctx context.Context, name string) *Packager {
	return s.packagerFor(ctx, name)
}

// packagerFor 找到请求对应的流并返回 manager 里的 packager。
func (s *Server) packagerFor(ctx context.Context, name string) *Packager {
	if st := s.resolveStream(ctx, name); st != nil {
		spec, ok := s.waitStreamSpec(ctx, st)
		if !ok {
			return nil
		}
		return s.getManager().Get(spec)
	}
	if spec, ok := s.localSpec(name); ok {
		return s.getManager().Get(spec)
	}
	return nil
}

func (s *Server) resolveStream(ctx context.Context, name string) *stream.Stream {
	if st := s.registry.Get(name); st != nil {
		return st
	}
	if i := strings.LastIndexByte(name, '/'); i >= 0 && i+1 < len(name) {
		if st := s.registry.Get(name[i+1:]); st != nil {
			return st
		}
	}
	if s.origin == nil {
		return nil
	}
	if st := s.origin.Ensure(ctx, name); st != nil {
		return st
	}
	if i := strings.LastIndexByte(name, '/'); i >= 0 && i+1 < len(name) {
		return s.registry.Get(name[i+1:])
	}
	return nil
}

// waitStreamSpec 等 RTMP 序列头/RTSP SDP 把音频参数补齐。视频可以没有参数，
// packager 会从带内 SPS/PPS 捕获。
func (s *Server) waitStreamSpec(ctx context.Context, st *stream.Stream) (PackagerSpec, bool) {
	deadline := time.Now().Add(waitForTracks)
	for {
		spec := SpecFromStream(st)
		hasAudio := st.SourceFor("audio") != nil
		ready := !hasAudio || spec.Audio != nil
		if ready && (spec.Video != nil || spec.Audio != nil) {
			return spec, true
		}
		if time.Now().After(deadline) {
			return spec, spec.Video != nil || spec.Audio != nil
		}
		if !sleepCtx(ctx, 50*time.Millisecond) {
			return spec, false
		}
	}
}

func (s *Server) localSpec(name string) (PackagerSpec, bool) {
	spec := PackagerSpec{Name: name}
	if s.localVideo != nil {
		spec.Video = &TrackSource{Subscriber: s.localVideo}
	}
	if s.localAudio != nil {
		if ac := s.localAudioParams(); ac != nil {
			spec.Audio = &TrackSource{Subscriber: s.localAudio, Audio: ac}
		}
	}
	if spec.Video == nil && spec.Audio == nil {
		return spec, false
	}
	return spec, true
}

func (s *Server) localAudioParams() *stream.AudioCodec {
	s.audioOnce.Do(func() {
		if s.localAudio == nil {
			return
		}
		info, err := media.GetAACInfo(s.localAudio.Path())
		if err != nil {
			log.Printf("hls: read local aac info from %s: %v", s.localAudio.Path(), err)
			return
		}
		rate := uint32(info.SamplingFrequency)
		s.audioCodec = &stream.AudioCodec{
			Codec:      stream.AudioAAC,
			Config:     info.Config,
			SampleRate: rate,
			ClockRate:  rate,
			Channels:   info.ChannelConfig,
		}
	})
	return s.audioCodec
}

// ---- master ----

func (s *Server) serveMaster(w http.ResponseWriter, r *http.Request, name string) {
	s.metrics.IncHLSRequest("master")
	if g, ok := s.abrGroup(name); ok {
		if variants, audios, version, ok := s.serveABRMaster(r.Context(), name, g); ok {
			m := &MasterPlaylist{
				Version:             version,
				IndependentSegments: true,
				Variants:            variants,
				Audios:              audios,
				Token:               s.token,
			}
			b, err := m.Generate()
			if err != nil {
				s.opts.logf("hls[%s]: ABR master playlist: %v", name, err)
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
			s.writePlaylist(w, r, b)
			return
		}
	}
	p := s.packagerFor(r.Context(), name)
	if p == nil {
		http.NotFound(w, r)
		return
	}
	v, a := p.Video(), p.Audio()
	if v == nil && a == nil {
		http.NotFound(w, r)
		return
	}

	partTarget := time.Duration(0)
	if v != nil {
		partTarget = v.Snapshot().PartTarget
	}
	if partTarget == 0 && a != nil {
		partTarget = a.Snapshot().PartTarget
	}
	version := 7
	if partTarget > 0 {
		version = 9
	}

	var variants []Variant
	var audio *AudioRendition
	if v != nil {
		bw := v.Bandwidth()
		if a != nil {
			bw += a.Bandwidth()
		}
		if bw == 0 {
			bw = 1_000_000
		}
		res := ""
		if width, height := v.Size(); width > 0 && height > 0 {
			res = fmt.Sprintf("%dx%d", width, height)
		}
		codecs := v.Codec()
		group := ""
		if a != nil {
			group = "aud"
			if codecs != "" && a.Codec() != "" {
				codecs += "," + a.Codec()
			}
		}
		variants = append(variants, Variant{
			URI:        "video.m3u8",
			Bandwidth:  bw,
			Resolution: res,
			Codecs:     codecs,
			AudioGroup: group,
		})
	}
	if a != nil {
		if v == nil {
			bw := a.Bandwidth()
			if bw == 0 {
				bw = 64_000
			}
			variants = append(variants, Variant{
				URI:       "audio.m3u8",
				Bandwidth: bw,
				Codecs:    a.Codec(),
			})
		} else {
			audio = &AudioRendition{
				GroupID:    "aud",
				Name:       "audio",
				URI:        "audio.m3u8",
				Channels:   a.Channels(),
				Default:    true,
				Autoselect: true,
			}
		}
	}

	m := &MasterPlaylist{
		Version:             version,
		IndependentSegments: true,
		Variants:            variants,
		Audio:               audio,
		Token:               s.token,
	}
	b, err := m.Generate()
	if err != nil {
		s.opts.logf("hls[%s]: master playlist: %v", name, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	s.writePlaylist(w, r, b)
}

// ---- media ----

func (s *Server) serveMedia(w http.ResponseWriter, r *http.Request, name, track string) {
	s.metrics.IncHLSRequest("media")
	p := s.packagerFor(r.Context(), name)
	if p == nil {
		http.NotFound(w, r)
		return
	}
	tr, other, otherKind := p.Video(), p.Audio(), "audio"
	if track == "audio" {
		tr, other, otherKind = p.Audio(), p.Video(), "video"
	}
	if tr == nil {
		http.NotFound(w, r)
		return
	}

	msn, part, block, ok := parseBlockingQuery(r)
	if !ok {
		http.Error(w, "invalid _HLS_msn/_HLS_part", http.StatusBadRequest)
		return
	}
	if block && s.canBlockReload {
		if !s.waitMedia(r.Context(), tr, msn, part) && r.Context().Err() != nil {
			return
		}
	}

	snap := tr.Snapshot()
	mp := &MediaPlaylist{
		TargetDuration: snap.TargetDuration,
		PartTarget:     snap.PartTarget.Seconds(),
		Segments:       snap.Segments,
		Open:           snap.Open,
		EndList:        snap.Closed,
		Token:          s.token,
		CanBlockReload: s.canBlockReload,
	}
	if other != nil {
		if osnap := other.Snapshot(); len(osnap.Segments) > 0 || osnap.Open != nil {
			omsn, opart := other.Progress()
			mp.Rendition = &RenditionReport{URI: otherKind + ".m3u8", LastMSN: omsn, LastPart: opart}
		}
	}
	b, err := mp.Generate()
	if err != nil {
		s.opts.logf("hls[%s/%s]: media playlist: %v", name, track, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	s.writePlaylist(w, r, b)
}

func parseBlockingQuery(r *http.Request) (msn uint64, part int, block, ok bool) {
	q := r.URL.Query()
	ms := q.Get("_HLS_msn")
	ps := q.Get("_HLS_part")
	if ms == "" {
		if ps != "" {
			return 0, -1, false, false
		}
		return 0, -1, false, true
	}
	n, err := strconv.ParseUint(ms, 10, 64)
	if err != nil {
		return 0, -1, false, false
	}
	if ps == "" {
		return n, -1, true, true
	}
	p, err := strconv.Atoi(ps)
	if err != nil || p < 0 {
		return 0, -1, false, false
	}
	return n, p, true, true
}

// waitMedia 阻塞到请求的 msn/part 已生成、窗口已经滑过它，或超时。
// 返回 false 只表示超时/取消；调用方仍会用当前快照生成 200 列表。
func (s *Server) waitMedia(ctx context.Context, tr *Track, msn uint64, part int) bool {
	snap := tr.Snapshot()
	timeout := 2 * time.Duration(snap.TargetDuration) * time.Second
	if snap.PartTarget > 0 {
		timeout = 2 * snap.PartTarget
	}
	if timeout < time.Second {
		timeout = time.Second
	}
	if timeout > maxBlockWait {
		timeout = maxBlockWait
	}
	deadline := time.Now().Add(timeout)
	for {
		snap = tr.Snapshot()
		if mediaSatisfied(snap, msn, part) || snap.Closed {
			return true
		}
		watch := tr.Watch()
		// Watch 之后重新取一次，避免在 snapshot 与取 channel 之间漏掉通知。
		snap = tr.Snapshot()
		if mediaSatisfied(snap, msn, part) || snap.Closed {
			return true
		}
		remain := time.Until(deadline)
		if remain <= 0 {
			return false
		}
		timer := time.NewTimer(remain)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-watch:
			timer.Stop()
		case <-timer.C:
			return false
		}
	}
}

func mediaSatisfied(s Snapshot, msn uint64, part int) bool {
	if len(s.Segments) > 0 && s.Segments[0].MSN > msn {
		return true
	}
	for _, seg := range s.Segments {
		if seg.MSN == msn || seg.MSN > msn {
			return true
		}
	}
	if s.Open != nil {
		if s.Open.MSN == msn {
			if part < 0 {
				return false
			}
			return len(s.Open.Parts) > part
		}
		if s.Open.MSN > msn {
			return true
		}
	}
	return s.Closed
}

// ---- 二进制 ----

func (s *Server) serveBinary(w http.ResponseWriter, r *http.Request, route hlsRoute) {
	kind := binaryKind(route.resource)
	s.metrics.IncHLSRequest(kind)
	p := s.packagerFor(r.Context(), route.name)
	if p == nil {
		http.NotFound(w, r)
		return
	}
	tr := p.Video()
	if route.track == "audio" {
		tr = p.Audio()
	}
	if tr == nil {
		http.NotFound(w, r)
		return
	}
	uri := route.track + "/" + route.resource
	serve := func(data []byte) {
		if r.Method != http.MethodHead {
			s.metrics.AddHLSBytes(kind, len(data))
		}
		s.writeBinary(w, r, uri, data)
	}
	if data, ok := tr.Data(uri); ok {
		serve(data)
		return
	}
	if route.part >= 0 && s.waitPart(r.Context(), tr, route.msn, route.part) {
		if data, ok := tr.Data(uri); ok {
			serve(data)
			return
		}
	}
	http.NotFound(w, r)
}

func binaryKind(resource string) string {
	switch {
	case strings.HasPrefix(resource, "part-"):
		return "part"
	case strings.HasPrefix(resource, "seg-"):
		return "segment"
	default:
		return "init"
	}
}

// waitPart 等待 preload hint 指向的下一个 part 被生成。
func (s *Server) waitPart(ctx context.Context, tr *Track, msn uint64, part int) bool {
	snap := tr.Snapshot()
	if snap.PartTarget <= 0 {
		return false
	}
	if snap.Open == nil || snap.Open.MSN != msn || len(snap.Open.Parts) != part {
		return false
	}
	timeout := 3 * snap.PartTarget
	if timeout < time.Second {
		timeout = time.Second
	}
	if timeout > maxBlockWait {
		timeout = maxBlockWait
	}
	deadline := time.Now().Add(timeout)
	for {
		if _, ok := tr.Data(tr.partURI(msn, part)); ok {
			return true
		}
		snap = tr.Snapshot()
		if snap.Closed || snap.Open == nil || snap.Open.MSN != msn || len(snap.Open.Parts) != part {
			return false
		}
		watch := tr.Watch()
		if _, ok := tr.Data(tr.partURI(msn, part)); ok {
			return true
		}
		remain := time.Until(deadline)
		if remain <= 0 {
			return false
		}
		timer := time.NewTimer(remain)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-watch:
			timer.Stop()
		case <-timer.C:
			return false
		}
	}
}

// ---- stats ----

type hlsTrackStats struct {
	Kind           string  `json:"kind"`
	Codec          string  `json:"codec,omitempty"`
	Width          int     `json:"width,omitempty"`
	Height         int     `json:"height,omitempty"`
	TargetDuration int     `json:"targetDuration"`
	PartTarget     float64 `json:"partTarget"`
	Segments       int     `json:"segments"`
	Closed         bool    `json:"closed"`
	ProgressMSN    uint64  `json:"progressMsn"`
	ProgressPart   int     `json:"progressPart"`
	Bandwidth      uint32  `json:"bandwidth"`
}

type hlsStats struct {
	Name   string                   `json:"name"`
	Tracks map[string]hlsTrackStats `json:"tracks"`
}

func (s *Server) serveStats(w http.ResponseWriter, r *http.Request, name string) {
	s.metrics.IncHLSRequest("stats")
	p := s.packagerFor(r.Context(), name)
	if p == nil {
		http.NotFound(w, r)
		return
	}
	out := hlsStats{Name: p.Name(), Tracks: map[string]hlsTrackStats{}}
	for _, tr := range []*Track{p.Video(), p.Audio()} {
		if tr == nil {
			continue
		}
		snap := tr.Snapshot()
		width, height := tr.Size()
		msn, part := tr.Progress()
		out.Tracks[tr.Kind()] = hlsTrackStats{
			Kind:           tr.Kind(),
			Codec:          tr.Codec(),
			Width:          width,
			Height:         height,
			TargetDuration: snap.TargetDuration,
			PartTarget:     snap.PartTarget.Seconds(),
			Segments:       len(snap.Segments),
			Closed:         snap.Closed,
			ProgressMSN:    msn,
			ProgressPart:   part,
			Bandwidth:      tr.Bandwidth(),
		}
	}
	b, err := json.Marshal(out)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(b)
	}
}

// ---- 响应 ----

func (s *Server) writePlaylist(w http.ResponseWriter, r *http.Request, b []byte) {
	h := w.Header()
	h.Set("Content-Type", playlistContentType)
	h.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	h.Set("Pragma", "no-cache")
	h.Set("Expires", "0")
	h.Set("X-Accel-Buffering", "no")
	h.Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(b)
	}
}

func (s *Server) writeBinary(w http.ResponseWriter, r *http.Request, uri string, data []byte) {
	h := w.Header()
	h.Set("Content-Type", mp4ContentType)
	h.Set("Cache-Control", "public, max-age=31536000, immutable")
	http.ServeContent(w, r, path.Base(uri), time.Time{}, bytes.NewReader(data))
}

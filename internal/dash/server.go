package dash

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"flow/internal/access"
	"flow/internal/hls"
	"flow/internal/metrics"
)

const (
	manifestContentType = "application/dash+xml"
	mp4ContentType      = "video/mp4"

	// maxBlockWait 限制等首段的最长时间，避免客户端连接长期挂起。
	maxBlockWait = 10 * time.Second
)

// PackagerProvider 提供按流名复用的 HLS packager。cmd/server 注入同一个
// hls.Server，因此 HLS 与 DASH 共用一份切片结果和内存窗口。
type PackagerProvider interface {
	PackagerFor(ctx context.Context, name string) *hls.Packager
}

// Server MPEG-DASH 分发器。配置项必须在 Serve/第一个请求之前设置。
type Server struct {
	provider  PackagerProvider
	opts      Options
	whitelist *access.Whitelist

	cors           []string
	referers       []string
	requireReferer bool
	token          string
	metrics        *metrics.Registry
}

// New 构造 DASH 服务器。provider 不能为 nil。
func New(provider PackagerProvider) *Server {
	return &Server{
		provider: provider,
		opts: Options{
			MinimumUpdatePeriod: 2 * time.Second,
			SegmentTarget:       4 * time.Second,
		},
	}
}

// SetWhitelist 注入 IP 白名单。nil 表示不启用。
func (s *Server) SetWhitelist(w *access.Whitelist) { s.whitelist = w }

// SetCORS 设置允许的 Origin，逗号分隔；"*" 表示任意来源。
func (s *Server) SetCORS(origins string) { s.cors = splitList(origins) }

// SetReferer 设置防盗链允许的 Referer 主机名，逗号分隔。
func (s *Server) SetReferer(list string, require bool) {
	s.referers = splitList(list)
	s.requireReferer = require
}

// SetToken 设置播放令牌。非空时 MPD 和段的请求必须带 ?token=... 或
// X-Flow-Token；MPD 里的 initialization/media 模板会自动附带 token，
// 因为 dash.js 自己拼段 URL，不会继承 MPD 请求的查询参数。
func (s *Server) SetToken(token string) { s.token = token }

// SetMetrics 注入进程级指标注册表（nil-safe）。
func (s *Server) SetMetrics(reg *metrics.Registry) { s.metrics = reg }

// SetManifestOptions 设置 MPD 时间参数。SegmentTarget 应等于 HLS 的
// -hls-segment，因为两者共用同一份切片。
func (s *Server) SetManifestOptions(opts Options) { s.opts = opts }

// Serve 启动 HTTP 服务，ctx 取消时关闭监听并断开现有连接。
func (s *Server) Serve(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler:           s,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	fmt.Printf("dash server listen on %s\n", addr)
	err = srv.Serve(ln)
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

	route, ok := parseDashPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	switch route.kind {
	case routeManifest:
		s.serveManifest(w, r, route.name)
	case routeUTC:
		s.serveUTC(w, r)
	case routeBinary:
		s.serveBinary(w, r, route)
	default:
		http.NotFound(w, r)
	}
}

// ---- 路由 ----

type routeKind uint8

const (
	routeManifest routeKind = iota
	routeUTC
	routeBinary
)

type dashRoute struct {
	kind     routeKind
	name     string
	track    string
	resource string
}

// parseDashPath 解析 /dash/ 下的路径。流名可以包含 /，从固定后缀反推。
func parseDashPath(p string) (dashRoute, bool) {
	const prefix = "/dash/"
	if !strings.HasPrefix(p, prefix) {
		return dashRoute{}, false
	}
	rest := strings.TrimPrefix(p, prefix)
	if rest == "" || strings.Contains(rest, "..") || strings.Contains(rest, "//") || strings.ContainsRune(rest, '\\') {
		return dashRoute{}, false
	}

	for _, suffix := range []string{"/manifest.mpd", "/index.mpd"} {
		if strings.HasSuffix(rest, suffix) {
			name := strings.TrimSuffix(rest, suffix)
			if validStreamName(name) {
				return dashRoute{kind: routeManifest, name: name}, true
			}
			return dashRoute{}, false
		}
	}
	if strings.HasSuffix(rest, "/utc") {
		name := strings.TrimSuffix(rest, "/utc")
		if validStreamName(name) {
			return dashRoute{kind: routeUTC, name: name}, true
		}
		return dashRoute{}, false
	}

	for _, track := range []string{"video", "audio"} {
		marker := "/" + track + "/"
		idx := strings.LastIndex(rest, marker)
		if idx < 0 {
			continue
		}
		name := rest[:idx]
		resource := rest[idx+len(marker):]
		if !validStreamName(name) || strings.Contains(resource, "/") || !validResource(resource) {
			return dashRoute{}, false
		}
		return dashRoute{kind: routeBinary, name: name, track: track, resource: resource}, true
	}
	return dashRoute{}, false
}

func validResource(resource string) bool {
	switch {
	case resource == "init.mp4":
		return true
	case strings.HasPrefix(resource, "init-") && strings.HasSuffix(resource, ".mp4"):
		n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(resource, "init-"), ".mp4"))
		return err == nil && n > 0
	case strings.HasPrefix(resource, "seg-") && strings.HasSuffix(resource, ".m4s"):
		_, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(resource, "seg-"), ".m4s"), 10, 64)
		return err == nil
	default:
		return false
	}
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

// ---- 处理 ----

func (s *Server) serveManifest(w http.ResponseWriter, r *http.Request, name string) {
	s.metrics.IncDashRequest("manifest")
	p := s.provider.PackagerFor(r.Context(), name)
	if p == nil {
		http.NotFound(w, r)
		return
	}
	s.waitReady(r.Context(), p)
	if r.Context().Err() != nil {
		return
	}

	opts := s.opts
	opts.Token = s.token
	b, err := Build(p, time.Now(), opts)
	if err != nil {
		if errors.Is(err, ErrNoSegments) {
			w.Header().Set("Retry-After", "2")
			http.Error(w, "stream not ready", http.StatusServiceUnavailable)
			return
		}
		log.Printf("dash[%s]: build manifest: %v", name, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	h := w.Header()
	h.Set("Content-Type", manifestContentType)
	h.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	h.Set("Pragma", "no-cache")
	h.Set("Expires", "0")
	h.Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(b)
	}
}

// waitReady 等音视频轨各自至少产生一个完整段；超时或流结束时返回，
// 由 Build 决定是否还能生成 MPD。
func (s *Server) waitReady(ctx context.Context, p *hls.Packager) {
	deadline := time.Now().Add(maxBlockWait)
	for {
		if Ready(p) {
			return
		}
		if p.Closed() {
			return
		}
		var vch, ach <-chan struct{}
		if v := p.Video(); v != nil && len(latestSegments(v.Snapshot())) == 0 && !v.Closed() {
			vch = v.Watch()
		}
		if a := p.Audio(); a != nil && len(latestSegments(a.Snapshot())) == 0 && !a.Closed() {
			ach = a.Watch()
		}
		if vch == nil && ach == nil {
			return
		}
		remain := time.Until(deadline)
		if remain <= 0 {
			return
		}
		timer := time.NewTimer(remain)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-vch:
			timer.Stop()
		case <-ach:
			timer.Stop()
		case <-timer.C:
			return
		}
	}
}

func (s *Server) serveUTC(w http.ResponseWriter, r *http.Request) {
	s.metrics.IncDashRequest("utc")
	body := []byte(time.Now().UTC().Format("2006-01-02T15:04:05.000Z"))
	h := w.Header()
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}

func (s *Server) serveBinary(w http.ResponseWriter, r *http.Request, route dashRoute) {
	kind := "init"
	if strings.HasPrefix(route.resource, "seg-") {
		kind = "segment"
	}
	s.metrics.IncDashRequest(kind)

	p := s.provider.PackagerFor(r.Context(), route.name)
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
	data, ok := tr.Data(uri)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodHead {
		s.metrics.AddDashBytes(kind, len(data))
	}
	writeBinary(w, r, uri, data)
}

func writeBinary(w http.ResponseWriter, r *http.Request, uri string, data []byte) {
	h := w.Header()
	h.Set("Content-Type", mp4ContentType)
	h.Set("Cache-Control", "public, max-age=31536000, immutable")
	http.ServeContent(w, r, path.Base(uri), time.Time{}, bytes.NewReader(data))
}

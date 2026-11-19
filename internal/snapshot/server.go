package snapshot

import (
	"context"
	"crypto/subtle"
	"errors"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"flow/internal/access"
	"flow/internal/health"
	"flow/internal/metrics"
	"flow/internal/source"
	"flow/internal/stream"
)

const pathPrefix = "/snapshot/"

// Server 提供 HTTP 截图接口：
//
//	GET /snapshot/<name>[.jpg|.png][?format=&quality=&width=&fresh=1]
//
// 安全模型与 HTTP-FLV/HLS 一致：IP 白名单、CORS、Referer、token 都在
// 进入解码之前完成。
type Server struct {
	grabber   *Grabber
	whitelist *access.Whitelist

	cors           []string
	referers       []string
	requireReferer bool
	token          string
}

// New 构造截图服务器。reg 为 nil 时自建；local 是注册表未命中时的本地
// testdata 回退源，可为 nil。
func New(reg *stream.Registry, local *source.H264Source) *Server {
	return &Server{grabber: NewGrabber(reg, local)}
}

// SetOptions 设置超时、缓存、并发、质量、ffmpeg 等参数。
func (s *Server) SetOptions(opts Options) { s.grabber.SetOptions(opts) }

// SetMetrics 注入进程级指标注册表（nil-safe）。
func (s *Server) SetMetrics(reg *metrics.Registry) { s.grabber.SetMetrics(reg) }

// SetOrigin 注入按需回源解析器。
func (s *Server) SetOrigin(o OriginResolver) { s.grabber.SetOrigin(o) }

// SetWhitelist 注入 IP 白名单。nil 表示不启用。
func (s *Server) SetWhitelist(w *access.Whitelist) { s.whitelist = w }

// SetCORS 设置允许的 Origin，逗号分隔；"*" 表示任意来源。
func (s *Server) SetCORS(origins string) { s.cors = splitList(origins) }

// SetReferer 设置防盗链允许的 Referer 主机名，逗号分隔。
// require 为 true 时，没有 Referer 头的请求也拒绝。
func (s *Server) SetReferer(list string, require bool) {
	s.referers = splitList(list)
	s.requireReferer = require
}

// SetToken 设置访问令牌：?token=... 或 X-Flow-Token。
func (s *Server) SetToken(token string) { s.token = token }

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
	log.Printf("snapshot server listen on %s", addr)
	err = srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// ServeHTTP 路由并处理一个请求。
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ip := remoteHost(r.RemoteAddr)
	if s.whitelist != nil && s.whitelist.Enabled() && !s.whitelist.Allows(ip) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	s.applyCORS(w, r)
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

	name, format, ok := parsePath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	q := r.URL.Query()
	if v := strings.TrimSpace(q.Get("format")); v != "" {
		format = v
	}
	switch strings.ToLower(format) {
	case "", "jpg", "jpeg":
		format = FormatJPEG
	case "png":
		format = FormatPNG
	default:
		http.Error(w, "unsupported format (want jpeg or png)", http.StatusBadRequest)
		return
	}

	quality, err := intParam(q, "quality")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	width, err := intParam(q, "width")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fresh := q.Get("fresh") == "1" || strings.EqualFold(q.Get("fresh"), "true")

	img, err := s.grabber.Snapshot(r.Context(), name, Request{
		Format:  format,
		Quality: quality,
		Width:   width,
		Fresh:   fresh,
	})
	if err != nil {
		s.writeError(w, name, err)
		return
	}

	if s.grabber.metrics != nil {
		s.grabber.metrics.IncSnapshotRequest("ok")
	}
	w.Header().Set("Content-Type", img.ContentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(img.Body)))
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Flow-Snapshot-Pts", strconv.FormatUint(uint64(img.PTS), 10))
	w.Header().Set("X-Flow-Snapshot-Age-Ms", strconv.FormatInt(img.Age.Milliseconds(), 10))
	if img.Cached {
		w.Header().Set("X-Flow-Snapshot-Cache", "hit")
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	if _, err := w.Write(img.Body); err != nil {
		log.Printf("snapshot[%s]: write: %v", name, err)
	}
}

func (s *Server) writeError(w http.ResponseWriter, name string, err error) {
	result := "error"
	status := http.StatusInternalServerError
	var unsupported *UnsupportedCodecError
	switch {
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrNoVideo):
		result, status = "not_found", http.StatusNotFound
	case errors.As(err, &unsupported):
		result, status = "unsupported", http.StatusUnsupportedMediaType
	case errors.Is(err, ErrNoKeyframe),
		errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, context.Canceled):
		result, status = "timeout", http.StatusServiceUnavailable
	}
	if s.grabber.metrics != nil {
		s.grabber.metrics.IncSnapshotRequest(result)
	}
	if status >= 500 {
		log.Printf("snapshot[%s]: %v", name, err)
	}
	http.Error(w, err.Error(), status)
}

// parsePath 解析 /snapshot/<name>[.jpg|.jpeg|.png]。
func parsePath(p string) (name, format string, ok bool) {
	if !strings.HasPrefix(p, pathPrefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(p, pathPrefix)
	if rest == "" || strings.HasSuffix(rest, "/") || strings.Contains(rest, "..") {
		return "", "", false
	}
	lower := strings.ToLower(rest)
	for _, ext := range []struct{ suffix, format string }{
		{".jpg", FormatJPEG},
		{".jpeg", FormatJPEG},
		{".png", FormatPNG},
	} {
		if strings.HasSuffix(lower, ext.suffix) {
			return rest[:len(rest)-len(ext.suffix)], ext.format, true
		}
	}
	return rest, "", true
}

func intParam(q url.Values, key string) (int, error) {
	raw := strings.TrimSpace(q.Get(key))
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 0 {
		return 0, errors.New("invalid " + key)
	}
	return v, nil
}

// ---- 安全 gate（与 HTTP-FLV/HLS 保持同一套语义） ----

func (s *Server) applyCORS(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if origin == "" || len(s.cors) == 0 {
		return
	}
	allow := ""
	for _, o := range s.cors {
		if o == "*" {
			allow = "*"
			break
		}
		if strings.EqualFold(o, origin) {
			allow = origin
			break
		}
	}
	if allow == "" {
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", allow)
	if allow != "*" {
		w.Header().Add("Vary", "Origin")
	}
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
		allowHeaders := r.Header.Get("Access-Control-Request-Headers")
		if allowHeaders == "" {
			allowHeaders = "X-Flow-Token"
		}
		w.Header().Set("Access-Control-Allow-Headers", allowHeaders)
		w.Header().Set("Access-Control-Max-Age", "86400")
	}
}

func (s *Server) refererAllowed(r *http.Request) bool {
	if len(s.referers) == 0 {
		return true
	}
	ref := r.Header.Get("Referer")
	if ref == "" {
		return !s.requireReferer
	}
	u, err := url.Parse(ref)
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return false
	}
	for _, pat := range s.referers {
		pat = strings.ToLower(pat)
		if pat == "*" {
			return true
		}
		if strings.HasPrefix(pat, "*.") {
			base := pat[2:]
			if host == base || strings.HasSuffix(host, "."+base) {
				return true
			}
			continue
		}
		if host == pat || strings.HasSuffix(host, "."+pat) {
			return true
		}
	}
	return false
}

func (s *Server) tokenAllowed(r *http.Request) bool {
	if s.token == "" {
		return true
	}
	got := r.URL.Query().Get("token")
	if got == "" {
		got = r.Header.Get("X-Flow-Token")
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.token)) == 1
}

func splitList(v string) []string {
	var out []string
	for part := range strings.SplitSeq(v, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func remoteHost(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

package hls

import (
	"context"
	"crypto/subtle"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// applyCORS 按配置回写 Origin；预检请求额外声明允许的方法和头。
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
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", allow)
	if allow != "*" {
		h.Add("Vary", "Origin")
	}
	h.Set("Access-Control-Expose-Headers", "Content-Length, Content-Range, Date")
	if r.Method == http.MethodOptions {
		h.Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
		allowHeaders := r.Header.Get("Access-Control-Request-Headers")
		if allowHeaders == "" {
			allowHeaders = "Range,X-Flow-Token"
		}
		h.Set("Access-Control-Allow-Headers", allowHeaders)
		h.Set("Access-Control-Max-Age", "86400")
	}
}

// refererAllowed 校验防盗链允许列表。
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

// tokenAllowed 校验播放令牌，支持 query 和请求头两种携带方式。
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
	for _, part := range strings.Split(v, ",") {
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

// sleepCtx 等待 d，ctx 取消时提前返回 false。
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

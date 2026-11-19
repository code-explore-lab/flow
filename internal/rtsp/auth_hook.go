package rtsp

// O-3：推/拉鉴权 HTTP hook 接入。
//
// 静态 Basic/Digest 仍然先行校验；通过后（或未配置静态鉴权时）把
// ANNOUNCE / DESCRIBE 交给外部鉴权服务。只在这两个方法上调用，是因为
// 网络客户端必须先 DESCRIBE 才能拿到 Session 做 SETUP，必须先 ANNOUNCE
// 才能推流；后续请求复用同一条会话，再由 hook 客户端自身的缓存去重。
//
// 拒绝时回 403 而不是 401：凭据本身已经通过了静态校验，再发 challenge
// 只会让客户端拿同一份凭据反复重试。

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"

	"flow/internal/authhook"
)

// SetAuthHook 注入 O-3 鉴权回调。nil 表示不启用。
// 必须在 Serve 之前调用。
func (s *Server) SetAuthHook(c *authhook.Client) {
	s.authHook = c
}

// authHookAction 把 RTSP 方法映射成推/拉动作。
// 只对建立方向的方法返回 true。
func authHookAction(method string) (string, bool) {
	switch method {
	case "ANNOUNCE":
		return "publish", true
	case "DESCRIBE":
		return "play", true
	default:
		return "", false
	}
}

// checkAuthHook 调用外部鉴权服务。未配置时恒为放行。
// err 非空表示本次结果来自超时/故障的默认策略，调用方应记录日志。
func (s *Server) checkAuthHook(req *Request, ip, action string) (bool, error) {
	if !s.authHook.Enabled() {
		return true, nil
	}
	stream, token := rtspStreamToken(req.URI)
	return s.authHook.Authorize(context.Background(), authhook.Request{
		Action:   action,
		Protocol: "rtsp",
		Stream:   stream,
		IP:       ip,
		User:     s.authUsername(req),
		Token:    token,
		Method:   req.Method,
		URI:      req.URI,
	})
}

// rtspStreamToken 从请求 URI 取流路径和查询串里的 token。
// 兼容绝对 URL（rtsp://host/live/cam）和路径形式（/live/cam）。
func rtspStreamToken(uri string) (stream, token string) {
	u, err := url.Parse(uri)
	if err != nil {
		base, _, _ := strings.Cut(uri, "?")
		return strings.Trim(base, "/"), ""
	}
	q := u.Query()
	tok := q.Get("token")
	if tok == "" {
		tok = q.Get("key")
	}
	return strings.Trim(u.Path, "/"), tok
}

// authUsername 取本次请求携带的用户名；没有凭据时返回空串。
// Digest 的密码只用于服务端本地校验，不会暴露给 hook。
func (s *Server) authUsername(req *Request) string {
	header := headerValue(req, "Authorization")
	if len(header) > 6 && strings.EqualFold(header[:6], "Basic ") {
		raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(header[6:]))
		if err != nil {
			return ""
		}
		user, _, _ := strings.Cut(string(raw), ":")
		return user
	}
	if len(header) > 7 && strings.EqualFold(header[:7], "Digest ") {
		return parseAuthParams(header[7:])["username"]
	}
	return ""
}

// writeAuthHookDenied 回 403。拒绝原因只打日志，不把 hook 的细节回给客户端。
func writeAuthHookDenied(conn *Conn, hd map[string]string) error {
	hd["Content-Length"] = "0"
	return writeResponse(conn, "403 Forbidden", hd, nil)
}

// logAuthHookResult 统一日志格式：只打方法、流名、IP 和结果，不打 token。
func logAuthHookResult(method, uri, ip string, allowed bool, err error) {
	stream, _ := rtspStreamToken(uri)
	switch {
	case err != nil && allowed:
		fmt.Printf("auth hook: %s %s from %s: %v (fallback allow)\n", method, stream, ip, err)
	case err != nil:
		fmt.Printf("auth hook: %s %s from %s: %v (fallback deny)\n", method, stream, ip, err)
	case !allowed:
		fmt.Printf("auth hook denied: %s %s from %s\n", method, stream, ip)
	}
}

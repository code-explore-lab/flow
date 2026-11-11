package rtsp

import (
	"crypto/subtle"
	"encoding/base64"
	"strings"
)

// basicRealm 是 401 响应里 WWW-Authenticate 的 realm。
const basicRealm = "flow"

// checkRequestAuth 校验请求的 Authorization 头。
//
// 未配置鉴权时恒为 true；配置后只接受
//
//	Authorization: Basic base64(user:pass)
//
// 用户名/密码用 ConstantTimeCompare 比较，避免按字节短路泄露时序。
// OPTIONS 由 handleConn 单独放行，不经过这里。
func (s *Server) checkRequestAuth(req *Request) bool {
	s.authMu.RLock()
	user, pass := s.authUser, s.authPass
	s.authMu.RUnlock()
	if user == "" && pass == "" {
		return true
	}

	auth := headerValue(req, "Authorization")
	const scheme = "Basic "
	if len(auth) <= len(scheme) || !strings.EqualFold(auth[:len(scheme)], scheme) {
		return false
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(auth[len(scheme):]))
	if err != nil {
		return false
	}
	// Basic 规定用户名不能含冒号，所以按第一个冒号切分：余下都是密码。
	gotUser, gotPass, ok := strings.Cut(string(raw), ":")
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(gotUser), []byte(user)) == 1 &&
		subtle.ConstantTimeCompare([]byte(gotPass), []byte(pass)) == 1
}

// headerValue 按 RTSP 头名做大小写不敏感查找。
// parseHeaderBlock 原样保存头名，而客户端对 Authorization 的大小写并不统一。
func headerValue(req *Request, name string) string {
	if v, ok := req.Headers[name]; ok {
		return v
	}
	for k, v := range req.Headers {
		if strings.EqualFold(k, name) {
			return v
		}
	}
	return ""
}

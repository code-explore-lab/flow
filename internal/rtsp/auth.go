package rtsp

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strings"
	"time"
)

const (
	// basicRealm 是 401 响应里 WWW-Authenticate 的 realm。
	basicRealm = "flow"

	// digestNonceLifetime 是 Digest nonce 的有效期。过期后返回 stale=true，
	// 客户端会拿新 nonce 自动重试，不需要用户重新输入密码。
	digestNonceLifetime = 10 * time.Minute

	// nonce 布局：8 字节时间戳 + 16 字节随机数 + 16 字节 HMAC-SHA256 截断。
	// 服务端只保存一把随机密钥，不维护 nonce 表，重启后旧 nonce 自动失效。
	digestNoncePayload = 24
	digestNonceMAC     = 16
)

// AuthScheme 是 401 challenge 使用的认证方案。
type AuthScheme string

const (
	// AuthBasic 明文 base64 凭据（RFC 7617），默认方案。
	AuthBasic AuthScheme = "basic"
	// AuthDigest MD5 摘要（RFC 2617，qop=auth）。
	AuthDigest AuthScheme = "digest"
)

// authResult 是一次认证的结果。
// stale 只在 Digest nonce 签名有效但已过期时为 true，
// 用于告诉客户端「凭据没问题，换个 nonce 重试」。
type authResult struct {
	ok    bool
	stale bool
}

// newAuthSecret 生成服务进程内的 nonce 签名密钥。
func newAuthSecret() [32]byte {
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		panic("rtsp: cannot initialize auth secret: " + err.Error())
	}
	return secret
}

// checkRequestAuth 校验请求的 Authorization 头。
//
// 未配置鉴权时恒为 true；配置后按 scheme 走 Basic 或 Digest。
// OPTIONS 由 handleConn 单独放行，不经过这里。
func (s *Server) checkRequestAuth(req *Request) authResult {
	s.authMu.RLock()
	user, pass, scheme := s.authUser, s.authPass, s.authScheme
	s.authMu.RUnlock()
	if user == "" && pass == "" {
		return authResult{ok: true}
	}

	header := headerValue(req, "Authorization")
	if scheme == AuthDigest {
		return s.checkDigestAuth(req, header, user, pass)
	}
	return authResult{ok: checkBasicAuth(header, user, pass)}
}

// authChallenge 生成 401 响应的 WWW-Authenticate 值。
// Digest 每次 challenge 都发新 nonce；stale=true 表示旧 nonce 过期而非密码错误。
func (s *Server) authChallenge(stale bool) string {
	s.authMu.RLock()
	scheme := s.authScheme
	s.authMu.RUnlock()
	if scheme == AuthDigest {
		challenge := fmt.Sprintf(`Digest realm="%s", nonce="%s", qop="auth", algorithm=MD5`,
			basicRealm, s.newNonce())
		if stale {
			challenge += ", stale=true"
		}
		return challenge
	}
	return `Basic realm="` + basicRealm + `"`
}

// checkBasicAuth 校验 Authorization: Basic base64(user:pass)。
func checkBasicAuth(header, user, pass string) bool {
	const scheme = "Basic "
	if len(header) <= len(scheme) || !strings.EqualFold(header[:len(scheme)], scheme) {
		return false
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(header[len(scheme):]))
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

// checkDigestAuth 校验 RFC 2617 Digest（algorithm=MD5，qop=auth）。
//
//	HA1 = MD5(username:realm:password)
//	HA2 = MD5(method:uri)
//	response = MD5(HA1:nonce:nc:cnonce:qop:HA2)
//
// 客户端没带 qop 时兼容旧式 MD5(HA1:nonce:HA2)。
func (s *Server) checkDigestAuth(req *Request, header, user, pass string) authResult {
	const scheme = "Digest "
	if len(header) <= len(scheme) || !strings.EqualFold(header[:len(scheme)], scheme) {
		return authResult{}
	}
	p := parseAuthParams(header[len(scheme):])

	username := p["username"]
	realm := p["realm"]
	nonce := p["nonce"]
	uri := p["uri"]
	response := strings.ToLower(p["response"])
	qop := p["qop"]
	nc := p["nc"]
	cnonce := p["cnonce"]
	algorithm := p["algorithm"]

	if realm != basicRealm || nonce == "" || uri == "" || response == "" {
		return authResult{}
	}
	// digest-uri 必须和本次请求的 URI 一致，避免拿别的资源算出的摘要复用。
	if uri != req.URI {
		return authResult{}
	}
	if algorithm != "" && !strings.EqualFold(algorithm, "MD5") {
		return authResult{}
	}
	if subtle.ConstantTimeCompare([]byte(username), []byte(user)) != 1 {
		return authResult{}
	}

	nonceValid, nonceStale := s.digestNonceStatus(nonce)
	if !nonceValid {
		return authResult{}
	}
	if nonceStale {
		return authResult{stale: true}
	}

	ha1 := md5Hex(username + ":" + realm + ":" + pass)
	ha2 := md5Hex(req.Method + ":" + uri)
	var want string
	if qop == "" {
		want = md5Hex(ha1 + ":" + nonce + ":" + ha2)
	} else {
		if !strings.EqualFold(qop, "auth") || nc == "" || cnonce == "" || !isHexN(nc, 8) {
			return authResult{}
		}
		want = md5Hex(ha1 + ":" + nonce + ":" + nc + ":" + cnonce + ":" + qop + ":" + ha2)
	}
	if subtle.ConstantTimeCompare([]byte(want), []byte(response)) != 1 {
		return authResult{}
	}
	return authResult{ok: true}
}

// newNonce 用当前时间生成一个签名 nonce。
func (s *Server) newNonce() string {
	return s.newNonceAt(time.Now())
}

// newNonceAt 抽出来便于测试过期逻辑。
func (s *Server) newNonceAt(now time.Time) string {
	payload := make([]byte, digestNoncePayload)
	binary.BigEndian.PutUint64(payload[:8], uint64(now.UnixNano()))
	if _, err := rand.Read(payload[8:]); err != nil {
		panic("rtsp: cannot generate digest nonce: " + err.Error())
	}
	raw := append(payload, s.nonceMAC(payload)...)
	return base64.RawURLEncoding.EncodeToString(raw)
}

// digestNonceStatus 校验 nonce 的 HMAC 和有效期。
// valid=false 表示伪造/损坏；stale=true 表示签名有效但已过期。
func (s *Server) digestNonceStatus(nonce string) (valid, stale bool) {
	raw, err := base64.RawURLEncoding.DecodeString(nonce)
	if err != nil || len(raw) != digestNoncePayload+digestNonceMAC {
		return false, false
	}
	payload, mac := raw[:digestNoncePayload], raw[digestNoncePayload:]
	if subtle.ConstantTimeCompare(mac, s.nonceMAC(payload)) != 1 {
		return false, false
	}
	issued := time.Unix(0, int64(binary.BigEndian.Uint64(payload[:8])))
	now := time.Now()
	if issued.After(now.Add(time.Minute)) { // 明显来自未来的时钟，按伪造处理
		return false, false
	}
	return true, now.Sub(issued) > digestNonceLifetime
}

func (s *Server) nonceMAC(payload []byte) []byte {
	mac := hmac.New(sha256.New, s.authSecret[:])
	mac.Write(payload)
	return mac.Sum(nil)[:digestNonceMAC]
}

// parseAuthParams 解析 `key="value", key2=token` 形式的认证参数。
// 支持引号内的逗号和反斜杠转义；键名统一小写。
func parseAuthParams(s string) map[string]string {
	params := make(map[string]string)
	for {
		s = strings.TrimLeft(s, " \t,")
		if s == "" {
			return params
		}
		eq := strings.IndexByte(s, '=')
		if eq < 0 {
			return params
		}
		key := strings.ToLower(strings.TrimSpace(s[:eq]))
		s = s[eq+1:]

		var value string
		if strings.HasPrefix(s, `"`) {
			s = s[1:]
			var b strings.Builder
			closed := false
			for i := 0; i < len(s); i++ {
				if s[i] == '\\' && i+1 < len(s) {
					b.WriteByte(s[i+1])
					i++
					continue
				}
				if s[i] == '"' {
					s = s[i+1:]
					closed = true
					break
				}
				b.WriteByte(s[i])
			}
			if !closed {
				return params
			}
			value = b.String()
		} else {
			if comma := strings.IndexByte(s, ','); comma >= 0 {
				value = strings.TrimSpace(s[:comma])
				s = s[comma:]
			} else {
				value = strings.TrimSpace(s)
				s = ""
			}
		}
		params[key] = value
	}
}

func md5Hex(s string) string {
	sum := md5.Sum([]byte(s))
	return fmt.Sprintf("%x", sum)
}

func isHexN(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
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

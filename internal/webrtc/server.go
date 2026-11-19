// Package webrtc 提供基于 WHIP/WHEP 的浏览器推流与拉流入口（P-4）。
//
// WHIP（RFC 9725）和 WHEP 都只是一次 HTTP SDP 交换：客户端 POST 一个
// offer，服务端返回 answer，随后媒体走 ICE/DTLS/SRTP。传输层（ICE 连通性、
// DTLS 握手、SRTP 加解密、RTCP）交给 pion/webrtc；本包只做两件事：
//
//   - 把 WHIP 收到的 RTP 解包成 H.264 NALU / Opus 帧，写进共享的
//     stream.Registry，于是 RTSP/RTMP/HTTP-FLV/SRT/HLS/DASH 都能拉；
//   - 从 stream.Registry 订阅帧，按仓库自己的 internal/rtp 封包器重新
//     打成 RTP，通过 WHEP 推给浏览器。
//
// 首版只协商 H.264 视频和 Opus 音频：浏览器 WHIP 推上来的 Opus 可以直接
// 透传给 WHEP；AAC 源（摄像头 RTSP 常见）在 WHEP 侧没有音频，后续再做
// AAC→Opus 转码。
package webrtc

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pioninterceptor "github.com/pion/interceptor"
	pion "github.com/pion/webrtc/v4"

	"flow/internal/access"
	"flow/internal/health"
	"flow/internal/metrics"
	"flow/internal/stream"
)

const (
	maxSDPBytes = 1 << 20
	maxNameLen  = 256

	defaultGatherTimeout = 3 * time.Second
)

// OriginResolver 按需回源。接口定义在本包，cmd/server 注入 origin.Manager，
// 避免 webrtc → origin → rtsp/rtmp 的 import 环。
type OriginResolver interface {
	Ensure(ctx context.Context, name string) *stream.Stream
}

type sessionKind int

const (
	kindWHIP sessionKind = iota
	kindWHEP
)

func (k sessionKind) String() string {
	if k == kindWHIP {
		return "whip"
	}
	return "whep"
}

// session 是一次已协商的 WHIP/WHEP 连接。
type session struct {
	id   string
	name string
	kind sessionKind
	pc   *pion.PeerConnection

	cancel context.CancelFunc
	done   chan struct{}
}

// Server 是 WHIP/WHEP 的 HTTP 入口。配置项必须在 Serve/第一个请求之前设置。
type Server struct {
	registry *stream.Registry
	metrics  *metrics.Registry
	origin   OriginResolver

	whitelist *access.Whitelist
	token     string
	cors      []string

	publicIPs       []string
	iceServers      []pion.ICEServer
	udpPortMin      uint16
	udpPortMax      uint16
	includeLoopback bool
	gatherTimeout   time.Duration

	api *pion.API

	mu       sync.Mutex
	sessions map[string]*session

	listenAddr atomic.Value // net.Addr
}

// New 构造 WebRTC 服务器。reg 为 nil 时自建一份。
func New(reg *stream.Registry) *Server {
	if reg == nil {
		reg = stream.NewRegistry()
	}
	s := &Server{
		registry:      reg,
		metrics:       metrics.New(),
		sessions:      make(map[string]*session),
		gatherTimeout: defaultGatherTimeout,
	}
	s.buildAPI()
	return s
}

// SetMetrics 注入进程级指标注册表。
func (s *Server) SetMetrics(m *metrics.Registry) {
	if m != nil {
		s.metrics = m
	}
}

// SetOrigin 注入按需回源解析器。
func (s *Server) SetOrigin(o OriginResolver) { s.origin = o }

// SetWhitelist 注入 IP 白名单。nil 表示不启用。
func (s *Server) SetWhitelist(w *access.Whitelist) { s.whitelist = w }

// SetCORS 设置允许的 Origin，逗号分隔；"*" 表示任意来源。
func (s *Server) SetCORS(origins string) { s.cors = splitList(origins) }

// SetToken 设置推拉流令牌。非空时请求必须带 ?token=... 或 X-Flow-Token 头。
func (s *Server) SetToken(token string) { s.token = token }

// SetPublicIPs 设置 NAT 1:1 映射的公网 IP（容器/云主机场景）。
// 传入空列表表示只上报本机收集到的候选地址。
func (s *Server) SetPublicIPs(ips []string) error {
	for _, ip := range ips {
		if net.ParseIP(ip) == nil {
			return fmt.Errorf("webrtc: invalid public IP %q", ip)
		}
	}
	s.publicIPs = append([]string(nil), ips...)
	s.buildAPI()
	return nil
}

// SetICEServers 设置 STUN/TURN 服务器。所有 URL 共用同一组凭据
// （传空凭据即匿名 STUN）。
func (s *Server) SetICEServers(urls []string, username, credential string) {
	s.iceServers = nil
	for _, u := range urls {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		server := pion.ICEServer{URLs: []string{u}}
		if username != "" || credential != "" {
			server.Username = username
			server.Credential = credential
		}
		s.iceServers = append(s.iceServers, server)
	}
}

// SetUDPPortRange 限制 ICE 使用的 UDP 端口范围（0:0 表示系统分配）。
func (s *Server) SetUDPPortRange(minPort, maxPort uint16) error {
	if (minPort == 0) != (maxPort == 0) || (minPort != 0 && maxPort < minPort) {
		return fmt.Errorf("webrtc: invalid UDP port range %d:%d", minPort, maxPort)
	}
	s.udpPortMin, s.udpPortMax = minPort, maxPort
	s.buildAPI()
	return nil
}

// SetIncludeLoopback 控制是否收集回环候选。本机浏览器/测试连接需要开启。
func (s *Server) SetIncludeLoopback(v bool) {
	s.includeLoopback = v
	s.buildAPI()
}

// SetGatherTimeout 设置非 trickle 模式下等待 ICE 收集完成的上限。
func (s *Server) SetGatherTimeout(d time.Duration) {
	if d > 0 {
		s.gatherTimeout = d
	}
}

// Addr 返回实际监听的地址；Serve 尚未完成监听时返回 nil。
func (s *Server) Addr() net.Addr {
	if v := s.listenAddr.Load(); v != nil {
		return v.(net.Addr)
	}
	return nil
}

// buildAPI 按当前配置重建 MediaEngine / SettingEngine。
// 只在 Serve 之前调用（配置项在服务启动后不再变化）。
func (s *Server) buildAPI() {
	me := &pion.MediaEngine{}

	// H.264：WebRTC 浏览器普遍支持 packetization-mode=1。不写死
	// profile-level-id，让 Pion 的模糊匹配接受 Chrome/Safari 的各种 profile。
	_ = me.RegisterCodec(pion.RTPCodecParameters{
		RTPCodecCapability: pion.RTPCodecCapability{
			MimeType:    pion.MimeTypeH264,
			ClockRate:   90000,
			SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1",
		},
		PayloadType: 102,
	}, pion.RTPCodecTypeVideo)

	// Opus：48kHz、双声道，payload 就是编码器输出。
	_ = me.RegisterCodec(pion.RTPCodecParameters{
		RTPCodecCapability: pion.RTPCodecCapability{
			MimeType:    pion.MimeTypeOpus,
			ClockRate:   48000,
			Channels:    2,
			SDPFmtpLine: "minptime=10;useinbandfec=1",
		},
		PayloadType: 111,
	}, pion.RTPCodecTypeAudio)

	ir := &pioninterceptor.Registry{}
	if err := pion.RegisterDefaultInterceptors(me, ir); err != nil {
		log.Printf("webrtc: register interceptors: %v", err)
	}

	se := pion.SettingEngine{}
	if s.includeLoopback {
		se.SetIncludeLoopbackCandidate(true)
	}
	if len(s.publicIPs) > 0 {
		// 用公网 IP 替换收集到的 host 候选（等价于旧的 NAT1To1 映射）。
		_ = se.SetICEAddressRewriteRules(pion.ICEAddressRewriteRule{
			External:        s.publicIPs,
			AsCandidateType: pion.ICECandidateTypeHost,
		})
	}
	if s.udpPortMin > 0 && s.udpPortMax >= s.udpPortMin {
		_ = se.SetEphemeralUDPPortRange(s.udpPortMin, s.udpPortMax)
	}

	s.api = pion.NewAPI(
		pion.WithMediaEngine(me),
		pion.WithInterceptorRegistry(ir),
		pion.WithSettingEngine(se),
	)
}

func (s *Server) pcConfig() pion.Configuration {
	return pion.Configuration{
		ICEServers:   s.iceServers,
		BundlePolicy: pion.BundlePolicyMaxBundle,
	}
}

// Serve 在 addr 上提供 WHIP/WHEP 的 HTTP 接口，ctx 取消时关闭监听和会话。
func (s *Server) Serve(ctx context.Context, addr string) error {
	if err := ctx.Err(); err != nil {
		return nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	health.NotifyListenerReady(ctx)
	s.listenAddr.Store(ln.Addr())

	srv := &http.Server{
		Handler:           s,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
		s.closeAll()
	}()
	fmt.Printf("webrtc server listen on %s (WHIP: /whip/<name>, WHEP: /whep/<name>)\n", ln.Addr())
	err = srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) closeAll() {
	s.mu.Lock()
	sessions := make([]*session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	s.mu.Unlock()
	for _, sess := range sessions {
		sess.cancel()
	}
}

// ServeHTTP 路由并处理一个请求。
//
//	POST   /whip/<name>           WHIP 推流
//	POST   /whep/<name>           WHEP 拉流
//	PATCH  /whip|whep/<name>/<id> trickle ICE 补充候选
//	DELETE /whip|whep/<name>/<id> 结束会话
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.applyCORS(w, r)

	ip := remoteHost(r.RemoteAddr)
	if s.whitelist != nil && s.whitelist.Enabled() && !s.whitelist.Allows(ip) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if !s.tokenAllowed(r) {
		http.Error(w, "invalid token", http.StatusForbidden)
		return
	}

	kind, name, id, ok := parsePath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}

	switch r.Method {
	case http.MethodPost:
		if id != "" {
			w.Header().Set("Allow", "PATCH, DELETE, OPTIONS")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if kind == kindWHIP {
			s.handleWHIP(w, r, name)
			return
		}
		s.handleWHEP(w, r, name)
	case http.MethodPatch:
		s.handlePatch(w, r, id)
	case http.MethodDelete:
		s.handleDelete(w, id)
	default:
		w.Header().Set("Allow", "POST, PATCH, DELETE, OPTIONS")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// parsePath 解析 /whip|whep/<name>[/<id>]。流名可以包含斜杠，
// 最后一个路径段才可能是会话 ID。
func parsePath(p string) (kind sessionKind, name, id string, ok bool) {
	var rest string
	switch {
	case strings.HasPrefix(p, "/whip/"):
		kind, rest = kindWHIP, strings.TrimPrefix(p, "/whip/")
	case strings.HasPrefix(p, "/whep/"):
		kind, rest = kindWHEP, strings.TrimPrefix(p, "/whep/")
	default:
		return 0, "", "", false
	}
	rest = strings.Trim(rest, "/")
	if rest == "" {
		return 0, "", "", false
	}
	// 会话 ID 固定 32 位十六进制；其余路径都算流名（允许 live/cam/1）。
	if i := strings.LastIndexByte(rest, '/'); i >= 0 {
		last := rest[i+1:]
		if isSessionID(last) {
			return kind, strings.Trim(rest[:i], "/"), last, true
		}
	}
	if len(rest) > maxNameLen {
		return 0, "", "", false
	}
	return kind, rest, "", true
}

func isSessionID(s string) bool {
	if len(s) != 32 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func newSessionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 失败时退化为时间戳，只影响唯一性，不影响安全边界。
		return fmt.Sprintf("%032x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func (s *Server) addSession(sess *session) {
	s.mu.Lock()
	s.sessions[sess.id] = sess
	s.mu.Unlock()
	if s.metrics != nil {
		s.metrics.IncWebRTCSession(sess.kind.String())
	}
}

func (s *Server) removeSession(sess *session) {
	s.mu.Lock()
	if cur, ok := s.sessions[sess.id]; ok && cur == sess {
		delete(s.sessions, sess.id)
	}
	s.mu.Unlock()
	if s.metrics != nil {
		s.metrics.DecWebRTCSession(sess.kind.String())
	}
}

func (s *Server) getSession(id string) *session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[id]
}

// handlePatch 处理 WHIP/WHEP 的 trickle ICE 候选。服务端默认等收集完成
// 才回 answer，所以这里主要兼容会在 answer 之后继续 trickle 的客户端。
func (s *Server) handlePatch(w http.ResponseWriter, r *http.Request, id string) {
	sess := s.getSession(id)
	if sess == nil {
		http.NotFound(w, r)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxSDPBytes))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "a=candidate:") {
			continue
		}
		cand := strings.TrimPrefix(line, "a=")
		if err := sess.pc.AddICECandidate(pion.ICECandidateInit{Candidate: cand}); err != nil {
			log.Printf("webrtc: %s %q: add candidate: %v", sess.kind, sess.name, err)
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleDelete 结束会话。重复删除也返回 204（幂等）。
func (s *Server) handleDelete(w http.ResponseWriter, id string) {
	if sess := s.getSession(id); sess != nil {
		sess.cancel()
	}
	w.WriteHeader(http.StatusNoContent)
}

// startSession 注册一个刚协商好的会话，并在退出时统一清理。
// cleanup 在会话结束后执行（WHIP 用来从注册表摘流）。
func (s *Server) startSession(sess *session, cleanup func()) {
	s.addSession(sess)
	go func() {
		<-sess.done
		_ = sess.pc.Close()
		if cleanup != nil {
			cleanup()
		}
		s.removeSession(sess)
	}()
}

// watchConnection 在连接失败/关闭时结束会话。Pion 只保留一个连接状态
// 回调，所以 onConnected 必须和失败处理注册在同一个回调里，不能分开设置。
func watchConnection(pc *pion.PeerConnection, cancel context.CancelFunc, onConnected func()) {
	pc.OnConnectionStateChange(func(state pion.PeerConnectionState) {
		switch state {
		case pion.PeerConnectionStateConnected:
			if onConnected != nil {
				onConnected()
			}
		case pion.PeerConnectionStateFailed, pion.PeerConnectionStateClosed:
			cancel()
		}
	})
}

// answerSDP 把 offer 交给 Pion，返回带完整候选的 answer。
func (s *Server) answerSDP(pc *pion.PeerConnection, offer pion.SessionDescription) (string, error) {
	if err := pc.SetRemoteDescription(offer); err != nil {
		return "", err
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return "", err
	}
	gatherComplete := pion.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		return "", err
	}
	select {
	case <-gatherComplete:
	case <-time.After(s.gatherTimeout):
	}
	return pc.LocalDescription().SDP, nil
}

func readOffer(r *http.Request) (pion.SessionDescription, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxSDPBytes))
	if err != nil {
		return pion.SessionDescription{}, err
	}
	if len(body) == 0 {
		return pion.SessionDescription{}, errors.New("empty SDP body")
	}
	return pion.SessionDescription{Type: pion.SDPTypeOffer, SDP: string(body)}, nil
}

// writeAnswer 按 WHIP/WHEP 的要求返回 201 + Location + ETag。
func writeAnswer(w http.ResponseWriter, r *http.Request, loc, id, sdp string) {
	h := w.Header()
	h.Set("Content-Type", "application/sdp")
	h.Set("Location", loc)
	h.Set("ETag", `"`+id+`"`)
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusCreated)
	_, _ = io.WriteString(w, sdp)
}

// location 构造会话资源 URL；客户端带着 ?token= 来时保留它，方便后续
// PATCH/DELETE 继续通过鉴权。
func location(r *http.Request, kind sessionKind, name, id string) string {
	loc := "/" + kind.String() + "/" + strings.Trim(name, "/") + "/" + id
	if q := r.URL.RawQuery; q != "" {
		loc += "?" + q
	}
	return loc
}

// applyCORS 按配置回写 Origin；预检请求声明允许的方法和头。
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
	h.Set("Access-Control-Expose-Headers", "Location, ETag, Content-Type")
	if r.Method == http.MethodOptions {
		h.Set("Access-Control-Allow-Methods", "POST, PATCH, DELETE, OPTIONS")
		allowHeaders := r.Header.Get("Access-Control-Request-Headers")
		if allowHeaders == "" {
			allowHeaders = "Content-Type, If-Match, X-Flow-Token"
		}
		h.Set("Access-Control-Allow-Headers", allowHeaders)
		h.Set("Access-Control-Max-Age", "86400")
	}
}

// tokenAllowed 校验推拉流令牌，支持 query 和请求头两种携带方式。
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

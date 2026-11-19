// Package rtsp 实现 RTSP 协议层的服务器：接受 TCP 连接、解析 RTSP 请求、
// 管理会话与媒体轨、把共享源的数据按 UDP / TCP interleaved 两种方式推给客户端。
//
// 这一层从 Stage 5 的快照里拆出来，做成一个可以被 main 配置并启动的 Server，
// 不再依赖包级全局变量。媒体源、会话表、连接注册表都挂在 Server 实例上。
package rtsp

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	"flow/internal/access"
	"flow/internal/health"
	"flow/internal/limiter"
	"flow/internal/metrics"
	"flow/internal/session"
	"flow/internal/source"
	"flow/internal/stream"
)

// Server 一个 RTSP 服务器实例。一个进程通常只有一个：共享一组媒体源，
// 多个客户端各自建立会话、订阅同一批数据。
type Server struct {
	video     *source.H264Source
	audio     *source.AACSource
	live      bool   //true 实时流， false 文件回放
	videoPath string // 文件模式 SDP 里取 SPS/PPS 用的源文件
	audioPath string // 文件模式 SDP 里取 AudioSpecificConfig 用的源文件

	sessions *session.Manager

	// sessionExpireHook 是 O-4 的会话异常通知：会话因超时/断网被 Sweep
	// 回收时调用，main 转发给 health.Checker 产生 session_expired 告警。
	// 只在 Serve 前配置。
	sessionExpireHook func(id, remote string, created, lastActive time.Time)

	// 已发布流注册表（ANNOUNCE 创建，拉流端按 URL 路径查找）。P1-8。
	// 与 RTMP 共用同一份 internal/stream.Registry；New 默认自建一份，
	// main 可以用 SetRegistry 注入共享实例。
	registry *stream.Registry

	// Basic / Digest 鉴权配置。空用户表表示不鉴权（默认）。
	// 只在 Serve 前配置，读取时加读锁以兼容测试并发。
	authMu     sync.RWMutex
	authUsers  map[string]string // username -> password
	authScheme AuthScheme
	// authSecret 用于给 Digest nonce 签名；进程内随机生成，不持久化。
	authSecret [32]byte

	// cseqCheck 控制每条连接上的 CSeq 是否必须严格递增。
	// cseqMaxGap > 0 时额外拒绝跳号超过该值的请求；0 表示不限制。
	// 只在 Serve 前配置。
	cseqCheck  bool
	cseqMaxGap int

	// redirectURI 非空时服务器进入重定向模式：OPTIONS/DESCRIBE 回
	// 302 Found + Location，不建会话、不推流。只在 Serve 前配置。
	redirectURI string

	// tlsConfig 非空时 Serve 用 tls.Listen 起 RTSPS（E-6）；nil 表示明文 RTSP。
	// 只在 Serve 前配置。
	tlsConfig *tls.Config

	// multicastAddr / multicastTTL 组播发送配置。addr 为空表示关闭组播，
	// 默认启用 239.255.0.1 / TTL 1。只在 Serve 前配置。
	multicastAddr string
	multicastTTL  int

	// C-5：所有在发轨共享的 RTCP 带宽预算与 SR 调度状态。
	// C-6：拉流轨的丢包降级策略，SETUP 时复制到 mediaTrack。
	rtcp    *rtcpScheduler
	degrade degradeConfig

	connsMu sync.Mutex
	conns   map[*Conn]struct{}
	connWG  sync.WaitGroup

	// E-4：进程级指标注册表，与 RTMP 服务器及 /metrics 端点共享。
	metrics *metrics.Registry

	// E-5：连接数/单 IP 连接数限流。nil 表示不限制。
	limiter *limiter.ConnLimiter

	// E-8：IP 白名单、每 IP 建连速率、失败认证退避。nil 表示对应功能未启用。
	whitelist *access.Whitelist
	rate      *access.RateLimiter
	backoff   *access.Backoff

	// HTTP 隧道（RTSP over HTTP）的 GET/POST 配对表，按 x-sessioncookie 索引。
	// GET 先登记，POST 到达时按同一个 cookie 找回数据通道。
	tunnelsMu sync.Mutex
	tunnels   map[string]*httpTunnel

	// S-1：按需回源解析器。origin 包依赖本包的拉流客户端，所以这里只
	// 定义接口，由 cmd/server 注入实现，避免 import 环。
	originMu  sync.RWMutex
	origin    OriginResolver
	originCtx context.Context

	// S-3：动态发现。本地没有的流若在别的节点上，OPTIONS/DESCRIBE 回
	// 302 + Location。nil 表示未启用。只在 Serve 之前配置。
	finder           StreamFinder
	discoveryTimeout time.Duration
}

// New 构造一个服务器。live 为 true 时 video 应是从 reader 构造的实时源；
// audio 传 nil 表示只有视频轨（实时模式就是这么用）。
func New(video *source.H264Source, audio *source.AACSource, live bool) *Server {
	return &Server{
		video:            video,
		audio:            audio,
		live:             live,
		videoPath:        "testdata/test.h264",
		audioPath:        "testdata/test.aac",
		sessions:         session.NewManager(session.DefaultTimeout),
		registry:         stream.NewRegistry(),
		conns:            make(map[*Conn]struct{}),
		tunnels:          make(map[string]*httpTunnel),
		authScheme:       AuthBasic,
		authSecret:       newAuthSecret(),
		cseqCheck:        true,
		multicastAddr:    defaultMulticastAddr,
		multicastTTL:     defaultMulticastTTL,
		rtcp:             newRTCPScheduler(),
		degrade:          defaultDegradeConfig,
		metrics:          metrics.New(),
		discoveryTimeout: defaultDiscoveryTimeout,
	}
}

// SetMetrics 注入进程级指标注册表（E-4）。传 nil 时保留自建的那份。
// 必须在 Serve 之前调用。
func (s *Server) SetMetrics(reg *metrics.Registry) {
	if reg != nil {
		s.metrics = reg
	}
}

// SetLimits 注入连接/单 IP 连接限流器（E-5）。nil 表示不限制。
// 必须在 Serve 之前调用。
func (s *Server) SetLimits(l *limiter.ConnLimiter) {
	s.limiter = l
}

// SetMaxSessions 设置并发 RTSP 会话上限（E-5），0 表示不限制。
// 必须在 Serve 之前调用。
func (s *Server) SetMaxSessions(n int) {
	s.sessions.SetMax(n)
}

// SetSessionExpireHook 注册会话超时回收的回调（O-4）。只在 Sweep 回收
// 路径触发；客户端正常 TEARDOWN 不会调用。传 nil 取消。
// 必须在 Serve 之前调用。
func (s *Server) SetSessionExpireHook(fn func(id, remote string, created, lastActive time.Time)) {
	s.sessionExpireHook = fn
}

// SetWhitelist 注入 IP 白名单（E-8）。nil 表示不启用白名单。
// 必须在 Serve 之前调用。
func (s *Server) SetWhitelist(w *access.Whitelist) {
	s.whitelist = w
}

// SetRateLimit 注入每 IP 建连速率限制（E-8）。nil 表示不限速。
// 必须在 Serve 之前调用。
func (s *Server) SetRateLimit(r *access.RateLimiter) {
	s.rate = r
}

// SetBackoff 注入失败认证退避（E-8）。nil 表示不退避。
// 必须在 Serve 之前调用。
func (s *Server) SetBackoff(b *access.Backoff) {
	s.backoff = b
}

// SetRegistry 注入共享的已发布流注册表（main 里让 RTSP 与 RTMP 互通）。
// 传 nil 时保持自建的那份。必须在 Serve 之前调用。
func (s *Server) SetRegistry(reg *stream.Registry) {
	if reg != nil {
		s.registry = reg
	}
}

// SetAuth 启用单用户 RTSP 鉴权。user 与 pass 都为空表示关闭鉴权（默认）。
// 多用户请用 SetUsers。认证方案由 SetAuthScheme 选择，默认 Basic。
// 必须在 Serve 之前调用；运行期修改不会让已建立的连接重新认证。
func (s *Server) SetAuth(user, pass string) {
	if user == "" && pass == "" {
		s.SetUsers(nil)
		return
	}
	s.SetUsers(map[string]string{user: pass})
}

// SetUsers 整体替换鉴权用户表；nil 或空表表示关闭鉴权。
// 用户名为空的条目会被忽略。
func (s *Server) SetUsers(users map[string]string) {
	cp := make(map[string]string, len(users))
	for user, pass := range users {
		if user != "" {
			cp[user] = pass
		}
	}
	if len(cp) == 0 {
		cp = nil
	}
	s.authMu.Lock()
	s.authUsers = cp
	s.authMu.Unlock()
}

// passwordFor 返回用户名对应的密码；用户不存在时 ok=false。
func (s *Server) passwordFor(user string) (string, bool) {
	s.authMu.RLock()
	pass, ok := s.authUsers[user]
	s.authMu.RUnlock()
	return pass, ok
}

// SetAuthScheme 选择 401 challenge 的方案（AuthBasic / AuthDigest）。
// 其他值按 AuthBasic 处理。必须在 Serve 之前调用。
func (s *Server) SetAuthScheme(scheme AuthScheme) {
	if scheme != AuthDigest {
		scheme = AuthBasic
	}
	s.authMu.Lock()
	s.authScheme = scheme
	s.authMu.Unlock()
}

// SetCSeqCheck 配置每条连接上的 CSeq 校验。
// enabled=false 完全关闭；maxGap>0 时额外拒绝比上一条大超过 maxGap 的
// 请求，0 表示只要求严格递增、不限制跳号。负数按 0 处理。
// 必须在 Serve 之前调用。
func (s *Server) SetCSeqCheck(enabled bool, maxGap int) {
	if maxGap < 0 {
		maxGap = 0
	}
	s.cseqCheck = enabled
	s.cseqMaxGap = maxGap
}

// SetRedirect 让服务器只做重定向：对 OPTIONS/DESCRIBE 回 302 Found 和
// Location: uri，由客户端自己连到真正的流媒体服务器。空串表示正常服务。
// 必须在 Serve 之前调用；重定向响应在鉴权之前发出。
func (s *Server) SetRedirect(uri string) {
	s.redirectURI = uri
}

// SetTLS 启用 RTSPS：从 PEM 文件加载证书与私钥，Serve 改用 tls.Listen。
// 证书或私钥读取/匹配失败时返回错误，服务器保持明文模式。
// 必须成对提供；只在 Serve 之前调用（证书轮换需重启）。
// 启动前配置， 不支持热证书轮换
func (s *Server) SetTLS(certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("rtsp tls: %w", err)
	}
	s.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	return nil
}

// SetMulticast 配置组播发送的地址与 TTL。addr 为空关闭组播；ttl 越界时
// 夹到 1~255。必须在 Serve 之前调用。
func (s *Server) SetMulticast(addr string, ttl int) {
	if ttl <= 0 {
		ttl = defaultMulticastTTL
	}
	if ttl > 255 {
		ttl = 255
	}
	s.multicastAddr = addr
	s.multicastTTL = ttl
}

// SetDegradePolicy 配置 C-6 的丢包降级策略：
// 区间丢包率连续 sustain 超过 lossThreshold 后只发参数集和 IDR，
// 连续 recover 低于阈值后恢复。阈值不在 (0,1] 或窗口非正时保持默认值。
// 必须在 Serve 之前配置；已 SETUP 的轨沿用创建时的策略。
func (s *Server) SetDegradePolicy(lossThreshold float64, sustain, recover time.Duration) {
	cfg := degradeConfig{
		LossThreshold: lossThreshold,
		Sustain:       sustain,
		Recover:       recover,
	}
	if cfg.valid() {
		s.degrade = cfg
	}
}

// authEnabled 报告是否配置了鉴权用户。
func (s *Server) authEnabled() bool {
	s.authMu.RLock()
	defer s.authMu.RUnlock()
	return len(s.authUsers) > 0
}

// tlsHandshakeTimeout RTSPS 的显式 TLS 握手超时。tls.Conn 的握手是懒的
// （首次读写才触发），不显式设限就要等到 handleConn 的读 deadline
// （会话超时 + 扫描间隔）才失败，慢客户端会白占 goroutine。
const tlsHandshakeTimeout = 10 * time.Second

// Serve 监听 addr，接受连接直到 ctx 取消，然后优雅退出。
// 返回的错误只有监听失败这一种（Accept 错误按信号区分，正常退出返回 nil）。
func (s *Server) Serve(ctx context.Context, addr string) error {
	s.setOriginContext(ctx)
	s.installExpireHook()
	go s.sessions.Run(ctx) //启动会话管理器

	// E-6：配置了证书就用 tls.Listen（RTSPS），否则保持明文 RTSP。
	// TLS 只是包在 TCP 外面一层，accept 循环与后续处理逻辑完全复用；
	// tls.Conn 的 RemoteAddr() 也仍返回底层 *net.TCPAddr。
	var listener net.Listener
	var err error
	scheme := "RTSP"
	if s.tlsConfig != nil {
		scheme = "RTSPS"
		listener, err = tls.Listen("tcp", addr, s.tlsConfig)
	} else {
		listener, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return err
	}
	defer listener.Close()
	health.NotifyListenerReady(ctx)
	fmt.Printf("%s server listen on %s (Ctrl+C to stop)\n", scheme, addr)
	if s.live {
		fmt.Println("input: stdin (live H.264, audio disabled)")
	} else {
		fmt.Println("input: testdata/test.h264 + testdata/test.aac")
	}
	if s.authEnabled() {
		s.authMu.RLock()
		scheme := s.authScheme
		s.authMu.RUnlock()
		fmt.Printf("auth: %s (realm=flow)\n", scheme)
	}

	// 收到退出信号后关掉 listener，让下面阻塞的 Accept 立即返回。
	go func() {
		<-ctx.Done()
		fmt.Println("shutting down: stop accepting new connections")
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break // 退出流程关的 listener，正常走 shutdown
			}
			fmt.Printf("accept err: %v\n", err)
			continue
		}
		// E-8：白名单外的来源直接拒绝（最先判定，最便宜）。
		ip := remoteIP(conn)
		if !s.whitelist.Allows(ip) {
			fmt.Printf("reject client %s: not in whitelist\n", ip)
			rejectRTSPConn(conn)
			continue
		}
		// E-8：每 IP 建连速率。超速在 accept 阶段拒绝，不影响已有连接。
		if !s.rate.Allow(ip) {
			fmt.Printf("reject client %s: connection rate exceeded\n", ip)
			rejectRTSPConn(conn)
			continue
		}
		// E-5：连接/单 IP 上限。超限的连接在 accept 阶段就拒绝，
		// 不进入 goroutine，也不影响已建立连接。
		if ok, reason := s.limiter.Allow(ip); !ok {
			fmt.Printf("reject client %s: %s\n", ip, reason)
			rejectRTSPConn(conn)
			continue
		}
		fmt.Printf("new client: %s\n", conn.RemoteAddr())
		s.metrics.IncConn("rtsp")
		s.metrics.IncConnTotal("rtsp")
		s.connWG.Add(1)
		go func(c net.Conn, ip string) {
			defer s.connWG.Done()
			defer s.metrics.DecConn("rtsp")
			defer s.limiter.Release(ip)
			// E-6：显式完成 TLS 握手并限时，明文客户端或卡住的握手快速失败。
			// 握手完成后清掉 deadline，交给 handleConn 按会话超时重新设置。
			if tc, ok := c.(*tls.Conn); ok {
				_ = tc.SetDeadline(time.Now().Add(tlsHandshakeTimeout))
				if err := tc.Handshake(); err != nil {
					fmt.Printf("rtsp: tls handshake from %s: %v\n", ip, err)
					_ = tc.Close()
					return
				}
				_ = tc.SetDeadline(time.Time{})
			}
			s.handleConn(&Conn{Conn: c})
		}(conn, ip)
	}
	s.shutdown()
	return nil
}

// remoteIP 取连接的来源 IP（不带端口）。取不到时退回 RemoteAddr 字符串。
func remoteIP(conn net.Conn) string {
	if addr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		return addr.IP.String()
	}
	host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		return conn.RemoteAddr().String()
	}
	return host
}

// rejectRTSPConn 对超限连接回一条 503 后关闭。此时还没读到请求，
// CSeq 填 0；客户端至少能读到协议层错误，而不是无声断开。
func rejectRTSPConn(conn net.Conn) {
	_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
	_, _ = conn.Write([]byte("RTSP/1.0 503 Service Unavailable\r\nCSeq: 0\r\nContent-Length: 0\r\n\r\n"))
	_ = conn.Close()
}

// installExpireHook 安装会话超时回收钩子：记一条日志，并关闭该会话独占的
// 控制连接（E-9）。只在 Sweep 回收（超时/断网/半开）时触发，不干扰客户端的
// 正常 TEARDOWN（那条路径在回 200 OK 时连接必须还开着）。
// 测试替换过 s.sessions 后需要重新调用一次。
func (s *Server) installExpireHook() {
	s.sessions.OnExpire = func(sess *session.Session) {
		fmt.Printf("session %s auto teardown: no RTCP/RTSP for %s (last RTCP %s)\n",
			sess.ID, s.sessions.Timeout(), sess.LastRTCP().Format(time.RFC3339))
		if st, ok := sess.Data.(*Session); ok && st.conn != nil {
			s.closeConnIfIdle(st.conn, sess.ID)
		}
		if hook := s.sessionExpireHook; hook != nil {
			hook(sess.ID, sess.RemoteIP, sess.CreatedAt, sess.LastActivity())
		}
	}
}

// closeConnIfIdle 在该连接上除 selfID 外没有其他活跃会话时关闭它，
// 让 handleConn 的读阻塞立即返回（E-9：半开/断网时连接随会话一起回收，
// 不再干等 30s 读超时兜底）。还有兄弟会话则不动，避免误杀。
func (s *Server) closeConnIfIdle(conn *Conn, selfID string) {
	for _, id := range conn.boundSessions() {
		if id == selfID {
			continue
		}
		if _, ok := s.sessions.Get(id); ok {
			return
		}
	}
	conn.Close()
}

func (s *Server) registerConn(c *Conn) {
	s.connsMu.Lock()
	s.conns[c] = struct{}{}
	s.connsMu.Unlock()
}

func (s *Server) unregisterConn(c *Conn) {
	s.connsMu.Lock()
	delete(s.conns, c)
	s.connsMu.Unlock()
}

// closeAllConns 关闭所有活跃的 RTSP 连接，返回关闭的数量。
func (s *Server) closeAllConns() int {
	s.connsMu.Lock()
	list := make([]*Conn, 0, len(s.conns))
	for c := range s.conns {
		list = append(list, c)
	}
	s.connsMu.Unlock()

	for _, c := range list {
		c.Close()
	}
	return len(list)
}

// shutdown 优雅退出的收尾步骤：
//  1. 关闭所有 RTSP TCP 连接，解除 handleConn 的阻塞读；
//  2. 关闭所有会话（停推流、关 UDP RTP/RTCP socket）；
//  3. 等所有连接 goroutine 退出（最多 5s，防止半开连接卡住进程）。
func (s *Server) shutdown() {
	n := s.closeAllConns()
	fmt.Printf("shutdown: closing %d rtsp connection(s)\n", n)

	s.sessions.CloseAll()
	fmt.Printf("shutdown: sessions closed (remaining=%d)\n", s.sessions.Len())

	done := make(chan struct{})
	go func() {
		s.connWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		fmt.Println("shutdown: done")
	case <-time.After(5 * time.Second):
		fmt.Println("shutdown: timeout waiting for connections")
	}
}

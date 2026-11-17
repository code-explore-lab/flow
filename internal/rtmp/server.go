// Package rtmp 实现 RTMP 协议层的服务器：接受 TCP 连接，完成握手后按
// Chunk Stream 收发命令与控制消息，最终支持推流（publish）与拉流（play）。
//
// 与 internal/rtsp 一样是纯标准库自研；媒体数据面复用 internal/source，
// 已发布流注册表与 RTSP 共用 internal/stream，实现两协议互通。
package rtmp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"flow/internal/access"
	"flow/internal/limiter"
	"flow/internal/metrics"
	"flow/internal/source"
	"flow/internal/stream"
)

// Server 一个 RTMP 服务器实例。一个进程通常只有一个：共享
// internal/stream 注册表，多个连接各自推流/拉流。
type Server struct {
	registry *stream.Registry
	video    *source.H264Source // 本地回退视频源（注册表未命中时用）
	audio    *source.AACSource  // 本地回退音频源，可为 nil

	connsMu sync.Mutex
	conns   map[*Conn]struct{}
	connWG  sync.WaitGroup

	// E-4：进程级指标注册表，与 RTSP 服务器及 /metrics 端点共享。
	metrics *metrics.Registry

	// E-5：连接数/单 IP 连接数限流。nil 表示不限制。
	limiter *limiter.ConnLimiter

	// E-8：IP 白名单与每 IP 建连速率。nil 表示对应功能未启用。
	whitelist *access.Whitelist
	rate      *access.RateLimiter

	// E-5：握手超时与消息读/写空闲超时。0 表示不设超时。
	handshakeTimeout time.Duration
	connTimeout      time.Duration

	// T-2：非空时 Serve 用 tls.Listen 起 RTMPS；nil 表示明文 RTMP。
	tlsConfig *tls.Config

	// T-3：RTMP token / app 白名单鉴权。nil 表示不鉴权。
	auth *Auth

	// T-7：connect 时向对端通告的 chunk size。0 表示用 connectChunkSize。
	chunkSize uint32
}

// New 构造一个 RTMP 服务器。reg 为 nil 时自建一份；video/audio 可以为 nil
// （只做推流或只做互通时不需要本地回退源）。
func New(reg *stream.Registry, video *source.H264Source, audio *source.AACSource) *Server {
	if reg == nil {
		reg = stream.NewRegistry()
	}
	return &Server{
		registry:         reg,
		video:            video,
		audio:            audio,
		conns:            make(map[*Conn]struct{}),
		metrics:          metrics.New(),
		handshakeTimeout: 10 * time.Second,
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

// SetChunkSize 设置 connect 时向对端通告的 chunk size（T-7），默认
// connectChunkSize（4096）。非法值（0 或超过 maxChunkSize）忽略。
// 必须在 Serve 之前调用。
func (s *Server) SetChunkSize(n uint32) {
	if n == 0 || n > maxChunkSize {
		return
	}
	s.chunkSize = n
}

// SetHandshakeTimeout 设置握手超时（E-5）。<=0 表示不设超时。
// 必须在 Serve 之前调用。
func (s *Server) SetHandshakeTimeout(d time.Duration) {
	if d > 0 {
		s.handshakeTimeout = d
	}
}

// SetConnTimeout 设置消息读/写空闲超时（E-5）。<=0 表示不设超时。
// 必须在 Serve 之前调用。
func (s *Server) SetConnTimeout(d time.Duration) {
	s.connTimeout = d
}

// SetAuth 注入 RTMP 鉴权配置（T-3）。nil 表示不鉴权。
// 必须在 Serve 之前调用。
func (s *Server) SetAuth(a *Auth) {
	s.auth = a
}

// SetTLS 启用 RTMPS：从 PEM 文件加载证书与私钥，Serve 改用 tls.Listen。
// 证书或私钥读取/匹配失败时返回错误，服务器保持明文模式。
// 必须成对提供；只在 Serve 之前调用（证书轮换需重启）。
func (s *Server) SetTLS(certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("rtmp tls: %w", err)
	}
	s.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	return nil
}

// tlsHandshakeTimeout RTMPS 的显式 TLS 握手超时。tls.Conn 的握手是懒的
// （首次读写才触发），不显式设限就要等到 RTMP 握手超时才失败。
const tlsHandshakeTimeout = 10 * time.Second

// Serve 监听 addr，接受连接直到 ctx 取消，然后优雅退出。
// 返回的错误只有监听失败这一种（Accept 错误按信号区分，正常退出返回 nil）。
func (s *Server) Serve(ctx context.Context, addr string) error {
	// T-2：配置了证书就用 tls.Listen（RTMPS），否则保持明文 RTMP。
	// TLS 只是包在 TCP 外面一层，accept 循环与 RTMP 处理逻辑完全复用；
	// tls.Conn 的 RemoteAddr() 也仍返回底层 *net.TCPAddr。
	var listener net.Listener
	var err error
	scheme := "RTMP"
	if s.tlsConfig != nil {
		scheme = "RTMPS"
		listener, err = tls.Listen("tcp", addr, s.tlsConfig)
	} else {
		listener, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return err
	}
	defer listener.Close()
	fmt.Printf("%s server listen on %s (Ctrl+C to stop)\n", scheme, addr)

	// 收到退出信号后关掉 listener，让下面阻塞的 Accept 立即返回。
	go func() {
		<-ctx.Done()
		fmt.Println("rtmp shutdown: stop accepting new connections")
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break // 退出流程关的 listener，正常走 shutdown
			}
			fmt.Printf("rtmp accept err: %v\n", err)
			continue
		}
		// E-8：白名单 / 每 IP 建连速率。RTMP 没有 503 语义，拒绝即断开。
		ip := remoteIP(conn)
		if !s.whitelist.Allows(ip) {
			fmt.Printf("rtmp: reject client %s: not in whitelist\n", ip)
			_ = conn.Close()
			continue
		}
		if !s.rate.Allow(ip) {
			fmt.Printf("rtmp: reject client %s: connection rate exceeded\n", ip)
			_ = conn.Close()
			continue
		}
		// E-5：连接/单 IP 上限。RTMP 没有 503 语义，超限直接断开。
		if ok, reason := s.limiter.Allow(ip); !ok {
			fmt.Printf("rtmp: reject client %s: %s\n", ip, reason)
			_ = conn.Close()
			continue
		}
		fmt.Printf("rtmp: new client %s\n", conn.RemoteAddr())
		s.metrics.IncConn("rtmp")
		s.metrics.IncConnTotal("rtmp")
		s.connWG.Add(1)
		go func(c net.Conn, ip string) {
			defer s.connWG.Done()
			defer s.metrics.DecConn("rtmp")
			defer s.limiter.Release(ip)
			// T-2：显式完成 TLS 握手并限时，明文客户端或卡住的握手快速失败。
			// 握手完成后清掉 deadline，交给 RTMP 握手自己的超时重新设置。
			if tc, ok := c.(*tls.Conn); ok {
				_ = tc.SetDeadline(time.Now().Add(tlsHandshakeTimeout))
				if err := tc.Handshake(); err != nil {
					fmt.Printf("rtmp: tls handshake from %s: %v\n", ip, err)
					_ = tc.Close()
					return
				}
				_ = tc.SetDeadline(time.Time{})
			}
			s.handleConn(newConn(c))
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

// handleConn 处理一条 RTMP 连接：握手 → Chunk Stream 消息循环 → 断开清理。
func (s *Server) handleConn(conn *Conn) {
	s.registerConn(conn)
	conn.srv = s
	conn.handshakeTimeout = s.handshakeTimeout
	conn.connTimeout = s.connTimeout
	// 清理顺序有讲究：先 Close 让阻塞中的写立即失败，再停播（等播放
	// goroutine 退出），最后注销推流并登记表。
	defer func() {
		conn.Close()
		conn.stopPlay()
		conn.stopPublish()
		s.unregisterConn(conn)
	}()

	if err := conn.handshake(); err != nil {
		fmt.Printf("rtmp: handshake with %s failed: %v\n", conn.RemoteAddr(), err)
		return
	}

	for {
		// E-5：空闲读超时，防止半开连接让读循环永久挂住。
		if conn.connTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(conn.connTimeout))
		}
		m, err := conn.readMessage()
		if err != nil {
			if err != io.EOF {
				fmt.Printf("rtmp: client %s disconnected: %v\n", conn.RemoteAddr(), err)
			}
			return
		}
		if err := conn.maybeAck(); err != nil {
			fmt.Printf("rtmp: ack to %s failed: %v\n", conn.RemoteAddr(), err)
			return
		}
		// 控制消息优先
		if handled, err := conn.handleControl(m); err != nil {
			fmt.Printf("rtmp: control message from %s: %v\n", conn.RemoteAddr(), err)
			return
		} else if handled {
			continue
		}

		switch m.TypeID {
		case msgCommandAMF0:
			if err := conn.handleCommand(m); err != nil {
				// 鉴权拒绝已经回过 _error/onStatus 并单独打过日志，不再刷一条。
				if !errors.Is(err, errAuthRejected) {
					fmt.Printf("rtmp: command from %s: %v\n", conn.RemoteAddr(), err)
				}
				return
			}
		case msgVideo:
			conn.handleVideo(m)
		case msgAudio:
			conn.handleAudio(m)
		case msgDataAMF0:
			conn.handleData(m)
		case msgAggregate:
			// T-7：聚合消息拆成子消息后按普通消息处理。
			if err := conn.handleAggregate(m); err != nil {
				fmt.Printf("rtmp: aggregate from %s: %v\n", conn.RemoteAddr(), err)
				return
			}
		default:
			// msgDataAMF3(15)、msgCommandAMF3(17) 等不支持的 AMF3 消息
			// 与未知类型忽略。
		}
	}
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

// closeAllConns 关闭所有活跃连接，返回关闭的数量。
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

// shutdown 优雅退出：关闭所有连接，等连接 goroutine 退出（最多 5s）。
func (s *Server) shutdown() {
	n := s.closeAllConns()
	fmt.Printf("rtmp shutdown: closing %d connection(s)\n", n)

	done := make(chan struct{})
	go func() {
		s.connWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		fmt.Println("rtmp shutdown: done")
	case <-time.After(5 * time.Second):
		fmt.Println("rtmp shutdown: timeout waiting for connections")
	}
}

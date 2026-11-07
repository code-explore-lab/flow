// Package rtsp 实现 RTSP 协议层的服务器：接受 TCP 连接、解析 RTSP 请求、
// 管理会话与媒体轨、把共享源的数据按 UDP / TCP interleaved 两种方式推给客户端。
//
// 这一层从 Stage 5 的快照里拆出来，做成一个可以被 main 配置并启动的 Server，
// 不再依赖包级全局变量。媒体源、会话表、连接注册表都挂在 Server 实例上。
package rtsp

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"flow/internal/session"
	"flow/internal/source"
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

	// 已发布流注册表（ANNOUNCE 创建，拉流端按 URL 路径查找）。P1-8。
	pubMu     sync.RWMutex
	published map[string]*publishedStream //key是RTSP url路径

	connsMu sync.Mutex
	conns   map[*Conn]struct{}
	connWG  sync.WaitGroup
}

// New 构造一个服务器。live 为 true 时 video 应是从 reader 构造的实时源；
// audio 传 nil 表示只有视频轨（实时模式就是这么用）。
func New(video *source.H264Source, audio *source.AACSource, live bool) *Server {
	return &Server{
		video:     video,
		audio:     audio,
		live:      live,
		videoPath: "testdata/test.h264",
		audioPath: "testdata/test.aac",
		sessions:  session.NewManager(session.DefaultTimeout),
		published: make(map[string]*publishedStream),
		conns:     make(map[*Conn]struct{}),
	}
}

// Serve 监听 addr，接受连接直到 ctx 取消，然后优雅退出。
// 返回的错误只有监听失败这一种（Accept 错误按信号区分，正常退出返回 nil）。
func (s *Server) Serve(ctx context.Context, addr string) error {
	s.sessions.OnExpire = func(sess *session.Session) {
		fmt.Printf("session %s auto teardown: no RTCP/RTSP for %s (last RTCP %s)\n",
			sess.ID, s.sessions.Timeout(), sess.LastRTCP().Format(time.RFC3339))
	}
	go s.sessions.Run(ctx) //启动会话管理器

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	fmt.Printf("RTSP server listen on %s (Ctrl+C to stop)\n", addr)
	if s.live {
		fmt.Println("input: stdin (live H.264, audio disabled)")
	} else {
		fmt.Println("input: testdata/test.h264 + testdata/test.aac")
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
		fmt.Printf("new client: %s\n", conn.RemoteAddr())
		s.connWG.Add(1)
		go func() {
			defer s.connWG.Done()
			s.handleConn(&Conn{Conn: conn})
		}()
	}
	s.shutdown()
	return nil
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

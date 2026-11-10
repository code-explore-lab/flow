// Package rtmp 实现 RTMP 协议层的服务器：接受 TCP 连接，完成握手后按
// Chunk Stream 收发命令与控制消息，最终支持推流（publish）与拉流（play）。
//
// 与 internal/rtsp 一样是纯标准库自研；媒体数据面复用 internal/source，
// 已发布流注册表与 RTSP 共用 internal/stream，实现两协议互通。
package rtmp

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

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
}

// New 构造一个 RTMP 服务器。reg 为 nil 时自建一份；video/audio 可以为 nil
// （只做推流或只做互通时不需要本地回退源）。
func New(reg *stream.Registry, video *source.H264Source, audio *source.AACSource) *Server {
	if reg == nil {
		reg = stream.NewRegistry()
	}
	return &Server{
		registry: reg,
		video:    video,
		audio:    audio,
		conns:    make(map[*Conn]struct{}),
	}
}

// Serve 监听 addr，接受连接直到 ctx 取消，然后优雅退出。
// 返回的错误只有监听失败这一种（Accept 错误按信号区分，正常退出返回 nil）。
func (s *Server) Serve(ctx context.Context, addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	fmt.Printf("RTMP server listen on %s (Ctrl+C to stop)\n", addr)

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
		fmt.Printf("rtmp: new client %s\n", conn.RemoteAddr())
		s.connWG.Add(1)
		go func() {
			defer s.connWG.Done()
			s.handleConn(newConn(conn))
		}()
	}
	s.shutdown()
	return nil
}

// handleConn 处理一条 RTMP 连接：握手 → Chunk Stream 消息循环 → 断开清理。
func (s *Server) handleConn(conn *Conn) {
	s.registerConn(conn)
	conn.srv = s
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

		if handled, err := conn.handleControl(m); err != nil {
			fmt.Printf("rtmp: control message from %s: %v\n", conn.RemoteAddr(), err)
			return
		} else if handled {
			continue
		}

		switch m.TypeID {
		case msgCommandAMF0:
			if err := conn.handleCommand(m); err != nil {
				fmt.Printf("rtmp: command from %s: %v\n", conn.RemoteAddr(), err)
				return
			}
		case msgVideo:
			conn.handleVideo(m)
		case msgAudio:
			conn.handleAudio(m)
		default:
			// onMetaData（type 18）等数据消息与未知类型暂不处理。
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

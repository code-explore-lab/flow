package tsingest

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"flow/internal/mpegts"
	"flow/internal/stream"
)

const (
	defaultUDPIdle   = 10 * time.Second
	defaultUDPBuffer = 4 << 20
	udpReadDeadline  = time.Second
	publishLogEvery  = 5 * time.Second
)

// UDPOptions 是一路 UDP TS 输入的参数。
type UDPOptions struct {
	Addr   string        // "host:port"；组播地址会自动加入组
	Idle   time.Duration // 无包超过这个时间就摘流，0 用默认 10s
	Buffer int           // 内核接收缓冲，0 用默认 4MB
}

// RunUDP 监听 UDP 上的 MPEG-TS 并发布成注册表流。
//
// 一个端口只应有一个发送端：原始 TS 没有会话标识，多个发送端混在同一
// socket 上无法区分。空闲超过 Idle 后摘流（下游看到流结束）；下一包到达
// 时重新发布，所以发送端重启不需要重启 flow。
func RunUDP(ctx context.Context, reg *stream.Registry, name string, opt UDPOptions) error {
	addr, err := net.ResolveUDPAddr("udp", opt.Addr)
	if err != nil {
		return fmt.Errorf("tsingest: udp %q: resolve %q: %w", name, opt.Addr, err)
	}

	var conn *net.UDPConn
	if addr.IP != nil && addr.IP.IsMulticast() {
		conn, err = net.ListenMulticastUDP("udp4", nil, addr)
	} else {
		conn, err = net.ListenUDP("udp4", addr)
	}
	if err != nil {
		return fmt.Errorf("tsingest: udp %q: listen %s: %w", name, opt.Addr, err)
	}
	return runUDPConn(ctx, reg, name, conn, opt)
}

// runUDPConn 是 RunUDP 的收包循环，连接由调用方创建（便于测试用临时端口）。
func runUDPConn(ctx context.Context, reg *stream.Registry, name string, conn *net.UDPConn, opt UDPOptions) error {
	if reg == nil {
		conn.Close()
		return errors.New("tsingest: nil registry")
	}
	if name == "" {
		conn.Close()
		return errors.New("tsingest: empty stream name")
	}
	if opt.Idle <= 0 {
		opt.Idle = defaultUDPIdle
	}
	if opt.Buffer <= 0 {
		opt.Buffer = defaultUDPBuffer
	}
	defer conn.Close()
	if err := conn.SetReadBuffer(opt.Buffer); err != nil {
		log.Printf("tsingest: udp %q: set read buffer: %v", name, err)
	}

	// ctx 取消时关闭 socket，让阻塞的 ReadFromUDP 立刻返回。
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-stop:
		}
	}()

	var (
		d          = mpegts.NewDemuxer()
		st         *stream.Stream
		ing        *Ingest
		lastPacket = time.Now()
		warnedAt   time.Time
	)

	reset := func() {
		st = nil
		ing = nil
		d = mpegts.NewDemuxer()
	}
	unpublish := func() {
		if st == nil {
			return
		}
		d.Flush()
		reg.Unpublish(name, st)
		reset()
	}
	publish := func(now time.Time) {
		s := &stream.Stream{}
		published, err := reg.Publish(name, s)
		if err != nil || published == nil {
			if now.Sub(warnedAt) >= publishLogEvery {
				warnedAt = now
				if err == nil {
					err = errors.New("publish rejected")
				}
				log.Printf("tsingest: udp %q: publish: %v", name, err)
			}
			return
		}
		st = s
		ing = NewIngest(s)
		d.OnError = func(err error) { log.Printf("tsingest: udp %q: %v", name, err) }
		d.OnVideo = ing.OnVideo
		d.OnAudio = ing.OnAudio
		lastPacket = now
	}

	buf := make([]byte, 65536)
	for {
		if ctx.Err() != nil {
			break
		}
		_ = conn.SetReadDeadline(time.Now().Add(udpReadDeadline))
		n, _, err := conn.ReadFromUDP(buf)
		now := time.Now()
		if n > 0 {
			lastPacket = now
			if st == nil {
				publish(now)
			} else if cur := reg.Get(name); cur != st {
				// janitor 按空闲回收了：重新发布；被别人 replace：让出名字。
				if cur != nil {
					return fmt.Errorf("tsingest: udp %q: stream was replaced by another publisher", name)
				}
				reset()
				publish(now)
			}
			if st != nil {
				d.Write(buf[:n])
			}
			continue
		}
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				if st != nil && now.Sub(lastPacket) > opt.Idle {
					log.Printf("tsingest: udp %q: no packets for %s, unpublishing", name, opt.Idle)
					unpublish()
				}
				continue
			}
			if ctx.Err() != nil {
				break
			}
			return fmt.Errorf("tsingest: udp %q: read: %w", name, err)
		}
	}

	unpublish()
	return nil
}

// Package srt 提供基于 SRT 的推流与拉流（P-5）。
//
// SRT 只是传输层：ffmpeg/OBS 在 SRT 连接里传的是 MPEG-TS。本包用
// internal/mpegts 解出 H.264/AAC 帧后登记到共享的 stream.Registry，
// 因此 SRT 推上来的流可以从 RTSP/RTMP/HTTP-FLV 拉取；反向地，注册表里
// 已有的流（不论来自哪个协议）也能通过 SRT 拉走。
//
// StreamID 约定与 ffmpeg 示例一致：
//
//	srt://host:9000?streamid=publish:live/demo  推流
//	srt://host:9000?streamid=read:live/demo     拉流
//
// 同时兼容 OBS 的 #!::r=live/demo,m=publish 形式。加密由 SRT 自带的
// AES 完成：服务端配置 passphrase 后，未加密或口令错误的连接会被拒绝。
package srt

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"time"

	gosrt "github.com/datarhei/gosrt"

	"flow/internal/health"
	"flow/internal/metrics"
	"flow/internal/stream"
)

const (
	// DefaultLatency 是 SRT 的接收缓冲延迟。直播场景下 120ms 与 ffmpeg
	// 的默认值一致，兼顾弱网重传和延迟。
	DefaultLatency = 120 * time.Millisecond

	// minPassphrase / maxPassphrase 是 SRT 协议规定的口令长度。
	minPassphrase = 10
	maxPassphrase = 80

	// waitForTracks 是拉流等待推流端建好音视频轨的最长时间。
	waitForTracks = 5 * time.Second
)

// Server SRT 服务器。配置项必须在 Serve 之前设置。
type Server struct {
	registry *stream.Registry
	metrics  *metrics.Registry

	passphrase string
	latency    time.Duration

	// listenAddr 是 Serve 成功后实际绑定的地址（测试里用 :0 拿随机端口）。
	listenAddr atomic.Value // net.Addr
}

// New 构造 SRT 服务器。reg 为 nil 时自建一份。
func New(reg *stream.Registry) *Server {
	if reg == nil {
		reg = stream.NewRegistry()
	}
	return &Server{
		registry: reg,
		metrics:  metrics.New(),
		latency:  DefaultLatency,
	}
}

// SetMetrics 注入进程级指标注册表。
func (s *Server) SetMetrics(m *metrics.Registry) {
	if m != nil {
		s.metrics = m
	}
}

// SetPassphrase 设置连接口令（10..80 字符）。非空时只接受加密连接，
// 且口令必须匹配；空串表示不需要加密，加密连接也会被拒绝。
func (s *Server) SetPassphrase(pass string) error {
	if pass == "" {
		s.passphrase = ""
		return nil
	}
	if len(pass) < minPassphrase || len(pass) > maxPassphrase {
		return fmt.Errorf("srt: passphrase must be %d..%d characters, got %d",
			minPassphrase, maxPassphrase, len(pass))
	}
	s.passphrase = pass
	return nil
}

// SetLatency 设置 SRT 延迟（SRTO_LATENCY），必须大于 0。
func (s *Server) SetLatency(d time.Duration) {
	if d > 0 {
		s.latency = d
	}
}

// Latency 返回当前配置的 SRT 延迟。
func (s *Server) Latency() time.Duration { return s.latency }

// Addr 返回实际监听的 UDP 地址；Serve 尚未完成监听时返回 nil。
func (s *Server) Addr() net.Addr {
	if v := s.listenAddr.Load(); v != nil {
		return v.(net.Addr)
	}
	return nil
}

// Serve 在 addr 上监听 SRT，ctx 取消时关闭监听和已有连接。
func (s *Server) Serve(ctx context.Context, addr string) error {
	if err := ctx.Err(); err != nil {
		return nil
	}

	cfg := gosrt.DefaultConfig()
	cfg.TransmissionType = "live"
	cfg.Congestion = "live"
	cfg.Latency = s.latency
	cfg.ReceiverLatency = s.latency
	cfg.PeerLatency = s.latency
	cfg.Passphrase = s.passphrase

	ln, err := gosrt.Listen("srt", addr, cfg)
	if err != nil {
		return err
	}
	health.NotifyListenerReady(ctx)
	s.listenAddr.Store(ln.Addr())

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			ln.Close()
		case <-done:
		}
	}()
	defer close(done)
	defer ln.Close()

	for {
		req, err := ln.Accept2()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, gosrt.ErrListenerClosed) {
				return nil
			}
			return err
		}
		go s.handle(ctx, req)
	}
}

func (s *Server) handle(ctx context.Context, req gosrt.ConnRequest) {
	r, err := parseStreamID(req.StreamId())
	if err != nil {
		log.Printf("srt: reject %s: %v", req.RemoteAddr(), err)
		req.Reject(gosrt.REJX_BAD_REQUEST)
		return
	}

	if reason, ok := s.checkEncryption(req); !ok {
		log.Printf("srt: reject %s stream %q: encryption policy (%v)", req.RemoteAddr(), r.Name, reason)
		req.Reject(reason)
		return
	}

	if !r.Publish && s.registry.Get(r.Name) == nil {
		req.Reject(gosrt.REJX_NOTFOUND)
		return
	}

	conn, err := req.Accept()
	if err != nil {
		log.Printf("srt: accept %s stream %q: %v", req.RemoteAddr(), r.Name, err)
		return
	}
	defer conn.Close()

	s.metrics.IncConn("srt")
	s.metrics.IncConnTotal("srt")
	defer s.metrics.DecConn("srt")

	connDone := make(chan struct{})
	defer close(connDone)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-connDone:
		}
	}()

	log.Printf("srt: %s %s %q from %s", modeName(r.Publish), "connected", r.Name, req.RemoteAddr())
	defer log.Printf("srt: %s %s %q", modeName(r.Publish), "disconnected", r.Name)

	if r.Publish {
		s.metrics.IncSRTPublish()
		s.metrics.IncSRTPublishTotal()
		defer s.metrics.DecSRTPublish()
		s.servePublish(ctx, conn, r.Name)
		return
	}

	s.metrics.IncSRTPlay()
	s.metrics.IncSRTPlayTotal()
	defer s.metrics.DecSRTPlay()
	s.servePlay(ctx, conn, r.Name)
}

// checkEncryption 校验连接是否满足服务端的口令策略。返回拒绝原因。
func (s *Server) checkEncryption(req gosrt.ConnRequest) (gosrt.RejectionReason, bool) {
	if s.passphrase == "" {
		if req.IsEncrypted() {
			return gosrt.REJ_UNSECURE, false
		}
		return 0, true
	}
	if !req.IsEncrypted() {
		return gosrt.REJ_UNSECURE, false
	}
	if err := req.SetPassphrase(s.passphrase); err != nil {
		return gosrt.REJ_BADSECRET, false
	}
	return 0, true
}

func modeName(publish bool) string {
	if publish {
		return "publish"
	}
	return "read"
}

// streamRequest 是从 StreamID 解析出的请求。
type streamRequest struct {
	Name    string
	Publish bool
}

// parseStreamID 解析 SRT StreamID。支持：
//
//	publish:live/demo
//	read:live/demo / play:live/demo
//	#!::r=live/demo,m=publish
//	#!::r=live/demo,m=request
//
// 裸流名按拉流处理（推流必须显式声明，避免误把播放端当成推流端）。
func parseStreamID(id string) (streamRequest, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return streamRequest{}, errors.New("empty streamid")
	}

	if strings.HasPrefix(id, "#!::") {
		var name, mode string
		for kv := range strings.SplitSeq(id[len("#!::"):], ",") {
			k, v, ok := strings.Cut(kv, "=")
			if !ok {
				continue
			}
			switch strings.TrimSpace(k) {
			case "r":
				name = v
			case "m":
				mode = strings.ToLower(strings.TrimSpace(v))
			}
		}
		switch mode {
		case "publish":
			name = cleanStreamName(name)
			if name == "" {
				return streamRequest{}, errors.New("streamid has m=publish but no resource name")
			}
			return streamRequest{Name: name, Publish: true}, nil
		case "", "request", "play":
			name = cleanStreamName(name)
			if name == "" {
				return streamRequest{}, errors.New("streamid has no resource name")
			}
			return streamRequest{Name: name}, nil
		default:
			return streamRequest{}, fmt.Errorf("unsupported streamid mode %q", mode)
		}
	}

	if i := strings.IndexByte(id, ':'); i > 0 {
		switch strings.ToLower(strings.TrimSpace(id[:i])) {
		case "publish":
			name := cleanStreamName(id[i+1:])
			if name == "" {
				return streamRequest{}, errors.New("publish streamid has no name")
			}
			return streamRequest{Name: name, Publish: true}, nil
		case "read", "play":
			name := cleanStreamName(id[i+1:])
			if name == "" {
				return streamRequest{}, errors.New("read streamid has no name")
			}
			return streamRequest{Name: name}, nil
		}
	}

	name := cleanStreamName(id)
	if name == "" {
		return streamRequest{}, errors.New("streamid has no name")
	}
	return streamRequest{Name: name}, nil
}

// cleanStreamName 去掉查询串、首尾空白和斜杠，得到注册表里的流名。
// 与 RTMP 的 sanitizeStreamName、RTSP 的 streamKeyFromURI 保持一致。
func cleanStreamName(name string) string {
	if i := strings.IndexAny(name, "?#"); i >= 0 {
		name = name[:i]
	}
	return strings.Trim(strings.TrimSpace(name), "/")
}

// Package httpflv 通过 HTTP 长连接分发 FLV（P-1）。
//
// 拉流地址为 /live/<name>.flv，<name> 与 RTSP/RTMP 推流名一致。服务端从
// 共享注册表订阅帧，用 internal/flvmux 转成 FLV Tag 后按 chunked 编码写出：
//
//	FLV 文件头 → onMetaData（若有）→ AVC/AAC 序列头 → 音视频 Tag 流
//
// 每个请求持有两个订阅者（视频/音频各一），写操作由互斥锁串行化，因此
// 两条轨的 tag 不会交错损坏。客户端断开或写超时都会结束该条连接。
//
// 防盗链/跨域：CORS 预检、Referer 允许列表、token 校验和 IP 白名单都在
// 进入流式响应之前完成，失败时返回普通 HTTP 错误，不建立长连接。
package httpflv

import (
	"bufio"
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"flow/internal/access"
	"flow/internal/flv"
	"flow/internal/flvmux"
	"flow/internal/media"
	"flow/internal/metrics"
	"flow/internal/source"
	"flow/internal/stream"
)

// 写超时：客户端不读导致 socket 缓冲区满时，Write 不能永久阻塞，
// 否则订阅者和 goroutine 会泄漏。每次写 tag 前重新设置。
const writeTimeout = 15 * time.Second

// flushThreshold 是缓冲区攒到多少字节就刷给客户端。太小会增加系统调用，
// 太大会增加起播延迟；2KB 对 20ms 一帧的 G.711 约 250ms。
const flushThreshold = 2 << 10

// 等待推流建轨的最长时间。RTMP 在 publish 命令时就登记流，但音视频轨要等
// 第一条媒体消息/序列头才创建；客户端可能抢在这之前连接。
const waitForTracks = 2 * time.Second

// Server HTTP-FLV 服务器。配置项必须在 Serve 之前设置。
type Server struct {
	registry  *stream.Registry
	video     *source.H264Source
	audio     *source.AACSource
	metrics   *metrics.Registry
	whitelist *access.Whitelist

	cors           []string
	referers       []string
	requireReferer bool
	token          string

	// S-1：按需回源。接口定义在本包，cmd/server 注入 origin.Manager，
	// 避免 httpflv → origin → rtsp/rtmp 的 import 环。
	origin OriginResolver
}

// New 构造 HTTP-FLV 服务器。reg 为 nil 时自建一份；video/audio 是本地文件
// 回退源，可以为 nil（只做协议互通时不需要）。
func New(reg *stream.Registry, video *source.H264Source, audio *source.AACSource) *Server {
	if reg == nil {
		reg = stream.NewRegistry()
	}
	return &Server{
		registry: reg,
		video:    video,
		audio:    audio,
		metrics:  metrics.New(),
	}
}

// SetMetrics 注入进程级指标注册表。传 nil 时保留自建的那份。
func (s *Server) SetMetrics(reg *metrics.Registry) {
	if reg != nil {
		s.metrics = reg
	}
}

// SetWhitelist 注入 IP 白名单（E-8）。nil 表示不启用。
func (s *Server) SetWhitelist(w *access.Whitelist) {
	s.whitelist = w
}

// SetCORS 设置允许的 Origin，逗号分隔；"*" 表示任意来源。
// 传空串表示不发送任何 CORS 响应头。
func (s *Server) SetCORS(origins string) {
	s.cors = splitList(origins)
}

// SetReferer 设置防盗链允许的 Referer 主机名，逗号分隔。
// 支持 example.com（含子域）和 *.example.com；"*" 表示任意。
// require 为 true 时，没有 Referer 头的请求也拒绝。传空列表表示不校验。
func (s *Server) SetReferer(list string, require bool) {
	s.referers = splitList(list)
	s.requireReferer = require
}

// SetToken 设置播放令牌。非空时请求必须带 ?token=... 或 X-Flow-Token 头。
func (s *Server) SetToken(token string) {
	s.token = token
}

// Serve 启动 HTTP 服务，ctx 取消时关闭监听并断开现有连接。
func (s *Server) Serve(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler:           s,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		<-ctx.Done()
		// Close 会立即断开活跃的长连接，handler 里的写操作随之返回错误。
		_ = srv.Close()
	}()
	fmt.Printf("http-flv server listen on %s\n", addr)
	err = srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// ServeHTTP 路由并处理一个请求。
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ip := remoteHost(r.RemoteAddr)
	if s.whitelist != nil && s.whitelist.Enabled() && !s.whitelist.Allows(ip) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	s.applyCORS(w, r)
	if r.Method == http.MethodOptions {
		w.Header().Set("Allow", "GET, HEAD, OPTIONS")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD, OPTIONS")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	name, ok := streamName(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if !s.refererAllowed(r) {
		http.Error(w, "referer not allowed", http.StatusForbidden)
		return
	}
	if !s.tokenAllowed(r) {
		http.Error(w, "invalid token", http.StatusForbidden)
		return
	}

	res, ok := s.resolve(r.Context(), name)
	if !ok {
		http.NotFound(w, r)
		return
	}
	s.serveStream(w, r, res)
}

// resolved 一次播放所需的源与编码参数快照。
type resolved struct {
	name    string
	video   frameSource
	audio   frameSource
	vparams *stream.VideoCodec
	aparams *stream.AudioCodec
	meta    []byte
}

// frameSource 是各订阅源的公共接口：H264Source、AACSource 和
// ExternalSource 都满足。
type frameSource interface {
	Subscribe() (*source.Reader[source.Frame], func())
}

// resolve 按流名找源：先精确匹配注册表，再按路径最后一段匹配（RTSP 注册
// 的名是 live/<name>，RTMP 是裸名，HTTP-FLV 两种写法都能用）。注册表和本地
// 回退都没有时返回 false。
func (s *Server) resolve(ctx context.Context, name string) (*resolved, bool) {
	st := s.registry.Get(name)
	if st == nil {
		if i := strings.LastIndexByte(name, '/'); i >= 0 && i+1 < len(name) {
			st = s.registry.Get(name[i+1:])
		}
	}
	if st != nil {
		// 注册表命中后不再回退本地文件：流存在但暂时没有可用轨道时
		// 返回 404，比悄悄播放 testdata 更符合预期。
		res := s.resolveRegistry(ctx, name, st)
		return res, res != nil
	}

	// 本地没有这路流：先尝试按需回源（S-1）。回源成功时注册表里已经有
	// 这条流，直接按注册表路径处理；失败再退回本地文件。
	if s.origin != nil {
		if st = s.origin.Ensure(ctx, name); st != nil {
			res := s.resolveRegistry(ctx, name, st)
			return res, res != nil
		}
	}

	// 本地文件回退，与 RTMP 播放保持一致：没有推流时播放 testdata。
	if s.video == nil && s.audio == nil {
		return nil, false
	}
	res := &resolved{name: name, video: s.video, audio: s.audio}
	if s.audio != nil {
		res.aparams = localAudioParams(s.audio)
		if res.aparams == nil {
			res.audio = nil
		}
	}
	return res, true
}

func (s *Server) resolveRegistry(ctx context.Context, name string, st *stream.Stream) *resolved {
	// 等推流端建轨；视频源和音频源都可能晚于 publish 命令出现。
	deadline := time.Now().Add(waitForTracks)
	for st.VideoTrack() == nil && st.AudioTrack() == nil {
		if !sleepCtx(ctx, 50*time.Millisecond) || time.Now().After(deadline) {
			break
		}
	}

	video := st.SourceFor("video")
	audio := st.SourceFor("audio")
	if video == nil && audio == nil {
		return nil
	}

	res := &resolved{name: name, meta: st.MetaDataPayload()}
	if video != nil {
		res.video = video
		p, ok := st.VideoParams()
		if !ok {
			// RTMP 的 SPS/PPS 在 AVC 序列头里，轨道创建后紧随其后到达。
			// 稍等一下再构造封装器，避免错过序列头后首帧被丢弃。
			waitDeadline := time.Now().Add(time.Second)
			for !ok && time.Now().Before(waitDeadline) {
				if !sleepCtx(ctx, 20*time.Millisecond) {
					break
				}
				p, ok = st.VideoParams()
			}
		}
		if ok {
			res.vparams = p
		}
	}
	if audio != nil {
		p, ok := st.AudioParams()
		if !ok {
			// 音频序列头通常紧跟轨道创建到达，稍等一下才能判断编码。
			waitDeadline := time.Now().Add(time.Second)
			for !ok && time.Now().Before(waitDeadline) {
				if !sleepCtx(ctx, 20*time.Millisecond) {
					break
				}
				p, ok = st.AudioParams()
			}
		}
		if ok {
			if !flvAudioSupported(p.Codec) {
				audio = nil
			} else {
				res.aparams = p
			}
		}
		if audio != nil {
			res.audio = audio
		}
	}
	if res.video == nil && res.audio == nil {
		// 只有 FLV 承载不了的音频轨（例如 Opus），没有可分发的内容。
		return nil
	}
	return res
}

// serveStream 写出 FLV 头并并行泵送两条轨，直到客户端断开或源结束。
func (s *Server) serveStream(w http.ResponseWriter, r *http.Request, res *resolved) {
	rc := http.NewResponseController(w)
	h := w.Header()
	h.Set("Content-Type", "video/x-flv")
	h.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	h.Set("Pragma", "no-cache")
	h.Set("Expires", "0")
	// nginx 等反代默认会缓冲响应，长连接推流会变成攒一大块再发。
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}

	mux := flvmux.New(res.vparams, res.aparams)
	if res.audio != nil && !mux.AudioEnabled() {
		res.audio = nil
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	s.metrics.IncPlay()
	s.metrics.IncPlayTotal()
	defer s.metrics.DecPlay()
	fmt.Printf("http-flv: play %q from %s\n", res.name, remoteHost(r.RemoteAddr))

	bw := bufio.NewWriterSize(w, 32<<10)
	var writeMu sync.Mutex

	writeTag := func(t flv.Tag) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		// 客户端不读时让 Write 超时失败，而不是永久挂住。
		_ = rc.SetWriteDeadline(time.Now().Add(writeTimeout))
		if err := flv.WriteTag(bw, t); err != nil {
			return err
		}
		if bw.Buffered() >= flushThreshold {
			// 先刷本地 bufio，再让 net/http 把响应头和已写数据推到 socket。
			// 只刷本地缓冲时，net/http 自己的 2KB 缓冲会让小包一直卡着。
			if err := bw.Flush(); err != nil {
				return err
			}
			return rc.Flush()
		}
		return nil
	}
	flush := func() error {
		writeMu.Lock()
		defer writeMu.Unlock()
		if err := bw.Flush(); err != nil {
			return err
		}
		return rc.Flush()
	}

	if err := rc.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		log.Printf("http-flv: set write deadline: %v", err)
	}
	if err := flv.WriteHeader(bw, flv.Header{
		HasVideo: res.video != nil,
		HasAudio: res.audio != nil,
	}); err != nil {
		return
	}
	if len(res.meta) > 0 {
		if err := writeTag(flv.Tag{Type: flv.TagScript, Data: res.meta}); err != nil {
			return
		}
	}
	for _, t := range mux.SequenceTags() {
		if err := writeTag(t); err != nil {
			return
		}
	}
	// 序列头立即发出，播放器拿到后就能初始化解码器。
	if err := flush(); err != nil {
		return
	}

	var wg sync.WaitGroup
	pump := func(src frameSource, kind string, fn func(source.Frame) []flv.Tag, flushPending func() []flv.Tag) {
		defer wg.Done()
		rd, unsubscribe := src.Subscribe()
		defer unsubscribe()
		for {
			f, ok := rd.Next(ctx.Done())
			if !ok {
				for _, t := range flushPending() {
					if err := writeTag(t); err != nil {
						cancel()
						return
					}
				}
				return
			}
			s.metrics.AddPackets(kind, 1)
			s.metrics.AddOctets(kind, uint64(len(f.Data)))
			for _, t := range fn(f) {
				if err := writeTag(t); err != nil {
					cancel()
					return
				}
			}
		}
	}

	if res.video != nil {
		wg.Add(1)
		go pump(res.video, "video", mux.Video, mux.FlushVideo)
	}
	if res.audio != nil {
		wg.Add(1)
		go pump(res.audio, "audio", mux.Audio, func() []flv.Tag { return nil })
	}
	wg.Wait()
	_ = flush()
	fmt.Printf("http-flv: play %q finished\n", res.name)
}

// streamName 从 URL 路径解析流名：/live/<name>.flv → <name>。
func streamName(p string) (string, bool) {
	for strings.HasPrefix(p, "/") {
		p = p[1:]
	}
	// 去掉可能重复的斜杠，拒绝路径穿越。
	if p == "" || strings.Contains(p, "..") || strings.Contains(p, "//") {
		return "", false
	}
	if len(p) < 4 || !strings.EqualFold(p[len(p)-4:], ".flv") {
		return "", false
	}
	name := strings.TrimSuffix(p[:len(p)-4], "/")
	if name == "" || len(name) > 512 {
		return "", false
	}
	return name, true
}

// flvAudioSupported 报告编码能否写进 FLV。
func flvAudioSupported(codec string) bool {
	switch stream.NormalizeAudioCodec(codec) {
	case stream.AudioAAC, stream.AudioMP3, stream.AudioPCMA, stream.AudioPCMU:
		return true
	}
	return false
}

// localAudioParams 用 ADTS 首帧解析本地音频源的 ASC。
func localAudioParams(src *source.AACSource) *stream.AudioCodec {
	info, err := media.GetAACInfo(src.Path())
	if err != nil {
		fmt.Printf("http-flv: read aac info from %s: %v\n", src.Path(), err)
		return nil
	}
	rate := uint32(info.SamplingFrequency)
	return &stream.AudioCodec{
		Codec:      stream.AudioAAC,
		Config:     info.Config,
		SampleRate: rate,
		ClockRate:  rate,
		Channels:   info.ChannelConfig,
	}
}

// applyCORS 按配置回写 Origin；预检请求额外声明允许的方法和头。
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
	w.Header().Set("Access-Control-Allow-Origin", allow)
	if allow != "*" {
		w.Header().Add("Vary", "Origin")
	}
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
		allowHeaders := r.Header.Get("Access-Control-Request-Headers")
		if allowHeaders == "" {
			allowHeaders = "Range,X-Flow-Token"
		}
		w.Header().Set("Access-Control-Allow-Headers", allowHeaders)
		w.Header().Set("Access-Control-Max-Age", "86400")
	}
}

// refererAllowed 校验防盗链允许列表。
func (s *Server) refererAllowed(r *http.Request) bool {
	if len(s.referers) == 0 {
		return true
	}
	ref := r.Header.Get("Referer")
	if ref == "" {
		return !s.requireReferer
	}
	u, err := url.Parse(ref)
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return false
	}
	for _, pat := range s.referers {
		pat = strings.ToLower(pat)
		if pat == "*" {
			return true
		}
		if strings.HasPrefix(pat, "*.") {
			base := pat[2:]
			if host == base || strings.HasSuffix(host, "."+base) {
				return true
			}
			continue
		}
		if host == pat || strings.HasSuffix(host, "."+pat) {
			return true
		}
	}
	return false
}

// tokenAllowed 校验播放令牌，支持 query 和请求头两种携带方式。
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

// sleepCtx 等待 d，ctx 取消时提前返回 false。
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

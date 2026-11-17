package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"flow/internal/access"
	"flow/internal/limiter"
	"flow/internal/metrics"
	"flow/internal/rtmp"
	"flow/internal/rtsp"
	"flow/internal/source"
	"flow/internal/stream"
)

// 入口：解析参数、搭好媒体源、并发启动 RTSP(:8554) 与 RTMP(:1935) 服务器。
//
// 两个服务器共用同一个 stream.Registry：任一协议推上来的流，另一协议都能
// 按流名拉到；注册表未命中时回退到下面的本地文件源，所以
// ffplay rtmp://127.0.0.1:1935/live/test 也能直接放 testdata。
//
// -live 让视频源从 stdin 读裸 H.264，模拟实时摄像头。
// Windows DirectShow（先用 ffmpeg -list_devices true -f dshow -i dummy 查设备名）：
//
//	ffmpeg -f dshow -rtbufsize 100M -i video="USB Camera" \
//	       -c:v libx264 -preset ultrafast -tune zerolatency -f h264 pipe:1 | flow.exe -live
//
// Linux v4l2：
//
//	ffmpeg -f v4l2 -i /dev/video0 -c:v libx264 -preset ultrafast -tune zerolatency \
//	       -f h264 pipe:1 | ./flow -live
func main() {
	// 开启实时流
	live := flag.Bool("live", false, "read Annex-B H.264 from stdin (e.g. ffmpeg camera pipe); audio disabled")
	redirect := flag.String("redirect", "", "answer OPTIONS/DESCRIBE with 302 + this Location instead of serving streams")
	// 组播配置
	multicastAddr := flag.String("multicast-addr", "239.255.0.1", "UDP multicast group address (empty disables multicast)")
	multicastTTL := flag.Int("multicast-ttl", 1, "UDP multicast TTL (1 = loopback/本网段)")
	// RTSP认证相关
	user := flag.String("user", "", "RTSP auth username (also FLOW_RTSP_USER; empty with -pass disables auth)")
	pass := flag.String("pass", "", "RTSP auth password (also FLOW_RTSP_PASS)")
	authFile := flag.String("auth-file", "", "credential file, username:password per line (also FLOW_RTSP_AUTH_FILE)")
	authMode := flag.String("auth-mode", "basic", "RTSP auth scheme: basic or digest")
	// CSeq序列号校验
	cseqCheck := flag.Bool("cseq-check", true, "reject replayed or out-of-order CSeq on RTSP connections")
	cseqMaxGap := flag.Int("cseq-max-gap", 0, "reject CSeq jumps larger than this (0 = unlimited)")
	// 弱网降级
	degradeLoss := flag.Float64("degrade-loss", 0.10, "C-6: fraction lost threshold (0~1) that triggers frame skipping")
	degradeSustain := flag.Duration("degrade-sustain", 3*time.Second, "C-6: how long loss must stay above threshold before degrading")
	degradeRecover := flag.Duration("degrade-recover", 5*time.Second, "C-6: how long loss must stay below threshold before recovering")
	// E-2：pprof 调试端口，空表示关闭；只允许回环地址。
	pprofAddr := flag.String("pprof", "", "E-2: pprof listen address, e.g. :6060 (loopback only; empty disables)")
	// E-4：Prometheus 指标端点，空表示关闭；只允许回环地址。
	metricsAddr := flag.String("metrics", "", "E-4: Prometheus /metrics listen address, e.g. :9090 (loopback only; empty disables)")
	// E-5：连接/会话上限与超时。
	maxConns := flag.Int("max-conns", 0, "E-5: max concurrent TCP connections per protocol (0 = unlimited)")
	maxSessions := flag.Int("max-sessions", 0, "E-5: max concurrent RTSP sessions (0 = unlimited)")
	maxIPConns := flag.Int("max-ip-conns", 0, "E-5: max concurrent connections per source IP per protocol (0 = unlimited)")
	handshakeTimeout := flag.Duration("handshake-timeout", 10*time.Second, "E-5: RTMP handshake timeout")
	connTimeout := flag.Duration("conn-timeout", 60*time.Second, "E-5: RTMP idle read/write timeout (0 disables)")
	// E-8：访问控制与限流。
	whitelistFile := flag.String("whitelist-file", "", "E-8: CIDR whitelist file, one CIDR per line (empty disables)")
	connRate := flag.Float64("conn-rate", 0, "E-8: per-IP connection rate in conns/sec per protocol (0 disables)")
	connBurst := flag.Int("conn-burst", 5, "E-8: per-IP connection burst capacity (used when -conn-rate > 0)")
	backoffThreshold := flag.Int("auth-backoff-threshold", 5, "E-8: failed RTSP auth attempts before backoff")
	backoffBase := flag.Duration("auth-backoff-base", time.Second, "E-8: base auth backoff delay (0 disables)")
	backoffMax := flag.Duration("auth-backoff-max", 30*time.Second, "E-8: max auth backoff delay")
	// E-6：RTSPS（RTSP over TLS）。两个 flag 必须成对出现；空表示明文 RTSP。
	// 命名与协议无关，后续 T-2（RTMPS）可以直接复用。
	tlsCert := flag.String("tls-cert", "", "E-6: TLS certificate file for RTSPS (requires -tls-key)")
	tlsKey := flag.String("tls-key", "", "E-6: TLS private key file for RTSPS (requires -tls-cert)")
	flag.Parse()
	// Exit (2) 在 unix 惯例：**命令行参数错误**；Exit (1) 一般是运行时错误（比如读文件失败）
	if *authMode != "basic" && *authMode != "digest" {
		fmt.Fprintf(os.Stderr, "invalid -auth-mode %q (want basic or digest)\n", *authMode)
		os.Exit(2)
	}
	if *cseqMaxGap < 0 {
		fmt.Fprintln(os.Stderr, "invalid -cseq-max-gap (must be >= 0)")
		os.Exit(2)
	}
	if *degradeLoss <= 0 || *degradeLoss > 1 {
		fmt.Fprintln(os.Stderr, "invalid -degrade-loss (must be in (0,1])")
		os.Exit(2)
	}
	if *degradeSustain <= 0 || *degradeRecover <= 0 {
		fmt.Fprintln(os.Stderr, "invalid -degrade-sustain/-degrade-recover (must be > 0)")
		os.Exit(2)
	}
	if *redirect != "" && !strings.HasPrefix(*redirect, "rtsp://") && !strings.HasPrefix(*redirect, "rtsps://") {
		fmt.Fprintf(os.Stderr, "invalid -redirect %q (want rtsp://host:port/path)\n", *redirect)
		os.Exit(2)
	}
	if *maxConns < 0 || *maxSessions < 0 || *maxIPConns < 0 {
		fmt.Fprintln(os.Stderr, "invalid -max-conns/-max-sessions/-max-ip-conns (must be >= 0)")
		os.Exit(2)
	}
	if *handshakeTimeout <= 0 {
		fmt.Fprintln(os.Stderr, "invalid -handshake-timeout (must be > 0)")
		os.Exit(2)
	}
	if *connTimeout < 0 {
		fmt.Fprintln(os.Stderr, "invalid -conn-timeout (must be >= 0)")
		os.Exit(2)
	}
	if *connRate < 0 || *connBurst < 0 {
		fmt.Fprintln(os.Stderr, "invalid -conn-rate/-conn-burst (must be >= 0)")
		os.Exit(2)
	}
	if *backoffThreshold < 0 || *backoffBase < 0 || *backoffMax <= 0 {
		fmt.Fprintln(os.Stderr, "invalid -auth-backoff-threshold/-auth-backoff-base/-auth-backoff-max (threshold>=0, base>=0, max>0)")
		os.Exit(2)
	}
	if (*tlsCert == "") != (*tlsKey == "") {
		fmt.Fprintln(os.Stderr, "-tls-cert and -tls-key must be set together")
		os.Exit(2)
	}

	// 凭据优先级：文件 < 环境变量 < 命令行参数。
	// 文件里可以放多个账号；环境变量/命令行用来覆盖或追加单个账号。
	authUser := *user
	if authUser == "" {
		authUser = os.Getenv("FLOW_RTSP_USER")
	}
	authPass := *pass
	if authPass == "" {
		authPass = os.Getenv("FLOW_RTSP_PASS")
	}
	authFilePath := *authFile
	if authFilePath == "" {
		authFilePath = os.Getenv("FLOW_RTSP_AUTH_FILE")
	}
	if authUser == "" && authPass != "" {
		fmt.Fprintln(os.Stderr, "auth: username is empty but password is set (use -user or FLOW_RTSP_USER)")
		os.Exit(2)
	}
	users := map[string]string{}
	if authFilePath != "" {
		loaded, err := rtsp.LoadAuthFile(authFilePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load auth file: %v\n", err)
			os.Exit(1)
		}
		users = loaded
		fmt.Printf("auth: loaded %d credential(s) from %s\n", len(users), authFilePath)
	}
	if authUser != "" {
		users[authUser] = authPass
	}
	// 媒体源构造
	// 不带 -live 本地文件回放源
	video := source.NewH264Source("testdata/test.h264")
	audio := source.NewAACSource("testdata/test.aac")
	if *live {
		video = source.NewH264SourceFromReader(os.Stdin)
		audio = nil // 实时模式没有配套音频源，只有视频轨
	}

	// Ctrl+C / SIGTERM 触发优雅退出。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// E-2：pprof 调试端口。必须显式开启，且只允许回环地址。
	if *pprofAddr != "" {
		if err := startPprof(ctx, *pprofAddr); err != nil {
			fmt.Fprintf(os.Stderr, "pprof: %v\n", err)
			os.Exit(2)
		}
	}

	// 两个协议共用一份注册表，实现互通。
	reg := stream.NewRegistry()
	// E-4：两个协议共用一个指标注册表，/metrics 与两个服务器都读它。
	flowMetrics := metrics.New()
	// E-5：每个协议各自一份连接限流器（per-protocol 独立计数）。
	rtspLimiter := limiter.New(*maxConns, *maxIPConns)
	rtmpLimiter := limiter.New(*maxConns, *maxIPConns)
	// E-8：白名单全局共享（同一份 CIDR 列表）；建连速率每协议一份，
	// 失败认证退避只有 RTSP 鉴权会用到。
	whitelist, err := access.ParseWhitelistFile(*whitelistFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "whitelist: %v\n", err)
		os.Exit(1)
	}
	if whitelist.Enabled() {
		fmt.Printf("access: whitelist enabled from %s\n", *whitelistFile)
	}
	rtspRate := access.NewRateLimiter(*connRate, *connBurst)
	rtmpRate := access.NewRateLimiter(*connRate, *connBurst)
	authBackoff := access.NewBackoff(*backoffThreshold, *backoffBase, *backoffMax)

	rtspSrv := rtsp.New(video, audio, *live)
	rtspSrv.SetRegistry(reg)
	rtspSrv.SetUsers(users)
	rtspSrv.SetCSeqCheck(*cseqCheck, *cseqMaxGap)
	rtspSrv.SetRedirect(*redirect)
	rtspSrv.SetMulticast(*multicastAddr, *multicastTTL)
	rtspSrv.SetDegradePolicy(*degradeLoss, *degradeSustain, *degradeRecover)
	rtspSrv.SetMetrics(flowMetrics)
	rtspSrv.SetLimits(rtspLimiter)
	rtspSrv.SetMaxSessions(*maxSessions)
	rtspSrv.SetWhitelist(whitelist)
	rtspSrv.SetRateLimit(rtspRate)
	rtspSrv.SetBackoff(authBackoff)
	// 切换basic / digest认证
	if *authMode == "digest" {
		rtspSrv.SetAuthScheme(rtsp.AuthDigest)
	}
	// E-6：配置了 -tls-cert/-tls-key 就让 RTSP 监听走 TLS（RTSPS）。
	if *tlsCert != "" {
		if err := rtspSrv.SetTLS(*tlsCert, *tlsKey); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	rtmpSrv := rtmp.New(reg, video, audio)
	rtmpSrv.SetMetrics(flowMetrics)
	rtmpSrv.SetLimits(rtmpLimiter)
	rtmpSrv.SetWhitelist(whitelist)
	rtmpSrv.SetRateLimit(rtmpRate)
	rtmpSrv.SetHandshakeTimeout(*handshakeTimeout)
	rtmpSrv.SetConnTimeout(*connTimeout)

	// E-4：Prometheus 指标端点。必须显式开启，且只允许回环地址。
	if *metricsAddr != "" {
		if err := startMetrics(ctx, *metricsAddr, flowMetrics, rtspSrv); err != nil {
			fmt.Fprintf(os.Stderr, "metrics: %v\n", err)
			os.Exit(2)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	// 任何一个监听失败就取消 ctx，让另一个也退出。
	serve := func(name string, addr string, f func(context.Context, string) error) {
		defer wg.Done()
		if err := f(ctx, addr); err != nil {
			fmt.Printf("%s listen failed on %s: %v\n", name, addr, err)
			stop()
		}
	}
	go serve("rtsp", ":8554", rtspSrv.Serve)
	go serve("rtmp", ":1935", rtmpSrv.Serve)
	wg.Wait()
}

// loopbackListenAddr 规范化调试/指标端点的监听地址，并强制只监听回环。
// ":6060" 会补成 "127.0.0.1:6060"；"0.0.0.0:6060"、局域网 IP、
// "localhost" 一律拒绝，避免把调试接口暴露到外网。
func loopbackListenAddr(raw string) (string, error) {
	host, port, err := net.SplitHostPort(raw)
	if err != nil {
		return "", fmt.Errorf("invalid listen address %q (want :6060 or 127.0.0.1:6060): %w", raw, err)
	}
	if port == "" {
		return "", fmt.Errorf("invalid listen address %q: missing port", raw)
	}
	if host == "" {
		host = "127.0.0.1"
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return "", fmt.Errorf("only loopback is allowed, got %q", host)
	}
	return net.JoinHostPort(host, port), nil
}

// startPprof 启动 pprof HTTP 服务，仅回环可达；ctx 取消时优雅关闭。
// block/mutex 采样只在开启 pprof 时设置，平时没有额外开销。
func startPprof(ctx context.Context, raw string) error {
	addr, err := loopbackListenAddr(raw)
	if err != nil {
		return err
	}
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)

	// 显式注册到独立 mux，不依赖 net/http/pprof 对 DefaultServeMux 的副作用。
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	if err := serveLoopback(ctx, addr, mux, "pprof"); err != nil {
		return err
	}
	fmt.Printf("pprof endpoints under http://%s/debug/pprof/\n", addr)
	return nil
}

// startMetrics 启动 Prometheus 文本端点（E-4），仅回环可达。
// 全局计数器来自 metrics.Registry，活跃会话的丢包 gauge 来自 rtsp.Server。
func startMetrics(ctx context.Context, raw string, reg *metrics.Registry, loss *rtsp.Server) error {
	addr, err := loopbackListenAddr(raw)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		reg.WritePrometheus(w)
		loss.WriteLossMetrics(w)
	})
	return serveLoopback(ctx, addr, mux, "metrics")
}

// serveLoopback 在 addr 上启动一个小型 HTTP 服务，ctx 取消时优雅关闭。
func serveLoopback(ctx context.Context, addr string, mux *http.ServeMux, name string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Printf("%s serve: %v\n", name, err)
		}
	}()
	fmt.Printf("%s listen on http://%s/ (loopback only)\n", name, addr)
	return nil
}

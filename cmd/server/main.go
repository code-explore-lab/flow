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
	"flow/internal/dash"
	"flow/internal/discovery"
	"flow/internal/hls"
	"flow/internal/httpflv"
	"flow/internal/limiter"
	"flow/internal/metrics"
	"flow/internal/origin"
	"flow/internal/rtmp"
	"flow/internal/rtsp"
	"flow/internal/source"
	"flow/internal/srt"
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
	// T-3：RTMP token 鉴权。token 放在推/拉流名的查询串里：?token=SECRET。
	rtmpToken := flag.String("rtmp-token", "", "T-3: RTMP token for both publish and play (also FLOW_RTMP_TOKEN)")
	rtmpPublishToken := flag.String("rtmp-publish-token", "", "T-3: RTMP token for publish only (also FLOW_RTMP_PUBLISH_TOKEN)")
	rtmpPlayToken := flag.String("rtmp-play-token", "", "T-3: RTMP token for play only (also FLOW_RTMP_PLAY_TOKEN)")
	rtmpAuthFile := flag.String("rtmp-auth-file", "", "T-3: RTMP token file, lines publish:TOKEN / play:TOKEN / all:TOKEN / app:NAME (also FLOW_RTMP_AUTH_FILE)")
	rtmpApps := flag.String("rtmp-app-whitelist", "", "T-3: comma-separated RTMP app whitelist (also FLOW_RTMP_APPS)")
	rtmpChunkSize := flag.Uint("rtmp-chunk-size", 4096, "T-7: RTMP outbound chunk size advertised to peers (bytes, 1..16777215)")
	publishPolicyFlag := flag.String("publish-policy", "replace", "T-8: duplicate publish policy: replace|reject|reject-if-active (applies to RTSP and RTMP)")
	// T-9：RTMP 回源。逗号分隔，每项 [本地名=]rtmp://host[:port]/app/stream。
	pull := flag.String("pull", "", "T-9: relay upstream RTMP, comma-separated [localName=]rtmp://host[:port]/app/stream")
	// S-1：按需回源。逗号分隔，每项 [name=]scheme://host[:port]/base。
	originFlag := flag.String("origin", "", "S-1: on-demand relay, comma-separated [name=]scheme://host[:port]/base (scheme: rtsp|rtmp|http)")
	originTimeout := flag.Duration("origin-timeout", 5*time.Second, "S-1: how long a request waits for first upstream media before falling back")
	// S-3：动态源注册与发现。节点把活跃流登记到注册中心，边缘节点按发现结果
	// 用 D-1 的 302 重定向；本地 janitor 负责源静默后的自动清理。
	nodeID := flag.String("node-id", "", "S-3: node identity advertised to the discovery registry (default hostname)")
	advertise := flag.String("advertise", "", "S-3: comma-separated public base URLs, e.g. rtsp://10.0.0.5:8554,rtmp://10.0.0.5:1935")
	discoveryURL := flag.String("discovery-url", "", "S-3: discovery registry base URL, e.g. http://127.0.0.1:7000 (empty disables)")
	discoveryTTL := flag.Duration("discovery-ttl", discovery.DefaultTTL, "S-3: lease TTL; nodes renew every TTL/3")
	discoveryTimeout := flag.Duration("discovery-timeout", 750*time.Millisecond, "S-3: per-request discovery lookup timeout")
	streamIdle := flag.Duration("stream-idle", stream.DefaultIdleTimeout, "S-3: unpublish streams with no media for this long (0 disables)")
	// S-2：边缘分发。redirect 只回 302；proxy 直接用发现结果回源，上游
	// 宕机后由本节点的 puller 切换，下游连接不中断。
	discoveryMode := flag.String("discovery-mode", "redirect", "S-2: redirect (RTSP 302) or proxy (edge pulls from the owning node)")
	discoveryMaxHops := flag.Int("discovery-max-hops", 1, "S-2: maximum relay hops used/advertised (0 = publisher only)")
	// E-6 / T-2：RTSPS 与 RTMPS 共用证书。两个 flag 必须成对出现；
	// 空表示 RTSP/RTMP 都保持明文。
	tlsCert := flag.String("tls-cert", "", "E-6/T-2: TLS certificate file for RTSPS/RTMPS (requires -tls-key)")
	tlsKey := flag.String("tls-key", "", "E-6/T-2: TLS private key file for RTSPS/RTMPS (requires -tls-cert)")
	// T-10：明文 RTSP/RTMP 监听地址可配置，方便 e2e 用空闲端口起服、
	// 与本机已占用 8554/1935 的进程共存。
	rtspAddr := flag.String("rtsp-addr", ":8554", "RTSP listen address")
	rtmpAddr := flag.String("rtmp-addr", ":1935", "RTMP listen address")
	// T-2：RTMPS 独立监听端口，明文 RTMP :1935 继续可用。
	rtmpsAddr := flag.String("rtmps-addr", ":1936", "T-2: RTMPS listen address (only used with -tls-cert/-tls-key)")
	// P-1：HTTP-FLV 分发。默认 :8080，空表示关闭。
	httpflvAddr := flag.String("httpflv-addr", ":8080", "P-1: HTTP-FLV listen address (empty disables)")
	httpflvCORS := flag.String("httpflv-cors", "", "P-1: allowed CORS origins, comma-separated (\"*\" allows any; empty disables CORS headers)")
	httpflvReferer := flag.String("httpflv-referer", "", "P-1: allowed Referer hosts, comma-separated (supports *.example.com; empty disables)")
	httpflvRequireReferer := flag.Bool("httpflv-require-referer", false, "P-1: reject requests without a Referer when -httpflv-referer is set")
	httpflvToken := flag.String("httpflv-token", "", "P-1: playback token, passed as ?token=... or X-Flow-Token (empty disables)")
	// P-2：HLS/LL-HLS 分发。默认 :8082，空表示关闭；part=0 退化成经典 HLS。
	hlsAddr := flag.String("hls-addr", ":8082", "P-2: HLS listen address (empty disables)")
	hlsSegment := flag.Duration("hls-segment", 4*time.Second, "P-2: HLS segment target duration")
	hlsPart := flag.Duration("hls-part", 500*time.Millisecond, "P-2: LL-HLS part target duration (0 disables LL)")
	hlsWindow := flag.Int("hls-window", 6, "P-2: number of complete segments kept in memory (>= 3)")
	hlsIdle := flag.Duration("hls-idle", 60*time.Second, "P-2: close a packager after this long without requests")
	hlsMaxBytes := flag.Int64("hls-max-bytes", 64<<20, "P-2: per-stream HLS memory budget in bytes (0 = only window limit)")
	hlsCORS := flag.String("hls-cors", "", "P-2: allowed CORS origins, comma-separated (\"*\" allows any; empty disables CORS headers)")
	hlsReferer := flag.String("hls-referer", "", "P-2: allowed Referer hosts, comma-separated (supports *.example.com; empty disables)")
	hlsRequireReferer := flag.Bool("hls-require-referer", false, "P-2: reject requests without a Referer when -hls-referer is set")
	hlsToken := flag.String("hls-token", "", "P-2: playback token, passed as ?token=... or X-Flow-Token (empty disables)")
	// P-3：MPEG-DASH 分发。复用 P-2 的切片管线，因此切片参数仍由 -hls-* 控制；
	// 这里只新增端口、MPD 刷新周期和安全策略。
	dashAddr := flag.String("dash-addr", ":8083", "P-3: MPEG-DASH listen address (empty disables)")
	dashUpdate := flag.Duration("dash-update", 2*time.Second, "P-3: MPD minimumUpdatePeriod")
	dashDelay := flag.Duration("dash-delay", 0, "P-3: MPD suggestedPresentationDelay (0 = 2x -hls-segment)")
	dashCORS := flag.String("dash-cors", "", "P-3: allowed CORS origins, comma-separated (\"*\" allows any; empty disables CORS headers)")
	dashReferer := flag.String("dash-referer", "", "P-3: allowed Referer hosts, comma-separated (supports *.example.com; empty disables)")
	dashRequireReferer := flag.Bool("dash-require-referer", false, "P-3: reject requests without a Referer when -dash-referer is set")
	dashToken := flag.String("dash-token", "", "P-3: playback token, appended to MPD segment URLs (empty disables)")
	// P-5：SRT 推拉流。SRT 里承载 MPEG-TS，与注册表互通后 RTSP/RTMP/
	// HTTP-FLV 都能拉同一路流。
	srtAddr := flag.String("srt-addr", ":9000", "P-5: SRT listen address (empty disables)")
	srtPassphrase := flag.String("srt-passphrase", "", "P-5: SRT encryption passphrase, 10-80 chars (empty disables)")
	srtLatency := flag.Duration("srt-latency", srt.DefaultLatency, "P-5: SRT receive latency")
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
	if *rtmpChunkSize == 0 || *rtmpChunkSize > 0xFFFFFF {
		fmt.Fprintf(os.Stderr, "invalid -rtmp-chunk-size %d (want 1..16777215)\n", *rtmpChunkSize)
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
	if *srtLatency <= 0 {
		fmt.Fprintln(os.Stderr, "invalid -srt-latency (must be > 0)")
		os.Exit(2)
	}
	if err := validateHLSOptions(*hlsSegment, *hlsPart, *hlsWindow, *hlsIdle, *hlsMaxBytes); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := validateDashOptions(*dashUpdate, *dashDelay); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	publishPolicy, err := stream.ParsePublishPolicy(*publishPolicyFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if *originTimeout <= 0 {
		fmt.Fprintln(os.Stderr, "invalid -origin-timeout (must be > 0)")
		os.Exit(2)
	}
	originSpecs, err := origin.ParseSpecs(*originFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	// S-3：发现配置。启用上报时必须提供可对外播放的 advertise 地址，
	// 否则注册中心只知道节点活着、不知道客户端该连哪里。
	if *streamIdle < 0 {
		fmt.Fprintln(os.Stderr, "invalid -stream-idle (must be >= 0)")
		os.Exit(2)
	}
	if *discoveryTTL <= 0 {
		fmt.Fprintln(os.Stderr, "invalid -discovery-ttl (must be > 0)")
		os.Exit(2)
	}
	if *discoveryTimeout <= 0 {
		fmt.Fprintln(os.Stderr, "invalid -discovery-timeout (must be > 0)")
		os.Exit(2)
	}
	if *discoveryMode != "redirect" && *discoveryMode != "proxy" {
		fmt.Fprintf(os.Stderr, "invalid -discovery-mode %q (want redirect or proxy)\n", *discoveryMode)
		os.Exit(2)
	}
	if *discoveryMaxHops < 0 {
		fmt.Fprintln(os.Stderr, "invalid -discovery-max-hops (must be >= 0)")
		os.Exit(2)
	}
	node := strings.TrimSpace(*nodeID)
	if node == "" {
		node, _ = os.Hostname()
		if node == "" {
			node = "flow"
		}
	}
	var advertiseMap map[string]string
	if *discoveryURL != "" {
		if !strings.HasPrefix(*discoveryURL, "http://") && !strings.HasPrefix(*discoveryURL, "https://") {
			fmt.Fprintf(os.Stderr, "invalid -discovery-url %q (want http://host:port)\n", *discoveryURL)
			os.Exit(2)
		}
		if strings.TrimSpace(*advertise) == "" {
			// 纯边缘代理可以只消费发现结果、不上报；redirect 模式必须
			// 告诉注册中心客户端该连哪里。
			if *discoveryMode == "redirect" {
				fmt.Fprintln(os.Stderr, "-discovery-url requires -advertise in redirect mode (e.g. rtsp://10.0.0.5:8554)")
				os.Exit(2)
			}
		} else {
			advertiseMap, err = discovery.ParseAdvertise(*advertise)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
		}
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

	// T-3：RTMP 鉴权，优先级同 RTSP：文件 < 环境变量 < 命令行参数。
	// token 通过流名查询串传递；日志只打拒绝原因，不打印明文 token。
	rtmpAuthCfg := rtmp.AuthConfig{}
	if path := envOrFlag(*rtmpAuthFile, "FLOW_RTMP_AUTH_FILE"); path != "" {
		loaded, err := rtmp.LoadAuthFile(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load rtmp auth file: %v\n", err)
			os.Exit(1)
		}
		rtmpAuthCfg = loaded
		fmt.Printf("rtmp auth: loaded credentials from %s\n", path)
	}
	rtmpAuthCfg = rtmpAuthCfg.Add(rtmp.AuthConfig{
		AllTokens:     oneOrNone(envOrFlag(*rtmpToken, "FLOW_RTMP_TOKEN")),
		PublishTokens: oneOrNone(envOrFlag(*rtmpPublishToken, "FLOW_RTMP_PUBLISH_TOKEN")),
		PlayTokens:    oneOrNone(envOrFlag(*rtmpPlayToken, "FLOW_RTMP_PLAY_TOKEN")),
		Apps:          splitCommaList(envOrFlag(*rtmpApps, "FLOW_RTMP_APPS")),
	})
	rtmpAuth := rtmp.NewAuth(rtmpAuthCfg)
	if rtmpAuth.Enabled() {
		fmt.Println("rtmp auth: enabled (tokens and/or app whitelist)")
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

	// 两个协议共用一份注册表，实现互通。同名推流策略在这里统一配置，
	// RTSP / RTMP / RTMPS 三个 Server 同时生效。
	reg := stream.NewRegistry()
	reg.SetPublishPolicy(publishPolicy)

	// S-3：本地空闲回收。推流端进程被强杀/半开 TCP 时协议层收不到 FIN，
	// 这里按「最近一次媒体写入」兜底摘除并关闭源，下游读到流结束而非挂死。
	// 回源（S-1）和 -pull（T-9）的流在各自构造时已标记豁免。
	if *streamIdle > 0 {
		janitor := stream.NewJanitor(reg, *streamIdle)
		go janitor.Run(ctx)
		fmt.Printf("stream janitor: drop streams with no media for %s (relay streams exempt)\n", janitor.Idle())
	}
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

	// S-3/S-2：先建发现客户端，S-2 的代理回源要用它解析上游节点。
	var discoveryClient *discovery.Client
	if *discoveryURL != "" {
		discoveryClient = discovery.NewClient(*discoveryURL, node, *discoveryTTL)
		discoveryClient.SetTimeout(*discoveryTimeout)
	}

	// S-1：按需回源管理器。配置了 -origin 或 S-2 代理发现时创建；
	// 其余情况保持 nil，服务端走原有 404/本地回退行为。
	var originMgr *origin.Manager
	if len(originSpecs) > 0 || (discoveryClient != nil && *discoveryMode == "proxy") {
		originMgr = origin.NewManager(ctx, reg, flowMetrics, originSpecs, *originTimeout)
		if discoveryClient != nil && *discoveryMode == "proxy" {
			originMgr.SetLocator(discoveryLocator{
				client:  discoveryClient,
				self:    node,
				maxHops: *discoveryMaxHops,
			}.Specs)
		}
		fmt.Printf("origin: %d template(s), discovery proxy=%v, wait up to %s for first media\n",
			len(originSpecs), discoveryClient != nil && *discoveryMode == "proxy", *originTimeout)
	}

	// S-3：把本地正在出帧的流按 TTL 租约登记到发现服务。整批心跳的语义
	// 是「本节点当前持有这些流」，源主动下线时一次心跳内即被清除；节点
	// 进程消失则由 TTL 过期兜底。S-2 代理模式可以只消费不上报。
	if discoveryClient != nil && len(advertiseMap) > 0 {
		announcer := discovery.NewAnnouncer(discoveryClient, reg, advertiseMap,
			*discoveryTTL/3, stream.DefaultActiveWindow)
		announcer.SetLoad(flowMetrics.ActiveViewers)
		announcer.SetMaxHops(*discoveryMaxHops)
		go announcer.Run(ctx)
		fmt.Printf("discovery: node %q advertising %d protocol(s) to %s, heartbeat %s, ttl %s\n",
			node, len(advertiseMap), *discoveryURL, announcer.Interval(), *discoveryTTL)
	} else if discoveryClient != nil {
		fmt.Printf("discovery: node %q consuming %s in %s mode (not advertising)\n",
			node, *discoveryURL, *discoveryMode)
	}

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
	if originMgr != nil {
		rtspSrv.SetOrigin(originMgr)
	}
	// S-3/S-2：redirect 模式下本地没有的流先查发现服务，命中后回 302
	// （D-1 语义）；proxy 模式由上面的 origin locator 直接回源，不再 302。
	if discoveryClient != nil && *discoveryMode == "redirect" {
		rtspSrv.SetDiscovery(discoveryFinder{
			client:  discoveryClient,
			self:    node,
			maxHops: *discoveryMaxHops,
		})
		rtspSrv.SetDiscoveryTimeout(*discoveryTimeout)
	}
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
	// T-2：RTMPS 与 RTMP 各起一个 Server 实例（共享注册表/指标/限流器），
	// 两个监听独立 accept、独立回收连接，互不影响关闭流程。
	newRTMP := func() *rtmp.Server {
		srv := rtmp.New(reg, video, audio)
		srv.SetMetrics(flowMetrics)
		srv.SetLimits(rtmpLimiter)
		srv.SetWhitelist(whitelist)
		srv.SetRateLimit(rtmpRate)
		srv.SetHandshakeTimeout(*handshakeTimeout)
		srv.SetConnTimeout(*connTimeout)
		srv.SetAuth(rtmpAuth)
		srv.SetChunkSize(uint32(*rtmpChunkSize))
		if originMgr != nil {
			srv.SetOrigin(originMgr)
		}
		return srv
	}
	rtmpSrv := newRTMP()
	var rtmpsSrv *rtmp.Server
	if *tlsCert != "" {
		rtmpsSrv = newRTMP()
		if err := rtmpsSrv.SetTLS(*tlsCert, *tlsKey); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	// P-1：HTTP-FLV 复用同一份注册表，RTSP/RTMP 推上来的流都能用
	// http://host:8080/live/<name>.flv 拉取。
	var httpflvSrv *httpflv.Server
	if *httpflvAddr != "" {
		httpflvSrv = httpflv.New(reg, video, audio)
		httpflvSrv.SetMetrics(flowMetrics)
		httpflvSrv.SetWhitelist(whitelist)
		httpflvSrv.SetCORS(*httpflvCORS)
		httpflvSrv.SetReferer(*httpflvReferer, *httpflvRequireReferer)
		httpflvSrv.SetToken(*httpflvToken)
		if originMgr != nil {
			httpflvSrv.SetOrigin(originMgr)
		}
	}

	// P-2：HLS/LL-HLS 复用同一份注册表与本地回退源。默认 :8082；
	// -hls-addr 为空时整体关闭（其它 -hls-* 参数仍做校验）。
	//
	// P-3：DASH 直接复用这个 hls.Server 的 manager。只要 HLS 或 DASH 有一个
	// 开启，就构造包装宿主；HLS 关闭时它不监听端口，只负责按需切片。
	hlsEnabled := *hlsAddr != ""
	dashEnabled := *dashAddr != ""
	var hlsSrv *hls.Server
	if hlsEnabled || dashEnabled {
		hlsSrv = hls.New(reg, video, audio)
		hlsSrv.SetMetrics(flowMetrics)
		hlsSrv.SetPackaging(hls.Options{
			SegmentTarget: *hlsSegment,
			PartTarget:    *hlsPart,
			Window:        *hlsWindow,
			MaxBytes:      *hlsMaxBytes,
		}, *hlsIdle)
		if originMgr != nil {
			hlsSrv.SetOrigin(originMgr)
		}
	}
	if hlsEnabled {
		hlsSrv.SetWhitelist(whitelist)
		hlsSrv.SetCORS(*hlsCORS)
		hlsSrv.SetReferer(*hlsReferer, *hlsRequireReferer)
		hlsSrv.SetToken(*hlsToken)
	}

	// P-3：MPEG-DASH。默认 :8083；与 HLS 共用同一份切片结果。
	var dashSrv *dash.Server
	if dashEnabled {
		dashSrv = dash.New(hlsSrv)
		dashSrv.SetMetrics(flowMetrics)
		dashSrv.SetWhitelist(whitelist)
		dashSrv.SetCORS(*dashCORS)
		dashSrv.SetReferer(*dashReferer, *dashRequireReferer)
		dashSrv.SetToken(*dashToken)
		dashSrv.SetManifestOptions(dash.Options{
			MinimumUpdatePeriod:        *dashUpdate,
			SegmentTarget:              *hlsSegment,
			SuggestedPresentationDelay: *dashDelay,
		})
	}

	// P-5：SRT 服务器。与其它协议共享注册表和指标；配置了口令后只接受
	// 加密连接（SRT 自带 AES 和 NAK 重传）。
	var srtSrv *srt.Server
	if *srtAddr != "" {
		srtSrv = srt.New(reg)
		srtSrv.SetMetrics(flowMetrics)
		srtSrv.SetLatency(*srtLatency)
		if err := srtSrv.SetPassphrase(*srtPassphrase); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}

	// E-4：Prometheus 指标端点。必须显式开启，且只允许回环地址。
	if *metricsAddr != "" {
		if err := startMetrics(ctx, *metricsAddr, flowMetrics, rtspSrv); err != nil {
			fmt.Fprintf(os.Stderr, "metrics: %v\n", err)
			os.Exit(2)
		}
	}

	// T-9：建立 RTMP 回源客户端。非法 spec 按参数错误退出（码 2）。
	var pulls []*rtmp.Client
	for _, spec := range splitCommaList(*pull) {
		cl, err := rtmp.NewClient(spec, reg, flowMetrics)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		cl.SetTimeouts(*handshakeTimeout, *connTimeout)
		pulls = append(pulls, cl)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	if rtmpsSrv != nil {
		wg.Add(1)
	}
	if httpflvSrv != nil {
		wg.Add(1)
	}
	if srtSrv != nil {
		wg.Add(1)
	}
	if hlsEnabled {
		wg.Add(1)
	}
	if dashEnabled {
		wg.Add(1)
	}
	wg.Add(len(pulls))
	// 任何一个监听失败就取消 ctx，让另一个也退出。
	serve := func(name string, addr string, f func(context.Context, string) error) {
		defer wg.Done()
		if err := f(ctx, addr); err != nil {
			fmt.Printf("%s listen failed on %s: %v\n", name, addr, err)
			stop()
		}
	}
	go serve("rtsp", *rtspAddr, rtspSrv.Serve)
	go serve("rtmp", *rtmpAddr, rtmpSrv.Serve)
	if rtmpsSrv != nil {
		go serve("rtmps", *rtmpsAddr, rtmpsSrv.Serve)
	}
	if httpflvSrv != nil {
		go serve("httpflv", *httpflvAddr, httpflvSrv.Serve)
	}
	if srtSrv != nil {
		go serve("srt", *srtAddr, srtSrv.Serve)
	}
	if hlsEnabled {
		go serve("hls", *hlsAddr, hlsSrv.Serve)
	}
	if dashEnabled {
		go serve("dash", *dashAddr, dashSrv.Serve)
	}
	// T-9：每个上游一路 goroutine，ctx 取消后自动退出并注销本地流。
	for _, cl := range pulls {
		go func(cl *rtmp.Client) {
			defer wg.Done()
			cl.Run(ctx)
		}(cl)
	}
	wg.Wait()
}

// validateHLSOptions 校验 P-2 的切片参数。part=0 表示经典 HLS；
// 其余情况 part 必须小于 segment，且窗口至少保留 3 段。
func validateHLSOptions(segment, part time.Duration, window int, idle time.Duration, maxBytes int64) error {
	if segment <= 0 {
		return errors.New("invalid -hls-segment (must be > 0)")
	}
	if part < 0 || part >= segment {
		return errors.New("invalid -hls-part (must be >= 0 and < -hls-segment)")
	}
	if window < 3 {
		return errors.New("invalid -hls-window (must be >= 3)")
	}
	if idle < 0 {
		return errors.New("invalid -hls-idle (must be >= 0)")
	}
	if maxBytes < 0 {
		return errors.New("invalid -hls-max-bytes (must be >= 0)")
	}
	return nil
}

// validateDashOptions 校验 P-3 的 MPD 参数。
func validateDashOptions(update, delay time.Duration) error {
	if update <= 0 {
		return errors.New("invalid -dash-update (must be > 0)")
	}
	if delay < 0 {
		return errors.New("invalid -dash-delay (must be >= 0)")
	}
	return nil
}

// discoveryFinder 把发现服务客户端适配成 rtsp.StreamFinder（S-3/S-2）。
// 在全部候选里按负载选一个，并忽略本节点和会让拓扑成环的条目。
type discoveryFinder struct {
	client  *discovery.Client
	self    string
	maxHops int
}

func (f discoveryFinder) Find(ctx context.Context, name string) (string, bool) {
	entries, err := f.client.LookupAll(ctx, name)
	if err != nil || len(entries) == 0 {
		return "", false
	}
	for _, scheme := range []string{"rtsp", "rtsps"} {
		if e, ok := discovery.Pick(entries, f.self, scheme, f.maxHops); ok {
			return e.URL(scheme), true
		}
	}
	return "", false
}

// discoveryLocator 把注册中心的候选转成 S-1 回源用的 Spec 列表（S-2）。
// 每个节点取一个可拉的协议地址（rtsp > rtmp > http），整体按负载排序；
// Hops/Upstream 会由 puller 写回本地流，announcer 据此上报副本拓扑。
type discoveryLocator struct {
	client  *discovery.Client
	self    string
	maxHops int
}

func (l discoveryLocator) Specs(ctx context.Context, name string) []origin.Spec {
	entries, err := l.client.LookupAll(ctx, name)
	if err != nil {
		// 发现服务不可用不应阻塞回源：静态 -origin 模板仍可兜底。
		return nil
	}
	var out []origin.Spec
	for _, e := range discovery.PickAll(entries, l.self, l.maxHops) {
		for _, scheme := range []string{"rtsp", "rtmp", "http"} {
			u := e.URL(scheme)
			if u == "" {
				continue
			}
			spec, err := origin.SpecFromURL(u)
			if err != nil {
				continue
			}
			spec.Hops = e.Hops + 1
			spec.Upstream = e.Node
			out = append(out, spec)
			break
		}
	}
	return out
}

// envOrFlag 返回命令行 flag 值，为空时回退到环境变量。
func envOrFlag(flagValue, envName string) string {
	if flagValue != "" {
		return flagValue
	}
	return os.Getenv(envName)
}

// oneOrNone 把非空字符串包成 0/1 个元素的切片，方便合并鉴权配置。
func oneOrNone(v string) []string {
	if v = strings.TrimSpace(v); v != "" {
		return []string{v}
	}
	return nil
}

// splitCommaList 按逗号切分，去掉空白和空项。
func splitCommaList(s string) []string {
	var out []string
	for v := range strings.SplitSeq(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
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

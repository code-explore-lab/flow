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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"flow/internal/access"
	"flow/internal/admin"
	"flow/internal/dash"
	"flow/internal/discovery"
	"flow/internal/health"
	"flow/internal/hls"
	"flow/internal/httpflv"
	"flow/internal/limiter"
	"flow/internal/metrics"
	"flow/internal/origin"
	"flow/internal/record"
	"flow/internal/rtmp"
	"flow/internal/rtsp"
	"flow/internal/snapshot"
	"flow/internal/source"
	"flow/internal/srt"
	"flow/internal/stream"
	"flow/internal/transcode"
	"flow/internal/tsingest"
	"flow/internal/webrtc"
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
	// O-1：HTTP 管理 API。空表示关闭；没有 token 时只允许回环监听。
	adminAddr := flag.String("admin-addr", "", "O-1: management HTTP API listen address, e.g. 127.0.0.1:9091 (empty disables)")
	adminToken := flag.String("admin-token", "", "O-1: bearer token for the management API (required for non-loopback listeners)")
	// O-4：健康检查与告警。默认保持原行为：任一监听口 bind 失败就退出；
	// 设为 false 时保留其它监听口继续运行，失败协议在 /readyz 和
	// /api/v1/alerts 里标记为 down，便于排查端口占用。
	listenFatal := flag.Bool("listen-failure-fatal", true, "O-4: exit when a protocol listener fails to bind (false keeps other listeners running so /readyz can report it)")
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
	// F-3：把注册表里的实时流转封装成 MP4/FLV/TS 文件。
	recordNames := flag.String("record", "", "F-3: comma-separated stream names to record (\"*\" = all; empty disables)")
	recordDir := flag.String("record-dir", "recordings", "F-3: recording output directory")
	recordFormatFlag := flag.String("record-format", "mp4", "F-3: recording container: mp4|flv|ts")
	recordSegment := flag.Duration("record-segment", 60*time.Second, "F-3: rotate to a new file at the first keyframe after this duration")
	recordQuota := flag.Int64("record-quota", 0, "F-3: total byte budget for recordings in -record-dir (0 = unlimited; oldest files are removed first)")
	recordRetention := flag.Duration("record-retention", 0, "F-3: delete completed recordings older than this (0 = keep forever)")
	recordResume := flag.Bool("record-resume", true, "F-3: append to an unfinished file after a crash (state is kept in <dir>/<stream>.state)")
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
	// P-4：WebRTC（WHIP 推 / WHEP 拉）。ICE/DTLS/SRTP 交给 pion/webrtc，
	// 媒体仍走仓库自己的 RTP 封包/解包，并写进同一个注册表。
	webrtcAddr := flag.String("webrtc-addr", ":8084", "P-4: WebRTC (WHIP/WHEP) HTTP listen address (empty disables)")
	webrtcPublicIP := flag.String("webrtc-public-ip", "", "P-4: comma-separated public IPs for NAT 1:1 ICE candidates")
	webrtcICEServers := flag.String("webrtc-ice-servers", "", "P-4: comma-separated STUN/TURN URLs, e.g. stun:stun.l.google.com:19302")
	webrtcICEUser := flag.String("webrtc-ice-username", "", "P-4: TURN username applied to all -webrtc-ice-servers")
	webrtcICEPass := flag.String("webrtc-ice-credential", "", "P-4: TURN credential")
	webrtcUDPPorts := flag.String("webrtc-udp-ports", "", "P-4: ICE UDP port range min:max (empty = ephemeral)")
	webrtcCORS := flag.String("webrtc-cors", "", "P-4: allowed CORS origins, comma-separated (\"*\" allows any; empty disables)")
	webrtcToken := flag.String("webrtc-token", "", "P-4: WHIP/WHEP token, passed as ?token=... or X-Flow-Token (empty disables)")
	webrtcLoopback := flag.Bool("webrtc-loopback", false, "P-4: include loopback ICE candidates (local browser/tests)")
	webrtcGatherTimeout := flag.Duration("webrtc-gather-timeout", 3*time.Second, "P-4: ICE gathering timeout for non-trickle answers")

	// F-6：截图/缩略图。
	snapshotAddr := flag.String("snapshot-addr", ":8085", "F-6: snapshot HTTP listen address (empty disables)")
	snapshotTimeout := flag.Duration("snapshot-timeout", snapshot.DefaultTimeout, "F-6: how long to wait for a keyframe")
	snapshotCache := flag.Duration("snapshot-cache", snapshot.DefaultCacheTTL, "F-6: reuse an encoded image for this long (0 disables)")
	snapshotConcurrency := flag.Int("snapshot-concurrency", snapshot.DefaultConcurrency, "F-6: concurrent decode limit")
	snapshotQuality := flag.Int("snapshot-quality", snapshot.DefaultQuality, "F-6: default JPEG quality (1..100)")
	snapshotMaxWidth := flag.Int("snapshot-max-width", 0, "F-6: default thumbnail max width (0 = original size)")
	snapshotFFmpeg := flag.String("snapshot-ffmpeg", "", "F-6: ffmpeg path for H.265 / undecodable H.264 (empty disables)")
	snapshotFFmpegTimeout := flag.Duration("snapshot-ffmpeg-timeout", 5*time.Second, "F-6: ffmpeg decode timeout")
	snapshotCORS := flag.String("snapshot-cors", "", "F-6: allowed CORS origins, comma-separated (\"*\" allows any; empty disables)")
	snapshotReferer := flag.String("snapshot-referer", "", "F-6: allowed Referer hosts, comma-separated (supports *.example.com; empty disables)")
	snapshotRequireReferer := flag.Bool("snapshot-require-referer", false, "F-6: reject requests without a Referer when -snapshot-referer is set")
	snapshotToken := flag.String("snapshot-token", "", "F-6: snapshot token, passed as ?token=... or X-Flow-Token (empty disables)")

	// F-4：转码。每个档位把源流缩放/重编码后以新流名发布回注册表，
	// 下游 RTSP/RTMP/HTTP-FLV/HLS/DASH 都能直接拉。
	transcodeSpec := flag.String("transcode", "", "F-4: transcode profiles, e.g. \"live/test=w=1280,h=720;live/test=size=640x360,out=live/test_360p\" (empty disables)")
	transcodeFFmpeg := flag.String("transcode-ffmpeg", "ffmpeg", "F-4: ffmpeg binary used for transcoding")
	transcodePreset := flag.String("transcode-preset", transcode.DefaultPreset, "F-4: software encoder preset")
	transcodeGOP := flag.Duration("transcode-gop", transcode.DefaultGOP, "F-4: forced keyframe interval")
	transcodeHW := flag.String("transcode-hw", "none", "F-4: hardware encoder: none|nvenc|qsv")
	transcodeABR := flag.Bool("transcode-abr", true, "F-5: expose same-source transcode profiles as an HLS ABR master playlist")

	// P-6：MPEG-TS 输入。UDP（单播/组播）与本地文件解复用后写进同一个
	// 注册表，RTSP/RTMP/HTTP-FLV/HLS 都能直接拉。
	tsUDP := flag.String("ts-udp", "", "P-6: UDP TS inputs, comma-separated name=host:port, e.g. live/udp=239.1.1.1:5000 (empty disables)")
	tsUDPIdle := flag.Duration("ts-udp-idle", 10*time.Second, "P-6: unpublish a UDP input after this long without packets")
	tsFile := flag.String("ts-file", "", "P-6: file TS inputs, comma-separated name=path.ts (empty disables)")
	tsFileLoop := flag.Bool("ts-file-loop", false, "P-6: restart -ts-file inputs at EOF with continuous timestamps")
	tsFileRate := flag.Float64("ts-file-rate", 1, "P-6: -ts-file playback speed (1 = by PTS, 2 = 2x)")
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
	if *snapshotQuality < 0 || *snapshotQuality > 100 {
		fmt.Fprintln(os.Stderr, "invalid -snapshot-quality (must be 0..100)")
		os.Exit(2)
	}
	if *snapshotConcurrency < 1 {
		fmt.Fprintln(os.Stderr, "invalid -snapshot-concurrency (must be >= 1)")
		os.Exit(2)
	}
	if *snapshotMaxWidth < 0 {
		fmt.Fprintln(os.Stderr, "invalid -snapshot-max-width (must be >= 0)")
		os.Exit(2)
	}
	if *snapshotTimeout <= 0 {
		fmt.Fprintln(os.Stderr, "invalid -snapshot-timeout (must be > 0)")
		os.Exit(2)
	}
	if *snapshotCache < 0 {
		fmt.Fprintln(os.Stderr, "invalid -snapshot-cache (must be >= 0)")
		os.Exit(2)
	}
	if *snapshotFFmpegTimeout <= 0 {
		fmt.Fprintln(os.Stderr, "invalid -snapshot-ffmpeg-timeout (must be > 0)")
		os.Exit(2)
	}
	// O-1：管理接口能中断推流，没有 token 时强制回环监听；配置了 token
	// 才允许绑定非回环地址。
	adminListen := ""
	if *adminAddr != "" {
		adminListen = *adminAddr
		if *adminToken == "" {
			resolved, err := loopbackListenAddr(*adminAddr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "admin: %v (set -admin-token to allow non-loopback)\n", err)
				os.Exit(2)
			}
			adminListen = resolved
		}
	}
	if err := validateHLSOptions(*hlsSegment, *hlsPart, *hlsWindow, *hlsIdle, *hlsMaxBytes); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := validateDashOptions(*dashUpdate, *dashDelay); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	recordFormat, err := record.ParseFormat(*recordFormatFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := validateRecordOptions(*recordSegment, *recordQuota, *recordRetention, recordFormat); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	transcodeProfiles, err := transcode.ParseProfiles(*transcodeSpec)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := validateTranscodeOptions(*transcodeGOP, *transcodeHW); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	tsUDPInputs, err := tsingest.ParseUDPInputs(*tsUDP)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	tsFileInputs, err := tsingest.ParseFileInputs(*tsFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if *tsUDPIdle <= 0 {
		fmt.Fprintln(os.Stderr, "invalid -ts-udp-idle (must be > 0)")
		os.Exit(2)
	}
	if *tsFileRate <= 0 {
		fmt.Fprintln(os.Stderr, "invalid -ts-file-rate (must be > 0)")
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

	// F-3：录制管理器。只录制注册表里命中名字的流；轮询启动，
	// 因此 RTMP 首帧建立轨之后才会真正开始。
	var recordMgr *record.Manager
	if recordList := splitCommaList(*recordNames); len(recordList) > 0 {
		recordMgr = record.NewManager(reg, record.Options{
			Dir:             *recordDir,
			Format:          recordFormat,
			SegmentDuration: *recordSegment,
			Quota:           *recordQuota,
			Retention:       *recordRetention,
			Resume:          *recordResume,
		}, recordList)
		fmt.Printf("record: %s to %s (segment %s, quota %d, resume=%v)\n",
			strings.Join(recordList, ","), *recordDir, *recordSegment, *recordQuota, *recordResume)
	}

	// S-3：本地空闲回收。推流端进程被强杀/半开 TCP 时协议层收不到 FIN，
	// 这里按「最近一次媒体写入」兜底摘除并关闭源，下游读到流结束而非挂死。
	// 回源（S-1）和 -pull（T-9）的流在各自构造时已标记豁免。
	var janitor *stream.Janitor
	if *streamIdle > 0 {
		janitor = stream.NewJanitor(reg, *streamIdle)
		go janitor.Run(ctx)
		fmt.Printf("stream janitor: drop streams with no media for %s (relay streams exempt)\n", janitor.Idle())
	}
	// O-4：健康聚合。监听口状态由各 Serve 在 bind 成功后上报；流的上下线
	// 通过注册表观察者实时感知，静默流由周期扫描 + Janitor.SweepOnce 兜底。
	healthChecker := health.New(health.Options{Registry: reg, Janitor: janitor})
	go healthChecker.Run(ctx)
	fmt.Printf("health: stream alert window %s, scan interval 1s\n", healthChecker.ActiveWindow())
	// E-4：两个协议共用一个指标注册表，/metrics 与两个服务器都读它。
	flowMetrics := metrics.New()

	// F-4：转码管理器。同样轮询注册表，源流（重新）上线后按档位起
	// ffmpeg；输出以新流名发布，和其他协议共用同一份注册表。
	var transcodeMgr *transcode.Manager
	if len(transcodeProfiles) > 0 {
		transcodeMgr = transcode.NewManager(reg, transcode.Options{
			FFmpegPath: *transcodeFFmpeg,
			Preset:     *transcodePreset,
			GOP:        *transcodeGOP,
			HW:         *transcodeHW,
			Metrics:    flowMetrics,
		}, transcodeProfiles)
		for _, p := range transcodeProfiles {
			fmt.Printf("transcode: %s → %s (%dx%d %s %d bps)\n",
				p.Source, p.Output, p.Width, p.Height, p.Codec, p.Bitrate)
		}
	}

	// P-6：TS 输入。UDP 空闲超时后摘流、下一包到达时重新发布；文件输入
	// 按 PTS 节奏播放，播完（或 ctx 取消）后摘流。
	for _, in := range tsUDPInputs {
		fmt.Printf("ts-udp: %s ← %s (idle %s)\n", in.Name, in.Addr, *tsUDPIdle)
	}
	for _, in := range tsFileInputs {
		fmt.Printf("ts-file: %s ← %s (loop=%v rate=%g)\n", in.Name, in.Path, *tsFileLoop, *tsFileRate)
	}
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
	// O-4：会话超时/断网被 Sweep 回收时上报会话异常告警。
	rtspSrv.SetSessionExpireHook(func(id, remote string, _, lastActive time.Time) {
		healthChecker.RecordSessionExpired("rtsp", id, remote, lastActive)
	})
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
	// O-1：管理 API。流列表直接来自共享注册表；会话查询和踢除由各协议
	// 服务器实现 admin.Backend。配置项只放非敏感值。
	var adminSrv *admin.Server
	if adminListen != "" {
		backends := []admin.Backend{rtspSrv, rtmpSrv}
		if rtmpsSrv != nil {
			backends = append(backends, rtmpsSrv)
		}
		adminSrv = admin.New(reg, backends...)
		adminSrv.SetToken(*adminToken)
		adminSrv.SetHealth(healthChecker)
		adminSrv.SetConfig(map[string]string{
			"node":          node,
			"live":          strconv.FormatBool(*live),
			"publishPolicy": publishPolicy.String(),
			"rtspAddr":      *rtspAddr,
			"rtmpAddr":      *rtmpAddr,
			"rtmpsAddr":     *rtmpsAddr,
			"httpflvAddr":   *httpflvAddr,
			"hlsAddr":       *hlsAddr,
			"dashAddr":      *dashAddr,
			"srtAddr":       *srtAddr,
			"webrtcAddr":    *webrtcAddr,
			"snapshotAddr":  *snapshotAddr,
			"multicastAddr": *multicastAddr,
			"rtspAuthUsers": strconv.Itoa(len(users)),
			"rtmpAuth":      strconv.FormatBool(rtmpAuth.Enabled()),
			"discoveryMode": *discoveryMode,
			"discovery":     strconv.FormatBool(discoveryClient != nil),
			"streamIdle":    streamIdle.String(),
		})
		if *adminToken != "" {
			fmt.Printf("admin api listen on http://%s/api/v1/ (bearer token required)\n", adminListen)
		} else {
			fmt.Printf("admin api listen on http://%s/api/v1/ (loopback, no token)\n", adminListen)
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
		// F-5：同一源的多档转码输出合成一个 ABR 阶梯。
		if *transcodeABR {
			if groups := abrGroups(transcodeProfiles); len(groups) > 0 {
				hlsSrv.SetABR(groups)
				for _, g := range groups {
					fmt.Printf("abr: %s (%d variants)\n", g.Source, len(g.Variants))
				}
			}
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

	// P-4：WebRTC（WHIP/WHEP）。推上来的 H.264/Opus 写进同一个注册表，
	// WHEP 再从注册表订阅，因此与 RTSP/RTMP/HTTP-FLV/SRT/HLS/DASH 互通。
	var webrtcSrv *webrtc.Server
	if *webrtcAddr != "" {
		webrtcSrv = webrtc.New(reg)
		webrtcSrv.SetMetrics(flowMetrics)
		webrtcSrv.SetWhitelist(whitelist)
		webrtcSrv.SetCORS(*webrtcCORS)
		webrtcSrv.SetToken(*webrtcToken)
		webrtcSrv.SetIncludeLoopback(*webrtcLoopback)
		webrtcSrv.SetGatherTimeout(*webrtcGatherTimeout)
		if err := webrtcSrv.SetPublicIPs(splitCommaList(*webrtcPublicIP)); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		webrtcSrv.SetICEServers(splitCommaList(*webrtcICEServers), *webrtcICEUser, *webrtcICEPass)
		if *webrtcUDPPorts != "" {
			minPort, maxPort, err := parsePortRange(*webrtcUDPPorts)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
			if err := webrtcSrv.SetUDPPortRange(minPort, maxPort); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
		}
		if originMgr != nil {
			webrtcSrv.SetOrigin(originMgr)
		}
	}

	// F-6：截图/缩略图。从注册表的视频源取最近一个 IDR，纯 Go 解码成
	// JPEG/PNG；未配置 ffmpeg 时 H.265 返回 415。
	var snapshotSrv *snapshot.Server
	if *snapshotAddr != "" {
		snapshotSrv = snapshot.New(reg, video)
		snapshotSrv.SetMetrics(flowMetrics)
		snapshotSrv.SetWhitelist(whitelist)
		snapshotSrv.SetOptions(snapshot.Options{
			Timeout:       *snapshotTimeout,
			CacheTTL:      *snapshotCache,
			Concurrency:   *snapshotConcurrency,
			Quality:       *snapshotQuality,
			MaxWidth:      *snapshotMaxWidth,
			FFmpegPath:    *snapshotFFmpeg,
			FFmpegTimeout: *snapshotFFmpegTimeout,
		})
		snapshotSrv.SetCORS(*snapshotCORS)
		snapshotSrv.SetReferer(*snapshotReferer, *snapshotRequireReferer)
		snapshotSrv.SetToken(*snapshotToken)
		if originMgr != nil {
			snapshotSrv.SetOrigin(originMgr)
		}
	}

	// E-4：Prometheus 指标端点。必须显式开启，且只允许回环地址。
	if *metricsAddr != "" {
		if err := startMetrics(ctx, *metricsAddr, flowMetrics, rtspSrv, healthChecker); err != nil {
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
	if webrtcSrv != nil {
		wg.Add(1)
	}
	if snapshotSrv != nil {
		wg.Add(1)
	}
	if adminSrv != nil {
		wg.Add(1)
	}
	if hlsEnabled {
		wg.Add(1)
	}
	if dashEnabled {
		wg.Add(1)
	}
	if recordMgr != nil {
		wg.Add(1)
	}
	if transcodeMgr != nil {
		wg.Add(1)
	}
	wg.Add(len(pulls))
	wg.Add(len(tsUDPInputs) + len(tsFileInputs))

	// O-4：先登记所有将启用的监听口（pending），Serve bind 成功后通过
	// health.NotifyListenerReady 置为 up；失败则由下面的 serve 包装置为
	// down。可选协议只在真正启用时登记。
	healthChecker.RegisterListener("rtsp", *rtspAddr)
	healthChecker.RegisterListener("rtmp", *rtmpAddr)
	if rtmpsSrv != nil {
		healthChecker.RegisterListener("rtmps", *rtmpsAddr)
	}
	if httpflvSrv != nil {
		healthChecker.RegisterListener("httpflv", *httpflvAddr)
	}
	if srtSrv != nil {
		healthChecker.RegisterListener("srt", *srtAddr)
	}
	if webrtcSrv != nil {
		healthChecker.RegisterListener("webrtc", *webrtcAddr)
	}
	if snapshotSrv != nil {
		healthChecker.RegisterListener("snapshot", *snapshotAddr)
	}
	if adminSrv != nil {
		healthChecker.RegisterListener("admin", adminListen)
	}
	if hlsEnabled {
		healthChecker.RegisterListener("hls", *hlsAddr)
	}
	if dashEnabled {
		healthChecker.RegisterListener("dash", *dashAddr)
	}

	// 默认行为与之前一致：任何一个监听失败就取消 ctx，让其它协议也退出。
	// -listen-failure-fatal=false 时保留其它监听口，失败原因交给
	// health（/readyz 503 + port_bind_failed 告警）暴露。
	serve := func(name string, addr string, f func(context.Context, string) error) {
		defer wg.Done()
		serveCtx := health.WithListenerReady(ctx, func() { healthChecker.ListenerUp(name) })
		if err := f(serveCtx, addr); err != nil {
			healthChecker.ListenerDown(name, err)
			fmt.Printf("%s listen failed on %s: %v\n", name, addr, err)
			if *listenFatal {
				stop()
			}
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
	if webrtcSrv != nil {
		go serve("webrtc", *webrtcAddr, webrtcSrv.Serve)
	}
	if snapshotSrv != nil {
		go serve("snapshot", *snapshotAddr, snapshotSrv.Serve)
	}
	if adminSrv != nil {
		go serve("admin", adminListen, adminSrv.Serve)
	}
	if hlsEnabled {
		go serve("hls", *hlsAddr, hlsSrv.Serve)
	}
	if dashEnabled {
		go serve("dash", *dashAddr, dashSrv.Serve)
	}
	if recordMgr != nil {
		go func() {
			defer wg.Done()
			recordMgr.Run(ctx)
		}()
	}
	if transcodeMgr != nil {
		go func() {
			defer wg.Done()
			transcodeMgr.Run(ctx)
		}()
	}
	// T-9：每个上游一路 goroutine，ctx 取消后自动退出并注销本地流。
	for _, cl := range pulls {
		go func(cl *rtmp.Client) {
			defer wg.Done()
			cl.Run(ctx)
		}(cl)
	}
	// P-6：UDP 输入长期运行（含收包空闲时），文件输入播完或 ctx 取消后返回。
	for _, in := range tsUDPInputs {
		go func(in tsingest.UDPInput) {
			defer wg.Done()
			if err := tsingest.RunUDP(ctx, reg, in.Name, tsingest.UDPOptions{Addr: in.Addr, Idle: *tsUDPIdle}); err != nil {
				fmt.Printf("ts-udp %s: %v\n", in.Name, err)
			}
		}(in)
	}
	for _, in := range tsFileInputs {
		go func(in tsingest.FileInput) {
			defer wg.Done()
			err := tsingest.RunFile(ctx, reg, in.Name, in.Path, tsingest.FileOptions{Loop: *tsFileLoop, Rate: *tsFileRate})
			if err != nil {
				fmt.Printf("ts-file %s: %v\n", in.Name, err)
			}
		}(in)
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

// validateRecordOptions 校验 F-3 的录制参数。
// validateTranscodeOptions 校验 F-4 的参数。输出档位本身由
// transcode.ParseProfiles 校验，这里只看全局编码选项。
func validateTranscodeOptions(gop time.Duration, hw string) error {
	if gop <= 0 {
		return errors.New("invalid -transcode-gop (must be > 0)")
	}
	switch strings.ToLower(hw) {
	case "", "none", "nvenc", "qsv":
		return nil
	default:
		return fmt.Errorf("invalid -transcode-hw %q (want none, nvenc or qsv)", hw)
	}
}

// abrGroups 把同一源流的两档及以上转码输出归为一个 HLS ABR 阶梯。
// 只有一档的源保持原来的单流 master，不做无意义的聚合。
func abrGroups(profiles []transcode.Profile) []hls.ABRGroup {
	var order []string
	bySource := make(map[string][]hls.ABRVariant, len(profiles))
	for _, p := range profiles {
		if _, ok := bySource[p.Source]; !ok {
			order = append(order, p.Source)
		}
		bySource[p.Source] = append(bySource[p.Source], hls.ABRVariant{
			Stream:    p.Output,
			Width:     p.Width,
			Height:    p.Height,
			Bitrate:   uint32(p.Bitrate),
			FrameRate: p.FPS,
			Codec:     p.Codec,
		})
	}
	out := make([]hls.ABRGroup, 0, len(order))
	for _, src := range order {
		if vs := bySource[src]; len(vs) >= 2 {
			out = append(out, hls.ABRGroup{Source: src, Variants: vs})
		}
	}
	return out
}

func validateRecordOptions(segment time.Duration, quota int64, retention time.Duration, format record.Format) error {
	if segment <= 0 {
		return errors.New("invalid -record-segment (must be > 0)")
	}
	if quota < 0 {
		return errors.New("invalid -record-quota (must be >= 0)")
	}
	if retention < 0 {
		return errors.New("invalid -record-retention (must be >= 0)")
	}
	if format != record.FormatMP4 && format != record.FormatFLV && format != record.FormatTS {
		return errors.New("invalid -record-format (want mp4, flv or ts)")
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

// parsePortRange 解析 "min:max" 形式的 UDP 端口范围（P-4 ICE）。
func parsePortRange(s string) (uint16, uint16, error) {
	minStr, maxStr, ok := strings.Cut(strings.TrimSpace(s), ":")
	if !ok {
		return 0, 0, fmt.Errorf("invalid -webrtc-udp-ports %q (want min:max)", s)
	}
	minPort, err := strconv.ParseUint(strings.TrimSpace(minStr), 10, 16)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid -webrtc-udp-ports %q: %w", s, err)
	}
	maxPort, err := strconv.ParseUint(strings.TrimSpace(maxStr), 10, 16)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid -webrtc-udp-ports %q: %w", s, err)
	}
	return uint16(minPort), uint16(maxPort), nil
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
func startMetrics(ctx context.Context, raw string, reg *metrics.Registry, loss *rtsp.Server, healthChecker *health.Checker) error {
	addr, err := loopbackListenAddr(raw)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		reg.WritePrometheus(w)
		loss.WriteLossMetrics(w)
		// O-4：flow_ready / flow_alerts / 监听口状态。
		healthChecker.WritePrometheus(w)
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

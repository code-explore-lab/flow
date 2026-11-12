package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

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
	live := flag.Bool("live", false, "read Annex-B H.264 from stdin (e.g. ffmpeg camera pipe); audio disabled")
	redirect := flag.String("redirect", "", "answer OPTIONS/DESCRIBE with 302 + this Location instead of serving streams")
	multicastAddr := flag.String("multicast-addr", "239.255.0.1", "UDP multicast group address (empty disables multicast)")
	multicastTTL := flag.Int("multicast-ttl", 1, "UDP multicast TTL (1 = loopback/本网段)")
	user := flag.String("user", "", "RTSP auth username (also FLOW_RTSP_USER; empty with -pass disables auth)")
	pass := flag.String("pass", "", "RTSP auth password (also FLOW_RTSP_PASS)")
	authFile := flag.String("auth-file", "", "credential file, username:password per line (also FLOW_RTSP_AUTH_FILE)")
	authMode := flag.String("auth-mode", "basic", "RTSP auth scheme: basic or digest")
	cseqCheck := flag.Bool("cseq-check", true, "reject replayed or out-of-order CSeq on RTSP connections")
	cseqMaxGap := flag.Int("cseq-max-gap", 0, "reject CSeq jumps larger than this (0 = unlimited)")
	flag.Parse()
	if *authMode != "basic" && *authMode != "digest" {
		fmt.Fprintf(os.Stderr, "invalid -auth-mode %q (want basic or digest)\n", *authMode)
		os.Exit(2)
	}
	if *cseqMaxGap < 0 {
		fmt.Fprintln(os.Stderr, "invalid -cseq-max-gap (must be >= 0)")
		os.Exit(2)
	}
	if *redirect != "" && !strings.HasPrefix(*redirect, "rtsp://") && !strings.HasPrefix(*redirect, "rtsps://") {
		fmt.Fprintf(os.Stderr, "invalid -redirect %q (want rtsp://host:port/path)\n", *redirect)
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
	video := source.NewH264Source("testdata/test.h264")
	audio := source.NewAACSource("testdata/test.aac")
	if *live {
		video = source.NewH264SourceFromReader(os.Stdin)
		audio = nil // 实时模式没有配套音频源，只有视频轨
	}

	// Ctrl+C / SIGTERM 触发优雅退出。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 两个协议共用一份注册表，实现互通。
	reg := stream.NewRegistry()
	rtspSrv := rtsp.New(video, audio, *live)
	rtspSrv.SetRegistry(reg)
	rtspSrv.SetUsers(users)
	rtspSrv.SetCSeqCheck(*cseqCheck, *cseqMaxGap)
	rtspSrv.SetRedirect(*redirect)
	rtspSrv.SetMulticast(*multicastAddr, *multicastTTL)
	if *authMode == "digest" {
		rtspSrv.SetAuthScheme(rtsp.AuthDigest)
	}
	rtmpSrv := rtmp.New(reg, video, audio)

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

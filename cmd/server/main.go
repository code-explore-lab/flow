package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"flow/internal/rtsp"
	"flow/internal/source"
)

// 入口：解析参数、搭好媒体源、启动 RTSP 服务器。
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
	flag.Parse()

	video := source.NewH264Source("testdata/test.h264")
	audio := source.NewAACSource("testdata/test.aac")
	if *live {
		video = source.NewH264SourceFromReader(os.Stdin)
		audio = nil // 实时模式没有配套音频源，只有视频轨
	}

	// Ctrl+C / SIGTERM 触发优雅退出。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := rtsp.New(video, audio, *live)
	if err := srv.Serve(ctx, ":8554"); err != nil {
		fmt.Printf("listen failed: %v\n", err)
	}
}

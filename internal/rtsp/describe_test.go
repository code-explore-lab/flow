package rtsp

import (
	"io"
	"strings"
	"testing"
	"time"

	"flow/internal/source"
)

// TestBuildSDPLiveParamSets 覆盖 live 模式 DESCRIBE 的 sprop-parameter-sets：
// 源未就绪时省略（维持现状），参数集已缓存时补上。
// TestBuildSDPAdvertisesRTCPMux 覆盖 R-12：本地文件/live 的 SDP 也要
// 宣告 a=rtcp-mux，客户端才会在 SETUP 里用 client_port=A-A 接受单端口。
func TestBuildSDPAdvertisesRTCPMux(t *testing.T) {
	s := New(nil, nil, true)
	sdp, err := s.buildSDP("rtsp://127.0.0.1:8554/live")
	if err != nil {
		t.Fatalf("buildSDP: %v", err)
	}
	if !strings.Contains(sdp, "a=rtcp-mux\r\n") {
		t.Fatalf("SDP missing a=rtcp-mux:\n%s", sdp)
	}
}

func TestBuildSDPLiveParamSets(t *testing.T) {
	t.Run("源未就绪省略 sprop", func(t *testing.T) {
		s := New(nil, nil, true) // live 且无视频源
		sdp, err := s.buildSDP("rtsp://127.0.0.1:8554/live")
		if err != nil {
			t.Fatalf("buildSDP: %v", err)
		}
		if !strings.Contains(sdp, "a=fmtp:96 packetization-mode=1") {
			t.Fatalf("SDP missing video fmtp: %q", sdp)
		}
		if strings.Contains(sdp, "sprop-parameter-sets=") {
			t.Fatalf("SDP should not contain sprop when source not ready: %q", sdp)
		}
	})

	t.Run("参数集已缓存则补上 sprop", func(t *testing.T) {
		pr, pw := io.Pipe()
		vid := source.NewH264SourceFromReader(pr)
		s := New(vid, nil, true)

		// 启动源（第一次 Subscribe 模拟 PLAY），保持 pipe 不关闭让 run 停在
		// NextNALU，cur 不会被 finish 清掉。
		_, cancel := vid.Subscribe()
		defer cancel()

		sps := []byte{0x67, 0x42, 0xC0, 0x1E}
		pps := []byte{0x68, 0xCE, 0x3C, 0x80}
		idr := []byte{0x65, 0x88, 0x84, 0x00}
		// 写 SPS/PPS/IDR：流式 demux 读到下一个起始码才会吐出上一个 NALU，
		// 只写 SPS/PPS 时 PPS 会卡在内部缓冲区，参数集永远就绪不了。
		go func() {
			_, _ = pw.Write(append(append(append([]byte{0, 0, 0, 1}, sps...), append([]byte{0, 0, 0, 1}, pps...)...), append([]byte{0, 0, 0, 1}, idr...)...))
		}()

		deadline := time.Now().Add(time.Second)
		for vid.SPSPPSBase64() == "" && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if vid.SPSPPSBase64() == "" {
			pw.Close()
			t.Fatal("param sets not cached in time")
		}

		sdp, err := s.buildSDP("rtsp://127.0.0.1:8554/live")
		pw.Close()
		if err != nil {
			t.Fatalf("buildSDP: %v", err)
		}
		if !strings.Contains(sdp, "sprop-parameter-sets="+vid.SPSPPSBase64()) {
			t.Fatalf("SDP missing cached sprop: %q", sdp)
		}
	})
}

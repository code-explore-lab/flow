package transcode

import (
	"context"
	"io"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"flow/internal/media"
	"flow/internal/source"
	"flow/internal/stream"
)

// loadParentStream 用 ffmpeg 现场生成一段可解码的 640x480 H.264，
// 把全部 NALU 写进 ExternalSource（时间戳按访问单元递增）。
// testdata/test.h264 只是 NAL 片段，ffmpeg 无法解码，不能用于集成测试。
func loadParentStream(t *testing.T, ffmpeg string) *stream.Stream {
	t.Helper()
	path := filepath.Join(t.TempDir(), "src.h264")
	cmd := exec.Command(ffmpeg,
		"-hide_banner", "-loglevel", "error", "-y",
		"-f", "lavfi", "-i", "testsrc=size=640x480:rate=25",
		"-t", "2",
		"-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
		"-pix_fmt", "yuv420p", "-f", "h264", path)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generate source: %v\n%s", err, out)
	}
	demux, err := media.NewH264Demux(path)
	if err != nil {
		t.Fatal(err)
	}

	st := &stream.Stream{}
	st.SetVideoParams("H264", nil, nil, nil)
	src := st.SourceFor("video")
	if src == nil {
		t.Fatal("no video source")
	}

	var ts uint32
	started := false
	for {
		nalu, err := demux.NextNALU()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		cls := media.ClassifyVideoNAL("H264", nalu)
		newAU := cls.IsVCL && media.StartsVideoAU("H264", nalu)
		if newAU {
			if started {
				ts += 3600
			} else {
				started = true
			}
		}
		src.Write(source.Frame{
			Data:      nalu,
			Timestamp: ts,
			Keyframe:  cls.IsIDR,
			AUStart:   newAU,
		})
	}
	return st
}

// TestTranscoderEndToEnd 用真实 ffmpeg 走一遍完整链路：
// 640x480 H.264 源 → 缩放 320x240 → 重新编码 → 新流名发布回注册表。
// 没有 ffmpeg 的环境自动跳过。
func TestTranscoderEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping ffmpeg integration test in -short mode")
	}
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg not found in PATH")
	}

	reg := stream.NewRegistry()
	parent := loadParentStream(t, ffmpeg)
	pub, err := reg.Publish("live/src", parent)
	if err != nil || pub != parent {
		t.Fatalf("publish parent: %v", err)
	}

	profile := Profile{
		Source:  "live/src",
		Output:  "live/src_240p",
		Width:   320,
		Height:  240,
		Codec:   "H264",
		Bitrate: 400_000,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tr, err := NewTranscoder(ctx, reg, parent, profile, Options{
		FFmpegPath: ffmpeg,
		Logf:       t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	tr.Start()
	defer tr.Close()

	out := reg.Get(profile.Output)
	if out == nil {
		t.Fatalf("output stream %q not registered", profile.Output)
	}

	// 等输出 SPS 出现，解析分辨率。
	deadline := time.Now().Add(20 * time.Second)
	var info *media.SPSInfo
	for time.Now().Before(deadline) {
		if vp, ok := out.VideoParams(); ok && len(vp.SPS) > 0 {
			if parsed, err := media.ParseSPS(vp.SPS); err == nil {
				info = parsed
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if info == nil {
		t.Fatal("no output video parameters within 20s")
	}
	if info.Width != 320 || info.Height != 240 {
		t.Errorf("output size = %dx%d, want 320x240", info.Width, info.Height)
	}

	// 至少收到一个完整的输出访问单元。
	done := make(chan struct{})
	timer := time.AfterFunc(10*time.Second, func() { close(done) })
	defer timer.Stop()

	rd, unsubscribe := out.SourceFor("video").Subscribe()
	defer unsubscribe()
	var nalu int
	for nalu == 0 {
		f, ok := rd.Next(done)
		if !ok {
			t.Fatal("timed out waiting for an output frame")
		}
		if len(f.Data) > 0 {
			nalu++
		}
	}

	// Close 后输出流必须从注册表摘除，否则下次启动会被同名冲突挡住。
	tr.Close()
	if reg.Get(profile.Output) != nil {
		t.Errorf("output stream still registered after Close")
	}
}

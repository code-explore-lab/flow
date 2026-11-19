package transcode

import (
	"log"
	"strconv"
	"time"

	"flow/internal/metrics"
)

const (
	initialRestartBackoff = 500 * time.Millisecond
	defaultMaxBackoff     = 10 * time.Second
	defaultPollInterval   = 500 * time.Millisecond
	defaultJoinDelay      = 500 * time.Millisecond
)

// Options 是转码管理器的运行参数。
type Options struct {
	FFmpegPath string
	Preset     string
	GOP        time.Duration
	HW         string // "none" / "nvenc" / "qsv"

	Metrics *metrics.Registry
	Logf    func(format string, args ...any)
	Now     func() time.Time

	// Interval 是注册表轮询间隔；JoinDelay 是等待音轨出现的时间
	// （RTMP 的音轨往往比视频晚几百毫秒）。
	Interval  time.Duration
	JoinDelay time.Duration

	// MaxRestartBackoff 是 ffmpeg 连续失败时的最大重启间隔。
	MaxRestartBackoff time.Duration
}

func (o Options) withDefaults() Options {
	if o.FFmpegPath == "" {
		o.FFmpegPath = "ffmpeg"
	}
	if o.Preset == "" {
		o.Preset = DefaultPreset
	}
	if o.GOP <= 0 {
		o.GOP = DefaultGOP
	}
	if o.HW == "" {
		o.HW = "none"
	}
	if o.Logf == nil {
		o.Logf = log.Printf
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Interval <= 0 {
		o.Interval = defaultPollInterval
	}
	if o.JoinDelay <= 0 {
		o.JoinDelay = defaultJoinDelay
	}
	if o.MaxRestartBackoff <= 0 {
		o.MaxRestartBackoff = defaultMaxBackoff
	}
	return o
}

// buildArgs 拼出 ffmpeg 命令行。
//
// 输入、输出都是 MPEG-TS，走 stdin/stdout：Windows 上 Go 不支持向子进程
// 传多个额外管道，而 TS 单管道能同时携带视频和 PTS/DTS。
// 只转视频，音频在 Go 侧直通（见 transcoder.audioLoop）。
func buildArgs(p Profile, o Options) []string {
	encoder, software := encoderFor(p, o.HW)
	args := []string{
		// 注意不要加 -fflags nobuffer：管道输入无法回退，探测期间读到的
		// 数据会被直接丢弃，开头的 IDR 被吞掉后解码器一帧都解不出来。
		"-hide_banner", "-nostdin", "-loglevel", "warning",
		"-flags", "low_delay",
		"-analyzeduration", "0", "-probesize", "32768",
		"-f", "mpegts", "-i", "pipe:0",
		"-map", "0:v:0", "-an", "-sn", "-dn",
		"-vf", "scale=" + strconv.Itoa(p.Width) + ":" + strconv.Itoa(p.Height) + ":flags=bicubic,setsar=1",
		"-c:v", encoder,
		"-pix_fmt", "yuv420p",
		"-b:v", strconv.Itoa(p.Bitrate),
		"-maxrate", strconv.Itoa(p.Bitrate * 11 / 10),
		"-bufsize", strconv.Itoa(p.Bitrate * 2),
		"-force_key_frames", "expr:gte(t,n_forced*" + formatSeconds(o.GOP) + ")",
		"-sc_threshold", "0",
		"-bf", "0",
	}
	if software {
		args = append(args, "-preset", o.Preset, "-tune", "zerolatency")
	} else if o.HW == "nvenc" {
		// NVENC 的 preset 是 p1..p7，低延迟用 ll；沿用 -transcode-preset
		// 会传入无效值，因此这里固定。
		args = append(args, "-preset", "p4", "-tune", "ll")
	} else {
		args = append(args, "-preset", o.Preset, "-look_ahead", "0")
	}
	if p.FPS > 0 {
		args = append(args, "-r", strconv.FormatFloat(p.FPS, 'f', -1, 64), "-fps_mode", "cfr")
	} else {
		args = append(args, "-fps_mode", "passthrough")
	}
	args = append(args,
		"-copyts",
		"-avoid_negative_ts", "disabled",
		"-muxdelay", "0", "-muxpreload", "0",
		"-flush_packets", "1",
		"-f", "mpegts", "pipe:1",
	)
	return args
}

func encoderFor(p Profile, hw string) (name string, software bool) {
	software = hw == "" || hw == "none"
	hevc := p.Codec == "H265"
	switch {
	case software && hevc:
		return "libx265", true
	case software:
		return "libx264", true
	case hw == "nvenc" && hevc:
		return "hevc_nvenc", false
	case hw == "nvenc":
		return "h264_nvenc", false
	case hw == "qsv" && hevc:
		return "hevc_qsv", false
	default:
		return "h264_qsv", false
	}
}

func formatSeconds(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64)
}

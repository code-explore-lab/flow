// Package transcode 用外部 ffmpeg 进程把注册表里的一路流解码、缩放、重新编码，
// 再把结果作为新流发布回注册表（F-4）。
//
// Go 侧只负责帧源订阅、MPEG-TS 封装/解封装和进程生命周期；编解码交给
// ffmpeg。这样不需要 cgo，编码器崩溃也只影响这一路转码，可以按退避重启。
package transcode

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultBitrate 是未指定 bitrate 时的输出码率。
	DefaultBitrate = 2_000_000
	// DefaultPreset 是软件编码器的默认 preset。
	DefaultPreset = "veryfast"
	// DefaultGOP 是未指定时的强制关键帧间隔。下游 HLS/录制/中途加入
	// 都需要周期性的 IDR。
	DefaultGOP = 2 * time.Second
)

// Profile 一路转码输出档位。
//
// 配置格式（多路用分号或换行分隔）：
//
//	live/test=w=1280,h=720
//	live/test=w=640,h=360,bitrate=800k,out=live/test_360p
//
// 支持的字段：w / h（或 size=1280x720）、codec=h264|h265、bitrate=2000k、
// fps=25、out=输出流名。输出流名默认是 <源流名>_<高度>p。
type Profile struct {
	Source  string
	Output  string
	Width   int
	Height  int
	Codec   string  // "H264" / "H265"
	Bitrate int     // bits/s
	FPS     float64 // 0 = 保持源帧率
}

// Label 返回日志里使用的简短名字。
func (p Profile) Label() string {
	return p.Source + "→" + p.Output
}

// ParseProfiles 解析 -transcode 的配置串。空串返回 nil。
func ParseProfiles(value string) ([]Profile, error) {
	var out []Profile
	seenOut := make(map[string]bool)
	for _, raw := range splitProfiles(value) {
		p, err := ParseProfile(raw)
		if err != nil {
			return nil, err
		}
		if seenOut[p.Output] {
			return nil, fmt.Errorf("transcode: duplicate output stream %q", p.Output)
		}
		seenOut[p.Output] = true
		out = append(out, p)
	}
	return out, nil
}

func splitProfiles(value string) []string {
	replacer := strings.NewReplacer(";", "\n")
	var out []string
	for _, line := range strings.Split(replacer.Replace(value), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			out = append(out, line)
		}
	}
	return out
}

// ParseProfile 解析单路配置：source=key=value,key=value。
func ParseProfile(spec string) (Profile, error) {
	spec = strings.TrimSpace(spec)
	source, rest, ok := strings.Cut(spec, "=")
	if !ok {
		return Profile{}, fmt.Errorf("transcode: %q: want source=w=...,h=...", spec)
	}
	source = strings.TrimSpace(source)
	if source == "" {
		return Profile{}, fmt.Errorf("transcode: %q: empty source stream name", spec)
	}

	p := Profile{Source: source, Codec: "H264", Bitrate: DefaultBitrate}
	seenSize := false
	for _, field := range strings.Split(rest, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			return Profile{}, fmt.Errorf("transcode: %q: invalid field %q (want key=value)", spec, field)
		}
		key = strings.ToLower(strings.TrimSpace(key))
		value = strings.TrimSpace(value)
		switch key {
		case "w", "width":
			n, err := parsePositiveInt(value)
			if err != nil {
				return Profile{}, fmt.Errorf("transcode: %q: %w", spec, err)
			}
			p.Width = n
			seenSize = true
		case "h", "height":
			n, err := parsePositiveInt(value)
			if err != nil {
				return Profile{}, fmt.Errorf("transcode: %q: %w", spec, err)
			}
			p.Height = n
			seenSize = true
		case "size":
			w, h, err := parseSize(value)
			if err != nil {
				return Profile{}, fmt.Errorf("transcode: %q: %w", spec, err)
			}
			p.Width, p.Height = w, h
			seenSize = true
		case "codec":
			switch strings.ToLower(value) {
			case "h264", "avc", "avc1":
				p.Codec = "H264"
			case "h265", "hevc", "hvc1":
				p.Codec = "H265"
			default:
				return Profile{}, fmt.Errorf("transcode: %q: unsupported codec %q (want h264 or h265)", spec, value)
			}
		case "bitrate", "b":
			n, err := parseBitrate(value)
			if err != nil {
				return Profile{}, fmt.Errorf("transcode: %q: %w", spec, err)
			}
			p.Bitrate = n
		case "fps", "r":
			f, err := strconv.ParseFloat(value, 64)
			if err != nil || f <= 0 || f > 240 {
				return Profile{}, fmt.Errorf("transcode: %q: invalid fps %q", spec, value)
			}
			p.FPS = f
		case "out", "output":
			p.Output = strings.TrimSpace(value)
		default:
			return Profile{}, fmt.Errorf("transcode: %q: unknown field %q", spec, key)
		}
	}

	if !seenSize || p.Width <= 0 || p.Height <= 0 {
		return Profile{}, fmt.Errorf("transcode: %q: w and h are required", spec)
	}
	// yuv420p 要求偶数尺寸；向下取整而不是报错，避免用户写 853x480。
	p.Width &^= 1
	p.Height &^= 1
	if p.Width < 2 || p.Height < 2 {
		return Profile{}, fmt.Errorf("transcode: %q: size %dx%d is too small", spec, p.Width, p.Height)
	}
	if p.Bitrate < 16_000 || p.Bitrate > 100_000_000 {
		return Profile{}, fmt.Errorf("transcode: %q: bitrate %d out of range", spec, p.Bitrate)
	}
	if p.Output == "" {
		p.Output = fmt.Sprintf("%s_%dp", p.Source, p.Height)
	}
	if p.Output == p.Source {
		return Profile{}, fmt.Errorf("transcode: %q: output stream must differ from source", spec)
	}
	return p, nil
}

func parsePositiveInt(value string) (int, error) {
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid positive integer %q", value)
	}
	return n, nil
}

func parseSize(value string) (int, int, error) {
	w, h, ok := strings.Cut(strings.ToLower(value), "x")
	if !ok {
		return 0, 0, fmt.Errorf("invalid size %q (want 1280x720)", value)
	}
	width, err := parsePositiveInt(w)
	if err != nil {
		return 0, 0, err
	}
	height, err := parsePositiveInt(h)
	if err != nil {
		return 0, 0, err
	}
	return width, height, nil
}

// parseBitrate 支持纯数字（bps）以及 k/m 后缀（1000/1024 进制不敏感，
// 这里按 1000 处理，与码率常用写法一致）。
func parseBitrate(value string) (int, error) {
	value = strings.TrimSpace(strings.ToLower(value))
	mult := 1
	switch {
	case strings.HasSuffix(value, "k"):
		mult = 1000
		value = strings.TrimSuffix(value, "k")
	case strings.HasSuffix(value, "m"):
		mult = 1_000_000
		value = strings.TrimSuffix(value, "m")
	}
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid bitrate %q", value)
	}
	// 防止 mult 溢出 int；上限在 ParseProfile 里统一校验。
	if n > 100_000_000/mult {
		return 100_000_001, nil
	}
	return n * mult, nil
}

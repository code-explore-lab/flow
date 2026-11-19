package snapshot

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"flow/internal/media"
)

// annexB 把裸 NALU 拼成 Annex-B 字节流（4 字节起始码），供 ffmpeg 的
// h264/hevc demuxer 读取。
func annexB(nalus [][]byte) []byte {
	var buf bytes.Buffer
	startCode := []byte{0, 0, 0, 1}
	for _, nalu := range nalus {
		if len(nalu) == 0 {
			continue
		}
		buf.Write(startCode)
		buf.Write(nalu)
	}
	return buf.Bytes()
}

// encodeWithFFmpeg 用外部 ffmpeg 解码一个 IDR 访问单元并输出图片。
// 用于 H.265，以及 hi264 不支持的 H.264 码流（POC type=1、隔行等）。
// 参数固定、不经过 shell，输入只走 stdin。
func encodeWithFFmpeg(ctx context.Context, path, codec string, nalus [][]byte, format string, quality, maxWidth int) ([]byte, string, error) {
	demuxer := "h264"
	if media.IsHEVCCodec(codec) {
		demuxer = "hevc"
	}
	args := []string{
		"-hide_banner", "-loglevel", "error",
		"-f", demuxer, "-i", "pipe:0",
		"-frames:v", "1",
	}
	if maxWidth > 0 {
		// ffmpeg 表达式里的逗号需要引号，避免被当成 filter 分隔符。
		args = append(args, "-vf", fmt.Sprintf("scale=w='min(iw,%d)':h=-2", maxWidth))
	}
	if format == FormatPNG {
		args = append(args, "-f", "image2pipe", "-c:v", "png", "pipe:1")
	} else {
		// quality 1..100 映射到 mjpeg 的 qscale 31..2。
		q := min(max(31-quality*29/100, 2), 31)
		args = append(args, "-f", "mjpeg", "-q:v", strconv.Itoa(q), "pipe:1")
	}

	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Stdin = bytes.NewReader(annexB(nalus))
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return nil, "", fmt.Errorf("ffmpeg: %w: %s", err, msg)
		}
		return nil, "", fmt.Errorf("ffmpeg: %w", err)
	}
	if stdout.Len() == 0 {
		return nil, "", errors.New("ffmpeg: empty output")
	}
	if format == FormatPNG {
		return stdout.Bytes(), "image/png", nil
	}
	return stdout.Bytes(), "image/jpeg", nil
}

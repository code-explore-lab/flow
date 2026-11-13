package rtsp

// G-3 Scale / Rate。
//
// PLAY 的 Scale 头表示播放速率（RFC 2326 §12.34）：Scale: 2.0 表示以两倍
// 速度播放，0.5 表示半速。它只改变墙上时钟的发送节奏，不改变每帧的 RTP
// 时间戳步进，所以客户端看到的时间轴保持不变。
//
// live/推流源没有可调速的文件，非 1 倍速一律 551 Option Not Supported；
// Scale: 1.0 视为无操作，避免误伤带默认值的客户端。

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// parseScale 解析 Scale 头的值：必须是有限正数。
func parseScale(header string) (float64, error) {
	v, err := strconv.ParseFloat(strings.TrimSpace(header), 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 {
		return 0, fmt.Errorf("invalid Scale %q", header)
	}
	return v, nil
}

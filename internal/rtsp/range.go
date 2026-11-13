package rtsp

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// G-1 Range 解析。
//
// PLAY 可以带 Range: npt=... 指定播放区间。本服务只做「解析 + 校验 + 记录」：
//   - live 模式（stdin 实时源、ANNOUNCE 推上来的流）没有可定位的文件，
//     只接受从 0 开始的开放区间 npt=0-，其余一律 457 Invalid Range；
//   - 文件模式记录起点/终点，真正按位置读文件由 G-2 的 seek 完成。
//
// 语法见 RFC 2326 §3.5：
//
//	range   = npt-time "-" [ npt-time ] / "now-"
//	npt-time = npt-sec / npt-hhmmss
//	npt-sec  = 1*DIGIT [ "." *DIGIT ]
//	npt-hhmmss = npt-hh ":" npt-mm ":" npt-ss [ "." *DIGIT ]
type nptRange struct {
	start    time.Duration // 起点；now 或未写起点时为 0
	end      time.Duration // 终点（仅 hasEnd 时有效）
	hasStart bool
	hasEnd   bool
	now      bool // npt=now-：实时流当前位置
}

// parseNPTRange 解析 Range 头的值，例如 "npt=0-"、"npt=3.5-10"、
// "npt=00:01:00-00:02:00"。单位只支持 npt，其他（clock=）返回错误。
func parseNPTRange(header string) (nptRange, error) {
	var r nptRange
	unit, spec, ok := strings.Cut(strings.TrimSpace(header), "=")
	if !ok || !strings.EqualFold(strings.TrimSpace(unit), "npt") {
		return r, fmt.Errorf("unsupported range %q (want npt=...)", header)
	}
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return r, fmt.Errorf("empty npt range %q", header)
	}

	// 时间本身不含 '-'，第一个 '-' 就是区间分隔符。
	startStr, endStr, hasEnd := strings.Cut(spec, "-")
	startStr = strings.TrimSpace(startStr)
	endStr = strings.TrimSpace(endStr)

	switch {
	case startStr == "":
		// npt=-10：从流开头到 10s。
		r.hasStart = true
	case strings.EqualFold(startStr, "now"):
		r.now = true
		r.hasStart = true
	default:
		d, err := parseNPTTime(startStr)
		if err != nil {
			return nptRange{}, fmt.Errorf("invalid npt start %q: %w", startStr, err)
		}
		r.start = d
		r.hasStart = true
	}

	if hasEnd && endStr != "" {
		d, err := parseNPTTime(endStr)
		if err != nil {
			return nptRange{}, fmt.Errorf("invalid npt end %q: %w", endStr, err)
		}
		r.end = d
		r.hasEnd = true
	}
	if r.hasEnd && r.end <= r.start {
		return nptRange{}, fmt.Errorf("npt end %s must be after start %s", r.end, r.start)
	}
	return r, nil
}

// parseNPTTime 解析一个 npt-time：秒数（可带小数）、mm:ss 或 hh:mm:ss。
func parseNPTTime(s string) (time.Duration, error) {
	parts := strings.Split(s, ":")
	if len(parts) > 3 {
		return 0, fmt.Errorf("too many time fields")
	}

	sec, err := strconv.ParseFloat(parts[len(parts)-1], 64)
	if err != nil || sec < 0 || (len(parts) > 1 && sec >= 60) {
		return 0, fmt.Errorf("invalid seconds %q", parts[len(parts)-1])
	}

	var hh, mm int
	if len(parts) == 3 {
		hh, err = strconv.Atoi(parts[0])
		if err != nil || hh < 0 {
			return 0, fmt.Errorf("invalid hours %q", parts[0])
		}
	}
	if len(parts) >= 2 {
		mm, err = strconv.Atoi(parts[len(parts)-2])
		if err != nil || mm < 0 || mm > 59 {
			return 0, fmt.Errorf("invalid minutes %q", parts[len(parts)-2])
		}
	}

	total := float64(hh)*3600 + float64(mm)*60 + sec
	return time.Duration(total * float64(time.Second)), nil
}

// validForLive 判断 live 流能否接受这个区间：只允许从 0 开始的开放区间
// （npt=0-）。npt=5-、npt=0-10、npt=now- 都返回 false。
func (r nptRange) validForLive() bool {
	return r.hasStart && !r.now && r.start == 0 && !r.hasEnd
}

package tsingest

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// UDPInput 一路 UDP TS 输入：把 Addr 上收到的 TS 发布成 Name。
type UDPInput struct {
	Name string
	Addr string
}

// FileInput 一路文件 TS 输入：把 Path 播放成 Name。
type FileInput struct {
	Name string
	Path string
}

// ParseUDPInputs 解析 -ts-udp 的配置串，形如
//
//	live/udp=239.1.1.1:5000,live/udp2=0.0.0.0:5001
//
// 空串返回 nil。条目之间用逗号、分号或换行分隔。
func ParseUDPInputs(value string) ([]UDPInput, error) {
	var out []UDPInput
	seen := make(map[string]bool)
	for _, raw := range splitInputs(value) {
		name, target, err := parseInput(raw, "name=host:port")
		if err != nil {
			return nil, fmt.Errorf("tsingest: -ts-udp: %w", err)
		}
		if err := validateUDPAddr(target); err != nil {
			return nil, fmt.Errorf("tsingest: -ts-udp: %q: %w", raw, err)
		}
		if seen[name] {
			return nil, fmt.Errorf("tsingest: -ts-udp: duplicate stream name %q", name)
		}
		seen[name] = true
		out = append(out, UDPInput{Name: name, Addr: target})
	}
	return out, nil
}

// ParseFileInputs 解析 -ts-file 的配置串，形如
//
//	live/file=clips/demo.ts
//
// 空串返回 nil。条目之间用逗号、分号或换行分隔。
func ParseFileInputs(value string) ([]FileInput, error) {
	var out []FileInput
	seen := make(map[string]bool)
	for _, raw := range splitInputs(value) {
		name, path, err := parseInput(raw, "name=path/to/file.ts")
		if err != nil {
			return nil, fmt.Errorf("tsingest: -ts-file: %w", err)
		}
		if seen[name] {
			return nil, fmt.Errorf("tsingest: -ts-file: duplicate stream name %q", name)
		}
		seen[name] = true
		out = append(out, FileInput{Name: name, Path: path})
	}
	return out, nil
}

func splitInputs(value string) []string {
	replacer := strings.NewReplacer(";", "\n", ",", "\n")
	var out []string
	for _, line := range strings.Split(replacer.Replace(value), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			out = append(out, line)
		}
	}
	return out
}

func parseInput(raw, want string) (name, target string, err error) {
	name, target, ok := strings.Cut(raw, "=")
	if !ok {
		return "", "", fmt.Errorf("%q: want %s", raw, want)
	}
	name = strings.TrimSpace(name)
	target = strings.TrimSpace(target)
	if name == "" {
		return "", "", fmt.Errorf("%q: empty stream name", raw)
	}
	if target == "" {
		return "", "", fmt.Errorf("%q: empty target", raw)
	}
	return name, target, nil
}

func validateUDPAddr(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid address %q: %w", addr, err)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n <= 0 || n > 65535 {
		return fmt.Errorf("invalid port %q", port)
	}
	if host != "" && net.ParseIP(host) == nil {
		return fmt.Errorf("host %q must be an IP address (empty means all interfaces)", host)
	}
	return nil
}

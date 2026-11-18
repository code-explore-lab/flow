// Package origin 实现 S-1 按需回源（边缘节点级联）。
//
// 本地注册表没有某路流时，三个下游协议（RTSP / RTMP / HTTP-FLV）把流名交给
// Manager.Ensure；Manager 按 -origin 模板匹配上游地址，懒启动一路 puller：
//
//	上游握手 → 收到首帧（Ready）→ 以本地流名登记到注册表
//	上游断开 → 指数退避重连（不关闭本地源，下游继续等帧）
//	首次回源时上游明确不存在 → 立即失败，下游走原有 404/本地文件回退
//	Ready 之后上游短暂消失（源站重启）→ 按退避继续重连，不放弃
//
// 回源只在第一次被请求时启动，不做空闲回收：与 T-9 的透明续传语义一致，
// 避免下游正在播放时源被摘掉。
package origin

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// defaultPorts 是三种协议省略端口时的默认值。
var defaultPorts = map[string]string{
	"rtsp": "554",
	"rtmp": "1935",
	"http": "80",
}

// Spec 一条 -origin 模板。
//
//	[name=]scheme://host[:port]/base
//
// Local 非空是显式映射：本地流名等于 Local 时，从 Raw 这个精确 URL 拉。
// 否则是通配模板：本地缺 X 时拉 scheme://host/base/X（http 追加 .flv）。
type Spec struct {
	Local  string // 显式本地流名；空表示通配
	Scheme string // rtsp / rtmp / http
	Host   string // host[:port]，缺省端口时已补齐
	Base   string // 路径前缀，去掉首尾斜杠；通配时拼在流名前
	Raw    string // 显式映射的完整 URL（含 query/userinfo）
	// Query / User 是通配模板的上游 URL 参数：RTMP 的 ?token= 拉流鉴权、
	// RTSP URL 里的 user:pass。显式映射已把它们保存在 Raw 里。
	Query string
	User  *url.Userinfo
}

// ParseSpecs 解析逗号分隔的 -origin 配置。空串返回 nil。
func ParseSpecs(raw string) ([]Spec, error) {
	var out []Spec
	for i, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		s, err := parseSpec(item)
		if err != nil {
			return nil, fmt.Errorf("origin spec #%d: %w", i+1, err)
		}
		out = append(out, s)
	}
	return out, nil
}

func parseSpec(item string) (Spec, error) {
	local := ""
	rawURL := item
	if i := strings.IndexByte(item, '='); i >= 0 {
		// 只把 scheme 之前的 "=" 当作本地名；query 里的 "=" 不算。
		if j := strings.Index(item, "://"); j < 0 || i < j {
			local = strings.TrimSpace(item[:i])
			rawURL = strings.TrimSpace(item[i+1:])
		}
	}
	if local != "" && strings.ContainsAny(local, " /?#") {
		return Spec{}, fmt.Errorf("invalid local name %q", local)
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return Spec{}, fmt.Errorf("parse %q: %w", rawURL, err)
	}
	scheme := strings.ToLower(u.Scheme)
	if _, ok := defaultPorts[scheme]; !ok {
		return Spec{}, fmt.Errorf("unsupported scheme %q in %q (want rtsp, rtmp or http)", u.Scheme, item)
	}
	if u.Host == "" {
		return Spec{}, fmt.Errorf("missing host in %q", item)
	}

	host := u.Host
	if u.Port() == "" {
		host = net.JoinHostPort(u.Hostname(), defaultPorts[scheme])
	}

	base := strings.Trim(u.Path, "/")
	if local != "" {
		if base == "" {
			return Spec{}, fmt.Errorf("explicit mapping %q needs a stream path", item)
		}
		// 显式映射保留原始 URL（包括 query/token），但补齐默认端口。
		explicit := *u
		explicit.Scheme = scheme
		explicit.Host = host
		return Spec{
			Local:  local,
			Scheme: scheme,
			Host:   host,
			Base:   base,
			Raw:    explicit.String(),
			Query:  u.RawQuery,
			User:   u.User,
		}, nil
	}
	return Spec{Scheme: scheme, Host: host, Base: base, Query: u.RawQuery, User: u.User}, nil
}

// urlFor 生成本地流名 name 的上游 URL。显式映射忽略 name。
func (s Spec) urlFor(name string) string {
	if s.Local != "" {
		return s.Raw
	}
	streamName := s.streamName(name)
	path := streamName
	if s.Base != "" {
		path = s.Base + "/" + streamName
	}
	u := url.URL{Scheme: s.Scheme, Host: s.Host, Path: "/" + path, RawQuery: s.Query, User: s.User}
	if s.Scheme == "http" && !strings.HasSuffix(strings.ToLower(path), ".flv") {
		u.Path += ".flv"
	}
	return u.String()
}

// streamName 从本地流名推出要拼到上游 base 后面的部分。
// 本地名常常带应用前缀（RTSP 注册成 live/src），而 -origin 的 base 通常
// 已经包含同一个应用（rtmp://host/live），这里去掉重复的前缀。
func (s Spec) streamName(name string) string {
	name = strings.Trim(name, "/")
	if name == "" || s.Base == "" {
		return name
	}
	leaf := lastPathSegment(s.Base)
	if leaf != "" && strings.HasPrefix(name, leaf+"/") {
		name = name[len(leaf)+1:]
	}
	if name == "" {
		// 本地名恰好等于 base 的最后一段：用它本身。
		name = leaf
	}
	return name
}

// lastPathSegment 返回路径最后一段。
func lastPathSegment(p string) string {
	p = strings.Trim(p, "/")
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[i+1:]
	}
	return p
}

// streamLeaf 返回流名的最后一段（RTMP/HTTP 常用裸名，RTSP 常带应用前缀）。
func streamLeaf(name string) string {
	return lastPathSegment(name)
}

// ErrNotFound 表示上游明确回答「没有这路流」。
var ErrNotFound = errors.New("origin: upstream stream not found")

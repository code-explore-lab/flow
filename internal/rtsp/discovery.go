package rtsp

// S-3：动态发现与 D-1 Redirect 联动。
//
// 本地注册表没有某路流时，先问发现服务「别的节点有没有」。命中就复用
// D-1 的 302 + Location 语义，让客户端自己连过去；未命中再走 S-1 按需
// 回源，最后才回退本地文件。接口定义在这里而不是直接依赖 discovery 包，
// 与 OriginResolver 的做法一致，避免协议层绑定具体注册中心。
//
// 指向本节点的条目由注入方过滤（main 的适配器比较 node ID），协议层只
// 负责「查到了就回 302」。

import (
	"context"
	"fmt"
	"time"
)

// defaultDiscoveryTimeout 是发现查询的上限，超时按未命中处理。
// DESCRIBE 在关键路径上，不能让客户端等太久。
const defaultDiscoveryTimeout = 750 * time.Millisecond

// StreamFinder 查询别的节点是否持有某路流。
// ok=false 表示未配置、未命中或查询失败，调用方继续原有逻辑。
type StreamFinder interface {
	Find(ctx context.Context, name string) (url string, ok bool)
}

// SetDiscovery 注入发现查询实现。只在 Serve 之前调用。
func (s *Server) SetDiscovery(f StreamFinder) {
	s.finder = f
}

// SetDiscoveryTimeout 调整发现查询超时。d <= 0 时保持默认值。
// 只在 Serve 之前调用。
func (s *Server) SetDiscoveryTimeout(d time.Duration) {
	if d > 0 {
		s.discoveryTimeout = d
	}
}

// redirectForURI 检查请求的流是否在别的节点上，返回 302 的 Location。
// 本地已登记的流永远优先本地，不重定向。
func (s *Server) redirectForURI(uri string) (string, bool) {
	if s.finder == nil {
		return "", false
	}
	name := streamKeyFromURI(uri)
	if name == "" {
		return "", false
	}
	if s.publishedFromURI(uri) != nil {
		return "", false
	}
	timeout := s.discoveryTimeout
	if timeout <= 0 {
		timeout = defaultDiscoveryTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	loc, ok := s.finder.Find(ctx, name)
	if !ok || loc == "" {
		return "", false
	}
	return loc, true
}

// writeDiscoveryRedirect 发送与 D-1 相同的 302 响应。
func writeDiscoveryRedirect(conn *Conn, hd map[string]string, method, uri, loc string) error {
	fmt.Printf("discovery redirect: %s %s -> %s\n", method, uri, loc)
	hd["Location"] = loc
	hd["Content-Length"] = "0"
	return writeResponse(conn, "302 Found", hd, nil)
}

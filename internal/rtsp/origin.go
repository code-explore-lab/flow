package rtsp

// S-1：RTSP 服务端的回源钩子。
//
// 注册表里没有请求的流时，DESCRIBE/SETUP 先问 OriginResolver 能不能从上游
// 拉回来；返回 nil（上游不可用/明确不存在）时走原有的本地回退。接口定义在
// 这里而不是直接 import origin，因为 origin 依赖本包的拉流客户端。

import (
	"context"

	"flow/internal/stream"
)

// OriginResolver 按本地流名按需回源。
type OriginResolver interface {
	// Ensure 保证流 name 可用并返回共享流；失败返回 nil。
	Ensure(ctx context.Context, name string) *stream.Stream
}

// SetOrigin 注入回源解析器。只在 Serve 之前调用。
func (s *Server) SetOrigin(o OriginResolver) {
	s.originMu.Lock()
	s.origin = o
	s.originMu.Unlock()
}

// setOriginContext 记录服务端根 ctx，供 Ensure 在进程退出时尽快返回。
func (s *Server) setOriginContext(ctx context.Context) {
	s.originMu.Lock()
	s.originCtx = ctx
	s.originMu.Unlock()
}

// ensureOrigin 按 URI 的流名尝试回源；未配置或失败时返回 nil。
func (s *Server) ensureOrigin(uri string) *stream.Stream {
	s.originMu.RLock()
	o, ctx := s.origin, s.originCtx
	s.originMu.RUnlock()
	if o == nil {
		return nil
	}
	name := streamKeyFromURI(uri)
	if name == "" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return o.Ensure(ctx, name)
}

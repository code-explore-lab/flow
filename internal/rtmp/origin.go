package rtmp

// S-1：RTMP 服务端的回源钩子。handlePlay 在注册表未命中时先问
// OriginResolver；接口定义在本包，由 cmd/server 注入 origin.Manager，
// 避免 rtmp → origin 的 import 环。

import (
	"context"

	"flow/internal/stream"
)

// OriginResolver 按本地流名按需回源。
type OriginResolver interface {
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

// ensureOrigin 按流名尝试回源；未配置或失败时返回 nil。
func (s *Server) ensureOrigin(name string) *stream.Stream {
	s.originMu.RLock()
	o, ctx := s.origin, s.originCtx
	s.originMu.RUnlock()
	if o == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return o.Ensure(ctx, name)
}

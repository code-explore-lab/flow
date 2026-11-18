package httpflv

// S-1：HTTP-FLV 服务端的回源钩子。接口定义在本包，避免 httpflv → origin
// → rtsp/rtmp 的 import 环；cmd/server 注入 origin.Manager。

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
	s.origin = o
}

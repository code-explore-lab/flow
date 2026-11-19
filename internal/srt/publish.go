package srt

import (
	"context"
	"errors"
	"io"
	"log"

	gosrt "github.com/datarhei/gosrt"

	"flow/internal/mpegts"
	"flow/internal/stream"
	"flow/internal/tsingest"
)

// servePublish 读取 SRT 上的 MPEG-TS，解出 H.264/H.265/AAC 帧写进注册表。
func (s *Server) servePublish(ctx context.Context, conn gosrt.Conn, name string) {
	st := &stream.Stream{}
	published, err := s.registry.Publish(name, st)
	if err != nil {
		log.Printf("srt: publish %q rejected: %v", name, err)
		return
	}
	if published == nil {
		return
	}
	defer s.registry.Unpublish(name, published)

	ing := tsingest.NewIngest(st)
	d := mpegts.NewDemuxer()
	d.OnError = func(err error) { log.Printf("srt: publish %q: %v", name, err) }
	d.OnVideo = ing.OnVideo
	d.OnAudio = ing.OnAudio

	buf := make([]byte, 1316)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			d.Write(buf[:n])
		}
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, io.EOF) {
				log.Printf("srt: publish %q read: %v", name, err)
			}
			break
		}
	}
	d.Flush()
}

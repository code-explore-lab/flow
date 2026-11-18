package origin

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"flow/internal/rtsp"
)

// pullRTSP 拉一路 RTSP 上游；时间戳平移与首帧回调交给 rtsp.Pull。
func (p *Puller) pullRTSP(ctx context.Context, u *url.URL) (bool, error) {
	played, err := rtsp.Pull(ctx, rtsp.PullOptions{
		URL:          u,
		Stream:       p.st,
		VideoRebase:  p.rtspVideo.apply,
		AudioRebase:  p.rtspAudio.apply,
		OnFirstMedia: p.onFirst,
	})
	if err != nil && errors.Is(err, rtsp.ErrStreamNotFound) {
		return played, fmt.Errorf("%w: %v", ErrNotFound, err)
	}
	return played, err
}

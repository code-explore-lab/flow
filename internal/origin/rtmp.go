package origin

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"flow/internal/rtmp"
)

// pullRTMP 拉一路 RTMP 上游。时间戳平移与首帧回调由 rtmp.Pull 内部应用。
func (p *Puller) pullRTMP(ctx context.Context, u *url.URL) (bool, error) {
	path := strings.Trim(u.Path, "/")
	i := strings.LastIndexByte(path, '/')
	if i <= 0 || i == len(path)-1 {
		return false, fmt.Errorf("rtmp origin %q needs app/stream", u.String())
	}
	app := path[:i]
	name := path[i+1:]
	if u.RawQuery != "" {
		// 上游鉴权参数通常挂在 play 的流名上。
		name += "?" + u.RawQuery
	}
	played, err := rtmp.Pull(ctx, rtmp.PullOptions{
		Addr:         u.Host,
		App:          app,
		TcURL:        u.Scheme + "://" + u.Host + "/" + app,
		StreamName:   name,
		Stream:       p.st,
		Rebase:       p.rtmpClock.applyDTSCTS,
		OnFirstMedia: p.onFirst,
	})
	if err != nil && errors.Is(err, rtmp.ErrStreamNotFound) {
		return played, fmt.Errorf("%w: %v", ErrNotFound, err)
	}
	return played, err
}

package origin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"flow/internal/flv"
	"flow/internal/flvingest"
)

const (
	httpDialTimeout  = 10 * time.Second
	httpHeaderTimeot = 10 * time.Second
	// httpStallTimeout 是上游「连接还在但不再发数据」的判定窗口。上游正常
	// 关闭时读循环会拿到 EOF，用它兜住挂死的情况。
	httpStallTimeout = 60 * time.Second
)

// httpClient 拉 HTTP-FLV。不设整体 Timeout：直播流是无限长的；连接与响应
// 头阶段各自限时，读循环用 ResponseController 的读截止时间检测停滞。
var httpClient = &http.Client{
	Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   httpDialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ResponseHeaderTimeout: httpHeaderTimeot,
	},
}

// pullHTTP 拉一路 HTTP-FLV。played 表示已经读到 FLV 头并开始解 tag。
func (p *Puller) pullHTTP(ctx context.Context, raw string) (played bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return false, fmt.Errorf("http origin %q: %w", raw, err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("http get %q: %w", raw, err)
	}
	defer resp.Body.Close()

	switch code := resp.StatusCode; {
	case code == http.StatusOK:
	case code == http.StatusNotFound || code == http.StatusGone:
		return false, fmt.Errorf("%w: http %d from %s", ErrNotFound, code, raw)
	default:
		return false, fmt.Errorf("http %d from %s", code, raw)
	}

	if _, err := flv.ReadHeader(resp.Body); err != nil {
		return false, err
	}

	ingest := flvingest.NewIngest(p.st)
	ingest.OnFirstMedia = p.onFirst
	ingest.Rebase = p.httpClock.applyDTSCTS

	// 停滞看门狗：连接还在但长时间没有 tag 时主动关闭 body，唤醒阻塞的
	// 读循环去重连。resp.Body.Close 会中断正在进行的 Read。
	var last atomic.Int64
	last.Store(time.Now().UnixNano())
	stopWatch := make(chan struct{})
	defer close(stopWatch)
	go func() {
		t := time.NewTicker(httpStallTimeout)
		defer t.Stop()
		for {
			select {
			case <-stopWatch:
				return
			case <-t.C:
				if time.Since(time.Unix(0, last.Load())) >= httpStallTimeout {
					fmt.Printf("origin %q: http upstream stalled for %s\n", p.name, httpStallTimeout)
					_ = resp.Body.Close()
					return
				}
			}
		}
	}()

	for {
		tag, err := flv.ReadTag(resp.Body)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return true, nil
			}
			return true, fmt.Errorf("read flv tag: %w", err)
		}
		last.Store(time.Now().UnixNano())
		switch tag.Type {
		case flv.TagVideo:
			ingest.Video(tag.Data, tag.Timestamp)
		case flv.TagAudio:
			ingest.Audio(tag.Data, tag.Timestamp)
		case flv.TagScript:
			// onMetaData 原样留给 HTTP-FLV 转发。
			p.st.SetMetaDataPayload(tag.Data)
		}
	}
}

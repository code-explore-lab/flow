package snapshot

import (
	"context"
	"time"

	"flow/internal/media"
	"flow/internal/source"
	"flow/internal/stream"
)

// videoSource 是各视频帧源的公共接口：H264Source 和 ExternalSource 都满足。
type videoSource interface {
	Subscribe() (*source.Reader[source.Frame], func())
}

// liveVideoSource 支持从当前直播位置订阅、不预热旧 GOP。
type liveVideoSource interface {
	SubscribeLive() (*source.Reader[source.Frame], func())
}

// keyframe 是一个可直接送进解码器的完整 IDR 访问单元。
// nalus 不含 Annex-B 起始码，顺序为 VPS(可选)/SPS/PPS/各 slice。
type keyframe struct {
	nalus [][]byte
	pts   uint32 // 该访问单元的 RTP 时间戳（视频 90kHz）
}

// grabKeyframe 从帧源里取「参数集 + 最近一个完整 IDR AU」。
//
// 默认使用 Subscribe() 的预热 GOP，因此请求通常立刻返回；fresh 为 true 时
// 改用 SubscribeLive()，等下一个关键帧。timeout 从函数进入开始计时。
//
// 多 slice 的 IDR 会被拆成多个 NALU，全部收集后解码器才能重建整帧：
// 优先用 AUEnd 判断访问单元结束，源不提供 AU 标记时用下一个不同的
// Timestamp 判断。
func grabKeyframe(ctx context.Context, sub videoSource, codec string, params *stream.VideoCodec, fresh bool, timeout time.Duration) (*keyframe, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var (
		rd    *source.Reader[source.Frame]
		unsub func()
	)
	if fresh {
		if live, ok := sub.(liveVideoSource); ok {
			rd, unsub = live.SubscribeLive()
		} else {
			rd, unsub = sub.Subscribe()
		}
	} else {
		rd, unsub = sub.Subscribe()
	}
	defer unsub()

	// Reader.Next 只接受一个 done channel；用 ctx 关它，这样客户端断开和
	// 超时都能立即唤醒读循环。
	done := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			close(done)
		case <-watchDone:
		}
	}()
	defer close(watchDone)

	// 参数集优先用流上已经登记的（RTSP ANNOUNCE / RTMP 序列头），
	// 带内参数集在订阅过程中补。
	var vps, sps, pps []byte
	if params != nil {
		vps, sps, pps = params.VPS, params.SPS, params.PPS
	}

	var idr [][]byte
	var pts uint32
	seen := false

	for {
		f, ok := rd.Next(done)
		if !ok {
			break
		}
		cls := media.ClassifyVideoNAL(codec, f.Data)
		switch {
		case cls.IsVPS:
			if !seen {
				vps = f.Data
			}
		case cls.IsSPS:
			if !seen {
				sps = f.Data
			}
		case cls.IsPPS:
			if !seen {
				pps = f.Data
			}
		case cls.IsIDR:
			if !seen {
				seen = true
				pts = f.Timestamp
			} else if f.Timestamp != pts {
				// 下一个图像开始了，手里的 IDR AU 已经完整。
				return buildKeyframe(vps, sps, pps, idr, pts), nil
			}
			idr = append(idr, f.Data)
			if f.AUEnd {
				return buildKeyframe(vps, sps, pps, idr, pts), nil
			}
		case seen && f.Timestamp != pts:
			// IDR 之后的第一帧（P/B）到达，说明 AU 结束。
			return buildKeyframe(vps, sps, pps, idr, pts), nil
		}
	}

	if seen {
		return buildKeyframe(vps, sps, pps, idr, pts), nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, ErrNoKeyframe
}

func buildKeyframe(vps, sps, pps []byte, idr [][]byte, pts uint32) *keyframe {
	nalus := make([][]byte, 0, len(idr)+3)
	for _, p := range [][]byte{vps, sps, pps} {
		if len(p) > 0 {
			nalus = append(nalus, p)
		}
	}
	nalus = append(nalus, idr...)
	return &keyframe{nalus: nalus, pts: pts}
}

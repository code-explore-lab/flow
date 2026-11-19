package tsingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"flow/internal/mpegts"
	"flow/internal/stream"
)

const fileReadChunk = 64 << 10

// FileOptions 是一路文件 TS 输入的参数。
type FileOptions struct {
	Loop bool
	Rate float64 // 1 = 按 PTS 实时；2 = 2 倍速；0/负数按 1
}

// RunFile 把 MPEG-TS 文件按时间戳节奏喂进注册表。Loop 时每圈把时间戳
// 接到上一圈之后，下游不会看到回退。文件播完后摘流（Loop=false）。
func RunFile(ctx context.Context, reg *stream.Registry, name, path string, opt FileOptions) error {
	if reg == nil {
		return errors.New("tsingest: nil registry")
	}
	if name == "" {
		return errors.New("tsingest: empty stream name")
	}
	if opt.Rate <= 0 {
		opt.Rate = 1
	}

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("tsingest: file %q: %w", name, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("tsingest: file %q: %w", name, err)
	}
	if info.IsDir() {
		return fmt.Errorf("tsingest: file %q: %s is a directory", name, path)
	}

	st := &stream.Stream{}
	published, err := reg.Publish(name, st)
	if err != nil {
		return fmt.Errorf("tsingest: file %q: publish: %w", name, err)
	}
	if published == nil {
		return fmt.Errorf("tsingest: file %q: publish rejected", name)
	}
	defer reg.Unpublish(name, st)

	ing := NewIngest(st)

	// 每圈的时间轴状态：本地首/末 PTS 用来算这一圈的长度，
	// off90k/offAudio 是累计到当前圈之后的输出偏移。
	var (
		off90k   int64
		offAudio int64

		haveV, haveA     bool
		startV, lastV    int64
		prevV, stepV     int64
		startA, lastA    int64
		audioClock       int64 = 44100
		vunwrap, aunwrap tsUnwrapper
		p                = pacer{rate: opt.Rate}
	)

	attach := func(d *mpegts.Demuxer) {
		d.OnError = func(err error) { log.Printf("tsingest: file %q: %v", name, err) }
		d.OnVideo = func(au mpegts.VideoAU) {
			v := vunwrap.unwrap(au.PTS)
			if !haveV {
				startV, prevV, stepV = v, v, 3600
				haveV = true
			}
			if v > prevV {
				stepV = v - prevV
			}
			prevV = v
			lastV = v

			p.wait(ctx, v, 90000)

			cts := au.CompositionTime()
			au.PTS = uint32(v + off90k)
			au.DTS = uint32(int64(au.PTS) - int64(cts))
			ing.OnVideo(au)
		}
		d.OnAudio = func(au mpegts.AudioAU) {
			if au.SampleRate > 0 {
				audioClock = int64(au.SampleRate)
			}
			a := aunwrap.unwrap(au.Timestamp)
			if !haveA {
				startA = a
				haveA = true
			}
			lastA = a

			// 有视频时由视频节拍，音频跟着读；只有音频时按采样时钟节拍。
			if !haveV {
				p.wait(ctx, a, audioClock)
			}

			au.Timestamp = uint32(a + offAudio)
			ing.OnAudio(au)
		}
	}

	d := mpegts.NewDemuxer()
	attach(d)

	buf := make([]byte, fileReadChunk)
	for {
		if ctx.Err() != nil {
			d.Flush()
			return nil
		}
		if cur := reg.Get(name); cur != st {
			return fmt.Errorf("tsingest: file %q: stream was replaced by another publisher", name)
		}

		n, rerr := f.Read(buf)
		if n > 0 {
			d.Write(buf[:n])
		}
		if rerr == nil {
			continue
		}
		if rerr != io.EOF {
			d.Flush()
			return fmt.Errorf("tsingest: file %q: read: %w", name, rerr)
		}

		d.Flush()
		if !opt.Loop {
			return nil
		}

		// 这一圈的实际时长：音视频取较长者，视频再加一帧间隔，避免边界重叠。
		var dur int64
		if haveV && lastV >= startV {
			dur = lastV - startV + stepV
		}
		if haveA && lastA >= startA {
			audioDur := (lastA - startA + 1024) * 90000 / audioClock
			if audioDur > dur {
				dur = audioDur
			}
		}
		if dur <= 0 {
			return fmt.Errorf("tsingest: file %q: no media in one loop, refusing to spin", name)
		}
		off90k += dur
		offAudio = off90k * audioClock / 90000

		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("tsingest: file %q: seek: %w", name, err)
		}
		haveV, haveA = false, false
		vunwrap, aunwrap = tsUnwrapper{}, tsUnwrapper{}
		p.have = false
		d = mpegts.NewDemuxer()
		attach(d)
	}
}

// tsUnwrapper 把 32 位时间戳展开成 64 位，跨回绕保持单调。
// 文件 loop 时重新计数（配合 off90k 的累计偏移）。
type tsUnwrapper struct {
	have bool
	last uint32
	val  int64
}

func (u *tsUnwrapper) unwrap(raw uint32) int64 {
	if !u.have {
		u.have = true
		u.last = raw
		u.val = int64(raw)
		return u.val
	}
	u.val += int64(int32(raw - u.last))
	u.last = raw
	return u.val
}

// pacer 按媒体时间戳把读取节奏限制在墙钟上。
type pacer struct {
	rate     float64
	have     bool
	start    time.Time
	startPTS int64
}

func (p *pacer) wait(ctx context.Context, pts, clock int64) {
	if clock <= 0 {
		return
	}
	if !p.have {
		p.have = true
		p.start = time.Now()
		p.startPTS = pts
		return
	}
	media := float64(pts-p.startPTS) / float64(clock)
	target := p.start.Add(time.Duration(media / p.rate * float64(time.Second)))
	sleepCtx(ctx, time.Until(target))
}

func sleepCtx(ctx context.Context, d time.Duration) {
	const slice = 100 * time.Millisecond
	for d > 0 {
		step := d
		if step > slice {
			step = slice
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(step):
		}
		d -= step
	}
}

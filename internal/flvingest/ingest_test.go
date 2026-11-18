package flvingest

import (
	"bytes"
	"sync/atomic"
	"testing"
	"time"

	"flow/internal/flv"
	"flow/internal/source"
	"flow/internal/stream"
)

// nextFrame 带超时地取一帧。Ingest 是同步写入的，正常情况立刻能拿到。
func nextFrame(t *testing.T, rd *source.Reader[source.Frame]) source.Frame {
	t.Helper()
	done := make(chan struct{})
	type result struct {
		f  source.Frame
		ok bool
	}
	ch := make(chan result, 1)
	go func() {
		f, ok := rd.Next(done)
		ch <- result{f, ok}
	}()
	select {
	case r := <-ch:
		if !r.ok {
			t.Fatal("source ended before a frame arrived")
		}
		return r.f
	case <-time.After(2 * time.Second):
		close(done)
		t.Fatal("timed out waiting for a frame")
		return source.Frame{}
	}
}

// TestIngestRebaseUsesCTS 验证回源重连的时间戳平移按 DTS+CTS 计算：
// Rebase 拿到的是原始 DTS 和 CTS，返回值作为平移后的 DTS，PTS 仍由
// DTS+CTS 得出（B 帧不会把 PTS 拉回）。
func TestIngestRebaseUsesCTS(t *testing.T) {
	idr := []byte{0x65, 0x88, 0x84, 0x00}
	st := &stream.Stream{Name: "v"}
	st.EnsureVideo()
	rd, cancel := st.SourceFor("video").Subscribe()
	defer cancel()

	g := NewIngest(st)
	var gotDTS uint32
	var gotCTS int32
	g.Rebase = func(dts uint32, cts int32) uint32 {
		gotDTS, gotCTS = dts, cts
		return dts + 1000 // 模拟重连后的平移
	}

	// DTS=40ms、CTS=80ms，平移后 DTS=1040ms（93600），PTS=1120ms（100800）。
	g.Video(flv.BuildVideoTag(idr, true, 80), 40)
	f := nextFrame(t, rd)
	if gotDTS != 40 || gotCTS != 80 {
		t.Fatalf("Rebase got dts/cts = %d/%d, want 40/80", gotDTS, gotCTS)
	}
	if f.Timestamp != 100800 || f.CompositionTime != 7200 {
		t.Fatalf("ts/cts = %d/%d, want 100800/7200", f.Timestamp, f.CompositionTime)
	}
}

func TestIngestH264(t *testing.T) {
	sps := []byte{0x67, 0x42, 0x00, 0x1f, 0x96, 0x54, 0x05, 0x01, 0xed}
	pps := []byte{0x68, 0xee, 0x3c, 0x80}
	idr := []byte{0x65, 0x88, 0x84, 0x00}

	st := &stream.Stream{Name: "v"}
	var first int32
	g := NewIngest(st)
	g.OnFirstMedia = func() { atomic.AddInt32(&first, 1) }

	g.Video(flv.BuildVideoSequenceHeader(sps, pps), 0)
	vc, ok := st.VideoParams()
	if !ok {
		t.Fatal("video params not set after sequence header")
	}
	if vc.Codec != "H264" || !bytes.Equal(vc.SPS, sps) || !bytes.Equal(vc.PPS, pps) {
		t.Fatalf("params = %+v", vc)
	}
	if got := atomic.LoadInt32(&first); got != 1 {
		t.Fatalf("OnFirstMedia fired %d times after sequence header, want 1", got)
	}

	src := st.SourceFor("video")
	reader, cancelReader := src.Subscribe()
	defer cancelReader()

	// DTS=40ms、CTS=80ms → 帧源存 PTS=120ms（10800）和 CTS（7200）。
	g.Video(flv.BuildVideoTag(idr, true, 80), 40)
	f := nextFrame(t, reader)
	if !bytes.Equal(f.Data, idr) {
		t.Fatalf("data = %x, want idr", f.Data)
	}
	if f.Timestamp != 10800 || f.CompositionTime != 7200 {
		t.Fatalf("ts/cts = %d/%d, want 10800/7200", f.Timestamp, f.CompositionTime)
	}
	if !f.Keyframe || !f.AUStart || !f.AUEnd {
		t.Fatalf("flags = key %v start %v end %v, want all true", f.Keyframe, f.AUStart, f.AUEnd)
	}
	if got := atomic.LoadInt32(&first); got != 1 {
		t.Fatalf("OnFirstMedia fired %d times, want exactly 1", got)
	}
}

func TestIngestH265(t *testing.T) {
	vps := []byte{0x40, 0x01, 0x0C, 0x01}
	sps := []byte{0x42, 0x01, 0x01, 0x01, 0x60, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x5D}
	pps := []byte{0x44, 0x01, 0xC0, 0x73}

	st := &stream.Stream{}
	st.EnsureVideo()
	src := st.SourceFor("video")
	reader, cancel := src.Subscribe()
	defer cancel()

	g := NewIngest(st)
	g.Video(flv.BuildHEVCVideoSequenceHeader(vps, sps, pps), 0)

	vc, ok := st.VideoParams()
	if !ok {
		t.Fatal("video params not set")
	}
	if vc.Codec != "H265" || !bytes.Equal(vc.VPS, vps) || !bytes.Equal(vc.SPS, sps) || !bytes.Equal(vc.PPS, pps) {
		t.Fatalf("params = %+v", vc)
	}
	if got := src.Codec(); got != "H265" {
		t.Fatalf("source codec = %q, want H265", got)
	}
	// 参数集按 VPS → SPS → PPS 预热给订阅者。
	for i, want := range [][]byte{vps, sps, pps} {
		if f := nextFrame(t, reader); !bytes.Equal(f.Data, want) {
			t.Fatalf("frame %d = %x, want %x", i, f.Data, want)
		}
	}
}

func TestIngestAAC(t *testing.T) {
	asc := []byte{0x12, 0x10}
	au := []byte{0x21, 0x10, 0x05}

	st := &stream.Stream{}
	g := NewIngest(st)
	g.Audio(flv.BuildAudioSequenceHeader(asc, 44100, 2), 0)

	ap, ok := st.AudioParams()
	if !ok {
		t.Fatal("audio params not set")
	}
	if ap.Codec != stream.AudioAAC || !bytes.Equal(ap.Config, asc) ||
		ap.SampleRate != 44100 || ap.Channels != 2 {
		t.Fatalf("params = %+v", ap)
	}

	reader, cancel := st.SourceFor("audio").Subscribe()
	defer cancel()
	// 23ms @44100Hz = 1014。
	g.Audio(flv.BuildAudioTag(au, 44100, 2), 23)
	f := nextFrame(t, reader)
	if !bytes.Equal(f.Data, au) || f.Timestamp != 1014 || !f.Keyframe {
		t.Fatalf("frame = data %x ts %d key %v", f.Data, f.Timestamp, f.Keyframe)
	}
}

func TestIngestMP3AndG711(t *testing.T) {
	mp3Frame := append([]byte{0xFF, 0xFB, 0x90, 0x00}, bytes.Repeat([]byte{0x55}, 32)...)
	cases := []struct {
		name    string
		codec   string
		data    []byte
		rate    uint32
		ch      uint8
		want    string
		wantClk uint32
		tsMs    uint32
		wantTS  uint32
	}{
		{"mp3", "MP3", mp3Frame, 44100, 2, stream.AudioMP3, 90000, 100, 9000},
		{"pcma", "PCMA", []byte{0xD5, 0x00, 0x7F}, 8000, 1, stream.AudioPCMA, 8000, 20, 160},
		{"pcmu", "PCMU", []byte{0xD5, 0x00, 0x7F}, 8000, 1, stream.AudioPCMU, 8000, 20, 160},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := &stream.Stream{}
			st.EnsureAudio(0)
			reader, cancel := st.SourceFor("audio").Subscribe()
			defer cancel()

			g := NewIngest(st)
			g.Audio(flv.BuildAudioTagFor(tc.codec, tc.data, tc.rate, tc.ch), tc.tsMs)

			ap, ok := st.AudioParams()
			if !ok {
				t.Fatal("audio params not set")
			}
			if ap.Codec != tc.want || ap.SampleRate != tc.rate ||
				ap.Channels != tc.ch || ap.ClockRate != tc.wantClk {
				t.Fatalf("params = %+v, want codec %s rate %d ch %d clock %d",
					ap, tc.want, tc.rate, tc.ch, tc.wantClk)
			}
			f := nextFrame(t, reader)
			if !bytes.Equal(f.Data, tc.data) {
				t.Fatalf("data = %x, want %x", f.Data, tc.data)
			}
			if f.Timestamp != tc.wantTS || !f.Keyframe {
				t.Fatalf("ts = %d key %v, want %d/true", f.Timestamp, f.Keyframe, tc.wantTS)
			}
		})
	}
}

func TestIngestBadTagsIgnored(t *testing.T) {
	st := &stream.Stream{}
	g := NewIngest(st)
	// 空负载/垃圾数据不应 panic，也不应触发首帧回调。
	g.Video([]byte{0xFF}, 0)
	g.Audio(nil, 0)
	if _, ok := st.VideoParams(); ok {
		t.Fatal("garbage video tag created params")
	}
	if st.AudioTrack() != nil {
		t.Fatal("garbage audio tag created a track")
	}
}

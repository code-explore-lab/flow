package hls

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"flow/internal/media"
	"flow/internal/source"
	"flow/internal/stream"
)

func realH264Params(t *testing.T) (sps, pps []byte) {
	t.Helper()
	var err error
	sps, err = hex.DecodeString("6764001eacd940a03db011000003000100000300320f162d9600")
	if err != nil {
		t.Fatal(err)
	}
	pps, err = hex.DecodeString("68ebecb22c")
	if err != nil {
		t.Fatal(err)
	}
	return sps, pps
}

// newStreamSpec 构造一路带 H.264 + AAC 参数的 registry stream 与 PackagerSpec。
func newStreamSpec(t *testing.T, name string) (PackagerSpec, *stream.Stream, *source.ExternalSource, *source.ExternalSource) {
	t.Helper()
	sps, pps := realH264Params(t)
	st := &stream.Stream{Name: name}
	st.SetVideoParams("H264", nil, sps, pps)
	st.SetAudioCodec(stream.AudioAAC, []byte{0x12, 0x10}, 44100, 2, 44100)
	vp, ok := st.VideoParams()
	if !ok {
		t.Fatal("video params not set")
	}
	ap, ok := st.AudioParams()
	if !ok {
		t.Fatal("audio params not set")
	}
	vs := st.SourceFor("video")
	as := st.SourceFor("audio")
	spec := PackagerSpec{
		Name:   name,
		Stream: st,
		Video:  &TrackSource{Subscriber: vs, Video: vp},
		Audio:  &TrackSource{Subscriber: as, Audio: ap},
	}
	return spec, st, vs, as
}

func writeVideoFrames(vs *source.ExternalSource, start, count, gop int) {
	for i := start; i < start+count; i++ {
		key := gop > 0 && i%gop == 0
		nalu := []byte{0x41, 0x9A, byte(i), byte(i >> 8)}
		if key {
			nalu = []byte{0x65, 0x88, byte(i), byte(i >> 8)}
		}
		vs.Write(source.Frame{
			Data:      nalu,
			Timestamp: uint32(i) * 3600,
			Keyframe:  key,
			AUStart:   true,
			AUEnd:     true,
		})
	}
}

func writeAudioFrames(as *source.ExternalSource, start, count int) {
	for i := start; i < start+count; i++ {
		as.Write(source.Frame{
			Data:      []byte{0x21, 0x10, byte(i), byte(i >> 8)},
			Timestamp: uint32(i) * 1024,
			Keyframe:  true,
		})
	}
}

func waitUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func waitClosed(t *testing.T, p *Packager) {
	t.Helper()
	waitUntil(t, 5*time.Second, "packager closed", p.Closed)
}

func TestSpecFromStream(t *testing.T) {
	_, st, _, _ := newStreamSpec(t, "live/spec")
	spec := SpecFromStream(st)
	if spec.Name != "live/spec" || spec.Stream != st {
		t.Errorf("spec = %+v", spec)
	}
	if spec.Video == nil || spec.Video.Subscriber == nil || spec.Video.Video == nil {
		t.Errorf("video track = %+v", spec.Video)
	}
	if spec.Audio == nil || spec.Audio.Subscriber == nil || spec.Audio.Audio == nil {
		t.Errorf("audio track = %+v", spec.Audio)
	}
}

func TestPackagerMaxBytes(t *testing.T) {
	spec, _, vs, as := newStreamSpec(t, "live/maxbytes")
	p, err := NewPackager(spec, Options{
		SegmentTarget: time.Second,
		PartTarget:    500 * time.Millisecond,
		Window:        100, // 只让 MaxBytes 触发回收
		MaxBytes:      20_000,
		Logf:          t.Logf,
	})
	if err != nil {
		t.Fatalf("NewPackager: %v", err)
	}
	defer p.Close()

	payload := bytes.Repeat([]byte{0xAA}, 500)
	for i := 0; i < 75; i++ {
		key := i%25 == 0
		nalu := []byte{0x41, 0x9A, byte(i)}
		if key {
			nalu[0] = 0x65
		}
		nalu = append(nalu, payload...)
		vs.Write(source.Frame{
			Data:      nalu,
			Timestamp: uint32(i) * 3600,
			Keyframe:  key,
			AUStart:   true,
			AUEnd:     true,
		})
	}
	writeAudioFrames(as, 0, 50)
	vs.Close()
	as.Close()
	waitClosed(t, p)

	snap := p.Video().Snapshot()
	if len(snap.Segments) == 0 {
		t.Fatal("no segments")
	}
	if len(snap.Segments) >= 3 {
		t.Errorf("segments = %d, MaxBytes should evict below 3", len(snap.Segments))
	}
	if got := p.Video().StoredBytes(); got <= 0 || got > 30_000 {
		t.Errorf("stored bytes = %d, want (0,30000]", got)
	}
}

func TestPackagerVideoAndAudio(t *testing.T) {
	spec, _, vs, as := newStreamSpec(t, "live/test")
	p, err := NewPackager(spec, Options{
		SegmentTarget: time.Second,
		PartTarget:    500 * time.Millisecond,
		Window:        6,
		Logf:          t.Logf,
	})
	if err != nil {
		t.Fatalf("NewPackager: %v", err)
	}
	defer p.Close()

	writeVideoFrames(vs, 0, 75, 25) // 3s，GOP 1s
	writeAudioFrames(as, 0, 100)    // ≈2.3s
	vs.Close()
	as.Close()
	waitClosed(t, p)

	v, a := p.Video(), p.Audio()
	if v == nil || a == nil {
		t.Fatalf("tracks: video=%v audio=%v", v != nil, a != nil)
	}
	vsnap, asnap := v.Snapshot(), a.Snapshot()
	if !vsnap.Closed || !asnap.Closed {
		t.Errorf("snapshots not closed: video=%v audio=%v", vsnap.Closed, asnap.Closed)
	}
	if vsnap.Open != nil || asnap.Open != nil {
		t.Errorf("open segment after end: video=%+v audio=%+v", vsnap.Open, asnap.Open)
	}
	if len(vsnap.Segments) < 2 {
		t.Fatalf("video segments = %d, want >= 2", len(vsnap.Segments))
	}
	if len(asnap.Segments) == 0 {
		t.Fatal("no audio segments")
	}
	if vsnap.TargetDuration < 1 || vsnap.PartTarget <= 0 {
		t.Errorf("video target/part = %d/%s", vsnap.TargetDuration, vsnap.PartTarget)
	}
	if v.Codec() != "avc1.64001e" {
		t.Errorf("video codec = %q", v.Codec())
	}
	if w, h := v.Size(); w != 640 || h != 480 {
		t.Errorf("video size = %dx%d", w, h)
	}
	if a.Codec() != "mp4a.40.2" {
		t.Errorf("audio codec = %q", a.Codec())
	}

	for i, seg := range vsnap.Segments {
		if seg.MSN != uint64(i) {
			t.Errorf("video segment %d msn = %d", i, seg.MSN)
		}
		if seg.Duration <= 0 || seg.URI == "" || seg.MapURI == "" || seg.PDT.IsZero() {
			t.Errorf("video segment %+v incomplete", seg)
		}
		if data, ok := v.Data(seg.URI); !ok || len(data) == 0 {
			t.Errorf("video segment data missing for %s", seg.URI)
		}
	}
	if _, ok := v.Data("video/init.mp4"); !ok {
		t.Error("video init data missing")
	}
	if _, ok := a.Data("audio/init.mp4"); !ok {
		t.Error("audio init data missing")
	}
	if v.Bandwidth() == 0 {
		t.Error("video bandwidth = 0")
	}
}

func TestPackagerTrackBasePreservesAVOffset(t *testing.T) {
	spec, _, vs, as := newStreamSpec(t, "live/sync")

	var mu sync.Mutex
	calls := 0
	base := time.Date(2026, 11, 18, 8, 30, 0, 0, time.UTC)
	now := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return base.Add(time.Duration(calls) * 100 * time.Millisecond)
	}
	p, err := NewPackager(spec, Options{
		SegmentTarget: time.Second,
		PartTarget:    500 * time.Millisecond,
		Now:           now,
		Logf:          t.Logf,
	})
	if err != nil {
		t.Fatalf("NewPackager: %v", err)
	}
	defer p.Close()

	// 两帧视频：第一帧在第二个 AU 开始时结算，能确认视频首帧到达已被记录。
	writeVideoFrames(vs, 0, 2, 25)
	waitUntil(t, 2*time.Second, "video open segment", func() bool {
		snap := p.Video().Snapshot()
		return snap.Open != nil
	})
	writeAudioFrames(as, 0, 1)
	vs.Close()
	as.Close()
	waitClosed(t, p)

	vseg := p.Video().Snapshot().Segments
	aseg := p.Audio().Snapshot().Segments
	if len(vseg) == 0 || len(aseg) == 0 {
		t.Fatalf("segments: video=%d audio=%d", len(vseg), len(aseg))
	}
	vd := ticksToDuration(vseg[0].StartPTS, 90000)
	ad := ticksToDuration(aseg[0].StartPTS, 44100)
	// epoch=100ms，视频首帧=200ms，音频首帧=400ms（中间两帧视频各一次 Now）。
	if got, want := vd, 100*time.Millisecond; got != want {
		t.Errorf("video track base = %s, want %s", got, want)
	}
	if got, want := ad, 300*time.Millisecond; got != want {
		t.Errorf("audio track base = %s, want %s", got, want)
	}
	if got, want := ad-vd, 200*time.Millisecond; got != want {
		t.Errorf("A/V track base offset = %s, want %s", got, want)
	}
}

func TestPackagerOpenPartsAndWatch(t *testing.T) {
	spec, _, vs, as := newStreamSpec(t, "live/open")
	p, err := NewPackager(spec, Options{
		SegmentTarget: time.Second,
		PartTarget:    500 * time.Millisecond,
		Logf:          t.Logf,
	})
	if err != nil {
		t.Fatalf("NewPackager: %v", err)
	}
	defer p.Close()

	watch := p.Video().Watch()
	writeVideoFrames(vs, 0, 45, 25)
	waitUntil(t, 2*time.Second, "open segment with parts", func() bool {
		snap := p.Video().Snapshot()
		return snap.Open != nil && len(snap.Open.Parts) > 0
	})
	select {
	case <-watch:
	case <-time.After(time.Second):
		t.Error("Watch channel was not notified")
	}

	snap := p.Video().Snapshot()
	if snap.Open == nil {
		t.Fatal("Open = nil")
	}
	if snap.Open.PDT.IsZero() || snap.Open.MapURI == "" || snap.Open.NextPartURI == "" {
		t.Errorf("open segment incomplete: %+v", snap.Open)
	}
	part := snap.Open.Parts[0]
	if data, ok := p.Video().Data(part.URI); !ok || len(data) == 0 {
		t.Errorf("part data missing for %s", part.URI)
	}
	msn, partIdx := p.Video().Progress()
	if msn != snap.Open.MSN || partIdx != snap.Open.Parts[len(snap.Open.Parts)-1].Index {
		t.Errorf("Progress = %d/%d, open = %d/%d", msn, partIdx,
			snap.Open.MSN, snap.Open.Parts[len(snap.Open.Parts)-1].Index)
	}

	vs.Close()
	as.Close()
	waitClosed(t, p)
}

func TestPackagerParamChange(t *testing.T) {
	spec, _, vs, as := newStreamSpec(t, "live/param")
	p, err := NewPackager(spec, Options{
		SegmentTarget: time.Second,
		PartTarget:    500 * time.Millisecond,
		Logf:          t.Logf,
	})
	if err != nil {
		t.Fatalf("NewPackager: %v", err)
	}
	defer p.Close()

	writeVideoFrames(vs, 0, 30, 25)
	waitUntil(t, 2*time.Second, "first video segment", func() bool {
		return len(p.Video().Snapshot().Segments) >= 1
	})

	sps, pps := realH264Params(t)
	sps2 := append([]byte(nil), sps...)
	sps2[3] = 0x1f // 改 level，构造不同的参数集
	vs.Write(source.Frame{Data: sps2})
	vs.Write(source.Frame{Data: pps})
	vs.Write(source.Frame{
		Data: []byte{0x65, 0x88, 0xAA, 0xBB}, Timestamp: 30 * 3600,
		Keyframe: true, AUStart: true, AUEnd: true,
	})
	writeVideoFrames(vs, 31, 30, 25)
	vs.Close()
	as.Close()
	waitClosed(t, p)

	snap := p.Video().Snapshot()
	found := false
	for _, seg := range snap.Segments {
		if seg.MapURI == "video/init-1.mp4" {
			found = true
			if !seg.Discontinuity {
				t.Error("new init segment should carry discontinuity")
			}
			if seg.DiscSeq == 0 {
				t.Error("new init segment DiscSeq = 0")
			}
		}
	}
	if !found {
		t.Fatalf("no segment with init-1: %+v", snap.Segments)
	}
	if _, ok := p.Video().Data("video/init-1.mp4"); !ok {
		t.Error("init-1 data missing")
	}
}

// ---- 本地文件回退 ----

func annexB(nalus ...[]byte) []byte {
	var out []byte
	for _, n := range nalus {
		out = append(out, 0, 0, 0, 1)
		out = append(out, n...)
	}
	return out
}

func adtsFrame(payload []byte) []byte {
	frameLen := 7 + len(payload)
	const (
		profile    = 1
		sfi        = 4
		channelCfg = 2
		fullness   = 0x7FF
	)
	h := make([]byte, 7)
	h[0] = 0xFF
	h[1] = 0xF1
	h[2] = byte(profile<<6) | byte(sfi<<2)
	h[3] = byte(channelCfg&0x03)<<6 | byte((frameLen>>11)&0x03)
	h[4] = byte((frameLen >> 3) & 0xFF)
	h[5] = byte((frameLen&0x07)<<5) | byte((fullness>>6)&0x1F)
	h[6] = byte((fullness & 0x3F) << 2)
	return append(h, payload...)
}

func TestPackagerLocalFileSources(t *testing.T) {
	dir := t.TempDir()
	sps := []byte{0x67, 0x42, 0xC0, 0x1E}
	pps := []byte{0x68, 0xCE, 0x3C, 0x80}
	idr := []byte{0x65, 0x88, 0x84, 0x00}
	nalus := [][]byte{sps, pps, idr}
	for i := 0; i < 6; i++ {
		nalus = append(nalus, []byte{0x41, 0x9A, byte(i)})
	}
	h264Path := filepath.Join(dir, "test.h264")
	if err := os.WriteFile(h264Path, annexB(nalus...), 0o644); err != nil {
		t.Fatal(err)
	}
	var ablob []byte
	for i := 0; i < 6; i++ {
		ablob = append(ablob, adtsFrame([]byte{0x21, 0x10, byte(i)})...)
	}
	aacPath := filepath.Join(dir, "test.aac")
	if err := os.WriteFile(aacPath, ablob, 0o644); err != nil {
		t.Fatal(err)
	}
	info, err := media.GetAACInfo(aacPath)
	if err != nil {
		t.Fatalf("GetAACInfo: %v", err)
	}

	spec := PackagerSpec{
		Name:  "local/test",
		Video: &TrackSource{Subscriber: source.NewH264Source(h264Path)},
		Audio: &TrackSource{
			Subscriber: source.NewAACSource(aacPath),
			Audio: &stream.AudioCodec{
				Codec:      stream.AudioAAC,
				Config:     info.Config,
				SampleRate: uint32(info.SamplingFrequency),
				ClockRate:  uint32(info.SamplingFrequency),
				Channels:   info.ChannelConfig,
			},
		},
	}
	p, err := NewPackager(spec, Options{
		SegmentTarget: time.Second,
		PartTarget:    500 * time.Millisecond,
		Logf:          t.Logf,
	})
	if err != nil {
		t.Fatalf("NewPackager: %v", err)
	}
	defer p.Close()
	waitClosed(t, p)

	if len(p.Video().Snapshot().Segments) == 0 {
		t.Error("no video segments from local file")
	}
	if len(p.Audio().Snapshot().Segments) == 0 {
		t.Error("no audio segments from local file")
	}
}

// ---- manager ----

func TestManagerReuseReplaceSweep(t *testing.T) {
	spec1, _, _, _ := newStreamSpec(t, "m1")
	spec2, _, _, _ := newStreamSpec(t, "m1") // 同名，不同 Stream 指针
	base := time.Date(2026, 11, 18, 8, 30, 0, 0, time.UTC)
	now := base
	opts := Options{
		SegmentTarget: time.Second,
		PartTarget:    500 * time.Millisecond,
		Now:           func() time.Time { return now },
		Logf:          t.Logf,
	}
	m := NewManager(opts, 10*time.Second)
	defer m.Close()

	p1 := m.Get(spec1)
	if p1 == nil {
		t.Fatal("Get(spec1) = nil")
	}
	if p1b := m.Get(spec1); p1b != p1 {
		t.Fatal("same stream should reuse packager")
	}
	if got := m.Len(); got != 1 {
		t.Errorf("Len = %d, want 1", got)
	}

	p2 := m.Get(spec2)
	if p2 == nil || p2 == p1 {
		t.Fatalf("replace: p1=%v p2=%v", p1 != nil, p2 != nil)
	}
	if !p1.Closed() {
		t.Error("old packager should be closed after stream replacement")
	}
	if got := m.Len(); got != 1 {
		t.Errorf("Len after replace = %d, want 1", got)
	}

	now = base.Add(11 * time.Second)
	m.Sweep(now)
	if !p2.Closed() {
		t.Error("idle packager should be closed")
	}
	if got := m.Len(); got != 0 {
		t.Errorf("Len after sweep = %d, want 0", got)
	}
}

func TestManagerClose(t *testing.T) {
	spec, _, _, _ := newStreamSpec(t, "m2")
	m := NewManager(Options{SegmentTarget: time.Second, PartTarget: 500 * time.Millisecond, Logf: t.Logf}, 0)
	p := m.Get(spec)
	if p == nil {
		t.Fatal("Get = nil")
	}
	m.Close()
	if !p.Closed() {
		t.Error("packager not closed by Manager.Close")
	}
	if m.Len() != 0 {
		t.Errorf("Len = %d, want 0", m.Len())
	}
}

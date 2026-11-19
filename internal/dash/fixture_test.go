package dash

import (
	"encoding/hex"
	"testing"
	"time"

	"flow/internal/hls"
	"flow/internal/source"
	"flow/internal/stream"
)

var testEpoch = time.Date(2026, 11, 19, 8, 0, 0, 0, time.UTC)

type fixture struct {
	packager *hls.Packager
	video    *source.ExternalSource
	audio    *source.ExternalSource
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	return newNamedFixture(t, "live/demo")
}

func newNamedFixture(t *testing.T, name string) *fixture {
	t.Helper()
	return newNamedFixtureAt(t, name, testEpoch)
}

func newNamedFixtureAt(t *testing.T, name string, epoch time.Time) *fixture {
	t.Helper()
	sps, err := hex.DecodeString("6764001eacd940a03db011000003000100000300320f162d9600")
	if err != nil {
		t.Fatal(err)
	}
	pps, err := hex.DecodeString("68ebecb22c")
	if err != nil {
		t.Fatal(err)
	}

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

	p, err := hls.NewPackager(hls.PackagerSpec{
		Name:   name,
		Stream: st,
		Video:  &hls.TrackSource{Subscriber: vs, Video: vp},
		Audio:  &hls.TrackSource{Subscriber: as, Audio: ap},
	}, hls.Options{
		SegmentTarget: time.Second,
		Window:        8,
		Now:           func() time.Time { return epoch },
		Logf:          t.Logf,
	})
	if err != nil {
		t.Fatalf("NewPackager: %v", err)
	}
	t.Cleanup(p.Close)
	return &fixture{packager: p, video: vs, audio: as}
}

func writeVideo(vs *source.ExternalSource, from, count int) {
	for i := from; i < from+count; i++ {
		key := i%25 == 0
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
		if i%10 == 9 {
			time.Sleep(time.Millisecond)
		}
	}
}

// writeVideoB 写视频帧，cts 给出每个访问单元的 CTS（PTS − DTS），用来模拟
// 带 B 帧的重排延迟。Timestamp 是 PTS，CompositionTime 是 CTS。
func writeVideoB(vs *source.ExternalSource, from, count int, cts func(i int) int32) {
	for i := from; i < from+count; i++ {
		key := i%25 == 0
		nalu := []byte{0x41, 0x9A, byte(i), byte(i >> 8)}
		if key {
			nalu = []byte{0x65, 0x88, byte(i), byte(i >> 8)}
		}
		c := cts(i)
		vs.Write(source.Frame{
			Data:            nalu,
			Timestamp:       uint32(int64(i)*3600 + int64(c)),
			CompositionTime: c,
			Keyframe:        key,
			AUStart:         true,
			AUEnd:           true,
		})
		if i%10 == 9 {
			time.Sleep(time.Millisecond)
		}
	}
}

func writeAudio(as *source.ExternalSource, from, count int) {
	for i := from; i < from+count; i++ {
		as.Write(source.Frame{
			Data:      []byte{0x21, 0x10, byte(i), byte(i >> 8)},
			Timestamp: uint32(i) * 1024,
			Keyframe:  true,
		})
		if i%20 == 19 {
			time.Sleep(time.Millisecond)
		}
	}
}

// feedBaseline 写入约 3.2s 媒体，等音视频各自攒够完整段。
func feedBaseline(t *testing.T, f *fixture) {
	t.Helper()
	writeVideo(f.video, 0, 80)
	writeAudio(f.audio, 0, 160)
	waitSegments(t, f.packager.Video(), 3)
	waitSegments(t, f.packager.Audio(), 2)
}

func waitSegments(t *testing.T, tr *hls.Track, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(tr.Snapshot().Segments) >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d segments, got %d", n, len(tr.Snapshot().Segments))
}

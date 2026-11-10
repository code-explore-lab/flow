package stream

import (
	"bytes"
	"testing"
	"time"

	"flow/internal/source"
)

// expectReaderEnded 断言源关闭后读者读到结束。
func expectReaderEnded(t *testing.T, rd *source.Reader[source.Frame]) {
	t.Helper()
	done := make(chan struct{})
	defer close(done)
	result := make(chan bool, 1)
	go func() {
		_, ok := rd.Next(done)
		result <- ok
	}()
	select {
	case ok := <-result:
		if ok {
			t.Fatal("reader returned a frame, want end of stream")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reader did not observe end of stream")
	}
}

func TestEnsureIdempotent(t *testing.T) {
	s := &Stream{Name: "live"}

	v1 := s.EnsureVideo()
	v2 := s.EnsureVideo()
	if v1 != v2 {
		t.Fatal("EnsureVideo returned different tracks")
	}
	if v1.Kind != "video" || v1.Src == nil || v1.Video == nil {
		t.Fatalf("video track = %+v, want Kind/Src/Video set", v1)
	}

	a1 := s.EnsureAudio(48000)
	a2 := s.EnsureAudio(44100)
	if a1 != a2 {
		t.Fatal("EnsureAudio returned different tracks")
	}
	if got := a1.Src.SamplingRate(); got != 44100 {
		t.Fatalf("SamplingRate = %d, want updated 44100", got)
	}
	if a1.Audio.SampleRate != 44100 {
		t.Fatalf("Audio.SampleRate = %d, want 44100", a1.Audio.SampleRate)
	}
}

func TestSetParamsCopiesSlices(t *testing.T) {
	s := &Stream{}
	sps := []byte{0x67, 0x42}
	pps := []byte{0x68, 0xCE}
	s.SetVideoParams(sps, pps)
	sps[0] = 0xFF
	pps[0] = 0xFF

	vc, ok := s.VideoParams()
	if !ok {
		t.Fatal("VideoParams not ok after SetVideoParams")
	}
	if !bytes.Equal(vc.SPS, []byte{0x67, 0x42}) || !bytes.Equal(vc.PPS, []byte{0x68, 0xCE}) {
		t.Fatalf("params changed through caller buffer: % x / % x", vc.SPS, vc.PPS)
	}

	// 返回的副本也不能改到内部状态。
	vc.SPS[0] = 0x00
	vc2, _ := s.VideoParams()
	if vc2.SPS[0] != 0x67 {
		t.Fatal("VideoParams exposed internal slice")
	}
}

func TestRegistryPublishReplacesAndClosesOld(t *testing.T) {
	r := NewRegistry()
	old := &Stream{Name: "live"}
	old.EnsureVideo()
	rd, cancel := old.VideoTrack().Src.Subscribe()
	defer cancel()

	r.Publish("live", old)
	if got := r.Get("live"); got != old {
		t.Fatal("Get did not return the published stream")
	}

	newer := &Stream{}
	r.Publish("live", newer)
	if got := r.Get("live"); got != newer {
		t.Fatal("Publish did not replace the old stream")
	}
	// 旧流的源被关闭，晚到的拉流端读者能读到结束而不是一直等。
	expectReaderEnded(t, rd)
}

func TestUnpublishOnlyRemovesMatchingPointer(t *testing.T) {
	r := NewRegistry()
	cur := &Stream{}
	r.Publish("live", cur)

	// 旧会话晚到的清理：指针不相等，不能把新流摘掉。
	r.Unpublish("live", &Stream{})
	if got := r.Get("live"); got != cur {
		t.Fatal("Unpublish removed a different stream")
	}

	r.Unpublish("live", cur)
	if got := r.Get("live"); got != nil {
		t.Fatal("Unpublish did not remove the matching stream")
	}
	if n := r.Len(); n != 0 {
		t.Fatalf("Len = %d, want 0", n)
	}
}

func TestSourceFor(t *testing.T) {
	s := &Stream{}
	if got := s.SourceFor("video"); got != nil {
		t.Fatal("SourceFor(video) on empty stream != nil")
	}
	s.EnsureVideo()
	s.EnsureAudio(44100)
	if got := s.SourceFor("video"); got != s.VideoTrack().Src {
		t.Fatal("SourceFor(video) did not return the video source")
	}
	if got := s.SourceFor("audio"); got != s.AudioTrack().Src {
		t.Fatal("SourceFor(audio) did not return the audio source")
	}
	if got := s.SourceFor("data"); got != nil {
		t.Fatal("SourceFor(data) != nil")
	}
}

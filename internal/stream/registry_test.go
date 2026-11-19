package stream

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
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
	s.SetVideoParams("H264", nil, sps, pps)
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

func TestSetVideoParamsHEVC(t *testing.T) {
	s := &Stream{}
	vps := []byte{0x40, 0x01}
	sps := []byte{0x42, 0x01}
	pps := []byte{0x44, 0x01}
	s.SetVideoParams("H265", vps, sps, pps)
	vps[0] = 0xFF

	vc, ok := s.VideoParams()
	if !ok {
		t.Fatal("VideoParams not ok after SetVideoParams")
	}
	if vc.Codec != "H265" {
		t.Fatalf("Codec = %q, want H265", vc.Codec)
	}
	if !bytes.Equal(vc.VPS, []byte{0x40, 0x01}) {
		t.Fatalf("VPS changed through caller buffer: %x", vc.VPS)
	}
	// 编码要同步给帧源，推流端解包时会按它分类 NAL。
	if got := s.VideoTrack().Src.Codec(); got != "H265" {
		t.Fatalf("source codec = %q, want H265", got)
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

func TestParsePublishPolicy(t *testing.T) {
	valid := map[string]PublishPolicy{
		"":                  PublishReplace,
		"replace":           PublishReplace,
		" REPLACE ":         PublishReplace,
		"reject":            PublishReject,
		"reject-if-active":  PublishRejectIfActive,
		"Reject-If-Active ": PublishRejectIfActive,
	}
	for in, want := range valid {
		got, err := ParsePublishPolicy(in)
		if err != nil {
			t.Fatalf("ParsePublishPolicy(%q): %v", in, err)
		}
		if got != want {
			t.Fatalf("ParsePublishPolicy(%q) = %v, want %v", in, got, want)
		}
	}
	for _, in := range []string{"drop", "reject_if_active", "replace-all"} {
		if _, err := ParsePublishPolicy(in); err == nil {
			t.Fatalf("ParsePublishPolicy(%q) accepted an invalid policy", in)
		}
	}
}

// expectStreamStillWritable 确认旧源没有被关闭：写入一帧后订阅者能读到。
func expectStreamStillWritable(t *testing.T, s *Stream, rd *source.Reader[source.Frame]) {
	t.Helper()
	s.VideoTrack().Src.Write(source.Frame{Data: []byte{0x01, 0x02}})

	done := make(chan struct{})
	defer close(done)
	result := make(chan bool, 1)
	go func() {
		_, ok := rd.Next(done)
		result <- ok
	}()
	select {
	case ok := <-result:
		if !ok {
			t.Fatal("reader ended: old source was closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reader did not receive a frame from the old source")
	}
}

func TestPublishPolicyReject(t *testing.T) {
	r := NewRegistry()
	r.SetPublishPolicy(PublishReject)

	old := &Stream{}
	old.EnsureVideo()
	rd, cancel := old.VideoTrack().Src.Subscribe()
	defer cancel()
	if _, err := r.Publish("live", old); err != nil {
		t.Fatalf("first publish: %v", err)
	}

	newer := &Stream{}
	if _, err := r.Publish("live", newer); !errors.Is(err, ErrPublishRejected) {
		t.Fatalf("second publish err = %v, want ErrPublishRejected", err)
	}
	if got := r.Get("live"); got != old {
		t.Fatal("rejected publish replaced the old stream")
	}
	expectStreamStillWritable(t, old, rd)
}

func TestPublishPolicyRejectIfActive(t *testing.T) {
	t.Run("active stream rejected", func(t *testing.T) {
		r := NewRegistry()
		r.SetPublishPolicy(PublishRejectIfActive)

		old := &Stream{}
		old.EnsureVideo()
		rd, cancel := old.VideoTrack().Src.Subscribe()
		defer cancel()
		if _, err := r.Publish("live", old); err != nil {
			t.Fatalf("first publish: %v", err)
		}
		old.VideoTrack().Src.Write(source.Frame{Data: []byte{0x01}})

		if _, err := r.Publish("live", &Stream{}); !errors.Is(err, ErrPublishRejected) {
			t.Fatalf("publish err = %v, want ErrPublishRejected", err)
		}
		if got := r.Get("live"); got != old {
			t.Fatal("reject-if-active replaced an active stream")
		}
		expectStreamStillWritable(t, old, rd)
	})

	t.Run("inactive stream replaced", func(t *testing.T) {
		r := NewRegistry()
		r.SetPublishPolicy(PublishRejectIfActive)
		r.activeWindow = time.Millisecond

		old := &Stream{}
		old.EnsureVideo()
		rd, cancel := old.VideoTrack().Src.Subscribe()
		defer cancel()
		if _, err := r.Publish("live", old); err != nil {
			t.Fatalf("first publish: %v", err)
		}
		time.Sleep(5 * time.Millisecond) // 旧流超过活跃窗口
		if old.Active(r.activeWindow) {
			t.Fatal("old stream still reports active")
		}

		newer := &Stream{}
		if _, err := r.Publish("live", newer); err != nil {
			t.Fatalf("publish over inactive stream: %v", err)
		}
		if got := r.Get("live"); got != newer {
			t.Fatal("inactive old stream was not replaced")
		}
		expectReaderEnded(t, rd)
	})
}

// TestPublishPolicyRejectConcurrent 并发同名推流只能有一个成功。
func TestPublishPolicyRejectConcurrent(t *testing.T) {
	r := NewRegistry()
	r.SetPublishPolicy(PublishReject)

	const n = 16
	var wg sync.WaitGroup
	var accepted atomic.Int32
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := r.Publish("live", &Stream{}); err == nil {
				accepted.Add(1)
			} else if !errors.Is(err, ErrPublishRejected) {
				t.Errorf("Publish err = %v, want ErrPublishRejected", err)
			}
		}()
	}
	wg.Wait()
	if got := accepted.Load(); got != 1 {
		t.Fatalf("accepted publishes = %d, want 1", got)
	}
	if r.Len() != 1 {
		t.Fatalf("registry Len = %d, want 1", r.Len())
	}
}

// TestAddObserverMultiple 验证 O-4 的健康告警和 S-3 的发现上报可以同时
// 订阅注册表变更：AddObserver 追加而不是覆盖。
func TestAddObserverMultiple(t *testing.T) {
	reg := NewRegistry()
	var mu sync.Mutex
	seen := map[string][]string{}
	for _, who := range []string{"health", "discovery"} {
		who := who
		reg.AddObserver(func(ev Event, name string, _ *Stream) {
			mu.Lock()
			seen[who] = append(seen[who], ev.String()+":"+name)
			mu.Unlock()
		})
	}

	st := &Stream{}
	if _, err := reg.Publish("live/x", st); err != nil {
		t.Fatal(err)
	}
	reg.Unpublish("live/x", st)

	mu.Lock()
	defer mu.Unlock()
	for _, who := range []string{"health", "discovery"} {
		got := seen[who]
		if len(got) != 2 || got[0] != "published:live/x" || got[1] != "unpublished:live/x" {
			t.Fatalf("%s observer events = %v", who, got)
		}
	}
}

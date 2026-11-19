package dash

import (
	"encoding/xml"
	"errors"
	"strings"
	"testing"
	"time"

	"flow/internal/source"
)

func parseManifest(t *testing.T, b []byte) MPD {
	t.Helper()
	var m MPD
	if err := xml.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal MPD: %v\n%s", err, b)
	}
	return m
}

func findAdaptation(m MPD, kind string) *AdaptationSet {
	for i := range m.Periods[0].AdaptationSets {
		if m.Periods[0].AdaptationSets[i].ContentType == kind {
			return &m.Periods[0].AdaptationSets[i]
		}
	}
	return nil
}

func TestBuildManifest(t *testing.T) {
	f := newFixture(t)
	feedBaseline(t, f)

	now := testEpoch.Add(10 * time.Second)
	b, err := Build(f.packager, now, Options{
		MinimumUpdatePeriod: 2 * time.Second,
		SegmentTarget:       time.Second,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !strings.HasPrefix(string(b), `<?xml version="1.0" encoding="utf-8"?>`) {
		t.Fatalf("missing xml declaration:\n%s", b)
	}

	m := parseManifest(t, b)
	if m.Type != "dynamic" {
		t.Errorf("type = %q", m.Type)
	}
	if m.Profiles != "urn:mpeg:dash:profile:isoff-live:2011" {
		t.Errorf("profiles = %q", m.Profiles)
	}
	if m.AvailabilityStartTime != isoTime(testEpoch) {
		t.Errorf("availabilityStartTime = %q, want %q", m.AvailabilityStartTime, isoTime(testEpoch))
	}
	if m.PublishTime != isoTime(now) {
		t.Errorf("publishTime = %q", m.PublishTime)
	}
	if m.MinimumUpdatePeriod != "PT2S" || m.MinBufferTime != "PT1S" {
		t.Errorf("update/minbuffer = %q/%q", m.MinimumUpdatePeriod, m.MinBufferTime)
	}
	if m.UTCTiming.SchemeIDURI != "urn:mpeg:dash:utc:http-iso:2014" || m.UTCTiming.Value != "utc" {
		t.Errorf("UTCTiming = %+v", m.UTCTiming)
	}
	if len(m.Periods) != 1 || m.Periods[0].Start != "PT0S" {
		t.Fatalf("periods = %+v", m.Periods)
	}

	video := findAdaptation(m, "video")
	if video == nil {
		t.Fatal("no video AdaptationSet")
	}
	if video.MimeType != "video/mp4" {
		t.Errorf("video mime = %q", video.MimeType)
	}
	if len(video.Representations) != 1 {
		t.Fatalf("video reps = %d", len(video.Representations))
	}
	vrep := video.Representations[0]
	if !strings.HasPrefix(vrep.Codecs, "avc1.") {
		t.Errorf("video codecs = %q", vrep.Codecs)
	}
	if vrep.Width != 640 || vrep.Height != 480 {
		t.Errorf("video size = %dx%d", vrep.Width, vrep.Height)
	}
	if vrep.Bandwidth == 0 {
		t.Error("video bandwidth is zero")
	}
	vsnap := f.packager.Video().Snapshot()
	if vrep.SegmentTemplate.Timescale != f.packager.Video().Timescale() {
		t.Errorf("video timescale = %d", vrep.SegmentTemplate.Timescale)
	}
	if vrep.SegmentTemplate.Media != "video/seg-$Number$.m4s" {
		t.Errorf("video media = %q", vrep.SegmentTemplate.Media)
	}
	if vrep.SegmentTemplate.Initialization != vsnap.Segments[0].MapURI {
		t.Errorf("video init = %q", vrep.SegmentTemplate.Initialization)
	}
	if vrep.SegmentTemplate.StartNumber != vsnap.Segments[0].MSN {
		t.Errorf("video startNumber = %d, want %d", vrep.SegmentTemplate.StartNumber, vsnap.Segments[0].MSN)
	}
	entries := vrep.SegmentTemplate.Timeline.S
	if len(entries) != len(vsnap.Segments) {
		t.Fatalf("video timeline entries = %d, segments = %d", len(entries), len(vsnap.Segments))
	}
	if entries[0].T == nil || *entries[0].T != vsnap.Segments[0].StartPTS {
		t.Errorf("first t = %v, want %d", entries[0].T, vsnap.Segments[0].StartPTS)
	}
	for i, e := range entries {
		if e.D != vsnap.Segments[i].DurationTicks {
			t.Errorf("entry %d d = %d, want %d", i, e.D, vsnap.Segments[i].DurationTicks)
		}
	}

	audio := findAdaptation(m, "audio")
	if audio == nil {
		t.Fatal("no audio AdaptationSet")
	}
	arep := audio.Representations[0]
	if arep.Codecs != "mp4a.40.2" {
		t.Errorf("audio codecs = %q", arep.Codecs)
	}
	if arep.AudioSamplingRate != 44100 || arep.SegmentTemplate.Timescale != 44100 {
		t.Errorf("audio sampling/timescale = %d/%d", arep.AudioSamplingRate, arep.SegmentTemplate.Timescale)
	}
	if arep.SegmentTemplate.Media != "audio/seg-$Number$.m4s" {
		t.Errorf("audio media = %q", arep.SegmentTemplate.Media)
	}
}

func TestBuildNoSegments(t *testing.T) {
	f := newFixture(t)
	_, err := Build(f.packager, testEpoch, Options{})
	if !errors.Is(err, ErrNoSegments) {
		t.Fatalf("err = %v, want ErrNoSegments", err)
	}
	if Ready(f.packager) {
		t.Fatal("Ready should be false before the first segment")
	}
}

func TestBuildToken(t *testing.T) {
	f := newFixture(t)
	feedBaseline(t, f)

	b, err := Build(f.packager, testEpoch, Options{Token: "se cret"})
	if err != nil {
		t.Fatal(err)
	}
	m := parseManifest(t, b)
	for _, kind := range []string{"video", "audio"} {
		as := findAdaptation(m, kind)
		if as == nil {
			t.Fatalf("missing %s", kind)
		}
		st := as.Representations[0].SegmentTemplate
		if !strings.Contains(st.Media, "?token=se%20cret") {
			t.Errorf("%s media = %q", kind, st.Media)
		}
		if !strings.Contains(st.Initialization, "?token=se%20cret") {
			t.Errorf("%s init = %q", kind, st.Initialization)
		}
	}
}

func TestBuildOnlyLatestInitVersion(t *testing.T) {
	f := newFixture(t)
	feedBaseline(t, f)

	oldMap := f.packager.Video().Snapshot().Segments[0].MapURI

	// 带内下发不同的 SPS/PPS，触发 init version 1 + discontinuity。
	newSPS := []byte{0x67, 0x64, 0x00, 0x1f, 0xac, 0xd9, 0x40, 0x50}
	newPPS := []byte{0x68, 0xeb, 0xec, 0xb2, 0x2c, 0x01}
	f.video.Write(source.Frame{Data: newSPS, Timestamp: 80 * 3600, AUStart: true, AUEnd: true})
	f.video.Write(source.Frame{Data: newPPS, Timestamp: 80 * 3600, AUStart: true, AUEnd: true})
	writeVideo(f.video, 80, 80)

	deadline := time.Now().Add(5 * time.Second)
	var newSegs []string
	for time.Now().Before(deadline) {
		newSegs = newSegs[:0]
		for _, s := range f.packager.Video().Snapshot().Segments {
			if s.MapURI != oldMap {
				newSegs = append(newSegs, s.MapURI)
			}
		}
		if len(newSegs) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(newSegs) == 0 {
		t.Fatal("no segment with a new init version")
	}
	newMap := newSegs[0]

	b, err := Build(f.packager, testEpoch.Add(20*time.Second), Options{})
	if err != nil {
		t.Fatal(err)
	}
	m := parseManifest(t, b)
	video := findAdaptation(m, "video")
	if video == nil {
		t.Fatal("no video AdaptationSet")
	}
	st := video.Representations[0].SegmentTemplate
	if st.Initialization != newMap {
		t.Errorf("initialization = %q, want %q", st.Initialization, newMap)
	}

	var want []int64
	for _, s := range f.packager.Video().Snapshot().Segments {
		if s.MapURI == newMap {
			want = append(want, s.StartPTS)
		}
	}
	if len(st.Timeline.S) != len(want) {
		t.Fatalf("timeline entries = %d, want %d", len(st.Timeline.S), len(want))
	}
	if len(want) > 0 && (st.Timeline.S[0].T == nil || *st.Timeline.S[0].T != want[0]) {
		t.Errorf("first t = %v, want %d", st.Timeline.S[0].T, want[0])
	}
}

func TestISOExpressions(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{2 * time.Second, "PT2S"},
		{500 * time.Millisecond, "PT0.5S"},
		{1500 * time.Millisecond, "PT1.5S"},
		{0, "PT0S"},
		{-time.Second, "PT0S"},
	}
	for _, c := range cases {
		if got := isoDuration(c.d); got != c.want {
			t.Errorf("isoDuration(%v) = %q, want %q", c.d, got, c.want)
		}
	}
	if got := isoTime(time.Date(2026, 11, 19, 8, 0, 1, 123*int(time.Millisecond), time.FixedZone("x", 3600))); got != "2026-11-19T07:00:01.123Z" {
		t.Errorf("isoTime = %q", got)
	}
}

package hls

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"flow/internal/fmp4"
)

const (
	testVTS  = 90000
	testFR25 = 3600 // 25fps
	testFR30 = 3000 // 30fps
	testAT   = 44100
	testADur = 1024
)

func videoSamples(n int, frameDur uint32, gop int) []Sample {
	out := make([]Sample, n)
	for i := range out {
		out[i] = Sample{
			Data:     []byte{0, 0, 0, 2, 0x09, 0xF0, byte(i), byte(i >> 8)},
			DTS:      int64(i) * int64(frameDur),
			Duration: frameDur,
			Keyframe: gop > 0 && i%gop == 0,
		}
	}
	return out
}

func audioSamples(n int) []Sample {
	out := make([]Sample, n)
	for i := range out {
		out[i] = Sample{
			Data:     []byte{byte(i), byte(i >> 8), 0x21, 0x10},
			DTS:      int64(i) * testADur,
			Duration: testADur,
			Keyframe: true,
		}
	}
	return out
}

func feed(t *testing.T, s *Segmenter, samples []Sample) []Event {
	t.Helper()
	var events []Event
	for i, smp := range samples {
		ev, err := s.Add(smp)
		if err != nil {
			t.Fatalf("Add(%d): %v", i, err)
		}
		events = append(events, ev...)
	}
	return events
}

func finish(t *testing.T, s *Segmenter) []Event {
	t.Helper()
	ev, err := s.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	return ev
}

func splitEvents(t *testing.T, events []Event) (parts []*Part, segments []*Segment, ends int) {
	t.Helper()
	for i, ev := range events {
		switch ev.Kind {
		case EventPart:
			if ev.Part == nil {
				t.Fatalf("event %d: nil Part", i)
			}
			parts = append(parts, ev.Part)
		case EventSegment:
			if ev.Segment == nil {
				t.Fatalf("event %d: nil Segment", i)
			}
			segments = append(segments, ev.Segment)
		case EventEnd:
			ends++
		default:
			t.Fatalf("event %d: unknown kind %d", i, ev.Kind)
		}
	}
	return parts, segments, ends
}

// checkPartRules 校验 RFC 8216bis §4.4.4.9 的 part 时长规则。
// 最后一个 part（父段结束）和 independent part 不受 85% 下限约束。
func checkPartRules(t *testing.T, partTargetTicks int64, parts []*Part) {
	t.Helper()
	for i, p := range parts {
		if p.DurationTicks <= 0 {
			t.Errorf("part %d/%d duration ticks = %d", p.MSN, p.Index, p.DurationTicks)
			continue
		}
		if partTargetTicks > 0 && p.DurationTicks > partTargetTicks {
			t.Errorf("part %d/%d duration %d > part target %d", p.MSN, p.Index, p.DurationTicks, partTargetTicks)
		}
		last := i == len(parts)-1
		if partTargetTicks > 0 && !p.Independent && !last && p.DurationTicks*100 < partTargetTicks*85 {
			t.Errorf("part %d/%d duration %d < 85%% of %d (not independent, not last)",
				p.MSN, p.Index, p.DurationTicks, partTargetTicks)
		}
	}
}

// checkInvariants 校验事件序列的自洽性：part 属于父段、序号连续、时长相加、
// 段/part 字节以 styp/moof 开头、85% 规则。
func checkInvariants(t *testing.T, s *Segmenter, events []Event) {
	t.Helper()
	partTarget := s.PartTargetTicks()
	var cur []*Part
	var sawEnd bool
	for _, ev := range events {
		switch ev.Kind {
		case EventPart:
			if sawEnd {
				t.Fatal("part event after End")
			}
			p := ev.Part
			if len(cur) > 0 && p.MSN != cur[0].MSN {
				t.Fatalf("part MSN changed from %d to %d before segment close", cur[0].MSN, p.MSN)
			}
			if p.Index != len(cur) {
				t.Errorf("part index = %d, want %d", p.Index, len(cur))
			}
			if len(p.Data) < 8 || string(p.Data[4:8]) != "moof" {
				t.Errorf("part %d/%d does not start with moof", p.MSN, p.Index)
			}
			cur = append(cur, p)
		case EventSegment:
			if sawEnd {
				t.Fatal("segment event after End")
			}
			seg := ev.Segment
			if len(cur) == 0 {
				t.Fatalf("segment %d has no part events", seg.MSN)
			}
			if seg.PartCount != len(cur) {
				t.Errorf("segment %d PartCount = %d, want %d", seg.MSN, seg.PartCount, len(cur))
			}
			var sum int64
			for i, p := range cur {
				if p.MSN != seg.MSN {
					t.Errorf("segment %d contains part MSN %d", seg.MSN, p.MSN)
				}
				if i < len(cur)-1 && p.Index+1 != cur[i+1].Index {
					t.Errorf("segment %d part indices not consecutive: %d -> %d", seg.MSN, p.Index, cur[i+1].Index)
				}
				sum += p.DurationTicks
			}
			if sum != seg.DurationTicks {
				t.Errorf("segment %d duration %d != sum of parts %d", seg.MSN, seg.DurationTicks, sum)
			}
			if len(seg.Data) < 8 || string(seg.Data[4:8]) != "styp" {
				t.Errorf("segment %d does not start with styp", seg.MSN)
			}
			wantLen := len(fmp4.Styp())
			for _, p := range cur {
				wantLen += len(p.Data)
			}
			if len(seg.Data) != wantLen {
				t.Errorf("segment %d data len = %d, want %d", seg.MSN, len(seg.Data), wantLen)
			}
			checkPartRules(t, partTarget, cur)
			cur = nil
		case EventEnd:
			sawEnd = true
			if len(cur) != 0 {
				t.Fatalf("End with %d parts not closed by a segment", len(cur))
			}
		}
	}
	if len(cur) != 0 {
		t.Fatalf("%d parts not closed by a segment", len(cur))
	}
}

func TestNewSegmenterValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{"zero track", Config{TrackID: 0, Timescale: testVTS, SegmentTarget: time.Second}},
		{"zero timescale", Config{TrackID: 1, Timescale: 0, SegmentTarget: time.Second}},
		{"zero segment", Config{TrackID: 1, Timescale: testVTS}},
		{"negative part", Config{TrackID: 1, Timescale: testVTS, SegmentTarget: 4 * time.Second, PartTarget: -time.Millisecond}},
		{"part > segment", Config{TrackID: 1, Timescale: testVTS, SegmentTarget: time.Second, PartTarget: 2 * time.Second}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewSegmenter(tt.cfg); err == nil {
				t.Fatal("NewSegmenter returned nil error")
			}
		})
	}
}

func TestVideo25fpsParts(t *testing.T) {
	s, err := NewSegmenter(Config{
		TrackID:       1,
		Timescale:     testVTS,
		SegmentTarget: 4 * time.Second,
		PartTarget:    500 * time.Millisecond,
		Video:         true,
	})
	if err != nil {
		t.Fatalf("NewSegmenter: %v", err)
	}
	// 25fps 的 500ms 向上取整到 13 帧 = 520ms，避免 part 边界漂移。
	events := feed(t, s, videoSamples(250, testFR25, 25)) // 10s，GOP 1s
	events = append(events, finish(t, s)...)

	parts, segments, ends := splitEvents(t, events)
	if ends != 1 {
		t.Fatalf("End events = %d, want 1", ends)
	}
	if len(segments) != 3 {
		t.Fatalf("segments = %d, want 3", len(segments))
	}
	if got, want := s.PartTargetTicks(), int64(46800); got != want {
		t.Errorf("effective part target = %d ticks, want %d (13x3600)", got, want)
	}
	if got := s.TargetDuration(); got != 4 {
		t.Errorf("target duration = %d, want 4", got)
	}
	if got := segments[0].DurationTicks; got != 4*testVTS/1 {
		t.Errorf("segment 0 duration = %d, want 4s", got)
	}
	if got := segments[1].DurationTicks; got != 4*testVTS/1 {
		t.Errorf("segment 1 duration = %d, want 4s", got)
	}
	if got := segments[2].DurationTicks; got != 2*testVTS/1 {
		t.Errorf("segment 2 duration = %d, want 2s", got)
	}
	for i, seg := range segments {
		if seg.MSN != uint64(i) {
			t.Errorf("segment %d MSN = %d", i, seg.MSN)
		}
		if seg.Discontinuity {
			t.Errorf("segment %d unexpectedly has discontinuity", i)
		}
		if seg.DiscSeq != 0 {
			t.Errorf("segment %d DiscSeq = %d", i, seg.DiscSeq)
		}
	}
	// 25fps + 520ms part + 1s GOP：关键帧处应该能切出 independent part。
	independent := 0
	for _, p := range parts {
		if p.Independent {
			independent++
		}
	}
	if independent < len(segments) {
		t.Errorf("independent parts = %d, want at least %d", independent, len(segments))
	}
	checkInvariants(t, s, events)
	if _, ok := s.Open(); ok {
		t.Error("Open() = true after Finish")
	}
	if got := s.NextMSN(); got != 3 {
		t.Errorf("NextMSN = %d, want 3", got)
	}
}

func TestVideo30fpsParts(t *testing.T) {
	s, err := NewSegmenter(Config{
		TrackID:       1,
		Timescale:     testVTS,
		SegmentTarget: 4 * time.Second,
		PartTarget:    500 * time.Millisecond,
		Video:         true,
	})
	if err != nil {
		t.Fatalf("NewSegmenter: %v", err)
	}
	events := feed(t, s, videoSamples(300, testFR30, 30)) // 10s，GOP 1s
	events = append(events, finish(t, s)...)
	_, segments, _ := splitEvents(t, events)
	if len(segments) != 3 {
		t.Fatalf("segments = %d, want 3", len(segments))
	}
	if got := s.PartTargetTicks(); got != int64(45000) {
		t.Errorf("effective part target = %d, want 45000 (15x3000)", got)
	}
	if got := s.TargetDuration(); got != 4 {
		t.Errorf("target duration = %d, want 4", got)
	}
	checkInvariants(t, s, events)
}

func TestVideoIDRMidPartNotIndependent(t *testing.T) {
	s, err := NewSegmenter(Config{
		TrackID:       1,
		Timescale:     testVTS,
		SegmentTarget: 4 * time.Second,
		PartTarget:    500 * time.Millisecond,
		Video:         true,
	})
	if err != nil {
		t.Fatalf("NewSegmenter: %v", err)
	}
	samples := videoSamples(31, testFR30, 0)
	samples[0].Keyframe = true
	samples[17].Keyframe = true // 落在第二个 part 内部（part 从第 15 帧开始）
	events := feed(t, s, samples)
	events = append(events, finish(t, s)...)

	parts, _, _ := splitEvents(t, events)
	if len(parts) < 2 {
		t.Fatalf("parts = %d, want >= 2", len(parts))
	}
	if !parts[0].Independent {
		t.Error("first part should start with the initial IDR")
	}
	// 第二个 part 从非关键帧开始，IDR 在中间，不能标 independent。
	if parts[1].Index != 1 {
		t.Fatalf("part[1].Index = %d, want 1", parts[1].Index)
	}
	if parts[1].Independent {
		t.Error("part with mid-part IDR should not be independent")
	}
	checkInvariants(t, s, events)
}

func TestAudioPartsAndSegments(t *testing.T) {
	s, err := NewSegmenter(Config{
		TrackID:       2,
		Timescale:     testAT,
		SegmentTarget: 4 * time.Second,
		PartTarget:    500 * time.Millisecond,
		Video:         false,
	})
	if err != nil {
		t.Fatalf("NewSegmenter: %v", err)
	}
	events := feed(t, s, audioSamples(431)) // ≈10s
	events = append(events, finish(t, s)...)

	_, segments, _ := splitEvents(t, events)
	if len(segments) != 3 {
		t.Fatalf("segments = %d, want 3", len(segments))
	}
	// 音频 part target 22528 ticks（22 个 1024 采样）。
	if got := s.PartTargetTicks(); got != 22528 {
		t.Errorf("effective part target = %d, want 22528", got)
	}
	if got := s.TargetDuration(); got != 4 {
		t.Errorf("target duration = %d, want 4", got)
	}
	for i, seg := range segments {
		if seg.DurationTicks > 4*int64(testAT) {
			t.Errorf("segment %d duration %d exceeds 4s", i, seg.DurationTicks)
		}
	}
	checkInvariants(t, s, events)
}

func TestPartTargetZeroClassic(t *testing.T) {
	s, err := NewSegmenter(Config{
		TrackID:       1,
		Timescale:     testVTS,
		SegmentTarget: 2 * time.Second,
		PartTarget:    0,
		Video:         true,
	})
	if err != nil {
		t.Fatalf("NewSegmenter: %v", err)
	}
	if got := s.PartTarget(); got != 0 {
		t.Errorf("PartTarget before data = %s, want 0", got)
	}
	events := feed(t, s, videoSamples(120, testFR30, 30)) // 4s
	events = append(events, finish(t, s)...)
	parts, segments, _ := splitEvents(t, events)
	if len(segments) != 2 {
		t.Fatalf("segments = %d, want 2", len(segments))
	}
	for _, seg := range segments {
		if seg.PartCount != 1 {
			t.Errorf("segment %d PartCount = %d, want 1 (LL disabled)", seg.MSN, seg.PartCount)
		}
	}
	if len(parts) != 2 {
		t.Fatalf("parts = %d, want 2", len(parts))
	}
	if s.PartTargetTicks() != 0 {
		t.Errorf("PartTargetTicks = %d, want 0", s.PartTargetTicks())
	}
	checkInvariants(t, s, events)
}

func TestTargetDurationGrowthOnLongGOP(t *testing.T) {
	s, err := NewSegmenter(Config{
		TrackID:       1,
		Timescale:     testVTS,
		SegmentTarget: 4 * time.Second,
		PartTarget:    500 * time.Millisecond,
		Video:         true,
	})
	if err != nil {
		t.Fatalf("NewSegmenter: %v", err)
	}
	// GOP 6s：没有 IDR 就不能切段，第一段会变成 6s，TARGETDURATION 必须跟着长。
	events := feed(t, s, videoSamples(360, testFR30, 180))
	events = append(events, finish(t, s)...)
	_, segments, _ := splitEvents(t, events)
	if len(segments) != 2 {
		t.Fatalf("segments = %d, want 2", len(segments))
	}
	if got := segments[0].DurationTicks; got != 6*testVTS {
		t.Errorf("segment 0 duration = %d, want 6s", got)
	}
	if got := s.TargetDuration(); got != 6 {
		t.Errorf("target duration = %d, want 6", got)
	}
	checkInvariants(t, s, events)
}

func TestDiscontinuity(t *testing.T) {
	s, err := NewSegmenter(Config{
		TrackID:       1,
		Timescale:     testVTS,
		SegmentTarget: 4 * time.Second,
		PartTarget:    500 * time.Millisecond,
		Video:         true,
	})
	if err != nil {
		t.Fatalf("NewSegmenter: %v", err)
	}
	events := feed(t, s, videoSamples(60, testFR30, 30)) // 2s
	mark, err := s.MarkDiscontinuity(1)
	if err != nil {
		t.Fatalf("MarkDiscontinuity: %v", err)
	}
	events = append(events, mark...)

	// 断点后的非关键帧应被丢弃（不能接到旧 init 上）。
	for i := 0; i < 5; i++ {
		ev, err := s.Add(Sample{
			Data:     []byte{0xAA, byte(i)},
			DTS:      180000 + int64(i)*testFR30,
			Duration: testFR30,
			Keyframe: false,
		})
		if err != nil {
			t.Fatalf("Add non-key after discontinuity: %v", err)
		}
		if len(ev) != 0 {
			t.Fatalf("non-key after discontinuity produced events: %+v", ev)
		}
	}
	// 下一个 IDR 开新段。
	resume := []Sample{
		{Data: []byte{0, 0, 0, 2, 0x09, 0xF0, 0x55}, DTS: 195000, Duration: testFR30, Keyframe: true},
		{Data: []byte{0, 0, 0, 2, 0x09, 0xF0, 0x56}, DTS: 198000, Duration: testFR30},
	}
	events = append(events, feed(t, s, resume)...)
	events = append(events, finish(t, s)...)

	_, segments, _ := splitEvents(t, events)
	if len(segments) != 2 {
		t.Fatalf("segments = %d, want 2", len(segments))
	}
	if segments[0].Discontinuity {
		t.Error("segment 0 should not have discontinuity")
	}
	if segments[0].MapVersion != 0 {
		t.Errorf("segment 0 MapVersion = %d, want 0", segments[0].MapVersion)
	}
	if !segments[1].Discontinuity {
		t.Error("segment 1 should have discontinuity")
	}
	if segments[1].DiscSeq != 1 {
		t.Errorf("segment 1 DiscSeq = %d, want 1", segments[1].DiscSeq)
	}
	if segments[1].MapVersion != 1 {
		t.Errorf("segment 1 MapVersion = %d, want 1", segments[1].MapVersion)
	}
	if got := s.DiscSeq(); got != 1 {
		t.Errorf("DiscSeq = %d, want 1", got)
	}
	checkInvariants(t, s, events)
}

func TestDiscontinuityBeforeFirstSegment(t *testing.T) {
	s, err := NewSegmenter(Config{
		TrackID:       1,
		Timescale:     testVTS,
		SegmentTarget: 4 * time.Second,
		PartTarget:    500 * time.Millisecond,
		Video:         true,
	})
	if err != nil {
		t.Fatalf("NewSegmenter: %v", err)
	}
	mark, err := s.MarkDiscontinuity(2)
	if err != nil {
		t.Fatalf("MarkDiscontinuity: %v", err)
	}
	if len(mark) != 0 {
		t.Fatalf("Mark before first segment produced %d events", len(mark))
	}
	events := feed(t, s, videoSamples(30, testFR30, 30))
	events = append(events, finish(t, s)...)
	_, segments, _ := splitEvents(t, events)
	if len(segments) != 1 {
		t.Fatalf("segments = %d, want 1", len(segments))
	}
	if segments[0].Discontinuity {
		t.Error("first segment should not carry a discontinuity")
	}
	if segments[0].MapVersion != 2 {
		t.Errorf("MapVersion = %d, want 2", segments[0].MapVersion)
	}
	if segments[0].DiscSeq != 0 {
		t.Errorf("DiscSeq = %d, want 0", segments[0].DiscSeq)
	}
}

func TestOpenState(t *testing.T) {
	s, err := NewSegmenter(Config{
		TrackID:       1,
		Timescale:     testVTS,
		SegmentTarget: 4 * time.Second,
		PartTarget:    500 * time.Millisecond,
		Video:         true,
	})
	if err != nil {
		t.Fatalf("NewSegmenter: %v", err)
	}
	if _, ok := s.Open(); ok {
		t.Fatal("Open() = true before any sample")
	}
	if _, err := s.Add(videoSamples(1, testFR30, 30)[0]); err != nil {
		t.Fatalf("Add: %v", err)
	}
	open, ok := s.Open()
	if !ok {
		t.Fatal("Open() = false after first keyframe")
	}
	if open.MSN != 0 || open.PartIndex != 0 || open.PartCount != 0 {
		t.Fatalf("open = %+v, want msn 0 / part 0 / count 0", open)
	}
	// 喂够一个 part，partIndex/PartCount 应前进。
	feed(t, s, videoSamples(30, testFR30, 30)[1:])
	open, ok = s.Open()
	if !ok {
		t.Fatal("Open() = false while segment open")
	}
	if open.PartCount == 0 {
		t.Error("PartCount = 0 after 1s of media")
	}
	if open.PartIndex != open.PartCount {
		t.Errorf("PartIndex = %d, want %d (next index)", open.PartIndex, open.PartCount)
	}
}

func TestBFrameNegativeCTS(t *testing.T) {
	s, err := NewSegmenter(Config{
		TrackID:       1,
		Timescale:     testVTS,
		SegmentTarget: 4 * time.Second,
		PartTarget:    500 * time.Millisecond,
		Video:         true,
	})
	if err != nil {
		t.Fatalf("NewSegmenter: %v", err)
	}
	samples := videoSamples(60, testFR30, 30)
	for i := range samples {
		if i > 0 && i%3 != 0 {
			samples[i].CTS = -3000
		}
	}
	events := feed(t, s, samples)
	events = append(events, finish(t, s)...)
	_, segments, _ := splitEvents(t, events)
	if len(segments) == 0 {
		t.Fatal("no segments")
	}
	if got := segments[0].StartPTS; got != 0 {
		t.Errorf("segment 0 StartPTS = %d, want 0", got)
	}
	checkInvariants(t, s, events)
}

func TestFinishIdempotentAndAddAfterFinish(t *testing.T) {
	s, err := NewSegmenter(Config{
		TrackID:       1,
		Timescale:     testVTS,
		SegmentTarget: time.Second,
		Video:         true,
	})
	if err != nil {
		t.Fatalf("NewSegmenter: %v", err)
	}
	feed(t, s, videoSamples(10, testFR30, 30))
	finish(t, s)
	ev, err := s.Finish()
	if err != nil {
		t.Fatalf("second Finish: %v", err)
	}
	if len(ev) != 0 {
		t.Errorf("second Finish events = %d, want 0", len(ev))
	}
	if _, err := s.Add(videoSamples(1, testFR30, 30)[0]); !errors.Is(err, ErrFinished) {
		t.Errorf("Add after Finish err = %v, want ErrFinished", err)
	}
	if _, err := s.MarkDiscontinuity(1); !errors.Is(err, ErrFinished) {
		t.Errorf("Mark after Finish err = %v, want ErrFinished", err)
	}
}

func TestSampleValidation(t *testing.T) {
	video, err := NewSegmenter(Config{
		TrackID:       1,
		Timescale:     testVTS,
		SegmentTarget: 4 * time.Second,
		PartTarget:    500 * time.Millisecond,
		Video:         true,
	})
	if err != nil {
		t.Fatalf("NewSegmenter: %v", err)
	}
	audio, err := NewSegmenter(Config{
		TrackID:       2,
		Timescale:     testAT,
		SegmentTarget: 4 * time.Second,
		PartTarget:    500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSegmenter(audio): %v", err)
	}
	base := Sample{Data: []byte{1}, DTS: 0, Duration: 3000, Keyframe: true}

	tests := []struct {
		name string
		s    *Segmenter
		smp  Sample
	}{
		{"empty data", video, func() Sample { s := base; s.Data = nil; return s }()},
		{"zero duration", video, func() Sample { s := base; s.Duration = 0; return s }()},
		{"negative DTS", video, func() Sample { s := base; s.DTS = -1; return s }()},
		{"audio CTS", audio, func() Sample { s := base; s.CTS = 1; return s }()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := tt.s.Add(tt.smp); err == nil {
				t.Fatal("Add returned nil error")
			}
		})
	}

	// DTS 不连续
	if _, err := video.Add(base); err != nil {
		t.Fatalf("first Add: %v", err)
	}
	bad := base
	bad.DTS = 6000 // 期望 3000
	bad.Keyframe = false
	if _, err := video.Add(bad); err == nil {
		t.Fatal("non-contiguous DTS returned nil error")
	}
}

func TestLargeTimestamps(t *testing.T) {
	s, err := NewSegmenter(Config{
		TrackID:       1,
		Timescale:     testVTS,
		SegmentTarget: 2 * time.Second,
		PartTarget:    500 * time.Millisecond,
		Video:         true,
	})
	if err != nil {
		t.Fatalf("NewSegmenter: %v", err)
	}
	// 从 2^32 附近开始：状态机用 int64，不能因为时间戳接近 uint32 上限就溢出/负时长。
	base := int64(1<<32) - int64(10*testFR30)
	samples := make([]Sample, 60)
	for i := range samples {
		samples[i] = Sample{
			Data:     []byte{byte(i), 0, 0, 2, 0x09, 0xF0},
			DTS:      base + int64(i)*testFR30,
			Duration: testFR30,
			Keyframe: i%30 == 0,
		}
	}
	events := feed(t, s, samples)
	events = append(events, finish(t, s)...)
	_, segments, _ := splitEvents(t, events)
	if len(segments) != 1 {
		t.Fatalf("segments = %d, want 1", len(segments))
	}
	if got := segments[0].StartPTS; got != base {
		t.Errorf("StartPTS = %d, want %d", got, base)
	}
	checkInvariants(t, s, events)
}

func TestSegmentDataIsConcatenation(t *testing.T) {
	s, err := NewSegmenter(Config{
		TrackID:       1,
		Timescale:     testVTS,
		SegmentTarget: 2 * time.Second,
		PartTarget:    500 * time.Millisecond,
		Video:         true,
	})
	if err != nil {
		t.Fatalf("NewSegmenter: %v", err)
	}
	events := feed(t, s, videoSamples(60, testFR30, 30))
	events = append(events, finish(t, s)...)
	parts, segments, _ := splitEvents(t, events)
	if len(segments) != 1 {
		t.Fatalf("segments = %d, want 1", len(segments))
	}
	var want []byte
	want = append(want, fmp4.Styp()...)
	for _, p := range parts {
		want = append(want, p.Data...)
	}
	if !bytes.Equal(segments[0].Data, want) {
		t.Error("segment data != styp + concatenated parts")
	}
}

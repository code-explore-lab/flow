package record

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"flow/internal/source"
	"flow/internal/stream"
)

// 这些是 fmp4/mpegts 测试里用过的真实 SPS（640x360）和一个合法 PPS/AAC ASC。
const (
	testSPSHex = "6764001eacd940a03db011000003000100000300320f162d9600"
	testPPSHex = "68ee3c80"
)

func testVideoCodec(t *testing.T) *stream.VideoCodec {
	t.Helper()
	sps, err := hex.DecodeString(testSPSHex)
	if err != nil {
		t.Fatalf("decode SPS: %v", err)
	}
	pps, err := hex.DecodeString(testPPSHex)
	if err != nil {
		t.Fatalf("decode PPS: %v", err)
	}
	return &stream.VideoCodec{Codec: "H264", SPS: sps, PPS: pps}
}

func testAudioCodec() *stream.AudioCodec {
	return &stream.AudioCodec{
		Codec:      stream.AudioAAC,
		Config:     []byte{0x12, 0x10}, // AAC-LC 44.1kHz stereo
		SampleRate: 44100,
		Channels:   2,
		ClockRate:  44100,
	}
}

// feedTrack 是一个只走 ring 的假订阅源。和 ExternalSource 不同，它不做
// GOP 预热，测试可以精确控制“订阅之后才写入”的帧。
type feedTrack struct {
	ring *source.RingBuffer[source.Frame]
	sub  chan struct{}
	once sync.Once
}

func newFeedTrack() *feedTrack {
	return &feedTrack{
		ring: source.NewRingBuffer[source.Frame](512),
		sub:  make(chan struct{}),
	}
}

func (f *feedTrack) Subscribe() (*source.Reader[source.Frame], func()) {
	rd := f.ring.Subscribe()
	f.once.Do(func() { close(f.sub) })
	return rd, rd.Close
}

func (f *feedTrack) write(frame source.Frame) { f.ring.Write(frame) }
func (f *feedTrack) close()                   { f.ring.Close() }

// feedVideo 写入 SPS/PPS 和 count 帧 25fps 视频，每 25 帧一个 IDR。
func feedVideo(t *testing.T, f *feedTrack, vc *stream.VideoCodec, count int) {
	t.Helper()
	f.write(source.Frame{Data: append([]byte(nil), vc.SPS...)})
	f.write(source.Frame{Data: append([]byte(nil), vc.PPS...)})
	for i := 0; i < count; i++ {
		key := i%25 == 0
		nal := []byte{0x41, 0x9a, byte(i), byte(i >> 8)}
		if key {
			nal[0] = 0x65
		}
		f.write(source.Frame{
			Data:      nal,
			Timestamp: uint32(i) * 3600,
			Keyframe:  key,
			AUStart:   true,
			AUEnd:     true,
		})
	}
}

// feedAudio 写入 count 个 AAC 帧，每帧 1024 个采样。
func feedAudio(f *feedTrack, count int) {
	for i := 0; i < count; i++ {
		f.write(source.Frame{Data: []byte{0x21, byte(i), byte(i >> 8)}, Timestamp: uint32(i) * 1024})
	}
}

func waitRecorderDone(t *testing.T, r *Recorder) {
	t.Helper()
	select {
	case <-r.done:
	case <-time.After(10 * time.Second):
		t.Fatal("recorder did not finish")
	}
	if err := r.Err(); err != nil {
		t.Fatalf("recorder error: %v", err)
	}
}

func listFiles(t *testing.T, dir, ext string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ext {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(out)
	return out
}

func mp4BoxTypes(t *testing.T, data []byte) []string {
	t.Helper()
	var types []string
	for off := 0; off < len(data); {
		if off+8 > len(data) {
			t.Fatalf("truncated box header at %d/%d", off, len(data))
		}
		size := int(binary.BigEndian.Uint32(data[off:]))
		typ := string(data[off+4 : off+8])
		switch size {
		case 0:
			size = len(data) - off
		case 1:
			if off+16 > len(data) {
				t.Fatalf("truncated large box header at %d", off)
			}
			size = int(binary.BigEndian.Uint64(data[off+8:]))
		}
		if size < 8 || off+size > len(data) {
			t.Fatalf("box %q size %d out of range at %d/%d", typ, size, off, len(data))
		}
		types = append(types, typ)
		off += size
	}
	return types
}

func flvTagTypes(t *testing.T, data []byte) []byte {
	t.Helper()
	if len(data) < 13 || string(data[:3]) != "FLV" {
		t.Fatalf("not an FLV file: % x", data[:min(13, len(data))])
	}
	headerLen := int(binary.BigEndian.Uint32(data[5:9]))
	pos := headerLen + 4 // FLV header + PreviousTagSize0
	var types []byte
	for pos < len(data) {
		if pos+11 > len(data) {
			t.Fatalf("truncated FLV tag header at %d/%d", pos, len(data))
		}
		typ := data[pos] & 0x1f
		size := int(data[pos+1])<<16 | int(data[pos+2])<<8 | int(data[pos+3])
		if pos+11+size+4 > len(data) {
			t.Fatalf("FLV tag %d size %d out of range at %d/%d", typ, size, pos, len(data))
		}
		types = append(types, typ)
		pos += 11 + size + 4
	}
	return types
}

func TestRecorderMP4SegmentRotation(t *testing.T) {
	dir := t.TempDir()
	vt := newFeedTrack()
	vc := testVideoCodec(t)

	rec, err := NewRecorder(Spec{
		Name:  "cam",
		Video: &TrackSource{Subscriber: vt, Video: vc},
	}, Options{
		Dir:             dir,
		Format:          FormatMP4,
		SegmentDuration: time.Second,
		Logf:            t.Logf,
	})
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	rec.Start()
	<-vt.sub
	feedVideo(t, vt, vc, 75)
	vt.close()
	waitRecorderDone(t, rec)

	files := listFiles(t, dir, ".mp4")
	if len(files) != 3 {
		t.Fatalf("got %d mp4 files, want 3: %v", len(files), files)
	}
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		types := mp4BoxTypes(t, data)
		if len(types) < 3 || types[0] != "ftyp" || types[1] != "moov" {
			t.Fatalf("%s: boxes = %v, want ftyp/moov first", filepath.Base(path), types)
		}
		moofs := 0
		for _, typ := range types {
			if typ == "moof" {
				moofs++
			}
		}
		if moofs == 0 {
			t.Fatalf("%s: no moof in %v", filepath.Base(path), types)
		}
		if _, err := os.Stat(filepath.Join(dir, "cam.state")); !os.IsNotExist(err) {
			t.Fatalf("state file left behind after clean close: %v", err)
		}
	}
}

func TestRecorderMP4AudioOnly(t *testing.T) {
	dir := t.TempDir()
	at := newFeedTrack()
	ac := testAudioCodec()

	rec, err := NewRecorder(Spec{
		Name:  "radio",
		Audio: &TrackSource{Subscriber: at, Audio: ac},
	}, Options{
		Dir:             dir,
		Format:          FormatMP4,
		SegmentDuration: time.Second,
		Logf:            t.Logf,
	})
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	rec.Start()
	<-at.sub
	feedAudio(at, 200)
	at.close()
	waitRecorderDone(t, rec)

	files := listFiles(t, dir, ".mp4")
	if len(files) < 3 {
		t.Fatalf("got %d mp4 files, want >= 3: %v", len(files), files)
	}
	for _, path := range files {
		types := mp4BoxTypes(t, mustRead(t, path))
		if len(types) < 3 || types[0] != "ftyp" || types[1] != "moov" {
			t.Fatalf("%s: boxes = %v", filepath.Base(path), types)
		}
	}
}

func TestRecorderFLVWithAudio(t *testing.T) {
	dir := t.TempDir()
	vt, at := newFeedTrack(), newFeedTrack()
	vc, ac := testVideoCodec(t), testAudioCodec()

	rec, err := NewRecorder(Spec{
		Name:  "cam",
		Video: &TrackSource{Subscriber: vt, Video: vc},
		Audio: &TrackSource{Subscriber: at, Audio: ac},
	}, Options{
		Dir:             dir,
		Format:          FormatFLV,
		SegmentDuration: 2 * time.Second,
		Logf:            t.Logf,
	})
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	rec.Start()
	<-vt.sub
	<-at.sub
	feedVideo(t, vt, vc, 60)
	feedAudio(at, 100)
	vt.close()
	at.close()
	waitRecorderDone(t, rec)

	files := listFiles(t, dir, ".flv")
	if len(files) != 2 {
		t.Fatalf("got %d flv files, want 2: %v", len(files), files)
	}
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		types := flvTagTypes(t, data)
		var haveVideo, haveAudioSeq, haveVideoSeq bool
		pos := int(binary.BigEndian.Uint32(data[5:9])) + 4
		for _, typ := range types {
			size := int(data[pos+1])<<16 | int(data[pos+2])<<8 | int(data[pos+3])
			body := data[pos+11 : pos+11+size]
			switch typ {
			case 9:
				haveVideo = true
				if len(body) >= 2 && body[0]&0x0f == 7 && body[1] == 0 {
					haveVideoSeq = true
				}
			case 8:
				if len(body) >= 2 && body[0]>>4 == 10 && body[1] == 0 {
					haveAudioSeq = true
				}
			}
			pos += 11 + size + 4
		}
		if !haveVideo || !haveVideoSeq || !haveAudioSeq {
			t.Fatalf("%s: video=%v videoSeq=%v audioSeq=%v tags=%v",
				filepath.Base(path), haveVideo, haveVideoSeq, haveAudioSeq, types)
		}
	}
}

func TestRecorderTSRotation(t *testing.T) {
	dir := t.TempDir()
	vt, at := newFeedTrack(), newFeedTrack()
	vc, ac := testVideoCodec(t), testAudioCodec()

	rec, err := NewRecorder(Spec{
		Name:  "cam",
		Video: &TrackSource{Subscriber: vt, Video: vc},
		Audio: &TrackSource{Subscriber: at, Audio: ac},
	}, Options{
		Dir:             dir,
		Format:          FormatTS,
		SegmentDuration: time.Second,
		Logf:            t.Logf,
	})
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	rec.Start()
	<-vt.sub
	<-at.sub
	feedVideo(t, vt, vc, 75)
	feedAudio(at, 120)
	vt.close()
	at.close()
	waitRecorderDone(t, rec)

	files := listFiles(t, dir, ".ts")
	if len(files) != 3 {
		t.Fatalf("got %d ts files, want 3: %v", len(files), files)
	}
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if len(data) == 0 || len(data)%188 != 0 {
			t.Fatalf("%s: size %d is not a multiple of 188", filepath.Base(path), len(data))
		}
		for off := 0; off < len(data); off += 188 {
			if data[off] != 0x47 {
				t.Fatalf("%s: bad sync byte at %d: %#x", filepath.Base(path), off, data[off])
			}
		}
	}
}

func TestMP4WriterResume(t *testing.T) {
	dir := t.TempDir()
	vc, ac := testVideoCodec(t), testAudioCodec()
	opts := Options{
		Dir:             dir,
		Format:          FormatMP4,
		SegmentDuration: time.Minute,
		Resume:          true,
		Logf:            t.Logf,
	}.withDefaults()

	first, err := newMP4Writer(opts, "cam", vc, ac, true, true)
	if err != nil {
		t.Fatalf("newMP4Writer: %v", err)
	}
	for i := 0; i < 3; i++ {
		s := sample{kind: videoTrack, dts: int64(i) * 3600, dur: 3600, key: i == 0,
			nalus: [][]byte{{0x65, 0x01}}}
		if i > 0 {
			s.nalus = [][]byte{{0x41, 0x02}}
		}
		if err := first.Write(s); err != nil {
			t.Fatalf("write #%d: %v", i, err)
		}
	}
	// 音频故意落后视频：续录时两条轨要对齐到同一个时间点。
	for i := 0; i < 2; i++ {
		if err := first.Write(sample{kind: audioTrack, dts: int64(i) * 1024, dur: 1024, data: []byte{0x21, byte(i)}}); err != nil {
			t.Fatalf("write audio #%d: %v", i, err)
		}
	}
	if err := first.flushFragment(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	path, size := first.path, first.size
	// 模拟进程崩溃：保留 .state，截断到最后一个完整 fragment。
	if err := first.closeFile(false); err != nil {
		t.Fatalf("closeFile: %v", err)
	}
	st, err := readState(filepath.Join(dir, "cam.state"))
	if err != nil {
		t.Fatalf("readState: %v", err)
	}
	if st.Path != path || st.Size != size {
		t.Fatalf("state path/size = %s/%d, want %s/%d", st.Path, st.Size, path, size)
	}

	second, err := newMP4Writer(opts, "cam", vc, ac, true, true)
	if err != nil {
		t.Fatalf("resume writer: %v", err)
	}
	if second.path != path || second.size != size {
		t.Fatalf("resumed at %s/%d, want %s/%d", second.path, second.size, path, size)
	}
	if got := second.vclock.next; got != st.VideoNext {
		t.Fatalf("resumed video clock = %d, want %d", got, st.VideoNext)
	}
	// 音频比视频落后约 0.07s，续录后必须被抬到视频的终点。
	wantAudioNext := st.VideoNext * 44100 / 90000
	if got := second.aclock.next; got != wantAudioNext {
		t.Fatalf("resumed audio clock = %d, want %d (aligned with video %d)",
			got, wantAudioNext, st.VideoNext)
	}
	s := sample{kind: videoTrack, dts: 3 * 3600, dur: 3600, key: true,
		nalus: [][]byte{{0x65, 0x03}}}
	if err := second.Write(s); err != nil {
		t.Fatalf("write after resume: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("close resumed writer: %v", err)
	}

	files := listFiles(t, dir, ".mp4")
	if len(files) != 1 || files[0] != path {
		t.Fatalf("files = %v, want only %s", files, path)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() <= size {
		t.Fatalf("resumed file did not grow: %d <= %d", info.Size(), size)
	}
	if _, err := os.Stat(filepath.Join(dir, "cam.state")); !os.IsNotExist(err) {
		t.Fatalf("state file left after clean close: %v", err)
	}
	types := mp4BoxTypes(t, mustRead(t, path))
	if types[0] != "ftyp" || types[1] != "moov" {
		t.Fatalf("boxes = %v", types)
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func TestQuotaEvictsOldest(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(1_700_000_000, 0)
	mk := func(name string, size int, mod time.Time) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, bytes.Repeat([]byte{1}, size), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		if err := os.Chtimes(path, mod, mod); err != nil {
			t.Fatalf("chtimes %s: %v", name, err)
		}
		return path
	}
	oldest := mk("cam-20231114-000001.mp4", 100, now.Add(-3*time.Hour))
	mk("cam-20231114-000002.mp4", 100, now.Add(-2*time.Hour))
	active := mk("cam-20231114-000003.mp4", 100, now.Add(-1*time.Hour))
	state := filepath.Join(dir, "cam.state")
	if err := os.WriteFile(state, []byte(`{}`), 0o644); err != nil {
		t.Fatalf("write state: %v", err)
	}

	q := &quota{dir: dir, prefix: "cam", max: 150, now: func() time.Time { return now }, logf: t.Logf}
	if err := q.enforce(active, state); err != nil {
		t.Fatalf("enforce: %v", err)
	}
	if _, err := os.Stat(oldest); !os.IsNotExist(err) {
		t.Fatalf("oldest file was not evicted: %v", err)
	}
	if _, err := os.Stat(active); err != nil {
		t.Fatalf("active file was removed: %v", err)
	}
	if _, err := os.Stat(state); err != nil {
		t.Fatalf("state file was removed: %v", err)
	}
}

func TestQuotaRetention(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(1_700_000_000, 0)
	write := func(name string, mod time.Time) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		if err := os.Chtimes(path, mod, mod); err != nil {
			t.Fatalf("chtimes %s: %v", name, err)
		}
		return path
	}
	old := write("cam-a.mp4", now.Add(-2*time.Hour))
	fresh := write("cam-b.mp4", now.Add(-time.Minute))
	other := write("other-c.mp4", now.Add(-2*time.Hour))

	q := &quota{dir: dir, prefix: "cam", retention: time.Hour, now: func() time.Time { return now }, logf: t.Logf}
	if err := q.enforce("", filepath.Join(dir, "cam.state")); err != nil {
		t.Fatalf("enforce: %v", err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("old recording was not removed: %v", err)
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Fatalf("fresh recording was removed: %v", err)
	}
	if _, err := os.Stat(other); err != nil {
		t.Fatalf("unrelated stream was removed: %v", err)
	}
}

func TestTrimPendingAudio(t *testing.T) {
	r := &Recorder{
		spec: Spec{Audio: &TrackSource{Audio: testAudioCodec()}},
	}
	r.aparams = testAudioCodec()
	r.bufferAudio(sample{kind: audioTrack, dts: 0, data: []byte{1}})
	r.bufferAudio(sample{kind: audioTrack, dts: 2205, data: []byte{2}}) // 50ms
	r.bufferAudio(sample{kind: audioTrack, dts: 8820, data: []byte{3}}) // 200ms
	// 关键帧在 0.1s（9000/90kHz = 4410 个音频采样），之前的音频应被丢掉。
	r.trimPendingAudio(9000)
	if len(r.pendingAudio) != 1 || r.pendingAudio[0].dts != 8820 {
		t.Fatalf("pending = %+v, want only dts=8820", r.pendingAudio)
	}
	if r.pendingAudioBytes != 1 {
		t.Fatalf("pendingAudioBytes = %d, want 1", r.pendingAudioBytes)
	}
}

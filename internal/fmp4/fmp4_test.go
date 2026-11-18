package fmp4

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"
)

const (
	testSPSHex = "6764001eacd940a03db011000003000100000300320f162d9600"
	testPPSHex = "68ebecb22c"
)

// ---- 测试用的最小 MP4 盒解析器 ----

type testBox struct {
	typ      string
	start    int
	size     int
	payload  []byte
	children []testBox
}

func parseBoxes(t *testing.T, data []byte) []testBox {
	t.Helper()
	var out []testBox
	for off := 0; off+8 <= len(data); {
		size := int(binary.BigEndian.Uint32(data[off : off+4]))
		typ := string(data[off+4 : off+8])
		if size < 8 || off+size > len(data) {
			t.Fatalf("bad box %q size=%d at %d (len=%d)", typ, size, off, len(data))
		}
		bx := testBox{typ: typ, start: off, size: size, payload: data[off+8 : off+size]}
		if isTestContainer(typ) {
			skip := testChildSkip(typ)
			if skip > len(bx.payload) {
				t.Fatalf("box %q payload too short for child skip %d", typ, skip)
			}
			bx.children = parseBoxes(t, bx.payload[skip:])
		}
		out = append(out, bx)
		off += size
	}
	return out
}

func isTestContainer(typ string) bool {
	switch typ {
	case "moov", "trak", "mdia", "minf", "stbl", "mvex", "moof", "traf",
		"dinf", "edts", "udta", "stsd", "avc1", "mp4a", "dref":
		return true
	}
	return false
}

// testChildSkip 返回盒头之后、子盒之前要跳过的字节数。
func testChildSkip(typ string) int {
	switch typ {
	case "stsd", "dref":
		return 8 // version/flags + entry_count
	case "avc1":
		return 78 // SampleEntry(8) + VisualSampleEntry 固定字段
	case "mp4a":
		return 28 // SampleEntry(8) + AudioSampleEntry 固定字段
	}
	return 0
}

func mustFind(t *testing.T, boxes []testBox, typ string) *testBox {
	t.Helper()
	for i := range boxes {
		if boxes[i].typ == typ {
			return &boxes[i]
		}
	}
	var have []string
	for _, b := range boxes {
		have = append(have, b.typ)
	}
	t.Fatalf("box %q not found in %v", typ, have)
	return nil
}

func findAll(boxes []testBox, typ string) []testBox {
	var out []testBox
	for _, b := range boxes {
		if b.typ == typ {
			out = append(out, b)
		}
	}
	return out
}

func mustPath(t *testing.T, boxes []testBox, path ...string) *testBox {
	t.Helper()
	cur := boxes
	var bx *testBox
	for i, name := range path {
		bx = mustFind(t, cur, name)
		if i == len(path)-1 {
			return bx
		}
		cur = bx.children
	}
	return bx
}

func be32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func be24(b []byte) uint32 {
	return uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
}

func be64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// ---- 夹具 ----

func testVideoConfig() *VideoConfig {
	sps, err := hex.DecodeString(testSPSHex)
	if err != nil {
		panic(err)
	}
	pps, err := hex.DecodeString(testPPSHex)
	if err != nil {
		panic(err)
	}
	return &VideoConfig{Timescale: 90000, SPS: sps, PPS: pps, Width: 640, Height: 480}
}

func testAudioConfig() *AudioConfig {
	return &AudioConfig{
		Timescale:  44100,
		ASC:        []byte{0x12, 0x10},
		SampleRate: 44100,
		Channels:   2,
		MaxBitrate: 128000,
		AvgBitrate: 96000,
	}
}

// ---- init ----

func TestInitVideo(t *testing.T) {
	data, err := Init(Config{Video: testVideoConfig()})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	boxes := parseBoxes(t, data)
	if boxes[0].typ != "ftyp" {
		t.Fatalf("first box = %q, want ftyp", boxes[0].typ)
	}
	if got := string(boxes[0].payload[0:4]); got != "iso6" {
		t.Errorf("major brand = %q, want iso6", got)
	}
	if got := string(boxes[0].payload[8:20]); got != "iso6mp41cmfc" {
		t.Errorf("compatible brands = %q, want iso6mp41cmfc", got)
	}

	mvhd := mustPath(t, boxes, "moov", "mvhd")
	if got := be32(mvhd.payload[12:16]); got != movieTimescale {
		t.Errorf("mvhd timescale = %d, want %d", got, movieTimescale)
	}
	if got := be32(mvhd.payload[16:20]); got != 0 {
		t.Errorf("mvhd duration = %d, want 0", got)
	}
	if got := be32(mvhd.payload[len(mvhd.payload)-4:]); got != 2 {
		t.Errorf("mvhd next_track_ID = %d, want 2", got)
	}

	trak := mustPath(t, boxes, "moov", "trak")
	tkhd := mustFind(t, trak.children, "tkhd")
	if got := be32(tkhd.payload[12:16]); got != videoTrackID {
		t.Errorf("tkhd track_ID = %d, want %d", got, videoTrackID)
	}
	if got := be32(tkhd.payload[76:80]); got != 640<<16 {
		t.Errorf("tkhd width = %d, want %d", got, 640<<16)
	}
	if got := be32(tkhd.payload[80:84]); got != 480<<16 {
		t.Errorf("tkhd height = %d, want %d", got, 480<<16)
	}
	if got := binary.BigEndian.Uint16(tkhd.payload[36:38]); got != 0 {
		t.Errorf("video tkhd volume = %d, want 0", got)
	}

	mdhd := mustPath(t, boxes, "moov", "trak", "mdia", "mdhd")
	if got := be32(mdhd.payload[12:16]); got != 90000 {
		t.Errorf("mdhd timescale = %d, want 90000", got)
	}
	hdlr := mustPath(t, boxes, "moov", "trak", "mdia", "hdlr")
	if got := string(hdlr.payload[8:12]); got != "vide" {
		t.Errorf("handler = %q, want vide", got)
	}

	// 空样本表
	for _, typ := range []string{"stts", "stsc", "stsz", "stco"} {
		bx := mustPath(t, boxes, "moov", "trak", "mdia", "minf", "stbl", typ)
		if got := be32(bx.payload[4:8]); got != 0 {
			t.Errorf("%s entry_count = %d, want 0", typ, got)
		}
	}

	avcC := mustPath(t, boxes, "moov", "trak", "mdia", "minf", "stbl", "stsd", "avc1", "avcC")
	if avcC.payload[0] != 1 {
		t.Errorf("avcC version = %d, want 1", avcC.payload[0])
	}
	if avcC.payload[1] != 0x64 || avcC.payload[2] != 0x00 || avcC.payload[3] != 0x1e {
		t.Errorf("avcC profile/compat/level = %02x/%02x/%02x, want 64/00/1e",
			avcC.payload[1], avcC.payload[2], avcC.payload[3])
	}
	if got := avcC.payload[4] & 0x03; got != 3 {
		t.Errorf("lengthSizeMinusOne = %d, want 3", got)
	}
	if got := avcC.payload[5] & 0x1F; got != 1 {
		t.Errorf("numSPS = %d, want 1", got)
	}
	spsLen := int(binary.BigEndian.Uint16(avcC.payload[6:8]))
	sps := avcC.payload[8 : 8+spsLen]
	if got := hex.EncodeToString(sps); got != testSPSHex {
		t.Errorf("avcC SPS = %s, want %s", got, testSPSHex)
	}
	pos := 8 + spsLen
	if avcC.payload[pos] != 1 {
		t.Errorf("numPPS = %d, want 1", avcC.payload[pos])
	}
	ppsLen := int(binary.BigEndian.Uint16(avcC.payload[pos+1 : pos+3]))
	pps := avcC.payload[pos+3 : pos+3+ppsLen]
	if got := hex.EncodeToString(pps); got != testPPSHex {
		t.Errorf("avcC PPS = %s, want %s", got, testPPSHex)
	}

	trex := mustPath(t, boxes, "moov", "mvex", "trex")
	if got := be32(trex.payload[4:8]); got != videoTrackID {
		t.Errorf("trex track_ID = %d, want %d", got, videoTrackID)
	}
	if got := be32(trex.payload[8:12]); got != 1 {
		t.Errorf("trex sample_description_index = %d, want 1", got)
	}
}

func TestInitAudio(t *testing.T) {
	data, err := Init(Config{Audio: testAudioConfig()})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	boxes := parseBoxes(t, data)

	trak := mustPath(t, boxes, "moov", "trak")
	tkhd := mustFind(t, trak.children, "tkhd")
	if got := be32(tkhd.payload[12:16]); got != audioTrackID {
		t.Errorf("tkhd track_ID = %d, want %d", got, audioTrackID)
	}
	if got := binary.BigEndian.Uint16(tkhd.payload[36:38]); got != 0x0100 {
		t.Errorf("audio tkhd volume = %#x, want 0x0100", got)
	}
	if got := be32(tkhd.payload[76:80]); got != 0 {
		t.Errorf("audio tkhd width = %d, want 0", got)
	}

	mdhd := mustPath(t, boxes, "moov", "trak", "mdia", "mdhd")
	if got := be32(mdhd.payload[12:16]); got != 44100 {
		t.Errorf("mdhd timescale = %d, want 44100", got)
	}
	hdlr := mustPath(t, boxes, "moov", "trak", "mdia", "hdlr")
	if got := string(hdlr.payload[8:12]); got != "soun" {
		t.Errorf("handler = %q, want soun", got)
	}

	mp4a := mustPath(t, boxes, "moov", "trak", "mdia", "minf", "stbl", "stsd", "mp4a")
	if got := binary.BigEndian.Uint16(mp4a.payload[16:18]); got != 2 {
		t.Errorf("mp4a channels = %d, want 2", got)
	}
	if got := binary.BigEndian.Uint16(mp4a.payload[18:20]); got != 16 {
		t.Errorf("mp4a samplesize = %d, want 16", got)
	}
	if got := be32(mp4a.payload[24:28]); got != 44100<<16 {
		t.Errorf("mp4a samplerate = %#x, want %#x", got, 44100<<16)
	}

	esds := mustFind(t, mp4a.children, "esds")
	tag, es, _ := readDescriptor(t, esds.payload, 4)
	if tag != 0x03 {
		t.Fatalf("esds first descriptor tag = %#x, want 0x03", tag)
	}
	tag, dec, _ := readDescriptor(t, es, 3)
	if tag != 0x04 {
		t.Fatalf("ES descriptor tag = %#x, want 0x04", tag)
	}
	if dec[0] != 0x40 || dec[1] != 0x15 {
		t.Errorf("decoder config = %#x/%#x, want 0x40/0x15", dec[0], dec[1])
	}
	if got := be32(dec[5:9]); got != 128000 {
		t.Errorf("max bitrate = %d, want 128000", got)
	}
	if got := be32(dec[9:13]); got != 96000 {
		t.Errorf("avg bitrate = %d, want 96000", got)
	}
	tag, asc, _ := readDescriptor(t, dec, 13)
	if tag != 0x05 {
		t.Fatalf("DecoderSpecificInfo tag = %#x, want 0x05", tag)
	}
	if !bytes.Equal(asc, []byte{0x12, 0x10}) {
		t.Errorf("ASC = % x, want 12 10", asc)
	}
}

func TestInitBothTracks(t *testing.T) {
	data, err := Init(Config{Video: testVideoConfig(), Audio: testAudioConfig()})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	boxes := parseBoxes(t, data)
	moov := mustFind(t, boxes, "moov")
	traks := findAll(moov.children, "trak")
	if len(traks) != 2 {
		t.Fatalf("trak count = %d, want 2", len(traks))
	}
	trexes := findAll(mustFind(t, moov.children, "mvex").children, "trex")
	if len(trexes) != 2 {
		t.Fatalf("trex count = %d, want 2", len(trexes))
	}
	if got := be32(trexes[0].payload[4:8]); got != videoTrackID {
		t.Errorf("trex[0] track = %d, want %d", got, videoTrackID)
	}
	if got := be32(trexes[1].payload[4:8]); got != audioTrackID {
		t.Errorf("trex[1] track = %d, want %d", got, audioTrackID)
	}
	mvhd := mustFind(t, moov.children, "mvhd")
	if got := be32(mvhd.payload[len(mvhd.payload)-4:]); got != 3 {
		t.Errorf("next_track_ID = %d, want 3", got)
	}
}

func TestInitErrors(t *testing.T) {
	sps, _ := hex.DecodeString(testSPSHex)
	pps, _ := hex.DecodeString(testPPSHex)
	tests := []struct {
		name string
		cfg  Config
	}{
		{"no tracks", Config{}},
		{"video no timescale", Config{Video: &VideoConfig{SPS: sps, PPS: pps}}},
		{"video no pps", Config{Video: &VideoConfig{Timescale: 90000, SPS: sps}}},
		{"video short sps", Config{Video: &VideoConfig{Timescale: 90000, SPS: []byte{0x67}, PPS: pps}}},
		{"video bad size", Config{Video: &VideoConfig{Timescale: 90000, SPS: sps, PPS: pps, Width: -1}}},
		{"audio no timescale", Config{Audio: &AudioConfig{ASC: []byte{0x12, 0x10}, Channels: 2}}},
		{"audio short asc", Config{Audio: &AudioConfig{Timescale: 44100, ASC: []byte{0x12}, Channels: 2}}},
		{"audio no channels", Config{Audio: &AudioConfig{Timescale: 44100, ASC: []byte{0x12, 0x10}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Init(tt.cfg); err == nil {
				t.Fatal("Init returned nil error")
			}
		})
	}
}

// ---- fragment ----

func videoSamples() []Sample {
	return []Sample{
		{Data: []byte{1, 2, 3, 4, 5}, DTS: 0, Duration: 3600, CTS: 0, Keyframe: true},
		{Data: []byte{6, 7, 8, 9, 10, 11}, DTS: 3600, Duration: 3600, CTS: -3600, Keyframe: false},
		{Data: []byte{12, 13, 14, 15, 16, 17, 18}, DTS: 7200, Duration: 3600, CTS: 3600, Keyframe: true},
	}
}

func TestFragmentVideo(t *testing.T) {
	samples := videoSamples()
	data, err := Fragment(42, TrackFragment{TrackID: videoTrackID, Samples: samples, WriteCTS: true})
	if err != nil {
		t.Fatalf("Fragment: %v", err)
	}
	boxes := parseBoxes(t, data)
	if len(boxes) != 2 || boxes[0].typ != "moof" || boxes[1].typ != "mdat" {
		t.Fatalf("boxes = %v, want moof + mdat", boxTypes(boxes))
	}
	moof, mdat := boxes[0], boxes[1]

	mfhd := mustFind(t, moof.children, "mfhd")
	if got := be32(mfhd.payload[4:8]); got != 42 {
		t.Errorf("mfhd sequence = %d, want 42", got)
	}

	traf := mustFind(t, moof.children, "traf")
	tfhd := mustFind(t, traf.children, "tfhd")
	if got := be24(tfhd.payload[1:4]); got != 0x020002 {
		t.Errorf("tfhd flags = %#x, want 0x020002", got)
	}
	if got := be32(tfhd.payload[4:8]); got != videoTrackID {
		t.Errorf("tfhd track = %d, want %d", got, videoTrackID)
	}
	if got := be32(tfhd.payload[8:12]); got != 1 {
		t.Errorf("tfhd sample_description_index = %d, want 1", got)
	}

	tfdt := mustFind(t, traf.children, "tfdt")
	if tfdt.payload[0] != 1 {
		t.Errorf("tfdt version = %d, want 1", tfdt.payload[0])
	}
	if got := be64(tfdt.payload[4:12]); got != 0 {
		t.Errorf("tfdt base = %d, want 0", got)
	}

	trun := mustFind(t, traf.children, "trun")
	if trun.payload[0] != 1 {
		t.Errorf("trun version = %d, want 1", trun.payload[0])
	}
	if got := be24(trun.payload[1:4]); got != 0x000F01 {
		t.Errorf("trun flags = %#x, want 0x000F01", got)
	}
	if got := be32(trun.payload[4:8]); got != uint32(len(samples)) {
		t.Errorf("trun sample_count = %d, want %d", got, len(samples))
	}
	if got := be32(trun.payload[8:12]); got != uint32(moof.size+8) {
		t.Errorf("trun data_offset = %d, want %d (moof size %d + mdat header)",
			got, moof.size+8, moof.size)
	}

	wantFlags := []uint32{0x02000000, 0x01010000, 0x02000000}
	for i, s := range samples {
		off := 12 + i*16
		if got := be32(trun.payload[off : off+4]); got != s.Duration {
			t.Errorf("sample %d duration = %d, want %d", i, got, s.Duration)
		}
		if got := be32(trun.payload[off+4 : off+8]); got != uint32(len(s.Data)) {
			t.Errorf("sample %d size = %d, want %d", i, got, len(s.Data))
		}
		if got := be32(trun.payload[off+8 : off+12]); got != wantFlags[i] {
			t.Errorf("sample %d flags = %#x, want %#x", i, got, wantFlags[i])
		}
		if got := int32(be32(trun.payload[off+12 : off+16])); got != s.CTS {
			t.Errorf("sample %d CTS = %d, want %d", i, got, s.CTS)
		}
	}

	var want []byte
	for _, s := range samples {
		want = append(want, s.Data...)
	}
	if !bytes.Equal(mdat.payload, want) {
		t.Errorf("mdat payload = % x, want % x", mdat.payload, want)
	}
	if mdat.start != moof.size {
		t.Errorf("mdat starts at %d, want %d", mdat.start, moof.size)
	}
}

func TestFragmentNonZeroTFDT(t *testing.T) {
	data, err := Fragment(9, TrackFragment{
		TrackID:  videoTrackID,
		WriteCTS: true,
		Samples:  []Sample{{Data: []byte{1}, DTS: 9000, Duration: 3600, Keyframe: true}},
	})
	if err != nil {
		t.Fatalf("Fragment: %v", err)
	}
	tfdt := mustPath(t, parseBoxes(t, data), "moof", "traf", "tfdt")
	if got := be64(tfdt.payload[4:12]); got != 9000 {
		t.Errorf("tfdt base = %d, want 9000", got)
	}
}

func TestFragmentMultiTrack(t *testing.T) {
	video := []Sample{{Data: []byte{1, 2, 3}, DTS: 0, Duration: 3600, CTS: 0, Keyframe: true}}
	audio := []Sample{
		{Data: []byte{4, 5}, DTS: 0, Duration: 1024, Keyframe: true},
		{Data: []byte{6}, DTS: 1024, Duration: 1024, Keyframe: true},
	}
	data, err := Fragment(7,
		TrackFragment{TrackID: videoTrackID, Samples: video, WriteCTS: true},
		TrackFragment{TrackID: audioTrackID, Samples: audio},
	)
	if err != nil {
		t.Fatalf("Fragment: %v", err)
	}
	boxes := parseBoxes(t, data)
	moof, mdat := boxes[0], boxes[1]
	trafs := findAll(moof.children, "traf")
	if len(trafs) != 2 {
		t.Fatalf("traf count = %d, want 2", len(trafs))
	}

	trun0 := mustFind(t, trafs[0].children, "trun")
	trun1 := mustFind(t, trafs[1].children, "trun")
	if got, want := be32(trun0.payload[8:12]), uint32(moof.size+8); got != want {
		t.Errorf("traf0 data_offset = %d, want %d", got, want)
	}
	if got, want := be32(trun1.payload[8:12]), uint32(moof.size+8+len(video[0].Data)); got != want {
		t.Errorf("traf1 data_offset = %d, want %d", got, want)
	}

	want := append(append([]byte{}, video[0].Data...), audio[0].Data...)
	want = append(want, audio[1].Data...)
	if !bytes.Equal(mdat.payload, want) {
		t.Errorf("mdat payload = % x, want % x", mdat.payload, want)
	}

	// 音频没有 CTS：version 0 + 0x000701，且每个样本 12 字节。
	if trun1.payload[0] != 0 {
		t.Errorf("audio trun version = %d, want 0", trun1.payload[0])
	}
	if got := be24(trun1.payload[1:4]); got != 0x000701 {
		t.Errorf("audio trun flags = %#x, want 0x000701", got)
	}
	if got, want := len(trun1.payload), 12+len(audio)*12; got != want {
		t.Errorf("audio trun payload len = %d, want %d", got, want)
	}
}

func TestFragmentStypPrefix(t *testing.T) {
	frag, err := Fragment(1, TrackFragment{
		TrackID:  videoTrackID,
		Samples:  []Sample{{Data: []byte{1}, DTS: 0, Duration: 3600, Keyframe: true}},
		WriteCTS: true,
	})
	if err != nil {
		t.Fatalf("Fragment: %v", err)
	}
	segment := append(Styp(), frag...)
	boxes := parseBoxes(t, segment)
	if len(boxes) != 3 || boxes[0].typ != "styp" || boxes[1].typ != "moof" || boxes[2].typ != "mdat" {
		t.Fatalf("boxes = %v, want styp + moof + mdat", boxTypes(boxes))
	}
	if got := string(boxes[0].payload[0:4]); got != "msdh" {
		t.Errorf("styp major = %q, want msdh", got)
	}
	if got := string(boxes[0].payload[8:16]); got != "msdhmsix" {
		t.Errorf("styp compatible = %q, want msdhmsix", got)
	}
	// data_offset 相对 moof 起点，不是文件起点。
	trun := mustPath(t, boxes[1:2], "moof", "traf", "trun")
	if got, want := be32(trun.payload[8:12]), uint32(boxes[1].size+8); got != want {
		t.Errorf("data_offset = %d, want %d (moof-relative)", got, want)
	}
}

func TestFragmentErrors(t *testing.T) {
	sample := func() Sample {
		return Sample{Data: []byte{1, 2, 3}, DTS: 0, Duration: 3600, Keyframe: true}
	}
	tests := []struct {
		name  string
		frags []TrackFragment
	}{
		{"no tracks", nil},
		{"track id zero", []TrackFragment{{TrackID: 0, Samples: []Sample{sample()}}}},
		{"no samples", []TrackFragment{{TrackID: 1}}},
		{"empty data", []TrackFragment{{TrackID: 1, Samples: []Sample{{Duration: 3600}}}}},
		{"zero duration", []TrackFragment{{TrackID: 1, Samples: []Sample{{Data: []byte{1}}}}}},
		{
			"non-contiguous DTS",
			[]TrackFragment{{TrackID: 1, Samples: []Sample{
				sample(),
				{Data: []byte{4}, DTS: 7200, Duration: 3600},
			}}},
		},
		{
			"negative DTS",
			[]TrackFragment{{TrackID: 1, Samples: []Sample{
				{Data: []byte{1}, DTS: -1, Duration: 3600},
			}}},
		},
		{
			"CTS without WriteCTS",
			[]TrackFragment{{TrackID: 1, Samples: []Sample{
				{Data: []byte{1}, DTS: 0, Duration: 3600, CTS: -1},
			}}},
		},
		{
			"duplicate track",
			[]TrackFragment{
				{TrackID: 1, Samples: []Sample{sample()}},
				{TrackID: 1, Samples: []Sample{sample()}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Fragment(1, tt.frags...); err == nil {
				t.Fatal("Fragment returned nil error")
			}
		})
	}
}

func boxTypes(boxes []testBox) []string {
	out := make([]string, len(boxes))
	for i, b := range boxes {
		out[i] = b.typ
	}
	return out
}

// ---- esds descriptor 解析 ----

func readDescriptor(t *testing.T, b []byte, pos int) (tag byte, payload []byte, next int) {
	t.Helper()
	if pos >= len(b) {
		t.Fatalf("descriptor pos %d out of range (len %d)", pos, len(b))
	}
	tag = b[pos]
	pos++
	n, pos, ok := readDescriptorLen(b, pos)
	if !ok || pos+n > len(b) {
		t.Fatalf("bad descriptor length %d at pos %d (len %d)", n, pos, len(b))
	}
	return tag, b[pos : pos+n], pos + n
}

func readDescriptorLen(b []byte, pos int) (n int, next int, ok bool) {
	if pos >= len(b) {
		return 0, pos, false
	}
	first := b[pos]
	pos++
	if first&0x80 == 0 {
		return int(first), pos, true
	}
	num := int(first & 0x7F)
	if num == 0 || num > 4 || pos+num > len(b) {
		return 0, pos, false
	}
	for i := 0; i < num; i++ {
		n = n<<8 | int(b[pos+i])
	}
	return n, pos + num, true
}

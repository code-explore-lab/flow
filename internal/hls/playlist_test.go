package hls

import (
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"flow/internal/media"
)

func pdtAt(sec int) time.Time {
	return time.Date(2026, 11, 18, 8, 30, sec, 0, time.UTC)
}

func TestMediaPlaylistLLGolden(t *testing.T) {
	p := &MediaPlaylist{
		TargetDuration: 4,
		PartTarget:     0.52,
		CanBlockReload: true,
		Segments: []SegmentInfo{{
			MSN:      12,
			MapURI:   "video/init.mp4",
			Duration: 4,
			PDT:      pdtAt(0),
			URI:      "video/seg-12.m4s",
		}},
		Open: &OpenSegmentInfo{
			MSN:    13,
			MapURI: "video/init.mp4",
			PDT:    pdtAt(4),
			Parts: []PartInfo{
				{MSN: 13, Index: 0, Duration: 0.48, Independent: true, URI: "video/part-13-0.m4s"},
				{MSN: 13, Index: 1, Duration: 0.52, URI: "video/part-13-1.m4s"},
			},
			NextPartURI: "video/part-13-2.m4s",
		},
		Rendition: &RenditionReport{URI: "audio.m3u8", LastMSN: 13, LastPart: 3},
	}
	got, err := p.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	want := `#EXTM3U
#EXT-X-VERSION:9
#EXT-X-TARGETDURATION:4
#EXT-X-MEDIA-SEQUENCE:12
#EXT-X-DISCONTINUITY-SEQUENCE:0
#EXT-X-PART-INF:PART-TARGET=0.520
#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,HOLD-BACK=12.000,PART-HOLD-BACK=1.560
#EXT-X-MAP:URI="video/init.mp4"
#EXT-X-PROGRAM-DATE-TIME:2026-11-18T08:30:00.000Z
#EXTINF:4.000,
video/seg-12.m4s
#EXT-X-PROGRAM-DATE-TIME:2026-11-18T08:30:04.000Z
#EXT-X-PART:DURATION=0.480,URI="video/part-13-0.m4s",INDEPENDENT=YES
#EXT-X-PART:DURATION=0.520,URI="video/part-13-1.m4s"
#EXT-X-PRELOAD-HINT:TYPE=PART,URI="video/part-13-2.m4s"
#EXT-X-RENDITION-REPORT:URI="audio.m3u8",LAST-MSN=13,LAST-PART=3
`
	if string(got) != want {
		t.Fatalf("playlist mismatch\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
	parsed := parseMediaPlaylist(t, got)
	if parsed.version != 9 || parsed.target != 4 {
		t.Errorf("version/target = %d/%d, want 9/4", parsed.version, parsed.target)
	}
	if parsed.mediaSeq != 12 || parsed.discSeq != 0 {
		t.Errorf("sequences = %d/%d, want 12/0", parsed.mediaSeq, parsed.discSeq)
	}
	if parsed.partTarget != 0.52 {
		t.Errorf("part target = %f, want 0.52", parsed.partTarget)
	}
	if len(parsed.entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(parsed.entries))
	}
	e := parsed.entries[0]
	if e.uri != "video/seg-12.m4s" || e.mapURI != "video/init.mp4" || e.duration != 4 ||
		e.pdt != "2026-11-18T08:30:00.000Z" {
		t.Errorf("entry = %+v", e)
	}
	if len(parsed.parts) != 2 {
		t.Fatalf("parts = %d, want 2", len(parsed.parts))
	}
	if !parsed.parts[0].independent || parsed.parts[0].duration != 0.48 {
		t.Errorf("part 0 = %+v", parsed.parts[0])
	}
	if parsed.preload != "video/part-13-2.m4s" {
		t.Errorf("preload = %q", parsed.preload)
	}
	if parsed.rendition["LAST-MSN"] != "13" || parsed.rendition["LAST-PART"] != "3" {
		t.Errorf("rendition = %v", parsed.rendition)
	}
	checkParsedMedia(t, parsed)
}

func TestMediaPlaylistClassicGolden(t *testing.T) {
	p := &MediaPlaylist{
		TargetDuration: 4,
		Segments: []SegmentInfo{
			{MSN: 0, MapURI: "video/init.mp4", Duration: 4, PDT: pdtAt(0), URI: "video/seg-0.m4s"},
			{MSN: 1, MapURI: "video/init.mp4", Duration: 4, PDT: pdtAt(4), URI: "video/seg-1.m4s"},
		},
		EndList: true,
	}
	got, err := p.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	want := `#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:4
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-DISCONTINUITY-SEQUENCE:0
#EXT-X-MAP:URI="video/init.mp4"
#EXT-X-PROGRAM-DATE-TIME:2026-11-18T08:30:00.000Z
#EXTINF:4.000,
video/seg-0.m4s
#EXT-X-PROGRAM-DATE-TIME:2026-11-18T08:30:04.000Z
#EXTINF:4.000,
video/seg-1.m4s
#EXT-X-ENDLIST
`
	if string(got) != want {
		t.Fatalf("playlist mismatch\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
	parsed := parseMediaPlaylist(t, got)
	if parsed.version != 7 || parsed.partTarget != 0 || !parsed.endList {
		t.Errorf("version/part/end = %d/%f/%v", parsed.version, parsed.partTarget, parsed.endList)
	}
	if len(parsed.parts) != 0 || parsed.preload != "" {
		t.Errorf("classic playlist has parts/preload: %+v %q", parsed.parts, parsed.preload)
	}
	checkParsedMedia(t, parsed)
}

func TestMediaPlaylistClassicBlockingReload(t *testing.T) {
	p := &MediaPlaylist{
		TargetDuration: 4,
		CanBlockReload: true,
		Segments: []SegmentInfo{
			{MSN: 0, MapURI: "video/init.mp4", Duration: 4, URI: "video/seg-0.m4s"},
		},
	}
	got, err := p.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	s := string(got)
	if !strings.Contains(s, "#EXT-X-VERSION:9\n") {
		t.Error("classic + blocking should use version 9")
	}
	if !strings.Contains(s, "#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,HOLD-BACK=12.000\n") {
		t.Errorf("missing server control:\n%s", s)
	}
	if strings.Contains(s, "PART-HOLD-BACK") || strings.Contains(s, "EXT-X-PART:") {
		t.Error("classic playlist should not contain part tags")
	}
}

func TestMediaPlaylistDiscontinuityAndMapChange(t *testing.T) {
	p := &MediaPlaylist{
		TargetDuration: 4,
		PartTarget:     0.52,
		Segments: []SegmentInfo{
			{MSN: 0, MapURI: "video/init.mp4", Duration: 4, PDT: pdtAt(0), URI: "video/seg-0.m4s"},
			{MSN: 1, MapURI: "video/init.mp4", Duration: 4, PDT: pdtAt(4), URI: "video/seg-1.m4s"},
			{MSN: 2, DiscSeq: 1, MapURI: "video/init-1.mp4", Duration: 4, PDT: pdtAt(8), URI: "video/seg-2.m4s"},
		},
		Open: &OpenSegmentInfo{
			MSN:     3,
			DiscSeq: 1,
			MapURI:  "video/init-1.mp4",
			PDT:     pdtAt(12),
			Parts:   []PartInfo{{MSN: 3, Index: 0, Duration: 0.52, Independent: true, URI: "video/part-3-0.m4s"}},
		},
	}
	got, err := p.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	s := string(got)
	if strings.Count(s, "#EXT-X-DISCONTINUITY\n") != 1 {
		t.Errorf("discontinuity tags = %d, want 1\n%s", strings.Count(s, "#EXT-X-DISCONTINUITY\n"), s)
	}
	if strings.Count(s, "#EXT-X-MAP:") != 2 {
		t.Errorf("map tags = %d, want 2\n%s", strings.Count(s, "#EXT-X-MAP:"), s)
	}
	if !strings.Contains(s, "#EXT-X-DISCONTINUITY\n#EXT-X-MAP:URI=\"video/init-1.mp4\"\n") {
		t.Errorf("discontinuity/map order wrong:\n%s", s)
	}
	parsed := parseMediaPlaylist(t, got)
	if len(parsed.entries) != 3 || !parsed.entries[2].discontinuity || parsed.entries[2].mapURI != "video/init-1.mp4" {
		t.Errorf("entries = %+v", parsed.entries)
	}
	checkParsedMedia(t, parsed)
}

func TestMediaPlaylistWindowStartsWithDiscontinuity(t *testing.T) {
	p := &MediaPlaylist{
		TargetDuration: 4,
		Segments: []SegmentInfo{
			{MSN: 5, DiscSeq: 1, Discontinuity: true, MapURI: "video/init-1.mp4", Duration: 4, PDT: pdtAt(0), URI: "video/seg-5.m4s"},
		},
	}
	got, err := p.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	s := string(got)
	if !strings.Contains(s, "#EXT-X-DISCONTINUITY-SEQUENCE:1\n") {
		t.Errorf("missing disc sequence:\n%s", s)
	}
	if strings.Contains(s, "#EXT-X-DISCONTINUITY\n") {
		t.Errorf("first segment should not carry a discontinuity tag:\n%s", s)
	}
	parsed := parseMediaPlaylist(t, got)
	if parsed.discSeq != 1 || parsed.mediaSeq != 5 {
		t.Errorf("sequences = %d/%d, want 1/5", parsed.discSeq, parsed.mediaSeq)
	}
}

func TestMediaPlaylistToken(t *testing.T) {
	p := &MediaPlaylist{
		TargetDuration: 4,
		PartTarget:     0.52,
		Token:          "a b&c",
		Segments: []SegmentInfo{{
			MSN: 12, MapURI: "video/init.mp4", Duration: 4, URI: "video/seg-12.m4s",
		}},
		Open: &OpenSegmentInfo{
			MSN: 13, MapURI: "video/init.mp4",
			Parts:       []PartInfo{{MSN: 13, Index: 0, Duration: 0.52, Independent: true, URI: "video/part-13-0.m4s"}},
			NextPartURI: "video/part-13-1.m4s",
		},
		Rendition: &RenditionReport{URI: "audio.m3u8", LastMSN: 13, LastPart: 0},
	}
	got, err := p.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	s := string(got)
	const tok = "?token=a+b%26c"
	for _, want := range []string{
		`#EXT-X-MAP:URI="video/init.mp4` + tok + `"`,
		"video/seg-12.m4s" + tok,
		`URI="video/part-13-0.m4s` + tok + `"`,
		`URI="video/part-13-1.m4s` + tok + `"`,
		`URI="audio.m3u8` + tok + `"`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %q in:\n%s", want, s)
		}
	}
}

func TestMediaPlaylistEmpty(t *testing.T) {
	p := &MediaPlaylist{TargetDuration: 4}
	got, err := p.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	want := `#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:4
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-DISCONTINUITY-SEQUENCE:0
`
	if string(got) != want {
		t.Fatalf("empty playlist mismatch:\n%s", got)
	}

	ll := &MediaPlaylist{TargetDuration: 4, PartTarget: 0.52, CanBlockReload: true}
	got, err = ll.Generate()
	if err != nil {
		t.Fatalf("Generate LL empty: %v", err)
	}
	s := string(got)
	if !strings.Contains(s, "#EXT-X-VERSION:9\n") ||
		!strings.Contains(s, "#EXT-X-PART-INF:PART-TARGET=0.520\n") ||
		!strings.Contains(s, "#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,HOLD-BACK=12.000,PART-HOLD-BACK=1.560\n") {
		t.Errorf("LL empty playlist tags missing:\n%s", s)
	}
}

func TestMediaPlaylistValidation(t *testing.T) {
	base := func() *MediaPlaylist {
		return &MediaPlaylist{
			TargetDuration: 4,
			PartTarget:     0.52,
			Segments: []SegmentInfo{{
				MSN: 0, MapURI: "video/init.mp4", Duration: 4, URI: "video/seg-0.m4s",
			}},
			Open: &OpenSegmentInfo{
				MSN: 1, MapURI: "video/init.mp4",
				Parts: []PartInfo{{MSN: 1, Index: 0, Duration: 0.52, Independent: true, URI: "video/part-1-0.m4s"}},
			},
		}
	}
	tests := []struct {
		name   string
		mutate func(*MediaPlaylist)
	}{
		{"target zero", func(p *MediaPlaylist) { p.TargetDuration = 0 }},
		{"negative part", func(p *MediaPlaylist) { p.PartTarget = -1 }},
		{"endlist with open", func(p *MediaPlaylist) { p.EndList = true }},
		{"segment too long", func(p *MediaPlaylist) { p.Segments[0].Duration = 4.6 }},
		{"segment sequence gap", func(p *MediaPlaylist) {
			p.Segments = append(p.Segments, SegmentInfo{MSN: 2, Duration: 4, URI: "video/seg-2.m4s"})
		}},
		{"disc sequence jump", func(p *MediaPlaylist) {
			p.Segments = append(p.Segments, SegmentInfo{MSN: 1, DiscSeq: 2, Duration: 4, URI: "video/seg-1.m4s"})
		}},
		{"part too long", func(p *MediaPlaylist) { p.Open.Parts[0].Duration = 0.6 }},
		{"part below 85%", func(p *MediaPlaylist) {
			p.Open.Parts[0].Independent = false
			p.Open.Parts[0].Duration = 0.4
			p.Open.Parts = append(p.Open.Parts, PartInfo{
				MSN: 1, Index: 1, Duration: 0.52, Independent: true, URI: "video/part-1-1.m4s",
			})
		}},
		{"part index mismatch", func(p *MediaPlaylist) { p.Open.Parts[0].Index = 3 }},
		{"part msn mismatch", func(p *MediaPlaylist) { p.Open.Parts[0].MSN = 9 }},
		{"parts without target", func(p *MediaPlaylist) { p.PartTarget = 0 }},
		{"open sequence gap", func(p *MediaPlaylist) { p.Open.MSN = 3 }},
		{"invalid segment uri", func(p *MediaPlaylist) { p.Segments[0].URI = "video/bad\"uri" }},
		{"invalid map uri", func(p *MediaPlaylist) { p.Segments[0].MapURI = "video/init\n.mp4" }},
		{"invalid part uri", func(p *MediaPlaylist) { p.Open.Parts[0].URI = "video/p\"1" }},
		{"invalid hint uri", func(p *MediaPlaylist) { p.Open.NextPartURI = "bad\nhint" }},
		{"invalid rendition", func(p *MediaPlaylist) {
			p.Rendition = &RenditionReport{URI: "audio.m3u8", LastMSN: 1, LastPart: -2}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := base()
			tt.mutate(p)
			if _, err := p.Generate(); err == nil {
				t.Fatal("Generate returned nil error")
			}
		})
	}
}

func TestMediaPlaylistShortLastPartAllowed(t *testing.T) {
	// 打开父段的最后一片可能是即将结束父段的 final part，85% 规则豁免。
	p := &MediaPlaylist{
		TargetDuration: 4,
		PartTarget:     0.52,
		Segments: []SegmentInfo{{
			MSN: 0, MapURI: "video/init.mp4", Duration: 4, URI: "video/seg-0.m4s",
		}},
		Open: &OpenSegmentInfo{
			MSN: 1, MapURI: "video/init.mp4",
			Parts: []PartInfo{
				{MSN: 1, Index: 0, Duration: 0.52, Independent: true, URI: "video/part-1-0.m4s"},
				{MSN: 1, Index: 1, Duration: 0.10, URI: "video/part-1-1.m4s"},
			},
		},
	}
	if _, err := p.Generate(); err != nil {
		t.Fatalf("short final part should be allowed: %v", err)
	}
}

func TestMasterCodecsFromSPSAndASC(t *testing.T) {
	sps, err := hex.DecodeString("6764001eacd940a03db011000003000100000300320f162d9600")
	if err != nil {
		t.Fatalf("decode SPS: %v", err)
	}
	info, err := media.ParseSPS(sps)
	if err != nil {
		t.Fatalf("ParseSPS: %v", err)
	}
	asc := []byte{0x12, 0x10} // AAC-LC, 44100, stereo
	aot := asc[0] >> 3
	codecs := fmt.Sprintf("%s,mp4a.40.%d", info.CodecString(), aot)
	if codecs != "avc1.64001e,mp4a.40.2" {
		t.Fatalf("codecs = %q, want avc1.64001e,mp4a.40.2", codecs)
	}
	m := &MasterPlaylist{
		Version:             9,
		IndependentSegments: true,
		Audio: &AudioRendition{
			GroupID: "aud", Name: "audio", URI: "audio.m3u8", Channels: 2, Default: true,
		},
		Variants: []Variant{{
			URI: "video.m3u8", Bandwidth: 1000000, Resolution: fmt.Sprintf("%dx%d", info.Width, info.Height),
			Codecs: codecs, AudioGroup: "aud",
		}},
	}
	got, err := m.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	s := string(got)
	if !strings.Contains(s, `CODECS="avc1.64001e,mp4a.40.2"`) {
		t.Errorf("master missing derived codecs:\n%s", s)
	}
	if !strings.Contains(s, "RESOLUTION=640x480") {
		t.Errorf("master missing resolution:\n%s", s)
	}
}

func TestMasterPlaylistGolden(t *testing.T) {
	m := &MasterPlaylist{
		Version:             9,
		IndependentSegments: true,
		Audio: &AudioRendition{
			GroupID: "aud", Name: "audio", URI: "audio.m3u8", Channels: 2,
			Default: true, Autoselect: true,
		},
		Variants: []Variant{{
			URI:        "video.m3u8",
			Bandwidth:  1200000,
			Resolution: "640x480",
			Codecs:     "avc1.64001e,mp4a.40.2",
			AudioGroup: "aud",
		}},
	}
	got, err := m.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	want := `#EXTM3U
#EXT-X-VERSION:9
#EXT-X-INDEPENDENT-SEGMENTS
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud",NAME="audio",DEFAULT=YES,AUTOSELECT=YES,CHANNELS="2",URI="audio.m3u8"
#EXT-X-STREAM-INF:BANDWIDTH=1200000,RESOLUTION=640x480,CODECS="avc1.64001e,mp4a.40.2",AUDIO="aud"
video.m3u8
`
	if string(got) != want {
		t.Fatalf("master mismatch\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
	parsed := parseMasterPlaylist(t, got)
	if parsed.version != 9 || !parsed.independent {
		t.Errorf("version/independent = %d/%v", parsed.version, parsed.independent)
	}
	if parsed.media["TYPE"] != "AUDIO" || parsed.media["GROUP-ID"] != "aud" ||
		parsed.media["CHANNELS"] != "2" || parsed.media["URI"] != "audio.m3u8" {
		t.Errorf("media = %v", parsed.media)
	}
	if len(parsed.variants) != 1 {
		t.Fatalf("variants = %d, want 1", len(parsed.variants))
	}
	v := parsed.variants[0]
	if v.uri != "video.m3u8" || v.attrs["BANDWIDTH"] != "1200000" ||
		v.attrs["RESOLUTION"] != "640x480" || v.attrs["AUDIO"] != "aud" ||
		v.attrs["CODECS"] != "avc1.64001e,mp4a.40.2" {
		t.Errorf("variant = %+v", v)
	}
}

func TestMasterPlaylistVideoOnly(t *testing.T) {
	m := &MasterPlaylist{
		Version: 7,
		Variants: []Variant{{
			URI: "video.m3u8", Bandwidth: 800000, Resolution: "1280x720", FrameRate: 25,
			Codecs: "avc1.64001f",
		}},
	}
	got, err := m.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	s := string(got)
	if strings.Contains(s, "EXT-X-MEDIA") {
		t.Error("video-only master should not have EXT-X-MEDIA")
	}
	if !strings.Contains(s, "#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=1280x720,FRAME-RATE=25.000,CODECS=\"avc1.64001f\"\n") {
		t.Errorf("variant line wrong:\n%s", s)
	}
}

func TestMasterPlaylistMultipleAudios(t *testing.T) {
	m := &MasterPlaylist{
		Version: 9,
		Audios: []AudioRendition{
			{GroupID: "aud0", Name: "audio", URI: "/hls/a/audio.m3u8", Channels: 2, Default: true, Autoselect: true},
			{GroupID: "aud1", Name: "audio", URI: "/hls/b/audio.m3u8", Channels: 2, Autoselect: true},
		},
		Variants: []Variant{
			{URI: "/hls/a/video.m3u8", Bandwidth: 500000, Resolution: "640x360", AudioGroup: "aud0"},
			{URI: "/hls/b/video.m3u8", Bandwidth: 2000000, Resolution: "1280x720", AudioGroup: "aud1"},
		},
	}
	got, err := m.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	s := string(got)
	for _, want := range []string{
		`GROUP-ID="aud0"`, `GROUP-ID="aud1"`,
		`DEFAULT=YES,AUTOSELECT=YES`, `DEFAULT=NO,AUTOSELECT=YES`,
		`AUDIO="aud0"`, `AUDIO="aud1"`,
		`BANDWIDTH=500000`, `BANDWIDTH=2000000`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("master missing %q:\n%s", want, s)
		}
	}
	if strings.Count(s, "#EXT-X-STREAM-INF:") != 2 {
		t.Errorf("variants = %d, want 2:\n%s", strings.Count(s, "#EXT-X-STREAM-INF:"), s)
	}

	// 没有 variant 引用的音频组应被拒绝；重复组 ID 也拒绝。
	unused := *m
	unused.Variants = []Variant{{URI: "v.m3u8", Bandwidth: 1, AudioGroup: "aud0"}}
	if _, err := unused.Generate(); err == nil {
		t.Error("unused audio group accepted")
	}
	dup := *m
	dup.Audios = append(append([]AudioRendition(nil), m.Audios...), m.Audios[0])
	if _, err := dup.Generate(); err == nil {
		t.Error("duplicate audio group accepted")
	}
}

func TestMasterPlaylistAudioOnly(t *testing.T) {
	m := &MasterPlaylist{
		Version: 9,
		Variants: []Variant{{
			URI: "audio.m3u8", Bandwidth: 128000, Codecs: "mp4a.40.2",
		}},
	}
	got, err := m.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	s := string(got)
	if strings.Contains(s, "EXT-X-MEDIA") {
		t.Error("audio-only master should not have EXT-X-MEDIA")
	}
	if !strings.Contains(s, "#EXT-X-STREAM-INF:BANDWIDTH=128000,CODECS=\"mp4a.40.2\"\naudio.m3u8\n") {
		t.Errorf("audio-only variant wrong:\n%s", s)
	}
}

func TestMasterPlaylistToken(t *testing.T) {
	m := &MasterPlaylist{
		Version: 9,
		Token:   "a b",
		Audio: &AudioRendition{
			GroupID: "aud", Name: "audio", URI: "audio.m3u8", Default: true,
		},
		Variants: []Variant{{URI: "video.m3u8", Bandwidth: 1000, AudioGroup: "aud"}},
	}
	got, err := m.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	s := string(got)
	if !strings.Contains(s, `URI="audio.m3u8?token=a+b"`) {
		t.Errorf("audio token missing:\n%s", s)
	}
	if !strings.Contains(s, "video.m3u8?token=a+b\n") {
		t.Errorf("video token missing:\n%s", s)
	}
}

func TestMasterPlaylistValidation(t *testing.T) {
	mk := func() *MasterPlaylist {
		return &MasterPlaylist{
			Version: 9,
			Audio:   &AudioRendition{GroupID: "aud", Name: "audio", URI: "audio.m3u8"},
			Variants: []Variant{{
				URI: "video.m3u8", Bandwidth: 1000, AudioGroup: "aud",
			}},
		}
	}
	tests := []struct {
		name   string
		mutate func(*MasterPlaylist)
	}{
		{"no variants", func(m *MasterPlaylist) { m.Variants = nil }},
		{"zero bandwidth", func(m *MasterPlaylist) { m.Variants[0].Bandwidth = 0 }},
		{"audio missing group", func(m *MasterPlaylist) { m.Audio.GroupID = "" }},
		{"audio missing name", func(m *MasterPlaylist) { m.Audio.Name = "" }},
		{"audio missing uri", func(m *MasterPlaylist) { m.Audio.URI = "" }},
		{"audio group unused", func(m *MasterPlaylist) { m.Variants[0].AudioGroup = "" }},
		{"dangling audio group", func(m *MasterPlaylist) { m.Audio = nil }},
		{"bad version", func(m *MasterPlaylist) { m.Version = 6 }},
		{"invalid variant uri", func(m *MasterPlaylist) { m.Variants[0].URI = "bad\"uri" }},
		{"invalid codecs", func(m *MasterPlaylist) { m.Variants[0].Codecs = "bad\"codec" }},
		{"invalid audio name", func(m *MasterPlaylist) { m.Audio.Name = "bad\nname" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := mk()
			tt.mutate(m)
			if _, err := m.Generate(); err == nil {
				t.Fatal("Generate returned nil error")
			}
		})
	}
}

func TestFormatPDT(t *testing.T) {
	ts := time.Date(2026, 11, 18, 8, 30, 0, 123_000_000, time.FixedZone("CST", 8*3600))
	if got, want := formatPDT(ts), "2026-11-18T00:30:00.123Z"; got != want {
		t.Errorf("formatPDT = %q, want %q", got, want)
	}
}

// ---- 测试用 m3u8 回读器 ----

type parsedEntry struct {
	mapURI        string
	pdt           string
	duration      float64
	uri           string
	discontinuity bool
}

type parsedPart struct {
	duration    float64
	uri         string
	independent bool
}

type parsedMedia struct {
	version    int
	target     int
	mediaSeq   uint64
	discSeq    uint64
	partTarget float64
	server     map[string]string
	entries    []parsedEntry
	parts      []parsedPart
	preload    string
	rendition  map[string]string
	endList    bool
}

func parseMediaPlaylist(t *testing.T, data []byte) *parsedMedia {
	t.Helper()
	p := &parsedMedia{server: map[string]string{}, rendition: map[string]string{}}
	currentMap := ""
	var pendingPDT string
	var pendingDuration float64
	var pendingDisc bool
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			switch {
			case strings.HasPrefix(line, "#EXT-X-VERSION:"):
				p.version = atoi(t, strings.TrimPrefix(line, "#EXT-X-VERSION:"))
			case strings.HasPrefix(line, "#EXT-X-TARGETDURATION:"):
				p.target = atoi(t, strings.TrimPrefix(line, "#EXT-X-TARGETDURATION:"))
			case strings.HasPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"):
				p.mediaSeq = atou(t, strings.TrimPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"))
			case strings.HasPrefix(line, "#EXT-X-DISCONTINUITY-SEQUENCE:"):
				p.discSeq = atou(t, strings.TrimPrefix(line, "#EXT-X-DISCONTINUITY-SEQUENCE:"))
			case strings.HasPrefix(line, "#EXT-X-PART-INF:"):
				p.partTarget = atof(t, parseAttrs(strings.TrimPrefix(line, "#EXT-X-PART-INF:"))["PART-TARGET"])
			case strings.HasPrefix(line, "#EXT-X-SERVER-CONTROL:"):
				p.server = parseAttrs(strings.TrimPrefix(line, "#EXT-X-SERVER-CONTROL:"))
			case strings.HasPrefix(line, "#EXT-X-MAP:"):
				currentMap = parseAttrs(strings.TrimPrefix(line, "#EXT-X-MAP:"))["URI"]
			case strings.HasPrefix(line, "#EXT-X-PROGRAM-DATE-TIME:"):
				pendingPDT = strings.TrimPrefix(line, "#EXT-X-PROGRAM-DATE-TIME:")
			case line == "#EXT-X-DISCONTINUITY":
				pendingDisc = true
			case strings.HasPrefix(line, "#EXTINF:"):
				v := strings.TrimPrefix(line, "#EXTINF:")
				if i := strings.IndexByte(v, ','); i >= 0 {
					v = v[:i]
				}
				pendingDuration = atof(t, v)
			case strings.HasPrefix(line, "#EXT-X-PART:"):
				attrs := parseAttrs(strings.TrimPrefix(line, "#EXT-X-PART:"))
				p.parts = append(p.parts, parsedPart{
					duration:    atof(t, attrs["DURATION"]),
					uri:         attrs["URI"],
					independent: attrs["INDEPENDENT"] == "YES",
				})
			case strings.HasPrefix(line, "#EXT-X-PRELOAD-HINT:"):
				p.preload = parseAttrs(strings.TrimPrefix(line, "#EXT-X-PRELOAD-HINT:"))["URI"]
			case strings.HasPrefix(line, "#EXT-X-RENDITION-REPORT:"):
				p.rendition = parseAttrs(strings.TrimPrefix(line, "#EXT-X-RENDITION-REPORT:"))
			case line == "#EXT-X-ENDLIST":
				p.endList = true
			}
			continue
		}
		p.entries = append(p.entries, parsedEntry{
			mapURI:        currentMap,
			pdt:           pendingPDT,
			duration:      pendingDuration,
			uri:           line,
			discontinuity: pendingDisc,
		})
		pendingPDT = ""
		pendingDuration = 0
		pendingDisc = false
	}
	return p
}

func checkParsedMedia(t *testing.T, p *parsedMedia) {
	t.Helper()
	if p.target < 1 {
		t.Errorf("target duration = %d", p.target)
	}
	for _, e := range p.entries {
		if e.duration <= 0 {
			t.Errorf("entry %q duration = %f", e.uri, e.duration)
		}
		if int(math.Round(e.duration)) > p.target {
			t.Errorf("entry %q duration %.3f rounds above target %d", e.uri, e.duration, p.target)
		}
	}
	if p.partTarget > 0 {
		for i, part := range p.parts {
			if part.duration > p.partTarget+1e-9 {
				t.Errorf("part %q duration %.3f > target %.3f", part.uri, part.duration, p.partTarget)
			}
			last := i == len(p.parts)-1
			if !part.independent && !last && part.duration*100+1e-9 < p.partTarget*85 {
				t.Errorf("part %q duration %.3f < 85%% of target %.3f and not independent",
					part.uri, part.duration, p.partTarget)
			}
		}
	}
	if p.endList && p.preload != "" {
		t.Errorf("endlist playlist has preload hint %q", p.preload)
	}
}

type parsedVariant struct {
	attrs map[string]string
	uri   string
}

type parsedMaster struct {
	version     int
	independent bool
	media       map[string]string
	variants    []parsedVariant
}

func parseMasterPlaylist(t *testing.T, data []byte) *parsedMaster {
	t.Helper()
	p := &parsedMaster{media: map[string]string{}}
	var pending map[string]string
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			switch {
			case strings.HasPrefix(line, "#EXT-X-VERSION:"):
				p.version = atoi(t, strings.TrimPrefix(line, "#EXT-X-VERSION:"))
			case line == "#EXT-X-INDEPENDENT-SEGMENTS":
				p.independent = true
			case strings.HasPrefix(line, "#EXT-X-MEDIA:"):
				p.media = parseAttrs(strings.TrimPrefix(line, "#EXT-X-MEDIA:"))
			case strings.HasPrefix(line, "#EXT-X-STREAM-INF:"):
				pending = parseAttrs(strings.TrimPrefix(line, "#EXT-X-STREAM-INF:"))
			}
			continue
		}
		if pending != nil {
			p.variants = append(p.variants, parsedVariant{attrs: pending, uri: line})
			pending = nil
		}
	}
	return p
}

func parseAttrs(s string) map[string]string {
	out := map[string]string{}
	var cur strings.Builder
	inQuotes := false
	for _, r := range s {
		switch r {
		case '"':
			inQuotes = !inQuotes
			cur.WriteRune(r)
		case ',':
			if inQuotes {
				cur.WriteRune(r)
			} else {
				addAttr(out, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteRune(r)
		}
	}
	addAttr(out, cur.String())
	return out
}

func addAttr(m map[string]string, s string) {
	k, v, ok := strings.Cut(s, "=")
	if !ok {
		return
	}
	m[strings.ToUpper(strings.TrimSpace(k))] = strings.Trim(strings.TrimSpace(v), `"`)
}

func atoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		t.Fatalf("atoi %q: %v", s, err)
	}
	return n
}

func atou(t *testing.T, s string) uint64 {
	t.Helper()
	n, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
	if err != nil {
		t.Fatalf("atou %q: %v", s, err)
	}
	return n
}

func atof(t *testing.T, s string) float64 {
	t.Helper()
	n, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		t.Fatalf("atof %q: %v", s, err)
	}
	return n
}

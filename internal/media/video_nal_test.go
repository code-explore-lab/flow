package media

import "testing"

// hevcHeader 构造一个 HEVC 2 字节 NAL 头：F=0、layer=0、TID=1。
func hevcHeader(typ uint8) []byte {
	return []byte{typ << 1, 0x01}
}

func TestClassifyVideoNALH264(t *testing.T) {
	cases := []struct {
		name  string
		nalu  []byte
		class VideoNALClass
	}{
		{"sps", []byte{0x67, 0x42}, VideoNALClass{IsSPS: true}},
		{"pps", []byte{0x68, 0xCE}, VideoNALClass{IsPPS: true}},
		{"idr", []byte{0x65, 0x88}, VideoNALClass{IsIDR: true, IsVCL: true}},
		{"non-idr slice", []byte{0x41, 0x9A}, VideoNALClass{IsVCL: true}},
		{"sei", []byte{0x06, 0x05}, VideoNALClass{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, codec := range []string{"", "H264"} {
				if got := ClassifyVideoNAL(codec, tc.nalu); got != tc.class {
					t.Errorf("codec %q: got %+v, want %+v", codec, got, tc.class)
				}
			}
		})
	}
}

func TestClassifyVideoNALH265(t *testing.T) {
	cases := []struct {
		name  string
		typ   uint8
		class VideoNALClass
	}{
		{"vps", HEVCNALTypeVPS, VideoNALClass{IsVPS: true}},
		{"sps", HEVCNALTypeSPS, VideoNALClass{IsSPS: true}},
		{"pps", HEVCNALTypePPS, VideoNALClass{IsPPS: true}},
		{"trail-r", 1, VideoNALClass{IsVCL: true}},
		{"idr_w_radl", HEVCNALTypeIDRWRADL, VideoNALClass{IsIDR: true, IsVCL: true}},
		{"idr_n_lp", HEVCNALTypeIDRNLP, VideoNALClass{IsIDR: true, IsVCL: true}},
		{"cra", HEVCNALTypeCRA, VideoNALClass{IsIDR: true, IsVCL: true}},
		{"bla_w_lp", HEVCNALTypeBLAWLP, VideoNALClass{IsIDR: true, IsVCL: true}},
		{"aud", 35, VideoNALClass{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, codec := range []string{"H265", "HEVC", "h265"} {
				nalu := append(hevcHeader(tc.typ), 0xAA, 0xBB)
				if got := ClassifyVideoNAL(codec, nalu); got != tc.class {
					t.Errorf("codec %q: got %+v, want %+v", codec, got, tc.class)
				}
			}
		})
	}
}

func TestClassifyVideoNALShortInput(t *testing.T) {
	if got := ClassifyVideoNAL("H265", []byte{0x40}); got != (VideoNALClass{}) {
		t.Fatalf("short HEVC nalu: got %+v, want zero", got)
	}
	if got := ClassifyVideoNAL("H264", nil); got != (VideoNALClass{}) {
		t.Fatalf("empty H.264 nalu: got %+v, want zero", got)
	}
}

func TestStartsVideoAUH264(t *testing.T) {
	cases := []struct {
		name string
		nalu []byte
		want bool
	}{
		{"idr first slice", []byte{0x65, 0x88, 0x84}, true},
		{"idr continuation slice", []byte{0x65, 0x00, 0x11}, false},
		{"non-idr first slice", []byte{0x41, 0x9A, 0x00}, true},
		{"non-idr continuation slice", []byte{0x41, 0x1A, 0x00}, false},
		{"partition A first slice", []byte{0x62, 0x80, 0x00}, true},
		{"partition B", []byte{0x63, 0x80, 0x00}, false},
		{"partition C", []byte{0x64, 0x80, 0x00}, false},
		{"sps", []byte{0x67, 0x80}, false},
		{"pps", []byte{0x68, 0x80}, false},
		{"sei", []byte{0x06, 0x80}, false},
		{"short idr", []byte{0x65}, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, codec := range []string{"", "H264", "h264"} {
				if got := StartsVideoAU(codec, tc.nalu); got != tc.want {
					t.Errorf("codec %q: got %v, want %v", codec, got, tc.want)
				}
			}
		})
	}
}

func TestStartsVideoAUH265(t *testing.T) {
	first := func(typ uint8) []byte { return append(hevcHeader(typ), 0x80, 0xAA) }
	cont := func(typ uint8) []byte { return append(hevcHeader(typ), 0x00, 0xAA) }
	cases := []struct {
		name string
		nalu []byte
		want bool
	}{
		{"idr first slice", first(HEVCNALTypeIDRWRADL), true},
		{"idr continuation slice", cont(HEVCNALTypeIDRWRADL), false},
		{"trail first slice", first(1), true},
		{"trail continuation slice", cont(1), false},
		{"vps", first(HEVCNALTypeVPS), false},
		{"sps", first(HEVCNALTypeSPS), false},
		{"short vcl", hevcHeader(1), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, codec := range []string{"H265", "HEVC", "h265"} {
				if got := StartsVideoAU(codec, tc.nalu); got != tc.want {
					t.Errorf("codec %q: got %v, want %v", codec, got, tc.want)
				}
			}
		})
	}
}

func TestVideoNALType(t *testing.T) {
	typ, err := VideoNALType("H265", hevcHeader(HEVCNALTypeSPS))
	if err != nil || typ != HEVCNALTypeSPS {
		t.Fatalf("HEVC type = %d, %v; want %d", typ, err, HEVCNALTypeSPS)
	}
	typ, err = VideoNALType("", []byte{0x65})
	if err != nil || typ != 5 {
		t.Fatalf("H.264 type = %d, %v; want 5", typ, err)
	}
}

func TestIsHEVCCodec(t *testing.T) {
	for _, codec := range []string{"H265", "h265", "HEVC", "hevc"} {
		if !IsHEVCCodec(codec) {
			t.Errorf("IsHEVCCodec(%q) = false", codec)
		}
	}
	for _, codec := range []string{"", "H264", "VP8"} {
		if IsHEVCCodec(codec) {
			t.Errorf("IsHEVCCodec(%q) = true", codec)
		}
	}
}

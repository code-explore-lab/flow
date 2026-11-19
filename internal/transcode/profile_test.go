package transcode

import (
	"strings"
	"testing"
)

func TestParseProfileDefaults(t *testing.T) {
	p, err := ParseProfile("live/test=w=1280,h=720")
	if err != nil {
		t.Fatal(err)
	}
	if p.Source != "live/test" {
		t.Errorf("Source = %q", p.Source)
	}
	if p.Output != "live/test_720p" {
		t.Errorf("Output = %q, want live/test_720p", p.Output)
	}
	if p.Width != 1280 || p.Height != 720 {
		t.Errorf("size = %dx%d", p.Width, p.Height)
	}
	if p.Codec != "H264" {
		t.Errorf("Codec = %q", p.Codec)
	}
	if p.Bitrate != DefaultBitrate {
		t.Errorf("Bitrate = %d, want %d", p.Bitrate, DefaultBitrate)
	}
	if p.FPS != 0 {
		t.Errorf("FPS = %v, want 0 (keep source)", p.FPS)
	}
}

func TestParseProfileFields(t *testing.T) {
	p, err := ParseProfile("live/test=size=853x481,codec=h265,bitrate=800k,fps=25,out=live/hd")
	if err != nil {
		t.Fatal(err)
	}
	if p.Width != 852 || p.Height != 480 {
		t.Errorf("size = %dx%d, want even 852x480", p.Width, p.Height)
	}
	if p.Codec != "H265" {
		t.Errorf("Codec = %q, want H265", p.Codec)
	}
	if p.Bitrate != 800_000 {
		t.Errorf("Bitrate = %d, want 800000", p.Bitrate)
	}
	if p.FPS != 25 {
		t.Errorf("FPS = %v, want 25", p.FPS)
	}
	if p.Output != "live/hd" {
		t.Errorf("Output = %q", p.Output)
	}
}

func TestParseProfileAliases(t *testing.T) {
	p, err := ParseProfile("cam=width=640,height=360,b=1m,r=30,output=cam_low")
	if err != nil {
		t.Fatal(err)
	}
	if p.Width != 640 || p.Height != 360 || p.Bitrate != 1_000_000 || p.FPS != 30 || p.Output != "cam_low" {
		t.Errorf("unexpected profile: %+v", p)
	}
}

func TestParseProfileErrors(t *testing.T) {
	cases := []struct {
		spec string
		want string
	}{
		{"", "want source="},
		{"live/test", "want source="},
		{"=w=640,h=360", "empty source"},
		{"live/test=w=640", "required"},
		{"live/test=w=640,h=0", "invalid positive integer"},
		{"live/test=size=640", "want 1280x720"},
		{"live/test=w=640,h=360,codec=vp9", "unsupported codec"},
		{"live/test=w=640,h=360,bitrate=0", "invalid bitrate"},
		{"live/test=w=640,h=360,bitrate=999999999", "out of range"},
		{"live/test=w=640,h=360,prefix=1", "unknown field"},
		{"live/test=w=640,h=360,out=live/test", "must differ"},
		{"live/test=w=1,h=1", "too small"},
		{"live/test=w=640,h=360,fps=0", "invalid fps"},
		{"live/test=w=640,h=360,fps=500", "invalid fps"},
	}
	for _, tc := range cases {
		_, err := ParseProfile(tc.spec)
		if err == nil {
			t.Errorf("ParseProfile(%q) succeeded, want error containing %q", tc.spec, tc.want)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("ParseProfile(%q) = %v, want substring %q", tc.spec, err, tc.want)
		}
	}
}

func TestParseProfilesList(t *testing.T) {
	profiles, err := ParseProfiles("live/a=w=1280,h=720; live/a=w=640,h=360\nlive/b=w=320,h=180")
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 3 {
		t.Fatalf("got %d profiles, want 3", len(profiles))
	}
	if profiles[1].Output != "live/a_360p" {
		t.Errorf("second output = %q", profiles[1].Output)
	}
}

func TestParseProfilesEmpty(t *testing.T) {
	for _, value := range []string{"", "  ", ";", "\n;\n"} {
		profiles, err := ParseProfiles(value)
		if err != nil {
			t.Errorf("ParseProfiles(%q): %v", value, err)
		}
		if len(profiles) != 0 {
			t.Errorf("ParseProfiles(%q) = %d profiles, want 0", value, len(profiles))
		}
	}
}

func TestParseProfilesDuplicateOutput(t *testing.T) {
	_, err := ParseProfiles("live/a=w=640,h=360;live/b=w=640,h=360,out=live/a_360p")
	if err == nil || !strings.Contains(err.Error(), "duplicate output") {
		t.Fatalf("err = %v, want duplicate output", err)
	}
}

func TestProfileLabel(t *testing.T) {
	p := Profile{Source: "live/a", Output: "live/a_360p"}
	if got := p.Label(); got != "live/a→live/a_360p" {
		t.Errorf("Label = %q", got)
	}
}

package transcode

import (
	"strings"
	"testing"
	"time"
)

func argValue(args []string, flag string) (string, bool) {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag {
			return args[i+1], true
		}
	}
	return "", false
}

func hasArg(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

func TestBuildArgsSoftwareH264(t *testing.T) {
	p := Profile{Source: "a", Output: "a_360p", Width: 640, Height: 360, Codec: "H264", Bitrate: 900_000}
	args := buildArgs(p, Options{}.withDefaults())

	if got, _ := argValue(args, "-c:v"); got != "libx264" {
		t.Errorf("-c:v = %q, want libx264", got)
	}
	if got, _ := argValue(args, "-vf"); !strings.HasPrefix(got, "scale=640:360:") {
		t.Errorf("-vf = %q", got)
	}
	if got, _ := argValue(args, "-b:v"); got != "900000" {
		t.Errorf("-b:v = %q", got)
	}
	if got, _ := argValue(args, "-maxrate"); got != "990000" {
		t.Errorf("-maxrate = %q", got)
	}
	if got, _ := argValue(args, "-bufsize"); got != "1800000" {
		t.Errorf("-bufsize = %q", got)
	}
	if got, _ := argValue(args, "-preset"); got != DefaultPreset {
		t.Errorf("-preset = %q, want %q", got, DefaultPreset)
	}
	if !hasArg(args, "-tune") || !hasArg(args, "zerolatency") {
		t.Error("software encoder should use -tune zerolatency")
	}
	if got, _ := argValue(args, "-fps_mode"); got != "passthrough" {
		t.Errorf("-fps_mode = %q, want passthrough", got)
	}
	if !hasArg(args, "-copyts") {
		t.Error("missing -copyts")
	}
	if got, _ := argValue(args, "-f"); got != "mpegts" {
		t.Errorf("last -f = %q", got)
	}
	if len(args) == 0 || args[len(args)-1] != "pipe:1" {
		t.Error("output should be pipe:1")
	}
}

func TestBuildArgsFPS(t *testing.T) {
	p := Profile{Width: 320, Height: 240, Codec: "H264", Bitrate: 300_000, FPS: 25}
	args := buildArgs(p, Options{}.withDefaults())
	if got, _ := argValue(args, "-r"); got != "25" {
		t.Errorf("-r = %q, want 25", got)
	}
	if got, _ := argValue(args, "-fps_mode"); got != "cfr" {
		t.Errorf("-fps_mode = %q, want cfr", got)
	}
}

func TestBuildArgsGOP(t *testing.T) {
	args := buildArgs(Profile{Width: 320, Height: 240}, Options{GOP: 1500 * time.Millisecond}.withDefaults())
	if got, _ := argValue(args, "-force_key_frames"); got != "expr:gte(t,n_forced*1.5)" {
		t.Errorf("-force_key_frames = %q", got)
	}
}

func TestEncoderSelection(t *testing.T) {
	cases := []struct {
		codec string
		hw    string
		want  string
	}{
		{"H264", "none", "libx264"},
		{"H265", "none", "libx265"},
		{"H264", "nvenc", "h264_nvenc"},
		{"H265", "nvenc", "hevc_nvenc"},
		{"H264", "qsv", "h264_qsv"},
		{"H265", "qsv", "hevc_qsv"},
	}
	for _, tc := range cases {
		got, _ := encoderFor(Profile{Codec: tc.codec}, tc.hw)
		if got != tc.want {
			t.Errorf("encoderFor(%s, %s) = %q, want %q", tc.codec, tc.hw, got, tc.want)
		}
	}
}

func TestBuildArgsNVENCPreset(t *testing.T) {
	args := buildArgs(Profile{Width: 320, Height: 240, Codec: "H264"}, Options{HW: "nvenc", Preset: "veryfast"}.withDefaults())
	if got, _ := argValue(args, "-preset"); got != "p4" {
		t.Errorf("-preset = %q, want p4 (nvenc uses p1..p7)", got)
	}
	if got, _ := argValue(args, "-tune"); got != "ll" {
		t.Errorf("-tune = %q, want ll", got)
	}
	if hasArg(args, "zerolatency") {
		t.Error("nvenc must not receive x264 tune values")
	}
}

func TestBuildArgsQSV(t *testing.T) {
	args := buildArgs(Profile{Width: 320, Height: 240, Codec: "H264"}, Options{HW: "qsv"}.withDefaults())
	if got, _ := argValue(args, "-look_ahead"); got != "0" {
		t.Errorf("-look_ahead = %q, want 0", got)
	}
}

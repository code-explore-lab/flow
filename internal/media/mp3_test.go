package media

import "testing"

func TestParseMP3Frame(t *testing.T) {
	// MPEG-1 Layer III, 128 kbps, 44100 Hz, 立体声，无 padding。
	// 帧长 = 144 * 128000 / 44100 = 417。
	frame := []byte{0xFF, 0xFB, 0x90, 0x00, 0x00, 0x00}
	info, ok := ParseMP3Frame(frame)
	if !ok {
		t.Fatal("ParseMP3Frame failed")
	}
	if info.Version != 1 || info.Layer != 3 {
		t.Fatalf("version/layer = %d/%d, want 1/3", info.Version, info.Layer)
	}
	if info.SampleRate != 44100 || info.Channels != 2 {
		t.Fatalf("rate/ch = %d/%d, want 44100/2", info.SampleRate, info.Channels)
	}
	if info.Samples != 1152 {
		t.Fatalf("samples = %d, want 1152", info.Samples)
	}
	if info.Bitrate != 128 || info.Length != 417 {
		t.Fatalf("bitrate/length = %d/%d, want 128/417", info.Bitrate, info.Length)
	}
}

func TestParseMP3FrameMonoAndVersion2(t *testing.T) {
	// MPEG-2 Layer III, 64 kbps, 22050 Hz, 单声道。
	// bitrate index=8 -> 64kbps；samplerate index=0 -> 22050；channel mode=11。
	frame := []byte{0xFF, 0xF3, 0x80, 0xC0}
	info, ok := ParseMP3Frame(frame)
	if !ok {
		t.Fatal("ParseMP3Frame failed")
	}
	if info.Version != 2 || info.SampleRate != 22050 || info.Channels != 1 {
		t.Fatalf("info = %+v", info)
	}
	if info.Samples != 576 {
		t.Fatalf("samples = %d, want 576", info.Samples)
	}
}

func TestParseMP3FrameErrors(t *testing.T) {
	if _, ok := ParseMP3Frame(nil); ok {
		t.Fatal("accepted empty frame")
	}
	if _, ok := ParseMP3Frame([]byte{0x00, 0x00, 0x00, 0x00}); ok {
		t.Fatal("accepted missing sync")
	}
	// 非法采样率索引（3）。
	if _, ok := ParseMP3Frame([]byte{0xFF, 0xFB, 0x9C, 0x00}); ok {
		t.Fatal("accepted invalid sample rate")
	}
	// 非法比特率索引（15）。
	if _, ok := ParseMP3Frame([]byte{0xFF, 0xFB, 0xF0, 0x00}); ok {
		t.Fatal("accepted invalid bitrate")
	}
}

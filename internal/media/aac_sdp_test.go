package media

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAudioSpecificConfig(t *testing.T) {
	// AAC LC(profile=1 → AOT=2) / 44100(sfi=4) / 立体声(chCfg=2)
	// 2<<11 | 4<<7 | 2<<3 = 0x1210
	h, err := ParseADTSHeader(buildADTSFrame([]byte{0x00}))
	if err != nil {
		t.Fatal(err)
	}
	asc := h.AudioSpecificConfig()
	if !bytes.Equal(asc, []byte{0x12, 0x10}) {
		t.Fatalf("AudioSpecificConfig = %x, want 1210", asc)
	}
}

func TestAudioSpecificConfigMono48k(t *testing.T) {
	// 现场构造一帧：AAC LC / 48000(sfi=3) / 单声道(chCfg=1)
	// AOT<<11 | sfi<<7 | chCfg<<3 = 0x1188
	frame := buildADTSFrame([]byte{0x00})
	frame[2] = byte(1<<6) | byte(3<<2) // profile=1, sfi=3
	frame[3] = byte(1&0x03)<<6 | frame[3]&0x03
	h, err := ParseADTSHeader(frame)
	if err != nil {
		t.Fatal(err)
	}
	asc := h.AudioSpecificConfig()
	if !bytes.Equal(asc, []byte{0x11, 0x88}) {
		t.Fatalf("AudioSpecificConfig = %x, want 1188", asc)
	}
}

func TestGetAACInfoAndMediaDescription(t *testing.T) {
	blob := buildADTSFrame([]byte{0x01, 0x02, 0x03, 0x04})
	blob = append(blob, buildADTSFrame([]byte{0x05, 0x06})...)

	dir := t.TempDir()
	path := filepath.Join(dir, "test.aac")
	if err := os.WriteFile(path, blob, 0o644); err != nil {
		t.Fatal(err)
	}

	info, err := GetAACInfo(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.SamplingFrequency != 44100 || info.ChannelConfig != 2 {
		t.Fatalf("info = %+v, want 44100/2", info)
	}
	if info.ConfigBase64() != "EhA=" { // base64(0x12 0x10)
		t.Fatalf("ConfigBase64 = %q, want EhA=", info.ConfigBase64())
	}

	sdp := info.MediaDescription(AACPT)
	want := []string{
		"m=audio 0 RTP/AVP 97\r\n",
		"a=rtpmap:97 mpeg4-generic/44100/2\r\n",
		"a=fmtp:97 streamtype=5; profile-level-id=1; mode=AAC-hbr; SizeLength=13; IndexLength=3; IndexDeltaLength=3; config=EhA=\r\n",
	}
	for _, w := range want {
		if !strings.Contains(sdp, w) {
			t.Fatalf("MediaDescription missing %q, got:\n%s", w, sdp)
		}
	}
}

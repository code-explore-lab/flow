package flv

import (
	"bytes"
	"testing"
)

func TestSoundRateIndex(t *testing.T) {
	cases := []struct {
		rate uint32
		want uint8
	}{
		{8000, 0},
		{11025, 1},
		{16000, 1},
		{22050, 2},
		{32000, 2},
		{44100, 3},
		{48000, 3},
		{96000, 3},
	}
	for _, c := range cases {
		if got := SoundRateIndex(c.rate); got != c.want {
			t.Fatalf("SoundRateIndex(%d) = %d, want %d", c.rate, got, c.want)
		}
	}
}

func TestAudioSequenceHeader(t *testing.T) {
	asc := []byte{0x12, 0x10} // AAC LC 44100 立体声
	body := BuildAudioSequenceHeader(asc, 44100, 2)

	if sf := body[0] >> 4; sf != soundFormatAAC {
		t.Fatalf("sound format = %d, want %d", sf, soundFormatAAC)
	}
	if got := body[1]; got != AACSeqHeader {
		t.Fatalf("packet type = %d, want 0", got)
	}
	if !bytes.Equal(body[2:], asc) {
		t.Fatalf("asc = % x, want % x", body[2:], asc)
	}

	at, err := ParseAudioTag(body)
	if err != nil {
		t.Fatalf("ParseAudioTag: %v", err)
	}
	if at.PacketType != AACSeqHeader || !bytes.Equal(at.Config, asc) {
		t.Fatalf("audio tag = %+v", at)
	}
}

func TestAudioTagRaw(t *testing.T) {
	au := []byte{0x21, 0x10, 0x11, 0x00} // 裸 AAC AU
	body := BuildAudioTag(au, 48000, 2)

	if body[1] != AACRaw {
		t.Fatalf("packet type = %d, want 1", body[1])
	}
	at, err := ParseAudioTag(body)
	if err != nil {
		t.Fatalf("ParseAudioTag: %v", err)
	}
	if at.PacketType != AACRaw || !bytes.Equal(at.AU, au) {
		t.Fatalf("audio tag = %+v", at)
	}
}

func TestAudioSoundTypeMonoVsStereo(t *testing.T) {
	asc := []byte{0x12, 0x08} // 单声道
	mono := BuildAudioSequenceHeader(asc, 44100, 1)
	stereo := BuildAudioSequenceHeader(asc, 44100, 2)

	if mono[0]&0x01 != 0 {
		t.Fatalf("mono sound type bit = %d, want 0", mono[0]&0x01)
	}
	if stereo[0]&0x01 != 1 {
		t.Fatalf("stereo sound type bit = %d, want 1", stereo[0]&0x01)
	}
	// SoundSize 恒为 1（16 位）。
	if mono[0]&0x02 == 0 || stereo[0]&0x02 == 0 {
		t.Fatal("SoundSize should be 16-bit (bit set)")
	}

	// 从 tag 读回声道数。
	if at, _ := ParseAudioTag(stereo); at.SoundType != 1 {
		t.Fatalf("stereo tag SoundType = %d", at.SoundType)
	}
	if at, _ := ParseAudioTag(mono); at.SoundType != 0 {
		t.Fatalf("mono tag SoundType = %d", at.SoundType)
	}
}

func TestParseAudioTagErrors(t *testing.T) {
	if _, err := ParseAudioTag([]byte{0xAF}); err == nil {
		t.Fatal("accepted too-short tag")
	}
	// MP3（SoundFormat=2）不是 AAC。
	if _, err := ParseAudioTag([]byte{0x2F, 0x00}); err == nil {
		t.Fatal("accepted non-AAC sound format")
	}
	// 非法 AACPacketType。
	if _, err := ParseAudioTag([]byte{0xAF, 0x03}); err == nil {
		t.Fatal("accepted unknown packet type")
	}
}

package flv

import (
	"bytes"
	"testing"
)

// TestBuildAudioTagForRawCodecs 校验 MP3 / G.711 的 tag 只有 1 字节格式头，
// 负载原样保留，解析后能还原格式与数据。
func TestBuildAudioTagForRawCodecs(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03}
	cases := []struct {
		codec    string
		format   uint8
		rate     uint32
		ch       uint8
		wantRate uint8
		wantType uint8
	}{
		{"MP3", SoundFormatMP3, 44100, 2, 3, 1},
		{"PCMA", SoundFormatALaw, 8000, 1, 0, 0},
		{"PCMU", SoundFormatMuLaw, 8000, 1, 0, 0},
		{"MP3", SoundFormatMP3, 22050, 1, 2, 0},
	}
	for _, c := range cases {
		body := BuildAudioTagFor(c.codec, data, c.rate, c.ch)
		if len(body) != 1+len(data) {
			t.Fatalf("%s: body len = %d, want %d", c.codec, len(body), 1+len(data))
		}
		if body[0]>>4 != c.format {
			t.Fatalf("%s: sound format = %d, want %d", c.codec, body[0]>>4, c.format)
		}

		at, err := ParseAudioTag(body)
		if err != nil {
			t.Fatalf("%s: ParseAudioTag: %v", c.codec, err)
		}
		if at.SoundFormat != c.format || !bytes.Equal(at.Data, data) {
			t.Fatalf("%s: tag = %+v", c.codec, at)
		}
		if at.PacketType != 0 {
			t.Fatalf("%s: packet type = %d, want 0", c.codec, at.PacketType)
		}
		if at.SoundRate != c.wantRate || at.SoundType != c.wantType {
			t.Fatalf("%s: rate index/type = %d/%d, want %d/%d",
				c.codec, at.SoundRate, at.SoundType, c.wantRate, c.wantType)
		}
		if at.SoundSize != 1 {
			t.Fatalf("%s: sound size = %d, want 16-bit", c.codec, at.SoundSize)
		}
	}
}

// TestBuildAudioTagForDefaultAAC 确认未知编码仍按 AAC 裸 AU 封装。
func TestBuildAudioTagForDefaultAAC(t *testing.T) {
	au := []byte{0x21, 0x10}
	for _, codec := range []string{"AAC", "", "unknown"} {
		body := BuildAudioTagFor(codec, au, 44100, 2)
		if body[0]>>4 != SoundFormatAAC || body[1] != AACRaw {
			t.Fatalf("%q: body head = %x", codec, body[:2])
		}
		at, err := ParseAudioTag(body)
		if err != nil {
			t.Fatalf("%q: ParseAudioTag: %v", codec, err)
		}
		if !bytes.Equal(at.AU, au) {
			t.Fatalf("%q: AU = %x, want %x", codec, at.AU, au)
		}
	}
}

// TestParseAudioTagG711 直接解析手写的 G.711 tag。
func TestParseAudioTagG711(t *testing.T) {
	payload := []byte{0xD5, 0x00, 0x7F}
	// A-law 8kHz 单声道：format=7, rate=0, size=1, type=0。
	body := append([]byte{0x7A}, payload...)
	at, err := ParseAudioTag(body)
	if err != nil {
		t.Fatal(err)
	}
	if at.SoundFormat != SoundFormatALaw || !bytes.Equal(at.Data, payload) {
		t.Fatalf("tag = %+v", at)
	}
}

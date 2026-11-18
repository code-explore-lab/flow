package flvmux

import (
	"bytes"
	"testing"

	"flow/internal/flv"
	"flow/internal/source"
	"flow/internal/stream"
)

func videoParams() *stream.VideoCodec {
	return &stream.VideoCodec{
		Codec: "H264",
		SPS:   []byte{0x67, 0x42, 0x00, 0x1e},
		PPS:   []byte{0x68, 0xce},
	}
}

func TestVideoSequenceAndAUMerge(t *testing.T) {
	m := New(videoParams(), nil)
	seq := m.SequenceTags()
	if len(seq) != 1 || seq[0].Type != flv.TagVideo {
		t.Fatalf("sequence tags = %+v", seq)
	}
	vt, err := flv.ParseVideoTag(seq[0].Data)
	if err != nil {
		t.Fatal(err)
	}
	if vt.PacketType != flv.AVCSeqHeader || len(vt.Config) == 0 {
		t.Fatalf("sequence tag = %+v", vt)
	}

	// 一帧两个 slice：必须合并成一个 tag，共用 DTS/CTS。
	tags := m.Video(source.Frame{
		Data: []byte{0x65, 1}, Timestamp: 9000, CompositionTime: 3000,
		Keyframe: true, AUStart: true,
	})
	if len(tags) != 0 {
		t.Fatalf("first slice emitted %d tag(s)", len(tags))
	}
	tags = m.Video(source.Frame{
		Data: []byte{0x65, 2}, Timestamp: 9000, CompositionTime: 3000, AUEnd: true,
	})
	if len(tags) != 1 {
		t.Fatalf("AU emitted %d tag(s)", len(tags))
	}
	if tags[0].Timestamp != 0 {
		t.Fatalf("first tag timestamp = %d, want 0", tags[0].Timestamp)
	}
	vt, err = flv.ParseVideoTag(tags[0].Data)
	if err != nil {
		t.Fatal(err)
	}
	if !vt.Keyframe || len(vt.NALUs) != 2 || vt.CTS != 33 {
		t.Fatalf("tag = %+v", vt)
	}

	// 第二帧 DTS = 12000 ticks = 133ms，相对首帧 67ms 应为 66ms。
	tags = m.Video(source.Frame{
		Data: []byte{0x41, 3}, Timestamp: 12000, AUEnd: true,
	})
	if len(tags) != 1 {
		t.Fatalf("second frame emitted %d tag(s)", len(tags))
	}
	if tags[0].Timestamp != 66 {
		t.Fatalf("second timestamp = %d, want 66", tags[0].Timestamp)
	}
	if vt, _ := flv.ParseVideoTag(tags[0].Data); vt.Keyframe {
		t.Fatal("second frame marked keyframe")
	}
}

// TestVideoLateParams 校验参数集晚于首帧到达时，首帧被丢弃、参数集齐后
// 先补发序列头再发媒体 tag。
func TestVideoLateParams(t *testing.T) {
	m := New(nil, nil)
	if tags := m.Video(source.Frame{Data: []byte{0x65, 1}, Timestamp: 9000, AUEnd: true}); len(tags) != 0 {
		t.Fatalf("frame without params emitted %d tag(s)", len(tags))
	}
	if tags := m.Video(source.Frame{Data: []byte{0x67, 0x42}, Timestamp: 9000}); len(tags) != 0 {
		t.Fatalf("SPS-only emitted %d tag(s)", len(tags))
	}
	tags := m.Video(source.Frame{Data: []byte{0x68, 0xce}, Timestamp: 9000})
	if len(tags) != 1 || tags[0].Type != flv.TagVideo {
		t.Fatalf("params emitted %+v", tags)
	}
	if vt, _ := flv.ParseVideoTag(tags[0].Data); vt.PacketType != flv.AVCSeqHeader {
		t.Fatalf("expected sequence header, got packet type %d", vt.PacketType)
	}
	tags = m.Video(source.Frame{Data: []byte{0x65, 1}, Timestamp: 9000, Keyframe: true, AUEnd: true})
	if len(tags) != 1 {
		t.Fatalf("frame after params emitted %d tag(s)", len(tags))
	}
}

func TestAudioAAC(t *testing.T) {
	m := New(nil, &stream.AudioCodec{
		Codec: stream.AudioAAC, Config: []byte{0x12, 0x10},
		SampleRate: 44100, ClockRate: 44100, Channels: 2,
	})
	seq := m.SequenceTags()
	if len(seq) != 1 || seq[0].Type != flv.TagAudio {
		t.Fatalf("sequence tags = %+v", seq)
	}
	at, err := flv.ParseAudioTag(seq[0].Data)
	if err != nil {
		t.Fatal(err)
	}
	if at.SoundFormat != flv.SoundFormatAAC || at.PacketType != flv.AACSeqHeader ||
		!bytes.Equal(at.Config, []byte{0x12, 0x10}) {
		t.Fatalf("sequence tag = %+v", at)
	}
	if !m.AudioEnabled() {
		t.Fatal("AAC should be muxable")
	}

	tags := m.Audio(source.Frame{Data: []byte{0xAA}, Timestamp: 44100})
	if len(tags) != 1 || tags[0].Timestamp != 0 {
		t.Fatalf("tags = %+v", tags)
	}
	at, err = flv.ParseAudioTag(tags[0].Data)
	if err != nil {
		t.Fatal(err)
	}
	if at.PacketType != flv.AACRaw || !bytes.Equal(at.AU, []byte{0xAA}) {
		t.Fatalf("audio tag = %+v", at)
	}

	tags = m.Audio(source.Frame{Data: []byte{0xBB}, Timestamp: 88200})
	if len(tags) != 1 || tags[0].Timestamp != 1000 {
		t.Fatalf("second tags = %+v", tags)
	}
}

func TestAudioMP3AndG711(t *testing.T) {
	mp3 := New(nil, &stream.AudioCodec{
		Codec: stream.AudioMP3, SampleRate: 44100, ClockRate: 90000, Channels: 2,
	})
	if seq := mp3.SequenceTags(); len(seq) != 0 {
		t.Fatalf("MP3 sequence tags = %+v", seq)
	}
	tags := mp3.Audio(source.Frame{Data: []byte{0xFF, 0xFB}, Timestamp: 9000})
	if len(tags) != 1 || tags[0].Timestamp != 0 {
		t.Fatalf("MP3 tags = %+v", tags)
	}
	if at, err := flv.ParseAudioTag(tags[0].Data); err != nil ||
		at.SoundFormat != flv.SoundFormatMP3 || !bytes.Equal(at.Data, []byte{0xFF, 0xFB}) {
		t.Fatalf("MP3 tag = %+v err = %v", at, err)
	}

	for _, c := range []struct {
		codec  string
		format uint8
	}{
		{stream.AudioPCMA, flv.SoundFormatALaw},
		{stream.AudioPCMU, flv.SoundFormatMuLaw},
	} {
		m := New(nil, &stream.AudioCodec{
			Codec: c.codec, SampleRate: 8000, ClockRate: 8000, Channels: 1,
		})
		tags := m.Audio(source.Frame{Data: []byte{0xD5}, Timestamp: 8000})
		if len(tags) != 1 {
			t.Fatalf("%s: tags = %+v", c.codec, tags)
		}
		at, err := flv.ParseAudioTag(tags[0].Data)
		if err != nil || at.SoundFormat != c.format || !bytes.Equal(at.Data, []byte{0xD5}) {
			t.Fatalf("%s: tag = %+v err = %v", c.codec, at, err)
		}
	}
}

// TestAudioOpusSkipped 校验 FLV 承载不了的编码会被整体跳过。
func TestAudioOpusSkipped(t *testing.T) {
	m := New(nil, &stream.AudioCodec{
		Codec: stream.AudioOpus, SampleRate: 48000, ClockRate: 48000, Channels: 2,
	})
	if m.AudioEnabled() {
		t.Fatal("Opus should not be muxable into FLV")
	}
	if seq := m.SequenceTags(); len(seq) != 0 {
		t.Fatalf("Opus sequence tags = %+v", seq)
	}
	if tags := m.Audio(source.Frame{Data: []byte{0x78}}); len(tags) != 0 {
		t.Fatalf("Opus emitted %+v", tags)
	}
}

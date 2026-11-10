package flv

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestHeaderRoundTrip(t *testing.T) {
	cases := []Header{
		{HasAudio: true, HasVideo: true},
		{HasAudio: true},
		{HasVideo: true},
		{Version: 2, HasAudio: true, HasVideo: true, DataOffset: 13},
	}
	for _, want := range cases {
		var buf bytes.Buffer
		if err := WriteHeader(&buf, want); err != nil {
			t.Fatalf("WriteHeader(%+v): %v", want, err)
		}
		got, err := ReadHeader(&buf)
		if err != nil {
			t.Fatalf("ReadHeader(%+v): %v", want, err)
		}
		wantVersion := want.Version
		if wantVersion == 0 {
			wantVersion = 1 // 写零值时规范化为 1
		}
		if got.HasAudio != want.HasAudio || got.HasVideo != want.HasVideo || got.Version != wantVersion {
			t.Fatalf("header = %+v, want audio=%v video=%v version=%d",
				got, want.HasAudio, want.HasVideo, wantVersion)
		}
		if got.DataOffset < 9 {
			t.Fatalf("DataOffset = %d, want >= 9", got.DataOffset)
		}
	}
}

// TestHeaderExtensionSkipsDataOffset 验证 DataOffset > 9 时能跳过扩展区，
// 并正确对齐到后面的 Tag。
func TestHeaderExtensionSkipsDataOffset(t *testing.T) {
	var buf bytes.Buffer
	raw := []byte{'F', 'L', 'V', 1, 0x05, 0, 0, 0, 13}
	raw = append(raw, 0xDE, 0xAD, 0xBE, 0xEF) // 自定义扩展数据
	raw = append(raw, 0, 0, 0, 0)             // PreviousTagSize0
	buf.Write(raw)

	h, err := ReadHeader(&buf)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if !h.HasAudio || !h.HasVideo || h.DataOffset != 13 {
		t.Fatalf("header = %+v, want audio+video offset 13", h)
	}

	// 紧接着一个 Tag，确认读位置正确。
	var tagBuf bytes.Buffer
	if err := WriteTag(&tagBuf, Tag{Type: TagVideo, Data: []byte{0x01}}); err != nil {
		t.Fatal(err)
	}
	buf.Write(tagBuf.Bytes())
	tag, err := ReadTag(&buf)
	if err != nil {
		t.Fatalf("ReadTag after extension: %v", err)
	}
	if tag.Type != TagVideo || !bytes.Equal(tag.Data, []byte{0x01}) {
		t.Fatalf("tag = %+v", tag)
	}
}

func TestTagTimestampRoundTrip(t *testing.T) {
	timestamps := []uint32{
		0,
		1,
		0x0000FFFF,
		0x00FFFFFF, // 24 位最大值
		0x01000000, // 刚好需要扩展字节
		0x12345678,
		0xFFFFFFFF,
	}
	for _, ts := range timestamps {
		var buf bytes.Buffer
		want := Tag{
			Type:      TagVideo,
			Timestamp: ts,
			StreamID:  7,
			Data:      []byte{0xAA, 0xBB, 0xCC},
		}
		if err := WriteTag(&buf, want); err != nil {
			t.Fatalf("WriteTag(ts=%#x): %v", ts, err)
		}

		raw := buf.Bytes()
		// 时间戳低 24 位在 4..6，高 8 位在 7。
		low := getUint24(raw[4:7])
		high := uint32(raw[7])
		if got := low | high<<24; got != ts {
			t.Fatalf("raw timestamp = %#x, want %#x (low=%#x high=%#x)", got, ts, low, high)
		}
		if want := uint32(tagHeaderSize + len(want.Data)); getUint32(raw[len(raw)-4:]) != want {
			t.Fatalf("PreviousTagSize = %d, want %d", getUint32(raw[len(raw)-4:]), want)
		}

		got, err := ReadTag(&buf)
		if err != nil {
			t.Fatalf("ReadTag(ts=%#x): %v", ts, err)
		}
		if got.Type != want.Type || got.Timestamp != ts || got.StreamID != want.StreamID ||
			got.DataSize != uint32(len(want.Data)) || !bytes.Equal(got.Data, want.Data) {
			t.Fatalf("tag = %+v, want %+v", got, want)
		}
	}
}

func TestTagTypesRoundTrip(t *testing.T) {
	for _, typ := range []uint8{TagAudio, TagVideo, TagScript} {
		var buf bytes.Buffer
		if err := WriteTag(&buf, Tag{Type: typ, Data: []byte{0x42}}); err != nil {
			t.Fatal(err)
		}
		got, err := ReadTag(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if got.Type != typ {
			t.Fatalf("type = %d, want %d", got.Type, typ)
		}
	}
}

func TestReadTagEOF(t *testing.T) {
	if _, err := ReadTag(bytes.NewReader(nil)); !errors.Is(err, io.EOF) {
		t.Fatalf("ReadTag(empty) = %v, want io.EOF", err)
	}
}

func TestWriteTagTooLarge(t *testing.T) {
	var buf bytes.Buffer
	err := WriteTag(&buf, Tag{Type: TagVideo, Data: make([]byte, 1<<24)})
	if err == nil {
		t.Fatal("WriteTag accepted a tag larger than 24 bits")
	}
}

package rtmp

import (
	"bytes"
	"strings"
	"testing"

	"flow/internal/flv"
)

// buildAggregateSub 按 FLV tag 布局追加一条子消息。
// backPointer=false 用于构造「最后一条省略 back pointer」的兼容用例。
func buildAggregateSub(buf []byte, typeID uint8, ts uint32, payload []byte, backPointer bool) []byte {
	buf = append(buf, typeID)
	buf = appendUint24(buf, uint32(len(payload)))
	buf = appendUint24(buf, ts&0xFFFFFF)
	buf = append(buf, byte(ts>>24))
	buf = append(buf, 0, 0, 0) // stream id 必须为 0，且会被忽略
	buf = append(buf, payload...)
	if backPointer {
		buf = appendUint32(buf, uint32(aggregateSubHeaderSize+len(payload)))
	}
	return buf
}

func TestSplitAggregateBasic(t *testing.T) {
	audio := []byte{0xAF, 0x01, 0x11}
	video := []byte{0x17, 0x00, 0x00, 0x00, 0x00}
	payload := buildAggregateSub(nil, msgAudio, 40, audio, true)
	payload = buildAggregateSub(payload, msgVideo, 80, video, true)

	subs, err := splitAggregate(payload, 40)
	if err != nil {
		t.Fatalf("splitAggregate: %v", err)
	}
	if len(subs) != 2 {
		t.Fatalf("subs = %d, want 2", len(subs))
	}
	if subs[0].TypeID != msgAudio || subs[0].Timestamp != 40 || !bytes.Equal(subs[0].Payload, audio) {
		t.Fatalf("sub0 = %+v", subs[0])
	}
	if subs[1].TypeID != msgVideo || subs[1].Timestamp != 80 || !bytes.Equal(subs[1].Payload, video) {
		t.Fatalf("sub1 = %+v", subs[1])
	}
}

func TestSplitAggregateTimestampNormalization(t *testing.T) {
	// 首条子消息时间戳与聚合消息不一致时，整体平移使首条等于外层时间戳，
	// 子消息之间的相对时间保持不变。
	payload := buildAggregateSub(nil, msgAudio, 1000, []byte{1}, true)
	payload = buildAggregateSub(payload, msgVideo, 1040, []byte{2}, true)
	payload = buildAggregateSub(payload, msgVideo, 1080, []byte{3}, true)

	subs, err := splitAggregate(payload, 5000)
	if err != nil {
		t.Fatalf("splitAggregate: %v", err)
	}
	want := []uint32{5000, 5040, 5080}
	for i, ts := range want {
		if subs[i].Timestamp != ts {
			t.Fatalf("sub%d timestamp = %d, want %d", i, subs[i].Timestamp, ts)
		}
	}
}

func TestSplitAggregateExtendedTimestamp(t *testing.T) {
	const ts = uint32(0x01000040) // 需要 4 字节扩展时间戳
	payload := buildAggregateSub(nil, msgVideo, ts, []byte{0xAA}, true)

	subs, err := splitAggregate(payload, ts)
	if err != nil {
		t.Fatalf("splitAggregate: %v", err)
	}
	if len(subs) != 1 || subs[0].Timestamp != ts {
		t.Fatalf("subs = %+v, want timestamp %d", subs, ts)
	}
}

func TestSplitAggregateEmpty(t *testing.T) {
	for _, payload := range [][]byte{nil, {}} {
		subs, err := splitAggregate(payload, 0)
		if err != nil {
			t.Fatalf("splitAggregate(empty): %v", err)
		}
		if len(subs) != 0 {
			t.Fatalf("subs = %d, want 0", len(subs))
		}
	}
}

func TestSplitAggregateLastWithoutBackPointer(t *testing.T) {
	// 老实现可能省略最后一条的 back pointer；剩余 0 字节时应正常结束。
	payload := buildAggregateSub(nil, msgAudio, 0, []byte{1, 2}, false)
	subs, err := splitAggregate(payload, 0)
	if err != nil {
		t.Fatalf("splitAggregate: %v", err)
	}
	if len(subs) != 1 || !bytes.Equal(subs[0].Payload, []byte{1, 2}) {
		t.Fatalf("subs = %+v", subs)
	}
}

func TestSplitAggregateZeroLengthPayload(t *testing.T) {
	payload := buildAggregateSub(nil, msgDataAMF0, 0, nil, true)
	payload = buildAggregateSub(payload, msgVideo, 40, []byte{0x17}, true)
	subs, err := splitAggregate(payload, 0)
	if err != nil {
		t.Fatalf("splitAggregate: %v", err)
	}
	if len(subs) != 2 || len(subs[0].Payload) != 0 || subs[0].TypeID != msgDataAMF0 {
		t.Fatalf("subs = %+v", subs)
	}
	if subs[1].Timestamp != 40 || len(subs[1].Payload) != 1 {
		t.Fatalf("sub1 = %+v", subs[1])
	}
}

func TestSplitAggregateErrors(t *testing.T) {
	valid := buildAggregateSub(nil, msgVideo, 0, []byte{1, 2, 3}, true)

	cases := []struct {
		name    string
		payload []byte
		want    string
	}{
		{"truncated-header", []byte{msgVideo, 0, 0, 5}, "truncated header"},
		{"length-overflow", []byte{msgVideo, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 1, 2}, "exceeds remaining"},
		{"truncated-backpointer", valid[:len(valid)-1], "truncated back pointer"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := splitAggregate(tc.payload, 0)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestHandleAggregateDispatches(t *testing.T) {
	_, c, st := newPublishConn(t)
	st.EnsureVideo()
	rd, cancel := st.SourceFor("video").Subscribe()
	defer cancel()

	// 两个视频子消息，DTS 40/80，各自带普通视频 tag。
	agg := buildAggregateSub(nil, msgVideo, 40,
		flv.BuildVideoTag([]byte{0x65, 0x01}, true, 0), true)
	agg = buildAggregateSub(agg, msgVideo, 80,
		flv.BuildVideoTag([]byte{0x41, 0x02}, false, 0), true)

	if err := c.handleAggregate(&Message{
		TypeID:    msgAggregate,
		StreamID:  7,
		Timestamp: 40,
		Payload:   agg,
	}); err != nil {
		t.Fatalf("handleAggregate: %v", err)
	}

	first := nextFrame(t, rd)
	second := nextFrame(t, rd)
	if !bytes.Equal(first.Data, []byte{0x65, 0x01}) || first.Timestamp != 3600 || !first.Keyframe {
		t.Fatalf("first = %+v", first)
	}
	if !bytes.Equal(second.Data, []byte{0x41, 0x02}) || second.Timestamp != 7200 || second.Keyframe {
		t.Fatalf("second = %+v", second)
	}
}

package rtcp

import (
	"encoding/binary"
	"reflect"
	"testing"
	"time"
)

func TestNACKRoundTrip(t *testing.T) {
	seqs := []uint16{10, 11, 12, 30, 65535}
	b := NACK(0x11223344, 0xAABBCCDD, seqs)
	if len(b) != NACKHeaderSize+4*3 {
		t.Fatalf("len = %d, want %d (three FCIs)", len(b), NACKHeaderSize+4*3)
	}
	if b[0] != 0x81 || b[1] != RTPFBType {
		t.Fatalf("header = %02x %02x, want 81 %d", b[0], b[1], RTPFBType)
	}
	if got := binary.BigEndian.Uint16(b[2:4]); got != 5 {
		t.Fatalf("length = %d, want 5", got)
	}
	// PID=10 覆盖 11、12；30、65535 各占一个 FCI。
	if pid, blp := binary.BigEndian.Uint16(b[12:14]), binary.BigEndian.Uint16(b[14:16]); pid != 10 || blp != 0b11 {
		t.Fatalf("fci0 = pid %d blp %#x", pid, blp)
	}
	if pid, blp := binary.BigEndian.Uint16(b[16:18]), binary.BigEndian.Uint16(b[18:20]); pid != 30 || blp != 0 {
		t.Fatalf("fci1 = pid %d blp %#x", pid, blp)
	}
	if pid, blp := binary.BigEndian.Uint16(b[20:22]), binary.BigEndian.Uint16(b[22:24]); pid != 65535 || blp != 0 {
		t.Fatalf("fci2 = pid %d blp %#x", pid, blp)
	}

	sender, media, got, ok := ParseNACK(b)
	if !ok || sender != 0x11223344 || media != 0xAABBCCDD {
		t.Fatalf("parsed sender=%08x media=%08x ok=%v", sender, media, ok)
	}
	if !sameSeqSet(got, seqs) {
		t.Fatalf("parsed seqs = %v, want %v", got, seqs)
	}
}

func TestNACKSequenceWrap(t *testing.T) {
	seqs := []uint16{65534, 65535, 0, 1}
	b := NACK(1, 2, seqs)
	_, _, got, ok := ParseNACK(b)
	if !ok {
		t.Fatal("ParseNACK failed")
	}
	if !sameSeqSet(got, seqs) {
		t.Fatalf("parsed seqs = %v, want %v", got, seqs)
	}
}

func TestNACKRejectsInvalid(t *testing.T) {
	good := NACK(1, 2, []uint16{5})
	cases := []struct {
		name string
		b    []byte
	}{
		{"empty", nil},
		{"header only", good[:11]},
		{"truncated fci", good[:len(good)-2]},
		{"bad version", mutate(good, 0, 0x41)},
		{"bad pt", mutate(good, 1, 200)},
		{"bad fmt", mutate(good, 0, 0x82)},
	}
	for _, c := range cases {
		if _, _, _, ok := ParseNACK(c.b); ok {
			t.Fatalf("%s accepted", c.name)
		}
	}
}

func TestNACKEmpty(t *testing.T) {
	if b := NACK(1, 2, nil); b != nil {
		t.Fatalf("empty NACK = % x, want nil", b)
	}
	if b := NACK(1, 2, []uint16{}); b != nil {
		t.Fatalf("empty NACK = % x, want nil", b)
	}
}

func TestNACKFCICap(t *testing.T) {
	seqs := make([]uint16, 0, 200)
	for i := range 200 {
		seqs = append(seqs, uint16(i*3)) // 每个都隔开，无法用 BLP 合并
	}
	b := NACK(1, 2, seqs)
	if len(b) != NACKHeaderSize+4*maxNACKFCIs {
		t.Fatalf("len = %d, want %d", len(b), NACKHeaderSize+4*maxNACKFCIs)
	}
	if _, _, _, ok := ParseNACK(b); !ok {
		t.Fatal("capped NACK does not parse")
	}
}

func mutate(b []byte, i int, v byte) []byte {
	out := append([]byte(nil), b...)
	out[i] = v
	return out
}

func sameSeqSet(a, b []uint16) bool {
	set := func(s []uint16) map[uint16]int {
		m := make(map[uint16]int, len(s))
		for _, v := range s {
			m[v]++
		}
		return m
	}
	return reflect.DeepEqual(set(a), set(b))
}

func TestNACKTrackerGap(t *testing.T) {
	tr := NewNACKTracker()
	t0 := time.Unix(100, 0)
	if tr.Observe(1, 0xAA, t0) {
		t.Fatal("first Observe reported SSRC change")
	}
	tr.Observe(4, 0xAA, t0) // 缺 2、3
	if got := tr.Pending(t0.Add(tr.Delay() - time.Millisecond)); got != nil {
		t.Fatalf("NACK before grace = %v", got)
	}
	got := tr.Pending(t0.Add(tr.Delay()))
	if !reflect.DeepEqual(got, []uint16{2, 3}) {
		t.Fatalf("pending = %v, want [2 3]", got)
	}
	if tr.MediaSSRC() != 0xAA {
		t.Fatalf("media ssrc = %08x", tr.MediaSSRC())
	}
	if out := tr.Outstanding(); out != 2 {
		t.Fatalf("outstanding = %d, want 2", out)
	}
}

func TestNACKTrackerRetransmitCancels(t *testing.T) {
	tr := NewNACKTracker()
	t0 := time.Unix(100, 0)
	tr.Observe(1, 0xAA, t0)
	tr.Observe(4, 0xAA, t0)
	tr.Observe(2, 0xAA, t0.Add(time.Millisecond)) // 重传到达
	tr.Observe(3, 0xAA, t0.Add(time.Millisecond))
	if got := tr.Pending(t0.Add(time.Second)); got != nil {
		t.Fatalf("pending after retransmit = %v", got)
	}
	if tr.Outstanding() != 0 {
		t.Fatalf("outstanding = %d, want 0", tr.Outstanding())
	}
}

func TestNACKTrackerReorderWithinGrace(t *testing.T) {
	tr := NewNACKTracker()
	t0 := time.Unix(100, 0)
	tr.Observe(1, 0xAA, t0)
	tr.Observe(3, 0xAA, t0.Add(2*time.Millisecond))
	tr.Observe(2, 0xAA, t0.Add(5*time.Millisecond))
	if got := tr.Pending(t0.Add(time.Second)); got != nil {
		t.Fatalf("pending for reordered packet = %v", got)
	}
}

func TestNACKTrackerRateLimitAndRetries(t *testing.T) {
	tr := NewNACKTracker()
	t0 := time.Unix(100, 0)
	tr.delay = 10 * time.Millisecond
	tr.minInterval = 10 * time.Millisecond
	tr.maxAttempts = 3
	tr.Observe(1, 0xAA, t0)
	tr.Observe(3, 0xAA, t0)

	first := tr.Pending(t0.Add(10 * time.Millisecond))
	if !reflect.DeepEqual(first, []uint16{2}) {
		t.Fatalf("first = %v", first)
	}
	if got := tr.Pending(t0.Add(15 * time.Millisecond)); got != nil {
		t.Fatalf("pending within min interval = %v", got)
	}
	second := tr.Pending(t0.Add(20 * time.Millisecond))
	if !reflect.DeepEqual(second, []uint16{2}) {
		t.Fatalf("second = %v", second)
	}
	third := tr.Pending(t0.Add(30 * time.Millisecond))
	if !reflect.DeepEqual(third, []uint16{2}) {
		t.Fatalf("third = %v", third)
	}
	// 第 4 次：达到上限，直接放弃，不再返回。
	if got := tr.Pending(t0.Add(40 * time.Millisecond)); got != nil {
		t.Fatalf("pending after max attempts = %v", got)
	}
	if tr.Outstanding() != 0 {
		t.Fatalf("outstanding = %d, want 0", tr.Outstanding())
	}
}

func TestNACKTrackerSSRCChange(t *testing.T) {
	tr := NewNACKTracker()
	t0 := time.Unix(100, 0)
	tr.Observe(1, 0xAA, t0)
	tr.Observe(4, 0xAA, t0)
	if !tr.Observe(100, 0xBB, t0) {
		t.Fatal("SSRC change not reported")
	}
	if tr.MediaSSRC() != 0xBB {
		t.Fatalf("media ssrc = %08x, want 000000bb", tr.MediaSSRC())
	}
	if got := tr.Pending(t0.Add(time.Second)); got != nil {
		t.Fatalf("pending across SSRC change = %v", got)
	}
}

func TestNACKTrackerWrap(t *testing.T) {
	tr := NewNACKTracker()
	t0 := time.Unix(100, 0)
	tr.Observe(65534, 0xAA, t0)
	tr.Observe(1, 0xAA, t0) // 缺 65535、0
	got := tr.Pending(t0.Add(tr.Delay()))
	if !reflect.DeepEqual(got, []uint16{0, 65535}) {
		t.Fatalf("pending = %v, want [0 65535]", got)
	}
}

func TestNACKTrackerDuplicateAndCap(t *testing.T) {
	tr := NewNACKTracker()
	t0 := time.Unix(100, 0)
	tr.Observe(5, 0xAA, t0)
	tr.Observe(5, 0xAA, t0) // 重复，不产生缺口
	if tr.Outstanding() != 0 {
		t.Fatalf("duplicate produced %d gaps", tr.Outstanding())
	}

	tr.maxMissing = 4
	tr.Observe(100, 0xAA, t0) // 中间 94 个缺口，只保留 4 个
	if tr.Outstanding() != 4 {
		t.Fatalf("outstanding = %d, want cap 4", tr.Outstanding())
	}
}

func TestNACKTrackerMaxPerPacket(t *testing.T) {
	tr := NewNACKTracker()
	t0 := time.Unix(100, 0)
	tr.maxPerPacket = 2
	tr.Observe(1, 0xAA, t0)
	tr.Observe(10, 0xAA, t0)
	got := tr.Pending(t0.Add(tr.Delay()))
	if len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("pending = %v, want first two", got)
	}
}

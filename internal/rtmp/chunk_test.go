package rtmp

import (
	"bufio"
	"bytes"
	"net"
	"testing"
)

// newReaderConn 构造一个只读的 Conn，br 直接读 raw 字节（readMessage 只用 br/chunkSize/chunks）。
func newReaderConn(t *testing.T, raw []byte) *Conn {
	t.Helper()
	return &Conn{
		br:             bufio.NewReader(bytes.NewReader(raw)),
		readChunkSize:  defaultChunkSize,
		writeChunkSize: defaultChunkSize,
		chunks:         make(map[uint32]*chunkStream),
	}
}

// pipeConns 建一对 net.Pipe 包装的 Conn，用于 writeMessage/readMessage 往返。
func pipeConns(t *testing.T) (server, client *Conn) {
	t.Helper()
	s, c := net.Pipe()
	t.Cleanup(func() { _ = s.Close(); _ = c.Close() })
	return newConn(s), newConn(c)
}

// writeThenRead 在 goroutine 里写、主流程读，避免 net.Pipe 同步阻塞。
func writeThenRead(t *testing.T, w, r *Conn, m *Message, csid uint32) *Message {
	t.Helper()
	errc := make(chan error, 1)
	go func() { errc <- w.writeMessage(m, csid) }()
	got, err := r.readMessage()
	if err != nil {
		t.Fatalf("readMessage: %v", err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("writeMessage: %v", err)
	}
	return got
}

func TestEncodeBasicHeader(t *testing.T) {
	cases := []struct {
		csid uint32
		want []byte
	}{
		{2, []byte{0x02}},         // 1 字节下界
		{63, []byte{0x3F}},        // 1 字节上界
		{64, []byte{0x00, 0x00}},  // 2 字节下界
		{319, []byte{0x00, 0xFF}}, // 2 字节上界
		{320, []byte{0x01, 0x01, 0x00}},
		{65599, []byte{0x01, 0xFF, 0xFF}},
	}
	for _, c := range cases {
		got := encodeBasicHeader(0, c.csid) // fmt 0
		if !bytes.Equal(got, c.want) {
			t.Fatalf("encodeBasicHeader(%d) = % x, want % x", c.csid, got, c.want)
		}
		// 反向 decode。
		dec, fmt, err := newReaderConn(t, got).readBasicHeader()
		if err != nil {
			t.Fatalf("readBasicHeader(% x): %v", got, err)
		}
		if dec != c.csid || fmt != 0 {
			t.Fatalf("decoded csid=%d fmt=%d, want %d/0", dec, fmt, c.csid)
		}
	}
}

func TestBasicHeaderFmtBits(t *testing.T) {
	for _, fmt := range []uint8{0, 1, 2, 3} {
		raw := encodeBasicHeader(fmt, 5)
		conn := newReaderConn(t, raw)
		_, got, err := conn.readBasicHeader()
		if err != nil {
			t.Fatal(err)
		}
		if got != fmt {
			t.Fatalf("fmt = %d, want %d", got, fmt)
		}
	}
}

func TestWriteReadRoundTrip(t *testing.T) {
	server, client := pipeConns(t)
	want := &Message{TypeID: 20, StreamID: 1, Timestamp: 1234, Payload: []byte("hello")}
	got := writeThenRead(t, server, client, want, 3)
	if got.TypeID != want.TypeID || got.StreamID != want.StreamID ||
		got.Timestamp != want.Timestamp || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("message = %+v, want %+v", got, want)
	}
}

func TestCrossChunkReassembly(t *testing.T) {
	server, client := pipeConns(t)
	payload := bytes.Repeat([]byte{0xAB}, 300) // 超过默认 128
	want := &Message{TypeID: 20, StreamID: 0, Timestamp: 0, Payload: payload}
	got := writeThenRead(t, server, client, want, 4)
	if len(got.Payload) != 300 || !bytes.Equal(got.Payload, payload) {
		t.Fatalf("reassembled len = %d, want 300", len(got.Payload))
	}
}

func TestReadFmt1DeltaTimestamp(t *testing.T) {
	// fmt0: ts=1000 len=4 type=20 streamID=0 "AAAA"
	// fmt1: delta=10 len=3 type=20 "BBB"（streamID 继承）
	raw := []byte{}
	raw = append(raw, encodeBasicHeader(0, 5)...)
	raw = appendMessageHeaderFmt0(raw, 1000, 4, 20, 0)
	raw = append(raw, []byte("AAAA")...)

	raw = append(raw, encodeBasicHeader(1, 5)...)
	raw = appendUint24(raw, 10)
	raw = appendUint24(raw, 3)
	raw = append(raw, 20)
	raw = append(raw, []byte("BBB")...)

	c := newReaderConn(t, raw)
	m1, err := c.readMessage()
	if err != nil {
		t.Fatal(err)
	}
	if m1.Timestamp != 1000 || m1.StreamID != 0 || string(m1.Payload) != "AAAA" {
		t.Fatalf("m1 = %+v", m1)
	}
	m2, err := c.readMessage()
	if err != nil {
		t.Fatal(err)
	}
	if m2.Timestamp != 1010 || m2.StreamID != 0 || m2.TypeID != 20 || string(m2.Payload) != "BBB" {
		t.Fatalf("m2 = %+v", m2)
	}
}

func TestReadFmt3Continuation(t *testing.T) {
	// 一条 200 字节的消息，128 + 72 两个 chunk（fmt0 + fmt3）。
	payload := bytes.Repeat([]byte{0xCD}, 200)
	raw := []byte{}
	raw = append(raw, encodeBasicHeader(0, 5)...)
	raw = appendMessageHeaderFmt0(raw, 0, 200, 20, 0)
	raw = append(raw, payload[:128]...)
	raw = append(raw, encodeBasicHeader(3, 5)...) // 续片
	raw = append(raw, payload[128:]...)

	c := newReaderConn(t, raw)
	m, err := c.readMessage()
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Payload) != 200 || !bytes.Equal(m.Payload, payload) {
		t.Fatalf("payload len = %d, want 200", len(m.Payload))
	}
}

func TestReadFmt3NewMessageAfterDelta(t *testing.T) {
	// fmt0(ts=1000, len=4) → fmt1(delta=10) → fmt3 新消息(继承 len=4，ts 再 +10)。
	raw := []byte{}
	raw = append(raw, encodeBasicHeader(0, 5)...)
	raw = appendMessageHeaderFmt0(raw, 1000, 4, 20, 0)
	raw = append(raw, []byte("AAAA")...)

	raw = append(raw, encodeBasicHeader(1, 5)...)
	raw = appendUint24(raw, 10)
	raw = appendUint24(raw, 4)
	raw = append(raw, 20)
	raw = append(raw, []byte("BBBB")...)

	raw = append(raw, encodeBasicHeader(3, 5)...)
	raw = append(raw, []byte("CCCC")...)

	c := newReaderConn(t, raw)
	_, _ = c.readMessage() // AAAA @1000
	_, _ = c.readMessage() // BBBB @1010
	m3, err := c.readMessage()
	if err != nil {
		t.Fatal(err)
	}
	if m3.Timestamp != 1020 || string(m3.Payload) != "CCCC" || m3.TypeID != 20 {
		t.Fatalf("m3 = %+v, want ts=1020 CCCC type=20", m3)
	}
}

func TestExtendedTimestamp(t *testing.T) {
	// 手写 fmt0：ts 字段 0xFFFFFF，4 字节扩展 0x01020304 在完整 header 之后
	// （stream id 之后），与 ffmpeg 的布局一致。
	raw := []byte{}
	raw = append(raw, encodeBasicHeader(0, 5)...)
	raw = appendUint24(raw, 0xFFFFFF) // ts 字段
	raw = appendUint24(raw, 2)        // length
	raw = append(raw, 20)             // type
	raw = appendUint32LE(raw, 0)      // streamID
	raw = appendUint32(raw, 0x01020304)
	raw = append(raw, []byte("xy")...)

	c := newReaderConn(t, raw)
	m, err := c.readMessage()
	if err != nil {
		t.Fatal(err)
	}
	if m.Timestamp != 0x01020304 {
		t.Fatalf("timestamp = %#x, want 0x01020304", m.Timestamp)
	}

	// write 侧：大时间戳自动用扩展时间戳。
	server, client := pipeConns(t)
	want := &Message{TypeID: 20, StreamID: 0, Timestamp: 0x01020304, Payload: []byte("x")}
	got := writeThenRead(t, server, client, want, 3)
	if got.Timestamp != want.Timestamp {
		t.Fatalf("round-trip timestamp = %#x, want %#x", got.Timestamp, want.Timestamp)
	}
}

func TestExtendedTimestampContinuation(t *testing.T) {
	// 带扩展时间戳的 200 字节消息：fmt0 头 + 4 字节扩展，续片 fmt3 也重复扩展。
	payload := bytes.Repeat([]byte{0x5A}, 200)
	raw := []byte{}
	raw = append(raw, encodeBasicHeader(0, 5)...)
	raw = appendUint24(raw, 0xFFFFFF)
	raw = appendUint24(raw, 200)
	raw = append(raw, 20)
	raw = appendUint32LE(raw, 0)
	raw = appendUint32(raw, 0x01020304)
	raw = append(raw, payload[:128]...)
	raw = append(raw, encodeBasicHeader(3, 5)...)
	raw = appendUint32(raw, 0x01020304)
	raw = append(raw, payload[128:]...)

	c := newReaderConn(t, raw)
	m, err := c.readMessage()
	if err != nil {
		t.Fatal(err)
	}
	if m.Timestamp != 0x01020304 || !bytes.Equal(m.Payload, payload) {
		t.Fatalf("m = ts %#x len %d, want 0x01020304/%d", m.Timestamp, len(m.Payload), len(payload))
	}

	// write 侧往返：跨 chunk 的扩展时间戳消息。
	server, client := pipeConns(t)
	want := &Message{TypeID: 20, StreamID: 0, Timestamp: 0x01020304, Payload: payload}
	got := writeThenRead(t, server, client, want, 3)
	if got.Timestamp != want.Timestamp || !bytes.Equal(got.Payload, payload) {
		t.Fatalf("round-trip ts=%#x len=%d", got.Timestamp, len(got.Payload))
	}
}

func TestSetChunkSizeAffectsReassembly(t *testing.T) {
	// 读端 chunk size 设为 200，发送端按 200 拆片（200 + 200 + 100）。
	payload := bytes.Repeat([]byte{0xEF}, 500)
	raw := []byte{}
	raw = append(raw, encodeBasicHeader(0, 5)...)
	raw = appendMessageHeaderFmt0(raw, 0, 500, 20, 0)
	raw = append(raw, payload[:200]...)
	raw = append(raw, encodeBasicHeader(3, 5)...)
	raw = append(raw, payload[200:400]...)
	raw = append(raw, encodeBasicHeader(3, 5)...)
	raw = append(raw, payload[400:]...)

	c := newReaderConn(t, raw)
	if err := c.setReadChunkSize(200); err != nil {
		t.Fatal(err)
	}
	m, err := c.readMessage()
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Payload) != 500 || !bytes.Equal(m.Payload, payload) {
		t.Fatalf("payload len = %d, want 500", len(m.Payload))
	}
}

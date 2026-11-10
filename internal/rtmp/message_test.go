package rtmp

import (
	"bytes"
	"testing"
)

// writeControlThenRead 在 goroutine 里执行一次控制消息发送，主流程读回消息。
func writeControlThenRead(t *testing.T, w, r *Conn, send func(*Conn) error) *Message {
	t.Helper()
	errc := make(chan error, 1)
	go func() { errc <- send(w) }()
	got, err := r.readMessage()
	if err != nil {
		t.Fatalf("readMessage: %v", err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("send: %v", err)
	}
	return got
}

func TestMessageTypeIDs(t *testing.T) {
	// 这些数值是协议常量，不能改。
	cases := map[uint8]uint8{
		msgSetChunkSize:     1,
		msgAbort:            2,
		msgAck:              3,
		msgUserControl:      4,
		msgWindowAckSize:    5,
		msgSetPeerBandwidth: 6,
		msgAudio:            8,
		msgVideo:            9,
		msgDataAMF3:         15,
		msgCommandAMF3:      17,
		msgDataAMF0:         18,
		msgCommandAMF0:      20,
	}
	for got, want := range cases {
		if got != want {
			t.Fatalf("message type id = %d, want %d", got, want)
		}
	}
}

func TestControlMessageBytes(t *testing.T) {
	cases := []struct {
		name string
		got  []byte
		want []byte
	}{
		{"set chunk size", buildSetChunkSize(0x01020304), []byte{1, 2, 3, 4}},
		{"abort", buildAbort(5), []byte{0, 0, 0, 5}},
		{"ack", buildAck(0xAABBCCDD), []byte{0xAA, 0xBB, 0xCC, 0xDD}},
		{"window ack size", buildWindowAckSize(2500000), []byte{0x00, 0x26, 0x25, 0xA0}},
		{"set peer bandwidth", buildSetPeerBandwidth(2500000, bandwidthDynamic),
			[]byte{0x00, 0x26, 0x25, 0xA0, 2}},
	}
	for _, c := range cases {
		if !bytes.Equal(c.got, c.want) {
			t.Fatalf("%s = % x, want % x", c.name, c.got, c.want)
		}
	}
}

func TestControlMessagesRoundTrip(t *testing.T) {
	cases := []struct {
		name   string
		typeID uint8
		send   func(*Conn) error
		check  func(*testing.T, *Message)
	}{
		{"set chunk size", msgSetChunkSize,
			func(c *Conn) error { return c.writeSetChunkSize(4096) },
			func(t *testing.T, m *Message) {
				if n, err := controlUint32(m.Payload); err != nil || n != 4096 {
					t.Fatalf("chunk size = %d, %v", n, err)
				}
			}},
		{"abort", msgAbort,
			func(c *Conn) error { return c.writeAbort(7) },
			func(t *testing.T, m *Message) {
				if csid, err := controlUint32(m.Payload); err != nil || csid != 7 {
					t.Fatalf("abort csid = %d, %v", csid, err)
				}
			}},
		{"ack", msgAck,
			func(c *Conn) error { return c.writeAck(123456) },
			func(t *testing.T, m *Message) {
				if seq, err := controlUint32(m.Payload); err != nil || seq != 123456 {
					t.Fatalf("ack seq = %d, %v", seq, err)
				}
			}},
		{"window ack size", msgWindowAckSize,
			func(c *Conn) error { return c.writeWindowAckSize(2500000) },
			func(t *testing.T, m *Message) {
				if n, err := controlUint32(m.Payload); err != nil || n != 2500000 {
					t.Fatalf("window = %d, %v", n, err)
				}
			}},
		{"set peer bandwidth", msgSetPeerBandwidth,
			func(c *Conn) error { return c.writeSetPeerBandwidth(2500000, bandwidthSoft) },
			func(t *testing.T, m *Message) {
				n, limit, err := parseSetPeerBandwidth(m.Payload)
				if err != nil || n != 2500000 || limit != bandwidthSoft {
					t.Fatalf("peer bandwidth = %d/%d, %v", n, limit, err)
				}
			}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			server, client := pipeConns(t)
			m := writeControlThenRead(t, server, client, c.send)
			if m.TypeID != c.typeID {
				t.Fatalf("type id = %d, want %d", m.TypeID, c.typeID)
			}
			if m.StreamID != controlStreamID || m.Timestamp != 0 {
				t.Fatalf("stream id = %d ts = %d, want %d/0", m.StreamID, m.Timestamp, controlStreamID)
			}
			c.check(t, m)
		})
	}
}

func TestHandleSetChunkSize(t *testing.T) {
	server, client := pipeConns(t)
	_ = server

	m := &Message{TypeID: msgSetChunkSize, Payload: buildSetChunkSize(4096)}
	handled, err := client.handleControl(m)
	if err != nil || !handled {
		t.Fatalf("handleControl = %v/%v, want true/nil", handled, err)
	}
	if client.readChunkSize != 4096 {
		t.Fatalf("readChunkSize = %d, want 4096", client.readChunkSize)
	}

	// 0 和超上限都非法，且不能改动当前值。
	for _, n := range []uint32{0, maxChunkSize + 1} {
		_, err := client.handleControl(&Message{TypeID: msgSetChunkSize, Payload: buildSetChunkSize(n)})
		if err == nil {
			t.Fatalf("accepted invalid chunk size %d", n)
		}
	}
	if client.readChunkSize != 4096 {
		t.Fatalf("invalid size changed readChunkSize to %d", client.readChunkSize)
	}

	// 负载不足 4 字节报错。
	if _, err := client.handleControl(&Message{TypeID: msgSetChunkSize, Payload: []byte{1, 2, 3}}); err == nil {
		t.Fatal("accepted short set chunk size payload")
	}
}

func TestHandleAbort(t *testing.T) {
	server, client := pipeConns(t)
	_ = server

	// 制造一条只收了一半的消息。
	st := client.chunkState(6)
	st.length = 100
	st.typeID = msgVideo
	st.streamID = 1
	st.buf = []byte{1, 2, 3}
	st.received = 3

	handled, err := client.handleControl(&Message{TypeID: msgAbort, Payload: buildAbort(6)})
	if err != nil || !handled {
		t.Fatalf("handleControl = %v/%v, want true/nil", handled, err)
	}
	if st.buf != nil || st.received != 0 {
		t.Fatalf("abort left buf=%v received=%d", st.buf, st.received)
	}
	// header 字段保留，后续 fmt 3 仍可继承。
	if st.length != 100 || st.typeID != msgVideo || st.streamID != 1 {
		t.Fatalf("abort cleared header: length=%d type=%d stream=%d", st.length, st.typeID, st.streamID)
	}
	// 其他 CSID 不受影响。
	if other := client.chunkState(7); other.buf != nil {
		t.Fatal("abort touched another csid")
	}
}

func TestHandleAckAndWindow(t *testing.T) {
	server, client := pipeConns(t)
	_ = server

	handled, err := client.handleControl(&Message{TypeID: msgWindowAckSize, Payload: buildWindowAckSize(2500000)})
	if err != nil || !handled || client.peerAckWindow != 2500000 {
		t.Fatalf("window ack size: handled=%v err=%v window=%d", handled, err, client.peerAckWindow)
	}

	handled, err = client.handleControl(&Message{TypeID: msgAck, Payload: buildAck(98765)})
	if err != nil || !handled || client.lastAckSeq != 98765 {
		t.Fatalf("ack: handled=%v err=%v seq=%d", handled, err, client.lastAckSeq)
	}

	handled, err = client.handleControl(&Message{TypeID: msgSetPeerBandwidth, Payload: buildSetPeerBandwidth(3000000, bandwidthHard)})
	if err != nil || !handled || client.peerBandwidth != 3000000 {
		t.Fatalf("set peer bandwidth: handled=%v err=%v bw=%d", handled, err, client.peerBandwidth)
	}

	// Set Peer Bandwidth 负载至少 5 字节。
	if _, err := client.handleControl(&Message{TypeID: msgSetPeerBandwidth, Payload: buildSetPeerBandwidth(1, bandwidthHard)[:4]}); err == nil {
		t.Fatal("accepted short set peer bandwidth payload")
	}
}

func TestHandleControlIgnoresNonControl(t *testing.T) {
	server, client := pipeConns(t)
	_ = server

	for _, typeID := range []uint8{msgAudio, msgVideo, msgDataAMF0, msgCommandAMF0, msgUserControl} {
		handled, err := client.handleControl(&Message{TypeID: typeID, Payload: []byte{0, 0, 0, 1}})
		if err != nil || handled {
			t.Fatalf("type %d: handled=%v err=%v, want false/nil", typeID, handled, err)
		}
	}
}

func TestWriteSetChunkSizeAppliesAfterSend(t *testing.T) {
	server, client := pipeConns(t)

	m := writeControlThenRead(t, server, client, func(c *Conn) error {
		return c.writeSetChunkSize(60000)
	})
	// 发送端在消息发出后才切换大小。
	if server.writeChunkSize != 60000 {
		t.Fatalf("writeChunkSize = %d, want 60000", server.writeChunkSize)
	}
	// 接收端只有处理了这条消息才切换。
	if client.readChunkSize != defaultChunkSize {
		t.Fatalf("readChunkSize = %d before handling, want %d", client.readChunkSize, defaultChunkSize)
	}
	if _, err := client.handleControl(m); err != nil {
		t.Fatal(err)
	}
	if client.readChunkSize != 60000 {
		t.Fatalf("readChunkSize = %d, want 60000", client.readChunkSize)
	}
}

func TestWriteSetChunkSizeRejectsInvalid(t *testing.T) {
	server, client := pipeConns(t)
	_ = client
	for _, n := range []uint32{0, maxChunkSize + 1} {
		if err := server.writeSetChunkSize(n); err == nil {
			t.Fatalf("writeSetChunkSize(%d) accepted invalid size", n)
		}
	}
	if server.writeChunkSize != defaultChunkSize {
		t.Fatalf("writeChunkSize = %d, want unchanged %d", server.writeChunkSize, defaultChunkSize)
	}
}

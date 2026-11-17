package rtmp

import (
	"bytes"
	"testing"

	"flow/internal/flv"
	"flow/internal/stream"
)

// buildMetaData 构造一条 onMetaData 数据消息负载（ECMA Array 体）。
func buildMetaData(fields ...flv.Member) []byte {
	var buf bytes.Buffer
	_ = flv.WriteAMF0(&buf, flv.Str("onMetaData"))
	_ = flv.WriteAMF0(&buf, flv.ECMA(fields...))
	return buf.Bytes()
}

func TestParseMetaData(t *testing.T) {
	payload := buildMetaData(
		flv.Elem("width", flv.Num(640)),
		flv.Elem("height", flv.Num(480)),
		flv.Elem("videocodecid", flv.Num(7)),
		flv.Elem("encoder", flv.Str("Lavf")),
	)
	name, fields, err := parseMetaData(payload)
	if err != nil {
		t.Fatalf("parseMetaData: %v", err)
	}
	if name != "onMetaData" {
		t.Fatalf("name = %q, want onMetaData", name)
	}
	if len(fields) != 4 {
		t.Fatalf("fields = %d, want 4", len(fields))
	}
	if fields[0].Key != "width" || fields[0].Value.Number != 640 {
		t.Fatalf("fields[0] = %+v", fields[0])
	}
}

func TestParseMetaDataObjectBody(t *testing.T) {
	// 有些推流端用普通 Object（0x03）而非 ECMA Array。
	var buf bytes.Buffer
	_ = flv.WriteAMF0(&buf, flv.Str("onMetaData"))
	_ = flv.WriteAMF0(&buf, flv.Obj(flv.Elem("width", flv.Num(320))))

	name, fields, err := parseMetaData(buf.Bytes())
	if err != nil || name != "onMetaData" || len(fields) != 1 {
		t.Fatalf("parseMetaData = %q/%d/%v", name, len(fields), err)
	}
}

func TestParseMetaDataNonObject(t *testing.T) {
	// |RtmpSampleAccess 等数据消息：字符串 + 布尔，无对象体，应返回空字段不报错。
	var buf bytes.Buffer
	_ = flv.WriteAMF0(&buf, flv.Str("|RtmpSampleAccess"))
	_ = flv.WriteAMF0(&buf, flv.Bool(true))
	_ = flv.WriteAMF0(&buf, flv.Bool(false))

	name, fields, err := parseMetaData(buf.Bytes())
	if err != nil || name != "|RtmpSampleAccess" || len(fields) != 0 {
		t.Fatalf("parseMetaData = %q/%d/%v", name, len(fields), err)
	}
}

func TestParseMetaDataErrors(t *testing.T) {
	if _, _, err := parseMetaData(nil); err == nil {
		t.Fatal("empty payload should fail")
	}
	// 首值不是字符串。
	var notString bytes.Buffer
	_ = flv.WriteAMF0(&notString, flv.Num(1))
	if _, _, err := parseMetaData(notString.Bytes()); err == nil {
		t.Fatal("non-string name should fail")
	}
}

func TestHandleDataStoresOnPublish(t *testing.T) {
	srv, c, st := newPublishConn(t)
	_ = srv

	c.handleData(&Message{TypeID: msgDataAMF0, Payload: buildMetaData(
		flv.Elem("width", flv.Num(640)),
		flv.Elem("height", flv.Num(480)),
	)})

	md, ok := st.MetaData().(*metaData)
	if !ok || md == nil {
		t.Fatal("onMetaData was not stored")
	}
	if !bytes.Equal(md.payload, buildMetaData(flv.Elem("width", flv.Num(640)), flv.Elem("height", flv.Num(480)))) {
		t.Fatal("stored payload differs from input")
	}
	if len(md.fields) != 2 {
		t.Fatalf("fields = %d, want 2", len(md.fields))
	}

	// 后到的覆盖先到的。
	c.handleData(&Message{TypeID: msgDataAMF0, Payload: buildMetaData(
		flv.Elem("width", flv.Num(320)),
	)})
	if md = st.MetaData().(*metaData); len(md.fields) != 1 {
		t.Fatalf("overwrite fields = %d, want 1", len(md.fields))
	}
}

func TestHandleDataIgnores(t *testing.T) {
	srv, c, st := newPublishConn(t)
	_ = srv

	// 非 onMetaData 数据消息不保存。
	var buf bytes.Buffer
	_ = flv.WriteAMF0(&buf, flv.Str("|RtmpSampleAccess"))
	_ = flv.WriteAMF0(&buf, flv.Bool(true))
	c.handleData(&Message{TypeID: msgDataAMF0, Payload: buf.Bytes()})
	if st.MetaData() != nil {
		t.Fatal("non-onMetaData data message was stored")
	}

	// 没有 publish 状态的连接收到数据消息应静默忽略（不 panic）。
	server, _ := pipeConns(t)
	server.handleData(&Message{TypeID: msgDataAMF0, Payload: buildMetaData()})
}

// TestPlayForwardsMetaData 验证 Play.Start 之后、视频序列头之前先收到 onMetaData。
func TestPlayForwardsMetaData(t *testing.T) {
	srv := New(nil, nil, nil)
	st := srv.registry.Publish("live/meta", &stream.Stream{})
	st.SetVideoParams("H264", nil,
		[]byte{0x67, 0x42, 0x00, 0x1f}, []byte{0x68, 0xee})
	st.EnsureVideo()
	st.SetMetaData(&metaData{payload: buildMetaData(
		flv.Elem("width", flv.Num(640)),
		flv.Elem("height", flv.Num(480)),
		flv.Elem("videocodecid", flv.Num(7)),
	)})

	server, client := pipeConns(t)
	server.srv = srv

	cmd, err := parseCommand(buildCommand("play", 0, flv.Null(), flv.Str("live/meta")))
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- server.handlePlay(1, cmd) }()
	defer stopPlayer(t, server, client)

	if code := readStatusWithin(t, client); code != "NetStream.Play.Reset" {
		t.Fatalf("status = %q, want NetStream.Play.Reset", code)
	}
	if code := readStatusWithin(t, client); code != "NetStream.Play.Start" {
		t.Fatalf("status = %q, want NetStream.Play.Start", code)
	}

	// Play.Start 后的下一条消息应是 onMetaData（type 18）。net.Pipe 是同步的，
	// handlePlay 正阻塞在写这条消息上，必须在这里读掉它才能再等 errc。
	m := readMessageWithin(t, client)
	if m.TypeID != msgDataAMF0 || m.StreamID != 1 {
		t.Fatalf("message type/stream = %d/%d, want %d/1", m.TypeID, m.StreamID, msgDataAMF0)
	}
	name, fields, err := parseMetaData(m.Payload)
	if err != nil || name != "onMetaData" {
		t.Fatalf("metadata = %q/%v", name, err)
	}
	if len(fields) != 3 {
		t.Fatalf("fields = %d, want 3", len(fields))
	}

	if err := <-errc; err != nil {
		t.Fatalf("handlePlay: %v", err)
	}

	// 其后才是视频序列头。
	m = readMessageWithin(t, client)
	if m.TypeID != msgVideo {
		t.Fatalf("next message type = %d, want %d (video sequence header)", m.TypeID, msgVideo)
	}
}

// TestPlayNoMetaData 注册表流没有 onMetaData 时不下发数据消息，直接进序列头。
func TestPlayNoMetaData(t *testing.T) {
	srv := New(nil, nil, nil)
	st := srv.registry.Publish("live/nometa", &stream.Stream{})
	st.SetVideoParams("H264", nil,
		[]byte{0x67, 0x42, 0x00, 0x1f}, []byte{0x68, 0xee})
	st.EnsureVideo()

	server, client := pipeConns(t)
	server.srv = srv

	cmd, err := parseCommand(buildCommand("play", 0, flv.Null(), flv.Str("live/nometa")))
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- server.handlePlay(1, cmd) }()
	defer stopPlayer(t, server, client)

	readStatusWithin(t, client)
	readStatusWithin(t, client)
	if err := <-errc; err != nil {
		t.Fatalf("handlePlay: %v", err)
	}

	m := readMessageWithin(t, client)
	if m.TypeID != msgVideo {
		t.Fatalf("message type = %d, want %d (video sequence header)", m.TypeID, msgVideo)
	}
}

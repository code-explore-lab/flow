package rtmp

import (
	"bytes"
	"testing"

	"flow/internal/flv"
)

func TestParseCommandRoundTrip(t *testing.T) {
	payload := buildCommand("publish", 7, flv.Null(), flv.Str("push"), flv.Str("live"))
	cmd, err := parseCommand(payload)
	if err != nil {
		t.Fatalf("parseCommand: %v", err)
	}
	if cmd.Name != "publish" || cmd.TransactionID != 7 {
		t.Fatalf("name=%q tx=%v, want publish/7", cmd.Name, cmd.TransactionID)
	}
	if got := cmd.stringArg(0); got != "push" {
		t.Fatalf("stringArg(0) = %q, want push", got)
	}
	if got := cmd.stringArg(1); got != "live" {
		t.Fatalf("stringArg(1) = %q, want live", got)
	}
	if got := cmd.stringArg(2); got != "" {
		t.Fatalf("stringArg(2) = %q, want empty", got)
	}
}

func TestParseCommandObjectFields(t *testing.T) {
	payload := buildCommand("connect", 1, flv.Obj(
		flv.Elem("app", flv.Str("live")),
		flv.Elem("tcUrl", flv.Str("rtmp://127.0.0.1:1935/live")),
		flv.Elem("objectEncoding", flv.Num(0)),
	))
	cmd, err := parseCommand(payload)
	if err != nil {
		t.Fatalf("parseCommand: %v", err)
	}
	if app, ok := cmd.objectString("app"); !ok || app != "live" {
		t.Fatalf("objectString(app) = %q/%v, want live/true", app, ok)
	}
	if _, ok := cmd.objectString("missing"); ok {
		t.Fatal("objectString found a missing key")
	}
}

func TestParseCommandErrors(t *testing.T) {
	// 命令名不是字符串。
	var notString bytes.Buffer
	_ = flv.WriteAMF0(&notString, flv.Num(1))
	_ = flv.WriteAMF0(&notString, flv.Num(1))

	for name, payload := range map[string][]byte{
		"empty":        nil,
		"not string":   notString.Bytes(),
		"missing tx":   flvOnlyName(),
		"empty string": buildCommand("", 1),
	} {
		if _, err := parseCommand(payload); err == nil {
			t.Fatalf("%s: parseCommand accepted invalid payload", name)
		}
	}
}

// flvOnlyName 构造只有命令名、没有事务号的负载。
func flvOnlyName() []byte {
	var buf bytes.Buffer
	_ = flv.WriteAMF0(&buf, flv.Str("connect"))
	return buf.Bytes()
}

func TestHandleConnect(t *testing.T) {
	server, client := pipeConns(t)
	cmd, err := parseCommand(buildCommand("connect", 1, flv.Obj(
		flv.Elem("app", flv.Str("live")),
	)))
	if err != nil {
		t.Fatal(err)
	}

	errc := make(chan error, 1)
	go func() { errc <- server.handleConnect(cmd) }()

	// connect 依次下发 Window Ack Size / Set Peer Bandwidth / Set Chunk Size / _result。
	wantTypes := []uint8{msgWindowAckSize, msgSetPeerBandwidth, msgSetChunkSize, msgCommandAMF0}
	var msgs []*Message
	for range wantTypes {
		m, err := client.readMessage()
		if err != nil {
			t.Fatalf("readMessage: %v", err)
		}
		// 应用控制消息（Set Chunk Size 会让后续读取按新大小重组）。
		if _, err := client.handleControl(m); err != nil {
			t.Fatalf("handleControl: %v", err)
		}
		msgs = append(msgs, m)
	}
	if err := <-errc; err != nil {
		t.Fatalf("handleConnect: %v", err)
	}

	for i, want := range wantTypes {
		if msgs[i].TypeID != want {
			t.Fatalf("message %d type = %d, want %d", i, msgs[i].TypeID, want)
		}
	}
	if server.app != "live" {
		t.Fatalf("app = %q, want live", server.app)
	}
	if server.writeChunkSize != connectChunkSize {
		t.Fatalf("writeChunkSize = %d, want %d", server.writeChunkSize, connectChunkSize)
	}

	res, err := parseCommand(msgs[3].Payload)
	if err != nil {
		t.Fatal(err)
	}
	if res.Name != "_result" || res.TransactionID != 1 {
		t.Fatalf("result = %q/%v, want _result/1", res.Name, res.TransactionID)
	}
	if code := commandCode(res); code != "NetConnection.Connect.Success" {
		t.Fatalf("code = %q, want NetConnection.Connect.Success", code)
	}
}

func TestHandleCreateStream(t *testing.T) {
	server, client := pipeConns(t)
	for want := 1; want <= 2; want++ {
		cmd, err := parseCommand(buildCommand("createStream", float64(want), flv.Null()))
		if err != nil {
			t.Fatal(err)
		}
		errc := make(chan error, 1)
		go func() { errc <- server.handleCreateStream(cmd) }()

		m, err := client.readMessage()
		if err != nil {
			t.Fatalf("readMessage: %v", err)
		}
		if err := <-errc; err != nil {
			t.Fatalf("handleCreateStream: %v", err)
		}
		res, err := parseCommand(m.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if res.Name != "_result" || res.TransactionID != float64(want) {
			t.Fatalf("result = %q/%v, want _result/%d", res.Name, res.TransactionID, want)
		}
		if got := lastNumber(res); got != float64(want) {
			t.Fatalf("stream id = %v, want %d", got, want)
		}
	}
}

func TestHandlePublish(t *testing.T) {
	srv := New(nil, nil, nil)
	server, client := pipeConns(t)
	server.srv = srv

	cmd, err := parseCommand(buildCommand("publish", 5, flv.Null(), flv.Str("push"), flv.Str("live")))
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- server.handlePublish(1, cmd) }()

	m, err := client.readMessage()
	if err != nil {
		t.Fatalf("readMessage: %v", err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("handlePublish: %v", err)
	}
	if m.StreamID != 1 {
		t.Fatalf("status stream id = %d, want 1", m.StreamID)
	}
	status, err := parseCommand(m.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if status.Name != "onStatus" || commandCode(status) != "NetStream.Publish.Start" {
		t.Fatalf("status = %q/%q", status.Name, commandCode(status))
	}

	st := srv.registry.Get("push")
	if st == nil {
		t.Fatal("publish did not register the stream")
	}
	if server.publish == nil || server.publish.stream != st {
		t.Fatal("publish state does not point at the registered stream")
	}

	server.stopPublish()
	if srv.registry.Get("push") != nil {
		t.Fatal("stopPublish left the stream registered")
	}
}

func TestHandlePublishEmptyName(t *testing.T) {
	srv := New(nil, nil, nil)
	server, client := pipeConns(t)
	server.srv = srv

	cmd, err := parseCommand(buildCommand("publish", 1, flv.Null(), flv.Str("/?x=1"), flv.Str("live")))
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- server.handlePublish(1, cmd) }()

	m, err := client.readMessage()
	if err != nil {
		t.Fatalf("readMessage: %v", err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("handlePublish: %v", err)
	}
	status, err := parseCommand(m.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if code := commandCode(status); code != "NetStream.Publish.BadName" {
		t.Fatalf("code = %q, want NetStream.Publish.BadName", code)
	}
	if srv.registry.Len() != 0 {
		t.Fatal("empty stream name was registered")
	}
}

func TestSanitizeStreamName(t *testing.T) {
	cases := map[string]string{
		"push":         "push",
		" push ":       "push",
		"/push":        "push",
		"live/push":    "live/push",
		"push?token=x": "push",
		" /push?x=1 ":  "push",
	}
	for in, want := range cases {
		if got := sanitizeStreamName(in); got != want {
			t.Fatalf("sanitizeStreamName(%q) = %q, want %q", in, got, want)
		}
	}
}

// commandCode 取命令参数里第一个 code 字符串（onStatus/_result 用）。
func commandCode(cmd *Command) string {
	for _, a := range cmd.Args {
		if a.Type != flv.TypeObject && a.Type != flv.TypeECMAArray {
			continue
		}
		for _, m := range a.Object {
			if m.Key == "code" && m.Value.Type == flv.TypeString {
				return m.Value.String
			}
		}
	}
	return ""
}

// lastNumber 返回最后一个 Number 参数（createStream 的 stream id）。
func lastNumber(cmd *Command) float64 {
	for i := len(cmd.Args) - 1; i >= 0; i-- {
		if cmd.Args[i].Type == flv.TypeNumber {
			return cmd.Args[i].Number
		}
	}
	return 0
}

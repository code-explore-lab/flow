package rtmp

import (
	"testing"

	"flow/internal/flv"
)

func TestAuthAllowPermissions(t *testing.T) {
	a := NewAuth(AuthConfig{
		PublishTokens: []string{"pub"},
		PlayTokens:    []string{"play"},
		AllTokens:     []string{"all", "a+b"},
	})
	cases := []struct {
		desc string
		raw  string
		app  string
		need tokenPerm
		want bool
	}{
		{"publish token on publish", "push?token=pub", "", permPublish, true},
		{"publish token on play", "push?token=pub", "", permPlay, false},
		{"play token on play", "push?token=play", "", permPlay, true},
		{"play token on publish", "push?token=play", "", permPublish, false},
		{"all token on publish", "push?token=all", "", permPublish, true},
		{"all token on play", "push?token=all", "", permPlay, true},
		{"wrong token", "push?token=nope", "", permPublish, false},
		{"missing token", "push", "", permPublish, false},
		{"key alias", "push?key=all", "", permPlay, true},
		{"url encoded token", "push?token=a%2Bb", "", permPublish, true},
		{"token in app query", "push", "live?token=all", permPublish, true},
	}
	for _, c := range cases {
		if got := a.Allow(c.raw, c.app, c.need); got != c.want {
			t.Fatalf("%s: Allow = %v, want %v", c.desc, got, c.want)
		}
	}
}

func TestAuthOptionalPerDirection(t *testing.T) {
	publishOnly := NewAuth(AuthConfig{PublishTokens: []string{"pub"}})
	if publishOnly.Allow("push", "", permPublish) {
		t.Fatal("publish should require a token")
	}
	if !publishOnly.Allow("push", "", permPlay) {
		t.Fatal("play should stay open when no play token is configured")
	}

	playOnly := NewAuth(AuthConfig{PlayTokens: []string{"play"}})
	if !playOnly.Allow("push", "", permPublish) {
		t.Fatal("publish should stay open when no publish token is configured")
	}
	if playOnly.Allow("push", "", permPlay) {
		t.Fatal("play should require a token")
	}
}

func TestAuthTokenMergedPermissions(t *testing.T) {
	a := NewAuth(AuthConfig{
		PublishTokens: []string{"same"},
		PlayTokens:    []string{"same"},
	})
	if !a.Allow("push?token=same", "", permPublish) || !a.Allow("push?token=same", "", permPlay) {
		t.Fatal("a token listed for both directions should allow both")
	}
}

func TestAuthAppWhitelist(t *testing.T) {
	a := NewAuth(AuthConfig{Apps: []string{"live", " studio "}})
	for _, app := range []string{"live", "studio", "live?token=x"} {
		if !a.AppAllowed(app) {
			t.Fatalf("app %q should be allowed", app)
		}
	}
	for _, app := range []string{"other", "", "live2"} {
		if a.AppAllowed(app) {
			t.Fatalf("app %q should be rejected", app)
		}
	}

	open := NewAuth(AuthConfig{})
	if !open.AppAllowed("anything") {
		t.Fatal("empty app whitelist should allow everything")
	}
}

func TestAuthNilAndEnabled(t *testing.T) {
	var nilAuth *Auth
	if nilAuth.Enabled() {
		t.Fatal("nil Auth should not be enabled")
	}
	if !nilAuth.Allow("push", "", permPublish) || !nilAuth.AppAllowed("live") {
		t.Fatal("nil Auth should allow everything")
	}

	empty := NewAuth(AuthConfig{})
	if empty.Enabled() || !empty.Allow("push", "", permPublish) {
		t.Fatal("empty Auth should be disabled and allow everything")
	}
	if !NewAuth(AuthConfig{Apps: []string{"live"}}).Enabled() {
		t.Fatal("app whitelist alone should enable auth")
	}
}

func TestHandleConnectRejectsApp(t *testing.T) {
	srv := New(nil, nil, nil)
	srv.SetAuth(NewAuth(AuthConfig{Apps: []string{"live"}}))
	server, client := pipeConns(t)
	server.srv = srv

	cmd, err := parseCommand(buildCommand("connect", 3, flv.Obj(
		flv.Elem("app", flv.Str("other")),
	)))
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- server.handleConnect(cmd) }()

	m := readMessageWithin(t, client)
	if m.TypeID != msgCommandAMF0 {
		t.Fatalf("type = %d, want command", m.TypeID)
	}
	res, err := parseCommand(m.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if res.Name != "_error" || res.TransactionID != 3 {
		t.Fatalf("reply = %q/%v, want _error/3", res.Name, res.TransactionID)
	}
	if code := commandCode(res); code != "NetConnection.Connect.Rejected" {
		t.Fatalf("code = %q, want NetConnection.Connect.Rejected", code)
	}
	if err := <-errc; err != errAuthRejected {
		t.Fatalf("handleConnect error = %v, want errAuthRejected", err)
	}
}

func TestHandlePublishAuth(t *testing.T) {
	srv := New(nil, nil, nil)
	srv.SetAuth(NewAuth(AuthConfig{PublishTokens: []string{"pub"}, Apps: []string{"live"}}))
	server, client := pipeConns(t)
	server.srv = srv
	server.app = "live"

	// 错误 token：回 NetConnection.Connect.Rejected，且不登记流。
	cmd, err := parseCommand(buildCommand("publish", 9, flv.Null(), flv.Str("push?token=bad"), flv.Str("live")))
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- server.handlePublish(1, cmd) }()
	res, err := parseCommand(readMessageWithin(t, client).Payload)
	if err != nil {
		t.Fatal(err)
	}
	if res.Name != "_error" || commandCode(res) != "NetConnection.Connect.Rejected" {
		t.Fatalf("reply = %q/%q, want _error/NetConnection.Connect.Rejected", res.Name, commandCode(res))
	}
	if err := <-errc; err != errAuthRejected {
		t.Fatalf("handlePublish error = %v, want errAuthRejected", err)
	}
	if srv.registry.Get("push") != nil {
		t.Fatal("rejected publish registered the stream")
	}

	// 正确 token：正常回 Publish.Start，流名去掉查询串。
	cmd, err = parseCommand(buildCommand("publish", 0, flv.Null(), flv.Str("push?token=pub"), flv.Str("live")))
	if err != nil {
		t.Fatal(err)
	}
	errc = make(chan error, 1)
	go func() { errc <- server.handlePublish(1, cmd) }()
	status, err := parseCommand(readMessageWithin(t, client).Payload)
	if err != nil {
		t.Fatal(err)
	}
	if status.Name != "onStatus" || commandCode(status) != "NetStream.Publish.Start" {
		t.Fatalf("reply = %q/%q, want onStatus/NetStream.Publish.Start", status.Name, commandCode(status))
	}
	if err := <-errc; err != nil {
		t.Fatalf("handlePublish: %v", err)
	}
	if srv.registry.Get("push") == nil {
		t.Fatal("authorized publish did not register the stream")
	}
	server.stopPublish()
}

func TestHandlePlayAuth(t *testing.T) {
	srv := New(nil, nil, nil)
	srv.SetAuth(NewAuth(AuthConfig{PlayTokens: []string{"play"}, Apps: []string{"live"}}))
	st := mustPublish(t, srv.registry, "live/test")
	st.SetVideoParams("H264", nil,
		[]byte{0x67, 0x42, 0x00, 0x1f}, []byte{0x68, 0xee})
	st.EnsureVideo()

	server, client := pipeConns(t)
	server.srv = srv
	server.app = "live"

	// 错误 token：回 StreamNotFound，不建立播放状态。
	cmd, err := parseCommand(buildCommand("play", 0, flv.Null(), flv.Str("live/test?token=bad")))
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- server.handlePlay(1, cmd) }()
	if code := readStatusWithin(t, client); code != "NetStream.Play.StreamNotFound" {
		t.Fatalf("status = %q, want NetStream.Play.StreamNotFound", code)
	}
	if err := <-errc; err != nil {
		t.Fatalf("handlePlay: %v", err)
	}
	if server.play != nil {
		t.Fatal("rejected play started playback")
	}

	// 正确 token：正常进入播放。
	startPlay(t, server, client, "live/test?token=play")
	stopPlayer(t, server, client)
}

// 客户端跳过 connect 直接发 publish/play 时，app 白名单仍然要生效。
func TestAppWhitelistEnforcedWithoutConnect(t *testing.T) {
	srv := New(nil, nil, nil)
	srv.SetAuth(NewAuth(AuthConfig{Apps: []string{"live"}}))
	server, client := pipeConns(t)
	server.srv = srv // 没有经过 connect，app 为空

	cmd, err := parseCommand(buildCommand("publish", 0, flv.Null(), flv.Str("push"), flv.Str("live")))
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- server.handlePublish(1, cmd) }()
	res, err := parseCommand(readMessageWithin(t, client).Payload)
	if err != nil {
		t.Fatal(err)
	}
	if res.Name != "_error" || commandCode(res) != "NetConnection.Connect.Rejected" {
		t.Fatalf("reply = %q/%q, want _error/NetConnection.Connect.Rejected", res.Name, commandCode(res))
	}
	if err := <-errc; err != errAuthRejected {
		t.Fatalf("handlePublish error = %v, want errAuthRejected", err)
	}
	if srv.registry.Get("push") != nil {
		t.Fatal("publish without connect bypassed the app whitelist")
	}

	playCmd, err := parseCommand(buildCommand("play", 0, flv.Null(), flv.Str("live/test")))
	if err != nil {
		t.Fatal(err)
	}
	errc = make(chan error, 1)
	go func() { errc <- server.handlePlay(1, playCmd) }()
	if code := readStatusWithin(t, client); code != "NetStream.Play.StreamNotFound" {
		t.Fatalf("status = %q, want NetStream.Play.StreamNotFound", code)
	}
	if err := <-errc; err != nil {
		t.Fatalf("handlePlay: %v", err)
	}
	if server.play != nil {
		t.Fatal("play without connect bypassed the app whitelist")
	}
}

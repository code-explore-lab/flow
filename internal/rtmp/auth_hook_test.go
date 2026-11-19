package rtmp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"flow/internal/authhook"
	"flow/internal/flv"
)

type rtmpHookCapture struct {
	mu   sync.Mutex
	reqs []authhook.Request
}

func (c *rtmpHookCapture) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.reqs)
}

func (c *rtmpHookCapture) last(t *testing.T) authhook.Request {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.reqs) == 0 {
		t.Fatal("auth hook was not called")
	}
	return c.reqs[len(c.reqs)-1]
}

// attachRTMPHook 给 RTMP 服务器挂一个 httptest hook。
func attachRTMPHook(t *testing.T, srv *Server, status int, body string, cfg authhook.Config) *rtmpHookCapture {
	t.Helper()
	cap := &rtmpHookCapture{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req authhook.Request
		_ = json.NewDecoder(r.Body).Decode(&req)
		cap.mu.Lock()
		cap.reqs = append(cap.reqs, req)
		cap.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(ts.Close)
	cfg.URL = ts.URL
	hook, err := authhook.New(cfg)
	if err != nil {
		t.Fatalf("authhook.New: %v", err)
	}
	srv.SetAuthHook(hook)
	return cap
}

// publishOverPipe 在一条 net.Pipe 连接上执行 publish，返回回复命令。
func publishOverPipe(t *testing.T, srv *Server, rawName string) (*Command, error) {
	t.Helper()
	server, client := pipeConns(t)
	server.srv = srv
	server.app = "live"

	cmd, err := parseCommand(buildCommand("publish", 0, flv.Null(), flv.Str(rawName), flv.Str("live")))
	if err != nil {
		t.Fatalf("parseCommand: %v", err)
	}
	errc := make(chan error, 1)
	go func() { errc <- server.handlePublish(1, cmd) }()
	reply, err := parseCommand(readMessageWithin(t, client).Payload)
	if err != nil {
		t.Fatalf("parseCommand reply: %v", err)
	}
	handleErr := <-errc
	if handleErr == nil {
		t.Cleanup(server.stopPublish)
	}
	return reply, handleErr
}

func TestAuthHookDeniesPublish(t *testing.T) {
	srv := New(nil, nil, nil)
	cap := attachRTMPHook(t, srv, http.StatusForbidden, "", authhook.Config{})

	reply, err := publishOverPipe(t, srv, "push?token=tok1")
	if err != errAuthRejected {
		t.Fatalf("handlePublish error = %v, want errAuthRejected", err)
	}
	if reply.Name != "_error" || commandCode(reply) != "NetConnection.Connect.Rejected" {
		t.Fatalf("reply = %q/%q", reply.Name, commandCode(reply))
	}
	if srv.registry.Get("push") != nil {
		t.Fatal("denied publish registered the stream")
	}

	got := cap.last(t)
	if got.Action != "publish" || got.Protocol != "rtmp" || got.App != "live" ||
		got.Stream != "push" || got.Token != "tok1" || got.IP != "pipe" {
		t.Fatalf("hook request = %+v", got)
	}
}

func TestAuthHookAllowsPublish(t *testing.T) {
	srv := New(nil, nil, nil)
	attachRTMPHook(t, srv, http.StatusOK, `{"allow":true}`, authhook.Config{})

	reply, err := publishOverPipe(t, srv, "push?token=tok1")
	if err != nil {
		t.Fatalf("handlePublish: %v", err)
	}
	if commandCode(reply) != "NetStream.Publish.Start" {
		t.Fatalf("reply = %q/%q, want Publish.Start", reply.Name, commandCode(reply))
	}
	if srv.registry.Get("push") == nil {
		t.Fatal("allowed publish did not register the stream")
	}
}

func TestAuthHookDeniesPlay(t *testing.T) {
	srv := New(nil, nil, nil)
	st := mustPublish(t, srv.registry, "live/test")
	st.SetVideoParams("H264", nil, []byte{0x67, 0x42, 0x00, 0x1f}, []byte{0x68, 0xee})
	st.EnsureVideo()

	cap := attachRTMPHook(t, srv, http.StatusForbidden, "", authhook.Config{})
	server, client := pipeConns(t)
	server.srv = srv
	server.app = "live"

	cmd, err := parseCommand(buildCommand("play", 0, flv.Null(), flv.Str("live/test?token=tok2")))
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- server.handlePlay(1, cmd) }()
	if code := readStatusWithin(t, client); code != "NetStream.Play.StreamNotFound" {
		t.Fatalf("status = %q, want StreamNotFound", code)
	}
	if err := <-errc; err != nil {
		t.Fatalf("handlePlay: %v", err)
	}
	if server.play != nil {
		t.Fatal("denied play started playback")
	}

	got := cap.last(t)
	if got.Action != "play" || got.Stream != "live/test" || got.Token != "tok2" || got.App != "live" {
		t.Fatalf("hook request = %+v", got)
	}
}

func TestAuthHookAllowsPlay(t *testing.T) {
	srv := New(nil, nil, nil)
	st := mustPublish(t, srv.registry, "live/test")
	st.SetVideoParams("H264", nil, []byte{0x67, 0x42, 0x00, 0x1f}, []byte{0x68, 0xee})
	st.EnsureVideo()

	attachRTMPHook(t, srv, http.StatusOK, `{"allow":true}`, authhook.Config{})
	server, client := pipeConns(t)
	server.srv = srv
	server.app = "live"

	startPlay(t, server, client, "live/test?token=tok2")
	stopPlayer(t, server, client)
}

func TestAuthHookTimeoutUsesDefaultPolicyRTMP(t *testing.T) {
	for _, allow := range []bool{false, true} {
		t.Run(fmt.Sprintf("allow=%v", allow), func(t *testing.T) {
			srv := New(nil, nil, nil)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(200 * time.Millisecond)
				_, _ = w.Write([]byte(`{"allow":true}`))
			}))
			defer ts.Close()
			hook, err := authhook.New(authhook.Config{
				URL: ts.URL, Timeout: 20 * time.Millisecond, DefaultAllow: allow,
			})
			if err != nil {
				t.Fatal(err)
			}
			srv.SetAuthHook(hook)

			reply, handleErr := publishOverPipe(t, srv, "push")
			if allow {
				if handleErr != nil || commandCode(reply) != "NetStream.Publish.Start" {
					t.Fatalf("fallback allow: err=%v reply=%q/%q", handleErr, reply.Name, commandCode(reply))
				}
				return
			}
			if handleErr != errAuthRejected {
				t.Fatalf("fallback deny: err = %v, want errAuthRejected", handleErr)
			}
			if srv.registry.Get("push") != nil {
				t.Fatal("fallback deny still registered the stream")
			}
		})
	}
}

func TestAuthHookDecisionCachedRTMP(t *testing.T) {
	srv := New(nil, nil, nil)
	cap := attachRTMPHook(t, srv, http.StatusOK, `{"allow":true}`, authhook.Config{CacheTTL: time.Minute})

	for range 2 {
		reply, err := publishOverPipe(t, srv, "push?token=tok1")
		if err != nil || commandCode(reply) != "NetStream.Publish.Start" {
			t.Fatalf("publish: err=%v reply=%q/%q", err, reply.Name, commandCode(reply))
		}
	}
	if n := cap.count(); n != 1 {
		t.Fatalf("hook calls = %d, want 1 (cache hit)", n)
	}
}

package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"flow/internal/stream"
)

// fakeBackend 按测试需要返回固定的会话列表，并记录被踢的对象。
type fakeBackend struct {
	protocol string
	sessions []SessionInfo

	kickedStreams  []string
	kickedSessions []string
	streamKicks    int
}

func (f *fakeBackend) Protocol() string { return f.protocol }
func (f *fakeBackend) Sessions() []SessionInfo {
	return append([]SessionInfo(nil), f.sessions...)
}
func (f *fakeBackend) KickSession(id string) (bool, error) {
	for i, s := range f.sessions {
		if s.ID == id {
			f.kickedSessions = append(f.kickedSessions, id)
			f.sessions = append(f.sessions[:i], f.sessions[i+1:]...)
			return true, nil
		}
	}
	return false, nil
}
func (f *fakeBackend) KickStream(name string) (int, error) {
	f.kickedStreams = append(f.kickedStreams, name)
	return f.streamKicks, nil
}

func newTestServer(backends ...Backend) (*Server, *stream.Registry) {
	reg := stream.NewRegistry()
	return New(reg, backends...), reg
}

func publishTestStream(t *testing.T, reg *stream.Registry, name string) {
	t.Helper()
	st := &stream.Stream{Name: name}
	st.EnsureVideo()
	st.SetVideoParams("H264", nil, []byte{1, 2, 3}, []byte{4, 5, 6})
	st.EnsureAudio(48000)
	st.SetAudioCodec(stream.AudioAAC, nil, 48000, 2, 48000)
	if _, err := reg.Publish(name, st); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func do(t *testing.T, s *Server, method, target string, headers map[string]string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	var body map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("%s %s: decode %q: %v", method, target, rec.Body.String(), err)
		}
	}
	return rec, body
}

func TestStreamsIncludeTracksAndPublishers(t *testing.T) {
	backend := &fakeBackend{
		protocol: "rtsp",
		sessions: []SessionInfo{{
			ID: "sess-1", Protocol: "rtsp", Remote: "127.0.0.1",
			State: "Recording", Direction: DirectionPublish, Stream: "live/test",
		}},
	}
	s, reg := newTestServer(backend)
	publishTestStream(t, reg, "live/test")

	rec, body := do(t, s, http.MethodGet, "/api/v1/streams", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	list, _ := body["streams"].([]any)
	if len(list) != 1 {
		t.Fatalf("streams = %v, want 1", body["streams"])
	}
	item := list[0].(map[string]any)
	if item["name"] != "live/test" {
		t.Fatalf("name = %v", item["name"])
	}
	if v, _ := item["video"].(map[string]any); v == nil || v["codec"] != "H264" {
		t.Fatalf("video = %v", item["video"])
	}
	if a, _ := item["audio"].(map[string]any); a == nil || a["codec"] != "AAC" {
		t.Fatalf("audio = %v", item["audio"])
	}
	pubs, _ := item["publishers"].([]any)
	if len(pubs) != 1 {
		t.Fatalf("publishers = %v", item["publishers"])
	}
	if p := pubs[0].(map[string]any); p["session"] != "sess-1" || p["protocol"] != "rtsp" {
		t.Fatalf("publisher = %v", p)
	}
}

func TestStreamDetailNotFound(t *testing.T) {
	s, _ := newTestServer()
	rec, _ := do(t, s, http.MethodGet, "/api/v1/streams/live/missing", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestSessionsFilterByProtocol(t *testing.T) {
	rtspBackend := &fakeBackend{protocol: "rtsp", sessions: []SessionInfo{
		{ID: "sess-1", Protocol: "rtsp", Direction: DirectionPlay},
	}}
	rtmpBackend := &fakeBackend{protocol: "rtmp", sessions: []SessionInfo{
		{ID: "rtmp-1", Protocol: "rtmp", Direction: DirectionPublish},
	}}
	s, _ := newTestServer(rtspBackend, rtmpBackend)

	_, body := do(t, s, http.MethodGet, "/api/v1/sessions", nil)
	if got := len(body["sessions"].([]any)); got != 2 {
		t.Fatalf("sessions = %d, want 2", got)
	}
	_, body = do(t, s, http.MethodGet, "/api/v1/sessions?protocol=rtmp", nil)
	sessions := body["sessions"].([]any)
	if len(sessions) != 1 || sessions[0].(map[string]any)["id"] != "rtmp-1" {
		t.Fatalf("filtered sessions = %v", sessions)
	}
}

func TestKickStream(t *testing.T) {
	backend := &fakeBackend{protocol: "rtsp", streamKicks: 1}
	s, reg := newTestServer(backend)
	publishTestStream(t, reg, "live/test")

	rec, body := do(t, s, http.MethodDelete, "/api/v1/streams/live/test", nil)
	if rec.Code != http.StatusOK || body["kicked"].(float64) != 1 {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	if len(backend.kickedStreams) != 1 || backend.kickedStreams[0] != "live/test" {
		t.Fatalf("kicked streams = %v", backend.kickedStreams)
	}
}

func TestKickStreamNotFound(t *testing.T) {
	s, _ := newTestServer(&fakeBackend{protocol: "rtsp"})
	rec, _ := do(t, s, http.MethodDelete, "/api/v1/streams/live/missing", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestKickStreamWithoutPublisher(t *testing.T) {
	s, reg := newTestServer(&fakeBackend{protocol: "rtsp"})
	publishTestStream(t, reg, "live/file")

	rec, body := do(t, s, http.MethodDelete, "/api/v1/streams/live/file", nil)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body = %s", rec.Code, rec.Body)
	}
	if body["error"] == "" {
		t.Fatalf("missing error message: %v", body)
	}
}

func TestKickSession(t *testing.T) {
	backend := &fakeBackend{protocol: "rtsp", sessions: []SessionInfo{
		{ID: "sess-7", Protocol: "rtsp", Direction: DirectionPlay},
	}}
	s, _ := newTestServer(backend)

	rec, body := do(t, s, http.MethodDelete, "/api/v1/sessions/sess-7", nil)
	if rec.Code != http.StatusOK || body["protocol"] != "rtsp" {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	rec, _ = do(t, s, http.MethodDelete, "/api/v1/sessions/sess-7", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("second kick status = %d, want 404", rec.Code)
	}
}

func TestTokenAuth(t *testing.T) {
	s, _ := newTestServer(&fakeBackend{protocol: "rtsp"})
	s.SetToken("s3cret")

	rec, _ := do(t, s, http.MethodGet, "/api/v1/streams", nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token status = %d, want 401", rec.Code)
	}
	if got := rec.Header().Get("WWW-Authenticate"); got == "" {
		t.Fatal("missing WWW-Authenticate header")
	}

	rec, _ = do(t, s, http.MethodGet, "/api/v1/streams", map[string]string{"Authorization": "Bearer wrong"})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token status = %d, want 401", rec.Code)
	}

	rec, _ = do(t, s, http.MethodGet, "/api/v1/streams", map[string]string{"Authorization": "Bearer s3cret"})
	if rec.Code != http.StatusOK {
		t.Fatalf("bearer status = %d, want 200", rec.Code)
	}
	rec, _ = do(t, s, http.MethodGet, "/api/v1/streams", map[string]string{"X-Flow-Token": "s3cret"})
	if rec.Code != http.StatusOK {
		t.Fatalf("header token status = %d, want 200", rec.Code)
	}
}

func TestConfig(t *testing.T) {
	s, _ := newTestServer()
	s.SetConfig(map[string]string{"rtspAddr": ":8554"})

	rec, body := do(t, s, http.MethodGet, "/api/v1/config", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	cfg, _ := body["config"].(map[string]any)
	if cfg["rtspAddr"] != ":8554" {
		t.Fatalf("config = %v", cfg)
	}
	if _, err := time.Parse(time.RFC3339, cfg["startedAt"].(string)); err != nil {
		t.Fatalf("startedAt = %v: %v", cfg["startedAt"], err)
	}
}

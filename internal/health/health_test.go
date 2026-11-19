package health

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"flow/internal/source"
	"flow/internal/stream"
)

// fakeClock 可手动推进，用于 TTL / 就绪窗口等不依赖真实等待的测试。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func activeByKind(a AlertsView, kind string) []Alert {
	var out []Alert
	for _, x := range a.Active {
		if x.Kind == kind {
			out = append(out, x)
		}
	}
	return out
}

// TestReadyTracksListenerStates 验证 pending/up/down 三种状态对 /readyz
// 的影响，以及端口失败告警的 fire/resolve 生命周期。
func TestReadyTracksListenerStates(t *testing.T) {
	c := New(Options{Registry: stream.NewRegistry()})
	c.RegisterListener("rtsp", ":8554")
	c.RegisterListener("rtmp", ":1935")

	if ready, reasons := c.Ready(); ready || len(reasons) != 2 {
		t.Fatalf("pending listeners: ready=%v reasons=%v, want false/2", ready, reasons)
	}

	c.ListenerUp("rtsp")
	if ready, _ := c.Ready(); ready {
		t.Fatal("ready with one pending listener, want not ready")
	}

	c.ListenerUp("rtmp")
	if ready, reasons := c.Ready(); !ready || len(reasons) != 0 {
		t.Fatalf("all up: ready=%v reasons=%v", ready, reasons)
	}

	bindErr := errors.New("listen tcp :1935: bind: address already in use")
	c.ListenerDown("rtmp", bindErr)
	ready, reasons := c.Ready()
	if ready {
		t.Fatal("ready after listener down, want not ready")
	}
	if len(reasons) != 1 || !strings.Contains(reasons[0], "address already in use") {
		t.Fatalf("reasons = %v, want EADDRINUSE detail", reasons)
	}

	alerts := c.Alerts()
	portAlerts := activeByKind(alerts, AlertPortBindFailed)
	if len(portAlerts) != 1 || portAlerts[0].Subject != "rtmp" {
		t.Fatalf("port alerts = %+v", portAlerts)
	}
	if !strings.Contains(portAlerts[0].Message, ":1935") {
		t.Fatalf("alert message = %q, want port in message", portAlerts[0].Message)
	}
	if alerts.Counters[AlertPortBindFailed] != 1 {
		t.Fatalf("fired counter = %d, want 1", alerts.Counters[AlertPortBindFailed])
	}

	c.ListenerUp("rtmp")
	alerts = c.Alerts()
	if got := activeByKind(alerts, AlertPortBindFailed); len(got) != 0 {
		t.Fatalf("port alert not resolved: %+v", got)
	}
	resolved := false
	for _, ev := range alerts.History {
		if ev.Kind == AlertPortBindFailed && ev.Action == "resolved" && ev.Subject == "rtmp" {
			resolved = true
		}
	}
	if !resolved {
		t.Fatalf("history missing resolved event: %+v", alerts.History)
	}
}

// TestReadyDoesNotRequireActiveStreams 验证 readiness 只由监听口和注册表
// 决定：没有流、流静默都不应把全局 readyz 打成 503。
// TestListenerDownLabelsAddrInUse 验证端口占用时告警消息带 EADDRINUSE，
// Windows 的 "Only one usage..." 文本本身不含这个符号。
func TestListenerDownLabelsAddrInUse(t *testing.T) {
	c := New(Options{Registry: stream.NewRegistry()})
	c.RegisterListener("rtsp", ":8554")
	c.ListenerDown("rtsp", fmt.Errorf("listen tcp :8554: %w", syscall.EADDRINUSE))

	alerts := activeByKind(c.Alerts(), AlertPortBindFailed)
	if len(alerts) != 1 {
		t.Fatalf("port alerts = %+v", alerts)
	}
	if !strings.Contains(alerts[0].Message, "EADDRINUSE") {
		t.Fatalf("message = %q, want EADDRINUSE marker", alerts[0].Message)
	}
	if !strings.Contains(alerts[0].Message, ":8554") {
		t.Fatalf("message = %q, want port", alerts[0].Message)
	}

	// Windows 上 net.Listen 返回的 errno 是 WSAEADDRINUSE(10048)。
	c2 := New(Options{Registry: stream.NewRegistry()})
	c2.RegisterListener("rtsp", ":8554")
	c2.ListenerDown("rtsp", fmt.Errorf("listen tcp :8554: bind: %w", syscall.Errno(10048)))
	win := activeByKind(c2.Alerts(), AlertPortBindFailed)
	if len(win) != 1 || !strings.Contains(win[0].Message, "EADDRINUSE") {
		t.Fatalf("windows errno alert = %+v, want EADDRINUSE marker", win)
	}
}

func TestReadyDoesNotRequireActiveStreams(t *testing.T) {
	reg := stream.NewRegistry()
	c := New(Options{Registry: reg, ActiveWindow: 20 * time.Millisecond})
	c.RegisterListener("rtsp", ":8554")
	c.ListenerUp("rtsp")

	if ready, _ := c.Ready(); !ready {
		t.Fatal("empty registry made node unready")
	}

	st := &stream.Stream{}
	st.EnsureVideo()
	if _, err := reg.Publish("live/idle", st); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	if ready, _ := c.Ready(); !ready {
		t.Fatal("idle stream made node unready; readiness must ignore stream activity")
	}
}

// TestStreamDownOnUnpublish 验证 kill 推流端（注册表摘除）立即产生
// stream_down 告警，重新发布后 resolve。
func TestStreamDownOnUnpublish(t *testing.T) {
	reg := stream.NewRegistry()
	c := New(Options{Registry: reg})

	st := &stream.Stream{}
	video := st.EnsureVideo()
	st.SetVideoParams("H264", nil, []byte{0x67, 0x42}, []byte{0x68, 0xce})
	if _, err := reg.Publish("live/test", st); err != nil {
		t.Fatal(err)
	}
	if st.HasMedia() {
		t.Fatal("stream reports media before any Write")
	}
	video.Src.Write(source.Frame{
		Data: []byte{0x65, 0x88, 0x84}, Timestamp: 0,
		Keyframe: true, AUStart: true, AUEnd: true,
	})
	if !st.HasMedia() {
		t.Fatal("stream should report media after Write")
	}

	reg.Unpublish("live/test", st)

	alerts := c.Alerts()
	down := activeByKind(alerts, AlertStreamDown)
	if len(down) != 1 || down[0].Subject != "live/test" {
		t.Fatalf("stream_down active = %+v", down)
	}
	if alerts.Counters[AlertStreamDown] != 1 {
		t.Fatalf("stream_down counter = %d", alerts.Counters[AlertStreamDown])
	}
	fired := false
	for _, ev := range alerts.History {
		if ev.Action == "fired" && ev.Kind == AlertStreamDown && ev.Subject == "live/test" {
			fired = true
		}
	}
	if !fired {
		t.Fatalf("history missing stream_down fired: %+v", alerts.History)
	}

	// 同名流重新发布：stream_down 自动 resolve。
	st2 := &stream.Stream{}
	st2.EnsureVideo()
	if _, err := reg.Publish("live/test", st2); err != nil {
		t.Fatal(err)
	}
	if got := activeByKind(c.Alerts(), AlertStreamDown); len(got) != 0 {
		t.Fatalf("stream_down not resolved after republish: %+v", got)
	}
}

// TestStreamWithoutMediaDoesNotAlert 验证 ANNOUNCE 后未出帧即断开不会
// 误报「流中断」。
func TestStreamWithoutMediaDoesNotAlert(t *testing.T) {
	reg := stream.NewRegistry()
	c := New(Options{Registry: reg})

	st := &stream.Stream{}
	st.EnsureVideo()
	if _, err := reg.Publish("live/announce-only", st); err != nil {
		t.Fatal(err)
	}
	reg.Unpublish("live/announce-only", st)

	if got := activeByKind(c.Alerts(), AlertStreamDown); len(got) != 0 {
		t.Fatalf("false stream_down for media-less stream: %+v", got)
	}
}

// TestStreamStalledAndRecovery 验证流仍在注册表但超过窗口没有新媒体时
// 产生 stream_stalled，恢复出帧后自动 resolve。
func TestStreamStalledAndRecovery(t *testing.T) {
	reg := stream.NewRegistry()
	c := New(Options{Registry: reg, ActiveWindow: 20 * time.Millisecond})

	st := &stream.Stream{}
	video := st.EnsureVideo()
	if _, err := reg.Publish("live/stall", st); err != nil {
		t.Fatal(err)
	}
	video.Src.Write(source.Frame{Data: []byte{0x65, 0x01}, Timestamp: 0, Keyframe: true})

	time.Sleep(50 * time.Millisecond)
	c.Scan()
	stalled := activeByKind(c.Alerts(), AlertStreamStalled)
	if len(stalled) != 1 || stalled[0].Subject != "live/stall" {
		t.Fatalf("stream_stalled = %+v", stalled)
	}

	video.Src.Write(source.Frame{Data: []byte{0x65, 0x02}, Timestamp: 3000, Keyframe: true})
	c.Scan()
	if got := activeByKind(c.Alerts(), AlertStreamStalled); len(got) != 0 {
		t.Fatalf("stream_stalled not resolved after media resumed: %+v", got)
	}
}

// TestSessionExpiredAlertTTL 验证会话超时告警会进入当前活跃列表，
// 超过 TTL 后由 Scan 清理并写历史。
func TestSessionExpiredAlertTTL(t *testing.T) {
	clock := newFakeClock()
	c := New(Options{SessionAlertTTL: time.Minute, Now: clock.Now})
	c.RecordSessionExpired("rtsp", "sess-1", "10.0.0.9", clock.Now().Add(-2*time.Minute))

	active := activeByKind(c.Alerts(), AlertSessionExpired)
	if len(active) != 1 || active[0].Subject != "rtsp/sess-1" {
		t.Fatalf("session_expired = %+v", active)
	}
	if active[0].ExpiresAt == nil {
		t.Fatal("session_expired alert has no expiry")
	}

	clock.Advance(time.Minute + time.Second)
	c.Scan()
	if got := activeByKind(c.Alerts(), AlertSessionExpired); len(got) != 0 {
		t.Fatalf("session_expired not expired: %+v", got)
	}
	resolved := false
	for _, ev := range c.Alerts().History {
		if ev.Kind == AlertSessionExpired && ev.Action == "resolved" {
			resolved = true
		}
	}
	if !resolved {
		t.Fatalf("history missing session_expired resolved: %+v", c.Alerts().History)
	}
}

// TestSnapshotReportsStatusAndStreams 验证 /api/v1/health 需要的字段。
func TestSnapshotReportsStatusAndStreams(t *testing.T) {
	reg := stream.NewRegistry()
	c := New(Options{Registry: reg})
	c.RegisterListener("rtsp", ":8554")
	c.ListenerUp("rtsp")

	st := &stream.Stream{}
	video := st.EnsureVideo()
	if _, err := reg.Publish("live/snap", st); err != nil {
		t.Fatal(err)
	}
	video.Src.Write(source.Frame{Data: []byte{0x65}, Timestamp: 0, Keyframe: true})

	snap := c.Snapshot()
	if !snap.Ready || snap.Status != "ok" {
		t.Fatalf("snapshot status=%q ready=%v", snap.Status, snap.Ready)
	}
	if len(snap.Listeners) != 1 || snap.Listeners[0].State != ListenerUp {
		t.Fatalf("listeners = %+v", snap.Listeners)
	}
	if len(snap.Streams) != 1 || !snap.Streams[0].HasMedia || !snap.Streams[0].Active {
		t.Fatalf("streams = %+v", snap.Streams)
	}

	c.ListenerDown("rtsp", errors.New("bind: address already in use"))
	snap = c.Snapshot()
	if snap.Ready || snap.Status != "degraded" {
		t.Fatalf("after listener down: status=%q ready=%v", snap.Status, snap.Ready)
	}
}

// TestPrometheusOutput 验证 flow_ready / flow_alerts 的输出格式。
func TestPrometheusOutput(t *testing.T) {
	c := New(Options{Registry: stream.NewRegistry()})
	c.RegisterListener("rtsp", ":8554")
	c.ListenerUp("rtsp")

	var buf bytes.Buffer
	c.WritePrometheus(&buf)
	out := buf.String()
	for _, want := range []string{
		"flow_ready 1",
		`flow_alerts{kind="port_bind_failed"} 0`,
		`flow_alerts{kind="stream_down"} 0`,
		`flow_health_listener_up{name="rtsp",addr=":8554"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics missing %q:\n%s", want, out)
		}
	}

	c.ListenerDown("rtsp", errors.New("address already in use"))
	buf.Reset()
	c.WritePrometheus(&buf)
	out = buf.String()
	for _, want := range []string{
		"flow_ready 0",
		`flow_alerts{kind="port_bind_failed"} 1`,
		`flow_alerts_fired_total{kind="port_bind_failed"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics missing %q:\n%s", want, out)
		}
	}
}

// TestNilChecker 验证未配置健康检查时的空对象行为，避免管理接口 panic。
func TestNilChecker(t *testing.T) {
	var c *Checker
	if ready, reasons := c.Ready(); !ready || len(reasons) != 0 {
		t.Fatalf("nil checker ready=%v reasons=%v", ready, reasons)
	}
	var buf bytes.Buffer
	c.WritePrometheus(&buf)
	if buf.Len() != 0 {
		t.Fatalf("nil checker wrote metrics: %q", buf.String())
	}
	if snap := c.Snapshot(); !snap.Ready {
		t.Fatal("nil snapshot not ready")
	}
}

// TestNotifyListenerReady 验证 ctx 上的 bind 回调。
func TestNotifyListenerReady(t *testing.T) {
	c := New(Options{Registry: stream.NewRegistry()})
	c.RegisterListener("rtsp", ":8554")

	ctx := WithListenerReady(context.Background(), func() { c.ListenerUp("rtsp") })
	NotifyListenerReady(ctx)
	if ready, _ := c.Ready(); !ready {
		t.Fatal("NotifyListenerReady did not mark listener up")
	}

	// 没有回调时是空操作，不影响直接调用 Serve 的测试。
	NotifyListenerReady(context.Background())
}

package origin

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"flow/internal/httpflv"
	"flow/internal/rtmp"
	"flow/internal/rtsp"
	"flow/internal/source"
	"flow/internal/stream"
)

// 一路 H.264 测试流的最小参数集与关键帧。
var (
	relaySPS = []byte{0x67, 0x42, 0x00, 0x1f, 0x96, 0x54, 0x05, 0x01, 0xed}
	relayPPS = []byte{0x68, 0xee, 0x3c, 0x80}
	relayIDR = []byte{0x65, 0x88, 0x84, 0x00}
)

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// waitDial 等服务器开始监听。连上就关，服务器会打一条读错误日志，无害。
func waitDial(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server at %s never started listening", addr)
}

// publishUpstream 在上游注册表登记一路流，并写入 SPS/PPS/IDR。
// 先写再连：回源端订阅时 ExternalSource 会把参数集和最近的 GOP 预热给它。
func publishUpstream(t *testing.T, reg *stream.Registry, name string) *stream.Stream {
	t.Helper()
	st := &stream.Stream{Name: name}
	st.EnsureVideo()
	st.SetVideoParams("H264", nil, relaySPS, relayPPS)
	if _, err := reg.Publish(name, st); err != nil {
		t.Fatalf("publish upstream %q: %v", name, err)
	}
	t.Cleanup(func() { reg.Unpublish(name, st) })
	src := st.SourceFor("video")
	src.Write(source.Frame{Data: relaySPS})
	src.Write(source.Frame{Data: relayPPS})
	src.Write(source.Frame{Data: relayIDR, Keyframe: true})
	return st
}

// streamFrames 持续往上流源写 IDR，模拟在线推流。
func streamFrames(t *testing.T, st *stream.Stream) {
	t.Helper()
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go func() {
		ts := uint32(0)
		for {
			select {
			case <-stop:
				return
			default:
			}
			st.SourceFor("video").Write(source.Frame{Data: relayIDR, Timestamp: ts, Keyframe: true})
			ts += 3600
			time.Sleep(20 * time.Millisecond)
		}
	}()
}

// nextFrame 读一帧；超时或流结束时让测试失败（done 关闭会顺带退订）。
func nextFrame(t *testing.T, rd *source.Reader[source.Frame], within time.Duration) source.Frame {
	t.Helper()
	type result struct {
		f  source.Frame
		ok bool
	}
	ch := make(chan result, 1)
	done := make(chan struct{})
	go func() {
		f, ok := rd.Next(done)
		ch <- result{f, ok}
	}()
	select {
	case r := <-ch:
		if !r.ok {
			t.Fatalf("stream ended while waiting for a frame")
		}
		return r.f
	case <-time.After(within):
		close(done)
		t.Fatalf("no frame within %s", within)
		return source.Frame{}
	}
}

// awaitFrame 读到满足条件的帧为止。
func awaitFrame(t *testing.T, rd *source.Reader[source.Frame], within time.Duration, desc string,
	ok func(source.Frame) bool) source.Frame {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		remain := time.Until(deadline)
		if remain <= 0 {
			t.Fatalf("no %s within %s", desc, within)
		}
		if f := nextFrame(t, rd, remain); ok(f) {
			return f
		}
	}
}

// awaitStreamEnd 等下游读到流结束（ctx 取消后 Unpublish + Close）。
func awaitStreamEnd(t *testing.T, rd *source.Reader[source.Frame], within time.Duration) {
	t.Helper()
	done := make(chan struct{})
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		for {
			if _, ok := rd.Next(done); !ok {
				return
			}
		}
	}()
	select {
	case <-ended:
	case <-time.After(within):
		close(done)
		t.Fatalf("stream did not end within %s", within)
	}
}

// checkRelayed 断言回源得到的流已经登记、带正确的编码参数。
func checkRelayed(t *testing.T, reg *stream.Registry, name string, st *stream.Stream) {
	t.Helper()
	if st == nil {
		t.Fatalf("Ensure(%q) = nil", name)
	}
	if got := reg.Get(name); got != st {
		t.Fatalf("registry[%q] = %p, want relayed stream %p", name, got, st)
	}
	vc, ok := st.VideoParams()
	if !ok {
		t.Fatalf("relayed stream %q has no video params", name)
	}
	if vc.Codec != "H264" {
		t.Errorf("video codec = %q, want H264", vc.Codec)
	}
	if !bytes.Equal(vc.SPS, relaySPS) || !bytes.Equal(vc.PPS, relayPPS) {
		t.Errorf("relayed parameter sets = SPS % x PPS % x, want SPS % x PPS % x",
			vc.SPS, vc.PPS, relaySPS, relayPPS)
	}
}

// readIDR 从订阅里读到一个 IDR 帧。
func readIDR(t *testing.T, rd *source.Reader[source.Frame]) source.Frame {
	t.Helper()
	return awaitFrame(t, rd, 3*time.Second, "IDR frame", func(f source.Frame) bool {
		return bytes.Equal(f.Data, relayIDR)
	})
}

func TestEnsureRelaysRTMP(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	upReg := stream.NewRegistry()
	up := rtmp.New(upReg, nil, nil)
	addr := freeAddr(t)
	go func() { _ = up.Serve(ctx, addr) }()
	waitDial(t, addr)

	ust := publishUpstream(t, upReg, "src")
	streamFrames(t, ust)

	edgeReg := stream.NewRegistry()
	specs, err := ParseSpecs("rtmp://" + addr + "/live")
	if err != nil {
		t.Fatalf("ParseSpecs: %v", err)
	}
	m := newTestManager(ctx, edgeReg, specs, 3*time.Second)

	// RTMP 的本地流名是裸名；上游按 live/src 拉。
	st := m.Ensure(ctx, "src")
	checkRelayed(t, edgeReg, "src", st)

	rd, cancelSub := st.SourceFor("video").Subscribe()
	defer cancelSub()
	readIDR(t, rd)
}

func TestEnsureRelaysHTTPFLV(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	upReg := stream.NewRegistry()
	up := httpflv.New(upReg, nil, nil)
	srv := httptest.NewServer(up)
	defer srv.Close()

	ust := publishUpstream(t, upReg, "live/src")
	streamFrames(t, ust)

	edgeReg := stream.NewRegistry()
	specs, err := ParseSpecs("http://" + srv.Listener.Addr().String() + "/live")
	if err != nil {
		t.Fatalf("ParseSpecs: %v", err)
	}
	m := newTestManager(ctx, edgeReg, specs, 3*time.Second)

	st := m.Ensure(ctx, "live/src")
	checkRelayed(t, edgeReg, "live/src", st)

	rd, cancelSub := st.SourceFor("video").Subscribe()
	defer cancelSub()
	readIDR(t, rd)

	// 先停 puller 再关 httptest.Server：Close 会等活跃连接结束，而回源
	// 请求正挂在响应流上。
	cancel()
	waitGone(t, m)
	awaitStreamEnd(t, rd, 3*time.Second)
}

func TestEnsureRelaysRTSP(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	upReg := stream.NewRegistry()
	up := rtsp.New(nil, nil, true)
	up.SetRegistry(upReg)
	addr := freeAddr(t)
	go func() { _ = up.Serve(ctx, addr) }()
	waitDial(t, addr)

	ust := publishUpstream(t, upReg, "live/src")
	streamFrames(t, ust)

	edgeReg := stream.NewRegistry()
	specs, err := ParseSpecs("rtsp://" + addr + "/live")
	if err != nil {
		t.Fatalf("ParseSpecs: %v", err)
	}
	m := newTestManager(ctx, edgeReg, specs, 3*time.Second)

	st := m.Ensure(ctx, "live/src")
	checkRelayed(t, edgeReg, "live/src", st)

	rd, cancelSub := st.SourceFor("video").Subscribe()
	defer cancelSub()
	readIDR(t, rd)
}

// TestEnsureRelaysRTSPAudio 上游同时有音视频轨时两条轨都要转发。
// 回归：第二次 SETUP 不带 Session 时上游会新建会话，PLAY 只启动第一个
// 轨所在会话，音频静默丢失（下游能播放但一直等不到音频帧）。
func TestEnsureRelaysRTSPAudio(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	upReg := stream.NewRegistry()
	up := rtsp.New(nil, nil, true)
	up.SetRegistry(upReg)
	addr := freeAddr(t)
	go func() { _ = up.Serve(ctx, addr) }()
	waitDial(t, addr)

	ust := publishUpstream(t, upReg, "live/src")
	ust.SetAudioCodec(stream.AudioPCMA, nil, 8000, 1, 8000)
	streamFrames(t, ust)
	audioSrc := ust.SourceFor("audio")
	if audioSrc == nil {
		t.Fatal("upstream has no audio source")
	}
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go func() {
		ts := uint32(0)
		for {
			select {
			case <-stop:
				return
			default:
			}
			audioSrc.Write(source.Frame{Data: []byte{0xd5, 0x00}, Timestamp: ts, Keyframe: true})
			ts += 160
			time.Sleep(20 * time.Millisecond)
		}
	}()

	edgeReg := stream.NewRegistry()
	specs, err := ParseSpecs("rtsp://" + addr + "/live")
	if err != nil {
		t.Fatalf("ParseSpecs: %v", err)
	}
	m := newTestManager(ctx, edgeReg, specs, 3*time.Second)

	st := m.Ensure(ctx, "live/src")
	checkRelayed(t, edgeReg, "live/src", st)

	rd, cancelSub := st.SourceFor("audio").Subscribe()
	defer cancelSub()
	awaitFrame(t, rd, 5*time.Second, "audio frame", func(source.Frame) bool { return true })
}

// TestEnsureReconnectsRTSP 上游连接断开后 puller 按退避重连，本地流不被关闭，
// 下游继续收到帧且时间戳保持单调；ctx 取消后流注销、下游读到结束。
func TestEnsureReconnectsRTSP(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	upReg := stream.NewRegistry()
	up := rtsp.New(nil, nil, true)
	up.SetRegistry(upReg)
	addr := freeAddr(t)
	go func() { _ = up.Serve(ctx, addr) }()
	waitDial(t, addr)

	ust := publishUpstream(t, upReg, "live/src")

	// 中间加一层 TCP 代理，dropAll 可以精确掐断当前回源连接。
	relay := newTCPRelay(t, addr)
	edgeReg := stream.NewRegistry()
	specs, err := ParseSpecs("rtsp://" + relay.addr() + "/live")
	if err != nil {
		t.Fatalf("ParseSpecs: %v", err)
	}
	m := newTestManager(ctx, edgeReg, specs, 3*time.Second)

	st := m.Ensure(ctx, "live/src")
	checkRelayed(t, edgeReg, "live/src", st)

	rd, cancelSub := st.SourceFor("video").Subscribe()
	defer cancelSub()
	before := readIDR(t, rd)

	// 掐断上游连接；等 puller 发现并重连，再写一帧时间戳远大于之前的 IDR。
	// 首条连接里上游只发过时间戳 0，所以重连后的平移基准也是 0。
	relay.dropAll()
	time.Sleep(300 * time.Millisecond)
	ust.SourceFor("video").Write(source.Frame{Data: relayIDR, Timestamp: 900000, Keyframe: true})

	// 订阅者没有被关闭：断线期间它只是阻塞在 Next 上，重连后继续收到帧，
	// 而且时间戳单调不倒退。
	f := awaitFrame(t, rd, 5*time.Second, "frame after reconnect", func(f source.Frame) bool {
		return f.Timestamp >= 900000
	})
	if f.Timestamp < before.Timestamp {
		t.Fatalf("timestamp went backwards across reconnect: %d -> %d", before.Timestamp, f.Timestamp)
	}

	// ctx 取消：puller 退出，本地流注销，下游读到结束。
	cancel()
	waitGone(t, m)
	if st := edgeReg.Get("live/src"); st != nil {
		t.Errorf("relayed stream still registered after cancel")
	}
	awaitStreamEnd(t, rd, 3*time.Second)
}

// tcpRelay 把一个本地端口转发到 target，dropAll 关闭当前所有连接。
type tcpRelay struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func newTCPRelay(t *testing.T, target string) *tcpRelay {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("relay listen: %v", err)
	}
	r := &tcpRelay{ln: ln, target: target, conns: make(map[net.Conn]struct{})}
	t.Cleanup(func() {
		ln.Close()
		r.dropAll()
	})
	go r.serve()
	return r
}

func (r *tcpRelay) addr() string { return r.ln.Addr().String() }

func (r *tcpRelay) serve() {
	for {
		client, err := r.ln.Accept()
		if err != nil {
			return
		}
		upstream, err := net.Dial("tcp", r.target)
		if err != nil {
			client.Close()
			continue
		}
		r.mu.Lock()
		r.conns[client] = struct{}{}
		r.conns[upstream] = struct{}{}
		r.mu.Unlock()
		go func() {
			defer upstream.Close()
			_, _ = io.Copy(upstream, client)
		}()
		go func() {
			defer client.Close()
			_, _ = io.Copy(client, upstream)
		}()
	}
}

func (r *tcpRelay) dropAll() {
	r.mu.Lock()
	list := make([]net.Conn, 0, len(r.conns))
	for c := range r.conns {
		list = append(list, c)
	}
	r.conns = make(map[net.Conn]struct{})
	r.mu.Unlock()
	for _, c := range list {
		c.Close()
	}
}

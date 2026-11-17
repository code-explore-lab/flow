// Package metrics 提供进程级的轻量指标注册表，供 RTSP/RTMP 两个服务器与
// cmd/server 的 /metrics 端点共享（E-4）。
//
// 本包刻意不依赖第三方库：指标数量固定，直接按 Prometheus 文本格式
// （text/plain; version=0.0.4）输出，避免为几个计数器引入
// prometheus/client_golang。计数器用原子操作，热路径（每发一个 RTP 包）
// 没有锁开销。
package metrics

import (
	"fmt"
	"io"
	"runtime"
	"sync/atomic"
)

// Registry 是一组并发安全的计数器与仪表。用 New 构造。
// 所有方法都对 nil 接收者安全：没配置指标的测试服务器不会 panic。
type Registry struct {
	rtspConns      atomic.Int64 // 当前活跃 RTSP 连接
	rtmpConns      atomic.Int64 // 当前活跃 RTMP 连接
	rtspConnsTotal atomic.Uint64
	rtmpConnsTotal atomic.Uint64

	sessions      atomic.Int64 // 当前活跃 RTSP 会话
	sessionsTotal atomic.Uint64

	publishes      atomic.Int64 // 当前活跃 RTMP 推流
	plays          atomic.Int64 // 当前活跃 RTMP 拉流
	publishesTotal atomic.Uint64
	playsTotal     atomic.Uint64

	videoPackets atomic.Uint64
	audioPackets atomic.Uint64
	videoOctets  atomic.Uint64
	audioOctets  atomic.Uint64
	videoDropped atomic.Uint64
	audioDropped atomic.Uint64

	srSent     atomic.Uint64
	rrReceived atomic.Uint64
}

// New 创建一个空的注册表。
func New() *Registry {
	return &Registry{}
}

// IncConn 记录一个新建的 TCP 连接（proto: "rtsp" / "rtmp"）。
func (r *Registry) IncConn(proto string) {
	if r == nil {
		return
	}
	switch proto {
	case "rtsp":
		r.rtspConns.Add(1)
	case "rtmp":
		r.rtmpConns.Add(1)
	}
}

// DecConn 记录一个 TCP 连接关闭。
func (r *Registry) DecConn(proto string) {
	if r == nil {
		return
	}
	switch proto {
	case "rtsp":
		r.rtspConns.Add(-1)
	case "rtmp":
		r.rtmpConns.Add(-1)
	}
}

// IncConnTotal 累计接受过的 TCP 连接数。
func (r *Registry) IncConnTotal(proto string) {
	if r == nil {
		return
	}
	switch proto {
	case "rtsp":
		r.rtspConnsTotal.Add(1)
	case "rtmp":
		r.rtmpConnsTotal.Add(1)
	}
}

// IncSession 记录一个新建的 RTSP 会话。
func (r *Registry) IncSession() {
	if r == nil {
		return
	}
	r.sessions.Add(1)
}

// DecSession 记录一个 RTSP 会话关闭。
func (r *Registry) DecSession() {
	if r == nil {
		return
	}
	r.sessions.Add(-1)
}

// IncSessionTotal 累计创建过的 RTSP 会话数。
func (r *Registry) IncSessionTotal() {
	if r == nil {
		return
	}
	r.sessionsTotal.Add(1)
}

// IncPublish / DecPublish 记录 RTMP 推流连接数。
func (r *Registry) IncPublish() {
	if r == nil {
		return
	}
	r.publishes.Add(1)
}

func (r *Registry) DecPublish() {
	if r == nil {
		return
	}
	r.publishes.Add(-1)
}

// IncPublishTotal 累计 RTMP 推流次数。
func (r *Registry) IncPublishTotal() {
	if r == nil {
		return
	}
	r.publishesTotal.Add(1)
}

// IncPlay / DecPlay 记录 RTMP 拉流连接数。
func (r *Registry) IncPlay() {
	if r == nil {
		return
	}
	r.plays.Add(1)
}

func (r *Registry) DecPlay() {
	if r == nil {
		return
	}
	r.plays.Add(-1)
}

// IncPlayTotal 累计 RTMP 拉流次数。
func (r *Registry) IncPlayTotal() {
	if r == nil {
		return
	}
	r.playsTotal.Add(1)
}

// AddPackets 累计某条轨发出的 RTP 包数（kind: "video" / "audio"）。
func (r *Registry) AddPackets(kind string, n uint64) {
	if r == nil || n == 0 {
		return
	}
	switch kind {
	case "video":
		r.videoPackets.Add(n)
	case "audio":
		r.audioPackets.Add(n)
	}
}

// AddOctets 累计某条轨发出的 RTP 负载字节数（不含 RTP 头）。
func (r *Registry) AddOctets(kind string, n uint64) {
	if r == nil || n == 0 {
		return
	}
	switch kind {
	case "video":
		r.videoOctets.Add(n)
	case "audio":
		r.audioOctets.Add(n)
	}
}

// AddDropped 累计某条轨被丢弃的帧数（降级丢帧 + 慢消费者丢帧）。
func (r *Registry) AddDropped(kind string, n uint64) {
	if r == nil || n == 0 {
		return
	}
	switch kind {
	case "video":
		r.videoDropped.Add(n)
	case "audio":
		r.audioDropped.Add(n)
	}
}

// IncSR 累计发出的 RTCP SR 包数。
func (r *Registry) IncSR() {
	if r == nil {
		return
	}
	r.srSent.Add(1)
}

// IncRR 累计收到的 RTCP RR 包数。
func (r *Registry) IncRR() {
	if r == nil {
		return
	}
	r.rrReceived.Add(1)
}

// writeMetric 写一个指标的 HELP/TYPE 头和若干样本行。
func writeMetric(w io.Writer, name, typ, help string, lines ...string) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s %s\n", name, typ)
	for _, line := range lines {
		fmt.Fprintln(w, line)
	}
}

// WritePrometheus 以 Prometheus 文本格式写出全部指标。
func (r *Registry) WritePrometheus(w io.Writer) {
	if r == nil {
		return
	}
	writeMetric(w, "flow_connections", "gauge", "当前活跃 TCP 连接数",
		fmt.Sprintf(`flow_connections{proto="rtsp"} %d`, r.rtspConns.Load()),
		fmt.Sprintf(`flow_connections{proto="rtmp"} %d`, r.rtmpConns.Load()),
	)
	writeMetric(w, "flow_connections_total", "counter", "累计接受的 TCP 连接数",
		fmt.Sprintf(`flow_connections_total{proto="rtsp"} %d`, r.rtspConnsTotal.Load()),
		fmt.Sprintf(`flow_connections_total{proto="rtmp"} %d`, r.rtmpConnsTotal.Load()),
	)
	writeMetric(w, "flow_sessions", "gauge", "当前活跃 RTSP 会话数",
		fmt.Sprintf(`flow_sessions{proto="rtsp"} %d`, r.sessions.Load()),
	)
	writeMetric(w, "flow_sessions_total", "counter", "累计创建的 RTSP 会话数",
		fmt.Sprintf(`flow_sessions_total{proto="rtsp"} %d`, r.sessionsTotal.Load()),
	)
	writeMetric(w, "flow_rtmp_publishes", "gauge", "当前活跃 RTMP 推流数",
		fmt.Sprintf("flow_rtmp_publishes %d", r.publishes.Load()),
	)
	writeMetric(w, "flow_rtmp_publishes_total", "counter", "累计 RTMP 推流次数",
		fmt.Sprintf("flow_rtmp_publishes_total %d", r.publishesTotal.Load()),
	)
	writeMetric(w, "flow_rtmp_plays", "gauge", "当前活跃 RTMP 拉流数",
		fmt.Sprintf("flow_rtmp_plays %d", r.plays.Load()),
	)
	writeMetric(w, "flow_rtmp_plays_total", "counter", "累计 RTMP 拉流次数",
		fmt.Sprintf("flow_rtmp_plays_total %d", r.playsTotal.Load()),
	)
	writeMetric(w, "flow_rtsp_packets_sent_total", "counter", "RTSP 每轨已发送 RTP 包数",
		fmt.Sprintf(`flow_rtsp_packets_sent_total{kind="video"} %d`, r.videoPackets.Load()),
		fmt.Sprintf(`flow_rtsp_packets_sent_total{kind="audio"} %d`, r.audioPackets.Load()),
	)
	writeMetric(w, "flow_rtsp_octets_sent_total", "counter", "RTSP 每轨已发送负载字节数",
		fmt.Sprintf(`flow_rtsp_octets_sent_total{kind="video"} %d`, r.videoOctets.Load()),
		fmt.Sprintf(`flow_rtsp_octets_sent_total{kind="audio"} %d`, r.audioOctets.Load()),
	)
	writeMetric(w, "flow_rtsp_frames_dropped_total", "counter", "RTSP 每轨丢帧数",
		fmt.Sprintf(`flow_rtsp_frames_dropped_total{kind="video"} %d`, r.videoDropped.Load()),
		fmt.Sprintf(`flow_rtsp_frames_dropped_total{kind="audio"} %d`, r.audioDropped.Load()),
	)
	writeMetric(w, "flow_rtsp_sr_sent_total", "counter", "已发出的 RTCP SR 包数",
		fmt.Sprintf("flow_rtsp_sr_sent_total %d", r.srSent.Load()),
	)
	writeMetric(w, "flow_rtsp_rr_received_total", "counter", "已收到的 RTCP RR 包数",
		fmt.Sprintf("flow_rtsp_rr_received_total %d", r.rrReceived.Load()),
	)
	// E-9：抓取时采样当前 goroutine 数，用于观察资源回收后是否回落。
	writeMetric(w, "flow_goroutines", "gauge", "当前 goroutine 数",
		fmt.Sprintf("flow_goroutines %d", runtime.NumGoroutine()),
	)
}

package rtsp

import (
	"fmt"
	"io"
	"time"

	"flow/internal/rtcp"
	"flow/internal/session"
)

// TrackMetrics 是一条媒体轨在最近一次 RR 之后的服务端视角指标（C-4）。
// 数据来源：发送侧 trackStats + 接收侧最后一条 RR report block。
// E-4 的 /metrics 端点以后直接读这些字段即可。
type TrackMetrics struct {
	Kind        string
	SSRC        uint32
	PacketsSent uint32 // 已发送 RTP 包数（SR packet count）
	OctetsSent  uint32 // 已发送负载字节数

	HasRR           bool
	LastReport      time.Time // RR 到达服务端的时刻
	FractionLost    float64   // 本次报告区间的丢包率（0~1）
	CumulativeLost  int32     // 24 位有符号累计丢包数，可能为负（重复包）
	ExpectedPackets uint64    // 客户端期望收到的包数 = ExtendedHighestSeq+1
	LossRate        float64   // 累计丢包率（0~1）

	JitterUnits uint32  // RR 原始 jitter，单位是 RTP 时间戳
	JitterMs    float64 // 按 clockRate 换算成毫秒

	HasRTT bool
	RTT    time.Duration
}

// SessionMetrics 是会话级聚合：各轨明细 + 按包数加权的总量。
type SessionMetrics struct {
	SessionID       string
	Tracks          []TrackMetrics
	PacketsSent     uint32
	OctetsSent      uint32
	CumulativeLost  int64
	ExpectedPackets uint64
	LossRate        float64 // Σlost / Σexpected，而不是各轨百分比平均
}

// rttFromReport 用 RR 的 LSR/DLSR 算 RTT（RFC 3550 §6.4.1/§6.4.2）。
// LSR 是本端上次 SR 的 NTP 时间戳中间 32 位，DLSR 单位 1/65536 秒；
// 三者的差值同样以 1/65536 秒为单位，uint32 减法自带 2^32 回绕
// （该中间 32 位约 18.2 小时回绕一次）。
//
// LSR 为 0 表示客户端还没收到过本端的 SR，此时无法计算 RTT。
func rttFromReport(blk rtcp.ReportBlock, at time.Time) (time.Duration, bool) {
	if blk.LastSR == 0 {
		return 0, false
	}
	nowMid := uint32(rtcp.NTPTime(at) >> 16)
	units := nowMid - blk.LastSR - blk.DelaySinceLastSR
	return time.Duration(units) * time.Second / 65536, true
}

// metricsSnapshot 汇总这条轨当前的发送统计和最近一次 RR 指标。
// 没有收到过 RR 时仍返回发送侧数据，HasRR=false。
func (t *mediaTrack) metricsSnapshot() TrackMetrics {
	t.stats.mu.Lock()
	m := TrackMetrics{
		Kind:        t.kind,
		SSRC:        t.stats.ssrc,
		PacketsSent: t.stats.packets,
		OctetsSent:  t.stats.octets,
	}
	clockRate := t.clockRate
	t.stats.mu.Unlock()

	t.rrMu.Lock()
	blk, at, hasRR := t.lastRR, t.rrAt, t.hasRR
	t.rrMu.Unlock()

	if !hasRR {
		return m
	}

	m.HasRR = true
	m.LastReport = at
	m.FractionLost = float64(blk.FractionLost) / 256
	m.CumulativeLost = blk.CumulativeLost
	// 客户端期望收到的包数：扩展最高序列号 + 1（本轨 seq 从 0 开始）。
	m.ExpectedPackets = uint64(blk.ExtendedHighestSeq) + 1
	if m.ExpectedPackets > 0 {
		lost := blk.CumulativeLost
		if lost < 0 { // 重复包会让累计丢包为负，按 0 处理
			lost = 0
		}
		m.LossRate = float64(lost) / float64(m.ExpectedPackets)
	}
	m.JitterUnits = blk.Jitter
	if clockRate > 0 {
		m.JitterMs = float64(blk.Jitter) / float64(clockRate) * 1000
	}
	m.RTT, m.HasRTT = rttFromReport(blk, at)
	return m
}

// metricsSnapshot 汇总会话下所有轨。各轨快照在 s.mu 之外获取，
// 避免和发送/RTCP 路径形成锁序环。
func (s *Session) metricsSnapshot(id string) SessionMetrics {
	s.mu.Lock()
	tracks := make([]*mediaTrack, 0, len(s.tracks))
	for _, t := range s.tracks {
		tracks = append(tracks, t)
	}
	s.mu.Unlock()

	sm := SessionMetrics{SessionID: id, Tracks: make([]TrackMetrics, 0, len(tracks))}
	var lost int64
	for _, t := range tracks {
		m := t.metricsSnapshot()
		sm.Tracks = append(sm.Tracks, m)
		sm.PacketsSent += m.PacketsSent
		sm.OctetsSent += m.OctetsSent
		if m.HasRR {
			sm.ExpectedPackets += m.ExpectedPackets
			l := m.CumulativeLost
			if l > 0 {
				lost += int64(l)
			}
		}
	}
	sm.CumulativeLost = lost
	if sm.ExpectedPackets > 0 {
		sm.LossRate = float64(lost) / float64(sm.ExpectedPackets)
	}
	return sm
}

// WriteLossMetrics 输出「活跃会话累计丢包」相关的 gauge（E-4）。
//
// 丢包只能从每条会话最近一次 RR 的累计值算出来，而且会话来了又走，
// 没法用单调递增的全局 counter 表达，所以每次抓取时遍历活跃会话重新求和。
// 没收到过 RR 的会话不贡献 ExpectedPackets，输出为 0。
func (s *Server) WriteLossMetrics(w io.Writer) {
	var lost uint64
	var expected uint64
	s.sessions.Range(func(sess *session.Session) bool {
		st, ok := sess.Data.(*Session)
		if !ok || st == nil {
			return true
		}
		m := st.metricsSnapshot(sess.ID)
		if m.CumulativeLost > 0 {
			lost += uint64(m.CumulativeLost)
		}
		expected += m.ExpectedPackets
		return true
	})
	rate := 0.0
	if expected > 0 {
		rate = float64(lost) / float64(expected)
	}
	fmt.Fprintln(w, "# HELP flow_rtsp_packets_lost 活跃会话累计丢包数（来自最近一次 RR）")
	fmt.Fprintln(w, "# TYPE flow_rtsp_packets_lost gauge")
	fmt.Fprintf(w, "flow_rtsp_packets_lost %d\n", lost)
	fmt.Fprintln(w, "# HELP flow_rtsp_loss_rate 活跃会话累计丢包率（Σlost/Σexpected）")
	fmt.Fprintln(w, "# TYPE flow_rtsp_loss_rate gauge")
	fmt.Fprintf(w, "flow_rtsp_loss_rate %g\n", rate)
}

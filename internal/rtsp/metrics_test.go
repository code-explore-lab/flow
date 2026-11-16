package rtsp

import (
	"math"
	"testing"
	"time"

	"flow/internal/rtcp"
)

func newMetricsTrack(kind string, rate, ssrc, packets, octets uint32) *mediaTrack {
	t := &mediaTrack{kind: kind, clockRate: rate}
	t.stats.ssrc = ssrc
	t.stats.packets = packets
	t.stats.octets = octets
	return t
}

func TestTrackMetricsRTT(t *testing.T) {
	at := time.Now()
	blk := rtcp.ReportBlock{
		SSRC:             7,
		LastSR:           uint32(rtcp.NTPTime(at.Add(-1500*time.Millisecond)) >> 16),
		DelaySinceLastSR: 32768, // 0.5s，单位 1/65536 秒
	}
	track := newMetricsTrack("video", 90000, 7, 10, 100)
	track.recordReceiverReport(1, blk, at)

	m := track.metricsSnapshot()
	if !m.HasRR {
		t.Fatal("metricsSnapshot lost the RR state")
	}
	if !m.HasRTT {
		t.Fatal("RTT should be available when LSR is set")
	}
	// 1.5s 的 NTP 差减去 0.5s 的 DLSR。
	if diff := (m.RTT - time.Second).Abs(); diff > 2*time.Millisecond {
		t.Fatalf("RTT = %v, want ~1s", m.RTT)
	}
}

func TestTrackMetricsNoRTTBeforeFirstSR(t *testing.T) {
	track := newMetricsTrack("video", 90000, 7, 10, 100)
	track.recordReceiverReport(1, rtcp.ReportBlock{SSRC: 7, FractionLost: 0}, time.Now())

	m := track.metricsSnapshot()
	if !m.HasRR {
		t.Fatal("RR state should be stored")
	}
	if m.HasRTT {
		t.Fatalf("RTT = %v, want unavailable when LSR=0", m.RTT)
	}
}

func TestTrackMetricsLossAndJitter(t *testing.T) {
	track := newMetricsTrack("video", 90000, 1, 500, 5000)
	track.recordReceiverReport(1, rtcp.ReportBlock{
		SSRC:               1,
		FractionLost:       128, // 0.5
		CumulativeLost:     10,
		ExtendedHighestSeq: 999, // 期望 1000 包
		Jitter:             900, // 90000Hz 下是 10ms
	}, time.Now())

	m := track.metricsSnapshot()
	if math.Abs(m.FractionLost-0.5) > 1e-9 {
		t.Fatalf("FractionLost = %v, want 0.5", m.FractionLost)
	}
	if m.ExpectedPackets != 1000 {
		t.Fatalf("ExpectedPackets = %d, want 1000", m.ExpectedPackets)
	}
	if math.Abs(m.LossRate-0.01) > 1e-9 {
		t.Fatalf("LossRate = %v, want 0.01", m.LossRate)
	}
	if m.CumulativeLost != 10 {
		t.Fatalf("CumulativeLost = %d, want 10", m.CumulativeLost)
	}
	if math.Abs(m.JitterMs-10) > 1e-9 {
		t.Fatalf("JitterMs = %v, want 10", m.JitterMs)
	}
}

func TestTrackMetricsNegativeCumulativeLost(t *testing.T) {
	track := newMetricsTrack("audio", 44100, 1, 10, 100)
	track.recordReceiverReport(1, rtcp.ReportBlock{
		SSRC:               1,
		CumulativeLost:     -12, // 重复包导致
		ExtendedHighestSeq: 99,
		Jitter:             441, // 44100Hz 下是 10ms
	}, time.Now())

	m := track.metricsSnapshot()
	if m.CumulativeLost != -12 {
		t.Fatalf("CumulativeLost = %d, want -12", m.CumulativeLost)
	}
	if m.LossRate != 0 {
		t.Fatalf("LossRate = %v, want 0 for negative cumulative loss", m.LossRate)
	}
	if math.Abs(m.JitterMs-10) > 1e-9 {
		t.Fatalf("JitterMs = %v, want 10", m.JitterMs)
	}
}

func TestTrackMetricsWithoutRR(t *testing.T) {
	track := newMetricsTrack("video", 90000, 9, 42, 4200)

	m := track.metricsSnapshot()
	if m.HasRR {
		t.Fatal("HasRR should be false before any report")
	}
	if m.PacketsSent != 42 || m.OctetsSent != 4200 {
		t.Fatalf("send stats = %d/%d, want 42/4200", m.PacketsSent, m.OctetsSent)
	}
	if m.LossRate != 0 || m.JitterMs != 0 || m.HasRTT {
		t.Fatalf("unexpected metrics without RR: %+v", m)
	}
}

func TestSessionMetricsAggregation(t *testing.T) {
	video := newMetricsTrack("video", 90000, 1, 100, 1000)
	audio := newMetricsTrack("audio", 44100, 2, 200, 2000)
	at := time.Now()
	video.recordReceiverReport(1, rtcp.ReportBlock{
		SSRC: 1, ExtendedHighestSeq: 999, CumulativeLost: 10, // 期望 1000，丢 10
	}, at)
	audio.recordReceiverReport(1, rtcp.ReportBlock{
		SSRC: 2, ExtendedHighestSeq: 499, CumulativeLost: 20, // 期望 500，丢 20
	}, at)

	st := newSession()
	st.setTrack("video", video)
	st.setTrack("audio", audio)

	sm := st.metricsSnapshot("sess-9")
	if sm.SessionID != "sess-9" {
		t.Fatalf("SessionID = %q", sm.SessionID)
	}
	if len(sm.Tracks) != 2 {
		t.Fatalf("track count = %d, want 2", len(sm.Tracks))
	}
	if sm.PacketsSent != 300 || sm.OctetsSent != 3000 {
		t.Fatalf("totals = %d/%d, want 300/3000", sm.PacketsSent, sm.OctetsSent)
	}
	if sm.CumulativeLost != 30 || sm.ExpectedPackets != 1500 {
		t.Fatalf("lost/expected = %d/%d, want 30/1500", sm.CumulativeLost, sm.ExpectedPackets)
	}
	// 加权聚合：30/1500，而不是 (1% + 4%)/2。
	if math.Abs(sm.LossRate-0.02) > 1e-9 {
		t.Fatalf("session LossRate = %v, want 0.02", sm.LossRate)
	}
}

func TestSessionMetricsEmpty(t *testing.T) {
	st := newSession()
	if sm := st.metricsSnapshot("empty"); len(sm.Tracks) != 0 || sm.LossRate != 0 {
		t.Fatalf("empty session metrics = %+v", sm)
	}
}

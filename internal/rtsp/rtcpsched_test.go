package rtsp

import (
	"math"
	"strings"
	"testing"
	"time"

	"flow/internal/stream"
)

// TestRTCPBaseIntervalFloor 单会话/正常码率时，间隔由 5 秒下限决定。
func TestRTCPBaseIntervalFloor(t *testing.T) {
	got := rtcpBaseInterval(2, 60, 2_000_000) // 2 Mbps 视频 + 音频
	if got != rtcpMinInterval.Seconds() {
		t.Fatalf("base interval = %.3fs, want %.0fs floor", got, rtcpMinInterval.Seconds())
	}
	if got := rtcpBaseInterval(0, 60, 2_000_000); got != rtcpMinInterval.Seconds() {
		t.Fatalf("empty scheduler interval = %.3fs, want floor", got)
	}
	if got := rtcpBaseInterval(2, 60, 0); got != rtcpMinInterval.Seconds() {
		t.Fatalf("no media interval = %.3fs, want floor", got)
	}
}

// TestRTCPBaseIntervalScalesWithTracks 低码率 + 多轨时，间隔必须随 n 增长。
func TestRTCPBaseIntervalScalesWithTracks(t *testing.T) {
	const mediaBps = 100_000 // 100 kbps
	one := rtcpBaseInterval(1, 60, mediaBps)
	many := rtcpBaseInterval(40, 60, mediaBps)
	if one != rtcpMinInterval.Seconds() {
		t.Fatalf("single-track interval = %.3fs, want floor", one)
	}
	if many <= rtcpMinInterval.Seconds() {
		t.Fatalf("40-track interval = %.3fs, want > floor", many)
	}
	if many <= one {
		t.Fatalf("interval did not grow with tracks: %.3fs vs %.3fs", many, one)
	}
}

// TestRTCPBaseIntervalBudget 验证 RFC 3550 的带宽约束：
// n 个 SR 在 Td 内发出的字节数不超过媒体带宽的 5%。
func TestRTCPBaseIntervalBudget(t *testing.T) {
	const (
		n        = 100
		avgSize  = 60.0 // SR+SDES，不含 UDP/IP
		mediaBps = 8_000.0
	)
	td := rtcpBaseInterval(n, avgSize, mediaBps)
	if td <= rtcpMinInterval.Seconds() {
		t.Fatalf("interval = %.3fs, test case should exceed the floor", td)
	}
	// rtcpBaseInterval 内部按 avgSize+28 字节计算，这里用同一口径。
	// 服务端 SR 只占 RTCP 预算的发送方份额（单播两端各半）。
	srPerSec := float64(n) * (avgSize + rtcpIPUDPOverhead) / td
	budget := rtcpBandwidthFraction * rtcpSenderShare * mediaBps / 8
	if srPerSec > budget+1e-9 {
		t.Fatalf("SR bandwidth = %.3f B/s, budget = %.3f B/s", srPerSec, budget)
	}
	if math.Abs(srPerSec-budget) > 1e-9 {
		t.Fatalf("SR bandwidth = %.6f B/s, want budget %.6f B/s", srPerSec, budget)
	}
}

// TestRTCPIntervalRandomRange 随机因子必须落在 [0.5, 1.5) × Td。
func TestRTCPIntervalRandomRange(t *testing.T) {
	const (
		n        = 40
		avgSize  = 60.0
		mediaBps = 100_000.0
	)
	td := rtcpBaseInterval(n, avgSize, mediaBps)
	lo := time.Duration(0.5 * td * float64(time.Second))
	hi := time.Duration(1.5 * td * float64(time.Second))
	for i := 0; i < 200; i++ {
		got := rtcpInterval(n, avgSize, mediaBps)
		if got < lo || got >= hi {
			t.Fatalf("interval = %s, want in [%s, %s)", got, lo, hi)
		}
	}
}

// TestRTCPSchedulerAvgSizeEWMA 平均包长按 1/16 权重平滑。
func TestRTCPSchedulerAvgSizeEWMA(t *testing.T) {
	sc := newRTCPScheduler()
	if sc.avgSize != rtcpInitialAvgSize {
		t.Fatalf("initial avgSize = %v, want %v", sc.avgSize, rtcpInitialAvgSize)
	}
	sc.noteSent(100)
	want := rtcpInitialAvgSize + (100-rtcpInitialAvgSize)/16
	if math.Abs(sc.avgSize-want) > 1e-9 {
		t.Fatalf("avgSize = %v, want %v", sc.avgSize, want)
	}
}

// TestRTCPSchedulerRegister 注册/注销影响间隔计算里的 n。
func TestRTCPSchedulerRegister(t *testing.T) {
	sc := newRTCPScheduler()
	track := &mediaTrack{kind: "video"}
	sc.add(track)
	sc.add(track) // 重复注册不应重复计数
	if len(sc.samples) != 1 {
		t.Fatalf("samples = %d, want 1", len(sc.samples))
	}
	// 没有码率样本时 Td 退回 5 秒下限；RFC 3550 在 Td 之后再乘
	// [0.5,1.5) 的随机因子，所以实际间隔是 [2.5s, 7.5s)。
	lo := time.Duration(0.5 * rtcpMinInterval.Seconds() * float64(time.Second))
	hi := time.Duration(1.5 * rtcpMinInterval.Seconds() * float64(time.Second))
	got := sc.nextInterval(time.Now())
	if got < lo || got >= hi {
		t.Fatalf("first interval = %s, want in [%s, %s)", got, lo, hi)
	}
	sc.remove(track)
	if len(sc.samples) != 0 {
		t.Fatalf("samples = %d after remove, want 0", len(sc.samples))
	}
}

// TestRTCPSchedulerUsesMeasuredBitrate 调度器用 octets 增量估算总码率，
// 低码率时按 RFC 3550 公式拉长间隔。
func TestRTCPSchedulerUsesMeasuredBitrate(t *testing.T) {
	sc := newRTCPScheduler()
	base := time.Unix(1000, 0)
	const n = 20
	for i := 0; i < n; i++ {
		tr := &mediaTrack{kind: "audio"}
		sc.add(tr)
		// add() 用 time.Now() 做首个采样点，测试里替换成假时间。
		sc.mu.Lock()
		sc.samples[tr].lastAt = base
		sc.samples[tr].lastOctets = 0
		sc.mu.Unlock()
		tr.stats.mu.Lock()
		tr.stats.octets = 100 // 每轨每秒 100 字节 = 800 bit/s
		tr.stats.mu.Unlock()
	}
	// B = 20 × 800 = 16000 bit/s，Td = 8×20×88 / (0.025×16000) = 35.2s。
	td := 8 * float64(n) * (rtcpInitialAvgSize + rtcpIPUDPOverhead) /
		(rtcpBandwidthFraction * rtcpSenderShare * 16_000)
	lo := time.Duration(0.5 * td * float64(time.Second))
	hi := time.Duration(1.5 * td * float64(time.Second))
	got := sc.nextInterval(base.Add(time.Second))
	if got < lo || got >= hi {
		t.Fatalf("interval = %s, want in [%s, %s)", got, lo, hi)
	}
}

// TestSDPAdvertisesReducedSize 本地/live SDP 必须宣告 a=rtcp-rsize。
func TestSDPAdvertisesReducedSize(t *testing.T) {
	s := New(nil, nil, true)
	sdp, err := s.buildSDP("rtsp://127.0.0.1/live")
	if err != nil {
		t.Fatalf("buildSDP: %v", err)
	}
	if n := strings.Count(sdp, "a=rtcp-rsize"); n != 1 {
		t.Fatalf("a=rtcp-rsize count = %d, want 1\n%s", n, sdp)
	}
}

// TestPublishedSDPAdvertisesReducedSize 已发布流（ANNOUNCE/RTMP）的 SDP
// 每个 m= 段也要宣告 a=rtcp-rsize。
func TestPublishedSDPAdvertisesReducedSize(t *testing.T) {
	p := &stream.Stream{Name: "test"}
	p.EnsureVideo()
	p.EnsureAudio(48000)

	sdp := publishedSDP(p, "", 0)
	if n := strings.Count(sdp, "a=rtcp-rsize"); n != 2 {
		t.Fatalf("a=rtcp-rsize count = %d, want 2\n%s", n, sdp)
	}
}

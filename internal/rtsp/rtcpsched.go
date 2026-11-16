package rtsp

import (
	"math/rand"
	"sync"
	"time"
)

// C-5：RTCP SR 发送策略。
//
// RFC 3550 §6.3.1 用会话带宽的 5% 作为 RTCP 带宽预算，并按参与者数量
// 反推发送间隔：
//
//	rtcp_bw = 5% × 媒体带宽
//	Td      = max(Tmin, n × avg_rtcp_size × 8 / rtcp_bw)
//	T       = Td × (0.5 + rand())        // 多参与者随机化，避免同步
//
// 每个单播 RTP 会话固定两个成员（服务端发送 + 客户端接收），senders 占比
// 50%，超过 25% 的门限，因此不做 25%/75% 分割；两端各占预算的一半，
// 服务端 SR 只用 2.5% 媒体带宽，剩下的留给客户端 RR。
//
// 本服务端每个客户端是独立的单播 RTP 会话，如果各会话仍然固定 5s 发 SR，
// 总 RTCP 流量会随会话数线性增长，没有任何上限。这里把「所有正在发送的
// 轨」当成一个共享预算池：n 取注册在发的轨数，媒体带宽取各轨 octets 的
// 实测增量之和。会话数增加时 n 与 B 同时增长，单路码率不低时间隔仍由 5s
// 下限决定；真正把间隔拉长的是单路码率低（avg_size / 单路码率变大）。
// 两种情况下都满足
//
//	n × avg_size / Td ≤ 2.5% × 媒体总带宽
//
// Tmin 沿用 RFC 3550 单播的 5 秒下限；单会话时行为与原来的固定 5s 一致。
const (
	rtcpBandwidthFraction = 0.05
	// rtcpSenderShare 是 RTCP 预算中分给发送方的一半（单播两端各半）。
	rtcpSenderShare = 0.5
	rtcpMinInterval = 5 * time.Second

	// rtcpIPUDPOverhead 是 IPv4 + UDP 的固定头长度。RFC 3550 §6.3.1
	// 要求平均 RTCP 包长包含下层协议开销，不能只算 RTCP 本身。
	rtcpIPUDPOverhead = 28

	// rtcpInitialAvgSize 是 SR+SDES 长度的初值（不含 UDP/IP 头）。
	// 实际长度由 noteSent 用 EWMA 修正（RFC 3550 用 1/16 权重）。
	rtcpInitialAvgSize = 60.0
	rtcpAvgSizeWeight  = 1.0 / 16.0
)

// rtcpBaseInterval 返回未随机化的间隔 Td，单位秒。
// mediaBps 是服务端所有在发轨的媒体码率之和（bit/s）；
// avgSize 是 SR+SDES 的平均字节数（不含 UDP/IP 头）。
//
// 第一轮采样时还没有码率数据（mediaBps=0），返回 Tmin 让首包照常发出；
// 之后的调用会带上实测码率。
func rtcpBaseInterval(n int, avgSize, mediaBps float64) float64 {
	if n <= 0 || mediaBps <= 0 {
		return rtcpMinInterval.Seconds()
	}
	rtcpBw := rtcpBandwidthFraction * rtcpSenderShare * mediaBps
	td := 8 * float64(n) * (avgSize + rtcpIPUDPOverhead) / rtcpBw
	if td < rtcpMinInterval.Seconds() {
		return rtcpMinInterval.Seconds()
	}
	return td
}

// rtcpInterval 在 Td 上叠加 [0.5, 1.5) 的随机因子（RFC 3550 §6.3.1）。
// 随机化让不同会话的 SR 不会长期对齐在同一时刻。
func rtcpInterval(n int, avgSize, mediaBps float64) time.Duration {
	td := rtcpBaseInterval(n, avgSize, mediaBps)
	return time.Duration((0.5 + rand.Float64()) * td * float64(time.Second))
}

// rtcpSample 是一条发送轨的码率采样状态。
type rtcpSample struct {
	lastOctets uint32
	lastAt     time.Time
	rateBps    float64
}

// rtcpScheduler 汇总所有正在发 RTP 的轨，为 SR 定时器提供间隔。
// 只在注册/注销和每次 nextInterval/noteSent 时加锁，发送路径本身不受影响。
type rtcpScheduler struct {
	mu      sync.Mutex
	samples map[*mediaTrack]*rtcpSample
	avgSize float64
}

func newRTCPScheduler() *rtcpScheduler {
	return &rtcpScheduler{
		samples: make(map[*mediaTrack]*rtcpSample),
		avgSize: rtcpInitialAvgSize,
	}
}

// add 把一条开始 PLAY 的轨纳入预算。重复注册是安全的。
func (sc *rtcpScheduler) add(t *mediaTrack) {
	t.stats.mu.Lock()
	octets := t.stats.octets
	t.stats.mu.Unlock()

	sc.mu.Lock()
	if _, ok := sc.samples[t]; !ok {
		sc.samples[t] = &rtcpSample{lastOctets: octets, lastAt: time.Now()}
	}
	sc.mu.Unlock()
}

// remove 在 PLAY 生命周期结束时注销该轨，释放它占的 SR 预算份额。
func (sc *rtcpScheduler) remove(t *mediaTrack) {
	sc.mu.Lock()
	delete(sc.samples, t)
	sc.mu.Unlock()
}

// noteSent 用刚发出的 SR 长度更新平均包长（EWMA）。
func (sc *rtcpScheduler) noteSent(size int) {
	sc.mu.Lock()
	sc.avgSize += rtcpAvgSizeWeight * (float64(size) - sc.avgSize)
	sc.mu.Unlock()
}

// nextInterval 刷新每条轨的实测码率并返回下一次 SR 间隔。
// 码率用「两次调用之间的 octets 增量 / 时间」估算，再做一次指数平滑，
// 避免单个采样窗口的抖动把间隔算飞。
func (sc *rtcpScheduler) nextInterval(now time.Time) time.Duration {
	type sample struct {
		track  *mediaTrack
		octets uint32
	}
	sc.mu.Lock()
	snapshot := make([]sample, 0, len(sc.samples))
	for t := range sc.samples {
		snapshot = append(snapshot, sample{track: t})
	}
	sc.mu.Unlock()

	// 先读各轨 octets，再统一回填，避免拿着调度器锁去抢 track 的统计锁。
	for i := range snapshot {
		snapshot[i].track.stats.mu.Lock()
		snapshot[i].octets = snapshot[i].track.stats.octets
		snapshot[i].track.stats.mu.Unlock()
	}

	sc.mu.Lock()
	var totalBps float64
	for _, s := range snapshot {
		smp, ok := sc.samples[s.track]
		if !ok {
			continue // 采样期间被 remove 了
		}
		if !smp.lastAt.IsZero() {
			if elapsed := now.Sub(smp.lastAt); elapsed > 0 {
				inst := float64(s.octets-smp.lastOctets) * 8 / elapsed.Seconds()
				if smp.rateBps == 0 {
					smp.rateBps = inst
				} else {
					smp.rateBps = 0.75*smp.rateBps + 0.25*inst
				}
			}
		}
		smp.lastOctets = s.octets
		smp.lastAt = now
		totalBps += smp.rateBps
	}
	n := len(sc.samples)
	avgSize := sc.avgSize
	sc.mu.Unlock()

	return rtcpInterval(n, avgSize, totalBps)
}

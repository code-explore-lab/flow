package rtsp

import (
	"fmt"
	"time"
)

const (
	// maxNACKSeqsPerPacket 限制单条 NACK 实际处理的 seq 数。RFC 4585 的
	// FCI 上限能带 272 个 seq，一次性全重传容易把小丢包放大成流量尖峰。
	maxNACKSeqsPerPacket = 64

	// 每轨重传令牌桶：稳态 200 包/秒、最多 64 包突发。
	// 2 Mbps 视频按 1400 字节/包约 180 包/秒，重传上限大致与媒体同量级。
	rtxRatePerSecond = 200.0
	rtxBurst         = 64.0
)

// takeRTXTokens 从该轨的重传令牌桶取最多 want 个令牌，返回实际可用数。
// 令牌按经过时间补充，重复 NACK 只会持续消耗同一份额度。
func (t *mediaTrack) takeRTXTokens(now time.Time, want int) int {
	if want <= 0 {
		return 0
	}
	t.rtxMu.Lock()
	defer t.rtxMu.Unlock()
	if t.rtxLast.IsZero() {
		t.rtxTokens = rtxBurst
	} else {
		t.rtxTokens += now.Sub(t.rtxLast).Seconds() * rtxRatePerSecond
		if t.rtxTokens > rtxBurst {
			t.rtxTokens = rtxBurst
		}
	}
	t.rtxLast = now

	n := want
	if float64(n) > t.rtxTokens {
		n = int(t.rtxTokens)
	}
	t.rtxTokens -= float64(n)
	return n
}

// dedupeSeqs 去掉重复 seq，保持首次出现的顺序。
func dedupeSeqs(seqs []uint16) []uint16 {
	seen := make(map[uint16]struct{}, len(seqs))
	out := make([]uint16, 0, len(seqs))
	for _, seq := range seqs {
		if _, ok := seen[seq]; ok {
			continue
		}
		seen[seq] = struct{}{}
		out = append(out, seq)
	}
	return out
}

// retransmit 处理客户端发来的 NACK：按 seq 从 R-1 发送缓存取回原始
// RTP 包原样重发。seq/timestamp/SSRC 都不改，也不更新 SR 统计——
// 重传不是新发出的媒体，Wireshark 里应显示为 duplicate。
func (s *Server) retransmit(track *mediaTrack, seqs []uint16) {
	if track.sendCache == nil || track.isMulticast {
		return // 推流轨/组播轨不参与按客户端重传
	}
	seqs = dedupeSeqs(seqs)
	requested := len(seqs)
	if len(seqs) > maxNACKSeqsPerPacket {
		seqs = seqs[:maxNACKSeqsPerPacket]
	}

	var sent, missed, throttled int
	for _, seq := range seqs {
		pkt, ok := track.cachedPacket(seq)
		if !ok {
			missed++ // 已滑出缓存窗口，或这个 seq 还没发过
			continue
		}
		if track.takeRTXTokens(time.Now(), 1) == 0 {
			throttled++
			break
		}
		if err := track.writeRTP(pkt); err != nil {
			fmt.Printf("NACK retransmit (%s): %v\n", track.kind, err)
			return
		}
		sent++
	}
	fmt.Printf("RTCP NACK (%s): requested=%d retransmitted=%d missed=%d throttled=%d\n",
		track.kind, requested, sent, missed, throttled)
}

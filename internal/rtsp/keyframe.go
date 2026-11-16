package rtsp

import (
	"fmt"
	"time"
)

// keyframeReplayInterval 限制同一轨的 PLI/FIR 重放频率。
// 播放器在卡顿时可能连续发 PLI，重放整个关键帧 AU 流量不小。
const keyframeReplayInterval = time.Second

// storeKeyframe 记录最近一个含 IDR 的 AU 的完整 RTP 包。
// pkts 的所有权移交给 mediaTrack：调用方不再修改这些字节。
func (t *mediaTrack) storeKeyframe(pkts [][]byte, hasParams bool) {
	if len(pkts) == 0 {
		return
	}
	t.kfMu.Lock()
	t.lastKeyframe = pkts
	t.kfHasParams = hasParams
	t.kfMu.Unlock()
}

// storeParamSets 记录最近一个只含 SPS/PPS 的 AU。
// 如果参数集和 IDR 在同一帧，storeKeyframe 已经覆盖，不需要再存。
func (t *mediaTrack) storeParamSets(pkts [][]byte) {
	if len(pkts) == 0 {
		return
	}
	t.kfMu.Lock()
	t.lastParamSets = pkts
	t.kfMu.Unlock()
}

// replayKeyframe 响应客户端的 PLI/FIR：按原 seq/timestamp/SSRC 重放
// SPS/PPS + 最近 IDR。限频 keyframeReplayInterval。
//
// 与 NACK 重传一样，不调用 recordSent/cacheSent：重放不是新媒体，
// SR 的包数/字节数保持不变。
func (t *mediaTrack) replayKeyframe(reason string) {
	t.kfMu.Lock()
	if !t.lastKFReply.IsZero() && time.Since(t.lastKFReply) < keyframeReplayInterval {
		t.kfMu.Unlock()
		return
	}
	pkts := make([][]byte, 0, len(t.lastParamSets)+len(t.lastKeyframe))
	if !t.kfHasParams {
		pkts = append(pkts, t.lastParamSets...)
	}
	pkts = append(pkts, t.lastKeyframe...)
	if len(pkts) == 0 {
		t.kfMu.Unlock()
		return // 还没发出过 IDR，无从重放
	}
	t.lastKFReply = time.Now()
	t.kfMu.Unlock()

	fmt.Printf("RTCP %s (%s): replay keyframe (%d packets)\n", reason, t.kind, len(pkts))
	for _, pkt := range pkts {
		if err := t.writeRTP(pkt); err != nil {
			fmt.Printf("keyframe replay (%s): %v\n", t.kind, err)
			return
		}
	}
}

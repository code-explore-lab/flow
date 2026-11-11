package rtsp

// cseqGuard 校验一条 TCP 连接上 RTSP 请求的 CSeq 顺序。
//
// RFC 2326 要求每个不同的请求 CSeq 加一，因此同一条连接上严格递增。
// handleConn 单 goroutine 串行读请求，guard 不需要加锁；不同连接各持一份，
// 互不影响，客户端在新连接上从 1 重新计数也不会被误伤。
type cseqGuard struct {
	enabled bool
	maxGap  int // 0 表示不限制跳号
	last    int
	seen    bool
}

func newCSeqGuard(enabled bool, maxGap int) *cseqGuard {
	if maxGap < 0 {
		maxGap = 0
	}
	return &cseqGuard{enabled: enabled, maxGap: maxGap}
}

// allow 判断一条请求的 CSeq 是否可以处理。
//
// present=false 表示请求没带 CSeq 头（RFC 2326 要求所有请求都必须带）。
// 第一条请求只要求存在且非负，不要求从 1 开始；之后必须严格大于上一条。
// 被拒绝的请求不推进 last，避免一条异常请求把后续正常请求全部卡死。
func (g *cseqGuard) allow(cseq int, present bool) bool {
	if !g.enabled {
		return true
	}
	if !present || cseq < 0 {
		return false
	}
	if !g.seen {
		g.seen, g.last = true, cseq
		return true
	}
	if cseq <= g.last {
		return false
	}
	if g.maxGap > 0 && cseq-g.last > g.maxGap {
		return false
	}
	g.last = cseq
	return true
}

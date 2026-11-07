package rtsp

// 推流接收侧（ANNOUNCE / SETUP / RECORD）。
//
// 数据流：
//
//	$ 帧 / UDP → rtp.Parse → 按 PT 选轨 → 每轨解包器 → ExternalSource.Write
//
// H.264 走单 NALU / STAP-A / FU-A 重组，AAC 走 RFC 3640 AU-header；
// 解包后的 NALU/AU 复制一份再写源，因为 RTP 接收缓冲区会被复用。
//
// 丢包/乱序不 panic：H264Depacketizer 自己丢弃不完整 FU-A 半成品，
// 这里只记录 seq 缺口；AAC 一个包内自洽，头部非法就跳过该包。

import (
	"fmt"
	"strings"

	"flow/internal/media"
	"flow/internal/rtp"
	"flow/internal/session"
	"flow/internal/source"
)

// pushReceiver 一条推流轨的接收状态：解包器 + 解包后的帧去向。
// 每个 mediaTrack 独占一个，接收路径是单 goroutine（interleaved 由
// handleConn 串行处理，UDP 每轨一个读 goroutine），所以解包器不需要锁。
type pushReceiver struct {
	external *source.ExternalSource
	h264     *rtp.H264Depacketizer
	aac      *rtp.AACDepacketizer
}

// newPushReceiver 按 ANNOUNCE 协商出的编码创建解包器。
// external 是已发布流的帧源（拉流端也要订阅同一个）；为空时退回私有源，
// 兼容不经过 ANNOUNCE 注册发布的直接调用（单测）。
func newPushReceiver(encoding, fmtp string, external *source.ExternalSource) (*pushReceiver, error) {
	rx := &pushReceiver{external: external}
	switch strings.ToUpper(encoding) {
	case "H264":
		if rx.external == nil {
			rx.external = source.NewExternalSource()
		}
		rx.h264 = &rtp.H264Depacketizer{}
	case "MPEG4-GENERIC":
		if rx.external == nil {
			rx.external = source.NewExternalAudioSource()
		}
		cfg, err := rtp.ParseAACFmtp(fmtp)
		if err != nil {
			return nil, fmt.Errorf("parse AAC fmtp: %w", err)
		}
		d, err := rtp.NewAACDepacketizer(cfg)
		if err != nil {
			return nil, err
		}
		rx.aac = d
	default:
		return nil, fmt.Errorf("unsupported push encoding %q", encoding)
	}
	return rx, nil
}

// ensurePushReceiver 返回本轨的接收器，第一次调用时创建。
// 用 recvMu 保护 pushRx，RECORD 和 UDP 读 goroutine 可能同时到达。
func (t *mediaTrack) ensurePushReceiver() (*pushReceiver, error) {
	t.recvMu.Lock()
	defer t.recvMu.Unlock()
	if t.pushRx != nil {
		return t.pushRx, nil
	}
	rx, err := newPushReceiver(t.encoding, t.fmtp, t.external)
	if err != nil {
		return nil, err
	}
	t.pushRx = rx
	return rx, nil
}

// closePushReceiver 关闭 ExternalSource，唤醒拉流端读者。
// 之后到达的 RTP 仍然会被解包，但 Write 会被源忽略。
func (t *mediaTrack) closePushReceiver() {
	t.recvMu.Lock()
	rx := t.pushRx
	t.pushRx = nil
	t.recvMu.Unlock()
	if rx != nil {
		rx.external.Close()
	}
}

// handlePushedRTP 处理一个推流端发来的 RTP 报文。
// hint 是按 interleaved channel / UDP 端口初步确定的轨，最终以 PT 为准；
// 这样同一条连接上多轨复用或通道号未登记时也能路由。
func (s *Server) handlePushedRTP(st *Session, hint *mediaTrack, raw []byte) {
	pkt, err := rtp.Parse(raw)
	if err != nil {
		fmt.Printf("push RTP parse error: %v\n", err)
		return
	}

	track := hint
	if byPT, ok := st.pushTrackByPT(int(pkt.Header.PT)); ok {
		track = byPT
	}
	if track == nil {
		fmt.Printf("push RTP dropped: no push track for pt=%d\n", pkt.Header.PT)
		return
	}
	if track.payloadType != 0 && int(pkt.Header.PT) != track.payloadType {
		fmt.Printf("push RTP dropped: pt=%d does not match %s track pt=%d\n",
			pkt.Header.PT, track.kind, track.payloadType)
		return
	}

	rx, err := track.ensurePushReceiver()
	if err != nil {
		fmt.Printf("push receiver init (%s): %v\n", track.kind, err)
		return
	}

	ev := track.recordReceived(pkt, len(raw))
	if ev.first {
		fmt.Printf("push RTP recv (%s): first packet seq=%d ts=%d ssrc=%08x pt=%d len=%d\n",
			track.kind, pkt.Header.Seq, pkt.Header.Timestamp, pkt.Header.SSRC, pkt.Header.PT, len(raw))
	} else if ev.ssrcChanged {
		fmt.Printf("push RTP SSRC changed (%s): %08x -> %08x (in-progress FU-A will be dropped)\n",
			track.kind, ev.prevSSRC, pkt.Header.SSRC)
	}
	if ev.gap != 0 {
		gaps := track.gapCount()
		// 丢包密集时限制日志频率，但保留前 10 条和每 100 条一次。
		if gaps <= 10 || gaps%100 == 0 {
			if ev.gap > 0 {
				fmt.Printf("push RTP seq gap (%s): expected %d got %d (lost %d)\n",
					track.kind, ev.expected, pkt.Header.Seq, ev.gap)
			} else {
				fmt.Printf("push RTP seq out of order (%s): expected %d got %d (duplicate/reordered)\n",
					track.kind, ev.expected, pkt.Header.Seq)
			}
		}
	}

	switch {
	case rx.h264 != nil:
		before := rx.h264.Dropped
		nalus, err := rx.h264.Depacketize(pkt)
		if err != nil {
			fmt.Printf("push H.264 depacketize (%s): %v\n", track.kind, err)
			return
		}
		if lost := rx.h264.Dropped - before; lost > 0 {
			fmt.Printf("push H.264 incomplete NALU dropped (%s): %d\n", track.kind, lost)
		}
		for _, nalu := range nalus {
			if len(nalu) == 0 {
				continue
			}
			typ, _ := media.NALType(nalu)
			// 单 NALU/STAP-A 的切片指向接收缓冲区，必须复制后长期保存。
			data := append([]byte(nil), nalu...)
			rx.external.Write(source.Frame{
				Data:      data,
				Timestamp: pkt.Header.Timestamp,
				Keyframe:  typ == rtp.NALUTypeIDR,
			})
			if frames := track.recordFrame(); frames == 1 {
				fmt.Printf("push frame recv (%s): first frame type=%d ts=%d %d bytes\n",
					track.kind, typ, pkt.Header.Timestamp, len(data))
			}
		}

	case rx.aac != nil:
		aus, err := rx.aac.Depacketize(pkt)
		if err != nil {
			fmt.Printf("push AAC depacketize (%s): %v\n", track.kind, err)
			return
		}
		for i, au := range aus {
			// 一个 RTP 包可带多个 AU，时间戳按每 AU 1024 采样递增
			// （RFC 3640：包时间戳对应第一个 AU）。
			ts := pkt.Header.Timestamp + uint32(i)*rtp.AACFrameSamples
			data := append([]byte(nil), au.Data...)
			rx.external.Write(source.Frame{
				Data:      data,
				Timestamp: ts,
				Keyframe:  true,
			})
			if frames := track.recordFrame(); frames == 1 {
				fmt.Printf("push frame recv (%s): first AU %d bytes ts=%d\n",
					track.kind, len(data), ts)
			}
		}
	}

	if ev.count%100 == 0 {
		packets, bytes, frames, gaps := track.recvSummary()
		fmt.Printf("push RTP recv (%s): packets=%d frames=%d bytes=%d gaps=%d\n",
			track.kind, packets, frames, bytes, gaps)
	}
}

// serveUDPRTP 从 SETUP 分配的 UDP RTP 端口收推流数据。
// 端口关闭（teardown / 会话回收 / 重复 SETUP 替换旧轨）时 ReadFromUDP 返回错误，
// goroutine 随之退出。
func (s *Server) serveUDPRTP(track *mediaTrack, sess *session.Session) {
	st := sess.Data.(*Session)
	fmt.Printf("push UDP RTP receiver started (%s, port=%d)\n", track.kind, track.serverRtpPort)
	buf := make([]byte, 64*1024)
	for {
		n, _, err := track.rtpConn.ReadFromUDP(buf)
		if err != nil {
			fmt.Printf("push UDP RTP receiver stopped (%s): %v\n", track.kind, err)
			return
		}
		// 推流端可能不发 RTSP 保活，收到的媒体报文本身就是活跃证据，
		// 否则 60s 会被会话回收误杀。
		sess.TouchRTCP()
		s.handlePushedRTP(st, track, buf[:n])
	}
}

// recvEvent 记录一个 RTP 报文后的变化，供日志和统计使用。
type recvEvent struct {
	count       uint64
	first       bool
	expected    uint16 // 按上一包推算出的期望 seq
	gap         int    // 正数=丢包数，负数=重复/乱序
	prevSSRC    uint32
	ssrcChanged bool
}

// recordReceived 记录一个收到的 RTP 报文并更新统计。
func (t *mediaTrack) recordReceived(pkt *rtp.RTPPacket, rawLen int) recvEvent {
	t.recvMu.Lock()
	defer t.recvMu.Unlock()

	ev := recvEvent{first: t.recvPackets == 0}
	if !ev.first {
		ev.expected = t.recvLastSeq + 1
		if delta := pkt.Header.Seq - ev.expected; delta != 0 {
			if delta < 0x8000 {
				ev.gap = int(delta) // 正整数：中间缺了 gap 个包
			} else {
				ev.gap = int(delta) - 0x10000 // 负数：重复或乱序
			}
			t.recvGaps++
		}
		ev.ssrcChanged = pkt.Header.SSRC != t.recvLastSSRC
		ev.prevSSRC = t.recvLastSSRC
	}

	t.recvPackets++
	t.recvBytes += uint64(rawLen)
	t.recvLastSeq = pkt.Header.Seq
	t.recvLastSSRC = pkt.Header.SSRC
	t.recvLastPT = pkt.Header.PT
	ev.count = t.recvPackets
	return ev
}

// recordFrame 记录一帧写入了 ExternalSource，返回累计帧数。
func (t *mediaTrack) recordFrame() uint64 {
	t.recvMu.Lock()
	defer t.recvMu.Unlock()
	t.recvFrames++
	return t.recvFrames
}

// gapCount 返回累计 seq 缺口次数（日志限频用）。
func (t *mediaTrack) gapCount() uint64 {
	t.recvMu.Lock()
	defer t.recvMu.Unlock()
	return t.recvGaps
}

// recvSummary 返回接收统计终态：包数、字节数、帧数、缺口次数。
func (t *mediaTrack) recvSummary() (packets, bytes, frames, gaps uint64) {
	t.recvMu.Lock()
	defer t.recvMu.Unlock()
	return t.recvPackets, t.recvBytes, t.recvFrames, t.recvGaps
}

// recvSnapshot 返回接收统计快照（测试使用）。
func (t *mediaTrack) recvSnapshot() (packets, bytes uint64, lastSeq uint16, lastSSRC uint32, lastPT uint8) {
	t.recvMu.Lock()
	defer t.recvMu.Unlock()
	return t.recvPackets, t.recvBytes, t.recvLastSeq, t.recvLastSSRC, t.recvLastPT
}

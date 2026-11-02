package rtsp

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"flow/internal/rtcp"
	"flow/internal/session"
)

// mediaTrack 一条媒体轨（video/audio）的传输参数。
// 一个 Session 下有两条轨，各自一对端口、各自的目标地址。
type mediaTrack struct {
	kind            string // "video" / "audio"
	isInterleaved   bool
	interleavedCh   string       // 原始文本，如 "0-1"
	interleavedRTP  int          // RTP channel（写 $ 帧时用）
	interleavedRTCP int          // RTCP channel
	conn            *Conn        // TCP 模式下共用的 RTSP 连接（UDP 模式为 nil）
	rtpConn         *net.UDPConn // 服务端 RTP socket（用来发）
	rtcpConn        *net.UDPConn // 服务端 RTCP socket（用来收发）
	clientAddr      *net.UDPAddr // 客户端 RTP 目标地址 ip:port
	clientRTCPAddr  *net.UDPAddr // 客户端 RTCP 目标地址 ip:port（= client_port+1）
	serverRtpPort   int          // 偶数
	serverRtcpPort  int          // 奇数 = RTP+1

	// RTCP SR 的发送者状态。clockRate 是 RTP 时间戳的时钟频率
	// （视频固定 90000，音频 = AAC 采样率），stats 由推流 goroutine 更新、
	// SR goroutine 每 5s 读一次。
	clockRate uint32
	stats     trackStats
}

// trackStats 记录这条轨已经发了多少 RTP，供 RTCP SR 填入 packet/octet count
// 和 NTP↔RTP 映射。推流 goroutine 写、SR goroutine 读，用锁串行化。
//
// ssrc 和 seq 存在 mediaTrack 上、跨 PAUSE/PLAY 保持：
//   - SSRC 在同一个 RTSP 会话里必须不变（RFC 3550），否则播放器会当成新的同步源；
//   - seq 要连续递增，恢复播放后不回退、不重置，Wireshark 里才看不到断层。
type trackStats struct {
	mu       sync.Mutex
	ssrc     uint32
	seq      uint32 // 下一个要用的 RTP seq（写进 RTP 头时截断成 uint16）
	packets  uint32
	octets   uint32
	lastRTP  uint32
	lastSent time.Time
}

// ensureSSRC 返回本路的 SSRC：第一次调用时随机生成并固定下来，
// PAUSE 之后再 PLAY 复用同一个值。0 表示还没分配过。
func (t *mediaTrack) ensureSSRC() uint32 {
	t.stats.mu.Lock()
	defer t.stats.mu.Unlock()
	if t.stats.ssrc == 0 {
		t.stats.ssrc = rand.Uint32()
	}
	return t.stats.ssrc
}

// nextSeq 返回下一个要用的 RTP seq 并自增。内部用 uint32 存，避免长会话
// 很快溢出；写进 RTP 头时再截断成 uint16（RFC 3550 的 seq 本来就是 16 位回绕）。
func (t *mediaTrack) nextSeq() uint32 {
	t.stats.mu.Lock()
	defer t.stats.mu.Unlock()
	n := t.stats.seq
	t.stats.seq++
	return n
}

// currentSSRC 返回本路已登记的 SSRC（RTCP SR 和日志用）。
func (t *mediaTrack) currentSSRC() uint32 {
	t.stats.mu.Lock()
	defer t.stats.mu.Unlock()
	return t.stats.ssrc
}

// setClockRate 设置 RTP 时钟频率（音频源解析出采样率后调用）。
func (t *mediaTrack) setClockRate(rate uint32) {
	if rate == 0 {
		return
	}
	t.stats.mu.Lock()
	t.clockRate = rate
	t.stats.mu.Unlock()
}

// recordSent 在一包 RTP 成功写出后更新统计：
// 包数、负载字节数（不含 RTP 头，RFC 3550 的 octet count 定义）、
// 最后一个时间戳及其发送时刻（用来推算 SR 的 RTP timestamp）。
func (t *mediaTrack) recordSent(rtpTS uint32, payloadLen int) {
	t.stats.mu.Lock()
	t.stats.packets++
	t.stats.octets += uint32(payloadLen)
	t.stats.lastRTP = rtpTS
	t.stats.lastSent = time.Now()
	t.stats.mu.Unlock()
}

// writeRTP 按这条轨的传输方式发送一个已经序列化好的 RTP 包：
// UDP 模式直接 WriteToUDP，TCP 模式套上 $ 帧再写。
func (t *mediaTrack) writeRTP(pkt []byte) error {
	if t.isInterleaved {
		return t.writeInterleavedChannel(t.interleavedRTP, pkt)
	}
	_, err := t.rtpConn.WriteToUDP(pkt, t.clientAddr)
	return err
}

// writeRTCP 按这条轨的传输方式发送 RTCP 包：
// UDP 模式发给客户端的 RTCP 端口（client_port+1），
// TCP 模式走协商出来的 RTCP channel（ffmpeg 下视频=1、音频=3）。
func (t *mediaTrack) writeRTCP(pkt []byte) error {
	if t.isInterleaved {
		return t.writeInterleavedChannel(t.interleavedRTCP, pkt)
	}
	if t.rtcpConn == nil || t.clientRTCPAddr == nil {
		return fmt.Errorf("rtcp transport is not configured")
	}
	_, err := t.rtcpConn.WriteToUDP(pkt, t.clientRTCPAddr)
	return err
}

// writeInterleavedChannel 把一个 RTCP/RTP 包按 $ 帧格式写到 RTSP 的 TCP 连接：
//
//	$ + channel(1) + length(2, 大端) + packet
func (t *mediaTrack) writeInterleavedChannel(channel int, pkt []byte) error {
	if len(pkt) > 0xFFFF {
		return fmt.Errorf("packet too large for interleaved: %d bytes", len(pkt))
	}
	if t.conn == nil {
		return fmt.Errorf("interleaved track has no tcp conn")
	}
	frame := make([]byte, 4+len(pkt))
	frame[0] = interleavedMagic
	frame[1] = byte(channel)
	binary.BigEndian.PutUint16(frame[2:4], uint16(len(pkt)))
	copy(frame[4:], pkt)
	return t.conn.write(frame)
}

// senderReport 生成当前时刻的 RTCP SR（PT=200）。
// NTP 填 now，RTP timestamp 填“如果现在发一包会用到的值”：
// 上一包的 RTP timestamp + 距上一包发送时刻经过的时间 × 时钟频率。
// 还没发过任何 RTP 时返回 false，避免客户端拿到错误的时间映射。
func (t *mediaTrack) senderReport(now time.Time) ([]byte, bool) {
	t.stats.mu.Lock()
	ssrc := t.stats.ssrc
	packets := t.stats.packets
	octets := t.stats.octets
	rtp := t.stats.lastRTP
	lastSent := t.stats.lastSent
	clockRate := t.clockRate
	t.stats.mu.Unlock()

	if lastSent.IsZero() {
		return nil, false
	}
	if elapsed := now.Sub(lastSent); elapsed > 0 && clockRate > 0 {
		rtp += uint32(elapsed.Seconds() * float64(clockRate))
	}
	sr := rtcp.SenderReport{
		SSRC:        ssrc,
		NTP:         rtcp.NTPTime(now),
		RTP:         rtp,
		PacketCount: packets,
		OctetCount:  octets,
	}
	return sr.Marshal(), true
}

// Session 是挂在 session.Session.Data 上的 RTSP 层状态：
// 一个会话下的 video/audio 两条轨。
// internal/session.Manager 只管生命周期（ID、状态、超时），不关心这些传输细节。
type Session struct {
	mu     sync.Mutex
	tracks map[string]*mediaTrack // key: "video" / "audio"

	// 每次 PLAY 的独立生命周期。PAUSE 时取消它，TEARDOWN 时由会话
	// context 级联取消。playMu 保护下面两个字段。
	playMu     sync.Mutex
	playCancel context.CancelFunc
	playDone   <-chan struct{}
}

func newSession() *Session {
	return &Session{tracks: make(map[string]*mediaTrack)}
}

// setTrack 登记一条轨，返回被替换掉的旧轨（重复 SETUP 同一条轨时用来关旧端口）。
func (s *Session) setTrack(kind string, t *mediaTrack) *mediaTrack {
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.tracks[kind]
	s.tracks[kind] = t
	return old
}

func (s *Session) track(kind string) *mediaTrack {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tracks[kind]
}

// startPlay 为一次 PLAY 建立独立的推流生命周期。
// parent 是会话 context：会话关闭时它会被取消，playCtx 随之级联取消。
// 如果已经有一个在播放的生命周期，返回它的 done 且 started=false，
// 调用方据此避免重复 PLAY 时再起一组推流 goroutine。
func (s *Session) startPlay(parent context.Context) (done <-chan struct{}, started bool) {
	if parent == nil {
		parent = context.Background()
	}
	s.playMu.Lock()
	defer s.playMu.Unlock()
	if s.playCancel != nil {
		return s.playDone, false
	}
	ctx, cancel := context.WithCancel(parent)
	s.playCancel = cancel
	s.playDone = ctx.Done()
	return s.playDone, true
}

// stopPlay 取消当前推流生命周期。PAUSE 调用；重复调用是安全的。
// 只停推流和 SR，不动 Session、源订阅和 UDP 端口，PLAY 可以重新开始。
func (s *Session) stopPlay() {
	s.playMu.Lock()
	cancel := s.playCancel
	s.playCancel = nil
	s.playDone = nil
	s.playMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// closeAll 关掉会话下所有轨的 UDP socket（TEARDOWN / 超时回收时由 OnClose 调用）。
func (s *Session) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.tracks {
		if t.rtpConn != nil {
			t.rtpConn.Close()
		}
		if t.rtcpConn != nil {
			t.rtcpConn.Close()
		}
	}
}

// hasRTCPChannel 判断 channel 是不是这个会话某条轨的 RTCP 通道。
// interleaved 模式下客户端发来的 RTCP RR 就走这个 channel。
func (s *Session) hasRTCPChannel(channel int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.tracks {
		if t.isInterleaved && t.interleavedRTCP == channel {
			return true
		}
	}
	return false
}

// sessionHeaderValue 拼 SETUP/PLAY/PAUSE/TEARDOWN 响应里的 Session 头。
func (s *Server) sessionHeaderValue(sess *session.Session) string {
	return fmt.Sprintf("%s;timeout=%d", sess.ID, int(s.sessions.Timeout()/time.Second))
}

// openUDPPortPair 开一对 RTP(偶)+RTCP(奇) 的 UDP socket，冲突就换下一对。
func openUDPPortPair() (rtpConn, rtcpConn *net.UDPConn, err error) {
	for base := 50000; base < 60000; base += 2 { // base 一定是偶数
		rtp, err := net.ListenUDP("udp", &net.UDPAddr{Port: base})
		if err != nil {
			continue
		}
		rtcp, err := net.ListenUDP("udp", &net.UDPAddr{Port: base + 1})
		if err != nil {
			rtp.Close()
			continue
		}
		return rtp, rtcp, nil
	}
	return nil, nil, fmt.Errorf("no udp port pair available")
}

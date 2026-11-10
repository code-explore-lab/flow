package rtsp

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"flow/internal/rtcp"
	"flow/internal/session"
	"flow/internal/source"
	"flow/internal/stream"
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

	// 推流（ANNOUNCE/RECORD）方向：客户端把 RTP 发到服务端，服务端只接收。
	// 与拉流共用 mediaTrack，但 SETUP 的语义相反，靠 isPush 区分。
	isPush      bool
	payloadType int    // ANNOUNCE SDP 协商出的 RTP payload type
	encoding    string // "H264" / "MPEG4-GENERIC"
	fmtp        string // ANNOUNCE SDP 的 a=fmtp（AAC 解包位宽用）

	// external 这条轨对应的推流源：
	//   - 推流端 SETUP 时指向已发布流（stream.Stream）的源（解包后写进去）；
	//   - 拉流端 SETUP 时指向同一个源（PLAY 后订阅它转发）；
	//   - 文件/live 模式为 nil，PLAY 时退回 Server 上的全局源。
	external *source.ExternalSource

	// 接收侧状态。pushRx 在 RECORD/SETUP 时创建，解包后的帧写进它的
	// ExternalSource；统计字段供验收对比「收到多少包/帧」。
	recvMu       sync.Mutex
	pushRx       *pushReceiver
	recvPackets  uint64
	recvBytes    uint64
	recvFrames   uint64
	recvGaps     uint64
	recvLastSeq  uint16
	recvLastSSRC uint32
	recvLastPT   uint8
}

// trackStats 记录这条轨已经发了多少 RTP，供 RTCP SR 填入 packet/octet count
// 和 NTP↔RTP 映射。推流 goroutine 写、SR goroutine 读，用锁串行化。
//
// ssrc 和 seq 存在 mediaTrack 上、跨 PAUSE/PLAY 保持：
//   - SSRC 在同一个 RTSP 会话里必须不变（RFC 3550），否则播放器会当成新的同步源；
//   - seq 要连续递增，恢复播放后不回退、不重置，Wireshark 里才看不到断层。
type trackStats struct {
	mu       sync.Mutex
	ssrc     uint32    // 本轨道RTP SSRC，整条RTSP会话固定不变
	seq      uint32    // 下一个RTP序列号，内部用uint32，发包截断uint16
	packets  uint32    // 已经发送RTP包总数（RTCP SR packet count）
	octets   uint32    // 负载字节总数（不含RTP头，SR octet count）
	lastRTP  uint32    // 最后一个发出包的RTP timestamp
	lastSent time.Time // 最后一包发送的系统时间，用来算SR里NTP <-> RTP时间映射
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
/*
$      channel     length(2B大端)     payload(原始RTP/RTCP包)
0x24   1字节        2字节              N字节

*/
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
	//elapsed 从上一包发出，到现在过去了多久
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

	// ANNOUNCE 带来的推流描述（P1-5）。announcedIndex 把规范化后的
	// a=control（streamid=0 / trackID=0 / 裸 "0"）映射到 announced 下标，
	// SETUP 时按 URI 里的 control 找回具体轨道（P1-6 使用）。
	//
	// push 为 true 表示这个会话是客户端推流（ANNOUNCE 创建），
	// SETUP 的方向是“服务端收 RTP”，而不是拉流会话的“服务端发 RTP”。
	announced      []announcedTrack
	announcedIndex map[string]int //setup根据url去找到对应的announcedTrack
	push           bool

	// pub 这个会话关联的已发布流：推流会话在 ANNOUNCE 时创建，
	// 拉流会话在 SETUP 时按 URL 找到并挂上。SETUP/PLAY 用它取编码参数和
	// ExternalSource。pubCleanup 在会话关闭时把这条流从 Server 注册表摘掉。
	pub        *stream.Stream
	pubCleanup func()

	// 每次 PLAY 的独立生命周期。PAUSE 时取消它，TEARDOWN 时由会话
	// context 级联取消。playMu 保护下面两个字段。
	playMu     sync.Mutex
	playCancel context.CancelFunc
	playDone   <-chan struct{}

	// endOnce 保证「推流源结束 → 回收拉流会话」只执行一次。
	// 视频/音频两条转发的 goroutine 可能同时看到源关闭。
	endOnce sync.Once
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

// setAnnounced 保存 ANNOUNCE 解析出的轨道列表，并建立 control → track 索引。
// 同一个会话重复 ANNOUNCE 时整体替换。
func (s *Session) setAnnounced(tracks []announcedTrack) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.push = true
	s.announced = append([]announcedTrack(nil), tracks...)
	s.announcedIndex = make(map[string]int, len(tracks)*3)
	for i, t := range tracks {
		// 客户端 SDP 里可能写 streamid=N，SETUP URI 也可能用 trackID=N，
		// 两种写法和裸 N 都指向同一条轨。
		//sdp带trackID
		s.announcedIndex[controlLookupKey(t.Control)] = i
		if t.TrackID != "" {
			s.announcedIndex[controlLookupKey("trackID="+t.TrackID)] = i
			s.announcedIndex[strings.ToLower(t.TrackID)] = i
		}
	}
}

// isPush 判断会话是不是推流会话（ANNOUNCE 创建，客户端向服务端发 RTP）。
func (s *Session) isPush() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.push
}

// setPublished 给会话挂上已发布流。cleanup 可为 nil（拉流会话不需要注销）。
// 重复 ANNOUNCE 时整体替换，并保留最后一次注册的 cleanup。
func (s *Session) setPublished(pub *stream.Stream, cleanup func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pub = pub
	if cleanup != nil {
		s.pubCleanup = cleanup
	}
}

// markEnded 标记「源结束回收」已经执行过，返回本次是否是第一次调用。
func (s *Session) markEnded() bool {
	first := false
	s.endOnce.Do(func() { first = true })
	return first
}

// published 返回会话关联的已发布流（推流/拉流都可能非 nil）。
func (s *Session) published() *stream.Stream {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pub
}

// pushTrackByRTPChannel 按 interleaved 的 RTP channel 找推流轨。
// 只有推流轨才匹配，拉流会话同一通道号不会被误判成收到的媒体。
func (s *Session) pushTrackByRTPChannel(channel int) (*mediaTrack, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.tracks {
		if t.isPush && t.isInterleaved && t.interleavedRTP == channel {
			return t, true
		}
	}
	return nil, false
}

// pushTrackByPT 按 RTP payload type 找推流轨。
// interleaved 已经用 channel 定位，UDP 用端口定位；PT 路由用于
// 通道号未登记或同一条连接上多轨复用时的兜底，也能校验报文归属。
func (s *Session) pushTrackByPT(pt int) (*mediaTrack, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.tracks {
		if t.isPush && t.payloadType == pt {
			return t, true
		}
	}
	return nil, false
}

// trackCount 返回已 SETUP 的轨数量（RECORD 校验用）。
func (s *Session) trackCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.tracks)
}

// tracksSnapshot 返回已 SETUP 轨的快照（RECORD 时逐条初始化接收器）。
func (s *Session) tracksSnapshot() []*mediaTrack {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*mediaTrack, 0, len(s.tracks))
	for _, t := range s.tracks {
		out = append(out, t)
	}
	return out
}

// announcedTracks 返回 ANNOUNCE 轨道列表的快照。
func (s *Session) announcedTracks() []announcedTrack {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]announcedTrack(nil), s.announced...)
}

// announcedTrack 按 control 查找轨道。control 可以是 "streamid=0"、
// "trackID=0"、裸 "0"，也可以是带路径的完整 URL。
func (s *Session) announcedTrack(control string) (announcedTrack, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, ok := s.announcedIndex[controlLookupKey(control)]
	if !ok {
		return announcedTrack{}, false
	}
	return s.announced[i], true
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

// closeAll 关掉会话下所有轨的 UDP socket 和推流接收源
// （TEARDOWN / 超时回收时由 OnClose 调用）。
//
// 推流会话还会把已发布流从 Server 注册表摘掉并关闭源：拉流端的 reader
// 读完剩余帧后返回 false，不会永远挂在等待上。
func (s *Session) closeAll() {
	s.mu.Lock()
	tracks := make([]*mediaTrack, 0, len(s.tracks))
	for _, t := range s.tracks {
		tracks = append(tracks, t)
	}
	cleanup := s.pubCleanup
	s.pubCleanup = nil
	s.mu.Unlock()

	for _, t := range tracks {
		if t.isPush {
			// 打一条终态统计，便于和推流端（ffmpeg）的发送量对比。
			packets, bytes, frames, gaps := t.recvSummary()
			fmt.Printf("push recv summary (%s): packets=%d frames=%d bytes=%d gaps=%d\n",
				t.kind, packets, frames, bytes, gaps)
			t.closePushReceiver()
		}
		if t.rtpConn != nil {
			t.rtpConn.Close()
		}
		if t.rtcpConn != nil {
			t.rtcpConn.Close()
		}
	}
	// 放在锁外：unpublish 会拿 Server 的锁并关闭源，避免和别的路径形成锁序环。
	if cleanup != nil {
		cleanup()
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

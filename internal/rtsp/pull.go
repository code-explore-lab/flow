package rtsp

// S-1：RTSP 拉流客户端（回源）。
//
// 与服务端共用 SDP 解析（parseAnnounceSDP）与 RTP 解包器；区别只在于
// 这里是主动连上游：DESCRIBE → 逐轨 SETUP（TCP interleaved）→ PLAY，
// 然后把收到的 RTP 解成 source.Frame 写进调用方给的共享流。
//
// 一次 Pull 只覆盖一条上游连接；重连、退避与本地流注册由 origin.Puller
// 负责。时间戳通过调用方传入的 rebase 函数平移，跨重连保持单调。

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"flow/internal/media"
	"flow/internal/rtp"
	"flow/internal/source"
	"flow/internal/stream"
)

// 拉流默认参数。
const (
	defaultPullDialTimeout = 10 * time.Second
	defaultPullIdleTimeout = 60 * time.Second
	pullKeepaliveInterval  = 20 * time.Second
	defaultRTSPPort        = "554"
)

// ErrStreamNotFound 表示上游明确回答「没有这路流」（DESCRIBE 404/454）。
var ErrStreamNotFound = errors.New("rtsp: stream not found")

// PullOptions 一次 RTSP 拉流会话的参数。
//
// URL 里带 userinfo（rtsp://user:pass@host/live）时按上游 401 challenge
// 自动做 Basic 或 Digest 鉴权；不带凭据时收到 401 直接失败。
type PullOptions struct {
	URL    *url.URL
	Stream *stream.Stream
	// VideoRebase / AudioRebase 可选，用来把 RTP 时间戳平移到跨重连单调。
	VideoRebase func(uint32) uint32
	AudioRebase func(uint32) uint32
	// OnFirstMedia 在第一条音视频帧写出后调用（每次会话首次）。
	OnFirstMedia func()
	// DialTimeout 覆盖建连/请求阶段超时；<=0 用默认值。
	DialTimeout time.Duration
	// IdleTimeout 是「上游还连着但完全没有媒体」的判定窗口；<=0 用默认值。
	IdleTimeout time.Duration
}

// Pull 建立一次上游 RTSP 会话并读到断开。played 表示 PLAY 已成功。
func Pull(ctx context.Context, o PullOptions) (played bool, err error) {
	if o.Stream == nil {
		return false, errors.New("rtsp pull: nil stream")
	}
	if o.URL == nil {
		return false, errors.New("rtsp pull: nil url")
	}
	if o.DialTimeout <= 0 {
		o.DialTimeout = defaultPullDialTimeout
	}
	if o.IdleTimeout <= 0 {
		o.IdleTimeout = defaultPullIdleTimeout
	}

	scheme := o.URL.Scheme
	if scheme == "" {
		scheme = "rtsp"
	}
	addr := o.URL.Host
	if o.URL.Port() == "" {
		addr = net.JoinHostPort(o.URL.Hostname(), defaultRTSPPort)
	}
	base := scheme + "://" + addr + strings.TrimRight(o.URL.Path, "/")
	describeURI := base
	if o.URL.RawQuery != "" {
		describeURI += "?" + o.URL.RawQuery
	}

	dialer := net.Dialer{Timeout: o.DialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return false, fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	c := &pullConn{
		conn:         conn,
		br:           bufio.NewReaderSize(conn, 64<<10),
		base:         base,
		stream:       o.Stream,
		channels:     make(map[int]*pullTrack),
		writeTimeout: o.DialTimeout,
		readTimeout:  o.DialTimeout,
		idleTimeout:  o.IdleTimeout,
		onFirst:      o.OnFirstMedia,
		auth:         newPullAuth(o.URL.User),
	}

	// ctx 取消时关闭连接，唤醒阻塞在 Read 上的读循环。
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()

	resp, err := c.request("DESCRIBE", describeURI, [][2]string{{"Accept", "application/sdp"}})
	if err != nil {
		return false, fmt.Errorf("DESCRIBE: %w", err)
	}
	switch code := resp.code(); code {
	case 200:
	case 404, 454:
		return false, fmt.Errorf("%w: DESCRIBE %s: %s", ErrStreamNotFound, describeURI, resp.status)
	default:
		return false, fmt.Errorf("DESCRIBE %s: %s", describeURI, resp.status)
	}

	tracks, err := parseAnnounceSDP(resp.body)
	if err != nil {
		return false, fmt.Errorf("parse SDP: %w", err)
	}
	applyTracks(o.Stream, tracks)

	// 每条轨一个 interleaved 通道对；同一类型只建第一条（本地流模型是
	// 单视频 + 单音频）。
	nextCh := 0
	session := ""
	seen := make(map[string]bool)
	var ok bool
	for _, at := range tracks {
		if seen[at.Kind] {
			continue
		}
		seen[at.Kind] = true

		rtpCh, rtcpCh := nextCh, nextCh+1
		nextCh += 2
		resp, err := c.request("SETUP", setupURL(base, at), [][2]string{
			{"Transport", fmt.Sprintf("RTP/AVP/TCP;unicast;interleaved=%d-%d", rtpCh, rtcpCh)},
		})
		if err != nil {
			return false, fmt.Errorf("SETUP %s: %w", at.Control, err)
		}
		if resp.code() != 200 {
			return false, fmt.Errorf("SETUP %s: %s", at.Control, resp.status)
		}
		if session == "" {
			session = sessionID(resp.headers["session"])
		}
		// 上游可能调整通道号，以响应里的 Transport 为准。
		if _, inter, _, err := extractTransportInfo(resp.headers["transport"]); err == nil && inter != "" {
			if got, _, err := parseInterleaved(inter); err == nil {
				rtpCh = got
			}
		}

		rebase := o.VideoRebase
		if at.Kind == "audio" {
			rebase = o.AudioRebase
		}
		track, err := newPullTrack(at, rebase)
		if err != nil {
			return false, fmt.Errorf("track %s/%s: %w", at.Kind, at.Encoding, err)
		}
		c.tracks = append(c.tracks, track)
		c.channels[rtpCh] = track
		ok = true
	}
	if !ok {
		return false, errors.New("SDP has no supported audio/video track")
	}
	if session == "" {
		return false, errors.New("SETUP responses carry no Session")
	}
	c.videoParams = videoParamsFromSDP(tracks)

	resp, err = c.request("PLAY", describeURI, [][2]string{{"Session", session}})
	if err != nil {
		return false, fmt.Errorf("PLAY: %w", err)
	}
	if resp.code() != 200 {
		return false, fmt.Errorf("PLAY %s: %s", describeURI, resp.status)
	}

	c.lastMedia = time.Now()
	c.mediaLoop(session)
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	return true, c.err
}

// mediaLoop 读 interleaved 媒体与保活响应，直到出错或 ctx 取消。
func (c *pullConn) mediaLoop(session string) {
	nextKeepalive := time.Now().Add(pullKeepaliveInterval)
	for {
		now := time.Now()
		if now.Sub(c.lastMedia) >= c.idleTimeout {
			c.err = fmt.Errorf("no media for %s", c.idleTimeout)
			return
		}
		if !now.Before(nextKeepalive) {
			if _, err := c.request("GET_PARAMETER", c.base, [][2]string{{"Session", session}}); err != nil {
				c.err = fmt.Errorf("keepalive: %w", err)
				return
			}
			nextKeepalive = time.Now().Add(pullKeepaliveInterval)
			continue
		}

		deadline := nextKeepalive
		if idle := c.lastMedia.Add(c.idleTimeout); idle.Before(deadline) {
			deadline = idle
		}
		_ = c.conn.SetReadDeadline(deadline)
		resp, err := c.readMessage()
		if err != nil {
			if isNetTimeout(err) {
				continue
			}
			c.err = fmt.Errorf("read: %w", err)
			return
		}
		if resp != nil {
			// PLAY 之后的响应（保活、ANNOUNCE 等）不影响媒体转发，忽略。
			continue
		}
	}
}

// pullConn 一条 RTSP 客户端连接。
type pullConn struct {
	conn net.Conn
	br   *bufio.Reader
	base string

	stream *stream.Stream
	tracks []*pullTrack

	channels map[int]*pullTrack // interleaved RTP channel → 轨

	cseq          int
	writeTimeout  time.Duration
	readTimeout   time.Duration
	idleTimeout   time.Duration
	lastMedia     time.Time
	onFirst       func()
	auth          *pullAuth
	fired         bool
	videoParams   [][]byte
	paramsWritten bool
	err           error
}

// pullTrack 一条上游媒体轨的接收状态。
type pullTrack struct {
	kind        string // "video" / "audio"
	encoding    string // "H264" / "H265"
	payloadType uint8

	h264     *rtp.H264Depacketizer
	h265     *rtp.H265Depacketizer
	aac      *rtp.AACDepacketizer
	mpa      *rtp.MPADepacketizer
	rawAudio bool

	rebase func(uint32) uint32

	au         auAssembler
	frameDur   float64
	dts        float64
	dtsSet     bool
	lastDTS    uint32
	mp3RateSet bool
}

// newPullTrack 按 SDP 编码创建解包器。
func newPullTrack(at announcedTrack, rebase func(uint32) uint32) (*pullTrack, error) {
	if rebase == nil {
		rebase = func(v uint32) uint32 { return v }
	}
	t := &pullTrack{
		kind:        at.Kind,
		encoding:    at.Encoding,
		payloadType: uint8(at.PayloadType),
		rebase:      rebase,
	}
	if at.Framerate > 0 {
		t.frameDur = 90000 / at.Framerate
	}
	switch at.Kind {
	case "video":
		if isHEVCEncoding(at.Encoding) {
			t.encoding = "H265"
			t.h265 = &rtp.H265Depacketizer{}
		} else {
			t.encoding = "H264"
			t.h264 = &rtp.H264Depacketizer{}
		}
	case "audio":
		switch audioCodecFromSDP(at.Encoding) {
		case stream.AudioAAC:
			cfg, err := rtp.ParseAACFmtp(at.FMTP)
			if err != nil {
				return nil, err
			}
			d, err := rtp.NewAACDepacketizer(cfg)
			if err != nil {
				return nil, err
			}
			t.aac = d
		case stream.AudioMP3:
			t.mpa = rtp.NewMPADepacketizer()
		default:
			// PCMA / PCMU / Opus：RTP 负载就是完整音频帧。
			t.rawAudio = true
		}
	default:
		return nil, fmt.Errorf("unsupported track kind %q", at.Kind)
	}
	return t, nil
}

// request 发一条 RTSP 请求并等响应；途中到达的 interleaved 媒体照常处理。
// 带凭据时收到 401 会按 challenge 重发一次（Digest 的 nonce 过期也走这里）。
func (c *pullConn) request(method, uri string, headers [][2]string) (*rtspResponse, error) {
	for attempt := 0; attempt < 2; attempt++ {
		c.cseq++
		var b strings.Builder
		fmt.Fprintf(&b, "%s %s RTSP/1.0\r\nCSeq: %d\r\n", method, uri, c.cseq)
		for _, h := range headers {
			fmt.Fprintf(&b, "%s: %s\r\n", h[0], h[1])
		}
		if auth := c.auth.header(method, uri); auth != "" {
			fmt.Fprintf(&b, "Authorization: %s\r\n", auth)
		}
		b.WriteString("\r\n")

		_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		if _, err := io.WriteString(c.conn, b.String()); err != nil {
			return nil, err
		}
		var resp *rtspResponse
		for {
			_ = c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
			r, err := c.readMessage()
			if err != nil {
				return nil, err
			}
			if r != nil {
				resp = r
				break
			}
		}
		if resp.code() == 401 && attempt == 0 && c.auth.challenge(resp.headers["www-authenticate"]) {
			continue
		}
		return resp, nil
	}
	return nil, errors.New("rtsp pull: authentication failed")
}

// pullAuth 是拉流端的上游鉴权状态。凭据来自 URL 的 user:pass，方案由上游
// 401 响应的 WWW-Authenticate 决定（Basic 或 Digest MD5）。
//
// 首条请求不带 Authorization；拿到 challenge 后按方案计算并重发。Digest 的
// 每个请求都换 cnonce 并递增 nc，nonce 过期时上游会再次 401，request 会
// 重新学习 challenge 后重试。
type pullAuth struct {
	user, pass string
	scheme     string // "" / "basic" / "digest"
	realm      string
	nonce      string
	qop        string // "auth" 或空（RFC 2069 旧式摘要）
	nc         uint32
}

// newPullAuth 从 URL userinfo 创建鉴权状态；没有凭据时返回 nil。
func newPullAuth(u *url.Userinfo) *pullAuth {
	if u == nil {
		return nil
	}
	pass, _ := u.Password()
	return &pullAuth{user: u.Username(), pass: pass}
}

// challenge 学习 WWW-Authenticate；不支持的方案返回 false。
func (a *pullAuth) challenge(header string) bool {
	if a == nil {
		return false
	}
	h := strings.TrimSpace(header)
	switch {
	case len(h) >= 5 && strings.EqualFold(h[:5], "basic"):
		a.scheme = "basic"
		return true
	case len(h) >= 6 && strings.EqualFold(h[:6], "digest"):
		p := parseAuthParams(h[6:])
		if p["nonce"] == "" {
			return false
		}
		if alg := p["algorithm"]; alg != "" && !strings.EqualFold(alg, "MD5") {
			return false
		}
		a.scheme = "digest"
		a.realm = p["realm"]
		a.nonce = p["nonce"]
		a.qop = ""
		if q := strings.TrimSpace(p["qop"]); q != "" {
			ok := false
			for _, v := range strings.Split(q, ",") {
				if strings.EqualFold(strings.TrimSpace(v), "auth") {
					ok = true
					break
				}
			}
			if !ok {
				return false // auth-int 需要摘要请求体，这里不支持
			}
			a.qop = "auth"
		}
		return true
	}
	return false
}

// header 生成当前方案的 Authorization 值；未认证时返回空串。
func (a *pullAuth) header(method, uri string) string {
	if a == nil {
		return ""
	}
	switch a.scheme {
	case "basic":
		raw := a.user + ":" + a.pass
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(raw))
	case "digest":
		a.nc++
		nc := fmt.Sprintf("%08x", a.nc)
		var seed [8]byte
		_, _ = rand.Read(seed[:])
		cnonce := hex.EncodeToString(seed[:])

		ha1 := md5Hex(a.user + ":" + a.realm + ":" + a.pass)
		ha2 := md5Hex(method + ":" + uri)
		var response string
		if a.qop == "auth" {
			response = md5Hex(ha1 + ":" + a.nonce + ":" + nc + ":" + cnonce + ":auth:" + ha2)
		} else {
			response = md5Hex(ha1 + ":" + a.nonce + ":" + ha2)
		}
		h := fmt.Sprintf(`Digest username=%s, realm=%s, nonce=%s, uri=%s, response=%s, algorithm=MD5`,
			quoteAuth(a.user), quoteAuth(a.realm), quoteAuth(a.nonce), quoteAuth(uri), quoteAuth(response))
		if a.qop == "auth" {
			h += fmt.Sprintf(`, qop=auth, nc=%s, cnonce=%s`, nc, quoteAuth(cnonce))
		}
		return h
	}
	return ""
}

// quoteAuth 给 Digest 头里的字符串加引号并转义。
func quoteAuth(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s) + `"`
}

// readMessage 读一条消息：interleaved 媒体返回 (nil, nil)，RTSP 响应返回
// (resp, nil)。
func (c *pullConn) readMessage() (*rtspResponse, error) {
	first, err := c.br.Peek(1)
	if err != nil {
		return nil, err
	}
	if first[0] != '$' {
		return c.readResponse()
	}
	var head [4]byte
	if _, err := io.ReadFull(c.br, head[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint16(head[2:4]))
	payload := make([]byte, n)
	if _, err := io.ReadFull(c.br, payload); err != nil {
		return nil, err
	}
	c.handleInterleaved(int(head[1]), payload)
	return nil, nil
}

// handleInterleaved 把一帧 interleaved 数据交给对应轨。
func (c *pullConn) handleInterleaved(channel int, payload []byte) {
	track := c.channels[channel]
	if track == nil {
		return // RTCP 或未使用的通道
	}
	pkt, err := rtp.Parse(payload)
	if err != nil {
		return
	}
	if track.payloadType != 0 && pkt.Header.PT != track.payloadType {
		return
	}
	c.lastMedia = time.Now()
	if track.kind == "video" {
		track.feedVideo(c, pkt)
		return
	}
	track.feedAudio(c, pkt)
}

// feedVideo 解包一个视频 RTP 包，按 AU 攒齐后写源。
func (t *pullTrack) feedVideo(c *pullConn, pkt *rtp.RTPPacket) {
	ts := t.rebase(pkt.Header.Timestamp)
	var (
		nalus [][]byte
		err   error
	)
	if t.h265 != nil {
		nalus, err = t.h265.Depacketize(pkt)
	} else {
		nalus, err = t.h264.Depacketize(pkt)
	}
	if err != nil {
		return
	}

	if t.au.has && t.au.ts != ts {
		// 没等到 marker 就换时间戳：上一个 AU 的尾包丢了。
		c.writeVideoAU(t)
	}
	if !t.au.has {
		t.au.has = true
		t.au.ts = ts
	}
	for _, nalu := range nalus {
		if len(nalu) == 0 {
			continue
		}
		// 解包结果指向接收缓冲区，复制后长期保存。
		t.au.nalus = append(t.au.nalus, append([]byte(nil), nalu...))
	}
	if pkt.Header.Marker {
		c.writeVideoAU(t)
	}
}

// writeVideoAU 把攒好的一组 NALU 写进共享流。
func (c *pullConn) writeVideoAU(t *pullTrack) {
	if !t.au.has {
		return
	}
	ts := t.au.ts
	nalus := t.au.nalus
	t.au.reset()
	if len(nalus) == 0 {
		return
	}
	src := c.stream.SourceFor("video")
	if src == nil {
		return
	}
	// SDP 里的 sprop 参数集先于首帧写入：ExternalSource 按 NAL 类型缓存，
	// 晚加入的拉流端订阅时无需等下一个关键帧。时间戳跟随首帧，避免把
	// 重连后的下游时间轴拉回 0。
	if !c.paramsWritten {
		c.paramsWritten = true
		for _, p := range c.videoParams {
			src.Write(source.Frame{Data: p, Timestamp: ts})
		}
	}
	cts := t.compositionTime(ts)
	for i, nalu := range nalus {
		class := media.ClassifyVideoNAL(t.encoding, nalu)
		src.Write(source.Frame{
			Data:            nalu,
			Timestamp:       ts,
			CompositionTime: cts,
			Keyframe:        class.IsIDR,
			AUStart:         i == 0,
			AUEnd:           i == len(nalus)-1,
		})
	}
	c.fireFirst()
}

// compositionTime 按 SDP 的 a=framerate 生成 DTS，返回 CTS = PTS − DTS。
// 没有帧率信息时返回 0（DTS=PTS）。
func (t *pullTrack) compositionTime(pts uint32) int32 {
	if t.frameDur <= 0 {
		return 0
	}
	if !t.dtsSet {
		t.dts = float64(pts)
		t.dtsSet = true
		t.lastDTS = pts
		return 0
	}
	t.dts += t.frameDur
	dts := uint32(t.dts + 0.5)
	if dts <= t.lastDTS {
		dts = t.lastDTS + 1
	}
	t.lastDTS = dts
	return int32(pts - dts)
}

// feedAudio 解包一个音频 RTP 包并写源。
func (t *pullTrack) feedAudio(c *pullConn, pkt *rtp.RTPPacket) {
	ts := t.rebase(pkt.Header.Timestamp)
	switch {
	case t.aac != nil:
		aus, err := t.aac.Depacketize(pkt)
		if err != nil {
			return
		}
		for i, au := range aus {
			c.writeAudio(append([]byte(nil), au.Data...), ts+uint32(i)*rtp.AACFrameSamples)
		}
	case t.mpa != nil:
		frames, ok := t.mpa.Depacketize(pkt)
		if !ok {
			return
		}
		for _, f := range frames {
			if !t.mp3RateSet {
				if info, ok := media.ParseMP3Frame(f.Data); ok {
					t.mp3RateSet = true
					// SDP 拿不到 MP3 采样率，用首个完整帧头修正。
					c.stream.SetAudioCodec(stream.AudioMP3, nil,
						info.SampleRate, info.Channels, 90000)
				}
			}
			c.writeAudio(f.Data, t.rebase(f.Timestamp))
		}
	default:
		if len(pkt.Payload) == 0 {
			return
		}
		c.writeAudio(append([]byte(nil), pkt.Payload...), ts)
	}
}

// writeAudio 把一帧音频写进共享流。
func (c *pullConn) writeAudio(data []byte, ts uint32) {
	src := c.stream.SourceFor("audio")
	if src == nil || len(data) == 0 {
		return
	}
	src.Write(source.Frame{Data: data, Timestamp: ts, Keyframe: true})
	c.fireFirst()
}

// fireFirst 触发首帧回调（每次会话只触发一次）。
func (c *pullConn) fireFirst() {
	if c.fired || c.onFirst == nil {
		return
	}
	c.fired = true
	c.onFirst()
}

// readResponse 读一条 RTSP 响应（状态行 + 头 + Content-Length 指定的 body）。
func (c *pullConn) readResponse() (*rtspResponse, error) {
	line, err := c.br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "RTSP/") {
		return nil, fmt.Errorf("invalid status line %q", line)
	}
	resp := &rtspResponse{status: line, headers: make(map[string]string)}
	for {
		raw, err := c.br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		raw = strings.TrimRight(raw, "\r\n")
		if raw == "" {
			break
		}
		key, value, ok := strings.Cut(raw, ":")
		if !ok {
			continue
		}
		resp.headers[strings.ToLower(strings.TrimSpace(key))] = strings.TrimSpace(value)
	}
	if cl := resp.headers["content-length"]; cl != "" {
		n, err := strconv.Atoi(cl)
		if err != nil || n < 0 || n > maxRequestBody {
			return nil, fmt.Errorf("bad Content-Length %q", cl)
		}
		resp.body = make([]byte, n)
		if n > 0 {
			if _, err := io.ReadFull(c.br, resp.body); err != nil {
				return nil, err
			}
		}
	}
	return resp, nil
}

// rtspResponse 客户端看到的响应。
type rtspResponse struct {
	status  string
	headers map[string]string
	body    []byte
}

// code 返回状态码；解析失败时为 0。
func (r *rtspResponse) code() int {
	f := strings.Fields(r.status)
	if len(f) < 2 {
		return 0
	}
	n, _ := strconv.Atoi(f[1])
	return n
}

// sessionID 去掉 Session 头里的 ;timeout= 等参数。
func sessionID(v string) string {
	if i := strings.IndexByte(v, ';'); i >= 0 {
		return strings.TrimSpace(v[:i])
	}
	return strings.TrimSpace(v)
}

// setupURL 拼出某条轨的 SETUP URI。
func setupURL(base string, at announcedTrack) string {
	control := strings.TrimSpace(at.Control)
	if control == "" {
		control = "trackID=" + at.TrackID
	}
	if strings.HasPrefix(control, "rtsp://") || strings.HasPrefix(control, "rtsps://") {
		return control
	}
	if strings.HasPrefix(control, "/") {
		return base + control
	}
	return base + "/" + control
}

// videoParamsFromSDP 取 H.264 / H.265 的 sprop 参数集，供首帧前写入源。
func videoParamsFromSDP(tracks []announcedTrack) [][]byte {
	for _, at := range tracks {
		if at.Kind != "video" {
			continue
		}
		if isHEVCEncoding(at.Encoding) {
			vps, sps, pps, ok := parseSpropHEVC(at.FMTP)
			if !ok {
				return nil
			}
			return [][]byte{vps, sps, pps}
		}
		sps, pps, ok := parseSpropParameterSets(at.FMTP)
		if !ok {
			return nil
		}
		return [][]byte{sps, pps}
	}
	return nil
}

// isNetTimeout 判断错误是否为读超时（用于区分保活唤醒与真正断开）。
func isNetTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

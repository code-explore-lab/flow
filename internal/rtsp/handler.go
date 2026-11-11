package rtsp

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"flow/internal/media"
	"flow/internal/session"
	"flow/internal/stream"
)

// handleConn 处理一条 RTSP TCP 连接直到关闭。
func (s *Server) handleConn(conn *Conn) {
	s.registerConn(conn)
	defer s.unregisterConn(conn)
	defer func() {
		conn.Close()
		// 连接断了说明客户端没了：立刻回收这条连接上的会话，
		// 而不是干等 30s 扫描 + 60s 超时（最坏 90s）。
		for _, id := range conn.boundSessions() {
			if sess, ok := s.sessions.Get(id); ok {
				s.sessions.Delete(id)
				sess.Close()
				fmt.Printf("session %s closed with tcp conn\n", id)
			}
		}
	}()

	// 整个连接共用一个 reader，RTSP 文本和 $ 二进制帧都从它读。
	br := bufio.NewReader(conn)
	for {
		// 控制连接静默超过「会话超时 + 一次扫描周期」才算连接异常：
		// 会话回收由 session.Manager 负责，这里只是兜底，防止对端半开时 goroutine 永久挂住。
		_ = conn.SetReadDeadline(time.Now().Add(s.sessions.Timeout() + session.DefaultScanInterval))

		// 先 peek 一个字节做分支：'$' 走 interleaved，字母走 RTSP 文本。
		first, err := br.Peek(1)
		if err != nil {
			fmt.Println("read conn err:", err)
			return
		}
		if first[0] == interleavedMagic {
			ch, payload, err := readInterleavedFrame(br)
			if err != nil {
				fmt.Println("read interleaved err:", err)
				return
			}
			s.handleInterleaved(conn, ch, payload)
			continue
		}

		req, err := readRequest(br)
		if err != nil {
			fmt.Println("readRequest err:", err)
			return
		}
		fmt.Printf("recv: %s %s, CSeq=%d\n", req.Method, req.URI, req.CSeq)
		hd := map[string]string{"CSeq": strconv.Itoa(req.CSeq)}

		// OPTIONS 用于探测服务端能力，放行不鉴权；其余方法在启用鉴权时
		// 必须带有效凭据。Digest nonce 过期时 challenge 带 stale=true，
		// 客户端会自动换 nonce 重试。401 不泄露请求细节。
		if req.Method != "OPTIONS" {
			if res := s.checkRequestAuth(req); !res.ok {
				if res.stale {
					fmt.Printf("auth stale: %s %s\n", req.Method, req.URI)
				} else {
					fmt.Printf("auth failed: %s %s\n", req.Method, req.URI)
				}
				hd["WWW-Authenticate"] = s.authChallenge(res.stale)
				hd["Content-Length"] = "0"
				if err := writeResponse(conn, "401 Unauthorized", hd, nil); err != nil {
					fmt.Println("write resp err:", err)
					return
				}
				continue
			}
		}

		var respErr error
		switch req.Method {
		case "OPTIONS":
			hd["Public"] = "OPTIONS, DESCRIBE, ANNOUNCE, SETUP, PLAY, PAUSE, RECORD, TEARDOWN"
			respErr = writeResponse(conn, "200 OK", hd, nil)
		case "DESCRIBE":
			hd["Content-Type"] = "application/sdp"
			// 记住命中的已发布流，后续 SETUP 即使 URI 写法不同也能找回来。
			if pub := s.publishedFromURI(req.URI); pub != nil {
				conn.setDescribeStream(pub.Name)
			} else {
				conn.setDescribeStream("")
			}
			sdp, errSDP := s.buildSDP(req.URI)
			if errSDP != nil {
				respErr = writeResponse(conn, "500 Internal Server Error", hd, nil)
				break
			}
			respErr = writeResponse(conn, "200 OK", hd, []byte(sdp))
		case "ANNOUNCE":
			respErr = s.handleAnnounce(conn, req, hd)
		case "SETUP":
			respErr = s.handleSetup(conn, req, hd)
		case "PLAY":
			respErr = s.handlePlay(conn, req, hd)
		case "PAUSE":
			respErr = s.handlePause(conn, req, hd)
		case "RECORD":
			respErr = s.handleRecord(conn, req, hd)
		case "TEARDOWN":
			respErr = s.handleTeardown(conn, req, hd)
		default:
			respErr = writeResponse(conn, "501 Not Implemented", hd, nil)
		}
		if respErr != nil {
			fmt.Println("write resp err:", respErr)
			return
		}
	}
}

// handleInterleaved 处理客户端通过 TCP 发来的 interleaved 帧。
//
// 拉流会话主要用它发 RTCP（RR/SDES 等），只需要刷新活跃时间；
// 推流会话（ANNOUNCE/RECORD）则会在协商出的 RTP channel 上发来媒体数据，
// 这里按 channel 找到对应轨，交给 handlePushedRTP（P1-6 只计数，P1-7 解包）。
func (s *Server) handleInterleaved(conn *Conn, channel byte, payload []byte) {
	// 先按推流轨的 RTP channel 路由。RTP 的 PT 是 96/97 这类动态值，
	// 与 RTCP 的 200~204 不冲突，但必须优先判断：拉流会话没有 isPush 轨，
	// 不会把对端 RTCP 误当媒体。
	for _, id := range conn.boundSessions() {
		sess, ok := s.sessions.Get(id)
		if !ok {
			continue
		}
		st := sess.Data.(*Session)
		if track, ok := st.pushTrackByRTPChannel(int(channel)); ok {
			if len(payload) >= 12 { // 小于 RTP 固定头，丢弃
				sess.TouchRTCP() // 收到的媒体报文也算活跃
				s.handlePushedRTP(st, track, payload)
			}
			return
		}
	}

	// 通道号没登记时退回按 PT 路由（例如客户端协商后改用其他 channel）。
	// 只对推流会话生效，拉流会话的 RTCP channel 不会走到这里。
	if len(payload) >= 12 {
		pt := int(payload[1] & 0x7f)
		for _, id := range conn.boundSessions() {
			sess, ok := s.sessions.Get(id)
			if !ok {
				continue
			}
			st := sess.Data.(*Session)
			if !st.isPush() {
				continue
			}
			if _, ok := st.pushTrackByPT(pt); !ok {
				continue
			}
			sess.TouchRTCP()
			s.handlePushedRTP(st, nil, payload)
			return
		}
	}

	if len(payload) < 2 {
		return
	}
	// RTCP 包类型在第二个字节（第一字节是 V/P/RC），200~204 是 SR/RR/SDES/BYE/APP。
	pt := payload[1]
	if pt < 200 || pt > 204 {
		fmt.Printf("interleaved recv: channel=%d len=%d (not rtcp)\n", channel, len(payload))
		return
	}
	for _, id := range conn.boundSessions() {
		sess, ok := s.sessions.Get(id)
		if !ok {
			continue
		}
		st := sess.Data.(*Session)
		if st.hasRTCPChannel(int(channel)) {
			sess.TouchRTCP()
			fmt.Printf("RTCP recv: channel=%d len=%d pt=%d (session=%s)\n",
				channel, len(payload), pt, id)
		}
	}
}

// buildSDP 生成 DESCRIBE 响应的 SDP 文本。
//
// 优先按 URI 查找已发布流：推流场景下编码参数（rtpmap/fmtp，包括
// sprop-parameter-sets 和 AAC config）全部来自 ANNOUNCE，拉流端与推流端一致。
// 没有已发布流时退回本地文件/实时源的原有逻辑。
func (s *Server) buildSDP(uri string) (string, error) {
	if pub := s.publishedFromURI(uri); pub != nil {
		return publishedSDP(pub), nil
	}

	sdp := "v=0\r\n" +
		"o=- 0 0 IN IP4 127.0.0.1\r\n" +
		"s=TestStream\r\n" +
		"c=IN IP4 127.0.0.1\r\n" +
		"t=0 0\r\n" +
		"m=video 0 RTP/AVP 96\r\n" +
		"a=rtpmap:96 H264/90000\r\n"
	if s.live {
		// 实时模式下源还没启动（第一次 Subscribe 在 PLAY），DESCRIBE 时拿不到
		// SPS/PPS，只能省略 sprop-parameter-sets，让客户端从带内参数集初始化。
		sdp += "a=fmtp:96 packetization-mode=1\r\n"
	} else {
		spspps, err := media.GetSPSPPSBase64(s.videoPath)
		if err != nil {
			return "", err
		}
		sdp += "a=fmtp:96 packetization-mode=1; sprop-parameter-sets=" + spspps + "\r\n"
	}
	sdp += "a=control:trackID=0\r\n"

	if s.audio != nil {
		aacInfo, err := media.GetAACInfo(s.audioPath)
		if err != nil {
			return "", err
		}
		sdp += aacInfo.MediaDescription(media.AACPT) +
			"a=control:trackID=1\r\n"
	}
	return sdp, nil
}

// newClientSession 新建一个 RTSP 会话，并把 RTSP 层的轨道状态挂到 Data 上。
// 拉流（SETUP 建会话）和推流（ANNOUNCE 建会话）共用。
func (s *Server) newClientSession(conn *Conn) *session.Session {
	ip, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
	sess := s.sessions.NewSession(ip)
	st := newSession()
	sess.Data = st
	sess.OnClose = st.closeAll
	fmt.Printf("new session %s from %s\n", sess.ID, ip)
	return sess
}

// handleAnnounce 处理 ANNOUNCE：解析推流端带来的 SDP，记录轨道列表和
// control → trackID 映射，返回 200 和 Session（后续 SETUP/RECORD 复用）。
//
//	400 Bad Request            SDP 结构非法（空 body、没有 m=、rtpmap 缺失等）
//	415 Unsupported Media Type SDP 合法但编码不支持（只支持 H264 / MPEG4-GENERIC）
//	454 Session Not Found      带了 Session 头但服务端没有这个会话
func (s *Server) handleAnnounce(conn *Conn, req *Request, hd map[string]string) error {
	tracks, err := parseAnnounceSDP(req.Body)
	if err != nil {
		if errors.Is(err, errUnsupportedCodec) {
			fmt.Printf("ANNOUNCE rejected (415): %v\n", err)
			return writeResponse(conn, "415 Unsupported Media Type", hd, nil)
		}
		fmt.Printf("ANNOUNCE rejected (400): %v\n", err)
		return writeResponse(conn, "400 Bad Request", hd, nil)
	}

	// ANNOUNCE 可以带 Session 更新已有会话；ffmpeg 首次推送不带，新建一个。
	var sess *session.Session
	if sh, ok := req.Headers["Session"]; ok {
		id := strings.Split(sh, ";")[0]
		found, foundOK := s.sessions.Get(id)
		if !foundOK {
			return writeResponse(conn, "454 Session Not Found", hd, nil)
		}
		sess = found
		sess.TouchRTSP()
	} else if id := conn.announceSession(); id != "" {
		// 同一条连接上重复 ANNOUNCE：复用上次建的会话。
		if found, foundOK := s.sessions.Get(id); foundOK {
			sess = found
		}
	}
	if sess == nil {
		sess = s.newClientSession(conn)
	}

	rtspSess := sess.Data.(*Session)
	rtspSess.setAnnounced(tracks)

	// 按 URL 路径登记已发布流：拉流端 DESCRIBE/SETUP 用同一个路径找到它。
	// 同一个会话重复 ANNOUNCE 时替换旧流；会话关闭时再从注册表摘掉。
	name := streamKeyFromURI(req.URI)
	pub := s.publish(name, tracks)
	rtspSess.setPublished(pub, func() { s.unpublish(name, pub) })

	conn.bindSession(sess.ID)
	conn.setAnnounceSession(sess.ID)

	fmt.Printf("ANNOUNCE session %s: %d track(s) from %s\n", sess.ID, len(tracks), sess.RemoteIP)
	for i, t := range tracks {
		fmt.Printf("  track %d: %s\n", i, t)
	}

	hd["Session"] = s.sessionHeaderValue(sess)
	return writeResponse(conn, "200 OK", hd, nil)
}

// handleSetup 处理 SETUP：分配/复用会话，建视频或音频轨，回 Transport + Session。
func (s *Server) handleSetup(conn *Conn, req *Request, hd map[string]string) error {
	transReq, ok := req.Headers["Transport"]
	if !ok {
		return writeResponse(conn, "400 Bad Request", hd, nil)
	}
	isInter, interStr, clientPortStr, errPort := extractTransportInfo(transReq)
	if errPort != nil {
		return writeResponse(conn, "400 Bad Request", hd, nil)
	}

	// 第二次 SETUP 会带回第一次下发的 SessionID，必须复用它，不能新建会话。
	var sess *session.Session
	if sh, ok := req.Headers["Session"]; ok {
		sessID := strings.Split(sh, ";")[0]
		var found bool
		sess, found = s.sessions.Get(sessID)
		if !found {
			return writeResponse(conn, "454 Session Not Found", hd, nil)
		}
		sess.TouchRTSP()
	} else if id := conn.announceSession(); id != "" {
		// 推流端在 ANNOUNCE 响应里拿到 Session 通常会回带；
		// 万一没带，用同一条连接上 ANNOUNCE 建的会话，保留已解析的 SDP。
		if found, foundOK := s.sessions.Get(id); foundOK {
			sess = found
			sess.TouchRTSP()
		}
	}
	if sess == nil {
		sess = s.newClientSession(conn)
	}
	conn.bindSession(sess.ID)
	rtspSess := sess.Data.(*Session)

	// 判断这次 SETUP 建哪条轨。
	// 推流模式（ANNOUNCE 已建会话）：URI 里的 control 去 announced 索引查，
	//   拿到客户端 SDP 里写死的 kind / clockRate / payloadType。
	// 拉流模式：先按 URL 找到已发布流，用它 ANNOUNCE 的编码参数；
	//   找不到已发布流时才退回 trackID/streamid 映射和本地源。
	announced := rtspSess.announcedTracks()
	// 有 ANNOUNCE 轨道说明这是推流会话：SETUP 的方向是客户端 → 服务端。
	push := len(announced) > 0

	var pub *stream.Stream
	if !push {
		describeName := conn.describeStreamName()
		pub = s.publishedFromURI(req.URI)
		if pub == nil && describeName != "" {
			// 有些客户端 SETUP 的 URL 与 DESCRIBE 不同，用连接上记录的流兜底。
			pub = s.publishedStreamByName(describeName)
		}
		if pub == nil && describeName != "" {
			// DESCRIBE 时流还在、SETUP 时已经下线：不能退回本地文件源冒充，
			// 否则客户端会收到完全不相关的 testdata 流。
			return writeResponse(conn, "404 Not Found", hd, nil)
		}
		if pub != nil {
			rtspSess.setPublished(pub, nil)
		}
	}

	kind := ""
	clockRate := uint32(90000)
	payloadType := 0
	encoding := ""
	fmtp := ""
	switch {
	case push:
		if at, ok := rtspSess.announcedTrack(req.URI); ok {
			kind = at.Kind
			clockRate = uint32(at.ClockRate)
			payloadType = at.PayloadType
			encoding = at.Encoding
			fmtp = at.FMTP
		}
	case pub != nil:
		if at, ok := trackByControl(pub, req.URI); ok {
			kind = at.Kind
			clockRate = uint32(at.ClockRate)
			payloadType = at.PayloadType
			encoding = at.Encoding
			fmtp = at.FMTP
		}
	}
	if kind == "" {
		kind = trackKindFromURI(req.URI)
	}
	if kind == "" {
		kind = "video"
		if rtspSess.track("video") != nil {
			kind = "audio"
		}
	}
	// URI 里没有 control 段时按 kind 补齐已发布流的编码参数。
	if pub != nil && payloadType == 0 {
		if at, ok := firstTrackOfKind(pub, kind); ok {
			clockRate = uint32(at.ClockRate)
			payloadType = at.PayloadType
			encoding = at.Encoding
			fmtp = at.FMTP
		}
	}

	// 音频轨是否存在：
	//   推流模式 → announced SDP 里有没有 m=audio；
	//   拉流模式 → 已发布流里有没有音频轨，或服务端有没有挂本地音频源。
	if kind == "audio" {
		hasAudio := s.audio != nil
		if pub != nil && pub.AudioTrack() != nil {
			hasAudio = true
		}
		for _, at := range announced {
			if at.Kind == "audio" {
				hasAudio = true
				break
			}
		}
		if !hasAudio {
			return writeResponse(conn, "404 Not Found", hd, nil)
		}
		// 本地文件模式音频 clockRate 先用 44100 占位，streamAAC 拿到首帧后会覆盖；
		// 推流/已发布流模式已从 ANNOUNCE 的 rtpmap 取到实际采样率。
		if len(announced) == 0 && pub == nil {
			clockRate = 44100
		}
	}

	t := &mediaTrack{
		kind:          kind,
		isInterleaved: isInter,
		interleavedCh: interStr,
		conn:          conn,
		clockRate:     clockRate,
		isPush:        push,
		payloadType:   payloadType,
		encoding:      encoding,
		fmtp:          fmtp,
	}
	// 推流端写这个源、拉流端订阅同一个源；本地文件/live 模式保持 nil。
	pubForTrack := pub
	if push {
		pubForTrack = rtspSess.published()
	}
	if pubForTrack != nil {
		t.external = pubForTrack.SourceFor(kind)
	}

	var respTransport string
	if isInter {
		// TCP interleaved 模式：不开任何 UDP 端口，只记录 channel 对。
		rtpCh, rtcpCh, errCh := parseInterleaved(interStr)
		if errCh != nil {
			return writeResponse(conn, "400 Bad Request", hd, nil)
		}
		t.interleavedRTP = rtpCh
		t.interleavedRTCP = rtcpCh
		respTransport = fmt.Sprintf("RTP/AVP/TCP;unicast;interleaved=%d-%d", rtpCh, rtcpCh)
		if push {
			respTransport += ";mode=record"
		}
	} else {
		rtpConn, rtcpConn, errUDP := openUDPPortPair()
		if errUDP != nil {
			return writeResponse(conn, "500 Internal Server Error", hd, nil)
		}
		cRtpPort, cRtcpPort := parseClientPorts(clientPortStr)
		t.rtpConn = rtpConn
		t.rtcpConn = rtcpConn
		t.clientAddr = &net.UDPAddr{IP: net.ParseIP(sess.RemoteIP), Port: cRtpPort}
		t.clientRTCPAddr = &net.UDPAddr{IP: net.ParseIP(sess.RemoteIP), Port: cRtcpPort}
		t.serverRtpPort = rtpConn.LocalAddr().(*net.UDPAddr).Port
		t.serverRtcpPort = rtcpConn.LocalAddr().(*net.UDPAddr).Port
		respTransport = fmt.Sprintf("RTP/AVP;unicast;client_port=%s;server_port=%d-%d",
			clientPortStr, t.serverRtpPort, t.serverRtcpPort)
		if push {
			respTransport += ";mode=record"
		}
		// 收 RTCP：给会话打活跃点，超时回收靠它。
		go func(kind string, rtcpConn *net.UDPConn, sess *session.Session) {
			buf := make([]byte, 2048)
			for {
				n, _, err := rtcpConn.ReadFromUDP(buf)
				if err != nil {
					return
				}
				sess.TouchRTCP()
				fmt.Printf("RTCP recv %d bytes (%s, session=%s)\n", n, kind, sess.ID)
			}
		}(kind, rtcpConn, sess)
	}

	// 同一个 Session 下按轨登记；重复 SETUP 同一条轨时先关掉旧端口。
	if old := rtspSess.setTrack(kind, t); old != nil {
		if old.rtpConn != nil {
			old.rtpConn.Close()
		}
		if old.rtcpConn != nil {
			old.rtcpConn.Close()
		}
	}
	// 推流 UDP 模式：客户端把 RTP 发到 server_port，服务端必须有人读，
	// 否则内核缓冲区满了以后丢包。TCP interleaved 由 handleConn 统一读。
	if push && !isInter {
		go s.serveUDPRTP(t, sess)
	}

	hd["Transport"] = respTransport
	hd["Session"] = s.sessionHeaderValue(sess)
	return writeResponse(conn, "200 OK", hd, nil)
}

// handlePlay 处理 PLAY：校验状态、建立推流生命周期、起音视频和 SR goroutine。
func (s *Server) handlePlay(conn *Conn, req *Request, hd map[string]string) error {
	sessHeader, ok := req.Headers["Session"]
	if !ok {
		return writeResponse(conn, "400 Bad Request", hd, nil)
	}
	sessID := strings.Split(sessHeader, ";")[0]
	sess, ok := s.sessions.Get(sessID)
	if !ok {
		return writeResponse(conn, "454 Session Not Found", hd, nil)
	}
	sess.TouchRTSP()

	// Init / Paused / Playing 都允许 PLAY。
	// Playing 状态下重复 PLAY 按 RFC 2326 是“更新播放参数”（例如改 Range），
	// 这里直接回 200，复用已有推流生命周期，不重复起 goroutine。
	st := sess.State()
	if st != session.StateInit && st != session.StatePaused && st != session.StatePlaying {
		return writeResponse(conn, "455 Method Not Valid In This State", hd, nil)
	}

	sess.SetState(session.StatePlaying)
	rtspSess := sess.Data.(*Session)
	conn.bindSession(sess.ID)

	hd["Session"] = s.sessionHeaderValue(sess)
	if err := writeResponse(conn, "200 OK", hd, nil); err != nil {
		return err
	}

	// 每次 PLAY 一条独立的推流生命周期，父 context 是会话 context，
	// TEARDOWN/超时回收时会级联取消。PAUSE 通过 stopPlay 单独取消。
	playDone, started := rtspSess.startPlay(sess.Context())
	if !started {
		fmt.Printf("session %s PLAY ignored: stream already running\n", sess.ID)
		return nil
	}
	video := rtspSess.track("video")
	audio := rtspSess.track("audio")
	// 已发布流的轨用 track.external（推流端解包后写入的源），
	// 本地文件/live 模式退回 Server 上的全局源。
	// 源结束（推流端断开）时回收本拉流会话，让客户端立即收到 EOF；
	// 只是 PAUSE/TEARDOWN 导致 done 关闭时不做额外清理。
	if video != nil {
		if src := s.videoSourceFor(video); src != nil {
			go func() {
				if s.streamH264(video, src, playDone) {
					s.endPullSession(sess, conn, rtspSess)
				}
			}()
		}
	}
	if audio != nil {
		if src := s.audioSourceFor(audio); src != nil {
			go func() {
				if s.streamAAC(audio, src, playDone) {
					s.endPullSession(sess, conn, rtspSess)
				}
			}()
		}
	}
	// 每 5s 发一次 RTCP SR，带 NTP↔RTP 映射；随 PLAY 生命周期启停。
	if video != nil {
		go sendSenderReports(video, playDone)
	}
	if audio != nil {
		go sendSenderReports(audio, playDone)
	}
	return nil
}

// endPullSession 在推流源结束后回收拉流会话（P1-9）。
//
// 源关闭只意味着「没有新帧了」：拉流端的转发 goroutine 会退出，但 RTSP
// 连接还开着，播放器会一直等下去，直到 60s 会话超时才被回收。这里主动
// 关掉会话和 TCP 连接，客户端读到 EOF，立即知道流结束。
//
// 只处理源自然结束的情况；PAUSE/TEARDOWN/连接断开走原有的关闭路径。
// endOnce 防止音视频两条转发 goroutine 同时触发。
func (s *Server) endPullSession(sess *session.Session, conn *Conn, rtspSess *Session) {
	if !rtspSess.markEnded() {
		return
	}
	fmt.Printf("session %s closed: pull source ended\n", sess.ID)
	rtspSess.stopPlay()
	s.sessions.Delete(sess.ID)
	sess.Close()
	if conn != nil {
		conn.Close()
	}
}

// handlePause 处理 PAUSE：先停推流再回 200，客户端收到响应后不会再看到新 RTP。
func (s *Server) handlePause(conn *Conn, req *Request, hd map[string]string) error {
	sessHeader, ok := req.Headers["Session"]
	if !ok {
		return writeResponse(conn, "400 Bad Request", hd, nil)
	}
	sessID := strings.Split(sessHeader, ";")[0]
	sess, ok := s.sessions.Get(sessID)
	if !ok {
		return writeResponse(conn, "454 Session Not Found", hd, nil)
	}
	sess.TouchRTSP()
	if sess.State() != session.StatePlaying {
		return writeResponse(conn, "455 Method Not Valid In This State", hd, nil)
	}
	rtspSess := sess.Data.(*Session)
	rtspSess.stopPlay()
	sess.SetState(session.StatePaused)
	hd["Session"] = s.sessionHeaderValue(sess)
	return writeResponse(conn, "200 OK", hd, nil)
}

// handleRecord 处理推流端的 RECORD：确认会话和 SETUP 好的轨都存在，
// 进入 Recording 状态并回 200。ffmpeg 收到 200 后才开始发 $ 帧/RTP 报文；
// 报文本身的解包和落源在 P1-7。
//
//	400 Bad Request           没带 Session
//	454 Session Not Found     会话不存在
//	455 Method Not Valid In This State 不是推流会话，或还没 SETUP 任何轨
func (s *Server) handleRecord(conn *Conn, req *Request, hd map[string]string) error {
	sessHeader, ok := req.Headers["Session"]
	if !ok {
		return writeResponse(conn, "400 Bad Request", hd, nil)
	}
	sessID := strings.Split(sessHeader, ";")[0]
	sess, ok := s.sessions.Get(sessID)
	if !ok {
		return writeResponse(conn, "454 Session Not Found", hd, nil)
	}
	sess.TouchRTSP()

	rtspSess := sess.Data.(*Session)
	if !rtspSess.isPush() || rtspSess.trackCount() == 0 {
		return writeResponse(conn, "455 Method Not Valid In This State", hd, nil)
	}
	// 每条推流轨按 ANNOUNCE 的编码准备解包器和 ExternalSource。
	for _, t := range rtspSess.tracksSnapshot() {
		if _, err := t.ensurePushReceiver(); err != nil {
			fmt.Printf("RECORD session %s: init %s receiver: %v\n", sess.ID, t.kind, err)
			return writeResponse(conn, "500 Internal Server Error", hd, nil)
		}
	}

	conn.bindSession(sess.ID)
	sess.SetState(session.StateRecording)
	hd["Session"] = s.sessionHeaderValue(sess)
	fmt.Printf("RECORD session %s: %d track(s) ready to receive\n", sess.ID, rtspSess.trackCount())
	return writeResponse(conn, "200 OK", hd, nil)
}

// handleTeardown 处理 TEARDOWN：关闭并移除会话。
func (s *Server) handleTeardown(conn *Conn, req *Request, hd map[string]string) error {
	sessHeader, ok := req.Headers["Session"]
	if !ok {
		return writeResponse(conn, "400 Bad Request", hd, nil)
	}
	sessID := strings.Split(sessHeader, ";")[0]
	if sess, ok := s.sessions.Get(sessID); ok {
		sess.TouchRTSP()
		s.sessions.Delete(sessID)
		sess.Close()
		fmt.Printf("session %s teardown by client\n", sessID)
	}
	hd["Session"] = fmt.Sprintf("%s;timeout=%d", sessID, int(s.sessions.Timeout()/time.Second))
	return writeResponse(conn, "200 OK", hd, nil)
}

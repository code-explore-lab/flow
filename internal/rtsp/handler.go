package rtsp

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"flow/internal/media"
	"flow/internal/session"
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
		var respErr error
		switch req.Method {
		case "OPTIONS":
			hd["Public"] = "OPTIONS, DESCRIBE, SETUP, PLAY, PAUSE, TEARDOWN"
			respErr = writeResponse(conn, "200 OK", hd, nil)
		case "DESCRIBE":
			hd["Content-Type"] = "application/sdp"
			sdp, errSDP := s.buildSDP()
			if errSDP != nil {
				respErr = writeResponse(conn, "500 Internal Server Error", hd, nil)
				break
			}
			respErr = writeResponse(conn, "200 OK", hd, []byte(sdp))
		case "SETUP":
			respErr = s.handleSetup(conn, req, hd)
		case "PLAY":
			respErr = s.handlePlay(conn, req, hd)
		case "PAUSE":
			respErr = s.handlePause(conn, req, hd)
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
// 客户端主要用它发 RTCP（RR/SDES 等），这里只刷新会话活跃时间。
func (s *Server) handleInterleaved(conn *Conn, channel byte, payload []byte) {
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
// 文件模式下 video + audio 两条 m= 行；实时模式只有 video（带内参数集）。
func (s *Server) buildSDP() (string, error) {
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
	} else {
		ip, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
		sess = s.sessions.NewSession(ip)
		st := newSession()
		sess.Data = st
		sess.OnClose = st.closeAll
		fmt.Printf("new session %s from %s\n", sess.ID, ip)
	}
	conn.bindSession(sess.ID)
	rtspSess := sess.Data.(*Session)

	// 判断这次 SETUP 建哪条轨：优先看 URI 里的 trackID/streamid，
	// 客户端没带就按“video 先、audio 后”的顺序推断。
	kind := trackKindFromURI(req.URI)
	if kind == "" {
		kind = "video"
		if rtspSess.track("video") != nil {
			kind = "audio"
		}
	}
	// 实时模式只有视频源，没有音频可建。
	if kind == "audio" && s.audio == nil {
		return writeResponse(conn, "404 Not Found", hd, nil)
	}

	// RTCP SR 的 RTP 时钟频率：视频固定 90kHz；音频是 AAC 采样率，
	// streamAAC 拿到第一帧后会按实际值覆盖。
	clockRate := uint32(90000)
	if kind == "audio" {
		clockRate = 44100
	}
	t := &mediaTrack{
		kind:          kind,
		isInterleaved: isInter,
		interleavedCh: interStr,
		conn:          conn,
		clockRate:     clockRate,
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
	if video != nil {
		go s.streamH264(video, playDone)
	}
	if audio != nil && s.audio != nil {
		go s.streamAAC(audio, playDone)
	}
	// 每 5s 发一次 RTCP SR，带 NTP↔RTP 映射；随 PLAY 生命周期启停。
	if video != nil {
		go sendSenderReports(video, playDone)
	}
	if audio != nil && s.audio != nil {
		go sendSenderReports(audio, playDone)
	}
	return nil
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

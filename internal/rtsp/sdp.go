package rtsp

// ANNOUNCE 请求体 SDP 的解析（RFC 2326 §10.2 / RFC 4566）。
//
// 推流端（ffmpeg）在 ANNOUNCE 里带一份描述将要推什么流的 SDP，本文件只关心：
//
//	m=video/audio <port> RTP/AVP <payload type>
//	a=rtpmap:<pt> <encoding>/<clock>[/<channels>]
//	a=fmtp:<pt> <format specific parameters>
//	a=control:<control>
//
// 每条 m= 行对应一条媒体轨；a=control 用来把后面的 SETUP URI
// （例如 rtsp://host/live/streamid=0）映射回具体轨道。

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	errInvalidSDP       = errors.New("invalid SDP")
	errUnsupportedCodec = errors.New("unsupported SDP codec")
)

// announcedTrack 从 ANNOUNCE SDP 解析出的一条媒体轨。
type announcedTrack struct {
	Kind        string // "video" / "audio"
	Control     string // 规范化后的 a=control，如 "streamid=0"；缺失时为 "trackID=<n>"
	TrackID     string // control 里的值，如 "0"
	PayloadType int    // SDP 动态 payload type
	Encoding    string // rtpmap 的编码名，如 "H264" / "MPEG4-GENERIC"
	ClockRate   int    // RTP 时钟频率
	Channels    int    // 音频声道数，视频为 0
	FMTP        string // a=fmtp 的原始参数串
}

// String 用于 ANNOUNCE 日志。
func (t announcedTrack) String() string {
	s := fmt.Sprintf("%s pt=%d encoding=%s clock=%d", t.Kind, t.PayloadType, t.Encoding, t.ClockRate)
	if t.Channels > 0 {
		s += fmt.Sprintf(" channels=%d", t.Channels)
	}
	s += fmt.Sprintf(" control=%s (trackID=%s)", t.Control, t.TrackID)
	if t.FMTP != "" {
		s += " fmtp={" + t.FMTP + "}"
	}
	return s
}

// sdpMedia 一个 m= 段的解析中间状态。
type sdpMedia struct {
	kind    string
	formats []string // 规范化后的 payload type 列表
	rtpmap  map[string]sdpRTPMap
	fmtp    map[string]string
	control string
}

type sdpRTPMap struct {
	encoding  string
	clockRate int
	channels  int
}

// parseAnnounceSDP 解析 ANNOUNCE 的 SDP 请求体。
//
// 返回的错误分为两类，调用方据此回不同状态码：
//   - errUnsupportedCodec：SDP 合法，但编码/媒体类型不支持 → 415；
//   - errInvalidSDP：SDP 结构非法 → 400。
func parseAnnounceSDP(body []byte) ([]announcedTrack, error) {
	text := strings.TrimSpace(string(body))
	if text == "" {
		return nil, fmt.Errorf("%w: empty body", errInvalidSDP)
	}

	lines := strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
	var (
		tracks     []announcedTrack
		cur        *sdpMedia
		sawVersion bool
	)

	// flush 把当前 m= 段解析成一条轨。延迟到下一个 m= 或文末，
	// 这样该段的 a= 属性都收齐了。
	flush := func() error {
		if cur == nil {
			return nil
		}
		t, err := cur.resolve(len(tracks))
		cur = nil
		if err != nil {
			return err
		}
		tracks = append(tracks, t)
		return nil
	}

	for _, raw := range lines {
		line := strings.TrimRight(raw, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		if len(line) < 2 || line[1] != '=' {
			return nil, fmt.Errorf("%w: malformed line %q", errInvalidSDP, line)
		}
		typ, val := line[0], line[2:]
		switch typ {
		case 'v':
			sawVersion = true
		case 'm':
			if err := flush(); err != nil {
				return nil, err
			}
			m, err := newSDPMedia(val)
			if err != nil {
				return nil, err
			}
			cur = m
		case 'a':
			// 只处理媒体级属性；第一个 m= 之前的是会话级属性，与具体轨道无关。
			if cur == nil {
				continue
			}
			if err := cur.addAttribute(val); err != nil {
				return nil, err
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	if !sawVersion {
		return nil, fmt.Errorf("%w: missing v= line", errInvalidSDP)
	}
	if len(tracks) == 0 {
		return nil, fmt.Errorf("%w: no m= line", errInvalidSDP)
	}
	return tracks, nil
}

// newSDPMedia 解析 m=<media> <port> <proto> <fmt ...> 行。
func newSDPMedia(val string) (*sdpMedia, error) {
	fields := strings.Fields(val)
	if len(fields) < 4 {
		return nil, fmt.Errorf("%w: bad m= line %q", errInvalidSDP, val)
	}
	kind := strings.ToLower(fields[0])
	if kind != "video" && kind != "audio" {
		return nil, fmt.Errorf("%w: media type %q", errUnsupportedCodec, fields[0])
	}
	if !strings.HasPrefix(strings.ToUpper(fields[2]), "RTP/AVP") {
		return nil, fmt.Errorf("%w: unsupported transport %q", errInvalidSDP, fields[2])
	}

	formats := make([]string, 0, len(fields)-3)
	for _, f := range fields[3:] {
		n, err := strconv.Atoi(f)
		if err != nil || n < 0 || n > 127 {
			return nil, fmt.Errorf("%w: bad payload type %q", errInvalidSDP, f)
		}
		formats = append(formats, strconv.Itoa(n))
	}
	if len(formats) == 0 {
		return nil, fmt.Errorf("%w: m= line has no payload type", errInvalidSDP)
	}
	return &sdpMedia{
		kind:    kind,
		formats: formats,
		rtpmap:  make(map[string]sdpRTPMap),
		fmtp:    make(map[string]string),
	}, nil
}

// addAttribute 解析媒体级的 a=rtpmap / a=fmtp / a=control。
func (m *sdpMedia) addAttribute(attr string) error {
	key, val, ok := strings.Cut(attr, ":")
	if !ok {
		return nil // a=recvonly 之类没有值，忽略
	}
	switch strings.ToLower(key) {
	case "rtpmap":
		pt, rm, err := parseRTPMap(val)
		if err != nil {
			return err
		}
		m.rtpmap[pt] = rm
	case "fmtp":
		pt, params, err := parseFMTP(val)
		if err != nil {
			return err
		}
		m.fmtp[pt] = params
	case "control":
		m.control = strings.TrimSpace(val)
	}
	return nil
}

// parseRTPMap 解析 "<pt> <encoding>/<clock>[/<channels>]"。
func parseRTPMap(val string) (string, sdpRTPMap, error) {
	fields := strings.Fields(val)
	if len(fields) != 2 {
		return "", sdpRTPMap{}, fmt.Errorf("%w: bad rtpmap %q", errInvalidSDP, val)
	}
	pt, err := strconv.Atoi(fields[0])
	if err != nil || pt < 0 || pt > 127 {
		return "", sdpRTPMap{}, fmt.Errorf("%w: bad rtpmap payload type %q", errInvalidSDP, fields[0])
	}
	parts := strings.Split(fields[1], "/")
	if len(parts) < 2 || len(parts) > 3 || parts[0] == "" {
		return "", sdpRTPMap{}, fmt.Errorf("%w: bad rtpmap encoding %q", errInvalidSDP, fields[1])
	}
	clock, err := strconv.Atoi(parts[1])
	if err != nil || clock <= 0 {
		return "", sdpRTPMap{}, fmt.Errorf("%w: bad rtpmap clock rate %q", errInvalidSDP, parts[1])
	}
	rm := sdpRTPMap{encoding: parts[0], clockRate: clock}
	if len(parts) == 3 {
		channels, err := strconv.Atoi(parts[2])
		if err != nil || channels <= 0 {
			return "", sdpRTPMap{}, fmt.Errorf("%w: bad rtpmap channels %q", errInvalidSDP, parts[2])
		}
		rm.channels = channels
	}
	return strconv.Itoa(pt), rm, nil
}

// parseFMTP 解析 "<pt> <format specific parameters>"。
func parseFMTP(val string) (string, string, error) {
	fields := strings.SplitN(strings.TrimSpace(val), " ", 2)
	if len(fields) != 2 || strings.TrimSpace(fields[1]) == "" {
		return "", "", fmt.Errorf("%w: bad fmtp %q", errInvalidSDP, val)
	}
	pt, err := strconv.Atoi(fields[0])
	if err != nil || pt < 0 || pt > 127 {
		return "", "", fmt.Errorf("%w: bad fmtp payload type %q", errInvalidSDP, fields[0])
	}
	return strconv.Itoa(pt), strings.TrimSpace(fields[1]), nil
}

// resolve 把收齐属性的 m= 段变成一条 announcedTrack。
func (m *sdpMedia) resolve(index int) (announcedTrack, error) {
	// m= 可以列出多个 payload type，取第一个带 rtpmap 的。
	var (
		pt string
		rm sdpRTPMap
	)
	for _, f := range m.formats {
		if r, ok := m.rtpmap[f]; ok {
			pt, rm = f, r
			break
		}
	}
	if pt == "" {
		return announcedTrack{}, fmt.Errorf("%w: %s section has no a=rtpmap", errInvalidSDP, m.kind)
	}
	if err := checkCodec(m.kind, rm.encoding); err != nil {
		return announcedTrack{}, err
	}

	control, trackID := normalizeControl(m.control)
	if control == "" {
		// SDP 没写 a=control 时按轨道顺序给一个隐式 ID，
		// 后面的 SETUP 用 trackID=<n> 也能命中。
		trackID = strconv.Itoa(index)
		control = "trackID=" + trackID
	}
	ptNum, _ := strconv.Atoi(pt)

	return announcedTrack{
		Kind:        m.kind,
		Control:     control,
		TrackID:     trackID,
		PayloadType: ptNum,
		Encoding:    rm.encoding,
		ClockRate:   rm.clockRate,
		Channels:    rm.channels,
		FMTP:        m.fmtp[pt],
	}, nil
}

// checkCodec 只接受本服务能处理的编码：视频 H.264，音频 RFC 3640 AAC。
func checkCodec(kind, encoding string) error {
	switch kind {
	case "video":
		if strings.EqualFold(encoding, "H264") {
			return nil
		}
	case "audio":
		if strings.EqualFold(encoding, "MPEG4-GENERIC") {
			return nil
		}
	}
	return fmt.Errorf("%w: %s/%s", errUnsupportedCodec, kind, encoding)
}

// normalizeControl 把 a=control 规范化成 "key=value" 形式，并取出 trackID。
// 支持 "streamid=0"、"trackID=1" 以及带路径的完整 URL。
// 缺失或 "*" 返回空串，由调用方生成默认值。
func normalizeControl(raw string) (control, trackID string) {
	c := strings.TrimSpace(raw)
	if c == "" || c == "*" {
		return "", ""
	}
	// rtsp://host/live/streamid=0 → streamid=0
	if i := strings.LastIndex(c, "/"); i >= 0 {
		c = c[i+1:]
	}
	if c == "" || c == "*" {
		return "", ""
	}
	if _, v, ok := strings.Cut(c, "="); ok {
		return c, v
	}
	// 没有 key=value 时，整个 control 就是 ID。
	return c, c
}

// controlLookupKey 生成不区分大小写的 control 映射键。
func controlLookupKey(raw string) string {
	c, _ := normalizeControl(raw)
	return strings.ToLower(c)
}

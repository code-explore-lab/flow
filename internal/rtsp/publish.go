package rtsp

// 已发布流的注册表（P1-8 / M0）。
//
// ANNOUNCE 到达时按 URL 路径登记一条 stream.Stream：保存推流端 SDP 里的
// 编码参数，并为每条轨创建一个 ExternalSource。推流端 RECORD 后把解包出的
// NALU/AU 写进这些源；拉流端 DESCRIBE/SETUP 按同一个 URL 找到它，
// 用 ANNOUNCE 的参数生成 SDP，PLAY 后订阅同一个源转发。
//
// 注册表本身由 internal/stream 提供，RTSP 与 RTMP 共用同一份：
// RTMP 推的流也能被 RTSP 拉流端按流名找到（反之亦然）。
//
// 生命周期：
//
//	ANNOUNCE  → publish(name)     创建/替换，旧流关闭
//	TEARDOWN  → Session.closeAll → unpublish(name) 删除并关闭源
//
// 多路推流靠 URL 路径区分；同一个路径重复 ANNOUNCE 视为重新发布。

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"flow/internal/stream"
)

// rtspStreamMeta RTSP 自己的流描述：ANNOUNCE SDP 里解析出的轨道列表。
// 放在 Stream.Meta 里，RTSP 拉流时用它还原精确的 SDP；RTMP 推的流没有它，
// 由编码参数合成。
type rtspStreamMeta struct {
	tracks []announcedTrack
}

// newPublishedStream 按 ANNOUNCE 轨道创建共享流：帧源 + 编码参数 + SDP 描述。
func newPublishedStream(name string, tracks []announcedTrack) *stream.Stream {
	p := &stream.Stream{
		Name: name,
		Meta: &rtspStreamMeta{tracks: append([]announcedTrack(nil), tracks...)},
	}
	for _, t := range tracks {
		switch t.Kind {
		case "video":
			p.EnsureVideo()
			if sps, pps, ok := parseSpropParameterSets(t.FMTP); ok {
				p.SetVideoParams(sps, pps)
			}
		case "audio":
			p.EnsureAudio(uint32(t.ClockRate))
			if asc, ok := parseConfigParam(t.FMTP); ok {
				p.SetAudioParams(asc, uint32(t.ClockRate), uint8(t.Channels))
			}
		}
	}
	return p
}

// parseSpropParameterSets 从 H.264 fmtp 里解出 sprop-parameter-sets
// （逗号分隔的两个 base64 NALU）。缺失或非法时返回 false。
func parseSpropParameterSets(fmtp string) (sps, pps []byte, ok bool) {
	value := fmtpParam(fmtp, "sprop-parameter-sets")
	if value == "" {
		return nil, nil, false
	}
	parts := strings.Split(value, ",")
	if len(parts) < 2 {
		return nil, nil, false
	}
	sps, err := base64.StdEncoding.DecodeString(strings.TrimSpace(parts[0]))
	if err != nil || len(sps) == 0 {
		return nil, nil, false
	}
	pps, err = base64.StdEncoding.DecodeString(strings.TrimSpace(parts[1]))
	if err != nil || len(pps) == 0 {
		return nil, nil, false
	}
	return sps, pps, true
}

// parseConfigParam 从 AAC fmtp 里解出 config=（AudioSpecificConfig）。
// ffmpeg 习惯用十六进制（如 config=1210），部分客户端用 base64（如 EhA=），
// 两种都接受。
func parseConfigParam(fmtp string) ([]byte, bool) {
	value := fmtpParam(fmtp, "config")
	if value == "" {
		return nil, false
	}
	if b, err := hex.DecodeString(value); err == nil && len(b) > 0 {
		return b, true
	}
	if b, err := base64.StdEncoding.DecodeString(value); err == nil && len(b) > 0 {
		return b, true
	}
	return nil, false
}

// fmtpParam 从 "a=fmtp" 参数串里取指定键的值（分号分隔，键名不区分大小写）。
func fmtpParam(fmtp, key string) string {
	for _, part := range strings.Split(fmtp, ";") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok && strings.EqualFold(strings.TrimSpace(k), key) {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// rtspTracks 返回这条流的 SDP 轨道描述：
//   - RTSP 推的流用 ANNOUNCE 原样参数；
//   - RTMP 推的流（Meta 为空）按编码参数合成固定的 PT 96/97。
func rtspTracks(p *stream.Stream) []announcedTrack {
	if meta, ok := p.Meta.(*rtspStreamMeta); ok && len(meta.tracks) > 0 {
		return meta.tracks
	}
	return synthesizeTracks(p)
}

// synthesizeTracks 为非 RTSP 推入的流生成 SDP 轨道描述。
func synthesizeTracks(p *stream.Stream) []announcedTrack {
	var out []announcedTrack
	if p.VideoTrack() != nil {
		at := announcedTrack{
			Kind:        "video",
			PayloadType: 96,
			Encoding:    "H264",
			ClockRate:   90000,
		}
		at.FMTP = "packetization-mode=1"
		if vc, ok := p.VideoParams(); ok && len(vc.PPS) > 0 {
			at.FMTP += "; sprop-parameter-sets=" +
				base64.StdEncoding.EncodeToString(vc.SPS) + "," +
				base64.StdEncoding.EncodeToString(vc.PPS)
		}
		out = append(out, at)
	}
	if p.AudioTrack() != nil {
		ac, ok := p.AudioParams()
		rate, ch := 44100, uint8(2)
		if ok {
			if ac.SampleRate > 0 {
				rate = int(ac.SampleRate)
			}
			if ac.Channels > 0 {
				ch = uint8(ac.Channels)
			}
		}
		at := announcedTrack{
			Kind:        "audio",
			PayloadType: 97,
			Encoding:    "MPEG4-GENERIC",
			ClockRate:   rate,
			Channels:    int(ch),
		}
		at.FMTP = "profile-level-id=1; mode=AAC-hbr; sizelength=13; indexlength=3; indexdeltalength=3"
		if ok {
			at.FMTP += "; config=" + hex.EncodeToString(ac.Config)
		}
		out = append(out, at)
	}
	for i := range out {
		out[i].TrackID = strconv.Itoa(i)
		out[i].Control = "trackID=" + out[i].TrackID
	}
	return out
}

// firstTrackOfKind 按 kind 找第一条轨（拉流 SETUP 用）。
func firstTrackOfKind(p *stream.Stream, kind string) (announcedTrack, bool) {
	for _, t := range rtspTracks(p) {
		if t.Kind == kind {
			return t, true
		}
	}
	return announcedTrack{}, false
}

// trackByControl 按 SETUP URI 里的 control 段找到对应轨道。
// 下发 SDP 时 control 统一写成 trackID=<下标>，同时兼容推流端原来的
// streamid=N / trackID=N / 裸 N，客户端三种写法都能命中。
func trackByControl(p *stream.Stream, uri string) (announcedTrack, bool) {
	key := controlLookupKey(controlFromURI(uri))
	if key == "" {
		return announcedTrack{}, false
	}
	for i, t := range rtspTracks(p) {
		if controlLookupKey("trackID="+strconv.Itoa(i)) == key {
			return t, true
		}
		if t.Control != "" && controlLookupKey(t.Control) == key {
			return t, true
		}
		if t.TrackID != "" && controlLookupKey(t.TrackID) == key {
			return t, true
		}
	}
	return announcedTrack{}, false
}

// publishedSDP 用流的编码参数生成拉流端 DESCRIBE 的 SDP。
// control 统一下发 trackID=<下标>，与现有 trackKindFromURI 的映射一致。
// mcAddr 非空时每条轨补上组播属性 a=destination / a=ttl。
func publishedSDP(p *stream.Stream, mcAddr string, mcTTL int) string {
	var b strings.Builder
	b.WriteString("v=0\r\n")
	b.WriteString("o=- 0 0 IN IP4 127.0.0.1\r\n")
	b.WriteString("s=flow stream\r\n")
	b.WriteString("c=IN IP4 127.0.0.1\r\n")
	b.WriteString("t=0 0\r\n")
	for i, t := range rtspTracks(p) {
		fmt.Fprintf(&b, "m=%s 0 RTP/AVP %d\r\n", t.Kind, t.PayloadType)

		rtpmap := fmt.Sprintf("%s/%d", t.Encoding, t.ClockRate)
		if t.Channels > 0 {
			rtpmap += fmt.Sprintf("/%d", t.Channels)
		}
		fmt.Fprintf(&b, "a=rtpmap:%d %s\r\n", t.PayloadType, rtpmap)
		if t.FMTP != "" {
			fmt.Fprintf(&b, "a=fmtp:%d %s\r\n", t.PayloadType, t.FMTP)
		}
		fmt.Fprintf(&b, "a=control:trackID=%d\r\n", i)
		b.WriteString(multicastSDPAttrs(mcAddr, mcTTL))
	}
	return b.String()
}

// publish 登记一路已发布流。同名流已存在时整体替换并关闭旧源。
func (s *Server) publish(name string, tracks []announcedTrack) *stream.Stream {
	pub := newPublishedStream(name, tracks)
	s.registry.Publish(name, pub)
	fmt.Printf("published stream %q: %d track(s), video=%v audio=%v\n",
		name, len(tracks), pub.VideoTrack() != nil, pub.AudioTrack() != nil)
	return pub
}

// unpublish 从注册表删除并关闭源。
func (s *Server) unpublish(name string, pub *stream.Stream) {
	s.registry.Unpublish(name, pub)
}

// publishedStreamByName 查找已发布流。
func (s *Server) publishedStreamByName(name string) *stream.Stream {
	return s.registry.Get(name)
}

// publishedFromURI 按 RTSP URL 的路径查找已发布流。
func (s *Server) publishedFromURI(uri string) *stream.Stream {
	return s.publishedStreamByName(streamKeyFromURI(uri))
}

// streamKeyFromURI 从 RTSP URL 提取流名（去掉首尾斜杠）。
// SETUP URI 末尾可能带 trackID=0 / streamid=0 这样的 control 段，
// 查找流时要把它剥掉（DESCRIBE/ANNOUNCE 的 URL 本身不带 control）。
func streamKeyFromURI(uri string) string {
	u, err := url.Parse(strings.TrimSpace(uri))
	if err != nil {
		return ""
	}
	p := strings.Trim(u.Path, "/")
	if i := strings.LastIndex(p, "/"); i >= 0 {
		if isControlSegment(p[i+1:]) {
			p = p[:i]
		}
	} else if isControlSegment(p) {
		p = ""
	}
	return p
}

// controlFromURI 返回 SETUP URI 的最后一个路径段（即 a=control 的值）。
func controlFromURI(uri string) string {
	u, err := url.Parse(strings.TrimSpace(uri))
	if err != nil {
		return ""
	}
	p := strings.Trim(u.Path, "/")
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[i+1:]
	}
	return p
}

// isControlSegment 判断路径段是不是媒体轨 control，而不是流名。
func isControlSegment(segment string) bool {
	lower := strings.ToLower(segment)
	return strings.HasPrefix(lower, "trackid=") || strings.HasPrefix(lower, "streamid=")
}

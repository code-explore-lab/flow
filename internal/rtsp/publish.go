package rtsp

// 已发布流的注册表（P1-8）。
//
// ANNOUNCE 到达时按 URL 路径登记一条 publishedStream：保存推流端 SDP 里的
// 编码参数，并为每条轨创建一个 ExternalSource。推流端 RECORD 后把解包出的
// NALU/AU 写进这些源；拉流端 DESCRIBE/SETUP 按同一个 URL 找到它，
// 用 ANNOUNCE 的参数生成 SDP，PLAY 后订阅同一个源转发。
//
// 生命周期：
//
//	ANNOUNCE  → publish(name)     创建/替换，旧流关闭
//	TEARDOWN  → Session.closeAll → unpublish(name) 删除并关闭源
//
// 多路推流靠 URL 路径区分；同一个路径重复 ANNOUNCE 视为重新发布。

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"flow/internal/source"
)

// publishedStream 一路正在推的流：ANNOUNCE 的轨道描述 + 解包后的帧源。
type publishedStream struct {
	name   string
	tracks []announcedTrack

	// 每类轨一个源。h264 与 AAC 的帧格式不同，不能共用一个源。
	// 多个同类型轨（极少见）只取第一条。
	video *source.ExternalSource
	audio *source.ExternalSource
}

// newPublishedStream 按 ANNOUNCE 轨道创建帧源。
func newPublishedStream(name string, tracks []announcedTrack) *publishedStream {
	p := &publishedStream{
		name:   name,
		tracks: append([]announcedTrack(nil), tracks...),
	}
	for _, t := range tracks {
		switch t.Kind {
		case "video":
			if p.video == nil {
				p.video = source.NewExternalSource()
			}
		case "audio":
			if p.audio == nil {
				p.audio = source.NewExternalAudioSource()
				p.audio.SetSamplingRate(uint32(t.ClockRate))
			}
		}
	}
	return p
}

// sourceFor 返回指定轨的帧源，没有该轨时返回 nil。
func (p *publishedStream) sourceFor(kind string) *source.ExternalSource {
	switch kind {
	case "video":
		return p.video
	case "audio":
		return p.audio
	default:
		return nil
	}
}

// firstTrackOfKind 按 kind 找 ANNOUNCE 里的第一条轨（拉流 SETUP 用）。
func (p *publishedStream) firstTrackOfKind(kind string) (announcedTrack, bool) {
	for _, t := range p.tracks {
		if t.Kind == kind {
			return t, true
		}
	}
	return announcedTrack{}, false
}

// trackByControl 按 SETUP URI 里的 control 段找到对应轨道。
// 下发 SDP 时 control 统一写成 trackID=<下标>，同时兼容推流端原来的
// streamid=N / trackID=N / 裸 N，客户端三种写法都能命中。
func (p *publishedStream) trackByControl(uri string) (announcedTrack, bool) {
	key := controlLookupKey(controlFromURI(uri))
	if key == "" {
		return announcedTrack{}, false
	}
	for i, t := range p.tracks {
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

// sdp 用 ANNOUNCE 带来的编码参数生成拉流端 DESCRIBE 的 SDP。
// control 统一下发 trackID=<下标>，与现有 trackKindFromURI 的映射一致。
func (p *publishedStream) sdp() string {
	var b strings.Builder
	b.WriteString("v=0\r\n")
	b.WriteString("o=- 0 0 IN IP4 127.0.0.1\r\n")
	b.WriteString("s=flow stream\r\n")
	b.WriteString("c=IN IP4 127.0.0.1\r\n")
	b.WriteString("t=0 0\r\n")
	for i, t := range p.tracks {
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
	}
	return b.String()
}

// Close 关闭所有帧源，唤醒拉流端 reader。
func (p *publishedStream) Close() {
	if p.video != nil {
		p.video.Close()
	}
	if p.audio != nil {
		p.audio.Close()
	}
}

// publish 登记一路已发布流。同名流已存在时整体替换并关闭旧源。
func (s *Server) publish(name string, tracks []announcedTrack) *publishedStream {
	pub := newPublishedStream(name, tracks)

	s.pubMu.Lock()
	old := s.published[name]
	s.published[name] = pub
	s.pubMu.Unlock()

	if old != nil && old != pub {
		fmt.Printf("published stream %q replaced\n", name)
		old.Close()
	}
	fmt.Printf("published stream %q: %d track(s), video=%v audio=%v\n",
		name, len(tracks), pub.video != nil, pub.audio != nil)
	return pub
}

// unpublish 从注册表删除。只有当前登记的仍是 pub 时才删，避免旧会话
// 晚到的清理把重新发布的新流摘掉。
func (s *Server) unpublish(name string, pub *publishedStream) {
	s.pubMu.Lock()
	if s.published[name] == pub {
		delete(s.published, name)
		fmt.Printf("published stream %q removed\n", name)
	}
	s.pubMu.Unlock()
	pub.Close()
}

// publishedStreamByName 查找已发布流。
func (s *Server) publishedStreamByName(name string) *publishedStream {
	s.pubMu.RLock()
	defer s.pubMu.RUnlock()
	return s.published[name]
}

// publishedFromURI 按 RTSP URL 的路径查找已发布流。
func (s *Server) publishedFromURI(uri string) *publishedStream {
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

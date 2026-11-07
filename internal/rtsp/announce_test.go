package rtsp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
)

// ffmpegAnnounceSDP 是 ffmpeg 推流时 ANNOUNCE 携带的典型 SDP。
const ffmpegAnnounceSDP = "v=0\r\n" +
	"o=- 0 0 IN IP4 127.0.0.1\r\n" +
	"s=No Name\r\n" +
	"c=IN IP4 127.0.0.1\r\n" +
	"t=0 0\r\n" +
	"a=tool:libavformat 60.16.100\r\n" +
	"m=video 0 RTP/AVP 96\r\n" +
	"b=AS:2500\r\n" +
	"a=rtpmap:96 H264/90000\r\n" +
	"a=fmtp:96 packetization-mode=1; sprop-parameter-sets=Z0IAH5WoFAFuQA==,aM48gA==\r\n" +
	"a=control:streamid=0\r\n" +
	"m=audio 0 RTP/AVP 97\r\n" +
	"b=AS:128\r\n" +
	"a=rtpmap:97 MPEG4-GENERIC/44100/2\r\n" +
	"a=fmtp:97 profile-level-id=1; mode=AAC-hbr; sizelength=13; indexlength=3; indexdeltalength=3; config=1210\r\n" +
	"a=control:streamid=1\r\n"

func minVideoSDP(codec string) string {
	return "v=0\r\n" +
		"m=video 0 RTP/AVP 96\r\n" +
		"a=rtpmap:96 " + codec + "/90000\r\n" +
		"a=control:streamid=0\r\n"
}

func TestParseAnnounceSDPFFmpeg(t *testing.T) {
	tracks, err := parseAnnounceSDP([]byte(ffmpegAnnounceSDP))
	if err != nil {
		t.Fatalf("parseAnnounceSDP: %v", err)
	}
	if len(tracks) != 2 {
		t.Fatalf("got %d track(s), want 2", len(tracks))
	}

	v := tracks[0]
	if v.Kind != "video" || v.Encoding != "H264" || v.PayloadType != 96 || v.ClockRate != 90000 {
		t.Fatalf("video track = %+v", v)
	}
	if v.Control != "streamid=0" || v.TrackID != "0" || v.Channels != 0 {
		t.Fatalf("video control/trackID/channels = %q/%q/%d", v.Control, v.TrackID, v.Channels)
	}
	if !strings.Contains(v.FMTP, "sprop-parameter-sets=") || !strings.Contains(v.FMTP, "packetization-mode=1") {
		t.Fatalf("video fmtp = %q", v.FMTP)
	}

	a := tracks[1]
	if a.Kind != "audio" || a.Encoding != "MPEG4-GENERIC" || a.PayloadType != 97 || a.ClockRate != 44100 || a.Channels != 2 {
		t.Fatalf("audio track = %+v", a)
	}
	if a.Control != "streamid=1" || a.TrackID != "1" {
		t.Fatalf("audio control/trackID = %q/%q", a.Control, a.TrackID)
	}
	if !strings.Contains(a.FMTP, "mode=AAC-hbr") || !strings.Contains(a.FMTP, "config=1210") {
		t.Fatalf("audio fmtp = %q", a.FMTP)
	}
}

func TestParseAnnounceSDPControlForms(t *testing.T) {
	tests := []struct {
		name        string
		controlLine string
		wantControl string
		wantTrackID string
	}{
		{name: "streamid", controlLine: "a=control:streamid=7\r\n", wantControl: "streamid=7", wantTrackID: "7"},
		{name: "trackID", controlLine: "a=control:trackID=3\r\n", wantControl: "trackID=3", wantTrackID: "3"},
		{
			name:        "完整 URL",
			controlLine: "a=control:rtsp://127.0.0.1:8554/live/trackID=4\r\n",
			wantControl: "trackID=4", wantTrackID: "4",
		},
		{name: "只给 ID", controlLine: "a=control:5\r\n", wantControl: "5", wantTrackID: "5"},
		{name: "缺失", controlLine: "", wantControl: "trackID=0", wantTrackID: "0"},
		{name: "星号", controlLine: "a=control:*\r\n", wantControl: "trackID=0", wantTrackID: "0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := "v=0\r\nm=video 0 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\n" + tt.controlLine
			tracks, err := parseAnnounceSDP([]byte(body))
			if err != nil {
				t.Fatalf("parseAnnounceSDP: %v", err)
			}
			if len(tracks) != 1 {
				t.Fatalf("got %d track(s), want 1", len(tracks))
			}
			if tracks[0].Control != tt.wantControl || tracks[0].TrackID != tt.wantTrackID {
				t.Fatalf("control/trackID = %q/%q, want %q/%q",
					tracks[0].Control, tracks[0].TrackID, tt.wantControl, tt.wantTrackID)
			}
		})
	}
}

func TestParseAnnounceSDPErrors(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr error
	}{
		{name: "空 body", body: "", wantErr: errInvalidSDP},
		{name: "缺少 v=", body: "m=video 0 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\n", wantErr: errInvalidSDP},
		{name: "没有 m=", body: "v=0\r\nt=0 0\r\n", wantErr: errInvalidSDP},
		{name: "非法行", body: "v=0\r\nthis is not sdp\r\n", wantErr: errInvalidSDP},
		{name: "m= 字段不足", body: "v=0\r\nm=video 0 RTP/AVP\r\n", wantErr: errInvalidSDP},
		{name: "payload type 非法", body: "v=0\r\nm=video 0 RTP/AVP abc\r\n", wantErr: errInvalidSDP},
		{name: "缺少 rtpmap", body: "v=0\r\nm=video 0 RTP/AVP 96\r\n", wantErr: errInvalidSDP},
		{name: "rtpmap 缺时钟", body: "v=0\r\nm=video 0 RTP/AVP 96\r\na=rtpmap:96 H264\r\n", wantErr: errInvalidSDP},
		{name: "fmtp 缺参数", body: "v=0\r\nm=video 0 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\na=fmtp:96\r\n", wantErr: errInvalidSDP},
		{name: "传输协议不支持", body: "v=0\r\nm=video 0 UDP 96\r\na=rtpmap:96 H264/90000\r\n", wantErr: errInvalidSDP},
		{
			name:    "媒体类型不支持",
			body:    "v=0\r\nm=text 0 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\n",
			wantErr: errUnsupportedCodec,
		},
		{name: "视频编码不支持", body: minVideoSDP("H265"), wantErr: errUnsupportedCodec},
		{
			name:    "音频编码不支持",
			body:    "v=0\r\nm=audio 0 RTP/AVP 98\r\na=rtpmap:98 OPUS/48000/2\r\n",
			wantErr: errUnsupportedCodec,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseAnnounceSDP([]byte(tt.body)); !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestSessionAnnouncedTrackMapping(t *testing.T) {
	tracks, err := parseAnnounceSDP([]byte(ffmpegAnnounceSDP))
	if err != nil {
		t.Fatal(err)
	}
	rs := newSession()
	rs.setAnnounced(tracks)

	tests := []struct {
		control  string
		wantKind string
		wantOK   bool
	}{
		{"streamid=0", "video", true},
		{"trackID=0", "video", true},
		{"trackid=0", "video", true},
		{"0", "video", true},
		{"rtsp://host/live/streamid=1", "audio", true},
		{"streamid=1", "audio", true},
		{"trackID=1", "audio", true},
		{"streamid=9", "", false},
	}
	for _, tt := range tests {
		got, ok := rs.announcedTrack(tt.control)
		if ok != tt.wantOK {
			t.Fatalf("announcedTrack(%q) ok = %v, want %v", tt.control, ok, tt.wantOK)
		}
		if ok && got.Kind != tt.wantKind {
			t.Fatalf("announcedTrack(%q).Kind = %q, want %q", tt.control, got.Kind, tt.wantKind)
		}
	}

	snapshot := rs.announcedTracks()
	if len(snapshot) != 2 {
		t.Fatalf("announcedTracks = %d, want 2", len(snapshot))
	}
	// 返回的是副本，修改快照不应影响会话内部状态。
	snapshot[0].Kind = "changed"
	if got, _ := rs.announcedTrack("streamid=0"); got.Kind != "video" {
		t.Fatalf("internal announced tracks were mutated: %+v", got)
	}
}

func TestReadRequestWithBody(t *testing.T) {
	sdp := ffmpegAnnounceSDP
	raw := fmt.Sprintf("ANNOUNCE rtsp://127.0.0.1:8554/live RTSP/1.0\r\n"+
		"CSeq: 2\r\nContent-Type: application/sdp\r\nContent-Length: %d\r\n\r\n%s",
		len(sdp), sdp)

	br := bufio.NewReader(strings.NewReader(raw + "NEXT"))
	req, err := readRequest(br)
	if err != nil {
		t.Fatalf("readRequest: %v", err)
	}
	if req.Method != "ANNOUNCE" || req.CSeq != 2 {
		t.Fatalf("request = %+v", req)
	}
	if string(req.Body) != sdp {
		t.Fatalf("body length = %d, want %d", len(req.Body), len(sdp))
	}
	// 请求体之后的数据不能被吞掉（可能是下一条请求或 $ 帧）。
	rest, err := io.ReadAll(br)
	if err != nil {
		t.Fatalf("read rest: %v", err)
	}
	if string(rest) != "NEXT" {
		t.Fatalf("rest = %q, want NEXT", rest)
	}
}

func TestReadRequestZeroAndInvalidBody(t *testing.T) {
	t.Run("Content-Length 0", func(t *testing.T) {
		req, err := readRequest(bufio.NewReader(strings.NewReader(
			"OPTIONS rtsp://127.0.0.1/live RTSP/1.0\r\nCSeq: 1\r\nContent-Length: 0\r\n\r\n")))
		if err != nil {
			t.Fatalf("readRequest: %v", err)
		}
		if len(req.Body) != 0 {
			t.Fatalf("body = %q, want empty", req.Body)
		}
	})

	t.Run("Content-Length 非数字", func(t *testing.T) {
		_, err := readRequest(bufio.NewReader(strings.NewReader(
			"ANNOUNCE rtsp://x/live RTSP/1.0\r\nCSeq: 1\r\nContent-Length: abc\r\n\r\n")))
		if err == nil {
			t.Fatal("expected error for invalid Content-Length")
		}
	})

	t.Run("body 不完整", func(t *testing.T) {
		_, err := readRequest(bufio.NewReader(strings.NewReader(
			"ANNOUNCE rtsp://x/live RTSP/1.0\r\nCSeq: 1\r\nContent-Length: 10\r\n\r\nshort")))
		if err == nil {
			t.Fatal("expected error for truncated body")
		}
	})
}

func TestHandleAnnounceStatuses(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		headers map[string]string
		status  string
	}{
		{name: "合法 SDP", body: ffmpegAnnounceSDP, status: "200 OK"},
		{name: "非法 SDP", body: "not an sdp", status: "400 Bad Request"},
		{name: "不支持的编码", body: minVideoSDP("H265"), status: "415 Unsupported Media Type"},
		{
			name:    "未知会话",
			body:    ffmpegAnnounceSDP,
			headers: map[string]string{"Session": "sess-999"},
			status:  "454 Session Not Found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New(nil, nil, false)
			serverConn, clientConn := net.Pipe()
			defer serverConn.Close()
			conn := &Conn{Conn: serverConn}

			respCh := make(chan string, 1)
			go func() {
				var buf bytes.Buffer
				tmp := make([]byte, 4096)
				for {
					n, err := clientConn.Read(tmp)
					if n > 0 {
						buf.Write(tmp[:n])
					}
					if err != nil {
						respCh <- buf.String()
						return
					}
				}
			}()

			headers := map[string]string{"CSeq": "2"}
			for k, v := range tt.headers {
				headers[k] = v
			}
			req := &Request{
				Method:  "ANNOUNCE",
				URI:     "rtsp://127.0.0.1:8554/live",
				Version: "RTSP/1.0",
				CSeq:    2,
				Headers: headers,
				Body:    []byte(tt.body),
			}
			hd := map[string]string{"CSeq": "2"}

			if err := s.handleAnnounce(conn, req, hd); err != nil {
				t.Fatalf("handleAnnounce: %v", err)
			}
			clientConn.Close()

			resp := <-respCh
			if !strings.HasPrefix(resp, "RTSP/1.0 "+tt.status+"\r\n") {
				t.Fatalf("response status = %q, want %q", firstLine(resp), tt.status)
			}

			if tt.status == "200 OK" {
				sessionHeader := hd["Session"]
				if sessionHeader == "" {
					t.Fatal("200 response missing Session header")
				}
				sessID := strings.Split(sessionHeader, ";")[0]
				sess, ok := s.sessions.Get(sessID)
				if !ok {
					t.Fatalf("session %s not registered", sessID)
				}
				tracks := sess.Data.(*Session).announcedTracks()
				if len(tracks) != 2 || tracks[0].Kind != "video" || tracks[1].Kind != "audio" {
					t.Fatalf("announced tracks = %+v", tracks)
				}
			}
		})
	}
}

func firstLine(s string) string {
	if i := strings.Index(s, "\r\n"); i >= 0 {
		return s[:i]
	}
	return s
}

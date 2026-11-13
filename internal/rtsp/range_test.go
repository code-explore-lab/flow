package rtsp

import (
	"strings"
	"testing"
	"time"

	"flow/internal/session"
)

func TestParseNPTRange(t *testing.T) {
	tests := []struct {
		header  string
		start   time.Duration
		end     time.Duration
		hasEnd  bool
		now     bool
		wantErr bool
	}{
		{header: "npt=0-"},
		{header: "npt=0.000-"},
		{header: "NPT= 3-", start: 3 * time.Second},
		{header: "npt=3.5-10", start: 3500 * time.Millisecond, end: 10 * time.Second, hasEnd: true},
		{header: "npt=00:01:00-00:02:00", start: time.Minute, end: 2 * time.Minute, hasEnd: true},
		{header: "npt=01:30-", start: 90 * time.Second},
		{header: "npt=now-", now: true},
		{header: "npt=-10", end: 10 * time.Second, hasEnd: true},
		{header: "clock=19961108T143720Z-", wantErr: true},
		{header: "npt=", wantErr: true},
		{header: "npt=abc-", wantErr: true},
		{header: "npt=5-3", wantErr: true},
		{header: "npt=10-10", wantErr: true},
		{header: "npt=00:99:00-", wantErr: true},
		{header: "npt=1:2:3:4-", wantErr: true},
	}

	for _, tt := range tests {
		r, err := parseNPTRange(tt.header)
		if tt.wantErr {
			if err == nil {
				t.Fatalf("parseNPTRange(%q) = %+v, want error", tt.header, r)
			}
			continue
		}
		if err != nil {
			t.Fatalf("parseNPTRange(%q): %v", tt.header, err)
		}
		if r.start != tt.start || r.end != tt.end || r.hasEnd != tt.hasEnd || r.now != tt.now {
			t.Fatalf("parseNPTRange(%q) = %+v, want start=%s end=%s hasEnd=%v now=%v",
				tt.header, r, tt.start, tt.end, tt.hasEnd, tt.now)
		}
	}
}

// newPlaySession 建一个 net.Pipe 上的服务器和已登记会话。
func newPlaySession(t *testing.T, live bool) (s *Server, conn *Conn, sess *session.Session, finish func() string) {
	t.Helper()
	s, conn, finish = newTestPipe(t)
	s.live = live
	sess = s.newClientSession(conn)
	return s, conn, sess, finish
}

func playRequest(sessID, rangeHeader string) *Request {
	headers := map[string]string{"Session": sessID}
	if rangeHeader != "" {
		headers["Range"] = rangeHeader
	}
	return &Request{
		Method:  "PLAY",
		URI:     "rtsp://127.0.0.1:8554/live",
		Version: "RTSP/1.0",
		CSeq:    3,
		Headers: headers,
	}
}

// TestPlayRangeLive live 模式只接受 npt=0-，其他 Range 返回 457 且不改状态。
func TestPlayRangeLive(t *testing.T) {
	cases := []struct {
		rangeHeader string
		wantStatus  string
	}{
		{"", "RTSP/1.0 200 OK\r\n"},
		{"npt=0-", "RTSP/1.0 200 OK\r\n"},
		{"npt=0.000-", "RTSP/1.0 200 OK\r\n"},
		{"npt=5-", "RTSP/1.0 457 Invalid Range\r\n"},
		{"npt=0-10", "RTSP/1.0 457 Invalid Range\r\n"},
		{"npt=now-", "RTSP/1.0 457 Invalid Range\r\n"},
		{"npt=abc-", "RTSP/1.0 457 Invalid Range\r\n"},
		{"clock=19961108T143720Z-", "RTSP/1.0 457 Invalid Range\r\n"},
	}
	for _, tc := range cases {
		t.Run(tc.rangeHeader, func(t *testing.T) {
			s, conn, sess, finish := newPlaySession(t, true)
			hd := map[string]string{"CSeq": "3"}
			resp := callAndRead(t, func() error {
				return s.handlePlay(conn, playRequest(sess.ID, tc.rangeHeader), hd)
			}, finish)

			if !strings.HasPrefix(resp, tc.wantStatus) {
				t.Fatalf("status = %q, want %q", firstLine(resp), strings.TrimSpace(tc.wantStatus))
			}
			if strings.Contains(tc.wantStatus, "457") {
				if sess.State() != session.StateInit {
					t.Fatalf("session state = %v after 457, want Init", sess.State())
				}
				if _, _, hasEnd := sess.Data.(*Session).playRange(); hasEnd {
					t.Fatal("rejected Range must not be recorded")
				}
				return
			}
			if tc.rangeHeader != "" {
				if got := responseHeader(resp, "Range"); got != tc.rangeHeader {
					t.Fatalf("Range response header = %q, want %q", got, tc.rangeHeader)
				}
				if start, _, hasEnd := sess.Data.(*Session).playRange(); start != 0 || hasEnd {
					t.Fatalf("recorded range = start %s hasEnd %v, want 0/false", start, hasEnd)
				}
			}
		})
	}
}

// TestPlayRangeFileRecordsStart 文件模式记录起始位置（供 G-2 seek），并回显 Range。
func TestPlayRangeFileRecordsStart(t *testing.T) {
	s, conn, sess, finish := newPlaySession(t, false)
	hd := map[string]string{"CSeq": "3"}
	raw := "npt=3.5-10"
	resp := callAndRead(t, func() error {
		return s.handlePlay(conn, playRequest(sess.ID, raw), hd)
	}, finish)

	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("status = %q", firstLine(resp))
	}
	if got := responseHeader(resp, "Range"); got != raw {
		t.Fatalf("Range response header = %q, want %q", got, raw)
	}
	start, end, hasEnd := sess.Data.(*Session).playRange()
	if start != 3500*time.Millisecond || end != 10*time.Second || !hasEnd {
		t.Fatalf("recorded range = %s-%s hasEnd=%v, want 3.5s-10s true", start, end, hasEnd)
	}
}

// TestPlayRangeFileRejectsInvalid 文件模式下语法错误的 Range 同样回 457。
func TestPlayRangeFileRejectsInvalid(t *testing.T) {
	s, conn, sess, finish := newPlaySession(t, false)
	hd := map[string]string{"CSeq": "3"}
	resp := callAndRead(t, func() error {
		return s.handlePlay(conn, playRequest(sess.ID, "npt=10-3"), hd)
	}, finish)

	if !strings.HasPrefix(resp, "RTSP/1.0 457 Invalid Range\r\n") {
		t.Fatalf("status = %q", firstLine(resp))
	}
	if sess.State() != session.StateInit {
		t.Fatalf("session state = %v after 457, want Init", sess.State())
	}
}

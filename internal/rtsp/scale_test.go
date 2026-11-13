package rtsp

import (
	"strings"
	"testing"

	"flow/internal/session"
)

func TestParseScale(t *testing.T) {
	valid := map[string]float64{
		"2.0":   2,
		"0.5":   0.5,
		"1":     1,
		" 1.5 ": 1.5,
	}
	for header, want := range valid {
		got, err := parseScale(header)
		if err != nil {
			t.Fatalf("parseScale(%q): %v", header, err)
		}
		if got != want {
			t.Fatalf("parseScale(%q) = %v, want %v", header, got, want)
		}
	}

	for _, header := range []string{"", "abc", "0", "-1", "NaN", "Inf"} {
		if _, err := parseScale(header); err == nil {
			t.Fatalf("parseScale(%q) should fail", header)
		}
	}
}

// TestPlayScale live 模式非 1 倍速回 551 且不改状态；文件模式记录并回显。
func TestPlayScale(t *testing.T) {
	cases := []struct {
		name       string
		live       bool
		scale      string
		wantStatus string
		wantScale  float64
	}{
		{"live 2x", true, "2.0", "RTSP/1.0 551 Option Not Supported\r\n", 1},
		{"live 1x", true, "1.0", "RTSP/1.0 200 OK\r\n", 1},
		{"file 2x", false, "2.0", "RTSP/1.0 200 OK\r\n", 2},
		{"file 0.5x", false, "0.5", "RTSP/1.0 200 OK\r\n", 0.5},
		{"file invalid", false, "abc", "RTSP/1.0 400 Bad Request\r\n", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, conn, sess, finish := newPlaySession(t, tc.live)
			req := playRequest(sess.ID, "")
			req.Headers["Scale"] = tc.scale
			hd := map[string]string{"CSeq": "3"}
			resp := callAndRead(t, func() error {
				return s.handlePlay(conn, req, hd)
			}, finish)

			if !strings.HasPrefix(resp, tc.wantStatus) {
				t.Fatalf("status = %q, want %q", firstLine(resp), strings.TrimSpace(tc.wantStatus))
			}
			rs := sess.Data.(*Session)
			if tc.wantStatus != "RTSP/1.0 200 OK\r\n" {
				if sess.State() != session.StateInit {
					t.Fatalf("state = %v after rejection, want Init", sess.State())
				}
				if rs.playbackScale() != 1 {
					t.Fatalf("scale = %v after rejection, want 1", rs.playbackScale())
				}
				return
			}
			if got := responseHeader(resp, "Scale"); got != tc.scale {
				t.Fatalf("Scale response header = %q, want %q", got, tc.scale)
			}
			if rs.playbackScale() != tc.wantScale {
				t.Fatalf("recorded scale = %v, want %v", rs.playbackScale(), tc.wantScale)
			}
		})
	}
}

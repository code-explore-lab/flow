package rtsp

import (
	"strings"
	"testing"
	"time"

	"flow/internal/rtcp"
)

// TestSenderReportIsCompoundWithCNAME 验证每 5 秒发出的 RTCP 是
// SR + SDES(CNAME) 复合包，且 CNAME 与 SR 里的 SSRC 对应。
func TestSenderReportIsCompoundWithCNAME(t *testing.T) {
	_, _, track, _ := newRTXTestTrack(t)
	track.recordSent(9000, 120)

	pkt, ok := track.senderReport(time.Now())
	if !ok {
		t.Fatal("senderReport returned false after RTP was sent")
	}

	var pts []uint8
	var sr rtcp.SenderReport
	var cname string
	var cnameSSRC uint32
	rtcp.Walk(pkt, func(pt uint8, sub []byte) bool {
		pts = append(pts, pt)
		switch pt {
		case rtcp.SenderReportType:
			got, ok := rtcp.ParseSenderReport(sub)
			if !ok {
				t.Fatal("first sub-packet is not a valid SR")
			}
			sr = got
		case rtcp.SDESType:
			gotSSRC, gotName, ok := rtcp.ParseCNAME(sub)
			if !ok {
				t.Fatal("SDES sub-packet has no parseable CNAME")
			}
			cnameSSRC, cname = gotSSRC, gotName
		}
		return true
	})

	if len(pts) != 2 || pts[0] != rtcp.SenderReportType || pts[1] != rtcp.SDESType {
		t.Fatalf("sub-packet types = %v, want [SR SDES]", pts)
	}
	if cname != localCNAME {
		t.Fatalf("CNAME = %q, want %q", cname, localCNAME)
	}
	if cnameSSRC != sr.SSRC || cnameSSRC != track.currentSSRC() {
		t.Fatalf("CNAME SSRC = %#x, SR SSRC = %#x, track SSRC = %#x",
			cnameSSRC, sr.SSRC, track.currentSSRC())
	}
	if !strings.HasPrefix(cname, "flow@") {
		t.Fatalf("CNAME = %q, want flow@<host>", cname)
	}
}

// TestSenderReportCNAMEStableAcrossTracks 验证 video/audio 两条轨的 SSRC
// 不同、但 CNAME 相同：CNAME 标识源，播放器靠它把两路归到同一个源。
func TestSenderReportCNAMEStableAcrossTracks(t *testing.T) {
	_, _, video, _ := newRTXTestTrack(t)
	_, _, audio, _ := newRTXTestTrack(t)
	video.kind, audio.kind = "video", "audio"
	video.stats.ssrc, audio.stats.ssrc = 0x11111111, 0x22222222
	video.recordSent(9000, 100)
	audio.recordSent(1024, 100)

	cnameOf := func(track *mediaTrack) (uint32, string) {
		t.Helper()
		pkt, ok := track.senderReport(time.Now())
		if !ok {
			t.Fatal("senderReport returned false")
		}
		var ssrc uint32
		var name string
		rtcp.Walk(pkt, func(pt uint8, sub []byte) bool {
			if pt == rtcp.SDESType {
				ssrc, name, _ = rtcp.ParseCNAME(sub)
			}
			return true
		})
		return ssrc, name
	}

	videoSSRC, videoName := cnameOf(video)
	audioSSRC, audioName := cnameOf(audio)
	if videoSSRC == audioSSRC {
		t.Fatal("video and audio tracks must have distinct SSRCs")
	}
	if videoName != audioName {
		t.Fatalf("CNAME differs across tracks: %q vs %q", videoName, audioName)
	}
}

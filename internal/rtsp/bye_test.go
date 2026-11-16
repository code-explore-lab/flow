package rtsp

import (
	"testing"
	"time"

	"flow/internal/rtcp"
)

// TestSendByeAfterRTP 验证回收时发的是 SR + SDES + BYE 复合包：
// RFC 3550 §6.1 要求复合包以 SR/RR 开头，BYE 只能接在后面。
func TestSendByeAfterRTP(t *testing.T) {
	_, _, track, client := newRTXTestTrack(t)
	track.rtcpMux = true // 复用 RTP socket，简化测试传输配置
	track.recordSent(9000, 100)

	track.sendBye()

	pkt, ok := recvUDP(t, client, time.Second)
	if !ok {
		t.Fatal("no BYE compound received")
	}

	var pts []uint8
	var sr rtcp.SenderReport
	var cname string
	var byeSSRCs []uint32
	rtcp.Walk(pkt, func(pt uint8, sub []byte) bool {
		pts = append(pts, pt)
		switch pt {
		case rtcp.SenderReportType:
			sr, _ = rtcp.ParseSenderReport(sub)
		case rtcp.SDESType:
			_, cname, _ = rtcp.ParseCNAME(sub)
		case rtcp.ByeType:
			byeSSRCs, _ = rtcp.ParseBYE(sub)
		}
		return true
	})

	if len(pts) != 3 || pts[0] != rtcp.SenderReportType ||
		pts[1] != rtcp.SDESType || pts[2] != rtcp.ByeType {
		t.Fatalf("sub-packet types = %v, want [SR SDES BYE]", pts)
	}
	if sr.SSRC != track.currentSSRC() {
		t.Fatalf("SR SSRC = %#x, want %#x", sr.SSRC, track.currentSSRC())
	}
	if cname != localCNAME {
		t.Fatalf("CNAME = %q, want %q", cname, localCNAME)
	}
	if len(byeSSRCs) != 1 || byeSSRCs[0] != track.currentSSRC() {
		t.Fatalf("BYE SSRCs = %v, want [%#x]", byeSSRCs, track.currentSSRC())
	}
}

// TestSendByeSkippedWithoutRTP 覆盖 RFC 3550 §6.3.7：
// 从未发过 RTP/RTCP 的参与者不得发 BYE。
func TestSendByeSkippedWithoutRTP(t *testing.T) {
	_, _, track, client := newRTXTestTrack(t)
	track.rtcpMux = true

	track.sendBye()

	if _, ok := recvUDP(t, client, 150*time.Millisecond); ok {
		t.Fatal("BYE sent even though no RTP was ever sent")
	}
}

// TestCloseAllSendsBye 验证会话回收路径（TEARDOWN/超时/源结束共用）
// 在关闭 socket 之前发出 BYE。
func TestCloseAllSendsBye(t *testing.T) {
	_, _, track, client := newRTXTestTrack(t)
	track.rtcpMux = true
	track.recordSent(9000, 100)

	st := newSession()
	st.setTrack("video", track)
	st.closeAll()

	pkt, ok := recvUDP(t, client, time.Second)
	if !ok {
		t.Fatal("closeAll did not send BYE")
	}
	found := false
	rtcp.Walk(pkt, func(pt uint8, sub []byte) bool {
		if pt == rtcp.ByeType {
			found = true
		}
		return true
	})
	if !found {
		t.Fatal("closeAll packet has no BYE sub-packet")
	}
}

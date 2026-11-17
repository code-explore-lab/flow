package rtsp

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"net"
	"strings"
	"testing"
	"time"

	"flow/internal/rtp"
	"flow/internal/source"
	"flow/internal/stream"
)

// HEVC 参数集（裸 NALU）。
var (
	hevcVPS = []byte{0x40, 0x01, 0x0C, 0x01, 0xFF, 0xFF}
	hevcSPS = []byte{
		0x42, 0x01, 0x01, 0x01, 0x60, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x5D,
	}
	hevcPPS = []byte{0x44, 0x01, 0xC0, 0x73}
	hevcIDR = []byte{0x26, 0x01, 0xAF, 0x12}
)

// hevcAnnounceSDP 构造客户端推 HEVC 时的 ANNOUNCE SDP。
func hevcAnnounceSDP() string {
	return "v=0\r\n" +
		"o=- 0 0 IN IP4 127.0.0.1\r\n" +
		"s=No Name\r\n" +
		"c=IN IP4 127.0.0.1\r\n" +
		"t=0 0\r\n" +
		"m=video 0 RTP/AVP 96\r\n" +
		"a=rtpmap:96 H265/90000\r\n" +
		"a=fmtp:96 sprop-vps=" + base64.StdEncoding.EncodeToString(hevcVPS) +
		";sprop-sps=" + base64.StdEncoding.EncodeToString(hevcSPS) +
		";sprop-pps=" + base64.StdEncoding.EncodeToString(hevcPPS) + "\r\n" +
		"a=control:streamid=0\r\n"
}

// TestHEVCAnnouncePublishAndDescribe：HEVC ANNOUNCE 后编码参数进注册表，
// DESCRIBE 的 SDP 仍按 H265/90000 + sprop-vps/sps/pps 下发。
func TestHEVCAnnouncePublishAndDescribe(t *testing.T) {
	s, conn, finish := newTestPipe(t)

	req := &Request{
		Method:  "ANNOUNCE",
		URI:     "rtsp://127.0.0.1:8554/hevc",
		Version: "RTSP/1.0",
		CSeq:    2,
		Headers: map[string]string{"Content-Type": "application/sdp"},
		Body:    []byte(hevcAnnounceSDP()),
	}
	hd := map[string]string{"CSeq": "2"}
	callAndRead(t, func() error { return s.handleAnnounce(conn, req, hd) }, finish)

	pub := s.publishedFromURI("rtsp://127.0.0.1:8554/hevc")
	if pub == nil {
		t.Fatal("ANNOUNCE did not register a published stream")
	}
	vc, ok := pub.VideoParams()
	if !ok {
		t.Fatal("VideoParams not set")
	}
	if vc.Codec != "H265" {
		t.Fatalf("Codec = %q, want H265", vc.Codec)
	}
	if !bytes.Equal(vc.VPS, hevcVPS) || !bytes.Equal(vc.SPS, hevcSPS) || !bytes.Equal(vc.PPS, hevcPPS) {
		t.Fatalf("params = %x/%x/%x", vc.VPS, vc.SPS, vc.PPS)
	}
	if got := pub.VideoTrack().Src.Codec(); got != "H265" {
		t.Fatalf("source codec = %q, want H265", got)
	}

	sdp, err := s.buildSDP("rtsp://127.0.0.1:8554/hevc")
	if err != nil {
		t.Fatalf("buildSDP: %v", err)
	}
	want := []string{
		"m=video 0 RTP/AVP 96",
		"a=rtpmap:96 H265/90000",
		"sprop-vps=" + base64.StdEncoding.EncodeToString(hevcVPS),
		"sprop-sps=" + base64.StdEncoding.EncodeToString(hevcSPS),
		"sprop-pps=" + base64.StdEncoding.EncodeToString(hevcPPS),
		"a=control:trackID=0",
	}
	for _, line := range want {
		if !strings.Contains(sdp, line) {
			t.Fatalf("SDP missing %q:\n%s", line, sdp)
		}
	}
}

// TestHEVCPushFeedsExternalSource：H.265 RTP 包经解包后写进共享源，
// 编码、时间戳、关键帧标志都正确（RTSP 推 → RTMP 拉时的数据基础）。
func TestHEVCPushFeedsExternalSource(t *testing.T) {
	ext := source.NewExternalSource()
	ext.SetCodec("H265")
	rx, err := newPushReceiver("H265", "", 0, ext)
	if err != nil {
		t.Fatalf("newPushReceiver: %v", err)
	}
	track := &mediaTrack{kind: "video", encoding: "H265", external: ext}

	packer := rtp.NewH265Packer(1400)
	seq := uint16(1)
	for i, nalu := range [][]byte{hevcVPS, hevcSPS, hevcPPS, hevcIDR} {
		pkts, err := packer.Pack(nalu)
		if err != nil {
			t.Fatalf("pack nalu %d: %v", i, err)
		}
		for _, pkt := range pkts {
			pkt.Header.Seq = seq
			pkt.Header.Timestamp = uint32(i * 3000)
			pkt.Header.SSRC = 0xABCD
			seq++
			rx.handleH265(track, pkt, recvEvent{})
		}
	}

	rd, cancel := ext.Subscribe()
	defer cancel()
	want := [][]byte{hevcVPS, hevcSPS, hevcPPS, hevcIDR}
	for i, wantNALU := range want {
		f, ok := rd.Next(nil)
		if !ok {
			t.Fatalf("frame %d: source ended early", i)
		}
		if !bytes.Equal(f.Data, wantNALU) {
			t.Fatalf("frame %d = %x, want %x", i, f.Data, wantNALU)
		}
		if f.Timestamp != uint32(i*3000) {
			t.Fatalf("frame %d ts = %d, want %d", i, f.Timestamp, i*3000)
		}
		if f.Keyframe != (i == 3) {
			t.Fatalf("frame %d keyframe = %v, want %v", i, f.Keyframe, i == 3)
		}
	}
}

// TestStreamH265ForwardsPublishedFrames：拉流端按 track.encoding 选 H.265
// 封包器，输出 RFC 7798 的 AP / 单 NALU 包。
func TestStreamH265ForwardsPublishedFrames(t *testing.T) {
	src := source.NewExternalSource()
	src.SetCodec("H265")
	for _, nalu := range [][]byte{hevcVPS, hevcSPS, hevcPPS} {
		src.Write(source.Frame{Data: nalu})
	}
	src.Write(source.Frame{Data: hevcIDR, Keyframe: true})

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	s := New(nil, nil, false)
	track := &mediaTrack{
		kind:           "video",
		encoding:       "H265",
		isInterleaved:  true,
		interleavedRTP: 0,
		conn:           &Conn{Conn: serverConn},
		payloadType:    96,
	}
	done := make(chan struct{})
	defer close(done)
	go s.streamH264(track, src, done, 0, 1)

	br := bufio.NewReader(clientConn)
	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))

	// 预写的参数集 + IDR 时间戳相同，聚合成一个 AP。
	raw := readInterleavedPayload(t, br, 0)
	pkt, err := rtp.Parse(raw)
	if err != nil {
		t.Fatalf("rtp.Parse: %v", err)
	}
	if pkt.Header.PT != 96 {
		t.Fatalf("PT = %d, want 96", pkt.Header.PT)
	}
	if got := (pkt.Payload[0] >> 1) & 0x3F; got != rtp.H265TypeAP {
		t.Fatalf("payload type = %d, want AP(48)", got)
	}

	dep := &rtp.H265Depacketizer{}
	pkt2, err := rtp.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	nalus, err := dep.Depacketize(pkt2)
	if err != nil {
		t.Fatalf("depacketize AP: %v", err)
	}
	want := [][]byte{hevcVPS, hevcSPS, hevcPPS, hevcIDR}
	if len(nalus) != len(want) {
		t.Fatalf("AP contains %d NALUs, want %d", len(nalus), len(want))
	}
	for i := range want {
		if !bytes.Equal(nalus[i], want[i]) {
			t.Fatalf("NALU %d = %x, want %x", i, nalus[i], want[i])
		}
	}

	// 实时非关键帧：单 NALU 包。
	live := []byte{0x02, 0x01, 0xAA, 0xBB}
	src.Write(source.Frame{Data: live, Timestamp: 3600})
	pkt, err = rtp.Parse(readInterleavedPayload(t, br, 0))
	if err != nil {
		t.Fatalf("parse live frame: %v", err)
	}
	if !bytes.Equal(pkt.Payload, live) {
		t.Fatalf("live payload = %x, want %x", pkt.Payload, live)
	}
}

// TestSynthesizeTracksHEVC：RTMP 推入的 HEVC 流没有 RTSP Meta，
// 由编码参数合成 SDP 轨道。
func TestSynthesizeTracksHEVC(t *testing.T) {
	st := &stream.Stream{}
	st.SetVideoParams("H265", hevcVPS, hevcSPS, hevcPPS)

	tracks := synthesizeTracks(st)
	if len(tracks) != 1 {
		t.Fatalf("got %d tracks, want 1", len(tracks))
	}
	vt := tracks[0]
	if vt.Encoding != "H265" || vt.ClockRate != 90000 {
		t.Fatalf("track = %s/%d", vt.Encoding, vt.ClockRate)
	}
	for _, key := range []string{"sprop-vps=", "sprop-sps=", "sprop-pps="} {
		if !strings.Contains(vt.FMTP, key) {
			t.Fatalf("FMTP missing %q: %s", key, vt.FMTP)
		}
	}
}

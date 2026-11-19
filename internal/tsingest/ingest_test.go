package tsingest

import (
	"bytes"
	"testing"
	"time"

	"flow/internal/mpegts"
	"flow/internal/stream"
)

func TestIngestH264AAC(t *testing.T) {
	st := &stream.Stream{}
	st.EnsureVideo()
	st.EnsureAudio(44100)
	videoSrc := st.SourceFor("video")
	audioSrc := st.SourceFor("audio")
	if videoSrc == nil || audioSrc == nil {
		t.Fatal("sources were not created")
	}

	// 必须在写入前订阅：音频帧没有参数集预热，后订阅会错过已写入的帧。
	videoRd, vCancel := videoSrc.Subscribe()
	defer vCancel()
	audioRd, aCancel := audioSrc.Subscribe()
	defer aCancel()

	ing := NewIngest(st)
	d := mpegts.NewDemuxer()
	d.OnVideo = ing.OnVideo
	d.OnAudio = ing.OnAudio
	d.Write(buildTS(t, 3))
	d.Flush()

	vp, ok := st.VideoParams()
	if !ok {
		t.Fatal("video params were not recorded")
	}
	if vp.Codec != "H264" {
		t.Errorf("codec = %q, want H264", vp.Codec)
	}
	if !bytes.Equal(vp.SPS, testSPS) || !bytes.Equal(vp.PPS, testPPS) {
		t.Errorf("SPS/PPS = %x/%x, want %x/%x", vp.SPS, vp.PPS, testSPS, testPPS)
	}
	if len(vp.VPS) != 0 {
		t.Errorf("H.264 should not record VPS, got %x", vp.VPS)
	}

	ap, ok := st.AudioParams()
	if !ok {
		t.Fatal("audio params were not recorded")
	}
	if ap.Codec != stream.AudioAAC {
		t.Errorf("audio codec = %q, want AAC", ap.Codec)
	}
	if !bytes.Equal(ap.Config, testASC) {
		t.Errorf("ASC = %x, want %x", ap.Config, testASC)
	}
	if ap.SampleRate != 44100 || ap.Channels != 2 {
		t.Errorf("audio params = %d Hz / %d ch, want 44100 / 2", ap.SampleRate, ap.Channels)
	}

	// 第 0 帧是 AU：[AUD, SPS, PPS, IDR]，随后每个 P 帧是 [AUD, slice]。
	frames := readFrames(videoRd, 8, 2*time.Second)
	if len(frames) != 8 {
		t.Fatalf("got %d video frames, want 8", len(frames))
	}
	wantPTS := []uint32{90000, 90000, 90000, 90000, 93600, 93600, 97200, 97200}
	for i, f := range frames {
		if f.Timestamp != wantPTS[i] {
			t.Errorf("frame %d timestamp = %d, want %d", i, f.Timestamp, wantPTS[i])
		}
	}
	if !frames[0].AUStart || !frames[3].AUEnd {
		t.Errorf("first AU boundaries not marked: start=%v end=%v", frames[0].AUStart, frames[3].AUEnd)
	}
	if !frames[4].AUStart || !frames[7].AUEnd {
		t.Errorf("second AU boundaries not marked: start=%v end=%v", frames[4].AUStart, frames[7].AUEnd)
	}
	if !frames[3].Keyframe {
		t.Error("IDR frame was not marked as keyframe")
	}

	audio := readFrames(audioRd, 3, 2*time.Second)
	if len(audio) != 3 {
		t.Fatalf("got %d audio frames, want 3", len(audio))
	}
	for i, f := range audio {
		if len(f.Data) != 128 {
			t.Errorf("audio frame %d length = %d, want 128", i, len(f.Data))
		}
		if !f.Keyframe {
			t.Errorf("audio frame %d should be marked keyframe", i)
		}
	}
	if audio[0].Timestamp >= audio[1].Timestamp || audio[1].Timestamp >= audio[2].Timestamp {
		t.Errorf("audio timestamps not increasing: %d, %d, %d",
			audio[0].Timestamp, audio[1].Timestamp, audio[2].Timestamp)
	}
}

func TestIngestH265(t *testing.T) {
	st := &stream.Stream{}
	st.EnsureVideo()
	videoSrc := st.SourceFor("video")
	if videoSrc == nil {
		t.Fatal("video source was not created")
	}

	videoRd, vCancel := videoSrc.Subscribe()
	defer vCancel()

	ing := NewIngest(st)
	d := mpegts.NewDemuxer()
	d.OnVideo = ing.OnVideo
	d.OnAudio = ing.OnAudio
	d.Write(buildTSHEVC(t, 2))
	d.Flush()

	if ing.VideoCodec != "H265" {
		t.Errorf("VideoCodec = %q, want H265", ing.VideoCodec)
	}
	vp, ok := st.VideoParams()
	if !ok {
		t.Fatal("video params were not recorded")
	}
	if vp.Codec != "H265" {
		t.Errorf("codec = %q, want H265", vp.Codec)
	}
	if !bytes.Equal(vp.VPS, hevcVPS) || !bytes.Equal(vp.SPS, hevcSPS) || !bytes.Equal(vp.PPS, hevcPPS) {
		t.Errorf("VPS/SPS/PPS = %x/%x/%x, want %x/%x/%x",
			vp.VPS, vp.SPS, vp.PPS, hevcVPS, hevcSPS, hevcPPS)
	}

	// 第 0 帧是 AU：[AUD, VPS, SPS, PPS, IDR]，第 2 帧是 [AUD, slice]。
	frames := readFrames(videoRd, 7, 2*time.Second)
	if len(frames) != 7 {
		t.Fatalf("got %d video frames, want 7", len(frames))
	}
	if frames[4].Timestamp != 90000 || frames[5].Timestamp != 90000+testFrameDur {
		t.Errorf("timestamps = %d, %d", frames[4].Timestamp, frames[5].Timestamp)
	}
}

func TestIngestIgnoresEmptyInput(t *testing.T) {
	var ing *Ingest
	ing.OnVideo(mpegts.VideoAU{}) // 不应 panic
	ing.OnAudio(mpegts.AudioAU{})

	st := &stream.Stream{}
	g := NewIngest(st)
	// 没有参数集、没有音频配置：不应创建轨道。
	g.OnVideo(mpegts.VideoAU{NALUs: [][]byte{{0x41, 0x01, 0x02}}})
	g.OnAudio(mpegts.AudioAU{AU: []byte{1, 2, 3}, ASC: []byte{0x12}})
	if _, ok := st.VideoParams(); ok {
		t.Error("unexpected video params")
	}
	if _, ok := st.AudioParams(); ok {
		t.Error("unexpected audio params")
	}
}

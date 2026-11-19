package tsingest

import (
	"bytes"
	"testing"
	"time"

	"flow/internal/mpegts"
	"flow/internal/source"
)

var (
	testSPS = []byte{0x67, 0x42, 0x00, 0x1E, 0x95, 0xA8, 0x28, 0x0F, 0x00, 0x44, 0xFC, 0xB8, 0x08, 0x80}
	testPPS = []byte{0x68, 0xCE, 0x06, 0xE2}
	testASC = []byte{0x12, 0x10} // AAC LC / 44100 / stereo

	testIDR = []byte{0x65, 0x80, 0x11, 0x22, 0x33}
	testP   = []byte{0x41, 0x80, 0x66, 0x77}

	hevcVPS = []byte{0x40, 0x01, 0x0C, 0x01, 0xFF, 0xFF}
	hevcSPS = []byte{0x42, 0x01, 0x01, 0x01, 0x60, 0x00}
	hevcPPS = []byte{0x44, 0x01, 0xC0, 0x73}
	hevcIDR = []byte{0x26, 0x01, 0x80, 0xAA, 0xBB}
	hevcP   = []byte{0x02, 0x01, 0x80, 0xCC}
)

const testFrameDur = 3600 // 25 fps @ 90kHz

// buildTS 生成 frames 帧 H.264 视频 + 对应 AAC 的 MPEG-TS。
func buildTS(t *testing.T, frames int) []byte {
	t.Helper()
	var buf bytes.Buffer
	m := mpegts.NewMuxer(&buf)
	m.SetVideoParams(testSPS, testPPS)
	if err := m.SetAudioParams(testASC); err != nil {
		t.Fatalf("SetAudioParams: %v", err)
	}
	for i := 0; i < frames; i++ {
		pts := uint32(90000 + i*testFrameDur)
		nals := [][]byte{testIDR}
		if i > 0 {
			nals = [][]byte{testP}
		}
		if err := m.WriteVideo(nals, pts, pts); err != nil {
			t.Fatalf("WriteVideo: %v", err)
		}
		if err := m.WriteAudio(bytes.Repeat([]byte{0xAA}, 128), uint32(i*1024)); err != nil {
			t.Fatalf("WriteAudio: %v", err)
		}
	}
	if err := m.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return buf.Bytes()
}

// buildTSHEVC 生成 frames 帧 H.265 视频的 MPEG-TS（不带音频）。
func buildTSHEVC(t *testing.T, frames int) []byte {
	t.Helper()
	var buf bytes.Buffer
	m := mpegts.NewMuxer(&buf)
	m.SetVideoParamsFor("H265", hevcVPS, hevcSPS, hevcPPS)
	for i := 0; i < frames; i++ {
		pts := uint32(90000 + i*testFrameDur)
		nals := [][]byte{hevcIDR}
		if i > 0 {
			nals = [][]byte{hevcP}
		}
		if err := m.WriteVideo(nals, pts, pts); err != nil {
			t.Fatalf("WriteVideo: %v", err)
		}
	}
	if err := m.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return buf.Bytes()
}

// collectFrames 从订阅点开始读取最多 want 帧，超时或源关闭即返回。
func collectFrames(src *source.ExternalSource, want int, timeout time.Duration) []source.Frame {
	rd, cancel := src.Subscribe()
	defer cancel()
	return readFrames(rd, want, timeout)
}

// readFrames 从已有 reader 读取最多 want 帧，超时或源关闭即返回。
func readFrames(rd *source.Reader[source.Frame], want int, timeout time.Duration) []source.Frame {
	done := make(chan struct{})
	timer := time.AfterFunc(timeout, func() { close(done) })
	defer timer.Stop()

	var frames []source.Frame
	for len(frames) < want {
		f, ok := rd.Next(done)
		if !ok {
			break
		}
		frames = append(frames, f)
	}
	return frames
}

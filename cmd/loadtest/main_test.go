package main

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestParseFlags(t *testing.T) {
	cfg, err := parseFlags([]string{
		"-url", "rtsp://127.0.0.1:8554/live/",
		"-n", "3",
		"-d", "2m",
		"-transport", "udp",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if cfg.baseURL != "rtsp://127.0.0.1:8554/live" {
		t.Errorf("baseURL = %q, want trailing slash trimmed", cfg.baseURL)
	}
	if cfg.setupURL != "rtsp://127.0.0.1:8554/live/trackID=0" {
		t.Errorf("setupURL = %q", cfg.setupURL)
	}
	if cfg.dialAddr != "127.0.0.1:8554" {
		t.Errorf("dialAddr = %q", cfg.dialAddr)
	}
	if cfg.clients != 3 || cfg.duration != 2*time.Minute || cfg.transport != "udp" {
		t.Errorf("cfg = %+v", cfg)
	}
}

func TestParseFlagsDefaultsPort(t *testing.T) {
	cfg, err := parseFlags([]string{"-url", "rtsp://example.com/live"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if cfg.dialAddr != "example.com:554" {
		t.Errorf("dialAddr = %q, want default RTSP port", cfg.dialAddr)
	}
}

func TestParseFlagsRejectsBadInput(t *testing.T) {
	for _, args := range [][]string{
		{"-url", "http://example.com/live"},
		{"-n", "0"},
		{"-d", "0s"},
		{"-transport", "sctp"},
		{"-max-loss", "1.5"},
	} {
		if _, err := parseFlags(args); err == nil {
			t.Errorf("parseFlags(%v) 应当失败", args)
		}
	}
}

func TestSeqTracker(t *testing.T) {
	var tr seqTracker
	steps := []struct {
		seq  uint16
		gaps uint64
	}{
		{100, 0},
		{101, 0},
		{103, 1}, // 102 丢失
		{104, 0},
		{104, 0}, // 重复包
		{103, 0}, // 乱序旧包，不计空洞
		{106, 1}, // 105 丢失
	}
	for i, s := range steps {
		if got := tr.observe(s.seq); got != s.gaps {
			t.Fatalf("step %d seq=%d: gaps = %d, want %d", i, s.seq, got, s.gaps)
		}
	}
}

func TestSeqTrackerWrap(t *testing.T) {
	var tr seqTracker
	for _, seq := range []uint16{65533, 65534, 65535, 0, 1, 2} {
		if got := tr.observe(seq); got != 0 {
			t.Fatalf("seq %d: unexpected gaps %d", seq, got)
		}
	}
}

func TestReadResponse(t *testing.T) {
	raw := "RTSP/1.0 200 OK\r\n" +
		"CSeq: 1\r\n" +
		"Session: sess-7;timeout=60\r\n" +
		"Content-Length: 5\r\n" +
		"\r\n" +
		"hello"
	c := &client{br: bufio.NewReader(strings.NewReader(raw))}
	resp, err := c.readResponse()
	if err != nil {
		t.Fatalf("readResponse: %v", err)
	}
	if !isOK(resp) {
		t.Errorf("status = %q", resp.status)
	}
	if got := sessionValue(resp.headers["session"]); got != "sess-7" {
		t.Errorf("session = %q", got)
	}
	if string(resp.body) != "hello" {
		t.Errorf("body = %q", resp.body)
	}
}

func TestReadTCPMessageCountsRTPOnly(t *testing.T) {
	rtp := []byte{0x80, 96, 0x00, 0x2a, 0, 0, 0, 0, 0, 0, 0, 1, 0xde, 0xad}
	rtcp := []byte{0x80, 200, 0x00, 0x01, 0, 0, 0, 1}
	var raw []byte
	raw = appendFrame(raw, 0, rtp)
	raw = appendFrame(raw, 1, rtcp)

	agg := &counters{}
	c := &client{cfg: config{}, agg: agg, br: bufio.NewReader(bytes.NewReader(raw))}
	for i := range 2 {
		if resp, err := c.readTCPMessage(); err != nil || resp != nil {
			t.Fatalf("frame %d: resp=%v err=%v", i, resp, err)
		}
	}
	if c.packets != 1 || agg.packets.Load() != 1 {
		t.Errorf("packets = %d, agg = %d; RTCP 不应计数", c.packets, agg.packets.Load())
	}
	if c.tracker.last != 0x2a {
		t.Errorf("seq = %d, want 42", c.tracker.last)
	}
}

func appendFrame(dst []byte, channel byte, payload []byte) []byte {
	dst = append(dst, '$', channel, byte(len(payload)>>8), byte(len(payload)))
	return append(dst, payload...)
}

func TestPercentile(t *testing.T) {
	ds := []time.Duration{40 * time.Millisecond, 10 * time.Millisecond, 30 * time.Millisecond, 20 * time.Millisecond}
	if got := percentile(ds, 50); got != 20*time.Millisecond {
		t.Errorf("p50 = %s", got)
	}
	if got := percentile(ds, 95); got != 40*time.Millisecond {
		t.Errorf("p95 = %s", got)
	}
	if got := percentile(nil, 50); got != 0 {
		t.Errorf("empty p50 = %s", got)
	}
}

func TestLossRate(t *testing.T) {
	if got := lossRate(990, 10); got < 0.99 || got > 1.01 {
		t.Errorf("lossRate = %f, want ~1%%", got)
	}
	if got := lossRate(0, 0); got != 0 {
		t.Errorf("empty lossRate = %f", got)
	}
}

package metrics

import (
	"strings"
	"testing"
)

func TestRegistryPrometheus(t *testing.T) {
	r := New()
	r.IncConn("rtsp")
	r.IncConn("rtsp")
	r.IncConnTotal("rtsp")
	r.DecConn("rtsp")
	r.IncConn("rtmp")
	r.IncSession()
	r.IncSessionTotal()
	r.IncPublish()
	r.IncPublishTotal()
	r.IncPlay()
	r.IncPlayTotal()
	r.AddPackets("video", 42)
	r.AddOctets("video", 4200)
	r.AddDropped("audio", 3)
	r.IncSR()
	r.IncRR()
	r.IncHLSPackager()
	r.IncHLSRequest("master")
	r.IncHLSRequest("segment")
	r.AddHLSBytes("segment", 4096)
	r.IncHLSSegment("video")
	r.IncHLSPart("audio")

	var b strings.Builder
	r.WritePrometheus(&b)
	out := b.String()

	wants := []string{
		`flow_connections{proto="rtsp"} 1`,
		`flow_connections{proto="rtmp"} 1`,
		`flow_connections_total{proto="rtsp"} 1`,
		`flow_sessions{proto="rtsp"} 1`,
		`flow_sessions_total{proto="rtsp"} 1`,
		`flow_rtmp_publishes 1`,
		`flow_rtmp_publishes_total 1`,
		`flow_rtmp_plays 1`,
		`flow_rtmp_plays_total 1`,
		`flow_rtsp_packets_sent_total{kind="video"} 42`,
		`flow_rtsp_octets_sent_total{kind="video"} 4200`,
		`flow_rtsp_frames_dropped_total{kind="audio"} 3`,
		`flow_rtsp_sr_sent_total 1`,
		`flow_rtsp_rr_received_total 1`,
		`flow_hls_packagers 1`,
		`flow_hls_requests_total{kind="master"} 1`,
		`flow_hls_requests_total{kind="segment"} 1`,
		`flow_hls_bytes_served_total{kind="segment"} 4096`,
		`flow_hls_segments_total{track="video"} 1`,
		`flow_hls_parts_total{track="audio"} 1`,
	}
	for _, w := range wants {
		if !strings.Contains(out, w) {
			t.Errorf("metrics output missing %q\n%s", w, out)
		}
	}

	// 每个指标都要有 TYPE 行，Prometheus 才能正确解析 counter/gauge。
	for _, typ := range []string{
		"# TYPE flow_connections gauge",
		"# TYPE flow_connections_total counter",
		"# TYPE flow_rtsp_packets_sent_total counter",
	} {
		if !strings.Contains(out, typ) {
			t.Errorf("metrics output missing %q", typ)
		}
	}
}

func TestRegistryGaugesReturnToZero(t *testing.T) {
	r := New()
	r.IncConn("rtsp")
	r.DecConn("rtsp")
	r.IncSession()
	r.DecSession()
	r.IncPublish()
	r.DecPublish()
	r.IncPlay()
	r.DecPlay()

	var b strings.Builder
	r.WritePrometheus(&b)
	out := b.String()
	for _, want := range []string{
		`flow_connections{proto="rtsp"} 0`,
		`flow_sessions{proto="rtsp"} 0`,
		"flow_rtmp_publishes 0",
		"flow_rtmp_plays 0",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("want %q after increment+decrement\n%s", want, out)
		}
	}
}

func TestNilRegistry(t *testing.T) {
	var r *Registry
	// 不应 panic。
	r.IncConn("rtsp")
	r.DecConn("rtsp")
	r.IncSession()
	r.AddPackets("video", 1)
	var b strings.Builder
	r.WritePrometheus(&b)
	if b.Len() != 0 {
		t.Errorf("nil registry wrote %q", b.String())
	}
}

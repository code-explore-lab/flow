package rtsp

import (
	"io"
	"net"
	"testing"
	"time"

	"flow/internal/source"
)

// fakeSeekSource 记录 streamH264 用的是哪个订阅入口。
type fakeSeekSource struct {
	plainCalled bool
	fromCalled  bool
	fromStart   time.Duration
}

func closedFrameReader(f source.Frame) *source.Reader[source.Frame] {
	ring := source.NewRingBuffer[source.Frame](4)
	rd := ring.Subscribe(f)
	ring.Close()
	return rd
}

func (f *fakeSeekSource) Subscribe() (*source.Reader[source.Frame], func()) {
	f.plainCalled = true
	rd := closedFrameReader(source.Frame{Data: []byte{0x65, 0x88, 0x84, 0x00}, Keyframe: true})
	return rd, rd.Close
}

func (f *fakeSeekSource) SubscribeFrom(start time.Duration) (*source.Reader[source.Frame], func(), error) {
	f.fromCalled = true
	f.fromStart = start
	rd := closedFrameReader(source.Frame{Data: []byte{0x65, 0x88, 0x84, 0x00}, Keyframe: true})
	return rd, rd.Close, nil
}

// TestStreamH264UsesSubscribeFromForSeek start>0 时必须走 SubscribeFrom，
// start=0 时维持共享源的普通订阅。
func TestStreamH264UsesSubscribeFromForSeek(t *testing.T) {
	cases := []struct {
		name  string
		start time.Duration
	}{
		{"seek", 3 * time.Second},
		{"no seek", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := New(nil, nil, false)
			src := &fakeSeekSource{}

			serverConn, clientConn := net.Pipe()
			defer clientConn.Close()
			defer serverConn.Close()
			go func() { _, _ = io.Copy(io.Discard, clientConn) }()

			track := &mediaTrack{
				kind:           "video",
				isInterleaved:  true,
				interleavedRTP: 0,
				conn:           &Conn{Conn: serverConn},
			}
			done := make(chan struct{})
			defer close(done)

			s.streamH264(track, src, done, tc.start)

			if tc.start > 0 {
				if !src.fromCalled || src.plainCalled {
					t.Fatalf("start=%s: fromCalled=%v plainCalled=%v, want SubscribeFrom only",
						tc.start, src.fromCalled, src.plainCalled)
				}
				if src.fromStart != tc.start {
					t.Fatalf("SubscribeFrom start = %s, want %s", src.fromStart, tc.start)
				}
				return
			}
			if !src.plainCalled || src.fromCalled {
				t.Fatalf("start=0: fromCalled=%v plainCalled=%v, want Subscribe only",
					src.fromCalled, src.plainCalled)
			}
		})
	}
}

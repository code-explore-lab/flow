package rtmp

// T-7：chunk 层基准测试，量化 Set Chunk Size 对吞吐/延迟的影响。
//
// 三个维度：
//   - BenchmarkWriteMessage：纯编码 + 写调用开销（写到 nopConn，不经过内核）；
//   - BenchmarkReadMessage：预编码数据上的重组开销；
//   - BenchmarkRoundTrip：net.Pipe 上一写一读的往返延迟（同步管道，写调用
//     次数直接体现为阻塞次数，用于观察 chunk size 变小时的延迟退化）。
//
// 运行：
//
//	go test ./internal/rtmp -run '^$' -bench 'Chunk' -benchmem
//	go test ./internal/rtmp -run '^$' -bench 'Chunk' -benchtime=2000x

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// nopConn 实现 net.Conn，Write 只统计字节数，用于隔离编码开销。
type nopConn struct{ written int64 }

func (c *nopConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *nopConn) Write(b []byte) (int, error)      { c.written += int64(len(b)); return len(b), nil }
func (c *nopConn) Close() error                     { return nil }
func (c *nopConn) LocalAddr() net.Addr              { return nopAddr{} }
func (c *nopConn) RemoteAddr() net.Addr             { return nopAddr{} }
func (c *nopConn) SetDeadline(time.Time) error      { return nil }
func (c *nopConn) SetReadDeadline(time.Time) error  { return nil }
func (c *nopConn) SetWriteDeadline(time.Time) error { return nil }

type nopAddr struct{}

func (nopAddr) Network() string { return "nop" }
func (nopAddr) String() string  { return "nop" }

// bufConn 把 Write 追加进 bytes.Buffer，用于预编码参照数据。
type bufConn struct {
	bytes.Buffer
}

func (c *bufConn) Close() error                     { return nil }
func (c *bufConn) LocalAddr() net.Addr              { return nopAddr{} }
func (c *bufConn) RemoteAddr() net.Addr             { return nopAddr{} }
func (c *bufConn) SetDeadline(time.Time) error      { return nil }
func (c *bufConn) SetReadDeadline(time.Time) error  { return nil }
func (c *bufConn) SetWriteDeadline(time.Time) error { return nil }

// benchChunkSizes 覆盖协议默认值、当前服务端值（4096）、常见实现值以及上限附近。
var benchChunkSizes = []uint32{128, 1024, 4096, 30000, 60000, 131072}

// benchPayloadSizes 对应音频帧、普通视频帧、关键帧三种常见消息体。
var benchPayloadSizes = []struct {
	name string
	size int
}{
	{"audio-1k", 1 << 10},
	{"video-30k", 30 << 10},
	{"keyframe-150k", 150 << 10},
}

func benchPayload(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(i * 31)
	}
	return p
}

func BenchmarkWriteMessage(b *testing.B) {
	for _, chunk := range benchChunkSizes {
		for _, ps := range benchPayloadSizes {
			name := fmt.Sprintf("chunk=%d/%s", chunk, ps.name)
			b.Run(name, func(b *testing.B) {
				c := &Conn{Conn: &nopConn{}, writeChunkSize: chunk}
				m := &Message{TypeID: msgVideo, StreamID: 1, Timestamp: 4000, Payload: benchPayload(ps.size)}
				b.SetBytes(int64(ps.size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := c.writeMessage(m, videoCSID); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkReadMessage(b *testing.B) {
	for _, chunk := range benchChunkSizes {
		for _, ps := range benchPayloadSizes {
			name := fmt.Sprintf("chunk=%d/%s", chunk, ps.name)
			b.Run(name, func(b *testing.B) {
				payload := benchPayload(ps.size)
				m := &Message{TypeID: msgVideo, StreamID: 1, Timestamp: 4000, Payload: payload}
				// 预编码一条消息作为读取输入。
				w := &Conn{Conn: &bufConn{}, writeChunkSize: chunk}
				if err := w.writeMessage(m, videoCSID); err != nil {
					b.Fatal(err)
				}
				data := w.Conn.(*bufConn).Bytes()

				b.SetBytes(int64(ps.size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					r := &Conn{
						br:            bufio.NewReader(bytes.NewReader(data)),
						readChunkSize: chunk,
						chunks:        make(map[uint32]*chunkStream),
					}
					got, err := r.readMessage()
					if err != nil {
						b.Fatal(err)
					}
					if len(got.Payload) != ps.size {
						b.Fatalf("payload = %d, want %d", len(got.Payload), ps.size)
					}
				}
			})
		}
	}
}

// BenchmarkRoundTrip 用 net.Pipe 测单条消息从写出到读回的完整延迟。
func BenchmarkRoundTrip(b *testing.B) {
	for _, chunk := range benchChunkSizes {
		for _, ps := range benchPayloadSizes {
			name := fmt.Sprintf("chunk=%d/%s", chunk, ps.name)
			b.Run(name, func(b *testing.B) {
				local, remote := net.Pipe()
				defer local.Close()
				defer remote.Close()

				w := newConn(local)
				w.writeChunkSize = chunk
				r := newConn(remote)
				r.readChunkSize = chunk

				m := &Message{TypeID: msgVideo, StreamID: 1, Timestamp: 4000, Payload: benchPayload(ps.size)}
				rc := make(chan error, 1)
				go func() {
					for i := 0; i < b.N; i++ {
						got, err := r.readMessage()
						if err != nil {
							rc <- err
							return
						}
						if len(got.Payload) != ps.size {
							rc <- fmt.Errorf("payload = %d, want %d", len(got.Payload), ps.size)
							return
						}
					}
					rc <- nil
				}()

				b.SetBytes(int64(ps.size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := w.writeMessage(m, videoCSID); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if err := <-rc; err != nil {
					b.Fatal(err)
				}
			})
		}
	}
}

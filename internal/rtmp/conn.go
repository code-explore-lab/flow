package rtmp

// 一条 RTMP 连接的读写状态。
//
// RTMP 把每条消息拆成若干 chunk 传输：每条 chunk 带 basic header（fmt + csid），
// 同一 csid 上的 chunk 属于同一条逻辑流。接收时需要按 csid 保存「上一个
// message header + 已收字节数」，等数据收满再拼成一条完整消息。
//
// 读是单 goroutine（每条连接一个），所以 chunk 状态不需要加锁；
// 写可能来自命令响应和媒体发送等多个 goroutine，统一走 mu。

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

// RTMP 默认 chunk size（协议规定 128）。
const defaultChunkSize uint32 = 128

// maxChunkSize 是本实现接受的上限。协议允许 31 位，但没有真实实现会发
// 这么大的 chunk；限制一下避免对端用 Set Chunk Size 触发超大分配。
const maxChunkSize uint32 = 0xFFFFFF // 16 MiB - 1

// Conn 包装一条 TCP 连接。
type Conn struct {
	net.Conn

	// br 缓冲读；chunk 的 basic/message header 和负载都从这里读。
	// cr 在底层统计已接收字节数，供 Ack 使用。
	br *bufio.Reader
	cr *countingReader

	mu sync.Mutex // 串行化写操作

	closeOnce sync.Once

	// 读写方向各自维护 chunk size：Set Chunk Size 控制的是「对端发送」的
	// 大小（即本端 read），本端写出去的大小由 writeChunkSize 决定。
	readChunkSize  uint32
	writeChunkSize uint32

	// 每个 CSID 的重组状态，key 是 chunk stream id。
	chunks map[uint32]*chunkStream

	// 对端声明的流控参数（见 message.go 的 handleControl）：
	//   peerAckWindow  对端要求本端每收这么多字节回一次 Ack；
	//   peerBandwidth  对端要求本端未确认的发送窗口上限；
	//   lastAckSeq     对端最近确认的本端已发字节数。
	peerAckWindow uint32
	peerBandwidth uint32
	lastAckSeq    uint32

	// lastAckSent 上次发送 Ack 时的已接收字节数（只由读循环更新）。
	lastAckSent uint64

	// 命令层状态（command.go / publish.go），只在连接的读循环里使用。
	srv          *Server
	app          string
	streamName   string
	nextStreamID uint32
	publish      *publishState
	play         *playState
}

// countingReader 统计底层连接读到的字节总数，用于周期性回 Ack。
type countingReader struct {
	r io.Reader
	n atomic.Uint64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(uint64(n))
	return n, err
}

// chunkStream 一个 CSID 上正在接收/最近收到的消息状态。
type chunkStream struct {
	// message header（fmt 0 完整给出，fmt 1/2/3 按规则继承）
	timestamp      uint32 // 当前消息的绝对时间戳
	timestampDelta uint32 // 最近一次 fmt 1/2 的增量；fmt 3 新消息复用它
	length         uint32 // message length
	typeID         uint8  // message type id
	streamID       uint32 // message stream id

	// 重组缓冲
	buf      []byte
	received uint32 // 已收到该消息的字节数（不含 chunk header）
}

// newConn 包装一条裸 TCP 连接。
func newConn(c net.Conn) *Conn {
	cr := &countingReader{r: c}
	return &Conn{
		Conn:           c,
		br:             bufio.NewReader(cr),
		cr:             cr,
		readChunkSize:  defaultChunkSize,
		writeChunkSize: defaultChunkSize,
		chunks:         make(map[uint32]*chunkStream),
	}
}

// chunkState 返回 csid 对应的重组状态，没有则创建。
func (c *Conn) chunkState(csid uint32) *chunkStream {
	st := c.chunks[csid]
	if st == nil {
		st = &chunkStream{}
		c.chunks[csid] = st
	}
	return st
}

// setReadChunkSize 更新接收方向 chunk size（收到 Set Chunk Size 时调用）。
func (c *Conn) setReadChunkSize(n uint32) error {
	if n == 0 || n > maxChunkSize {
		return fmt.Errorf("rtmp: invalid chunk size %d", n)
	}
	c.readChunkSize = n
	return nil
}

// setWriteChunkSize 更新发送方向 chunk size（本端决定并通知对端时调用）。
func (c *Conn) setWriteChunkSize(n uint32) error {
	if n == 0 || n > maxChunkSize {
		return fmt.Errorf("rtmp: invalid chunk size %d", n)
	}
	c.writeChunkSize = n
	return nil
}

// reset 丢弃当前消息已重组的部分（Abort Message 或收到新的 fmt 0/1/2
// header 时使用）。header 字段保留，下一次 chunk 继续按规则继承。
func (st *chunkStream) reset() {
	st.buf = nil
	st.received = 0
}

// write 串行写一整块字节。
func (c *Conn) write(b []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.Conn.Write(b)
	return err
}

// Close 幂等关闭：服务端 shutdown 和连接处理 goroutine 都可能调用。
func (c *Conn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		err = c.Conn.Close()
	})
	return err
}

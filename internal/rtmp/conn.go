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
	"time"
)

// connSeq 给每条连接生成稳定的管理 ID（rtmp-1、rtmp-2 ...）。
// RTSP 的会话 ID 由 session.Manager 分配，两套 ID 在管理接口里并列展示。
var connSeq atomic.Uint64

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

	// E-5 超时配置，由 Server.handleConn 从 Server 下发。
	handshakeTimeout time.Duration // 握手总超时；0 表示不设
	connTimeout      time.Duration // 消息读/写空闲超时；0 表示不设

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

	// fourCCs 是 connect 命令里客户端声明的 fourCcList（Enhanced RTMP）。
	// nil 表示客户端没声明，按支持 HEVC 的新客户端处理。
	fourCCs []string

	// O-1 管理接口元数据。id/createdAt 创建后不变；app/stream/publishing/
	// playing 由连接自身的 goroutine 写入、管理 HTTP goroutine 读取，统一
	// 由 adminMu 保护，避免直接读上面的命令层字段造成数据竞争。
	id              string
	createdAt       time.Time
	lastActive      atomic.Int64 // UnixNano；读循环每收到一条消息刷新
	adminMu         sync.Mutex
	adminApp        string
	adminPubStream  string
	adminPlayStream string
	adminPub        bool
	adminPlay       bool
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

	// extTimestamp 最近一次 fmt 0/1/2 header 的时间戳字段是否为 0xFFFFFF。
	// 是的话每个 fmt 3 chunk 的 header 后都要跟 4 字节扩展时间戳。
	extTimestamp bool

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
		id:             fmt.Sprintf("rtmp-%d", connSeq.Add(1)),
		createdAt:      time.Now(),
	}
}

// lastActivity 返回最近一次收到消息的时间；从未收到时回退到建连时间。
func (c *Conn) lastActivity() time.Time {
	if n := c.lastActive.Load(); n > 0 {
		return time.Unix(0, n)
	}
	return c.createdAt
}

// setAdminApp 记录 connect 声明的 app。
func (c *Conn) setAdminApp(app string) {
	c.adminMu.Lock()
	c.adminApp = app
	c.adminMu.Unlock()
}

// setAdminPublish 记录推流状态；publishing=false 时同时清掉流名。
func (c *Conn) setAdminPublish(name string, publishing bool) {
	c.adminMu.Lock()
	c.adminPub = publishing
	if publishing {
		c.adminPubStream = name
	} else {
		c.adminPubStream = ""
	}
	c.adminMu.Unlock()
}

// setAdminPlaying 记录拉流状态；playing=false 时同时清掉流名。
func (c *Conn) setAdminPlaying(name string, playing bool) {
	c.adminMu.Lock()
	c.adminPlay = playing
	if playing {
		c.adminPlayStream = name
	} else {
		c.adminPlayStream = ""
	}
	c.adminMu.Unlock()
}

// adminSnapshot 返回管理接口需要的连接状态快照。
func (c *Conn) adminSnapshot() (app, stream string, publishing, playing bool) {
	c.adminMu.Lock()
	defer c.adminMu.Unlock()
	stream = c.adminPubStream
	if !c.adminPub {
		stream = c.adminPlayStream
	}
	return c.adminApp, stream, c.adminPub, c.adminPlay
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

// maxAssemblePrealloc 是单条消息预分配重组缓冲的上限。常见视频帧都
// 小于 1MB；更大的消息（协议上限 16MB）按需增长，避免对端只发一个
// 超大 length 就让本端立刻分配十几 MB。
const maxAssemblePrealloc = 1 << 20

// begin 开始一条新消息：复用上次缓冲的容量，按 message length 一次
// 预分配，后续每个 chunk 直接读进缓冲区尾部，避免逐片 append 的增长和
// 拷贝（T-7：4096 chunk 下 150KB 关键帧原来要 append 37 次）。
func (st *chunkStream) begin(length uint32) {
	st.received = 0
	want := min(int(length), maxAssemblePrealloc)
	if cap(st.buf) >= want {
		st.buf = st.buf[:want]
		return
	}
	st.buf = make([]byte, want)
}

// write 串行写一整块字节。
func (c *Conn) write(b []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.connTimeout > 0 {
		_ = c.Conn.SetWriteDeadline(time.Now().Add(c.connTimeout))
	}
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

// dispatch 处理一条已重组消息：回 Ack、应用控制消息、按类型分发给命令/媒体
// 处理器。返回 error 表示连接应终止。服务端读循环和 RTMP client 共用。
func (c *Conn) dispatch(m *Message) error {
	if err := c.maybeAck(); err != nil {
		return err
	}
	// 控制消息优先
	if handled, err := c.handleControl(m); err != nil {
		return err
	} else if handled {
		return nil
	}

	switch m.TypeID {
	case msgCommandAMF0:
		return c.handleCommand(m)
	case msgVideo:
		c.handleVideo(m)
	case msgAudio:
		c.handleAudio(m)
	case msgDataAMF0:
		c.handleData(m)
	case msgAggregate:
		// T-7：聚合消息拆成子消息后按普通消息处理。
		return c.handleAggregate(m)
	default:
		// msgDataAMF3(15)、msgCommandAMF3(17) 等不支持的 AMF3 消息
		// 与未知类型忽略。
	}
	return nil
}

// pump 是连接的读循环：按 connTimeout 设读超时、重组消息、分发。
// 返回 error 表示连接终止（含 ctx 触发的外部 Close）。
func (c *Conn) pump() error {
	for {
		// E-5：空闲读超时，防止半开连接让读循环永久挂住。
		if c.connTimeout > 0 {
			_ = c.SetReadDeadline(time.Now().Add(c.connTimeout))
		}
		m, err := c.readMessage()
		if err == nil {
			c.lastActive.Store(time.Now().UnixNano())
		}
		if err != nil {
			return err
		}
		if err := c.dispatch(m); err != nil {
			return err
		}
	}
}

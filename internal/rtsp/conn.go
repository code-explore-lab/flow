package rtsp

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
)

// interleavedMagic 是 RTSP interleaved 二进制帧的起始字节 '$'。
const interleavedMagic = '$'

// Conn 包装一条 TCP 连接，串行化所有写操作。
// RTSP 文本响应和 interleaved $ 帧共用同一条字节流，
// 音视频两个推流 goroutine 如果各自直接 conn.Write 会互相穿插，所以统一走这把锁。
type Conn struct {
	net.Conn
	mu sync.Mutex

	// 这条连接上建立/复用过的会话 ID。连接断开时（客户端 kill -9、网线拔掉）
	// handleConn 退出，顺手把这些会话回收掉，不用干等 60s 超时。
	sessMu     sync.Mutex
	sessionIDs []string

	// announceID 记录 ANNOUNCE 创建的会话 ID。ffmpeg 等客户端在 ANNOUNCE
	// 响应里拿到 Session 后会复用；万一它不带，后续 SETUP 用这条连接上的
	// 会话兜底，避免同一路推流裂成两个会话、丢失已解析的 SDP。
	announceID string

	// describeStream 记录 DESCRIBE 命中的已发布流名。拉流 SETUP 的 URI
	// 写法可能与 DESCRIBE 不同（有的客户端会改写路径），按连接记住更稳。
	describeStream string
}

func (c *Conn) write(b []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.Conn.Write(b)
	return err
}

// bindSession 记下这条 TCP 连接用到的会话（一个连接可以带多个会话）。
func (c *Conn) bindSession(id string) {
	c.sessMu.Lock()
	defer c.sessMu.Unlock()
	for _, v := range c.sessionIDs {
		if v == id {
			return
		}
	}
	c.sessionIDs = append(c.sessionIDs, id)
}

// boundSessions 返回这条连接上的会话 ID 快照。
func (c *Conn) boundSessions() []string {
	c.sessMu.Lock()
	defer c.sessMu.Unlock()
	out := make([]string, len(c.sessionIDs))
	copy(out, c.sessionIDs)
	return out
}

// setAnnounceSession 记住 ANNOUNCE 创建的会话。
func (c *Conn) setAnnounceSession(id string) {
	c.sessMu.Lock()
	defer c.sessMu.Unlock()
	c.announceID = id
}

// announceSession 返回 ANNOUNCE 创建的会话 ID，没有则返回空串。
func (c *Conn) announceSession() string {
	c.sessMu.Lock()
	defer c.sessMu.Unlock()
	return c.announceID
}

// setDescribeStream 记录这条连接 DESCRIBE 命中的已发布流名。
func (c *Conn) setDescribeStream(name string) {
	c.sessMu.Lock()
	defer c.sessMu.Unlock()
	c.describeStream = name
}

// describeStreamName 返回这条连接 DESCRIBE 命中的流名，没有则返回空串。
func (c *Conn) describeStreamName() string {
	c.sessMu.Lock()
	defer c.sessMu.Unlock()
	return c.describeStream
}

// readInterleavedFrame 从同一个 TCP 字节流里切出一个 $ 帧：
//
//	$ + channel(1) + length(2, 大端) + payload
//
// 注意 length 只算 payload，不含这 4 字节头。
func readInterleavedFrame(br *bufio.Reader) (channel byte, payload []byte, err error) {
	var hdr [4]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return 0, nil, err
	}
	if hdr[0] != interleavedMagic {
		return 0, nil, fmt.Errorf("bad interleaved magic: %#x", hdr[0])
	}
	length := binary.BigEndian.Uint16(hdr[2:4])
	payload = make([]byte, length)
	// io.ReadFUll(r, buf) 必须把buf填满
	if _, err := io.ReadFull(br, payload); err != nil {
		return 0, nil, err
	}
	return hdr[1], payload, nil
}

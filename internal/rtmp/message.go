package rtmp

// RTMP 消息类型与协议控制消息。
//
// message header 里的 message type id 决定负载语义：1/2/3/5/6 是协议控制
// 消息，8/9 是音视频，18/20 是 AMF0 编码的数据/命令。本文件实现控制消息
// 的构造与解析，命令层和媒体层负责在 Chunk Stream 循环里调用。
//
// 控制消息负载都是 4 字节大端（Set Peer Bandwidth 额外带 1 字节限速类型），
// 固定走 CSID 2、message stream id 0、时间戳 0。

import "fmt"

// 消息类型 id。
const (
	msgSetChunkSize     uint8 = 1 // 通知对端后续 chunk 的最大负载
	msgAbort            uint8 = 2 // 丢弃某 CSID 上没收完的消息
	msgAck              uint8 = 3 // 确认已收到的字节数
	msgUserControl      uint8 = 4 // 流事件（本实现不解析）
	msgWindowAckSize    uint8 = 5 // 要求对端每收这么多字节回一次 Ack
	msgSetPeerBandwidth uint8 = 6 // 限制对端未确认的发送窗口

	msgAudio       uint8 = 8  // FLV audio tag
	msgVideo       uint8 = 9  // FLV video tag
	msgDataAMF3    uint8 = 15 // AMF3 数据（本实现不支持）
	msgCommandAMF3 uint8 = 17 // AMF3 命令（本实现不支持）
	msgDataAMF0    uint8 = 18 // onMetaData 等
	msgCommandAMF0 uint8 = 20 // connect/publish/play 等
)

// Set Peer Bandwidth 的限速类型。
const (
	bandwidthHard    uint8 = 0 // 对端必须限制在给定窗口
	bandwidthSoft    uint8 = 1 // 限制为给定窗口与当前窗口的较小值
	bandwidthDynamic uint8 = 2 // 前一个是硬限制则取较小值，否则视为硬限制
)

// 协议控制消息固定使用的 CSID 与 message stream id。
const (
	controlCSID     uint32 = 2 //CSID
	controlStreamID uint32 = 0 // MSID
)

// ---- 构造 ----

func buildSetChunkSize(n uint32) []byte { return appendUint32(nil, n) }

func buildAbort(csid uint32) []byte { return appendUint32(nil, csid) }

func buildAck(seq uint32) []byte { return appendUint32(nil, seq) }

func buildWindowAckSize(n uint32) []byte { return appendUint32(nil, n) }

func buildSetPeerBandwidth(n uint32, limit uint8) []byte {
	return append(appendUint32(nil, n), limit)
}

// ---- 解析 ----

// controlUint32 读控制消息负载开头的 4 字节大端整数。
func controlUint32(p []byte) (uint32, error) {
	if len(p) < 4 {
		return 0, fmt.Errorf("rtmp: control message too short: %d bytes", len(p))
	}
	return uint32(p[0])<<24 | uint32(p[1])<<16 | uint32(p[2])<<8 | uint32(p[3]), nil
}

// parseSetPeerBandwidth 解出窗口大小和限速类型。
func parseSetPeerBandwidth(p []byte) (n uint32, limit uint8, err error) {
	if len(p) < 5 {
		return 0, 0, fmt.Errorf("rtmp: set peer bandwidth too short: %d bytes", len(p))
	}
	n, err = controlUint32(p)
	return n, p[4], err
}

// ---- 发送 ----

// writeControl 用控制消息固定的 CSID 2 / stream 0 发一条控制消息。
func (c *Conn) writeControl(typeID uint8, payload []byte) error {
	return c.writeMessage(&Message{TypeID: typeID, StreamID: controlStreamID, Payload: payload}, controlCSID)
}

// writeSetChunkSize 先按旧大小发出 Set Chunk Size，之后的消息才用新大小
// 分片（消息本身只有 4 字节，不受影响）。
func (c *Conn) writeSetChunkSize(n uint32) error {
	if n == 0 || n > maxChunkSize {
		return fmt.Errorf("rtmp: invalid chunk size %d", n)
	}
	if err := c.writeControl(msgSetChunkSize, buildSetChunkSize(n)); err != nil {
		return err
	}
	c.writeChunkSize = n
	return nil
}

func (c *Conn) writeAbort(csid uint32) error {
	return c.writeControl(msgAbort, buildAbort(csid))
}

func (c *Conn) writeAck(seq uint32) error {
	return c.writeControl(msgAck, buildAck(seq))
}

func (c *Conn) writeWindowAckSize(n uint32) error {
	return c.writeControl(msgWindowAckSize, buildWindowAckSize(n))
}

func (c *Conn) writeSetPeerBandwidth(n uint32, limit uint8) error {
	return c.writeControl(msgSetPeerBandwidth, buildSetPeerBandwidth(n, limit))
}

// ---- 接收 ----

// handleControl 处理一条协议控制消息，返回 handled=false 表示这不是控制
// 消息，调用方继续按命令/媒体消息处理。
func (c *Conn) handleControl(m *Message) (handled bool, err error) {
	switch m.TypeID {
	case msgSetChunkSize:
		n, err := controlUint32(m.Payload)
		if err != nil {
			return true, err
		}
		return true, c.setReadChunkSize(n)
	case msgAbort:
		csid, err := controlUint32(m.Payload)
		if err != nil {
			return true, err
		}
		// 只丢没收完的负载，header 状态保留：该 CSID 的下一条消息仍按
		// 继承规则解析。
		c.chunkState(csid).reset()
		return true, nil
	case msgAck:
		seq, err := controlUint32(m.Payload)
		if err != nil {
			return true, err
		}
		c.lastAckSeq = seq
		return true, nil
	case msgWindowAckSize:
		n, err := controlUint32(m.Payload)
		if err != nil {
			return true, err
		}
		c.peerAckWindow = n
		return true, nil
	case msgSetPeerBandwidth:
		n, _, err := parseSetPeerBandwidth(m.Payload)
		if err != nil {
			return true, err
		}
		c.peerBandwidth = n
		return true, nil
	}
	return false, nil
}

package rtmp

// T-7：Aggregate Message（type 22）解包。
//
// 聚合消息把多条音视频/数据消息装进一条 RTMP 消息，减少 chunk 头和
// 系统调用次数。消息体是一串 FLV tag 风格的子消息：
//
//	type (1) + length (3 BE) + timestamp (3 BE) + timestampExtended (1)
//	+ streamID (3，恒为 0 并被外层覆盖) + payload (length) + backPointer (4 BE)
//
// backPointer 记录前一条子消息的字节数（11 + length），与 FLV 文件的
// PreviousTagSize 语义相同。规范要求首个子消息时间戳等于聚合消息时间戳；
// 不一致时按 offset = 聚合时间戳 - 首个时间戳 归一化，后续子消息整体平移。

import "fmt"

// aggregateSubHeaderSize 子消息固定头长度。
const aggregateSubHeaderSize = 11

// aggregateBackPointerSize 每条子消息尾部的 back pointer 长度。
const aggregateBackPointerSize = 4

// aggregateSubMessage 一条拆出来的子消息（Payload 与输入共享底层数组）。
type aggregateSubMessage struct {
	TypeID    uint8
	Timestamp uint32
	Payload   []byte
}

// splitAggregate 把聚合消息负载拆成子消息。
//
// 容错策略：
//   - 空负载返回空列表，不报错；
//   - 头不完整、length 越过剩余负载、back pointer 被截断（只剩 1~3 字节）
//     都视为损坏，返回错误（调用方断开连接）；
//   - 最后一条子消息可以不带 back pointer（部分老实现省略），此时剩余
//     字节为 0，正常结束；
//   - 子消息的 stream id 一律忽略，由聚合消息的 StreamID 覆盖。
func splitAggregate(payload []byte, outerTimestamp uint32) ([]aggregateSubMessage, error) {
	var (
		subs      []aggregateSubMessage
		offset    uint32
		haveFirst bool
	)
	for len(payload) > 0 {
		if len(payload) < aggregateSubHeaderSize {
			return nil, fmt.Errorf("rtmp: aggregate truncated header: %d bytes left", len(payload))
		}
		typeID := payload[0]
		length := uint32(payload[1])<<16 | uint32(payload[2])<<8 | uint32(payload[3])
		// FLV tag 时间戳：低 24 位在前，高 8 位单独一字节。
		ts := uint32(payload[4])<<16 | uint32(payload[5])<<8 | uint32(payload[6]) | uint32(payload[7])<<24
		payload = payload[aggregateSubHeaderSize:]

		if uint32(len(payload)) < length {
			return nil, fmt.Errorf("rtmp: aggregate sub-message length %d exceeds remaining %d bytes",
				length, len(payload))
		}
		body := payload[:length]
		payload = payload[length:]

		if n := len(payload); n > 0 {
			if n < aggregateBackPointerSize {
				return nil, fmt.Errorf("rtmp: aggregate truncated back pointer: %d bytes left", n)
			}
			payload = payload[aggregateBackPointerSize:]
		}

		if !haveFirst {
			// 规范要求首个时间戳等于聚合时间戳，此时 offset 为 0；
			// 不满足时保留子消息之间的相对时间。
			offset = outerTimestamp - ts
			haveFirst = true
		}
		subs = append(subs, aggregateSubMessage{
			TypeID:    typeID,
			Timestamp: ts + offset,
			Payload:   body,
		})
	}
	return subs, nil
}

// handleAggregate 拆包并把子消息交给与普通消息相同的处理器。
// 只处理音视频和数据消息；其它类型（包括嵌套聚合）按未知类型忽略。
func (c *Conn) handleAggregate(m *Message) error {
	subs, err := splitAggregate(m.Payload, m.Timestamp)
	if err != nil {
		return err
	}
	for i := range subs {
		sub := &subs[i]
		sm := &Message{
			TypeID:    sub.TypeID,
			StreamID:  m.StreamID,
			Timestamp: sub.Timestamp,
			Payload:   sub.Payload,
		}
		switch sub.TypeID {
		case msgVideo:
			c.handleVideo(sm)
		case msgAudio:
			c.handleAudio(sm)
		case msgDataAMF0:
			c.handleData(sm)
		}
	}
	return nil
}

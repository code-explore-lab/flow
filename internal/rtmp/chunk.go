package rtmp

// RTMP Chunk Stream：把每条消息拆成若干 chunk 传输，同一 CSID 上的 chunk
// 属于同一条逻辑流。本文件实现 basic header（1/2/3 字节）、message header
// （fmt 0/1/2/3）、扩展时间戳（0xFFFFFF），以及 readMessage/writeMessage。
//
// 布局：
//
//	basic header:   fmt(2 位) + csid（1/2/3 字节）
//	message header: fmt 0=11 字节(含 4 字节 LE 的 stream id)、fmt 1=7、
//	                fmt 2=3、fmt 3=0；时间戳字段为 0xFFFFFF 时后面跟 4 字节扩展
//	payload:        按 chunk size 拆分，续片用 fmt 3

import (
	"fmt"
	"io"
)

// Message 一条重组后的完整 RTMP 消息。
type Message struct {
	TypeID    uint8  // 消息类型：20=命令、18=数据，控制消息见 message.go
	StreamID  uint32 // message stream id
	Timestamp uint32 // 毫秒时间戳
	Payload   []byte
}

// readBasicHeader 读 basic header，返回 (csid, fmt)。
func (c *Conn) readBasicHeader() (csid uint32, fmt uint8, err error) {
	b0, err := readByte(c.br)
	if err != nil {
		return 0, 0, err
	}
	fmt = b0 >> 6
	csid = uint32(b0 & 0x3F)
	switch csid {
	case 0: // 2 字节：csid = 64 + b1
		b1, err := readByte(c.br)
		if err != nil {
			return 0, 0, err
		}
		csid = 64 + uint32(b1)
	case 1: // 3 字节：csid = 64 + (b1<<8 | b2)
		b1, err := readByte(c.br)
		if err != nil {
			return 0, 0, err
		}
		b2, err := readByte(c.br)
		if err != nil {
			return 0, 0, err
		}
		csid = 64 + uint32(b1)<<8 + uint32(b2)
	}
	return csid, fmt, nil
}

// readMessage 从 chunk 流里重组出下一条完整消息。
//
// fmt 3 有两种语义：
//   - received > 0：当前消息的续片，继承一切，继续拼数据；
//   - received == 0：上一条消息已完整，用同样的 header 开一条新消息，
//     时间戳推进 timestampDelta。
func (c *Conn) readMessage() (*Message, error) {
	for {
		csid, hfmt, err := c.readBasicHeader()
		if err != nil {
			return nil, err
		}
		st := c.chunkState(csid)

		switch hfmt {
		case 0:
			ts, err := readUint24(c.br)
			if err != nil {
				return nil, err
			}
			ts, err = readExtendedTimestamp(c.br, ts)
			if err != nil {
				return nil, err
			}
			length, err := readUint24(c.br)
			if err != nil {
				return nil, err
			}
			typeID, err := readByte(c.br)
			if err != nil {
				return nil, err
			}
			streamID, err := readUint32LE(c.br)
			if err != nil {
				return nil, err
			}
			st.timestamp = ts
			st.timestampDelta = 0 // fmt 0 没有增量可继承
			st.length = length
			st.typeID = typeID
			st.streamID = streamID
			st.reset()
		case 1:
			delta, err := readUint24(c.br)
			if err != nil {
				return nil, err
			}
			delta, err = readExtendedTimestamp(c.br, delta)
			if err != nil {
				return nil, err
			}
			length, err := readUint24(c.br)
			if err != nil {
				return nil, err
			}
			typeID, err := readByte(c.br)
			if err != nil {
				return nil, err
			}
			st.timestampDelta = delta
			st.timestamp += delta
			st.length = length
			st.typeID = typeID
			st.reset()
		case 2:
			delta, err := readUint24(c.br)
			if err != nil {
				return nil, err
			}
			delta, err = readExtendedTimestamp(c.br, delta)
			if err != nil {
				return nil, err
			}
			st.timestampDelta = delta
			st.timestamp += delta
			st.reset()
		case 3:
			if st.received == 0 {
				// 新消息复用上一个 header。
				if st.length == 0 {
					return nil, fmt.Errorf("rtmp: fmt3 chunk with no prior message on csid %d", csid)
				}
				st.timestamp += st.timestampDelta
				st.reset()
			}
			// received > 0：续片，什么都不改。
		default:
			return nil, fmt.Errorf("rtmp: invalid fmt %d", hfmt)
		}

		// 读这一片数据（最多 chunk size 字节，但不超过消息剩余量）。
		n := c.readChunkSize
		if remain := st.length - st.received; n > remain {
			n = remain
		}
		if n > 0 {
			data := make([]byte, n)
			if _, err := io.ReadFull(c.br, data); err != nil {
				return nil, err
			}
			st.buf = append(st.buf, data...)
			st.received += n
		}
		if st.received == st.length {
			msg := &Message{
				TypeID:    st.typeID,
				StreamID:  st.streamID,
				Timestamp: st.timestamp,
				Payload:   st.buf,
			}
			st.reset()
			return msg, nil
		}
	}
}

// readExtendedTimestamp 时间戳字段为 0xFFFFFF 时读 4 字节扩展时间戳，否则原样返回。
func readExtendedTimestamp(r io.Reader, v24 uint32) (uint32, error) {
	if v24 == 0xFFFFFF {
		return readUint32(r)
	}
	return v24, nil
}

// writeMessage 把一条消息按当前 writeChunkSize 拆成 chunk 写出去。
// 第一个 chunk 用 fmt 0 完整 header，续片用 fmt 3。
func (c *Conn) writeMessage(m *Message, csid uint32) error {
	if len(m.Payload) > 0xFFFFFF {
		return fmt.Errorf("rtmp: message too large: %d bytes", len(m.Payload))
	}

	buf := make([]byte, 0, 16+len(m.Payload))
	buf = append(buf, encodeBasicHeader(0, csid)...)
	buf = appendMessageHeaderFmt0(buf, m.Timestamp, uint32(len(m.Payload)), m.TypeID, m.StreamID)

	payload := m.Payload
	chunkSize := c.writeChunkSize
	if chunkSize == 0 {
		chunkSize = defaultChunkSize
	}

	// 第一片。
	n := min(chunkSize, uint32(len(payload)))
	buf = append(buf, payload[:n]...)
	if err := c.write(buf); err != nil {
		return err
	}
	payload = payload[n:]

	// fmt 3 续片。
	for len(payload) > 0 {
		n := min(chunkSize, uint32(len(payload)))
		chunk := make([]byte, 0, len(encodeBasicHeader(3, csid))+int(n))
		chunk = append(chunk, encodeBasicHeader(3, csid)...)
		chunk = append(chunk, payload[:n]...)
		if err := c.write(chunk); err != nil {
			return err
		}
		payload = payload[n:]
	}
	return nil
}

// encodeBasicHeader 编码 basic header。
func encodeBasicHeader(fmt uint8, csid uint32) []byte {
	switch {
	case csid >= 2 && csid <= 63:
		return []byte{fmt<<6 | byte(csid)}
	case csid >= 64 && csid <= 319:
		return []byte{fmt << 6, byte(csid - 64)}
	default: // csid >= 320
		v := csid - 64
		return []byte{fmt<<6 | 1, byte(v >> 8), byte(v & 0xFF)}
	}
}

// appendMessageHeaderFmt0 追加 fmt 0 的 11 字节 header（含可选的扩展时间戳）。
// 扩展时间戳紧跟 3 字节时间戳字段之后、message length 之前（与 readMessage 对应）。
func appendMessageHeaderFmt0(buf []byte, ts, length uint32, typeID uint8, streamID uint32) []byte {
	if ts >= 0xFFFFFF {
		buf = appendUint24(buf, 0xFFFFFF)
		buf = appendUint32(buf, ts) // 扩展时间戳紧跟时间戳字段
	} else {
		buf = appendUint24(buf, ts)
	}
	buf = appendUint24(buf, length)
	buf = append(buf, typeID)
	buf = appendUint32LE(buf, streamID)
	return buf
}

// readByte 读 1 字节。
func readByte(r io.Reader) (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return b[0], nil
}

// readUint24 读 3 字节大端。
func readUint24(r io.Reader) (uint32, error) {
	var b [3]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2]), nil
}

// readUint32 读 4 字节大端。
func readUint32(r io.Reader) (uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3]), nil
}

// readUint32LE 读 4 字节小端（message stream id）。
func readUint32LE(r io.Reader) (uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24, nil
}

// appendUint24 追加 3 字节大端。
func appendUint24(b []byte, v uint32) []byte {
	return append(b, byte(v>>16), byte(v>>8), byte(v))
}

// appendUint32 追加 4 字节大端。
func appendUint32(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// appendUint32LE 追加 4 字节小端。
func appendUint32LE(b []byte, v uint32) []byte {
	return append(b, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

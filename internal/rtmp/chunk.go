package rtmp

// RTMP Chunk Stream：把每条消息拆成若干 chunk 传输，同一 CSID 上的 chunk
// 属于同一条逻辑流。本文件实现 basic header（1/2/3 字节）、message header
// （fmt 0/1/2/3）、扩展时间戳（0xFFFFFF），以及 readMessage/writeMessage。
//
// 布局：
//
//	basic header:   fmt(2 位) + csid（1/2/3 字节）
//	message header: fmt 0=11 字节(含 4 字节 LE 的 stream id)、fmt 1=7、
//	                fmt 2=3、fmt 3=0
//	ext timestamp:  时间戳字段为 0xFFFFFF 时，在完整 message header 之后再跟
//	                4 字节大端扩展时间戳；该消息的每个 fmt 3 chunk 也重复这
//	                4 字节（ffmpeg 按此布局解析）
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
			ts, ext, err := readUint24Ext(c.br)
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
			if ext {
				if ts, err = readUint32(c.br); err != nil {
					return nil, err
				}
			}
			st.timestamp = ts
			st.timestampDelta = 0 // fmt 0 没有增量可继承
			st.length = length
			st.typeID = typeID
			st.streamID = streamID
			st.extTimestamp = ext
			st.begin(st.length)
		case 1:
			delta, ext, err := readUint24Ext(c.br)
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
			if ext {
				if delta, err = readUint32(c.br); err != nil {
					return nil, err
				}
			}
			st.timestampDelta = delta
			st.timestamp += delta
			st.length = length
			st.typeID = typeID
			st.extTimestamp = ext
			st.begin(st.length)
		case 2:
			delta, ext, err := readUint24Ext(c.br)
			if err != nil {
				return nil, err
			}
			if ext {
				if delta, err = readUint32(c.br); err != nil {
					return nil, err
				}
			}
			st.timestampDelta = delta
			st.timestamp += delta
			st.extTimestamp = ext
			st.begin(st.length)
		case 3:
			if st.extTimestamp {
				// 该消息用扩展时间戳时，每个 fmt 3 chunk 都重复 4 字节；
				// 续片沿用已记录的时间戳，值本身可以忽略。
				if _, err := readUint32(c.br); err != nil {
					return nil, err
				}
			}
			if st.received == 0 {
				// 新消息复用上一个 header。
				if st.length == 0 {
					return nil, fmt.Errorf("rtmp: fmt3 chunk with no prior message on csid %d", csid)
				}
				st.timestamp += st.timestampDelta
				st.begin(st.length)
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
			// 直接读进预分配缓冲的尾部。begin 已按 length 备好空间；
			// 只有 Abort 后从 fmt 3 续片等异常路径才需要补足。
			end := st.received + n
			if int(end) > len(st.buf) {
				st.buf = append(st.buf, make([]byte, int(end)-len(st.buf))...)
			}
			if _, err := io.ReadFull(c.br, st.buf[st.received:end]); err != nil {
				return nil, err
			}
			st.received = end
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

// readUint24Ext 读 3 字节时间戳字段，并报告是否为 0xFFFFFF（扩展时间戳标记）。
func readUint24Ext(r io.ByteReader) (uint32, bool, error) {
	v, err := readUint24(r)
	if err != nil {
		return 0, false, err
	}
	return v, v == 0xFFFFFF, nil
}

// writeMessage 把一条消息按当前 writeChunkSize 拆成 chunk 写出去。
// 第一个 chunk 用 fmt 0 完整 header，续片用 fmt 3。
//
// T-7：整条消息（全部 chunk）先拼进一个缓冲区，最后一次 write 写出。
// 这样每条消息只有一次加锁/系统调用；也避免同一消息的续片之间被其它
// goroutine 的写插入，导致接收端把 fmt 3 续片误判成另一条消息。
func (c *Conn) writeMessage(m *Message, csid uint32) error {
	if len(m.Payload) > 0xFFFFFF {
		return fmt.Errorf("rtmp: message too large: %d bytes", len(m.Payload))
	}

	chunkSize := c.writeChunkSize
	if chunkSize == 0 {
		chunkSize = defaultChunkSize
	}

	ext := m.Timestamp >= 0xFFFFFF
	basic := encodeBasicHeader(0, csid)
	cont := encodeBasicHeader(3, csid)

	// 预分配：fmt0 header + 扩展时间戳 + 负载 + 每个续片的 basic header
	// （以及用扩展时间戳时每片重复的 4 字节）。
	continuations := 0
	if len(m.Payload) > 0 {
		continuations = (len(m.Payload) - 1) / int(chunkSize)
	}
	capHint := len(basic) + 11 + len(m.Payload) + continuations*len(cont)
	if ext {
		capHint += 4 * (continuations + 1)
	}
	buf := make([]byte, 0, capHint)
	buf = append(buf, basic...)
	buf = appendMessageHeaderFmt0(buf, m.Timestamp, uint32(len(m.Payload)), m.TypeID, m.StreamID)

	for off := 0; off < len(m.Payload); {
		if off > 0 {
			buf = append(buf, cont...)
			if ext {
				// 消息用扩展时间戳时，每个续片也要重复 4 字节。
				buf = appendUint32(buf, m.Timestamp)
			}
		}
		n := min(len(m.Payload)-off, int(chunkSize))
		buf = append(buf, m.Payload[off:off+n]...)
		off += n
	}
	return c.write(buf)
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

// appendMessageHeaderFmt0 追加 fmt 0 的 header。
// 时间戳 >= 0xFFFFFF 时，时间戳字段写 0xFFFFFF，并在完整的 11 字节 header
// （时间戳/length/type/stream id）之后再追加 4 字节扩展时间戳 —— 这是
// ffmpeg 的布局；放在时间戳字段后面会导致对端 packet 边界错位。
func appendMessageHeaderFmt0(buf []byte, ts, length uint32, typeID uint8, streamID uint32) []byte {
	ext := ts >= 0xFFFFFF
	if ext {
		buf = appendUint24(buf, 0xFFFFFF)
	} else {
		buf = appendUint24(buf, ts)
	}
	buf = appendUint24(buf, length)
	buf = append(buf, typeID)
	buf = appendUint32LE(buf, streamID)
	if ext {
		buf = appendUint32(buf, ts)
	}
	return buf
}

// 以下读取函数走 io.ByteReader（bufio.Reader.ReadByte），逐字节读。
// 不用 io.ReadFull(r, b[:])：数组经 io.Reader 接口传递会让每个 header
// 字段堆分配一次，4096 chunk 下每帧多出几十次小分配（T-7 基准可见）。

// readByte 读 1 字节。
func readByte(r io.ByteReader) (byte, error) {
	return r.ReadByte()
}

// readUint24 读 3 字节大端。
func readUint24(r io.ByteReader) (uint32, error) {
	b0, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	b1, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	b2, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	return uint32(b0)<<16 | uint32(b1)<<8 | uint32(b2), nil
}

// readUint32 读 4 字节大端。
func readUint32(r io.ByteReader) (uint32, error) {
	b0, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	b1, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	b2, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	b3, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	return uint32(b0)<<24 | uint32(b1)<<16 | uint32(b2)<<8 | uint32(b3), nil
}

// readUint32LE 读 4 字节小端（message stream id）。
func readUint32LE(r io.ByteReader) (uint32, error) {
	b0, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	b1, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	b2, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	b3, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	return uint32(b0) | uint32(b1)<<8 | uint32(b2)<<16 | uint32(b3)<<24, nil
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

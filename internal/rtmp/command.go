package rtmp

// RTMP 命令层：AMF0 命令消息的解析与应答。
//
// 命令消息的负载是一串 AMF0 值：
//
//	命令名(String) + 事务号(Number) + 命令对象(Object/Null) + 参数...
//
// 推流客户端（ffmpeg）的典型时序：
//
//	connect        → 服务端回 Window Ack Size / Set Peer Bandwidth /
//	                 Set Chunk Size + _result(NetConnection.Connect.Success)
//	releaseStream  → _result
//	FCPublish      → onFCPublish
//	createStream   → _result(stream id)
//	publish        → onStatus(NetStream.Publish.Start)，进入 ingest
//
// 拉流客户端：connect → createStream → play → onStatus(Play.Reset/Play.Start)
// → 序列头 + 逐帧 FLV tag（见 play.go）。

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"flow/internal/flv"
	"flow/internal/stream"
)

// 各消息类型使用的 chunk stream id（与主流实现一致：命令 3、音频 4、
// 数据 5、视频 6；协议控制消息固定 2）。
const (
	commandCSID uint32 = 3
	audioCSID   uint32 = 4
	dataCSID    uint32 = 5
	videoCSID   uint32 = 6
)

// connect 后下发给客户端的 chunk size。
const connectChunkSize uint32 = 4096

// Command 一条解析后的命令消息。
type Command struct {
	Name          string
	TransactionID float64
	Args          []flv.Value // 命令对象 + 参数，顺序与线上一致
}

// parseCommand 解析命令消息负载。
func parseCommand(payload []byte) (*Command, error) {
	r := bytes.NewReader(payload)

	v, err := flv.ReadAMF0(r)
	if err != nil {
		return nil, fmt.Errorf("rtmp: parse command name: %w", err)
	}
	if v.Type != flv.TypeString || v.String == "" {
		return nil, errors.New("rtmp: command name is not a string")
	}
	cmd := &Command{Name: v.String}

	v, err = flv.ReadAMF0(r)
	if err != nil {
		return nil, fmt.Errorf("rtmp: parse command transaction id: %w", err)
	}
	if v.Type != flv.TypeNumber {
		return nil, errors.New("rtmp: command transaction id is not a number")
	}
	cmd.TransactionID = v.Number

	for r.Len() > 0 {
		v, err := flv.ReadAMF0(r)
		if err != nil {
			return nil, fmt.Errorf("rtmp: parse command argument: %w", err)
		}
		if v.Type == flv.TypeUnsupported {
			continue
		}
		cmd.Args = append(cmd.Args, v)
	}
	return cmd, nil
}

// stringArg 返回第 i 个字符串参数（跳过 Null/Object 等非字符串参数）。
func (c *Command) stringArg(i int) string {
	n := 0
	for _, a := range c.Args {
		if a.Type != flv.TypeString {
			continue
		}
		if n == i {
			return a.String
		}
		n++
	}
	return ""
}

// objectString 取命令对象（Args[0]）里的字符串字段。
func (c *Command) objectString(key string) (string, bool) {
	if len(c.Args) == 0 {
		return "", false
	}
	obj := c.Args[0]
	if obj.Type != flv.TypeObject && obj.Type != flv.TypeECMAArray {
		return "", false
	}
	for _, m := range obj.Object {
		if m.Key == key && m.Value.Type == flv.TypeString {
			return m.Value.String, true
		}
	}
	return "", false
}

// buildCommand 序列化一条命令消息的负载。
func buildCommand(name string, txID float64, args ...flv.Value) []byte {
	var buf bytes.Buffer
	_ = flv.WriteAMF0(&buf, flv.Str(name))
	_ = flv.WriteAMF0(&buf, flv.Num(txID))
	for _, a := range args {
		_ = flv.WriteAMF0(&buf, a)
	}
	return buf.Bytes()
}

// statusObj 构造 onStatus 的信息对象。
func statusObj(level, code, description string) flv.Value {
	return flv.Obj(
		flv.Elem("level", flv.Str(level)),
		flv.Elem("code", flv.Str(code)),
		flv.Elem("description", flv.Str(description)),
	)
}

// writeCommand 发送一条 AMF0 命令消息。
func (c *Conn) writeCommand(streamID uint32, name string, txID float64, args ...flv.Value) error {
	return c.writeMessage(&Message{
		TypeID:   msgCommandAMF0,
		StreamID: streamID,
		Payload:  buildCommand(name, txID, args...),
	}, commandCSID)
}

// handleCommand 分发一条命令消息。
func (c *Conn) handleCommand(m *Message) error {
	cmd, err := parseCommand(m.Payload)
	if err != nil {
		return err
	}

	switch cmd.Name {
	case "connect":
		return c.handleConnect(cmd)
	case "releaseStream":
		return c.writeCommand(m.StreamID, "_result", cmd.TransactionID, flv.Null(), flv.Null())
	case "FCPublish":
		return c.writeCommand(m.StreamID, "onFCPublish", 0, flv.Null(),
			statusObj("status", "NetStream.Publish.Start", "FCPublish"))
	case "createStream":
		return c.handleCreateStream(cmd)
	case "publish":
		return c.handlePublish(m.StreamID, cmd)
	case "deleteStream", "FCUnpublish", "closeStream":
		c.stopPublish()
		c.stopPlay()
		return nil
	case "play":
		return c.handlePlay(m.StreamID, cmd)
	}
	// 未知命令（如 onBWDone 之类的通知）直接忽略。
	return nil
}

// handleConnect 处理 connect：下发流控参数并回 NetConnection.Connect.Success。
func (c *Conn) handleConnect(cmd *Command) error {
	if app, ok := cmd.objectString("app"); ok {
		c.app = app
	}

	if err := c.writeWindowAckSize(ackWindow); err != nil {
		return err
	}
	if err := c.writeSetPeerBandwidth(ackWindow, bandwidthDynamic); err != nil {
		return err
	}
	if err := c.writeSetChunkSize(connectChunkSize); err != nil {
		return err
	}

	props := flv.Obj(
		flv.Elem("fmsVer", flv.Str("FMS/3,0,1,123")),
		flv.Elem("capabilities", flv.Num(31)),
	)
	info := flv.Obj(
		flv.Elem("level", flv.Str("status")),
		flv.Elem("code", flv.Str("NetConnection.Connect.Success")),
		flv.Elem("description", flv.Str("Connection succeeded.")),
		flv.Elem("objectEncoding", flv.Num(0)),
	)
	fmt.Printf("rtmp: connect app=%q from %s\n", c.app, c.RemoteAddr())
	return c.writeCommand(0, "_result", cmd.TransactionID, props, info)
}

// handleCreateStream 分配一个 message stream id（从 1 开始）并回 _result。
func (c *Conn) handleCreateStream(cmd *Command) error {
	c.nextStreamID++
	if c.nextStreamID == 0 {
		c.nextStreamID = 1
	}
	return c.writeCommand(0, "_result", cmd.TransactionID,
		flv.Null(), flv.Num(float64(c.nextStreamID)))
}

// handlePublish 处理 publish：登记流并回 NetStream.Publish.Start。
func (c *Conn) handlePublish(streamID uint32, cmd *Command) error {
	name := sanitizeStreamName(cmd.stringArg(0))
	if name == "" {
		return c.writeCommand(streamID, "onStatus", 0, flv.Null(),
			statusObj("error", "NetStream.Publish.BadName", "empty stream name"))
	}
	if c.srv == nil {
		return errors.New("rtmp: publish without server")
	}

	// 同一个名字重复 publish 视为重新发布，Registry 会替换并关闭旧流。
	st := c.srv.registry.Publish(name, &stream.Stream{})
	c.publish = &publishState{stream: st, audioRate: defaultAACRate}
	c.streamName = name

	fmt.Printf("rtmp: publish %q (app %q) from %s\n", name, c.app, c.RemoteAddr())
	return c.writeCommand(streamID, "onStatus", 0, flv.Null(),
		statusObj("status", "NetStream.Publish.Start", "Start publishing"))
}

// sanitizeStreamName 规整 publish 的流名：去掉查询串和首尾斜杠。
func sanitizeStreamName(name string) string {
	name = strings.TrimSpace(name)
	if i := strings.IndexByte(name, '?'); i >= 0 {
		name = name[:i]
	}
	return strings.Trim(name, "/")
}

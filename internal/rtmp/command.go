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

// errAuthRejected 表示连接因鉴权失败被拒绝：错误消息已经写回客户端，
// 读循环收到后只需关闭连接。
var errAuthRejected = errors.New("rtmp: connection rejected")

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

// boolArg 返回第 i 个布尔参数（跳过 Null/Object 等非布尔参数）。
// pause 命令的参数布局是（命令对象, 暂停标志, 毫秒位置）。
// 过滤掉所有不是布尔类型的参数，只看布尔值，取出里面第 i 个布尔；找不到就返回 false。
func (c *Command) boolArg(i int) bool {
	n := 0
	for _, a := range c.Args {
		if a.Type != flv.TypeBoolean {
			continue
		}
		if n == i {
			return a.Boolean
		}
		n++
	}
	return false
}

// numberArg 返回第 i 个数字参数（跳过 Null/Object 等非数字参数）。
// pause 的毫秒位置和 seek 的目标时间都用它取。
func (c *Command) numberArg(i int) (float64, bool) {
	n := 0
	for _, a := range c.Args {
		if a.Type != flv.TypeNumber {
			continue
		}
		if n == i {
			return a.Number, true
		}
		n++
	}
	return 0, false
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

// objectStringArray 取命令对象（Args[0]）里的字符串数组字段，如 fourCcList。
// 字段不存在或不是 strict array 时返回 ok=false。
func (c *Command) objectStringArray(key string) ([]string, bool) {
	if len(c.Args) == 0 {
		return nil, false
	}
	obj := c.Args[0]
	if obj.Type != flv.TypeObject && obj.Type != flv.TypeECMAArray {
		return nil, false
	}
	for _, m := range obj.Object {
		if m.Key != key || m.Value.Type != flv.TypeStrictArray {
			continue
		}
		out := make([]string, 0, len(m.Value.Array))
		for _, e := range m.Value.Array {
			if e.Type == flv.TypeString {
				out = append(out, e.String)
			}
		}
		return out, true
	}
	return nil, false
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
	case "pause":
		return c.handlePause(m.StreamID, cmd)
	case "seek":
		return c.handleSeek(m.StreamID, cmd)
	}
	// 未知命令（如 onBWDone 之类的通知）直接忽略。
	return nil
}

// handleConnect 处理 connect：下发流控参数并回 NetConnection.Connect.Success。
func (c *Conn) handleConnect(cmd *Command) error {
	if app, ok := cmd.objectString("app"); ok {
		c.app = app
	}
	if list, ok := cmd.objectStringArray("fourCcList"); ok {
		c.fourCCs = list
	}

	// T-3：app 白名单。拒绝时回 NetConnection.Connect.Rejected 并断开。
	if c.srv != nil && !c.srv.auth.AppAllowed(c.app) {
		fmt.Printf("rtmp: reject app=%q from %s: app not allowed\n", appBase(c.app), c.RemoteAddr())
		return c.rejectConnection(cmd.TransactionID, "Application not allowed.")
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
	fmt.Printf("rtmp: connect app=%q from %s\n", appBase(c.app), c.RemoteAddr())
	return c.writeCommand(0, "_result", cmd.TransactionID, props, info)
}

// rejectConnection 回 NetConnection.Connect.Rejected 并返回 errAuthRejected，
// 让读循环在错误消息写完后关闭连接。
func (c *Conn) rejectConnection(txID float64, description string) error {
	if err := c.writeCommand(0, "_error", txID, flv.Null(),
		statusObj("error", "NetConnection.Connect.Rejected", description)); err != nil {
		return err
	}
	return errAuthRejected
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
	rawName := cmd.stringArg(0)
	// T-3：token 在查询串里，必须在 sanitizeStreamName 剥掉之前校验。
	// app 白名单在这里再查一次，防止客户端跳过 connect 直接 publish。
	if c.srv != nil && !c.srv.auth.AppAllowed(c.app) {
		fmt.Printf("rtmp: reject publish (app %q) from %s: app not allowed\n",
			appBase(c.app), c.RemoteAddr())
		return c.rejectConnection(cmd.TransactionID, "Application not allowed.")
	}
	if c.srv != nil && !c.srv.auth.Allow(rawName, c.app, permPublish) {
		fmt.Printf("rtmp: reject publish (app %q) from %s: invalid or missing token\n",
			appBase(c.app), c.RemoteAddr())
		return c.rejectConnection(cmd.TransactionID, "Publish not authorized.")
	}
	name := sanitizeStreamName(rawName)
	if name == "" {
		return c.writeCommand(streamID, "onStatus", 0, flv.Null(),
			statusObj("error", "NetStream.Publish.BadName", "empty stream name"))
	}
	if c.srv == nil {
		return errors.New("rtmp: publish without server")
	}

	// 同一条连接上重复 publish：先注销上一次推流，避免旧流残留在
	// Registry、也保证发布计数不会只增不减。
	c.stopPublish()

	// 同一个名字重复 publish 视为重新发布，Registry 会替换并关闭旧流。
	st := c.srv.registry.Publish(name, &stream.Stream{})
	c.publish = &publishState{stream: st, audioRate: defaultAACRate}
	c.streamName = name
	c.srv.metrics.IncPublish()
	c.srv.metrics.IncPublishTotal()

	fmt.Printf("rtmp: publish %q (app %q) from %s\n", name, appBase(c.app), c.RemoteAddr())
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

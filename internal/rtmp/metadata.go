package rtmp

// onMetaData 处理：推流端保存、拉流端转发。
//
// ffmpeg 推流时在 publish 后、音视频序列头之前发一条 type 18 数据消息
// （msgDataAMF0），负载是 AMF0 的 String("onMetaData") + ECMA Array/Object，
// 里面带 duration、videocodecid、audiocodecid、width、height、framerate、
// audiosamplerate、stereo 等描述信息。ffprobe/ffplay 依赖它读出时长/编码/
// 分辨率，并在缺失时打印告警。
//
// 处理策略：推流端只认 onMetaData，其它 type 18 消息（|RtmpSampleAccess、
// onTextData 等）忽略；拉流端在 Play.Start 之后、音视频序列头之前原样转发。
// 保存原始负载副本而非重建，保证字节零损耗。

import (
	"bytes"
	"errors"
	"fmt"

	"flow/internal/flv"
)

// metaData 一路流推流端发来的 onMetaData。
type metaData struct {
	payload []byte       // onMetaData 消息的原始负载副本，转发时零重建
	fields  []flv.Member // 解析后的键值对（日志/测试用）
}

// parseMetaData 解析一条数据消息负载：首值应是 String 命令名，其后是可选的
// Object/ECMA Array。命令名不是 onMetaData、或负载没有对象体时返回空字段、
// 不报错，由调用方决定是否忽略。
func parseMetaData(payload []byte) (name string, fields []flv.Member, err error) {
	r := bytes.NewReader(payload)
	v, err := flv.ReadAMF0(r)
	if err != nil {
		return "", nil, fmt.Errorf("rtmp: read metadata name: %w", err)
	}
	if v.Type != flv.TypeString {
		return "", nil, errors.New("rtmp: metadata name is not a string")
	}
	name = v.String
	if r.Len() == 0 {
		return name, nil, nil
	}
	v, err = flv.ReadAMF0(r)
	if err != nil {
		return "", nil, fmt.Errorf("rtmp: read metadata body: %w", err)
	}
	if v.Type == flv.TypeObject || v.Type == flv.TypeECMAArray {
		return name, v.Object, nil
	}
	// 布尔/Null 等非对象体（如 |RtmpSampleAccess）按无字段处理。
	return name, nil, nil
}

// handleData 处理一条 type 18 数据消息。只在推流侧有意义：把 onMetaData
// 存进共享流；拉流侧收到数据消息直接忽略（播放端不消费它）。
func (c *Conn) handleData(m *Message) {
	p := c.publish
	if p == nil {
		return
	}
	name, fields, err := parseMetaData(m.Payload)
	if err != nil {
		fmt.Printf("rtmp: bad data message from %s: %v\n", c.RemoteAddr(), err)
		return
	}
	if name != "onMetaData" {
		return
	}
	p.stream.SetMetaData(&metaData{
		payload: append([]byte(nil), m.Payload...),
		fields:  fields,
	})
}

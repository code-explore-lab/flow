package flv

// AMF0 编解码（Adobe 的 Action Message Format 0）。
//
// RTMP 的命令消息用 AMF0 序列化参数：connect/publish/play 等都是
// 「命令名(String) + 事务号(Number) + 若干参数(Object/Null/...）」。
// 本文件只覆盖 RTMP 用得到的类型，其余按类型跳过以保持字节流同步。
//
// 布局（大端）：
//
//	Number       0x00 + 8 字节 IEEE-754 double
//	Boolean      0x01 + 1 字节
//	String       0x02 + 2 字节长度 + UTF-8
//	Object       0x03 + 若干 key/value，空 key(0x00 0x00)+0x09 结尾
//	Null         0x05
//	ECMA Array   0x08 + 4 字节数量 + 同 Object 的 key/value，结尾同上
//	Strict Array 0x0A + 4 字节数量 + 连续 value（无 key）
//	Long String  0x0C + 4 字节长度 + UTF-8

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

// AMF0 类型标记。
const (
	TypeNumber      byte = 0x00
	TypeBoolean     byte = 0x01
	TypeString      byte = 0x02
	TypeObject      byte = 0x03
	TypeMovieClip   byte = 0x04
	TypeNull        byte = 0x05
	TypeUndefined   byte = 0x06
	TypeReference   byte = 0x07
	TypeECMAArray   byte = 0x08
	TypeObjectEnd   byte = 0x09
	TypeStrictArray byte = 0x0A
	TypeDate        byte = 0x0B
	TypeLongString  byte = 0x0C
	TypeUnsupported byte = 0x0D
	TypeRecordset   byte = 0x0E
	TypeXML         byte = 0x0F
	TypeTypedObject byte = 0x10
	TypeAMF3        byte = 0x11
)

// Value 一个 AMF0 值。Type 决定用哪个字段：
//
//	Number       → Number
//	Boolean      → Boolean
//	String       → String
//	Object       → Object
//	Null/Undefined → 无字段
//	ECMAArray    → Object（数量前缀读写时自动处理）
//	StrictArray  → Array
type Value struct {
	Type    byte
	Number  float64
	Boolean bool
	String  string
	Object  []Member
	Array   []Value
}

// Member 对象/ECMA 数组里的一个有序键值对。
type Member struct {
	Key   string
	Value Value
}

// 构造便捷值，避免手写 Value 结构。

// Num 构造一个 Number。
func Num(n float64) Value { return Value{Type: TypeNumber, Number: n} }

// Bool 构造一个 Boolean。
func Bool(b bool) Value { return Value{Type: TypeBoolean, Boolean: b} }

// Str 构造一个 String。
func Str(s string) Value { return Value{Type: TypeString, String: s} }

// Null 构造一个 Null。
func Null() Value { return Value{Type: TypeNull} }

// Obj 构造一个 Object。
func Obj(members ...Member) Value { return Value{Type: TypeObject, Object: members} }

// ECMA 构造一个 ECMA Array。
func ECMA(members ...Member) Value { return Value{Type: TypeECMAArray, Object: members} }

// Arr 构造一个 Strict Array。
func Arr(elems ...Value) Value { return Value{Type: TypeStrictArray, Array: elems} }

// Elem 构造一个键值对，供 Obj/ECMA 使用。
func Elem(key string, v Value) Member { return Member{Key: key, Value: v} }

// WriteAMF0 写一个 AMF0 值。
func WriteAMF0(w io.Writer, v Value) error {
	return writeAMF0Value(w, v)
}

func writeAMF0Value(w io.Writer, v Value) error {
	switch v.Type {
	case TypeNumber:
		if err := writeU8(w, TypeNumber); err != nil {
			return err
		}
		return writeU64(w, math.Float64bits(v.Number))
	case TypeBoolean:
		b := byte(0)
		if v.Boolean {
			b = 1
		}
		return writeU8(w, TypeBoolean, b)
	case TypeString:
		if len(v.String) > 0xFFFF {
			// 短字符串放不下，退化成 Long String。
			if err := writeU8(w, TypeLongString); err != nil {
				return err
			}
			if err := writeU32(w, uint32(len(v.String))); err != nil {
				return err
			}
			_, err := io.WriteString(w, v.String)
			return err
		}
		if err := writeU8(w, TypeString, byte(len(v.String)>>8), byte(len(v.String))); err != nil {
			return err
		}
		_, err := io.WriteString(w, v.String)
		return err
	case TypeObject:
		if err := writeU8(w, TypeObject); err != nil {
			return err
		}
		return writeMembers(w, v.Object)
	case TypeECMAArray:
		if err := writeU8(w, TypeECMAArray); err != nil {
			return err
		}
		if err := writeU32(w, uint32(len(v.Object))); err != nil {
			return err
		}
		return writeMembers(w, v.Object)
	case TypeStrictArray:
		if err := writeU8(w, TypeStrictArray); err != nil {
			return err
		}
		if err := writeU32(w, uint32(len(v.Array))); err != nil {
			return err
		}
		for _, e := range v.Array {
			if err := writeAMF0Value(w, e); err != nil {
				return err
			}
		}
		return nil
	case TypeNull, TypeUndefined:
		return writeU8(w, v.Type)
	default:
		return fmt.Errorf("amf0: unsupported value type %#x", v.Type)
	}
}

// writeMembers 写 key/value 序列和结尾的空 key + 0x09。
func writeMembers(w io.Writer, members []Member) error {
	for _, m := range members {
		if err := writeU8(w, byte(len(m.Key)>>8), byte(len(m.Key))); err != nil {
			return err
		}
		if _, err := io.WriteString(w, m.Key); err != nil {
			return err
		}
		if err := writeAMF0Value(w, m.Value); err != nil {
			return err
		}
	}
	// 空 key + ObjectEnd 标记。
	return writeU8(w, 0, 0, TypeObjectEnd)
}

// ReadAMF0 读一个 AMF0 值。
func ReadAMF0(r io.Reader) (Value, error) {
	typ, err := readU8(r)
	if err != nil {
		return Value{}, err
	}
	return readAMF0Value(r, typ)
}

func readAMF0Value(r io.Reader, typ byte) (Value, error) {
	v := Value{Type: typ}
	switch typ {
	case TypeNumber:
		bits, err := readU64(r)
		if err != nil {
			return Value{}, err
		}
		v.Number = math.Float64frombits(bits)
	case TypeBoolean:
		b, err := readU8(r)
		if err != nil {
			return Value{}, err
		}
		v.Boolean = b != 0
	case TypeString:
		s, err := readStringN(r, readU16)
		if err != nil {
			return Value{}, err
		}
		v.String = s
	case TypeLongString:
		s, err := readStringN(r, readU32)
		if err != nil {
			return Value{}, err
		}
		v.String = s
		v.Type = TypeString // 归一化成 String，回写时按长度再选短/长
	case TypeObject:
		members, err := readMembers(r)
		if err != nil {
			return Value{}, err
		}
		v.Object = members
	case TypeECMAArray:
		// 数量前缀只是提示，真实条目数以结尾标记为准。
		if _, err := readU32(r); err != nil {
			return Value{}, err
		}
		members, err := readMembers(r)
		if err != nil {
			return Value{}, err
		}
		v.Object = members
	case TypeStrictArray:
		n, err := readU32(r)
		if err != nil {
			return Value{}, err
		}
		v.Array = make([]Value, 0, n)
		for range n {
			e, err := ReadAMF0(r)
			if err != nil {
				return Value{}, err
			}
			v.Array = append(v.Array, e)
		}
	case TypeNull, TypeUndefined:
		// 无负载
	default:
		// 不认识的类型跳过负载，保持后续字节对齐。
		if err := skipAMF0Value(r, typ); err != nil {
			return Value{}, err
		}
		v.Type = TypeUnsupported
	}
	return v, nil
}

// readMembers 读 key/value 序列，遇到空 key + 0x09 结束。
func readMembers(r io.Reader) ([]Member, error) {
	var members []Member
	for {
		key, err := readStringN(r, readU16)
		if err != nil {
			return nil, err
		}
		if key == "" {
			end, err := readU8(r)
			if err != nil {
				return nil, err
			}
			if end != TypeObjectEnd {
				return nil, fmt.Errorf("amf0: expected object end marker, got %#x", end)
			}
			return members, nil
		}
		val, err := ReadAMF0(r)
		if err != nil {
			return nil, err
		}
		members = append(members, Member{Key: key, Value: val})
	}
}

// skipAMF0Value 跳过不支持的 AMF0 值的负载，保持同步。
func skipAMF0Value(r io.Reader, typ byte) error {
	switch typ {
	case TypeMovieClip, TypeUndefined, TypeUnsupported, TypeRecordset, TypeObjectEnd:
		return nil
	case TypeReference:
		_, err := readU16(r)
		return err
	case TypeDate:
		var buf [10]byte
		_, err := io.ReadFull(r, buf[:])
		return err
	case TypeXML:
		_, err := readStringN(r, readU32)
		return err
	case TypeTypedObject:
		if _, err := readStringN(r, readU16); err != nil { // 类名
			return err
		}
		_, err := readMembers(r)
		return err
	case TypeAMF3:
		return errors.New("amf0: AMF3 value cannot be skipped")
	default:
		return fmt.Errorf("amf0: unknown type %#x", typ)
	}
}

// readStringN 读「长度 + 内容」的字符串，长度由 len 函数读取。
func readStringN(r io.Reader, readLen func(io.Reader) (uint32, error)) (string, error) {
	n, err := readLen(r)
	if err != nil {
		return "", err
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func readU8(r io.Reader) (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return b[0], nil
}

func readU16(r io.Reader) (uint32, error) {
	var b [2]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return uint32(binary.BigEndian.Uint16(b[:])), nil
}

func readU32(r io.Reader) (uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b[:]), nil
}

func readU64(r io.Reader) (uint64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b[:]), nil
}

func writeU8(w io.Writer, bytes ...byte) error {
	_, err := w.Write(bytes)
	return err
}

func writeU16(w io.Writer, v uint16) error {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func writeU32(w io.Writer, v uint32) error {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func writeU64(w io.Writer, v uint64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	_, err := w.Write(b[:])
	return err
}

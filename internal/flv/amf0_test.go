package flv

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

// roundTrip 写一遍读一遍，返回读到的值。
func roundTrip(t *testing.T, v Value) Value {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteAMF0(&buf, v); err != nil {
		t.Fatalf("WriteAMF0(%+v): %v", v, err)
	}
	got, err := ReadAMF0(&buf)
	if err != nil {
		t.Fatalf("ReadAMF0(%+v): %v", v, err)
	}
	if buf.Len() != 0 {
		t.Fatalf("leftover bytes after reading %+v: %d", v, buf.Len())
	}
	return got
}

func TestAMF0Scalars(t *testing.T) {
	// Number
	got := roundTrip(t, Num(3.5))
	if got.Type != TypeNumber || got.Number != 3.5 {
		t.Fatalf("number = %+v", got)
	}

	// Boolean
	for _, b := range []bool{true, false} {
		got := roundTrip(t, Bool(b))
		if got.Type != TypeBoolean || got.Boolean != b {
			t.Fatalf("bool(%v) = %+v", b, got)
		}
	}

	// String
	got = roundTrip(t, Str("connect"))
	if got.Type != TypeString || got.String != "connect" {
		t.Fatalf("string = %+v", got)
	}

	// 空串（注意：作为值可含空串，对象里的空 key 才是结尾标记）。
	got = roundTrip(t, Str(""))
	if got.Type != TypeString || got.String != "" {
		t.Fatalf("empty string = %+v", got)
	}

	// Null / Undefined
	for _, v := range []Value{Null(), {Type: TypeUndefined}} {
		got := roundTrip(t, v)
		if got.Type != v.Type {
			t.Fatalf("null/undefined = %+v, want type %#x", got, v.Type)
		}
	}
}

func TestAMF0ObjectNested(t *testing.T) {
	v := Obj(
		Elem("app", Str("live")),
		Elem("tcUrl", Str("rtmp://127.0.0.1/live")),
		Elem("flashVer", Str("FMLE/3.0")),
		Elem("audioCodecs", Num(3575)),
		Elem("videoFunction", Num(1)),
		Elem("meta", Obj(Elem("x", Num(1)), Elem("y", Num(2)))),
	)
	got := roundTrip(t, v)
	if got.Type != TypeObject || len(got.Object) != 6 {
		t.Fatalf("object = %+v", got)
	}
	byKey := map[string]Value{}
	for _, m := range got.Object {
		byKey[m.Key] = m.Value
	}
	if byKey["app"].String != "live" {
		t.Fatalf("app = %+v", byKey["app"])
	}
	if byKey["audioCodecs"].Number != 3575 {
		t.Fatalf("audioCodecs = %+v", byKey["audioCodecs"])
	}
	if byKey["meta"].Type != TypeObject || len(byKey["meta"].Object) != 2 {
		t.Fatalf("meta = %+v", byKey["meta"])
	}
}

func TestAMF0ECMAArray(t *testing.T) {
	v := ECMA(
		Elem("duration", Num(0)),
		Elem("width", Num(1280)),
		Elem("height", Num(720)),
	)
	got := roundTrip(t, v)
	if got.Type != TypeECMAArray || len(got.Object) != 3 {
		t.Fatalf("ecma array = %+v", got)
	}
	if got.Object[1].Key != "width" || got.Object[1].Value.Number != 1280 {
		t.Fatalf("ecma[1] = %+v", got.Object[1])
	}
}

func TestAMF0StrictArray(t *testing.T) {
	v := Arr(Num(1), Str("two"), Bool(true), Null())
	got := roundTrip(t, v)
	if got.Type != TypeStrictArray || len(got.Array) != 4 {
		t.Fatalf("array = %+v", got)
	}
	if got.Array[0].Number != 1 || got.Array[1].String != "two" ||
		!got.Array[2].Boolean || got.Array[3].Type != TypeNull {
		t.Fatalf("array elements = %+v", got.Array)
	}
}

// TestAMF0UnknownTypeSkipped 验证不支持的 Date 类型被跳过，后面的值仍能正确读。
func TestAMF0UnknownTypeSkipped(t *testing.T) {
	var buf bytes.Buffer
	// 先写一个已知值。
	if err := WriteAMF0(&buf, Str("head")); err != nil {
		t.Fatal(err)
	}
	// 手写一个 Date：0x0B + 8 字节 double + 2 字节时区。
	buf.WriteByte(TypeDate)
	buf.Write(make([]byte, 10))
	// 再写一个已知值。
	if err := WriteAMF0(&buf, Num(42)); err != nil {
		t.Fatal(err)
	}

	head, err := ReadAMF0(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if head.String != "head" {
		t.Fatalf("head = %+v", head)
	}
	unknown, err := ReadAMF0(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if unknown.Type != TypeUnsupported {
		t.Fatalf("unknown type = %#x, want unsupported", unknown.Type)
	}
	tail, err := ReadAMF0(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if tail.Type != TypeNumber || tail.Number != 42 {
		t.Fatalf("tail = %+v, want 42", tail)
	}
	if buf.Len() != 0 {
		t.Fatalf("leftover bytes: %d", buf.Len())
	}
}

// TestAMF0LongString 验证超过 65535 字节的字符串退化成 Long String，并能读回。
func TestAMF0LongString(t *testing.T) {
	long := strings.Repeat("a", 70000)
	got := roundTrip(t, Str(long))
	if got.Type != TypeString || got.String != long {
		t.Fatalf("long string = type %#x len %d, want %d", got.Type, len(got.String), len(long))
	}
}

// TestAMF0RawBytes 直接核对一个 Object 的序列化字节，确保格式无误。
func TestAMF0RawBytes(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteAMF0(&buf, Obj(Elem("n", Num(1.5)))); err != nil {
		t.Fatal(err)
	}
	want := []byte{
		0x03,            // Object
		0x00, 0x01, 'n', // key "n"
		0x00,                                           // Number
		0x3F, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 1.5
		0x00, 0x00, 0x09, // 空 key + ObjectEnd
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("raw = % x, want % x", buf.Bytes(), want)
	}

	// 反向：从这些字节读回。
	v, err := ReadAMF0(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if v.Type != TypeObject || len(v.Object) != 1 || v.Object[0].Key != "n" ||
		v.Object[0].Value.Number != 1.5 {
		t.Fatalf("parsed = %+v", v)
	}
}

// TestAMF0NumberNaNInf 验证特殊浮点值能无损往返。
func TestAMF0NumberNaNInf(t *testing.T) {
	for _, n := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		got := roundTrip(t, Num(n))
		if math.IsNaN(n) {
			if !math.IsNaN(got.Number) {
				t.Fatalf("NaN not preserved: in=%v out=%v", n, got.Number)
			}
			continue
		}
		if got.Number != n {
			t.Fatalf("number(%v) = %v", n, got.Number)
		}
	}

	// 负零的符号位要保留。
	negZero := math.Copysign(0, -1)
	got := roundTrip(t, Num(negZero))
	if !math.Signbit(got.Number) {
		t.Fatalf("negative zero sign lost: %v", got.Number)
	}
}

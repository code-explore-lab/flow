package rtp

// AAC 接收侧解包（RFC 3640 mpeg4-generic / AAC-hbr）。
//
// RTP payload 布局：
//
//	 0                   1                   2                   3
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|          AU-headers-length (16 bit，单位是 bit)                 |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|  AU-size (SizeLength) | AU-Index (IndexLength)  ...            |  第一个 AU
//	|  AU-size (SizeLength) | AU-Index-delta (IndexDeltaLength) ...  |  第二个及以后
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                       AU data ...                             |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//
// AAC-hbr 的每个 AU 都完整放在一个 RTP 包里，不需要跨包重组；
// 同一个包可以携带多个 AU，靠 AU-headers-length 定位头部结束、AU-size 切分数据。
//
// 参数（SizeLength / IndexLength / IndexDeltaLength）来自 SDP 的 a=fmtp，
// 用 ParseAACFmtp 解析后传给 NewAACDepacketizer。

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	ErrEmptyAACPayload     = errors.New("rtp: empty AAC payload")
	ErrAACHeadersTruncated = errors.New("rtp: AAC AU headers truncated")
	ErrAACBadAUHeader      = errors.New("rtp: malformed AAC AU header")
	ErrAACSizeMismatch     = errors.New("rtp: AAC AU sizes do not match payload")
	ErrAACInvalidConfig    = errors.New("rtp: invalid AAC payload format config")
)

// AACConfig RFC 3640 的 AU header 位宽配置，对应 SDP fmtp 里的三个参数。
type AACConfig struct {
	SizeLength       int // 每个 AU 的 size 占多少 bit
	IndexLength      int // 第一个 AU 的 index 占多少 bit
	IndexDeltaLength int // 第二个及以后 AU 的 index 增量占多少 bit
}

// DefaultAACConfig 返回与发送侧 AACPacker / SDP 模板一致的默认值（13/3/3）。
func DefaultAACConfig() AACConfig {
	return AACConfig{
		SizeLength:       AACSizeLength,
		IndexLength:      AACIndexLength,
		IndexDeltaLength: AACIndexDeltaLength,
	}
}

// validate 检查位宽是否在可解析范围内。
func (c AACConfig) validate() error {
	if c.SizeLength < 1 || c.SizeLength > 32 {
		return fmt.Errorf("%w: SizeLength=%d (want 1..32)", ErrAACInvalidConfig, c.SizeLength)
	}
	if c.IndexLength < 0 || c.IndexLength > 32 {
		return fmt.Errorf("%w: IndexLength=%d (want 0..32)", ErrAACInvalidConfig, c.IndexLength)
	}
	if c.IndexDeltaLength < 0 || c.IndexDeltaLength > 32 {
		return fmt.Errorf("%w: IndexDeltaLength=%d (want 0..32)", ErrAACInvalidConfig, c.IndexDeltaLength)
	}
	return nil
}

// ParseAACFmtp 从 SDP 的 fmtp 值里读取 SizeLength / IndexLength / IndexDeltaLength。
//
// 输入可以是 fmtp 的值，也可以带 "a=fmtp:97 " 前缀：
//
//	streamtype=5; profile-level-id=1; mode=AAC-hbr; SizeLength=13; IndexLength=3; IndexDeltaLength=3
//
// 未出现的参数沿用 DefaultAACConfig 的默认值；未知参数（streamtype、config 等）忽略。
func ParseAACFmtp(fmtp string) (AACConfig, error) {
	cfg := DefaultAACConfig()

	s := strings.TrimSpace(fmtp)
	// 容忍调用方直接传整行 SDP。
	if strings.HasPrefix(strings.ToLower(s), "a=fmtp:") {
		if sp := strings.IndexAny(s, " \t"); sp >= 0 {
			s = s[sp+1:]
		} else {
			s = ""
		}
	}

	for _, field := range strings.Split(s, ";") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		key, val, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		val = strings.TrimSpace(val)

		switch key {
		case "sizelength":
			n, err := strconv.Atoi(val)
			if err != nil {
				return AACConfig{}, fmt.Errorf("%w: SizeLength=%q", ErrAACInvalidConfig, val)
			}
			cfg.SizeLength = n
		case "indexlength":
			n, err := strconv.Atoi(val)
			if err != nil {
				return AACConfig{}, fmt.Errorf("%w: IndexLength=%q", ErrAACInvalidConfig, val)
			}
			cfg.IndexLength = n
		case "indexdeltalength":
			n, err := strconv.Atoi(val)
			if err != nil {
				return AACConfig{}, fmt.Errorf("%w: IndexDeltaLength=%q", ErrAACInvalidConfig, val)
			}
			cfg.IndexDeltaLength = n
		}
	}

	if err := cfg.validate(); err != nil {
		return AACConfig{}, err
	}
	return cfg, nil
}

// AACAU 一个解包出来的 AAC Access Unit。
type AACAU struct {
	Data  []byte // 裸 AAC 帧（不含 ADTS 头），与 RTP payload 共享底层数组
	Size  int    // AU-header 里声明的字节数
	Index int    // AU-Index：第一个用绝对值，后续由 IndexDelta 累加得到
}

// AACDepacketizer 按配置的位宽拆一个 RTP payload 里的所有 AU。
//
// 同一个音频轨只需要一个实例；它无跨包状态，非并发安全。
type AACDepacketizer struct {
	Config AACConfig
}

// NewAACDepacketizer 校验配置并创建解包器。
func NewAACDepacketizer(cfg AACConfig) (*AACDepacketizer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &AACDepacketizer{Config: cfg}, nil
}

// Depacketize 解析一个 RTP 包，返回其中所有完整的 AU（按 payload 中的顺序）。
//
// 与 H.264 不同，AAC-hbr 没有跨包分片，所以这里不需要丢包重组状态；
// 头部或长度不自洽时返回 error，调用方跳过这个包即可。
func (d *AACDepacketizer) Depacketize(pkt *RTPPacket) ([]AACAU, error) {
	if pkt == nil || len(pkt.Payload) == 0 {
		return nil, ErrEmptyAACPayload
	}
	payload := pkt.Payload
	if len(payload) < 2 {
		return nil, fmt.Errorf("%w: %d byte(s)", ErrAACHeadersTruncated, len(payload))
	}

	// AU-headers-length 的单位是 bit，不是字节。
	headerBits := int(binary.BigEndian.Uint16(payload[:2]))
	if headerBits == 0 {
		return nil, fmt.Errorf("%w: AU-headers-length is zero", ErrAACHeadersTruncated)
	}
	if headerBits%8 != 0 {
		// 头部之后紧跟字节对齐的 AU data，位宽不整字节时无法定位数据。
		return nil, fmt.Errorf("%w: AU-headers-length %d is not byte-aligned", ErrAACHeadersTruncated, headerBits)
	}
	headerBytes := headerBits / 8
	if 2+headerBytes > len(payload) {
		return nil, fmt.Errorf("%w: need %d header bytes, have %d", ErrAACHeadersTruncated, headerBytes, len(payload)-2)
	}

	headerData := payload[2 : 2+headerBytes]
	data := payload[2+headerBytes:]

	aus, err := d.parseHeaders(headerData, headerBits)
	if err != nil {
		return nil, err
	}

	// 按 AU-size 依次切分 AU-data；总长度必须刚好等于剩余数据。
	off := 0
	for i := range aus {
		if aus[i].Size > len(data)-off {
			return nil, fmt.Errorf("%w: AU %d size %d exceeds remaining %d", ErrAACSizeMismatch, i, aus[i].Size, len(data)-off)
		}
		aus[i].Data = data[off : off+aus[i].Size]
		off += aus[i].Size
	}
	if off != len(data) {
		return nil, fmt.Errorf("%w: %d trailing byte(s)", ErrAACSizeMismatch, len(data)-off)
	}
	return aus, nil
}

// parseHeaders 从 AU-header 区读出每个 AU 的 size/index。
//
// 第一个 AU 的 header 是 size + IndexLength 位的绝对 index；
// 第二个及以后是 size + IndexDeltaLength 位的增量，index 在上一 AU 基础上累加。
func (d *AACDepacketizer) parseHeaders(headerData []byte, headerBits int) ([]AACAU, error) {
	r := bitReader{data: headerData}
	var aus []AACAU
	prevIndex := 0

	for r.bit < headerBits {
		size, err := r.read(d.Config.SizeLength)
		if err != nil {
			return nil, fmt.Errorf("%w: AU-size at bit %d", ErrAACBadAUHeader, r.bit)
		}
		if size > uint64(^uint(0)>>1) {
			return nil, fmt.Errorf("%w: AU-size %d overflows int", ErrAACBadAUHeader, size)
		}

		var index uint64
		if len(aus) == 0 {
			index, err = r.read(d.Config.IndexLength)
			if err != nil {
				return nil, fmt.Errorf("%w: AU-Index at bit %d", ErrAACBadAUHeader, r.bit)
			}
		} else {
			delta, derr := r.read(d.Config.IndexDeltaLength)
			if derr != nil {
				return nil, fmt.Errorf("%w: AU-Index-delta at bit %d", ErrAACBadAUHeader, r.bit)
			}
			index = uint64(prevIndex) + delta
		}

		aus = append(aus, AACAU{Size: int(size), Index: int(index)})
		prevIndex = int(index)
	}

	// 所有字段读完后必须正好用完 AU-headers-length，否则位宽配置和头部对不上。
	if r.bit != headerBits {
		return nil, fmt.Errorf("%w: consumed %d of %d header bits", ErrAACBadAUHeader, r.bit, headerBits)
	}
	return aus, nil
}

// bitReader 按 MSB-first 从字节切片里读任意位数，最多 32 位一次。
type bitReader struct {
	data []byte
	bit  int
}

func (r *bitReader) read(n int) (uint64, error) {
	if n < 0 || r.bit+n > len(r.data)*8 {
		return 0, ErrAACBadAUHeader
	}
	var v uint64
	for i := 0; i < n; i++ {
		pos := r.bit + i
		bit := (r.data[pos/8] >> uint(7-pos%8)) & 1
		v = v<<1 | uint64(bit)
	}
	r.bit += n
	return v, nil
}

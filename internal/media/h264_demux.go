package media

//从 `test.h264`（Annex-B，`00 00 00 01` 起始码）里切出每个 NALU。
import (
	"bufio"
	"errors"
	"io"
	"os"
)

// Annex-B h264分离器 提取NALU
type H264Demux struct {
	buf []byte
	off int
}

func NewH264Demux(filePath string) (*H264Demux, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	return &H264Demux{
		buf: buf,
		off: 0,
	}, nil
}

// NextNALU 读取下一个NALU
func (d *H264Demux) NextNALU() ([]byte, error) {
	n := len(d.buf)
	if d.off >= n {
		return nil, io.EOF
	}
	startPos := d.findStartCode(d.off)
	if startPos == -1 {
		return nil, io.EOF
	}
	// skip code, find NALU data start address
	nalStart := d.skipStartCode(startPos)
	if nalStart == -1 {
		// 以后一下递归调用
		d.off = startPos + 1
		return d.NextNALU()
	}
	// find startCode, as NALU end edge
	nextStart := d.findStartCode(nalStart)
	var nalu []byte
	if nextStart == -1 {
		nalu = d.buf[nalStart:]
		d.off = n
	} else {
		// 后面还有下一个七十码 [nalStart, nextStart)
		nalu = d.buf[nalStart:nextStart]
		d.off = nextStart
	}
	if len(nalu) == 0 {
		return d.NextNALU()
	}
	return nalu, nil
}

// 00 00 01
// 00 00 00 01
// return startCode index, not found then return -1
// findStartCode 查找 H264/H265 起始码
// 支持 3字节(00 00 01) / 4字节(00 00 00 01)
// 返回起始码第一个字节下标，找不到返回 -1
func (d *H264Demux) findStartCode(offset int) int {
	data := d.buf
	n := len(data)
	for i := offset; i <= n-3; { // 至少留3字节用于判断
		if data[i] == 0 && data[i+1] == 0 {
			// 优先判断4字节起始码 00 00 00 01
			if i+3 < n && data[i+2] == 0 && data[i+3] == 1 {
				return i
			}
			// 再判断3字节起始码 00 00 01
			if data[i+2] == 1 {
				return i
			}
			// 连续两个0，但后面不是起始码，跳过一个0，避免重叠漏检
			i += 2
			continue
		}
		i++
	}
	return -1
}

func (d *H264Demux) skipStartCode(startPos int) int {
	data := d.buf
	i := startPos
	n := len(data)
	// 3字节起始码 00 00 01
	if i+3 <= n && data[i] == 0 && data[i+1] == 0 && data[i+2] == 1 {
		return i + 3
	}
	// 4字节起始码 00 00 00 01
	if i+4 <= n && data[i] == 0 && data[i+1] == 0 && data[i+2] == 0 && data[i+3] == 1 {
		return i + 4
	}
	return -1
}

// H264StreamDemux 从连续字节流（ffmpeg 的 pipe:1、摄像头、网络）里切 NALU。
// 与 H264Demux 的区别是它不把整个流读进内存，而是一边读一边找起始码，
// 适合没有尽头的实时流。
type H264StreamDemux struct {
	r *bufio.Reader
	// started 表示下一个 NALU 的起始码在上次调用里已经消费掉了。
	// 这样跨调用不会重复扫描起始码，也不会把两个 NALU 之间的字节吞掉。
	started bool
}

// NewH264StreamDemux 包装一个字节流。读多少取决于调用方什么时候要下一帧。
func NewH264StreamDemux(r io.Reader) *H264StreamDemux {
	return &H264StreamDemux{r: bufio.NewReaderSize(r, 64*1024)}
}

// NextNALU 读取下一个 NALU。没有新数据时会阻塞在 ReadByte 上；
// 流结束返回 io.EOF；最后一个 NALU 后面没有起始码时，先返回数据，下一次再返回 EOF。
func (d *H264StreamDemux) NextNALU() ([]byte, error) {
	if !d.started {
		if err := d.skipToStartCode(); err != nil {
			return nil, err
		}
	}
	d.started = false

	var nalu []byte
	zeros := 0 // 连续 0 的个数：起始码 00 00 01 / 00 00 00 01 的候选
	for {
		b, err := d.r.ReadByte()
		if err != nil {
			if len(nalu) > 0 {
				// 流结束时最后一个 NALU 可能没有后继起始码
				return nalu, nil
			}
			return nil, err
		}
		if b == 0 {
			zeros++
			continue
		}
		if b == 1 && zeros >= 2 {
			// 找到下一个起始码（已消费），下一次调用直接从 NALU 数据开始读
			d.started = true
			return nalu, nil
		}
		// 这些 0 不是起始码的一部分，属于 NALU 数据
		for ; zeros > 0; zeros-- {
			nalu = append(nalu, 0)
		}
		nalu = append(nalu, b)
	}
}

// skipToStartCode 丢弃起始码之前的字节（比如文件开头的脏数据），
// 停在 00 00 01 的 1 之后。
func (d *H264StreamDemux) skipToStartCode() error {
	zeros := 0
	for {
		b, err := d.r.ReadByte()
		if err != nil {
			return err
		}
		switch {
		case b == 0:
			zeros++
		case b == 1 && zeros >= 2:
			return nil
		default:
			zeros = 0
		}
	}
}

/*
bit:  7   6 5   4 3 2 1 0
     F | NRI |   Type
	 NRI 有SPS PPS IDR
*/
// NALType 获取NAL类型：nalu[0] & 0x1F
func NALType(nalu []byte) (uint8, error) {
	if len(nalu) < 1 {
		return 0, errors.New("empty nalu")
	}
	// 把后五位取出来， 做type
	return nalu[0] & 0x1F, nil
}

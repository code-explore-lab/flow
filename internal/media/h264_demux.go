package media

//从 `test.h264`（Annex-B，`00 00 00 01` 起始码）里切出每个 NALU。
import (
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

package snapshot

import (
	"bytes"

	"github.com/Eyevinn/hi264/pkg/decoder"
	"github.com/Eyevinn/hi264/pkg/frame"
	"github.com/Eyevinn/hi264/pkg/yuv"
)

// decodeAndEncode 用纯 Go 的 hi264 解码一个 IDR 访问单元，再编码成
// JPEG/PNG。只支持 H.264 8-bit 4:2:0 逐行；H.265 或 hi264 不支持的
// 码流（POC type=1、隔行等）由调用方回退到 ffmpeg。
func decodeAndEncode(nalus [][]byte, format string, quality, maxWidth int) ([]byte, string, error) {
	dec := decoder.New()
	f, err := dec.DecodeNALUs(nalus)
	if err != nil {
		return nil, "", err
	}
	if maxWidth > 0 {
		f = downscale(f, maxWidth)
	}

	cs, rng := frameColor(f)
	var buf bytes.Buffer
	buf.Grow(f.Width * f.Height / 2)
	if format == FormatPNG {
		if err := yuv.WritePNGCS(&buf, f, cs, rng); err != nil {
			return nil, "", err
		}
		return buf.Bytes(), "image/png", nil
	}
	if err := yuv.WriteJPEGCS(&buf, f, quality, cs, rng); err != nil {
		return nil, "", err
	}
	return buf.Bytes(), "image/jpeg", nil
}

// frameColor 按码流 VUI 选择色彩矩阵和取值范围，缺省按 BT.601 limited。
func frameColor(f *frame.Frame) (yuv.ColorSpace, yuv.Range) {
	cs := yuv.BT601
	if f.ColorDescriptionValid {
		cs = yuv.ColorSpaceFromMatrixCoefficients(f.MatrixCoefficients)
	}
	rng := yuv.LimitedRange
	if f.VideoFullRangeFlag {
		rng = yuv.FullRange
	}
	return cs, rng
}

// downscale 用最近邻把帧缩到 maxWidth 宽（保持宽高比、宽高取偶数）。
// 缩略图场景下这点画质损失可以接受，换来的编码耗时下降非常明显：
// 1080p 全尺寸 JPEG 约 160ms，320 宽通常只有几毫秒。
func downscale(f *frame.Frame, maxWidth int) *frame.Frame {
	if maxWidth <= 0 || f.Width <= maxWidth || f.Width < 2 || f.Height < 2 {
		return f
	}
	w := max(maxWidth&^1, 2)
	h := max((f.Height*w/f.Width)&^1, 2)

	out := frame.NewFrame(w, h)
	for y := range h {
		sy := y * f.Height / h
		src := f.Y[sy*f.StrideY:]
		dst := out.Y[y*out.StrideY : y*out.StrideY+w]
		for x := 0; x < w; x++ {
			dst[x] = src[x*f.Width/w]
		}
	}

	cw, ch := w/2, h/2
	scw, sch := f.Width/2, f.Height/2
	if cw == 0 || ch == 0 || scw == 0 || sch == 0 {
		return out
	}
	for y := range ch {
		sy := y * sch / ch
		cbSrc := f.Cb[sy*f.StrideC:]
		crSrc := f.Cr[sy*f.StrideC:]
		cbDst := out.Cb[y*out.StrideC : y*out.StrideC+cw]
		crDst := out.Cr[y*out.StrideC : y*out.StrideC+cw]
		for x := range cw {
			sx := x * scw / cw
			cbDst[x] = cbSrc[sx]
			crDst[x] = crSrc[sx]
		}
	}
	return out
}

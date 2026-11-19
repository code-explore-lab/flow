// Package mpegts 实现 flow 直播链路需要的 MPEG-TS 子集：
//
//   - Demuxer：PAT/PMT/PES 解复用，输出 H.264 访问单元与 AAC AU；
//   - Muxer：把 H.264 NALU / AAC AU 封装成 MPEG-TS（SRT 输出，后续 HLS 也可复用）。
//
// 只支持直播场景下的 H.264（stream_type 0x1B）和 AAC ADTS（0x0F）。
// 文件传输、加密 TS、AC-3 等不在范围内。
package mpegts

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"flow/internal/media"
)

const (
	// PacketSize 是一个 MPEG-TS 包的固定长度。
	PacketSize = 188

	syncByte = 0x47

	// StreamTypeH264 / StreamTypeHEVC / StreamTypeAAC 是 PMT 里的 stream_type。
	StreamTypeH264 = 0x1B
	StreamTypeHEVC = 0x24
	StreamTypeAAC  = 0x0F
)

// Default PID 布局。和 ffmpeg 的默认值不同，但不影响播放器。
const (
	DefaultPMTPID   uint16 = 0x1000
	DefaultVideoPID uint16 = 0x0100
	DefaultAudioPID uint16 = 0x0101
	DefaultProgram  uint16 = 1
)

const maxSectionSize = 4096

// VideoAU 是一个视频访问单元（一帧）。NALUs 不含 Annex-B 起始码。
// Codec 为 "H264" / "H265"，由 PMT 的 stream_type 推导。
type VideoAU struct {
	NALUs    [][]byte
	PTS, DTS uint32 // 90kHz
	Keyframe bool
	Codec    string
}

// CompositionTime 返回 PTS−DTS（90kHz，可为负）。
func (v VideoAU) CompositionTime() int32 { return int32(v.PTS - v.DTS) }

// AudioAU 是一个 AAC 音频访问单元。Timestamp 使用采样时钟（AAC 等于采样率），
// 与 source.Frame 的音频时间戳约定一致。
type AudioAU struct {
	AU         []byte
	Timestamp  uint32
	SampleRate uint32
	Channels   uint8
	ASC        []byte
}

// Demuxer 把任意分片的 TS 字节流解析成音视频访问单元。
//
// 回调在 Write 的调用 goroutine 上同步执行；回调返回后 demuxer 可能复用
// 内部缓冲，需要长期保存的数据由回调自行复制（AU 的 Data 已经是独立副本，
// 只有 VideoAU.NALUs 共享同一块 PES 缓冲）。
type Demuxer struct {
	OnVideo func(VideoAU)
	OnAudio func(AudioAU)
	OnError func(error)

	buf []byte

	pmtPID  uint16
	havePMT bool

	videoPID, audioPID uint16
	videoCodec         string // "H264" / "H265"

	cc     map[uint16]byte
	ccSeen map[uint16]bool

	sections map[uint16]*sectionAsm
	pes      map[uint16]*pesBuf

	audioCarry  []byte
	audioLastTS uint32
	audioHaveTS bool

	warnedUnsupported bool
}

type sectionAsm struct {
	buf []byte
}

type pesBuf struct {
	data []byte
	want int // 完整 PES 长度（含 6 字节前缀）；0 表示长度未知（视频常见）
}

// NewDemuxer 创建一个空的解复用器。
func NewDemuxer() *Demuxer {
	return &Demuxer{
		cc:       make(map[uint16]byte),
		ccSeen:   make(map[uint16]bool),
		sections: make(map[uint16]*sectionAsm),
		pes:      make(map[uint16]*pesBuf),
	}
}

// Write 喂入 TS 字节流。返回的错误总是 nil；解析问题通过 OnError 报告，
// 这样上层读循环不会因为个别坏包断开。
func (d *Demuxer) Write(p []byte) (int, error) {
	d.buf = append(d.buf, p...)

	for len(d.buf) >= PacketSize {
		if d.buf[0] != syncByte {
			i := bytes.IndexByte(d.buf, syncByte)
			if i < 0 {
				d.buf = d.buf[:0]
				break
			}
			d.buf = d.buf[i:]
			if len(d.buf) < PacketSize {
				break
			}
		}
		d.processPacket(d.buf[:PacketSize])
		d.buf = d.buf[PacketSize:]
	}

	// 长时间找不到同步字时丢弃，避免坏流把内存吃满。
	if len(d.buf) > 1<<20 {
		d.buf = d.buf[:0]
		d.errorf("mpegts: demux buffer overflow, resyncing")
	}
	return len(p), nil
}

// Flush 冲刷尚未成型的 PES。推流结束时调用一次即可。
func (d *Demuxer) Flush() {
	for pid := range d.pes {
		d.flushPES(pid)
	}
	d.audioCarry = nil
}

func (d *Demuxer) errorf(format string, args ...any) {
	if d.OnError != nil {
		d.OnError(fmt.Errorf(format, args...))
	}
}

func (d *Demuxer) processPacket(pkt []byte) {
	if len(pkt) != PacketSize || pkt[0] != syncByte {
		return
	}
	if pkt[1]&0x80 != 0 { // transport_error_indicator
		return
	}
	pusi := pkt[1]&0x40 != 0
	pid := uint16(pkt[1]&0x1F)<<8 | uint16(pkt[2])
	afc := (pkt[3] >> 4) & 0x03
	cc := pkt[3] & 0x0F

	hasPayload := afc == 0x01 || afc == 0x03
	if !hasPayload {
		return
	}

	off := 4
	if afc == 0x03 {
		afLen := int(pkt[4])
		off = 5 + afLen
		if off > PacketSize {
			return
		}
	}

	// 连续计数器：TS over SRT 是有序可靠的，这里主要用来丢弃损坏或
	// 重连后不连续的半截 PES。带 PUSI 的包即使 CC 重复也要处理：
	// 有些封装器（或拼接的分片）会从头重新计数，直接丢弃会丢整帧。
	if d.ccSeen[pid] {
		last := d.cc[pid]
		if cc == last {
			if !pusi {
				return // 重复的续包
			}
			d.dropPES(pid)
		} else if cc != (last+1)&0x0F {
			d.dropPES(pid)
		}
	}
	d.cc[pid] = cc
	d.ccSeen[pid] = true

	payload := pkt[off:]
	switch {
	case pid == 0:
		d.handleSection(pid, pusi, payload)
	case d.pmtPID != 0 && pid == d.pmtPID:
		d.handleSection(pid, pusi, payload)
	case d.havePMT && pid == d.videoPID:
		d.handlePES(pid, pusi, payload)
	case d.havePMT && pid == d.audioPID:
		d.handlePES(pid, pusi, payload)
	}
}

func (d *Demuxer) handleSection(pid uint16, pusi bool, payload []byte) {
	asm := d.sections[pid]
	if asm == nil {
		asm = &sectionAsm{}
		d.sections[pid] = asm
	}

	if pusi {
		if len(payload) == 0 {
			return
		}
		ptr := int(payload[0])
		if 1+ptr > len(payload) {
			asm.buf = asm.buf[:0]
			return
		}
		if len(asm.buf) > 0 {
			asm.buf = append(asm.buf, payload[1:1+ptr]...)
			d.consumeSection(pid, asm)
		}
		if rest := payload[1+ptr:]; len(rest) > 0 {
			asm.buf = append(asm.buf, rest...)
			d.consumeSection(pid, asm)
		}
		return
	}

	if len(asm.buf) == 0 {
		return
	}
	asm.buf = append(asm.buf, payload...)
	d.consumeSection(pid, asm)
}

func (d *Demuxer) consumeSection(pid uint16, asm *sectionAsm) {
	for len(asm.buf) >= 3 {
		if asm.buf[0] == 0xFF { // TS 填充
			asm.buf = asm.buf[:0]
			return
		}
		length := int(binary.BigEndian.Uint16(asm.buf[1:3]) & 0x0FFF)
		total := 3 + length
		if total < 3 || total > maxSectionSize {
			asm.buf = asm.buf[:0]
			return
		}
		if len(asm.buf) < total {
			return
		}
		section := asm.buf[:total]
		if pid == 0 {
			d.parsePAT(section)
		} else {
			d.parsePMT(section)
		}
		rest := append([]byte(nil), asm.buf[total:]...)
		asm.buf = rest
		if len(asm.buf) > 0 && asm.buf[0] == 0xFF {
			asm.buf = asm.buf[:0]
			return
		}
	}
}

func (d *Demuxer) parsePAT(section []byte) {
	if len(section) < 12 || section[0] != 0x00 {
		return
	}
	end := len(section) - 4 // CRC32
	for i := 8; i+4 <= end; i += 4 {
		program := binary.BigEndian.Uint16(section[i:])
		pid := binary.BigEndian.Uint16(section[i+2:]) & 0x1FFF
		if program == 0 || pid == 0x1FFF {
			continue
		}
		if d.pmtPID != pid {
			d.pmtPID = pid
			d.havePMT = false
			d.videoPID, d.audioPID = 0, 0
			d.videoCodec = ""
		}
		return
	}
}

func (d *Demuxer) parsePMT(section []byte) {
	if len(section) < 16 || section[0] != 0x02 {
		return
	}
	end := len(section) - 4
	programInfoLen := int(binary.BigEndian.Uint16(section[10:12]) & 0x0FFF)
	i := 12 + programInfoLen
	if i > end {
		return
	}

	var videoPID, audioPID uint16
	var videoCodec string
	for i+5 <= end {
		streamType := section[i]
		pid := binary.BigEndian.Uint16(section[i+1:]) & 0x1FFF
		esLen := int(binary.BigEndian.Uint16(section[i+3:]) & 0x0FFF)
		i += 5 + esLen
		if i > len(section) {
			return
		}
		switch streamType {
		case StreamTypeH264:
			if videoPID == 0 {
				videoPID, videoCodec = pid, "H264"
			}
		case StreamTypeHEVC:
			if videoPID == 0 {
				videoPID, videoCodec = pid, "H265"
			}
		case StreamTypeAAC:
			if audioPID == 0 {
				audioPID = pid
			}
		}
	}

	if videoPID == 0 && audioPID == 0 {
		if !d.warnedUnsupported {
			d.warnedUnsupported = true
			d.errorf("mpegts: PMT has no supported stream (H.264/H.265/AAC)")
		}
		return
	}

	d.videoPID, d.audioPID = videoPID, audioPID
	d.videoCodec = videoCodec
	d.havePMT = true
}

func (d *Demuxer) dropPES(pid uint16) {
	if pb := d.pes[pid]; pb != nil {
		pb.data = pb.data[:0]
	}
}

func (d *Demuxer) handlePES(pid uint16, pusi bool, payload []byte) {
	if pusi {
		if pb := d.pes[pid]; pb != nil && len(pb.data) > 0 {
			d.flushPES(pid)
		}
		d.pes[pid] = &pesBuf{}
	}
	pb := d.pes[pid]
	if pb == nil {
		return
	}
	pb.data = append(pb.data, payload...)
	if pb.want == 0 && len(pb.data) >= 6 {
		if length := int(binary.BigEndian.Uint16(pb.data[4:6])); length > 0 {
			pb.want = 6 + length
		}
	}
	if pb.want > 0 && len(pb.data) >= pb.want {
		pb.data = pb.data[:pb.want]
		d.flushPES(pid)
	}
}

func (d *Demuxer) flushPES(pid uint16) {
	pb := d.pes[pid]
	delete(d.pes, pid)
	if pb == nil || len(pb.data) < 9 {
		return
	}
	data := pb.data
	if data[0] != 0 || data[1] != 0 || data[2] != 1 {
		return
	}

	headerDataLen := int(data[8])
	if 9+headerDataLen > len(data) {
		return
	}

	pts, dts, hasPTS, hasDTS := parsePESTimestamps(data)
	payload := data[9+headerDataLen:]

	switch pid {
	case d.videoPID:
		if !hasPTS && !hasDTS {
			return
		}
		if !hasDTS {
			dts = pts
		}
		if !hasPTS {
			pts = dts
		}
		d.handleVideoPES(payload, pts, dts)
	case d.audioPID:
		if !hasPTS {
			return
		}
		d.handleAudioPES(payload, pts)
	}
}

// parsePESTimestamps 解析 PES 头里的 PTS/DTS（33 位，90kHz）。
func parsePESTimestamps(data []byte) (pts, dts uint32, hasPTS, hasDTS bool) {
	if len(data) < 14 {
		return 0, 0, false, false
	}
	flags := (data[7] >> 6) & 0x03
	switch flags {
	case 0x02:
		if len(data) < 14 {
			return 0, 0, false, false
		}
		return readTimestamp(data[9:14]), 0, true, false
	case 0x03:
		if len(data) < 19 {
			return 0, 0, false, false
		}
		return readTimestamp(data[9:14]), readTimestamp(data[14:19]), true, true
	}
	return 0, 0, false, false
}

func readTimestamp(b []byte) uint32 {
	return uint32(b[0]&0x0E)<<29 |
		uint32(b[1])<<22 |
		uint32(b[2]&0xFE)<<14 |
		uint32(b[3])<<7 |
		uint32(b[4])>>1
}

func (d *Demuxer) handleVideoPES(payload []byte, pts, dts uint32) {
	nalus := splitNALUs(payload)
	if len(nalus) == 0 {
		return
	}
	codec := d.videoCodec
	if codec == "" {
		codec = "H264"
	}
	for _, group := range groupAUs(nalus, codec) {
		keyframe := false
		for _, nalu := range group {
			if media.ClassifyVideoNAL(codec, nalu).IsIDR {
				keyframe = true
				break
			}
		}
		if d.OnVideo != nil {
			d.OnVideo(VideoAU{NALUs: group, PTS: pts, DTS: dts, Keyframe: keyframe, Codec: codec})
		}
	}
}

func (d *Demuxer) handleAudioPES(payload []byte, pts uint32) {
	fresh := len(d.audioCarry) == 0
	buf := payload
	if len(d.audioCarry) > 0 {
		buf = append(d.audioCarry, payload...)
		d.audioCarry = nil
	}

	index := 0
	for len(buf) >= 7 {
		h, err := media.ParseADTSHeader(buf)
		if err != nil {
			// 丢失同步时向后找一个可能的 ADTS 同步字。
			if i := bytes.IndexByte(buf[1:], 0xFF); i >= 0 {
				buf = buf[i+1:]
				continue
			}
			break
		}
		if h.FrameLength <= h.HeaderLength || h.FrameLength > len(buf) || h.SamplingFrequency == 0 {
			break
		}

		var ts uint32
		switch {
		case fresh && index == 0:
			ts = uint32(uint64(pts) * uint64(h.SamplingFrequency) / 90000)
		case d.audioHaveTS:
			ts = d.audioLastTS + 1024
		}
		d.audioLastTS = ts
		d.audioHaveTS = true

		au := append([]byte(nil), buf[h.HeaderLength:h.FrameLength]...)
		if d.OnAudio != nil {
			d.OnAudio(AudioAU{
				AU:         au,
				Timestamp:  ts,
				SampleRate: uint32(h.SamplingFrequency),
				Channels:   h.ChannelConfig,
				ASC:        h.AudioSpecificConfig(),
			})
		}
		buf = buf[h.FrameLength:]
		index++
	}

	if len(buf) > 0 {
		d.audioCarry = append(d.audioCarry[:0], buf...)
	}
}

// splitNALUs 按 Annex-B 起始码切分 NALU，兼容 3/4 字节起始码。
func splitNALUs(p []byte) [][]byte {
	start := findStartCode(p, 0)
	if start < 0 {
		return nil
	}
	dataStart := start + startCodeLen(p, start)
	var out [][]byte
	for {
		next := findStartCode(p, dataStart)
		if next < 0 {
			if dataStart < len(p) {
				out = append(out, p[dataStart:])
			}
			return out
		}
		if next > dataStart {
			out = append(out, p[dataStart:next])
		}
		dataStart = next + startCodeLen(p, next)
	}
}

func findStartCode(p []byte, offset int) int {
	for i := offset; i+3 <= len(p); i++ {
		if p[i] != 0 || p[i+1] != 0 {
			continue
		}
		if p[i+2] == 1 {
			return i
		}
		if i+3 < len(p) && p[i+2] == 0 && p[i+3] == 1 {
			return i
		}
	}
	return -1
}

func startCodeLen(p []byte, i int) int {
	if i+3 < len(p) && p[i+2] == 0 && p[i+3] == 1 {
		return 4
	}
	return 3
}

// isAudioDelimiter 判断 NALU 是否是访问单元分隔符（AUD）。
// H.264 是类型 9，H.265 是类型 35（AUD_NUT）。
func isAudioDelimiter(codec string, nalu []byte) bool {
	typ, err := media.VideoNALType(codec, nalu)
	if err != nil {
		return false
	}
	if media.IsHEVCCodec(codec) {
		return typ == 35
	}
	return typ == 9
}

// groupAUs 把一个 PES 里的 NALU 按访问单元分组。
// AUD 或 first_mb_in_slice==0 的 VCL NALU 开启新的 AU；参数集/SEI
// 与随后的第一个 VCL 属于同一 AU。
func groupAUs(nalus [][]byte, codec string) [][][]byte {
	var groups [][][]byte
	var cur [][]byte
	hasVCL := false

	flush := func() {
		if len(cur) > 0 {
			groups = append(groups, cur)
			cur = nil
			hasVCL = false
		}
	}

	for _, nalu := range nalus {
		if len(nalu) == 0 {
			continue
		}
		class := media.ClassifyVideoNAL(codec, nalu)
		isAUD := isAudioDelimiter(codec, nalu)
		startsAU := media.StartsVideoAU(codec, nalu)

		if isAUD && len(cur) > 0 {
			flush()
		} else if class.IsVCL && startsAU && hasVCL {
			flush()
		}

		cur = append(cur, nalu)
		if class.IsVCL {
			hasVCL = true
		}
	}
	flush()
	return groups
}

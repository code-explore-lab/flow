package mpegts

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"flow/internal/media"
)

const (
	psiInterval = 500 * time.Millisecond
	maxWrite    = 1316 // 单次写入的 TS 字节数，避免超过 SRT 的典型 MTU
)

// Muxer 把 H.264 NALU 与 AAC AU 封装成 MPEG-TS。
//
// 每次 WriteVideo/WriteAudio 都会同步写完本次产生的所有 TS 包，
// 多个 goroutine 可以并发调用（内部串行化，包不会交错）。
type Muxer struct {
	mu sync.Mutex
	w  io.Writer

	pmtPID   uint16
	videoPID uint16
	audioPID uint16

	cc map[uint16]byte

	// videoCodec 是视频基本流的编码（"H264" / "H265"），决定 PMT 的
	// stream_type、AUD 的字节和参数集顺序。默认 H.264。
	videoCodec string
	vps, sps   []byte
	pps        []byte

	hasVideo bool

	asc           []byte
	audioRate     uint32
	audioChannels uint8
	audioProfile  uint8
	audioSFI      uint8
	hasAudio      bool

	out     []byte
	lastPSI time.Time
}

// NewMuxer 创建一个写入 w 的封装器。
func NewMuxer(w io.Writer) *Muxer {
	return &Muxer{
		w:        w,
		pmtPID:   DefaultPMTPID,
		videoPID: DefaultVideoPID,
		audioPID: DefaultAudioPID,
		cc:       make(map[uint16]byte),
	}
}

// SetVideoParams 设置 H.264 的 SPS/PPS，关键帧前会自动补上。
func (m *Muxer) SetVideoParams(sps, pps []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.videoCodec = "H264"
	m.vps = m.vps[:0]
	m.sps = append(m.sps[:0], sps...)
	m.pps = append(m.pps[:0], pps...)
	if len(sps) > 0 && len(pps) > 0 {
		m.hasVideo = true
		m.lastPSI = time.Time{}
	}
}

// SetVideoParamsFor 按编码设置参数集：H.265 走 VPS/SPS/PPS，其余按 H.264。
func (m *Muxer) SetVideoParamsFor(codec string, vps, sps, pps []byte) {
	if media.IsHEVCCodec(codec) {
		m.SetVideoParamsHEVC(vps, sps, pps)
		return
	}
	m.SetVideoParams(sps, pps)
}

// SetVideoParamsHEVC 设置 H.265 的 VPS/SPS/PPS，关键帧前会自动补上。
// 与 SetVideoParams 互斥，调用哪一个就决定 PMT 里的视频 stream_type。
func (m *Muxer) SetVideoParamsHEVC(vps, sps, pps []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.videoCodec = "H265"
	m.vps = append(m.vps[:0], vps...)
	m.sps = append(m.sps[:0], sps...)
	m.pps = append(m.pps[:0], pps...)
	if len(sps) > 0 && len(pps) > 0 {
		m.hasVideo = true
		m.lastPSI = time.Time{}
	}
}

// SetAudioParams 用 AudioSpecificConfig 设置 AAC 参数。只支持 AAC LC 等
// 能被 ADTS profile 表达的音频对象类型。
func (m *Muxer) SetAudioParams(asc []byte) error {
	if len(asc) < 2 {
		return errors.New("mpegts: audio specific config too short")
	}
	rate, channels, err := media.ParseASC(asc)
	if err != nil {
		return fmt.Errorf("mpegts: invalid audio specific config: %w", err)
	}
	aot := asc[0] >> 3
	if aot == 0 || aot > 4 {
		return fmt.Errorf("mpegts: audio object type %d cannot be expressed in ADTS", aot)
	}
	sfi := (asc[0]&0x07)<<1 | asc[1]>>7

	m.mu.Lock()
	defer m.mu.Unlock()
	m.asc = append(m.asc[:0], asc...)
	m.audioRate = uint32(rate)
	m.audioChannels = channels
	m.audioProfile = aot - 1
	m.audioSFI = sfi
	m.hasAudio = true
	m.lastPSI = time.Time{}
	return nil
}

// WriteVideo 写入一个视频访问单元。pts/dts 为 90kHz。
// 不含 VCL NALU 的访问单元会被忽略。编码由 SetVideoParams /
// SetVideoParamsHEVC 决定（默认 H.264）。
func (m *Muxer) WriteVideo(nalus [][]byte, pts, dts uint32) error {
	codec := m.videoCodec
	if codec == "" {
		codec = "H264"
	}
	hasVCL := false
	keyframe := false
	hasVPS, hasSPS, hasPPS := false, false, false
	for _, nalu := range nalus {
		if len(nalu) == 0 {
			continue
		}
		class := media.ClassifyVideoNAL(codec, nalu)
		if class.IsVCL {
			hasVCL = true
		}
		if class.IsIDR {
			keyframe = true
		}
		if class.IsVPS {
			hasVPS = true
		}
		if class.IsSPS {
			hasSPS = true
		}
		if class.IsPPS {
			hasPPS = true
		}
	}
	if !hasVCL {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.hasVideo = true
	if m.needPSILocked() {
		m.writePSILocked()
	}

	payload := make([]byte, 0, 256)
	appendNALU := func(nalu []byte) {
		payload = append(payload, 0, 0, 0, 1)
		payload = append(payload, nalu...)
	}

	if codec == "H265" {
		payload = append(payload, 0, 0, 0, 1, 0x46, 0x01) // AUD_NUT
	} else {
		payload = append(payload, 0, 0, 0, 1, 0x09, 0xF0) // AUD
	}
	if keyframe {
		if codec == "H265" && !hasVPS && len(m.vps) > 0 {
			appendNALU(m.vps)
		}
		if !hasSPS && len(m.sps) > 0 {
			appendNALU(m.sps)
		}
		if !hasPPS && len(m.pps) > 0 {
			appendNALU(m.pps)
		}
	}
	for _, nalu := range nalus {
		if len(nalu) > 0 {
			appendNALU(nalu)
		}
	}

	pcr := uint64(dts) * 300
	if err := m.writePESLocked(m.videoPID, 0xE0, payload, pts, dts, 0, &pcr); err != nil {
		return err
	}
	return m.flushLocked()
}

// WriteAudio 写入一个 AAC 访问单元（不含 ADTS 头）。timestamp 使用采样时钟。
// 未设置音频参数时直接忽略。
func (m *Muxer) WriteAudio(au []byte, timestamp uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.hasAudio || m.audioRate == 0 || len(au) == 0 {
		return nil
	}
	if m.needPSILocked() {
		m.writePSILocked()
	}

	frameLength := 7 + len(au)
	if frameLength > 0x1FFF {
		return errors.New("mpegts: AAC frame too large for ADTS")
	}
	payload := make([]byte, frameLength)
	payload[0] = 0xFF
	payload[1] = 0xF1 // MPEG-4, layer 0, no CRC
	payload[2] = m.audioProfile<<6 | (m.audioSFI&0x0F)<<2 | (m.audioChannels>>2)&0x01
	payload[3] = (m.audioChannels&0x03)<<6 | byte(frameLength>>11)&0x03
	payload[4] = byte(frameLength >> 3)
	payload[5] = byte(frameLength&0x07)<<5 | 0x1F
	payload[6] = 0xFC
	copy(payload[7:], au)

	pts := uint32((uint64(timestamp)*90000 + uint64(m.audioRate)/2) / uint64(m.audioRate))
	if err := m.writePESLocked(m.audioPID, 0xC0, payload, pts, pts, len(payload)+8, nil); err != nil {
		return err
	}
	return m.flushLocked()
}

// WritePSI 立即重写 PAT/PMT 并发送。
func (m *Muxer) WritePSI() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.hasVideo && !m.hasAudio {
		return nil
	}
	m.writePSILocked()
	return m.flushLocked()
}

// Flush 把内部缓冲写给底层 writer。正常情况下 WriteVideo/WriteAudio
// 已经同步写完，这里主要用于将来做批量发送。
func (m *Muxer) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.flushLocked()
}

func (m *Muxer) needPSILocked() bool {
	return m.lastPSI.IsZero() || time.Since(m.lastPSI) >= psiInterval
}

func (m *Muxer) flushLocked() error {
	for off := 0; off < len(m.out); {
		n := len(m.out) - off
		if n > maxWrite {
			n = maxWrite
		}
		if _, err := m.w.Write(m.out[off : off+n]); err != nil {
			m.out = m.out[:0]
			return err
		}
		off += n
	}
	m.out = m.out[:0]
	return nil
}

func (m *Muxer) writePSILocked() {
	if m.hasVideo {
		m.writeSectionLocked(0, m.buildPAT())
	}
	if m.hasVideo || m.hasAudio {
		m.writeSectionLocked(m.pmtPID, m.buildPMT())
	}
	m.lastPSI = time.Now()
}

func (m *Muxer) buildPAT() []byte {
	section := []byte{
		0x00,       // table_id
		0xB0, 0x0D, // section_length = 13
		0x00, 0x01, // transport_stream_id
		0xC1,       // version 0, current_next 1
		0x00, 0x00, // section_number, last_section_number
		byte(DefaultProgram >> 8), byte(DefaultProgram),
		byte(0xE0 | m.pmtPID>>8), byte(m.pmtPID),
	}
	return appendCRC(section)
}

func (m *Muxer) buildPMT() []byte {
	var streams []byte
	if m.hasVideo {
		streamType := byte(StreamTypeH264)
		if m.videoCodec == "H265" {
			streamType = StreamTypeHEVC
		}
		streams = append(streams, streamType,
			byte(0xE0|m.videoPID>>8), byte(m.videoPID), 0xF0, 0x00)
	}
	if m.hasAudio {
		streams = append(streams, StreamTypeAAC,
			byte(0xE0|m.audioPID>>8), byte(m.audioPID), 0xF0, 0x00)
	}

	sectionLength := 9 + len(streams) + 4
	section := []byte{
		0x02,
		byte(0xB0 | sectionLength>>8), byte(sectionLength),
		byte(DefaultProgram >> 8), byte(DefaultProgram),
		0xC1,
		0x00, 0x00,
	}
	pcrPID := m.videoPID
	if !m.hasVideo {
		pcrPID = m.audioPID
	}
	section = append(section, byte(0xE0|pcrPID>>8), byte(pcrPID), 0xF0, 0x00)
	section = append(section, streams...)
	return appendCRC(section)
}

func appendCRC(section []byte) []byte {
	crc := crc32MPEG2(section)
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], crc)
	return append(section, b[:]...)
}

func crc32MPEG2(data []byte) uint32 {
	crc := uint32(0xFFFFFFFF)
	for _, b := range data {
		crc ^= uint32(b) << 24
		for i := 0; i < 8; i++ {
			if crc&0x80000000 != 0 {
				crc = crc<<1 ^ 0x04C11DB7
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

func (m *Muxer) writeSectionLocked(pid uint16, section []byte) {
	if len(section) > 183 {
		return // 单包 PSI 足够；超出说明表结构有误
	}
	payload := make([]byte, 1+len(section), 184)
	payload[0] = 0x00 // pointer_field
	copy(payload[1:], section)
	for len(payload) < 184 {
		payload = append(payload, 0xFF)
	}

	cc := m.cc[pid]
	m.cc[pid] = (cc + 1) & 0x0F
	pkt := []byte{
		0x47,
		byte(0x40 | pid>>8),
		byte(pid),
		byte(0x10 | cc),
	}
	m.out = append(m.out, pkt...)
	m.out = append(m.out, payload...)
}

// writePESLocked 把一个 PES 负载拆成 TS 包。pesLen 为 0 表示长度未知
// （视频，由下一个 PUSI 界定）；pcr 非空时在第一个包写入 PCR。
func (m *Muxer) writePESLocked(pid uint16, streamID byte, payload []byte, pts, dts uint32, pesLen int, pcr *uint64) error {
	header := []byte{
		0x00, 0x00, 0x01, streamID,
		byte(pesLen >> 8), byte(pesLen),
		0x80, // '10' + flags
		0x80, // PTS only
		0x05,
	}
	if dts != pts {
		header[7] = 0xC0 // PTS + DTS
		header[8] = 0x0A
		header = append(header, encodeTimestamp(0x3, pts)...)
		header = append(header, encodeTimestamp(0x1, dts)...)
	} else {
		header = append(header, encodeTimestamp(0x2, pts)...)
	}
	payload = append(header, payload...)

	first := true
	for len(payload) > 0 {
		var af []byte
		if first && pcr != nil {
			afLen := 183 - min(len(payload), 176)
			af = make([]byte, 1+afLen)
			af[0] = byte(afLen)
			af[1] = 0x10 // PCR_flag
			putPCR(af[2:8], *pcr)
			for i := 8; i < len(af); i++ {
				af[i] = 0xFF
			}
		}

		capacity := 184 - len(af)
		n := min(len(payload), capacity)
		chunk := payload[:n]
		payload = payload[n:]

		if af == nil && len(chunk) < 184 {
			afLen := 183 - len(chunk)
			af = make([]byte, 1+afLen)
			af[0] = byte(afLen)
			for i := 1; i < len(af); i++ {
				af[i] = 0xFF
			}
		}

		cc := m.cc[pid]
		m.cc[pid] = (cc + 1) & 0x0F
		afc := byte(0x10)
		if af != nil {
			afc = 0x30
		}
		pkt := []byte{0x47}
		b1 := byte(pid >> 8)
		if first {
			b1 |= 0x40
		}
		pkt = append(pkt, b1, byte(pid), afc|cc)
		pkt = append(pkt, af...)
		pkt = append(pkt, chunk...)
		if len(pkt) != PacketSize {
			return fmt.Errorf("mpegts: internal packet size %d", len(pkt))
		}
		m.out = append(m.out, pkt...)
		first = false
	}
	return nil
}

func encodeTimestamp(prefix byte, ts uint32) []byte {
	return []byte{
		prefix<<4 | byte((ts>>29)&0x0E) | 0x01,
		byte(ts >> 22),
		byte((ts>>14)&0xFE) | 0x01,
		byte(ts >> 7),
		byte((ts<<1)&0xFE) | 0x01,
	}
}

func putPCR(dst []byte, base uint64) {
	base %= 1 << 33
	dst[0] = byte(base >> 25)
	dst[1] = byte(base >> 17)
	dst[2] = byte(base >> 9)
	dst[3] = byte(base >> 1)
	dst[4] = byte(base&1)<<7 | 0x7E
	dst[5] = 0x00
}

package rtp

// H.265 / HEVC RTP 封装（RFC 7798）：
//
//   - 单 NAL 单元包：Type 0~47，payload 就是完整 NAL；
//   - AP（聚合包）  ：Type 48，把多个小 NAL 合进一个 RTP 包；
//   - FU（分片单元） ：Type 49，把大 NAL 拆成多片。
//
// HEVC 的 NAL 头是 2 字节（H.264 只有 1 字节），所以 RTP payload header、
// AP 头和 FU 头都比 H.264 多一个字节。语义与 h264_packer.go 一一对应。

import (
	"errors"
)

// HEVC RTP payload header 的 NAL 单元类型（RFC 7798 §4.4）。
const (
	H265TypeAP = 48 // Aggregation Packet
	H265TypeFU = 49 // Fragmentation Unit
)

// H265Packer 把一个 HEVC NALU 封成一个或多个 RTP 包。
// 输入是去掉 Annex-B 起始码后的裸 NALU，含 2 字节 NAL header。
type H265Packer struct {
	MTU int   // 单个 RTP 载荷上限，默认 1400
	PT  uint8 // RTP payload type，默认 96
}

// NewH265Packer 创建封包器，mtu<=0 时取默认 1400。
func NewH265Packer(mtu int) *H265Packer {
	if mtu <= 0 {
		mtu = 1400
	}
	return &H265Packer{MTU: mtu, PT: 96}
}

// Pack 输入一个 NALU，输出一个或多个 RTP 包。
// Version、PT、Marker 已填好，Seq / Timestamp / SSRC 留给调用方。
func (p *H265Packer) Pack(nalu []byte) ([]*RTPPacket, error) {
	if len(nalu) < 2 {
		return nil, errors.New("h265: nalu shorter than 2-byte nal header")
	}
	if p.MTU <= 0 {
		p.MTU = 1400
	}
	if p.PT == 0 {
		p.PT = 96
	}
	if len(nalu) <= p.MTU {
		return []*RTPPacket{newPacket(p.PT, nalu, true)}, nil
	}
	return p.packFU(nalu)
}

// PackFrame 把一个访问单元（同一 RTP 时间戳）里的多个 NALU 封装成 RTP 包。
// 相邻小 NALU 贪心聚合成 AP，放不下的走单 NAL / FU。整帧只在最后一个包置 marker。
func (p *H265Packer) PackFrame(nalus [][]byte) ([]*RTPPacket, error) {
	if p.MTU <= 0 {
		p.MTU = 1400
	}
	if p.PT == 0 {
		p.PT = 96
	}

	var (
		pkts  []*RTPPacket
		batch [][]byte
		size  = 2 // AP payload header（2 字节）
	)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if len(batch) == 1 {
			ps, err := p.Pack(batch[0])
			if err != nil {
				return err
			}
			pkts = append(pkts, ps...)
		} else {
			pkts = append(pkts, newPacket(p.PT, packHEVCAP(batch), true))
		}
		batch = nil
		size = 2
		return nil
	}

	sawNALU := false
	for _, nalu := range nalus {
		if len(nalu) < 2 {
			continue
		}
		sawNALU = true
		// 单个 NALU 连 AP 头 + 长度字段都放不下：先结算当前批次再单独发。
		if 2+2+len(nalu) > p.MTU {
			if err := flush(); err != nil {
				return nil, err
			}
			ps, err := p.Pack(nalu)
			if err != nil {
				return nil, err
			}
			pkts = append(pkts, ps...)
			continue
		}
		if len(batch) > 0 && size+2+len(nalu) > p.MTU {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		batch = append(batch, nalu)
		size += 2 + len(nalu)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	if !sawNALU {
		return nil, errors.New("empty nalu")
	}

	// 整帧只有一个 marker。
	for i, pkt := range pkts {
		pkt.Header.Marker = i == len(pkts)-1
	}
	return pkts, nil
}

// packHEVCAP 按 RFC 7798 §4.4.2 组装 AP 载荷：
//
//	PayloadHdr(2B) | 2B len + NALU | 2B len + NALU ...
//
// F=0、Type=48、LayerId=0、TID=1。
func packHEVCAP(nalus [][]byte) []byte {
	total := 2
	for _, nalu := range nalus {
		total += 2 + len(nalu)
	}
	buf := make([]byte, 2, total)
	buf[0] = H265TypeAP << 1 // F=0, Type=48, LayerId 高位=0
	buf[1] = 0x01            // LayerId 低位=0, TID=1
	for _, nalu := range nalus {
		buf = append(buf, byte(len(nalu)>>8), byte(len(nalu)))
		buf = append(buf, nalu...)
	}
	return buf
}

// packFU 按 RFC 7798 §4.4.3 把大 NALU 拆成多个 FU 包。
func (p *H265Packer) packFU(nalu []byte) ([]*RTPPacket, error) {
	const fuHeaderLen = 3 // payload header(2) + FU header(1)
	maxData := p.MTU - fuHeaderLen
	if maxData <= 0 {
		return nil, errors.New("mtu too small for hevc fu fragmentation")
	}

	// payload header：F 保留，Type 改成 49，LayerId/TID 原样。
	ph0 := (nalu[0] & 0x81) | (H265TypeFU << 1)
	ph1 := nalu[1]
	fuType := (nalu[0] >> 1) & 0x3F

	data := nalu[2:] // 分片里不重复写 2 字节 NAL header
	n := len(data)
	numFrags := (n + maxData - 1) / maxData
	pkts := make([]*RTPPacket, 0, numFrags)

	for i := 0; i < n; i += maxData {
		end := i + maxData
		if end > n {
			end = n
		}

		// FU header：S(1) E(1) FuType(6)。
		fuHeader := fuType
		if i == 0 {
			fuHeader |= 0x80
		}
		if end == n {
			fuHeader |= 0x40
		}

		payload := make([]byte, 0, fuHeaderLen+(end-i))
		payload = append(payload, ph0, ph1, fuHeader)
		payload = append(payload, data[i:end]...)

		pkts = append(pkts, newPacket(p.PT, payload, end == n))
	}
	return pkts, nil
}

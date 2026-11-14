package rtp

//输入一个 NALU，输出一个或多个 RTP packet（小包直接 single NALU，大包走 FU-A）。
import (
	"errors"
)

// H.264 NAL 单元类型（RFC 6184）
const (
	NALUTypeNonIDR = 1  // non-IDR图像slice，可能是 I/P/B/SP/SI，需要解析slice头区分
	NALUTypeIDR    = 5  // IDR图像slice（IDR帧，特殊I帧，解码时清空参考帧）
	NALUTypeSPS    = 7  // 序列参数集
	NALUTypePPS    = 8  // 图像参数集
	NALUTypeSTAPA  = 24 // STAP-A 聚合RTP包（PackFrame 使用）
	NALUTypeFUA    = 28 // FU-A RTP分片单元
)

// H264Packer 把一个 H.264 NALU 封成一个或多个 RTP 包。
// 小包（长度 <= MTU）直接走 single NALU；大包按 RFC 6184 拆成多个 FU-A。
// 输入是去掉了 Annex-B 起始码（00 00 00 01）之后的裸 NALU，含 1 字节 NAL header。
type H264Packer struct {
	MTU int   // 单个 RTP 载荷上限（含 FU-A 的 2 字节头），默认 1400
	PT  uint8 // RTP payload type，默认 96（H.264 动态类型）
}

// NewH264Packer 创建封包器，mtu<=0 时取默认 1400。
func NewH264Packer(mtu int) *H264Packer {
	if mtu <= 0 {
		mtu = 1400
	}
	return &H264Packer{MTU: mtu, PT: 96}
}

// Pack 输入一个 NALU，输出一个或多个 RTP 包。
// 返回的包：Version、PT、Marker（最后一个包为 true）已填好；
// Seq / Timestamp / SSRC 留给调用方（见任务 2.2.6）。
func (p *H264Packer) Pack(nalu []byte) ([]*RTPPacket, error) {
	if len(nalu) == 0 {
		return nil, errors.New("empty nalu")
	}
	if p.MTU <= 0 {
		p.MTU = 1400
	}
	if p.PT == 0 {
		p.PT = 96
	}

	// single NALU：整包直接作为 RTP payload
	if len(nalu) <= p.MTU {
		return []*RTPPacket{newPacket(p.PT, nalu, true)}, nil
	}

	// FU-A 分片
	return p.packFUA(nalu)
}

// PackFrame 把一个访问单元（同一 RTP 时间戳）里的多个 NALU 封装成 RTP 包。
//
// 相邻的小 NALU 会贪心聚合成 STAP-A（RFC 6184 §5.7.1），前提是整个
// STAP-A 载荷不超过 MTU；放不下的批次先 flush，单个 NALU 继续走
// Pack 的单包 / FU-A 逻辑。所以 SPS+PPS+大 IDR 会输出
// STAP-A(SPS,PPS) + FU-A(IDR)，而不是把参数集拆成两个单包。
//
// 返回包的 Marker 只在整帧最后一个包上置位；STAP-A 头里的 NRI 取
// 所有被聚合 NALU 的最大值。
func (p *H264Packer) PackFrame(nalus [][]byte) ([]*RTPPacket, error) {
	if p.MTU <= 0 {
		p.MTU = 1400
	}
	if p.PT == 0 {
		p.PT = 96
	}

	var (
		pkts  []*RTPPacket
		batch [][]byte // 待聚合的同一批次 NALU
		size  = 1      // STAP-A 头（1 字节）
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
			pkts = append(pkts, newPacket(p.PT, packSTAPA(batch), true))
		}
		batch = nil
		size = 1
		return nil
	}

	sawNALU := false
	for _, nalu := range nalus {
		if len(nalu) == 0 {
			continue
		}
		sawNALU = true
		// 单个 NALU 连 STAP-A 头 + 长度字段都放不下：先结算当前批次，
		// 再按单 NALU / FU-A 发。
		if 1+2+len(nalu) > p.MTU {
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

	// 整帧只有一个 marker：封包器内部每个 NALU 的尾包都会置 marker，
	// 聚合后必须收回到最后一个包。
	for i, pkt := range pkts {
		pkt.Header.Marker = i == len(pkts)-1
	}
	return pkts, nil
}

// packSTAPA 按 RFC 6184 §5.7.1 组装 STAP-A 载荷：
//
//	STAP-A header | 2B len + NALU | 2B len + NALU ...
//
// header 的 NRI 取所有 NALU 的最大 NRI，F 置 0。
func packSTAPA(nalus [][]byte) []byte {
	nri := byte(0)
	total := 1
	for _, nalu := range nalus {
		nri = max(nri, nalu[0]&0x60)
		total += 2 + len(nalu)
	}
	buf := make([]byte, 1, total)
	buf[0] = nri | NALUTypeSTAPA
	for _, nalu := range nalus {
		buf = append(buf, byte(len(nalu)>>8), byte(len(nalu)))
		buf = append(buf, nalu...)
	}
	return buf
}

// packFUA 按 RFC 6184 把大 NALU 拆成多个 FU-A 包。
func (p *H264Packer) packFUA(nalu []byte) ([]*RTPPacket, error) {
	const fuaHeaderLen = 2 // FU indicator(1) + FU header(1)
	// 每个分片里， 能放的NAL净数据上限
	maxData := p.MTU - fuaHeaderLen
	if maxData <= 0 {
		return nil, errors.New("mtu too small for fu-a fragmentation")
	}

	nalHeader := nalu[0]
	data := nalu[1:] // 去掉原始 NAL header，分片里不重复写它
	n := len(data)
	numFrags := (n + maxData - 1) / maxData
	pkts := make([]*RTPPacket, 0, numFrags)

	for i := 0; i < n; i += maxData {
		end := i + maxData
		if end > n {
			end = n
		}

		// FU indicator：F/NRI 复制自原 NAL header，Type=28
		//NAL Header布局
		//bit7   bit6 bit5    bit4 bit3 bit2 bit1 bit0
		//F      NRI NRI      Type Type Type Type Type
		// 1110 保留最高3位
		fuIndicator := (nalHeader & 0xE0) | NALUTypeFUA
		//bit7   bit6   bit5   bit4~bit0
		//S      E      R      Type

		// FU header：S(1) E(1) R(1) + 原始 NAL Type(5)
		fuHeader := nalHeader & 0x1F
		if i == 0 {
			fuHeader |= 0x80 // 首包 S=1
		}
		if end == n {
			fuHeader |= 0x40 // 尾包 E=1
		}

		payload := make([]byte, 0, fuaHeaderLen+(end-i))
		payload = append(payload, fuIndicator, fuHeader)
		payload = append(payload, data[i:end]...)

		// marker 只打在尾包
		pkts = append(pkts, newPacket(p.PT, payload, end == n))
	}

	return pkts, nil
}

// newPacket 构造一个只填了 Version/PT/Marker 的 RTP 包，Seq/Timestamp/SSRC 由调用方填。
func newPacket(pt uint8, payload []byte, marker bool) *RTPPacket {
	return &RTPPacket{
		Header: RTPHeader{
			Version: 2,
			PT:      pt,
			Marker:  marker,
		},
		Payload: payload,
	}
}

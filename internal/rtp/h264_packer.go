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
	NALUTypeSTAPA  = 24 // STAP-A 聚合RTP包（本阶段不用）
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

// internal/media/sps_pps.go
package media

import (
	"encoding/base64"
	"fmt"
)

// GetSPSPPSBase64 返回 "base64(SPS),base64(PPS)"，供 SDP 的 sprop-parameter-sets 用
func GetSPSPPSBase64(filePath string) (string, error) {
	demux, err := NewH264Demux(filePath)
	if err != nil {
		return "", err
	}
	var sps, pps []byte
	for {
		nalu, err := demux.NextNALU()
		if err != nil {
			break // io.EOF 或没更多了
		}
		typ, _ := NALType(nalu)
		switch typ {
		case 7: // SPSQ //序列参数集
			sps = nalu
		case 8: // PPS 图像参数集
			pps = nalu
		}
	}
	if sps == nil || pps == nil {
		return "", fmt.Errorf("no SPS/PPS found")
	}
	return base64.StdEncoding.EncodeToString(sps) + "," +
		base64.StdEncoding.EncodeToString(pps), nil
}

// internal/media/sps_pps.go
package media

import (
	"encoding/base64"
	"fmt"
)

// SPSPPSBase64 把 SPS / PPS 编码成 "base64(SPS),base64(PPS)"，供 SDP 的
// sprop-parameter-sets 用。GetSPSPPSBase64 和实时源的参数集缓存共用这个格式。
func SPSPPSBase64(sps, pps []byte) string {
	return base64.StdEncoding.EncodeToString(sps) + "," +
		base64.StdEncoding.EncodeToString(pps)
}

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
	return SPSPPSBase64(sps, pps), nil
}

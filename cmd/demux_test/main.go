package main

import (
	"fmt"
	"io"

	"flow/internal/media"
)

func main() {
	demux, err := media.NewH264Demux("testdata/test.h264")
	if err != nil {
		panic(err)
	}

	idx := 0
	for {
		nalu, err := demux.NextNALU()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		typ, _ := media.NALType(nalu)
		idx++
		fmt.Printf("nal #%d | len=%4d | type=%d\n", idx, len(nalu), typ)
	}
	fmt.Println("done, total nalu =", idx)
}

package source

// Subscriber 是能被拉流端订阅的帧源。
//
// H264Source / AACSource / ExternalSource 都满足这个接口。
type Subscriber interface {
	Subscribe() (*Reader[Frame], func())
}

// LiveSubscriber 是支持「从当前直播位置订阅、不预热旧 GOP」的帧源。
//
// HLS 打包器必须用它：Subscribe 会先补 SPS/PPS + 最近一个 GOP，这些帧的
// 到达时刻是现在、PTS 是过去，会把 PDT 锚点整体推后；对低延迟也没有意义。
type LiveSubscriber interface {
	SubscribeLive() (*Reader[Frame], func())
}

var (
	_ Subscriber     = (*H264Source)(nil)
	_ LiveSubscriber = (*H264Source)(nil)
	_ Subscriber     = (*AACSource)(nil)
	_ LiveSubscriber = (*AACSource)(nil)
	_ Subscriber     = (*ExternalSource)(nil)
	_ LiveSubscriber = (*ExternalSource)(nil)
)

# flow

Go 实现的 RTSP / RTMP 流媒体服务器：本地文件或 stdin 摄像头作为媒体源，
RTSP（UDP / TCP interleaved）与 RTMP 按流名共享同一组源，任一协议推上来的流
另一种协议都能直接拉取。

## 构建与运行

```bash
go build ./...

go run ./cmd/server        # RTSP :8554，RTMP :1935
```

播放本地测试媒体（`testdata/` 会自动作为兜底源）：

```bash
ffplay rtsp://127.0.0.1:8554/live
ffplay -rtsp_transport tcp rtsp://127.0.0.1:8554/live
ffplay rtmp://127.0.0.1:1935/live/test
```

从摄像头推实时 H.264（`-live` 表示从 stdin 读 Annex-B 裸流）：

```bash
ffmpeg -f dshow -rtbufsize 100M -i video="USB Camera" \
       -c:v libx264 -preset ultrafast -tune zerolatency -f h264 pipe:1 | flow.exe -live
```

常用参数见 `go run ./cmd/server -h`（鉴权、重定向、组播、CSeq 校验等）。

## 目录结构

| 路径 | 说明 |
| --- | --- |
| `cmd/server/` | 服务入口：解析参数、组装媒体源、并发启动 RTSP 与 RTMP 服务 |
| `cmd/demux_test/` | 裸 H.264 文件解复用的小工具 |
| `internal/rtsp/` | RTSP 协议层：请求解析、会话、鉴权、SR/RR、NACK/PLI、组播等 |
| `internal/rtmp/` | RTMP 协议层：握手、Chunk、AMF0、publish/play |
| `internal/rtp/` | RTP 打包/解包，H.264 与 AAC |
| `internal/rtcp/` | RTCP 包解析：SR/RR、SDES、BYE、NACK |
| `internal/rtpbuf/`, `internal/plc/` | 接收侧抖动缓冲、丢包补偿 |
| `internal/flv/`, `internal/media/` | FLV 封装、H.264/AAC 解复用与 SDP |
| `internal/source/`, `internal/stream/`, `internal/session/` | 媒体源抽象、共享流注册表、会话管理 |
| `test/e2e/` | 端到端验收脚本 |
| `testdata/` | 测试用媒体文件 |
| `docs/` | 学习笔记与任务拆解 |

## 关于 Stage 1 ~ Stage 5

`Stage 1` ~ `Stage5` 是项目早期按阶段推进的单文件练习版本，已从 `dev` 主线移除，
完整代码保留在 **`stages` 分支**：

```bash
git switch stages
```

主线只需要 `cmd/` + `internal/` 下的包结构，不再保留重复的历史实现。
对应的学习大纲和自测题见 `docs/TASK.md`。

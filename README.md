# flow

Go 实现的 RTSP / RTMP / HTTP-FLV 流媒体服务器：本地文件或 stdin 摄像头作为
媒体源，三种协议按流名共享同一组源，任一协议推上来的流其他协议都能直接拉取。

## 构建与运行

```bash
go build ./...

go run ./cmd/server        # RTSP :8554，RTMP :1935，HTTP-FLV :8080
```

播放本地测试媒体（`testdata/` 会自动作为兜底源）：

```bash
ffplay rtsp://127.0.0.1:8554/live
ffplay -rtsp_transport tcp rtsp://127.0.0.1:8554/live
ffplay rtmp://127.0.0.1:1935/live/test
ffplay http://127.0.0.1:8080/live/test.flv
```

HTTP-FLV 支持跨域与防盗链：`-httpflv-cors` 配置允许的 Origin，
`-httpflv-referer` / `-httpflv-require-referer` 校验 Referer，
`-httpflv-token` 要求 `?token=` 或 `X-Flow-Token` 头；
`-httpflv-addr` 传空可关闭该服务。

RTMP 回源（从上游拉流并在本地重新发布，供 RTSP/RTMP/HTTP-FLV 下游拉取；
上游断开后自动重连，下游保持连接）：

```bash
# 拉 rtmp://upstream/live/src，本地按 relay 名字发布
./flow -pull "relay=rtmp://upstream/live/src"

# 可配置多路（逗号分隔）；省略本地名时取 URL 最后一节
./flow -pull "rtmp://upstream/live/a,rtmp://upstream/live/b"
ffplay rtmp://127.0.0.1:1935/live/relay
```

从摄像头推实时 H.264（`-live` 表示从 stdin 读 Annex-B 裸流）：

```bash
ffmpeg -f dshow -rtbufsize 100M -i video="USB Camera" \
       -c:v libx264 -preset ultrafast -tune zerolatency -f h264 pipe:1 | flow.exe -live
```

常用参数见 `go run ./cmd/server -h`（鉴权、重定向、组播、CSeq 校验等）。

压测：N 路并发 DESCRIBE/SETUP/PLAY，统计收包数、序号空洞和建连耗时；
`-transport udp` 可切到 UDP 收流，`-d` 控制时长。

```bash
go run ./cmd/loadtest -url rtsp://127.0.0.1:8554/live -n 20 -d 5m
```

性能分析（仅回环可访问，空值关闭）：

```bash
go run ./cmd/server -pprof :6060

# CPU 30s / heap / goroutine / block / mutex
curl -o cpu.pprof "http://127.0.0.1:6060/debug/pprof/profile?seconds=30"
curl -o heap.pprof "http://127.0.0.1:6060/debug/pprof/heap?gc=1"
curl "http://127.0.0.1:6060/debug/pprof/goroutine?debug=1"
curl -o block.pprof "http://127.0.0.1:6060/debug/pprof/block"
curl -o mutex.pprof "http://127.0.0.1:6060/debug/pprof/mutex"

# 浏览器里看 Top / Graph / Flame Graph
go tool pprof -http=127.0.0.1:8081 cpu.pprof
```

## 目录结构

| 路径 | 说明 |
| --- | --- |
| `cmd/server/` | 服务入口：解析参数、组装媒体源、并发启动 RTSP / RTMP / HTTP-FLV 服务 |
| `cmd/loadtest/` | 压测客户端：N 路并发拉流，统计收包/丢包/建连耗时 |
| `cmd/demux_test/` | 裸 H.264 文件解复用的小工具 |
| `internal/rtsp/` | RTSP 协议层：请求解析、会话、鉴权、SR/RR、NACK/PLI、组播等 |
| `internal/rtmp/` | RTMP 协议层：握手、Chunk、AMF0、publish/play，以及回源客户端（`-pull`） |
| `internal/httpflv/`, `internal/flvmux/` | HTTP-FLV 分发与 frame→FLV Tag 封装 |
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

## 许可证

本项目使用 [MIT License](LICENSE)。

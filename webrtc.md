# WebRTC（WHIP/WHEP）实现（P-4）

> 对应 `done.md` 的 **P-4 WebRTC（WHIP/WHEP）**：ICE/DTLS/SRTP、H.264 打包；
> 推流/拉流入口；验收：浏览器 WHIP 推、WHEP 拉，延迟 < 500ms 基线。
>
> 本文记录实现方案、关键取舍与本机验收数据，施工清单见 §8。

## 0. 结论先行

| 决策 | 选择 | 理由 |
| --- | --- | --- |
| 传输栈 | **pion/webrtc v4 全栈** | ICE/DTLS/SRTP/RTCP/NACK 都是协议细节，自研收益低、风险高；Pion 是纯 Go，和仓库“单二进制”一致 |
| 媒体面 | 只做 **RTP ↔ 注册表** 的桥接 | 沿用 `internal/rtp` 已有的 H.264/Opus 封包与解包，和 RTSP/RTMP/SRT 共用同一份帧源 |
| 编码 | **H.264 视频 + Opus 音频直通** | 浏览器 WebRTC 只稳定支持这两种；AAC 源在 WHEP 侧无音频，不做转码 |
| 信令 | WHIP/WHEP 的 HTTP SDP 交换 | 不自定义信令；一条 POST 就是一次会话，天然适合 CDN/边缘 |
| 入口 | `POST /whip/<name>`、`POST /whep/<name>` | 推流写注册表、拉流读注册表，其他协议立刻互通 |
| 时间戳 | WHIP 入口按首包归零 | 浏览器 RTP 起点随机；不归零会把 RTMP/FLV/HLS 下游时间戳推到巨大值 |
| 首帧 | WHEP 等 ICE/DTLS `connected` 再订阅 | 建连前写出的 RTP 会被静默丢弃；订阅时 `Subscribe()` 会补 SPS/PPS + 最近 GOP |

**非目标**：H.265/AV1/VP8 协商、AAC→Opus 转码、simulcast/SVC、ICE restart、
SFU 级联、按观众选层、TURN 鉴权轮换。

---

## 1. 本机验收（2026-11-19，Windows 11，Edge 157）

`python test/e2e/webrtc_e2e.py`（自建 flow + headless Edge）：

```
WHIP  201 location=/whip/live/e2e/fff7888d5e13bf5342d22a41d36232de
WHEP  201 location=/whep/live/e2e/56f81c843c575fbb7c87072d5162ace6
codec video: h264 | audio: opus / video: h264 | audio: opus
video 640x360, decoded 191 frames
latency p50=20ms p95=44ms max=1207ms (150 samples)
PASS
```

- 端到端延迟 = 画布绘制时刻 → 浏览器解码显示时刻，含 H.264 编码、SRTP 上行、
  服务端解包/重打包、SRTP 下行、抖动缓冲与解码。
- p50 ≈ 20ms、p95 ≈ 40–45ms，**远低于 500ms 验收线**。
- `max` 偶发 0.9–1.2s：单个帧的启动/追帧毛刺（每 150 帧出现 0 或 1 次），
  不影响持续延迟；页面上的条码带 CRC-8，误码帧已被丢弃，不是测量噪声。
- `--no-audio` 同样通过（p50 19ms / p95 39ms）。

单测（`go test -race ./internal/webrtc/`）：WHIP 视频/Opus 入站、WHEP
视频/Opus 出站、404、路径解析、token、CORS 全部通过。

指标（`/metrics`）：`flow_webrtc_sessions{kind="whip|whep"}` 为当前会话数，
`flow_webrtc_sessions_total{kind}` 为累计会话数，WHEP 计入 `ActiveViewers`。

---

## 2. WHIP / WHEP 规格要点

| 项 | 实现 |
| --- | --- |
| `POST /whip/<name>` | 读 `application/sdp` offer → answer，返回 `201 Created`、`Location: /whip/<name>/<32hex>`、`ETag: "<id>"`、`Cache-Control: no-store` |
| `POST /whep/<name>` | 同上，`Location: /whep/<name>/<id>` |
| `DELETE <Location>` | 结束会话，`204 No Content`，幂等（重复删除也是 204） |
| `PATCH <Location>` | 解析 `a=candidate:` 行并 `AddICECandidate`（best-effort；服务端 answer 默认已带完整候选） |
| ICE | 非 trickle：`answerSDP` 等 gathering 完成（`-webrtc-gather-timeout`，默认 3s）后返回；超时也会返回已收集的候选 |
| ICE 候选 | 默认收集物理网卡；`-webrtc-loopback` 额外收集 127.0.0.1；`-webrtc-public-ip` 做 NAT 1:1 地址改写；`-webrtc-ice-servers` 配 STUN/TURN；`-webrtc-udp-ports min:max` 固定端口段 |
| CORS | `-webrtc-cors`；预检声明 `POST, PATCH, DELETE, OPTIONS`，暴露 `Location, ETag, Content-Type` |
| 鉴权 | `-webrtc-token`，`?token=` 或 `X-Flow-Token`，常量时间比较；`Location` 保留 query，DELETE/PATCH 不用重新传 token |
| 安全 | 复用全局 `-whitelist-file`、连接数/速率限制不涉及（WebRTC 是 UDP 媒体面，HTTP 只做信令） |

路由细节：流名允许包含 `/`（与 RTSP 的 `live/cam` 一致），最后一个路径段是
32 位十六进制时才被当作会话 ID，否则整段都是流名。

---

## 3. 架构

### 3.1 包结构

| 文件 | 职责 |
| --- | --- |
| `internal/webrtc/server.go` | `Server`、MediaEngine/SettingEngine、HTTP 路由、CORS/token/白名单、会话表、`answerSDP` |
| `internal/webrtc/whip.go` | WHIP 入站：`OnTrack` → `ingestVideo`/`ingestAudio` → 注册表 |
| `internal/webrtc/whep.go` | WHEP 出站：按注册表编码建发送轨 → `playVideo`/`playAudio` → `WriteRTP` |
| `internal/webrtc/bridge.go` | `pion/rtp.Packet` ↔ `internal/rtp.RTPPacket` 字段拷贝（版本/PT/序号/时间戳/SSRC 原样保留） |
| `internal/webrtc/webrtc_test.go` | Pion 客户端做的双端集成测试 |
| `test/e2e/webrtc_e2e.html` | 浏览器验收页：canvas 时钟源 + WHIP 推 + WHEP 拉 + 时间码延迟测量 |
| `test/e2e/webrtc_e2e.py` | headless 浏览器探针：起服务、开页面、校验 SDP/编码/延迟/DELETE/指标 |

### 3.2 WHIP 推流数据流

```
浏览器 ── H.264/Opus RTP ──► Pion TrackRemote.ReadRTP
                              │  H264Depacketizer（单 NALU / STAP-A / FU-A）
                              ▼
                     按 RTP 时间戳聚合 AU（marker 或时间戳变化结尾）
                              │  时间戳减去首包基准
                              ▼
                     Stream.EnsureVideo()/EnsureAudio() → source.Frame
                              │  SPS/PPS 变化时 SetVideoParams
                              ▼
                       共享 stream.Registry
                              │
             RTSP / RTMP / HTTP-FLV / SRT / HLS / DASH / WHEP
```

- 每个 AU 内的 NALU 依次写入，`AUStart` 标在第一个、`AUEnd` 标在最后一个，
  `Keyframe` 由 IDR 判定；下游按 AU 合并的封装器（HTTP-FLV、fMP4）可直接用。
- Opus 没有分片，一个 RTP 包就是一个编码帧；`SetAudioCodec(Opus, 48000, ch, 48000)`。
- 参数集变化（SPS/PPS 不同）会更新 `Stream.VideoParams()`，HLS/DASH 的 init
  版本与 RTSP 的 sprop 会随之刷新。

### 3.3 WHEP 拉流数据流

```
offer（recvonly 的 m=video/m=audio）
      │  只在实际提议了 H.264/Opus 时 AddTrack
      ▼
TrackLocalStaticRTP（H264/90000、Opus/48000/2）
      │  等 PeerConnectionStateConnected（建连前 RTP 会被丢）
      ▼
Subscribe()（补 SPS/PPS + 最近 GOP）→ 按 AU 聚合 → internal/rtp H264Packer（MTU 1200）
      │  seq 从随机值开始，RTP 时间戳用注册表的相对时间戳
      ▼
track.WriteRTP()  → Pion 覆写 SSRC/PT，其余字段（seq/ts/marker）原样保留
```

- 每条拉流连接独立 packer/seq，互不影响；源帧时间戳已由上游归一化。
- 音频只发送 Opus 源；AAC/PCMA/PCMU/MP3 源在 WHEP 侧保持静音（视频不受影响）。
- `waitVideoSource` 最多等 5s，覆盖“WHEP 先连、WHIP 后推”的顺序。

### 3.4 会话生命周期

- `POST` 成功 → `startSession` 注册会话（指标 +1）。
- `DELETE` / 连接失败 / 连接关闭 → `cancel()` → `pc.Close()`；WHIP 额外从注册表
  `Unpublish`，WHEP 只退订。
- 进程退出（`Serve` 的 ctx 取消）关闭监听并 cancel 所有会话。
- WHIP 发布成功前失败（协商错误等）会撤销发布，不留下空流。

---

## 4. 关键取舍与踩过的坑

### 4.1 不写死 H.264 `profile-level-id`

MediaEngine 里注册的 H.264 只写
`level-asymmetry-allowed=1;packetization-mode=1`，不写 `profile-level-id`。
Pion 的 fmtp 精确匹配要求两侧都有 `profile-level-id`，缺失时会退化为
“编码名 + 时钟”的 partial match；因为服务端没有别的视频编码，partial match
足以接受 Chrome/Edge 的 42001f、42e01f、4d001f、64001f 等全部 profile，answer
里回显浏览器提议的 payload type 与 fmtp。

### 4.2 Pion 只有一个连接状态回调

`OnConnectionStateChange` 是“设置”而不是“追加”。WHEP 初版先注册一个关闭
`connected` 通道的回调，再调用 `watchConnection` 注册失败处理，后者把前者覆盖，
导致 `playVideo` 永远等到 10s 超时才退出。现在 `watchConnection(pc, cancel, onConnected)`
在一个回调里同时处理 `connected` 与 `failed/closed`。

### 4.3 `OnTrack` 在第一个 RTP 包之后触发

浏览器/Pion 的 unified-plan offer 不写 `a=ssrc`，服务端 `TrackRemote` 的 SSRC
为 0，Pion 要等首包到达并校验 SSRC 才调用 `OnTrack`。因此：

- 测试必须“先持续发帧、再等视频轨”，不能先等 `OnTrack` 再发；
- 真实浏览器推流天然持续发包，无影响；
- 想让 `OnTrack` 提前触发可开 `SettingEngine.SetFireOnTrackBeforeFirstRTP(true)`，
  但会用首包之外的启发式判断编码，暂不启用。

### 4.4 时间戳归一化

浏览器 RTP 时间戳是随机 32 位起点。WHIP 入口以每个轨的首包为基准做减法，
写进注册表的时间戳从 0 开始；否则 RTMP 的 32 位时间戳、HLS/DASH 的 fMP4
时间轴都会被推到一个巨大的值（甚至溢出）。

### 4.5 WHEP 建连后再订阅

`TrackLocalStaticRTP.WriteRTP` 在没有 binding 时静默成功但丢包。若在
`SetLocalDescription` 后立刻订阅并写入，前几百毫秒的 RTP 会丢失，浏览器
要等下一个关键帧。现在等 `PeerConnectionStateConnected` 再订阅；而
`Subscribe()` 的 GOP 预热保证了首个关键帧、SPS/PPS 完整送达。

### 4.6 MTU 1200

SRTP + DTLS + IP/UDP 头会吃掉约 60–100 字节，1200 留出余量，避免
IP 分片；`internal/rtp.H264Packer` 的 FU-A 分片按此 MTU 切分。

---

## 5. 配置项（`cmd/server`）

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `-webrtc-addr` | `:8084` | WHIP/WHEP HTTP 监听地址；空表示关闭 |
| `-webrtc-public-ip` | 空 | NAT 1:1 公网 IP（逗号分隔），改写 host 候选 |
| `-webrtc-ice-servers` | 空 | STUN/TURN URL，逗号分隔 |
| `-webrtc-ice-username` / `-webrtc-ice-credential` | 空 | 所有 ICE 服务器的统一凭据 |
| `-webrtc-udp-ports` | 空 | ICE UDP 端口段 `min:max`；空表示系统分配 |
| `-webrtc-loopback` | `false` | 收集 127.0.0.1 候选（本机浏览器/测试） |
| `-webrtc-cors` | 空 | 允许的 Origin，`*` 表示任意 |
| `-webrtc-token` | 空 | WHIP/WHEP token |
| `-webrtc-gather-timeout` | `3s` | 非 trickle answer 等待 ICE 收集的上限 |

示例：

```bash
go run ./cmd/server -webrtc-addr :8084 -webrtc-cors '*' -webrtc-loopback
# 浏览器打开 test/e2e/webrtc_e2e.html?base=http://127.0.0.1:8084&name=live/demo
```

`test/e2e/webrtc_e2e.py` 会自己 build 服务、挑选支持 H.264 的浏览器
（Chromium 无专有编解码器时自动改用 Edge），并完成全部断言。

---

## 6. 测试清单

| 测试 | 覆盖 |
| --- | --- |
| `TestWHIPIngest` | 客户端 H.264 推流 → NALU/AU 写入注册表、SPS/PPS 登记、DELETE 撤流 |
| `TestWHIPOpusIngest` | Opus 推流 → 负载原样入表、编码参数登记 |
| `TestWHEPPlayback` | 注册表 H.264 源 → WHEP 客户端收齐 SPS/PPS/IDR |
| `TestWHEPOpusPlayback` | Opus 源 → WHEP 负载原样送达 |
| `TestWHEPMissingStream` | 未知流返回 404 |
| `TestParsePath` | 流名可含 `/`，32 位十六进制才是会话 ID |
| `TestTokenGate` / `TestCORS` | token 与预检 |

e2e（`test/e2e/webrtc_e2e.py`）另外校验：201 + `Location`/`ETag`、协商编码为
H.264（从 answer 的 `m=` 段解析）、解码帧持续增长、端到端延迟 p95 < 500ms、
DELETE 返回 204、`flow_webrtc_sessions*` 指标回落。

---

## 7. 局限

- 只协商 H.264 与 Opus；VP8/VP9/AV1 的浏览器（无 H.264 的 Chromium 构建）
  会收到没有视频的 answer（页面上会明确报错，探针会自动换 Edge）。
- 不做 AAC→Opus：摄像头 RTSP 流（AAC）在 WHEP 侧只有视频。
- 单 SSRC/单层，不做 simulcast、SVC、ABR。
- 不支持 ICE restart / renegotiation；一条 POST 对应一条连接，重连需重新 POST。
- PATCH 只在 answer 之后补候选，不是完整 trickle ICE。
- 未做 TURN 长连接凭据轮换与鉴权服务端。
- 无拥塞控制自适应（浏览器侧是 `transport-cc`，服务端按固定码率发送；
  弱网下依赖浏览器的 NACK/PLI 与抖动缓冲）。

---

## 8. 施工清单

- [x] P-4-1 `internal/webrtc` 骨架：MediaEngine（H.264 + Opus）、SettingEngine
      （loopback/NAT/端口段）、HTTP 路由与 CORS/token/白名单。
- [x] P-4-2 WHIP：SDP 交换、`OnTrack`、H.264 解包与 AU 聚合、Opus 帧、时间戳归一化。
- [x] P-4-3 WHEP：按 offer 建轨、等建连后订阅、H.264/Opus 打包、MTU 1200。
- [x] P-4-4 生命周期与指标：会话表、DELETE/PATCH、`flow_webrtc_sessions*`、
      `cmd/server` 参数接入。
- [x] P-4-5 单测：双向 H.264/Opus、404、路由、token、CORS。
- [x] P-4-6 浏览器验收：`webrtc_e2e.html` + `webrtc_e2e.py`，延迟 p95 < 500ms。
- [x] P-4-7 文档：本文 + README + `done.md`。

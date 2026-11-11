# flow.md — 这个项目要学什么（课堂版学习路线）

> 这份文档不是任务清单（任务在 `done.md`），而是**知识地图**：
> 告诉你「已经写出来的代码背后对应哪些协议知识」「接下来做推流端之前必须先懂什么」。
>
> 用法：下午上课时按第 1 节的 90 分钟安排往下读，每个知识点先问自己一遍，
> 答不上来再翻代码和 `docs/rtsp.md`。晚上回宿舍再动手验证。

---

## 0. 先记住这一张全景图

```
                    RTSP（TCP，纯文本，遥控器）
    ffplay  ──OPTIONS/DESCRIBE/SETUP/PLAY/PAUSE/TEARDOWN──▶  flow server
       │                                                        │
       │  SDP（DESCRIBE 的 body，文本说明书）                     │
       │◀───────────────────────────────────────────────────────┘
       │
       │  RTP（UDP 或 TCP interleaved，媒体数据）
       │◀───────────────────────────────────────────────────────┐
       │  RTCP（SR 发送者报告，NTP↔RTP 映射、包数/字节数）        │
       │◀───────────────────────────────────────────────────────┘
       
     source goroutine ──▶ RingBuffer[Frame] ──▶ 每个客户端 reader
     (读文件/stdin)         (单源多消费者)         └─▶ H264Packer/AACPacker ──▶ RTP
```

四层职责，面试必须能一句话分清：

| 协议 | 跑在哪 | 干什么 | 传不传视频 |
| --- | --- | --- | --- |
| RTSP | TCP | 会话控制：协商、播放、暂停、停止（有状态） | ❌ 纯文本信令 |
| SDP | RTSP 的 body | 文本说明书：几条轨、什么编码、时钟频率、`control` | ❌ |
| RTP | UDP（或 TCP 复用） | 装 H264/AAC 媒体帧，带 seq/timestamp/SSRC | ✅ |
| RTCP | UDP（或 TCP 复用） | 质量反馈、发送者报告、音视频同步 | ❌ 只传统计 |

---

## 1. 课堂上 90 分钟怎么用（照这个顺序）

| 时间 | 主题 | 读什么 | 课堂动作 |
| --- | --- | --- | --- |
| 0–15min | RTSP 状态机 + 请求解析 | `docs/rtsp.md` 前半 + `internal/rtsp/handler.go:64-88` | 手画 OPTIONS→TEARDOWN 时序 |
| 15–35min | RTP 头 + H.264 封包 | `internal/rtp/rtp.go`、`h264_packer.go:58-105` | 纸上画一次 FU-A 分片 |
| 35–50min | AAC + SDP | `internal/media/aac_demux.go:30-77`、`aac_packer.go`、`handler.go:124-154` | 默写 SDP 两条 `m=` 行 |
| 50–65min | RTCP SR + 两种传输 | `internal/rtcp/rtcp.go`、`session.go:148-176`、`conn.go:75-94` | 算一遍 NTP↔RTP 映射 |
| 65–80min | Go 并发与生命周期 | `source/ring.go`、`session.go:255-285`、`session/manager.go` | 画出 goroutine 谁 cancel 谁 |
| 80–90min | 推流端设计预习 | `done.md` 的 P1 + `rtsp/sdp.go` | 纸上设计 RECORD 流程 |

> 只够吃透 P0。P1/P2 的知识当预习，不用今天全背。

---

## 2. P0：必须吃透的 7 块知识

### 2.1 RTSP 信令与状态机

**必须能说清：**

- 方法顺序：`OPTIONS → DESCRIBE → SETUP → PLAY → PAUSE → PLAY → TEARDOWN`
- `CSeq`：请求/响应配对；**新请求 +1，重传不变**
- `Session`：SETUP 响应里由服务端下发，之后的 PLAY/PAUSE/TEARDOWN 必须带；OPTIONS/DESCRIBE 不带
- `Transport`：SETUP 才协商
  - UDP：`RTP/AVP;unicast;client_port=9000-9001`，服务端回 `server_port=...`
  - TCP：`RTP/AVP/TCP;unicast;interleaved=0-1`
- 状态机：`Init → Playing ⇄ Paused → Teardown`
- 本项目的状态码语义：
  - `400` 请求格式错 / `404` 没有这条轨
  - `415` 编码不支持（ANNOUNCE）
  - `454` Session 不存在
  - `455` 当前状态不允许该操作
  - `501` 方法没实现

**关键代码：**

- 方法分发：`internal/rtsp/handler.go:64-88`
- 请求头 + body 读取：`internal/rtsp/request.go:65-104`
- 会话状态与超时：`internal/session/manager.go`

**这一阶段最值得理解的 3 个设计点：**

1. **PAUSE 的语义**：停推流 goroutine，但**保留** Session、SSRC、seq、UDP 端口、源订阅。见 `session.go:255-285`。
2. **PLAY 幂等**：`Playing` 状态重复 PLAY 只回 200，不再起第二组 goroutine，否则 RTP 速率翻倍。见 `handler.go:336-388`。
3. **一次 PLAY 一个生命周期**：`playCtx` 的父是 `sess.Context()`；PAUSE 调 `stopPlay()`，TEARDOWN 级联取消。这是理解 context 树的最好例子。

**自测：**

- [ ] PAUSE 之后，服务端哪些东西必须保留？哪些必须停？
- [ ] 连续两次 PLAY，为什么不能起两组推流 goroutine？
- [ ] `454` 和 `455` 分别在什么情况下返回？
- [ ] 一个 TCP 连接上能不能有多个 Session？（本项目 `Conn.bindSession` 允许多个）

---

### 2.2 RTP 固定头与时间戳

**12 字节头，逐位默写：**

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|V=2|P|X|  CC   |M|     PT      |       sequence number         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                           timestamp                           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                           SSRC                                |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

**必须懂：**

- 全大端；`PT` 7 bit；`seq` 16 bit 会回绕
- `Marker`：**一帧的最后一个 RTP 包**置 1，不是每个分片都置
- `SSRC`：一路流全程固定，**PAUSE 后也不能变**
- `timestamp` 不是毫秒，是采样点计数：
  - 视频：90000 Hz，25fps → 每帧 `+3600`
  - 音频：= 采样率（44100/48000），AAC LC 每帧 1024 个采样 → 每帧 `+1024`，**和帧率无关**
- 本项目 `trackStats.ssrc/seq` 跨 PAUSE 保活，见 `internal/rtsp/session.go:46-75`

**关键代码：** `internal/rtp/rtp.go`（Marshal/Unmarshal）、`internal/rtsp/stream.go`

**自测：**

- [ ] 为什么 RTP 自带 seq 和 timestamp，而不是靠 TCP？
- [ ] 25fps、45fps 时 timestamp 步进分别是多少？
- [ ] seq 回绕到 0 之后，接收端怎么判断丢包？
- [ ] 一个 640x480 的 IDR 帧 30KB，会被切成多少个 RTP 包（MTU 1400）？

---

### 2.3 H.264 over RTP（RFC 6184）

**三层概念别混：**

1. **Annex-B**：文件/裸流格式，NALU 前加起始码 `00 00 01` 或 `00 00 00 01`
2. **NALU**：`[1 字节 NAL Header][RBSP 数据]`，Header = `F(1) NRI(2) Type(5)`
3. **RTP 载荷**：**不带起始码**，直接是裸 NALU；大包走 FU-A

**NAL Type 必记：**

| Type | 含义 | 用途 |
| --- | --- | --- |
| 1 | non-IDR | 普通 P 帧 |
| 5 | IDR | 关键帧 |
| 7 | SPS | 序列参数集（分辨率/profile） |
| 8 | PPS | 图像参数集 |
| 24 | STAP-A | 多个小 NALU 聚合 |
| 28 | FU-A | 大 NALU 分片 |

**FU-A（重中之重）：**

```
RTP 头(12) + FU indicator(1) + FU header(1) + 分片数据
FU indicator: F|NRI|Type=28
FU header:    S|E|R|原始Type
```

- 首片 `S=1`，尾片 `E=1`，中间都 0
- **Marker 只打在尾片**
- FU-A 分片里**不重复写原始 NAL header**
- MTU：以太网 1500 − IP 20 − UDP 8 ≈ 1472，工程取 1400

**关键代码：**

- Annex-B 切 NALU：`internal/media/h264_demux.go:73-108`（文件）、`:127-161`（实时流）
- 封包：`internal/rtp/h264_packer.go:58-105`
- 解包/重组：`internal/rtp/h264_depacketizer.go`

**自测：**

- [ ] SPS/PPS 为什么要在每个 IDR 前重发？
- [ ] FU-A 的 S/E 和 RTP 的 Marker 有什么区别？
- [ ] 中途加入的播放器为什么要先补 SPS/PPS/IDR？（见 `source/h264.go` 的 preload）
- [ ] STAP-A 和 FU-A 分别在什么场景用？

---

### 2.4 AAC over RTP（RFC 3640）

**四个东西严格区分：**

| 概念 | 存在于 | 作用 |
| --- | --- | --- |
| ADTS 头 | 本地 `.aac` 文件 | 分隔 AAC 帧（7 或 9 字节） |
| AU | 一帧 AAC | Access Unit |
| AU-header | RTP 载荷内部 | 描述 AU 大小/序号 |
| AudioSpecificConfig | SDP `config=` | 2 字节，解码器初始化参数 |

**ADTS 必记：** `syncword=0xFFF`、`profile`（LC=1）、`sampling_frequency_index`（查表，不是采样值）、`channel_config`、`frame_length`（含头）。

**RTP 载荷布局（AAC-hbr，一 AU 一包）：**

```
AU-headers-length (16bit，单位 bit，单 AU 时 = 16)
AU-header         (16bit = Size 13bit << 3 | Index 3bit)
AAC ES data       (剥掉 ADTS 头的裸 AAC)
```

- **RTP 里不能带 ADTS 头**
- timestamp 每帧 `+1024`

**关键代码：** `media/aac_demux.go`、`rtp/aac_packer.go`、`rtp/aac_depacketizer.go`、`media/aac_sdp.go`

**自测：**

- [ ] ADTS 的 `profile` 和 AudioSpecificConfig 的 `audioObjectType` 差几？
- [ ] `AU-headers-length` 的单位是字节还是 bit？
- [ ] 为什么音频 timestamp 不能用 90000？
- [ ] 44.1kHz 下每帧多少毫秒？一秒钟多少帧？

---

### 2.5 RTCP SR（发送者报告）

**SR 的作用：**

1. 告诉客户端「我发了多少包、多少字节」
2. 给出 **NTP 墙上时间 ↔ RTP timestamp** 的映射，播放器靠它做音视频同步
3. 有些播放器收不到 SR 会判定发送方失联

**必须懂：**

- `PT=200`，`V=2`
- `length` 字段 = **32 位字数 − 1**；28 字节 SR → 填 6
- NTP 纪元是 **1900**，Go 的 Unix 纪元是 1970，差 `2208988800` 秒
- `octet count` 只算负载字节，**不含 RTP 头**
- 本项目 SR 的 RTP timestamp 推算：`上一包 RTP ts + 经过时间 × clockRate`，见 `session.go:148-176`

**关键代码：** `internal/rtcp/rtcp.go`、`internal/rtsp/stream.go:111-139`

**自测：**

- [ ] SR 里两个时间戳分别是什么？怎么算出音视频偏差？
- [ ] 为什么 SR 的 `length` 要减 1？
- [ ] 音频轨的 `clockRate` 为什么不能写死 90000？
- [ ] 本项目对客户端发来的 RR 做了什么？（提示：只打活跃点，没算丢包）

---

### 2.6 SDP：DESCRIBE 和 ANNOUNCE 的两个方向

**最小视频 SDP：**

```
m=video 0 RTP/AVP 96
a=rtpmap:96 H264/90000
a=fmtp:96 packetization-mode=1; sprop-parameter-sets=<base64 SPS>,<base64 PPS>
a=control:trackID=0
```

**最小音频 SDP：**

```
m=audio 0 RTP/AVP 97
a=rtpmap:97 mpeg4-generic/44100/2
a=fmtp:97 streamtype=5; profile-level-id=1; mode=AAC-hbr;
  SizeLength=13; IndexLength=3; IndexDeltaLength=3; config=1210
a=control:trackID=1
```

**必须懂：**

- `m=` 里的端口写 0 是正常做法，真正端口在 SETUP 协商
- `sprop-parameter-sets` = base64(SPS),base64(PPS)，让解码器握手期就拿到参数集
- `config=` = AudioSpecificConfig，2 字节十六进制
- `a=control` 是 SETUP URI 的索引：`trackID=0` / `streamid=0`
- **方向不同**：
  - DESCRIBE 的 SDP：服务端 → 客户端，描述「我要发给你的流」
  - ANNOUNCE 的 SDP：客户端 → 服务端，描述「我要推给你的流」
  - 两者的 `m=`/`rtpmap`/`fmtp` 结构一样，但解析目的不同

**关键代码：** `handler.go:124-154`（生成）、`rtsp/sdp.go`（解析 ANNOUNCE）、`media/aac_sdp.go`

**自测：**

- [ ] `packetization-mode=0` 和 `=1` 区别？
- [ ] 没有 `sprop-parameter-sets` 时播放器靠什么初始化？
- [ ] ANNOUNCE 的 SDP 解析失败，400 和 415 怎么区分？
- [ ] `a=control:streamid=0` 和 `trackID=0` 在 SETUP 时怎么映射到同一条轨？

---

### 2.7 UDP 模式 vs TCP interleaved 模式

**UDP 模式：**

- SETUP 协商 `client_port=RTP-RTCP`，服务端开一对端口（偶数 RTP、奇数 RTCP），回 `server_port`
- RTP 和 RTCP 各自独立 UDP socket
- 优点：延迟低；缺点：NAT/防火墙容易不通

**TCP interleaved 模式：**

- RTP 复用在 RTSP 这条 TCP 上，用 `$` 帧包裹：

```
$ (0x24) + channel(1B) + length(2B 大端，只算 payload) + RTP/RTCP 包
```

- 视频 RTP=0、RTCP=1；音频 RTP=2、RTCP=3（常见约定）
- 服务端读循环先 `Peek(1)`：`$` 走二进制帧，字母走 RTSP 文本
- **所有写必须过同一把锁**，否则音视频 goroutine 会写穿插

**关键代码：** `conn.go:75-94`、`handler.go:96-120`、`session.go:106-146`、`handler.go:282-318`

**自测：**

- [ ] `$` 帧的 length 含不含这 4 字节头？
- [ ] 为什么摄像头/NAT 环境更常用 interleaved？
- [ ] 同一条 TCP 上，怎么区分「RTSP 响应」和「RTP 数据」？
- [ ] 音视频两个 goroutine 同时写 conn 会出什么问题？本项目怎么解决？

---

### 2.8 Go 并发与资源生命周期（这个项目的工程核心）

**要能吃透的 5 个模式：**

1. **单源多消费者 + RingBuffer**
   - 源 goroutine 只写、永不阻塞；每个客户端一个 `Reader` 游标
   - 慢客户端被覆盖老帧，只增加自己的 `Dropped`，不拖慢整体
   - 见 `internal/source/ring.go`

2. **context 树**
   - `SIGINT → Serve(ctx)` 关 listener
   - `Session.Context()` → PLAY 的 `playCtx`
   - `PAUSE → cancel(playCtx)`；`TEARDOWN/超时 → cancel(session ctx)` 级联

3. **连接写串行化**
   - `Conn.mu` 保护 RTSP 响应和 `$` 帧；`writeRTP/writeRTCP` 都走 `Conn.write`

4. **会话回收双保险**
   - 正常：TEARDOWN 删除
   - 异常：TCP 断开时 `handleConn` defer 立即回收；兜底 `Manager.Run` 每 30s 扫一次，60s 无活跃就回收
   - 活跃 = max(最后 RTCP, 最后 RTSP 请求)

5. **优雅退出**
   - 关 listener → 关所有 conn → 关所有 session → 等 goroutine（最多 5s）
   - 见 `rtsp/server.go:125-146`

**自测：**

- [ ] 为什么 RingBuffer 的 Write 不用等读者？
- [ ] 中途加入的读者从哪里开始读？preload 解决了什么问题？
- [ ] `Reader.Next(done)` 返回 false 的三种情况？
- [ ] kill -9 客户端后，会话为什么能很快回收？

---

## 3. P1：推流端（ANNOUNCE / RECORD）要提前懂的知识

这一节对应 P1-6 ~ P1-9：推流 SETUP/RECORD 接包、RTP 解包写 ExternalSource、拉流端订阅推流源转发、推流断开后回收会话并让拉流端收到流结束都已完成。

### 3.1 完整推流时序

```
ffmpeg                                flow server
  │  OPTIONS ─────────────────────────▶
  │  ◀───────────────────────── 200 (Public)
  │  ANNOUNCE + SDP ──────────────────▶  解析轨道、建 Session
  │  ◀───────────────────────── 200 + Session
  │  SETUP (video, interleaved=0-1) ──▶  记录通道，不开 UDP
  │  ◀───────────────────────── 200 + Transport + Session
  │  SETUP (audio, interleaved=2-3) ──▶
  │  ◀───────────────────────── 200 + ...
  │  RECORD ──────────────────────────▶  开始接收 $ 帧
  │  ◀───────────────────────── 200
  │  $0 RTP 视频 ─────────────────────▶  解包 → 写 ExternalSource
  │  $2 RTP 音频 ─────────────────────▶  解包 → 写 ExternalSource
  │  ...
  │  TEARDOWN ────────────────────────▶  停接收、关源、删会话
```

### 3.2 做 P1 之前必须想清楚的 6 个问题

1. **推流 SETUP 和拉流 SETUP 怎么区分方向？**
   - 推流端在 `Transport` 里给 `interleaved`，服务端**收**数据；拉流端是服务端**发**数据。同一个 `extractTransportInfo` 需要多一个 direction/role 概念。

2. **RTP 怎么路由到 track？**
   - 优先用 RTP 头的 **PT** 匹配 ANNOUNCE 的 `rtpmap`，再用 **SSRC** 绑定到具体轨；`a=control` 只解决 SETUP 阶段。

3. **seq 乱序/缺口怎么处理？**
   - 只记日志、丢弃当前 FU-A 半成品，不 panic；`H264Depacketizer.Dropped` 已经做了这件事。

4. **推流数据写到哪里？**
   - `ExternalSource.Write(Frame)`；拉流端 `Subscribe()` 拿同一个源，中途加入靠 SPS/PPS/IDR preload。
   - **已落地的架构**：`Server.published map[streamPath]*publishedStream`（`internal/rtsp/publish.go`）。`publishedStream` 持有 ANNOUNCE 的轨道描述和每类轨一个 `ExternalSource`；ANNOUNCE 时登记，拉流 DESCRIBE/SETUP 按 URL 路径查找。不同路径可并存多路推流，同一路径重复 ANNOUNCE 视为重新发布。

5. **拉流 DESCRIBE 的 SDP 从哪来？**
   - `buildSDP(uri)` 优先按 URL 找 `publishedStream`，用它的 `announcedTrack`（rtpmap/fmtp、sprop-parameter-sets、AAC config）生成 SDP；找不到才退回本地 `testdata`/live 源。

6. **推流断开怎么让拉流端感知？**
   - TEARDOWN / TCP 断开 → 关闭 `ExternalSource` → 所有 reader 读完剩余帧后返回 false → 推流 goroutine 退出。不能只删会话不关源。

### 3.3 需要补读的协议细节

- RTSP `RECORD` 的语义（RFC 2326 §10.11）
- ANNOUNCE 的 SDP 可带 Session 头更新已有会话（本项目已支持）
- 推流 UDP 模式下 `client_port`/`server_port` 的方向与拉流相反
- RTSP 状态机在推流端的补充：`Init → Ready → Recording`

---

## 4. P2：后续知识（按 `done.md` 的任务对应）

| 任务 | 要学的知识 | 优先级 |
| --- | --- | --- |
| A-1/A-2/A-3 | HTTP Basic / Digest 鉴权：`nonce`、`HA1=MD5(user:realm:pass)`、`HA2=MD5(method:uri)`、`qop=auth`、`stale` | 高（部署前必须） |
| A-4 | `GET_PARAMETER` 保活语义 | 高 |
| R-1/R-7 | 发送缓存 + NACK（PT=205, FMT=1）：PID/BLP 位图、按 seq 重传 | 中高 |
| R-8 | PLI（PT=206, FMT=1）/ FIR（FMT=4）：请求关键帧、限频 | 中高 |
| R-4/R-6 | jitter buffer：按 seq 重排、playout 延迟、缺口超时 | 中 |
| C-1/C-2/C-3/C-4 | SDES CNAME、BYE、RR report block（fraction lost/cumulative lost/jitter/LSR/DLSR）、RTT 计算 | 中 |
| R-9/R-10/R-11 | RTX、FEC、TWCC/GCC | 选做 |
| R-12 | RTP/RTCP mux：SDP `a=rtcp-mux`，单端口按 PT 区分 | 选做 |
| E-1~E-4 | pprof、压测、Prometheus 指标 | 中 |
| E-5/E-6/E-8 | 连接/会话上限、TLS（RTSPS）、IP 白名单 | 中 |
| G-1~G-3 | Range、文件 seek、Scale | 低（文件模式） |
| H-1/D-1/D-2 | HTTP tunneling、Redirect、组播 | 低 |

**学习建议：** 一次只攻一个协议特性，先找 RFC 对应章节，再对照 `gortsplib`/`pion` 的实现看数据结构，最后写单测。

---

## 5. 代码阅读顺序（不要从 main 一路读到尾）

1. `cmd/server/main.go` — 入口、两种模式（文件 / live）
2. `internal/rtsp/server.go` — 监听、优雅退出、会话表
3. `internal/rtsp/handler.go` — 方法分发与各方法语义（**读 3 遍**）
4. `internal/rtsp/request.go` — 文本解析、Content-Length、Transport 解析
5. `internal/rtsp/conn.go` — `$` 帧、写锁、连接↔会话绑定
6. `internal/rtsp/session.go` — mediaTrack、seq/SSRC、PLAY 生命周期、SR
7. `internal/rtsp/stream.go` — 每个客户端的推流循环
8. `internal/source/{ring,h264,aac,external}.go` — 生产者/消费者模型
9. `internal/media/{h264_demux,aac_demux,spa_pps,aac_sdp}.go` — Annex-B/ADTS 解析
10. `internal/rtp/{rtp,h264_packer,aac_packer,h264_depacketizer,aac_depacketizer}.go` — 编解码核心
11. `internal/rtcp/rtcp.go` — SR 序列化
12. `internal/session/manager.go` — 超时回收

---

## 6. 课堂纸笔练习（不用电脑就能做）

1. **画 FU-A**：给定一个 5000 字节的 IDR（含 1 字节 NAL header），MTU=1400，画出每个 RTP 包的 `S/E/M`：
   - 净数据每片 `1400−2=1398` 字节
   - 5000−1=4999 字节数据 → 4 片（1398×3 + 805）
   - 只有第 4 片 `M=1`，第 1 片 `S=1`，第 4 片 `E=1`

2. **算时间戳**：25fps 视频、44.1kHz AAC，10 秒后各自的 RTP timestamp 增量是多少？

3. **写 SDP**：手写 video(H264, PT96) + audio(AAC, PT97, 44100, stereo) 两条 `m=`，带 `control` 和 `config`。

4. **算 Digest**：`realm=flow, user=u, pass=p, nonce=abc, method=DESCRIBE, uri=rtsp://127.0.0.1:8554/live, nc=00000001, cnonce=xyz, qop=auth`，写出 `HA1/HA2/response` 三个公式（不用真算 MD5）。

5. **画 context 树**：`SIGINT → Serve ctx → Session ctx → play ctx`，标出 PAUSE / TEARDOWN 各 cancel 哪一层。

---

## 7. 自测题集中营（先答再翻文档）

**协议基础**

- [ ] RTSP / SDP / RTP / RTCP 各自职责？谁传视频？
- [ ] CSeq 的作用？为什么重传不变、新请求 +1？
- [ ] SessionID 在哪一步下发？哪些方法必须带？
- [ ] 为什么控制面用 TCP、媒体面用 UDP？HLS 为什么反过来？
- [ ] 为什么视频时钟是 90000？音频为什么用采样率？
- [ ] Marker 位和 FU-A 的 S/E 位有什么区别？
- [ ] SPS/PPS 不重发会怎样？中途加入的播放器缺什么？
- [ ] ADTS 和 AU-header 的区别？RTP 里为什么不能带 ADTS？
- [ ] SR 的 NTP 和 RTP timestamp 怎么用于音视频同步？
- [ ] sdp 的 `sprop-parameter-sets` 和 `config=` 分别是什么？

**本项目**

- [ ] PAUSE 后保留什么、停掉什么？seq/SSRC 为什么不能重置？
- [ ] 重复 PLAY 为什么必须幂等？
- [ ] `$` 帧格式？length 含不含头？
- [ ] RingBuffer 为什么慢消费者不影响源？`Dropped` 怎么算？
- [ ] kill -9 客户端后，服务端靠哪两条路径回收会话？
- [ ] 一个 TCP 连接能不能带多个 Session？
- [ ] 文件模式和 live 模式的时间戳来源有什么不同？
- [ ] ANNOUNCE 的 SDP 和 DESCRIBE 的 SDP 方向有什么不同？

**推流设计（做完 P1 后回看）**

- [ ] RTP 按什么字段路由到 track？
- [ ] 多路推流时 `ExternalSource` 该怎么组织？
- [ ] RECORD 收到缺片时怎么处理？为什么不 panic？
- [ ] 推流断开后，拉流端怎么知道流结束？

---

## 8. 当前实现的边界（别学错 / 面试别吹过头）

- ✅ 拉流已完整：文件 + live stdin、UDP + TCP interleaved、视频 + 音频、SR、PAUSE 语义、幂等 PLAY、优雅退出
- ✅ ANNOUNCE/SETUP/RECORD + RTP 收包、解包、写 ExternalSource 已通（P1-6/P1-7）；拉流端按 URL 找到已发布流、订阅同一个 `ExternalSource` 转发（P1-8），DESCRIBE 用 ANNOUNCE 的编码参数
- ⚠️ RTCP 只发 SR；客户端发来的 RR 只刷新活跃时间，**不统计丢包/抖动**（C-3/C-4 未做）
- ⚠️ 客户端 NACK(205)/PLI(206) 目前在 `handleInterleaved` 中被忽略（只认 200~204）
- ⚠️ 无鉴权、无连接上限、无 TLS（A/E 段未做）
- ⚠️ CSeq 只解析不校验顺序/重放
- ⚠️ live 模式没有音频；DESCRIBE 实时模式不返回 `sprop-parameter-sets`（靠带内参数集）
- ⚠️ 文件/live 模式仍用全局源；推流模式按 URL 注册，多路推流靠不同路径区分，同一路径只保留最后一路
- ✅ 推流端 TEARDOWN / TCP 断开时立即删除会话、关闭 ExternalSource、注销已发布流；拉流端检测到源结束会主动关闭 RTSP 连接，客户端读到 EOF（P1-9）

---

## 9. 参考资料（只读需要的章节）

| 资料 | 读什么 |
| --- | --- |
| RFC 2326 (RTSP 1.0) | §10 方法语义、§12 头字段、§10.12 interleaved |
| RFC 3550 (RTP/RTCP) | §5 RTP 头、§6 RTCP SR/RR、§6.4 时间戳 |
| RFC 6184 (H.264/RTP) | §5.3 单 NALU、§5.4 STAP-A、§5.8 FU-A |
| RFC 3640 (AAC/RTP) | §3.2 AU-header、§3.3 AAC-hbr |
| RFC 4566 (SDP) | 只需认 `m= rtpmap fmtp control` 和方向 |
| `docs/rtsp.md` | 项目自己的精华笔记，先读它 |
| `docs/TASK.md` | Stage 1~5 的学习要点和自测题 |
| gortsplib / pion/rtp | 对照实现，只看不 import |

**验证命令（晚上动手）：**

```bash
go test ./...
go test -race ./internal/...
go run ./cmd/server
ffplay rtsp://127.0.0.1:8554/live
ffplay -rtsp_transport tcp rtsp://127.0.0.1:8554/live
python test/e2e/rtsp_pause_play.py
# 抓包：tshark -f "tcp port 8554" -Y "rtsp || rtp || rtcp"
```

---

## 10. 一句话总结

**RTSP 管「什么时候发」，SDP 管「发的是什么」，RTP 管「怎么切包发」，RTCP 管「发得怎么样」，Go 的 goroutine/context/ring buffer 管「怎么并发地发而不乱」。**

把第 2 节的 8 个主题吃透，这个项目就能讲清楚；把第 3 节的推流时序吃透，下一段代码就能自己设计。

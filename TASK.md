# Go 从零手写 RTSP 流媒体服务器 —— 实践任务清单

> 目标：不引入 `gortsplib`，自己用 Go 标准库撸一个能被 VLC / ffplay 直接拉流播放的 RTSP Server，支持 UDP-RTP 推 H.264+AAC、Interleaved TCP 模式，最终写进简历并能扛住面试深挖。
>
> 学习方式：**每完成一个 Stage 就打勾 ✅，写一份踩坑笔记（`notes/stageN.md`），再进下一个 Stage**。不要跳步。

---

## 0. 全局信息

| 项目                            | 值                                                                                                                            |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| 仓库名                          | `rtsp-from-zero`                                                                                                              |
| Go 版本                         | 1.22+（用 `go.work` 或单 module 都行）                                                                                        |
| 监听端口                        | 默认 `8554`（可改）                                                                                                           |
| 拉流地址                        | `rtsp://127.0.0.1:8554/live`                                                                                                  |
| 验证工具                        | VLC / ffplay（必须装）、Wireshark（抓 RTSP+RTP 包）                                                                           |
| 参考库（**只对照，不 import**） | `github.com/bluenviron/gortsplib`、`github.com/pion/rtp`                                                                      |
| 数据源                          | 先用本地一个 H.264 Annex-B 裸流文件（`test.h264`）+ 一个 AAC ADTS 文件（`test.aac`），用 ffmpeg 合成一路"假摄像头"喂给 server |

### 0.1 目录结构（建议）

```
rtsp-from-zero/
├── go.mod
├── cmd/server/main.go          # 入口
├── internal/
│   ├── rtsp/                   # RTSP 协议层（解析请求/组响应/状态机）
│   ├── rtp/                    # RTP 封包器（H264 / AAC）
│   ├── session/                # 会话管理（SessionID、超时、多消费者）
│   ├── sdp/                    # SDP 构造
│   └── media/                  # 数据源（读 h264/aac 文件，喂给封包器）
├── testdata/
│   ├── test.h264
│   └── test.aac
├── notes/
│   └── stageN.md              # 每阶段踩坑笔记
└── TASK.md
```

### 0.2 准备测试素材（一次性）

```powershell
# 合成一个 10 秒、分辨率 640x480 的测试视频，抽出裸 H264 轨 + ADTS AAC 轨
ffmpeg -f lavfi -i testsrc=duration=10:size=640x480:rate=25 `
       -f lavfi -i sine=frequency=440:duration=10 `
       -c:v libx264 -b:v 500k -g 25 -keyint_min 25 -an testdata/test.mp4

ffmpeg -i testdata/test.mp4 -c:v copy -bsf:v h264_mp4toannexb testdata/test.h264
ffmpeg -i testdata/test.mp4 -c:a copy -bsf:a aac_adtstoasc testdata/test.aac
```

> ✅ **验收**：用 `ffplay testdata/test.h264` 能播，用 `ffplay testdata/test.aac` 能听到声音。

---

## Stage 1 — RTSP 文本协议骨架（无媒体，先把状态机跑通）

**预计耗时：1~2 天**

### 1.1 学习要点（先看 RFC，再写代码）

| 主题      | 必须搞懂的点                                                       |
| --------- | ------------------------------------------------------------------ |
| RFC 2326  | RTSP 请求行格式、`CSeq`、`Session`、`Transport`、`Require` 头      |
| 方法集    | `OPTIONS` / `DESCRIBE` / `SETUP` / `PLAY` / `TEARDOWN`，顺序严格   |
| 状态机    | `Init → Ready(SETUP完成) → Playing(PLAY后) → (TEARDOWN回Init)`     |
| 传输头    | `Transport: RTP/AVP;unicast;client_port=xxxx-yyyy`（UDP 模式关键） |
| SessionID | 服务端在 SETUP 响应里下发 `Session: 123456;timeout=60`             |

### 1.2 任务清单

- [x] **1.2.1** 用 `net.Listen("tcp", ":8554")` 起服务，每 accept 一个连接起一个 goroutine。
- [x] **1.2.2** 写一个 `readRequest(conn)`：按 `\r\n\r\n` 切出 header block，解析出 Method、URI、Header map。
- [x] **1.2.3** 写一个 `writeResponse(conn, status, headers, body)`：按 RTSP 响应格式拼 `RTSP/1.0 200 OK\r\nCSeq: N\r\n...\r\n\r\n`。
- [x] **1.2.4** 实现 `OPTIONS`：返回 `Public: OPTIONS, DESCRIBE, SETUP, PLAY, PAUSE, TEARDOWN`。
- [x] **1.2.5** 实现 `DESCRIBE`：先返回一个**写死的最小 SDP**（`v=0/o=-/s=...` 那一套），body 后面阶段再补全。
- [x] **1.2.6** 实现 `SETUP`：解析 `client_port`，分配 UDP 端口（先随便给两个），生成 SessionID 回给客户端。
- [x] **1.2.7** 实现 `PLAY`：回 `200 OK`，**这一阶段先不推任何包**。
- [x] **1.2.8** 实现 `TEARDOWN`：关闭会话。
- [x] **1.2.9** 状态机校验：如果客户端顺序错乱（例如还没 DESCRIBE 就 PLAY），返回 `454 Session Not Found` 或 `455 Method Not Valid`。

### 1.3 验收标准

```powershell
# 用 ffprobe 或 telnet 手敲：
ffprobe -v trace rtsp://127.0.0.1:8554/live
```

✅ **通过条件**：

- 抓包能看到完整的 `OPTIONS → DESCRIBE → SETUP → PLAY → TEARDOWN` 五次往返；
- `CSeq` 每次都和请求里的对齐；
- SessionID 在 SETUP 之后所有请求里都带着；
- ffprobe 不报 4xx/5xx（哪怕 SDP 是写死的也没关系）。

### 1.4 本阶段必踩坑（先列出来，撞到再补笔记）

| 坑                      | 现象          | 解决                                                         |
| ----------------------- | ------------- | ------------------------------------------------------------ |
| 行尾是 `\r\n` 不是 `\n` | 解析卡死      | 用 `bufio.Scanner` + 自定义 split，或直接按字节找 `\r\n\r\n` |
| `CSeq` 不回             | VLC 直接断    | 每个响应必须带回请求里的 CSeq                                |
| SDP 没带 `Content-Type` | DESCRIBE 失败 | 响应头加 `Content-Type: application/sdp`                     |
| URL 里带 trailing slash | 路由匹配不上  | 统一 Trim 后再比较 path                                      |

### 1.5 自测题（写进 `notes/stage1.md`）

1. RTSP 默认端口是多少？和 HTTP/HTTPS 区别？
2. `CSeq` 作用是什么？为什么 PLAY 请求里的 CSeq 必须和之前 SETUP 的不一样？
3. SessionID 在哪一步下发？后续哪个请求必须带它？
4. 为什么 RTSP 用 TCP 控制通道，而 RTP 走 UDP？

---

## Stage 2 — UDP-RTP 推 H.264（第一个能播的里程碑）

**预计耗时：2~3 天**

### 2.1 学习要点

| 主题                           | 必须搞懂                                                                                  |
| ------------------------------ | ----------------------------------------------------------------------------------------- |
| RTP 固定头                     | 12 字节：V(2) P(1) X(1) CC(4) M(1) PT(7) + seq(16) + ts(32) + ssrc(32)                    |
| H.264 RTP 载荷格式（RFC 6184） | NALU 类型：`SPS=7, PPS=8, IDR=5, 非 IDR=1, FU-A=28, STAP-A=24`                            |
| FU-A 分片                      | NALU 大于 MTU（~1400 字节）时，拆成多个 FU-A 包；首包 S=1，尾包 E=1，marker 只在尾包打 1  |
| SDP for H264                   | `a=rtpmap:96 H264/90000` + `a=fmtp:96 packetization-mode=1; sprop-parameter-sets=SPS,PPS` |
| 时钟频率                       | 视频 RTP timestamp 用 **90000 Hz**                                                        |

### 2.2 任务清单

- [x] **2.2.1** 写 `internal/media/h264_demux.go`：从 `test.h264`（Annex-B，`00 00 00 01` 起始码）里切出每个 NALU。
- [x] **2.2.2** 写 `internal/rtp/h264_packer.go`：输入一个 NALU，输出一个或多个 RTP packet（小包直接 single NALU，大包走 FU-A）。
- [x] **2.2.3** 在 DESCRIBE 阶段动态读 `test.h264` 的 SPS/PPS，拼出真实的 SDP（`sprop-parameter-sets` 用 base64）。
- [x] **2.2.4** SETUP 阶段从 `Transport` 头解析 `client_port`，服务端开两个 UDP 端口：一个发 RTP、一个收 RTCP（RFC 3550 要求偶+奇）。
- [x] **2.2.5** PLAY 之后起一个 goroutine：按 25fps（40ms 一帧）节奏，从文件读 NALU → 封 RTP → `udpConn.WriteToUDP`。
- [x] **2.2.6** RTP 头部：PT=96（动态）、SSRC 随机、seq 从 0 自增、timestamp 每帧加 `90000/25 = 3600`。
- [x] **2.2.7** IDR 帧（SPS/PPS/IDR）前要立刻发，不能等下一帧节奏。

### 2.3 验收标准

```powershell
# 直接拉流！
ffplay -v debug rtsp://127.0.0.1:8554/live
```

✅ **通过条件**：

- ffplay 能看到 640x480 的测试彩条画面；
- Wireshark 抓 `udp` 包，能看到 RPT version=2、PT=96、seq 连续；
- 大帧（IDR）被拆成多个 FU-A，最后一个包 marker bit=1；
- 连续拉 30 秒不花屏、不卡顿（文件播完就退出，正常）。

### 2.4 必踩坑

| 坑                                                | 解决                                                                         |
| ------------------------------------------------- | ---------------------------------------------------------------------------- |
| Annex-B 起始码切错（`00 00 01` vs `00 00 00 01`） | 写个状态机扫：连续两个 `00` 后看下一个是不是 `01`                            |
| SPS/PPS 没 base64 编码                            | SDP 里的 `sprop-parameter-sets` 是 base64(SPS)+","+base64(PPS)               |
| timestamp 增量算错                                | 90000 / fps，整数除法；25fps 就是 3600                                       |
| FU-A header 拼错                                  | FU indicator（首字节）+ FU header（S/E/R/type），别把原始 NALU header 重复写 |
| UDP 端口被防火墙拦                                | 先在本机 127.0.0.1 测，再测局域网                                            |

### 2.5 自测题

1. 为什么 H.264 时钟频率是 90000？随便写一个不行吗？
2. FU-A 的 S、E 位分别在哪一包？Marker bit 打在哪个包？
3. SPS/PPS 为什么要在每个 IDR 前重发？不发会怎样？
4. `packetization-mode=0` 和 `=1` 区别？VLC 喜欢哪个？

---

## Stage 3 — UDP-RTP 推 AAC（音视频一起上）

**预计耗时：1~2 天**

### 3.1 学习要点

| 主题                         | 必须搞懂                                                                                                                                                       |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ADTS 头                      | 7 字节（无 CRC 时）：syncword `0xFFF`、profile、sampling_frequency_index、channel_config、frame_length                                                         |
| AAC RTP 载荷格式（RFC 3640） | 一个 AU 一个 RTP 包，header 后接 `AU-headers-length(16bit)` + `AU-header(13bit size + 3bit index)`                                                             |
| SDP for AAC                  | `a=rtpmap:97 mpeg4-generic/44100/2` + `a=fmtp:97 streamtype=5; profile-level-id=1; mode=AAC-hbr; SizeLength=13; IndexLength=3; IndexDeltaLength=3; config=...` |
| 时钟频率                     | 音频用采样率（44100），不是 90000                                                                                                                              |

### 3.2 任务清单

- [x] **3.2.1** 写 `internal/media/aac_demux.go`：按 ADTS 头里的 `frame_length` 切出每个 AU。
- [x] **3.2.2** 解析 ADTS 头，拿到 `sampling_frequency` 和 `channel_config`，动态拼 SDP 里的 `config`（AudioSpecificConfig，2 字节：`profile<<11 | freqIdx<<7 | chCfg<<3`）。
- [x] **3.2.3** 写 `internal/rtp/aac_packer.go`：按 RFC 3640 拼 RTP 包。
- [x] **3.2.4** DESCRIBE 返回的 SDP 改成两条 `m=video` + `m=audio`（其实是一个 SDP 里两个 `m=` 行）。
- [x] **3.2.5** SETUP 现在要被客户端调用两次：第一次 `m=video` 分配视频端口对，第二次 `m=audio` 分配音频端口对，**共用同一个 SessionID**。
- [x] **3.2.6** PLAY 后两个 goroutine 并行推：视频一路 UDP，音频一路 UDP，timestamp 各自从 0 累加。

### 3.3 验收标准

```powershell
ffplay -v debug rtsp://127.0.0.1:8554/live
```

✅ **通过条件**：

- 画面 + 440Hz 蜂鸣声同时出；
- 嘴型/动作和声音基本对齐（用 testsrc 无所谓，但要是换成真实视频，音差 < 80ms）；
- Wireshark 看到两组 UDP，视频 PT=96、音频 PT=97，seq 各自独立。

### 3.4 必踩坑

| 坑                          | 解决                                                                                              |
| --------------------------- | ------------------------------------------------------------------------------------------------- |
| AudioSpecificConfig 拼错    | profile 是 ADTS 里 `profile+1`（ADTS profile=1 对应 AAC LC，AudioSpecificConfig 里 profile=2）    |
| AU-header 长度算错          | `SizeLength=13; IndexLength=3` 意味着 AU-header 是 16bit：高 13 位放 AU size，低 3 位放 index(=0) |
| 音频 timestamp 用了 90000   | 用采样率！44100Hz 时每个 AU（1024 samples）加 1024                                                |
| SETUP 两次但 SessionID 变了 | 第二次 SETUP 必须带回第一次的 SessionID，否则当成新会话                                           |

### 3.5 自测题

1. 为什么 AAC RTP 要带 AU-header？不像 H.264 那样直接塞 NALU？
2. AudioSpecificConfig 和 ADTS header 什么关系？怎么互转？
3. 音视频 timestamp 基准不同，播放器怎么对齐？

---

## Stage 4 — Interleaved 模式（RTP over RTSP TCP，摄像头默认走这条）

**预计耗时：2 天**

### 4.1 学习要点

| 主题                | 必须搞懂                                                                  |
| ------------------- | ------------------------------------------------------------------------- |
| Interleaved 帧格式  | `$` (0x24) + channel(1B) + length(2B, 大端) + RTP payload                 |
| Transport 头变化    | 客户端请求 `RTP/AVP/TCP;unicast;interleaved=0-1`，服务端 SETUP 响应要确认 |
| 复用同一个 TCP 连接 | 控制消息（RTSP 请求/响应）和 RTP 数据共用一条 TCP，用 `$` 前缀区分        |
| Channel 分配        | 视频 RTP=0、RTCP=1；音频 RTP=2、RTCP=3（常见约定）                        |

### 4.2 任务清单

- [x] **4.2.1** SETUP 时解析 `Transport` 里有没有 `interleaved=`，有就走 TCP 模式，不再开 UDP 端口。
- [ ] **4.2.2** 给每条 TCP 连接起一个**读 goroutine**：循环读字节，第一个字节是 `$` 就按 interleaved 帧解析喂给 RTCP 接收；否则按 RTSP 文本行解析（状态机区分）。
- [ ] **4.2.3** 推 RTP 时：视频包前面拼 `\x24\x00\xLL\xLL`，音频拼 `\x24\x02\xLL\xLL`，直接 `conn.Write`。
- [ ] **4.2.4** ffplay 强制走 TCP：`ffplay -rtsp_transport tcp rtsp://127.0.0.1:8554/live`。
- [ ] **4.2.5** VLC 也能在菜单里把传输改成 TCP。

### 4.3 验收标准

✅ **通过条件**：

- `ffplay -rtsp_transport tcp` 能播画面 + 声音；
- Wireshark 只看到一条 TCP 流，里面混杂着 RTSP 文本和 `$` 开头的二进制帧；
- UDP 模式（Stage 2/3）也还能继续播 —— **两种模式都得支持**。

### 4.4 必踩坑

| 坑                                             | 解决                                                                                                    |
| ---------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| RTSP 文本和 interleaved 数据粘在同一个字节流里 | 用一个 reader 状态机：读完 header block 后先 peek 下一个字节，是 `$` 走数据分支，是字母走 RTSP 请求分支 |
| length 用小端                                  | 必须大端！`binary.BigEndian.Uint16`                                                                     |
| 视频/音频 channel 号冲突                       | 视频用 0/1，音频用 2/3，不要复用                                                                        |
| 写 `conn.Write` 时没加锁                       | 多个 goroutine 同时写会穿插，必须用 mutex 或专门一个写 goroutine + channel                              |

### 4.5 自测题

1. 为什么很多摄像头默认 TCP interleaved？UDP 在什么网络下会失败？
2. `$` 帧的 length 是 payload 长度还是包含 header？
3. 怎么区分"这一段字节是 RTSP 响应"还是"这一段是 interleaved RTP"？

---

## Stage 5 — 进阶优化 + 生产化收尾（写简历前必须做）

**预计耗时：3~5 天**

### 5.1 任务清单

- [ ] **5.1.1 会话管理**：`session.Manager` 用 `map[string]*Session`，每 30s 扫一次，超过 timeout 没收到 RTCP 就 TEARDOWN。
- [ ] **5.1.2 单源多消费者**：一个 `test.h264` 文件只解码一次，多个客户端同时拉流时用 fan-out（每路客户端一个 channel，源 goroutine 广播）。
- [ ] **5.1.3 RTCP 发送者报告（SR）**：每 5 秒发一次 RTCP SR（PT=200），带 NTP/RTP timestamp 映射，否则有些播放器会停。
- [ ] **5.1.4 环形缓冲区**：源 goroutine 解码后写到 ring buffer，消费者各自按 seq 读，慢消费者丢老帧而不是拖慢整体。
- [ ] **5.1.5 优雅退出**：捕获 `SIGINT`，关闭所有 listener 和 session，UDP/TCP 都 close。
- [ ] **5.1.6 接入真实摄像头**：用 ffmpeg 把 USB 摄像头（`/dev/video0` 或 Windows DirectShow）转成 `pipe:1` 输出裸 H264，server 改成从 stdin 读，模拟实时流。
- [ ] **5.1.7 RTSP ANNOUNCE（推流端，加分项）**：让 ffmpeg 把流**推**到你的 server，server 接收后再转发给拉流客户端。
- [ ] **5.1.8 压测**：写个脚本同时拉 20 路流，看 CPU/内存/Goroutine 数（用 `pprof`）。

### 5.2 验收标准

✅ **通过条件**：

- 20 路并发拉流，CPU < 30%、内存 < 200MB；
- 单个客户端 TEARDOWN 后，其他客户端不受影响；
- 拉流中途断网重连（`kill -9` 客户端），服务端 60s 内自动回收会话；
- `go tool pprof` 能出火焰图，找得到热点（应该在封包器和 `WriteToUDP`）。

### 5.3 简历话术（直接抄）

> 基于 Go 标准库从零实现 RTSP 流媒体服务器，完整实现 RFC 2326/6184/3640 协议栈：自研 RTSP 状态机（OPTIONS/DESCRIBE/SETUP/PLAY/TEARDOWN）、H.264 NALU 切分与 FU-A RTP 封包器、AAC ADTS 解封装与 RFC 3640 载荷打包；支持 UDP-RTP 与 Interleaved TCP 双通道传输，单源多消费者 fan-out 架构，兼容 VLC/ffplay 拉流；基于 goroutine + channel 实现会话管理与超时回收，pprof 优化后 20 路并发拉流 CPU < 30%。

---

## 附录 A — 面试高频追问（准备时默写答案）

| #   | 问题                                   | 你要能答到的深度                                                 |
| --- | -------------------------------------- | ---------------------------------------------------------------- |
| 1   | RTSP、RTP、RTCP 三个协议各自职责？     | RTSP 控制（类 HTTP）、RTP 传媒体、RTCP 传质量反馈和时间同步      |
| 2   | RTP 为什么自带 seq 和 timestamp？      | 抗丢包检测 + 抖动缓冲 + 音视频同步                               |
| 3   | H.264 FU-A 分片逻辑？                  | MTU 限制、S/E 位、marker 打尾包、首包必须是 IDR 才能解码         |
| 4   | SDP 里 `sprop-parameter-sets` 是什么？ | base64 编码的 SPS+PPS，让客户端提前拿到解码器初始化参数          |
| 5   | Interleaved 模式为什么用 `$` 前缀？    | RTSP 文本是行式协议，RTP 是二进制，需要一个 magic byte 区分      |
| 6   | 音视频怎么同步？                       | RTCP SR 里同时带 NTP wall clock 和 RTP timestamp，客户端据此对齐 |
| 7   | 为什么不用 HTTP-FLV/WebRTC 而用 RTSP？ | RTSP 是低延迟直播/摄像头标准，WebRTC 是浏览器 P2P，应用场景不同  |
| 8   | Go 写这个比 C++ 好在哪？               | goroutine 天然并发、标准库齐、单二进制部署；劣势是延迟没 C 可控  |

## 附录 B — 参考资料

| 类型           | 链接                                                 |
| -------------- | ---------------------------------------------------- |
| RFC 2326       | RTSP 1.0                                             |
| RFC 6184       | H.264 over RTP                                       |
| RFC 3640       | AAC over RTP                                         |
| RFC 3550       | RTP/RTCP 基础                                        |
| 对照源码       | `github.com/bluenviron/gortsplib`（只看，别 import） |
| Wireshark 样例 | 搜 "rtp h264 fu-a" 抓包教程                          |

---

## 进度追踪

| Stage             | 状态 | 开始日期 | 完成日期 | 笔记链接        |
| ----------------- | ---- | -------- | -------- | --------------- |
| 0 环境准备        | ⬜   |          |          |                 |
| 1 RTSP 骨架       | ⬜   |          |          | notes/stage1.md |
| 2 UDP-H264        | ⬜   |          |          | notes/stage2.md |
| 3 UDP-AAC         | ⬜   |          |          | notes/stage3.md |
| 4 Interleaved TCP | ⬜   |          |          | notes/stage4.md |
| 5 进阶优化        | ⬜   |          |          | notes/stage5.md |

> 打勾规则：每个 Stage 必须先写 `notes/stageN.md`（至少含踩坑表 + 自测题答案），再把上面表格对应行改成 ✅。

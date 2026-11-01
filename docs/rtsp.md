### RTP 协议（Real-time Transport Protocol，实时传输协议）

> 专门用来传输**音频、视频这类实时多媒体数据**，广泛用于视频会议、直播、VoIP、WebRTC、监控 RTSP 取流等场景RFC Editor
> 一般跑在 **UDP** 之上，优先低延迟，**不保证可靠送达、不保证 QoS**（丢包、乱序都可能发生）；配套 **RTCP（RTP 控制协议）** 一起工作：RTP 传媒体数据，RTCP 上报丢包、抖动、时延等质量统计、同步信息、参会者信息IETF
> RTSP ≠ RTP，RTSP ≠ RTCP。
> RTSP 只管 “控制流程”，**不负责传视频码流**；RTP+RTCP 才是媒体传输搭档。

- RTSP 8554 TCP 会话控制信令

```
OPTIONS → DESCRIBE → SETUP → PLAY → PAUSE → TEARDOWN
比：播放器和服务器之间的遥控器。

- 它只交换文本请求 / 响应、CSeq、Session、SDP、Transport
- **RTSP 本身不带任何 H264 视频数据**
- SETUP 里协商 `client_port=9000-9001`：
  - `9000`：**RTP 端口（视频数据）**
  - `9001`：**RTCP 端口（反馈）**

👉 你当前代码，**就是标准 RTSP 服务** 只做信令
- CSeq：**每一条请求必须带，响应必须回相同 CSeq，用来配对请求 / 应答**
- Session：只有 SETUP 成功之后，后续 PLAY/TEARDOWN 才带上这个 Session 头；OPTIONS/DESCRIBE 不带 Session
- Transport 头：SETUP 才会协商端口，OPTIONS/DESCRIBE 没有 Transport
```

## RTP

UDP，承载媒体载荷（H264/AAC 帧）

- 每个 RTP 包有包头：序列号、时间戳、SSRC、marker 位
- 负责把一帧帧 H264 切成包发给播放器

## RTCP（RTP Control Protocol）

UDP，和 RTP 绑定成对，**同一条流固定一对端口：RTP = 偶数，RTCP = 偶数 + 1**（你这里 9000/9001 完全符合规范）
RTCP 不传媒体！只发统计反馈包，几种类型：

1. **SR 发送者报告**：服务器发，告诉客户端发送时间戳、包计数
2. **RR 接收者报告**：ffplay（客户端）发，告诉服务器丢包率、抖动、延迟
3. SDES：源描述（名字、信息）
4. BYE：退出流

> ✅ 所以你说的没错：**RTP + RTCP 是一对搭档，一起工作**；而 RTSP 是独立的 “上层遥控器”，负责协商、启动、停止这一对 UDP 流。

## 交互

1. TCP 连接到 8554，RTSP OPTIONS
2. RTSP DESCRIBE → 返回 SDP，SDP 告诉客户端：媒体是 H264，用 RTP/AVP
3. RTSP SETUP → 协商 `client_port=9000-9001`
   - 约定：视频 RTP 发到客户端 UDP 9000
   - RTCP 发到客户端 UDP 9001
4. RTSP PLAY → **服务器开始往客户端 UDP 端口发送 RTP 包，同时收发 RTCP 包**
5. 播放器一边收 RTP 解码播放，一边发 RR（RTCP）反馈丢包
6. RTSP PAUSE → 停止发送 RTP
7. RTSP TEARDOWN → 结束会话

协议 传输层 核心作用 是否传视频
RTSP TCP 会话控制：协商、播放、暂停、停止 ❌ 纯文本信令
RTP UDP 封装 H264/AAC 媒体帧 ✅ 视频数据载体
RTCP UDP RTP 质量反馈、同步、统计 ❌ 只传统计，无画面

# SDP = Session Description Protocol，会话描述协议

> 一句话：**SDP 不是传输协议，它只是一段【文本说明书】**，告诉播放器：这个流是什么编码、走什么协议、地址端口是啥，播放器拿到说明书才知道怎么去收 RTP 包、解码画面IETF Datat...。
> 在你现在的 RTSP 代码里，它就是**DESCRIBE 响应的 body**，`Content-Type: application/sdp`。

> 重点区分：
>
> - RTSP：TCP 信令，发指令（OPTIONS/DESCRIBE/SETUP/PLAY）
> - SDP：**RTSP DESCRIBE 返回的文本说明书**，描述媒体
> - RTP：UDP，装 H264 视频帧
> - RTCP：UDP，RTP 配套的质量反馈

1. 告诉客户端：有几路流（video/audio）
2. 编码格式、时钟频率
3. 使用 RTP/AVP
4. 媒体端口、IP
   SDP 内部换行是 `\r\n`

## 时序

这个时序，就是你状态机的驱动事件！
Init → 收到合法 SETUP 且服务端返回 Session → Ready
Ready → 收到 PLAY → Playing
Playing → TEARDOWN → Init

## RTSP 是**文本协议，类似 HTTP，但不是 HTTP！**

- 区分：HTTP 是`GET /path HTTP/1.1`；RTSP 是`DESCRIBE rtsp://xxx RTSP/1.0`
- 头格式和 HTTP 类似：`Key: Value`，大小写**不敏感**，但工程一般统一首字母大写 `CSeq`
- 换行：`\r\n`，最后头部结束是 `\r\n\r\n`（和 HTTP 一样）
- Content-Length：如果带 SDP body（DESCRIBE 响应）必须填

## RTSP 默认端口是多少？和 HTTP/HTTPS 区别？

```txt
RTSP默认端口 554
加密RTSP 默认端口 332
RTSP 流媒体控制信令协议， 有状态维护Session。 本身不传视频画面，媒体交给RTP/RTCP
通信双向 客户端可以发请求， 服务端也可以主动发请求
```

## `CSeq` 作用是什么？为什么 PLAY 请求里的 CSeq 必须和之前 SETUP 的不一样？

**CSeq = Command Sequence，命令序列号**

1. **绑定请求和响应配对**：请求带 `CSeq:N`，对应的响应必须原样返回同一个 `CSeq:N`；客户端靠它区分「这个响应是回复哪一条请求」。
   > RTSP 允许**请求乱序、UDP 传输、重传**，同一个连接上多条请求并发，靠 CSeq 做应答匹配。
2. **识别重传**：客户端重发同一条请求时，CSeq 不变；服务端看到相同 CSeq 可以识别是重传，避免重复执行操作。
3. **简单时序标记**：客户端**每发一条新的独立请求，CSeq +1**。

> 重点区分：
>
> - **同一次请求重传：CSeq 不变**
> - **下一条全新请求：CSeq 必须 +1**

```
OPTIONS CSeq:1 → resp CSeq:1
DESCRIBE CSeq:2 → resp CSeq:2
SETUP   CSeq:3 → resp CSeq:3
PLAY    CSeq:4 → resp CSeq:4

```

## 为什么 PLAY 的 CSeq 和 SETUP 不能一样？

一句话：**SETUP、PLAY 是两条完全独立的新命令，不是同一个请求**，所以 CSeq 必须递增。
拆开讲：

1. SETUP 是一条请求，协商传输通道、创建 Session；
2. PLAY 是后续另一条独立控制指令（启动媒体发送）；
3. 如果 PLAY 复用 SETUP 的 CSeq=3：客户端收到 `CSeq:3` 的响应，分不清这个响应到底是回复 SETUP 还是 PLAY，配对直接错乱。

## Session哪一步下发

> 一个 RTSP 连接可以有**多个 Session**（一路摄像头一个 session），互不干扰

1. **SessionID 只在 SETUP 的响应里由服务端下发**

> 👉 这也是为什么：**SETUP 之前，不存在合法 Session**

- OPTIONS、DESCRIBE：**没有 Session**，请求、响应都不带 Session 头
- SETUP：客户端发的 SETUP 请求**不带 Session**；服务端处理成功之后，在 SETUP 响应头带上 `Session: sess-xxx;timeout=60`，会话正式创建

2. **SETUP 成功之后：PLAY / PAUSE / TEARDOWN 这三类控制请求，必须携带 Session 头**
   - PLAY：必须带
   - PAUSE：必须带
   - TEARDOWN：必须带
   - OPTIONS、DESCRIBE：可以不带（不需要绑定会话）

## 为什么 RTSP 用 TCP 控制通道，而 RTP 走 UDP？

> RTSP 控制信令走 TCP，看重**可靠、不乱、不丢**；RTP 媒体默认 UDP，看重**低延迟**，愿意接受少量丢包，由上层音视频自己做容错。

1. **RTSP（控制信令）→ TCP**
   RTSP 是命令：SETUP / PLAY / PAUSE / TEARDOWN。

- 不能丢：如果 PLAY 命令丢了，流起不来；PAUSE 丢了停不下来；
- 不能乱序：信令有严格状态机；
- 数据量很小，频率很低；
  TCP 自带确认、重传、有序，非常适合。

> 代价：TCP 重传会引入延迟，但是控制命令不在乎这点延迟。

2. **RTP（媒体）→ UDP**
   音视频实时流（监控、直播通话）最核心诉求：**延迟优先**

- UDP 无连接、没有 ACK、没有等待重传；包来了直接送解码；
- 视频丢一两个 RTP 包，最多画面马赛克、花一下，**人眼可以接受**；
- 如果 RTP 走 TCP：一旦丢包，TCP 卡住等重传，后面所有帧排队等待，**卡顿、延迟雪崩**（这个是重点！面试最爱问）；
- 丢包补偿交给 RTP/RTCP、FEC、帧内编码、丢包隐藏，而不是传输层 TCP。

##### Q A

> Q：UDP 丢包怎么办？
> A：靠 RTCP 反馈丢包、抖动；编码器 I 帧、FEC 前向纠错、帧隐藏；而不是传输层重传。

> Q：那为什么 HLS 走 HTTP (TCP)？
> A：HLS 是分片下载，属于点播 / 低实时性场景，延迟几十秒，不追求低延迟；和 RTSP 实时监控定位不一样。

## RTP 固定头12字节

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|V=2|P|X|  CC   |M|     PT      |       sequence number         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                           timestamp                           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|           synchronization source (SSRC) identifier            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| 字段                | bit | 含义      | 工程要点                                              |
| ------------------- | --- | --------- | ----------------------------------------------------- |
| V Version           | 2   | 版本      | RTP 固定 V=2 ✅                                       |
| P Padding           | 1   | 填充位    | 一般 0；末尾有填充字节才置 1                          |
| X Extension         | 1   | 扩展位    | 一般 0；带 RTP 扩展头才置 1                           |
| CC CSRC count       | 4   | CSRC 个数 | 混合流才用，单路视频固定 0                            |
| M Marker            | 1   | 标记位    | H264：一帧最后一个 RTP 包 M=1；同帧前面分片 M=0       |
| PT Payload Type     | 7   | 负载类型  | 静态 PT：H264 没有分配静态号；自定义 96（动态 PT）    |
| seq sequence number | 16  | 序列号    | 每发一个 RTP 包 +1；循环溢出 0~65535；RTCP 靠它算丢包 |
| timestamp           | 32  | 时间戳    | 单位：采样时钟；H264=90000Hz；不是毫秒！              |
| SSRC                | 32  | 同步源    | 随机生成一个 uint32，一路流全程固定，标识这个流       |

## H.264 NALU Header（RFC6184）

单个 NALU 第一个字节结构：

```
+---------------+
|0|1|2|3|4|5|6|7|
+-+-+-+-+-+-+-+-+
|F|NRI|  Type   |
+---------------+
```

- F：禁止位，视频流固定 0
- NRI：重要性 0~3；SPS/PPS/IDR 设高一点（3），普通 P 帧低一点
- Type：NAL 类型，就是你表里的：

| NAL Type | 名称    | 说明         | 工程要点                                                       |
| -------- | ------- | ------------ | -------------------------------------------------------------- |
| 0        | -       | 保留不用     | 禁止使用                                                       |
| 1        | Non-IDR | 普通 P 帧    | 可独立解码前提是 I 帧已收到                                    |
| 5        | IDR     | I 帧，关键帧 | 收到可以刷新解码上下文                                         |
| 7        | SPS     | 序列参数集   | 分辨率、profile、level                                         |
| 8        | PPS     | 图像参数集   | 分片、量化参数                                                 |
| 24       | STAP-A  | 单时间聚合包 | 把多个小 NALU 打包在一个 RTP 包（SPS+PPS 经常一起打包 STAP-A） |
| 28       | FU-A    | 分片单元 A   | 大于 MTU 时拆分 NALU，最常用                                   |

MTU 常规：以太网 1500，减去 IP 头 20 + UDP 头 8 → **RTP 载荷上限～1472**，工程保守取 **1400**。
NALU 长度 > 1400 → 必须 FU-A。

## FU-A 格式（Type=28，重中之重）

FU-A = 一个大 NAL 切成多个 RTP 包
每个 FU-A 包：
`RTP头(12) + FU indicator(1) + FU header(1) + NAL payload分片`

### FU indicator 字节

```
+---------------+
|F|NRI|  Type   | Type固定=28
+---------------+
```

> F/NRI 和原始 NALU 的 F/NRI 保持一致

### FU header 字节

```
+---------------+
|S|E|R|  Type   |
+---------------+
```

- S (Start)：1 bit，**分片第一个包 S=1，其余 0**
- E (End)：1 bit，**分片最后一个包 E=1，其余 0**
- R：保留位，固定 0
- Type：原始 NALU 的 type（比如 5=IDR，1=P 帧）

✅ 规则（面试必问）

1. 只有**最后一个 FU 分片 RTP 包**，RTP 头里的 Marker (M) = 1；前面分片 M=0
2. S=1 的包：带原始 NALU 头里的 NRI、Type 信息；中间分片只传裸数据
3. 接收端：收到 S=1 → 开始组包；收到 E=1 → 组包完成，送入解码器

> ❌ 新手高频错：把 M 标记打在每一个 FU 分片上；或者 S/E 同时等于 1（不合法）

## SDP 字段详解（对应你表里那一行）

```
m=video 9000 RTP/AVP 96
a=rtpmap:96 H264/90000
a=fmtp:96 packetization-mode=1; sprop-parameter-sets=Z0LgKdooKgC3A,aM44gA==
```

- `m=video port proto PT`：媒体类型、UDP 端口、RTP/AVP、负载类型 96
- `a=rtpmap`：PT=96，H264，时钟频率 90000
- `packetization-mode`
  - `0`：单个 NAL 一个 RTP 包；不支持 FU-A 分片
  - `1`：**支持单 NAL + FU-A 分片**（我们要用这个！）
  - `2`：支持 STAP-B、MTAP 等，安防很少用
- `sprop-parameter-sets`：base64 编码的 SPS,PPS，逗号分隔；解码器拿到 SDP 就可以提前初始化，不用等码流里出 SPS/PPS

> 注意：sprop-parameter-sets 是 SDP 里给解码器预加载用的；码流里依然建议每个 IDR 前面主动发 SPS+PPS。

**NALU = NAL Unit，Network Abstraction Layer Unit，网络抽象层单元**
H.264 裸流（Annex-B）的**最小传输 / 解析单元**。

> H.264 分成两层：
>
> - **VCL 视频编码层**：负责把图片压缩成宏块、分片（Slice），只管压缩
> - **NAL 网络抽象层**：把 VCL 输出打包成一个一个 NALU，打上类型标记，方便丢到文件、UDP、RTP、RTSP 里传输CSDN博...

## ✅ NALU 内部结构（裸 NALU，就是你 `h264_demux.go` 返回的东西）

```
[1字节 NAL Header] + [RBSP 载荷]
```

1. **NAL Header（1 字节）**

```
bit:  7   6 5   4 3 2 1 0
     F | NRI |   Type
```

- F：禁止位，0 正常
- NRI：2bit，优先级（SPS/PPS/IDR 高优先级）
- Type：低 5bit，NAL 类型（`nalu[0] & 0x1F`），就是我们之前记的 7=SPS，8=PPS，5=IDR，1=P 帧

2. **RBSP**：真正的数据，可能是 SPS 参数、PPS 参数、I 帧 / P 帧图像压缩数据

## ✅ Annex-B 文件格式（test.h264）

存到文件里的时候，**每个 NALU 前面会加起始码做分隔**：

```
起始码(00 00 01  或 00 00 00 01) + NALU
```

👉 **这就是为什么我们写 demux：扫描起始码，切掉起始码，拿出后面裸 NALU 给 RTP 打包！**
👉 RTP（RFC6184）传输**不要起始码**，直接传裸 NALUIETF

## ✅ 两大类 NALU

1. **VCL-NALU（图像数据）**：Type=1、5
   - Type=5：IDR（I 帧关键帧），解码器可以清空参考、独立解码
   - Type=1：普通非 IDR（P 帧），依赖前面已经解码的参考帧
2. **Non-VCL（参数集，解码配置）**

> 解码器必须先拿到 SPS+PPS，才能正常解码 IDR/P 帧

- Type=7：SPS 序列参数集（分辨率、profile、帧率等）
- Type=8：PPS 图像参数集（量化、分片参数）

## 从 `test.h264`（Annex-B，`00 00 00 01` 起始码）里切出每个 NALU。

1. 开头 `type=6`：SEI（增强信息，无关紧要，跳过即可）
2. 循环规律：`7(SPS) → 8(PPS) →5(IDR关键帧) →一堆1(P帧)`，完美符合 Annex-B 裸流结构
3. 大量 NAL 长度超过 1400：

FU-A 是 H.264 大 NALU 分片方案，**一个 NALU 切成多个 RTP 包**；
每个分片 RTP 载荷 = `FU indicator(1B) + FU header(1B) + 分片数据`
原始 NALU 结构：`[NAL Header(1字节)][nal数据负载...]`

### 为什么 H.264 RTP 时钟频率是 90000？随便写一个不行吗？

**不能随便写**，`90000` 是 RTP 针对视频的经典固定采样时钟（RFC3550 / RFC6184）

### FU-A 的 S、E 位分别在哪一包？Marker bit 打在哪个包？

先回顾 FU-A 的 FU Header（第二个 payload 字节）：
`S(bit7) E(bit6) R(bit5) NALType(bit4~0)`

- **S = Start bit（1）：该 NAL 的第一个分片包**
- **E = End bit（1）：该 NAL 的最后一个分片包**
- 中间分片：S=0，E=0

> ⚠️ 重点区分（面试高频坑）
> S/E 是 **属于同一个原始 NAL 的分片标记**（RFC6184 FU-A 内部标记）
> Marker bit（RTP 头 M 位）是 **属于同一视频帧的结束标记**（RFC3550 RTP 头标记）

### SPS/PPS 为什么要在每个 IDR 前重发？不发会怎样？

SPS：分辨率、profile、level、帧参考模式
PPS：熵编码、slice 参数、量化参数

解码器**只在收到 SPS/PPS 之后，才能解码后续 IDR**。
两种场景：

1. 播放器中途加入流（播放中途拉流、网络断连重连）：它**没有拿到之前 SDP 里的 sprop-parameter-sets**，缓存是空的；此时收到 IDR，没有 SPS/PPS → **无法解码，花屏 / 黑屏 / 丢弃这组 IDR**
2. 流里面 SPS/PPS 参数发生变化（分辨率切换），必须下发新 SPS/PPS

> 虽然 SDP 的 `sprop-parameter-sets` 会在 DESCRIBE 阶段下发一次，但是：
> SDP 只在握手阶段传一次；RTSP 链路断开重连、UDP 丢包导致播放器丢失 SDP 信息，播放器不会永久保存 SPS/PPS。

## ADTS 头（AAC 裸流，ADTS 格式）

ADTS = Audio Data Transport Stream
用途：**AAC 裸流文件（.aac）、本地裸流分割**，每一个 AAC 音频帧前面带 ADTS 头，解码器直接逐帧切。

> 和 H.264 AnnexB 起始码作用类似：用来分割一帧一帧 AAC。
> 无 CRC：**7 字节**；带 CRC 是 9 字节
> 字段（重点 5 个）：

- `syncword 0xFFF`：同步字，固定 `0xFFF`，用来找帧起始；抓包 / 解析先搜这个
- profile：AAC profile，AAC LC=1（最常用）
- sampling_frequency_index：采样率索引，不是直接采样值！查表映射：
  - 44100 → index=4
  - 48000 → index=3
- channel_config：声道数，1 单声道，2 立体声
- frame_length：**整个 ADTS 帧长度（ADTS 头 + AAC 原始音频数据）**，解析器靠这个跳到下一帧

> 一句话区分：
> ADTS 是**裸 AAC 帧分隔头**；RFC3640 AU-header 是**RTP 载荷内部的帧描述头**，两个东西不要混！
> ADTS 只存在本地 .aac 文件，**RTP 包里面不带 ADTS 头！**（大坑）
> 发给 RTSP/RTP 的 AAC payload，要剥掉 ADTS，只把 AAC ES 数据放到 RTP AU 里。
> **一个 AU 一个 RTP 包**（最简单，demo 首选）
> AU = Access Unit，对应**一帧 AAC**。

RTP payload 结构（RTP 头之后）

```
+-----------------------+
| AU-headers-length (16bit)
+-----------------------+
| AU-header （16bit：13bit Size + 3bit Index）
+-----------------------+
| AAC ES data（去掉ADTS的原始AAC帧）
+-----------------------+
```

- AU-headers-length：**AU-header 部分总 bit 长度**。
  只有 1 个 AU 时：16bit（就是一个 AU-header），所以这个字段填 `16`
- AU-header：固定 16bit
  - `Size: 13 bit`：当前这个 AAC AU（ES）的字节长度（最大支持 8191 字节）
  - `Index: 3 bit`：AU 序号增量，连续包递增，一般单 AU 场景填 0

1. AAC-hbr：**一个 AU 一个 RTP 包**（你现在学这个，简单）
2. AAC-lbr：多个 AU 打包进同一个 RTP 包，低延迟场景，复杂，初学不用碰

> 音频 RTP 时间戳基准 = **音频采样率**

- AAC 44100Hz → 时钟 = 44100
- AAC 48000Hz → 时钟 = 48000
- H.264 视频固定 90000

AAC 常见帧长：**1024 采样点每帧（AAC LC 标准）**
👉 每帧 TS 增量 = 1024
举例：44100 采样率 AAC
每秒帧数 = 44100 / 1024 ≈ 43.066 帧 /s
每发一帧 AAC，`ts += 1024`

> 对比 H264：
> H264：25fps，90000 时钟，ts += 3600
> AAC：每帧固定 1024 采样，ts +=1024，和帧率无关！

> 核心一句话：RTP 时间戳代表**采样点计数**，视频是虚拟采样时钟 90k，音频是真实 ADC 采样时钟。

## ADTS字段总表

字段 比特 说明
字段 比特 说明
syncword 12 固定 0xFFF（12 个 1），帧同步标记，解码器用来找帧起始
ID 1 0=MPEG-4 AAC，1=MPEG-2 AAC；我们一般是 0
layer 2 AAC 固定 00，保留
protection_absent 1 1 = 没有 CRC（7 字节头）；0 = 后面带 2 字节 CRC（9 字节）
profile 2 AAC 类型：0=Main，1=LC（最常用），2=SSR，3 保留
profile = AudioObjectType - 1
sampling_frequency_index 4 采样率索引，查表，不是直接采样值
3 → 48000；4 → 44100
private_bit 1 编码器私有位，一般置 0
channel_config 3 声道配置：1 单声道，2 立体声
original/copy 1 版权标记，一般 0
home 1 保留，一般 0
copyright_identification_bit 1 版权，一般 0
copyright_identification_start 1 版权，一般 0
frame_length 13 ✅ 整个 ADTS 帧总字节数：ADTS 头 + AAC ES 数据
buffer_fullness 11 码率缓冲满度；VBR 流填 0x7FF
number_of_aac_frames 2 这个 ADTS 里面包含几个 AAC raw block，0 代表 1 个（绝大多数情况）

```
ADTS 文件
  │ ① AACDemux.NextAU()        → 拆出裸 AAC 帧（AU）
  ▼
AU []byte
  │ ② 本文件 AACPacker.Pack()   → 按 RFC 3640 封装成 RTP payload
  ▼
RTP 包（准备发送）
```

### 为什么需要解析 interleaved 通道号？

```
所以 interleaved=0-1 意味着：

RTP 包用 $ 0 <len> <rtp> 发

RTCP 包用 $ 1 <len> <rtcp> 发
RTSP 的 SETUP 支持两种传输：

UDP 模式：

text
Transport: RTP/AVP;unicast;client_port=5000-5001 服务端回 server_port=50000-50001，RTP 走独立 UDP socket。
TCP interleaved 模式：

text
Transport: RTP/AVP/TCP;unicast;interleaved=0-1
RTP 不走 UDP，而是复用在 RTSP 的那条 TCP 连接上传输。这适合 NAT/防火墙环境（只需 RTSP 的 554/8554 端口通）。

$ 帧格式（RFC 2326 §10.12）
interleaved 模式下，RTSP 的 TCP 连接上除了 RTSP 文本请求/响应，还穿插二进制 RTP/RTCP 数据。为了区分，用 $ 帧包裹：

text
$ <channel> <length(2字节,大端)> <data>
$：1 字节，固定 0x24，标识"这是 interleaved 二进制帧"。

channel：1 字节，通道号。RTP 用 interleaved 的第一个号，RTCP 用第二个。

length：2 字节大端，后面 data 的字节数。

data：RTP 或 RTCP 包。

所以 interleaved=0-1 意味着：

RTP 包用 $ 0 <len> <rtp> 发

RTCP 包用 $ 1 <len> <rtcp> 发
```

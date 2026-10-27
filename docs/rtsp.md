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

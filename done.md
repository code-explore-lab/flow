# done.md — 剩余任务拆解（RTSP / RTP / RTCP）

> 当前基线：Stage 5/5-1-6 已完成（RTSP 拉流、UDP/TCP interleaved、H.264+AAC、SR、ring buffer、优雅退出、stdin 实时源）。
> 这份文件把「已实现」与「协议完整 + 生产可用」之间的差距拆成可独立完成、独立提交的小任务。
>
> 规则：
> - 一次只做一个任务，完成后打勾；
> - 每个任务都要有单元测试或可复现的验收命令；
> - 涉及协议行为的，用 ffmpeg/ffplay/VLC 或 Wireshark 做端到端验证；
> - 完成后按 AGENTS.md 把真实过程追加到 `日志-导出.md`。

## 建议顺序

1. **P0** 修 PAUSE（当前唯一功能性 bug）
2. **P1** ANNOUNCE/RECORD 推流端（原 5.1.7）
3. **A-1 ~ A-3、E-5** 鉴权 + 连接上限（部署前必须）
4. **R-6 ~ R-8、C-1 ~ C-4** 丢包恢复与质量反馈
5. **E-1 ~ E-4** 压测、pprof、指标
6. 其余按兴趣补

---

## P0 — 修复 PAUSE 语义

原问题（P0-1/P0-2 已修）：`PAUSE` 只把状态改成 `Paused`，[Stage5/5-1-6.go](Stage5/5-1-6.go) 里推流 goroutine 只监听 `sess.Done()`，不会停；再次 `PLAY` 会再起一组，RTP 重复发送。

- [x] **P0-1 每次 PLAY 建立独立的流生命周期**
  - 给 `rtspSession` 增加 `playCancel context.CancelFunc` 和对应 mutex；
  - PLAY 时以会话 context（`sess.Context()`，`Close` 时取消）为父派生 `playCtx`，把 `playCtx.Done()` 传给 `streamH264` / `streamAAC` / `sendSenderReports`；
  - 重复 PLAY 复用已有生命周期，不再起第二组 goroutine；
  - 验收：`Stage5/5-1-6_test.go` 覆盖 startPlay 幂等、stopPlay 取消消费循环、会话关闭级联取消；ffmpeg TCP 拉流 7s 后正常 TEARDOWN，SR 随 PLAY 启停。
- [x] **P0-2 PAUSE 时取消推流 goroutine，但保留会话**
  - PAUSE 调 `playCancel()`，state 置 `Paused`，Session、SSRC、UDP 端口、源订阅都保留；
  - 验收：PAUSE 后 1 秒内抓包无新增 RTP；会话仍在 `sessionMgr` 中；
  - ✅ 已实现：PAUSE 先 `stopPlay()` 再回 200；raw RTSP/TCP 客户端实测 PAUSE 后 1.2s 内 0 个 interleaved 包，再次 PLAY 返回 200 并恢复出流。
- [x] **P0-3 PLAY 幂等**
  - `Playing` 状态下重复 PLAY 返回 `200`，但不重复启动 goroutine；
  - `Paused → Playing` 只启动一组；
  - 验收：连续两次 PLAY，RTP 包速率不翻倍；
  - ✅ 已实现：PLAY 状态校验放行 `Playing`，`startPlay` 返回 `started=false` 时只回 200 并打印 `PLAY ignored: stream already running`。raw RTSP/TCP 实测 34 → 28 包/0.4s（未翻倍），PAUSE 后再 PLAY 42 包/0.4s（单组流）。
- [x] **P0-4 PAUSE → PLAY 的 seq/timestamp 连续性**
  - 每轨保存 `lastSeq`，恢复后继续递增，不回退、不重置；
  - 视频 timestamp 继续按源时间走，不重复上一帧；
  - 验收：ffmpeg 拉流中 PAUSE 10s 再 PLAY，画面正常、Wireshark 里 seq 单调；
  - ✅ 已实现：`trackStats` 增加 `seq`（uint32，写头时截断 uint16）、`ensureSSRC()`（首次随机生成后固定）、`nextSeq()`；`streamH264`/`streamAAC` 不再用局部 `seq`/`rand.Uint32()`。raw RTSP/TCP 实测 PAUSE 前后 ssrc=e39d1a52 不变、seq 17→18 连续。
- [ ] **P0-5 端到端验证脚本**
  - 用 raw socket 或 ffmpeg 构造 `PLAY → PAUSE → PLAY`；
  - 验收：服务端日志只有一次 `stream start`，无重复发送。

---

## P1 — RTSP 推流端（原 5.1.7，ANNOUNCE / RECORD）

目标：`ffmpeg -re -i input -f rtsp rtsp://127.0.0.1:8554/push` 推到本服务，其他客户端再拉流。

### P1-A 接收侧基础

- [ ] **P1-1 新增 `ExternalSource`：接受外部写入的 Frame**
  - `internal/source/external.go`：复用 `RingBuffer[Frame]`，提供 `Write(Frame)` / `Subscribe()`；
  - 缓存 SPS/PPS/最近 IDR，供中途加入的拉流客户端预热；
  - 验收：单测两个读者收到同一批帧，`Dropped()` 对慢读者生效。
- [ ] **P1-2 RTP 包解析**
  - `internal/rtp/depacketizer.go`：解析 12 字节固定头，处理 CSRC（CC）、扩展头（X）、padding（P）；
  - 记录 seq、timestamp、SSRC、marker；
  - 验收：表驱动测试覆盖普通包、带 CSRC、带扩展、padding。
- [ ] **P1-3 H.264 解包：单 NALU + FU-A 重组**
  - type 1~23 直接成包；type 28 按 S/E 位重组，丢包时丢弃半成品；
  - 处理 STAP-A（type 24）拆出多个 NALU；
  - 验收：单测覆盖单包、FU-A、乱序、缺首片、缺尾片、STAP-A。
- [ ] **P1-4 AAC 解包（RFC 3640）**
  - 解析 `AU-headers-length`、每个 `AU-header` 的 size/index，支持一包多 AU；
  - 从 SDP fmtp 读 `SizeLength` / `IndexLength` / `IndexDeltaLength`；
  - 验收：单测 1 AU/包、2 AU/包。

### P1-B RTSP 推流交互

- [ ] **P1-5 ANNOUNCE 处理**
  - 解析请求体 SDP：`m=video/audio`、`a=rtpmap`、`a=fmtp`、`a=control`；
  - 建 session 和 track 映射（control → trackID），返回 `200 OK`；
  - 不支持的编码返回 `415 Unsupported Media Type`，SDP 非法返回 `400`；
  - 验收：ffmpeg 推流时服务端日志打印 announce 的 track 列表。
- [ ] **P1-6 推流 SETUP**
  - 先支持 `RTP/AVP/TCP;interleaved=N-M`：记录通道号，不再开 UDP 端口；
  - 再支持 `RTP/AVP;unicast;client_port=x-y`：服务端分配端口对，回 `server_port`；
  - 验收：ffmpeg 推流 `-rtsp_transport tcp` 时服务端收到 `$` 帧。
- [ ] **P1-7 RECORD：接收 RTP → 解包 → 写 ExternalSource**
  - 按 PT/SSRC 路由到对应 track；
  - seq 跳变只记日志并丢弃半成品，不 panic；
  - 验收：服务端统计收到的包数/帧数与 ffmpeg 发送大致一致。
- [ ] **P1-8 转发给拉流客户端**
  - 拉流 DESCRIBE 使用 ANNOUNCE 带来的编码参数；
  - 拉流 SETUP/PLAY 复用现有逻辑，订阅 `ExternalSource`；
  - 验收：一路 ffmpeg 推、两路 ffplay 拉（TCP 和 UDP 各一路），画面同时正常。
- [ ] **P1-9 推流断开清理**
  - TEARDOWN / TCP 断开时停止接收、关闭 ExternalSource、删除会话；
  - 验收：推流端 Ctrl-C 后，会话 60s 内回收，拉流端收到流结束而不是卡死。

---

## A — RTSP 协议完整性

- [ ] **A-1 Basic 鉴权（401 challenge）**
  - 支持 `-user` / `-pass`；未带 `Authorization` 时返回 `401` + `WWW-Authenticate: Basic realm="flow"`；
  - 验收：`ffplay rtsp://user:pass@127.0.0.1:8554/live` 成功，错误密码 401。
- [ ] **A-2 Digest 鉴权（MD5，qop=auth）**
  - 生成 nonce，校验 `response = MD5(HA1:nonce:nc:cnonce:qop:HA2)`；
  - 支持 `stale=true` 让客户端重新认证；
  - 验收：ffmpeg 带 Digest 凭据可拉流；错误 nonce 返回 401。
- [ ] **A-3 鉴权接入所有方法**
  - OPTIONS 放行，其余方法要求认证；
  - 凭据从文件/环境变量读取，不硬编码；
  - 验收：未认证 DESCRIBE/SETUP/PLAY 全部 401。
- [ ] **A-4 GET_PARAMETER / SET_PARAMETER**
  - 实现为 keepalive：`GET_PARAMETER` 无 body 时返回 `200` 并 `TouchRTSP()`；
  - 验收：长时间拉流不会被 60s 超时误杀。
- [ ] **A-5 更新 OPTIONS 的 Public 头**
  - 按实际实现列出 `ANNOUNCE, RECORD, GET_PARAMETER, SET_PARAMETER` 等；
  - 验收：OPTIONS 响应与已实现方法一致。

## H / D / G — 传输与播放控制

- [ ] **H-1 HTTP tunneling（RTSP over HTTP）**
  - 支持 `GET /` 打开通道、`POST /` 发送 RTSP，双向按 base64 行编码；
  - 验收：`ffmpeg -rtsp_transport http` 能拉流。
- [ ] **D-1 Redirect（3xx）**
  - 开关 `-redirect rtsp://host:port/live`，响应 `302 Found` + `Location`；
  - 验收：ffprobe 跟随跳转成功。
- [ ] **D-2 Multicast 发送**
  - SDP 增加 `a=destination` / `a=ttl`，SETUP 支持 `Transport: RTP/AVP;multicast;...`；
  - 服务端向组播地址发送，不维护每客户端 UDP 连接；
  - 验收：本机 `239.255.0.1` 回环组播，ffplay 能收到；记录 Windows 防火墙限制。
- [ ] **G-1 Range 解析**
  - 解析 `Range: npt=...`；live 模式只接受 `npt=0-`，其他返回 `457 Invalid Range`；
  - 文件模式记录起始位置；
  - 验收：`ffprobe` 不报错，非法 Range 返回 457。
- [ ] **G-2 文件模式 seek**
  - 按 NALU 索引或字节偏移实现定位；由于 Annex-B 没有 PTS，按帧序号近似；
  - 定位后从最近 IDR 开始发；
  - 验收：指定 `Range: npt=3-` 时从约第 3 秒的 IDR 开始。
- [ ] **G-3 Scale / Rate**
  - 文件模式支持 `Scale: 2.0` 调速（改 ticker 间隔）；live 模式拒绝并返回 `551 Option Not Supported`；
  - 验收：2x 播放时长减半，timestamp 仍单调。

---

## R — RTP 数据面与接收侧

- [ ] **R-1 发送缓存（重传的基础）**
  - 每客户端/每轨保存最近 N 个已发送 RTP 包（seq → bytes），限制总内存；
  - 验收：单测环形覆盖、淘汰顺序、并发安全。
- [ ] **R-2 STAP-A 聚合**
  - 把 SPS+PPS（以及能塞进 MTU 的小 NALU）聚合成一个 STAP-A；
  - 不跨帧聚合，超过 MTU 退回单 NALU；
  - 验收：ffplay/VLC 正常播放，Wireshark 看到 type=24。
- [ ] **R-3 分片边界测试**
  - 覆盖 NALU 大小正好等于/略超 MTU、空 NALU、多片 FU-A、marker 只在尾包；
  - 加 fuzz 测试，禁止越界和死循环；
  - 验收：`go test -race` 稳定通过。
- [ ] **R-4 接收侧 jitter buffer**
  - 新增 `internal/rtpbuf`：按 seq 重排、固定 playout 延迟、重复包丢弃、缺口超时跳过；
  - 验收：单测乱序、重复、长缺口、延迟抖动。
- [ ] **R-5 丢包隐藏（PLC）**
  - 视频缺帧时重复上一帧或请求关键帧；音频缺包时静音填充；
  - 验收：模拟 5% 丢包，播放不断流、无明显卡死。
- [ ] **R-6 接收侧缺口检测 → 生成 NACK**
  - jitter buffer 发现 seq 缺口时发送 `PT=205, FMT=1`（PID/BLP），超时未补齐则放弃；
  - 验收：模拟丢包，抓包看到 NACK，重传到达后按序播放。
- [ ] **R-7 解析客户端 NACK 并重传**
  - 解析 `PT=205, FMT=1`（RTPFB/NACK）的 PID/BLP；
  - 从 R-1 缓存重发原包（同 seq）；限制重传速率防放大；
  - 验收：模拟丢包后 Wireshark 能看到重传，ffplay 花屏减少。
- [ ] **R-8 PLI / FIR 处理**
  - 解析 `PT=206, FMT=1`（PLI）和 `FMT=4`（FIR）；
  - 收到后补发 SPS/PPS + 最近 IDR，限频（如 1 次/秒）；
  - 验收：Wireshark 触发 PLI 后能看到关键帧重发。
- [ ] **R-9 RTX 重传通道**
  - SDP 协商 `rtx/90000` + `apt=96`，重传使用独立 SSRC/PT；
  - 验收：客户端支持 RTX 时走 RTX，不支持时回退同 seq 重传。
- [ ] **R-10 FEC（RFC 5109，选做）**
  - 实现简单 XOR 保护包，SDP 协商 `ulpfec`；
  - 验收：注入单包丢失，接收侧能恢复。
- [ ] **R-11 TWCC / GCC（选做）**
  - 发送 `transport-cc` 扩展头，解析 `PT=205, FMT=15` 反馈；
  - 做带宽估计与发送降级（丢非关键帧/降帧率），说明无编码器时的局限；
  - 验收：模拟带宽下降，发送速率随之下降。
- [ ] **R-12 RTP/RTCP mux（RFC 5761）**
  - SDP 增加 `a=rtcp-mux`，UDP 只开一个端口，按 PT 区分 RTP/RTCP；
  - 验收：`ffplay -rtsp_transport udp` 正常；抓包只有一个 UDP 端口对。

---

## C — RTCP 反馈闭环

- [ ] **C-1 SDES / CNAME + compound packet**
  - 每个 SR 前带 SDES CNAME（稳定值，如 `flow@host`），组成 compound packet；
  - 验收：Wireshark 解析出 CNAME，播放器不再因缺少 CNAME 告警。
- [ ] **C-2 BYE**
  - TEARDOWN / 会话回收时发送 RTCP BYE + SSRC；
  - 验收：客户端立即感知流结束。
- [ ] **C-3 解析 RR report block**
  - 解析 fraction lost、cumulative lost、extended highest seq、jitter、LSR、DLSR；
  - 验收：单测构造 RR 包，字段解析正确。
- [ ] **C-4 计算 RTT / 丢包率 / 抖动指标**
  - 用 LSR/DLSR 算 RTT；按会话/轨聚合丢包率和抖动；
  - 输出到日志和指标接口（依赖 E-4）；
  - 验收：人为丢包时指标随之变化。
- [ ] **C-5 SR 发送策略**
  - 按 RFC 3550 的带宽比例计算间隔，支持 reduced-size SR；
  - 验收：多会话时 SR 总带宽不超标。
- [ ] **C-6 反馈驱动降级**
  - 连续 N 秒丢包率 > 阈值时，跳过非关键帧或降低发送速率；
  - 说明纯转发场景无法改编码码率的限制；
  - 验收：模拟高丢包，发送帧率下降且不断流。

---

## E — 工程化与生产

- [ ] **E-1 压测客户端**
  - `cmd/loadtest`：N 路并发 DESCRIBE/SETUP/PLAY，统计收包数、丢包、建连耗时；
  - 验收：20 路并发稳定运行 5 分钟。
- [ ] **E-2 pprof 接入**
  - `-pprof :6060` 开关，仅监听 127.0.0.1；
  - 采集 CPU、heap、goroutine、block、mutex profile；
  - 验收：能出火焰图并定位到封包/发送热点。
- [ ] **E-3 压测基线与优化（原 5.1.8）**
  - 记录 20 路时的 CPU、内存、goroutine 数；
  - 目标：CPU < 30%、内存 < 200MB、goroutine 随会话线性且可回收；
  - 验收：优化前后数据对比写进 `日志-导出.md`。
- [ ] **E-4 指标与结构化日志**
  - 计数器：连接数、会话数、每轨包数/字节数、丢帧/丢包、SR/RR 数；
  - 提供 `/metrics`（Prometheus 文本）或 `/debug/vars`；
  - 验收：压测时能实时观察指标。
- [ ] **E-5 连接/会话上限与超时**
  - `-max-conns` / `-max-sessions` / 单 IP 上限；超限返回 `503` 或直接拒绝；
  - 读写超时、握手超时；
  - 验收：超限时新连接被拒，已有连接不受影响。
- [ ] **E-6 TLS（RTSPS）**
  - `-tls-cert` / `-tls-key`，`tls.Listen` 复用现有处理逻辑；
  - 验收：`ffmpeg -rtsp_transport tls -tls_verify 0` 拉流成功。
- [ ] **E-7 真实 USB 摄像头验证**
  - Windows：`ffmpeg -f dshow -i video="..." -c:v libx264 -tune zerolatency -f h264 pipe:1`；
  - Linux：`ffmpeg -f v4l2 -i /dev/video0 ...`；
  - 记录延迟、CPU、分辨率切换、断流重连；
  - 验收：实机连续运行 10 分钟无崩溃。
- [ ] **E-8 访问控制 / 限流**
  - IP 白名单（CIDR）、每 IP 建连速率限制、失败认证退避；
  - 验收：非白名单连接被拒，暴力尝试被限速。
- [ ] **E-9 资源回收测试**
  - `kill -9` 客户端、断网、半开连接各测一遍；
  - 验收：60s 内会话回收，UDP 端口释放，goroutine 数回落。
- [ ] **E-10 文档与笔记**
  - README：架构图、运行命令、协议实现范围、已知限制；
  - `docs/rtsp.md` 补充 ANNOUNCE/RECORD、RTCP 反馈、鉴权流程；
  - 验收：新人按文档能跑起来并完成一次推拉流。

---

## 完成定义（DoD）

每个任务完成需同时满足：

1. 代码 + 单元测试（涉及并发必须 `-race`）；
2. 至少一条端到端验收命令或抓包证据；
3. 不破坏既有 Stage 5 的 TCP/UDP 拉流；
4. `gofmt`、`go vet`、相关包 `go test` 通过；
5. 在 `日志-导出.md` 记录真实现象、根因和取舍。

# done.md — 剩余任务拆解（RTSP / RTP / RTCP / RTMP + 生产化）

> 当前基线（2026-11）：
>
> - **RTSP**：拉流 + 推流都已跑通 —— 文件/stdin 源、UDP/TCP interleaved、H.264/H.265（HEVC 由推流带入）+AAC、RTCP SR、PAUSE/PLAY、ANNOUNCE/SETUP/RECORD、RTP 收包解包、转发给拉流端、断开清理（P0-1~P0-5、P1-1~P1-9 全部完成）。
> - **RTMP**：simple/complex(digest) 握手、Chunk Stream、AMF0、FLV、控制消息、publish/play、Enhanced RTMP HEVC、共享注册表、main 双协议接线全部完成（M0~M5，历史计划见 [rtmp.md](rtmp.md)）。
> - **互通**：RTSP ↔ RTMP 按流名共享同一组 `ExternalSource`；本地文件回放、跨协议推拉、断开回收、HEVC 跨协议转出均已 e2e 验证。
> - **生产化**：pprof（E-2）、Prometheus `/metrics`（E-4）、连接/会话上限与超时（E-5）、白名单/建连限速/认证退避（E-8）、资源回收测试（E-9）均已实现并接入 main。
> - 当前 `go build` / `go vet` / `go test ./...` 全绿。下面这份清单是「协议完整 + 生产可用 + 产品化」之间剩余的差距。
>
> 规则：
>
> - 一次只做一个任务，完成后打勾；
> - 每个任务都要有单元测试或可复现的验收命令；
> - 涉及协议行为的，用 ffmpeg/ffplay/VLC 或 Wireshark 做端到端验证；
> - 完成后把真实现象/根因/取舍追加到 `日志-导出.md`（该文件尚未创建，见 DOC-2）。
>
> 优先级：**P0** 部署前必须；**P1** 质量/可观测性；**P2** 协议增强；**P3** 产品化扩展（按目标取舍，不属于「单机 RTSP/RTMP 转发器」这个窄场景）。

## 建议顺序

1. **P0 部署前**：A-1 ~ A-3（RTSP 鉴权）、T-3（RTMP 鉴权）、E-5（连接/会话上限）
2. **P0 基础一致性**：A-4 ~ A-6（keepalive / OPTIONS / CSeq）、E-9（资源回收）
3. **P1 弱网与反馈**：R-1 ~ R-3、R-7、R-8、C-1 ~ C-4、T-10（RTMP e2e）
4. **P1 生产化**：E-1 ~ E-4、E-6、E-8
5. **P2 协议增强**：H/D/G、R-4 ~ R-6、R-9 ~ R-12、C-5 ~ C-6、T-1/T-2/T-4/T-5/T-7/T-8
6. **P2 工程债**：DOC-1 ~ DOC-7
7. **P3 产品化**：F / P / S / O / T-6/T-9，按实际目标挑选

---

## P0（已完成）— 修复 PAUSE 语义

原问题（P0-1/P0-2 已修）：`PAUSE` 只把状态改成 `Paused`，当时的单文件版本 `Stage5/5-1-6.go`（现保留在 `stages` 分支）里推流 goroutine 只监听 `sess.Done()`，不会停；再次 `PLAY` 会再起一组，RTP 重复发送。

- [x] **P0-1 每次 PLAY 建立独立的流生命周期**
  - 给 `rtspSession` 增加 `playCancel context.CancelFunc` 和对应 mutex；
  - PLAY 时以会话 context（`sess.Context()`，`Close` 时取消）为父派生 `playCtx`，把 `playCtx.Done()` 传给 `streamH264` / `streamAAC` / `sendSenderReports`；
  - 重复 PLAY 复用已有生命周期，不再起第二组 goroutine；
  - 验收：当时的 `Stage5/5-1-6_test.go`（现保留在 `stages` 分支）覆盖 startPlay 幂等、stopPlay 取消消费循环、会话关闭级联取消；ffmpeg TCP 拉流 7s 后正常 TEARDOWN，SR 随 PLAY 启停。
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
- [x] **P0-5 端到端验证脚本**
  - 用 raw socket 或 ffmpeg 构造 `PLAY → PAUSE → PLAY`；
  - 验收：服务端日志只有一次 `stream start`，无重复发送；
  - ✅ 已实现：`test/e2e/rtsp_pause_play.py`（标准库 raw RTSP/TCP 客户端）固化 `PLAY→PAUSE→PLAY` 全流程，自动断言 SSRC 不变、seq 跨 PAUSE 单调、PAUSE 后静默、速率不翻倍、单通道单 SSRC。实测 PASS；服务端日志确认 h264/aac source 各 `start` 一次，PAUSE 后 `stream stopped`、再次 PLAY 恢复。

---

## P1（已完成）— RTSP 推流端（原 5.1.7，ANNOUNCE / RECORD）

目标：`ffmpeg -re -i input -f rtsp rtsp://127.0.0.1:8554/push` 推到本服务，其他客户端再拉流。

### P1-A 接收侧基础

- [x] **P1-1 新增 `ExternalSource`：接受外部写入的 Frame**
  - `internal/source/external.go`：复用 `RingBuffer[Frame]`，提供 `Write(Frame)` / `Subscribe()`；
  - 缓存 SPS/PPS/最近 IDR，供中途加入的拉流客户端预热；
  - 验收：单测两个读者收到同一批帧，`Dropped()` 对慢读者生效；
  - ✅ 已实现：`ExternalSource` 无源 goroutine，`Write` 按 NAL 类型缓存参数集、`Subscribe` 预加载 SPS/PPS/IDR、`Close` 结束源。3 个单测覆盖双读者同帧、慢读者丢帧（Dropped=6）、中途加入预加载，全部 `-race` 通过。
- [x] **P1-2 RTP 包解析**
  - `internal/rtp/depacketizer.go`：解析 12 字节固定头，处理 CSRC（CC）、扩展头（X）、padding（P）；
  - 记录 seq、timestamp、SSRC、marker；
  - 验收：表驱动测试覆盖普通包、带 CSRC、带扩展、padding。
  - ✅ 已实现：`Parse` 零拷贝解析，扩展头按 profile + 32 位字 length 跳过并暴露 `ExtensionProfile/ExtensionData`，padding 按尾字节截断；`Depacketizer` 在解析成功后记录最近 seq/timestamp/SSRC/marker 和包数，失败不改状态。`Unmarshal` 改为委托 `Parse`，`Marshal` 补上扩展头与 padding 写出。表驱动覆盖普通/CSRC/扩展/padding/三者组合 + 7 种错误输入 + 往返测试，全部 `-race` 通过。
- [x] **P1-3 H.264 解包：单 NALU + FU-A 重组**
  - type 1~23 直接成包；type 28 按 S/E 位重组，丢包时丢弃半成品；
  - 处理 STAP-A（type 24）拆出多个 NALU；
  - 验收：单测覆盖单包、FU-A、乱序、缺首片、缺尾片、STAP-A。
  - ✅ 已实现：`internal/rtp/h264_depacketizer.go` 新增 `H264Depacketizer.Depacketize`，单 NALU 直接返回，STAP-A 按 2 字节长度前缀拆包，FU-A 用 `frag` 跨包累积并按 S/E 收尾；seq 不连续、timestamp/SSRC 变化或新的单 NALU 到来时 `abandon()` 丢弃半成品并计入 `Dropped`，孤立中间片/尾片同样丢弃。`-race` 下单测覆盖单包（type 1/5/7/8/23）、多片 FU-A、S+E 单包、乱序、缺首片、缺中间片、缺尾片、timestamp/SSRC 变化、STAP-A 及非法长度、未知类型，另有 packer→depacketizer 往返测试。
- [x] **P1-4 AAC 解包（RFC 3640）**
  - 解析 `AU-headers-length`、每个 `AU-header` 的 size/index，支持一包多 AU；
  - 从 SDP fmtp 读 `SizeLength` / `IndexLength` / `IndexDeltaLength`；
  - 验收：单测 1 AU/包、2 AU/包。
  - ✅ 已实现：`internal/rtp/aac_depacketizer.go` 新增 `AACDepacketizer.Depacketize`，`AU-headers-length` 按 bit 解析并校验字节对齐，首个 AU-header 读 size + `IndexLength` 位绝对 index，后续读 size + `IndexDeltaLength` 位增量并累加；按 AU-size 切分 AU-data，总长不符报 `ErrAACSizeMismatch`。`ParseAACFmtp` 支持 fmtp 值或整行 SDP、大小写不敏感、缺省回退 13/3/3，非法位宽返回 `ErrAACInvalidConfig`。`-race` 下单测覆盖 1 AU/包、手写两 AU 位布局、index 累加、自定义 6/2/2 位宽、packer→depacketizer 往返及 8 类非法输入。

### P1-B RTSP 推流交互

- [x] **P1-5 ANNOUNCE 处理**
  - 解析请求体 SDP：`m=video/audio`、`a=rtpmap`、`a=fmtp`、`a=control`；
  - 建 session 和 track 映射（control → trackID），返回 `200 OK`；
  - 不支持的编码返回 `415 Unsupported Media Type`，SDP 非法返回 `400`；
  - 验收：ffmpeg 推流时服务端日志打印 announce 的 track 列表。
  - ✅ 已实现：`readRequest` 按 `Content-Length` 读取请求体（1 MiB 上限，读完后不影响后续 `$` 帧）；新增 `internal/rtsp/sdp.go` 解析 `v=`、`m=video/audio`、`a=rtpmap`、`a=fmtp`、`a=control`，支持 `streamid=N` / `trackID=N` / 完整 URL / 缺失时按轨道序号生成 `trackID=N`；只接受 H264 与 MPEG4-GENERIC，其余编码/媒体类型回 415，结构错误回 400。ANNOUNCE 创建（或复用）会话并把轨道列表存到 `Session`，建立 control→track 索引；响应带 `Session`，连接上也记录该会话，SETUP 未回带 Session 时自动复用。实测 `ffmpeg -f rtsp` 推送 mp4：OPTIONS → ANNOUNCE 返回 200，日志打印 2 条轨（video/H264/streamid=0、audio/MPEG4-GENERIC/streamid=1），后续 SETUP 复用 `sess-1`，RECORD 暂回 501（P1-6/P1-7）。`-race` 下单测覆盖 SDP 解析、control 映射、请求体读取及 200/400/415/454 响应。
- [x] **P1-6 推流 SETUP**
  - 先支持 `RTP/AVP/TCP;interleaved=N-M`：记录通道号，不再开 UDP 端口；
  - 再支持 `RTP/AVP;unicast;client_port=x-y`：服务端分配端口对，回 `server_port`；
  - 验收：ffmpeg 推流 `-rtsp_transport tcp` 时服务端收到 `$` 帧。
  - ✅ 已实现：`Session` 增加 `push` 方向标记，ANNOUNCE 建会话即置位；SETUP 记录 `isPush`、ANNOUNCE 协商出的 PT/编码，响应 Transport 追加 `mode=record`。interleaved 推流只记录 `interleavedRTP/RTCP`，不开 UDP socket；UDP 推流仍由 `openUDPPortPair` 分配 `server_port`，并新增 `serveUDPRTP` 读 goroutine（否则内核缓冲区满后丢包）。`handleInterleaved` 先按推流轨的 RTP channel 路由到 `handlePushedRTP`，再做原有 RTCP 判断；收到的媒体报文同时 `TouchRTCP` 续命。新增 `RECORD` 方法（200 + `Recording` 状态），ffmpeg 收到后才开始发 RTP；解包落源留给 P1-7。`-race` 下单测覆盖推流 interleaved/UDP SETUP、UDP server_port 实收 RTP、$ 帧 channel 路由、RECORD 的 200/454/455。端到端实测：`ffmpeg -rtsp_transport tcp` 推流日志出现 `push RTP recv (video): packets=1/100 ...`；`-rtsp_transport udp` 同样收到，server_port=50000。
- [x] **P1-7 RECORD：接收 RTP → 解包 → 写 ExternalSource**
  - 按 PT/SSRC 路由到对应 track；
  - seq 跳变只记日志并丢弃半成品，不 panic；
  - 验收：服务端统计收到的包数/帧数与 ffmpeg 发送大致一致。
  - ✅ 已实现：每条推流轨懒创建 `pushReceiver`（`H264Depacketizer` 或按 SDP fmtp 配置的 `AACDepacketizer` + `ExternalSource`）。路由以 SETUP 的 channel/端口为提示、最终按 RTP **PT** 选轨（`pushTrackByPT`），记录 SSRC 变化；H.264 支持单 NALU/STAP-A/FU-A 重组，AAC 支持一包多 AU（时间戳按每 AU +1024 递增）；解包结果复制后 `Write(Frame)`，避免复用 UDP 接收缓冲区导致帧数据被覆盖。seq 缺口按 16 位回绕计算（正数丢包/负数重复乱序），限频打日志；FU-A 缺片由解包器 `Drop` 半成品，不 panic。每 100 包打印 packets/frames/bytes/gaps，会话关闭时打印 `push recv summary`。`-race` 下单测覆盖单 NALU、IDR、FU-A 重组、FU-A 缺片丢弃后恢复、AAC 单 AU、按 PT 改路由；全量测试通过。端到端实测 5s TCP 推流（with_audio.mp4）：ffmpeg 127 视频帧 → 服务端 frames=128（含参数集）、RTP packets=183（FU-A 分片）；音频约 217 AU → frames=215、RTP packets=69（一个 RTP 包多个 AU），gaps=0。3s UDP 推流：video frames=78 / audio frames=128，gaps=0。
- [x] **P1-8 转发给拉流客户端**
  - 拉流 DESCRIBE 使用 ANNOUNCE 带来的编码参数；
  - 拉流 SETUP/PLAY 复用现有逻辑，订阅 `ExternalSource`；
  - 验收：一路 ffmpeg 推、两路 ffplay 拉（TCP 和 UDP 各一路），画面同时正常。
  - ✅ 已实现：新增 `Server.published map[streamPath]*publishedStream`（`internal/rtsp/publish.go`）。ANNOUNCE 按 URL 路径登记，`publishedStream` 保存 `announcedTrack` 和每类轨一个 `ExternalSource`；推流 SETUP 把 `mediaTrack.external` 指向它，RECORD 解包后写入。拉流 DESCRIBE 的 `buildSDP(uri)` 优先用该流的 rtpmap/fmtp（含 `sprop-parameter-sets`、AAC `config`）生成 SDP；SETUP 按 `trackID=<下标>` 或原 control 找到轨道，取 PT/编码/时钟并把 `external` 挂到拉流轨；PLAY 的 `streamH264/streamAAC` 改为接收帧源接口，已发布流轨订阅同一个 `ExternalSource`，文件/live 模式仍用全局源。推流会话关闭时 `Session.closeAll` 通过 cleanup 从注册表摘除并关闭源（reader 读完剩余帧后结束）。`ExternalSource` 增加采样率，供拉流端设置 RTCP SR 时钟。`-race` 下单测覆盖 ANNOUNCE 发布 + DESCRIBE SDP、拉流 SETUP 复用编码参数/共享源、`$` 帧转发；全量测试通过。端到端实测：ffmpeg（`-c copy`，H.264 96 + AAC 97）推 `rtsp://127.0.0.1:8554/push`，两路并发拉流分别 `-rtsp_transport tcp` 和 `udp`，5s 各解码 122/121 视频帧（约 25fps）且音视频均正常，推流端 `gaps=0`。
- [x] **P1-9 推流断开清理**
  - TEARDOWN / TCP 断开时停止接收、关闭 ExternalSource、删除会话；
  - 验收：推流端 Ctrl-C 后，会话 60s 内回收，拉流端收到流结束而不是卡死。
  - ✅ 已实现：推流 TEARDOWN 走 `handleTeardown` 删除会话；TCP 断开（Ctrl-C/kill）由 `handleConn` 的 defer 遍历 `conn.boundSessions()` 删除会话；两条路径都经 `Session.Close → closeAll` 停止接收（关闭 UDP RTP/RTCP socket，interleaved 报文因会话已删除而丢弃）、`closePushReceiver` 关闭 `ExternalSource`、cleanup 从注册表 `unpublish`。拉流端新增结束传播：`streamH264/streamAAC` 返回「源结束」标记（与 done 关闭/写失败区分），`handlePlay` 的包装 goroutine 调 `endPullSession`——先 `stopPlay`，再从会话管理器删除、`sess.Close()` 并关闭 RTSP TCP 连接，客户端读到 EOF 而不是等 60s 超时；`Session.endOnce` 防止音视频两路重复回收。`-race` 单测覆盖 TEARDOWN（会话/注册表删除、源关闭、读者读到结束）、推流 TCP 直接断开（无需 TEARDOWN 立即清理）、拉流源结束（连接关闭 + 会话回收）。端到端实测：ffmpeg 推流 + ffmpeg TCP 拉流，`taskkill /F` 模拟推流端消失后，服务端立即打印 `push recv summary` → `published stream "push" removed` → `session sess-1 closed with tcp conn` → `aac source ended` → `session sess-3 closed: pull source ended`，拉流进程随即退出（<1s），未卡到超时。

---

## A — RTSP 协议完整性

- [x] **A-1 Basic 鉴权（401 challenge）**
  - 支持 `-user` / `-pass`；未带 `Authorization` 时返回 `401` + `WWW-Authenticate: Basic realm="flow"`；
  - 验收：`ffplay rtsp://user:pass@127.0.0.1:8554/live` 成功，错误密码 401。
  - ✅ 已实现：`Server.SetAuth(user, pass)`（`internal/rtsp/server.go`），main 新增 `-user` / `-pass`；`internal/rtsp/auth.go` 解析 `Authorization: Basic base64(user:pass)`，头名/方案大小写不敏感，用户名密码用 `subtle.ConstantTimeCompare` 比较。`handleConn` 在方法分发前校验，失败返回 `401 Unauthorized` + `WWW-Authenticate: Basic realm="flow"` + `Content-Length: 0`，日志只打印 `auth failed: <method> <uri>`（不含凭据）。OPTIONS 按 A-3 约定放行；user/pass 都为空时保持原行为。`auth_test.go` 覆盖未配置/缺头/错误方案/非法 base64/缺冒号/错误用户名密码/大小写不敏感/空密码/密码含冒号，以及完整 handleConn 流程（OPTIONS 200 → 无凭据 DESCRIBE 401 → 错误密码 ANNOUNCE 401 且不建会话 → 正确凭据 ANNOUNCE 200）。`go test ./...`、`go test -race ./internal/rtsp/` 通过。e2e 实测：`flow-server -user alice -pass secret`，ffprobe 无凭据/错误密码均返回 401，`rtsp://alice:secret@127.0.0.1:8554/live` 解出 h264+aac，TEARDOWN 正常。
- [x] **A-2 Digest 鉴权（MD5，qop=auth）**
  - 生成 nonce，校验 `response = MD5(HA1:nonce:nc:cnonce:qop:HA2)`；
  - 支持 `stale=true` 让客户端重新认证；
  - 验收：ffmpeg 带 Digest 凭据可拉流；错误 nonce 返回 401。
  - ✅ 已实现：新增 `-auth-mode basic|digest`（默认 basic），`Server.SetAuthScheme(rtsp.AuthDigest)` 切换 challenge 方案。`internal/rtsp/auth.go` 实现 RFC 2617 MD5：`HA1=MD5(user:realm:pass)`、`HA2=MD5(method:uri)`、`response=MD5(HA1:nonce:nc:cnonce:qop:HA2)`，兼容无 qop 的 RFC 2069 旧式摘要；校验 realm、digest-uri 与请求 URI 一致、algorithm=MD5、qop=auth、8 位十六进制 nc，摘要用 `subtle.ConstantTimeCompare` 比较。nonce 采用「8 字节时间戳 + 16 字节随机数 + 16 字节 HMAC-SHA256 截断」的无状态格式，服务端只持有进程内随机密钥，不维护 nonce 表；有效期 10 分钟，签名有效但过期时返回 `401` + `WWW-Authenticate: Digest ..., stale=true`，伪造/未来时间戳的 nonce 直接 401。`auth_test.go` 覆盖合法摘要、错误密码/方法/URI/qop/algorithm/realm、伪造/过期/未来 nonce、无 qop 兼容、参数解析（引号内逗号与转义），以及完整 handleConn 流程（OPTIONS 200 → 无凭据 401 Digest challenge → 伪造 nonce 401 不建会话 → 过期 nonce 401+stale=true → 正确摘要 ANNOUNCE 200）。`go test ./...`、`go test -race ./internal/rtsp/` 通过。e2e 实测：`flow-server -user alice -pass secret -auth-mode digest`，ffprobe 无凭据/错误密码均 401，`rtsp://alice:secret@127.0.0.1:8554/live` 解出 h264+aac；日志确认 DESCRIBE 返回 Digest challenge，后续 SETUP/PLAY 携带摘要通过。
- [x] **A-3 鉴权接入所有方法**
  - OPTIONS 放行，其余方法要求认证；
  - 凭据从文件/环境变量读取，不硬编码；
  - 验收：未认证 DESCRIBE/SETUP/PLAY 全部 401。
  - ✅ 已实现：鉴权校验位于 `handleConn` 的 `switch req.Method` 之前，只对 OPTIONS 放行，因此 DESCRIBE/SETUP/PLAY/PAUSE/RECORD/TEARDOWN/ANNOUNCE/GET_PARAMETER/SET_PARAMETER 全部在分发前要求凭据。`Server` 的凭据存储从单账号改为 `map[username]password`，新增 `SetUsers`（`SetAuth` 保留为单账号兼容包装）与 `passwordFor`；Basic/Digest 都按用户名取各自密码，未知用户走 dummy 密码 + 常量时间比较，避免用户名枚举。新增 `internal/rtsp/auth_file.go`：`ParseAuthFile` 解析 `username:password`（忽略空行/`#` 注释，按第一个冒号切分以支持密码含冒号，拒绝空用户名/缺冒号/重复用户名/空文件），`LoadAuthFile` 读取并给错误带上路径。main 新增 `-auth-file`，并支持 `FLOW_RTSP_AUTH_FILE` / `FLOW_RTSP_USER` / `FLOW_RTSP_PASS`；优先级为文件 < 环境变量 < 命令行参数，文件多账号、环境变量/命令行为单个覆盖账号，用户名空但密码非空时启动报错。测试新增 `auth_file_test.go`（正常/注释/多账号/密码含冒号/各类错误/临时文件加载）与 `auth_methods_test.go`（多用户 Basic、多用户 Digest、九个非 OPTIONS 方法未认证全部 401 且不建会话；OPTIONS 仍 200）。`go test ./...`、`go test -race ./internal/rtsp/` 通过。e2e 实测：`-auth-file` 载入 alice/bob（bob 密码 `p@ss:word`），无凭据与错误密码 401，alice 与 URL 编码后的 bob 均解出 h264+aac；`FLOW_RTSP_USER`/`FLOW_RTSP_PASS` + `-auth-mode digest` 同样 401 → 认证成功，日志只打印方案与失败方法，不泄露凭据。
- [x] **A-4 GET_PARAMETER / SET_PARAMETER**
  - 实现为 keepalive：`GET_PARAMETER` 无 body 时返回 `200` 并 `TouchRTSP()`；
  - 验收：长时间拉流不会被 60s 超时误杀。
  - ✅ 已实现：`handleConn` 的 `switch req.Method` 新增 `GET_PARAMETER` / `SET_PARAMETER` 分支，统一走 `handleKeepalive`（`internal/rtsp/handler.go`）。本服务没有可读写参数，因此不解析 body：带 `Session` 头时查会话、`TouchRTSP()` 并在响应里回显 `Session`，会话不存在返回 `454 Session Not Found`；没有 `Session` 头按服务级查询直接 `200`；带 body 也回 `200` 忽略，兼容把 SET_PARAMETER 当保活的客户端。鉴权和 CSeq 沿用统一入口，不需要额外处理。测试新增 `internal/rtsp/keepalive_test.go`：直接调用 handler 覆盖有效会话 200 + `LastActivity` 前进、未知会话 454、无 Session 头 200；另把 Manager 超时/扫描周期压缩为 200ms/20ms，走真实 `Sweep` 路径每 50ms 发一次 GET_PARAMETER 持续 600ms，验证会话不会被回收，停止保活后仍会被兜底回收。`go test ./...`、`go test -race ./internal/rtsp/ ./internal/session/` 通过。e2e 用裸 RTSP 客户端实测：SETUP 建会话后 GET_PARAMETER / 带 body 的 SET_PARAMETER 均 200，未知会话 454，无 Session 头 200；OPTIONS 的 `Public` 头尚未同步列出这两个方法（A-5）。
- [x] **A-5 更新 OPTIONS 的 Public 头**
  - 现状 `Public` 只列到 `TEARDOWN`，缺 `GET_PARAMETER`/`SET_PARAMETER`；
  - 按实际实现列出 `ANNOUNCE, RECORD, GET_PARAMETER, SET_PARAMETER` 等；
  - 验收：OPTIONS 响应与已实现方法一致。
  - ✅ 已实现：`internal/rtsp/handler.go` 新增包级常量 `publicMethods`，内容为 `OPTIONS, DESCRIBE, ANNOUNCE, SETUP, PLAY, PAUSE, RECORD, TEARDOWN, GET_PARAMETER, SET_PARAMETER`，OPTIONS 分支直接引用，注释说明必须与 `handleConn` 的 switch 分支同步维护。新增 `internal/rtsp/options_test.go`，走完整 `handleConn` 发 OPTIONS，解析 `Public` 头后同时做正向（十个方法一个不少）和反向（不能出现未实现方法）校验，防止以后新增分支漏更新或声明了未实现的方法。`go test ./...`、`go test -race ./internal/rtsp/` 通过。e2e 实测 OPTIONS 响应头为 `Public: OPTIONS, DESCRIBE, ANNOUNCE, SETUP, PLAY, PAUSE, RECORD, TEARDOWN, GET_PARAMETER, SET_PARAMETER`。
- [x] **A-6 CSeq 顺序 / 重放校验（P1）**
  - 现在只解析 CSeq，不校验单调递增；同一 CSeq 重放或跳号仍会被处理；
  - 每连接记录上一条 CSeq，拒绝 ≤ 旧值或跳号过大的请求（可配置开关，避免误伤并发请求）；
  - 验收：重放旧 CSeq 返回 `400`/忽略，正常 ffmpeg 推拉不受影响。
  - ✅ 已实现：新增 `internal/rtsp/cseq.go` 的 `cseqGuard`，每条 TCP 连接一份，记录 `last/seen`。规则：首条请求只要求带 CSeq 且非负（不要求从 1 开始）；之后必须严格大于上一条；`maxGap>0` 时额外拒绝跳号超过该值的请求；缺少 CSeq、负数、重放、回退都返回 `400 Bad Request` 并回显冲突的 CSeq，只丢弃这一条、不关连接，且被拒绝的请求不推进 `last`，避免一条异常请求卡死后续正常请求。校验放在 `handleConn` 中 `readRequest` 之后、鉴权与分发之前，因此 401 challenge 请求也会正常推进计数；不同连接各自计数，新连接从 1 开始不受影响。`Request` 的 CSeq 解析改为头名大小写不敏感（`headerValue`）。配置新增 `Server.SetCSeqCheck(enabled, maxGap)`（默认开启、maxGap=0 不限制跳号）和 main 参数 `-cseq-check` / `-cseq-max-gap`。测试新增 `internal/rtsp/cseq_test.go`：guard 单元表（首条任意值、严格递增、重放/回退、缺 CSeq、负数、跳号边界、关闭开关、拒绝不推进）以及完整 `handleConn` 流程（重放/回退/缺 CSeq 返回 400、小写 cseq 可识别、重放的 TEARDOWN 不进入分发且会话保留、每连接重置、关闭开关、maxGap 生效）。`go test ./...`、`go test -race ./internal/rtsp/` 通过。e2e 实测：真实 TCP 上 CSeq 1/2 正常，重放 2 与回退 1 返回 400，随后 CSeq 3 仍正常；ffprobe 拉流解出 h264+aac，ffmpeg RTSP 推流后另一路 ffprobe 拉流解出 h264，服务端仅记录 2 次预期内的 cseq rejected。注意：CSeq 校验用于协议健壮性，不是防重放安全机制（Digest response 不覆盖 CSeq，且未校验 nc 单调递增）。
- [x] **A-7 live/文件模式 DESCRIBE 参数集一致性（P2）**
  - 现状 live 模式 DESCRIBE 不返回 `sprop-parameter-sets`，依赖带内参数集；
  - 源启动后缓存首个 SPS/PPS，DESCRIBE 时若已就绪则带上；未就绪保持现状；
  - 验收：`ffprobe` 能直接读出 SDP 参数集，带内回退仍可用。
  - ✅ 已实现：`internal/media/spa_pps.go` 抽出 `SPSPPSBase64(sps, pps []byte) string` 公共格式化函数，`GetSPSPPSBase64`（文件模式读文件）复用它。`internal/source/h264.go` 的 `H264Source` 新增 `SPSPPSBase64()`，从 run 已缓存的 SPS/PPS 直接编码，未启动或未读到参数集时返回空串（源结束 `finish` 后 cur 置空同样返回空）。`internal/rtsp/handler.go` 的 `buildSDP` live 分支改为：先尝试 `s.video.SPSPPSBase64()`，非空则拼上 `; sprop-parameter-sets=...`，否则维持现状省略（单个客户端 DESCRIBE 先于 PLAY、参数集还没缓存时仍是带内初始化）。测试新增 `internal/source/source_test.go` 的 `TestH264SourceSPSPPSBase64`（io.Pipe 实时源，启动前为空、写到 SPS/PPS/IDR 后能取到 base64；用三个 NALU 是因为流式 demux 要读到下一个起始码才吐出上一个 NALU）与 `internal/rtsp/describe_test.go` 的 `TestBuildSDPLiveParamSets`（源未就绪时 SDP 无 sprop、参数集缓存后 SDP 含 sprop）。`go test ./...`、`go test -race ./internal/source/ ./internal/rtsp/ ./internal/media/` 通过。

## H / D / G — 传输与播放控制

- [x] **H-1 HTTP tunneling（RTSP over HTTP）**
  - 同一端口嗅探 `GET `/`POST`，按 `x-sessioncookie` 配对 GET 数据通道与 POST 命令通道；
  - 上行（POST）流式 base64 解码后复用完整 RTSP 协议栈，下行（GET）明文（与 FFmpeg/LIVE555 一致，只有 POST 是 base64）；
  - 隧道内 SETUP 强制 `RTP/AVP/TCP;interleaved`；
  - 验收：`ffmpeg -rtsp_transport http -i rtsp://127.0.0.1:8554/live -t 3 -f null -` 拉流成功。
- [x] **D-1 Redirect（3xx）**
  - 开关 `-redirect rtsp://host:port/live`，对 OPTIONS/DESCRIBE 回 `302 Found` + `Location`（鉴权之前，不建会话）；
  - 验收：`ffprobe rtsp://127.0.0.1:8554/live` 打印 `Status 302: Redirecting to ...` 并成功列出目标服务器的流，`-rtsp_transport http` 隧道模式同样生效。
- [x] **D-2 Multicast 发送**
  - SDP 每条轨带 `a=destination` / `a=ttl`；SETUP 支持 `Transport: RTP/AVP;multicast`，回 `destination`/`port`/`ttl`；
  - 组播用固定组地址（默认 `239.255.0.1`，视频 5000-5001、音频 5002-5003），只开一个发送 socket（TTL + 回环），不按客户端开 UDP 端口对；
  - 验收：`ffmpeg -rtsp_transport udp_multicast` 本机回环拉流成功（54 帧视频 + 音频）；
  - Windows 防火墙限制：默认不放行来自网卡的组播入站流量，本机回环不受影响；跨主机组播需 `-multicast-ttl>1` 并放行防火墙规则。
- [x] **G-1 Range 解析**
  - `internal/rtsp/range.go` 解析 `Range: npt=...`（秒数、`mm:ss`、`hh:mm:ss`、`now-`，起止可省略）；
  - live 模式（`-live` 或 ANNOUNCE 推来的流）只接受从 0 开始的开放区间，`npt=0-` / `npt=0.000-` 放行，其他返回 `457 Invalid Range` 且不改会话状态；
  - 文件模式把起点/终点记录在 `Session` 上（`setPlayRange`/`playRange`），供 G-2 seek；200 响应回显 `Range`；
  - 验收：`ffprobe` 文件/live 模式均正常（它默认发 `Range: npt=0.000-`）；裸 RTSP 发 `npt=5-` / `npt=0-10` 返回 457，`npt=0-` 返回 200。
- [x] **G-2 文件模式 seek**
  - `H264Demux`/`AACDemux` 增加带偏移的读取（`NextNALUWithOffset` / `NextAUWithOffset`）和 `New...At` 从指定偏移打开；
  - `H264Source.SubscribeFrom` 首次调用时扫描文件建索引：只记 IDR 的字节偏移、帧序号（25fps 下 `npt/40ms`）、时间戳和参数集，取不超过目标的最后一个 IDR；seek 不复用共享 ring buffer，而是为这次播放单独起一条 run，时间戳从 `frameIdx*3600` 开始；
  - `AACSource.SubscribeFrom` 按「每 AU 1024 采样」算目标序号，从对应 ADTS 帧偏移开始，时间戳 `auIdx*1024`，保持音视频内容对齐；
  - RTSP 层 `streamH264`/`streamAAC` 接收本次 PLAY 的起点，`start>0` 走 `SubscribeFrom`；live/推流源在 G-1 已被 457 拦截，`ExternalSource.SubscribeFrom` 只做兜底报错；
  - 验收：裸 RTSP `Range: npt=3-` 首个 RTP 包 `ts=270000`（3.000s），NAL 顺序 SPS/PPS/IDR；`ffmpeg -ss 3 -t 2` exit 0，日志 `h264 seek source: start at frame 75 (ts=270000)`、`aac seek source: start at offset ... (ts=...)`；
  - 说明：Range 的终点仍只记录不执行；Playing 状态下重复 PLAY 改 Range 不会重启推流（ffmpeg 的 seek 走 PAUSE + PLAY，不影响验收）。
- [x] **G-3 Scale / Rate**
  - `internal/rtsp/scale.go` 解析 `Scale` 头（正浮点数，`NaN`/`Inf`/`0`/负数/非数字返回 400）；
  - live 模式（`-live` 或 ANNOUNCE 推来的流）非 1 倍速一律 `551 Option Not Supported`，不改会话状态；文件模式记录倍率并回显 `Scale`；
  - 调速不改时间戳，只改发送节奏：`H264Source`/`AACSource` 的 `SubscribeFrom` 把 `scale` 传给 `scaledInterval(base, scale)`（`base/scale`，下限 1ms）作为 ticker 间隔，所以 `start=0, scale!=1` 也走每会话独立 run；`streamH264`/`streamAAC` 用 `start>0 || scale!=1` 判断是否走 `SubscribeFrom`；
  - 验收：裸 RTSP `Scale: 2.0` 时 2s 窗口内 RTP 时间戳推进 4.000s（media/wall≈2.02x），`Scale: 0.5` 时 3.04s 窗口推进 1.520s（≈0.50x），时间戳始终单调；`npt=3-` + `Scale: 2.0` 从 `ts=270000` 起同样 2x；live 模式 `Scale: 2.0`/`0.5` 返回 551，`1.0` 返回 200；`ffprobe`/`ffmpeg` 回归正常；
  - 说明：已在 Playing 状态下重复 PLAY 改倍率不会重启推流（与 Range 一致）。

---

## R — RTP 数据面与接收侧

- [x] **R-1 发送缓存（重传的基础）**
  - `internal/rtp/send_cache.go` 新增 `SendCache`：固定长度环形数组按 seq 存完整 RTP 包（含头），同时受包数（默认 512）和字节数（默认 1 MiB）限制，超限时从最旧开始淘汰；单包超过字节预算不缓存，重复 seq 原地替换，seq 回绕（65535→0）按精确值查找；内部 mutex 保证发送/重传查询并发安全；
  - `mediaTrack` 增加 `sendCache`，SETUP 时只为单播拉流轨创建（推流轨只收不发、组播轨没有按客户端重传语义，保持 nil）；`streamH264`/`streamAAC` 在 `writeRTP` 成功后调用 `cacheSent`，只缓存成功发送的包；`cachedPacket(seq)` 供 R-7 取包重传；
  - 验收：`internal/rtp/send_cache_test.go` 覆盖环形覆盖与淘汰顺序、字节预算、单包超预算、seq 回绕、重复 seq、空缓存、默认容量、并发 Add/Get（`go test -race` 通过）；`internal/rtsp/send_cache_test.go` 验证发送循环确实把包写入缓存；`ffprobe`/`ffmpeg` 与 `Scale: 2.0` 回归正常。
  - 说明：缓存只占内存、不改变线上行为；NACK 解析与限速重传在 R-7 实现。
- [x] **R-2 STAP-A 聚合**
  - `internal/rtp/h264_packer.go` 新增 `PackFrame(nalus [][]byte)`：同一帧的 NALU 贪心聚合成 STAP-A（1 字节头 + 每个 NALU 2 字节长度 + 数据），头里 NRI 取所有被聚合 NALU 的最大值；批次放不下时先 flush，单个大 NALU 继续走原来的 single/FU-A；返回包的 marker 只在整帧最后一个包上置位；
  - `internal/rtsp/stream.go` 的 `streamH264` 把非 VCL NALU（SPS/PPS/SEI 等）按时间戳攒成 pending，遇到同时间戳的 slice 一起交给 `PackFrame`；时间戳变化、累计超过 MTU 或源结束时立即结算，保证不跨帧聚合；实际输出形如 `STAP-A(SEI,SPS,PPS)` + FU-A(IDR)；
  - 发送路径（seq/timestamp/SSRC/`recordSent`/R-1 发送缓存）统一走 `sendFrame`，STAP-A 包同样可被 NACK 重传；
  - 验收：`internal/rtp/h264_packer_test.go` 覆盖聚合、NRI、MTU 边界、放不下退回单包、大 NALU 回退 FU-A、marker、空 NALU；`internal/rtsp/stap_test.go` 验证 PLAY 侧聚合、不跨帧、marker 只在尾包；裸 RTSP 探针首包为 `nal=24`，STAP-A 内为 SEI(6)/SPS(7)/PPS(8)；`ffprobe`/`ffmpeg -t 3` exit 0，`Scale: 2.0` 回归 2.00x；`go test -race` 通过。
  - 说明：聚合按 RTP 时间戳判定同一访问单元；源的每个 NALU 仍单独入 ring buffer，未改动 Frame 语义。
- [x] **R-3 分片边界测试**
  - `internal/rtp/h264_packer_test.go` 补齐确定性边界：`TestPackFUAExactDivision`（数据正好整除以每片容量时不多出空尾片，多 1 字节才加片）、`TestPackTinyMTU`（MTU=1/2/3 的表驱动用例，要么合法产出要么报错）、`TestPackFrameTinyMTU`（极小 MTU 下聚合回退后仍能报错/单字节 NALU 正常成包）；原有 MTU/MTU+1、空 NALU、多片 FU-A、marker 用例保留；
  - 新增 `internal/rtp/packer_fuzz_test.go`：`FuzzPack` 与 `FuzzPackFrame`，共同校验载荷不超过 MTU、Version/PT、marker 有且仅在尾包、包数有上界（防死循环式产出），并分别用 `reassemblePack` 还原单 NALU、用 `flattenPackets` 还原 STAP-A/FU-A 后的 NALU 列表；fuzz 输入的 NALU 首字节规范成 type 1，避免随机类型与测试侧解析歧义；
  - 验收：`go test -race ./internal/rtp/ ./internal/rtsp/` 稳定通过；`FuzzPack` 约 247 万次、`FuzzPackFrame` 约 72 万次执行无崩溃/越界；`-race` 下 fuzz 短跑 6.5 万次通过；fuzz 未发现需要修改生产代码的问题。
- [x] **R-4 接收侧 jitter buffer**
  - 新增 `internal/rtpbuf/buffer.go`：`Buffer.Push(seq, ts, payload, now)` 拷贝载荷入队，`Pop(now)` 按 seq 顺序返回已到期的包；seq 用 `int16(seq - highestSeq)` 扩展成 64 位，65535→0 回绕不影响排序，向后超过 32767 或超出容量窗口的跳变丢弃；
  - playout 延迟固定为 `arrival + delay`；第一个包输出前 `next` 跟随队列最小 seq，避免先到的乱序包把后到的更小 seq 误判为过期；开始输出（或跳过缺口）后游标固定，重复包、已播放包、迟到补缺口包全部拒绝；
  - 缺口处理：`next` 缺失时，等缺口之后最早的包也满一个 delay 就跳过缺口继续输出，不会无限等待；容量满时淘汰最小 seq；内部互斥锁保证接收 goroutine 与播放/NACK goroutine 并发安全；时间全部由调用方传入，单测不 sleep；
  - 目前仅作为独立组件，尚未接入 `internal/rtsp/record.go` 的接收路径（留给 R-5/R-6）；
  - 验收：`internal/rtpbuf/buffer_test.go` 覆盖延迟边界（差 1ms 不输出、到点输出）、乱序重排、输出前/后重复包、长缺口超时跳过与迟到包拒绝、缺口在超时前补齐、到达抖动保持顺序与时间戳、seq 回绕、容量淘汰、空缓冲、零延迟、默认参数回退、并发 Push/Pop 计数一致；`gofmt`/`go vet`/`go test ./...` 通过，`go test -race ./internal/rtpbuf/ ./internal/rtp/ ./internal/rtsp/` 通过。
- [x] **R-5 丢包隐藏（PLC）**
  - 新增 `internal/plc/`：`PLI()`/`ParsePLI()` 生成/解析 RTCP PSFB PLI（PT=206、FMT=1、12 字节）；`Video` 缓存最近一个完整 AU，`Conceal()` 在连续重复上限（`DefaultMaxRepeat=5`）内返回上一帧；`SilenceAU()` 提供 AAC-LC 单/立体声静音 AU（`01 18 20 07` / `21 10 04 60 8c 1c`），`AACLCChannels()` 从 AudioSpecificConfig 解析声道；`MissingAUs()` 按 1024 采样步长计算缺失 AU 数并封顶；
  - `internal/rtsp/record.go` 视频改为按 AU 攒齐再发布（marker 或时间戳变化结算）：seq 缺口落在 AU 内、解包器丢弃不完整 FU-A、或 AU 首包前已有缺口时，整帧丢弃、用上一帧占住该时间戳，并按 500ms 限频向上游发 PLI 请求关键帧；完整 AU 才写入缓存作为后续重复源；
  - 音频按时间戳缺口补 AAC-LC 静音 AU（每个 1024 采样），非 AAC-LC/未知声道不补，避免写入非法 AU；迟到/重复包按扩展时间戳差丢弃；
  - 关键取舍：H.264 有 B 帧时 RTP 时间戳是 PTS，到达顺序不单调（编码顺序 I/P 先到、B 后到），因此 AU 边界只按“时间戳相等”分组、丢包只信 seq 缺口，不做基于时间戳步长的缺帧推断；否则正常流会被误判为缺帧并丢弃 B 帧。SSRC 变化时清空半成品和 PLC 缓存；
  - 验收：单测覆盖 PLI 往返/截断、静音 AU 与声道解析、缺失 AU 计算与封顶、重复上限/深拷贝/Reset，以及推流路径的损坏 AU 隐藏、PLI 限频、B 帧乱序时间戳不触发 PLC、视频/音频各 100 个 AU 每 20 包丢 1 包的 5% 丢包时间轴连续性；
  - 真实 e2e：`testdata/with_audio.mp4`（10s，含 B 帧）经 TCP 代理每 20 个 RTP 包丢 1 个（实测 509 包丢 25，约 4.9%），推流 10s、拉流 `ffmpeg -t 3 -f null -` exit 0 不断流；服务端 video packets=349 gaps=19 → plc_drop=19/repeat=19/PLI=11，audio packets=135 gaps=6 → silence=12；无丢包对照 gaps=0、plc_repeat=0、PLI=0；解码器对丢弃参考帧有 `Missing reference picture` 告警属预期，后续帧继续解码；
  - `gofmt -l .`/`go vet ./...`/`go test ./...` 通过，`go test -race ./internal/plc/ ./internal/rtsp/` 通过。
- [x] **R-6 接收侧缺口检测 → 生成 NACK**
  - 新增 `internal/rtcp/nack.go`：`NACK()`/`ParseNACK()` 生成/解析 RTCP Generic NACK（PT=205、FMT=1、12 字节头 + 4 字节 FCI）；seq 去重排序后用 PID/BLP 合并（BLP 第 i 位表示 PID+i+1 也缺失），处理 65535→0 回绕，单包最多 16 个 FCI；
  - 新增 `internal/rtcp/nack_tracker.go`：`Observe(seq)` 用带符号差值跟踪 `highest`，只在向前跳变时登记缺口；乱序/重传到达的包（差值 < 0）从缺口集合删除，SSRC 变化清空重来；`Pending(now)` 先过宽限期（`DefaultNACKDelay=20ms`，避免把乱序当丢包），再按全局最小间隔（20ms）限速，每个 seq 最多请求 3 次，超限直接放弃，单包最多 32 个 seq，最多跟踪 256 个缺口；
  - `internal/rtsp/record.go` 接入：`handlePushedRTP` 先 `Observe`，再入 jitter buffer，然后立即 `sendNACK`；NACK 走 `track.writeRTCP`（TCP 交织 channel 或 UDP `client_port+1`），媒体 SSRC 取推流端 SSRC、发送 SSRC 取本端；
  - 定时器：`scheduleDrainLocked` 为每轨维护一个 `time.AfterFunc`，唤醒时间取 `min(jitter buffer 到期, NACK 宽限期)`；出现待重传缺口时把定时器提前，保证 NACK 在 jitter buffer 跳过缺口之前发出；`drainAt`/`drainGen` 代次号防止 `Stop` 失败后旧回调清掉新定时器；缓冲为空但仍有待重传 seq 时也会继续调度；
  - `internal/rtpbuf/buffer.go` 补充 `Delay()`、`Drain()`、`Reset()`：`Drain` 按扩展 seq 顺序取走全部包并复位，用于接收端关闭时把尾部数据按序冲刷出去；
  - 验收：单测覆盖 NACK 往返、BLP 合并、回绕、非法包拒绝、FCI 上限，以及 tracker 的缺口登记、宽限期、乱序不误报、重传取消、限速与 3 次放弃、SSRC 切换、重复包/缺口上限、单包条数上限；`TestPushNACKRetransmitKeepsOrder` 丢 1 个包后读到含该 seq 的 NACK，注入重传包后 AU 按 `0x0A,0x0B,0x0C` 顺序恢复，`plc_drop=0`；
  - 真实 e2e：本地代理缓存转发的 RTP 并按 `PT=205/FMT=1` 的 PID/BLP 回灌原包；`testdata/with_audio.mp4` 每 20 包丢 1 包（509 包丢 25），服务端发出 25 个 NACK（video 20、audio 5），代理全部命中缓存重传 25 次；最终 video `packets=364 gaps=0 plc_drop=0 plc_repeat=0 pli=0`、audio `packets=137 gaps=0`，拉流 `ffmpeg -t 3 -f null -` exit 0，重传后无缺口、按序播放；
  - `gofmt -l .`/`go vet ./...`/`go test ./...` 通过，`go test -race ./internal/rtcp/ ./internal/rtpbuf/ ./internal/rtsp/` 通过。
- [x] **R-7 解析客户端 NACK 并重传**
  - 新增 `internal/rtcp/compound.go`：`Walk` 按 RTCP length 字段遍历复合包（客户端通常发 RR+NACK），`ParseNACKs` 提取所有 `PT=205, FMT=1` 的 PID/BLP，支持一个复合包里多条 NACK、多个 media SSRC；
  - 重传接在 `handleIncomingRTCP`，mux / 独立 UDP RTCP / TCP interleaved 三条路径自动生效；先按 `fb.MediaSSRC == track.currentSSRC()` 过滤，再交给 `retransmit`；
  - 新增 `internal/rtsp/retransmit.go`：seq 去重、单条 NACK 最多处理 64 个 seq；从 R-1 `sendCache` 按 seq 取回完整 RTP 包，用 `writeRTP` 原样重发（seq/timestamp/SSRC 不变，不调用 `recordSent`/`cacheSent`，SR 统计和缓存年龄不受影响）；缓存未命中（已淘汰/未发送）只记录不造包；
  - 限速：每轨令牌桶稳态 200 包/秒、突发 64 包，`takeRTXTokens` 按经过时间补充；超限停止本轮重传并打印 `throttled`；
  - 验收：`internal/rtcp/compound_test.go` 覆盖复合包、多 NACK、非 NACK、非法 FMT、截断 length；`internal/rtsp/retransmit_test.go` 覆盖原包字节一致重传、重复 seq 只发一次、未缓存 seq 不回应、其他 media SSRC 忽略、无缓存不 panic、令牌桶补充/上限、去重；真实 e2e 用循环推流 + 裸 RTSP 探针分别走 legacy 双端口与 rtcp-mux 单端口，NACK 一个已发送 seq 均收到 1 个字节完全相同的重传，NACK 未发送 seq 无回应；`gofmt`/`go vet`/`go test ./...` 与 `go test -race ./internal/rtcp/ ./internal/rtsp/` 通过。
- [x] **R-8 PLI / FIR 处理**
  - `internal/plc/fir.go` 新增 FIR（`PT=206, FMT=4`）构造/解析：20 字节，目标 SSRC 取 FCI 的 `[12:16]`（头部 media SSRC 按 RFC 惯例为 0），带 FIR 序号；PLI（`FMT=1`）继续复用 `plc.PLI/ParsePLI`；
  - `handleIncomingRTCP` 用 `rtcp.Walk` 逐个子包识别 PLI/FIR（支持 RR + PSFB 复合包）：PLI 匹配 media SSRC、FIR 匹配 FCI target SSRC，SSRC 为 0 时按通配处理，只对命中本轨的请求重放；
  - `streamH264` 的 `sendFrame` 识别 IDR（type 5）与 SPS/PPS（type 7/8）：整帧发送成功后把完整 RTP 包存入 `mediaTrack.lastKeyframe`；若参数集单独成帧则存 `lastParamSets`，重放时在 IDR 前补发（`kfHasParams` 避免重复）；
  - `internal/rtsp/keyframe.go` 实现 `replayKeyframe`：按原 seq/timestamp/SSRC 原样重放，不调用 `recordSent`/`cacheSent`（SR 统计不变）；每轨限频 1 秒；空缓存直接忽略；
  - 只对拉流轨生效（推流轨没有关键帧缓存）；UDP、rtcp-mux、TCP interleaved 三条路径因共用 `handleIncomingRTCP` 同时支持；
  - 验收：`internal/plc/fir_test.go` 覆盖 FIR 往返与非法包；`internal/rtsp/keyframe_test.go` 覆盖 PLI/FIR 重放、参数集前置、复合包、SSRC 不匹配忽略、1 秒限频与窗口恢复、空缓存、`streamH264` 对“参数集单独帧 + IDR 帧”的分别缓存；真实 e2e 用循环推流 + 裸 RTSP 探针等到 IDR 后发 PLI/FIR，legacy 双端口与 rtcp-mux 单端口均收到成组旧 seq 的关键帧包（服务端日志 `RTCP PLI/FIR (video): replay keyframe (N packets)`），直播流不中断；
  - `gofmt`/`go vet`/`go test ./...` 与 `go test -race ./internal/plc/ ./internal/rtcp/ ./internal/rtsp/` 通过。
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
- [x] **R-12 RTP/RTCP mux（RFC 5761）**
  - SDP 增加 `a=rtcp-mux`：本地文件/live 的 `buildSDP` 与推流 `publishedSDP` 的视频、音频段都宣告；
  - `SETUP` 判定 mux：unicast 且（Transport 带 `rtcp-mux` 参数，或 `client_port=A-A`）；TCP interleaved 与组播不走 mux；payload type 64~95 按 RFC 5761 §4 拒绝 mux（marker 置位后第二字节会撞上 RTCP 类型区间）；
  - mux 模式用 `openUDPPort` 只开一个 socket，响应回 `client_port=A-A;server_port=P-P`；`mediaTrack.rtcpMux` 标记后 `writeRTCP`（SR/PLI/NACK）全部走同一个 `rtpConn`；
  - 收包由 `serveUDPMux` 单读协程处理：`rtcp.IsRTCP` 按第二字节 PT（192~223）分派，RTCP 交给 `handleIncomingRTCP`，推流 RTP 仍进 `handlePushedRTP`。一个 socket 只能有一个并发 `ReadFromUDP`，因此 mux 时不再启动独立 RTCP 读协程；
  - `internal/rtcp.IsRTCP` 同时替换 TCP interleaved 原来的 200~204 判断，RTCP 类型范围扩到 192~223（含 SR/RR/SDES/BYE/APP、RTPFB/NACK、PSFB/PLI），校验 V=2 和 length 字段；三条传输路径的 RTCP 处理统一到 `handleIncomingRTCP`；
  - 未协商 mux 的客户端（如 ffmpeg 8.0.1，SETUP 固定发 `client_port=X-X+1`）继续走原双端口路径，行为不变；
  - 验收：单测覆盖 `client_port=A-A`、显式 `rtcp-mux`、单 socket 双向收发（客户端同一端口发 RTP/RTCP 给服务端，并收到服务端从 `server_port` 发回的 RTCP）、RTCP 不计入 RTP 统计、PT 冲突回退；e2e 用 `test.mp4` 循环推流 + 裸 RTSP 客户端 `client_port=31900-31900`，SETUP 回 `server_port=50000-50000`，7 秒内在同一 socket 收到 `rtp=294 rtcp=1`，不存在第二个 UDP 端口；`ffmpeg -rtsp_transport udp` 拉流 exit 0；
  - `gofmt`/`go vet`/`go test ./...` 通过，`go test -race ./internal/rtcp/ ./internal/rtsp/` 通过。

---

## C — RTCP 反馈闭环

- [x] **C-1 SDES / CNAME + compound packet**
  - 新增 `internal/rtcp/sdes.go`：`CNAME(ssrc, name)` 构造 `PT=202` 的 SDES 包（V=2、SC=1，item type=1，END item 后补零到 32 位边界，length 字段包含补齐）；`ParseCNAME` 供测试和抓包排查；
  - `internal/rtcp/compound.go` 增加 `Compound(...)` 按序拼接子包；`mediaTrack.senderReport` 改为返回 `Compound(SR, CNAME)`，SR 仍是首包（RFC 3550 §6.1）；
  - CNAME 取包级 `localCNAME = "flow@" + hostname`，跨会话、跨 PAUSE/PLAY 稳定；video/audio 共用同一个 CNAME（标识源），SSRC 各自独立；
  - UDP 双端口、rtcp-mux 单端口、TCP interleaved 三条路径共用 `writeRTCP`，无需改动；`IsRTCP` 对 compound 仍按首包 SR 的 PT 和 length 判定；
  - 验收：`internal/rtcp/sdes_test.go` 覆盖 CNAME 编解码、1~12 字节 CNAME 的 32 位对齐与 length 字段、非法包拒绝、`Compound(SR, SDES)` 遍历顺序与 `IsRTCP`；`internal/rtsp/sender_report_test.go` 覆盖 SR 后跟 SDES、CNAME 与 SR SSRC 一致、video/audio CNAME 相同而 SSRC 不同；
  - 真实 e2e：裸 RTSP 探针在 legacy 双端口与 rtcp-mux 单端口下均收到 `pts=[200, 202]` 的复合包，`cname='flow@DESKTOP-NTFNG9O'`，SDES SSRC 与 SR/RTP SSRC 一致；`ffmpeg -rtsp_transport udp` 拉流 4 秒 exit 0；
  - `gofmt`/`go vet`/`go test ./...` 与 `go test -race ./internal/rtcp/ ./internal/rtsp/` 通过。
- [x] **C-2 BYE**
  - `internal/rtcp/bye.go`：`BYE(ssrcs...)` 构造 `PT=203` 单/多 SSRC 包（SC=N、length=N，不带 reason），`ParseBYE` 供测试和抓包排查；
  - `mediaTrack.sendBye` 复用 `senderReport` 的 SR+SDES 再追加 BYE，组成 `SR+SDES+BYE` 复合包（RFC 3550 §6.1 要求以 SR/RR 开头）；`senderReport` 返回 false（从未发过 RTP）时按 §6.3.7 跳过；
  - 接在 `Session.closeAll` 关闭 socket 之前，TEARDOWN、TCP 断开、60s 超时、推流源结束四条回收路径全覆盖；UDP / rtcp-mux 正常送达，TCP 已断时 interleaved 写失败只记日志；
  - 验收：`internal/rtcp/bye_test.go` 覆盖单/多 SSRC 编解码与非法包；`internal/rtsp/bye_test.go` 覆盖复合包顺序 `[200,202,203]`、SSRC 与 RTP 一致、无 RTP 时不发 BYE、`closeAll` 在关 socket 前发出 BYE；真实 e2e 探针在 legacy 与 rtcp-mux 下 TEARDOWN 后均收到 `pts=[200,202,203]` 的 BYE；
  - `gofmt`/`go vet`/`go test ./...` 与 `go test -race ./internal/rtcp/ ./internal/rtsp/` 通过。
- [x] **C-3 解析 RR report block**
  - `internal/rtcp/rr.go`：`ReportBlock` 含 fraction lost、24 位有符号 cumulative lost（手工符号扩展）、extended highest seq、jitter、LSR、DLSR；`ParseRR` 按 RC 与 length 校验，`ParseRRs` 用 `Walk` 从复合包提取所有 `PT=201`；
  - `handleIncomingRTCP` 解析 RR，只保存 `blk.SSRC == track.currentSSRC()` 的块到 `mediaTrack.lastRR`（含上报方 SSRC、接收时刻），打印一行字段便于抓包对照；数据留给 C-4 计算 RTT/丢包率/抖动；
  - 验收：`internal/rtcp/rr_test.go` 覆盖单块/多块/空 RR、负累计丢包符号扩展、RC 与实际长度不符、错误 PT/version/short/length、`RR+SDES` 复合包；`internal/rtsp/rr_test.go` 覆盖复合包写入 `lastRR`、其他 media SSRC 忽略；真实 e2e 探针发送 `fraction=5/cumulative=12/ext_seq=10/jitter=1234` 的 RR，服务端日志输出解析结果，legacy 与 rtcp-mux 均通过；ffmpeg 拉流发送的空 RR（RC=0）也能正常解析；
  - `gofmt`/`go vet`/`go test ./...` 与 `go test -race ./internal/rtcp/ ./internal/rtsp/` 通过。
- [x] **C-4 计算 RTT / 丢包率 / 抖动指标**
  - 新增 `internal/rtsp/metrics.go`：`TrackMetrics`/`SessionMetrics` 快照；`rttFromReport` 用 RR 的 LSR/DLSR 与 NTP 中间 32 位（单位 1/65536 秒，uint32 减法自然回绕）计算 RTT；`metricsSnapshot` 把 `FractionLost/256` 作为区间丢包率、`clamp(CumulativeLost,0)/(ExtendedHighestSeq+1)` 作为累计丢包率、`Jitter/clockRate*1000` 换算抖动毫秒；会话聚合用 `Σlost/Σexpected` 加权而不是各轨百分比平均；
  - `handleIncomingRTCP` 保存 RR 后立即计算快照并输出 `RTCP metrics (session=..., video): rtt=... fraction_lost=... cumulative_lost=... (x%) jitter=...ms`；快照字段就是 E-4 `/metrics` 的数据源，E-4 落地后直接遍历 `Session.metricsSnapshot` 输出即可；
  - 验收：`internal/rtsp/metrics_test.go` 覆盖 LSR/DLSR RTT（含 LSR=0 不可用）、区间/累计丢包率、负累计丢包 clamp 为 0、90000/44100 抖动换算、无 RR 时只返回发送统计、会话加权聚合与空会话；真实 e2e 用自写 RTSP 接收端每 10 包丢 1 包并按秒发 RR，服务端日志显示 `cumulative_lost=25/259 (9.65%)`、`fraction_lost≈10%`、收到 SR 后 `rtt=0.17~1.7ms`；零丢包重跑同一探针，指标变为 `0/259 (0.00%)`、RTT 正常，证明指标随人为丢包变化；
  - `gofmt`/`go vet`/`go test ./...` 与 `go test -race ./internal/rtcp/ ./internal/rtsp/` 通过。
- [x] **C-5 SR 发送策略**
  - 新增 `internal/rtsp/rtcpsched.go`：把原来的每轨固定 5s ticker 换成服务端共享的 RTCP 带宽预算调度器。按 RFC 3550 §6.3.1 计算 `Td = max(Tmin, n × avg_rtcp_size × 8 / rtcp_bw)`，其中 `rtcp_bw = 5% × 发送方份额 50% × Σ 实测媒体码率`（单播两端各半，SR 不超过 2.5%）、`n` 是在发轨数、平均包长按 1/16 EWMA 维护并计入 28 字节 IPv4/UDP 头；实际间隔在 `Td` 之后再乘 `[0.5,1.5)` 随机因子（实际落在 2.5~7.5s、均值 5s），`Tmin` 取单播 5s；
  - 每条轨进入 PLAY 时向 `Server.rtcp` 注册、PLAY 生命周期结束时注销；`nextInterval` 用各轨 `trackStats.octets` 的增量估算码率（0.75/0.25 平滑）。会话数增加时 `n` 与总码率同时增长，单路码率不低时间隔仍由 5s 下限决定；单路码率低时 `avg_size / 码率` 变大、间隔随之拉长。两种情况下都保证 `n × avg_size / Td ≤ 2.5% × 媒体总带宽`；单会话均值仍为 5s；
  - SR 保持 `RC=0`（不带 report block，即 reduced-size SR 形态），`buildSDP`/`publishedSDP` 的每个 `m=` 段新增 `a=rtcp-rsize` 宣告（RFC 5506）；CNAME 仍随每次 SR 发送，避免削弱音视频同步；
  - 验收：`internal/rtsp/rtcpsched_test.go` 覆盖 5s 下限、间隔随轨数增长、带宽预算等式（`n×88/Td = 2.5%×B/8`）、随机区间、平均包长 EWMA、注册/注销计数、SDP 含 `a=rtcp-rsize`；`test/e2e/rtsp_rtcp_degrade.py` 用 ffmpeg 循环推流 + 自写 RTSP 接收端实测：DESCRIBE SDP 含 `a=rtcp-rsize`，9 秒内收到 1 个 60 字节 SR（落在 2.5~7.5s 的随机区间内），服务端日志每轮打印 `RTCP SR sent`；`gofmt`/`go vet`/`go test ./...` 与 `go test -race ./internal/rtsp/` 通过。
- [x] **C-6 反馈驱动降级**
  - 新增 `internal/rtsp/degrade.go`：`degradeState` 用 RR 的区间丢包率（`FractionLost/256`）驱动状态机，连续 `Sustain`（默认 3s）高于阈值（默认 10%）进入降级，连续 `Recover`（默认 5s）低于阈值才恢复；中途反向采样会重置窗口，形成迟滞。使用区间丢包而不是累计丢包，因为后者只增不减、无法作为恢复依据；
  - `handleSetup` 只为单播拉流视频轨创建降级状态（与 `sendCache` 同条件），推流/组播轨和音频轨为 nil（音频没有关键帧概念且码率小，保持完整发送）；`Server.SetDegradePolicy` + `-degrade-loss/-degrade-sustain/-degrade-recover` 可调，非法值保持默认；
  - `handleIncomingRTCP` 保存 RR 后调用 `observe`，切换时打印 `degrade on/off`；`streamH264` 的 `sendFrame` 在降级期间把含 VCL 且不含 IDR/参数集的 AU 整帧丢弃（`dropNonKeyFrame`），只保留 SPS/PPS 与 IDR，不拆 NALU、不改 seq/timestamp，音频轨不受影响；
  - 明确局限：纯转发没有编码器，无法改码率/QP/GOP，只能丢帧；丢 P 帧会破坏参考链，客户端在下一个 IDR 前可能卡顿/花屏，但 RTSP/RTP/SR 不断流。更优方案是向上游发 PLI/FIR 请求 IDR（R-8 目前只处理下游 PLI）或做 RTX/TWCC（R-9/R-11）；降级生效时间还受客户端 RR 发送周期（通常 5s）影响；
  - 验收：`internal/rtsp/degrade_test.go` 覆盖进入/恢复时间窗口、反向采样重置、非法配置回退、无状态/未激活不丢帧、降级时只发送参数集与 IDR（P 帧整 AU 被跳过、丢弃计数正确、流正常结束）；`test/e2e/rtsp_rtcp_degrade.py` 对真实 ffmpeg 推流注入 25% 丢包 RR：视频包速率从基线 39.0/s 降到 7.3/s（只剩关键帧），期间 RTP 持续流动不断流，改发 0% 丢包 RR 后恢复到 44.5/s；服务端日志对应输出 `degrade on` → `degrade (video): dropped N non-key frame(s)` → `degrade off`；`gofmt`/`go vet`/`go test ./...` 与 `go test -race ./internal/rtsp/` 通过。

---

## E — 工程化与生产

- [x] **E-1 压测客户端**
  - `cmd/loadtest`：N 路并发，每路独立 TCP 控制连接完成 DESCRIBE → SETUP(video) → PLAY，持续收流到时长达标后 TEARDOWN；
  - 支持 `-transport tcp`（interleaved，默认）与 `-transport udp`（自动绑定 RTP/RTCP 端口并写 `client_port`）；会话有 60s 空闲超时，客户端每 20s 用 `GET_PARAMETER` 保活；
  - 统计项：建连/DESCRIBE/SETUP/PLAY 耗时（p50/p95/max）、收包数与字节数、RTP 序号空洞（16 位回绕差分，乱序/重复不计丢包）、单客户端最少/平均包数；`-report` 周期输出实时速率，`-max-loss` 超阈值或会话卡流（最后 5s 无包）时退出码 1；
  - 验收：20 路并发 TCP interleaved 跑满 5 分钟，`established=20/20`、`226340` 包（754 pkt/s，190.98 MiB）、序号空洞 0（0.000%），每路 11317 包且全程无卡流；建连 p50=1.68ms/p95=2.89ms，PLAY p50<1ms。20 路 × 15s UDP 对照同样 0 丢包。`cmd/loadtest/main_test.go` 覆盖参数解析、序号回绕/乱序/重复、RTSP 响应解析与 interleaved 分流、百分位与丢包率计算。
- [x] **E-2 pprof 接入**
  - `cmd/server` 新增 `-pprof`（如 `-pprof :6060`），空值不启动；`pprofListenAddr` 强制回环：`:6060` 补成 `127.0.0.1:6060`，`0.0.0.0:6060`、局域网 IP、`localhost` 一律拒绝并以退出码 2 退出；
  - `startPprof` 用独立 `http.ServeMux` 显式注册 `Index/Cmdline/Profile/Symbol/Trace`（heap、goroutine、block、mutex 由 `Index` 按路径分发），设置 `ReadHeaderTimeout=5s`，复用 `signal.NotifyContext` 的 ctx 做 `Shutdown`；block/mutex 采样率只在开启 pprof 时设置，平时零开销；
  - 验收：20 路 TCP 拉流 + 4Mbps 转码推流跑 90s，同时抓 30s CPU profile，服务端收 500509 包（5560 pkt/s、327.74 MiB）且 0 丢包；CPU 总样本 5.92s/30s（19.73%），累计热点集中在发送链路：`streamH264` 78.9%、`writeRTP/writeInterleavedChannel` 69.3%、`WSASend` 64.5%，另有 `SendCache.Add` 2.5% 与 `findLocked` 2.2% 的 flat 开销；heap 14.9MB（`RTPHeader.Marshal` 58.5%、`NewSendCache` 10.5%），block 顶部为 `selectgo`（`streamH264` 48.5%、`sendSenderReports` 46.4% 定时等待），mutex 顶部为 `runtime.unlock`，goroutine profile 可正常导出；
  - 火焰图：`go tool pprof -http=127.0.0.1:8081 <profile>` 的 Flame Graph 页可正常打开；另用 `-raw` 输出渲染了静态 SVG（146 个栈、592 个样本），`writeInterleavedChannel`/`streamH264`/`SendCache` 均可见，确认热点能定位到封包/发送路径；
  - `cmd/server/main_test.go` 覆盖 `-pprof` 地址规范化与回环限制；`gofmt`/`go vet`/`go test ./...` 通过。
- [ ] **E-3 压测基线与优化（原 5.1.8）**
  - 记录 20 路时的 CPU、内存、goroutine 数；
  - 目标：CPU < 30%、内存 < 200MB、goroutine 随会话线性且可回收；
  - 验收：优化前后数据对比写进 `日志-导出.md`。
- [x] **E-4 指标与结构化日志**
  - 计数器：连接数、会话数、每轨包数/字节数、丢帧/丢包、SR/RR 数；
  - 提供 `/metrics`（Prometheus 文本）或 `/debug/vars`；
  - 验收：压测时能实时观察指标。
  - ✅ 已实现：新增 `internal/metrics` 并发安全注册表与 `-metrics :9090`（复用 E-2 的回环地址校验），`/metrics` 输出 `flow_connections(_total)`、`flow_sessions(_total)`、`flow_rtmp_publishes/plays(_total)`、`flow_rtsp_packets_sent_total` / `octets_sent_total` / `frames_dropped_total`（按 video/audio）、`flow_rtsp_sr_sent_total` / `rr_received_total`，抓取时额外采样 `flow_goroutines`（E-9）。RTSP/RTMP 共用同一注册表，在 accept/结束、PLAY/推流、发送与 RTCP 路径埋点；`metrics_test.go` 覆盖文本格式、gauge 回落与 nil 注册表。结构化日志未实现，日志仍为 `fmt.Printf`。
- [x] **E-5 连接/会话上限与超时**
  - `-max-conns` / `-max-sessions` / 单 IP 上限；超限返回 `503` 或直接拒绝；
  - 读写超时、握手超时；
  - 验收：超限时新连接被拒，已有连接不受影响。
  - ✅ 已实现：`internal/limiter` 为 RTSP/RTMP 各维护一份「总连接 + 每 IP」计数；accept 阶段超限时 RTSP 回 `503 Service Unavailable`、RTMP 直接断开，不进入 goroutine，已有连接不受影响。`-max-sessions` 限制 RTSP 会话数，ANNOUNCE/SETUP 超限回 503。`-handshake-timeout`（默认 10s）覆盖 RTMP 握手，`-conn-timeout`（默认 60s）覆盖 RTMP 消息读写空闲；RTSP 会话另有 60s 空闲回收。`limiter_test.go` 覆盖总数/单 IP/不限制/nil，`setup_test.go` 覆盖会话上限。
- [x] **E-6 TLS（RTSPS）**
  - `-tls-cert` / `-tls-key`，`tls.Listen` 复用现有处理逻辑；
  - 验收：`ffmpeg -tls_verify 0 -rtsp_transport tcp -i rtsps://host:8554/live/test` 拉流成功。
    （ffmpeg 8 的 `-rtsp_transport` 没有 `tls` 值，TLS 由 `rtsps://` scheme 选择。）
  - ✅ 已实现：`rtsp.Server.SetTLS(cert, key)` 用 `tls.LoadX509KeyPair` 加载 PEM（`MinVersion: TLS1.2`），`Serve` 在配置了证书时改用 `tls.Listen` 并打印 `RTSPS server listen`，accept 后的白名单/建连限速/连接上限、`remoteIP`、`RejectRTSPConn`、HTTP 隧道、鉴权与 play/publish 全部原样复用；每个 `*tls.Conn` 在 `handleConn` 前显式握手并设 10s 超时，明文客户端快速失败（日志 `tls: first record does not look like a TLS handshake`）。`cmd/server` 新增 `-tls-cert`/`-tls-key`（必须成对，不成对退出码 2；加载失败退出码 1）。`internal/rtsp/tls_test.go` 用测试内生成的自签证书覆盖：OPTIONS over TLS 成功、明文客户端被拒、缺失/不匹配证书报错。本机 e2e（自签证书 + ffmpeg 8.0.1）：`ffprobe -tls_verify 0 -rtsp_transport tcp rtsps://127.0.0.1:8554/live/test` → h264 640x480 + aac 44100；`ffmpeg ... -f rtsp rtsps://.../live/tls_push` 推流（ANNOUNCE/RECORD，881 包/827KB，gaps=0）后同 URL 用 RTSPS 拉回 → h264 320x240；明文 `rtsp://127.0.0.1:8554` 客户端被拒。证书轮换需重启，单端口 rtsp/rtsps 双协议嗅探未做。
- [x] **E-7 真实 USB 摄像头验证**
  - Windows：`ffmpeg -f dshow -i video="..." -c:v libx264 -tune zerolatency -f h264 pipe:1`；
  - Linux：`ffmpeg -f v4l2 -i /dev/video0 ...`；
  - 记录延迟、CPU、分辨率切换、断流重连；
  - 验收：实机连续运行 10 分钟无崩溃。
  - ✅ 已验证（Windows 11 + Integrated Webcam + ffmpeg 8.0.1，E-7 原始命令原样使用，未额外指定 `-preset`/`-threads`）：
    - **多 slice 缺陷与修复**：默认 `libx264 -tune zerolatency` 会启用切片线程，640x480/720p 下每帧输出 7 个 slice NALU。旧服务端把每个 slice 当成独立帧（各自时间戳、各自 RTP marker），且视频 ring 只按 64 个 NALU 计容，一次 64KB 管道读就能塞满并覆盖整批数据；15s 拉流只解出 207 帧，客户端报 `concealing 3280 DC/AC/MV` 与 `corrupt decoded frame`。修复内容：`media.StartsVideoAU` 按 H.264 `first_mb_in_slice`（H.265 用 `first_slice_segment_in_pic_flag`）识别 AU 首 NALU，同帧多 slice 共用时间戳并标记 `AUStart`，`streamH264` 把整帧聚合进同一个 RTP AU（marker 只打最后一包）；实时源时间戳强制严格递增（`nextLiveAUTS`）；视频 ring 改为 4096 帧硬上限 + 512KB 字节预算（`RingBuffer` 增加按字节淘汰）；seek 索引按 AU 计帧号。回归测试见 `internal/source/h264_au_test.go`、`internal/source/ring_test.go`、`internal/rtsp/stap_test.go`。
    - **15s 对照（640x480，客户端延迟 1s 接入）**：修复前 207 帧且大量花屏；修复后 459 帧（≈30fps）、服务端丢帧 0、客户端 0 concealment/corrupt；`-x264-params sliced-threads=0:threads=4` 对照组 444 帧、同样 0 丢帧，说明修复不依赖编码器关切片线程。
    - **10 分 10 秒 soak**：单条 RTSP 客户端全程不重连，分辨率 640x480 → 1280x720 → 960x540 各 200s，720p 结束后断流 5s；publisher 每段 6,001 帧（29.8fps），客户端 18,003 帧 / 10:07.5（稳定 30fps、`speed=1x`），`flow_rtsp_frames_dropped_total{kind="video"}=0`，客户端 0 concealment/0 corrupt；分辨率切换与 5s 断流期间流不中断，flow 结束时 `alive=True`、无 panic。
    - **资源占用**：soak 期间 flow 累计 CPU 2.45s / ≈588s（约 0.4% 单核），进程 WorkingSet 平均 18.1MB、峰值 21.0MB（PowerShell 39 次采样）。
    - **断流重连**：另一轮 6.5 分钟运行在 270s 时强杀拉流 ffmpeg，旧会话立即回收，客户端 1s 后自动重连成功，`flow_sessions_total{proto="rtsp"}` 1→2 且流继续；摄像头 ffmpeg 段间停流 5s 时服务端保持存活，恢复后从新的 SPS/PPS+IDR 继续解码。
    - **延迟**：低延迟客户端（`-fflags nobuffer -max_delay 0 -flags low_delay`）720p 下，服务端收流→客户端解码的排队延迟中位 3.3ms（p95 437ms 是连接初期管道积压一次性排空，不是稳态）；x264 编码耗时中位 5.3ms/p95 8.2ms。默认 ffmpeg RTSP 客户端带约 500ms 重排缓冲，实测端到端约 650ms，需要低延迟时要显式加上述参数。注意按帧号配对 publisher/decoder 日志会被启动积压错位，本项只报告两端各自可独立测量的数值。
    - **已知噪声**：客户端日志中的 `non monotonically increasing dts`（10 分钟 1,750 行）来自 ffmpeg null muxer 的既有告警，旧版本与非切片对照组同样出现，与服务端丢帧无关。
    - `gofmt` / `go vet` / `go test ./...` 通过；修复提交 `7aeb40b fix: group multi-slice NALUs into one RTP access unit`。
- [x] **E-8 访问控制 / 限流**
  - IP 白名单（CIDR）、每 IP 建连速率限制、失败认证退避；
  - 验收：非白名单连接被拒，暴力尝试被限速。
  - ✅ 已实现：新增 `internal/access`：`Whitelist` 解析 CIDR 文件（空文件 = 关闭，IPv4-mapped IPv6 先归一）、`RateLimiter` 每 IP 令牌桶（每分钟惰性清理 5 分钟空闲桶）、`Backoff` 每 IP 失败认证指数退避（认证成功清零）。RTSP/RTMP accept 顺序为白名单 → 建连限速 → E-5 连接上限；RTSP 认证失败在回 401 前按退避时长 sleep。参数：`-whitelist-file`、`-conn-rate/-conn-burst`、`-auth-backoff-threshold/-base/-max`。单测见 `access_test.go`；实测白名单只含 `10.0.0.0/8` 时 127.0.0.1 的 RTSP/RTMP 连接均被拒（日志 `not in whitelist`），`-conn-rate 0.01 -conn-burst 1` 连续 3 条连接 1 通过 2 被限（`connection rate exceeded`），`-auth-backoff-threshold 1 -auth-backoff-base 300ms` 连续 4 次无凭据 DESCRIBE 的 401 延迟为 36/332/631/1231ms。
- [x] **E-9 资源回收测试**
  - `kill -9` 客户端、断网、半开连接各测一遍；
  - 验收：60s 内会话回收，UDP 端口释放，goroutine 数回落。
  - ✅ 已实现：`internal/rtsp/reclaim_test.go` 覆盖客户端被 kill -9、半开连接（CloseWrite 发 FIN）、静默断网三种场景，会话在 3s 内回收、UDP 端口可重新绑定、控制连接随会话关闭；静默断网用例把会话超时压缩到 300ms 走真实 `Sweep` 路径。`internal/rtmp/server_test.go` 增加客户端被杀后 `flow_connections{proto="rtmp"}` 回落为 0 的测试。`/metrics` 的 `flow_goroutines` 用于观测回收后 goroutine 是否回落。
- [ ] **E-10 文档与笔记**
  - README：架构图、运行命令、协议实现范围、已知限制；
  - `docs/rtsp.md` 补充 ANNOUNCE/RECORD、RTCP 反馈、鉴权流程；
  - 验收：新人按文档能跑起来并完成一次推拉流。

---

## T — RTMP 完整性与生产化

- [x] **T-1 复杂握手（complex/digest handshake，P2）**
  - 现状只做 simple echo（S1 版本字段清零），ffmpeg/ffplay 能退化兼容；只支持 complex handshake 的客户端会失败；
  - 实现 C1/C2/S1/S2 的 HMAC-SHA256（FP9/FP10）校验，校验失败回退 simple；
  - 验收：ffmpeg/ffplay 推拉流不变；complex-only 客户端能完成握手。
  - ✅ 已实现：新增 `internal/rtmp/handshake_digest.go`，按 FP9/FP10 的两种 digest offset 方案（块首 / 块尾 4 字节求和 % 728）校验 C1 的 HMAC-SHA256（FP key 前 30 字节）；命中则生成带 FMS 摘要的 S1（前 36 字节）与 `random + HMAC(FMS 68B, C1 摘要)` 的 S2，并校验 C2 尾部摘要；两种方案都不命中时逐字节保持原 simple echo，C2 不符只记录不拒绝。`handshake_test.go` 覆盖两种 offset 方案、合法/损坏 C2、simple 回退；ffmpeg 8.0.1 RTMP 推拉 e2e 回归通过（h264 320x240）。本机无 librtmp/OBS，complex-only 真实客户端未联调，完整握手由合成 FP9 客户端单测覆盖。
- [x] **T-2 RTMPS（P2）**
  - 与 E-6 共用证书参数，用 `tls.Listen` 包住 RTMP 监听；
  - 验收：`ffmpeg -f flv rtmps://127.0.0.1:1936/live/test` 推流、`ffplay rtmps://...` 拉流成功。
  - ✅ 已实现：`rtmp.Server` 新增 `SetTLS(cert, key)`，用 `tls.LoadX509KeyPair` 加载 PEM（`MinVersion: TLS1.2`），`Serve` 在配置了证书时改用 `tls.Listen` 并打印 `RTMPS server listen`；accept 后先显式 `tls.Conn.Handshake()`（10s deadline）再复用原有 RTMP 握手/命令/推拉逻辑，明文客户端快速失败（日志 `rtmp: tls handshake ... first record does not look like a TLS handshake`）。`cmd/server` 复用 E-6 的 `-tls-cert`/`-tls-key`，新增 `-rtmps-addr`（默认 `:1936`，仅在配置证书时启用）；RTMPS 与明文 RTMP `:1935` 各用一个 `Server` 实例，共享注册表、指标与限流器，互不影响关闭流程。测试 `internal/rtmp/tls_test.go` 覆盖自签证书加载、TLS 上完成 RTMP 握手并收到 `NetConnection.Connect.Success`、明文客户端被拒且不影响后续连接、静默客户端握手超时、证书缺失/不匹配报错、未启用 TLS 时明文路径无回归。本机 e2e（自签证书 + ffmpeg 8.0.1）：`ffmpeg -re -i testdata/with_audio.mp4 -c copy -f flv rtmps://127.0.0.1:1936/live/rtmpstest` 推流成功（ffmpeg 默认不校验证书，无需额外参数），`ffprobe rtmps://...` 解出 h264 640x480 + aac 44100，`ffmpeg -t 3 -f null -` 解码 60 帧；同一进程明文 `rtmp://127.0.0.1:1935/...` 推拉正常；明文客户端连 1936 被 TLS 握手拒绝（WinSock -10054）。证书轮换需重启；未做 RTMP/RTMPS 单端口协议嗅探。`gofmt`/`go vet`/`go test ./...` 通过。
- [x] **T-3 RTMP 连接鉴权（P0）**
  - 现状 `connect` / `publish` / `play` 不校验任何凭据，任意 app/流名都能推拉；
  - 支持 token/密钥、app 白名单、推拉权限分离；凭据从文件/环境变量读取；
  - 失败回 `NetConnection.Connect.Rejected` 或 `NetStream.Play.StreamNotFound`；日志不打印明文密钥；
  - 验收：错误 token 被拒，正确 token 可推可拉。
  - ✅ 已实现：新增 `internal/rtmp/auth.go`：`AuthConfig`/`Auth` 支持推流 token、拉流 token、推拉通用 token 与 app 白名单，token 只保存 SHA-256（内存无明文、比较不泄露长度），权限位 `publish`/`play` 取并集；某方向没配置 token 时该方向不鉴权。凭据从流名查询串取（`?token=`，兼容 `?key=`，URL 解码，兜底取 app 的查询串），鉴权发生在 `sanitizeStreamName` 之前，注册表流名仍不含 query。新增 `internal/rtmp/auth_file.go`：文件每行 `publish:TOKEN` / `play:TOKEN` / `all:TOKEN` / `app:NAME`，无前缀行按 `all`，支持注释、token 含冒号、错误带行号。`Server.SetAuth` 注入配置；`connect` 的 app 不在白名单时回 `_error` + `NetConnection.Connect.Rejected` 并断开，`publish` token 缺失/错误/无推流权限同样回该错误，`play` 失败统一回 `onStatus` + `NetStream.Play.StreamNotFound`（不暴露流是否存在）。日志只打印 `invalid or missing token` / `app not allowed`，app 日志统一去掉查询串，确认不泄露 token。main 新增 `-rtmp-token`、`-rtmp-publish-token`、`-rtmp-play-token`、`-rtmp-auth-file`、`-rtmp-app-whitelist` 及对应 `FLOW_RTMP_*` 环境变量，优先级同 RTSP：文件 < 环境变量 < 命令行参数，RTMP 与 RTMPS 两个实例共用。测试新增 `auth_test.go`（权限矩阵、方向可选、token 权限合并、白名单、nil/空配置、connect/publish/play 的拒绝与放行）与 `auth_file_test.go`（解析/合并/错误/加载）；e2e 实测：错误 token、用 play token 推流、非白名单 app 均被拒（日志分别 `invalid or missing token` / `app not allowed`），正确 token 推流后 `?token=playsecret` 拉流解出 h264 640x480 + aac 44100，用 publish token 拉流与无 token 拉流均失败；`FLOW_RTMP_TOKEN` + `FLOW_RTMP_APPS` 环境变量组合同样可推可拉、非白名单 app 被拒，日志中未出现明文 token。`gofmt`/`go vet`/`go test ./...` 通过。
- [x] **T-4 onMetaData（P2）**
  - 现状 type 18 数据消息被忽略（`internal/rtmp/server.go` 注释）；
  - 推流端保存 onMetaData，拉流端在 `Play.Start` 后下发（duration、videocodecid、audiocodecid、宽高、帧率、采样率、声道）；
  - 验收：`ffprobe` 能读出时长/编码/分辨率，ffplay 无相关告警。
  - ✅ 已实现：新增 `internal/rtmp/metadata.go`：`parseMetaData` 解析 type 18 数据消息负载（首值 String 命令名 + 可选 Object/ECMA Array 体），`handleData` 只认 `onMetaData`，非 onMetaData（如 `|RtmpSampleAccess`）与无 publish 状态时静默忽略，解析失败只打日志不打断推流；保存原始负载副本（转发零重建）+ 解析后的键值对。`stream.Stream` 新增带锁的 `metaData any` 槽与 `SetMetaData`/`MetaData`（后到覆盖先到），保持 `internal/stream` 不依赖 `internal/flv`；RTSP 的 SDP 仍用独立的 `Meta` 字段，互不影响。`server.go` 消息分发新增 `case msgDataAMF0: conn.handleData(m)`。`play.go` 在 `Play.Reset`/`Play.Start` 之后、启动 `playVideo`/`playAudio` goroutine 之前同步转发 onMetaData，保证客户端先拿到元数据再收到序列头（仅注册表流有，本地回退源无）。测试新增 `metadata_test.go`：解析 ECMA Array / Object 体、非对象体、畸形负载；`handleData` 在 publish 时保存与覆盖、非 onMetaData 与无 publish 时忽略；`TestPlayForwardsMetaData` 断言 Play.Start 后首条是 type 18 且字段一致、其后才是视频序列头；`TestPlayNoMetaData` 验证无元数据时直接进序列头。e2e 实测（ffmpeg 8.0.1）：`ffmpeg -re -i testdata/with_audio.mp4 -c copy -f flv` 推流，抓包确认发布侧 onMetaData 含 duration=0（直播）、width=640、height=480、framerate=25、videocodecid=7、audiocodecid=10、audiosamplerate=44100、stereo、encoder、filesize；`ffprobe` 拉流读出 h264 640x480 + aac 44100，`ffmpeg -loglevel warning` 解码无 metadata/duration/non-monotonic 告警。直播流 duration 恒为 0 属协议固有（推流端无法预知总时长）。`gofmt`/`go vet`/`go test ./...` 通过。
- [x] **T-5 NetStream Pause/Unpause/Seek（P2）**
  - 现状只处理 `play`，没有 `pause` / `seek` 命令；
  - 解析 `pause`/`seek`；暂停停发但保留订阅，恢复后 seq/时间戳连续；文件源按 G-2 索引 seek，live 源返回不支持；
  - 验收：暂停 2s 恢复不重复/不跳变；文件 seek 后从 IDR 开始。
  - ✅ 已实现：`Command` 新增 `boolArg`/`numberArg`，`handleCommand` 分发 `pause`/`seek`。暂停：`playState` 增加带锁的暂停门闩，`playVideo`/`playAudio` 暂停期间继续消费源并缓存到每条轨 256 帧的本地队列（避免暂停超过 ring 窗口后丢帧），恢复后按原顺序补发；`relativeClock.rebase` 在缓存上限丢帧时把恢复首帧映射为上一帧 DTS+1，DTS 保持单调。暂停回 `NetStream.Pause.Notify`，恢复先回 `NetStream.Unpause.Notify` 再唤醒播放 goroutine，避免状态与媒体消息乱序。seek：新增可选接口 `seekSource`（`SubscribeFrom(start, scale)`），文件源 `H264Source`/`AACSource` 实现，实时 `ExternalSource` 不实现；`handleSeek` 先建立新订阅（失败时旧播放不受影响）、再停旧 goroutine、先回 `NetStream.Seek.Notify` 后启动新 goroutine，保证客户端在状态之后收到的媒体一定来自新位置；视频从不超过目标时间的最后一个 IDR 开始（G-2 索引），音频按 AU 对齐；新 `playState` 用 `tsOffset = 旧播放最后 DTS + 1` 叠加相对时间戳，前进/回退 seek 都不出现 DTS 倒退。live 源 seek 回 `NetStream.Seek.Failed` 且不打断当前播放。测试新增 `internal/rtmp/pause_seek_test.go`：暂停期间无媒体消息、恢复后按 160/200/240/280 连续补发；live seek 失败但播放状态保留；文件 seek 后先重发序列头、首帧为 IDR 且 ts 从 seek 前最后一帧继续；`rebase` 时间基准单测。e2e 实测（ffmpeg 8.0.1）：`ffmpeg -ss 3 -i rtmp://127.0.0.1:1935/live/test -t 1 -f null -` 触发客户端 `seek`（服务端收到 2870ms，ffmpeg 按 start time 调整），日志 `h264 seek source: start at frame 50 (ts=180000...)`、`aac seek source: start at offset 44773 (ts=125952...)`，解码 25 帧无告警。说明：暂停期间播放 goroutine 阻塞在 `Reader.Next` 时，恢复要等下一帧到达才生效（真实源持续出帧，≤1 帧间隔）；暂停超过 256 帧缓存上限时丢弃最老帧并重设时间基准，时间戳连续但内容跳到较新位置。`gofmt`/`go vet`/`go test ./...` 及 `go test -race ./internal/rtmp/` 通过。
- [x] **T-6 视频 CTS / B 帧时间戳（P2）**
  - 现状 FLV VideoTag 的 CompositionTime 固定 0，RTSP 侧也不重建 DTS，含 B 帧素材会出现 `non monotonically increasing dts` 告警；
  - 推流侧解析 PTS-DTS 写入 CTS，拉流侧按 CTS 排序；无 B 帧时保持 0；
  - 验收：`testdata/with_audio.mp4` 推拉后 ffmpeg 无 non-monotonic 告警，帧序正确。
  - ✅ 已实现：`source.Frame` 新增 `CompositionTime`（PTS−DTS，90kHz，有符号；音频恒 0）和 `AUEnd`（访问单元最后一个 NALU），与既有 `AUStart` 一起把「一帧多 NALU」的边界带到拉流端。FLV 侧新增 `BuildVideoTagNALUs` / `BuildHEVCVideoTagNALUs`，一个 tag 内按「4 字节长度前缀 + NALU」放多个 NALU 并写有符号 24 位 CTS；原单 NALU 构造函数改为委托。RTMP 推流（`publish.go`）把 FLV tag 时间戳当 DTS、`CompositionTime` 当 PTS−DTS 换算成 90kHz，写入帧源的 `Timestamp` 存 PTS、`CompositionTime` 存 CTS，并按 NALU 标 `AUStart`/`AUEnd`。RTMP 拉流（`play.go`）按 `(Timestamp, CompositionTime)` 聚合同一 AU 的多个 NALU，发一个 tag：`DTS = PTS−CTS`（`ticksToMillis` 四舍五入、负数向零取整，<0 钳到 0），tag 时间戳用相对 DTS，CTS 原样写回；参数集仍只进序列头，不打断 AU。RTSP 侧：ANNOUNCE SDP 解析 `a=framerate`（支持 `25`、`29.97`、`30000/1001`，非法/≤0/>240 视为无），经 `announcedTrack.Framerate` 传到 `mediaTrack`/`pushReceiver`；推流接收端 `videoDTS` 以首 AU 的 PTS 为 DTS 起点、按 `90000/framerate` 浮点累加出严格递增的 DTS，`CTS = PTS−DTS`（B 帧为负），完整 AU 和 PLC 补帧都走同一逻辑；无帧率信息时保持 DTS=PTS、CTS=0。另修复中途加入的断帧问题：`ExternalSource` 原来只预热 SPS/PPS+最近 IDR，缺 IDR 之后的参考帧，拉流端会报 `co located POCs unavailable` 继而偶发 non-monotonic；现在按最近一个 IDR 缓存整个 GOP（上限 256 帧/1MB，新 IDR 重置），订阅时连同参数集一起按序预发，缓存更新与 ring 写入同锁，保证不重复不丢帧。测试新增：FLV AVC/HEVC 多 NALU + 正负 CTS 往返；SDP `parseFramerate` 整数/小数/分数/非法值及 ANNOUNCE 带帧率；RTSP `videoDTS` 的 B 帧序列（I P B B P → DTS 0/3600/7200/10800/14400，CTS 0/7200/−3600/−3600/7200）、29.97fps 浮点累加无漂移、无帧率直通；RTMP 推流的 PTS/CTS/AU 边界（含负 CTS 与多 NALU tag）；RTMP 拉流按 CTS 还原 DTS、输出 CTS 与 AU 合并（I P B B → tag DTS 0/40/80/120ms、CTS 80/200/80/0）。e2e：`test/e2e/rtmp_matrix.py` 的 `check_pull` 增加 `non monotonically increasing dts` 断言；实测 ffmpeg 8.0.1 四项互通（RTMP↔RTSP 双向、RTMP↔RTMP、RTSP↔RTSP）在推流 2.5s 后中途加入，`-loglevel warning` 解码 4s 全部 0 条 non-monotonic、0 条 `co located POCs`；RTMP 推→拉 8s 用 `-c copy` 取流，解码 200 帧与源文件前 200 帧 `framemd5` 逐帧一致（帧序正确）；RTSP 推→RTMP 拉的 FLV tag 带正确 CTS（如 dts=40/cts=120）且 DTS 严格递增。`gofmt`/`go vet`/`go test ./...` 通过。
- [x] **T-7 Chunk 层优化（P2，可选）**
  - 评测 Set Chunk Size（当前 4096）对吞吐/延迟的影响；解包 Aggregate Message（type 22）；
  - 验收：压测下吞吐提升且低延迟不退化，单测覆盖聚合消息边界。
  - ✅ 已实现：新增 `internal/rtmp/chunk_bench_test.go` 三组基准（`go test ./internal/rtmp -run '^$' -bench Chunk -benchmem`）：写侧（nopConn 隔离编码/写调用）、读侧（预编码数据重组）、往返延迟（net.Pipe 一写一读），覆盖 chunk size 128/1024/4096/30000/60000/131072 与 1KB 音频 / 30KB 普通帧 / 150KB 关键帧。优化前（4096）：写 30KB 17.5µs/1754MB/s/8 allocs、150KB 111.7µs/1375MB/s/38 allocs；读 30KB 134µs/229MB/s/33 allocs、150KB 600µs/256MB/s/98 allocs；往返 30KB 143µs、150KB 746µs。优化后（4096）：写 30KB 5.8~7.1µs/4.4~5.3GB/s/1 alloc、150KB 19~24µs/6.4~8.0GB/s/1 alloc；读 30KB 7~16µs/2.0~4.3GB/s、150KB 30~47µs/3.2~5.2GB/s，分配数恒为 8（基准自身开销），往返 30KB 52~70µs、150KB 181~205µs。chunk size 结论：写侧改成整条消息单次 write 后，chunk size 对写吞吐几乎无影响（4096 与 60000 差异在噪声内）；读侧 chunk 越小 header/循环次数越多，128 最差（150KB 65~107µs），4096 已接近 30000/60000；往返延迟上 60000 对大帧略优（150KB 中位 168µs vs 4096 的 200µs，约 16%），1KB 小消息两者一致（~6µs），无低延迟退化。因此默认值仍保留协议常见的 4096（兼容性/内存突发更稳），新增 `Server.SetChunkSize` 与 `-rtmp-chunk-size`（默认 4096，范围 1..16777215）供运行时评测。数据面优化：`writeMessage` 把所有 chunk 拼进一个缓冲区后单次 `write`（原来每片一次加锁/系统调用、每片单独分配），同时避免其它 goroutine 的写插入同一条消息的 fmt 3 续片；读侧 `chunkStream.begin(length)` 按消息长度一次预分配（上限 1MB，更大消息按需增长），每个 chunk 用 `io.ReadFull` 直接读进缓冲尾部，header/时间戳字段改走 `io.ByteReader`，消除逐片 append 与每字段堆分配。Aggregate Message：新增 `internal/rtmp/aggregate.go` 与 `msgAggregate=22` 分发，按 FLV tag 布局拆分 type(1)+length(3 BE)+timestamp(3 BE)+ext(1)+streamID(3，忽略并由外层覆盖)+payload+backPointer(4 BE)，时间戳按 `offset = 聚合时间戳 − 首个子消息时间戳` 归一化（首条等于外层时间戳，后续保持相对间隔）；空负载返回空列表、最后一条可省略 back pointer（兼容老实现），头不完整、length 越过剩余负载、back pointer 只剩 1~3 字节均报错断开；只把 type 8/9/18 子消息交给 `handleVideo`/`handleAudio`/`handleData`，其它类型忽略。测试新增 `aggregate_test.go`（普通双消息、时间戳归一化、扩展时间戳、空负载、末条无 back pointer、零长度子消息、三种损坏边界、handler 分发帧内容/时间戳）与 `chunk_test.go::TestReadMessageLargeMessageAcrossChunks`（2MB+ 消息走按需增长路径）；`command_test.go` 增加自定义 chunk size 的 connect 协商与非法值拒绝。e2e：`python test/e2e/rtmp_matrix.py` 5/5 PASS；手动以 `-rtmp-chunk-size 128` / `60000` 起服，ffmpeg 推 6s + 拉 3s 均 rc=0、零告警，`-loglevel debug` 确认客户端日志 `New incoming chunk size = 60000`；ffmpeg 默认不产生聚合消息，聚合路径由单测覆盖。`gofmt`/`go vet`/`go test ./...`、`go test -race ./internal/rtmp/` 通过。
- [x] **T-8 同名推流策略（P2）**
  - 现状 `Registry.Publish` 同名整体替换并关闭旧源；
  - 改为可配置 `replace`（默认）/ `reject`（回 Already Publishing）/ `reject-if-active`；
  - 验收：三种策略行为符合预期，旧源 goroutine 正常回收。
  - ✅ 已实现：`internal/stream` 新增 `PublishPolicy`（`PublishReplace`/`PublishReject`/`PublishRejectIfActive`）、`ParsePublishPolicy`、`ErrPublishRejected` 与 `Registry.SetPublishPolicy`；`Publish` 改为返回 `(*Stream, error)`，查重与登记在同一把写锁内完成，同名并发推流只有一个成功（`reject` 下压测 16 路只 1 路登记）。默认 `replace` 完全保持原有行为；`reject` 只要同名存在就拒绝；`reject-if-active` 仅在旧流仍活跃时拒绝，否则视为僵尸流替换并关闭旧源。活跃定义为「音视频任一轨最近 10s（`DefaultActiveWindow`）内还有 `Write`」：`ExternalSource` 增加 atomic `lastWrite`（创建时初始化，`Write` 更新，`LastWrite()` 暴露），`Stream.Active(window)` 判断；还没有建轨的流（RTMP 首帧前）按登记时间 `createdAt` 兜底，避免把正常启动的流当僵尸。RTMP `handlePublish` 收到 `ErrPublishRejected` 时回 `onStatus` `NetStream.Publish.BadName` + 描述 `Already Publishing`，不设置发布状态、不增加发布指标、不动旧流，连接保持可换名重试；同连接重复 publish 仍先注销自己的旧流，因此可在 `reject` 下重新发布同名流。RTSP `publish()` 返回 error，`handleAnnounce` 回 `453 Not Enough Bandwidth`；若本次 ANNOUNCE 新建了会话则 `sessions.Delete` + `sess.Close` 回收，复用已有会话时不删，旧流保持不变。配置新增 `-publish-policy replace|reject|reject-if-active`（默认 replace，非法值启动退出码 2），写到两个协议共享的 `Registry`，RTSP / RTMP / RTMPS 同时生效。测试新增：`internal/stream/registry_test.go` 覆盖策略解析、`reject` 拒绝且旧源仍可写、`reject-if-active` 活跃拒绝/不活跃替换并唤醒旧读者、16 路并发只成功 1 路；`internal/rtmp/command_test.go` 断言 BadName/Already Publishing、发布状态为空、注册表不变；`internal/rtsp/announce_test.go` 断言 453、旧流不变、新建会话已回收。e2e（ffmpeg 8.0.1）：`-publish-policy reject` 下第二路 RTMP 推流报 `Server error: Already Publishing` 且服务端日志 `reject publish ... duplicate publish rejected`，第一路正常；`replace` 下两路 RTMP 均发布成功、无拒绝；`reject` 下第二路 RTSP ANNOUNCE 收到 `453 Not Enough Bandwidth`，日志 `ANNOUNCE rejected (453)` 且旧流继续。`gofmt -l .`、`go vet ./...`、`go test ./...`、`go test -race ./internal/rtmp/ ./internal/rtsp/ ./internal/stream/ ./internal/source/` 全部通过。
- [x] **T-9 RTMP 客户端 / 回源（P3，可选）**
  - 新增 client 模式：从上游 RTMP 拉流并作为一路源发布，支持断线重连；
  - 验收：上游断开后自动重连，下游明确收到流结束或持续。
  - ✅ 已实现：`internal/rtmp/client.go` 新增 `Client`，`NewClient(spec, registry, metrics)` 解析 `[本地名=]rtmp://host[:port]/app[/sub]/stream[?token=...]`（默认本地名取 URL 最后一节，复用 `sanitizeStreamName`），`Run(ctx)` 内自动重连。客户端以 simple 握手（`clientHandshake`：C0/C1 → S0/S1/S2 → 回显 S1 作 C2）连接上游，依次发 `connect`/`createStream`/`play`（`readCommand` 读取 `_result`/`onStatus`，途中就地处理控制消息），随后复用推流侧同一套 `readMessage`/`handleControl`/`handleVideo`/`handleAudio`/`handleData`/`handleAggregate` 把上游 FLV tag 写进一路本地 `stream.Stream`，几乎零新增解包逻辑；`server.go` 把原读循环抽成 `Conn.dispatch`/`Conn.pump`，服务端与 client 共用。重连语义为**透明续传**：整个生命周期只 `registry.Publish` 一次，上游断开时不关闭 `ExternalSource`（下游读者阻塞在 `Next`，不会收到结束），连上后继续写帧；仅 ctx 取消（进程退出）时 `Unpublish` 关流，下游明确收到流结束。退避 1s→2s→…上限 30s，走到 `NetStream.Play.Start` 后重置。上游重连后时间戳从 0 重新计时，client 通过 `rebaseClock`（`beforeMedia` 钩子）把每帧毫秒时间戳平移到上次最大值之后；`publish.go` 的 SPS/PPS/VPS 参数集帧也由硬编码 `Timestamp: 0` 改为跟随序列头消息时间戳，避免重连后 RTSP/HTTP-FLV 下游时间戳倒退。`cmd/server` 新增 `-pull` 参数（逗号分隔多条），非法 spec 退出码 2。测试新增 `internal/rtmp/client_test.go`：URL 解析、`rebaseClock` 跨重连单调、端到端转推（本地 RTMP 服务器作上游 → Client 拉到第二份注册表 → 断言帧与参数透传）、断线重连（`closeAllConns` 触发后仍继续出帧且时间戳单调、下游不结束）、ctx 取消后本地流注销且下游读到结束。`gofmt -l .`、`go vet ./...`、`go test ./...`、`go test -race ./internal/rtmp/ ./internal/stream/ ./internal/source/` 全部通过；ffmpeg 8.0.1 手工 e2e：上游 → `-pull` 回源 → 下游 RTMP 与 RTSP 均解出画面，杀掉上游进程再重启后回源自动重连、下游持续拉流正常退出。
- [x] **T-10 RTMP e2e 自动化（P1）**
  - 把 rtmp.md 的手工矩阵固化为脚本：本地回放、RTMP↔RTSP 双向、RTMP↔RTMP、断开回收；
  - 断言收到帧数/音视频轨/退出码；
  - 验收：一条命令跑通全矩阵。
  - ✅ 已实现：新增 `test/e2e/rtmp_matrix.py`（Python 3.7+ 标准库），`python test/e2e/rtmp_matrix.py` 一条命令完成编译、起服、全矩阵与清理。脚本自动挑选空闲端口，`cmd/server` 新增 `-rtsp-addr`/`-rtmp-addr` 两个监听地址参数（默认仍是 `:8554`/`:1935`），避免与本机已有进程或并行 e2e 抢端口。矩阵五项：本地文件回放（`/live/test`）、RTMP 推 → RTSP 拉、RTSP 推 → RTMP 拉、RTMP 推 → RTMP 拉、断开回收。每项用 ffmpeg `-progress pipe:2` 取解码帧数、从输入流行断言 `Video`/`Audio` 轨（并显示源编码），要求退出码 0；跨协议用例先等服务器日志 `rtmp: publish`/`published stream` 再起拉流，避免注册表未命中时 RTMP 回退本地源造成假阳性；断开回收用例直接终止推流端，断言 `rtmp: unpublished` 日志出现且 `/metrics` 的 `flow_connections`、`flow_rtmp_publishes`、`flow_rtmp_plays`、`flow_sessions` 全部回零。支持 `--case` 单跑、`--server-bin` 复用二进制、`--keep` 保留日志、`--verbose` 实时看日志；缺 ffmpeg/编译失败返回 2，用例失败返回 1。实测本机（ffmpeg 8.0.1）：五项全部 PASS，2s 拉流分别解出 48/51/48/47 帧，音视频轨为 h264+aac；全矩阵约 15s。`gofmt`/`go vet`/`go test ./...` 通过。

---

## F — 编解码与媒体能力（当前支持 H.264/H.265 + AAC，P3）

- [x] **F-1 H.265 / HEVC**
  - RTSP：SDP `H265/90000`、RTP 单 NALU/FU 解包打包、`sprop-vps/sps/pps`；
  - RTMP：Enhanced RTMP / HEVC FourCC；拉流端对老客户端优雅降级；
  - 验收：HEVC 推拉 RTSP/RTMP，另一协议能转出。
  - ✅ 已实现：`stream.VideoCodec` 增加 `Codec`/`VPS`，`media.ClassifyVideoNAL` 统一 H.264/H.265 的 VPS/SPS/PPS/IDR/VCL 判定；新增 RFC 7798 `H265Packer`/`H265Depacketizer`（单 NALU / AP / FU）。RTSP 侧支持 SDP `H265/90000`、ANNOUNCE 解析与合成 `sprop-vps/sps/pps`、推流解包与拉流按编码选 packer。RTMP 侧实现 Enhanced RTMP `hvc1` FourCC 的序列头 / CodedFrames / Metadata 忽略、HEVCDecoderConfigurationRecord 编解码，并按 `fourCcList` 对不含 `hvc1` 的老客户端回 `NetStream.Play.Failed`。本地 `-live`/testdata 源仍为 H.264，HEVC 只来自推流。测试覆盖 NAL 分类、packer/depacketizer 往返、FLV 配置记录与标签、RTSP/RTMP HEVC 路径、老客户端拒绝；e2e（ffmpeg 8.0.1/libx265）RTSP→RTMP、RTMP→RTSP、RTSP→RTSP 均解出 `hevc 640x360`。
- [x] **F-2 音频扩展**
  - RTSP：G.711（PCMA/PCMU）、Opus、MP3；RTMP：MP3/G.711（按需求）；
  - 验收：各编码单独推拉成功，音画同步。
  - ✅ 已实现：音频模型从「AAC + ASC」扩展为「编码名 + 配置 + 采样率 + RTP 时钟 + 声道」（`stream.AudioCodec` 新增 `Codec`/`SampleRate`/`ClockRate`/`Channels`，`SetAudioCodec`/`NormalizeAudioCodec`/`AudioClockRate`/`DefaultAudioSampleRate`）。关键点是 MP3 的 RTP 时钟固定 90000、采样率 44100，二者不能混用，否则 RTMP 时间戳会漂一倍；帧源里统一存各编码的 RTP 时钟时间戳，RTMP 拉流时再换算成毫秒。新增 `media.ParseMP3Frame`（MPEG1/2/2.5 各层帧长/采样率/声道）、`rtp.G711Packer`/`OpusPacker`/`MPAPacker` 与 `rtp.MPADepacketizer`（RFC 2250 4 字节头、跨包分片重组；兼容 ffmpeg「一个 RTP 包放多个完整帧且不置 marker」的封装，按帧头切分并逐帧递推时间戳，同步丢失时重找同步字）。FLV 支持 SoundFormat 2/7/8 的构造与解析（`BuildAudioTagFor`），未知格式仍按 AAC。RTSP 侧 SDP 对静态 PT 0/8/14 在缺少 `a=rtpmap` 时按 RFC 3551 兜底，ANNOUNCE 推导编码/时钟/声道，合成 SDP 输出 PCMU→PT0、PCMA→PT8、Opus→PT97(48000/2)、MP3→PT14(MPA/90000)、AAC→PT97(MPEG4-GENERIC)；推流端原始负载直写帧源，MP3 用首帧头回填采样率；拉流端 `streamAAC` 按编码选封包器，G.711 分片时间戳按字节数递增。RTMP 推流识别 MP3/A-law/μ-law 并登记参数，拉流按编码生成 FLV tag；FLV 无 Opus 映射，遇到 Opus 只跳过音频、视频继续。跨协议无需转码：AAC/MP3/G.711 在 RTSP↔RTMP 双向可用，Opus 仅 RTSP。测试新增 `media/mp3_test.go`、`rtp/audio_test.go`、`flv/audio_raw_test.go`、`rtsp/audio_test.go`、RTMP 推/拉音频用例与 Opus 跳过用例。e2e（ffmpeg 8.0.1）：RTSP PCMA/PCMU/Opus/MP3 与 RTMP MP3/PCMA/PCMU 单独推拉均解出正确编码（`pcm_alaw`/`pcm_mulaw`/`opus`/`mp3`），跨协议 RTSP PCMA→RTMP、RTMP MP3→RTSP 正常，RTSP Opus→RTMP 只出视频；各 5s 抓包音视频时间戳跨度差 ≤32ms，无累积漂移。`gofmt -l .`、`go vet ./...`、`go test ./...` 全部通过。
- [x] **F-3 转封装 / 录制**
  - 把实时流写成 MP4/FLV/TS（按关键帧切分、断点续录、磁盘配额）；
  - 验收：录制 1 分钟文件可被 ffprobe/ffplay 正常播放。
  - ✅ 已实现：新增 `internal/record`，只做转封装、不解码。`Recorder` 每条轨一个 goroutine 负责订阅、按 `AUStart/AUEnd` 聚合访问单元、修补时间轴（`dtsUnwrapper` 处理 uint32 回绕，`dtsClock` 把回退/大跳变接到上一个样本之后），再由单独的 writer 循环负责轮转、配额和落盘；`Manager` 每 500ms 轮询注册表（避开单槽位观察者，也解决 RTMP 轨晚于 PUBLISH 建立的问题），支持逗号分隔流名与 `*`，发现后到的第二条轨时换文件续录。容器：MP4 用 `internal/fmp4` 写 fMP4/CMAF（每个文件都带 ftyp+moov，一个 GOP 一个 moof），FLV 写 FLV 头 + AVC/HEVC 与 AAC 序列头，TS 复用 `internal/mpegts`。轮转只发生在「达到 `-record-segment` 后的第一个关键帧」，每个文件时间轴独立从 0 开始；纯音频按时间轮转。MP4/TS 只支持 H.264+AAC，FLV 额外支持 HEVC 与 MP3/PCMA/PCMU，放不进去的音轨自动退化为纯视频。断点续录：每个流一个 `<prefix>.state` JSON（原子 tmp+rename），记录已落盘字节数、参数集指纹、下一 fragment 序号和两条轨的 next DTS；重启校验格式/参数集指纹后截断到最后一个完整 fragment 再追加（MP4 的 fragment 本身就是一致性边界），并把音视频统一对齐到较晚那条轨的续录点，避免崩溃点音频落后视频几百毫秒变成整段固定音画偏移。配额按 `<prefix>-*` 统计、删最旧的完整文件，活动文件和 `.state` 不参与；retention 按 mtime 删除。启动时给首个 IDR 之前的音频做了裁剪，防止文件开头音画错位；TS 时间轴整体加 1s，保证带 B 帧时 PTS 不为负。`cmd/server` 新增 `-record`、`-record-dir`、`-record-format`、`-record-segment`、`-record-quota`、`-record-retention`、`-record-resume` 并做参数校验。测试新增 `internal/record/record_test.go` 七个用例：关键帧轮转与 MP4 box 结构、FLV 序列头/轮转、TS 188 字节包与轮转、崩溃续录与音画对齐、配额淘汰、retention、开头音频裁剪。e2e（ffmpeg 8.0.1，320x240@25fps + AAC 44.1kHz）：MP4 推 65s、20s 一段，得到 20+20+20+5s 四个文件，`ffprobe` 均识别 h264/aac 且 `start_time=0`，`ffmpeg -f null` 全量解码零错误；FLV/TS 各推 16s、5s 一段，所有分片同样全量解码零错误。崩溃续录用硬杀进程验证：重启后命中同一文件，从 1,167,774 字节续到 2,440,794 字节（时长 23s），音频在 11.000s 与视频关键帧对齐（崩溃点丢掉的 0.4s 音频表现为间隙），解码零错误。`gofmt -l .`、`go vet ./...`、`go test ./...` 全部通过。局限：MP4 是分片格式（moof）而非 faststart 渐进式；续录要求参数集不变，否则新开文件；尚未接入 DVR/时移，也没有加密。
- [x] **F-4 转码**
  - 解码 → 缩放/转码 → 编码（H.264/H.265、多分辨率）；纯 Go 性能有限，评估 cgo/外部进程方案；
  - 验收：1080p 输入转 720p 输出，延迟与 CPU 有基线数据。
  - ✅ 已实现：新增 `internal/transcode`，采用外部 ffmpeg 进程（不走 cgo）：Go 侧订阅注册表里的源流、按 `AUStart/AUEnd` 合并访问单元后封装成 MPEG-TS 写入 ffmpeg stdin，再从 stdout 解 TS 把结果写进一条新流，注册表发布后 RTSP/RTMP/HTTP-FLV/HLS/DASH 自动可拉。选 TS 作边界是因为 Windows 的 `exec.Cmd.ExtraFiles` 不可用，单管道既能带视频又能带 PTS/DTS；音频不解码，按原始帧直通到输出流（参数集变化时用 `audioFingerprint` 重设），因此 CPU 只花在视频上。参数集优先从源流的 `VideoParams()` 取，运行中收到新的 VPS/SPS/PPS 会更新封装器并用 `paramFingerprint` 去重；H.265 支持同步补齐到 `internal/mpegts`：PMT 用 `stream_type=0x24`、AUD 用 `0x46 0x01`、关键帧注入 VPS/SPS/PPS，demux 侧按 `VideoAU.Codec` 识别 HEVC，SRT 推流也改为按实际编码设置参数集。时间戳：输入 uint32 经 `tsUnwrapper` 展开，每次 ffmpeg 启动把 DTS 归零，ffmpeg 侧用 `-copyts -fps_mode passthrough -avoid_negative_ts disabled` 保持原时间轴；输出时间轴 `runBase + au.PTS`，重启时 `runBase = 上次输出 + 墙钟空洞`，并有单调保护，下游不会看到回退；`CompositionTime` 由输出 PTS-DTS 还原。编码参数由 `Profile` 表达（`w/h` 或 `size`、`codec=h264|h265`、`bitrate`、`fps`、`out`，输出默认 `<源>_<高度>p`，偶数取整），命令行可给多档（`;` 或换行分隔），同一输出名会拒绝重复。`Manager` 每 500ms 轮询注册表（与录制同样的理由：观察者单槽位、RTMP 音轨晚于 PUBLISH），源流消失/被替换/视频轨消失时关闭；等音轨出现或 `joinDelay` 到点后再启动，输出名被别的流占用时等待而不是覆盖；ffmpeg 崩溃、读写失败或订阅端丢帧（`Reader.Dropped()` 增长，说明参考帧已被覆盖）都按 500ms→10s 指数退避重启并重新订阅预热 GOP。`cmd/server` 新增 `-transcode`、`-transcode-ffmpeg`、`-transcode-preset`、`-transcode-gop`、`-transcode-hw` 并校验 GOP/硬件编码器取值。指标新增 `flow_transcode_streams`、`flow_transcode_input_frames_total`、`flow_transcode_output_frames_total`、`flow_transcode_dropped_frames_total`、`flow_transcode_restarts_total`、`flow_transcode_output_bytes_total`、`flow_transcode_latency_seconds_total`（输入 PTS 对应喂入时刻到输出帧写出的平均流水线延迟）与 `flow_transcode_latency_events_total`。测试新增 `internal/transcode`（profile 解析/默认值/错误、ffmpeg 参数与编码器选择、时间戳回绕、指纹、AUD、缓冲截断）以及 `internal/mpegts` 的 H.265 往返与 AU 分组用例；集成测试用 ffmpeg 现场生成 640x480 H.264，验证输出 SPS 为 320x240 且 Close 后输出流被摘除。e2e（ffmpeg 8.0.1，Windows）：1080p30 testsrc2 经 RTSP 推入，`-transcode "live/src=w=1280,h=720,bitrate=3m"`，用 ffprobe 拉 `live/src_720p` 得到 1280x720 H.264，10s 窗口输入/输出各约 305 帧（30fps），0 丢帧、0 重启；流水线平均延迟 104.8ms，输出帧约 1.5Mbps（testsrc2 内容偏易）；转码 ffmpeg 子进程占用约 81% 单核，flow 进程约 8% 单核（含 RTSP 收发与 TS 封装），发布端断开后转码器与输出流立即清理。踩坑记录：输入侧不能加 `-fflags nobuffer`——管道不可回退，探测阶段读到的开头 IDR 会被丢弃，ffmpeg 一帧都解不出来；去掉后其余低延迟参数（`-flags low_delay -analyzeduration 0 -probesize 32768 -bf 0 -tune zerolatency`）保持不变。`gofmt -l .`、`go vet ./...`、`go test ./...` 全部通过。局限：只转视频，音频直通；H.265/硬件编码依赖 ffmpeg 构建与驱动，未在本机 GPU 上验收。
- [x] **F-5 多码率 / ABR**
  - 同一路源生成多档码率，按客户端能力选择；输出 master playlist；
  - 验收：播放器可切换档位且不断流。
  - ✅ 已实现：不新增转码核心，复用 F-4 —— 同一 `Source` 的多个 `-transcode` profile 本来就各自发布一路输出流；`cmd/server` 新增 `-transcode-abr`（默认 true）和 `abrGroups`，按 Source 聚合、只保留 ≥2 档的分组，注册给 `hls.Server.SetABR`。`internal/hls/abr.go` 负责阶梯：档位按码率升序；`/hls/<source>/master.m3u8` 输出多个 `EXT-X-STREAM-INF`，`BANDWIDTH` = 配置的视频码率 + 实测音频码率（缺失时 64kbps），`RESOLUTION` 优先用 profile 配置、否则读 SPS，`CODECS` 用参数集解析结果并在未就绪时给 `avc1.42E01E`/`hvc1.1.6.L93.B0` 兜底；每档引用自己的一组 `EXT-X-MEDIA`（`aud0`/`aud1`…），因为每路转码的音频时间轴独立，不能共用音轨；只有最低档 `DEFAULT=YES`。master 请求内会预热所有档位的 packager，让它们从同一时刻开始切片（实测首段 PDT 差约 2ms）；有 LL-HLS part 时 master version=9，否则 7。一个关键坑：DASH 多 Representation 的档位必须只取注册表里**已发布**的流（新增 `hls.Server.ABRRenditions`），否则转码输出尚未注册时会落到本地 testdata 回退源，master 会短暂声明 640x480 的假分辨率——HLS 侧原有的 `registry.Get` 检查同理。`internal/hls/playlist.go` 的 master 生成器扩展为多音频 rendition（校验 GROUP-ID 唯一、每个 variant 引用的组存在且被引用）。`internal/dash` 新增 `LadderProvider` 接口与 ABR 分支：`BuildABR` 把各档放进同一个 video AdaptationSet 的多个 Representation（ID `video0..N`），`SegmentTemplate` 用绝对路径 `/dash/<stream>/video/{init.mp4,seg-$Number$.m4s}` 指向各档自己的段；音频只取第一档的单个 Representation（切视频时音轨不重缓冲）；`availabilityStartTime` 取所有档位中最早的 epoch，`waitReadyAll` 让各档共享 10s 等待预算；没有完整段时回退单流 MPD（与 HLS master 的回退一致）。测试：`playlist_test.go` 多音频组/重复组/未引用组；`abr_test.go` master 顺序/URI/分辨率/音频组/未发布档位过滤/token；`manifest_test.go` `BuildABR` 双 Representation、绝对路径、token、空窗口；`server_test.go` ABR MPD、binary 路由、回退单流；`cmd/server/main_test.go` 按 profile 聚合。e2e 新增 `test/e2e/hls_abr_e2e.py` 与 `test/e2e/dash_abr_e2e.py`（自建服务 + 两档 ffmpeg 转码 + 循环 RTSP 推流）：HLS master BANDWIDTH 升序、两档解码 320x240/640x480；DASH MPD 两个 video Representation、init+段解出对应分辨率；headless Edge + hls.js/dash.js 先固定最低档再切最高档，`currentLevel` 0→1 且 `currentTime` 继续前进（HLS 7.35s→11.35s、DASH 8.62s→12.62s），无 fatal error。注意 dash.js v5 移除了 v4 的 `getBitrateInfoListFor/setQualityFor/getQualityFor`，`dash_player.html` 对 v5 走 `getRepresentationsByType/setRepresentationForTypeByIndex/getCurrentRepresentationForType`、对 v4 保留旧 API。局限：每档一个外部 ffmpeg，CPU 随档位线性增长；切档发生在段边界（LL-HLS 也是段级切换，不是帧级）；播放器验收仅在 Edge（本机 Chromium 构建无 H.264/AAC）完成；音频固定用最低档的 rendition，不做音频码率阶梯或无缝音轨切换。`gofmt -l .`、`go vet ./...`、`go test ./...` 全部通过。
- [x] **F-6 截图 / 缩略图**
  - 从 IDR 解码一帧输出 JPEG/PNG，供预览；
  - 验收：接口返回可显示图片，CPU 开销可接受。
  - ✅ 已实现：新增 `internal/snapshot`，链路是「注册表取视频源 → 预热订阅拿 SPS/PPS 和最近 GOP → 收齐同一 Timestamp 的 IDR slice → 解码 → 编码图片」。取帧按 `AUStart/AUEnd` 聚合多 slice 访问单元，同时用「时间戳变化」兜底推流端没打 AU 标记的情况；参数集优先用帧内最新的 VPS/SPS/PPS，缺失时回退 `stream.VideoCodec`。H.264 用 `github.com/Eyevinn/hi264`（纯 Go，无 cgo，支持 CAVLC/CABAC、8-bit 4:2:0 渐进、POC 0/2），解码后经 `yuv.WriteJPEGCS`/`WritePNGCS` 直接输出 JPEG/PNG；按 `ColorDescriptionValid`/`VideoFullRangeFlag` 传递色彩信息。缩略图用 Y/Cb/Cr 三平面最近邻缩放（宽高取偶数），不做二次编码。H.265 与 hi264 不支持的 H.264（POC type=1、隔行等）在配置 `-snapshot-ffmpeg` 时回退到 ffmpeg：拼 Annex-B（4 字节起始码）从 stdin 喂 `-frames:v 1`，按格式输出 mjpeg/image2pipe，超时由 `-snapshot-ffmpeg-timeout` 控制；未配置时 H.265 直接 415，避免每个请求都先等关键帧再失败。HTTP 层 `GET/HEAD /snapshot/<name>[.jpg|.jpeg|.png]`，支持 `?format=&quality=&width=&fresh=1`：`fresh=1` 用 `SubscribeLive` 跳过预热 GOP，等下一个关键帧；`width` 超过源宽时不放大；`quality` 只影响 JPEG。缓存以「流指针 + 格式/质量/宽度/fresh」为键、默认 TTL 1s，命中时直接复用编码结果并带 `X-Flow-Snapshot-Cache: hit`；解码用带缓冲的信号量限流（默认 2），这两点是控制 CPU 的关键。安全模型复用 HTTP-FLV/HLS 的语义：IP 白名单、CORS（支持 `*`/Origin 列表与预检）、Referer（支持 `*.example.com`，防 `evil.com/example.com` 绕过）、token（`?token=` 或 `X-Flow-Token`，常量时间比较），全部在解码前完成。指标新增 `flow_snapshot_requests_total{result=ok|not_found|unsupported|timeout|error}`、`flow_snapshot_cache_hits_total`、`flow_snapshot_decodes_total`、`flow_snapshot_decode_seconds_total`。`cmd/server` 新增 `-snapshot-addr`（默认 `:8085`）、`-snapshot-timeout`、`-snapshot-cache`、`-snapshot-concurrency`、`-snapshot-quality`、`-snapshot-max-width`、`-snapshot-ffmpeg`、`-snapshot-ffmpeg-timeout`、`-snapshot-cors`、`-snapshot-referer`、`-snapshot-require-referer`、`-snapshot-token` 并做参数校验；流名解析与 HTTP-FLV 一致（精确匹配 → 最后一段 → `-origin` 回源 → 本地 testdata）。测试新增 `internal/snapshot`：JPEG/PNG 解码与缩放尺寸、HEAD/404/415/503、token/CORS/Referer、缓存命中与指标、本地回退源、`fresh` 等待新关键帧，以及 ffmpeg HEVC 兜底（缺 ffmpeg 或 libx265 时跳过）。e2e：本地 testdata 640x480，`/snapshot/live/test.jpg?width=320` 返回可解码的 320x240 JPEG，PNG 160x120 正常；首次「取帧+解码+缩放+编码」54ms，命中缓存后 1~2ms，连续 5 次请求只解码 1 次；hi264 单独基准 1080p IDR 解码约 55ms，因此默认 1s 缓存 + 2 路并发下 CPU 开销可接受。局限：只解 IDR（不做帧间预测），因此截图滞后最多一个 GOP；H.265 依赖外部 ffmpeg；hi264 仅支持 8-bit 4:2:0 渐进 H.264。`gofmt -l .`、`go vet ./...`、`go test ./...` 全部通过。

---

## P — 协议与分发扩展（P3）

- [x] **P-1 HTTP-FLV**
  - 复用 FLV 封装，HTTP 长连接推流；支持跨域/防盗链；
  - 验收：`ffplay http://127.0.0.1:8080/live/test.flv` 正常。
  - ✅ 已实现：新增 `internal/flvmux` 把 `source.Frame` 转成 FLV Tag，逻辑与 RTMP 拉流一致但独立成包供 HTTP-FLV 复用：视频按访问单元合并多 slice、DTS=PTS−CTS 写入 tag 时间戳、CTS 写进 CompositionTime 低 24 位，SPS/PPS（HEVC 含 VPS）生成序列头且支持参数集晚到（先丢媒体帧、补齐后补发序列头）；音频 AAC 生成 ASC 序列头、MP3/G.711 生成裸 tag、Opus 整体跳过；每条轨的时间戳各自从首帧建立相对基准，避免 RTSP 音视频 RTP 时钟相差上亿时互相污染。新增 `internal/httpflv` 服务器：默认监听 `:8080`，路由 `/live/<name>.flv`，先按完整路径查注册表（RTSP 的 `live/<name>`），未命中再用最后一段匹配 RTMP 裸流名，注册表未命中时回退 testdata 本地源；响应为 chunked FLV：文件头 → onMetaData（`stream.SetMetaDataPayload` 保存推流端原始 AMF0 负载，RTMP `handleData` 写入）→ 音视频序列头 → 两条轨的 goroutine 持续泵送。写路径用 `bufio.Writer` + 互斥锁串行化并显式 `http.ResponseController.Flush()`——只刷本地 bufio 时 net/http 自身 2KB 缓冲会让小包卡住、客户端连响应头都收不到（单测曾因此挂死）；每次写前重设 15s 写超时，客户端断开（`r.Context().Done()`）或写失败立即结束另一条轨。安全项都在进入流式响应前完成：CORS 预检（`-httpflv-cors`，支持 `*` 或 Origin 允许列表、回显请求头、`Vary: Origin`）、Referer 防盗链（`-httpflv-referer`/`-httpflv-require-referer`，支持 `example.com`、`*.example.com`，按主机名匹配防 `evil.com/example.com` 绕过）、token（`-httpflv-token`，query `?token=` 或 `X-Flow-Token`，常量时间比较）、复用全局 IP 白名单；`-httpflv-addr` 为空可整体关闭。`cmd/server` 新增对应 flag，与 RTSP/RTMP 共用注册表、指标与白名单。测试新增 `flvmux`（AU 合并、参数集晚到、AAC/MP3/G.711/Opus）与 `httpflv`（完整流式 tag 顺序、HEAD/404/405、路径穿越、CORS、Referer/token、白名单、流名解析），并跑了 `-race`。e2e（ffmpeg 8.0.1）：本地回退 `ffprobe` 解出 h264+aac；RTSP 推 H.264+AAC → HTTP-FLV 解出 h264+aac，RTSP PCMA → `pcm_alaw`，RTSP Opus → 仅视频；RTMP 推 H.264+MP3 → `mp3`；`ffplay -nodisp http://127.0.0.1:8080/live/test.flv` 正常播放；防盗链/令牌/CORS 预检的 403/200/204 分支均符合预期。`gofmt -l .`、`go vet ./...`、`go test ./...` 全部通过。
- [x] **P-2 HLS（含 LL-HLS）**
  - 切片、m3u8、音视频分离、低延迟部分段；
  - 验收：浏览器/Safari/ffplay 播放，端到端延迟有数据；
  - 施工计划与子任务（P-2-1 ~ P-2-7）见 [hls.md](hls.md)。
  - ✅ 已实现（P-2-1 ~ P-2-6；P-2-7 可选增强未做）：新增 `internal/fmp4`（CMAF init/moof、多 traf、v1 tfdt/trun、CTS）与 `internal/hls`（part/segment 状态机、LL/经典 media playlist、master、packager/store、manager 生命周期、HTTP 分发、blocking reload、preload hint 阻塞、CORS/Referer/token/白名单）。`internal/source` 增加 `SubscribeLive()`，跳过旧 GOP 预热；packager 用合成 DTS/`trackBase` 保持 A/V 偏移，支持带内 SPS/PPS 与 init 版本切换、discontinuity、ENDLIST，并用内存窗口 + `-hls-max-bytes` 回收。`cmd/server` 新增 `-hls-addr :8082`（空关闭）/`-hls-segment`/`-hls-part`/`-hls-window`/`-hls-idle`/`-hls-max-bytes`/`-hls-cors`/`-hls-referer`/`-hls-token`，与 RTSP/RTMP/HTTP-FLV 共用 registry、本地源、metrics、whitelist、origin；新增 `flow_hls_packagers`、`flow_hls_requests_total{kind}`、`flow_hls_bytes_served_total{kind}`、`flow_hls_segments_total{track}`、`flow_hls_parts_total{track}`。测试覆盖 fMP4/SPS、切片、playlist 黄金用例、packager A/V `trackBase`、参数集变化、本地回退、manager 回收/替换、HTTP 完整流程、阻塞/超时/窗口滑出、part 阻塞、安全、`-race`；`go test -race -cover ./internal/hls/` 通过（coverage 85.2%），`gofmt -l .` / `go vet ./...` / `go test ./...` 全绿。e2e：`python test/e2e/hls_e2e.py`（LL 1s/200ms）PASS：edge_age p50≈150–180ms、blocking reload p50=94ms、preload hint p50=94ms、A/V 首段 PDT 差 0–1ms；经典 1s PASS：edge_age p50≈230–460ms、reload p50≈890–910ms；下载 init+段后 ffprobe 解出 h264 640x480 + aac 44100 mono，ffmpeg 解码 exit 0，`ffplay -nodisp -autoexit -t 5` exit 0。播放器：`hls.js 1.7.4 + headless Chrome`（lowLatencyMode、liveSyncDurationCount=2）实测 `targetLatency=2.00s`、`hls.latency=2.33s`、状态 playing。**Safari 未实测**：本机无 macOS/iOS，按 hls.md §7.3 以 hls.js + 规范符合性为证据并在 [hls.md](hls.md) §6.3 保留人工验收行。延迟数据暂记在 hls.md §6.3（`日志-导出.md` 尚未创建，见 DOC-2）。
- [x] **P-3 MPEG-DASH**
  - fMP4 切片与 MPD，复用 P-2 的切片管线；
  - 验收：dash.js 参考播放器可播。
  - ✅ 已实现：切片完全复用 `internal/hls` 的 packager（每条流仍只有一个 packager，HLS 与 DASH 共享同一份 fMP4 段），新增 `internal/dash` 只做 MPD 生成与分发。`manifest.go` 输出 `type="dynamic"`、`profiles="urn:mpeg:dash:profile:isoff-live:2011"`，`availabilityStartTime` 取 packager 的切片时钟原点（新增 `hls.Packager.Epoch()`），`publishTime` 取请求时刻；`SegmentTemplate` 用 `$Number$` + `SegmentTimeline`（段长可变，精确到轨道 timescale，新增 `hls.SegmentInfo.DurationTicks`）；`@t` 用段首样本的合成 DTS（新增 `SegmentInfo.StartDTS`，与段内 tfdt 同一条时间轴），`@d` 用 DTS 时长，因此同一连续序列里时间线严格首尾相接，`@t` 只在首段、discontinuity/init 版本切换和时间轴真跳变处显式写出——早期用 PTS 作 `@t` 时，B 帧让相邻段首 CTS 不同，会在段边界留下几十毫秒的假间隙；`minimumUpdatePeriod`/`minBufferTime` 可在命令行配置，`suggestedPresentationDelay` 默认两倍段长并在超过窗口深度 75% 时压到深度的一半，`timeShiftBufferDepth` 按窗口实际覆盖的媒体时间计算；`UTCTiming` 用 `urn:mpeg:dash:utc:http-iso:2014` 指向 `/dash/<name>/utc`。参数集变化开启新 init 版本时不切 Period：一个 Representation 只能引用一份 initialization，因此窗口只暴露最新 `MapURI` 的段，新客户端从新版本开始（时间线前跳一次）。`server.go` 提供 `/dash/<name>/manifest.mpd`（别名 `index.mpd`）、`/utc`、`video|audio/{init.mp4,init-N.mp4,seg-N.m4s}`，流名可含 `/`，二进制走 `Track.Data` + `http.ServeContent`（Range/HEAD/immutable 缓存）；首个 MPD 请求最多阻塞 10s 等音视频各出一个完整段，仍为空时返回 503 + `Retry-After: 2`。`gate.go` 对齐 HLS 的白名单/CORS/Referer/`token`（token 会写进 MPD 的 initialization/media 模板）。`cmd/server` 新增 `-dash-addr :8083`（空关闭）、`-dash-update`、`-dash-delay`、`-dash-cors`、`-dash-referer`、`-dash-require-referer`、`-dash-token`；HLS 关闭而 DASH 打开时依然创建打包主机，保证能切片。指标新增 `flow_dash_requests_total{kind=manifest|init|segment|utc}` 与 `flow_dash_bytes_served_total{kind=init|segment}`。测试新增 `internal/dash/{fixture,manifest,server}_test.go`：MPD 结构与时间线、空窗口、token 注入、init 版本切换、ISO 表达式、路由/404/405/HEAD、UTC、CORS/Referer/token、首段阻塞；`go test -race ./internal/dash/ ./internal/hls/ ./internal/metrics/` 通过。e2e 新增 `test/e2e/dash_e2e.py`（自建服务 + ffmpeg 循环 RTSP 推流，校验 MPD 结构、窗口前进、段/init 字节与解码）与 `test/e2e/dash_player.html`（dash.js 参考播放器，`window.__flow.state()` 暴露状态，`?debug=1` 输出 dash.js 日志）：PASS —— 窗口 1→2 前进、下载窗口内 init+段解出 `h264 640x480 + aac 44100 mono`、`ffmpeg -i manifest.mpd` 解码 3s 无错误、dash.js 5.3.0 状态 `playing`（played 2.5s、fragments 7、`latency 2.57s`、视频 640x480）。注意：本机 Chrome 156 构建不含 H.264/AAC（`MediaSource.isTypeSupported` 直接 false，dash.js 只会报 "No streams to play"），e2e 会逐个探测浏览器能力并自动选用 Edge 157，播放器页面也给出明确提示。局限：单 Representation（不做 ABR）、不做多 Period 与时间线回看定位；dash.js 的 HTTPS 场景需要给 `-dash-cors` 配具体 Origin（token 方案同理）。
- [x] **P-4 WebRTC（WHIP/WHEP）**
  - ICE/DTLS/SRTP、H.264 打包；推流/拉流入口；
  - 验收：浏览器 WHIP 推、WHEP 拉，延迟 < 500ms 基线。
  - ✅ 已实现（详见 [webrtc.md](webrtc.md)）：新增 `internal/webrtc`，传输层用 `github.com/pion/webrtc/v4` 承担 ICE/DTLS/SRTP/RTCP/NACK，媒体面只做 RTP ↔ 注册表桥接。MediaEngine 注册 H.264（PT 102，`level-asymmetry-allowed=1;packetization-mode=1`，**不写 `profile-level-id`**）与 Opus（PT 111，48kHz 双声道）；浏览器带 42001f/42e01f/4d001f/64001f 等 profile 时走 Pion 的 partial match，answer 回显浏览器提议的 PT/fmtp。`POST /whip/<name>`：`OnTrack` 后按 RTP 时间戳把单 NALU/STAP-A/FU-A 解包聚合成 AU（marker 或时间戳变化结尾），每个 AU 写 `AUStart/AUEnd/Keyframe`，SPS/PPS 变化更新 `SetVideoParams`，Opus 一包一帧 `SetAudioCodec(Opus,48000,ch,48000)`，时间戳以各轨首包为基准归零（否则 RTMP/FLV/HLS 下游时间戳会溢出）。`POST /whep/<name>`：按 offer 里的 H.264/Opus 建 `TrackLocalStaticRTP`，等 `connected` 再 `Subscribe()`（建连前 WriteRTP 静默丢包），复用 `internal/rtp` 的 H264Packer（MTU 1200）/OpusPacker 打包，`WriteRTP` 只覆写 SSRC/PT，seq/timestamp/marker 由本地封包器决定。WHIP/WHEP 都返回 `201 + Location + ETag + application/sdp`，`DELETE` 204 幂等，`PATCH` best-effort 补 ICE 候选；`-webrtc-cors`/`-webrtc-token`/全局白名单复用现有安全模型，`Location` 保留 token query。`cmd/server` 新增 `-webrtc-addr :8084`（空关闭）、`-webrtc-public-ip`、`-webrtc-ice-servers`、`-webrtc-ice-username/-credential`、`-webrtc-udp-ports`、`-webrtc-cors`、`-webrtc-token`、`-webrtc-loopback`、`-webrtc-gather-timeout`。指标新增 `flow_webrtc_sessions{kind}`、`flow_webrtc_sessions_total{kind}`，WHEP 计入 `ActiveViewers`。踩坑记录：Pion 的 `OnConnectionStateChange` 是覆盖式单回调，WHEP 初版先注册 connected、再被 `watchConnection` 覆盖，导致拉流永远等 10s 超时——现在合并成一个回调；浏览器 unified-plan offer 不带 `a=ssrc`，服务端要收到首个 RTP 包才触发 `OnTrack`，测试改为“先持续发帧、再等视频轨”。测试新增 `internal/webrtc/webrtc_test.go`：WHIP H.264/Opus 入站、WHEP H.264/Opus 出站、404、路径解析、token、CORS，`go test -race ./internal/webrtc/` 通过。e2e 新增 `test/e2e/webrtc_e2e.html`（canvas 时钟源 + 底部 32bit 时间戳 + CRC-8 条码，拉流端逐帧解码算真实端到端延迟）与 `test/e2e/webrtc_e2e.py`（自建服务、自动挑选支持 H.264 的浏览器——本机 Chromium 无专有编解码器，自动改用 Edge——校验 201/Location/ETag、协商编码、解码帧数、延迟、DELETE 204 与指标回落）。验收 PASS：Edge 157 + 640x360 H.264 + Opus，**p50≈20ms、p95≈44ms**（`--no-audio` 为 19/39ms），远低于 500ms 基线；偶发单帧 max 0.9–1.2s 启动毛刺。局限：只支持 H.264/Opus 直通，AAC 源在 WHEP 侧无音频（不做 AAC→Opus），无 simulcast/SVC/ICE restart。`gofmt -l .`、`go vet ./...`、`go test ./...` 全部通过。
- [x] **P-5 SRT**
  - SRT 推拉、加密/重传；与注册表互通；
  - 验收：`ffmpeg -f mpegts srt://...` 推拉成功。
  - ✅ 已实现：新增 `internal/srt`，基于纯 Go 的 `github.com/datarhei/gosrt`。SRT 只做传输层，连接里是 MPEG-TS，服务端用 `internal/mpegts` 解出 H.264/AAC 帧写入共享 `stream.Registry`，因此 SRT 推上来的流可以从 RTSP/RTMP/HTTP-FLV 拉取，反向也可以把其他协议发布的流通过 SRT 拉走。StreamID 支持 `publish:live/demo`、`read:`/`play:live/demo`、OBS 的 `#!::r=live/demo,m=publish`，裸流名按拉流处理；拉流端在握手阶段先查注册表，不存在直接拒绝（`REJX_NOTFOUND`），避免接受后才失败。加密由 SRT 自带 AES 完成：`-srt-passphrase`（10..80 字符）配置后只接受加密连接且口令必须匹配，未加密连接或口令错误分别以 `REJ_UNSECURE`/`REJ_BADSECRET` 拒绝；未配置口令时加密连接同样被拒。重传/丢包恢复由 libsrt 的 NAK/TSBPD 负责，`-srt-latency` 默认 120ms（live/live 传输类型）。拉流端等待推流端建好音视频轨最多 5s，AAC 由 ASC 还原 ADTS 头，H.265 明确拒绝（TS 只实现了 H.264 stream type）。指标新增 `flow_connections{proto="srt"}`、`flow_srt_publishes(_total)`、`flow_srt_plays(_total)`，并计入 `ActiveViewers` 供 S-2 负载上报。顺带修复两处分片关键帧缺陷（SRT 发布侧每 slice 都清空 GOP 缓存、预热帧首个 IDR slice 丢失 AUStart），否则中途加入的拉流端首帧会大面积花屏、HTTP-FLV 会把每个 slice 当独立帧。测试新增 `internal/mpegts/mpegts_test.go`（封装/解复用往返、分块输入、PTS/DTS、PCR、PSI 刷新）、`internal/srt/srt_test.go`（StreamID、推拉回环、加密/错误口令/未知流拒绝）与 `source` 的多 slice GOP 回归用例。e2e（ffmpeg 8.0.1）：`-f mpegts srt://127.0.0.1:19000?streamid=publish:live/srt&transtype=live` 推流后，SRT/RTSP/HTTP-FLV 三种方式拉流均解出 `h264 640x480 + aac 44100 Hz mono`，无解码告警；RTMP 推流后按注册表名用 SRT 拉流正常；配置 `-srt-passphrase flowtest1234` 后加密推拉成功，明文拉流、明文推流、错误口令分别被拒（`UNSECURE`/`BADSECRET`）。`gofmt -l .`、`go vet ./...`、`go test ./...` 全部通过。
- [x] **P-6 MPEG-TS 输入输出**
  - UDP/文件 TS 解复用为 H.264/AAC 写入注册表；输出 TS 给 P-5/HLS；
  - 验收：TS 推流后 RTSP/RTMP 可拉。
  - ✅ 已实现：新增 `internal/tsingest`，把 SRT 推流里「解复用结果 → 注册表」的注入逻辑抽成共用的 `Ingest`（参数集登记 VPS/SPS/PPS/ASC、逐 NALU 写 `ExternalSource`、AU 边界与关键帧标记、H.264/H.265 自动识别），`internal/srt/publish.go` 改为复用它；SRT 拉流端改用 `mux.SetVideoParamsFor` 并去掉 H.265 拒绝，SRT 现在也能输出 H.265。新增两个独立入口：
    1) `RunUDP`：`-ts-udp "name=host:port"`，组播地址自动 `ListenMulticastUDP` 入组，单播走 `ListenUDP`；4MB 接收缓冲、1s 读超时；空闲超过 `-ts-udp-idle`（默认 10s）摘流，下一包到达时重新发布，发送端重启无需重启 flow；检测到同名流被其他推流端 replace 时让出流名并退出；ctx 取消时关闭 socket 立即退出。一个端口只支持一个发送端（原始 TS 无会话标识）。
    2) `RunFile`：`-ts-file "name=path.ts"`，64KB 分块读取、按 PTS 节奏播放（`-ts-file-rate` 调速，video 用 90kHz、纯音频用采样时钟），播完摘流；`-ts-file-loop` 每圈按「音视频较长者 + 一帧步长」累计 90kHz 偏移（音频偏移按采样率换算）并重置 demuxer/时间戳展开器/节拍器，时间戳跨圈严格单调。配置解析（`ParseUDPInputs`/`ParseFileInputs`，逗号/分号/换行分隔、重名与非法地址校验）放在 `internal/tsingest/config.go`。
    `cmd/server` 新增 `-ts-udp`、`-ts-udp-idle`、`-ts-file`、`-ts-file-loop`、`-ts-file-rate`，每路一个 goroutine 随 ctx 退出。测试新增 `internal/tsingest/{ingest,udp,file,config}_test.go`：H.264/AAC 与 H.265 参数集与逐帧时间戳/AU 边界、UDP 发布→空闲摘流→再发布、同名流被接管、文件循环时间戳单调与 EOF 摘流、配置解析与错误用例。e2e（ffmpeg 8.0.1）：`udp://127.0.0.1:16123?pkt_size=1316` 推 H.264+AAC 后，`ffprobe` 从 RTSP(:8555) 与 RTMP(:19355) 都解出 `h264 640x480 + aac`，并各解码 2s 无错误；`-ts-file srt-sample.ts -ts-file-loop`（3.02s 素材）连续解码 15s 跨约 5 圈无错误，RTMP 解码 8s 无告警（RTSP 拉流在循环素材上会打印 ffmpeg `non monotonically increasing dts`，用 RTMP 推流循环的同一素材可复现，属于 RTSP 出流路径的既有现象，与本次输入无关）。`gofmt -l .`、`go vet ./...`、`go test ./...` 全部通过。

---

## S — 架构与集群（P3）

- [x] **S-1 回源 / 级联**
  - 本机无该流时向上游 RTSP/RTMP/HTTP 拉流并缓存为源，失败回退；
  - 验收：边缘节点按需回源，源站断开后按策略重连。
  - ✅ 已实现：新增 `internal/origin/`，`-origin [name=]scheme://host[:port]/base`（逗号分隔多模板，`-origin-timeout` 默认 5s）支持通配模板与显式映射。RTSP / RTMP / HTTP-FLV 在注册表查不到流时调用 `Manager.Ensure`，首个媒体帧到达（Ready）后以本地流名登记到共享注册表，三种下游协议随即都能拉流；上游断开按 1s→30s 指数退避重连，本地 `*stream.Stream` 不替换、不关闭，下游阻塞等帧。首次回源超时或上游明确不存在时回退到原有 404 / 本地 testdata；Ready 之后 `ErrNotFound` 不再视为终态，源站重启期间继续重连。与 T-3/T-9 打通：RTMP 上游 `?token=` 透传到 play 流名，RTSP URL 的 `user:pass` 自动完成 Basic/Digest 401 challenge。RTMP / HTTP-FLV 跨重连时间戳按 PTS=DTS+CTS 平移（`flvingest.Ingest.Rebase`），避免带 B 帧的上游在源站重启后让下游 muxer 报 non-monotonic DTS。测试：`internal/origin/relay_edge_test.go`（RTMP play token、错误 token 快速失败、RTSP Basic/Digest、上游消失后重连、RTSP DESCRIBE 复用回源流 SDP）、`internal/rtsp/pull_auth_test.go`、`internal/origin/spec_test.go`、`internal/rtmp/client_test.go`（PTS 平移）、`internal/flvingest/ingest_test.go`，`go test -race` 通过。手工 e2e：上游 :8555/:1935 推流，边缘 :8556/:1936/:8081 `-origin rtmp://127.0.0.1:1935/live`，RTSP/RTMP/HTTP-FLV 下游 ffprobe 均 rc=0（h264 640x480 + aac 44100）；杀掉推流端后边缘日志 `origin "live/src": EOF (reconnect in 1s)`，重启推流后自动恢复且下游不中断，HTTP-FLV 抓包跨重连 DTS 无回退；不可达源 `-origin rtsp://127.0.0.1:1/live -origin-timeout 1s` 时边缘日志 `no media within 1s; falling back`，HTTP-FLV 返回 200（本地 testdata）、RTSP DESCRIBE 正常。已知取舍：回源流不做空闲回收，首次失败后后台仍按退避重试。
- [x] **S-2 边缘分发 / 负载均衡**
  - 多节点注册中心（etcd/Redis/自研）记录流位置；客户端重定向或代理；
  - 验收：两节点间推→拉成功，节点宕机后客户端可恢复。
  - ✅ 已实现：注册中心从「一条流一个节点」升级为「流 → 多节点副本」，边缘节点可选代理模式，在源站宕机时对已连接的客户端透明切换上游。
    1) 多副本注册：`discovery.Store` 改为 `map[流名]map[节点]*record`，同名流允许多个节点同时登记（推流源 hops=0，边缘副本 hops>0），只有两个 hops=0 的推流源互相冲突；每条租约携带 `hops`、`upstream`（上游节点）与 `load`（节点活跃观看数，来自 metrics）。新增 `Candidates`/`Pick`/`PickAll`：按负载升序、再按跳数、最后按节点名稳定排序；排除查询节点自身、`hops > -discovery-max-hops` 的副本，以及指向查询节点的反向边（A 从 B 拉时不会再把 B 引回 A）。`GET /v1/streams/<name>?all=1` 返回全部候选（`{"streams":[...]}`），原有单条查询保持不变并返回最优候选。
    2) 负载上报：`metrics.ActiveViewers()`（RTSP/RTMP 活跃会话 + HTTP-FLV 播放）作为节点负载随心跳上报；`Announcer` 增加 `SetLoad`/`SetMaxHops`，只广告 `hops <= maxHops` 的流，HTTP-FLV 广告地址自动补 `.flv`，RTMP 基础地址保留 app 路径（`rtmp://host:1935/live`）以便拼出合法播放地址。
    3) 代理模式（`-discovery-mode proxy`）：`origin.Manager` 增加可插拔 `Locator`，每次重连都重新向注册中心查询候选（发现结果排在静态 `-origin` 模板之前），`SpecFromURL` 把发现的完整播放地址转成回源 Spec，并标注 `Hops = 上游 hops + 1`、`Upstream = 上游节点`；本地流在 `dial` 建立连接时就写入拓扑元数据，announcer 据此上报新的副本身份。拉流仍复用 S-1 puller：首次拿到媒体后登记本地流，**重连期间不关闭、不替换本地流**，已连上的 RTSP / RTMP / HTTP-FLV 下游继续阻塞在 Reader 上，上游恢复后接着收帧，时间戳跨上游平移保持单调。`redirect`（默认）模式保持 S-3 的 RTSP 302 语义，但改为按负载选择最少观看者的节点。
    4) 顺带修复 e2e 暴露的 RTSP 拉流缺陷：RFC 2326 要求第二条 SETUP 带回第一条下发的 `Session`，原实现没带，上游（含 flow 自身）会为音频轨新建一个会话，PLAY 只启动视频会话——经代理链拉流时音频静默丢失。`rtsp.Pull` 现在在后续 SETUP 中携带 Session，并新增 `TestEnsureRelaysRTSPAudio` 回归（该测试在旧实现上失败）。
    测试：`internal/discovery/store_test.go` 覆盖多副本共存、仅推流源互斥、负载/跳数排序、自节点与反向边过滤；`announcer_test.go` 覆盖 hops/upstream/load 上报与超深副本不广告；`internal/origin/spec_test.go` 覆盖 `SpecFromURL` 与「发现优先于静态模板」；`internal/origin/discovery_locator_test.go` 用两路 RTMP 上游模拟 A/B，断言同一下游 reader 在上游切换后继续收到帧、本地流指针不变、元数据变为 `(1, "node-b")`；`internal/rtsp` 与 `internal/origin` 新增音频轨回源回归。`gofmt -l .`（除未跟踪的 `internal/hls/playlist.go`）、`go vet ./...`、`go test ./...`、`go test -race ./internal/discovery/ ./internal/origin/ ./internal/stream/ ./cmd/server/` 全绿。
    端到端（本机三进程，`E:\Desktop\flow_e2e_s2\run.sh`）：registry :17000（TTL 5s）+ 源站 A（RTSP :18554 / RTMP :11935，advertise `rtmp://127.0.0.1:11935/live`）+ 边缘 B（RTSP :18556 / RTMP :11936，`-discovery-mode proxy -discovery-max-hops 1`）。ffmpeg 向 A 推 `live/demo` 后，`GET /v1/streams/demo?all=1` 同时返回 origin（hops 0，load 1）与 edge-b（hops 1，upstream origin）；`ffprobe rtsp://127.0.0.1:18556/demo` 与 `ffprobe rtmp://127.0.0.1:11936/live/demo` 均直接解出 h264 + aac（无 302）；随后启动一个 RTSP 客户端连 B 播放 20s，第 6 秒强杀 A 和推流端、第 10 秒重启：B 日志依次出现 `read ... forcibly closed` → 1s/2s/4s 退避重连，上游恢复后自动接上；同一客户端进程正常跑满 20s（rc=0，375 帧，video 152KiB + audio 1149KiB），播放中途仅约 1.6s 媒体空档。已知取舍：代理模式媒体要经过边缘节点带宽；发现只做精确流名匹配；RTMP/HTTP-FLV 无法跟随 302，适合用代理模式。
- [x] **S-3 动态源注册与发现**
  - 流注册/心跳/过期；与 D-1 Redirect 联动；
  - 验收：源下线后注册表自动清理。
  - ✅ 已实现：分两层补齐「源下线自动清理」与「跨节点发现」。
    1) 本地租约：`internal/stream/janitor.go` 新增 `Janitor`，按 `ExternalSource.LastWrite()` 判断媒体活跃度，超过 `-stream-idle`（默认 30s，0 关闭）没有写入就调用新增的 `Registry.Reap`——判断当前登记指针、未豁免、仍在空闲窗口内全部在同一把锁内完成，避免与 `Publish` 竞态；摘除后 `Close()` 帧源并触发 `Unpublished`，下游 reader 读到流结束而不是一直等帧。`Stream.SetIdleExempt` 让 S-1 回源和 T-9 `-pull` 的流豁免：它们断线时本地流必须保留给下游等帧，由各自的指数退避重连维护。注册表同时新增 `Snapshot()`（按流名排序）、`SetObserver` 以及 Publish/Unpublish（含 replace 时的旧流 Unpublished）事件回调，回调在锁外同步执行且要求非阻塞。
    2) 注册中心：新增 `internal/discovery`，`Store` 以「流名 → 节点 + 各协议 URL + 过期时间」保存租约，支持假时钟测试；`POST /v1/streams` 是整批心跳（登记/续约，并删除该节点本轮未上报的旧条目）、`GET /v1/streams[/name]` 查询、`DELETE /v1/streams/<name>?node=` 立即注销。同名流被另一个仍存活的节点持有时返回 409 语义的 `conflicts`，旧租约过期后允许接管。`Client` 提供 Lookup/Heartbeat/Delete；`Announcer` 用一个后台 goroutine 每 `TTL/3` 上报一次本地 `Active` 的流（回源流上游断开时不广告，避免把其他边缘引过来），publish/unpublish 事件经 100ms 去抖立即刷新，退出时发送空批次清空本节点租约；注册中心不可达只限频打日志，不影响本地播放。新增独立二进制 `cmd/registry`（`-listen :7000 -ttl 15s`）。
    3) D-1 联动：`rtsp.Server.SetDiscovery` 注入 `StreamFinder`；OPTIONS/DESCRIBE 在本地注册表未命中时先查发现服务，命中且不是本节点就复用 D-1 的 `302 Found + Location`，未命中再走 S-1 回源/本地回退。本机条目由 `cmd/server` 的适配器按 `node-id` 过滤，防止重定向自环；查询超时 `-discovery-timeout`（默认 750ms），超时按未命中处理。新增参数 `-node-id`（默认 hostname）、`-advertise`（逗号分隔的 rtsp/rtmp/http 公网基础地址）、`-discovery-url`、`-discovery-ttl`、`-stream-idle`。
    测试：`internal/stream/janitor_test.go` 覆盖空闲回收、活跃流不误杀、豁免流保留、回收关闭源让读者读到结束、旧指针 Reap 不影响新流、Snapshot 排序；`internal/discovery` 覆盖租约边界（差 1ms）、整批替换、存活/过期冲突与接管、归属校验、非法 URL、List/Sweep、假时钟，以及 Client↔Server 的注册/查询/删除、空批次清空和 Announcer 的上下线同步与退出清理；`internal/rtsp/discovery_test.go` 覆盖 DESCRIBE/OPTIONS 302、本地流优先不查发现、未命中回退，`cmd/server` 覆盖本节点条目过滤。`gofmt -l .`、`go vet ./...`、`go test ./...` 全绿，`go test -race ./internal/stream/ ./internal/discovery/ ./internal/rtsp/ ./internal/origin/` 通过。
    端到端（本机三进程）：registry :17000（TTL 5s）+ 节点 A :18554 + 节点 B :18556，两者 `-advertise` 各自 RTSP 地址。ffmpeg 向 A 推 `live/demo` 后 `GET /v1/streams/live/demo` 返回 node-a 与 `rtsp://127.0.0.1:18554/live/demo`；`ffprobe rtsp://127.0.0.1:18556/live/demo` 输出 `Status 302: Redirecting to rtsp://127.0.0.1:18554/live/demo` 并成功解出 h264 640x480 + aac 44100。杀掉推流端后 1s 内注册中心返回 404（节点 A 立即摘流 + 批次心跳移除）；重新推流后强杀节点 A 进程，注册中心在 TTL 到期后（约 4s）返回 404，节点 B 的 DESCRIBE 不再收到 302，回退到本地源。已知取舍：发现只按精确流名匹配（RTSP `live/x` 与 RTMP 裸 `x` 需各自登记）；RTMP 不支持 302，HTTP-FLV 动态重定向与多租户命名空间留给 S-2/S-4。
- [ ] **S-4 多租户与资源隔离**
  - app/路径命名空间、配额、按租户限速/限连接；
  - 验收：租户 A 的流量不影响租户 B 的基线。
- [ ] **S-5 分布式会话**
  - 会话状态外置，多实例共享；拉流重定向时不重建会话；
  - 验收：实例重启后会话按策略恢复或明确失败。

---

## O — 运维与产品（P2/P3）

- [x] **O-1 HTTP 管理 API（P2）**
  - 流列表、会话列表、踢流/踢会话、配置查询；REST + JSON；
  - 验收：curl 能查询并踢掉指定推流，拉流端收到 EOF。
  - ✅ 已实现：新增 `internal/admin`，独立 HTTP 服务 + JSON 路由：`GET /api/v1/streams[/{name}]`、`DELETE /api/v1/streams/{name}`、`GET /api/v1/sessions[?protocol=rtsp|rtmp]`、`DELETE /api/v1/sessions/{id}`、`GET /api/v1/config`。流列表来自共享 `stream.Registry.Snapshot()`，并标注编码参数、创建/最近活跃时间、发布者（协议 + 会话 ID + IP）；`stream.Stream` 新增 `CreatedAt()`/`LastActivity()` 只读方法供展示。会话与踢除通过 `admin.Backend` 接口下放给协议实现：`internal/rtsp/admin.go`（`Sessions`/`KickSession`/`KickStream`）与 `internal/rtmp/admin.go`，本包不反向依赖协议，避免 import 环。踢 RTSP 推流会话走 `Session.Close → closeAll → unpublish → 关闭 ExternalSource`，踢拉流会话发 BYE 后关闭独占 TCP 连接；踢 RTMP 连接走 `Conn.Close → handleConn defer → stopPublish/stopPlay → registry.Unpublish`。RTMP 连接新增管理 ID（`rtmp-N`）、创建/最近收包时间与受锁保护的 app/stream/publishing/playing 快照，避免管理 HTTP goroutine 直接读命令层字段造成数据竞争。踢流只作用于网络推流（RTSP ANNOUNCE / RTMP publish）；`-ts-*` / 转码 / 回源等由配置驱动的流没有可踢会话，返回 `409` 并说明原因。安全：`-admin-addr` 默认关闭，未设置 `-admin-token` 时强制回环监听，非回环绑定必须配 token；支持 `Authorization: Bearer` 与 `X-Flow-Token`，常量时间比较。单元测试覆盖流/轨道/发布者视图、协议过滤、踢流 200/404/409、踢会话、token 401/200、配置查询，以及 RTSP 踢推流后源关闭、拉流端 reader 结束、踢拉流不影响已发布源和 RTMP 连接关闭。e2e：RTSP 推 `live/push` + ffmpeg TCP 拉流，curl 查询到 `sess-1` 发布者，`DELETE /api/v1/streams/live/push` 返回 `{"kicked":1}`，拉流 1s 内收到 EOF（`pull_rc=0`）且注册表清空；RTMP 推 `live/kicktest`（注册名 `kicktest`）+ RTSP 拉流，同样踢流后拉流立即退出、推流 Broken pipe，日志出现 `rtmp: unpublished "kicktest"` 与 `session sess-1 closed: pull source ended`。`gofmt -l .`/`go vet ./...`/`go test ./...` 全绿，`go test -race ./internal/admin/ ./internal/rtsp/ ./internal/rtmp/` 通过。说明：`/api/v1/config` 只返回非敏感运行时配置；`/healthz`/`/readyz` 与 Web 控制台仍属 O-2/O-4。
- [ ] **O-2 Web 控制台（P3，可选）**
  - 简洁页面展示流/会话/指标，支持操作；
  - 验收：浏览器完成查看与踢流。
- [ ] **O-3 鉴权回调（P2）**
  - 推/拉鉴权走 HTTP hook（token、IP、流名），支持超时与缓存；
  - 验收：hook 返回拒绝时推拉被拒，超时按默认策略。
- [ ] **O-4 健康检查与告警（P2）**
  - `/healthz`、`/readyz`；流中断/端口占用/会话异常时告警；
  - 验收：kill 推流端后健康状态与告警变化。
- [ ] **O-5 配置热加载（P2）**
  - YAML/环境变量/flag 合并；SIGHUP 或 watcher 热更新鉴权、限额、日志级别；
  - 验收：不重启服务修改限额生效。
- [ ] **O-6 DVR / 时移（P3）**
  - 环形录制 + 按时间回看；与 F-3 录制、T-5/G-2 seek 联动；
  - 验收：回看最近 5 分钟，拖动/暂停正常。
- [ ] **O-7 日志聚合 / 追踪（P3，可选）**
  - 结构化日志 + trace id（连接/会话/流），对接 Loki/OTel；
  - 验收：一次推拉链路可按 trace 串起来。

---

## DOC — 文档与工程债

- [ ] **DOC-1 CI（P1）**
  - GitHub Actions：gofmt、go vet、`go test -race`、构建，缓存 Go 模块；
  - 验收：PR 自动跑检查，失败阻断合并。
- [ ] **DOC-2 创建 `日志-导出.md`（P1）**
  - AGENTS.md 的 PR 日志流程与本文 DoD 都引用了它，但仓库里没有；
  - 建立索引表 + `devlog/NNN-<slug>.md` 模板；
  - 验收：完成一个任务后能按模板追加记录。
- [x] **DOC-3 清理 legacy 目录（P1）**
  - `Stage 1` ~ `Stage5` 是早期单文件版本；归档到 `legacy/` 或删除并在 README 说明；
  - 验收：`go build ./...` 不受影响，仓库结构清晰。
  - ✅ 已完成：`Stage 1` ~ `Stage5` 已从 `dev` 删除，代码保留在 `stages` 分支；根目录新增 `README.md` 说明目录结构与来源；`test/e2e/rtsp_pause_play.py` 的启动命令改为 `go run ./cmd/server`，删除误命名的 `.gitnore`。
- [ ] **DOC-4 覆盖率与 fuzz 基线（P2）**
  - 关键包（rtsp/rtmp/rtp/flv/stream）覆盖率目标；为各解析器加 fuzz（与 R-3 合并）；
  - 验收：覆盖率报告可查，fuzz 跑 10 分钟无崩溃。
- [x] **DOC-5 LICENSE（P2）**
  - 选择许可证并在仓库根目录添加文件；
  - 采用 MIT：无第三方依赖，许可宽松、与 Go 生态惯例一致；根目录新增 `LICENSE`，README 增加许可证小节；
  - 验收：仓库根目录有 LICENSE，GitHub 仓库页识别为 MIT。
- [ ] **DOC-6 版本与发布（P2）**
  - `-version` 输出、git commit/构建时间注入（ldflags）、CHANGELOG；
  - 验收：二进制能打印版本，发布流程有文档。
- [ ] **DOC-7 一键检查（P2）**
  - Makefile/Taskfile：`make check` = gofmt + vet + `go test -race`；`make e2e` 调 test/e2e；
  - 验收：本地一条命令跑完提交前检查。

---

## 完成定义（DoD）

每个任务完成需同时满足：

1. 代码 + 单元测试（涉及并发必须 `-race`）；
2. 至少一条端到端验收命令或抓包证据；
3. 不破坏既有 RTSP/RTMP 推拉、跨协议互通与 PAUSE 语义；
4. `gofmt`、`go vet`、相关包 `go test` 通过；
5. 在 `日志-导出.md`（需先创建，见 DOC-2）记录真实现象、根因和取舍。

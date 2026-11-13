# done.md — 剩余任务拆解（RTSP / RTP / RTCP / RTMP + 生产化）

> 当前基线（2026-11）：
>
> - **RTSP**：拉流 + 推流都已跑通 —— 文件/stdin 源、UDP/TCP interleaved、H.264+AAC、RTCP SR、PAUSE/PLAY、ANNOUNCE/SETUP/RECORD、RTP 收包解包、转发给拉流端、断开清理（P0-1~P0-5、P1-1~P1-9 全部完成）。
> - **RTMP**：握手、Chunk Stream、AMF0、FLV、控制消息、publish/play、共享注册表、main 双协议接线全部完成（M0~M5，历史计划见 [rtmp.md](rtmp.md)）。
> - **互通**：RTSP ↔ RTMP 按流名共享同一组 `ExternalSource`；本地文件回放、跨协议推拉、断开回收均已 e2e 验证。
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

## T — RTMP 完整性与生产化

- [ ] **T-1 复杂握手（complex/digest handshake，P2）**
  - 现状只做 simple echo（S1 版本字段清零），ffmpeg/ffplay 能退化兼容；只支持 complex handshake 的客户端会失败；
  - 实现 C1/C2/S1/S2 的 HMAC-SHA256（FP9/FP10）校验，校验失败回退 simple；
  - 验收：ffmpeg/ffplay 推拉流不变；complex-only 客户端能完成握手。
- [ ] **T-2 RTMPS（P2）**
  - 与 E-6 共用证书参数，用 `tls.Listen` 包住 RTMP 监听；
  - 验收：`ffmpeg -f flv rtmps://127.0.0.1:1936/live/test` 推流、`ffplay rtmps://...` 拉流成功。
- [ ] **T-3 RTMP 连接鉴权（P0）**
  - 现状 `connect` / `publish` / `play` 不校验任何凭据，任意 app/流名都能推拉；
  - 支持 token/密钥、app 白名单、推拉权限分离；凭据从文件/环境变量读取；
  - 失败回 `NetConnection.Connect.Rejected` 或 `NetStream.Play.StreamNotFound`；日志不打印明文密钥；
  - 验收：错误 token 被拒，正确 token 可推可拉。
- [ ] **T-4 onMetaData（P2）**
  - 现状 type 18 数据消息被忽略（`internal/rtmp/server.go` 注释）；
  - 推流端保存 onMetaData，拉流端在 `Play.Start` 后下发（duration、videocodecid、audiocodecid、宽高、帧率、采样率、声道）；
  - 验收：`ffprobe` 能读出时长/编码/分辨率，ffplay 无相关告警。
- [ ] **T-5 NetStream Pause/Unpause/Seek（P2）**
  - 现状只处理 `play`，没有 `pause` / `seek` 命令；
  - 解析 `pause`/`seek`；暂停停发但保留订阅，恢复后 seq/时间戳连续；文件源按 G-2 索引 seek，live 源返回不支持；
  - 验收：暂停 2s 恢复不重复/不跳变；文件 seek 后从 IDR 开始。
- [ ] **T-6 视频 CTS / B 帧时间戳（P2）**
  - 现状 FLV VideoTag 的 CompositionTime 固定 0，RTSP 侧也不重建 DTS，含 B 帧素材会出现 `non monotonically increasing dts` 告警；
  - 推流侧解析 PTS-DTS 写入 CTS，拉流侧按 CTS 排序；无 B 帧时保持 0；
  - 验收：`testdata/with_audio.mp4` 推拉后 ffmpeg 无 non-monotonic 告警，帧序正确。
- [ ] **T-7 Chunk 层优化（P2，可选）**
  - 评测 Set Chunk Size（当前 4096）对吞吐/延迟的影响；解包 Aggregate Message（type 22）；
  - 验收：压测下吞吐提升且低延迟不退化，单测覆盖聚合消息边界。
- [ ] **T-8 同名推流策略（P2）**
  - 现状 `Registry.Publish` 同名整体替换并关闭旧源；
  - 改为可配置 `replace`（默认）/ `reject`（回 Already Publishing）/ `reject-if-active`；
  - 验收：三种策略行为符合预期，旧源 goroutine 正常回收。
- [ ] **T-9 RTMP 客户端 / 回源（P3，可选）**
  - 新增 client 模式：从上游 RTMP 拉流并作为一路源发布，支持断线重连；
  - 验收：上游断开后自动重连，下游明确收到流结束或持续。
- [ ] **T-10 RTMP e2e 自动化（P1）**
  - 把 rtmp.md 的手工矩阵固化为脚本：本地回放、RTMP↔RTSP 双向、RTMP↔RTMP、断开回收；
  - 断言收到帧数/音视频轨/退出码；
  - 验收：一条命令跑通全矩阵。

---

## F — 编解码与媒体能力（当前只有 H.264 + AAC，P3）

- [ ] **F-1 H.265 / HEVC**
  - RTSP：SDP `H265/90000`、RTP 单 NALU/FU 解包打包、`sprop-vps/sps/pps`；
  - RTMP：Enhanced RTMP / HEVC FourCC；拉流端对老客户端优雅降级；
  - 验收：HEVC 推拉 RTSP/RTMP，另一协议能转出。
- [ ] **F-2 音频扩展**
  - RTSP：G.711（PCMA/PCMU）、Opus、MP3；RTMP：MP3/G.711（按需求）；
  - 验收：各编码单独推拉成功，音画同步。
- [ ] **F-3 转封装 / 录制**
  - 把实时流写成 MP4/FLV/TS（按关键帧切分、断点续录、磁盘配额）；
  - 验收：录制 1 分钟文件可被 ffprobe/ffplay 正常播放。
- [ ] **F-4 转码**
  - 解码 → 缩放/转码 → 编码（H.264/H.265、多分辨率）；纯 Go 性能有限，评估 cgo/外部进程方案；
  - 验收：1080p 输入转 720p 输出，延迟与 CPU 有基线数据。
- [ ] **F-5 多码率 / ABR**
  - 同一路源生成多档码率，按客户端能力选择；输出 master playlist；
  - 验收：播放器可切换档位且不断流。
- [ ] **F-6 截图 / 缩略图**
  - 从 IDR 解码一帧输出 JPEG/PNG，供预览；
  - 验收：接口返回可显示图片，CPU 开销可接受。

---

## P — 协议与分发扩展（P3）

- [ ] **P-1 HTTP-FLV**
  - 复用 FLV 封装，HTTP 长连接推流；支持跨域/防盗链；
  - 验收：`ffplay http://127.0.0.1:8080/live/test.flv` 正常。
- [ ] **P-2 HLS（含 LL-HLS）**
  - 切片、m3u8、音视频分离、低延迟部分段；
  - 验收：浏览器/Safari/ffplay 播放，端到端延迟有数据。
- [ ] **P-3 MPEG-DASH**
  - fMP4 切片与 MPD，复用 P-2 的切片管线；
  - 验收：dash.js 参考播放器可播。
- [ ] **P-4 WebRTC（WHIP/WHEP）**
  - ICE/DTLS/SRTP、H.264 打包；推流/拉流入口；
  - 验收：浏览器 WHIP 推、WHEP 拉，延迟 < 500ms 基线。
- [ ] **P-5 SRT**
  - SRT 推拉、加密/重传；与注册表互通；
  - 验收：`ffmpeg -f mpegts srt://...` 推拉成功。
- [ ] **P-6 MPEG-TS 输入输出**
  - UDP/文件 TS 解复用为 H.264/AAC 写入注册表；输出 TS 给 P-5/HLS；
  - 验收：TS 推流后 RTSP/RTMP 可拉。

---

## S — 架构与集群（P3）

- [ ] **S-1 回源 / 级联**
  - 本机无该流时向上游 RTSP/RTMP/HTTP 拉流并缓存为源，失败回退；
  - 验收：边缘节点按需回源，源站断开后按策略重连。
- [ ] **S-2 边缘分发 / 负载均衡**
  - 多节点注册中心（etcd/Redis/自研）记录流位置；客户端重定向或代理；
  - 验收：两节点间推→拉成功，节点宕机后客户端可恢复。
- [ ] **S-3 动态源注册与发现**
  - 流注册/心跳/过期；与 D-1 Redirect 联动；
  - 验收：源下线后注册表自动清理。
- [ ] **S-4 多租户与资源隔离**
  - app/路径命名空间、配额、按租户限速/限连接；
  - 验收：租户 A 的流量不影响租户 B 的基线。
- [ ] **S-5 分布式会话**
  - 会话状态外置，多实例共享；拉流重定向时不重建会话；
  - 验收：实例重启后会话按策略恢复或明确失败。

---

## O — 运维与产品（P2/P3）

- [ ] **O-1 HTTP 管理 API（P2）**
  - 流列表、会话列表、踢流/踢会话、配置查询；REST + JSON；
  - 验收：curl 能查询并踢掉指定推流，拉流端收到 EOF。
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
- [ ] **DOC-3 清理 legacy 目录（P1）**
  - `Stage 1` ~ `Stage5` 是早期单文件版本；归档到 `legacy/` 或删除并在 README 说明；
  - 验收：`go build ./...` 不受影响，仓库结构清晰。
- [ ] **DOC-4 覆盖率与 fuzz 基线（P2）**
  - 关键包（rtsp/rtmp/rtp/flv/stream）覆盖率目标；为各解析器加 fuzz（与 R-3 合并）；
  - 验收：覆盖率报告可查，fuzz 跑 10 分钟无崩溃。
- [ ] **DOC-5 LICENSE（P2）**
  - 选择许可证并在仓库根目录添加文件；
  - 验收：仓库根目录有 LICENSE。
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

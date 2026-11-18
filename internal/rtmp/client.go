package rtmp

// T-9：RTMP 客户端 / 回源。
//
// Client 主动连上游 RTMP 服务器，以客户端身份握手、connect / createStream /
// play，把上游返回的 FLV 音视频 tag 通过推流侧同一套解包逻辑写进一路本地
// stream.Stream，从而让本地 RTSP / RTMP / HTTP-FLV 都能按流名拉到这路「回源」。
//
// 重连语义是「透明续传」：整个生命周期只登记一路本地流，上游断开时保留
// ExternalSource（下游读者阻塞在 Next 上，不会收到结束），连上后继续写帧；
// 只有 ctx 取消（进程退出）才注销并关闭本地流。上游重连后时间戳从 0 重新
// 计时，Client 用 rebaseClock 把各轨时间戳平移到单调递增，避免下游 RTP
// 时间戳倒退。

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"flow/internal/flv"
	"flow/internal/flvingest"
	"flow/internal/metrics"
	"flow/internal/stream"
)

// 回源默认参数。
const (
	defaultPullHandshakeTimeout = 10 * time.Second
	defaultPullConnTimeout      = 60 * time.Second
	defaultPullMinBackoff       = time.Second
	defaultPullMaxBackoff       = 30 * time.Second
	defaultRTMPPort             = "1935"
)

// pullSpec 解析后的上游/本地参数。
type pullSpec struct {
	addr       string // host:port
	app        string // connect 的 app 字段
	streamName string // play 的流名（保留 ?query，供上游鉴权）
	tcURL      string // connect 的 tcUrl
	localName  string // 本地注册表流名
}

// Client 一路 RTMP 回源（一个上游流）。
type Client struct {
	spec     *pullSpec
	registry *stream.Registry
	metrics  *metrics.Registry

	handshakeTimeout time.Duration
	connTimeout      time.Duration
	minBackoff       time.Duration
	maxBackoff       time.Duration
}

// NewClient 解析 spec 并创建回源客户端。spec 形如：
//
//	rtmp://host[:port]/app[/sub]/stream[?token=...]
//	本地名=rtmp://host[:port]/app/stream        // 重命名后本地注册
//
// 本地流名默认取 URL 最后一节。reg 为 nil 时自建一份（仅测试用）；
// metrics 为 nil 时自建，保证计数调用安全。
func NewClient(spec string, reg *stream.Registry, m *metrics.Registry) (*Client, error) {
	ps, err := parsePullSpec(spec)
	if err != nil {
		return nil, err
	}
	if reg == nil {
		reg = stream.NewRegistry()
	}
	if m == nil {
		m = metrics.New()
	}
	return &Client{
		spec:             ps,
		registry:         reg,
		metrics:          m,
		handshakeTimeout: defaultPullHandshakeTimeout,
		connTimeout:      defaultPullConnTimeout,
		minBackoff:       defaultPullMinBackoff,
		maxBackoff:       defaultPullMaxBackoff,
	}, nil
}

// LocalName 返回本地注册表流名。
func (cl *Client) LocalName() string { return cl.spec.localName }

// SetTimeouts 设置握手与读写空闲超时。<=0 表示不设超时。
func (cl *Client) SetTimeouts(handshake, conn time.Duration) {
	cl.handshakeTimeout = handshake
	cl.connTimeout = conn
}

// SetBackoff 设置重连退避的初始值与上限。min 会被夹到 >=10ms，max < min 时
// 取 min。测试用短值加快重连。
func (cl *Client) SetBackoff(minBackoff, maxBackoff time.Duration) {
	if minBackoff <= 0 {
		minBackoff = defaultPullMinBackoff
	}
	if minBackoff < 10*time.Millisecond {
		minBackoff = 10 * time.Millisecond
	}
	if maxBackoff < minBackoff {
		maxBackoff = minBackoff
	}
	cl.minBackoff = minBackoff
	cl.maxBackoff = maxBackoff
}

// parsePullSpec 解析回源参数。
func parsePullSpec(raw string) (*pullSpec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("rtmp pull: empty spec")
	}
	local := ""
	if !strings.HasPrefix(raw, "rtmp://") && !strings.HasPrefix(raw, "rtmps://") {
		i := strings.IndexByte(raw, '=')
		if i <= 0 {
			return nil, fmt.Errorf("rtmp pull: invalid spec %q (want [name=]rtmp://host/app/stream)", raw)
		}
		local = strings.TrimSpace(raw[:i])
		raw = strings.TrimSpace(raw[i+1:])
	}

	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("rtmp pull: parse %q: %w", raw, err)
	}
	if u.Scheme != "rtmp" {
		return nil, fmt.Errorf("rtmp pull: unsupported scheme %q (want rtmp)", u.Scheme)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("rtmp pull: missing host in %q", raw)
	}
	addr := u.Host
	if u.Port() == "" {
		addr = net.JoinHostPort(u.Hostname(), defaultRTMPPort)
	}

	path := strings.Trim(u.Path, "/")
	i := strings.LastIndexByte(path, '/')
	if i <= 0 || i == len(path)-1 {
		return nil, fmt.Errorf("rtmp pull: %q needs app and stream (rtmp://host/app/stream)", raw)
	}
	app := path[:i]
	name := path[i+1:]

	streamName := name
	if u.RawQuery != "" {
		streamName += "?" + u.RawQuery
	}
	if local == "" {
		local = sanitizeStreamName(name)
	} else {
		local = sanitizeStreamName(local)
	}
	if local == "" {
		return nil, fmt.Errorf("rtmp pull: empty local stream name in %q", raw)
	}

	return &pullSpec{
		addr:       addr,
		app:        app,
		streamName: streamName,
		tcURL:      "rtmp://" + u.Host + "/" + app,
		localName:  local,
	}, nil
}

// Run 一直回源，直到 ctx 取消。内部自动重连（指数退避）；返回时注销并关闭
// 本地流，让下游读到流结束。
func (cl *Client) Run(ctx context.Context) {
	local := &stream.Stream{}
	// S-3：-pull 的回源流由客户端重连逻辑维护，断线期间不能被
	// 本地 janitor 当空闲流回收。
	local.SetIdleExempt(true)
	st, err := cl.registry.Publish(cl.spec.localName, local)
	if err != nil {
		fmt.Printf("rtmp pull %q: publish local stream %q: %v\n",
			cl.spec.streamName, cl.spec.localName, err)
		return
	}
	cl.metrics.IncPublish()
	cl.metrics.IncPublishTotal()
	defer func() {
		cl.metrics.DecPublish()
		cl.registry.Unpublish(cl.spec.localName, st)
		fmt.Printf("rtmp pull: stopped relaying %q to local %q\n",
			cl.spec.streamName, cl.spec.localName)
	}()

	clock := &rebaseClock{}
	backoff := cl.minBackoff
	for {
		if ctx.Err() != nil {
			return
		}
		// 幂等：本地同名被其它推流按 replace 策略抢占时，重新登记回来。
		// 仍是我们自己时不重复 Publish，避免无意义地关闭其它流或刷日志。
		if cl.registry.Get(cl.spec.localName) != st {
			if _, err := cl.registry.Publish(cl.spec.localName, st); err != nil {
				fmt.Printf("rtmp pull %q: re-publish local %q: %v\n",
					cl.spec.streamName, cl.spec.localName, err)
			}
		}

		played, err := cl.dialAndPull(ctx, st, clock)
		if ctx.Err() != nil {
			return
		}
		if played {
			// 成功播放过就重置退避，避免一次长时间在线后仍沿用大退避。
			backoff = cl.minBackoff
		}
		if err != nil {
			fmt.Printf("rtmp pull %s: %v (reconnect in %s)\n", cl.spec.addr, err, backoff)
		} else {
			fmt.Printf("rtmp pull %s: upstream ended (reconnect in %s)\n", cl.spec.addr, backoff)
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < cl.maxBackoff {
			backoff *= 2
			if backoff > cl.maxBackoff {
				backoff = cl.maxBackoff
			}
		}
	}
}

// ErrStreamNotFound 表示上游明确回复流不存在（NetStream.Play.StreamNotFound /
// FileNotFound）。回源据此停止重试，而不是把上游打满。
var ErrStreamNotFound = errors.New("rtmp: stream not found")

// PullOptions 一次 RTMP 拉流会话的参数。Stream 是本地共享流，重连时复用
// 同一个指针；Rebase 可选，用来把上游时间戳平移到跨重连单调；OnFirstMedia
// 可选，在第一个音视频 tag 写出后调用一次（回源的 Ready 信号）。
type PullOptions struct {
	Addr       string
	App        string
	TcURL      string
	StreamName string
	Stream     *stream.Stream
	// Rebase 可选：输入上游 DTS 与 CTS（毫秒），返回平移后的 DTS。
	Rebase func(dtsMs uint32, ctsMs int32) uint32
	// OnFirstMedia 在第一个成功处理的上游音视频 tag 后调用一次。
	OnFirstMedia func()
	// HandshakeTimeout 覆盖握手与命令阶段超时；<=0 用默认值。
	HandshakeTimeout time.Duration
	// ConnTimeout 是消息读空闲超时；<=0 用默认值。
	ConnTimeout time.Duration
}

// Pull 建立一次上游 RTMP 会话并读到断开。played 表示是否成功走到
// Play.Start（用于决定是否重置退避）。调用方负责重连。
func Pull(ctx context.Context, o PullOptions) (played bool, err error) {
	if o.Stream == nil {
		return false, errors.New("rtmp pull: nil stream")
	}
	if o.HandshakeTimeout <= 0 {
		o.HandshakeTimeout = defaultPullHandshakeTimeout
	}
	if o.ConnTimeout <= 0 {
		o.ConnTimeout = defaultPullConnTimeout
	}

	dialer := net.Dialer{Timeout: o.HandshakeTimeout}
	raw, err := dialer.DialContext(ctx, "tcp", o.Addr)
	if err != nil {
		return false, fmt.Errorf("dial: %w", err)
	}

	c := newConn(raw)
	c.handshakeTimeout = o.HandshakeTimeout
	c.connTimeout = o.ConnTimeout
	// 复用推流侧的解包逻辑：上游发来的 FLV tag 直接写进本地流。
	ingest := flvingest.NewIngest(o.Stream)
	ingest.OnFirstMedia = o.OnFirstMedia
	// 平移必须在 ingest 里按 DTS+CTS 做：先改 tag 时间戳再加 CTS 会让
	// 带正 CTS 的 B 帧在重连后 PTS 回退。
	ingest.Rebase = o.Rebase
	c.publish = ingest

	// ctx 取消时关闭连接，唤醒阻塞在 readMessage 上的 goroutine。
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = c.Close()
		case <-done:
		}
	}()
	defer c.Close()

	if err := c.clientHandshake(); err != nil {
		return false, err
	}

	// 命令阶段整体限时，避免上游接受 TCP 却不回 _result 时永久挂住。
	if c.handshakeTimeout > 0 {
		_ = c.SetReadDeadline(time.Now().Add(c.handshakeTimeout))
	}
	if err := c.clientConnect(o.App, o.TcURL); err != nil {
		return false, err
	}
	streamID, err := c.clientCreateStream()
	if err != nil {
		return false, err
	}
	if err := c.clientPlay(streamID, o.StreamName); err != nil {
		return false, err
	}
	_ = c.SetReadDeadline(time.Time{})
	played = true

	return true, c.pump()
}

// dialAndPull 建立一次上游会话并读到断开。played 表示是否成功走到
// Play.Start（用于决定是否重置退避）。
func (cl *Client) dialAndPull(ctx context.Context, st *stream.Stream, clock *rebaseClock) (played bool, err error) {
	// 每条新连接重新开始计时，rebaseClock 把时间戳平移到上次的最大值之后。
	clock.newConn()
	return Pull(ctx, PullOptions{
		Addr:             cl.spec.addr,
		App:              cl.spec.app,
		TcURL:            cl.spec.tcURL,
		StreamName:       cl.spec.streamName,
		Stream:           st,
		Rebase:           clock.apply,
		HandshakeTimeout: cl.handshakeTimeout,
		ConnTimeout:      cl.connTimeout,
	})
}

// clientConnect 发 connect 并等 NetConnection.Connect.Success。
func (c *Conn) clientConnect(app, tcURL string) error {
	obj := flv.Obj(
		flv.Elem("app", flv.Str(app)),
		flv.Elem("tcUrl", flv.Str(tcURL)),
		flv.Elem("flashVer", flv.Str("FMLE/3.0 (compatible; FMSc/1.0)")),
		flv.Elem("type", flv.Str("nonprivate")),
	)
	if err := c.writeCommand(0, "connect", 1, obj); err != nil {
		return fmt.Errorf("rtmp: send connect: %w", err)
	}
	cmd, err := c.readCommand("_result")
	if err != nil {
		return fmt.Errorf("rtmp: wait connect result: %w", err)
	}
	if code := commandStatusCode(cmd); code != "" && code != "NetConnection.Connect.Success" {
		return fmt.Errorf("rtmp: connect rejected: %s", code)
	}
	return nil
}

// clientCreateStream 发 createStream 并取回 message stream id。
func (c *Conn) clientCreateStream() (uint32, error) {
	if err := c.writeCommand(0, "createStream", 2, flv.Null()); err != nil {
		return 0, fmt.Errorf("rtmp: send createStream: %w", err)
	}
	cmd, err := c.readCommand("_result")
	if err != nil {
		return 0, fmt.Errorf("rtmp: wait createStream result: %w", err)
	}
	n, ok := cmd.numberArg(0)
	if !ok || n <= 0 {
		return 0, errors.New("rtmp: createStream returned no stream id")
	}
	return uint32(n), nil
}

// clientPlay 发 play 并等 NetStream.Play.Start（期间会先收到 Play.Reset）。
func (c *Conn) clientPlay(streamID uint32, name string) error {
	if err := c.writeCommand(streamID, "play", 3, flv.Null(), flv.Str(name)); err != nil {
		return fmt.Errorf("rtmp: send play: %w", err)
	}
	for {
		cmd, err := c.readCommand("onStatus")
		if err != nil {
			return fmt.Errorf("rtmp: wait play status: %w", err)
		}
		switch code := commandStatusCode(cmd); code {
		case "NetStream.Play.Start":
			return nil
		case "NetStream.Play.StreamNotFound", "NetStream.Play.FileNotFound":
			return fmt.Errorf("%w: play %q failed: %s", ErrStreamNotFound, name, code)
		case "NetStream.Play.Failed":
			return fmt.Errorf("rtmp: play %q failed: %s", name, code)
		}
		// Play.Reset 等中间状态，继续等 Play.Start。
	}
}

// readCommand 读到指定命令名为止；途经的控制消息就地应用。仅用于握手/
// 建流阶段（此时上游不应已有媒体数据）。
func (c *Conn) readCommand(name string) (*Command, error) {
	for {
		m, err := c.readMessage()
		if err != nil {
			return nil, err
		}
		if err := c.maybeAck(); err != nil {
			return nil, err
		}
		if handled, err := c.handleControl(m); err != nil {
			return nil, err
		} else if handled {
			continue
		}
		if m.TypeID != msgCommandAMF0 {
			continue
		}
		cmd, err := parseCommand(m.Payload)
		if err != nil {
			return nil, err
		}
		if cmd.Name == name {
			return cmd, nil
		}
	}
}

// commandStatusCode 取命令参数对象里的 code 字段（_result / onStatus 的第二个
// 参数通常是信息对象）。与测试里的 commandCode 区分命名。
func commandStatusCode(cmd *Command) string {
	for _, a := range cmd.Args {
		if a.Type != flv.TypeObject && a.Type != flv.TypeECMAArray {
			continue
		}
		for _, m := range a.Object {
			if m.Key == "code" && m.Value.Type == flv.TypeString {
				return m.Value.String
			}
		}
	}
	return ""
}

// rebaseClock 把上游时间戳平移成跨重连单调递增。
//
// 上游每次重连后从 0 重新计时；本时钟记录历史最大 PTS（last），新连接的
// 第一帧选择平移量，使它的 PTS=DTS+CTS 落在 last+1，后续帧保持连接内的
// 相对时间差。只有读循环一个 goroutine 访问，无需加锁。
type rebaseClock struct {
	started bool
	base    uint32
	last    uint32
	minCTS  int32 // 已见的最小 CTS（一般为负），重连时留出同样的重排窗口
}

// newConn 标记进入新连接：下一帧重新计算基准。
func (r *rebaseClock) newConn() { r.started = false }

// apply 平移视频/音频的 DTS。cts 是 PTS−DTS（毫秒）。
func (r *rebaseClock) apply(dts uint32, cts int32) uint32 {
	if cts < r.minCTS {
		r.minCTS = cts
	}
	if !r.started {
		r.started = true
		r.base = 0
		if r.last > 0 {
			// 首帧的 DTS 是本连接最小的 DTS；留出 minCTS 的重排窗口，
			// 保证之后带负 CTS 的 B 帧 PTS 也落在 last+1 之后。
			if shift := int64(r.last) + 1 - int64(dts) - int64(r.minCTS); shift > 0 {
				r.base = uint32(shift)
			}
		}
	}
	v := int64(dts) + int64(r.base)
	if v < 0 {
		v = 0
	}
	if p := v + int64(cts); p > int64(r.last) {
		r.last = uint32(p)
	}
	return uint32(v)
}

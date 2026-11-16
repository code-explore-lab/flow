// cmd/loadtest 是 E-1 的压测客户端。
//
// 启动 N 路并发 RTSP 拉流：每路独立建 TCP 控制连接，完成
// DESCRIBE -> SETUP(video) -> PLAY，随后持续读包直到指定时长，
// 统计建连/请求耗时、收包数、字节数和 RTP 序号空洞（丢包）。
//
// 会话有 60s 空闲超时，客户端每 20s 用 GET_PARAMETER 保活，
// 与服务端 session.Manager 的回收策略配合。
//
// 用法：
//
//	go run ./cmd/loadtest -url rtsp://127.0.0.1:8554/live -n 20 -d 5m
//	go run ./cmd/loadtest -url rtsp://127.0.0.1:8554/live -n 20 -transport udp
package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const keepaliveInterval = 20 * time.Second

type config struct {
	baseURL     string // 归一化后的 RTSP URL（无末尾斜杠）
	setupURL    string // baseURL + "/trackID=0"
	dialAddr    string // host:port
	clients     int
	duration    time.Duration
	transport   string // tcp | udp
	timeout     time.Duration
	reportEvery time.Duration
	maxLoss     float64
}

type counters struct {
	established atomic.Int64
	active      atomic.Int64
	packets     atomic.Uint64
	bytes       atomic.Uint64
	gaps        atomic.Uint64
}

func main() {
	os.Exit(run())
}

func run() int {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "参数错误:", err)
		return 2
	}

	agg := &counters{}
	clients := make([]*client, cfg.clients)
	for i := range clients {
		clients[i] = newClient(i, cfg, agg)
	}

	start := time.Now()
	fmt.Printf("E-1 压测开始：%d 路并发，持续 %s，传输 %s，目标 %s\n",
		cfg.clients, cfg.duration, strings.ToUpper(cfg.transport), cfg.baseURL)

	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Add(1)
		go func(c *client) {
			defer wg.Done()
			c.run()
		}(c)
	}

	done := make(chan struct{})
	go reportLoop(agg, start, cfg, done)

	wg.Wait()
	close(done)
	return summarize(clients, time.Since(start), cfg)
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("loadtest", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var (
		rawURL    = fs.String("url", "rtsp://127.0.0.1:8554/live", "RTSP 拉流地址")
		n         = fs.Int("n", 20, "并发客户端数")
		dur       = fs.Duration("d", 5*time.Minute, "压测持续时间")
		transport = fs.String("transport", "tcp", "RTP 传输：tcp（interleaved）或 udp")
		timeout   = fs.Duration("timeout", 5*time.Second, "单个 RTSP 请求超时")
		report    = fs.Duration("report", 30*time.Second, "进度输出间隔")
		maxLoss   = fs.Float64("max-loss", 0.01, "允许的最大丢包率（0~1），超过判定失败")
	)
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	u, err := url.Parse(*rawURL)
	if err != nil {
		return config{}, fmt.Errorf("解析 -url: %w", err)
	}
	if u.Scheme != "rtsp" || u.Host == "" {
		return config{}, fmt.Errorf("-url 必须是 rtsp://host[:port]/path，得到 %q", *rawURL)
	}
	host := u.Host
	if _, _, err := net.SplitHostPort(host); err != nil {
		host = net.JoinHostPort(host, "554")
	}
	base := strings.TrimRight(*rawURL, "/")

	if *n <= 0 {
		return config{}, errors.New("-n 必须大于 0")
	}
	if *dur <= 0 {
		return config{}, errors.New("-d 必须大于 0")
	}
	if *timeout <= 0 || *report <= 0 {
		return config{}, errors.New("-timeout 和 -report 必须大于 0")
	}
	if *maxLoss < 0 || *maxLoss > 1 {
		return config{}, errors.New("-max-loss 必须在 [0,1] 内")
	}
	switch *transport {
	case "tcp", "udp":
	default:
		return config{}, fmt.Errorf("-transport 只能是 tcp 或 udp，得到 %q", *transport)
	}

	return config{
		baseURL:     base,
		setupURL:    base + "/trackID=0",
		dialAddr:    host,
		clients:     *n,
		duration:    *dur,
		transport:   *transport,
		timeout:     *timeout,
		reportEvery: *report,
		maxLoss:     *maxLoss,
	}, nil
}

// client 是一路独立的拉流会话；除聚合计数器外，字段只在自身 goroutine 读写。
type client struct {
	id  int
	cfg config
	agg *counters

	conn     net.Conn
	br       *bufio.Reader
	rtpConn  *net.UDPConn
	rtcpConn *net.UDPConn
	cseq     int
	session  string

	connectLat  time.Duration
	describeLat time.Duration
	setupLat    time.Duration
	playLat     time.Duration

	playStart  time.Time
	stopAt     time.Time
	packets    uint64
	bytes      uint64
	gaps       uint64
	tracker    seqTracker
	lastPacket time.Time
	err        error
}

func newClient(id int, cfg config, agg *counters) *client {
	return &client{id: id, cfg: cfg, agg: agg}
}

func (c *client) run() {
	established := false
	defer func() {
		c.close()
		if established {
			c.agg.active.Add(-1)
		}
	}()

	if err := c.establish(); err != nil {
		c.err = fmt.Errorf("client %d: %w", c.id, err)
		return
	}
	established = true
	c.agg.established.Add(1)
	c.agg.active.Add(1)
	c.stream()
}

func (c *client) establish() error {
	start := time.Now()
	conn, err := net.DialTimeout("tcp", c.cfg.dialAddr, c.cfg.timeout)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	c.conn = conn
	c.br = bufio.NewReaderSize(conn, 64<<10)
	c.connectLat = time.Since(start)

	t0 := time.Now()
	resp, err := c.request("DESCRIBE", c.cfg.baseURL, hdr{"Accept", "application/sdp"})
	c.describeLat = time.Since(t0)
	if err != nil {
		return fmt.Errorf("DESCRIBE: %w", err)
	}
	if !isOK(resp) {
		return fmt.Errorf("DESCRIBE: %s", resp.status)
	}

	transport, err := c.setupTransport()
	if err != nil {
		return err
	}
	t0 = time.Now()
	resp, err = c.request("SETUP", c.cfg.setupURL, hdr{"Transport", transport})
	c.setupLat = time.Since(t0)
	if err != nil {
		return fmt.Errorf("SETUP: %w", err)
	}
	if !isOK(resp) {
		return fmt.Errorf("SETUP: %s", resp.status)
	}
	c.session = sessionValue(resp.headers["session"])
	if c.session == "" {
		return errors.New("SETUP: 响应缺少 Session")
	}

	t0 = time.Now()
	resp, err = c.request("PLAY", c.cfg.baseURL, hdr{"Session", c.session})
	c.playLat = time.Since(t0)
	if err != nil {
		return fmt.Errorf("PLAY: %w", err)
	}
	if !isOK(resp) {
		return fmt.Errorf("PLAY: %s", resp.status)
	}
	c.playStart = time.Now()
	c.stopAt = c.playStart.Add(c.cfg.duration)
	return nil
}

// setupTransport 返回 SETUP 的 Transport 头；UDP 模式先绑好收流端口。
func (c *client) setupTransport() (string, error) {
	if c.cfg.transport == "tcp" {
		return "RTP/AVP/TCP;unicast;interleaved=0-1", nil
	}
	rtpConn, err := net.ListenUDP("udp4", &net.UDPAddr{})
	if err != nil {
		return "", fmt.Errorf("bind rtp: %w", err)
	}
	rtcpConn, err := net.ListenUDP("udp4", &net.UDPAddr{})
	if err != nil {
		rtpConn.Close()
		return "", fmt.Errorf("bind rtcp: %w", err)
	}
	c.rtpConn, c.rtcpConn = rtpConn, rtcpConn
	rtpPort := rtpConn.LocalAddr().(*net.UDPAddr).Port
	rtcpPort := rtcpConn.LocalAddr().(*net.UDPAddr).Port
	return fmt.Sprintf("RTP/AVP;unicast;client_port=%d-%d", rtpPort, rtcpPort), nil
}

func (c *client) stream() {
	if c.cfg.transport == "udp" {
		c.streamUDP()
		return
	}
	c.streamTCP()
}

func (c *client) streamTCP() {
	nextKeepalive := time.Now().Add(keepaliveInterval)
	for {
		now := time.Now()
		if !now.Before(c.stopAt) {
			return
		}
		if !now.Before(nextKeepalive) {
			if err := c.keepalive(); err != nil {
				c.err = fmt.Errorf("client %d: keepalive: %w", c.id, err)
				return
			}
			nextKeepalive = now.Add(keepaliveInterval)
		}
		c.conn.SetReadDeadline(deadlineAt(now, c.stopAt))
		if _, err := c.readTCPMessage(); err != nil {
			if isTimeout(err) {
				continue
			}
			c.err = fmt.Errorf("client %d: read: %w", c.id, err)
			return
		}
	}
}

func (c *client) streamUDP() {
	nextKeepalive := time.Now().Add(keepaliveInterval)
	buf := make([]byte, 4096)
	for {
		now := time.Now()
		if !now.Before(c.stopAt) {
			return
		}
		if !now.Before(nextKeepalive) {
			if err := c.keepalive(); err != nil {
				c.err = fmt.Errorf("client %d: keepalive: %w", c.id, err)
				return
			}
			nextKeepalive = now.Add(keepaliveInterval)
		}
		c.rtpConn.SetReadDeadline(deadlineAt(now, c.stopAt))
		n, err := c.rtpConn.Read(buf)
		if err != nil {
			if isTimeout(err) {
				continue
			}
			c.err = fmt.Errorf("client %d: read rtp: %w", c.id, err)
			return
		}
		c.recordRTP(buf[:n])
	}
}

func deadlineAt(now, stop time.Time) time.Time {
	d := now.Add(time.Second)
	if d.After(stop) {
		return stop
	}
	return d
}

func (c *client) keepalive() error {
	resp, err := c.request("GET_PARAMETER", c.cfg.baseURL, hdr{"Session", c.session})
	if err != nil {
		return err
	}
	if !isOK(resp) {
		return fmt.Errorf("%s", resp.status)
	}
	return nil
}

func (c *client) recordRTP(pkt []byte) {
	if len(pkt) < 12 || pkt[0]&0xC0 != 0x80 {
		return
	}
	seq := binary.BigEndian.Uint16(pkt[2:4])
	gap := c.tracker.observe(seq)
	c.packets++
	c.bytes += uint64(len(pkt))
	c.gaps += gap
	now := time.Now()
	c.lastPacket = now

	c.agg.packets.Add(1)
	c.agg.bytes.Add(uint64(len(pkt)))
	if gap > 0 {
		c.agg.gaps.Add(gap)
	}
}

func (c *client) close() {
	if c.conn == nil {
		return
	}
	if c.session != "" {
		c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, _ = c.request("TEARDOWN", c.cfg.baseURL, hdr{"Session", c.session})
	}
	if c.rtpConn != nil {
		c.rtpConn.Close()
	}
	if c.rtcpConn != nil {
		c.rtcpConn.Close()
	}
	c.conn.Close()
}

// --- 最小 RTSP 客户端 ---

type hdr struct{ key, value string }

type rtspResponse struct {
	status  string
	headers map[string]string
	body    []byte
}

func (c *client) request(method, uri string, headers ...hdr) (*rtspResponse, error) {
	c.cseq++
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s RTSP/1.0\r\nCSeq: %d\r\n", method, uri, c.cseq)
	for _, h := range headers {
		fmt.Fprintf(&b, "%s: %s\r\n", h.key, h.value)
	}
	b.WriteString("\r\n")

	c.conn.SetWriteDeadline(time.Now().Add(c.cfg.timeout))
	if _, err := io.WriteString(c.conn, b.String()); err != nil {
		return nil, err
	}
	c.conn.SetReadDeadline(time.Now().Add(c.cfg.timeout))
	for {
		resp, err := c.readTCPMessage()
		if err != nil {
			return nil, err
		}
		if resp != nil {
			return resp, nil
		}
	}
}

// readTCPMessage 读一条消息：interleaved 帧计数后返回 (nil, nil)，
// RTSP 响应返回 (resp, nil)。每次调用只处理一条消息，调用方才能在
// 媒体连续到达时及时检查停止时间和 keepalive。
// SETUP/PLAY 响应与媒体可能交错到达，所以 request 里要循环到响应为止。
func (c *client) readTCPMessage() (*rtspResponse, error) {
	first, err := c.br.Peek(1)
	if err != nil {
		return nil, err
	}
	if first[0] != '$' {
		return c.readResponse()
	}
	var head [4]byte
	if _, err := io.ReadFull(c.br, head[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint16(head[2:4]))
	payload := make([]byte, n)
	if _, err := io.ReadFull(c.br, payload); err != nil {
		return nil, err
	}
	if head[1] == 0 { // channel 0 = RTP；channel 1 是 RTCP，忽略
		c.recordRTP(payload)
	}
	return nil, nil
}

func (c *client) readResponse() (*rtspResponse, error) {
	line, err := c.br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "RTSP/") {
		return nil, fmt.Errorf("非法状态行 %q", line)
	}
	resp := &rtspResponse{status: line, headers: make(map[string]string)}
	for {
		raw, err := c.br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		raw = strings.TrimRight(raw, "\r\n")
		if raw == "" {
			break
		}
		key, value, ok := strings.Cut(raw, ":")
		if !ok {
			continue
		}
		resp.headers[strings.ToLower(strings.TrimSpace(key))] = strings.TrimSpace(value)
	}
	if cl := resp.headers["content-length"]; cl != "" {
		n, err := strconv.Atoi(cl)
		if err != nil {
			return nil, fmt.Errorf("Content-Length %q: %w", cl, err)
		}
		resp.body = make([]byte, n)
		if _, err := io.ReadFull(c.br, resp.body); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func isOK(resp *rtspResponse) bool {
	return strings.HasPrefix(resp.status, "RTSP/1.0 200")
}

func sessionValue(v string) string {
	if i := strings.IndexByte(v, ';'); i >= 0 {
		return v[:i]
	}
	return v
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// --- 序号空洞统计 ---

// seqTracker 用 16 位序号回绕差分统计 RTP 丢包。
// 只把序号前进视为新包；乱序/重复不计为丢包，避免 UDP 轻微乱序导致负丢包。
type seqTracker struct {
	started bool
	last    uint16
}

// observe 返回本次观察新增的空洞数。
func (t *seqTracker) observe(seq uint16) uint64 {
	if !t.started {
		t.started = true
		t.last = seq
		return 0
	}
	delta := int16(seq - t.last)
	if delta <= 0 {
		return 0
	}
	t.last = seq
	if delta > 1 {
		return uint64(delta - 1)
	}
	return 0
}

// --- 进度与汇总 ---

type snapshot struct {
	packets, bytes, gaps uint64
}

func reportLoop(agg *counters, start time.Time, cfg config, done <-chan struct{}) {
	ticker := time.NewTicker(cfg.reportEvery)
	defer ticker.Stop()
	prev := snapshot{}
	prevAt := time.Now()
	for {
		select {
		case <-done:
			return
		case now := <-ticker.C:
			cur := snapshot{
				packets: agg.packets.Load(),
				bytes:   agg.bytes.Load(),
				gaps:    agg.gaps.Load(),
			}
			dt := now.Sub(prevAt).Seconds()
			pps := float64(cur.packets-prev.packets) / dt
			mbps := float64(cur.bytes-prev.bytes) / dt / (1 << 20)
			fmt.Printf("[%6.0fs] established=%d/%d active=%d pkt=%d (%.0f/s) %.2f MiB/s loss=%d (%.3f%%)\n",
				now.Sub(start).Seconds(), agg.established.Load(), cfg.clients,
				agg.active.Load(), cur.packets, pps, mbps, cur.gaps, lossRate(cur.packets, cur.gaps))
			prev, prevAt = cur, now
		}
	}
}

func summarize(clients []*client, elapsed time.Duration, cfg config) int {
	var (
		established  int
		totalPackets uint64
		totalBytes   uint64
		totalGaps    uint64
		minPackets   uint64 = math.MaxUint64
		failures     []string
		connectLat   []time.Duration
		describeLat  []time.Duration
		setupLat     []time.Duration
		playLat      []time.Duration
	)
	for _, c := range clients {
		if c.err != nil {
			failures = append(failures, c.err.Error())
			continue
		}
		established++
		totalPackets += c.packets
		totalBytes += c.bytes
		totalGaps += c.gaps
		if c.packets < minPackets {
			minPackets = c.packets
		}
		connectLat = append(connectLat, c.connectLat)
		describeLat = append(describeLat, c.describeLat)
		setupLat = append(setupLat, c.setupLat)
		playLat = append(playLat, c.playLat)

		if c.packets == 0 {
			failures = append(failures, fmt.Sprintf("client %d: 未收到任何 RTP 包", c.id))
			continue
		}
		if c.lastPacket.Before(c.stopAt.Add(-5 * time.Second)) {
			failures = append(failures, fmt.Sprintf("client %d: 最后收包于 %s，疑似卡流",
				c.id, c.lastPacket.Format("15:04:05")))
		}
	}
	if established != cfg.clients {
		failures = append(failures, fmt.Sprintf("建立成功 %d/%d", established, cfg.clients))
	}
	rate := lossRate(totalPackets, totalGaps)
	if rate > cfg.maxLoss*100 {
		failures = append(failures, fmt.Sprintf("丢包率 %.3f%% 超过阈值 %.3f%%", rate, cfg.maxLoss*100))
	}

	fmt.Println("\n=== E-1 压测结果 ===")
	fmt.Printf("目标        %s\n", cfg.baseURL)
	fmt.Printf("并发/时长   %d 路 / %s（实际 %s），传输 %s\n",
		cfg.clients, cfg.duration, elapsed.Round(time.Second), strings.ToUpper(cfg.transport))
	fmt.Printf("建立成功    %d/%d\n", established, cfg.clients)
	fmt.Printf("收包        %d（%.0f pkt/s），%.2f MiB，序号空洞 %d（%.3f%%）\n",
		totalPackets, float64(totalPackets)/elapsed.Seconds(),
		float64(totalBytes)/(1<<20), totalGaps, rate)
	if established > 0 {
		fmt.Printf("单客户端包数 最少 %d，平均 %d\n", minPackets, totalPackets/uint64(established))
	}
	fmt.Printf("建连耗时    %s\n", fmtLatencies(connectLat))
	fmt.Printf("DESCRIBE    %s\n", fmtLatencies(describeLat))
	fmt.Printf("SETUP       %s\n", fmtLatencies(setupLat))
	fmt.Printf("PLAY        %s\n", fmtLatencies(playLat))

	if len(failures) > 0 {
		fmt.Printf("\n判定        FAIL（%d 项）\n", len(failures))
		for _, f := range failures {
			fmt.Println("  -", f)
		}
		return 1
	}
	fmt.Println("\n判定        PASS：全部会话稳定收流，未超丢包阈值")
	return 0
}

func lossRate(packets, gaps uint64) float64 {
	total := packets + gaps
	if total == 0 {
		return 0
	}
	return float64(gaps) / float64(total) * 100
}

func fmtLatencies(ds []time.Duration) string {
	if len(ds) == 0 {
		return "n/a"
	}
	return fmt.Sprintf("p50=%s p95=%s max=%s",
		percentile(ds, 50), percentile(ds, 95), percentile(ds, 100))
}

func percentile(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), ds...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

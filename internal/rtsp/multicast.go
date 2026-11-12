package rtsp

import (
	"fmt"
	"net"
	"syscall"
)

// RTSP over UDP 组播（RFC 2326 §12.39）。
//
// 客户端在 SETUP 里带 Transport: RTP/AVP;multicast，服务端不按客户端开一对
// UDP socket（unicast 那样），而是把 RTP/RTCP 发到固定的组播地址:端口，
// 订阅同一个流的客户端共享这一份数据。DESCRIBE 的 SDP 里带 a=destination /
// a=ttl 宣告组播参数，SETUP 响应里再带 destination/port/ttl，ffmpeg/ffplay
// 读到后加入对应组播组。
//
// Windows 防火墙默认不放行来自网卡的组播入站流量，但本机回环（loopback）
// 组播不受影响，所以验收用 239.255.0.1 本机收发；跨主机组播需要 TTL>1 并
// 放行防火墙规则。

const (
	// defaultMulticastAddr 默认组播组地址。239.255.0.1 是本地作用域的
	// 组播地址，回环组播本机即可收发，跨网段需要 TTL>1 和防火墙放行。
	defaultMulticastAddr = "239.255.0.1"
	// defaultMulticastTTL 默认组播 TTL。1 表示只到本网段，回环足够。
	defaultMulticastTTL = 1

	// 视频/音频各占一对固定的组播端口（RTP 偶数 + RTCP 奇数）。
	multicastVideoRTPPort = 5000
	multicastAudioRTPPort = 5002
)

// multicastPortsFor 返回 kind 对应的组播 RTP/RTCP 端口。
func multicastPortsFor(kind string) (rtpPort, rtcpPort int) {
	if kind == "audio" {
		return multicastAudioRTPPort, multicastAudioRTPPort + 1
	}
	return multicastVideoRTPPort, multicastVideoRTPPort + 1
}

// openMulticastSender 打开一个用于发送组播的 UDP socket，并设置 TTL 与回环。
// socket 只绑定任意本地端口用来发，不绑定组播端口（那是接收端做的事）。
func openMulticastSender(ttl int) (*net.UDPConn, error) {
	conn, err := net.ListenUDP("udp4", nil)
	if err != nil {
		return nil, err
	}
	if err := setMulticastSocketOpts(conn, ttl); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// setMulticastSocketOpts 在 socket 上设置组播 TTL 和回环开关。
// 回环必须打开，否则本机收不到自己发往 239.255.0.1 的包。
func setMulticastSocketOpts(conn *net.UDPConn, ttl int) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	var optErr error
	if err := raw.Control(func(fd uintptr) {
		h := syscall.Handle(fd)
		if e := syscall.SetsockoptInt(h, syscall.IPPROTO_IP, syscall.IP_MULTICAST_TTL, ttl); e != nil {
			optErr = fmt.Errorf("set IP_MULTICAST_TTL: %w", e)
			return
		}
		if e := syscall.SetsockoptInt(h, syscall.IPPROTO_IP, syscall.IP_MULTICAST_LOOP, 1); e != nil {
			optErr = fmt.Errorf("set IP_MULTICAST_LOOP: %w", e)
			return
		}
	}); err != nil {
		return err
	}
	return optErr
}

// multicastSDPAttrs 生成媒体级组播属性 a=destination / a=ttl。
// addr 为空（组播关闭）时返回空串。
func multicastSDPAttrs(addr string, ttl int) string {
	if addr == "" {
		return ""
	}
	return fmt.Sprintf("a=destination:%s\r\na=ttl:%d\r\n", addr, ttl)
}

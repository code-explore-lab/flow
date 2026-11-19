package rtmp

// O-3：推/拉鉴权 HTTP hook 接入。
//
// 静态 token / app 白名单（T-3）仍然先行校验；通过后（或未配置静态
// 鉴权时）再把 publish / play 交给外部鉴权服务。token 继续从流名的
// 查询串取，传给 hook 之前不打印。

import (
	"context"
	"fmt"
	"net"

	"flow/internal/authhook"
)

// checkAuthHook 调用外部鉴权服务。未配置时恒为放行。
// err 非空表示本次结果来自超时/故障的默认策略，调用方应记录日志。
func (c *Conn) checkAuthHook(action, rawName string) (bool, error) {
	if c.srv == nil || !c.srv.authHook.Enabled() {
		return true, nil
	}
	token := tokenFromQuery(rawName)
	if token == "" {
		// 与 T-3 一致：少数客户端把 token 放在 app 的查询串里。
		token = tokenFromQuery(c.app)
	}
	return c.srv.authHook.Authorize(context.Background(), authhook.Request{
		Action:   action,
		Protocol: "rtmp",
		App:      appBase(c.app),
		Stream:   sanitizeStreamName(rawName),
		IP:       c.remoteIP(),
		Token:    token,
		Method:   action,
	})
}

// remoteIP 取连接的来源 IP；net.Pipe 等非 TCP 连接退回原始地址字符串。
func (c *Conn) remoteIP() string {
	addr := c.RemoteAddr()
	if tcp, ok := addr.(*net.TCPAddr); ok {
		return tcp.IP.String()
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
}

// logRTMPAuthHook 统一日志格式：只打动作、app、流名、IP 和结果，不打 token。
func logRTMPAuthHook(action, rawName, app, ip string, allowed bool, err error) {
	name := sanitizeStreamName(rawName)
	switch {
	case err != nil && allowed:
		fmt.Printf("rtmp auth hook: %s %s/%s from %s: %v (fallback allow)\n",
			action, appBase(app), name, ip, err)
	case err != nil:
		fmt.Printf("rtmp auth hook: %s %s/%s from %s: %v (fallback deny)\n",
			action, appBase(app), name, ip, err)
	case !allowed:
		fmt.Printf("rtmp auth hook denied: %s %s/%s from %s\n",
			action, appBase(app), name, ip)
	}
}

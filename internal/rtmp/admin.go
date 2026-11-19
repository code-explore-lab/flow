package rtmp

// O-1：管理接口需要的连接查询与踢除能力。实现 internal/admin.Backend。
//
// RTMP 以连接为会话单位（一条 TCP 连接上 publish/play 一个流）。踢连接
// 走已有的关闭路径：Conn.Close → handleConn 的 defer → stopPublish/
// stopPlay → registry.Unpublish，下游 reader 随即读到流结束。

import (
	"fmt"
	"net"
	"strings"

	"flow/internal/admin"
)

const (
	statePublishing = "publishing"
	statePlaying    = "playing"
	stateConnected  = "connected"
)

// Protocol 实现 admin.Backend。
func (s *Server) Protocol() string { return "rtmp" }

// Sessions 返回当前 RTMP 连接快照。
func (s *Server) Sessions() []admin.SessionInfo {
	s.connsMu.Lock()
	conns := make([]*Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.connsMu.Unlock()

	out := make([]admin.SessionInfo, 0, len(conns))
	for _, c := range conns {
		_, stream, publishing, playing := c.adminSnapshot()
		info := admin.SessionInfo{
			ID:         c.id,
			Protocol:   "rtmp",
			Remote:     remoteHost(c.RemoteAddr()),
			State:      stateConnected,
			Direction:  admin.DirectionIdle,
			Stream:     stream,
			CreatedAt:  c.createdAt,
			LastActive: c.lastActivity(),
		}
		switch {
		case publishing:
			info.State = statePublishing
			info.Direction = admin.DirectionPublish
		case playing:
			info.State = statePlaying
			info.Direction = admin.DirectionPlay
		}
		info.Stream = strings.Trim(info.Stream, "/")
		out = append(out, info)
	}
	return out
}

// KickSession 关闭指定 RTMP 连接。不存在时返回 (false, nil)。
func (s *Server) KickSession(id string) (bool, error) {
	s.connsMu.Lock()
	var target *Conn
	for c := range s.conns {
		if c.id == id {
			target = c
			break
		}
	}
	s.connsMu.Unlock()
	if target == nil {
		return false, nil
	}
	target.Close()
	fmt.Printf("admin: kicked rtmp connection %s (%s)\n", id, target.RemoteAddr())
	return true, nil
}

// KickStream 关闭发布该流名的 RTMP 连接，返回连接数。
func (s *Server) KickStream(name string) (int, error) {
	name = strings.Trim(name, "/")
	if name == "" {
		return 0, nil
	}
	s.connsMu.Lock()
	conns := make([]*Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.connsMu.Unlock()

	kicked := 0
	for _, c := range conns {
		_, stream, publishing, _ := c.adminSnapshot()
		if !publishing || strings.Trim(stream, "/") != name {
			continue
		}
		c.Close()
		kicked++
	}
	return kicked, nil
}

// remoteHost 从地址里取出 IP，解析失败时保留原串。
func remoteHost(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
}

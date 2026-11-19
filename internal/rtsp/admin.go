package rtsp

// O-1：管理接口需要的会话查询与踢除能力。实现 internal/admin.Backend。
//
// 踢除复用协议已有的回收路径：
//   - 推流会话：Session.Close → closeAll → unpublish → 关闭 ExternalSource，
//     拉流端 reader 读到流结束（P1-9）；
//   - 拉流会话：closeAll 发 BYE、关 UDP socket，再按需关闭 TCP 连接。
//
// 两个入口都不直接操作数据面，闭连接后由各自的 goroutine 完成收尾。

import (
	"fmt"
	"strings"

	"flow/internal/admin"
	"flow/internal/session"
)

// Protocol 实现 admin.Backend。
func (s *Server) Protocol() string { return "rtsp" }

// Sessions 返回当前 RTSP 会话快照。
func (s *Server) Sessions() []admin.SessionInfo {
	var out []admin.SessionInfo
	s.sessions.Range(func(sess *session.Session) bool {
		info := admin.SessionInfo{
			ID:         sess.ID,
			Protocol:   "rtsp",
			Remote:     sess.RemoteIP,
			State:      sess.State().String(),
			Direction:  admin.DirectionIdle,
			CreatedAt:  sess.CreatedAt,
			LastActive: sess.LastActivity(),
		}
		if st, ok := sess.Data.(*Session); ok {
			switch {
			case st.isPush():
				info.Direction = admin.DirectionPublish
			case sess.State() == session.StatePlaying, sess.State() == session.StatePaused:
				info.Direction = admin.DirectionPlay
			}
			if pub := st.published(); pub != nil {
				info.Stream = pub.Name
			}
		}
		out = append(out, info)
		return true
	})
	return out
}

// KickSession 关闭指定 RTSP 会话并断开它独占的控制连接。
// 不存在时返回 (false, nil)，与管理接口的 404 对应。
func (s *Server) KickSession(id string) (bool, error) {
	sess, ok := s.sessions.Get(id)
	if !ok {
		return false, nil
	}
	st, _ := sess.Data.(*Session)
	s.sessions.Delete(id)
	sess.Close()
	if st != nil && st.conn != nil {
		s.closeConnIfIdle(st.conn, id)
	}
	fmt.Printf("admin: kicked rtsp session %s\n", id)
	return true, nil
}

// KickStream 踢掉发布该流名的 RTSP 推流会话，返回会话数。
// 只匹配 ANNOUNCE 建立的推流会话；拉流会话订阅同一个源但不应被当成发布者。
func (s *Server) KickStream(name string) (int, error) {
	name = strings.Trim(name, "/")
	if name == "" {
		return 0, nil
	}
	var ids []string
	s.sessions.Range(func(sess *session.Session) bool {
		st, ok := sess.Data.(*Session)
		if !ok || !st.isPush() {
			return true
		}
		if pub := st.published(); pub != nil && pub.Name == name {
			ids = append(ids, sess.ID)
		}
		return true
	})
	kicked := 0
	for _, id := range ids {
		if ok, _ := s.KickSession(id); ok {
			kicked++
		}
	}
	return kicked, nil
}

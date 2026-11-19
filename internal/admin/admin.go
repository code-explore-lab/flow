// Package admin 提供 O-1 的 HTTP 管理接口：查询流/会话、踢掉推流或会话。
//
// 设计上只依赖 stream.Registry 和一个小接口 Backend；RTSP / RTMP 服务器
// 各自实现 Backend，本包不反向依赖协议实现，避免 import 环。命令本身
// 复用各协议已有的回收路径，因此「踢流」会自然触发下游读到流结束。
//
// 路由（全部返回 JSON）：
//
//	GET    /api/v1/streams          当前注册表里的流 + 发布者
//	GET    /api/v1/streams/<name>   单条流
//	DELETE /api/v1/streams/<name>   踢掉该流的网络推流会话
//	GET    /api/v1/sessions         所有协议会话/连接
//	DELETE /api/v1/sessions/<id>    踢掉一个会话
//	GET    /api/v1/config           脱敏后的运行时配置
//
// 管理接口能中断推流，默认只应监听回环地址；cmd/server 在未配置 token
// 时强制回环，非回环监听必须同时设置 bearer token。
package admin

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"maps"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"flow/internal/stream"
)

// 会话方向常量，Backend 实现的 Sessions 用来标注一条会话在做什么。
const (
	DirectionPublish = "publish"
	DirectionPlay    = "play"
	DirectionIdle    = "idle"
)

// TrackInfo 管理接口展示用的轨道编码信息。
type TrackInfo struct {
	Kind  string `json:"kind"`
	Codec string `json:"codec,omitempty"`
	Rate  uint32 `json:"sampleRate,omitempty"`
	Chans uint8  `json:"channels,omitempty"`
}

// PublisherInfo 一条流的网络发布者，踢流时按 Name 匹配。
type PublisherInfo struct {
	Protocol string `json:"protocol"`
	Session  string `json:"session"`
	Remote   string `json:"remote"`
}

// StreamInfo 一条已发布流。
type StreamInfo struct {
	Name       string          `json:"name"`
	Video      *TrackInfo      `json:"video,omitempty"`
	Audio      *TrackInfo      `json:"audio,omitempty"`
	CreatedAt  time.Time       `json:"createdAt"`
	LastActive time.Time       `json:"lastActive"`
	Active     bool            `json:"active"`
	Publishers []PublisherInfo `json:"publishers"`
}

// SessionInfo 一条协议会话（RTSP session 或 RTMP 连接）。
type SessionInfo struct {
	ID         string    `json:"id"`
	Protocol   string    `json:"protocol"`
	Remote     string    `json:"remote"`
	State      string    `json:"state"`
	Direction  string    `json:"direction"`
	Stream     string    `json:"stream,omitempty"`
	CreatedAt  time.Time `json:"createdAt"`
	LastActive time.Time `json:"lastActive"`
}

// Backend 一个协议服务器暴露给管理接口的能力。
//
// KickSession 返回 (found, error)；KickStream 返回被踢掉的会话数。
// 两个方法都必须是非阻塞的：关闭连接后立即返回，由各自的读循环完成回收。
type Backend interface {
	Protocol() string
	Sessions() []SessionInfo
	KickSession(id string) (bool, error)
	KickStream(name string) (int, error)
}

// Server 管理 HTTP 服务。
type Server struct {
	reg      *stream.Registry
	backends []Backend
	token    string
	config   map[string]string
	started  time.Time
	handler  http.Handler
}

// New 构造管理服务。reg 为 nil 时自建一份；backends 可以为空。
func New(reg *stream.Registry, backends ...Backend) *Server {
	if reg == nil {
		reg = stream.NewRegistry()
	}
	s := &Server{
		reg:      reg,
		backends: backends,
		started:  time.Now(),
	}
	s.handler = s.routes()
	return s
}

// SetToken 设置 bearer token。空字符串表示不校验（仅用于回环监听）。
func (s *Server) SetToken(token string) { s.token = token }

// SetConfig 设置 /api/v1/config 返回的键值。调用方负责脱敏。
func (s *Server) SetConfig(m map[string]string) { s.config = m }

// Serve 启动 HTTP 服务，ctx 取消时关闭监听。
func (s *Server) Serve(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler:           s,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	err = srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// ServeHTTP 校验 token 后分发到具体路由。
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="flow-admin"`)
		writeError(w, http.StatusUnauthorized, "invalid or missing token")
		return
	}
	s.handler.ServeHTTP(w, r)
}

func (s *Server) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/streams", s.handleStreams)
	mux.HandleFunc("GET /api/v1/streams/{name...}", s.handleStream)
	mux.HandleFunc("DELETE /api/v1/streams/{name...}", s.handleKickStream)
	mux.HandleFunc("GET /api/v1/sessions", s.handleSessions)
	mux.HandleFunc("DELETE /api/v1/sessions/{id}", s.handleKickSession)
	mux.HandleFunc("GET /api/v1/config", s.handleConfig)
	return mux
}

// authorized 校验 Authorization: Bearer / X-Flow-Token。
// token 为空时直接放行；比较用常量时间避免逐字节泄露。
func (s *Server) authorized(r *http.Request) bool {
	if s.token == "" {
		return true
	}
	got := r.Header.Get("X-Flow-Token")
	if auth := r.Header.Get("Authorization"); auth != "" {
		scheme, value, ok := strings.Cut(auth, " ")
		if ok && strings.EqualFold(scheme, "Bearer") {
			got = strings.TrimSpace(value)
		}
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.token)) == 1
}

// handleStreams 列出注册表里的全部流，并挂上各协议报告的发布者。
func (s *Server) handleStreams(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"streams": s.streams()})
}

// handleStream 返回单条流；不存在时 404。
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	name := cleanName(r.PathValue("name"))
	st := s.reg.Get(name)
	if st == nil {
		writeError(w, http.StatusNotFound, "stream not found")
		return
	}
	writeJSON(w, http.StatusOK, s.streamInfo(name, st, s.publishers(name)))
}

// handleKickStream 踢掉指定流的所有网络推流会话（RTSP ANNOUNCE / RTMP publish）。
//
//	200  已踢掉，body 带 kicked 数量
//	404  注册表里没有这条流
//	409  流存在但没有可踢的网络发布者（由 -ts-* / 转码 / 回源等配置驱动）
func (s *Server) handleKickStream(w http.ResponseWriter, r *http.Request) {
	name := cleanName(r.PathValue("name"))
	if name == "" {
		writeError(w, http.StatusBadRequest, "empty stream name")
		return
	}
	kicked := 0
	for _, b := range s.backends {
		n, err := b.KickStream(name)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		kicked += n
	}
	switch {
	case kicked > 0:
		writeJSON(w, http.StatusOK, map[string]any{"stream": name, "kicked": kicked})
	case s.reg.Get(name) != nil:
		writeError(w, http.StatusConflict,
			"stream has no network publisher (input is managed by configuration)")
	default:
		writeError(w, http.StatusNotFound, "stream not found")
	}
}

// handleSessions 列出所有协议会话；?protocol=rtsp|rtmp 可过滤。
func (s *Server) handleSessions(w http.ResponseWriter, r *http.Request) {
	protocol := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("protocol")))
	var out []SessionInfo
	for _, b := range s.backends {
		if protocol != "" && !strings.EqualFold(b.Protocol(), protocol) {
			continue
		}
		out = append(out, b.Sessions()...)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Protocol != out[j].Protocol {
			return out[i].Protocol < out[j].Protocol
		}
		return out[i].ID < out[j].ID
	})
	if out == nil {
		out = []SessionInfo{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessions": out})
}

// handleKickSession 踢掉一个会话：先在实现了该 ID 的协议里查找并关闭。
func (s *Server) handleKickSession(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeError(w, http.StatusBadRequest, "empty session id")
		return
	}
	for _, b := range s.backends {
		found, err := b.KickSession(id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if found {
			writeJSON(w, http.StatusOK, map[string]any{"session": id, "protocol": b.Protocol()})
			return
		}
	}
	writeError(w, http.StatusNotFound, "session not found")
}

// handleConfig 返回调用方设置过的脱敏配置和启动时间。
func (s *Server) handleConfig(w http.ResponseWriter, _ *http.Request) {
	cfg := make(map[string]string, len(s.config)+1)
	maps.Copy(cfg, s.config)
	cfg["startedAt"] = s.started.Format(time.RFC3339)
	writeJSON(w, http.StatusOK, map[string]any{"config": cfg})
}

// streams 把注册表快照和发布者信息拼成 API 视图。
func (s *Server) streams() []StreamInfo {
	snapshot := s.reg.Snapshot()
	out := make([]StreamInfo, 0, len(snapshot))
	for _, ns := range snapshot {
		out = append(out, s.streamInfo(ns.Name, ns.Stream, s.publishers(ns.Name)))
	}
	return out
}

func (s *Server) streamInfo(name string, st *stream.Stream, pubs []PublisherInfo) StreamInfo {
	info := StreamInfo{
		Name:       name,
		CreatedAt:  st.CreatedAt(),
		LastActive: st.LastActivity(),
		Active:     st.Active(stream.DefaultActiveWindow),
		Publishers: pubs,
	}
	if info.Publishers == nil {
		info.Publishers = []PublisherInfo{}
	}
	if st.Video != nil && st.Video.Video != nil {
		info.Video = &TrackInfo{Kind: "video", Codec: st.Video.Video.Codec}
	}
	if st.Audio != nil && st.Audio.Audio != nil {
		info.Audio = &TrackInfo{
			Kind:  "audio",
			Codec: st.Audio.Audio.Codec,
			Rate:  st.Audio.Audio.SampleRate,
			Chans: st.Audio.Audio.Channels,
		}
	}
	return info
}

// publishers 汇总各协议里正在发布该流名的会话。
func (s *Server) publishers(name string) []PublisherInfo {
	var out []PublisherInfo
	for _, b := range s.backends {
		for _, sess := range b.Sessions() {
			if sess.Direction == DirectionPublish && sess.Stream == name {
				out = append(out, PublisherInfo{
					Protocol: sess.Protocol,
					Session:  sess.ID,
					Remote:   sess.Remote,
				})
			}
		}
	}
	return out
}

// cleanName 去掉首尾斜杠，与 Registry 的流名格式保持一致。
func cleanName(name string) string {
	return strings.Trim(strings.TrimSpace(name), "/")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

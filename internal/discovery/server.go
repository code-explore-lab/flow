package discovery

// S-3 注册中心的 HTTP 接口。故意做成很小的 REST 集合，方便用 curl 调试，
// 也方便以后把 Store 换成 etcd/Redis 而保持节点侧协议不变。
//
//	GET    /v1/streams           列出所有有效租约
//	POST   /v1/streams           整批心跳（登记 + 续约 + 清理本节点旧条目）
//	GET    /v1/streams/<name>    查询一路流
//	DELETE /v1/streams/<name>    立即注销（?node=<节点 ID>）
//	GET    /healthz              存活探测

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

// maxBodyBytes 限制心跳请求体，防止畸形请求占用内存。
const maxBodyBytes = 1 << 20

// Server 是注册中心的 HTTP 服务。
type Server struct {
	store *Store
}

// NewServer 用给定的租约表构造服务。
func NewServer(store *Store) *Server {
	if store == nil {
		store = NewStore(DefaultTTL)
	}
	return &Server{store: store}
}

// Store 返回底层租约表（测试/运维用）。
func (s *Server) Store() *Store { return s.store }

// Handler 返回注册中心的 HTTP 路由。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "streams": len(s.store.List())})
	})
	mux.HandleFunc("/v1/streams", s.handleCollection)
	mux.HandleFunc("/v1/streams/", s.handleStream)
	return mux
}

type heartbeatRequest struct {
	Node       string   `json:"node"`
	TTLSeconds int      `json:"ttl_seconds,omitempty"`
	Streams    []Stream `json:"streams"`
}

type heartbeatResponse struct {
	Node string `json:"node"`
	BatchResult
}

func (s *Server) handleCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"streams": s.store.List()})
	case http.MethodPost:
		var req heartbeatRequest
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		if err := dec.Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
			return
		}
		if strings.TrimSpace(req.Node) == "" {
			writeError(w, http.StatusBadRequest, "node is required")
			return
		}
		ttl := time.Duration(req.TTLSeconds) * time.Second
		res := s.store.RegisterBatch(req.Node, req.Streams, ttl)
		if len(res.Conflicts) > 0 {
			fmt.Printf("discovery: node %q conflicts with live lease(s): %s\n",
				req.Node, strings.Join(res.Conflicts, ", "))
		}
		writeJSON(w, http.StatusOK, heartbeatResponse{Node: req.Node, BatchResult: res})
	default:
		w.Header().Set("Allow", "GET, POST")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	name := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/streams/"), "/")
	if name == "" {
		writeError(w, http.StatusBadRequest, "stream name is required")
		return
	}
	switch r.Method {
	case http.MethodGet:
		if r.URL.Query().Get("all") == "1" {
			entries := s.store.Candidates(name)
			if len(entries) == 0 {
				writeError(w, http.StatusNotFound, "stream not registered")
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"streams": entries})
			return
		}
		entry, ok := s.store.Lookup(name)
		if !ok {
			writeError(w, http.StatusNotFound, "stream not registered")
			return
		}
		writeJSON(w, http.StatusOK, entry)
	case http.MethodDelete:
		node := strings.TrimSpace(r.URL.Query().Get("node"))
		if node == "" {
			writeError(w, http.StatusBadRequest, "node query parameter is required")
			return
		}
		s.store.Unregister(node, name)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "GET, DELETE")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// Run 监听 addr 并在 ctx 取消时优雅关闭。
func (s *Server) Run(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	srv := &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	fmt.Printf("discovery registry listen on http://%s/ (ttl=%s)\n", addr, s.store.DefaultTTL())
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		fmt.Printf("discovery: write response: %v\n", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

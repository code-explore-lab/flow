package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultClientTimeout 是单次注册中心请求的上限。发现发生在 DESCRIBE
// 路径上，不能让客户端久等：超时就按未命中处理，继续走本地回源/404。
const DefaultClientTimeout = 2 * time.Second

// Client 是节点侧的注册中心客户端。
type Client struct {
	base string
	node string
	ttl  time.Duration
	hc   *http.Client
}

// NewClient 创建客户端。node 为空时用 "flow"；ttl <= 0 时用 DefaultTTL。
func NewClient(base, node string, ttl time.Duration) *Client {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	if strings.TrimSpace(node) == "" {
		node = "flow"
	}
	return &Client{
		base: strings.TrimRight(strings.TrimSpace(base), "/"),
		node: node,
		ttl:  ttl,
		hc:   &http.Client{Timeout: DefaultClientTimeout},
	}
}

// Node 返回上报使用的节点 ID。
func (c *Client) Node() string { return c.node }

// TTL 返回上报使用的租约时长。
func (c *Client) TTL() time.Duration { return c.ttl }

// SetTimeout 调整 HTTP 超时（测试用）。
func (c *Client) SetTimeout(d time.Duration) {
	if d > 0 {
		c.hc.Timeout = d
	}
}

// Lookup 查询一路流。未登记或租约过期返回 ok=false 和 nil error；
// 网络/服务端错误返回 error，调用方应按未命中处理。
func (c *Client) Lookup(ctx context.Context, name string) (Entry, bool, error) {
	name = strings.Trim(strings.TrimSpace(name), "/")
	if name == "" {
		return Entry{}, false, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.streamURL(name), nil)
	if err != nil {
		return Entry{}, false, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return Entry{}, false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		var e Entry
		if err := json.NewDecoder(io.LimitReader(resp.Body, maxBodyBytes)).Decode(&e); err != nil {
			return Entry{}, false, fmt.Errorf("discovery lookup %q: %w", name, err)
		}
		return e, true, nil
	case http.StatusNotFound:
		return Entry{}, false, nil
	default:
		return Entry{}, false, fmt.Errorf("discovery lookup %q: HTTP %d: %s",
			name, resp.StatusCode, readSnippet(resp.Body))
	}
}

// LookupAll 返回一路流的全部候选 provider（S-2 负载均衡/代理）。
// 未登记或租约过期返回 nil 和 nil error。
func (c *Client) LookupAll(ctx context.Context, name string) ([]Entry, error) {
	name = strings.Trim(strings.TrimSpace(name), "/")
	if name == "" {
		return nil, nil
	}
	u, err := url.Parse(c.streamURL(name))
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("all", "1")
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		var out struct {
			Streams []Entry `json:"streams"`
		}
		if err := json.NewDecoder(io.LimitReader(resp.Body, maxBodyBytes)).Decode(&out); err != nil {
			return nil, fmt.Errorf("discovery lookup all %q: %w", name, err)
		}
		return out.Streams, nil
	case http.StatusNotFound:
		return nil, nil
	default:
		return nil, fmt.Errorf("discovery lookup all %q: HTTP %d: %s",
			name, resp.StatusCode, readSnippet(resp.Body))
	}
}

// Heartbeat 发送一次整批心跳。streams 为空时表示本节点此刻没有任何
// 可广告的流，注册中心会清空该节点的租约。
func (c *Client) Heartbeat(ctx context.Context, streams []Stream) (BatchResult, error) {
	body, err := json.Marshal(heartbeatRequest{
		Node:       c.node,
		TTLSeconds: int(c.ttl / time.Second),
		Streams:    streams,
	})
	if err != nil {
		return BatchResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.base+"/v1/streams", bytes.NewReader(body))
	if err != nil {
		return BatchResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return BatchResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return BatchResult{}, fmt.Errorf("discovery heartbeat: HTTP %d: %s",
			resp.StatusCode, readSnippet(resp.Body))
	}
	var out heartbeatResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxBodyBytes)).Decode(&out); err != nil {
		return BatchResult{}, fmt.Errorf("discovery heartbeat: %w", err)
	}
	return out.BatchResult, nil
}

// Delete 立即注销一路流。注册中心按 node 校验归属，删除不存在的条目
// 也返回成功（幂等）。
func (c *Client) Delete(ctx context.Context, name string) error {
	name = strings.Trim(strings.TrimSpace(name), "/")
	if name == "" {
		return nil
	}
	u, err := url.Parse(c.streamURL(name))
	if err != nil {
		return err
	}
	q := u.Query()
	q.Set("node", c.node)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u.String(), nil)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("discovery delete %q: HTTP %d: %s",
			name, resp.StatusCode, readSnippet(resp.Body))
	}
	return nil
}

// streamURL 拼接 /v1/streams/<name>，保留流名里的斜杠（如 live/src）。
func (c *Client) streamURL(name string) string {
	u, err := url.Parse(c.base)
	if err != nil {
		// base 在 NewClient 时未校验；这里退回字符串拼接。
		return c.base + "/v1/streams/" + name
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/v1/streams/" + name
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

func readSnippet(r io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(r, 256))
	s := strings.TrimSpace(string(b))
	if s == "" {
		return "<empty>"
	}
	return s
}

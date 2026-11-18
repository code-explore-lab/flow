// Command registry 是 S-3 的流注册中心。
//
// 节点（flow 进程）配置 -discovery-url 后，会把本地正在出帧的流按 TTL
// 租约登记到这里，并周期性续约。边缘节点在本地没有某路流时查询这里，
// 命中后用 RTSP 302（D-1 Redirect）把客户端指到真正持有该流的节点。
//
//	registry -listen :7000 -ttl 15s
//	flow -discovery-url http://127.0.0.1:7000 \
//	     -node-id edge-a -advertise rtsp://127.0.0.1:8554
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"flow/internal/discovery"
)

func main() {
	listen := flag.String("listen", ":7000", "registry listen address")
	ttl := flag.Duration("ttl", discovery.DefaultTTL, "default lease TTL; nodes renew every TTL/3")
	sweep := flag.Duration("sweep", 0, "expired lease sweep interval (default TTL/3, min 1s, max 5s)")
	flag.Parse()

	if *ttl <= 0 {
		fmt.Fprintln(os.Stderr, "invalid -ttl (must be > 0)")
		os.Exit(2)
	}
	interval := *sweep
	if interval <= 0 {
		interval = *ttl / 3
	}
	if interval < time.Second {
		interval = time.Second
	}
	if interval > 5*time.Second {
		interval = 5 * time.Second
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store := discovery.NewStore(*ttl)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if n := store.Sweep(); n > 0 {
					fmt.Printf("registry: expired %d lease(s)\n", n)
				}
			}
		}
	}()

	if err := discovery.NewServer(store).Run(ctx, *listen); err != nil {
		fmt.Fprintf(os.Stderr, "registry: %v\n", err)
		os.Exit(1)
	}
}

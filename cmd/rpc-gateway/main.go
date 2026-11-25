// Command rpc-gateway load-balances JSON-RPC traffic across blockchain nodes.
//
//	rpc-gateway -upstreams "alchemy=https://...,local=http://localhost:8545"
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Chandrasura25/rpc-gateway/internal/gateway"
)

func main() {
	listen := flag.String("listen", ":8545", "address to listen on")
	upstreams := flag.String("upstreams", os.Getenv("RPC_UPSTREAMS"), "comma-separated name=url pairs (or RPC_UPSTREAMS)")
	interval := flag.Duration("interval", 5*time.Second, "health check interval")
	maxLag := flag.Uint64("max-lag", 3, "max blocks behind the best node before a node is skipped")
	timeout := flag.Duration("timeout", 10*time.Second, "per-upstream request timeout")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	urls, err := parseUpstreams(*upstreams)
	if err != nil {
		log.Error("bad -upstreams", "err", err)
		os.Exit(2)
	}

	client := &http.Client{Timeout: *timeout}
	pool := gateway.NewPool(urls)
	checker := &gateway.Checker{Pool: pool, Client: client, Interval: *interval, Timeout: 3 * time.Second, MaxLag: *maxLag, Log: log}

	mux := http.NewServeMux()
	mux.Handle("POST /", &gateway.Proxy{Pool: pool, Client: client, Log: log})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if len(pool.Candidates()) == 0 {
			http.Error(w, "no healthy upstreams", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok"))
	})

	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	// Stop on Ctrl+C / SIGTERM: stop accepting, let in-flight requests finish.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go checker.Run(ctx)

	go func() {
		log.Info("listening", "addr", *listen, "upstreams", len(urls))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown", "err", err)
	}
}

func parseUpstreams(s string) (map[string]string, error) {
	out := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, url, ok := strings.Cut(part, "=")
		if !ok || name == "" || url == "" {
			return nil, errors.New("expected name=url, got " + part)
		}
		out[name] = url
	}
	if len(out) == 0 {
		return nil, errors.New("at least one upstream is required")
	}
	return out, nil
}

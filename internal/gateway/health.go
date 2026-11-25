package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Checker polls every upstream with eth_blockNumber. A node is healthy when it
// answers in time AND is no more than MaxLag blocks behind the best node, so a
// node that is up but stuck on an old block stops getting traffic.
type Checker struct {
	Pool     *Pool
	Client   *http.Client
	Interval time.Duration
	Timeout  time.Duration
	MaxLag   uint64
	Log      *slog.Logger
}

// Run checks once immediately, then on every tick, until ctx is cancelled.
func (c *Checker) Run(ctx context.Context) {
	c.CheckOnce(ctx)
	t := time.NewTicker(c.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.CheckOnce(ctx)
		}
	}
}

// CheckOnce queries all upstreams in parallel, then marks each healthy or not.
func (c *Checker) CheckOnce(ctx context.Context) {
	ups := c.Pool.Upstreams()
	heights := make([]uint64, len(ups))
	ok := make([]bool, len(ups))

	var wg sync.WaitGroup
	for i, u := range ups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h, err := c.blockNumber(ctx, u.URL)
			if err != nil {
				c.log().Warn("health check failed", "upstream", u.Name, "err", err)
				return
			}
			heights[i], ok[i] = h, true
		}()
	}
	wg.Wait()

	var best uint64
	for i := range ups {
		if ok[i] && heights[i] > best {
			best = heights[i]
		}
	}
	for i, u := range ups {
		healthy := ok[i] && best-heights[i] <= c.MaxLag
		if ok[i] {
			u.height.Store(heights[i])
		}
		if was := u.healthy.Swap(healthy); was != healthy {
			c.log().Info("upstream health changed", "upstream", u.Name, "healthy", healthy, "height", heights[i], "best", best)
		}
	}
}

func (c *Checker) blockNumber(ctx context.Context, url string) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d", resp.StatusCode)
	}
	var out struct {
		Result string          `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	if len(out.Error) > 0 && string(out.Error) != "null" {
		return 0, fmt.Errorf("rpc error: %s", out.Error)
	}
	return strconv.ParseUint(strings.TrimPrefix(out.Result, "0x"), 16, 64)
}

func (c *Checker) log() *slog.Logger {
	if c.Log != nil {
		return c.Log
	}
	return slog.Default()
}

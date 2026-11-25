package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Twenty clients ask for the same old block at once from a slow node: one call
// goes upstream, everyone gets the right answer with their own id.
func TestIdenticalRequestsInFlightShareOneUpstreamCall(t *testing.T) {
	n := newFakeNode(t, 1000)
	n.delay.Store(200)
	// Like a real node, echo the caller's id back.
	block := func(body string) string {
		var r struct {
			ID json.RawMessage `json:"id"`
		}
		json.Unmarshal([]byte(body), &r)
		return `{"jsonrpc":"2.0","id":` + string(r.ID) + `,"result":{"number":"0x64"}}`
	}
	n.reply.Store(&block)

	client := &http.Client{Timeout: 5 * time.Second}
	pool := NewPool(map[string]string{"node": n.srv.URL})
	(&Checker{Pool: pool, Client: client, Timeout: time.Second, MaxLag: 3}).CheckOnce(context.Background())
	m := &Metrics{}
	gw := httptest.NewServer(&Proxy{Pool: pool, Client: client, Cache: &Cache{MaxEntries: 10, FinalityDepth: 64}, Metrics: m})
	t.Cleanup(gw.Close)

	var wg sync.WaitGroup
	errs := make(chan string, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			resp, body := rpc(t, gw.URL, req(id, "eth_getBlockByNumber", `["0x64",false]`))
			if resp.StatusCode != 200 || !strings.Contains(body, `"number":"0x64"`) ||
				!strings.Contains(body, `"id":`+strconv.Itoa(id)) {
				errs <- body
			}
		}(i + 100)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Errorf("bad answer: %s", e)
	}
	if got := n.hits.Load(); got != 1 {
		t.Fatalf("want exactly 1 upstream call for 20 identical requests, got %d", got)
	}
}

// A follower must not be handed an answer that couldn't be cached: if the
// block is too recent, each request goes upstream itself.
func TestUncacheableResultsAreNotShared(t *testing.T) {
	n := newFakeNode(t, 1000)
	n.delay.Store(100)
	block := func(string) string { return `{"jsonrpc":"2.0","id":1,"result":{"number":"0x3e7"}}` }
	n.reply.Store(&block)
	client := &http.Client{Timeout: 5 * time.Second}
	pool := NewPool(map[string]string{"node": n.srv.URL})
	(&Checker{Pool: pool, Client: client, Timeout: time.Second, MaxLag: 3}).CheckOnce(context.Background())
	gw := httptest.NewServer(&Proxy{Pool: pool, Client: client, Cache: &Cache{MaxEntries: 10, FinalityDepth: 64}})
	t.Cleanup(gw.Close)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rpc(t, gw.URL, req(1, "eth_getBlockByNumber", `["0x3e7",false]`)) // 1 block below head
		}()
	}
	wg.Wait()
	if got := n.hits.Load(); got != 5 {
		t.Fatalf("recent block can't be cached, so all 5 must go upstream, got %d", got)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	good, bad := newFakeNode(t, 500), newFakeNode(t, 500)
	bad.broken.Store(true)
	chain := func(string) string { return `{"jsonrpc":"2.0","id":1,"result":"0x2105"}` }
	good.reply.Store(&chain)

	client := &http.Client{Timeout: 2 * time.Second}
	pool := NewPool(map[string]string{"good": good.srv.URL, "bad": bad.srv.URL})
	(&Checker{Pool: pool, Client: client, Timeout: time.Second, MaxLag: 3}).CheckOnce(context.Background())
	bad.broken.Store(false) // back up, but it failed the last check so it stays out until the next one
	m := &Metrics{}
	limiter := &RateLimiter{Rate: 0.001, Burst: 3, Metrics: m}
	gw := httptest.NewServer(limiter.Middleware(&Proxy{Pool: pool, Client: client, Cache: &Cache{MaxEntries: 10}, Metrics: m}))
	t.Cleanup(gw.Close)

	for i := 0; i < 4; i++ { // miss, hit, hit, then rate limited
		rpc(t, gw.URL, req(i, "eth_chainId", "[]"))
	}

	rec := httptest.NewRecorder()
	m.Handler(pool).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	out := rec.Body.String()
	for _, want := range []string{
		"# TYPE rpc_gateway_upstream_requests_total counter",
		`rpc_gateway_upstream_requests_total{upstream="good",outcome="ok"} 1`,
		`rpc_gateway_upstream_latency_seconds_count{upstream="good"} 1`,
		`rpc_gateway_upstream_latency_seconds_bucket{upstream="good",le="+Inf"} 1`,
		`rpc_gateway_upstream_healthy{upstream="bad"} 0`,
		`rpc_gateway_upstream_healthy{upstream="good"} 1`,
		`rpc_gateway_upstream_block_height{upstream="good"} 500`,
		"rpc_gateway_cache_hits_total 2",
		"rpc_gateway_cache_misses_total 1",
		"rpc_gateway_rate_limited_total 1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics missing %q\n%s", want, out)
		}
	}
}

func TestNilMetricsIsSafe(t *testing.T) {
	var m *Metrics
	m.CacheHit()
	m.CacheMiss()
	m.RateLimited()
	m.Coalesced()
	m.ObserveUpstream("x", "ok", time.Millisecond)
}

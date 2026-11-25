package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// cachedSetup: one fake node at block 1000, a cache with finality depth 64.
func cachedSetup(t *testing.T, reply func(string) string) (*fakeNode, *Cache, *httptest.Server) {
	t.Helper()
	n := newFakeNode(t, 1000)
	n.reply.Store(&reply)
	client := &http.Client{Timeout: 2 * time.Second}
	pool := NewPool(map[string]string{"node": n.srv.URL})
	(&Checker{Pool: pool, Client: client, Timeout: time.Second, MaxLag: 3}).CheckOnce(context.Background())
	cache := &Cache{MaxEntries: 100, FinalityDepth: 64}
	gw := httptest.NewServer(&Proxy{Pool: pool, Client: client, Cache: cache})
	t.Cleanup(gw.Close)
	return n, cache, gw
}

func rpc(t *testing.T, url, body string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func req(id int, method, params string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":%q,"params":%s}`, id, method, params)
}

func TestChainIdIsCachedAndKeepsCallersId(t *testing.T) {
	n, _, gw := cachedSetup(t, func(string) string { return `{"jsonrpc":"2.0","id":1,"result":"0x2105"}` })

	r1, _ := rpc(t, gw.URL, req(1, "eth_chainId", "[]"))
	r2, body := rpc(t, gw.URL, req(42, "eth_chainId", "[]"))
	if r1.Header.Get("X-Cache") != "MISS" || r2.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("want MISS then HIT, got %q then %q", r1.Header.Get("X-Cache"), r2.Header.Get("X-Cache"))
	}
	if n.hits.Load() != 1 {
		t.Fatalf("node should be called once, got %d", n.hits.Load())
	}
	if !strings.Contains(body, `"id":42`) || !strings.Contains(body, `"result":"0x2105"`) {
		t.Fatalf("cached answer must carry the caller's id: %s", body)
	}
}

func TestParamWhitespaceSharesOneEntry(t *testing.T) {
	n, _, gw := cachedSetup(t, func(string) string { return `{"jsonrpc":"2.0","id":1,"result":{"number":"0x1"}}` })
	rpc(t, gw.URL, req(1, "eth_getBlockByHash", `["0xabc",false]`))
	rpc(t, gw.URL, req(2, "eth_getBlockByHash", `[ "0xabc" , false ]`))
	if n.hits.Load() != 1 {
		t.Fatalf("same params with different spacing should hit the cache, node called %d times", n.hits.Load())
	}
}

func TestLatestAndRecentBlocksAreNotCached(t *testing.T) {
	n, cache, gw := cachedSetup(t, func(string) string { return `{"jsonrpc":"2.0","id":1,"result":{"number":"0x1"}}` })
	rpc(t, gw.URL, req(1, "eth_getBlockByNumber", `["latest",false]`))
	rpc(t, gw.URL, req(1, "eth_getBlockByNumber", `["latest",false]`))
	recent := fmt.Sprintf(`["0x%x",false]`, 1000-10) // 10 blocks deep, finality is 64
	rpc(t, gw.URL, req(1, "eth_getBlockByNumber", recent))
	rpc(t, gw.URL, req(1, "eth_getBlockByNumber", recent))
	if n.hits.Load() != 4 || cache.Len() != 0 {
		t.Fatalf("latest/recent blocks must always go upstream: hits=%d cached=%d", n.hits.Load(), cache.Len())
	}
}

func TestOldBlockIsCached(t *testing.T) {
	n, _, gw := cachedSetup(t, func(string) string { return `{"jsonrpc":"2.0","id":1,"result":{"number":"0x64"}}` })
	old := fmt.Sprintf(`["0x%x",false]`, 100) // 900 blocks deep
	rpc(t, gw.URL, req(1, "eth_getBlockByNumber", old))
	r, _ := rpc(t, gw.URL, req(1, "eth_getBlockByNumber", old))
	if r.Header.Get("X-Cache") != "HIT" || n.hits.Load() != 1 {
		t.Fatalf("finalized block should be cached: %q hits=%d", r.Header.Get("X-Cache"), n.hits.Load())
	}
}

func TestPendingOrRecentTransactionNotCachedButFinalizedIs(t *testing.T) {
	var answer string
	n, _, gw := cachedSetup(t, func(string) string { return answer })

	answer = `{"jsonrpc":"2.0","id":1,"result":{"hash":"0xt","blockNumber":null}}` // pending
	rpc(t, gw.URL, req(1, "eth_getTransactionByHash", `["0xt"]`))
	answer = fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":{"hash":"0xt","blockNumber":"0x%x"}}`, 995) // too recent
	rpc(t, gw.URL, req(1, "eth_getTransactionByHash", `["0xt"]`))
	answer = fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":{"hash":"0xt","blockNumber":"0x%x"}}`, 500) // final
	rpc(t, gw.URL, req(1, "eth_getTransactionByHash", `["0xt"]`))
	r, _ := rpc(t, gw.URL, req(1, "eth_getTransactionByHash", `["0xt"]`))
	if n.hits.Load() != 3 || r.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("want 3 upstream calls then a HIT, got hits=%d %q", n.hits.Load(), r.Header.Get("X-Cache"))
	}
}

func TestNullsAndErrorsAreNeverCached(t *testing.T) {
	var answer string
	_, cache, gw := cachedSetup(t, func(string) string { return answer })
	answer = `{"jsonrpc":"2.0","id":1,"result":null}`
	rpc(t, gw.URL, req(1, "eth_getBlockByHash", `["0xnotyet",false]`))
	answer = `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"header not found"}}`
	rpc(t, gw.URL, req(1, "eth_chainId", "[]"))
	if cache.Len() != 0 {
		t.Fatalf("nothing should be cached, got %d entries", cache.Len())
	}
}

func TestBatchRequestsPassThrough(t *testing.T) {
	n, cache, gw := cachedSetup(t, func(string) string { return `[{"jsonrpc":"2.0","id":1,"result":"0x2105"}]` })
	batch := `[` + req(1, "eth_chainId", "[]") + `]`
	rpc(t, gw.URL, batch)
	r, _ := rpc(t, gw.URL, batch)
	if n.hits.Load() != 2 || cache.Len() != 0 || r.Header.Get("X-Cache") != "" {
		t.Fatalf("batches should not touch the cache: hits=%d cached=%d", n.hits.Load(), cache.Len())
	}
}

// Found against real Base nodes: a free node without archive access refused an
// old block, and the error went straight back to the caller.
func TestNodeLimitationErrorFailsOverButCallerErrorDoesNot(t *testing.T) {
	pruned, archive := newFakeNode(t, 1000), newFakeNode(t, 1000)
	limited := func(string) string {
		return `{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"Archive requests require a personal token"}}`
	}
	full := func(string) string { return `{"jsonrpc":"2.0","id":1,"result":{"number":"0x1"}}` }
	pruned.reply.Store(&limited)
	archive.reply.Store(&full)
	s := newSetup(t, map[string]*fakeNode{"pruned": pruned, "archive": archive})
	for i := 0; i < 4; i++ {
		resp, body := post(t, s.gateway.URL)
		if resp.Header.Get("X-Upstream") != "archive" || !strings.Contains(body, `"result"`) {
			t.Fatalf("request %d should be served by the archive node: %q %s", i, resp.Header.Get("X-Upstream"), body)
		}
	}

	// A genuine caller error (e.g. a revert) is the answer, not a node problem.
	revert := func(string) string {
		return `{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted"}}`
	}
	only := newFakeNode(t, 1000)
	only.reply.Store(&revert)
	s2 := newSetup(t, map[string]*fakeNode{"only": only})
	resp, body := post(t, s2.gateway.URL)
	if resp.StatusCode != 200 || !strings.Contains(body, "execution reverted") {
		t.Fatalf("caller errors must pass through unchanged, got %d %s", resp.StatusCode, body)
	}
}

func TestLeastRecentlyUsedIsEvicted(t *testing.T) {
	c := &Cache{MaxEntries: 2, FinalityDepth: 64}
	ok := `{"jsonrpc":"2.0","id":1,"result":{"number":"0x1"}}`
	a := rpcRequest{Method: "eth_getBlockByHash", Params: []byte(`["0xa"]`)}
	b := rpcRequest{Method: "eth_getBlockByHash", Params: []byte(`["0xb"]`)}
	d := rpcRequest{Method: "eth_getBlockByHash", Params: []byte(`["0xd"]`)}
	c.MaybeStore(a, []byte(ok), 1000)
	c.MaybeStore(b, []byte(ok), 1000)
	c.Lookup(a) // a is now most recently used
	c.MaybeStore(d, []byte(ok), 1000)
	if _, hit := c.Lookup(b); hit {
		t.Fatal("b was least recently used and should be evicted")
	}
	if _, hit := c.Lookup(a); !hit {
		t.Fatal("a should still be cached")
	}
}

package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeNode is a stand-in RPC node whose behaviour a test can change mid-flight.
type fakeNode struct {
	srv    *httptest.Server
	height atomic.Uint64
	broken atomic.Bool // answer 500
	hits   atomic.Int64
}

func newFakeNode(t *testing.T, height uint64) *fakeNode {
	t.Helper()
	n := &fakeNode{}
	n.height.Store(height)
	n.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if n.broken.Load() {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		if strings.Contains(string(body), "eth_blockNumber") {
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":"0x%x"}`, n.height.Load())
			return
		}
		n.hits.Add(1)
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`)
	}))
	t.Cleanup(n.srv.Close)
	return n
}

type setup struct {
	pool    *Pool
	checker *Checker
	gateway *httptest.Server
}

func newSetup(t *testing.T, nodes map[string]*fakeNode) setup {
	t.Helper()
	urls := map[string]string{}
	for name, n := range nodes {
		urls[name] = n.srv.URL
	}
	client := &http.Client{Timeout: 2 * time.Second}
	pool := NewPool(urls)
	gw := httptest.NewServer(&Proxy{Pool: pool, Client: client})
	t.Cleanup(gw.Close)
	return setup{
		pool:    pool,
		checker: &Checker{Pool: pool, Client: client, Interval: time.Hour, Timeout: time.Second, MaxLag: 3},
		gateway: gw,
	}
}

const call = `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`

func post(t *testing.T, url string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(call))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func TestSpreadsTrafficAcrossHealthyNodes(t *testing.T) {
	a, b := newFakeNode(t, 100), newFakeNode(t, 100)
	s := newSetup(t, map[string]*fakeNode{"a": a, "b": b})
	for i := 0; i < 10; i++ {
		if resp, _ := post(t, s.gateway.URL); resp.StatusCode != 200 {
			t.Fatalf("status %d", resp.StatusCode)
		}
	}
	if a.hits.Load() != 5 || b.hits.Load() != 5 {
		t.Fatalf("want 5/5 round robin, got a=%d b=%d", a.hits.Load(), b.hits.Load())
	}
}

func TestFailsOverWhenANodeErrors(t *testing.T) {
	good, bad := newFakeNode(t, 100), newFakeNode(t, 100)
	bad.broken.Store(true) // health checker hasn't noticed yet: still marked healthy
	s := newSetup(t, map[string]*fakeNode{"good": good, "bad": bad})
	for i := 0; i < 6; i++ {
		resp, body := post(t, s.gateway.URL)
		if resp.StatusCode != 200 || resp.Header.Get("X-Upstream") != "good" {
			t.Fatalf("request %d: status %d upstream %q body %s", i, resp.StatusCode, resp.Header.Get("X-Upstream"), body)
		}
	}
}

func TestLaggingNodeIsSkipped(t *testing.T) {
	fresh, stale := newFakeNode(t, 1000), newFakeNode(t, 990) // 10 blocks behind, MaxLag is 3
	s := newSetup(t, map[string]*fakeNode{"fresh": fresh, "stale": stale})
	s.checker.CheckOnce(context.Background())
	for i := 0; i < 4; i++ {
		post(t, s.gateway.URL)
	}
	if stale.hits.Load() != 0 {
		t.Fatalf("stale node got %d requests", stale.hits.Load())
	}
	// It catches up and comes back.
	stale.height.Store(999)
	s.checker.CheckOnce(context.Background())
	for i := 0; i < 4; i++ {
		post(t, s.gateway.URL)
	}
	if stale.hits.Load() == 0 {
		t.Fatal("node that caught up should get traffic again")
	}
}

func TestDeadNodeMarkedUnhealthyThenRecovers(t *testing.T) {
	a, b := newFakeNode(t, 50), newFakeNode(t, 50)
	s := newSetup(t, map[string]*fakeNode{"a": a, "b": b})
	b.broken.Store(true)
	s.checker.CheckOnce(context.Background())
	if got := len(s.pool.Candidates()); got != 1 {
		t.Fatalf("want 1 healthy, got %d", got)
	}
	b.broken.Store(false)
	s.checker.CheckOnce(context.Background())
	if got := len(s.pool.Candidates()); got != 2 {
		t.Fatalf("want 2 healthy after recovery, got %d", got)
	}
}

func TestAllNodesDownReturnsJSONRPCError(t *testing.T) {
	a := newFakeNode(t, 1)
	a.broken.Store(true)
	s := newSetup(t, map[string]*fakeNode{"a": a})
	resp, body := post(t, s.gateway.URL)
	if resp.StatusCode != http.StatusBadGateway || !strings.Contains(body, `"code":-32603`) {
		t.Fatalf("want 502 with JSON-RPC error, got %d %s", resp.StatusCode, body)
	}
}

// Run with -race: requests and health checks hit the pool at the same time.
func TestConcurrentTrafficWhileHealthFlaps(t *testing.T) {
	a, b, c := newFakeNode(t, 10), newFakeNode(t, 10), newFakeNode(t, 10)
	s := newSetup(t, map[string]*fakeNode{"a": a, "b": b, "c": c})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { // node b flaps while traffic flows
		for ctx.Err() == nil {
			b.broken.Store(!b.broken.Load())
			s.checker.CheckOnce(ctx)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	var wg sync.WaitGroup
	var failed atomic.Int64
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				resp, err := http.Post(s.gateway.URL, "application/json", strings.NewReader(call))
				if err != nil || resp.StatusCode != 200 {
					failed.Add(1)
				}
				if resp != nil {
					resp.Body.Close()
				}
			}
		}()
	}
	wg.Wait()
	if failed.Load() != 0 {
		t.Fatalf("%d of 500 requests failed even though 2 nodes stayed up", failed.Load())
	}
}

func TestRejectsNonPost(t *testing.T) {
	s := newSetup(t, map[string]*fakeNode{"a": newFakeNode(t, 1)})
	resp, err := http.Get(s.gateway.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("want 405, got %d", resp.StatusCode)
	}
}

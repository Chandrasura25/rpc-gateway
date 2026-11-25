package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock lets tests move time forward without sleeping.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func newLimiter(rate, burst float64) (*RateLimiter, *fakeClock) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	return &RateLimiter{Rate: rate, Burst: burst, IdleTTL: time.Minute, Now: clk.Now}, clk
}

func TestBurstThenLimited(t *testing.T) {
	l, _ := newLimiter(1, 3)
	for i := 0; i < 3; i++ {
		if ok, _ := l.Allow("a"); !ok {
			t.Fatalf("request %d of the burst was refused", i+1)
		}
	}
	ok, wait := l.Allow("a")
	if ok {
		t.Fatal("4th request should be limited")
	}
	if wait <= 0 || wait > time.Second {
		t.Fatalf("retry-after should be up to 1s at 1 req/s, got %v", wait)
	}
}

func TestTokensRefillOverTime(t *testing.T) {
	l, clk := newLimiter(2, 2) // 2 per second
	l.Allow("a")
	l.Allow("a")
	if ok, _ := l.Allow("a"); ok {
		t.Fatal("bucket should be empty")
	}
	clk.Advance(500 * time.Millisecond) // +1 token
	if ok, _ := l.Allow("a"); !ok {
		t.Fatal("one token should have refilled after 0.5s")
	}
	clk.Advance(time.Hour) // refill is capped at Burst
	for i := 0; i < 2; i++ {
		l.Allow("a")
	}
	if ok, _ := l.Allow("a"); ok {
		t.Fatal("refill must not exceed burst")
	}
}

func TestClientsHaveSeparateBuckets(t *testing.T) {
	l, _ := newLimiter(1, 1)
	l.Allow("noisy")
	if ok, _ := l.Allow("noisy"); ok {
		t.Fatal("noisy client should be limited")
	}
	if ok, _ := l.Allow("quiet"); !ok {
		t.Fatal("a different client must not be affected")
	}
}

func TestIdleBucketsAreDropped(t *testing.T) {
	l, clk := newLimiter(1, 1)
	l.Allow("gone")
	clk.Advance(2 * time.Minute)
	l.Allow("other") // triggers cleanup
	l.mu.Lock()
	_, still := l.buckets["gone"]
	l.mu.Unlock()
	if still {
		t.Fatal("idle client's bucket should be removed")
	}
}

func TestMiddlewareReturns429WithRetryAfter(t *testing.T) {
	l, _ := newLimiter(1, 1)
	h := l.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) }))

	req := func(key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(call))
		r.Header.Set("X-API-Key", key)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		return rec
	}
	if rec := req("k1"); rec.Code != 200 {
		t.Fatalf("first request: %d", rec.Code)
	}
	rec := req("k1")
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "1" ||
		!strings.Contains(rec.Body.String(), `"code":-32005`) {
		t.Fatalf("want 429 + Retry-After 1 + JSON-RPC error, got %d %q %s", rec.Code, rec.Header().Get("Retry-After"), rec.Body)
	}
	if rec := req("k2"); rec.Code != 200 {
		t.Fatalf("other API key should pass, got %d", rec.Code)
	}
}

// Many goroutines share one bucket: exactly Burst requests get through. Run with -race.
func TestConcurrentClientsNeverExceedBurst(t *testing.T) {
	l, _ := newLimiter(0.0001, 20) // effectively no refill during the test
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _ := l.Allow("shared"); ok {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if allowed.Load() != 20 {
		t.Fatalf("want exactly 20 allowed, got %d", allowed.Load())
	}
}

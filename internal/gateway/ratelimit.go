package gateway

import (
	"fmt"
	"math"
	"net"
	"net/http"
	"sync"
	"time"
)

// RateLimiter gives each client its own token bucket: it can burst up to
// Burst requests, then gets Rate requests per second. One noisy client can't
// spend the shared upstream quota (Alchemy/Infura limits) for everyone else.
type RateLimiter struct {
	Rate  float64 // tokens added per second
	Burst float64 // bucket size
	// IdleTTL drops buckets for clients not seen for this long, so the map
	// doesn't grow forever. A dropped client simply starts with a full bucket.
	IdleTTL time.Duration
	Now     func() time.Time // injectable clock for tests
	Metrics *Metrics         // optional: counts refused requests

	mu      sync.Mutex
	buckets map[string]*bucket
	lastGC  time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

// Allow takes one token for key. When the bucket is empty it returns false and
// how long until the next token arrives (sent to the client as Retry-After).
func (l *RateLimiter) Allow(key string) (bool, time.Duration) {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.buckets == nil {
		l.buckets = map[string]*bucket{}
		l.lastGC = now
	}
	l.gc(now)

	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.Burst, last: now}
		l.buckets[key] = b
	}
	// Refill for the time since this client was last seen, capped at Burst.
	b.tokens = math.Min(l.Burst, b.tokens+now.Sub(b.last).Seconds()*l.Rate)
	b.last = now

	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	wait := time.Duration((1 - b.tokens) / l.Rate * float64(time.Second))
	return false, wait
}

func (l *RateLimiter) gc(now time.Time) {
	if l.IdleTTL <= 0 || now.Sub(l.lastGC) < l.IdleTTL {
		return
	}
	for k, b := range l.buckets {
		if now.Sub(b.last) > l.IdleTTL {
			delete(l.buckets, k)
		}
	}
	l.lastGC = now
}

func (l *RateLimiter) now() time.Time {
	if l.Now != nil {
		return l.Now()
	}
	return time.Now()
}

// Middleware limits by API key header when present, otherwise by client IP.
func (l *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ok, wait := l.Allow(clientKey(r))
		if !ok {
			l.Metrics.RateLimited()
			secs := int(math.Ceil(wait.Seconds()))
			w.Header().Set("Retry-After", fmt.Sprint(secs))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":null,"error":{"code":-32005,"message":"rate limit exceeded, retry in %ds"}}`, secs)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func clientKey(r *http.Request) string {
	if k := r.Header.Get("X-API-Key"); k != "" {
		return "key:" + k
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return "ip:" + host
}

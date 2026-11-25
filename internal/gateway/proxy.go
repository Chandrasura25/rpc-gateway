package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// MaxBodyBytes caps request bodies so one client can't exhaust memory.
const MaxBodyBytes = 1 << 20

// MaxResponseBytes caps what we buffer from an upstream (big blocks with full
// transactions can be several MB).
const MaxResponseBytes = 32 << 20

// Proxy forwards each JSON-RPC request to a healthy upstream. If that node
// fails (network error, timeout, 5xx or 429), it tries the next one, so a
// single bad node costs one retry, not a failed request. With a Cache set,
// results that can never change are answered without touching a node, and
// identical cache-eligible requests in flight at the same time share one call.
type Proxy struct {
	Pool    *Pool
	Client  *http.Client
	Cache   *Cache   // optional
	Metrics *Metrics // optional
	Log     *slog.Logger

	inflight inflight
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "JSON-RPC needs POST", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	if err != nil {
		http.Error(w, "request body too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}

	req, single := parseSingle(body)
	cacheable := p.Cache != nil && single && cacheCandidate(req)
	if cacheable {
		if p.serveFromCache(w, req) {
			return
		}
		// Not cached yet. If an identical request is already on its way to a
		// node, wait for it and read the cache it fills instead of calling again.
		key := p.Cache.key(req)
		if p.inflight.join(key) {
			defer p.inflight.finish(key)
		} else {
			p.Metrics.Coalesced()
			if p.serveFromCache(w, req) {
				return
			}
		}
		p.Metrics.CacheMiss()
	}

	var lastErr error
	for _, u := range p.Pool.Candidates() {
		start := time.Now()
		status, respBody, err := p.forward(r, u, body)
		if err != nil {
			p.Metrics.ObserveUpstream(u.Name, "error", time.Since(start))
			lastErr = err
			p.log().Warn("upstream failed, trying next", "upstream", u.Name, "err", err)
			continue
		}
		p.Metrics.ObserveUpstream(u.Name, "ok", time.Since(start))
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Upstream", u.Name)
		if cacheable {
			w.Header().Set("X-Cache", "MISS")
			if status == http.StatusOK {
				p.Cache.MaybeStore(req, respBody, p.Pool.Head())
			}
		}
		w.WriteHeader(status)
		w.Write(respBody)
		return
	}

	if lastErr == nil {
		lastErr = errors.New("no healthy upstreams")
	}
	writeRPCError(w, fmt.Sprintf("all upstreams failed: %v", lastErr))
}

func (p *Proxy) serveFromCache(w http.ResponseWriter, req rpcRequest) bool {
	result, ok := p.Cache.Lookup(req)
	if !ok {
		return false
	}
	p.Metrics.CacheHit()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "HIT")
	w.Write(cachedResponse(req.ID, result))
	return true
}

// forward sends the body to one upstream and reads the whole answer. Responses
// that mean "this node is struggling" are errors so the caller moves on.
func (p *Proxy) forward(r *http.Request, u *Upstream, body []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, u.URL, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.Client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
		return 0, nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, MaxResponseBytes))
	if err != nil {
		return 0, nil, fmt.Errorf("reading response: %w", err)
	}
	if msg, ok := nodeLimitation(respBody); ok {
		return 0, nil, fmt.Errorf("node can't serve this: %s", msg)
	}
	return resp.StatusCode, respBody, nil
}

// nodeLimitationHints are JSON-RPC error messages that describe the node, not
// the request: a pruned node without history, a free tier without archive
// access, a node that hasn't synced that block yet, or a provider rate limit.
// Another node may well answer, so these fail over. Anything else (a malformed
// request, an execution revert) is the caller's answer and is passed through.
var nodeLimitationHints = []string{
	"archive", "missing trie node", "pruned", "header not found",
	"not available", "rate limit", "limit exceeded", "too many requests",
}

func nodeLimitation(body []byte) (string, bool) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' { // batches are passed through as-is
		return "", false
	}
	var resp struct {
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &resp) != nil || resp.Error == nil {
		return "", false
	}
	msg := strings.ToLower(resp.Error.Message)
	for _, hint := range nodeLimitationHints {
		if strings.Contains(msg, hint) {
			return resp.Error.Message, true
		}
	}
	return "", false
}

func writeRPCError(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadGateway)
	fmt.Fprintf(w, `{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":%q}}`, msg)
}

func (p *Proxy) log() *slog.Logger {
	if p.Log != nil {
		return p.Log
	}
	return slog.Default()
}

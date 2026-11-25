package gateway

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Metrics counts what the gateway does and serves it in the Prometheus text
// format at /metrics. Written by hand (standard library only); the format is
// small: "# TYPE", then "name{labels} value" lines.
type Metrics struct {
	mu        sync.Mutex
	upstream  map[upstreamKey]float64 // rpc_gateway_upstream_requests_total
	latency   map[string]*histogram   // rpc_gateway_upstream_latency_seconds, per upstream
	cacheHit  float64
	cacheMiss float64
	limited   float64
	coalesced float64
}

type upstreamKey struct{ name, outcome string }

// Latency buckets in seconds, from a fast cached-node answer to a slow archive call.
var latencyBuckets = []float64{0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

type histogram struct {
	counts []float64 // per bucket (not cumulative; made cumulative on output)
	sum    float64
	count  float64
}

// ObserveUpstream records one attempt against a node. outcome is "ok" or "error".
func (m *Metrics) ObserveUpstream(name, outcome string, d time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.upstream == nil {
		m.upstream = map[upstreamKey]float64{}
		m.latency = map[string]*histogram{}
	}
	m.upstream[upstreamKey{name, outcome}]++
	h := m.latency[name]
	if h == nil {
		h = &histogram{counts: make([]float64, len(latencyBuckets))}
		m.latency[name] = h
	}
	s := d.Seconds()
	for i, b := range latencyBuckets {
		if s <= b {
			h.counts[i]++
			break
		}
	}
	h.sum += s
	h.count++
}

// Counters are safe to call on a nil *Metrics (metrics turned off). The nil
// check must come before touching any field: &m.x on a nil m panics.
func (m *Metrics) CacheHit()    { m.inc(func(m *Metrics) { m.cacheHit++ }) }
func (m *Metrics) CacheMiss()   { m.inc(func(m *Metrics) { m.cacheMiss++ }) }
func (m *Metrics) RateLimited() { m.inc(func(m *Metrics) { m.limited++ }) }
func (m *Metrics) Coalesced()   { m.inc(func(m *Metrics) { m.coalesced++ }) }

func (m *Metrics) inc(f func(*Metrics)) {
	if m == nil {
		return
	}
	m.mu.Lock()
	f(m)
	m.mu.Unlock()
}

// Handler serves /metrics. Node health and height are read live from the pool.
func (m *Metrics) Handler(pool *Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		m.Write(w, pool)
	})
}

func (m *Metrics) Write(w io.Writer, pool *Pool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Fprintln(w, "# HELP rpc_gateway_upstream_requests_total Requests sent to each node, by outcome.")
	fmt.Fprintln(w, "# TYPE rpc_gateway_upstream_requests_total counter")
	keys := make([]upstreamKey, 0, len(m.upstream))
	for k := range m.upstream {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].name != keys[j].name {
			return keys[i].name < keys[j].name
		}
		return keys[i].outcome < keys[j].outcome
	})
	for _, k := range keys {
		fmt.Fprintf(w, "rpc_gateway_upstream_requests_total{upstream=%q,outcome=%q} %g\n", k.name, k.outcome, m.upstream[k])
	}

	fmt.Fprintln(w, "# HELP rpc_gateway_upstream_latency_seconds Time for a node to answer.")
	fmt.Fprintln(w, "# TYPE rpc_gateway_upstream_latency_seconds histogram")
	names := make([]string, 0, len(m.latency))
	for n := range m.latency {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		h := m.latency[n]
		var cum float64
		for i, b := range latencyBuckets {
			cum += h.counts[i]
			fmt.Fprintf(w, "rpc_gateway_upstream_latency_seconds_bucket{upstream=%q,le=\"%g\"} %g\n", n, b, cum)
		}
		fmt.Fprintf(w, "rpc_gateway_upstream_latency_seconds_bucket{upstream=%q,le=\"+Inf\"} %g\n", n, h.count)
		fmt.Fprintf(w, "rpc_gateway_upstream_latency_seconds_sum{upstream=%q} %g\n", n, h.sum)
		fmt.Fprintf(w, "rpc_gateway_upstream_latency_seconds_count{upstream=%q} %g\n", n, h.count)
	}

	if pool != nil {
		fmt.Fprintln(w, "# HELP rpc_gateway_upstream_healthy 1 if the node is passing health checks.")
		fmt.Fprintln(w, "# TYPE rpc_gateway_upstream_healthy gauge")
		ups := append([]*Upstream(nil), pool.Upstreams()...)
		sort.Slice(ups, func(i, j int) bool { return ups[i].Name < ups[j].Name })
		for _, u := range ups {
			v := 0
			if u.Healthy() {
				v = 1
			}
			fmt.Fprintf(w, "rpc_gateway_upstream_healthy{upstream=%q} %d\n", u.Name, v)
		}
		fmt.Fprintln(w, "# HELP rpc_gateway_upstream_block_height Latest block each node reported.")
		fmt.Fprintln(w, "# TYPE rpc_gateway_upstream_block_height gauge")
		for _, u := range ups {
			fmt.Fprintf(w, "rpc_gateway_upstream_block_height{upstream=%q} %d\n", u.Name, u.Height())
		}
	}

	for _, c := range []struct {
		name, help string
		v          float64
	}{
		{"rpc_gateway_cache_hits_total", "Requests answered from the cache.", m.cacheHit},
		{"rpc_gateway_cache_misses_total", "Cache-eligible requests that went to a node.", m.cacheMiss},
		{"rpc_gateway_rate_limited_total", "Requests refused by the per-client rate limit.", m.limited},
		{"rpc_gateway_coalesced_total", "Requests that waited for an identical in-flight request instead of calling a node.", m.coalesced},
	} {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %g\n", c.name, c.help, c.name, c.name, c.v)
	}
}

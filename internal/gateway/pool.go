// Package gateway load-balances JSON-RPC requests across blockchain nodes,
// skipping nodes that are down or behind on block height.
package gateway

import (
	"sync/atomic"
)

// Upstream is one RPC node. Health and height are written by the health
// checker and read by request goroutines, so they are atomics, not mutex-guarded fields.
type Upstream struct {
	Name    string
	URL     string
	healthy atomic.Bool
	height  atomic.Uint64
}

func (u *Upstream) Healthy() bool  { return u.healthy.Load() }
func (u *Upstream) Height() uint64 { return u.height.Load() }

// Pool hands out upstreams in round-robin order, healthy ones only.
type Pool struct {
	upstreams []*Upstream
	next      atomic.Uint64
}

// NewPool starts every upstream as healthy so traffic flows before the first check.
func NewPool(urls map[string]string) *Pool {
	p := &Pool{}
	for name, url := range urls {
		u := &Upstream{Name: name, URL: url}
		u.healthy.Store(true)
		p.upstreams = append(p.upstreams, u)
	}
	return p
}

func (p *Pool) Upstreams() []*Upstream { return p.upstreams }

// Candidates returns the healthy upstreams, rotated so each request starts
// at a different node. The caller tries them in order until one answers.
func (p *Pool) Candidates() []*Upstream {
	n := len(p.upstreams)
	if n == 0 {
		return nil
	}
	start := int(p.next.Add(1)-1) % n
	out := make([]*Upstream, 0, n)
	for i := 0; i < n; i++ {
		u := p.upstreams[(start+i)%n]
		if u.Healthy() {
			out = append(out, u)
		}
	}
	return out
}

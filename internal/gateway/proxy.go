package gateway

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
)

// MaxBodyBytes caps request bodies so one client can't exhaust memory.
const MaxBodyBytes = 1 << 20

// Proxy forwards each JSON-RPC request to a healthy upstream. If that node
// fails (network error, timeout, 5xx or 429), it tries the next one, so a
// single bad node costs one retry, not a failed request.
type Proxy struct {
	Pool   *Pool
	Client *http.Client
	Log    *slog.Logger
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

	candidates := p.Pool.Candidates()
	var lastErr error
	for _, u := range candidates {
		resp, err := p.forward(r, u, body)
		if err != nil {
			lastErr = err
			p.log().Warn("upstream failed, trying next", "upstream", u.Name, "err", err)
			continue
		}
		defer resp.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Upstream", u.Name)
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		return
	}

	if lastErr == nil {
		lastErr = errors.New("no healthy upstreams")
	}
	writeRPCError(w, fmt.Sprintf("all upstreams failed: %v", lastErr))
}

// forward sends the body to one upstream. Responses that mean "this node is
// struggling" are turned into errors so the caller moves on.
func (p *Proxy) forward(r *http.Request, u *Upstream, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, u.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.Client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
		resp.Body.Close()
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	return resp, nil
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

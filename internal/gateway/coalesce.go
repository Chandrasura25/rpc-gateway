package gateway

import "sync"

// inflight makes identical cache-eligible requests that arrive together share
// one upstream call (the "singleflight" pattern). The first caller (leader)
// does the work; the others wait on its WaitGroup, then read the cache the
// leader just filled. If the leader's answer couldn't be cached (an error, a
// block too recent), followers simply make their own call, so nobody gets an
// answer meant for someone else.
type inflight struct {
	mu    sync.Mutex
	calls map[string]*sync.WaitGroup
}

// join returns done=true for the leader, who must call finish(key) when done.
// Followers get done=false after the leader has finished.
func (f *inflight) join(key string) (leader bool) {
	f.mu.Lock()
	if f.calls == nil {
		f.calls = map[string]*sync.WaitGroup{}
	}
	if wg, ok := f.calls[key]; ok {
		f.mu.Unlock()
		wg.Wait()
		return false
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	f.calls[key] = wg
	f.mu.Unlock()
	return true
}

func (f *inflight) finish(key string) {
	f.mu.Lock()
	wg := f.calls[key]
	delete(f.calls, key)
	f.mu.Unlock()
	wg.Done()
}

// cacheCandidate reports whether a request could produce a cacheable result,
// i.e. whether it's worth coalescing. Writes like eth_sendRawTransaction never are.
func cacheCandidate(req rpcRequest) bool {
	switch req.Method {
	case "eth_chainId", "net_version", "eth_getBlockByHash", "eth_getBlockByNumber",
		"eth_getTransactionByHash", "eth_getTransactionReceipt":
		return true
	}
	return false
}

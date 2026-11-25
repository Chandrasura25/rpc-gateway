package gateway

import (
	"bytes"
	"container/list"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
)

// Cache keeps JSON-RPC results that can never change, so repeat calls skip the
// upstream entirely. It only stores results; the response sent back always
// carries the caller's own request id.
//
// What counts as "can never change":
//   - eth_chainId, net_version
//   - eth_getBlockByHash: a block's hash commits to its contents
//   - eth_getBlockByNumber for a numbered block at least FinalityDepth below
//     the chain head (recent blocks can be reorged, tags like "latest" move)
//   - eth_getTransactionByHash / eth_getTransactionReceipt once the transaction
//     sits in a block that deep (a pending or recent one can still move)
//
// null results (not found yet) and errors are never cached.
type Cache struct {
	MaxEntries    int
	FinalityDepth uint64

	mu    sync.Mutex
	ll    *list.List // front = most recently used
	items map[string]*list.Element
}

type cacheEntry struct {
	key    string
	result json.RawMessage
}

// rpcRequest is a single JSON-RPC call. Batches (JSON arrays) are not cached.
type rpcRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

// parseSingle returns the request if body is one JSON-RPC call (not a batch).
func parseSingle(body []byte) (rpcRequest, bool) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return rpcRequest{}, false
	}
	var req rpcRequest
	if err := json.Unmarshal(trimmed, &req); err != nil || req.Method == "" {
		return rpcRequest{}, false
	}
	return req, true
}

// key is the method plus the params in compact form, so whitespace differences
// in the request don't create separate entries.
func (c *Cache) key(req rpcRequest) string {
	var buf bytes.Buffer
	if len(req.Params) > 0 && json.Compact(&buf, req.Params) == nil {
		return req.Method + ":" + buf.String()
	}
	return req.Method + ":[]"
}

// Lookup returns a cached result for req, if there is one.
func (c *Cache) Lookup(req rpcRequest) (json.RawMessage, bool) {
	k := c.key(req)
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[k]; ok {
		c.ll.MoveToFront(el)
		return el.Value.(*cacheEntry).result, true
	}
	return nil, false
}

// MaybeStore saves the result of req if it is safe to keep forever, given the
// current best chain head. respBody is the full upstream response.
func (c *Cache) MaybeStore(req rpcRequest, respBody []byte, head uint64) bool {
	var resp struct {
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if json.Unmarshal(respBody, &resp) != nil {
		return false
	}
	if len(resp.Error) > 0 && string(resp.Error) != "null" {
		return false
	}
	if len(resp.Result) == 0 || string(resp.Result) == "null" {
		return false
	}
	if !c.immutable(req, resp.Result, head) {
		return false
	}
	c.put(c.key(req), resp.Result)
	return true
}

func (c *Cache) immutable(req rpcRequest, result json.RawMessage, head uint64) bool {
	switch req.Method {
	case "eth_chainId", "net_version", "eth_getBlockByHash":
		return true
	case "eth_getBlockByNumber":
		var params []json.RawMessage
		if json.Unmarshal(req.Params, &params) != nil || len(params) == 0 {
			return false
		}
		var tag string
		if json.Unmarshal(params[0], &tag) != nil {
			return false
		}
		n, ok := hexToUint(tag) // "latest", "pending", "safe", "finalized" are not numbers
		return ok && c.deepEnough(n, head)
	case "eth_getTransactionByHash", "eth_getTransactionReceipt":
		var tx struct {
			BlockNumber *string `json:"blockNumber"`
		}
		if json.Unmarshal(result, &tx) != nil || tx.BlockNumber == nil {
			return false // still pending
		}
		n, ok := hexToUint(*tx.BlockNumber)
		return ok && c.deepEnough(n, head)
	}
	return false
}

func (c *Cache) deepEnough(block, head uint64) bool {
	return head > 0 && block+c.FinalityDepth <= head
}

func (c *Cache) put(k string, result json.RawMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.items == nil {
		c.items = map[string]*list.Element{}
		c.ll = list.New()
	}
	if el, ok := c.items[k]; ok {
		c.ll.MoveToFront(el)
		return
	}
	c.items[k] = c.ll.PushFront(&cacheEntry{key: k, result: append(json.RawMessage(nil), result...)})
	for c.MaxEntries > 0 && c.ll.Len() > c.MaxEntries {
		oldest := c.ll.Back()
		c.ll.Remove(oldest)
		delete(c.items, oldest.Value.(*cacheEntry).key)
	}
}

// Len is the number of cached results.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ll == nil {
		return 0
	}
	return c.ll.Len()
}

func hexToUint(s string) (uint64, bool) {
	if !strings.HasPrefix(s, "0x") {
		return 0, false
	}
	n, err := strconv.ParseUint(s[2:], 16, 64)
	return n, err == nil
}

// cachedResponse builds a response for the caller's id around a cached result.
func cachedResponse(id json.RawMessage, result json.RawMessage) []byte {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	var buf bytes.Buffer
	buf.WriteString(`{"jsonrpc":"2.0","id":`)
	buf.Write(id)
	buf.WriteString(`,"result":`)
	buf.Write(result)
	buf.WriteString("}")
	return buf.Bytes()
}

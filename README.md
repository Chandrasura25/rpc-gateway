# rpc-gateway

A small Go service that sits in front of several blockchain RPC nodes (Alchemy, Infura, your own) and keeps JSON-RPC requests flowing when nodes go down, fall behind or start erroring.

Standard library only.

## What it does

- **Round-robin** across healthy nodes.
- **Failover per request**: if a node errors, times out, returns 5xx or 429, the same request is retried on the next node, so one bad node costs a retry, not a failed call.
- **Node-limitation failover**: errors that describe the node, not the request ("archive requests require a token", "missing trie node", "header not found", provider rate limits) move the request to the next node. Found while testing against real Base nodes: a free node refused an old block and the error went straight to the caller. Genuine caller errors (e.g. `execution reverted`) still pass through unchanged.
- **Caching only what can never change** (LRU, `-cache-size`): `eth_chainId`, `net_version`, `eth_getBlockByHash`, blocks by number at least `-finality-depth` below the head, and transactions/receipts once they sit that deep. `latest`, recent blocks, pending transactions, `null` results and errors are never cached, so a reorg can't leave stale data behind. Cached answers carry the caller's own request id. Header `X-Cache: HIT|MISS`.
- **Request coalescing** ("singleflight"): when identical cache-eligible requests arrive together, one goes to a node and the rest wait for it, then read the cache it filled. If that answer couldn't be cached (too recent, an error), each waiting request makes its own call, so nobody receives an answer meant for someone else.
- **Prometheus metrics** at `GET /metrics` (standard library, no client library): requests per node by outcome, a latency histogram per node, node health and block height, cache hits/misses, coalesced requests, rate-limited requests.
- **Health checks** every few seconds, all nodes in parallel (goroutines), using `eth_blockNumber`. A node is skipped when it's down **or more than `-max-lag` blocks behind the best node**, so a node that is up but stuck on an old block stops serving stale data. It comes back automatically once it catches up.
- Shared state (health, height, round-robin counter) uses atomics; the concurrency test runs under `go test -race`.
- **Graceful shutdown** on Ctrl+C / SIGTERM: stops accepting, lets in-flight requests finish.
- **Per-client rate limiting** (token bucket, by `X-API-Key` header or client IP): each client can burst, then gets a steady rate, so one noisy client can't burn the shared node quota. Over the limit → `429` with `Retry-After` and a JSON-RPC error. Idle clients' buckets are cleaned up.
- `GET /healthz` returns 503 when no node is healthy.

## Run

```
go run ./cmd/rpc-gateway -upstreams "alchemy=https://base-mainnet.g.alchemy.com/v2/KEY,local=http://localhost:8546"
curl -s localhost:8545 -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'
```

The response header `X-Upstream` says which node answered. Metrics: `curl localhost:8545/metrics`.

| Flag | Default | |
|---|---|---|
| `-listen` | `:8545` | address to listen on |
| `-upstreams` | `$RPC_UPSTREAMS` | `name=url` pairs, comma-separated |
| `-interval` | `5s` | health check interval |
| `-max-lag` | `3` | blocks behind the best node before a node is skipped |
| `-timeout` | `10s` | per-upstream request timeout |
| `-rate` | `20` | requests per second per client; `0` turns limiting off |
| `-burst` | `40` | burst each client may use before `-rate` applies |
| `-cache-size` | `10000` | max cached results; `0` turns caching off |
| `-finality-depth` | `64` | blocks below the head before a block/transaction is cacheable |

## Test

```
go test -race ./...
```

Fake nodes (`httptest`) can be made broken, slow to catch up, or flapping. The tests cover round robin, failover, lag detection and recovery, all-nodes-down, 50 concurrent clients while one node flaps (0 failures, race-free), and the rate limiter: burst, refill (with a fake clock, no sleeps), separate clients, idle cleanup, 429 + Retry-After, and 100 goroutines on one bucket letting exactly the burst through. Coalescing tests: 20 identical requests to a slow node make exactly 1 upstream call and each caller gets its own id; uncacheable answers are never shared. Metrics tests check the exposition output and that metrics can be switched off safely. Cache tests cover HIT/MISS with the caller's id, whitespace-insensitive keys, `latest`/recent/pending never cached, finalized blocks and transactions cached, nulls/errors never cached, batches passed through, LRU eviction, and archive-error failover vs. a revert passing through.

## Not done yet

- Batch requests are forwarded as one unit, not split across nodes.
- WebSocket subscriptions are not proxied.

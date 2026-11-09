# Architecture

## Request path

```
client ──HTTP/1.1──▶ worker (one per core, SO_REUSEPORT)
                       │
   1 authenticate      │  SHA-256 of the bearer token, looked up in a hash map
   2 validate & route  │  model alias → ordered list of provider targets
   3 per-key limits    │  RPM bucket, TPM bucket, daily quota; tokens are reserved
   4 cache lookup      │  sharded LRU; identical non-stream misses are coalesced
   5 admission         │  global concurrency cap, bounded fair queue, shed on full
   6 dispatch          │  retries with jitter, retry budget, breaker, failover
   7 forward           │  SSE passthrough or translation, with token accounting
   8 settle & record   │  quota, cache, metrics, one structured log line
                       ▼
             pooled HTTP/1.1 connection ──▶ TLS egress sidecar ──▶ provider
```

## Why these choices

**Thread-per-core runtime (`src/rt`).** Each worker owns an epoll instance, a run queue and a timer tree, and tasks
never move between threads. Sockets and upstream connection pools therefore need no locks. Wakers can still cross
threads through an injector queue and an eventfd, which is how a permit released on one worker wakes a request queued
on another. The kernel balances new connections across workers through `SO_REUSEPORT`.

The API deliberately mirrors tokio (`spawn`, `sleep`, `timeout`, `select`), so moving to tokio later would be mostly
mechanical. It exists because this project was built without access to crates.io. In a team setting, tokio and hyper
would be the default choice.

**Edge-triggered epoll.** A socket is registered once and never re-armed, which saves an `epoll_ctl` per wait. The
price is the rule that a task must read until `EAGAIN` before it can sleep; `TcpConn` enforces that in one place.

**Backpressure comes from the structure of the proxy loop.** The stream loop reads from upstream, writes to the client,
and awaits that write before reading again. When a client stalls, its socket buffer fills, the gateway stops reading
that upstream connection, and TCP flow control pushes back on the provider. Memory per stream stays bounded by design.
`client_write_ms` then cuts off clients that stall for too long.

**The first byte is the point of no return.** The client's response head is held until the first upstream event
arrives. Every failure before then can be retried or failed over invisibly. After it, the client has partial output, so
the gateway sends an SSE error event and ends the stream. It does not silently restart generation on another model.

**Overload becomes fast failure.** Admission has a concurrency cap and a bounded queue with a timeout. Past those, a
request gets a 503 with `Retry-After` immediately, which keeps latency bounded for the requests that are accepted.
The queue is round-robin across tenants, so one noisy key cannot starve the others.

**Retries are budgeted.** Retries are limited to a fraction of request volume per provider, and an open circuit breaker
removes a failing provider from rotation. Without both, retries multiply load at exactly the moment a provider is
struggling. Breaker half-open probes are limited, and a successful recovery restores traffic automatically.

**Tokens are reserved, then settled.** The request's estimated size plus `max_tokens` is reserved at admission and
replaced by actual usage afterward. That stops a burst of concurrent requests from overshooting a limit. Bookkeeping
finishes *before* the final bytes reach the client so that a fast follow-up request sees up-to-date state.

**Cache safety.** The key is SHA-256 over canonical JSON (sorted keys, `user` removed) and is scoped per tenant by
default. Only deterministic requests are cached unless a client opts in, because caching a sampled response changes
behavior. Truncated streams are never stored. Expired entries are retained for `stale_if_error_secs` so they can be
served during an outage, marked `x-cache: STALE`.

**HTTP strictness.** The parser rejects the classic request-smuggling inputs: `Content-Length` with
`Transfer-Encoding`, conflicting lengths, obsolete line folding, and malformed chunk framing.

## Failure handling

| Upstream behavior | Retry same provider | Fail over | Counts against breaker |
|---|---|---|---|
| Connect failure or timeout | yes | yes | yes |
| 408, 409, 425, 5xx | yes | yes | yes |
| Timeout before first byte | yes | yes | yes |
| 429 | no | yes | no |
| 401, 403, 404 | no | yes | no |
| 400, 413, 422 | no | no, returned to client | no |
| Stream dies after first byte | no | no, SSE error event | yes |
| Client disconnects | upstream is cancelled | n/a | no |

## Known limitations

- **State is per process.** Rate limits, quotas and the cache live in memory. Running N replicas multiplies each limit
  by up to N. A shared store such as Redis would be the next step for horizontal scaling.
- **Quota snapshots are periodic** (30 seconds, and on shutdown). A crash can lose up to 30 seconds of quota accounting.
- **No TLS in the process.** Ingress TLS belongs to a load balancer, and egress goes through `deploy/tls-egress`.
- **HTTP/1.1 only**, for both clients and upstreams.
- **Anthropic translation covers text.** Requests using tools, images or `response_format` skip Anthropic targets.
- **Token estimates are approximate** (four characters per token) when a provider does not report usage.
- **Only `/v1/chat/completions` is proxied.** Embeddings and the legacy completions API are not implemented.
- **Linux only**, because the runtime uses epoll.
- A client that half-closes its socket after sending a request is treated as disconnected.

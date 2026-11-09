# Deployment and operations

> The Docker, Compose, Kubernetes and CI files could not be executed in the environment where this project was built
> (no Docker daemon or cluster). The gateway binary and TLS sidecar were tested directly. Expect to do a first-run
> check of the container files.

## Configure

```bash
cp config/example.json config/config.json
cargo run --release --bin llm-gateway -- generate-key   # give the key to the client, put the sha256 in the config
```

Edit `config/config.json`: fill in `key_sha256`, real model names, and per-token prices. Then:

```bash
OPENAI_API_KEY=x ANTHROPIC_API_KEY=x METRICS_TOKEN=x \
  ./target/release/llm-gateway check --config config/config.json
```

Provider keys are always read from environment variables. Only hashes of client keys are stored in the config.

## Run with Compose

```bash
cd deploy
cat > ../.env <<'ENV'
OPENAI_API_KEY=...
ANTHROPIC_API_KEY=...
METRICS_TOKEN=...
GRAFANA_PASSWORD=...
ENV
grep METRICS_TOKEN ../.env | cut -d= -f2 | tr -d '\n' > metrics_token
docker compose --env-file ../.env up --build
```

Gateway on `:8080`, Prometheus on `127.0.0.1:9090`, Grafana on `127.0.0.1:3000` with the dashboard provisioned.

## TLS

Terminate inbound TLS at your load balancer or ingress. For outbound calls, `deploy/tls-egress` maps plaintext local
ports to HTTPS providers and verifies certificates against the system roots. Keep it on loopback or a private network,
because provider API keys cross that hop in plaintext. In Kubernetes it runs in the same pod, bound to `127.0.0.1`.

## Kubernetes

`deploy/k8s/gateway.yaml` uses `preStop.sleep`, which needs Kubernetes 1.30 or newer. On older clusters, replace it with
an `exec` of a sleep binary available in the image.

The manifest sets one replica on purpose. With more, per-key limits and the cache apply per pod, so divide each
tenant's limits by the replica count or add a shared store first. Quota snapshots use `emptyDir`; mount a
PersistentVolumeClaim if quota must survive pod replacement.

Set `nofile` high enough for your concurrency: each proxied stream uses two file descriptors.

## Shutdown behavior

On SIGTERM, `/readyz` returns 503, listeners close, and in-flight requests get up to `drain_secs` to finish. Quota is
then saved and the process exits 0. Keep `terminationGracePeriodSeconds` greater than `drain_secs` plus any preStop
delay.

## Runbook

| Alert or symptom | What it means | First actions |
|---|---|---|
| `GatewayProviderCircuitOpen` | A provider is failing and has been removed | Check `gateway_upstream_requests_total` by outcome and the provider's status page. Recovery is automatic. |
| `GatewayHighErrorRate` | Clients are seeing 5xx | Look at `outcome` in the logs. All providers down means 502/503; check `tls-egress` logs for dial errors. |
| `GatewayShedding` | Demand exceeds `max_inflight` | Check whether upstream latency rose. Then raise `max_inflight` or add capacity. |
| 429s on one tenant | Per-key limit reached | `gateway_rate_limited_total{reason}` shows which limit. Adjust the tenant's config. |
| `GatewayAddedLatencyHigh` | The gateway is CPU-starved | Check CPU throttling and `workers`, which defaults to the number of visible cores. |
| Cache hit rate near zero | Requests are not deterministic | Only `temperature: 0` or `seed` requests are cached by default. |

Every log line carries `trace_id`, `request_id`, `tenant`, `provider`, `outcome`, and phase timings. The gateway accepts
and forwards W3C `traceparent`, so a caller's trace id can be followed through these logs.

#!/usr/bin/env python3
"""Generates BENCHMARKS.md and docs/outage.png from loadtest/results/results.json."""
import json
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent.parent
R = json.loads((ROOT / "loadtest/results/results.json").read_text())
SURFACE, INK, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
PRIMARY, SECONDARY = "#2a78d6", "#eb6834"


def chart():
    case = R["outage"]["crash"]
    per_second = defaultdict(lambda: defaultdict(int))
    for second, label, count in case["timeline"]:
        per_second[second][label.split(":")[1]] += count
    seconds = sorted(s for s in per_second if s < 45)
    primary = [per_second[s]["primary"] for s in seconds]
    secondary = [per_second[s]["secondary"] for s in seconds]

    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11})
    fig, ax = plt.subplots(figsize=(10, 4.4), dpi=200, facecolor=SURFACE)
    ax.set_facecolor(SURFACE)
    ax.axvspan(9.5, 24.5, color="#efeeea", zorder=0, linewidth=0)
    ax.bar(seconds, primary, width=0.92, color=PRIMARY, label="Primary provider", zorder=2)
    ax.bar(seconds, secondary, width=0.92, bottom=primary, color=SECONDARY, label="Backup provider", zorder=2)
    ax.text(17, 345, "Primary process killed (t = 10 s to 25 s)", ha="center", color=MUTED, fontsize=10.5)
    ax.set_ylim(0, 380)
    ax.set_xlim(-0.8, 44.8)
    ax.set_xlabel("Seconds into test", color=MUTED)
    ax.set_ylabel("Successful responses per second", color=MUTED)
    ax.set_title("Traffic moved to the backup within a second, and back after recovery", loc="left", color=INK,
                 fontsize=13.5, pad=30)
    ax.yaxis.grid(True, color=GRID, linewidth=0.8, zorder=1)
    ax.tick_params(colors=MUTED, length=0)
    for side in ["top", "right", "left"]:
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.legend(loc="lower left", bbox_to_anchor=(0, 1.0), ncol=2, frameon=False, labelcolor=MUTED, borderaxespad=0.2)
    fig.tight_layout()
    fig.savefig(ROOT / "docs/outage.png", facecolor=SURFACE)


def k(v):
    return f"{v:,.0f}"


def report():
    o, t, c, con, ov = R["overhead"], R["throughput"], R["cache"], R["concurrency"], R["overload"]
    rows = []
    for name, label in [("error500", "Every request returns HTTP 500"), ("reset", "Connections reset"),
                        ("hang", "Accepts requests, never responds"), ("crash", "Provider process killed")]:
        x = R["outage"][name]
        rows.append(f"| {label} | {x['errors']} | {x['streams_broken_midway']:.0f} | {x['ttfb_ms']['p50']:.1f} ms | "
                    f"{x['ttfb_ms']['p99']:.1f} ms | {x['recovery_lag_s']} s |")
    saved, spent = c["cost_saved_micro_usd"] / 1e6, c["cost_spent_micro_usd"] / 1e6
    text = f"""# Benchmarks

Every number here comes from `loadtest/bench.py` and is rendered into this file by `loadtest/report.py`. Raw output is
in `loadtest/results/results.json`.

## Test setup, and what it means for these numbers

- 2-vCPU cloud sandbox, Linux, release build with LTO, loopback networking.
- The gateway ran with **one worker pinned to one vCPU**. The mock provider and load generator shared the other.
- Upstream is `mock-upstream`, not a real model provider. Real provider latency is hundreds of milliseconds or more,
  and would dwarf the gateway's overhead.
- Load came from the included `loadgen`, because k6 was unavailable. Open-loop runs measure from each request's
  scheduled start, which avoids coordinated omission.
- TLS, real network latency and multi-core scaling are **not** covered.

## Added latency

Same open-loop load at 1,000 req/s, sent directly to the mock and then through the gateway. Time to first byte, median
of 3 rounds of 15 seconds.

| Path | Direct p50 | Gateway p50 | Added p50 | Direct p99 | Gateway p99 | Added p99 |
|---|---|---|---|---|---|---|
| Non-streaming | {o['non_stream']['median']['direct_p50']:.3f} ms | {o['non_stream']['median']['gateway_p50']:.3f} ms | **{o['non_stream']['added_p50_ms']:.3f} ms** | {o['non_stream']['median']['direct_p99']:.3f} ms | {o['non_stream']['median']['gateway_p99']:.3f} ms | **{o['non_stream']['added_p99_ms']:.3f} ms** |
| Streaming | {o['stream']['median']['direct_p50']:.3f} ms | {o['stream']['median']['gateway_p50']:.3f} ms | **{o['stream']['added_p50_ms']:.3f} ms** | {o['stream']['median']['direct_p99']:.3f} ms | {o['stream']['median']['gateway_p99']:.3f} ms | **{o['stream']['added_p99_ms']:.3f} ms** |

Added values are differences between percentiles. These requests bypass the cache but go through everything else: auth,
limits, JSON handling, connection pooling, metrics and logging. No requests failed.

## Maximum throughput on one core

Closed loop, 64 connections, 15 seconds, 50-token responses, no upstream delay. Latency here is at saturation, so it
reflects queueing and should not be read as overhead.

| Case | Requests/s | TTFB p50 | TTFB p99 | Errors |
|---|---|---|---|---|
| Mock provider directly (baseline) | {k(t['direct_non_stream']['rps'])} | {t['direct_non_stream']['ttfb_ms']['p50']:.2f} ms | {t['direct_non_stream']['ttfb_ms']['p99']:.2f} ms | {t['direct_non_stream']['errors']} |
| Gateway, non-streaming, cache bypassed | **{k(t['gateway_non_stream']['rps'])}** | {t['gateway_non_stream']['ttfb_ms']['p50']:.2f} ms | {t['gateway_non_stream']['ttfb_ms']['p99']:.2f} ms | {t['gateway_non_stream']['errors']} |
| Gateway, streaming, cache bypassed | **{k(t['gateway_stream']['rps'])}** | {t['gateway_stream']['ttfb_ms']['p50']:.2f} ms | {t['gateway_stream']['ttfb_ms']['p99']:.2f} ms | {t['gateway_stream']['errors']} |
| Gateway, cache hits | **{k(t['gateway_cache_hit']['rps'])}** | {t['gateway_cache_hit']['ttfb_ms']['p50']:.2f} ms | {t['gateway_cache_hit']['ttfb_ms']['p99']:.2f} ms | {t['gateway_cache_hit']['errors']} |

## Cache

1,000 req/s for 30 seconds, starting cold. Prompts drawn from 10,000 distinct values on a Zipf (s = 1.0) distribution,
with a 50 ms upstream.

| Metric | Result |
|---|---|
| Hit rate | **{c['cache_hit_rate'] * 100:.1f}%** |
| Upstream calls for {k(c['ok'])} requests | {k(c['upstream_calls'])} |
| TTFB p50 | {c['ttfb_ms']['p50']:.2f} ms |
| TTFB p99 (cache misses) | {c['ttfb_ms']['p99']:.2f} ms |
| Simulated spend avoided / spent | ${saved:.2f} / ${spent:.2f} |

Hit rate is a property of the workload. Zipf-distributed repeats are a favorable, chosen assumption, and real traffic
can be far lower. By default only `temperature: 0` or seeded requests are cached.

## Concurrent streams

{k(con['concurrent_streams'])} simultaneous streams at 10 tokens per second, on one gateway core, with a 100 ms upstream TTFB.

| Metric | Result |
|---|---|
| In-flight requests observed | {k(con['observed_inflight'])} |
| Errors | {con['errors']} |
| Gateway memory, idle → loaded | {con['rss_idle_kb'] / 1024:.1f} MiB → {con['rss_loaded_kb'] / 1024:.1f} MiB |
| Memory per stream | {con['rss_per_stream_kb']:.1f} KiB |
| TTFB p50 / p99 | {con['ttfb_ms']['p50']:.1f} ms / {con['ttfb_ms']['p99']:.1f} ms |

The p99 reflects all streams starting and restarting in synchronized waves, which is harsher than real arrivals.

## Overload

{k(ov['target_rps'])} req/s offered against {k(ov['capacity_rps'])} req/s of capacity (100 concurrent slots, 100 ms upstream), with a 100-slot queue
and a 200 ms queue timeout.

| Metric | Result |
|---|---|
| Served (200) | {k(ov['ok'])} ({ov['ok'] / ov['secs']:,.0f} req/s) |
| Shed (503 with `Retry-After`) | {k(ov['statuses']['503'])} |
| Served request p99 | {ov['total_ok_ms']['p99']:.1f} ms |
| Overall median, dominated by immediate 503s | {ov['total_ms']['p50']:.2f} ms |

Served-request latency stayed bounded by the queue timeout, and the gateway held throughput close to capacity.

## Upstream outage

300 streams per second for 45 seconds, with default breaker settings and a 1-second first-byte timeout. The primary
fails at t = 10 and recovers at t = 25.

| Failure mode | HTTP errors to clients | Streams cut off midway | TTFB p50 | TTFB p99 | Back on primary after recovery |
|---|---|---|---|---|---|
{chr(10).join(rows)}

![Traffic per second by provider during a provider crash](docs/outage.png)

- **Streams already running when a provider dies cannot be saved.** In the crash run, {R['outage']['crash']['streams_broken_midway']:.0f} of {k(R['outage']['crash']['ok'])} streams ended with an SSE error
  event. Those responses still count as HTTP 200, so they do not appear in the HTTP error column.
- **A hanging provider is the slowest failure to detect.** Requests during detection wait for the 1-second timeout,
  which shows up in that row's p99. With the default 30-second `first_byte_ms`, that wait would be up to 30 seconds.
- **Recovery lag is intentional.** The breaker backs off between probes, up to `max_open_ms` (10 seconds by default).

## Reproduce

```bash
cargo build --release
python3 loadtest/bench.py     # about 9 minutes; requires taskset and 2 or more cores
python3 loadtest/report.py    # requires matplotlib
```

`loadtest/k6-gateway.js` is provided for testing a deployed gateway. It has not been run.
"""
    (ROOT / "BENCHMARKS.md").write_text(text)


chart()
report()

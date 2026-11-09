#!/usr/bin/env python3
"""Runs the gateway benchmark suite and writes loadtest/results/results.json.

Usage: loadtest/bench.py [scenario ...]   (default: all)

Processes are pinned so the numbers are repeatable: the gateway gets one core,
and the mock provider and load generator share another.
"""
import json
import os
import signal
import socket
import statistics
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BIN = ROOT / "target" / "release"
OUT = ROOT / "loadtest" / "results"
GW_CPU = "0"
LOAD_CPU = "1"
KEY = "bench-key"
GW_PORT = 18080
PRIMARY = 19101
SECONDARY = 19102


def sha(key):
    return subprocess.check_output([BIN / "llm-gateway", "hash-key", key], text=True).strip()


def wait_port(port):
    for _ in range(400):
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.01)
    raise RuntimeError(f"port {port} did not open")


def mock(port, ttfb=0, tokens=50, delay_us=0):
    p = subprocess.Popen(
        ["taskset", "-c", LOAD_CPU, BIN / "mock-upstream", "--listen", f"127.0.0.1:{port}", "--workers", "1",
         "--ttfb-ms", str(ttfb), "--tokens", str(tokens), "--token-delay-us", str(delay_us)],
        stdout=subprocess.DEVNULL)
    wait_port(port)
    return p


def control(port, **kw):
    req = urllib.request.Request(f"http://127.0.0.1:{port}/__control", data=json.dumps(kw).encode(), method="POST")
    urllib.request.urlopen(req, timeout=5).read()


def stats(port):
    return json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/__stats", timeout=5))


def gateway(extra=None, workers=1, cpu=GW_CPU):
    cfg = {
        "listen": f"127.0.0.1:{GW_PORT}",
        "workers": workers,
        "max_connections": 16000,
        "admission": {"max_inflight": 16000, "max_queue": 4096, "queue_timeout_ms": 5000},
        "providers": [
            {"name": "primary", "kind": "openai", "base_url": f"http://127.0.0.1:{PRIMARY}",
             "api_key_env": "BENCH_KEY", "pool_size": 4096},
            {"name": "secondary", "kind": "openai", "base_url": f"http://127.0.0.1:{SECONDARY}",
             "api_key_env": "BENCH_KEY", "pool_size": 4096},
        ],
        "models": [{"alias": "fast", "targets": [
            {"provider": "primary", "model": "m1", "input_usd_per_mtok": 3, "output_usd_per_mtok": 15},
            {"provider": "secondary", "model": "m2", "input_usd_per_mtok": 3, "output_usd_per_mtok": 15}]}],
        "tenants": [{"name": "bench", "key_sha256": [sha(KEY)], "rpm": 2_000_000_000, "tpm": 2_000_000_000_000,
                     "daily_tokens": 2_000_000_000_000_000, "max_concurrent": 16000, "burst_secs": 60}],
    }
    for k, v in (extra or {}).items():
        cfg[k] = {**cfg.get(k, {}), **v} if isinstance(v, dict) else v
    path = OUT / "bench-config.json"
    path.write_text(json.dumps(cfg))
    env = {**os.environ, "BENCH_KEY": "upstream-secret"}
    p = subprocess.Popen(["taskset", "-c", cpu, BIN / "llm-gateway", "--config", path],
                         stdout=subprocess.DEVNULL, env=env)
    wait_port(GW_PORT)
    return p


def load(port=GW_PORT, cpu=LOAD_CPU, timeline=False, **kw):
    cmd = ["taskset", "-c", cpu, BIN / "loadgen", "--addr", f"127.0.0.1:{port}", "--key", KEY]
    for k, v in kw.items():
        flag = "--" + k.replace("_", "-")
        if v is True:
            cmd.append(flag)
        elif v is not False:
            cmd += [flag, str(v)]
    if timeline:
        cmd.append("--timeline")
    done = subprocess.run(cmd, capture_output=True, text=True, check=True)
    result = json.loads(done.stdout)
    if timeline:
        rows = [line.split("\t") for line in done.stderr.strip().splitlines()]
        result["timeline"] = [[int(s), label, int(n)] for s, label, n in rows]
    return result


def stop(*procs):
    for p in procs:
        p.kill()
        p.wait()
    time.sleep(0.3)


def rss_kb(pid):
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmRSS"):
            return int(line.split()[1])
    return 0


def metric_sum(prefix):
    text = urllib.request.urlopen(f"http://127.0.0.1:{GW_PORT}/metrics", timeout=5).read().decode()
    return sum(float(l.rsplit(" ", 1)[1]) for l in text.splitlines() if l.startswith(prefix))


def overhead():
    """Same open-loop load, direct to the mock and through the gateway."""
    out = {}
    for name, stream, rate in [("non_stream", False, 1000), ("stream", True, 1000)]:
        rounds = {"direct_p50": [], "direct_p99": [], "gateway_p50": [], "gateway_p99": [], "errors": []}
        for _ in range(3):
            m = mock(PRIMARY)
            s = mock(SECONDARY)
            d = load(port=PRIMARY, rate=rate, conns=64, secs=15, stream=stream)
            g = gateway()
            time.sleep(0.2)
            r = load(rate=rate, conns=64, secs=15, stream=stream)
            stop(g, m, s)
            rounds["direct_p50"].append(d["ttfb_ms"]["p50"])
            rounds["direct_p99"].append(d["ttfb_ms"]["p99"])
            rounds["gateway_p50"].append(r["ttfb_ms"]["p50"])
            rounds["gateway_p99"].append(r["ttfb_ms"]["p99"])
            rounds["errors"].append(d["errors"] + r["errors"])
        med = {k: statistics.median(v) for k, v in rounds.items()}
        out[name] = {
            "rate_rps": rate, "rounds": rounds, "median": med,
            "added_p50_ms": round(med["gateway_p50"] - med["direct_p50"], 3),
            "added_p99_ms": round(med["gateway_p99"] - med["direct_p99"], 3),
        }
    return out


def throughput():
    """Closed-loop maximum with the gateway on a single core."""
    out = {}
    cases = [
        ("direct_non_stream", {"stream": False}, True, False),
        ("gateway_non_stream", {"stream": False}, False, False),
        ("gateway_stream", {"stream": True}, False, False),
        ("gateway_cache_hit", {"stream": False, "prompts": 100, "zipf": 0.5}, False, False),
    ]
    for name, args, direct, _ in cases:
        m = mock(PRIMARY)
        s = mock(SECONDARY)
        g = None if direct else gateway()
        result = load(port=PRIMARY if direct else GW_PORT, conns=64, secs=15, **args)
        if g:
            stop(g)
        stop(m, s)
        out[name] = result
    return out


def cache():
    """Zipf-distributed prompts with a 50 ms upstream."""
    m = mock(PRIMARY, ttfb=50)
    s = mock(SECONDARY, ttfb=50)
    g = gateway()
    result = load(rate=1000, conns=256, secs=30, prompts=10000, zipf=1.0)
    result["upstream_calls"] = stats(PRIMARY)["requests"]
    result["cost_saved_micro_usd"] = metric_sum("gateway_cost_saved_micro_usd_total")
    result["cost_spent_micro_usd"] = metric_sum("gateway_cost_micro_usd_total")
    stop(g, m, s)
    return result


def concurrency():
    """Many long-lived streams at a realistic token rate."""
    conns = 2000
    m = mock(PRIMARY, ttfb=100, tokens=30, delay_us=100_000)
    s = mock(SECONDARY)
    g = gateway()
    idle = rss_kb(g.pid)
    proc = subprocess.Popen(
        ["taskset", "-c", LOAD_CPU, BIN / "loadgen", "--addr", f"127.0.0.1:{GW_PORT}", "--key", KEY,
         "--conns", str(conns), "--secs", "25", "--stream"], stdout=subprocess.PIPE, text=True)
    time.sleep(15)
    peak = rss_kb(g.pid)
    inflight = metric_sum("gateway_inflight_requests")
    result = json.loads(proc.communicate()[0])
    result.update(concurrent_streams=conns, observed_inflight=inflight, rss_idle_kb=idle, rss_loaded_kb=peak,
                  rss_per_stream_kb=round((peak - idle) / conns, 1), upstream_ttfb_ms=100)
    stop(g, m, s)
    return result


def overload():
    """Offered load at 3x capacity."""
    m = mock(PRIMARY, ttfb=100)
    s = mock(SECONDARY, ttfb=100)
    g = gateway({"admission": {"max_inflight": 100, "max_queue": 100, "queue_timeout_ms": 200}})
    result = load(rate=3000, conns=2000, secs=20)
    result["capacity_rps"] = 1000
    result["shed"] = metric_sum("gateway_admission_rejected_total")
    stop(g, m, s)
    return result


def outage_case(mode):
    """300 streams/s for 45 s. The primary fails at t=10 and recovers at t=25.

    `crash` kills the provider process, which also breaks streams in flight. The
    other modes only affect requests that arrive during the outage.
    """
    args = dict(ttfb=20, tokens=20, delay_us=2000)
    m = mock(PRIMARY, **args)
    s = mock(SECONDARY, **args)
    g = gateway({"timeouts": {"first_byte_ms": 1000}, "cache": {"enabled": False}})
    proc = subprocess.Popen(
        ["taskset", "-c", LOAD_CPU, BIN / "loadgen", "--addr", f"127.0.0.1:{GW_PORT}", "--key", KEY, "--rate", "300",
         "--conns", "800", "--secs", "45", "--stream", "--timeline"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    time.sleep(10)
    if mode == "crash":
        m.kill()
        m.wait()
    else:
        control(PRIMARY, mode=mode)
    time.sleep(15)
    if mode == "crash":
        m = mock(PRIMARY, **args)
    else:
        control(PRIMARY, mode="ok")
    out, err = proc.communicate()
    result = json.loads(out)
    result["timeline"] = [[int(a), b, int(c)] for a, b, c in (l.split("\t") for l in err.strip().splitlines())]
    result["outage_window_s"] = [10, 25]
    result["secondary_calls"] = stats(SECONDARY)["requests"]
    result["breaker_opened"] = metric_sum("gateway_breaker_opened_total")
    result["streams_broken_midway"] = metric_sum("gateway_stream_errors_total")
    served_by = {}
    for second, label, count in result["timeline"]:
        if second >= 25:
            provider = label.split(":")[1]
            served_by.setdefault(second, {}).setdefault(provider, 0)
            served_by[second][provider] += count
    majority = [sec for sec, v in sorted(served_by.items()) if v.get("primary", 0) > sum(v.values()) / 2]
    result["recovery_lag_s"] = majority[0] - 25 if majority else None
    stop(g, m, s)
    return result


def outage():
    return {mode: outage_case(mode) for mode in ["error500", "reset", "hang", "crash"]}


SCENARIOS = {f.__name__: f for f in [overhead, throughput, cache, concurrency, overload, outage]}


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / "results.json"
    results = json.loads(path.read_text()) if path.exists() else {}
    for name in sys.argv[1:] or SCENARIOS:
        print(f"== {name}", flush=True)
        results[name] = SCENARIOS[name]()
        path.write_text(json.dumps(results, indent=1))
        summary = {k: v for k, v in results[name].items() if k != "timeline"}
        print(json.dumps(summary)[:1200], flush=True)
    (OUT / "bench-config.json").unlink(missing_ok=True)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.default_int_handler)
    main()

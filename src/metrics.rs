//! Prometheus exposition with lock-free updates on existing series.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, RwLock};
use std::time::Duration;

fn label_key(names: &[&str], values: &[&str]) -> String {
    let mut out = String::with_capacity(48);
    for (i, (n, v)) in names.iter().zip(values).enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(n);
        out.push_str("=\"");
        for c in v.chars() {
            match c {
                '\\' => out.push_str("\\\\"),
                '"' => out.push_str("\\\""),
                '\n' => out.push_str("\\n"),
                _ => out.push(c),
            }
        }
        out.push('"');
    }
    out
}

/// A labelled counter or gauge.
pub struct Family {
    name: &'static str,
    help: &'static str,
    kind: &'static str,
    labels: &'static [&'static str],
    series: RwLock<BTreeMap<String, Arc<AtomicU64>>>,
}

impl Family {
    fn new(kind: &'static str, name: &'static str, help: &'static str, labels: &'static [&'static str]) -> Self {
        Self { name, help, kind, labels, series: RwLock::new(BTreeMap::new()) }
    }

    fn cell(&self, values: &[&str]) -> Arc<AtomicU64> {
        let key = label_key(self.labels, values);
        if let Some(c) = self.series.read().unwrap().get(&key) {
            return c.clone();
        }
        self.series.write().unwrap().entry(key).or_default().clone()
    }

    pub fn inc(&self, values: &[&str]) {
        self.add(values, 1);
    }

    pub fn add(&self, values: &[&str], n: u64) {
        self.cell(values).fetch_add(n, Relaxed);
    }

    pub fn set(&self, values: &[&str], n: u64) {
        self.cell(values).store(n, Relaxed);
    }

    pub fn get(&self, values: &[&str]) -> u64 {
        self.cell(values).load(Relaxed)
    }

    fn render(&self, out: &mut String) {
        let _ = writeln!(out, "# HELP {} {}\n# TYPE {} {}", self.name, self.help, self.name, self.kind);
        for (labels, v) in self.series.read().unwrap().iter() {
            if labels.is_empty() {
                let _ = writeln!(out, "{} {}", self.name, v.load(Relaxed));
            } else {
                let _ = writeln!(out, "{}{{{}}} {}", self.name, labels, v.load(Relaxed));
            }
        }
    }
}

struct HistSeries {
    buckets: Vec<AtomicU64>,
    sum_micros: AtomicU64,
    count: AtomicU64,
}

/// A labelled histogram. Bounds are given in seconds or a plain unit; values
/// are stored at micro-unit precision.
pub struct Histogram {
    name: &'static str,
    help: &'static str,
    labels: &'static [&'static str],
    bounds: &'static [f64],
    series: RwLock<BTreeMap<String, Arc<HistSeries>>>,
}

impl Histogram {
    fn new(name: &'static str, help: &'static str, labels: &'static [&'static str], bounds: &'static [f64]) -> Self {
        Self { name, help, labels, bounds, series: RwLock::new(BTreeMap::new()) }
    }

    pub fn observe(&self, values: &[&str], v: f64) {
        let key = label_key(self.labels, values);
        let existing = self.series.read().unwrap().get(&key).cloned();
        let series = existing.unwrap_or_else(|| {
            self.series
                .write()
                .unwrap()
                .entry(key)
                .or_insert_with(|| {
                    Arc::new(HistSeries {
                        buckets: (0..self.bounds.len()).map(|_| AtomicU64::new(0)).collect(),
                        sum_micros: AtomicU64::new(0),
                        count: AtomicU64::new(0),
                    })
                })
                .clone()
        });
        let idx = self.bounds.partition_point(|b| *b < v);
        if let Some(b) = series.buckets.get(idx) {
            b.fetch_add(1, Relaxed);
        }
        series.sum_micros.fetch_add((v * 1e6) as u64, Relaxed);
        series.count.fetch_add(1, Relaxed);
    }

    pub fn observe_duration(&self, values: &[&str], d: Duration) {
        self.observe(values, d.as_secs_f64());
    }

    fn render(&self, out: &mut String) {
        let _ = writeln!(out, "# HELP {} {}\n# TYPE {} histogram", self.name, self.help, self.name);
        for (labels, s) in self.series.read().unwrap().iter() {
            let sep = if labels.is_empty() { "" } else { "," };
            let mut cumulative = 0;
            for (bound, bucket) in self.bounds.iter().zip(&s.buckets) {
                cumulative += bucket.load(Relaxed);
                let _ = writeln!(out, "{}_bucket{{{labels}{sep}le=\"{bound}\"}} {cumulative}", self.name);
            }
            let count = s.count.load(Relaxed);
            let _ = writeln!(out, "{}_bucket{{{labels}{sep}le=\"+Inf\"}} {count}", self.name);
            let sum = s.sum_micros.load(Relaxed) as f64 / 1e6;
            let _ = writeln!(out, "{}_sum{{{labels}}} {sum}\n{}_count{{{labels}}} {count}", self.name, self.name);
        }
    }
}

const OVERHEAD_BUCKETS: &[f64] =
    &[0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1];
const LATENCY_BUCKETS: &[f64] =
    &[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0];
const TPS_BUCKETS: &[f64] = &[1.0, 5.0, 10.0, 20.0, 40.0, 80.0, 160.0, 320.0, 640.0, 1280.0];

/// Every metric the gateway exports.
pub struct Metrics {
    pub requests: Family,
    pub cache_requests: Family,
    pub coalesced: Family,
    pub rate_limited: Family,
    pub admission_rejected: Family,
    pub upstream_requests: Family,
    pub upstream_retries: Family,
    pub failovers: Family,
    pub tokens: Family,
    pub cost_micro_usd: Family,
    pub cost_saved_micro_usd: Family,
    pub stream_errors: Family,
    pub client_disconnects: Family,
    pub connections_rejected: Family,
    pub cache_entries: Family,
    pub cache_bytes: Family,
    pub cache_evictions: Family,
    pub queue_depth: Family,
    pub inflight: Family,
    pub connections: Family,
    pub breaker_state: Family,
    pub breaker_opened: Family,
    pub task_panics: Family,
    pub logs_dropped: Family,
    pub duration: Histogram,
    pub added_latency: Histogram,
    pub ttfb: Histogram,
    pub queue_wait: Histogram,
    pub upstream_ttfb: Histogram,
    pub stream_tps: Histogram,
}

impl Metrics {
    pub fn new() -> Self {
        let c = |n, h, l| Family::new("counter", n, h, l);
        let g = |n, h, l| Family::new("gauge", n, h, l);
        Self {
            requests: c(
                "gateway_requests_total",
                "Requests by tenant, model, status and cache result",
                &["tenant", "model", "status", "cache"],
            ),
            cache_requests: c("gateway_cache_requests_total", "Cache lookups by result", &["result"]),
            coalesced: c(
                "gateway_coalesced_requests_total",
                "Requests served by joining an identical in-flight request",
                &[],
            ),
            rate_limited: c("gateway_rate_limited_total", "Requests denied by per-key limits", &["tenant", "reason"]),
            admission_rejected: c(
                "gateway_admission_rejected_total",
                "Requests shed by admission control",
                &["reason"],
            ),
            upstream_requests: c(
                "gateway_upstream_requests_total",
                "Upstream attempts by provider and outcome",
                &["provider", "outcome"],
            ),
            upstream_retries: c("gateway_upstream_retries_total", "Retries against the same provider", &["provider"]),
            failovers: c("gateway_failovers_total", "Moves from one provider to the next", &["from"]),
            tokens: c(
                "gateway_tokens_total",
                "Tokens by tenant, provider and direction",
                &["tenant", "provider", "direction"],
            ),
            cost_micro_usd: c(
                "gateway_cost_micro_usd_total",
                "Estimated upstream spend in micro-USD",
                &["tenant", "provider"],
            ),
            cost_saved_micro_usd: c(
                "gateway_cost_saved_micro_usd_total",
                "Estimated spend avoided by the cache in micro-USD",
                &["tenant"],
            ),
            stream_errors: c(
                "gateway_stream_errors_total",
                "Streams that failed after the first byte",
                &["provider", "reason"],
            ),
            client_disconnects: c(
                "gateway_client_disconnects_total",
                "Clients that left before completion",
                &["phase"],
            ),
            connections_rejected: c(
                "gateway_connections_rejected_total",
                "Connections refused at the connection limit",
                &[],
            ),
            cache_entries: g("gateway_cache_entries", "Cached responses", &[]),
            cache_bytes: g("gateway_cache_bytes", "Cache memory accounted in bytes", &[]),
            cache_evictions: c("gateway_cache_evictions_total", "LRU evictions", &[]),
            queue_depth: g("gateway_queue_depth", "Requests waiting for admission", &[]),
            inflight: g("gateway_inflight_requests", "Requests holding an admission permit", &[]),
            connections: g("gateway_open_connections", "Open client connections", &[]),
            breaker_state: g("gateway_breaker_state", "0 closed, 1 half-open, 2 open", &["provider"]),
            breaker_opened: c("gateway_breaker_opened_total", "Times the breaker tripped", &["provider"]),
            task_panics: c("gateway_task_panics_total", "Panics caught at task boundaries", &[]),
            logs_dropped: c("gateway_logs_dropped_total", "Log lines dropped because the writer lagged", &[]),
            duration: Histogram::new(
                "gateway_request_duration_seconds",
                "End-to-end request duration",
                &["stream"],
                LATENCY_BUCKETS,
            ),
            added_latency: Histogram::new(
                "gateway_added_latency_seconds",
                "Time to first byte spent in the gateway, excluding queueing and upstream wait",
                &["stream"],
                OVERHEAD_BUCKETS,
            ),
            ttfb: Histogram::new("gateway_ttfb_seconds", "Time to first response byte", &["stream"], LATENCY_BUCKETS),
            queue_wait: Histogram::new(
                "gateway_queue_wait_seconds",
                "Time waiting for admission",
                &[],
                LATENCY_BUCKETS,
            ),
            upstream_ttfb: Histogram::new(
                "gateway_upstream_ttfb_seconds",
                "Upstream time to first byte",
                &["provider"],
                LATENCY_BUCKETS,
            ),
            stream_tps: Histogram::new(
                "gateway_stream_tokens_per_second",
                "Output tokens per second per stream",
                &["provider"],
                TPS_BUCKETS,
            ),
        }
    }

    /// Renders all metrics in Prometheus text format.
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(16 * 1024);
        for f in [
            &self.requests,
            &self.cache_requests,
            &self.coalesced,
            &self.rate_limited,
            &self.admission_rejected,
            &self.upstream_requests,
            &self.upstream_retries,
            &self.failovers,
            &self.tokens,
            &self.cost_micro_usd,
            &self.cost_saved_micro_usd,
            &self.stream_errors,
            &self.client_disconnects,
            &self.connections_rejected,
            &self.cache_entries,
            &self.cache_bytes,
            &self.cache_evictions,
            &self.queue_depth,
            &self.inflight,
            &self.connections,
            &self.breaker_state,
            &self.breaker_opened,
            &self.task_panics,
            &self.logs_dropped,
        ] {
            f.render(&mut out);
        }
        for h in
            [&self.duration, &self.added_latency, &self.ttfb, &self.queue_wait, &self.upstream_ttfb, &self.stream_tps]
        {
            h.render(&mut out);
        }
        out
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_buckets_are_cumulative() {
        let m = Metrics::new();
        m.duration.observe(&["true"], 0.007);
        m.duration.observe(&["true"], 0.04);
        m.duration.observe(&["true"], 1000.0);
        let text = m.render();
        assert!(text.contains("gateway_request_duration_seconds_bucket{stream=\"true\",le=\"0.01\"} 1"));
        assert!(text.contains("gateway_request_duration_seconds_bucket{stream=\"true\",le=\"0.05\"} 2"));
        assert!(text.contains("gateway_request_duration_seconds_bucket{stream=\"true\",le=\"+Inf\"} 3"));
        assert!(text.contains("gateway_request_duration_seconds_count{stream=\"true\"} 3"));
    }

    #[test]
    fn label_values_are_escaped() {
        let m = Metrics::new();
        m.failovers.inc(&["a\"b\\c\n"]);
        assert!(m.render().contains(r#"gateway_failovers_total{from="a\"b\\c\n"} 1"#));
    }
}

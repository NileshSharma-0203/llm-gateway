//! Load generator. Open-loop mode schedules requests on a fixed timetable and
//! measures from the scheduled time, so a stall cannot hide its own queueing
//! delay (the coordinated-omission problem).

use llm_gateway::http::{self, ClientConn};
use llm_gateway::rt::{self, net::TcpConn};
use llm_gateway::util::fast_rand;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SUB_BITS: u32 = 5;

/// Log-linear histogram with about 3% resolution.
struct Hist {
    counts: Vec<AtomicU64>,
    total: AtomicU64,
}

impl Hist {
    fn new() -> Self {
        Self { counts: (0..(64usize << SUB_BITS)).map(|_| AtomicU64::new(0)).collect(), total: AtomicU64::new(0) }
    }

    fn index(v: u64) -> usize {
        if v < (1 << SUB_BITS) {
            return v as usize;
        }
        let msb = 63 - v.leading_zeros();
        let shift = msb - SUB_BITS;
        (((shift + 1) as usize) << SUB_BITS) + ((v >> shift) as usize & ((1 << SUB_BITS) - 1))
    }

    fn value(i: usize) -> u64 {
        let bucket = i >> SUB_BITS;
        let sub = (i & ((1 << SUB_BITS) - 1)) as u64;
        if bucket == 0 {
            return sub;
        }
        let shift = (bucket - 1) as u32;
        (((1u64 << SUB_BITS) + sub) << shift) + (1u64 << shift) / 2
    }

    fn record(&self, micros: u64) {
        self.counts[Self::index(micros)].fetch_add(1, Relaxed);
        self.total.fetch_add(1, Relaxed);
    }

    fn quantile(&self, q: f64) -> f64 {
        let total = self.total.load(Relaxed);
        if total == 0 {
            return 0.0;
        }
        let target = ((total as f64 * q).ceil() as u64).max(1);
        let mut seen = 0;
        for (i, c) in self.counts.iter().enumerate() {
            seen += c.load(Relaxed);
            if seen >= target {
                return Self::value(i) as f64;
            }
        }
        0.0
    }
}

struct Opts {
    addr: SocketAddr,
    key: String,
    model: String,
    conns: usize,
    workers: usize,
    secs: u64,
    rate: f64,
    stream: bool,
    prompts: u64,
    zipf: f64,
    max_tokens: u64,
    path: String,
    timeline: bool,
}

struct Shared {
    opts: Opts,
    ttfb: Hist,
    total: Hist,
    total_ok: Hist,
    next_slot: AtomicU64,
    start: Instant,
    stop: AtomicBool,
    ok: AtomicU64,
    errors: AtomicU64,
    cache_hits: AtomicU64,
    bytes: AtomicU64,
    statuses: Mutex<BTreeMap<u16, u64>>,
    /// (second, label) -> count, where label is "<status>:<provider>:<cache>".
    timeline: Mutex<BTreeMap<(u64, String), u64>>,
    zipf_cdf: Vec<f64>,
}

fn build_cdf(n: u64, s: f64) -> Vec<f64> {
    let mut acc = 0.0;
    let mut cdf: Vec<f64> = (1..=n)
        .map(|k| {
            acc += 1.0 / (k as f64).powf(s);
            acc
        })
        .collect();
    let total = acc;
    cdf.iter_mut().for_each(|v| *v /= total);
    cdf
}

fn pick_prompt(sh: &Shared) -> u64 {
    if sh.opts.prompts == 0 {
        return fast_rand();
    }
    let u = (fast_rand() >> 11) as f64 / (1u64 << 53) as f64;
    sh.zipf_cdf.partition_point(|c| *c < u) as u64
}

fn request_bytes(sh: &Shared) -> Vec<u8> {
    let o = &sh.opts;
    let temp = if o.prompts == 0 { "0.7" } else { "0" };
    let body = format!(
        "{{\"model\":\"{}\",\"temperature\":{temp},\"stream\":{},\"max_tokens\":{},\"messages\":[{{\"role\":\"user\",\"content\":\"prompt number {}: summarize the release notes for this version\"}}]}}",
        o.model, o.stream, o.max_tokens, pick_prompt(sh)
    );
    format!(
        "POST {} HTTP/1.1\r\nhost: {}\r\nauthorization: Bearer {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        o.path, o.addr, o.key, body.len(), body
    )
    .into_bytes()
}

async fn one_request(sh: &Shared, conn: &mut ClientConn, due: Instant) -> Result<(), ()> {
    let req = request_bytes(sh);
    conn.send(&req).await.map_err(|_| ())?;
    let head = conn.read_head(false).await.map_err(|_| ())?;
    let mut ttfb = due.elapsed();
    let mut buf = Vec::with_capacity(32 * 1024);
    let mut bytes = 0u64;
    let mut first = true;
    loop {
        buf.clear();
        let more = conn.read_body(&mut buf).await.map_err(|_| ())?;
        if first && !buf.is_empty() {
            ttfb = due.elapsed();
            first = false;
        }
        bytes += buf.len() as u64;
        if !more {
            break;
        }
    }
    let total = due.elapsed();
    sh.bytes.fetch_add(bytes, Relaxed);
    sh.ttfb.record(ttfb.as_micros() as u64);
    sh.total.record(total.as_micros() as u64);
    *sh.statuses.lock().unwrap().entry(head.status).or_default() += 1;
    if (200..300).contains(&head.status) {
        sh.ok.fetch_add(1, Relaxed);
        sh.total_ok.record(total.as_micros() as u64);
    } else {
        sh.errors.fetch_add(1, Relaxed);
    }
    let cache = http::header_value(&head.headers, "x-cache").unwrap_or("-");
    if cache == "HIT" {
        sh.cache_hits.fetch_add(1, Relaxed);
    }
    if sh.opts.timeline {
        let provider = http::header_value(&head.headers, "x-gateway-provider").unwrap_or("-");
        let second = sh.start.elapsed().as_secs();
        *sh.timeline.lock().unwrap().entry((second, format!("{}:{provider}:{cache}", head.status))).or_default() += 1;
    }
    Ok(())
}

async fn connection_task(sh: Arc<Shared>) {
    let mut conn: Option<ClientConn> = None;
    loop {
        if sh.stop.load(Relaxed) {
            return;
        }
        let due = if sh.opts.rate > 0.0 {
            let slot = sh.next_slot.fetch_add(1, Relaxed);
            let due = sh.start + Duration::from_secs_f64(slot as f64 / sh.opts.rate);
            if due > sh.start + Duration::from_secs(sh.opts.secs) {
                return;
            }
            rt::sleep_until(due).await;
            due
        } else {
            Instant::now()
        };
        if conn.is_none() {
            match TcpConn::connect(sh.opts.addr).await {
                Ok(c) => conn = Some(ClientConn::new(c)),
                Err(_) => {
                    sh.errors.fetch_add(1, Relaxed);
                    *sh.statuses.lock().unwrap().entry(0).or_default() += 1;
                    rt::sleep(Duration::from_millis(5)).await;
                    continue;
                }
            }
        }
        let c = conn.as_mut().expect("connected above");
        if one_request(&sh, c, due).await.is_err() {
            sh.errors.fetch_add(1, Relaxed);
            *sh.statuses.lock().unwrap().entry(0).or_default() += 1;
            conn = None;
        } else if !c.reusable() {
            conn = None;
        }
    }
}

fn arg<T: std::str::FromStr>(args: &[String], name: &str, default: T) -> T {
    args.windows(2).find(|w| w[0] == name).and_then(|w| w[1].parse().ok()).unwrap_or(default)
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let opts = Opts {
        addr: arg(&args, "--addr", "127.0.0.1:8080".parse().unwrap()),
        key: arg(&args, "--key", "test-key".to_string()),
        model: arg(&args, "--model", "fast".to_string()),
        conns: arg(&args, "--conns", 64),
        workers: arg(&args, "--workers", 1),
        secs: arg(&args, "--secs", 10),
        rate: arg(&args, "--rate", 0.0),
        stream: args.iter().any(|a| a == "--stream"),
        prompts: arg(&args, "--prompts", 0),
        zipf: arg(&args, "--zipf", 1.0),
        max_tokens: arg(&args, "--max-tokens", 64),
        path: arg(&args, "--path", "/v1/chat/completions".to_string()),
        timeline: args.iter().any(|a| a == "--timeline"),
    };
    let zipf_cdf = if opts.prompts > 0 { build_cdf(opts.prompts, opts.zipf) } else { Vec::new() };
    let (conns, workers, secs, rate) = (opts.conns, opts.workers.max(1), opts.secs, opts.rate);
    let shared = Arc::new(Shared {
        opts,
        ttfb: Hist::new(),
        total: Hist::new(),
        total_ok: Hist::new(),
        next_slot: AtomicU64::new(0),
        start: Instant::now(),
        stop: AtomicBool::new(false),
        ok: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        cache_hits: AtomicU64::new(0),
        bytes: AtomicU64::new(0),
        statuses: Mutex::new(BTreeMap::new()),
        timeline: Mutex::new(BTreeMap::new()),
        zipf_cdf,
    });
    for w in 0..workers {
        let sh = shared.clone();
        let share = conns / workers + usize::from(w < conns % workers);
        std::thread::spawn(move || {
            rt::block_on(async move {
                for _ in 0..share {
                    let s = sh.clone();
                    rt::spawn(connection_task(s));
                }
                std::future::pending::<()>().await;
            });
        });
    }
    std::thread::sleep(Duration::from_secs(secs));
    shared.stop.store(true, Relaxed);
    let measured = shared.start.elapsed().as_secs_f64();
    let s = &shared;
    let ok = s.ok.load(Relaxed);
    let errors = s.errors.load(Relaxed);
    let hits = s.cache_hits.load(Relaxed);
    let completed = s.total.total.load(Relaxed);
    let statuses: Vec<String> = s.statuses.lock().unwrap().iter().map(|(k, v)| format!("\"{k}\":{v}")).collect();
    let ms = |h: &Hist, q: f64| h.quantile(q) / 1000.0;
    println!(
        "{{\"mode\":\"{}\",\"target_rps\":{rate},\"conns\":{conns},\"secs\":{measured:.2},\"rps\":{:.1},\"ok\":{ok},\"errors\":{errors},\"cache_hit_rate\":{:.4},\"mb_per_s\":{:.2},\"statuses\":{{{}}},\
\"ttfb_ms\":{{\"p50\":{:.3},\"p90\":{:.3},\"p99\":{:.3},\"p999\":{:.3}}},\"total_ms\":{{\"p50\":{:.3},\"p90\":{:.3},\"p99\":{:.3},\"p999\":{:.3}}},\"total_ok_ms\":{{\"p50\":{:.3},\"p99\":{:.3}}}}}",
        if rate > 0.0 { "open" } else { "closed" },
        completed as f64 / measured,
        if completed > 0 { hits as f64 / completed as f64 } else { 0.0 },
        s.bytes.load(Relaxed) as f64 / measured / 1e6,
        statuses.join(","),
        ms(&s.ttfb, 0.5), ms(&s.ttfb, 0.9), ms(&s.ttfb, 0.99), ms(&s.ttfb, 0.999),
        ms(&s.total, 0.5), ms(&s.total, 0.9), ms(&s.total, 0.99), ms(&s.total, 0.999),
        ms(&s.total_ok, 0.5),
        ms(&s.total_ok, 0.99),
    );
    if s.opts.timeline {
        for ((sec, label), n) in s.timeline.lock().unwrap().iter() {
            eprintln!("{sec}\t{label}\t{n}");
        }
    }
    std::process::exit(0);
}

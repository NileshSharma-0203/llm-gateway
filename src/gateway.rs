//! The request pipeline: authenticate, limit, serve from cache, queue, proxy
//! with retries and failover, then account for what happened.

use crate::admission::{Admission, Reject};
use crate::breaker::{Breaker, RetryBudget, Ticket};
use crate::cache::{self, Cache, Entry, FlightRole, Lookup};
use crate::config::{Config, ProviderKind, Target};
use crate::http::{self, BufConn, ClientConn, Limits, Request};
use crate::json::{self, Value};
use crate::limiter::{Deny, Remaining, Reservation, TenantLimiter};
use crate::metrics::Metrics;
use crate::rt::{
    self,
    net::{Acceptor, TcpConn},
    sys, Either,
};
use crate::sha256;
use crate::sse::SseBuffer;
use crate::telemetry::{new_span_id, LogLine, Logger, Trace};
use crate::translate::{self, StreamTranslator, Usage};
use crate::util::{fast_rand, unix_secs};
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::pending;
use std::io;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering::Relaxed, Ordering::SeqCst};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

const POOL_IDLE: Duration = Duration::from_secs(60);
const MAX_ERROR_BODY: usize = 64 * 1024;
const MAX_FULL_BODY: usize = 64 * 1024 * 1024;
const DNS_REFRESH: Duration = Duration::from_secs(30);

/// One upstream provider and its health state.
pub struct Provider {
    idx: usize,
    cfg: crate::config::Provider,
    breaker: Arc<Breaker>,
    budget: RetryBudget,
    addrs: RwLock<Arc<Vec<SocketAddr>>>,
}

struct ResolvedTarget {
    provider: Arc<Provider>,
    target: Target,
}

struct Route {
    targets: Vec<ResolvedTarget>,
}

/// An authenticated API consumer.
pub struct Tenant {
    name: Arc<str>,
    limiter: Arc<TenantLimiter>,
    max_concurrent: usize,
    models: Vec<String>,
}

impl Tenant {
    fn allows(&self, alias: &str) -> bool {
        self.models.iter().any(|m| m == "*" || m == alias)
    }
}

/// Shared gateway state.
pub struct Gateway {
    cfg: Config,
    keys: HashMap<[u8; 32], Arc<Tenant>>,
    tenants: Vec<Arc<Tenant>>,
    providers: Vec<Arc<Provider>>,
    routes: HashMap<String, Route>,
    admission: Arc<Admission>,
    cache: Arc<Cache>,
    pub metrics: Metrics,
    log: Logger,
    epoch: Instant,
    draining: AtomicBool,
    active_requests: AtomicI64,
    connections: AtomicI64,
}

thread_local! {
    static POOL: RefCell<HashMap<usize, Vec<(ClientConn, Instant)>>> = RefCell::new(HashMap::new());
}

fn pool_take(idx: usize) -> Option<ClientConn> {
    POOL.with(|p| {
        let mut pool = p.borrow_mut();
        let conns = pool.get_mut(&idx)?;
        while let Some((conn, since)) = conns.pop() {
            if conn.reusable() && since.elapsed() < POOL_IDLE {
                return Some(conn);
            }
        }
        None
    })
}

fn pool_put(idx: usize, conn: ClientConn, cap: usize) {
    if !conn.reusable() {
        return;
    }
    POOL.with(|p| {
        let mut pool = p.borrow_mut();
        let conns = pool.entry(idx).or_default();
        if conns.len() < cap {
            conns.push((conn, Instant::now()));
        }
    });
}

/// Per-request state, reported once at the end.
struct Ctx {
    start: Instant,
    trace: Trace,
    request_id: String,
    tenant: String,
    model: String,
    provider: String,
    status: u16,
    cache: &'static str,
    outcome: &'static str,
    stream: bool,
    attempts: u32,
    queue_wait: Duration,
    upstream_wait: Duration,
    ttfb: Option<Duration>,
    added: Option<Duration>,
    prompt_tokens: u64,
    completion_tokens: u64,
    cost_micro_usd: u64,
    remaining: Option<Remaining>,
    limits: (u64, u64),
}

impl Ctx {
    fn new(req: &Request) -> Self {
        let trace = Trace::from_header(req.header("traceparent"));
        let request_id = req
            .header("x-request-id")
            .filter(|v| !v.is_empty() && v.len() <= 128 && v.bytes().all(|b| b.is_ascii_graphic()))
            .map(String::from)
            .unwrap_or_else(|| format!("req_{}", trace.span_id));
        Self {
            start: Instant::now(),
            trace,
            request_id,
            tenant: "-".into(),
            model: "-".into(),
            provider: "-".into(),
            status: 0,
            cache: "none",
            outcome: "ok",
            stream: false,
            attempts: 0,
            queue_wait: Duration::ZERO,
            upstream_wait: Duration::ZERO,
            ttfb: None,
            added: None,
            prompt_tokens: 0,
            completion_tokens: 0,
            cost_micro_usd: 0,
            remaining: None,
            limits: (0, 0),
        }
    }

    /// Records first-byte timing, separating gateway time from waiting.
    fn mark_first_byte(&mut self) {
        if self.ttfb.is_none() {
            let ttfb = self.start.elapsed();
            self.ttfb = Some(ttfb);
            self.added = Some(ttfb.saturating_sub(self.queue_wait).saturating_sub(self.upstream_wait));
        }
    }

    fn common_headers(&self, out: &mut Vec<u8>) {
        http::push_header(out, "x-request-id", &self.request_id);
        http::push_header(out, "traceparent", &self.trace.header_for(&self.trace.span_id));
        if self.provider != "-" {
            http::push_header(out, "x-gateway-provider", &self.provider);
        }
        if self.attempts > 0 {
            http::push_header(out, "x-gateway-attempts", &self.attempts.to_string());
        }
        http::push_header(
            out,
            "x-cache",
            match self.cache {
                "hit" | "coalesced" => "HIT",
                "stale" => "STALE",
                "miss" => "MISS",
                _ => "BYPASS",
            },
        );
        if let Some(r) = self.remaining {
            http::push_header(out, "x-ratelimit-limit-requests", &self.limits.0.to_string());
            http::push_header(out, "x-ratelimit-remaining-requests", &r.requests.to_string());
            http::push_header(out, "x-ratelimit-limit-tokens", &self.limits.1.to_string());
            http::push_header(out, "x-ratelimit-remaining-tokens", &r.tokens.to_string());
        }
    }
}

fn error_json(message: &str, kind: &str, code: &str) -> Vec<u8> {
    Value::Obj(vec![(
        "error".into(),
        Value::Obj(vec![
            ("message".into(), Value::str(message)),
            ("type".into(), Value::str(kind)),
            ("code".into(), Value::str(code)),
        ]),
    )])
    .to_bytes()
}

/// Why one upstream attempt did not produce a usable response.
enum AttemptError {
    /// The request itself was rejected; do not try anywhere else.
    Client { status: u16, body: Vec<u8> },
    /// Transient; may retry on this provider, then move on.
    Retryable { label: &'static str },
    /// This provider cannot serve the request now; move on immediately.
    NextProvider { label: &'static str, trip: bool },
}

enum Success {
    Full { body: Vec<u8>, usage: Usage },
    Stream(Box<LiveStream>),
}

struct LiveStream {
    conn: ClientConn,
    sse: SseBuffer,
    translator: StreamTranslator,
    first: Vec<u8>,
    done: bool,
}

struct Delivered {
    success: Success,
    target: usize,
    prices: (f64, f64),
}

enum Outcome {
    Delivered(Delivered),
    ClientError { status: u16, body: Vec<u8> },
    Exhausted { last: &'static str },
}

struct Chat {
    body: Value,
    alias: String,
    stream: bool,
    wants_usage: bool,
    est_prompt: u64,
    max_tokens: u64,
    anthropic_blocked: bool,
}

/// The client connection a response goes to.
#[derive(Clone, Copy)]
struct Client<'a> {
    io: &'a BufConn,
    keep: bool,
}

struct ActiveGuard<'a>(&'a AtomicI64);

impl<'a> ActiveGuard<'a> {
    fn new(c: &'a AtomicI64) -> Self {
        c.fetch_add(1, SeqCst);
        Self(c)
    }
}

impl Drop for ActiveGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, SeqCst);
    }
}

impl Gateway {
    /// Builds gateway state from validated configuration.
    pub fn new(cfg: Config, log: Logger) -> Arc<Self> {
        let providers: Vec<Arc<Provider>> = cfg
            .providers
            .iter()
            .enumerate()
            .map(|(idx, p)| {
                Arc::new(Provider {
                    idx,
                    cfg: p.clone(),
                    breaker: Breaker::new(
                        cfg.breaker.failure_threshold,
                        Duration::from_millis(cfg.breaker.open_ms),
                        Duration::from_millis(cfg.breaker.max_open_ms),
                        cfg.breaker.half_open_probes,
                    ),
                    budget: RetryBudget::new(cfg.retry.budget_ratio, cfg.retry.budget_min),
                    addrs: RwLock::new(Arc::new(Vec::new())),
                })
            })
            .collect();
        let mut routes = HashMap::new();
        for m in &cfg.models {
            let targets = m
                .targets
                .iter()
                .map(|t| ResolvedTarget {
                    provider: providers.iter().find(|p| p.cfg.name == t.provider).expect("validated").clone(),
                    target: t.clone(),
                })
                .collect();
            routes.insert(m.alias.clone(), Route { targets });
        }
        let mut keys = HashMap::new();
        let mut tenants = Vec::new();
        for t in &cfg.tenants {
            let tenant = Arc::new(Tenant {
                name: Arc::from(t.name.as_str()),
                limiter: TenantLimiter::new(t.rpm, t.tpm, t.daily_tokens, t.burst_secs),
                max_concurrent: t.max_concurrent,
                models: t.models.clone(),
            });
            for k in &t.key_sha256 {
                keys.insert(*k, tenant.clone());
            }
            tenants.push(tenant);
        }
        let admission = Admission::new(
            cfg.admission.max_inflight,
            cfg.admission.max_queue,
            cfg.admission.max_queue_per_tenant,
            Duration::from_millis(cfg.admission.queue_timeout_ms),
        );
        let cache = Arc::new(Cache::new(
            cfg.cache.max_bytes,
            Duration::from_secs(cfg.cache.ttl_secs),
            Duration::from_secs(cfg.cache.stale_if_error_secs),
            cfg.cache.max_entry_bytes,
        ));
        Arc::new(Self {
            keys,
            tenants,
            providers,
            routes,
            admission,
            cache,
            metrics: Metrics::new(),
            log,
            epoch: Instant::now(),
            draining: AtomicBool::new(false),
            active_requests: AtomicI64::new(0),
            connections: AtomicI64::new(0),
            cfg,
        })
    }

    fn resolve_all(&self) {
        for p in &self.providers {
            match (p.cfg.host.as_str(), p.cfg.port).to_socket_addrs() {
                Ok(found) => {
                    let list: Vec<SocketAddr> = found.collect();
                    if !list.is_empty() {
                        *p.addrs.write().unwrap() = Arc::new(list);
                    }
                }
                Err(e) => self.log.emit(
                    LogLine::new("dns_resolution_failed").str("provider", &p.cfg.name).str("error", &e.to_string()),
                ),
            }
        }
    }

    /// Stops accepting work. In-flight requests continue.
    pub fn begin_drain(&self) {
        self.draining.store(true, SeqCst);
    }

    /// Requests currently being handled.
    pub fn active_requests(&self) -> i64 {
        self.active_requests.load(SeqCst)
    }

    /// Writes per-tenant quota usage atomically.
    pub fn save_quota(&self, path: &str) -> io::Result<()> {
        let items = self
            .tenants
            .iter()
            .map(|t| {
                let (day, used) = t.limiter.used_today();
                (t.name.to_string(), Value::Arr(vec![Value::num(day), Value::num(used)]))
            })
            .collect();
        let tmp = format!("{path}.tmp");
        std::fs::write(&tmp, Value::Obj(items).to_bytes())?;
        std::fs::rename(tmp, path)
    }

    /// Restores quota usage. A missing or corrupt file starts from zero.
    pub fn load_quota(&self, path: &str) {
        let Ok(raw) = std::fs::read(path) else { return };
        let Ok(doc) = json::parse(&raw) else { return };
        for t in &self.tenants {
            if let Some([day, used]) = doc.get(&t.name).and_then(Value::as_arr) {
                if let (Some(day), Some(used)) = (day.as_u64(), used.as_u64()) {
                    t.limiter.restore(day, used);
                }
            }
        }
    }

    fn render_metrics(&self) -> String {
        let m = &self.metrics;
        let (entries, bytes) = self.cache.stats();
        m.cache_entries.set(&[], entries as u64);
        m.cache_bytes.set(&[], bytes as u64);
        m.cache_evictions.set(&[], self.cache.evictions.load(Relaxed));
        let (running, queued) = self.admission.depth();
        m.inflight.set(&[], running as u64);
        m.queue_depth.set(&[], queued as u64);
        m.connections.set(&[], self.connections.load(Relaxed).max(0) as u64);
        m.task_panics.set(&[], rt::TASK_PANICS.load(Relaxed));
        m.logs_dropped.set(&[], self.log.dropped.load(Relaxed));
        for p in &self.providers {
            m.breaker_state.set(&[&p.cfg.name], p.breaker.state_code() as u64);
            m.breaker_opened.set(&[&p.cfg.name], p.breaker.opened_total.load(Relaxed) as u64);
        }
        m.render()
    }

    async fn respond(
        &self,
        client: Client<'_>,
        ctx: &mut Ctx,
        status: u16,
        content_type: &str,
        body: &[u8],
        extra: &[(&str, &str)],
    ) -> bool {
        let Client { io, keep } = client;
        ctx.status = status;
        ctx.mark_first_byte();
        let mut out = Vec::with_capacity(body.len() + 512);
        http::start_head(status, keep, &mut out);
        http::push_header(&mut out, "content-type", content_type);
        http::push_header(&mut out, "content-length", &body.len().to_string());
        ctx.common_headers(&mut out);
        for (k, v) in extra {
            http::push_header(&mut out, k, v);
        }
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(body);
        io.conn.write_all(&out).await.is_ok() && keep
    }

    async fn plain(&self, io: &BufConn, status: u16, content_type: &str, body: &[u8], keep: bool) -> bool {
        let mut out = Vec::with_capacity(body.len() + 128);
        http::start_head(status, keep, &mut out);
        http::push_header(&mut out, "content-type", content_type);
        http::push_header(&mut out, "content-length", &body.len().to_string());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(body);
        io.conn.write_all(&out).await.is_ok() && keep
    }

    fn authenticate(&self, req: &Request) -> Option<Arc<Tenant>> {
        let header = req.header("authorization")?;
        let token = header.strip_prefix("Bearer ").or_else(|| header.strip_prefix("bearer "))?;
        self.keys.get(&sha256::digest(token.trim().as_bytes())).cloned()
    }

    async fn route(self: &Arc<Self>, req: &Request, io: &BufConn, keep: bool) -> bool {
        match (req.method.as_str(), req.path.as_str()) {
            ("POST", "/v1/chat/completions") => self.chat(req, io, keep).await,
            ("GET", "/v1/models") => self.list_models(req, io, keep).await,
            ("GET", "/healthz") => self.plain(io, 200, "text/plain", b"ok\n", keep).await,
            ("GET", "/readyz") => {
                if self.draining.load(SeqCst) {
                    self.plain(io, 503, "text/plain", b"draining\n", false).await
                } else {
                    self.plain(io, 200, "text/plain", b"ready\n", keep).await
                }
            }
            ("GET", "/metrics") => {
                if let Some(expected) = &self.cfg.metrics_token {
                    let supplied = req.header("authorization").and_then(|h| h.strip_prefix("Bearer "));
                    if supplied.map(|s| sha256::digest(s.as_bytes())) != Some(sha256::digest(expected.as_bytes())) {
                        return self.plain(io, 401, "text/plain", b"unauthorized\n", keep).await;
                    }
                }
                let text = self.render_metrics();
                self.plain(io, 200, "text/plain; version=0.0.4", text.as_bytes(), keep).await
            }
            (_, "/v1/chat/completions" | "/v1/models" | "/healthz" | "/readyz" | "/metrics") => {
                let body = error_json("method not allowed", "invalid_request_error", "method_not_allowed");
                self.plain(io, 405, "application/json", &body, keep).await
            }
            _ => {
                let body = error_json("unknown path", "invalid_request_error", "not_found");
                self.plain(io, 404, "application/json", &body, keep).await
            }
        }
    }

    async fn list_models(&self, req: &Request, io: &BufConn, keep: bool) -> bool {
        let Some(tenant) = self.authenticate(req) else {
            let body = error_json("invalid API key", "authentication_error", "invalid_api_key");
            return self.plain(io, 401, "application/json", &body, keep).await;
        };
        let mut aliases: Vec<&String> = self.routes.keys().filter(|a| tenant.allows(a)).collect();
        aliases.sort();
        let data = aliases
            .into_iter()
            .map(|a| {
                Value::Obj(vec![
                    ("id".into(), Value::str(a.as_str())),
                    ("object".into(), Value::str("model")),
                    ("owned_by".into(), Value::str("gateway")),
                ])
            })
            .collect();
        let doc = Value::Obj(vec![("object".into(), Value::str("list")), ("data".into(), Value::Arr(data))]);
        self.plain(io, 200, "application/json", &doc.to_bytes(), keep).await
    }

    async fn chat(self: &Arc<Self>, req: &Request, io: &BufConn, keep: bool) -> bool {
        let _active = ActiveGuard::new(&self.active_requests);
        let mut ctx = Ctx::new(req);
        let cont = self.chat_inner(req, io, keep, &mut ctx).await;
        self.finish(&ctx);
        cont
    }

    async fn reject(
        &self,
        client: Client<'_>,
        ctx: &mut Ctx,
        status: u16,
        (kind, code): (&str, &str),
        msg: &str,
        extra: &[(&str, &str)],
    ) -> bool {
        ctx.outcome = "rejected";
        let body = error_json(msg, kind, code);
        self.respond(client, ctx, status, "application/json", &body, extra).await
    }

    fn parse_chat(&self, req: &Request) -> Result<Chat, String> {
        let body = json::parse(&req.body).map_err(|e| format!("invalid JSON body: {e}"))?;
        if !matches!(body, Value::Obj(_)) {
            return Err("request body must be a JSON object".into());
        }
        let alias = body.get("model").and_then(Value::as_str).ok_or("\"model\" is required")?.to_string();
        let messages = body.get("messages").and_then(Value::as_arr).ok_or("\"messages\" must be an array")?;
        if messages.is_empty() {
            return Err("\"messages\" must not be empty".into());
        }
        let stream = match body.get("stream") {
            None | Some(Value::Null) => false,
            Some(Value::Bool(b)) => *b,
            Some(_) => return Err("\"stream\" must be a boolean".into()),
        };
        let wants_usage =
            body.get("stream_options").and_then(|o| o.get("include_usage")).and_then(Value::as_bool) == Some(true);
        let max_tokens = body
            .get("max_tokens")
            .or_else(|| body.get("max_completion_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(self.cfg.max_tokens_default);
        let anthropic_blocked = translate::anthropic_unsupported(&body);
        Ok(Chat {
            est_prompt: (req.body.len() as u64 / 4).max(1),
            body,
            alias,
            stream,
            wants_usage,
            max_tokens,
            anthropic_blocked,
        })
    }

    fn cache_plan(&self, req: &Request, chat: &Chat, tenant: &Tenant) -> Option<cache::Key> {
        let cfg = &self.cfg.cache;
        if !cfg.enabled {
            return None;
        }
        if let Some(cc) = req.header("cache-control") {
            let cc = cc.to_ascii_lowercase();
            if cc.contains("no-cache") || cc.contains("no-store") {
                return None;
            }
        }
        if chat.body.get("n").and_then(Value::as_u64).is_some_and(|n| n > 1) {
            return None;
        }
        let forced = req.header("x-gateway-cache").is_some_and(|v| v.eq_ignore_ascii_case("force"));
        let deterministic =
            chat.body.get("temperature").and_then(Value::as_f64) == Some(0.0) || chat.body.get("seed").is_some();
        if cfg.deterministic_only && !deterministic && !forced {
            return None;
        }
        let mut normalized = chat.body.clone();
        normalized.remove("user");
        normalized.remove("stream_options");
        normalized.set("stream", Value::Bool(chat.stream));
        let mut material = Vec::with_capacity(512);
        if !cfg.share_across_tenants {
            material.extend_from_slice(tenant.name.as_bytes());
        }
        material.push(0);
        material.push(chat.wants_usage as u8);
        material.extend_from_slice(&normalized.to_canonical());
        Some(sha256::digest(&material))
    }

    async fn serve_cached(
        &self,
        client: Client<'_>,
        ctx: &mut Ctx,
        tenant: &Tenant,
        entry: &Entry,
        stream: bool,
    ) -> bool {
        ctx.provider = entry.provider.clone();
        ctx.prompt_tokens = entry.prompt_tokens;
        ctx.completion_tokens = entry.completion_tokens;
        self.metrics.cost_saved_micro_usd.add(&[&tenant.name], entry.cost_micro_usd);
        let content_type = if stream { "text/event-stream" } else { "application/json" };
        self.respond(client, ctx, 200, content_type, &entry.body, &[]).await
    }

    async fn chat_inner(self: &Arc<Self>, req: &Request, io: &BufConn, keep: bool, ctx: &mut Ctx) -> bool {
        let client = Client { io, keep };
        let Some(tenant) = self.authenticate(req) else {
            return self
                .reject(
                    client,
                    ctx,
                    401,
                    ("authentication_error", "invalid_api_key"),
                    "invalid API key",
                    &[("www-authenticate", "Bearer")],
                )
                .await;
        };
        ctx.tenant = tenant.name.to_string();

        let chat = match self.parse_chat(req) {
            Ok(c) => c,
            Err(msg) => {
                return self.reject(client, ctx, 400, ("invalid_request_error", "invalid_request"), &msg, &[]).await
            }
        };
        ctx.model = chat.alias.clone();
        ctx.stream = chat.stream;

        let Some(route) = self.routes.get(&chat.alias) else {
            return self
                .reject(client, ctx, 404, ("invalid_request_error", "model_not_found"), "unknown model", &[])
                .await;
        };
        if !tenant.allows(&chat.alias) {
            return self
                .reject(
                    client,
                    ctx,
                    403,
                    ("permission_error", "model_not_allowed"),
                    "this key may not use this model",
                    &[],
                )
                .await;
        }

        let reserved_tokens = chat.est_prompt + chat.max_tokens;
        let reservation = match tenant.limiter.acquire(reserved_tokens, self.epoch.elapsed().as_secs_f64(), unix_secs())
        {
            Ok(r) => r,
            Err(deny) => return self.deny(client, ctx, &tenant, deny).await,
        };
        ctx.remaining = Some(reservation.remaining);
        ctx.limits = (tenant.limiter.rpm_limit, tenant.limiter.tpm_limit);

        let cache_key = self.cache_plan(req, &chat, &tenant);
        let mut flight = None;
        if let Some(key) = cache_key {
            if let Lookup::Fresh(entry) = self.cache.get(&key, Instant::now()) {
                ctx.cache = "hit";
                self.metrics.cache_requests.inc(&["hit"]);
                return self.serve_cached(client, ctx, &tenant, &entry, chat.stream).await;
            }
            ctx.cache = "miss";
            self.metrics.cache_requests.inc(&["miss"]);
            // Streams are not coalesced: followers would lose incremental output.
            if !chat.stream {
                match self.cache.join_flight(key) {
                    FlightRole::Leader(guard) => flight = Some(guard),
                    FlightRole::Follower(f) => {
                        let limit = Duration::from_millis(self.cfg.timeouts.first_byte_ms);
                        let wait = pin!(rt::timeout(limit, f.wait()));
                        if let Either::Right(()) = rt::select(wait, pin!(io.conn.closed())).await {
                            return self.client_gone(ctx, "coalescing");
                        }
                        if let Lookup::Fresh(entry) = self.cache.get(&key, Instant::now()) {
                            ctx.cache = "coalesced";
                            self.metrics.coalesced.inc(&[]);
                            return self.serve_cached(client, ctx, &tenant, &entry, false).await;
                        }
                    }
                }
            }
        } else {
            self.metrics.cache_requests.inc(&["bypass"]);
        }

        let queued_at = Instant::now();
        let acquire = pin!(self.admission.acquire(&tenant.name, tenant.max_concurrent));
        let permit = match rt::select(acquire, pin!(io.conn.closed())).await {
            Either::Left(Ok(p)) => p,
            Either::Left(Err(reason)) => return self.shed(client, ctx, cache_key, &tenant, &chat, reason).await,
            Either::Right(()) => return self.client_gone(ctx, "queued"),
        };
        ctx.queue_wait = queued_at.elapsed();
        self.metrics.queue_wait.observe_duration(&[], ctx.queue_wait);

        let outcome = {
            let dispatch = pin!(self.dispatch(ctx, route, &chat));
            match rt::select(dispatch, pin!(io.conn.closed())).await {
                Either::Left(o) => Some(o),
                Either::Right(()) => None,
            }
        };
        let Some(outcome) = outcome else {
            return self.client_gone(ctx, "upstream_wait");
        };

        match outcome {
            Outcome::ClientError { status, body } => {
                ctx.outcome = "upstream_rejected";
                self.respond(client, ctx, status, "application/json", &body, &[]).await
            }
            Outcome::Exhausted { last } => {
                drop(permit);
                self.exhausted(client, ctx, cache_key, &tenant, &chat, last).await
            }
            Outcome::Delivered(d) => {
                let cont = match d.success {
                    Success::Full { body, usage } => {
                        let provider = route.targets[d.target].provider.clone();
                        self.account(ctx, &tenant, d.prices, &usage, chat.est_prompt);
                        if let Some(key) = cache_key {
                            self.store(key, body.clone(), ctx, &provider);
                        }
                        // Finish all bookkeeping before the client can observe the
                        // response, so a follow-up request sees the cache and quota.
                        drop(flight.take());
                        reservation.settle(ctx.prompt_tokens + ctx.completion_tokens);
                        self.respond(client, ctx, 200, "application/json", &body, &[]).await
                    }
                    Success::Stream(live) => {
                        self.forward_stream(
                            io,
                            ctx,
                            &tenant,
                            route,
                            d.target,
                            d.prices,
                            live,
                            cache_key,
                            &chat,
                            reservation,
                            keep,
                        )
                        .await
                    }
                };
                drop(permit);
                cont
            }
        }
    }

    fn client_gone(&self, ctx: &mut Ctx, phase: &'static str) -> bool {
        ctx.outcome = "client_disconnected";
        ctx.status = 499;
        self.metrics.client_disconnects.inc(&[phase]);
        false
    }

    async fn deny(&self, client: Client<'_>, ctx: &mut Ctx, tenant: &Tenant, deny: Deny) -> bool {
        self.metrics.rate_limited.inc(&[&tenant.name, deny.reason.label()]);
        let retry = deny.retry_after_secs.to_string();
        let (code, msg) = match deny.reason {
            crate::limiter::DenyReason::Quota => ("insufficient_quota", "daily token quota exceeded"),
            crate::limiter::DenyReason::Requests => ("rate_limit_exceeded", "request rate limit exceeded"),
            crate::limiter::DenyReason::Tokens => ("rate_limit_exceeded", "token rate limit exceeded"),
        };
        self.reject(client, ctx, 429, ("rate_limit_error", code), msg, &[("retry-after", &retry)]).await
    }

    async fn shed(
        &self,
        client: Client<'_>,
        ctx: &mut Ctx,
        key: Option<cache::Key>,
        tenant: &Tenant,
        chat: &Chat,
        reason: Reject,
    ) -> bool {
        self.metrics.admission_rejected.inc(&[reason.label()]);
        if let Some(entry) = key.and_then(|k| self.cache.get_stale(&k, Instant::now())) {
            ctx.cache = "stale";
            self.metrics.cache_requests.inc(&["stale"]);
            return self.serve_cached(client, ctx, tenant, &entry, chat.stream).await;
        }
        self.reject(
            client,
            ctx,
            503,
            ("overloaded_error", reason.label()),
            "gateway is overloaded, retry shortly",
            &[("retry-after", "1")],
        )
        .await
    }

    async fn exhausted(
        &self,
        client: Client<'_>,
        ctx: &mut Ctx,
        key: Option<cache::Key>,
        tenant: &Tenant,
        chat: &Chat,
        last: &'static str,
    ) -> bool {
        if let Some(entry) = key.and_then(|k| self.cache.get_stale(&k, Instant::now())) {
            ctx.cache = "stale";
            self.metrics.cache_requests.inc(&["stale"]);
            return self.serve_cached(client, ctx, tenant, &entry, chat.stream).await;
        }
        ctx.outcome = last;
        let status = match last {
            "timeout" | "connect_timeout" => 504,
            "breaker_open" | "rate_limited" | "retry_budget_exhausted" | "no_available_provider" => 503,
            _ => 502,
        };
        let body = error_json("all upstream providers failed", "upstream_error", last);
        self.respond(client, ctx, status, "application/json", &body, &[("retry-after", "1")]).await
    }

    fn store(&self, key: cache::Key, body: Vec<u8>, ctx: &Ctx, provider: &Provider) {
        self.cache.put(
            key,
            Entry {
                body,
                provider: provider.cfg.name.clone(),
                prompt_tokens: ctx.prompt_tokens,
                completion_tokens: ctx.completion_tokens,
                cost_micro_usd: ctx.cost_micro_usd,
                created: Instant::now(),
            },
        );
    }

    fn account(&self, ctx: &mut Ctx, tenant: &Tenant, prices: (f64, f64), usage: &Usage, est_prompt: u64) {
        ctx.prompt_tokens = if usage.prompt_tokens > 0 { usage.prompt_tokens } else { est_prompt };
        ctx.completion_tokens = usage.completion();
        ctx.cost_micro_usd =
            (ctx.prompt_tokens as f64 * prices.0 + ctx.completion_tokens as f64 * prices.1).round() as u64;
        let m = &self.metrics;
        m.tokens.add(&[&tenant.name, &ctx.provider, "input"], ctx.prompt_tokens);
        m.tokens.add(&[&tenant.name, &ctx.provider, "output"], ctx.completion_tokens);
        m.cost_micro_usd.add(&[&tenant.name, &ctx.provider], ctx.cost_micro_usd);
    }

    async fn dispatch(&self, ctx: &mut Ctx, route: &Route, chat: &Chat) -> Outcome {
        let mut last = "no_available_provider";
        for (idx, rt_target) in route.targets.iter().enumerate() {
            let provider = &rt_target.provider;
            if provider.cfg.kind == ProviderKind::Anthropic && chat.anthropic_blocked {
                continue;
            }
            provider.budget.deposit();
            let body = match provider.cfg.kind {
                ProviderKind::OpenAi => translate::openai_request(&chat.body, &rt_target.target.model),
                ProviderKind::Anthropic => {
                    translate::anthropic_request(&chat.body, &rt_target.target.model, self.cfg.max_tokens_default)
                }
            };
            let mut attempted = false;
            for attempt in 0..self.cfg.retry.max_attempts {
                let Some(ticket) = provider.breaker.allow(Instant::now()) else {
                    last = "breaker_open";
                    self.metrics.upstream_requests.inc(&[&provider.cfg.name, "breaker_open"]);
                    break;
                };
                if attempt > 0 {
                    if !provider.budget.withdraw() {
                        last = "retry_budget_exhausted";
                        break;
                    }
                    self.metrics.upstream_retries.inc(&[&provider.cfg.name]);
                    let backoff_started = Instant::now();
                    rt::sleep(self.backoff(attempt)).await;
                    ctx.upstream_wait += backoff_started.elapsed();
                }
                attempted = true;
                ctx.attempts += 1;
                let started = Instant::now();
                let result = self.attempt(provider, chat, &body, ctx).await;
                ctx.upstream_wait += started.elapsed();
                match result {
                    Ok(success) => {
                        ticket.success();
                        self.metrics.upstream_requests.inc(&[&provider.cfg.name, "ok"]);
                        self.metrics.upstream_ttfb.observe_duration(&[&provider.cfg.name], started.elapsed());
                        ctx.provider = provider.cfg.name.clone();
                        let prices = (rt_target.target.input_usd_per_mtok, rt_target.target.output_usd_per_mtok);
                        return Outcome::Delivered(Delivered { success, target: idx, prices });
                    }
                    Err(AttemptError::Client { status, body }) => {
                        // The provider answered correctly, so this counts as healthy.
                        ticket.success();
                        self.metrics.upstream_requests.inc(&[&provider.cfg.name, "client_error"]);
                        ctx.provider = provider.cfg.name.clone();
                        return Outcome::ClientError { status, body };
                    }
                    Err(AttemptError::NextProvider { label, trip }) => {
                        self.resolve_ticket(ticket, trip);
                        self.metrics.upstream_requests.inc(&[&provider.cfg.name, label]);
                        last = label;
                        break;
                    }
                    Err(AttemptError::Retryable { label }) => {
                        ticket.failure(Instant::now());
                        self.metrics.upstream_requests.inc(&[&provider.cfg.name, label]);
                        last = label;
                    }
                }
            }
            if attempted && idx + 1 < route.targets.len() {
                self.metrics.failovers.inc(&[&provider.cfg.name]);
            }
        }
        Outcome::Exhausted { last }
    }

    fn resolve_ticket(&self, ticket: Ticket, trip: bool) {
        if trip {
            ticket.failure(Instant::now());
        } else {
            drop(ticket);
        }
    }

    fn backoff(&self, attempt: u32) -> Duration {
        let cap = self.cfg.retry.max_ms.min(self.cfg.retry.base_ms.saturating_mul(1u64 << attempt.min(16)));
        Duration::from_millis(fast_rand() % (cap.max(1) + 1))
    }

    fn upstream_head(&self, provider: &Provider, ctx: &Ctx, body_len: usize) -> Vec<u8> {
        let p = &provider.cfg;
        let mut out = Vec::with_capacity(512);
        let path = if p.kind == ProviderKind::Anthropic { "/v1/messages" } else { "/v1/chat/completions" };
        out.extend_from_slice(format!("POST {}{} HTTP/1.1\r\n", p.path_prefix, path).as_bytes());
        http::push_header(&mut out, "host", &p.host_header);
        http::push_header(&mut out, "content-type", "application/json");
        http::push_header(&mut out, "content-length", &body_len.to_string());
        http::push_header(&mut out, "accept", if ctx.stream { "text/event-stream" } else { "application/json" });
        http::push_header(&mut out, "user-agent", "llm-gateway/1.0");
        http::push_header(&mut out, "traceparent", &ctx.trace.header_for(&new_span_id()));
        http::push_header(&mut out, "x-request-id", &ctx.request_id);
        if p.kind == ProviderKind::Anthropic {
            http::push_header(&mut out, "x-api-key", &p.api_key);
            http::push_header(&mut out, "anthropic-version", "2023-06-01");
        } else {
            http::push_header(&mut out, "authorization", &format!("Bearer {}", p.api_key));
        }
        out.extend_from_slice(b"\r\n");
        out
    }

    async fn connect(&self, provider: &Provider) -> Result<ClientConn, AttemptError> {
        let addrs = provider.addrs.read().unwrap().clone();
        if addrs.is_empty() {
            return Err(AttemptError::Retryable { label: "dns_error" });
        }
        let addr = addrs[fast_rand() as usize % addrs.len()];
        match rt::timeout(Duration::from_millis(self.cfg.timeouts.connect_ms), TcpConn::connect(addr)).await {
            Ok(Ok(conn)) => Ok(ClientConn::new(conn)),
            Ok(Err(_)) => Err(AttemptError::Retryable { label: "connect_error" }),
            Err(_) => Err(AttemptError::Retryable { label: "connect_timeout" }),
        }
    }

    async fn attempt(&self, provider: &Provider, chat: &Chat, body: &[u8], ctx: &Ctx) -> Result<Success, AttemptError> {
        let mut request = self.upstream_head(provider, ctx, body.len());
        request.extend_from_slice(body);
        let first_byte = Duration::from_millis(self.cfg.timeouts.first_byte_ms);

        let mut pooled = pool_take(provider.idx);
        let (mut conn, head) = loop {
            let reused = pooled.is_some();
            let mut conn = match pooled.take() {
                Some(c) => c,
                None => self.connect(provider).await?,
            };
            let exchange = async {
                conn.send(&request).await?;
                conn.read_head(false).await
            };
            match rt::timeout(first_byte, exchange).await {
                Ok(Ok(head)) => break (conn, head),
                Err(_) => return Err(AttemptError::Retryable { label: "timeout" }),
                // A pooled connection may have been closed by the peer just as
                // we reused it. Nothing was processed, so resending is safe.
                Ok(Err(_)) if reused && !conn.got_bytes => {
                    pooled = pool_take(provider.idx);
                }
                Ok(Err(_)) => return Err(AttemptError::Retryable { label: "protocol_error" }),
            }
        };

        if !(200..300).contains(&head.status) {
            let mut err_body = Vec::new();
            let read = async {
                while err_body.len() < MAX_ERROR_BODY {
                    if !conn.read_body(&mut err_body).await? {
                        break;
                    }
                }
                Ok::<(), io::Error>(())
            };
            let _ = rt::timeout(Duration::from_secs(5), read).await;
            pool_put(provider.idx, conn, provider.cfg.pool_size);
            return Err(classify_status(head.status, err_body));
        }

        if chat.stream {
            let created = unix_secs();
            let mut translator = match provider.cfg.kind {
                ProviderKind::OpenAi => StreamTranslator::openai(chat.wants_usage),
                ProviderKind::Anthropic => StreamTranslator::anthropic(&chat.alias, created, chat.wants_usage),
            };
            let mut sse = SseBuffer::default();
            let mut first = Vec::with_capacity(4096);
            let mut buf = Vec::with_capacity(16 * 1024);
            let mut done = false;
            let wait = async {
                loop {
                    buf.clear();
                    let more = conn.read_body(&mut buf).await?;
                    sse.feed(&buf);
                    while let Some(event) = sse.next_event() {
                        translator.on_event(event, &mut first);
                    }
                    sse.compact();
                    if !more {
                        done = true;
                        return Ok::<(), io::Error>(());
                    }
                    if !first.is_empty() || translator.upstream_error {
                        return Ok(());
                    }
                }
            };
            match rt::timeout(first_byte, wait).await {
                Err(_) => return Err(AttemptError::Retryable { label: "timeout" }),
                Ok(Err(_)) => return Err(AttemptError::Retryable { label: "stream_failed_before_first_byte" }),
                Ok(Ok(())) => {}
            }
            if translator.upstream_error || first.is_empty() {
                return Err(AttemptError::Retryable { label: "stream_error_event" });
            }
            return Ok(Success::Stream(Box::new(LiveStream { conn, sse, translator, first, done })));
        }

        let mut raw = Vec::with_capacity(4096);
        let read = async {
            loop {
                if raw.len() > MAX_FULL_BODY {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "body too large"));
                }
                if !conn.read_body(&mut raw).await? {
                    return Ok::<(), io::Error>(());
                }
            }
        };
        match rt::timeout(Duration::from_millis(self.cfg.timeouts.total_ms), read).await {
            Err(_) => return Err(AttemptError::Retryable { label: "timeout" }),
            Ok(Err(_)) => return Err(AttemptError::Retryable { label: "protocol_error" }),
            Ok(Ok(())) => {}
        }
        pool_put(provider.idx, conn, provider.cfg.pool_size);
        let (body, usage) = match provider.cfg.kind {
            ProviderKind::OpenAi => {
                let mut parsed = json::parse(&raw).map_err(|_| AttemptError::Retryable { label: "invalid_json" })?;
                let usage = translate::openai_usage(&parsed);
                parsed.set("model", Value::str(chat.alias.as_str()));
                (parsed.to_bytes(), usage)
            }
            ProviderKind::Anthropic => translate::anthropic_response(&raw, &chat.alias, unix_secs())
                .map_err(|_| AttemptError::Retryable { label: "invalid_json" })?,
        };
        Ok(Success::Full { body, usage })
    }

    // Every argument is distinct per-request state; a one-use struct would only rename them.
    #[allow(clippy::too_many_arguments)]
    async fn forward_stream(
        &self,
        io: &BufConn,
        ctx: &mut Ctx,
        tenant: &Tenant,
        route: &Route,
        target: usize,
        prices: (f64, f64),
        mut live: Box<LiveStream>,
        key: Option<cache::Key>,
        chat: &Chat,
        reservation: Reservation,
        keep: bool,
    ) -> bool {
        let provider = route.targets[target].provider.clone();
        ctx.status = 200;
        ctx.mark_first_byte();
        let stream_started = Instant::now();

        let mut head = Vec::with_capacity(live.first.len() + 640);
        http::start_head(200, keep, &mut head);
        http::push_header(&mut head, "content-type", "text/event-stream");
        http::push_header(&mut head, "cache-control", "no-cache");
        http::push_header(&mut head, "x-accel-buffering", "no");
        http::push_header(&mut head, "transfer-encoding", "chunked");
        ctx.common_headers(&mut head);
        head.extend_from_slice(b"\r\n");
        http::push_chunk(&mut head, &live.first);

        let write_limit = Duration::from_millis(self.cfg.timeouts.client_write_ms);
        let idle_limit = Duration::from_millis(self.cfg.timeouts.idle_stream_ms);
        let deadline = ctx.start + Duration::from_millis(self.cfg.timeouts.total_ms);
        let mut record = key.map(|_| live.first.clone());
        let max_entry = self.cfg.cache.max_entry_bytes;

        let mut client_ok = matches!(rt::timeout(write_limit, io.conn.write_all(&head)).await, Ok(Ok(())));
        let mut failure: Option<&'static str> = None;
        let mut done = live.done;
        let mut buf = Vec::with_capacity(16 * 1024);
        let mut out = Vec::with_capacity(16 * 1024);
        let mut frame = Vec::with_capacity(16 * 1024);

        if !client_ok {
            self.metrics.client_disconnects.inc(&["streaming"]);
            ctx.outcome = "client_disconnected";
        }

        while client_ok && !done {
            buf.clear();
            let limit = idle_limit.min(deadline.saturating_duration_since(Instant::now()));
            let result = {
                let read = pin!(rt::timeout(limit, live.conn.read_body(&mut buf)));
                match rt::select(read, pin!(io.conn.closed())).await {
                    Either::Right(()) => None,
                    Either::Left(r) => Some(r),
                }
            };
            let Some(result) = result else {
                self.metrics.client_disconnects.inc(&["streaming"]);
                ctx.outcome = "client_disconnected";
                client_ok = false;
                break;
            };
            match result {
                Err(_) => {
                    failure = Some("idle_timeout");
                    break;
                }
                Ok(Err(_)) => {
                    failure = Some("upstream_reset");
                    break;
                }
                Ok(Ok(more)) => done = !more,
            }
            live.sse.feed(&buf);
            out.clear();
            while let Some(event) = live.sse.next_event() {
                live.translator.on_event(event, &mut out);
            }
            live.sse.compact();
            if live.translator.upstream_error {
                failure = Some("error_event");
                break;
            }
            if out.is_empty() {
                continue;
            }
            if let Some(rec) = record.as_mut() {
                if rec.len() + out.len() <= max_entry {
                    rec.extend_from_slice(&out);
                } else {
                    record = None;
                }
            }
            frame.clear();
            http::push_chunk(&mut frame, &out);
            // The upstream is not read again until this write completes, so a
            // slow client slows its upstream via TCP flow control.
            if !matches!(rt::timeout(write_limit, io.conn.write_all(&frame)).await, Ok(Ok(()))) {
                self.metrics.client_disconnects.inc(&["slow_or_gone"]);
                ctx.outcome = "client_disconnected";
                client_ok = false;
            }
        }

        if failure.is_none() && client_ok && !live.translator.saw_done {
            failure = Some("truncated");
        }

        let elapsed = stream_started.elapsed().as_secs_f64();
        let usage = std::mem::take(&mut live.translator.usage);
        self.account(ctx, tenant, prices, &usage, chat.est_prompt);
        reservation.settle(ctx.prompt_tokens + ctx.completion_tokens);
        if elapsed > 0.0 && ctx.completion_tokens > 0 && failure.is_none() && client_ok {
            self.metrics.stream_tps.observe(&[&provider.cfg.name], ctx.completion_tokens as f64 / elapsed);
        }

        let mut cont = keep && client_ok;
        if let Some(reason) = failure {
            ctx.outcome = "stream_failed";
            self.metrics.stream_errors.inc(&[&provider.cfg.name, reason]);
            provider.breaker.record_failure(Instant::now());
            let payload = error_json("upstream stream failed", "upstream_error", reason);
            let mut tail = Vec::with_capacity(payload.len() + 32);
            let mut event = Vec::with_capacity(payload.len() + 16);
            event.extend_from_slice(b"data: ");
            event.extend_from_slice(&payload);
            event.extend_from_slice(b"\n\n");
            http::push_chunk(&mut tail, &event);
            tail.extend_from_slice(http::CHUNK_END);
            cont = matches!(rt::timeout(write_limit, io.conn.write_all(&tail)).await, Ok(Ok(()))) && keep;
        } else if client_ok {
            if let (Some(k), Some(body)) = (key, record) {
                self.store(k, body, ctx, &provider);
            }
            if !matches!(rt::timeout(write_limit, io.conn.write_all(http::CHUNK_END)).await, Ok(Ok(()))) {
                cont = false;
            }
            pool_put(provider.idx, live.conn, provider.cfg.pool_size);
        }
        cont
    }

    fn finish(&self, ctx: &Ctx) {
        let total = ctx.start.elapsed();
        let stream = if ctx.stream { "true" } else { "false" };
        let m = &self.metrics;
        m.requests.inc(&[&ctx.tenant, &ctx.model, &ctx.status.to_string(), ctx.cache]);
        m.duration.observe_duration(&[stream], total);
        if let Some(t) = ctx.ttfb {
            m.ttfb.observe_duration(&[stream], t);
        }
        if let (Some(a), true) = (ctx.added, ctx.attempts > 0) {
            m.added_latency.observe_duration(&[stream], a);
        }
        let mut line = LogLine::new("request")
            .str("trace_id", &ctx.trace.trace_id)
            .str("span_id", &ctx.trace.span_id)
            .str("request_id", &ctx.request_id)
            .str("tenant", &ctx.tenant)
            .str("model", &ctx.model)
            .str("provider", &ctx.provider)
            .num("status", ctx.status)
            .str("outcome", ctx.outcome)
            .str("cache", ctx.cache)
            .bool("stream", ctx.stream)
            .num("attempts", ctx.attempts)
            .num("queue_ms", ctx.queue_wait.as_secs_f64() * 1e3)
            .num("upstream_wait_ms", ctx.upstream_wait.as_secs_f64() * 1e3)
            .num("total_ms", total.as_secs_f64() * 1e3)
            .num("prompt_tokens", ctx.prompt_tokens)
            .num("completion_tokens", ctx.completion_tokens)
            .num("cost_micro_usd", ctx.cost_micro_usd);
        if let Some(p) = &ctx.trace.parent_id {
            line = line.str("parent_span_id", p);
        }
        if let Some(t) = ctx.ttfb {
            line = line.num("ttfb_ms", t.as_secs_f64() * 1e3);
        }
        if let Some(a) = ctx.added {
            line = line.num("added_us", a.as_secs_f64() * 1e6);
        }
        self.log.emit(line);
    }

    async fn serve_conn(self: Arc<Self>, stream: TcpStream) {
        let _ = stream.set_nodelay(true);
        let Ok(conn) = TcpConn::from_std(stream) else { return };
        let mut io = BufConn::new(conn);
        let limits = Limits { max_head: 32 * 1024, max_body: self.cfg.max_body_bytes };
        let mut first = true;
        loop {
            let wait = if first { self.cfg.timeouts.client_request_ms } else { self.cfg.timeouts.keepalive_ms };
            first = false;
            let read = rt::timeout(Duration::from_millis(wait), io.read_request(limits)).await;
            let req = match read {
                Err(_) => {
                    if io.has_buffered() {
                        let body = error_json("request timed out", "invalid_request_error", "request_timeout");
                        self.plain(&io, 408, "application/json", &body, false).await;
                    }
                    return;
                }
                Ok(Ok(None)) => return,
                Ok(Err(e)) => {
                    let body = error_json(e.message, "invalid_request_error", "bad_request");
                    self.plain(&io, e.status, "application/json", &body, false).await;
                    return;
                }
                Ok(Ok(Some(r))) => r,
            };
            let keep = req.keep_alive && !self.draining.load(SeqCst);
            if !self.route(&req, &io, keep).await {
                return;
            }
        }
    }

    async fn accept_loop(self: Arc<Self>, fd: i32) {
        let acceptor = Acceptor::from_raw(fd).expect("register listener");
        while !self.draining.load(SeqCst) {
            let accept = pin!(acceptor.accept());
            match rt::select(accept, rt::sleep(Duration::from_millis(100))).await {
                Either::Right(()) => {}
                Either::Left(Ok(stream)) => {
                    if self.connections.load(Relaxed) >= self.cfg.max_connections as i64 {
                        self.metrics.connections_rejected.inc(&[]);
                        drop(stream);
                        continue;
                    }
                    let gw = self.clone();
                    self.connections.fetch_add(1, Relaxed);
                    rt::spawn(async move {
                        let counter = gw.clone();
                        gw.serve_conn(stream).await;
                        counter.connections.fetch_sub(1, Relaxed);
                    });
                }
                // Typically EMFILE: back off and let connections close.
                Either::Left(Err(_)) => rt::sleep(Duration::from_millis(50)).await,
            }
        }
        drop(acceptor);
        pending::<()>().await;
    }
}

/// Maps an upstream error status to what the gateway should do next.
fn classify_status(status: u16, body: Vec<u8>) -> AttemptError {
    match status {
        400 | 413 | 422 => AttemptError::Client { status, body },
        401 | 403 | 404 => AttemptError::NextProvider { label: "auth_or_config_error", trip: false },
        429 => AttemptError::NextProvider { label: "rate_limited", trip: false },
        408 | 409 | 425 | 500..=599 => AttemptError::Retryable { label: "server_error" },
        _ => AttemptError::NextProvider { label: "unexpected_status", trip: true },
    }
}

/// A started gateway.
pub struct Running {
    pub gateway: Arc<Gateway>,
    pub addr: SocketAddr,
}

/// Starts worker threads and returns once the listeners are bound.
pub fn start(cfg: Config, log: Logger) -> io::Result<Running> {
    let listen: SocketAddr =
        cfg.listen.parse().map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid listen address"))?;
    let workers =
        if cfg.workers == 0 { std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1) } else { cfg.workers };
    let gateway = Gateway::new(cfg, log);
    if let Some(path) = &gateway.cfg.quota_snapshot_path {
        gateway.load_quota(path);
    }
    gateway.resolve_all();
    {
        let gw = gateway.clone();
        std::thread::Builder::new().name("dns-refresh".into()).spawn(move || loop {
            std::thread::sleep(DNS_REFRESH);
            gw.resolve_all();
        })?;
    }
    let (first_fd, addr) = sys::listen_reuseport(&listen, 4096)?;
    for i in 0..workers {
        let fd = if i == 0 { first_fd } else { sys::listen_reuseport(&addr, 4096)?.0 };
        let gw = gateway.clone();
        std::thread::Builder::new().name(format!("worker-{i}")).spawn(move || {
            rt::block_on(gw.accept_loop(fd));
        })?;
    }
    gateway.log.emit(LogLine::new("started").str("addr", &addr.to_string()).num("workers", workers));
    Ok(Running { gateway, addr })
}

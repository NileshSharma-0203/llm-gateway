//! End-to-end tests against a real gateway and mock providers over TCP.

use llm_gateway::config::{self, Config};
use llm_gateway::gateway::{self, Running};
use llm_gateway::mock::{self, Mock};
use llm_gateway::telemetry::Logger;
use llm_gateway::{json, sha256};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

const KEY: &str = "test-key";
const KEY2: &str = "other-key";

fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

fn hash(k: &str) -> String {
    sha256::hex(&sha256::digest(k.as_bytes()))
}

fn mock_server() -> Mock {
    mock::start(loopback(), 1, 0, 8, 0).unwrap()
}

/// Options that vary between test gateways.
struct Setup {
    extra: String,
    tenant: String,
    second_kind: &'static str,
}

impl Default for Setup {
    fn default() -> Self {
        Self { extra: String::new(), tenant: String::new(), second_kind: "openai" }
    }
}

fn build_config(primary: &Mock, secondary: &Mock, s: &Setup) -> Config {
    let doc = format!(
        r#"{{
        "listen":"127.0.0.1:0","workers":2,{extra}
        "providers":[
          {{"name":"primary","kind":"openai","base_url":"http://{a}","api_key_env":"K"}},
          {{"name":"secondary","kind":"{kind}","base_url":"http://{b}","api_key_env":"K"}}],
        "models":[
          {{"alias":"fast","targets":[
            {{"provider":"primary","model":"m1","input_usd_per_mtok":1,"output_usd_per_mtok":2}},
            {{"provider":"secondary","model":"m2"}}]}},
          {{"alias":"solo","targets":[{{"provider":"primary","model":"m1"}}]}}],
        "tenants":[
          {{"name":"t1","key_sha256":["{k1}"],"rpm":600000,"tpm":100000000,"daily_tokens":100000000,{tenant}"burst_secs":60}},
          {{"name":"t2","key_sha256":["{k2}"],"models":["solo"]}}]
        }}"#,
        extra = s.extra,
        tenant = s.tenant,
        kind = s.second_kind,
        a = primary.addr,
        b = secondary.addr,
        k1 = hash(KEY),
        k2 = hash(KEY2),
    );
    config::parse(doc.as_bytes(), &|_| Some("upstream-secret".to_string())).unwrap()
}

struct Env {
    gw: Running,
    primary: Mock,
    secondary: Mock,
}

fn env(s: Setup) -> Env {
    let primary = mock_server();
    let secondary = mock_server();
    let gw = gateway::start(build_config(&primary, &secondary, &s), Logger::disabled()).unwrap();
    Env { gw, primary, secondary }
}

struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl Reply {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }

    fn json(&self) -> json::Value {
        json::parse(self.body.as_bytes()).unwrap_or_else(|e| panic!("bad JSON {e}: {}", self.body))
    }
}

fn dechunk(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut d = llm_gateway::http::ChunkedDecoder::default();
    d.decode(raw, &mut out).expect("valid chunked body");
    assert!(d.is_done(), "chunked body was not terminated");
    out
}

fn read_reply(s: &mut TcpStream) -> Reply {
    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    let head_end = loop {
        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break p + 4;
        }
        let n = s.read(&mut tmp).expect("read head");
        assert!(n > 0, "connection closed before response head");
        buf.extend_from_slice(&tmp[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let mut lines = head.split("\r\n");
    let status: u16 = lines.next().unwrap().split(' ').nth(1).unwrap().parse().unwrap();
    let headers: Vec<(String, String)> =
        lines.filter_map(|l| l.split_once(": ")).map(|(a, b)| (a.to_string(), b.to_string())).collect();
    let find = |n: &str| headers.iter().find(|(k, _)| k == n).map(|(_, v)| v.clone());
    let mut rest = buf[head_end..].to_vec();
    let body = if let Some(len) = find("content-length") {
        let len: usize = len.parse().unwrap();
        while rest.len() < len {
            let n = s.read(&mut tmp).expect("read body");
            assert!(n > 0, "closed mid-body");
            rest.extend_from_slice(&tmp[..n]);
        }
        rest
    } else {
        while !rest.ends_with(b"0\r\n\r\n") {
            let n = s.read(&mut tmp).expect("read chunks");
            assert!(n > 0, "closed mid-chunks");
            rest.extend_from_slice(&tmp[..n]);
        }
        dechunk(&rest)
    };
    Reply { status, headers, body: String::from_utf8(body).unwrap() }
}

fn post_raw(addr: SocketAddr, key: &str, body: &str, extra: &str) -> TcpStream {
    let mut s = TcpStream::connect(addr).unwrap();
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nhost: t\r\nauthorization: Bearer {key}\r\n{extra}content-length: {}\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).unwrap();
    s
}

fn post(addr: SocketAddr, body: &str) -> Reply {
    read_reply(&mut post_raw(addr, KEY, body, ""))
}

fn get(addr: SocketAddr, path: &str, extra: &str) -> Reply {
    let mut s = TcpStream::connect(addr).unwrap();
    s.write_all(format!("GET {path} HTTP/1.1\r\nhost: t\r\n{extra}\r\n").as_bytes()).unwrap();
    read_reply(&mut s)
}

fn chat(model: &str, stream: bool, temperature: f64, content: &str) -> String {
    format!(
        r#"{{"model":"{model}","stream":{stream},"temperature":{temperature},"max_tokens":16,"messages":[{{"role":"user","content":"{content}"}}]}}"#
    )
}

fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn metric(e: &Env, needle: &str) -> u64 {
    get(e.gw.addr, "/metrics", "")
        .body
        .lines()
        .find(|l| l.starts_with(needle))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

#[test]
fn proxies_non_streaming_and_rewrites_the_model() {
    let e = env(Setup::default());
    let r = post(e.gw.addr, &chat("fast", false, 0.7, "hello"));
    assert_eq!(r.status, 200, "{}", r.body);
    let v = r.json();
    assert_eq!(v.get("model").unwrap().as_str(), Some("fast"));
    assert_eq!(v.get("usage").unwrap().get("completion_tokens").unwrap().as_u64(), Some(8));
    assert_eq!(r.header("x-gateway-provider"), Some("primary"));
    assert_eq!(r.header("x-cache"), Some("BYPASS"));
    assert!(r.header("x-request-id").is_some());
}

#[test]
fn streams_events_and_hides_the_injected_usage_chunk() {
    let e = env(Setup::default());
    let r = post(e.gw.addr, &chat("fast", true, 0.7, "hello"));
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-type"), Some("text/event-stream"));
    assert_eq!(r.body.matches("tok").count(), 8);
    assert!(!r.body.contains("usage"), "usage chunk leaked: {}", r.body);
    assert!(r.body.ends_with("data: [DONE]\n\n"));
    wait_until("token accounting", || {
        metric(&e, "gateway_tokens_total{tenant=\"t1\",provider=\"primary\",direction=\"output\"}") == 8
    });
}

#[test]
fn passes_usage_through_when_the_client_asks() {
    let e = env(Setup::default());
    let body = r#"{"model":"fast","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"x"}]}"#;
    assert!(post(e.gw.addr, body).body.contains("\"total_tokens\":18"));
}

#[test]
fn keep_alive_serves_multiple_requests_on_one_connection() {
    let e = env(Setup::default());
    let body = chat("fast", true, 0.7, "a");
    let mut s = post_raw(e.gw.addr, KEY, &body, "");
    assert_eq!(read_reply(&mut s).status, 200);
    let again = format!("POST /v1/chat/completions HTTP/1.1\r\nhost: t\r\nauthorization: Bearer {KEY}\r\ncontent-length: {}\r\n\r\n{body}", body.len());
    s.write_all(again.as_bytes()).unwrap();
    assert_eq!(read_reply(&mut s).status, 200);
}

#[test]
fn rejects_bad_auth_and_unknown_or_forbidden_models() {
    let e = env(Setup::default());
    let r = read_reply(&mut post_raw(e.gw.addr, "wrong", &chat("fast", false, 1.0, "x"), ""));
    assert_eq!(r.status, 401);
    assert_eq!(r.json().get("error").unwrap().get("code").unwrap().as_str(), Some("invalid_api_key"));
    assert_eq!(post(e.gw.addr, &chat("nope", false, 1.0, "x")).status, 404);
    let forbidden = read_reply(&mut post_raw(e.gw.addr, KEY2, &chat("fast", false, 1.0, "x"), ""));
    assert_eq!(forbidden.status, 403);
    assert_eq!(e.primary.state.requests.load(Relaxed), 0);
}

#[test]
fn validates_request_bodies() {
    let e = env(Setup::default());
    for bad in [
        "{not json",
        "[]",
        r#"{"messages":[]}"#,
        r#"{"model":"fast","messages":[]}"#,
        r#"{"model":"fast","messages":[{}],"stream":"yes"}"#,
    ] {
        assert_eq!(post(e.gw.addr, bad).status, 400, "{bad}");
    }
}

#[test]
fn rejects_smuggling_and_oversized_requests() {
    let e = env(Setup { extra: r#""max_body_bytes":1000,"#.into(), ..Setup::default() });
    let mut s = TcpStream::connect(e.gw.addr).unwrap();
    s.write_all(
        b"POST /v1/chat/completions HTTP/1.1\r\nhost: t\r\ncontent-length: 4\r\ntransfer-encoding: chunked\r\n\r\n",
    )
    .unwrap();
    assert_eq!(read_reply(&mut s).status, 400);
    let mut s = TcpStream::connect(e.gw.addr).unwrap();
    s.write_all(b"POST /v1/chat/completions HTTP/1.1\r\nhost: t\r\ncontent-length: 5000\r\n\r\n").unwrap();
    assert_eq!(read_reply(&mut s).status, 413);
}

#[test]
fn accepts_chunked_request_bodies() {
    let e = env(Setup::default());
    let body = chat("fast", false, 0.7, "x");
    let mut s = TcpStream::connect(e.gw.addr).unwrap();
    let req = format!("POST /v1/chat/completions HTTP/1.1\r\nhost: t\r\nauthorization: Bearer {KEY}\r\ntransfer-encoding: chunked\r\n\r\n{:x}\r\n{body}\r\n0\r\n\r\n", body.len());
    s.write_all(req.as_bytes()).unwrap();
    assert_eq!(read_reply(&mut s).status, 200);
}

#[test]
fn enforces_rpm_with_retry_after() {
    let primary = mock_server();
    let doc = format!(
        r#"{{"listen":"127.0.0.1:0","workers":1,
        "providers":[{{"name":"primary","kind":"openai","base_url":"http://{}","api_key_env":"K"}}],
        "models":[{{"alias":"fast","targets":[{{"provider":"primary","model":"m"}}]}}],
        "tenants":[{{"name":"t","key_sha256":["{}"],"rpm":60,"burst_secs":2}}]}}"#,
        primary.addr,
        hash(KEY)
    );
    let gw = gateway::start(config::parse(doc.as_bytes(), &|_| Some("s".into())).unwrap(), Logger::disabled()).unwrap();
    assert_eq!(post(gw.addr, &chat("fast", false, 1.0, "a")).status, 200);
    let second = post(gw.addr, &chat("fast", false, 1.0, "a"));
    assert_eq!(second.status, 200);
    assert!(second.header("x-ratelimit-remaining-requests").is_some());
    let denied = post(gw.addr, &chat("fast", false, 1.0, "a"));
    assert_eq!(denied.status, 429);
    assert_eq!(denied.header("retry-after"), Some("1"));
    assert_eq!(primary.state.requests.load(Relaxed), 2);
}

#[test]
fn daily_quota_uses_actual_token_counts() {
    let primary = mock_server();
    let doc = format!(
        r#"{{"listen":"127.0.0.1:0","workers":1,"cache":{{"enabled":false}},
        "providers":[{{"name":"primary","kind":"openai","base_url":"http://{}","api_key_env":"K"}}],
        "models":[{{"alias":"fast","targets":[{{"provider":"primary","model":"m"}}]}}],
        "tenants":[{{"name":"t","key_sha256":["{}"],"daily_tokens":100}}]}}"#,
        primary.addr,
        hash(KEY)
    );
    let gw = gateway::start(config::parse(doc.as_bytes(), &|_| Some("s".into())).unwrap(), Logger::disabled()).unwrap();
    let mut served = 0;
    loop {
        let r = post(gw.addr, &chat("fast", false, 1.0, "x"));
        if r.status == 429 {
            assert_eq!(r.json().get("error").unwrap().get("code").unwrap().as_str(), Some("insufficient_quota"));
            break;
        }
        assert_eq!(r.status, 200);
        served += 1;
        assert!(served < 20, "quota never enforced");
    }
    assert!((3..=6).contains(&served), "each call costs 18 tokens against a 100 budget, served {served}");
}

#[test]
fn caches_deterministic_requests_and_replays_streams() {
    let e = env(Setup::default());
    let a = e.gw.addr;
    let first = post(a, &chat("fast", false, 0.0, "cached"));
    assert_eq!(first.header("x-cache"), Some("MISS"));
    let second = post(a, &chat("fast", false, 0.0, "cached"));
    assert_eq!(second.header("x-cache"), Some("HIT"));
    assert_eq!(first.body, second.body);
    assert_eq!(e.primary.state.requests.load(Relaxed), 1);

    let s1 = post(a, &chat("fast", true, 0.0, "cached"));
    assert_eq!(s1.header("x-cache"), Some("MISS"));
    let s2 = post(a, &chat("fast", true, 0.0, "cached"));
    assert_eq!(s2.header("x-cache"), Some("HIT"));
    assert_eq!(s1.body, s2.body);
    assert!(s2.body.ends_with("data: [DONE]\n\n"));
}

#[test]
fn cache_key_ignores_field_order_and_is_tenant_scoped() {
    let primary = mock_server();
    let doc = format!(
        r#"{{"listen":"127.0.0.1:0","workers":1,
        "providers":[{{"name":"p","kind":"openai","base_url":"http://{}","api_key_env":"K"}}],
        "models":[{{"alias":"fast","targets":[{{"provider":"p","model":"m"}}]}}],
        "tenants":[{{"name":"a","key_sha256":["{}"]}},{{"name":"b","key_sha256":["{}"]}}]}}"#,
        primary.addr,
        hash(KEY),
        hash(KEY2)
    );
    let gw = gateway::start(config::parse(doc.as_bytes(), &|_| Some("s".into())).unwrap(), Logger::disabled()).unwrap();
    let one = r#"{"model":"fast","temperature":0,"messages":[{"role":"user","content":"x"}]}"#;
    let two = r#"{"temperature":0,"messages":[{"content":"x","role":"user"}],"model":"fast","user":"abc"}"#;
    assert_eq!(post(gw.addr, one).header("x-cache"), Some("MISS"));
    assert_eq!(post(gw.addr, two).header("x-cache"), Some("HIT"));
    let other = read_reply(&mut post_raw(gw.addr, KEY2, one, ""));
    assert_eq!(other.header("x-cache"), Some("MISS"), "tenants must not share cache entries");
}

#[test]
fn honors_cache_bypass_and_force_headers() {
    let e = env(Setup::default());
    let body = chat("fast", false, 0.0, "bypass");
    assert_eq!(
        read_reply(&mut post_raw(e.gw.addr, KEY, &body, "cache-control: no-cache\r\n")).header("x-cache"),
        Some("BYPASS")
    );
    let random = chat("fast", false, 0.9, "forced");
    assert_eq!(post(e.gw.addr, &random).header("x-cache"), Some("BYPASS"));
    assert_eq!(
        read_reply(&mut post_raw(e.gw.addr, KEY, &random, "x-gateway-cache: force\r\n")).header("x-cache"),
        Some("MISS")
    );
    assert_eq!(
        read_reply(&mut post_raw(e.gw.addr, KEY, &random, "x-gateway-cache: force\r\n")).header("x-cache"),
        Some("HIT")
    );
}

#[test]
fn coalesces_identical_concurrent_requests() {
    let e = env(Setup::default());
    e.primary.state.ttfb_ms.store(200, Relaxed);
    let body = chat("fast", false, 0.0, "herd");
    let conns: Vec<_> = (0..20).map(|_| post_raw(e.gw.addr, KEY, &body, "")).collect();
    let replies: Vec<Reply> = conns.into_iter().map(|mut s| read_reply(&mut s)).collect();
    assert!(replies.iter().all(|r| r.status == 200));
    assert_eq!(e.primary.state.requests.load(Relaxed), 1, "only one upstream call expected");
}

#[test]
fn retries_transient_errors_on_the_same_provider() {
    let e = env(Setup::default());
    e.primary.state.fail_next.store(1, Relaxed);
    let r = post(e.gw.addr, &chat("fast", false, 0.7, "retry"));
    assert_eq!(r.status, 200);
    assert_eq!(r.header("x-gateway-provider"), Some("primary"));
    assert_eq!(r.header("x-gateway-attempts"), Some("2"));
    assert_eq!(e.secondary.state.requests.load(Relaxed), 0);
}

#[test]
fn fails_over_for_every_outage_mode() {
    for mode in [mock::Mode::Error500, mock::Mode::Reset, mock::Mode::Error429, mock::Mode::Error401] {
        let e = env(Setup::default());
        e.primary.state.set_mode(mode);
        for stream in [false, true] {
            let r = post(e.gw.addr, &chat("fast", stream, 0.7, "x"));
            assert_eq!(r.status, 200, "{mode:?} stream={stream}: {}", r.body);
            assert_eq!(r.header("x-gateway-provider"), Some("secondary"), "{mode:?}");
        }
    }
}

#[test]
fn fails_over_when_the_first_byte_times_out() {
    let e = env(Setup {
        extra: r#""timeouts":{"first_byte_ms":100},"retry":{"max_attempts":1},"#.into(),
        ..Setup::default()
    });
    e.primary.state.set_mode(mock::Mode::Hang);
    let started = Instant::now();
    let r = post(e.gw.addr, &chat("fast", true, 0.7, "x"));
    assert_eq!(r.status, 200);
    assert_eq!(r.header("x-gateway-provider"), Some("secondary"));
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn fails_over_across_provider_formats() {
    let e = env(Setup { second_kind: "anthropic", ..Setup::default() });
    e.primary.state.set_mode(mock::Mode::Error500);
    let full = post(e.gw.addr, &chat("fast", false, 0.7, "x"));
    assert_eq!(full.status, 200, "{}", full.body);
    let v = full.json();
    assert_eq!(v.get("object").unwrap().as_str(), Some("chat.completion"));
    assert_eq!(v.get("model").unwrap().as_str(), Some("fast"));
    assert_eq!(v.get("usage").unwrap().get("prompt_tokens").unwrap().as_u64(), Some(10));

    let stream = post(e.gw.addr, &chat("fast", true, 0.7, "x"));
    assert_eq!(stream.status, 200, "{}", stream.body);
    assert_eq!(stream.body.matches("chat.completion.chunk").count(), 10, "role + 8 tokens + finish");
    assert!(stream.body.contains(r#""finish_reason":"stop""#));
    assert!(stream.body.ends_with("data: [DONE]\n\n"));
}

#[test]
fn skips_anthropic_for_requests_it_cannot_translate() {
    let e = env(Setup { second_kind: "anthropic", ..Setup::default() });
    e.primary.state.set_mode(mock::Mode::Error500);
    let body = r#"{"model":"fast","tools":[{"type":"function"}],"messages":[{"role":"user","content":"x"}]}"#;
    assert_eq!(post(e.gw.addr, body).status, 502);
    assert_eq!(e.secondary.state.requests.load(Relaxed), 0);
}

#[test]
fn does_not_fail_over_client_errors() {
    let e = env(Setup::default());
    e.primary.state.set_mode(mock::Mode::Error400);
    let r = post(e.gw.addr, &chat("fast", false, 0.7, "x"));
    assert_eq!(r.status, 400);
    assert!(r.body.contains("invalid_request"));
    assert_eq!(e.secondary.state.requests.load(Relaxed), 0);
}

#[test]
fn circuit_breaker_stops_traffic_then_recovers() {
    let e = env(Setup {
        extra: r#""breaker":{"failure_threshold":3,"open_ms":300,"half_open_probes":1},"retry":{"max_attempts":1},"#
            .into(),
        ..Setup::default()
    });
    e.primary.state.set_mode(mock::Mode::Error500);
    for _ in 0..3 {
        assert_eq!(post(e.gw.addr, &chat("fast", false, 0.7, "x")).status, 200);
    }
    let tripped_at = e.primary.state.requests.load(Relaxed);
    assert_eq!(tripped_at, 3);
    for _ in 0..10 {
        let r = post(e.gw.addr, &chat("fast", false, 0.7, "x"));
        assert_eq!(r.header("x-gateway-provider"), Some("secondary"));
    }
    assert_eq!(e.primary.state.requests.load(Relaxed), tripped_at, "open breaker must not send traffic");
    assert_eq!(metric(&e, "gateway_breaker_state{provider=\"primary\"}"), 2);

    e.primary.state.set_mode(mock::Mode::Ok);
    std::thread::sleep(Duration::from_millis(350));
    let probe = post(e.gw.addr, &chat("fast", false, 0.7, "x"));
    assert_eq!(probe.header("x-gateway-provider"), Some("primary"));
    assert_eq!(metric(&e, "gateway_breaker_state{provider=\"primary\"}"), 0);
}

#[test]
fn outage_with_no_fallback_returns_a_clean_error() {
    let e = env(Setup { extra: r#""retry":{"max_attempts":1},"#.into(), ..Setup::default() });
    e.primary.state.set_mode(mock::Mode::Error500);
    let r = post(e.gw.addr, &chat("solo", false, 0.7, "x"));
    assert_eq!(r.status, 502);
    assert_eq!(r.json().get("error").unwrap().get("type").unwrap().as_str(), Some("upstream_error"));
    assert!(r.header("retry-after").is_some());
}

#[test]
fn serves_stale_cache_entries_during_an_outage() {
    let e = env(Setup { extra: r#""cache":{"ttl_secs":0},"retry":{"max_attempts":1},"#.into(), ..Setup::default() });
    let body = chat("solo", false, 0.0, "stale");
    let first = post(e.gw.addr, &body);
    assert_eq!(first.status, 200);
    std::thread::sleep(Duration::from_millis(1100));
    e.primary.state.set_mode(mock::Mode::Error500);
    let stale = post(e.gw.addr, &body);
    assert_eq!(stale.status, 200);
    assert_eq!(stale.header("x-cache"), Some("STALE"));
    assert_eq!(stale.body, first.body);
}

#[test]
fn reports_a_midstream_failure_as_an_error_event() {
    let e = env(Setup::default());
    e.primary.state.set_mode(mock::Mode::Midstream);
    e.primary.state.token_delay_us.store(2000, Relaxed);
    let r = post(e.gw.addr, &chat("fast", true, 0.7, "x"));
    assert_eq!(r.status, 200);
    assert!(r.body.contains("tok"), "partial output should arrive: {}", r.body);
    assert!(r.body.contains("upstream_error"), "{}", r.body);
    assert!(!r.body.contains("[DONE]"));
    assert_eq!(e.secondary.state.requests.load(Relaxed), 0, "cannot fail over once bytes were sent");
}

#[test]
fn does_not_cache_truncated_streams() {
    let e = env(Setup::default());
    e.primary.state.set_mode(mock::Mode::Midstream);
    e.primary.state.token_delay_us.store(2000, Relaxed);
    let body = chat("fast", true, 0.0, "truncated");
    assert!(post(e.gw.addr, &body).body.contains("upstream_error"));
    e.primary.state.set_mode(mock::Mode::Ok);
    let next = post(e.gw.addr, &body);
    assert_eq!(next.header("x-cache"), Some("MISS"));
    assert!(next.body.ends_with("data: [DONE]\n\n"));
}

#[test]
fn client_disconnect_cancels_the_upstream_stream() {
    let e = env(Setup::default());
    e.primary.state.tokens.store(100_000, Relaxed);
    e.primary.state.token_delay_us.store(1000, Relaxed);
    let mut s = post_raw(e.gw.addr, KEY, &chat("fast", true, 0.7, "x"), "");
    let mut first = [0u8; 256];
    assert!(s.read(&mut first).unwrap() > 0);
    drop(s);
    wait_until("upstream cancellation", || e.primary.state.cancelled.load(Relaxed) == 1);
    assert_eq!(e.primary.state.completed.load(Relaxed), 0);
}

#[test]
fn client_disconnect_during_upstream_wait_cancels_it() {
    let e = env(Setup::default());
    e.primary.state.set_mode(mock::Mode::Hang);
    let s = post_raw(e.gw.addr, KEY, &chat("solo", false, 0.7, "x"), "");
    wait_until("request reaching upstream", || e.primary.state.requests.load(Relaxed) == 1);
    drop(s);
    wait_until("upstream cancellation", || e.primary.state.cancelled.load(Relaxed) == 1);
}

#[test]
fn slow_reader_applies_backpressure_to_the_upstream() {
    let e = env(Setup::default());
    e.primary.state.tokens.store(5_000_000, Relaxed);
    let s = post_raw(e.gw.addr, KEY, &chat("fast", true, 0.7, "x"), "");
    wait_until("upstream to start", || e.primary.state.bytes_sent.load(Relaxed) > 0);
    let settled = || {
        let a = e.primary.state.bytes_sent.load(Relaxed);
        std::thread::sleep(Duration::from_millis(400));
        a == e.primary.state.bytes_sent.load(Relaxed)
    };
    wait_until("upstream to stall", settled);
    let sent = e.primary.state.bytes_sent.load(Relaxed);
    let full_size = 5_000_000u64 * 150;
    assert!(sent < full_size / 10, "gateway buffered too much: {sent} of {full_size} bytes");
    assert_eq!(e.primary.state.completed.load(Relaxed), 0);
    drop(s);
}

#[test]
fn sheds_load_when_the_queue_is_full() {
    let e = env(Setup {
        extra: r#""admission":{"max_inflight":1,"max_queue":1,"queue_timeout_ms":5000},"#.into(),
        ..Setup::default()
    });
    e.primary.state.ttfb_ms.store(400, Relaxed);
    let body = chat("solo", false, 0.7, "x");
    let held = post_raw(e.gw.addr, KEY, &body, "");
    wait_until("first request in flight", || e.primary.state.requests.load(Relaxed) == 1);
    let queued = post_raw(e.gw.addr, KEY, &body, "");
    wait_until("queue to fill", || metric(&e, "gateway_queue_depth") == 1);
    let shed_started = Instant::now();
    let shed = post(e.gw.addr, &body);
    assert_eq!(shed.status, 503);
    assert_eq!(shed.header("retry-after"), Some("1"));
    assert!(shed_started.elapsed() < Duration::from_millis(200), "shedding must be immediate");
    let (mut a, mut b) = (held, queued);
    assert_eq!(read_reply(&mut a).status, 200);
    assert_eq!(read_reply(&mut b).status, 200);
}

#[test]
fn queue_timeout_returns_503() {
    let e = env(Setup {
        extra: r#""admission":{"max_inflight":1,"max_queue":5,"queue_timeout_ms":50},"#.into(),
        ..Setup::default()
    });
    e.primary.state.ttfb_ms.store(500, Relaxed);
    let body = chat("solo", false, 0.7, "x");
    let held = post_raw(e.gw.addr, KEY, &body, "");
    wait_until("first in flight", || e.primary.state.requests.load(Relaxed) == 1);
    let r = post(e.gw.addr, &body);
    assert_eq!(r.status, 503);
    assert_eq!(r.json().get("error").unwrap().get("code").unwrap().as_str(), Some("queue_timeout"));
    drop(held);
}

#[test]
fn propagates_trace_context_and_exposes_metrics() {
    let e = env(Setup::default());
    let tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let r = read_reply(&mut post_raw(
        e.gw.addr,
        KEY,
        &chat("fast", false, 0.7, "x"),
        &format!("traceparent: {tp}\r\nx-request-id: abc-123\r\n"),
    ));
    assert!(r.header("traceparent").unwrap().contains("0af7651916cd43dd8448eb211c80319c"));
    assert_eq!(r.header("x-request-id"), Some("abc-123"));
    wait_until("request metric", || {
        metric(&e, "gateway_requests_total{tenant=\"t1\",model=\"fast\",status=\"200\",cache=\"none\"}") >= 1
            || metric(&e, "gateway_requests_total") >= 1
    });
    let text = get(e.gw.addr, "/metrics", "").body;
    for name in
        ["gateway_added_latency_seconds_bucket", "gateway_upstream_ttfb_seconds_count", "gateway_cost_micro_usd_total"]
    {
        assert!(text.contains(name), "missing {name}");
    }
}

#[test]
fn protects_metrics_with_a_token_when_configured() {
    let primary = mock_server();
    let doc = format!(
        r#"{{"listen":"127.0.0.1:0","workers":1,"metrics_token_env":"M",
        "providers":[{{"name":"p","kind":"openai","base_url":"http://{}","api_key_env":"K"}}],
        "models":[{{"alias":"fast","targets":[{{"provider":"p","model":"m"}}]}}],
        "tenants":[{{"name":"a","key_sha256":["{}"]}}]}}"#,
        primary.addr,
        hash(KEY)
    );
    let gw =
        gateway::start(config::parse(doc.as_bytes(), &|n| Some(format!("value-{n}"))).unwrap(), Logger::disabled())
            .unwrap();
    assert_eq!(get(gw.addr, "/metrics", "").status, 401);
    assert_eq!(get(gw.addr, "/metrics", "authorization: Bearer value-M\r\n").status, 200);
}

#[test]
fn health_models_and_drain_endpoints() {
    let e = env(Setup::default());
    assert_eq!(get(e.gw.addr, "/healthz", "").status, 200);
    assert_eq!(get(e.gw.addr, "/readyz", "").status, 200);
    let models = get(e.gw.addr, "/v1/models", &format!("authorization: Bearer {KEY2}\r\n"));
    assert_eq!(models.status, 200);
    assert!(models.body.contains("solo") && !models.body.contains("fast"));
    assert_eq!(get(e.gw.addr, "/nope", "").status, 404);
    e.gw.gateway.begin_drain();
    assert_eq!(get(e.gw.addr, "/readyz", "").status, 503);
}

#[test]
fn drain_lets_in_flight_streams_finish() {
    let e = env(Setup::default());
    e.primary.state.tokens.store(50, Relaxed);
    e.primary.state.token_delay_us.store(5000, Relaxed);
    let mut s = post_raw(e.gw.addr, KEY, &chat("fast", true, 0.7, "x"), "");
    wait_until("stream start", || e.gw.gateway.active_requests() == 1);
    e.gw.gateway.begin_drain();
    let r = read_reply(&mut s);
    assert!(r.body.ends_with("data: [DONE]\n\n"));
    wait_until("drain", || e.gw.gateway.active_requests() == 0);
}

#[test]
fn quota_usage_survives_a_restart() {
    let dir = std::env::temp_dir().join(format!("gw-quota-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("q.json").to_string_lossy().to_string();
    let primary = mock_server();
    let doc = format!(
        r#"{{"listen":"127.0.0.1:0","workers":1,"quota_snapshot_path":"{path}",
        "providers":[{{"name":"p","kind":"openai","base_url":"http://{}","api_key_env":"K"}}],
        "models":[{{"alias":"fast","targets":[{{"provider":"p","model":"m"}}]}}],
        "tenants":[{{"name":"a","key_sha256":["{}"],"daily_tokens":1000}}]}}"#,
        primary.addr,
        hash(KEY)
    );
    let first =
        gateway::start(config::parse(doc.as_bytes(), &|_| Some("s".into())).unwrap(), Logger::disabled()).unwrap();
    assert_eq!(post(first.addr, &chat("fast", false, 0.7, "x")).status, 200);
    first.gateway.save_quota(&path).unwrap();
    let saved = std::fs::read_to_string(&path).unwrap();
    let parsed = json::parse(saved.as_bytes()).unwrap();
    let used = parsed.get("a").unwrap().as_arr().unwrap()[1].as_u64().unwrap();
    assert_eq!(used, 18);
    let second =
        gateway::start(config::parse(doc.as_bytes(), &|_| Some("s".into())).unwrap(), Logger::disabled()).unwrap();
    second.gateway.save_quota(&path).unwrap();
    let again = json::parse(std::fs::read(&path).unwrap().as_slice()).unwrap();
    assert_eq!(again.get("a").unwrap().as_arr().unwrap()[1].as_u64().unwrap(), 18);
    std::fs::remove_dir_all(dir).ok();
}

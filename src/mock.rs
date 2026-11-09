//! A mock LLM provider with fault injection, for tests and load testing. It
//! speaks both the OpenAI and Anthropic wire formats.

use crate::http::{self, BufConn, Limits, Request};
use crate::json::{self, Value};
use crate::rt::{
    self,
    net::{Acceptor, TcpConn},
    sys, Either,
};
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Mode {
    Ok = 0,
    Error500 = 1,
    Error429 = 2,
    Hang = 3,
    Reset = 4,
    Midstream = 5,
    Error401 = 6,
    Error400 = 7,
}

impl Mode {
    fn from_u64(v: u64) -> Mode {
        match v {
            1 => Mode::Error500,
            2 => Mode::Error429,
            3 => Mode::Hang,
            4 => Mode::Reset,
            5 => Mode::Midstream,
            6 => Mode::Error401,
            7 => Mode::Error400,
            _ => Mode::Ok,
        }
    }

    fn parse(s: &str) -> Option<Mode> {
        Some(match s {
            "ok" => Mode::Ok,
            "error500" => Mode::Error500,
            "error429" => Mode::Error429,
            "hang" => Mode::Hang,
            "reset" => Mode::Reset,
            "midstream" => Mode::Midstream,
            "error401" => Mode::Error401,
            "error400" => Mode::Error400,
            _ => return None,
        })
    }
}

/// Live-tunable behavior and counters.
#[derive(Default)]
pub struct MockState {
    pub mode: AtomicU64,
    pub ttfb_ms: AtomicU64,
    pub tokens: AtomicU64,
    pub token_delay_us: AtomicU64,
    pub fail_next: AtomicU64,
    pub requests: AtomicU64,
    pub completed: AtomicU64,
    pub cancelled: AtomicU64,
    pub bytes_sent: AtomicU64,
}

impl MockState {
    pub fn set_mode(&self, m: Mode) {
        self.mode.store(m as u64, Relaxed);
    }
}

/// A running mock server.
pub struct Mock {
    pub addr: SocketAddr,
    pub state: Arc<MockState>,
}

/// Starts the mock on `workers` threads. Port 0 selects an ephemeral port.
pub fn start(
    listen: SocketAddr,
    workers: usize,
    ttfb_ms: u64,
    tokens: u64,
    token_delay_us: u64,
) -> std::io::Result<Mock> {
    let state = Arc::new(MockState::default());
    state.ttfb_ms.store(ttfb_ms, Relaxed);
    state.tokens.store(tokens, Relaxed);
    state.token_delay_us.store(token_delay_us, Relaxed);
    let (first_fd, addr) = sys::listen_reuseport(&listen, 4096)?;
    for i in 0..workers.max(1) {
        let fd = if i == 0 { first_fd } else { sys::listen_reuseport(&addr, 4096)?.0 };
        let st = state.clone();
        std::thread::Builder::new().name(format!("mock-{i}")).spawn(move || {
            rt::block_on(async move {
                let acceptor = Acceptor::from_raw(fd).expect("register mock listener");
                loop {
                    match acceptor.accept().await {
                        Ok(stream) => {
                            let _ = stream.set_nodelay(true);
                            let st = st.clone();
                            rt::spawn(async move {
                                if let Ok(conn) = TcpConn::from_std(stream) {
                                    serve(conn, st).await;
                                }
                            });
                        }
                        Err(_) => rt::sleep(Duration::from_millis(10)).await,
                    }
                }
            })
        })?;
    }
    Ok(Mock { addr, state })
}

const LIMITS: Limits = Limits { max_head: 64 * 1024, max_body: 16 * 1024 * 1024 };

async fn serve(conn: TcpConn, st: Arc<MockState>) {
    let mut io = BufConn::new(conn);
    loop {
        let req = match io.read_request(LIMITS).await {
            Ok(Some(r)) => r,
            _ => return,
        };
        let keep = match req.path.as_str() {
            "/__control" => control(&req, &io, &st).await,
            "/__stats" => stats(&io, &st).await,
            "/v1/chat/completions" => completion(&req, &io, &st, false).await,
            "/v1/messages" => completion(&req, &io, &st, true).await,
            _ => json_response(&io, 404, b"{}").await,
        };
        if !keep {
            return;
        }
    }
}

async fn json_response(io: &BufConn, status: u16, body: &[u8]) -> bool {
    let mut out = Vec::with_capacity(body.len() + 128);
    http::start_head(status, true, &mut out);
    http::push_header(&mut out, "content-type", "application/json");
    http::push_header(&mut out, "content-length", &body.len().to_string());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(body);
    io.conn.write_all(&out).await.is_ok()
}

async fn control(req: &Request, io: &BufConn, st: &MockState) -> bool {
    let v = json::parse(&req.body).unwrap_or(Value::Null);
    if let Some(m) = v.get("mode").and_then(Value::as_str).and_then(Mode::parse) {
        st.set_mode(m);
    }
    for (key, cell) in [
        ("ttfb_ms", &st.ttfb_ms),
        ("tokens", &st.tokens),
        ("token_delay_us", &st.token_delay_us),
        ("fail_next", &st.fail_next),
    ] {
        if let Some(n) = v.get(key).and_then(Value::as_u64) {
            cell.store(n, Relaxed);
        }
    }
    json_response(io, 200, b"{\"ok\":true}").await
}

async fn stats(io: &BufConn, st: &MockState) -> bool {
    let body = format!(
        "{{\"requests\":{},\"completed\":{},\"cancelled\":{},\"bytes_sent\":{}}}",
        st.requests.load(Relaxed),
        st.completed.load(Relaxed),
        st.cancelled.load(Relaxed),
        st.bytes_sent.load(Relaxed)
    );
    json_response(io, 200, body.as_bytes()).await
}

fn token_text(i: u64) -> String {
    format!("tok{} ", i % 1000)
}

fn openai_chunk(content: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(220);
    out.extend_from_slice(b"data: {\"id\":\"chatcmpl-mock\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"mock\",\"choices\":[{\"index\":0,\"delta\":{\"content\":");
    json::write_str(content, &mut out);
    out.extend_from_slice(b"},\"finish_reason\":null}]}\n\n");
    out
}

fn anthropic_delta(content: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(160);
    out.extend_from_slice(b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":");
    json::write_str(content, &mut out);
    out.extend_from_slice(b"}}\n\n");
    out
}

/// Waits `d`, returning false if the peer hangs up first.
async fn wait_or_cancel(io: &BufConn, d: Duration) -> bool {
    if d.is_zero() {
        return !io.conn.is_closed();
    }
    let closed = pin!(io.conn.closed());
    match rt::select(pin!(rt::sleep(d)), closed).await {
        Either::Left(()) => true,
        Either::Right(()) => false,
    }
}

async fn completion(req: &Request, io: &BufConn, st: &MockState, anthropic: bool) -> bool {
    st.requests.fetch_add(1, Relaxed);
    let parsed = json::parse(&req.body).unwrap_or(Value::Null);
    let stream = parsed.get("stream").and_then(Value::as_bool) == Some(true);
    let include_usage =
        parsed.get("stream_options").and_then(|o| o.get("include_usage")).and_then(Value::as_bool) == Some(true);

    let mut mode = Mode::from_u64(st.mode.load(Relaxed));
    let injected = st.fail_next.fetch_update(Relaxed, Relaxed, |n| n.checked_sub(1)).is_ok();
    if injected {
        mode = Mode::Error500;
    }

    let ttfb = Duration::from_millis(st.ttfb_ms.load(Relaxed));
    match mode {
        Mode::Reset => return false,
        Mode::Hang => {
            wait_or_cancel(io, Duration::from_secs(3600)).await;
            st.cancelled.fetch_add(1, Relaxed);
            return false;
        }
        Mode::Error500 => return error(io, 500, "server_error", None).await,
        Mode::Error429 => return error(io, 429, "rate_limited", Some("7")).await,
        Mode::Error401 => return error(io, 401, "bad_key", None).await,
        Mode::Error400 => return error(io, 400, "invalid_request", None).await,
        Mode::Ok | Mode::Midstream => {}
    }
    if !wait_or_cancel(io, ttfb).await {
        st.cancelled.fetch_add(1, Relaxed);
        return false;
    }

    let tokens = st.tokens.load(Relaxed);
    if !stream {
        let text: String = (0..tokens).map(token_text).collect();
        let body = if anthropic {
            let mut b = Vec::with_capacity(text.len() + 200);
            b.extend_from_slice(b"{\"id\":\"msg_mock\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":");
            json::write_str(&text, &mut b);
            b.extend_from_slice(
                format!(
                    "}}],\"stop_reason\":\"end_turn\",\"usage\":{{\"input_tokens\":10,\"output_tokens\":{tokens}}}}}"
                )
                .as_bytes(),
            );
            b
        } else {
            let mut b = Vec::with_capacity(text.len() + 240);
            b.extend_from_slice(b"{\"id\":\"chatcmpl-mock\",\"object\":\"chat.completion\",\"created\":1700000000,\"model\":\"mock\",\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":");
            json::write_str(&text, &mut b);
            b.extend_from_slice(format!("}},\"finish_reason\":\"stop\"}}],\"usage\":{{\"prompt_tokens\":10,\"completion_tokens\":{tokens},\"total_tokens\":{}}}}}", tokens + 10).as_bytes());
            b
        };
        st.bytes_sent.fetch_add(body.len() as u64, Relaxed);
        let ok = json_response(io, 200, &body).await;
        if ok {
            st.completed.fetch_add(1, Relaxed);
        }
        return ok;
    }

    let delay = Duration::from_micros(st.token_delay_us.load(Relaxed));
    let mut out = Vec::with_capacity(16 * 1024);
    http::start_head(200, true, &mut out);
    http::push_header(&mut out, "content-type", "text/event-stream");
    http::push_header(&mut out, "transfer-encoding", "chunked");
    out.extend_from_slice(b"\r\n");
    if anthropic {
        http::push_chunk(&mut out, b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_mock\",\"usage\":{\"input_tokens\":10}}}\n\n");
    }

    let stop_at = if mode == Mode::Midstream { tokens / 2 } else { tokens };
    for i in 0..stop_at {
        let text = token_text(i);
        let event = if anthropic { anthropic_delta(&text) } else { openai_chunk(&text) };
        http::push_chunk(&mut out, &event);
        if !delay.is_zero() || out.len() > 8 * 1024 {
            st.bytes_sent.fetch_add(out.len() as u64, Relaxed);
            if io.conn.write_all(&out).await.is_err() {
                st.cancelled.fetch_add(1, Relaxed);
                return false;
            }
            out.clear();
            if !wait_or_cancel(io, delay).await {
                st.cancelled.fetch_add(1, Relaxed);
                return false;
            }
        }
    }
    if mode == Mode::Midstream {
        let _ = io.conn.write_all(&out).await;
        return false;
    }
    if anthropic {
        http::push_chunk(&mut out, format!("event: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"end_turn\"}},\"usage\":{{\"output_tokens\":{tokens}}}}}\n\n").as_bytes());
        http::push_chunk(&mut out, b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n");
    } else {
        http::push_chunk(&mut out, b"data: {\"id\":\"chatcmpl-mock\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"mock\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n");
        if include_usage {
            http::push_chunk(&mut out, format!("data: {{\"id\":\"chatcmpl-mock\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"mock\",\"choices\":[],\"usage\":{{\"prompt_tokens\":10,\"completion_tokens\":{tokens},\"total_tokens\":{}}}}}\n\n", tokens + 10).as_bytes());
        }
        http::push_chunk(&mut out, b"data: [DONE]\n\n");
    }
    out.extend_from_slice(http::CHUNK_END);
    st.bytes_sent.fetch_add(out.len() as u64, Relaxed);
    if io.conn.write_all(&out).await.is_err() {
        st.cancelled.fetch_add(1, Relaxed);
        return false;
    }
    st.completed.fetch_add(1, Relaxed);
    true
}

async fn error(io: &BufConn, status: u16, code: &str, retry_after: Option<&str>) -> bool {
    let body = format!("{{\"error\":{{\"message\":\"mock {code}\",\"type\":\"{code}\"}}}}");
    let mut out = Vec::with_capacity(body.len() + 160);
    http::start_head(status, true, &mut out);
    http::push_header(&mut out, "content-type", "application/json");
    http::push_header(&mut out, "content-length", &body.len().to_string());
    if let Some(r) = retry_after {
        http::push_header(&mut out, "retry-after", r);
    }
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(body.as_bytes());
    io.conn.write_all(&out).await.is_ok()
}

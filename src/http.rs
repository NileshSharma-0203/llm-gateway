//! A strict HTTP/1.1 implementation covering what a proxy needs: request
//! parsing with smuggling defenses, chunked encoding, and a client.

use crate::rt::net::TcpConn;
use std::io::{self, ErrorKind};

const READ_CHUNK: usize = 16 * 1024;

/// Header and body limits for the inbound server.
#[derive(Clone, Copy)]
pub struct Limits {
    pub max_head: usize,
    pub max_body: usize,
}

#[derive(Debug)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub keep_alive: bool,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        header_value(&self.headers, name)
    }
}

pub fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
}

/// A request rejected before it reached a handler.
#[derive(Debug)]
pub struct HttpError {
    pub status: u16,
    pub message: &'static str,
}

fn bad(message: &'static str) -> HttpError {
    HttpError { status: 400, message }
}

fn is_token(b: u8) -> bool {
    matches!(b, b'!' | b'#' | b'$' | b'%' | b'&' | b'\'' | b'*' | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|' | b'~')
        || b.is_ascii_alphanumeric()
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
}

struct Head {
    first_line: String,
    headers: Vec<(String, String)>,
}

fn parse_head(raw: &[u8]) -> Result<Head, HttpError> {
    let text = std::str::from_utf8(raw).map_err(|_| bad("header is not valid UTF-8"))?;
    let mut lines = text.split("\r\n");
    let first_line = lines.next().unwrap_or_default().to_string();
    if first_line.contains(['\n', '\r']) {
        return Err(bad("bare line ending in request line"));
    }
    let mut headers = Vec::with_capacity(16);
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if line.starts_with([' ', '\t']) {
            return Err(bad("obsolete header folding is not allowed"));
        }
        let (name, value) = line.split_once(':').ok_or(bad("malformed header"))?;
        if name.is_empty() || !name.bytes().all(is_token) {
            return Err(bad("invalid header name"));
        }
        if value.bytes().any(|b| b == 0 || b == b'\n' || b == b'\r') {
            return Err(bad("invalid header value"));
        }
        headers.push((name.to_ascii_lowercase(), value.trim_matches([' ', '\t']).to_string()));
    }
    Ok(Head { first_line, headers })
}

/// Determines the request body framing, rejecting ambiguity that enables
/// request smuggling.
enum BodyKind {
    None,
    Length(usize),
    Chunked,
}

fn body_kind(headers: &[(String, String)]) -> Result<BodyKind, HttpError> {
    let mut length: Option<usize> = None;
    let mut chunked = false;
    for (k, v) in headers {
        if k == "content-length" {
            if v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) {
                return Err(bad("invalid content-length"));
            }
            let parsed: usize = v.parse().map_err(|_| bad("invalid content-length"))?;
            if length.is_some_and(|prev| prev != parsed) {
                return Err(bad("conflicting content-length headers"));
            }
            length = Some(parsed);
        } else if k == "transfer-encoding" {
            if !v.eq_ignore_ascii_case("chunked") || chunked {
                return Err(HttpError { status: 501, message: "unsupported transfer-encoding" });
            }
            chunked = true;
        }
    }
    match (length, chunked) {
        (Some(_), true) => Err(bad("both content-length and transfer-encoding present")),
        (Some(n), false) => Ok(BodyKind::Length(n)),
        (None, true) => Ok(BodyKind::Chunked),
        (None, false) => Ok(BodyKind::None),
    }
}

/// Connection plus read buffer.
pub struct BufConn {
    pub conn: TcpConn,
    buf: Vec<u8>,
    pos: usize,
}

impl BufConn {
    pub fn new(conn: TcpConn) -> Self {
        Self { conn, buf: Vec::with_capacity(READ_CHUNK), pos: 0 }
    }

    fn data(&self) -> &[u8] {
        &self.buf[self.pos..]
    }

    fn consume(&mut self, n: usize) {
        self.pos += n;
        if self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        }
    }

    async fn fill(&mut self) -> io::Result<usize> {
        if self.pos > 0 {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
        let old = self.buf.len();
        self.buf.resize(old + READ_CHUNK, 0);
        let result = self.conn.read(&mut self.buf[old..]).await;
        let n = *result.as_ref().unwrap_or(&0);
        self.buf.truncate(old + n);
        result
    }

    /// True if unread bytes remain.
    pub fn has_buffered(&self) -> bool {
        self.pos < self.buf.len()
    }

    /// Reads one request. Returns `Ok(None)` on a clean close between requests.
    pub async fn read_request(&mut self, limits: Limits) -> Result<Option<Request>, HttpError> {
        let head_end = loop {
            if let Some(end) = find_head_end(self.data()) {
                if end > limits.max_head {
                    return Err(HttpError { status: 431, message: "request header too large" });
                }
                break end;
            }
            if self.data().len() > limits.max_head {
                return Err(HttpError { status: 431, message: "request header too large" });
            }
            match self.fill().await {
                Ok(0) if self.data().is_empty() => return Ok(None),
                Ok(0) => return Err(bad("connection closed mid-request")),
                Ok(_) => {}
                Err(_) => return Ok(None),
            }
        };
        let head = parse_head(&self.data()[..head_end - 2])?;
        self.consume(head_end);

        let mut parts = head.first_line.split(' ');
        let (method, target, version) = match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some(m), Some(t), Some(v), None) if !m.is_empty() && !t.is_empty() => (m, t, v),
            _ => return Err(bad("malformed request line")),
        };
        if !method.bytes().all(is_token) {
            return Err(bad("invalid method"));
        }
        let keep_default = match version {
            "HTTP/1.1" => true,
            "HTTP/1.0" => false,
            _ => return Err(HttpError { status: 505, message: "unsupported HTTP version" }),
        };
        let connection = header_value(&head.headers, "connection").unwrap_or("").to_ascii_lowercase();
        let keep_alive =
            if connection.contains("close") { false } else { keep_default || connection.contains("keep-alive") };

        let kind = body_kind(&head.headers)?;
        if header_value(&head.headers, "expect").is_some_and(|v| v.eq_ignore_ascii_case("100-continue")) {
            let too_large = matches!(kind, BodyKind::Length(n) if n > limits.max_body);
            if !too_large {
                let _ = self.conn.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await;
            }
        }
        let body = match kind {
            BodyKind::None => Vec::new(),
            BodyKind::Length(n) => self.read_fixed(n, limits.max_body).await?,
            BodyKind::Chunked => self.read_chunked(limits.max_body).await?,
        };
        let path = target.split('?').next().unwrap_or(target).to_string();
        Ok(Some(Request { method: method.to_string(), path, headers: head.headers, body, keep_alive }))
    }

    async fn read_fixed(&mut self, n: usize, max: usize) -> Result<Vec<u8>, HttpError> {
        if n > max {
            return Err(HttpError { status: 413, message: "request body too large" });
        }
        let mut body = Vec::with_capacity(n);
        while body.len() < n {
            if self.data().is_empty() {
                match self.fill().await {
                    Ok(0) | Err(_) => return Err(bad("connection closed mid-body")),
                    Ok(_) => {}
                }
            }
            let take = (n - body.len()).min(self.data().len());
            body.extend_from_slice(&self.data()[..take]);
            self.consume(take);
        }
        Ok(body)
    }

    async fn read_chunked(&mut self, max: usize) -> Result<Vec<u8>, HttpError> {
        let mut decoder = ChunkedDecoder::default();
        let mut body = Vec::new();
        loop {
            if self.data().is_empty() {
                match self.fill().await {
                    Ok(0) | Err(_) => return Err(bad("connection closed mid-body")),
                    Ok(_) => {}
                }
            }
            let input_len = self.data().len();
            let used = {
                let data = &self.buf[self.pos..];
                decoder.decode(data, &mut body).map_err(|_| bad("invalid chunked encoding"))?
            };
            debug_assert!(used <= input_len);
            self.consume(used);
            if body.len() > max {
                return Err(HttpError { status: 413, message: "request body too large" });
            }
            if decoder.is_done() {
                return Ok(body);
            }
        }
    }
}

#[derive(Default, Clone, Copy, PartialEq, Debug)]
enum ChunkState {
    #[default]
    Size,
    SizeExt,
    SizeLf,
    Data,
    DataCr,
    DataLf,
    Trailer,
    TrailerLine,
    FinalLf,
    Done,
}

/// Malformed chunked framing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BadChunk;

/// Incremental chunked-transfer decoder.
#[derive(Default)]
pub struct ChunkedDecoder {
    state: ChunkState,
    remaining: u64,
    digits: u8,
}

impl ChunkedDecoder {
    pub fn is_done(&self) -> bool {
        self.state == ChunkState::Done
    }

    /// Decodes as much of `input` as possible and returns the bytes consumed.
    pub fn decode(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<usize, BadChunk> {
        let mut i = 0;
        while i < input.len() && self.state != ChunkState::Done {
            let b = input[i];
            match self.state {
                ChunkState::Size => {
                    if let Some(d) = (b as char).to_digit(16) {
                        self.digits += 1;
                        if self.digits > 12 {
                            return Err(BadChunk);
                        }
                        self.remaining = self.remaining * 16 + d as u64;
                        i += 1;
                    } else if self.digits == 0 {
                        return Err(BadChunk);
                    } else if b == b';' {
                        self.state = ChunkState::SizeExt;
                        i += 1;
                    } else if b == b'\r' {
                        self.state = ChunkState::SizeLf;
                        i += 1;
                    } else {
                        return Err(BadChunk);
                    }
                }
                ChunkState::SizeExt => {
                    if b == b'\r' {
                        self.state = ChunkState::SizeLf;
                    }
                    i += 1;
                }
                ChunkState::SizeLf => {
                    if b != b'\n' {
                        return Err(BadChunk);
                    }
                    i += 1;
                    self.digits = 0;
                    self.state = if self.remaining == 0 { ChunkState::Trailer } else { ChunkState::Data };
                }
                ChunkState::Data => {
                    let take = (self.remaining.min((input.len() - i) as u64)) as usize;
                    out.extend_from_slice(&input[i..i + take]);
                    i += take;
                    self.remaining -= take as u64;
                    if self.remaining == 0 {
                        self.state = ChunkState::DataCr;
                    }
                }
                ChunkState::DataCr => {
                    if b != b'\r' {
                        return Err(BadChunk);
                    }
                    i += 1;
                    self.state = ChunkState::DataLf;
                }
                ChunkState::DataLf => {
                    if b != b'\n' {
                        return Err(BadChunk);
                    }
                    i += 1;
                    self.state = ChunkState::Size;
                }
                ChunkState::Trailer => {
                    i += 1;
                    self.state = if b == b'\r' { ChunkState::FinalLf } else { ChunkState::TrailerLine };
                }
                ChunkState::TrailerLine => {
                    i += 1;
                    if b == b'\n' {
                        self.state = ChunkState::Trailer;
                    }
                }
                ChunkState::FinalLf => {
                    if b != b'\n' {
                        return Err(BadChunk);
                    }
                    i += 1;
                    self.state = ChunkState::Done;
                }
                ChunkState::Done => unreachable!(),
            }
        }
        Ok(i)
    }
}

/// Standard reason phrase for the statuses this gateway emits.
pub fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        413 => "Payload Too Large",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        505 => "HTTP Version Not Supported",
        _ => "Unknown",
    }
}

/// Serializes the status line and common headers. Caller appends the rest,
/// then a blank line.
pub fn start_head(status: u16, keep_alive: bool, out: &mut Vec<u8>) {
    out.extend_from_slice(b"HTTP/1.1 ");
    out.extend_from_slice(status.to_string().as_bytes());
    out.push(b' ');
    out.extend_from_slice(reason(status).as_bytes());
    out.extend_from_slice(if keep_alive { b"\r\nconnection: keep-alive\r\n" } else { b"\r\nconnection: close\r\n" });
}

pub fn push_header(out: &mut Vec<u8>, name: &str, value: &str) {
    out.extend_from_slice(name.as_bytes());
    out.extend_from_slice(b": ");
    out.extend_from_slice(value.as_bytes());
    out.extend_from_slice(b"\r\n");
}

/// Appends one chunked-encoding frame.
pub fn push_chunk(out: &mut Vec<u8>, data: &[u8]) {
    if data.is_empty() {
        return;
    }
    out.extend_from_slice(format!("{:x}\r\n", data.len()).as_bytes());
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
}

pub const CHUNK_END: &[u8] = b"0\r\n\r\n";

#[derive(Debug)]
enum Framing {
    Length(u64),
    Chunked(ChunkedDecoder),
    UntilClose,
    Empty,
    Finished,
}

/// A parsed upstream response head.
#[derive(Debug)]
pub struct ResponseHead {
    pub status: u16,
    pub headers: Vec<(String, String)>,
}

impl std::fmt::Debug for ChunkedDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ChunkedDecoder({:?})", self.state)
    }
}

/// HTTP/1.1 client connection.
pub struct ClientConn {
    io: BufConn,
    framing: Framing,
    keep_alive: bool,
    pub got_bytes: bool,
}

impl ClientConn {
    pub fn new(conn: TcpConn) -> Self {
        Self { io: BufConn::new(conn), framing: Framing::Finished, keep_alive: true, got_bytes: false }
    }

    pub async fn send(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.got_bytes = false;
        self.framing = Framing::Finished;
        self.io.conn.write_all(bytes).await
    }

    /// Reads the response head, skipping interim 1xx responses.
    pub async fn read_head(&mut self, is_head_request: bool) -> io::Result<ResponseHead> {
        loop {
            let end = loop {
                if let Some(end) = find_head_end(self.io.data()) {
                    break end;
                }
                if self.io.data().len() > 64 * 1024 {
                    return Err(invalid("upstream header too large"));
                }
                if self.io.fill().await? == 0 {
                    return Err(ErrorKind::UnexpectedEof.into());
                }
                self.got_bytes = true;
            };
            self.got_bytes = true;
            let head = parse_head(&self.io.data()[..end - 2]).map_err(|e| invalid(e.message))?;
            self.io.consume(end);
            let mut parts = head.first_line.splitn(3, ' ');
            let version = parts.next().unwrap_or("");
            if !version.starts_with("HTTP/1.") {
                return Err(invalid("upstream did not speak HTTP/1.x"));
            }
            let status: u16 = parts.next().and_then(|s| s.parse().ok()).ok_or_else(|| invalid("bad status"))?;
            if (100..200).contains(&status) {
                continue;
            }
            let connection = header_value(&head.headers, "connection").unwrap_or("").to_ascii_lowercase();
            self.keep_alive = version == "HTTP/1.1" && !connection.contains("close");
            let te_chunked = header_value(&head.headers, "transfer-encoding")
                .is_some_and(|v| v.to_ascii_lowercase().contains("chunked"));
            let length = header_value(&head.headers, "content-length").and_then(|v| v.parse::<u64>().ok());
            self.framing = if is_head_request || status == 204 || status == 304 {
                Framing::Empty
            } else if te_chunked {
                Framing::Chunked(ChunkedDecoder::default())
            } else if let Some(n) = length {
                if n == 0 {
                    Framing::Empty
                } else {
                    Framing::Length(n)
                }
            } else {
                self.keep_alive = false;
                Framing::UntilClose
            };
            return Ok(ResponseHead { status, headers: head.headers });
        }
    }

    /// Appends decoded body bytes to `out`. Returns `false` once the body is complete.
    pub async fn read_body(&mut self, out: &mut Vec<u8>) -> io::Result<bool> {
        loop {
            match &mut self.framing {
                Framing::Empty | Framing::Finished => {
                    self.framing = Framing::Finished;
                    return Ok(false);
                }
                Framing::Length(remaining) => {
                    let remaining = *remaining;
                    if self.io.data().is_empty() && self.io.fill().await? == 0 {
                        return Err(ErrorKind::UnexpectedEof.into());
                    }
                    let take = (remaining as usize).min(self.io.data().len());
                    out.extend_from_slice(&self.io.data()[..take]);
                    self.io.consume(take);
                    let left = remaining - take as u64;
                    self.framing = if left == 0 { Framing::Finished } else { Framing::Length(left) };
                    return Ok(left != 0);
                }
                Framing::UntilClose => {
                    if self.io.data().is_empty() && self.io.fill().await? == 0 {
                        self.framing = Framing::Finished;
                        return Ok(false);
                    }
                    let n = self.io.data().len();
                    out.extend_from_slice(self.io.data());
                    self.io.consume(n);
                    return Ok(true);
                }
                Framing::Chunked(_) => {
                    if self.io.data().is_empty() && self.io.fill().await? == 0 {
                        return Err(ErrorKind::UnexpectedEof.into());
                    }
                    let before = out.len();
                    let Framing::Chunked(decoder) = &mut self.framing else { unreachable!() };
                    let data = &self.io.buf[self.io.pos..];
                    let used = decoder.decode(data, out).map_err(|_| invalid("bad chunked framing"))?;
                    let done = decoder.is_done();
                    self.io.consume(used);
                    if done {
                        self.framing = Framing::Finished;
                        return Ok(false);
                    }
                    if out.len() > before {
                        return Ok(true);
                    }
                }
            }
        }
    }

    /// True if this connection can serve another request.
    pub fn reusable(&self) -> bool {
        self.keep_alive
            && matches!(self.framing, Framing::Finished)
            && !self.io.has_buffered()
            && !self.io.conn.is_closed()
    }
}

fn invalid(msg: &'static str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_all(input: &[u8], split: usize) -> Result<(Vec<u8>, bool), BadChunk> {
        let mut d = ChunkedDecoder::default();
        let mut out = Vec::new();
        d.decode(&input[..split], &mut out)?;
        d.decode(&input[split..], &mut out)?;
        Ok((out, d.is_done()))
    }

    #[test]
    fn chunked_decodes_at_every_split_point() {
        let input = b"5\r\nhello\r\n7;ext=1\r\n, world\r\n0\r\nx-trailer: 1\r\n\r\n";
        for split in 0..input.len() {
            assert_eq!(decode_all(input, split).unwrap(), (b"hello, world".to_vec(), true), "split {split}");
        }
    }

    #[test]
    fn chunked_rejects_bad_framing() {
        for bad in [&b"zz\r\n"[..], b"\r\n", b"5\r\nhelloX\r\n", b"5\nhello", b"1234567890abcdef\r\n"] {
            assert!(decode_all(bad, 0).is_err(), "{:?}", String::from_utf8_lossy(bad));
        }
    }

    fn kind_of(headers: &[(&str, &str)]) -> Result<BodyKind, HttpError> {
        let owned: Vec<_> = headers.iter().map(|(a, b)| (a.to_string(), b.to_string())).collect();
        body_kind(&owned)
    }

    #[test]
    fn body_framing_rejects_smuggling_vectors() {
        assert!(kind_of(&[("content-length", "5"), ("transfer-encoding", "chunked")]).is_err());
        assert!(kind_of(&[("content-length", "5"), ("content-length", "6")]).is_err());
        assert!(kind_of(&[("content-length", "+5")]).is_err());
        assert!(kind_of(&[("transfer-encoding", "gzip, chunked")]).is_err());
        assert!(matches!(kind_of(&[("content-length", "5"), ("content-length", "5")]), Ok(BodyKind::Length(5))));
        assert!(matches!(kind_of(&[("transfer-encoding", "Chunked")]), Ok(BodyKind::Chunked)));
    }

    #[test]
    fn head_parser_rejects_folding_and_bad_names() {
        assert!(parse_head(b"GET / HTTP/1.1\r\na: b\r\n continued\r\n\r\n").is_err());
        assert!(parse_head(b"GET / HTTP/1.1\r\nbad name: b\r\n\r\n").is_err());
        assert!(parse_head(b"GET / HTTP/1.1\r\nnocolon\r\n\r\n").is_err());
        let ok = parse_head(b"GET / HTTP/1.1\r\nX-Thing:  v \r\n\r\n").ok().unwrap();
        assert_eq!(ok.headers, vec![("x-thing".to_string(), "v".to_string())]);
    }

    #[test]
    fn chunk_framing_writes_hex_sizes() {
        let mut out = Vec::new();
        push_chunk(&mut out, b"0123456789abcdef");
        assert_eq!(out, b"10\r\n0123456789abcdef\r\n");
        let mut empty = Vec::new();
        push_chunk(&mut empty, b"");
        assert!(empty.is_empty());
    }
}

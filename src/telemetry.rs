//! W3C trace context and structured JSON logs. Lines are handed to a dedicated
//! thread, and dropped rather than queued without bound, so a blocked stdout
//! can never stall a worker.

use crate::json::write_str;
use crate::sha256::hex;
use crate::util::fast_rand;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::Arc;

/// W3C `traceparent` identifiers for one request.
#[derive(Clone, Debug)]
pub struct Trace {
    pub trace_id: String,
    pub span_id: String,
    pub parent_id: Option<String>,
    pub flags: String,
}

fn rand_hex(bytes: usize) -> String {
    let mut buf = Vec::with_capacity(bytes + 8);
    while buf.len() < bytes {
        buf.extend_from_slice(&fast_rand().to_ne_bytes());
    }
    buf.truncate(bytes);
    hex(&buf)
}

/// Generates a new span id.
pub fn new_span_id() -> String {
    rand_hex(8)
}

fn is_lower_hex(s: &str, len: usize) -> bool {
    s.len() == len
        && s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        && s.bytes().any(|b| b != b'0')
}

impl Trace {
    /// Continues the caller's trace if `header` is valid, else starts a new one.
    pub fn from_header(header: Option<&str>) -> Self {
        if let Some(h) = header {
            let parts: Vec<&str> = h.trim().split('-').collect();
            if let [version, trace, parent, flags] = parts[..] {
                if version != "ff"
                    && version.len() == 2
                    && is_lower_hex(trace, 32)
                    && is_lower_hex(parent, 16)
                    && flags.len() == 2
                {
                    return Self {
                        trace_id: trace.to_string(),
                        span_id: new_span_id(),
                        parent_id: Some(parent.to_string()),
                        flags: flags.to_string(),
                    };
                }
            }
        }
        Self { trace_id: rand_hex(16), span_id: new_span_id(), parent_id: None, flags: "01".into() }
    }

    /// `traceparent` value naming `span` under this trace.
    pub fn header_for(&self, span: &str) -> String {
        format!("00-{}-{}-{}", self.trace_id, span, self.flags)
    }
}

/// Builds one JSON log line.
pub struct LogLine(Vec<u8>);

impl LogLine {
    pub fn new(msg: &str) -> Self {
        let mut line = LogLine(Vec::with_capacity(512));
        line.0.extend_from_slice(b"{\"ts_ms\":");
        let ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
        line.0.extend_from_slice(ms.to_string().as_bytes());
        line.str("msg", msg)
    }

    fn key(&mut self, k: &str) {
        self.0.push(b',');
        write_str(k, &mut self.0);
        self.0.push(b':');
    }

    pub fn str(mut self, k: &str, v: &str) -> Self {
        self.key(k);
        write_str(v, &mut self.0);
        self
    }

    pub fn num(mut self, k: &str, v: impl std::fmt::Display) -> Self {
        self.key(k);
        self.0.extend_from_slice(v.to_string().as_bytes());
        self
    }

    pub fn bool(mut self, k: &str, v: bool) -> Self {
        self.key(k);
        self.0.extend_from_slice(if v { b"true" } else { b"false" });
        self
    }

    fn finish(mut self) -> Vec<u8> {
        self.0.extend_from_slice(b"}\n");
        self.0
    }
}

enum Item {
    Line(Vec<u8>),
    Flush(SyncSender<()>),
}

/// Asynchronous log sink.
#[derive(Clone)]
pub struct Logger {
    tx: Option<SyncSender<Item>>,
    pub dropped: Arc<AtomicU64>,
}

impl Logger {
    /// Starts a writer thread on stdout.
    pub fn stdout() -> Self {
        let (tx, rx) = sync_channel(64 * 1024);
        std::thread::Builder::new().name("log-writer".into()).spawn(move || write_loop(rx)).expect("spawn log writer");
        Self { tx: Some(tx), dropped: Arc::default() }
    }

    /// A sink that discards everything.
    pub fn disabled() -> Self {
        Self { tx: None, dropped: Arc::default() }
    }

    pub fn emit(&self, line: LogLine) {
        let Some(tx) = &self.tx else { return };
        if let Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) = tx.try_send(Item::Line(line.finish())) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Blocks until queued lines are written. Called only during shutdown.
    pub fn flush(&self) {
        if let Some(tx) = &self.tx {
            let (ack_tx, ack_rx) = sync_channel(1);
            if tx.send(Item::Flush(ack_tx)).is_ok() {
                let _ = ack_rx.recv_timeout(std::time::Duration::from_secs(2));
            }
        }
    }
}

fn write_loop(rx: Receiver<Item>) {
    let mut out = BufWriter::with_capacity(64 * 1024, std::io::stdout().lock());
    let handle = |item: Item, out: &mut BufWriter<_>| match item {
        Item::Line(line) => {
            let _ = out.write_all(&line);
        }
        Item::Flush(ack) => {
            let _ = out.flush();
            let _ = ack.send(());
        }
    };
    while let Ok(first) = rx.recv() {
        handle(first, &mut out);
        // Batch whatever is already queued, then flush so an idle gateway's
        // logs are never held back.
        while let Ok(item) = rx.try_recv() {
            handle(item, &mut out);
        }
        let _ = out.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn continues_a_valid_traceparent() {
        let h = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        let t = Trace::from_header(Some(h));
        assert_eq!(t.trace_id, "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(t.parent_id.as_deref(), Some("b7ad6b7169203331"));
        assert_eq!(t.header_for("aaaaaaaaaaaaaaaa"), "00-0af7651916cd43dd8448eb211c80319c-aaaaaaaaaaaaaaaa-01");
    }

    #[test]
    fn replaces_invalid_traceparents() {
        for bad in [
            "garbage",
            "00-00000000000000000000000000000000-b7ad6b7169203331-01",
            "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01",
            "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            "00-0AF7651916CD43DD8448EB211C80319C-b7ad6b7169203331-01",
        ] {
            let t = Trace::from_header(Some(bad));
            assert!(t.parent_id.is_none(), "{bad}");
            assert_eq!(t.trace_id.len(), 32);
        }
    }
}

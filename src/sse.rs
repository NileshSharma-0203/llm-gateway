//! Incremental server-sent-events framing across arbitrary read boundaries.

/// Buffers bytes and yields complete events.
#[derive(Default)]
pub struct SseBuffer {
    buf: Vec<u8>,
    pos: usize,
}

impl SseBuffer {
    pub fn feed(&mut self, bytes: &[u8]) {
        if self.pos > 0 && self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        }
        self.buf.extend_from_slice(bytes);
    }

    pub fn buffered(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Returns the next raw event, including its terminating blank line.
    pub fn next_event(&mut self) -> Option<&[u8]> {
        let hay = &self.buf[self.pos..];
        let end = event_end(hay)?;
        let start = self.pos;
        self.pos += end;
        Some(&self.buf[start..start + end])
    }

    /// Compacts consumed bytes.
    pub fn compact(&mut self) {
        if self.pos > 0 {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
    }
}

fn event_end(hay: &[u8]) -> Option<usize> {
    let mut i = 0;
    while i < hay.len() {
        if hay[i] == b'\n' {
            match hay.get(i + 1) {
                Some(b'\n') => return Some(i + 2),
                Some(b'\r') if hay.get(i + 2) == Some(&b'\n') => return Some(i + 3),
                _ => {}
            }
        }
        i += 1;
    }
    None
}

/// Joins the `data:` lines of a raw event, without allocating for the usual single line.
pub fn event_data(raw: &[u8]) -> std::borrow::Cow<'_, [u8]> {
    let mut first: Option<&[u8]> = None;
    let mut joined: Option<Vec<u8>> = None;
    for line in raw.split(|&b| b == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let Some(rest) = line.strip_prefix(b"data:") else { continue };
        let rest = rest.strip_prefix(b" ").unwrap_or(rest);
        match (&mut joined, first) {
            (Some(j), _) => {
                j.push(b'\n');
                j.extend_from_slice(rest);
            }
            (None, Some(f)) => {
                let mut j = f.to_vec();
                j.push(b'\n');
                j.extend_from_slice(rest);
                joined = Some(j);
            }
            (None, None) => first = Some(rest),
        }
    }
    match joined {
        Some(j) => j.into(),
        None => first.unwrap_or(b"").into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_events_split_at_every_byte() {
        let stream = b"data: one\n\ndata: two\r\n\r\ndata: three\n\n";
        for split in 0..stream.len() {
            let mut sse = SseBuffer::default();
            let mut seen = Vec::new();
            for part in [&stream[..split], &stream[split..]] {
                sse.feed(part);
                while let Some(ev) = sse.next_event() {
                    seen.push(event_data(ev).into_owned());
                }
                sse.compact();
            }
            assert_eq!(seen, vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()], "split {split}");
        }
    }

    #[test]
    fn joins_multiline_data_and_ignores_other_fields() {
        let raw = b"event: x\ndata: a\ndata: b\n: comment\n\n";
        assert_eq!(event_data(raw).as_ref(), b"a\nb");
    }

    #[test]
    fn incomplete_event_stays_buffered() {
        let mut sse = SseBuffer::default();
        sse.feed(b"data: partial\n");
        assert!(sse.next_event().is_none());
        assert_eq!(sse.buffered(), 14);
    }
}

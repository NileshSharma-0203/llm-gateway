//! Provider request building and response normalization. Clients always speak
//! the OpenAI chat-completions format, so targets of different kinds can be
//! failover candidates for one another.

use crate::json::{self, Value};
use crate::sse;
use crate::util::{contains, find};

/// Token accounting and state collected while a response flows through.
#[derive(Default, Debug)]
pub struct Usage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub content_chars: u64,
    pub reported: bool,
}

impl Usage {
    /// Reported completion tokens, or an estimate at four characters per token.
    pub fn completion(&self) -> u64 {
        if self.reported {
            self.completion_tokens
        } else {
            self.content_chars.div_ceil(4)
        }
    }
}

/// Builds the upstream body for an OpenAI-compatible provider.
pub fn openai_request(req: &Value, model: &str) -> Vec<u8> {
    let mut body = req.clone();
    body.set("model", Value::str(model));
    if body.get("stream").and_then(Value::as_bool) == Some(true) {
        let mut opts = body.get("stream_options").cloned().unwrap_or(Value::Obj(Vec::new()));
        opts.set("include_usage", Value::Bool(true));
        body.set("stream_options", opts);
    }
    body.to_bytes()
}

/// True if `req` uses features this gateway cannot translate to Anthropic.
pub fn anthropic_unsupported(req: &Value) -> bool {
    if req.get("tools").is_some() || req.get("functions").is_some() || req.get("response_format").is_some() {
        return true;
    }
    let Some(messages) = req.get("messages").and_then(Value::as_arr) else { return true };
    messages.iter().any(|m| match m.get("content") {
        Some(Value::Str(_)) => false,
        Some(Value::Arr(parts)) => parts.iter().any(|p| p.get("type").and_then(Value::as_str) != Some("text")),
        _ => true,
    })
}

fn message_text(content: &Value) -> String {
    match content {
        Value::Str(s) => s.clone(),
        Value::Arr(parts) => parts.iter().filter_map(|p| p.get("text").and_then(Value::as_str)).collect(),
        _ => String::new(),
    }
}

/// Builds an Anthropic Messages API body from an OpenAI-format request.
pub fn anthropic_request(req: &Value, model: &str, default_max_tokens: u64) -> Vec<u8> {
    let mut system = Vec::new();
    let mut messages = Vec::new();
    for m in req.get("messages").and_then(Value::as_arr).unwrap_or(&[]) {
        let role = m.get("role").and_then(Value::as_str).unwrap_or("user");
        let text = m.get("content").map(message_text).unwrap_or_default();
        if role == "system" || role == "developer" {
            system.push(text);
        } else {
            messages.push(Value::Obj(vec![("role".into(), Value::str(role)), ("content".into(), Value::str(text))]));
        }
    }
    let max_tokens = req
        .get("max_tokens")
        .or_else(|| req.get("max_completion_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(default_max_tokens);
    let mut out = vec![
        ("model".to_string(), Value::str(model)),
        ("max_tokens".to_string(), Value::num(max_tokens)),
        ("messages".to_string(), Value::Arr(messages)),
    ];
    if !system.is_empty() {
        out.push(("system".into(), Value::str(system.join("\n"))));
    }
    if let Some(t) = req.get("temperature").and_then(Value::as_f64) {
        out.push(("temperature".into(), Value::num(t.clamp(0.0, 1.0))));
    }
    if let Some(p) = req.get("top_p") {
        out.push(("top_p".into(), p.clone()));
    }
    match req.get("stop") {
        Some(Value::Str(s)) => out.push(("stop_sequences".into(), Value::Arr(vec![Value::str(s.as_str())]))),
        Some(arr @ Value::Arr(_)) => out.push(("stop_sequences".into(), arr.clone())),
        _ => {}
    }
    if req.get("stream").and_then(Value::as_bool) == Some(true) {
        out.push(("stream".into(), Value::Bool(true)));
    }
    Value::Obj(out).to_bytes()
}

fn map_stop_reason(reason: &str) -> &'static str {
    match reason {
        "max_tokens" => "length",
        "tool_use" => "tool_calls",
        _ => "stop",
    }
}

fn read_usage(usage: Option<&Value>, u: &mut Usage, prompt_key: &str, completion_key: &str) {
    if let Some(usage) = usage {
        if let Some(p) = usage.get(prompt_key).and_then(Value::as_u64) {
            u.prompt_tokens = p;
        }
        if let Some(c) = usage.get(completion_key).and_then(Value::as_u64) {
            u.completion_tokens = c;
            u.reported = true;
        }
    }
}

/// Extracts usage from a parsed non-streaming OpenAI-format response.
pub fn openai_usage(v: &Value) -> Usage {
    let mut u = Usage::default();
    read_usage(v.get("usage"), &mut u, "prompt_tokens", "completion_tokens");
    if let Some(choices) = v.get("choices").and_then(Value::as_arr) {
        for c in choices {
            if let Some(text) = c.get("message").and_then(|m| m.get("content")).and_then(Value::as_str) {
                u.content_chars += text.chars().count() as u64;
            }
        }
    }
    u
}

/// Converts a non-streaming Anthropic response to OpenAI format.
pub fn anthropic_response(body: &[u8], alias: &str, created: u64) -> Result<(Vec<u8>, Usage), String> {
    let v = json::parse(body)?;
    let text: String = v
        .get("content")
        .and_then(Value::as_arr)
        .unwrap_or(&[])
        .iter()
        .filter(|b| b.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|b| b.get("text").and_then(Value::as_str))
        .collect();
    let mut u = Usage { content_chars: text.chars().count() as u64, ..Usage::default() };
    read_usage(v.get("usage"), &mut u, "input_tokens", "output_tokens");
    let finish = v.get("stop_reason").and_then(Value::as_str).map(map_stop_reason).unwrap_or("stop");
    let id = v.get("id").and_then(Value::as_str).unwrap_or("msg");
    let out = Value::Obj(vec![
        ("id".into(), Value::str(format!("chatcmpl-{id}"))),
        ("object".into(), Value::str("chat.completion")),
        ("created".into(), Value::num(created)),
        ("model".into(), Value::str(alias)),
        (
            "choices".into(),
            Value::Arr(vec![Value::Obj(vec![
                ("index".into(), Value::num(0)),
                (
                    "message".into(),
                    Value::Obj(vec![("role".into(), Value::str("assistant")), ("content".into(), Value::str(text))]),
                ),
                ("finish_reason".into(), Value::str(finish)),
            ])]),
        ),
        (
            "usage".into(),
            Value::Obj(vec![
                ("prompt_tokens".into(), Value::num(u.prompt_tokens)),
                ("completion_tokens".into(), Value::num(u.completion_tokens)),
                ("total_tokens".into(), Value::num(u.prompt_tokens + u.completion_tokens)),
            ]),
        ),
    ]);
    Ok((out.to_bytes(), u))
}

/// Per-stream translation state.
pub struct StreamTranslator {
    mode: Mode,
    pub usage: Usage,
    pub saw_done: bool,
    pub upstream_error: bool,
}

enum Mode {
    OpenAi { strip_usage: bool },
    Anthropic { id: String, alias: String, created: u64, send_usage: bool },
}

impl StreamTranslator {
    pub fn openai(client_wants_usage: bool) -> Self {
        Self::with(Mode::OpenAi { strip_usage: !client_wants_usage })
    }

    pub fn anthropic(alias: &str, created: u64, client_wants_usage: bool) -> Self {
        Self::with(Mode::Anthropic {
            id: String::from("msg"),
            alias: alias.to_string(),
            created,
            send_usage: client_wants_usage,
        })
    }

    fn with(mode: Mode) -> Self {
        Self { mode, usage: Usage::default(), saw_done: false, upstream_error: false }
    }

    /// Processes one raw SSE event and appends client-bound bytes to `out`.
    pub fn on_event(&mut self, raw: &[u8], out: &mut Vec<u8>) {
        let data = sse::event_data(raw);
        match &self.mode {
            Mode::OpenAi { strip_usage } => {
                let strip = *strip_usage;
                self.on_openai(&data, raw, strip, out)
            }
            Mode::Anthropic { .. } => self.on_anthropic(&data, out),
        }
    }

    fn on_openai(&mut self, data: &[u8], raw: &[u8], strip: bool, out: &mut Vec<u8>) {
        if data == b"[DONE]" {
            self.saw_done = true;
            out.extend_from_slice(raw);
            return;
        }
        if contains(data, b"\"error\":") {
            if let Ok(v) = json::parse(data) {
                if v.get("error").is_some_and(|e| *e != Value::Null) {
                    self.upstream_error = true;
                }
            }
        }
        if contains(data, b"\"usage\":{") || contains(data, b"\"usage\": {") {
            if let Ok(v) = json::parse(data) {
                read_usage(v.get("usage"), &mut self.usage, "prompt_tokens", "completion_tokens");
                let no_choices = v.get("choices").and_then(Value::as_arr).is_some_and(<[Value]>::is_empty);
                if strip && no_choices {
                    return;
                }
            }
        }
        self.usage.content_chars += delta_content_len(data);
        out.extend_from_slice(raw);
    }

    fn on_anthropic(&mut self, data: &[u8], out: &mut Vec<u8>) {
        let Ok(v) = json::parse(data) else { return };
        match v.get("type").and_then(Value::as_str).unwrap_or("") {
            "message_start" => {
                let msg = v.get("message");
                let mut prompt = 0;
                if let Some(u) = msg.and_then(|m| m.get("usage")) {
                    prompt = u.get("input_tokens").and_then(Value::as_u64).unwrap_or(0);
                }
                self.usage.prompt_tokens = prompt;
                if let (Mode::Anthropic { id, .. }, Some(new_id)) =
                    (&mut self.mode, msg.and_then(|m| m.get("id")).and_then(Value::as_str))
                {
                    *id = new_id.to_string();
                }
                self.emit_delta(
                    Value::Obj(vec![("role".into(), Value::str("assistant")), ("content".into(), Value::str(""))]),
                    None,
                    out,
                );
            }
            "content_block_delta" => {
                if let Some(text) = v.get("delta").and_then(|d| d.get("text")).and_then(Value::as_str) {
                    self.usage.content_chars += text.chars().count() as u64;
                    self.emit_delta(Value::Obj(vec![("content".into(), Value::str(text))]), None, out);
                }
            }
            "message_delta" => {
                if let Some(c) = v.get("usage").and_then(|u| u.get("output_tokens")).and_then(Value::as_u64) {
                    self.usage.completion_tokens = c;
                    self.usage.reported = true;
                }
                let finish = v
                    .get("delta")
                    .and_then(|d| d.get("stop_reason"))
                    .and_then(Value::as_str)
                    .map(map_stop_reason)
                    .unwrap_or("stop");
                self.emit_delta(Value::Obj(Vec::new()), Some(finish), out);
            }
            "message_stop" => {
                self.emit_usage(out);
                out.extend_from_slice(b"data: [DONE]\n\n");
                self.saw_done = true;
            }
            "error" => self.upstream_error = true,
            _ => {}
        }
    }

    fn chunk_envelope(&self, choices: Vec<Value>) -> Vec<(String, Value)> {
        let Mode::Anthropic { id, alias, created, .. } = &self.mode else { return Vec::new() };
        vec![
            ("id".into(), Value::str(format!("chatcmpl-{id}"))),
            ("object".into(), Value::str("chat.completion.chunk")),
            ("created".into(), Value::num(*created)),
            ("model".into(), Value::str(alias.as_str())),
            ("choices".into(), Value::Arr(choices)),
        ]
    }

    fn emit_delta(&self, delta: Value, finish: Option<&str>, out: &mut Vec<u8>) {
        let choice = Value::Obj(vec![
            ("index".into(), Value::num(0)),
            ("delta".into(), delta),
            ("finish_reason".into(), finish.map(Value::str).unwrap_or(Value::Null)),
        ]);
        write_data(&Value::Obj(self.chunk_envelope(vec![choice])), out);
    }

    fn emit_usage(&self, out: &mut Vec<u8>) {
        if !matches!(self.mode, Mode::Anthropic { send_usage: true, .. }) {
            return;
        }
        let mut fields = self.chunk_envelope(Vec::new());
        let (p, c) = (self.usage.prompt_tokens, self.usage.completion());
        fields.push((
            "usage".into(),
            Value::Obj(vec![
                ("prompt_tokens".into(), Value::num(p)),
                ("completion_tokens".into(), Value::num(c)),
                ("total_tokens".into(), Value::num(p + c)),
            ]),
        ));
        write_data(&Value::Obj(fields), out);
    }
}

fn write_data(v: &Value, out: &mut Vec<u8>) {
    out.extend_from_slice(b"data: ");
    out.extend_from_slice(&v.to_bytes());
    out.extend_from_slice(b"\n\n");
}

/// Measures the first delta's content without a full JSON parse.
fn delta_content_len(data: &[u8]) -> u64 {
    const KEY: &[u8] = b"\"content\":\"";
    let Some(at) = find(data, KEY) else { return 0 };
    let rest = &data[at + KEY.len()..];
    let mut i = 0;
    let mut n = 0u64;
    while i < rest.len() {
        match rest[i] {
            b'"' => return n,
            b'\\' => i += 1,
            b if b & 0xC0 == 0x80 => {
                i += 1;
                continue;
            }
            _ => {}
        }
        n += 1;
        i += 1;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(s: &str) -> Value {
        json::parse(s.as_bytes()).unwrap()
    }

    fn run(t: &mut StreamTranslator, events: &[&str]) -> String {
        let mut out = Vec::new();
        for e in events {
            t.on_event(e.as_bytes(), &mut out);
        }
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn openai_body_rewrites_model_and_requests_usage() {
        let body = openai_request(&req(r#"{"model":"alias","stream":true,"messages":[]}"#), "real");
        let v = json::parse(&body).unwrap();
        assert_eq!(v.get("model").unwrap().as_str(), Some("real"));
        assert_eq!(v.get("stream_options").unwrap().get("include_usage").unwrap().as_bool(), Some(true));
    }

    #[test]
    fn anthropic_body_moves_system_and_clamps_temperature() {
        let r = req(
            r#"{"model":"a","temperature":1.7,"stop":"END","messages":[{"role":"system","content":"S"},{"role":"user","content":[{"type":"text","text":"hi"}]}]}"#,
        );
        let v = json::parse(&anthropic_request(&r, "claude-x", 99)).unwrap();
        assert_eq!(v.get("system").unwrap().as_str(), Some("S"));
        assert_eq!(v.get("max_tokens").unwrap().as_u64(), Some(99));
        assert_eq!(v.get("temperature").unwrap().as_f64(), Some(1.0));
        assert_eq!(v.get("stop_sequences").unwrap().as_arr().unwrap().len(), 1);
        let msgs = v.get("messages").unwrap().as_arr().unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].get("content").unwrap().as_str(), Some("hi"));
    }

    #[test]
    fn anthropic_gate_blocks_tools_and_images() {
        assert!(anthropic_unsupported(&req(r#"{"tools":[],"messages":[]}"#)));
        assert!(anthropic_unsupported(&req(r#"{"messages":[{"role":"user","content":[{"type":"image_url"}]}]}"#)));
        assert!(!anthropic_unsupported(&req(r#"{"messages":[{"role":"user","content":"x"}]}"#)));
    }

    #[test]
    fn anthropic_response_maps_fields() {
        let body = br#"{"id":"m1","content":[{"type":"text","text":"Hello"}],"stop_reason":"max_tokens","usage":{"input_tokens":3,"output_tokens":2}}"#;
        let (out, u) = anthropic_response(body, "alias", 7).unwrap();
        let v = json::parse(&out).unwrap();
        assert_eq!(v.get("model").unwrap().as_str(), Some("alias"));
        let choice = &v.get("choices").unwrap().as_arr().unwrap()[0];
        assert_eq!(choice.get("finish_reason").unwrap().as_str(), Some("length"));
        assert_eq!(choice.get("message").unwrap().get("content").unwrap().as_str(), Some("Hello"));
        assert_eq!((u.prompt_tokens, u.completion()), (3, 2));
    }

    const ANTHROPIC_STREAM: &[&str] = &[
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m9\",\"usage\":{\"input_tokens\":5}}}\n\n",
        "event: ping\ndata: {\"type\":\"ping\"}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi \\\"x\\\"\"}}\n\n",
        "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":4}}\n\n",
        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    ];

    #[test]
    fn anthropic_stream_becomes_openai_chunks() {
        let mut t = StreamTranslator::anthropic("alias", 1, true);
        let out = run(&mut t, ANTHROPIC_STREAM);
        assert!(out.contains("chatcmpl-m9"));
        assert!(out.contains(r#""content":"Hi \"x\"""#));
        assert!(out.contains(r#""finish_reason":"stop""#));
        assert!(out.contains(r#""total_tokens":9"#));
        assert!(out.ends_with("data: [DONE]\n\n"));
        assert!(t.saw_done);
        assert_eq!((t.usage.prompt_tokens, t.usage.completion()), (5, 4));
    }

    #[test]
    fn anthropic_stream_omits_usage_unless_requested() {
        let mut t = StreamTranslator::anthropic("alias", 1, false);
        assert!(!run(&mut t, ANTHROPIC_STREAM).contains("total_tokens"));
    }

    const USAGE_CHUNK: &str = "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3}}\n\n";

    #[test]
    fn openai_stream_strips_injected_usage_chunk() {
        let mut t = StreamTranslator::openai(false);
        let out = run(
            &mut t,
            &["data: {\"choices\":[{\"delta\":{\"content\":\"abcd\"}}]}\n\n", USAGE_CHUNK, "data: [DONE]\n\n"],
        );
        assert!(!out.contains("usage"));
        assert!(out.contains("abcd"));
        assert!(t.saw_done);
        assert_eq!(t.usage.completion(), 3);
    }

    #[test]
    fn openai_stream_keeps_usage_when_client_asked() {
        let mut t = StreamTranslator::openai(true);
        assert!(run(&mut t, &[USAGE_CHUNK]).contains("usage"));
    }

    #[test]
    fn estimates_tokens_when_usage_is_missing() {
        let mut t = StreamTranslator::openai(false);
        run(
            &mut t,
            &["data: {\"delta\":{\"content\":\"a\\nbé\"}}\n\n", "data: {\"delta\":{\"content\":\"1234\"}}\n\n"],
        );
        assert_eq!(t.usage.content_chars, 8);
        assert_eq!(t.usage.completion(), 2);
    }

    #[test]
    fn detects_in_stream_errors_but_not_error_text_in_content() {
        let mut t = StreamTranslator::openai(false);
        run(&mut t, &["data: {\"delta\":{\"content\":\"the \\\"error\\\": word\"}}\n\n"]);
        assert!(!t.upstream_error);
        run(&mut t, &["data: {\"error\":{\"message\":\"boom\"}}\n\n"]);
        assert!(t.upstream_error);
    }
}

//! A strict JSON parser and writer. Numbers keep their source text so that
//! large integers such as `seed` pass through unchanged.

const MAX_DEPTH: usize = 64;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Num(String),
    Str(String),
    Arr(Vec<Value>),
    Obj(Vec<(String, Value)>),
}

impl Value {
    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Obj(items) => items.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Num(n) => n.parse().ok(),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Value::Num(n) => n.parse::<u64>().ok().or_else(|| {
                let f: f64 = n.parse().ok()?;
                (f >= 0.0 && f.fract() == 0.0 && f < 1.8e19).then_some(f as u64)
            }),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_arr(&self) -> Option<&[Value]> {
        match self {
            Value::Arr(a) => Some(a),
            _ => None,
        }
    }

    /// Sets a key on an object, replacing any existing value.
    pub fn set(&mut self, key: &str, value: Value) {
        if let Value::Obj(items) = self {
            if let Some(slot) = items.iter_mut().find(|(k, _)| k == key) {
                slot.1 = value;
            } else {
                items.push((key.to_string(), value));
            }
        }
    }

    /// Removes a key from an object.
    pub fn remove(&mut self, key: &str) {
        if let Value::Obj(items) = self {
            items.retain(|(k, _)| k != key);
        }
    }

    pub fn str(s: impl Into<String>) -> Value {
        Value::Str(s.into())
    }

    pub fn num(n: impl std::fmt::Display) -> Value {
        Value::Num(n.to_string())
    }

    /// Serializes to compact JSON.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(256);
        write_value(self, &mut out, false);
        out
    }

    /// Serializes with object keys sorted recursively, for stable hashing.
    pub fn to_canonical(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(256);
        write_value(self, &mut out, true);
        out
    }
}

/// Appends `s` as a quoted JSON string.
pub fn write_str(s: &str, out: &mut Vec<u8>) {
    out.push(b'"');
    let bytes = s.as_bytes();
    let mut start = 0;
    for (i, &b) in bytes.iter().enumerate() {
        let escape: Option<&[u8]> = match b {
            b'"' => Some(b"\\\""),
            b'\\' => Some(b"\\\\"),
            b'\n' => Some(b"\\n"),
            b'\r' => Some(b"\\r"),
            b'\t' => Some(b"\\t"),
            0..=0x1f => None,
            _ => continue,
        };
        out.extend_from_slice(&bytes[start..i]);
        match escape {
            Some(e) => out.extend_from_slice(e),
            None => out.extend_from_slice(format!("\\u{b:04x}").as_bytes()),
        }
        start = i + 1;
    }
    out.extend_from_slice(&bytes[start..]);
    out.push(b'"');
}

fn write_value(v: &Value, out: &mut Vec<u8>, sorted: bool) {
    match v {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(true) => out.extend_from_slice(b"true"),
        Value::Bool(false) => out.extend_from_slice(b"false"),
        Value::Num(n) => out.extend_from_slice(n.as_bytes()),
        Value::Str(s) => write_str(s, out),
        Value::Arr(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_value(item, out, sorted);
            }
            out.push(b']');
        }
        Value::Obj(items) => {
            out.push(b'{');
            let mut refs: Vec<&(String, Value)> = items.iter().collect();
            if sorted {
                refs.sort_by(|a, b| a.0.cmp(&b.0));
            }
            for (i, (k, val)) in refs.into_iter().map(|p| (&p.0, &p.1)).enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_str(k, out);
                out.push(b':');
                write_value(val, out, sorted);
            }
            out.push(b'}');
        }
    }
}

struct Parser<'a> {
    b: &'a [u8],
    i: usize,
}

/// Parses a complete JSON document.
pub fn parse(input: &[u8]) -> Result<Value, String> {
    let mut p = Parser { b: input, i: 0 };
    p.ws();
    let v = p.value(0)?;
    p.ws();
    if p.i != input.len() {
        return Err(format!("trailing data at byte {}", p.i));
    }
    Ok(v)
}

impl Parser<'_> {
    fn ws(&mut self) {
        while matches!(self.b.get(self.i), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.i += 1;
        }
    }

    fn fail<T>(&self, msg: &str) -> Result<T, String> {
        Err(format!("{msg} at byte {}", self.i))
    }

    fn literal(&mut self, text: &[u8], v: Value) -> Result<Value, String> {
        if self.b[self.i..].starts_with(text) {
            self.i += text.len();
            Ok(v)
        } else {
            self.fail("invalid literal")
        }
    }

    fn value(&mut self, depth: usize) -> Result<Value, String> {
        if depth > MAX_DEPTH {
            return self.fail("nesting too deep");
        }
        match self.b.get(self.i) {
            Some(b'n') => self.literal(b"null", Value::Null),
            Some(b't') => self.literal(b"true", Value::Bool(true)),
            Some(b'f') => self.literal(b"false", Value::Bool(false)),
            Some(b'"') => Ok(Value::Str(self.string()?)),
            Some(b'[') => self.array(depth),
            Some(b'{') => self.object(depth),
            Some(b'-' | b'0'..=b'9') => self.number(),
            _ => self.fail("unexpected token"),
        }
    }

    fn array(&mut self, depth: usize) -> Result<Value, String> {
        self.i += 1;
        let mut items = Vec::new();
        self.ws();
        if self.b.get(self.i) == Some(&b']') {
            self.i += 1;
            return Ok(Value::Arr(items));
        }
        loop {
            self.ws();
            items.push(self.value(depth + 1)?);
            self.ws();
            match self.b.get(self.i) {
                Some(b',') => self.i += 1,
                Some(b']') => {
                    self.i += 1;
                    return Ok(Value::Arr(items));
                }
                _ => return self.fail("expected , or ]"),
            }
        }
    }

    fn object(&mut self, depth: usize) -> Result<Value, String> {
        self.i += 1;
        let mut items = Vec::new();
        self.ws();
        if self.b.get(self.i) == Some(&b'}') {
            self.i += 1;
            return Ok(Value::Obj(items));
        }
        loop {
            self.ws();
            if self.b.get(self.i) != Some(&b'"') {
                return self.fail("expected object key");
            }
            let key = self.string()?;
            self.ws();
            if self.b.get(self.i) != Some(&b':') {
                return self.fail("expected :");
            }
            self.i += 1;
            self.ws();
            items.push((key, self.value(depth + 1)?));
            self.ws();
            match self.b.get(self.i) {
                Some(b',') => self.i += 1,
                Some(b'}') => {
                    self.i += 1;
                    return Ok(Value::Obj(items));
                }
                _ => return self.fail("expected , or }"),
            }
        }
    }

    fn digits(&mut self) -> usize {
        let start = self.i;
        while matches!(self.b.get(self.i), Some(b'0'..=b'9')) {
            self.i += 1;
        }
        self.i - start
    }

    fn number(&mut self) -> Result<Value, String> {
        let start = self.i;
        if self.b[self.i] == b'-' {
            self.i += 1;
        }
        if self.b.get(self.i) == Some(&b'0') {
            self.i += 1;
        } else if self.digits() == 0 {
            return self.fail("invalid number");
        }
        if self.b.get(self.i) == Some(&b'.') {
            self.i += 1;
            if self.digits() == 0 {
                return self.fail("invalid fraction");
            }
        }
        if matches!(self.b.get(self.i), Some(b'e' | b'E')) {
            self.i += 1;
            if matches!(self.b.get(self.i), Some(b'+' | b'-')) {
                self.i += 1;
            }
            if self.digits() == 0 {
                return self.fail("invalid exponent");
            }
        }
        Ok(Value::Num(String::from_utf8_lossy(&self.b[start..self.i]).into_owned()))
    }

    fn hex4(&mut self) -> Result<u32, String> {
        let Some(slice) = self.b.get(self.i..self.i + 4) else {
            return self.fail("truncated escape");
        };
        let text = std::str::from_utf8(slice).map_err(|_| "invalid escape".to_string())?;
        if !text.bytes().all(|c| c.is_ascii_hexdigit()) {
            return self.fail("invalid escape");
        }
        self.i += 4;
        u32::from_str_radix(text, 16).map_err(|e| e.to_string())
    }

    fn string(&mut self) -> Result<String, String> {
        self.i += 1;
        let mut out: Vec<u8> = Vec::new();
        loop {
            let start = self.i;
            while let Some(&c) = self.b.get(self.i) {
                if c == b'"' || c == b'\\' || c < 0x20 {
                    break;
                }
                self.i += 1;
            }
            out.extend_from_slice(&self.b[start..self.i]);
            match self.b.get(self.i) {
                Some(b'"') => {
                    self.i += 1;
                    return String::from_utf8(out).map_err(|_| "invalid UTF-8 in string".to_string());
                }
                Some(b'\\') => {
                    self.i += 1;
                    let esc = *self.b.get(self.i).ok_or("truncated escape")?;
                    self.i += 1;
                    match esc {
                        b'"' => out.push(b'"'),
                        b'\\' => out.push(b'\\'),
                        b'/' => out.push(b'/'),
                        b'b' => out.push(8),
                        b'f' => out.push(12),
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'u' => {
                            let mut code = self.hex4()?;
                            if (0xD800..0xDC00).contains(&code) {
                                if self.b.get(self.i..self.i + 2) != Some(b"\\u") {
                                    return self.fail("unpaired surrogate");
                                }
                                self.i += 2;
                                let low = self.hex4()?;
                                if !(0xDC00..0xE000).contains(&low) {
                                    return self.fail("invalid low surrogate");
                                }
                                code = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
                            }
                            let ch = char::from_u32(code).ok_or("invalid code point")?;
                            let mut tmp = [0u8; 4];
                            out.extend_from_slice(ch.encode_utf8(&mut tmp).as_bytes());
                        }
                        _ => return self.fail("invalid escape"),
                    }
                }
                Some(_) => return self.fail("control character in string"),
                None => return self.fail("unterminated string"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_keeps_number_text() {
        let src = br#"{"a":[1,2.50,{"b":null}],"seed":12345678901234567890,"s":"x\ny"}"#;
        assert_eq!(parse(src).unwrap().to_bytes(), src.to_vec());
    }

    #[test]
    fn canonical_sorts_nested_keys() {
        let a = parse(br#"{"b":1,"a":{"z":1,"y":2}}"#).unwrap();
        let b = parse(br#"{"a":{"y":2,"z":1},"b":1}"#).unwrap();
        assert_eq!(a.to_canonical(), b.to_canonical());
        assert_ne!(a.to_bytes(), b.to_bytes());
    }

    #[test]
    fn decodes_escapes_and_surrogates() {
        let v = parse(br#""\u00e9\ud83d\ude00\t""#).unwrap();
        assert_eq!(v.as_str(), Some("é😀\t"));
    }

    #[test]
    fn rejects_malformed_documents() {
        for bad in
            [&b"{"[..], b"[1,]", b"{\"a\" 1}", b"01", b"1.", b"\"\\x\"", b"\"\x01\"", b"nul", b"1 2", b"\"\\ud800\""]
        {
            assert!(parse(bad).is_err(), "should reject {:?}", String::from_utf8_lossy(bad));
        }
    }

    #[test]
    fn rejects_excessive_depth() {
        let deep = "[".repeat(100) + &"]".repeat(100);
        assert!(parse(deep.as_bytes()).is_err());
    }

    #[test]
    fn writes_control_characters_as_escapes() {
        let mut out = Vec::new();
        write_str("a\u{1}\"", &mut out);
        assert_eq!(out, br#""a\u0001\"""#);
    }
}

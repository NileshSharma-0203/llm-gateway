//! Typed configuration loaded from JSON. Secrets come from the environment.

use crate::json::{self, Value};
use crate::sha256;
use std::collections::HashSet;

#[derive(Clone, Debug)]
pub struct Config {
    pub listen: String,
    pub workers: usize,
    pub max_connections: usize,
    pub max_body_bytes: usize,
    pub max_tokens_default: u64,
    pub drain_secs: u64,
    pub metrics_token: Option<String>,
    pub quota_snapshot_path: Option<String>,
    pub admission: Admission,
    pub cache: Cache,
    pub retry: Retry,
    pub breaker: Breaker,
    pub timeouts: Timeouts,
    pub providers: Vec<Provider>,
    pub models: Vec<Model>,
    pub tenants: Vec<Tenant>,
}

#[derive(Clone, Debug)]
pub struct Admission {
    pub max_inflight: usize,
    pub max_queue: usize,
    pub max_queue_per_tenant: usize,
    pub queue_timeout_ms: u64,
}

#[derive(Clone, Debug)]
pub struct Cache {
    pub enabled: bool,
    pub max_bytes: usize,
    pub max_entry_bytes: usize,
    pub ttl_secs: u64,
    pub stale_if_error_secs: u64,
    pub deterministic_only: bool,
    pub share_across_tenants: bool,
}

#[derive(Clone, Debug)]
pub struct Retry {
    pub max_attempts: u32,
    pub base_ms: u64,
    pub max_ms: u64,
    pub budget_ratio: f64,
    pub budget_min: f64,
}

#[derive(Clone, Debug)]
pub struct Breaker {
    pub failure_threshold: u32,
    pub open_ms: u64,
    pub max_open_ms: u64,
    pub half_open_probes: u32,
}

#[derive(Clone, Debug)]
pub struct Timeouts {
    pub connect_ms: u64,
    pub first_byte_ms: u64,
    pub idle_stream_ms: u64,
    pub total_ms: u64,
    pub client_write_ms: u64,
    pub client_request_ms: u64,
    pub keepalive_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderKind {
    OpenAi,
    Anthropic,
}

#[derive(Clone, Debug)]
pub struct Provider {
    pub name: String,
    pub kind: ProviderKind,
    pub host: String,
    pub port: u16,
    pub path_prefix: String,
    pub host_header: String,
    pub api_key: String,
    pub pool_size: usize,
}

#[derive(Clone, Debug)]
pub struct Target {
    pub provider: String,
    pub model: String,
    pub input_usd_per_mtok: f64,
    pub output_usd_per_mtok: f64,
}

#[derive(Clone, Debug)]
pub struct Model {
    pub alias: String,
    pub targets: Vec<Target>,
}

#[derive(Clone, Debug)]
pub struct Tenant {
    pub name: String,
    pub key_sha256: Vec<[u8; 32]>,
    pub rpm: u64,
    pub tpm: u64,
    pub daily_tokens: u64,
    pub max_concurrent: usize,
    pub burst_secs: u64,
    pub models: Vec<String>,
}

struct Section<'a> {
    value: Option<&'a Value>,
    path: String,
}

impl<'a> Section<'a> {
    fn new(value: Option<&'a Value>, path: impl Into<String>) -> Self {
        Self { value, path: path.into() }
    }

    fn field(&self, key: &str) -> Option<&'a Value> {
        self.value.and_then(|v| v.get(key))
    }

    fn sub(&self, key: &str) -> Section<'a> {
        Section::new(self.field(key), format!("{}.{key}", self.path))
    }

    fn u64_or(&self, key: &str, default: u64) -> Result<u64, String> {
        match self.field(key) {
            None => Ok(default),
            Some(v) => v.as_u64().ok_or(format!("{}.{key} must be a non-negative integer", self.path)),
        }
    }

    fn f64_or(&self, key: &str, default: f64) -> Result<f64, String> {
        match self.field(key) {
            None => Ok(default),
            Some(v) => v.as_f64().ok_or(format!("{}.{key} must be a number", self.path)),
        }
    }

    fn bool_or(&self, key: &str, default: bool) -> Result<bool, String> {
        match self.field(key) {
            None => Ok(default),
            Some(v) => v.as_bool().ok_or(format!("{}.{key} must be a boolean", self.path)),
        }
    }

    fn opt_str(&self, key: &str) -> Result<Option<String>, String> {
        match self.field(key) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => v.as_str().map(|s| Some(s.to_string())).ok_or(format!("{}.{key} must be a string", self.path)),
        }
    }

    fn req_str(&self, key: &str) -> Result<String, String> {
        self.opt_str(key)?.ok_or(format!("{}.{key} is required", self.path))
    }
}

fn parse_base_url(url: &str, path: &str) -> Result<(String, u16, String), String> {
    let rest = url.strip_prefix("http://").ok_or(format!(
        "{path}: base_url must be http:// because TLS is terminated by the egress sidecar (see docs/DEPLOY.md)"
    ))?;
    let (authority, prefix) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].trim_end_matches('/')),
        None => (rest, ""),
    };
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h, p.parse::<u16>().map_err(|_| format!("{path}: invalid port"))?),
        None => (authority, 80),
    };
    if host.is_empty() {
        return Err(format!("{path}: missing host"));
    }
    Ok((host.to_string(), port, prefix.to_string()))
}

/// Parses and validates configuration. `env` resolves secret names.
pub fn parse(text: &[u8], env: &dyn Fn(&str) -> Option<String>) -> Result<Config, String> {
    let root = json::parse(text).map_err(|e| format!("config is not valid JSON: {e}"))?;
    let s = Section::new(Some(&root), "config");

    let a = s.sub("admission");
    let c = s.sub("cache");
    let r = s.sub("retry");
    let b = s.sub("breaker");
    let t = s.sub("timeouts");

    let mut providers = Vec::new();
    let empty: &[Value] = &[];
    for (i, p) in s.field("providers").and_then(Value::as_arr).unwrap_or(empty).iter().enumerate() {
        let sec = Section::new(Some(p), format!("providers[{i}]"));
        let name = sec.req_str("name")?;
        let kind = match sec.req_str("kind")?.as_str() {
            "openai" => ProviderKind::OpenAi,
            "anthropic" => ProviderKind::Anthropic,
            other => return Err(format!("providers[{i}].kind: unknown kind {other:?}")),
        };
        let url = sec.req_str("base_url")?;
        let (host, port, path_prefix) = parse_base_url(&url, &format!("providers[{i}]"))?;
        let host_header = sec.opt_str("host_header")?.unwrap_or_else(|| format!("{host}:{port}"));
        let key_env = sec.req_str("api_key_env")?;
        let api_key = env(&key_env).ok_or(format!("providers[{i}]: environment variable {key_env} is not set"))?;
        providers.push(Provider {
            name,
            kind,
            host,
            port,
            path_prefix,
            host_header,
            api_key,
            pool_size: sec.u64_or("pool_size", 256)? as usize,
        });
    }
    if providers.is_empty() {
        return Err("config.providers must not be empty".into());
    }
    let names: HashSet<&str> = providers.iter().map(|p| p.name.as_str()).collect();
    if names.len() != providers.len() {
        return Err("config.providers has duplicate names".into());
    }

    let mut models = Vec::new();
    for (i, m) in s.field("models").and_then(Value::as_arr).unwrap_or(empty).iter().enumerate() {
        let sec = Section::new(Some(m), format!("models[{i}]"));
        let alias = sec.req_str("alias")?;
        let mut targets = Vec::new();
        for (j, tv) in sec.field("targets").and_then(Value::as_arr).unwrap_or(empty).iter().enumerate() {
            let ts = Section::new(Some(tv), format!("models[{i}].targets[{j}]"));
            let provider = ts.req_str("provider")?;
            if !names.contains(provider.as_str()) {
                return Err(format!("models[{i}].targets[{j}]: unknown provider {provider:?}"));
            }
            targets.push(Target {
                provider,
                model: ts.req_str("model")?,
                input_usd_per_mtok: ts.f64_or("input_usd_per_mtok", 0.0)?,
                output_usd_per_mtok: ts.f64_or("output_usd_per_mtok", 0.0)?,
            });
        }
        if targets.is_empty() {
            return Err(format!("models[{i}].targets must not be empty"));
        }
        models.push(Model { alias, targets });
    }
    if models.is_empty() {
        return Err("config.models must not be empty".into());
    }

    let mut tenants = Vec::new();
    let mut seen_keys = HashSet::new();
    for (i, tv) in s.field("tenants").and_then(Value::as_arr).unwrap_or(empty).iter().enumerate() {
        let sec = Section::new(Some(tv), format!("tenants[{i}]"));
        let mut key_sha256 = Vec::new();
        for k in sec.field("key_sha256").and_then(Value::as_arr).unwrap_or(empty) {
            let digest = k
                .as_str()
                .and_then(sha256::parse_digest)
                .ok_or(format!("tenants[{i}].key_sha256 entries must be 64-char hex digests"))?;
            if !seen_keys.insert(digest) {
                return Err(format!("tenants[{i}]: duplicate API key hash"));
            }
            key_sha256.push(digest);
        }
        if key_sha256.is_empty() {
            return Err(format!("tenants[{i}].key_sha256 must not be empty"));
        }
        let models = sec
            .field("models")
            .and_then(Value::as_arr)
            .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_else(|| vec!["*".to_string()]);
        tenants.push(Tenant {
            name: sec.req_str("name")?,
            key_sha256,
            rpm: sec.u64_or("rpm", 600)?,
            tpm: sec.u64_or("tpm", 200_000)?,
            daily_tokens: sec.u64_or("daily_tokens", 10_000_000)?,
            max_concurrent: sec.u64_or("max_concurrent", 256)? as usize,
            burst_secs: sec.u64_or("burst_secs", 10)?.max(1),
            models,
        });
    }
    if tenants.is_empty() {
        return Err("config.tenants must not be empty".into());
    }
    let tenant_names: HashSet<&str> = tenants.iter().map(|t| t.name.as_str()).collect();
    if tenant_names.len() != tenants.len() {
        return Err("config.tenants has duplicate names".into());
    }

    let cfg = Config {
        listen: s.opt_str("listen")?.unwrap_or_else(|| "0.0.0.0:8080".into()),
        workers: s.u64_or("workers", 0)? as usize,
        max_connections: s.u64_or("max_connections", 16_000)? as usize,
        max_body_bytes: s.u64_or("max_body_bytes", 8 * 1024 * 1024)? as usize,
        max_tokens_default: s.u64_or("max_tokens_default", 4096)?,
        drain_secs: s.u64_or("drain_secs", 30)?,
        metrics_token: s.opt_str("metrics_token_env")?.and_then(|name| env(&name)),
        quota_snapshot_path: s.opt_str("quota_snapshot_path")?,
        admission: Admission {
            max_inflight: a.u64_or("max_inflight", 2048)? as usize,
            max_queue: a.u64_or("max_queue", 4096)? as usize,
            max_queue_per_tenant: a.u64_or("max_queue_per_tenant", 1024)? as usize,
            queue_timeout_ms: a.u64_or("queue_timeout_ms", 5000)?,
        },
        cache: Cache {
            enabled: c.bool_or("enabled", true)?,
            max_bytes: c.u64_or("max_bytes", 256 * 1024 * 1024)? as usize,
            max_entry_bytes: c.u64_or("max_entry_bytes", 1024 * 1024)? as usize,
            ttl_secs: c.u64_or("ttl_secs", 3600)?,
            stale_if_error_secs: c.u64_or("stale_if_error_secs", 86_400)?,
            deterministic_only: c.bool_or("deterministic_only", true)?,
            share_across_tenants: c.bool_or("share_across_tenants", false)?,
        },
        retry: Retry {
            max_attempts: r.u64_or("max_attempts", 2)?.max(1) as u32,
            base_ms: r.u64_or("base_ms", 50)?,
            max_ms: r.u64_or("max_ms", 1000)?,
            budget_ratio: r.f64_or("budget_ratio", 0.2)?,
            budget_min: r.f64_or("budget_min", 20.0)?,
        },
        breaker: Breaker {
            failure_threshold: b.u64_or("failure_threshold", 5)?.max(1) as u32,
            open_ms: b.u64_or("open_ms", 2000)?,
            max_open_ms: b.u64_or("max_open_ms", 10_000)?,
            half_open_probes: b.u64_or("half_open_probes", 3)?.max(1) as u32,
        },
        timeouts: Timeouts {
            connect_ms: t.u64_or("connect_ms", 2000)?,
            first_byte_ms: t.u64_or("first_byte_ms", 30_000)?,
            idle_stream_ms: t.u64_or("idle_stream_ms", 30_000)?,
            total_ms: t.u64_or("total_ms", 300_000)?,
            client_write_ms: t.u64_or("client_write_ms", 30_000)?,
            client_request_ms: t.u64_or("client_request_ms", 15_000)?,
            keepalive_ms: t.u64_or("keepalive_ms", 75_000)?,
        },
        providers,
        models,
        tenants,
    };
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    fn doc(url: &str, provider_ref: &str, key: &str) -> String {
        format!(
            r#"{{"providers":[{{"name":"a","kind":"openai","base_url":"{url}","api_key_env":"K"}}],
                "models":[{{"alias":"m","targets":[{{"provider":"{provider_ref}","model":"x"}}]}}],
                "tenants":[{{"name":"t","key_sha256":["{key}"]}}]}}"#
        )
    }

    fn env(name: &str) -> Option<String> {
        (name == "K").then(|| "secret".to_string())
    }

    #[test]
    fn parses_with_defaults() {
        let cfg = parse(doc("http://egress:9001/v1/", "a", KEY).as_bytes(), &env).unwrap();
        assert_eq!(cfg.providers[0].host, "egress");
        assert_eq!(cfg.providers[0].port, 9001);
        assert_eq!(cfg.providers[0].path_prefix, "/v1");
        assert_eq!(cfg.providers[0].api_key, "secret");
        assert_eq!(cfg.admission.max_inflight, 2048);
        assert_eq!(cfg.tenants[0].models, vec!["*"]);
    }

    #[test]
    fn rejects_invalid_references_and_values() {
        assert!(parse(doc("https://api.openai.com", "a", KEY).as_bytes(), &env).unwrap_err().contains("egress"));
        assert!(parse(doc("http://h", "missing", KEY).as_bytes(), &env).unwrap_err().contains("unknown provider"));
        assert!(parse(doc("http://h", "a", "abc").as_bytes(), &env).unwrap_err().contains("hex"));
        assert!(parse(doc("http://h", "a", KEY).as_bytes(), &|_| None).unwrap_err().contains("not set"));
    }
}

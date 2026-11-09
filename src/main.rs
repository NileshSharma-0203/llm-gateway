//! Gateway entry point.

use llm_gateway::rt::sys;
use llm_gateway::telemetry::{LogLine, Logger};
use llm_gateway::{config, gateway, sha256, util};
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::{Duration, Instant};

static SIGNAL: AtomicI32 = AtomicI32::new(0);

extern "C" fn on_signal(sig: i32) {
    SIGNAL.store(sig, Ordering::SeqCst);
}

const USAGE: &str = "usage:
  llm-gateway --config <path>         run the gateway
  llm-gateway check --config <path>   validate configuration and exit
  llm-gateway generate-key            print a new API key and its SHA-256
  llm-gateway hash-key <key>          print the SHA-256 of an API key";

fn load_config(path: &str) -> Result<config::Config, String> {
    let text = std::fs::read(path).map_err(|e| format!("cannot read {path}: {e}"))?;
    config::parse(&text, &|name| std::env::var(name).ok())
}

fn config_arg(args: &[String]) -> Option<&str> {
    args.windows(2).find(|w| w[0] == "--config").map(|w| w[1].as_str())
}

fn generate_key() -> std::io::Result<()> {
    let mut raw = [0u8; 24];
    util::os_random(&mut raw)?;
    let key = format!("gw-{}", sha256::hex(&raw));
    println!("key:    {key}\nsha256: {}", sha256::hex(&sha256::digest(key.as_bytes())));
    Ok(())
}

fn fail(message: &str) -> ! {
    eprintln!("error: {message}");
    std::process::exit(2);
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("generate-key") => {
            generate_key().unwrap_or_else(|e| fail(&e.to_string()));
            return;
        }
        Some("hash-key") => {
            let Some(key) = args.get(1) else { fail(USAGE) };
            println!("{}", sha256::hex(&sha256::digest(key.as_bytes())));
            return;
        }
        Some("check") => {
            let Some(path) = config_arg(&args) else { fail(USAGE) };
            let cfg = load_config(path).unwrap_or_else(|e| fail(&e));
            println!(
                "ok: {} providers, {} models, {} tenants",
                cfg.providers.len(),
                cfg.models.len(),
                cfg.tenants.len()
            );
            return;
        }
        Some("--config") => {}
        _ => fail(USAGE),
    }
    let Some(path) = config_arg(&args) else { fail(USAGE) };
    let cfg = load_config(path).unwrap_or_else(|e| fail(&e));
    let drain_secs = cfg.drain_secs;
    let snapshot = cfg.quota_snapshot_path.clone();

    let log = Logger::stdout();
    let running = gateway::start(cfg, log.clone()).unwrap_or_else(|e| fail(&format!("cannot start: {e}")));
    sys::install_signal(sys::SIGTERM, on_signal);
    sys::install_signal(sys::SIGINT, on_signal);

    let mut last_snapshot = Instant::now();
    while SIGNAL.load(Ordering::SeqCst) == 0 {
        std::thread::sleep(Duration::from_millis(100));
        if last_snapshot.elapsed() >= Duration::from_secs(30) {
            last_snapshot = Instant::now();
            if let Some(p) = &snapshot {
                let _ = running.gateway.save_quota(p);
            }
        }
    }

    log.emit(LogLine::new("draining").num("active", running.gateway.active_requests()));
    running.gateway.begin_drain();
    let deadline = Instant::now() + Duration::from_secs(drain_secs);
    while running.gateway.active_requests() > 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    if let Some(p) = &snapshot {
        if let Err(e) = running.gateway.save_quota(p) {
            log.emit(LogLine::new("quota_snapshot_failed").str("error", &e.to_string()));
        }
    }
    log.emit(LogLine::new("stopped").num("abandoned", running.gateway.active_requests()));
    log.flush();
    std::process::exit(0);
}

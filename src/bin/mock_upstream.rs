//! Standalone mock LLM provider for load tests.

use llm_gateway::mock;
use std::net::SocketAddr;

fn arg<T: std::str::FromStr>(args: &[String], name: &str, default: T) -> T {
    args.windows(2).find(|w| w[0] == name).and_then(|w| w[1].parse().ok()).unwrap_or(default)
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let listen: SocketAddr = arg(&args, "--listen", "127.0.0.1:9100".parse().unwrap());
    let m = mock::start(
        listen,
        arg(&args, "--workers", 1usize),
        arg(&args, "--ttfb-ms", 0u64),
        arg(&args, "--tokens", 50u64),
        arg(&args, "--token-delay-us", 0u64),
    )
    .expect("start mock");
    println!("mock listening on {}", m.addr);
    loop {
        std::thread::park();
    }
}

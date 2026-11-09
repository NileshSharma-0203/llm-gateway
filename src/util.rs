//! Small helpers: fast non-cryptographic randomness and OS entropy.

use std::cell::Cell;
use std::fs::File;
use std::io::Read;

/// Fills `buf` from the OS entropy source.
pub fn os_random(buf: &mut [u8]) -> std::io::Result<()> {
    File::open("/dev/urandom")?.read_exact(buf)
}

thread_local! {
    static RNG: Cell<u64> = Cell::new({
        let mut seed = [0u8; 8];
        let _ = os_random(&mut seed);
        u64::from_ne_bytes(seed) | 1
    });
}

/// xorshift64*. Used for jitter and trace ids, never for secrets.
pub fn fast_rand() -> u64 {
    RNG.with(|cell| {
        let mut x = cell.get();
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        cell.set(x);
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    })
}

/// Seconds since the Unix epoch.
pub fn unix_secs() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// Substring search; used on small SSE payloads.
pub fn contains(hay: &[u8], needle: &[u8]) -> bool {
    find(hay, needle).is_some()
}

/// Returns the offset of the first occurrence of `needle`.
pub fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    let first = needle[0];
    let last_start = hay.len() - needle.len();
    let mut i = 0;
    while i <= last_start {
        if hay[i] == first && &hay[i..i + needle.len()] == needle {
            return Some(i);
        }
        i += 1;
    }
    None
}

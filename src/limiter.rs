//! Per-key limits: requests per minute, tokens per minute, and a daily token
//! quota. Tokens are reserved on admission and settled against actual usage,
//! so concurrent requests cannot overshoot by more than estimation error.

use std::sync::{Arc, Mutex};

const SECS_PER_DAY: u64 = 86_400;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DenyReason {
    Requests,
    Tokens,
    Quota,
}

impl DenyReason {
    pub fn label(self) -> &'static str {
        match self {
            DenyReason::Requests => "rpm",
            DenyReason::Tokens => "tpm",
            DenyReason::Quota => "daily_quota",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Deny {
    pub reason: DenyReason,
    pub retry_after_secs: u64,
}

#[derive(Clone, Copy)]
struct Bucket {
    level: f64,
    capacity: f64,
    per_sec: f64,
    last: f64,
}

impl Bucket {
    fn new(capacity: f64, per_sec: f64) -> Self {
        Self { level: capacity, capacity, per_sec, last: 0.0 }
    }

    fn refill(&mut self, now: f64) {
        let elapsed = (now - self.last).max(0.0);
        self.level = (self.level + elapsed * self.per_sec).min(self.capacity);
        self.last = now;
    }

    fn wait_for(&self, amount: f64) -> u64 {
        let needed = amount.min(self.capacity) - self.level;
        if self.per_sec <= 0.0 {
            return 60;
        }
        (needed / self.per_sec).ceil().max(1.0) as u64
    }
}

struct State {
    rpm: Bucket,
    tpm: Bucket,
    day: u64,
    used_today: u64,
}

/// Limits for one tenant.
pub struct TenantLimiter {
    state: Mutex<State>,
    pub daily_tokens: u64,
    pub rpm_limit: u64,
    pub tpm_limit: u64,
}

/// Snapshot of remaining capacity, returned to clients as headers.
#[derive(Debug, Clone, Copy)]
pub struct Remaining {
    pub requests: u64,
    pub tokens: u64,
}

/// Tokens held for an in-flight request.
pub struct Reservation {
    limiter: Arc<TenantLimiter>,
    reserved: u64,
    settled: bool,
    pub remaining: Remaining,
}

impl TenantLimiter {
    pub fn new(rpm: u64, tpm: u64, daily_tokens: u64, burst_secs: u64) -> Arc<Self> {
        let rps = rpm as f64 / 60.0;
        let burst = (rps * burst_secs as f64).ceil().max(1.0);
        Arc::new(Self {
            state: Mutex::new(State {
                rpm: Bucket::new(burst, rps),
                tpm: Bucket::new(tpm as f64, tpm as f64 / 60.0),
                day: 0,
                used_today: 0,
            }),
            daily_tokens,
            rpm_limit: rpm,
            tpm_limit: tpm,
        })
    }

    /// Checks all limits and reserves `est_tokens`. `now` is monotonic seconds;
    /// `unix` selects the UTC day for the quota.
    pub fn acquire(self: &Arc<Self>, est_tokens: u64, now: f64, unix: u64) -> Result<Reservation, Deny> {
        let mut s = self.state.lock().unwrap();
        s.rpm.refill(now);
        s.tpm.refill(now);
        let day = unix / SECS_PER_DAY;
        if day != s.day {
            s.day = day;
            s.used_today = 0;
        }
        if s.used_today.saturating_add(est_tokens) > self.daily_tokens {
            let until_midnight = SECS_PER_DAY - unix % SECS_PER_DAY;
            return Err(Deny { reason: DenyReason::Quota, retry_after_secs: until_midnight });
        }
        if s.rpm.level < 1.0 {
            return Err(Deny { reason: DenyReason::Requests, retry_after_secs: s.rpm.wait_for(1.0) });
        }
        let need = (est_tokens as f64).min(s.tpm.capacity);
        if s.tpm.level < need {
            return Err(Deny { reason: DenyReason::Tokens, retry_after_secs: s.tpm.wait_for(need) });
        }
        s.rpm.level -= 1.0;
        s.tpm.level -= need;
        s.used_today += est_tokens;
        let remaining = Remaining { requests: s.rpm.level.max(0.0) as u64, tokens: s.tpm.level.max(0.0) as u64 };
        drop(s);
        Ok(Reservation { limiter: self.clone(), reserved: est_tokens, settled: false, remaining })
    }

    fn adjust(&self, reserved: u64, actual: u64) {
        let mut s = self.state.lock().unwrap();
        let reserved_tpm = (reserved as f64).min(s.tpm.capacity);
        s.tpm.level = (s.tpm.level + reserved_tpm - actual as f64).min(s.tpm.capacity);
        s.used_today = s.used_today.saturating_sub(reserved).saturating_add(actual);
    }

    /// Tokens consumed in the current UTC day.
    pub fn used_today(&self) -> (u64, u64) {
        let s = self.state.lock().unwrap();
        (s.day, s.used_today)
    }

    /// Restores persisted usage after a restart.
    pub fn restore(&self, day: u64, used: u64) {
        let mut s = self.state.lock().unwrap();
        s.day = day;
        s.used_today = used;
    }
}

impl Reservation {
    /// Replaces the estimate with actual usage.
    pub fn settle(mut self, actual_tokens: u64) {
        self.limiter.adjust(self.reserved, actual_tokens);
        self.settled = true;
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.settled {
            self.limiter.adjust(self.reserved, 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOON: u64 = 20_000 * SECS_PER_DAY + 43_200;

    #[test]
    fn rpm_burst_then_refill() {
        let l = TenantLimiter::new(60, 1_000_000, 1_000_000, 2);
        std::mem::forget(l.acquire(1, 0.0, NOON).unwrap());
        std::mem::forget(l.acquire(1, 0.0, NOON).unwrap());
        let deny = l.acquire(1, 0.0, NOON).err().unwrap();
        assert_eq!(deny.reason, DenyReason::Requests);
        assert_eq!(deny.retry_after_secs, 1);
        assert!(l.acquire(1, 1.01, NOON).is_ok());
    }

    #[test]
    fn tokens_are_reserved_then_refunded_on_drop() {
        let l = TenantLimiter::new(6000, 100, 1_000_000, 10);
        let held = l.acquire(80, 0.0, NOON).unwrap();
        assert_eq!(l.acquire(30, 0.0, NOON).err().unwrap().reason, DenyReason::Tokens);
        drop(held);
        assert!(l.acquire(30, 0.0, NOON).is_ok());
    }

    #[test]
    fn settle_charges_actual_usage() {
        let l = TenantLimiter::new(6000, 1000, 1000, 10);
        l.acquire(500, 0.0, NOON).unwrap().settle(100);
        assert_eq!(l.used_today().1, 100);
    }

    #[test]
    fn underestimates_create_debt() {
        let l = TenantLimiter::new(6000, 100, 1_000_000, 10);
        l.acquire(10, 0.0, NOON).unwrap().settle(200);
        assert_eq!(l.acquire(1, 0.0, NOON).err().unwrap().reason, DenyReason::Tokens);
    }

    #[test]
    fn quota_blocks_until_utc_midnight_and_resets() {
        let l = TenantLimiter::new(6000, 1_000_000, 100, 10);
        l.acquire(90, 0.0, NOON).unwrap().settle(90);
        let deny = l.acquire(20, 0.0, NOON).err().unwrap();
        assert_eq!(deny.reason, DenyReason::Quota);
        assert_eq!(deny.retry_after_secs, 43_200);
        assert!(l.acquire(20, 1.0, NOON + SECS_PER_DAY).is_ok());
    }

    #[test]
    fn oversized_request_can_pass_when_bucket_is_full() {
        let l = TenantLimiter::new(6000, 100, 1_000_000, 10);
        assert!(l.acquire(500, 0.0, NOON).is_ok());
    }
}

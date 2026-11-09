//! Circuit breaker and retry budget for one provider.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, PartialEq, Debug)]
enum Phase {
    Closed,
    Open { until: Instant },
    HalfOpen,
}

struct State {
    phase: Phase,
    consecutive_failures: u32,
    probes_inflight: u32,
    probe_successes: u32,
    open_for: Duration,
}

pub struct Breaker {
    state: Mutex<State>,
    threshold: u32,
    base_open: Duration,
    max_open: Duration,
    probes: u32,
    pub opened_total: AtomicU32,
}

/// Permission to send one request. Dropping it unresolved releases a probe slot
/// without counting for or against the provider, as when a client disconnects.
pub struct Ticket {
    breaker: Arc<Breaker>,
    probe: bool,
    resolved: bool,
}

impl Breaker {
    pub fn new(threshold: u32, base_open: Duration, max_open: Duration, probes: u32) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                phase: Phase::Closed,
                consecutive_failures: 0,
                probes_inflight: 0,
                probe_successes: 0,
                open_for: base_open,
            }),
            threshold,
            base_open,
            max_open,
            probes,
            opened_total: AtomicU32::new(0),
        })
    }

    /// Returns a ticket if a request may be sent now.
    pub fn allow(self: &Arc<Self>, now: Instant) -> Option<Ticket> {
        let mut s = self.state.lock().unwrap();
        if let Phase::Open { until } = s.phase {
            if now < until {
                return None;
            }
            s.phase = Phase::HalfOpen;
            s.probes_inflight = 0;
            s.probe_successes = 0;
        }
        let probe = s.phase == Phase::HalfOpen;
        if probe {
            if s.probes_inflight >= self.probes {
                return None;
            }
            s.probes_inflight += 1;
        }
        Some(Ticket { breaker: self.clone(), probe, resolved: false })
    }

    /// Records a failure that happened after the ticket resolved, such as a
    /// stream dying midway.
    pub fn record_failure(&self, now: Instant) {
        let mut s = self.state.lock().unwrap();
        self.fail_locked(&mut s, now);
    }

    fn fail_locked(&self, s: &mut State, now: Instant) {
        s.consecutive_failures += 1;
        match s.phase {
            Phase::HalfOpen => {
                s.open_for = (s.open_for * 2).min(self.max_open);
                s.phase = Phase::Open { until: now + s.open_for };
                self.opened_total.fetch_add(1, Ordering::Relaxed);
            }
            Phase::Closed if s.consecutive_failures >= self.threshold => {
                s.open_for = self.base_open;
                s.phase = Phase::Open { until: now + s.open_for };
                self.opened_total.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// 0 = closed, 1 = half-open, 2 = open.
    pub fn state_code(&self) -> u8 {
        match self.state.lock().unwrap().phase {
            Phase::Closed => 0,
            Phase::HalfOpen => 1,
            Phase::Open { .. } => 2,
        }
    }
}

impl Ticket {
    pub fn success(mut self) {
        self.resolved = true;
        let mut s = self.breaker.state.lock().unwrap();
        s.consecutive_failures = 0;
        if self.probe && s.phase == Phase::HalfOpen {
            s.probes_inflight = s.probes_inflight.saturating_sub(1);
            s.probe_successes += 1;
            if s.probe_successes >= self.breaker.probes {
                s.phase = Phase::Closed;
                s.open_for = self.breaker.base_open;
            }
        }
    }

    pub fn failure(mut self, now: Instant) {
        self.resolved = true;
        let breaker = self.breaker.clone();
        let mut s = breaker.state.lock().unwrap();
        if self.probe {
            s.probes_inflight = s.probes_inflight.saturating_sub(1);
        }
        breaker.fail_locked(&mut s, now);
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        if self.probe && !self.resolved {
            let mut s = self.breaker.state.lock().unwrap();
            s.probes_inflight = s.probes_inflight.saturating_sub(1);
        }
    }
}

/// Caps retries to a fraction of request volume so that retries cannot amplify
/// an outage.
pub struct RetryBudget {
    tokens: Mutex<f64>,
    ratio: f64,
    max: f64,
}

impl RetryBudget {
    pub fn new(ratio: f64, min: f64) -> Self {
        Self { tokens: Mutex::new(min), ratio, max: min.max(1.0) * 10.0 }
    }

    /// Credits the budget for one original request.
    pub fn deposit(&self) {
        let mut t = self.tokens.lock().unwrap();
        *t = (*t + self.ratio).min(self.max);
    }

    /// Spends one retry if available.
    pub fn withdraw(&self) -> bool {
        let mut t = self.tokens.lock().unwrap();
        if *t >= 1.0 {
            *t -= 1.0;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn breaker() -> Arc<Breaker> {
        Breaker::new(3, Duration::from_secs(1), Duration::from_secs(4), 2)
    }

    fn fail(b: &Arc<Breaker>, now: Instant) {
        b.allow(now).unwrap().failure(now);
    }

    #[test]
    fn opens_after_consecutive_failures_only() {
        let b = breaker();
        let t = Instant::now();
        fail(&b, t);
        fail(&b, t);
        b.allow(t).unwrap().success();
        fail(&b, t);
        fail(&b, t);
        assert_eq!(b.state_code(), 0);
        fail(&b, t);
        assert_eq!(b.state_code(), 2);
        assert!(b.allow(t).is_none());
    }

    #[test]
    fn half_open_limits_probes_and_closes_after_successes() {
        let b = breaker();
        let t = Instant::now();
        (0..3).for_each(|_| fail(&b, t));
        let later = t + Duration::from_millis(1001);
        let p1 = b.allow(later).unwrap();
        let p2 = b.allow(later).unwrap();
        assert!(b.allow(later).is_none());
        p1.success();
        assert_eq!(b.state_code(), 1);
        p2.success();
        assert_eq!(b.state_code(), 0);
    }

    #[test]
    fn failed_probe_reopens_with_exponential_backoff() {
        let b = breaker();
        let t = Instant::now();
        (0..3).for_each(|_| fail(&b, t));
        let t1 = t + Duration::from_millis(1001);
        fail(&b, t1);
        assert!(b.allow(t1 + Duration::from_millis(1999)).is_none());
        let t2 = t1 + Duration::from_millis(2001);
        fail(&b, t2);
        let t3 = t2 + Duration::from_millis(3999);
        assert!(b.allow(t3).is_none(), "open period should have doubled to 4s");
        assert!(b.allow(t2 + Duration::from_millis(4001)).is_some());
    }

    #[test]
    fn abandoned_probe_releases_its_slot() {
        let b = breaker();
        let t = Instant::now();
        (0..3).for_each(|_| fail(&b, t));
        let later = t + Duration::from_secs(2);
        drop(b.allow(later).unwrap());
        drop(b.allow(later).unwrap());
        assert!(b.allow(later).is_some());
    }

    #[test]
    fn retry_budget_limits_amplification() {
        let budget = RetryBudget::new(0.2, 2.0);
        assert!(budget.withdraw() && budget.withdraw());
        assert!(!budget.withdraw());
        (0..5).for_each(|_| budget.deposit());
        assert!(budget.withdraw());
        assert!(!budget.withdraw());
    }
}

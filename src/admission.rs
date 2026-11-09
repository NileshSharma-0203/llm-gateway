//! Admission control: a global concurrency limit in front of a bounded queue
//! with round-robin fairness between tenants. When the queue is full, requests
//! are shed immediately so that overload turns into fast failures rather than
//! unbounded latency.

use crate::rt;
use std::collections::{HashMap, VecDeque};
use std::future::poll_fn;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Reject {
    QueueFull,
    TenantQueueFull,
    QueueTimeout,
}

impl Reject {
    pub fn label(self) -> &'static str {
        match self {
            Reject::QueueFull => "queue_full",
            Reject::TenantQueueFull => "tenant_queue_full",
            Reject::QueueTimeout => "queue_timeout",
        }
    }
}

struct Waiter {
    granted: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

#[derive(Default)]
struct TenantSlot {
    running: usize,
    limit: usize,
    queue: VecDeque<Arc<Waiter>>,
    in_rotation: bool,
}

struct Inner {
    running: usize,
    queued: usize,
    tenants: HashMap<Arc<str>, TenantSlot>,
    rotation: VecDeque<Arc<str>>,
}

pub struct Admission {
    inner: Mutex<Inner>,
    max_inflight: usize,
    max_queue: usize,
    max_queue_per_tenant: usize,
    queue_timeout: Duration,
}

/// Held for the lifetime of an upstream request.
pub struct Permit {
    admission: Arc<Admission>,
    tenant: Arc<str>,
}

impl Admission {
    pub fn new(
        max_inflight: usize,
        max_queue: usize,
        max_queue_per_tenant: usize,
        queue_timeout: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner { running: 0, queued: 0, tenants: HashMap::new(), rotation: VecDeque::new() }),
            max_inflight,
            max_queue,
            max_queue_per_tenant,
            queue_timeout,
        })
    }

    /// Returns `(running, queued)`.
    pub fn depth(&self) -> (usize, usize) {
        let inner = self.inner.lock().unwrap();
        (inner.running, inner.queued)
    }

    /// Waits for capacity, subject to queue limits and the queue timeout.
    pub async fn acquire(self: &Arc<Self>, tenant: &Arc<str>, tenant_limit: usize) -> Result<Permit, Reject> {
        let waiter = {
            let mut inner = self.inner.lock().unwrap();
            let global_free = inner.running < self.max_inflight;
            let slot = inner.tenants.entry(tenant.clone()).or_default();
            slot.limit = tenant_limit;
            if global_free && slot.running < tenant_limit {
                slot.running += 1;
                inner.running += 1;
                return Ok(self.permit(tenant));
            }
            if slot.queue.len() >= self.max_queue_per_tenant {
                return Err(Reject::TenantQueueFull);
            }
            if inner.queued >= self.max_queue {
                return Err(Reject::QueueFull);
            }
            let waiter = Arc::new(Waiter { granted: AtomicBool::new(false), waker: Mutex::new(None) });
            let slot = inner.tenants.get_mut(tenant).expect("slot inserted above");
            slot.queue.push_back(waiter.clone());
            let needs_rotation = !slot.in_rotation;
            slot.in_rotation = true;
            if needs_rotation {
                inner.rotation.push_back(tenant.clone());
            }
            inner.queued += 1;
            waiter
        };

        let mut guard = CancelGuard { admission: self, tenant, waiter: &waiter, armed: true };
        let wait = poll_fn(|cx| {
            *waiter.waker.lock().unwrap() = Some(cx.waker().clone());
            if waiter.granted.load(Ordering::Acquire) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        });
        match rt::timeout(self.queue_timeout, wait).await {
            Ok(()) => {
                guard.armed = false;
                Ok(self.permit(tenant))
            }
            Err(_) => Err(Reject::QueueTimeout),
        }
    }

    fn permit(self: &Arc<Self>, tenant: &Arc<str>) -> Permit {
        Permit { admission: self.clone(), tenant: tenant.clone() }
    }

    /// Grants freed capacity to the next eligible waiter, one tenant per turn.
    fn dispatch(&self, inner: &mut Inner) {
        while inner.running < self.max_inflight {
            let mut granted = false;
            for _ in 0..inner.rotation.len() {
                let Some(name) = inner.rotation.pop_front() else { break };
                let slot = inner.tenants.get_mut(&name).expect("rotation holds known tenants");
                if slot.queue.is_empty() {
                    slot.in_rotation = false;
                    continue;
                }
                if slot.running >= slot.limit {
                    inner.rotation.push_back(name);
                    continue;
                }
                let waiter = slot.queue.pop_front().expect("checked non-empty");
                slot.running += 1;
                if slot.queue.is_empty() {
                    slot.in_rotation = false;
                } else {
                    inner.rotation.push_back(name);
                }
                inner.running += 1;
                inner.queued -= 1;
                waiter.granted.store(true, Ordering::Release);
                let waker = waiter.waker.lock().unwrap().take();
                if let Some(w) = waker {
                    w.wake();
                }
                granted = true;
                break;
            }
            if !granted {
                return;
            }
        }
    }

    fn release(&self, tenant: &Arc<str>) {
        let mut inner = self.inner.lock().unwrap();
        inner.running -= 1;
        if let Some(slot) = inner.tenants.get_mut(tenant) {
            slot.running -= 1;
        }
        self.dispatch(&mut inner);
    }
}

struct CancelGuard<'a> {
    admission: &'a Arc<Admission>,
    tenant: &'a Arc<str>,
    waiter: &'a Arc<Waiter>,
    armed: bool,
}

impl Drop for CancelGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut inner = self.admission.inner.lock().unwrap();
        if self.waiter.granted.load(Ordering::Acquire) {
            drop(inner);
            self.admission.release(self.tenant);
            return;
        }
        if let Some(slot) = inner.tenants.get_mut(self.tenant) {
            let before = slot.queue.len();
            slot.queue.retain(|w| !Arc::ptr_eq(w, self.waiter));
            if slot.queue.len() != before {
                inner.queued -= 1;
            }
        }
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.admission.release(&self.tenant);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    fn tenant(s: &str) -> Arc<str> {
        Arc::from(s)
    }

    #[test]
    fn sheds_when_queue_is_full() {
        rt::block_on(async {
            let a = Admission::new(1, 1, 10, Duration::from_secs(5));
            let t = tenant("a");
            let _held = a.acquire(&t, 10).await.unwrap();
            let waiter_a = a.clone();
            let waiter_t = t.clone();
            rt::spawn(async move {
                let _ = waiter_a.acquire(&waiter_t, 10).await;
            });
            rt::yield_now().await;
            assert_eq!(a.depth(), (1, 1));
            assert_eq!(a.acquire(&t, 10).await.err(), Some(Reject::QueueFull));
        });
    }

    #[test]
    fn queue_timeout_removes_waiter() {
        rt::block_on(async {
            let a = Admission::new(1, 5, 5, Duration::from_millis(10));
            let t = tenant("a");
            let _held = a.acquire(&t, 10).await.unwrap();
            assert_eq!(a.acquire(&t, 10).await.err(), Some(Reject::QueueTimeout));
            assert_eq!(a.depth(), (1, 0));
        });
    }

    #[test]
    fn tenants_are_served_round_robin() {
        rt::block_on(async {
            let a = Admission::new(1, 100, 100, Duration::from_secs(5));
            let order: Rc<RefCell<Vec<String>>> = Rc::default();
            let noisy = tenant("noisy");
            let quiet = tenant("quiet");
            let first = a.acquire(&noisy, 100).await.unwrap();
            for name in ["noisy", "noisy", "noisy", "quiet"] {
                let (a, order, t) = (a.clone(), order.clone(), tenant(name));
                rt::spawn(async move {
                    let p = a.acquire(&t, 100).await.unwrap();
                    order.borrow_mut().push(t.to_string());
                    rt::yield_now().await;
                    drop(p);
                });
                rt::yield_now().await;
            }
            let _ = &quiet;
            drop(first);
            rt::sleep(Duration::from_millis(20)).await;
            assert_eq!(*order.borrow(), vec!["noisy", "quiet", "noisy", "noisy"]);
        });
    }

    #[test]
    fn tenant_concurrency_cap_applies() {
        rt::block_on(async {
            let a = Admission::new(10, 10, 10, Duration::from_millis(10));
            let t = tenant("a");
            let _p = a.acquire(&t, 1).await.unwrap();
            assert_eq!(a.acquire(&t, 1).await.err(), Some(Reject::QueueTimeout));
            let other = tenant("b");
            assert!(a.acquire(&other, 1).await.is_ok());
        });
    }

    #[test]
    fn wakes_across_threads() {
        let a = Admission::new(1, 10, 10, Duration::from_secs(5));
        let t = tenant("a");
        let first = rt::block_on({
            let (a, t) = (a.clone(), t.clone());
            async move { a.acquire(&t, 10).await.unwrap() }
        });
        let (a2, t2) = (a.clone(), t.clone());
        let handle = std::thread::spawn(move || rt::block_on(async move { a2.acquire(&t2, 10).await.is_ok() }));
        while a.depth().1 == 0 {
            std::thread::yield_now();
        }
        drop(first);
        assert!(handle.join().unwrap());
    }
}

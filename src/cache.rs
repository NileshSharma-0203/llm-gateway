//! Sharded LRU response cache with TTL, serve-stale-on-error, and request
//! coalescing for identical in-flight requests.

use std::collections::HashMap;
use std::future::poll_fn;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::time::{Duration, Instant};

pub type Key = [u8; 32];

const SHARDS: usize = 32;
const NIL: u32 = u32::MAX;

/// A stored response.
#[derive(Debug)]
pub struct Entry {
    pub body: Vec<u8>,
    pub provider: String,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub cost_micro_usd: u64,
    pub created: Instant,
}

struct Node {
    key: Key,
    entry: Arc<Entry>,
    prev: u32,
    next: u32,
}

struct Shard {
    map: HashMap<Key, u32>,
    nodes: Vec<Option<Node>>,
    free: Vec<u32>,
    head: u32,
    tail: u32,
    bytes: usize,
}

impl Shard {
    fn new() -> Self {
        Self { map: HashMap::new(), nodes: Vec::new(), free: Vec::new(), head: NIL, tail: NIL, bytes: 0 }
    }

    fn node(&mut self, i: u32) -> &mut Node {
        self.nodes[i as usize].as_mut().expect("linked node exists")
    }

    fn unlink(&mut self, i: u32) {
        let (prev, next) = {
            let n = self.node(i);
            (n.prev, n.next)
        };
        if prev != NIL {
            self.node(prev).next = next;
        } else {
            self.head = next;
        }
        if next != NIL {
            self.node(next).prev = prev;
        } else {
            self.tail = prev;
        }
    }

    fn push_front(&mut self, i: u32) {
        let old = self.head;
        {
            let n = self.node(i);
            n.prev = NIL;
            n.next = old;
        }
        if old != NIL {
            self.node(old).prev = i;
        } else {
            self.tail = i;
        }
        self.head = i;
    }

    fn remove(&mut self, i: u32) {
        self.unlink(i);
        let node = self.nodes[i as usize].take().expect("linked node exists");
        self.map.remove(&node.key);
        self.bytes -= entry_cost(&node.entry);
        self.free.push(i);
    }
}

fn entry_cost(entry: &Entry) -> usize {
    entry.body.len() + std::mem::size_of::<Node>() + 64
}

/// Result of a cache lookup.
pub enum Lookup {
    Fresh(Arc<Entry>),
    Miss,
}

pub struct Cache {
    shards: Vec<Mutex<Shard>>,
    shard_bytes: usize,
    pub ttl: Duration,
    pub stale_window: Duration,
    pub max_entry_bytes: usize,
    pub evictions: AtomicU64,
    flights: Mutex<HashMap<Key, Arc<Flight>>>,
}

fn shard_of(key: &Key) -> usize {
    key[0] as usize % SHARDS
}

impl Cache {
    pub fn new(max_bytes: usize, ttl: Duration, stale_window: Duration, max_entry_bytes: usize) -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Mutex::new(Shard::new())).collect(),
            shard_bytes: max_bytes / SHARDS,
            ttl,
            stale_window,
            max_entry_bytes,
            evictions: AtomicU64::new(0),
            flights: Mutex::new(HashMap::new()),
        }
    }

    /// Returns a fresh entry and marks it most recently used.
    pub fn get(&self, key: &Key, now: Instant) -> Lookup {
        let mut shard = self.shards[shard_of(key)].lock().unwrap();
        let Some(&i) = shard.map.get(key) else { return Lookup::Miss };
        let entry = shard.node(i).entry.clone();
        if now.saturating_duration_since(entry.created) > self.ttl {
            return Lookup::Miss;
        }
        shard.unlink(i);
        shard.push_front(i);
        Lookup::Fresh(entry)
    }

    /// Returns an expired entry that is still within the stale window.
    pub fn get_stale(&self, key: &Key, now: Instant) -> Option<Arc<Entry>> {
        let mut shard = self.shards[shard_of(key)].lock().unwrap();
        let &i = shard.map.get(key)?;
        let entry = shard.node(i).entry.clone();
        (now.saturating_duration_since(entry.created) <= self.ttl + self.stale_window).then_some(entry)
    }

    /// Inserts an entry, evicting least recently used ones to stay within budget.
    pub fn put(&self, key: Key, entry: Entry) {
        if entry.body.len() > self.max_entry_bytes || entry_cost(&entry) > self.shard_bytes {
            return;
        }
        let cost = entry_cost(&entry);
        let mut shard = self.shards[shard_of(&key)].lock().unwrap();
        if let Some(&i) = shard.map.get(&key) {
            shard.remove(i);
        }
        while shard.bytes + cost > self.shard_bytes && shard.tail != NIL {
            let tail = shard.tail;
            shard.remove(tail);
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
        let node = Node { key, entry: Arc::new(entry), prev: NIL, next: NIL };
        let i = match shard.free.pop() {
            Some(i) => {
                shard.nodes[i as usize] = Some(node);
                i
            }
            None => {
                shard.nodes.push(Some(node));
                (shard.nodes.len() - 1) as u32
            }
        };
        shard.map.insert(key, i);
        shard.push_front(i);
        shard.bytes += cost;
    }

    /// Returns `(entries, bytes)`.
    pub fn stats(&self) -> (usize, usize) {
        self.shards.iter().fold((0, 0), |(e, b), s| {
            let s = s.lock().unwrap();
            (e + s.map.len(), b + s.bytes)
        })
    }

    /// Joins an in-flight request for `key`, or becomes its leader.
    pub fn join_flight(self: &Arc<Self>, key: Key) -> FlightRole {
        let mut flights = self.flights.lock().unwrap();
        if let Some(f) = flights.get(&key) {
            return FlightRole::Follower(f.clone());
        }
        let flight = Arc::new(Flight { done: AtomicBool::new(false), wakers: Mutex::new(Vec::new()) });
        flights.insert(key, flight.clone());
        FlightRole::Leader(FlightGuard { cache: self.clone(), key, flight })
    }
}

/// One in-flight upstream request that others are waiting on.
pub struct Flight {
    done: AtomicBool,
    wakers: Mutex<Vec<Waker>>,
}

impl Flight {
    /// Waits for the leader to finish, whether it succeeded or not.
    pub async fn wait(&self) {
        poll_fn(|cx| {
            if self.done.load(Ordering::Acquire) {
                return Poll::Ready(());
            }
            let mut wakers = self.wakers.lock().unwrap();
            if self.done.load(Ordering::Acquire) {
                return Poll::Ready(());
            }
            wakers.push(cx.waker().clone());
            Poll::Pending
        })
        .await
    }
}

pub enum FlightRole {
    Leader(FlightGuard),
    Follower(Arc<Flight>),
}

/// Completes the flight on drop, including when the leader is cancelled.
pub struct FlightGuard {
    cache: Arc<Cache>,
    key: Key,
    flight: Arc<Flight>,
}

impl Drop for FlightGuard {
    fn drop(&mut self) {
        self.cache.flights.lock().unwrap().remove(&self.key);
        let wakers = {
            let mut w = self.flight.wakers.lock().unwrap();
            self.flight.done.store(true, Ordering::Release);
            std::mem::take(&mut *w)
        };
        for w in wakers {
            w.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(b: u8, shard_byte: u8) -> Key {
        let mut k = [b; 32];
        k[0] = shard_byte;
        k
    }

    fn entry(size: usize, created: Instant) -> Entry {
        Entry {
            body: vec![0; size],
            provider: "p".into(),
            prompt_tokens: 1,
            completion_tokens: 1,
            cost_micro_usd: 0,
            created,
        }
    }

    fn cache(max: usize) -> Arc<Cache> {
        Arc::new(Cache::new(max, Duration::from_secs(10), Duration::from_secs(20), 1 << 20))
    }

    fn fresh(c: &Cache, k: &Key, now: Instant) -> bool {
        matches!(c.get(k, now), Lookup::Fresh(_))
    }

    #[test]
    fn evicts_least_recently_used_within_a_shard() {
        let unit = 100 + std::mem::size_of::<Node>() + 64;
        let c = cache(unit * 2 * SHARDS);
        let now = Instant::now();
        let (a, b, d) = (key(1, 0), key(2, 0), key(3, 0));
        c.put(a, entry(100, now));
        c.put(b, entry(100, now));
        assert!(fresh(&c, &a, now));
        c.put(d, entry(100, now));
        assert!(fresh(&c, &a, now));
        assert!(!fresh(&c, &b, now));
        assert!(fresh(&c, &d, now));
        assert_eq!(c.evictions.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn expired_entries_are_only_served_as_stale() {
        let c = cache(1 << 20);
        let t0 = Instant::now();
        let k = key(1, 1);
        c.put(k, entry(10, t0));
        assert!(fresh(&c, &k, t0 + Duration::from_secs(9)));
        let expired = t0 + Duration::from_secs(11);
        assert!(!fresh(&c, &k, expired));
        assert!(c.get_stale(&k, expired).is_some());
        assert!(c.get_stale(&k, t0 + Duration::from_secs(31)).is_none());
    }

    #[test]
    fn replacing_a_key_keeps_accounting_correct() {
        let c = cache(1 << 20);
        let now = Instant::now();
        let k = key(1, 2);
        c.put(k, entry(1000, now));
        let (_, first) = c.stats();
        c.put(k, entry(1000, now));
        assert_eq!(c.stats(), (1, first));
    }

    #[test]
    fn rejects_entries_over_the_size_limit() {
        let c = Arc::new(Cache::new(1 << 20, Duration::from_secs(1), Duration::ZERO, 10));
        let k = key(1, 3);
        c.put(k, entry(11, Instant::now()));
        assert_eq!(c.stats().0, 0);
    }

    #[test]
    fn slab_slots_are_reused() {
        let unit = 10 + std::mem::size_of::<Node>() + 64;
        let c = cache(unit * SHARDS);
        let now = Instant::now();
        for i in 0..100u8 {
            c.put(key(i, 4), entry(10, now));
        }
        assert_eq!(c.shards[4].lock().unwrap().nodes.len(), 1);
    }

    #[test]
    fn coalescing_releases_followers_when_leader_finishes() {
        let c = cache(1 << 20);
        let k = key(9, 9);
        let leader = c.join_flight(k);
        let FlightRole::Follower(f) = c.join_flight(k) else { panic!("expected follower") };
        assert!(matches!(leader, FlightRole::Leader(_)));
        drop(leader);
        crate::rt::block_on(async move { f.wait().await });
        assert!(matches!(c.join_flight(k), FlightRole::Leader(_)));
    }
}

//! A small thread-per-core async runtime built directly on epoll.
//!
//! Each worker owns one epoll instance, one run queue and one timer tree.
//! Tasks never migrate, so sockets and connection pools need no locks. Wakers
//! are still `Send + Sync`: a wake from another thread goes through an injector
//! queue and an eventfd, which lets shared structures such as the admission
//! queue hand work between workers.

pub mod net;
pub mod sys;

use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

/// Number of panics caught at task boundaries; exported as a metric.
pub static TASK_PANICS: AtomicU64 = AtomicU64::new(0);

static NEXT_WORKER_ID: AtomicU64 = AtomicU64::new(1);

const TASK_BUDGET: usize = 256;
const MAX_EVENTS: usize = 1024;

struct Shared {
    id: u64,
    injector: Mutex<VecDeque<Arc<Task>>>,
    eventfd: i32,
    parked: AtomicBool,
}

impl Shared {
    fn notify(&self) {
        if self.parked.load(Ordering::SeqCst) {
            let one: u64 = 1;
            // SAFETY: eventfd is open for the process lifetime and `one` is 8 bytes.
            unsafe { sys::write(self.eventfd, (&one as *const u64).cast(), 8) };
        }
    }
}

type LocalFuture = Pin<Box<dyn Future<Output = ()>>>;

struct Task {
    future: UnsafeCell<Option<LocalFuture>>,
    scheduled: AtomicBool,
    shared: Arc<Shared>,
}

// SAFETY: `future` is only touched on the owning worker thread (see `run_task`
// and `Drop`). Other threads only reach `scheduled` and `shared`.
unsafe impl Send for Task {}
unsafe impl Sync for Task {}

impl Drop for Task {
    fn drop(&mut self) {
        let on_owner = current_id() == Some(self.shared.id);
        if !on_owner {
            // Leak rather than run non-Send destructors on a foreign thread.
            std::mem::forget(self.future.get_mut().take());
        }
    }
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.scheduled.swap(true, Ordering::AcqRel) {
            return;
        }
        if current_id() == Some(self.shared.id) {
            with_worker(|w| w.queue.borrow_mut().push_back(self.clone()));
        } else {
            self.shared.injector.lock().unwrap().push_back(self.clone());
            self.shared.notify();
        }
    }
}

pub(crate) struct Source {
    pub readable: Cell<bool>,
    pub writable: Cell<bool>,
    pub closed: Cell<bool>,
    pub read_waker: RefCell<Option<Waker>>,
    pub write_waker: RefCell<Option<Waker>>,
}

struct Worker {
    shared: Arc<Shared>,
    queue: RefCell<VecDeque<Arc<Task>>>,
    epfd: i32,
    timers: RefCell<BTreeMap<(Instant, u64), Waker>>,
    timer_seq: Cell<u64>,
}

thread_local! {
    static WORKER: Cell<*const Worker> = const { Cell::new(std::ptr::null()) };
}

fn current_id() -> Option<u64> {
    let ptr = WORKER.with(|w| w.get());
    if ptr.is_null() {
        None
    } else {
        // SAFETY: the pointer is set for the duration of `block_on` on this thread.
        Some(unsafe { (&*ptr).shared.id })
    }
}

fn with_worker<R>(f: impl FnOnce(&Worker) -> R) -> R {
    let ptr = WORKER.with(|w| w.get());
    assert!(!ptr.is_null(), "called outside the runtime");
    // SAFETY: the pointer is set for the duration of `block_on` on this thread.
    f(unsafe { &*ptr })
}

pub(crate) fn register_fd(fd: i32) -> std::io::Result<Rc<Source>> {
    let source = Rc::new(Source {
        readable: Cell::new(true),
        writable: Cell::new(true),
        closed: Cell::new(false),
        read_waker: RefCell::new(None),
        write_waker: RefCell::new(None),
    });
    let mut ev = sys::epoll_event {
        events: sys::EPOLLIN | sys::EPOLLOUT | sys::EPOLLRDHUP | sys::EPOLLET,
        data: Rc::as_ptr(&source) as u64,
    };
    let epfd = with_worker(|w| w.epfd);
    // SAFETY: `ev` is valid for the call; the Source outlives its registration.
    if unsafe { sys::epoll_ctl(epfd, sys::EPOLL_CTL_ADD, fd, &mut ev) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(source)
}

pub(crate) fn deregister_fd(fd: i32) {
    let epfd = with_worker(|w| w.epfd);
    let mut ev = sys::epoll_event { events: 0, data: 0 };
    // SAFETY: `ev` is valid for the call.
    unsafe { sys::epoll_ctl(epfd, sys::EPOLL_CTL_DEL, fd, &mut ev) };
}

/// Spawns a task on the current worker.
pub fn spawn<F: Future<Output = ()> + 'static>(fut: F) {
    with_worker(|w| {
        let task = Arc::new(Task {
            future: UnsafeCell::new(Some(Box::pin(fut))),
            scheduled: AtomicBool::new(true),
            shared: w.shared.clone(),
        });
        w.queue.borrow_mut().push_back(task);
    });
}

fn run_task(task: Arc<Task>) {
    task.scheduled.store(false, Ordering::Release);
    let waker = Waker::from(task.clone());
    let mut cx = Context::from_waker(&waker);
    // SAFETY: run_task is only called on the task's owning worker and is not
    // reentrant, so this is the only live reference to the future.
    let slot = unsafe { &mut *task.future.get() };
    if let Some(fut) = slot.as_mut() {
        let outcome = catch_unwind(AssertUnwindSafe(|| fut.as_mut().poll(&mut cx)));
        match outcome {
            Ok(Poll::Pending) => {}
            Ok(Poll::Ready(())) => *slot = None,
            Err(_) => {
                TASK_PANICS.fetch_add(1, Ordering::Relaxed);
                *slot = None;
            }
        }
    }
}

/// Runs `root` to completion on this thread, driving every task spawned from it.
pub fn block_on<T: 'static>(root: impl Future<Output = T> + 'static) -> T {
    // SAFETY: plain syscalls.
    let (epfd, evfd) =
        unsafe { (sys::epoll_create1(sys::EPOLL_CLOEXEC), sys::eventfd(0, sys::EFD_NONBLOCK | sys::EFD_CLOEXEC)) };
    assert!(epfd >= 0 && evfd >= 0, "failed to create epoll or eventfd");
    let mut ev = sys::epoll_event { events: sys::EPOLLIN, data: 0 };
    // SAFETY: `ev` is valid for the call.
    assert_eq!(unsafe { sys::epoll_ctl(epfd, sys::EPOLL_CTL_ADD, evfd, &mut ev) }, 0);

    let worker = Box::new(Worker {
        shared: Arc::new(Shared {
            id: NEXT_WORKER_ID.fetch_add(1, Ordering::Relaxed),
            injector: Mutex::new(VecDeque::new()),
            eventfd: evfd,
            parked: AtomicBool::new(false),
        }),
        queue: RefCell::new(VecDeque::new()),
        epfd,
        timers: RefCell::new(BTreeMap::new()),
        timer_seq: Cell::new(0),
    });
    WORKER.with(|w| w.set(&*worker));

    let result: Rc<RefCell<Option<T>>> = Rc::new(RefCell::new(None));
    let slot = result.clone();
    spawn(async move {
        let value = root.await;
        *slot.borrow_mut() = Some(value);
    });

    let mut events = vec![sys::epoll_event { events: 0, data: 0 }; MAX_EVENTS];
    loop {
        {
            let mut injected = worker.shared.injector.lock().unwrap();
            worker.queue.borrow_mut().extend(injected.drain(..));
        }
        for _ in 0..TASK_BUDGET {
            let next = worker.queue.borrow_mut().pop_front();
            match next {
                Some(task) => run_task(task),
                None => break,
            }
        }
        if let Some(value) = result.borrow_mut().take() {
            WORKER.with(|w| w.set(std::ptr::null()));
            // Intentionally leaked: live tasks may still hold wakers to this worker.
            std::mem::forget(worker);
            return value;
        }

        let now = Instant::now();
        fire_timers(&worker, now);

        worker.shared.parked.store(true, Ordering::SeqCst);
        let busy = !worker.queue.borrow().is_empty() || !worker.shared.injector.lock().unwrap().is_empty();
        let timeout = if busy {
            0
        } else {
            match worker.timers.borrow().keys().next() {
                Some((deadline, _)) => {
                    let wait = deadline.saturating_duration_since(now);
                    (wait.as_micros().div_ceil(1000)).min(1000) as i32
                }
                None => 1000,
            }
        };
        // SAFETY: `events` holds MAX_EVENTS initialized entries.
        let n = unsafe { sys::epoll_wait(epfd, events.as_mut_ptr(), MAX_EVENTS as i32, timeout) };
        worker.shared.parked.store(false, Ordering::SeqCst);

        for ev in events.iter().take(n.max(0) as usize) {
            let (flags, data) = (ev.events, ev.data);
            if data == 0 {
                let mut drain = [0u8; 8];
                // SAFETY: `drain` is 8 writable bytes, as eventfd requires.
                unsafe { sys::read(evfd, drain.as_mut_ptr().cast(), 8) };
                continue;
            }
            // SAFETY: sources stay alive while registered, and deregistration
            // happens only in tasks, which never run during this dispatch loop.
            let source = unsafe { &*(data as *const Source) };
            let hangup = flags & (sys::EPOLLERR | sys::EPOLLHUP | sys::EPOLLRDHUP) != 0;
            if hangup {
                source.closed.set(true);
            }
            if hangup || flags & sys::EPOLLIN != 0 {
                source.readable.set(true);
                if let Some(w) = source.read_waker.borrow_mut().take() {
                    w.wake();
                }
            }
            if hangup || flags & sys::EPOLLOUT != 0 {
                source.writable.set(true);
                if let Some(w) = source.write_waker.borrow_mut().take() {
                    w.wake();
                }
            }
        }
    }
}

fn fire_timers(worker: &Worker, now: Instant) {
    loop {
        let key = match worker.timers.borrow().keys().next() {
            Some(k) if k.0 <= now => *k,
            _ => return,
        };
        let waker = worker.timers.borrow_mut().remove(&key);
        if let Some(w) = waker {
            w.wake();
        }
    }
}

/// Future that completes at a deadline.
pub struct Sleep {
    deadline: Instant,
    key: Option<(Instant, u64)>,
}

/// Completes after `d`.
pub fn sleep(d: Duration) -> Sleep {
    Sleep { deadline: Instant::now() + d, key: None }
}

/// Completes at `deadline`.
pub fn sleep_until(deadline: Instant) -> Sleep {
    Sleep { deadline, key: None }
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if Instant::now() >= self.deadline {
            if let Some(key) = self.key.take() {
                with_worker(|w| w.timers.borrow_mut().remove(&key));
            }
            return Poll::Ready(());
        }
        let deadline = self.deadline;
        let existing = self.key;
        let key = with_worker(|w| {
            let key = existing.unwrap_or_else(|| {
                let seq = w.timer_seq.get();
                w.timer_seq.set(seq + 1);
                (deadline, seq)
            });
            w.timers.borrow_mut().insert(key, cx.waker().clone());
            key
        });
        self.key = Some(key);
        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            with_worker(|w| w.timers.borrow_mut().remove(&key));
        }
    }
}

/// Result of [`select`].
pub enum Either<A, B> {
    Left(A),
    Right(B),
}

/// Polls two pinned futures and returns whichever finishes first.
pub fn select<A: Future + Unpin, B: Future + Unpin>(a: A, b: B) -> Select<A, B> {
    Select { a, b }
}

pub struct Select<A, B> {
    a: A,
    b: B,
}

impl<A: Future + Unpin, B: Future + Unpin> Future for Select<A, B> {
    type Output = Either<A::Output, B::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Poll::Ready(v) = Pin::new(&mut self.a).poll(cx) {
            return Poll::Ready(Either::Left(v));
        }
        if let Poll::Ready(v) = Pin::new(&mut self.b).poll(cx) {
            return Poll::Ready(Either::Right(v));
        }
        Poll::Pending
    }
}

/// Error returned by [`timeout`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed;

/// Runs `fut` with a time limit, cancelling it by drop on expiry.
pub async fn timeout<F: Future>(d: Duration, fut: F) -> Result<F::Output, Elapsed> {
    let fut = std::pin::pin!(fut);
    match select(fut, sleep(d)).await {
        Either::Left(v) => Ok(v),
        Either::Right(()) => Err(Elapsed),
    }
}

/// Yields to the scheduler once.
pub async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(move |cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await
}

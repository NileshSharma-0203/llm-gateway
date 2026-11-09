//! Async TCP primitives over the per-worker reactor.

use super::{deregister_fd, register_fd, sys, Source};
use std::future::poll_fn;
use std::io::{self, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd};
use std::rc::Rc;
use std::task::Poll;

/// A non-blocking TCP stream registered with the current worker.
pub struct TcpConn {
    stream: TcpStream,
    source: Rc<Source>,
}

impl TcpConn {
    /// Adopts an accepted stream.
    pub fn from_std(stream: TcpStream) -> io::Result<Self> {
        stream.set_nonblocking(true)?;
        let source = register_fd(stream.as_raw_fd())?;
        Ok(Self { stream, source })
    }

    /// Connects without blocking the worker.
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let (fd, in_progress) = sys::connect_nonblocking(&addr)?;
        // SAFETY: `fd` is a freshly created socket owned by nothing else.
        let stream = unsafe { TcpStream::from_raw_fd(fd) };
        stream.set_nodelay(true)?;
        let source = register_fd(fd)?;
        let conn = Self { stream, source };
        if in_progress {
            conn.source.writable.set(false);
            poll_fn(|cx| {
                if conn.source.writable.get() {
                    Poll::Ready(())
                } else {
                    *conn.source.write_waker.borrow_mut() = Some(cx.waker().clone());
                    Poll::Pending
                }
            })
            .await;
            sys::take_socket_error(fd)?;
        }
        Ok(conn)
    }

    /// Reads once, waiting until data or EOF is available.
    pub async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        poll_fn(|cx| loop {
            if !self.source.readable.get() {
                *self.source.read_waker.borrow_mut() = Some(cx.waker().clone());
                return Poll::Pending;
            }
            match (&self.stream).read(buf) {
                Ok(n) => return Poll::Ready(Ok(n)),
                Err(e) if e.kind() == ErrorKind::WouldBlock => self.source.readable.set(false),
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
        })
        .await
    }

    /// Writes the whole buffer. A slow peer suspends only this task, which is
    /// what carries backpressure from a client back to its upstream read loop.
    pub async fn write_all(&self, buf: &[u8]) -> io::Result<()> {
        let mut written = 0;
        poll_fn(|cx| loop {
            if written == buf.len() {
                return Poll::Ready(Ok(()));
            }
            if !self.source.writable.get() {
                *self.source.write_waker.borrow_mut() = Some(cx.waker().clone());
                return Poll::Pending;
            }
            match (&self.stream).write(&buf[written..]) {
                Ok(0) => return Poll::Ready(Err(ErrorKind::WriteZero.into())),
                Ok(n) => written += n,
                Err(e) if e.kind() == ErrorKind::WouldBlock => self.source.writable.set(false),
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
        })
        .await
    }

    /// Completes when the peer hangs up. Must not run alongside `read` on the
    /// same connection, because both use the read waker slot.
    pub async fn closed(&self) {
        poll_fn(|cx| {
            if self.source.closed.get() {
                Poll::Ready(())
            } else {
                *self.source.read_waker.borrow_mut() = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }

    /// True if the reactor has observed a hangup.
    pub fn is_closed(&self) -> bool {
        self.source.closed.get()
    }
}

impl Drop for TcpConn {
    fn drop(&mut self) {
        deregister_fd(self.stream.as_raw_fd());
    }
}

/// A non-blocking listener registered with the current worker.
pub struct Acceptor {
    listener: TcpListener,
    source: Rc<Source>,
}

impl Acceptor {
    /// Adopts a raw listening fd from [`sys::listen_reuseport`].
    pub fn from_raw(fd: i32) -> io::Result<Self> {
        // SAFETY: the caller transfers ownership of a valid listening socket.
        let listener = unsafe { TcpListener::from_raw_fd(fd) };
        let source = register_fd(fd)?;
        Ok(Self { listener, source })
    }

    /// Accepts one connection.
    pub async fn accept(&self) -> io::Result<TcpStream> {
        poll_fn(|cx| loop {
            if !self.source.readable.get() {
                *self.source.read_waker.borrow_mut() = Some(cx.waker().clone());
                return Poll::Pending;
            }
            match self.listener.accept() {
                Ok((stream, _)) => return Poll::Ready(Ok(stream)),
                Err(e) if e.kind() == ErrorKind::WouldBlock => self.source.readable.set(false),
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
        })
        .await
    }
}

impl Drop for Acceptor {
    fn drop(&mut self) {
        deregister_fd(self.listener.as_raw_fd());
    }
}

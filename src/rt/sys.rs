//! Minimal Linux FFI surface. `std` already links libc, so these declarations
//! are all the runtime needs in place of the `libc` crate.
#![allow(non_camel_case_types)]

use std::io;
use std::net::SocketAddr;
use std::os::raw::{c_int, c_void};

pub const EPOLL_CLOEXEC: c_int = 0o2000000;
pub const EPOLL_CTL_ADD: c_int = 1;
pub const EPOLL_CTL_DEL: c_int = 2;
pub const EPOLLIN: u32 = 0x001;
pub const EPOLLOUT: u32 = 0x004;
pub const EPOLLERR: u32 = 0x008;
pub const EPOLLHUP: u32 = 0x010;
pub const EPOLLRDHUP: u32 = 0x2000;
pub const EPOLLET: u32 = 1 << 31;

pub const EFD_NONBLOCK: c_int = 0o4000;
pub const EFD_CLOEXEC: c_int = 0o2000000;

const AF_INET: c_int = 2;
const AF_INET6: c_int = 10;
const SOCK_STREAM: c_int = 1;
const SOCK_NONBLOCK: c_int = 0o4000;
const SOCK_CLOEXEC: c_int = 0o2000000;
const SOL_SOCKET: c_int = 1;
const SO_REUSEADDR: c_int = 2;
const SO_ERROR: c_int = 4;
const SO_REUSEPORT: c_int = 15;
const EINPROGRESS: i32 = 115;

pub const SIGINT: c_int = 2;
pub const SIGTERM: c_int = 15;

#[cfg_attr(target_arch = "x86_64", repr(C, packed))]
#[cfg_attr(not(target_arch = "x86_64"), repr(C))]
#[derive(Clone, Copy)]
pub struct epoll_event {
    pub events: u32,
    pub data: u64,
}

extern "C" {
    pub fn epoll_create1(flags: c_int) -> c_int;
    pub fn epoll_ctl(epfd: c_int, op: c_int, fd: c_int, event: *mut epoll_event) -> c_int;
    pub fn epoll_wait(epfd: c_int, events: *mut epoll_event, max: c_int, timeout: c_int) -> c_int;
    pub fn eventfd(initval: u32, flags: c_int) -> c_int;
    pub fn read(fd: c_int, buf: *mut c_void, count: usize) -> isize;
    pub fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
    fn socket(domain: c_int, ty: c_int, protocol: c_int) -> c_int;
    fn setsockopt(fd: c_int, level: c_int, name: c_int, val: *const c_void, len: u32) -> c_int;
    fn getsockopt(fd: c_int, level: c_int, name: c_int, val: *mut c_void, len: *mut u32) -> c_int;
    fn bind(fd: c_int, addr: *const c_void, len: u32) -> c_int;
    fn listen(fd: c_int, backlog: c_int) -> c_int;
    fn connect(fd: c_int, addr: *const c_void, len: u32) -> c_int;
    fn getsockname(fd: c_int, addr: *mut c_void, len: *mut u32) -> c_int;
    fn close(fd: c_int) -> c_int;
    fn signal(sig: c_int, handler: usize) -> usize;
}

#[repr(C)]
struct SockaddrStorage {
    bytes: [u8; 28],
}

fn encode_addr(addr: &SocketAddr) -> (SockaddrStorage, u32, c_int) {
    let mut bytes = [0u8; 28];
    match addr {
        SocketAddr::V4(a) => {
            bytes[0..2].copy_from_slice(&(AF_INET as u16).to_ne_bytes());
            bytes[2..4].copy_from_slice(&a.port().to_be_bytes());
            bytes[4..8].copy_from_slice(&a.ip().octets());
            (SockaddrStorage { bytes }, 16, AF_INET)
        }
        SocketAddr::V6(a) => {
            bytes[0..2].copy_from_slice(&(AF_INET6 as u16).to_ne_bytes());
            bytes[2..4].copy_from_slice(&a.port().to_be_bytes());
            bytes[4..8].copy_from_slice(&a.flowinfo().to_be_bytes());
            bytes[8..24].copy_from_slice(&a.ip().octets());
            bytes[24..28].copy_from_slice(&a.scope_id().to_ne_bytes());
            (SockaddrStorage { bytes }, 28, AF_INET6)
        }
    }
}

fn check(ret: c_int) -> io::Result<c_int> {
    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(ret)
    }
}

fn set_opt(fd: c_int, name: c_int) -> io::Result<()> {
    let one: c_int = 1;
    // SAFETY: `one` outlives the call and the length matches its size.
    check(unsafe { setsockopt(fd, SOL_SOCKET, name, (&one as *const c_int).cast(), 4) })?;
    Ok(())
}

/// Binds a listening socket with `SO_REUSEPORT`, so each worker owns its own
/// accept queue and the kernel balances connections across workers.
pub fn listen_reuseport(addr: &SocketAddr, backlog: i32) -> io::Result<(c_int, SocketAddr)> {
    let (storage, len, family) = encode_addr(addr);
    // SAFETY: plain syscalls; `storage` is valid for `len` bytes.
    unsafe {
        let fd = check(socket(family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0))?;
        let setup = (|| {
            set_opt(fd, SO_REUSEADDR)?;
            set_opt(fd, SO_REUSEPORT)?;
            check(bind(fd, storage.bytes.as_ptr().cast(), len))?;
            check(listen(fd, backlog))?;
            let mut out = [0u8; 28];
            let mut out_len = 28u32;
            check(getsockname(fd, out.as_mut_ptr().cast(), &mut out_len))?;
            let port = u16::from_be_bytes([out[2], out[3]]);
            let mut bound = *addr;
            bound.set_port(port);
            Ok(bound)
        })();
        match setup {
            Ok(bound) => Ok((fd, bound)),
            Err(e) => {
                close(fd);
                Err(e)
            }
        }
    }
}

/// Starts a non-blocking connect. Returns the fd and whether it is still in progress.
pub fn connect_nonblocking(addr: &SocketAddr) -> io::Result<(c_int, bool)> {
    let (storage, len, family) = encode_addr(addr);
    // SAFETY: plain syscalls; `storage` is valid for `len` bytes.
    unsafe {
        let fd = check(socket(family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0))?;
        if connect(fd, storage.bytes.as_ptr().cast(), len) == 0 {
            return Ok((fd, false));
        }
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(EINPROGRESS) {
            Ok((fd, true))
        } else {
            close(fd);
            Err(err)
        }
    }
}

/// Reads and clears the pending socket error after a non-blocking connect.
pub fn take_socket_error(fd: c_int) -> io::Result<()> {
    let mut value: c_int = 0;
    let mut len = 4u32;
    // SAFETY: `value` is valid for `len` bytes for the duration of the call.
    check(unsafe { getsockopt(fd, SOL_SOCKET, SO_ERROR, (&mut value as *mut c_int).cast(), &mut len) })?;
    if value != 0 {
        Err(io::Error::from_raw_os_error(value))
    } else {
        Ok(())
    }
}

/// Installs a signal handler. The handler must be async-signal-safe.
pub fn install_signal(sig: c_int, handler: extern "C" fn(c_int)) {
    // SAFETY: `handler` is a valid `extern "C"` function for the process lifetime.
    unsafe {
        signal(sig, handler as usize);
    }
}

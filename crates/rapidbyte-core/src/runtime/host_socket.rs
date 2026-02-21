//! Host TCP socket helpers backing WIT host socket operations.

use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

/// Default milliseconds to wait for socket readiness before returning WouldBlock.
/// Override with RAPIDBYTE_SOCKET_POLL_MS env var for performance tuning.
pub(crate) const SOCKET_READY_POLL_MS: i32 = 1;

/// Number of consecutive WouldBlock events before activating poll(1ms).
/// At ~2M iterations/sec CPU speed, 1024 iterations ≈ 0.5ms of spinning.
/// This avoids adding 1ms poll overhead to transient WouldBlocks during active streaming.
pub(crate) const SOCKET_POLL_ACTIVATION_THRESHOLD: u32 = 1024;

/// Interest direction for socket readiness polling.
#[cfg(unix)]
pub(crate) enum SocketInterest {
    Read,
    Write,
}

/// Wait up to `timeout_ms` for a socket to become ready for the given interest.
///
/// Returns:
/// - `Ok(true)` if the socket is ready (including error/HUP conditions — the
///   caller should discover the specifics via the next I/O call).
/// - `Ok(false)` on a clean timeout (no events).
/// - `Err(io::Error)` on a non-EINTR poll failure (logged by caller for
///   observability, then treated as "ready" to let I/O surface the real error).
///
/// Handles EINTR by retrying. POLLERR/POLLHUP/POLLNVAL are returned as
/// `Ok(true)` so that the subsequent read()/write() surfaces the real error
/// through normal error handling.
#[cfg(unix)]
pub(crate) fn wait_socket_ready(
    stream: &TcpStream,
    interest: SocketInterest,
    timeout_ms: i32,
) -> std::io::Result<bool> {
    let events = match interest {
        SocketInterest::Read => libc::POLLIN,
        SocketInterest::Write => libc::POLLOUT,
    };
    let mut pfd = libc::pollfd {
        fd: stream.as_raw_fd(),
        events,
        revents: 0,
    };
    loop {
        let ret = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
        if ret < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue; // EINTR: retry
            }
            return Err(err);
        }
        // ret == 0: clean timeout, socket not ready.
        // ret > 0: socket has events. This includes POLLERR, POLLHUP, POLLNVAL
        // — all of which mean the next I/O call will return the real error/EOF.
        return Ok(ret > 0);
    }
}

/// Read the socket poll timeout, allowing env override for perf tuning.
pub(crate) fn socket_poll_timeout_ms() -> i32 {
    static CACHED: std::sync::OnceLock<i32> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("RAPIDBYTE_SOCKET_POLL_MS")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(SOCKET_READY_POLL_MS)
    })
}

/// Per-socket state tracking the TCP stream and consecutive WouldBlock streaks.
pub(crate) struct SocketEntry {
    pub(crate) stream: TcpStream,
    pub(crate) read_would_block_streak: u32,
    pub(crate) write_would_block_streak: u32,
}

pub(crate) fn resolve_socket_addrs(host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }

    let addrs: Vec<SocketAddr> = (host, port).to_socket_addrs()?.collect();
    if addrs.is_empty() {
        Err(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "No address",
        ))
    } else {
        Ok(addrs)
    }
}

pub(crate) enum SocketReadResultInternal {
    Data(Vec<u8>),
    Eof,
    WouldBlock,
}

pub(crate) enum SocketWriteResultInternal {
    Written(u64),
    WouldBlock,
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    mod socket_poll_tests {
        use super::super::{wait_socket_ready, SocketInterest};

        #[test]
        fn wait_ready_returns_true_when_data_available() {
            use std::io::Write;

            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let client = std::net::TcpStream::connect(addr).unwrap();
            let (mut server, _) = listener.accept().unwrap();
            client.set_nonblocking(true).unwrap();

            server.write_all(b"hello").unwrap();
            // Generous sleep to ensure data arrives in kernel buffer (CI-safe)
            std::thread::sleep(std::time::Duration::from_millis(50));

            assert!(wait_socket_ready(&client, SocketInterest::Read, 500).unwrap());
        }

        #[test]
        fn wait_ready_returns_false_on_timeout() {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let client = std::net::TcpStream::connect(addr).unwrap();
            let _server = listener.accept().unwrap();
            client.set_nonblocking(true).unwrap();

            // No data written — poll should timeout
            assert!(!wait_socket_ready(&client, SocketInterest::Read, 1).unwrap());
        }

        #[test]
        fn wait_ready_writable_for_connected_socket() {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let client = std::net::TcpStream::connect(addr).unwrap();
            let _server = listener.accept().unwrap();
            client.set_nonblocking(true).unwrap();

            // Fresh connected socket with empty send buffer should be writable
            assert!(wait_socket_ready(&client, SocketInterest::Write, 500).unwrap());
        }

        #[test]
        fn wait_ready_detects_peer_close_as_ready() {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let client = std::net::TcpStream::connect(addr).unwrap();
            let (server, _) = listener.accept().unwrap();
            client.set_nonblocking(true).unwrap();

            // Close server side — client should see HUP/readability for EOF
            drop(server);
            std::thread::sleep(std::time::Duration::from_millis(50));

            assert!(wait_socket_ready(&client, SocketInterest::Read, 500).unwrap());
        }
    }
}

//! Host TCP socket helpers for connector network I/O.

use std::net::{IpAddr, SocketAddr, ToSocketAddrs};

/// Resolve a hostname and port to a list of socket addresses.
///
/// # Errors
///
/// Returns an `io::Error` if DNS resolution fails or yields no addresses.
pub fn resolve_socket_addrs(host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>> {
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

pub enum SocketReadResult {
    Data(Vec<u8>),
    Eof,
    WouldBlock,
}

pub enum SocketWriteResult {
    Written(u64),
    WouldBlock,
}

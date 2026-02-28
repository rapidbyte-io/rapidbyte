//! AsyncRead/AsyncWrite adapter over Rapidbyte host-proxied sockets.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::{sleep, Sleep};

use crate::error::ConnectorError;
use crate::host_ffi::{self, SocketReadResult, SocketWriteResult};

/// TCP stream backed by host-side socket operations.
pub struct HostTcpStream {
    handle: u64,
    closed: bool,
    read_sleep: Option<Pin<Box<Sleep>>>,
    write_sleep: Option<Pin<Box<Sleep>>>,
}

impl HostTcpStream {
    pub fn connect(host: &str, port: u16) -> Result<Self, ConnectorError> {
        let handle = host_ffi::connect_tcp(host, port)?;
        Ok(Self {
            handle,
            closed: false,
            read_sleep: None,
            write_sleep: None,
        })
    }

    pub fn handle(&self) -> u64 {
        self.handle
    }

    fn close_inner(&mut self) {
        if !self.closed {
            host_ffi::socket_close(self.handle);
            self.closed = true;
        }
    }
}

impl Drop for HostTcpStream {
    fn drop(&mut self) {
        self.close_inner();
    }
}

fn to_io_error(err: ConnectorError) -> io::Error {
    io::Error::other(err.to_string())
}

impl AsyncRead for HostTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.closed {
            return Poll::Ready(Ok(()));
        }

        // If we are currently backing off from a previous WouldBlock,
        // poll the sleep future. If it's still pending, remain pending.
        if let Some(ref mut sleep_fut) = this.read_sleep {
            if sleep_fut.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            this.read_sleep = None;
        }

        let remaining = buf.remaining();
        if remaining == 0 {
            return Poll::Ready(Ok(()));
        }

        match host_ffi::socket_read(this.handle, remaining as u64) {
            Ok(SocketReadResult::Data(data)) => {
                if data.is_empty() {
                    return Poll::Ready(Ok(()));
                }
                let copy_len = data.len().min(buf.remaining());
                buf.put_slice(&data[..copy_len]);
                Poll::Ready(Ok(()))
            }
            Ok(SocketReadResult::Eof) => Poll::Ready(Ok(())),
            Ok(SocketReadResult::WouldBlock) => {
                // Cooperative backoff: wire up a 1ms sleep to Tokio's reactor.
                // Tokio converts this into a WASI clock pollable, which Wasmtime
                // will natively suspend on without spinning the CPU.
                let mut sleep_fut = Box::pin(sleep(Duration::from_millis(1)));
                let _ = sleep_fut.as_mut().poll(cx);
                this.read_sleep = Some(sleep_fut);
                Poll::Pending
            }
            Err(err) => Poll::Ready(Err(to_io_error(err))),
        }
    }
}

impl AsyncWrite for HostTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "socket closed",
            )));
        }

        if let Some(ref mut sleep_fut) = this.write_sleep {
            if sleep_fut.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            this.write_sleep = None;
        }

        if data.is_empty() {
            return Poll::Ready(Ok(0));
        }

        match host_ffi::socket_write(this.handle, data) {
            Ok(SocketWriteResult::Written(n)) => Poll::Ready(Ok(n as usize)),
            Ok(SocketWriteResult::WouldBlock) => {
                let mut sleep_fut = Box::pin(sleep(Duration::from_millis(1)));
                let _ = sleep_fut.as_mut().poll(cx);
                this.write_sleep = Some(sleep_fut);
                Poll::Pending
            }
            Err(err) => Poll::Ready(Err(to_io_error(err))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.close_inner();
        Poll::Ready(Ok(()))
    }
}

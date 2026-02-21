//! Minimal `whoami` shim for connector WASM builds.
//!
//! `tokio-postgres` only needs `whoami::username()`. The upstream `whoami`
//! crate currently uses unstable WASI APIs on `wasm32-wasip2`, so we provide a
//! tiny compatible surface for connector builds.

use std::env;
use std::io;

pub type Error = io::Error;
pub type Result<T = (), E = Error> = std::result::Result<T, E>;

pub fn username() -> Result<String> {
    if let Ok(user) = env::var("USER") {
        if !user.is_empty() {
            return Ok(user);
        }
    }

    if let Ok(user) = env::var("USERNAME") {
        if !user.is_empty() {
            return Ok(user);
        }
    }

    Ok("rapidbyte".to_string())
}

#![warn(clippy::pedantic)]

pub mod adapter;
pub mod application;
pub mod config;
pub mod domain;
pub mod proto;
pub mod traits;

mod server;
pub use config::{ControllerConfig, ServerTlsConfig};
pub use server::run;

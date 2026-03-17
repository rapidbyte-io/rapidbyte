#![warn(clippy::pedantic)]

pub mod adapter;
pub mod application;
pub mod config;
pub mod domain;
pub mod proto;

mod server;
pub use server::run;

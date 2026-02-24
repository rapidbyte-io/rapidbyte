//! Pipeline state persistence for the Rapidbyte engine.
//!
//! Provides the [`StateBackend`] trait and a [`SqliteStateBackend`]
//! implementation for cursor tracking, run history, and dead-letter
//! queue storage.

#![warn(clippy::pedantic)]

pub mod backend;
pub mod error;
pub mod schema;
pub mod sqlite;

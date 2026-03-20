//! Adapter layer: concrete implementations of domain ports.

pub mod channel_progress;

pub use channel_progress::AtomicProgressCollector;

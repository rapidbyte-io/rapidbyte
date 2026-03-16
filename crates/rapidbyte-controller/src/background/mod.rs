//! Background tasks for the controller.

mod lease_sweep;
mod preview_cleanup;
mod reaper;

pub use lease_sweep::spawn_lease_sweep;
pub use preview_cleanup::spawn_preview_cleanup;
pub use reaper::spawn_reaper;

// Re-export internals needed by tests in other modules
#[allow(unused_imports)]
pub(crate) use lease_sweep::{handle_expired_lease, sweep_reconciliation_timeouts};

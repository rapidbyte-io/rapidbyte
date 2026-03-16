//! Post-execution lifecycle: checkpoint correlation, DLQ persistence, and run finalization.

pub(crate) mod checkpoint;
pub(crate) mod dlq;

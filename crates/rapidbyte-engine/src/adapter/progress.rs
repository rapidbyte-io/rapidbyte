//! [`ProgressReporter`] adapter that delivers events over an unbounded channel.
//!
//! The CLI subscriber consumes events from the receiving end to drive
//! spinner updates, progress bars, or structured logging.

use tokio::sync::mpsc;

use crate::domain::progress::{ProgressEvent, ProgressReporter};

/// Progress reporter that sends events through a `tokio::sync::mpsc` channel.
///
/// Sending is best-effort: if the receiver has been dropped, events are
/// silently discarded so pipeline execution is never blocked by progress
/// delivery failures.
pub struct ChannelProgressReporter {
    tx: mpsc::UnboundedSender<ProgressEvent>,
}

impl ChannelProgressReporter {
    /// Create a new reporter from the sending half of an unbounded channel.
    #[must_use]
    pub fn new(tx: mpsc::UnboundedSender<ProgressEvent>) -> Self {
        Self { tx }
    }
}

impl ProgressReporter for ChannelProgressReporter {
    fn report(&self, event: ProgressEvent) {
        // Best-effort delivery: ignore closed-channel errors so the
        // pipeline never stalls on progress reporting.
        let _ = self.tx.send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::progress::Phase;

    #[test]
    fn channel_reporter_sends_events() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let reporter = ChannelProgressReporter::new(tx);
        reporter.report(ProgressEvent::PhaseChanged {
            phase: Phase::Running,
        });
        let event = rx.try_recv().unwrap();
        assert!(matches!(
            event,
            ProgressEvent::PhaseChanged {
                phase: Phase::Running
            }
        ));
    }

    #[test]
    fn channel_reporter_does_not_panic_when_receiver_dropped() {
        let (tx, rx) = mpsc::unbounded_channel();
        let reporter = ChannelProgressReporter::new(tx);
        drop(rx);
        reporter.report(ProgressEvent::PhaseChanged {
            phase: Phase::Finalizing,
        });
        // No panic = test passes
    }
}

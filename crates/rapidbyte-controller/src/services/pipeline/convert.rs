//! Maps internal `RunState` to proto `RunState`.

use crate::proto::rapidbyte::v1::RunState;
use crate::run_state::RunState as InternalRunState;

/// Maps internal `RunState` to proto `RunState` enum value.
#[must_use]
pub(crate) fn to_proto_state(s: InternalRunState) -> i32 {
    match s {
        InternalRunState::Pending => RunState::Pending.into(),
        InternalRunState::Assigned => RunState::Assigned.into(),
        InternalRunState::Reconciling => RunState::Reconciling.into(),
        InternalRunState::Running | InternalRunState::Cancelling => RunState::Running.into(),
        InternalRunState::PreviewReady => RunState::PreviewReady.into(),
        InternalRunState::Completed => RunState::Completed.into(),
        InternalRunState::RecoveryFailed => RunState::RecoveryFailed.into(),
        InternalRunState::Failed | InternalRunState::TimedOut => RunState::Failed.into(),
        InternalRunState::Cancelled => RunState::Cancelled.into(),
    }
}

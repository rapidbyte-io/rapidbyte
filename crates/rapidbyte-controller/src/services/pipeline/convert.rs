//! Maps internal `RunState` to proto `RunState`.

use crate::proto::rapidbyte::v1::RunState as ProtoRunState;
use crate::run_state::RunState;

/// Maps internal `RunState` to proto `RunState` enum value.
#[must_use]
pub(crate) fn to_proto_state(s: RunState) -> i32 {
    match s {
        RunState::Pending => ProtoRunState::Pending.into(),
        RunState::Assigned => ProtoRunState::Assigned.into(),
        RunState::Reconciling => ProtoRunState::Reconciling.into(),
        RunState::Running | RunState::Cancelling => ProtoRunState::Running.into(),
        RunState::PreviewReady => ProtoRunState::PreviewReady.into(),
        RunState::Completed => ProtoRunState::Completed.into(),
        RunState::RecoveryFailed => ProtoRunState::RecoveryFailed.into(),
        RunState::Failed | RunState::TimedOut => ProtoRunState::Failed.into(),
        RunState::Cancelled => ProtoRunState::Cancelled.into(),
    }
}

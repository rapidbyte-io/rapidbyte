//! Destination `PostgreSQL` operator-facing diagnostics.

use rapidbyte_sdk::prelude::CommitState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckpointSafetyPhase {
    LoopFailureBeforeNewDurableCommit,
    PreCommitFailureBeforePostgresCommit,
    PostCommitSwapOrCheckpointFailure,
}

pub(crate) fn schema_drift_summary(
    new_columns: usize,
    removed_columns: usize,
    type_changes: usize,
    nullability_changes: usize,
) -> String {
    format!(
        "dest-postgres: schema drift detected: {new_columns} new, {removed_columns} removed, {type_changes} type changes, {nullability_changes} nullability changes"
    )
}

pub(crate) fn stale_watermark_resume_warning(stream_name: &str, committed_rows: u64) -> String {
    format!(
        "dest-postgres: ignoring stale watermark for stream '{stream_name}' ({committed_rows} committed records); row-count resume is disabled until checkpoint-safe recovery lands"
    )
}

pub(crate) fn checkpoint_safety_message(
    phase: CheckpointSafetyPhase,
    commit_state: CommitState,
) -> &'static str {
    match phase {
        CheckpointSafetyPhase::LoopFailureBeforeNewDurableCommit => match commit_state {
            CommitState::BeforeCommit => {
                "dest-postgres: write loop failed before any new durable commit; rollback leaves no durable checkpoint"
            }
            CommitState::AfterCommitConfirmed => {
                "dest-postgres: write loop failed after a confirmed checkpoint but before a new durable commit; resume from the last checkpoint"
            }
            CommitState::AfterCommitUnknown => {
                "dest-postgres: write loop failed after a commit with unknown durability; inspect the destination before resuming"
            }
        },
        CheckpointSafetyPhase::PreCommitFailureBeforePostgresCommit => match commit_state {
            CommitState::BeforeCommit => {
                "dest-postgres: pre-finalization failed before PostgreSQL COMMIT; no durable commit was made"
            }
            CommitState::AfterCommitConfirmed => {
                "dest-postgres: pre-finalization failed before PostgreSQL COMMIT after a confirmed checkpoint; the last durable checkpoint remains the recovery boundary"
            }
            CommitState::AfterCommitUnknown => {
                "dest-postgres: pre-finalization failed before PostgreSQL COMMIT after a commit with unknown durability; inspect the destination before resuming"
            }
        },
        CheckpointSafetyPhase::PostCommitSwapOrCheckpointFailure => {
            "dest-postgres: post-COMMIT swap/checkpoint failed after PostgreSQL commit; durable data may already exist, but the final checkpoint may not have advanced, so resuming naively can duplicate rows"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_drift_summary_mentions_all_categories() {
        let summary = schema_drift_summary(2, 1, 3, 4);

        assert!(summary.contains("2 new"));
        assert!(summary.contains("1 removed"));
        assert!(summary.contains("3 type changes"));
        assert!(summary.contains("4 nullability changes"));
    }

    #[test]
    fn checkpoint_safety_message_distinguishes_commit_states() {
        assert!(checkpoint_safety_message(
            CheckpointSafetyPhase::LoopFailureBeforeNewDurableCommit,
            CommitState::BeforeCommit,
        )
        .contains("before any new durable commit"));
        let confirmed = checkpoint_safety_message(
            CheckpointSafetyPhase::PostCommitSwapOrCheckpointFailure,
            CommitState::AfterCommitConfirmed,
        );
        assert!(confirmed.contains("checkpoint may not have advanced"));
        assert!(confirmed.contains("duplicate rows"));
        assert!(checkpoint_safety_message(
            CheckpointSafetyPhase::LoopFailureBeforeNewDurableCommit,
            CommitState::AfterCommitUnknown,
        )
        .contains("unknown"));
    }

    #[test]
    fn checkpoint_safety_message_distinguishes_failure_phases() {
        let loop_failure = checkpoint_safety_message(
            CheckpointSafetyPhase::LoopFailureBeforeNewDurableCommit,
            CommitState::AfterCommitConfirmed,
        );
        assert!(loop_failure.contains("write loop failed after a confirmed checkpoint"));
        assert!(!loop_failure.contains("final checkpoint failed after PostgreSQL commit"));

        let pre_commit = checkpoint_safety_message(
            CheckpointSafetyPhase::PreCommitFailureBeforePostgresCommit,
            CommitState::AfterCommitConfirmed,
        );
        assert!(pre_commit.contains("pre-finalization failed before PostgreSQL COMMIT"));
        assert!(pre_commit.contains("last durable checkpoint remains the recovery boundary"));
        assert!(!pre_commit.contains("durable data may already exist"));

        let post_commit = checkpoint_safety_message(
            CheckpointSafetyPhase::PostCommitSwapOrCheckpointFailure,
            CommitState::AfterCommitConfirmed,
        );
        assert!(post_commit.contains("post-COMMIT swap/checkpoint failed"));
        assert!(post_commit.contains("duplicate rows"));
    }

    #[test]
    fn stale_watermark_resume_warning_is_checkpoint_safe() {
        let message = stale_watermark_resume_warning("users", 17);

        assert!(message.contains("users"));
        assert!(message.contains("17"));
        assert!(message.contains("checkpoint-safe recovery"));
    }
}

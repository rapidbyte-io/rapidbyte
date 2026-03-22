//! Destination `PostgreSQL` operator-facing diagnostics.

use rapidbyte_sdk::prelude::CommitState;

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

pub(crate) fn checkpoint_safety_message(commit_state: CommitState) -> &'static str {
    match commit_state {
        CommitState::BeforeCommit => {
            "dest-postgres: write loop failed before any commit; rollback leaves no durable checkpoint"
        }
        CommitState::AfterCommitConfirmed => {
            "dest-postgres: write loop failed after a confirmed checkpoint; resume is safe from the last checkpoint"
        }
        CommitState::AfterCommitUnknown => {
            "dest-postgres: write loop failed after a commit with unknown durability; inspect the destination before resuming"
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
        assert!(checkpoint_safety_message(CommitState::BeforeCommit)
            .contains("before any commit"));
        assert!(checkpoint_safety_message(CommitState::AfterCommitConfirmed)
            .contains("confirmed checkpoint"));
        assert!(checkpoint_safety_message(CommitState::AfterCommitUnknown)
            .contains("unknown"));
    }

    #[test]
    fn stale_watermark_resume_warning_is_checkpoint_safe() {
        let message = stale_watermark_resume_warning("users", 17);

        assert!(message.contains("users"));
        assert!(message.contains("17"));
        assert!(message.contains("checkpoint-safe recovery"));
    }
}

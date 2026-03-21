//! Typed CDC diagnostics for operator-facing failure and recovery guidance.

use rapidbyte_sdk::cursor::CursorType;
use rapidbyte_sdk::stream::CdcResumeToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DiagnosticLevel {
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Diagnostic {
    pub level: DiagnosticLevel,
    pub message: String,
    pub fix_hint: Option<String>,
}

impl Diagnostic {
    fn warning(message: impl Into<String>, fix_hint: impl Into<String>) -> Self {
        Self {
            level: DiagnosticLevel::Warning,
            message: message.into(),
            fix_hint: Some(fix_hint.into()),
        }
    }

    fn error(message: impl Into<String>, fix_hint: impl Into<String>) -> Self {
        Self {
            level: DiagnosticLevel::Error,
            message: message.into(),
            fix_hint: Some(fix_hint.into()),
        }
    }

    pub(crate) fn render(&self) -> String {
        match &self.fix_hint {
            Some(hint) if !hint.is_empty() => format!("{} Fix: {}", self.message, hint),
            _ => self.message.clone(),
        }
    }
}

pub(crate) fn cdc_slot_mismatch_diagnostic(
    stream_name: &str,
    configured_slot: &str,
    expected_slot: &str,
) -> Diagnostic {
    Diagnostic::error(
        format!(
            "CDC slot mismatch for stream '{stream_name}': configured replication slot '{configured_slot}' does not match the expected slot '{expected_slot}'."
        ),
        "drop the stale slot and backfill the stream, or update the stream to use the matching replication slot.",
    )
}

pub(crate) fn cdc_publication_mismatch_diagnostic(
    stream_name: &str,
    configured_publication: &str,
    expected_publication: &str,
) -> Diagnostic {
    Diagnostic::error(
        format!(
            "CDC publication mismatch for stream '{stream_name}': configured publication '{configured_publication}' does not match the expected publication '{expected_publication}'."
        ),
        "Create the expected publication with CREATE PUBLICATION ... FOR TABLE ... or update the stream to use the matching publication.",
    )
}

pub(crate) fn cdc_checkpoint_failure_diagnostic(
    stream_name: &str,
    lsn: &str,
    err: &str,
) -> Diagnostic {
    Diagnostic::error(
        format!(
            "CDC checkpoint failed for stream '{stream_name}' at LSN '{lsn}'. WAL already consumed; the checkpoint could not be persisted. Original error: {err}"
        ),
        "backfill the stream or restore checkpoint state before retrying; destructive CDC reads cannot safely replay consumed WAL.",
    )
}

pub(crate) fn cdc_resume_ambiguity_diagnostic(
    stream_name: &str,
    resume: &CdcResumeToken,
) -> Option<Diagnostic> {
    match resume.cursor_type {
        CursorType::Lsn => match resume.value.as_deref() {
            None | Some("") => Some(Diagnostic::warning(
                format!(
                    "CDC resume token is missing for stream '{stream_name}'. Treating this as a fresh CDC start, which is ambiguous after destructive reads."
                ),
                "Verify the previous run checkpoint was persisted or backfill the stream if you need to recover from consumed WAL.",
            )),
            Some(_) => None,
        },
        other => Some(Diagnostic::error(
            format!(
                "CDC resume token for stream '{stream_name}' must be an LSN, got cursor type {other:?}."
            ),
            "Reset or backfill the stream so CDC can resume from a persisted LSN checkpoint.",
        )),
    }
}

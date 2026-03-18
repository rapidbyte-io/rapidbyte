//! Shared SQL query constants used by both async (sqlx) and sync (postgres)
//! implementations.
//!
//! Only queries that are **identical** between `cursor.rs` / `run_record.rs`
//! (async) and `state_backend.rs` (sync) live here. Queries that differ
//! (e.g., `SET_CURSOR` uses `now()` in async but a host-supplied timestamp
//! in sync, `COMPLETE_RUN` uses `finished_at = now()` vs
//! `to_char(now() AT TIME ZONE ...)`) stay inline with a comment explaining
//! the divergence.

/// Fetch the current cursor for a (pipeline, stream) pair.
///
/// Used identically in both async `CursorRepository::get` and
/// sync `StateBackend::get_cursor`.
pub(crate) const GET_CURSOR: &str = "\
    SELECT cursor_field, cursor_value, \
    to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') \
    FROM sync_cursors WHERE pipeline = $1 AND stream = $2";

/// Start a new sync run, returning the auto-generated id.
///
/// Used identically in both async `RunRecordRepository::start` and
/// sync `StateBackend::start_run`.
pub(crate) const START_RUN: &str = "\
    INSERT INTO sync_runs (pipeline, stream, status) \
    VALUES ($1, $2, $3) RETURNING id";

//! Shared SQL query constants used by both the async repository port
//! implementations and the `StateBackend` channel-based bridge.
//!
//! Only queries that are **identical** between `cursor.rs` / `run_record.rs`
//! (async) and `state_backend.rs` (sync bridge) live here. Queries that
//! differ stay inline with a comment explaining the divergence.

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

/// Upsert cursor for a (pipeline, stream) pair.
pub(crate) const SET_CURSOR: &str = "\
    INSERT INTO sync_cursors (pipeline, stream, cursor_field, cursor_value, updated_at) \
    VALUES ($1, $2, $3, $4, now()) \
    ON CONFLICT (pipeline, stream) \
    DO UPDATE SET cursor_field = $3, cursor_value = $4, updated_at = now()";

/// Finalize a sync run with status and aggregate stats.
pub(crate) const COMPLETE_RUN: &str = "\
    UPDATE sync_runs SET status = $1, finished_at = now(), \
    records_read = $2, records_written = $3, \
    bytes_read = $4, bytes_written = $5, error_message = $6 \
    WHERE id = $7";

/// Insert a single DLQ record.
pub(crate) const INSERT_DLQ_RECORD: &str = "\
    INSERT INTO dlq_records \
    (pipeline, run_id, stream_name, record_json, \
     error_message, error_category, failed_at) \
    VALUES ($1, $2, $3, $4, $5, $6, $7::timestamptz)";

/// Insert-if-absent cursor (for compare-and-set with expected=None).
pub(crate) const CAS_INSERT_CURSOR: &str = "\
    INSERT INTO sync_cursors (pipeline, stream, cursor_value, updated_at) \
    VALUES ($1, $2, $3, now()) ON CONFLICT DO NOTHING";

/// Conditional cursor update (for compare-and-set with expected=Some).
pub(crate) const CAS_UPDATE_CURSOR: &str = "\
    UPDATE sync_cursors SET cursor_value = $1, updated_at = now() \
    WHERE pipeline = $2 AND stream = $3 AND cursor_value = $4";

//! Checkpoint correlation and cursor advancement logic.

use anyhow::Result;
use rapidbyte_types::protocol::{Checkpoint, CursorValue};

use crate::state::backend::{CursorState, PipelineId, StateBackend, StreamName};

/// Correlate source and destination checkpoints, persisting cursor state only when
/// both sides confirm the data for a stream. Returns the number of cursors advanced.
pub(crate) fn correlate_and_persist_cursors(
    state_backend: &dyn StateBackend,
    pipeline: &PipelineId,
    source_checkpoints: &[Checkpoint],
    dest_checkpoints: &[Checkpoint],
) -> Result<u64> {
    let mut cursors_advanced = 0u64;

    for src_cp in source_checkpoints {
        let (cursor_field, cursor_value) = match (&src_cp.cursor_field, &src_cp.cursor_value) {
            (Some(f), Some(v)) => (f, v),
            _ => continue,
        };

        let dest_confirmed = dest_checkpoints
            .iter()
            .any(|dcp| dcp.stream == src_cp.stream);
        if !dest_confirmed {
            tracing::warn!(
                pipeline = pipeline.as_str(),
                stream = src_cp.stream,
                "Skipping cursor advancement: no destination checkpoint confirms stream data"
            );
            continue;
        }

        let value_str = match cursor_value {
            CursorValue::Utf8(s) => s.clone(),
            CursorValue::Int64(n) => n.to_string(),
            CursorValue::TimestampMillis(ms) => ms.to_string(),
            CursorValue::TimestampMicros(us) => us.to_string(),
            CursorValue::Decimal { value, .. } => value.clone(),
            CursorValue::Json(v) => v.to_string(),
            CursorValue::Lsn(s) => s.clone(),
            CursorValue::Null => continue,
        };

        let cursor = CursorState {
            cursor_field: Some(cursor_field.clone()),
            cursor_value: Some(value_str.clone()),
            updated_at: chrono::Utc::now(),
        };
        state_backend.set_cursor(pipeline, &StreamName(src_cp.stream.clone()), &cursor)?;
        tracing::info!(
            pipeline = pipeline.as_str(),
            stream = src_cp.stream,
            cursor_field = cursor_field,
            cursor_value = value_str,
            "Cursor advanced: source + destination checkpoints correlated"
        );
        cursors_advanced += 1;
    }

    Ok(cursors_advanced)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::sqlite::SqliteStateBackend;
    use rapidbyte_types::protocol::{CheckpointKind, ProtocolVersion};

    fn make_source_checkpoint(stream: &str, cursor_field: &str, cursor_value: &str) -> Checkpoint {
        Checkpoint {
            id: 1,
            kind: CheckpointKind::Source,
            stream: stream.to_string(),
            cursor_field: Some(cursor_field.to_string()),
            cursor_value: Some(CursorValue::Utf8(cursor_value.to_string())),
            records_processed: 100,
            bytes_processed: 5000,
        }
    }

    fn make_dest_checkpoint(stream: &str) -> Checkpoint {
        Checkpoint {
            id: 1,
            kind: CheckpointKind::Dest,
            stream: stream.to_string(),
            cursor_field: None,
            cursor_value: None,
            records_processed: 100,
            bytes_processed: 5000,
        }
    }

    fn pid() -> PipelineId {
        PipelineId("test_pipe".to_string())
    }

    #[test]
    fn test_correlate_both_checkpoints_advances_cursor() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let src = vec![make_source_checkpoint("users", "id", "42")];
        let dst = vec![make_dest_checkpoint("users")];

        let advanced = correlate_and_persist_cursors(&backend, &pid(), &src, &dst).unwrap();
        assert_eq!(advanced, 1);

        let cursor = backend
            .get_cursor(&pid(), &StreamName("users".to_string()))
            .unwrap()
            .unwrap();
        assert_eq!(cursor.cursor_value, Some("42".to_string()));
        assert_eq!(cursor.cursor_field, Some("id".to_string()));
    }

    #[test]
    fn test_correlate_no_dest_checkpoint_does_not_advance() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let src = vec![make_source_checkpoint("users", "id", "42")];
        let dst: Vec<Checkpoint> = vec![];

        let advanced = correlate_and_persist_cursors(&backend, &pid(), &src, &dst).unwrap();
        assert_eq!(advanced, 0);

        let cursor = backend
            .get_cursor(&pid(), &StreamName("users".to_string()))
            .unwrap();
        assert!(cursor.is_none());
    }

    #[test]
    fn test_correlate_partial_dest_advances_only_confirmed() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let src = vec![
            make_source_checkpoint("users", "id", "42"),
            make_source_checkpoint("orders", "order_id", "99"),
        ];
        let dst = vec![make_dest_checkpoint("users")];

        let advanced = correlate_and_persist_cursors(&backend, &pid(), &src, &dst).unwrap();
        assert_eq!(advanced, 1);

        let users = backend
            .get_cursor(&pid(), &StreamName("users".to_string()))
            .unwrap()
            .unwrap();
        assert_eq!(users.cursor_value, Some("42".to_string()));

        let orders = backend
            .get_cursor(&pid(), &StreamName("orders".to_string()))
            .unwrap();
        assert!(orders.is_none());
    }

    #[test]
    fn test_checkpoint_envelope_roundtrip_via_host_parsing() {
        use rapidbyte_types::protocol::PayloadEnvelope;

        let source_cp = Checkpoint {
            id: 1,
            kind: CheckpointKind::Source,
            stream: "users".to_string(),
            cursor_field: Some("id".to_string()),
            cursor_value: Some(CursorValue::Int64(42)),
            records_processed: 100,
            bytes_processed: 5000,
        };
        let source_env = PayloadEnvelope {
            protocol_version: ProtocolVersion::V2,
            connector_id: "source-postgres".to_string(),
            stream_name: "users".to_string(),
            payload: source_cp,
        };
        let src_value: serde_json::Value =
            serde_json::from_slice(&serde_json::to_vec(&source_env).unwrap()).unwrap();
        let parsed_source: Checkpoint = serde_json::from_value(
            src_value
                .get("payload")
                .cloned()
                .unwrap_or(src_value.clone()),
        )
        .unwrap();

        let dest_cp = Checkpoint {
            id: 1,
            kind: CheckpointKind::Dest,
            stream: "users".to_string(),
            cursor_field: None,
            cursor_value: None,
            records_processed: 100,
            bytes_processed: 5000,
        };
        let dest_env = PayloadEnvelope {
            protocol_version: ProtocolVersion::V2,
            connector_id: "dest-postgres".to_string(),
            stream_name: "users".to_string(),
            payload: dest_cp,
        };
        let dst_value: serde_json::Value =
            serde_json::from_slice(&serde_json::to_vec(&dest_env).unwrap()).unwrap();
        let parsed_dest: Checkpoint = serde_json::from_value(
            dst_value
                .get("payload")
                .cloned()
                .unwrap_or(dst_value.clone()),
        )
        .unwrap();

        let backend = SqliteStateBackend::in_memory().unwrap();
        let advanced =
            correlate_and_persist_cursors(&backend, &pid(), &[parsed_source], &[parsed_dest])
                .unwrap();
        assert_eq!(advanced, 1);
    }
}

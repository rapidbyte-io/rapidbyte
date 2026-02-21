use anyhow::Result;
use rapidbyte_sdk::protocol::{Checkpoint, CursorValue};

use crate::state::backend::{CursorState, StateBackend};

/// Correlate source and dest checkpoints, persisting cursor state only when
/// both sides confirm the data for a stream. Returns the number of cursors advanced.
pub(crate) fn correlate_and_persist_cursors(
    state_backend: &dyn StateBackend,
    pipeline: &str,
    source_checkpoints: &[Checkpoint],
    dest_checkpoints: &[Checkpoint],
) -> Result<u64> {
    let mut cursors_advanced = 0u64;

    for src_cp in source_checkpoints {
        let (cursor_field, cursor_value) = match (&src_cp.cursor_field, &src_cp.cursor_value) {
            (Some(f), Some(v)) => (f, v),
            _ => continue,
        };

        // Check if we have a dest checkpoint confirming this stream's data
        let dest_confirmed = dest_checkpoints
            .iter()
            .any(|dcp| dcp.stream == src_cp.stream);

        if !dest_confirmed {
            tracing::warn!(
                pipeline,
                stream = src_cp.stream,
                "Skipping cursor advancement: no dest checkpoint confirms stream data"
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
        state_backend.set_cursor(pipeline, &src_cp.stream, &cursor)?;
        tracing::info!(
            pipeline,
            stream = src_cp.stream,
            cursor_field = cursor_field,
            cursor_value = value_str,
            "Cursor advanced: source + dest checkpoints correlated"
        );
        cursors_advanced += 1;
    }

    Ok(cursors_advanced)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::sqlite::SqliteStateBackend;
    use rapidbyte_sdk::protocol::CheckpointKind;

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

    #[test]
    fn test_correlate_both_checkpoints_advances_cursor() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let src = vec![make_source_checkpoint("users", "id", "42")];
        let dst = vec![make_dest_checkpoint("users")];

        let advanced = correlate_and_persist_cursors(&backend, "test_pipe", &src, &dst).unwrap();
        assert_eq!(advanced, 1);

        let cursor = backend.get_cursor("test_pipe", "users").unwrap().unwrap();
        assert_eq!(cursor.cursor_value, Some("42".to_string()));
        assert_eq!(cursor.cursor_field, Some("id".to_string()));
    }

    #[test]
    fn test_correlate_no_dest_checkpoint_does_not_advance() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let src = vec![make_source_checkpoint("users", "id", "42")];
        let dst: Vec<Checkpoint> = vec![];

        let advanced = correlate_and_persist_cursors(&backend, "test_pipe", &src, &dst).unwrap();
        assert_eq!(advanced, 0);

        let cursor = backend.get_cursor("test_pipe", "users").unwrap();
        assert!(cursor.is_none());
    }

    #[test]
    fn test_correlate_dest_for_different_stream_does_not_advance() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let src = vec![make_source_checkpoint("users", "id", "42")];
        let dst = vec![make_dest_checkpoint("orders")];

        let advanced = correlate_and_persist_cursors(&backend, "test_pipe", &src, &dst).unwrap();
        assert_eq!(advanced, 0);

        let cursor = backend.get_cursor("test_pipe", "users").unwrap();
        assert!(cursor.is_none());
    }

    #[test]
    fn test_correlate_multiple_streams() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let src = vec![
            make_source_checkpoint("users", "id", "42"),
            make_source_checkpoint("orders", "order_id", "99"),
        ];
        let dst = vec![
            make_dest_checkpoint("users"),
            make_dest_checkpoint("orders"),
        ];

        let advanced = correlate_and_persist_cursors(&backend, "test_pipe", &src, &dst).unwrap();
        assert_eq!(advanced, 2);

        let u = backend.get_cursor("test_pipe", "users").unwrap().unwrap();
        assert_eq!(u.cursor_value, Some("42".to_string()));

        let o = backend.get_cursor("test_pipe", "orders").unwrap().unwrap();
        assert_eq!(o.cursor_value, Some("99".to_string()));
    }

    #[test]
    fn test_correlate_partial_dest_advances_only_confirmed() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let src = vec![
            make_source_checkpoint("users", "id", "42"),
            make_source_checkpoint("orders", "order_id", "99"),
        ];
        let dst = vec![make_dest_checkpoint("users")];

        let advanced = correlate_and_persist_cursors(&backend, "test_pipe", &src, &dst).unwrap();
        assert_eq!(advanced, 1);

        let u = backend.get_cursor("test_pipe", "users").unwrap().unwrap();
        assert_eq!(u.cursor_value, Some("42".to_string()));

        let o = backend.get_cursor("test_pipe", "orders").unwrap();
        assert!(o.is_none());
    }

    #[test]
    fn test_correlate_source_without_cursor_value_skipped() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let src = vec![Checkpoint {
            id: 1,
            kind: CheckpointKind::Source,
            stream: "users".to_string(),
            cursor_field: None,
            cursor_value: None,
            records_processed: 100,
            bytes_processed: 5000,
        }];
        let dst = vec![make_dest_checkpoint("users")];

        let advanced = correlate_and_persist_cursors(&backend, "test_pipe", &src, &dst).unwrap();
        assert_eq!(advanced, 0);
    }

    #[test]
    fn test_checkpoint_envelope_roundtrip_via_host_parsing() {
        use rapidbyte_sdk::protocol::{CursorValue, PayloadEnvelope};

        // Simulate what the source connector does in host_ffi::checkpoint()
        let source_cp = Checkpoint {
            id: 1,
            kind: CheckpointKind::Source,
            stream: "users".to_string(),
            cursor_field: Some("id".to_string()),
            cursor_value: Some(CursorValue::Int64(42)),
            records_processed: 100,
            bytes_processed: 5000,
        };
        let envelope = PayloadEnvelope {
            protocol_version: "2".to_string(),
            connector_id: "source-postgres".to_string(),
            stream_name: "users".to_string(),
            payload: source_cp.clone(),
        };
        let bytes = serde_json::to_vec(&envelope).unwrap();

        // Simulate what host_checkpoint() does: parse as Value, then extract
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        // Debug: print the serialized envelope to see structure
        eprintln!(
            "Serialized envelope: {}",
            serde_json::to_string_pretty(&value).unwrap()
        );

        // This is the same parsing logic used by the component host runtime.
        let checkpoint_value = value.get("payload").cloned().unwrap_or(value.clone());
        let parsed: Checkpoint = serde_json::from_value(checkpoint_value).unwrap();

        assert_eq!(parsed.stream, "users");
        assert_eq!(parsed.cursor_field, Some("id".to_string()));
        assert_eq!(parsed.cursor_value, Some(CursorValue::Int64(42)));
        assert_eq!(parsed.records_processed, 100);

        // Now do the same for a dest checkpoint
        let dest_cp = Checkpoint {
            id: 1,
            kind: CheckpointKind::Dest,
            stream: "users".to_string(),
            cursor_field: None,
            cursor_value: None,
            records_processed: 100,
            bytes_processed: 5000,
        };
        let dest_envelope = PayloadEnvelope {
            protocol_version: "2".to_string(),
            connector_id: "dest-postgres".to_string(),
            stream_name: "users".to_string(),
            payload: dest_cp.clone(),
        };
        let dest_bytes = serde_json::to_vec(&dest_envelope).unwrap();
        let dest_value: serde_json::Value = serde_json::from_slice(&dest_bytes).unwrap();
        eprintln!(
            "Dest envelope: {}",
            serde_json::to_string_pretty(&dest_value).unwrap()
        );
        let dest_checkpoint_value = dest_value
            .get("payload")
            .cloned()
            .unwrap_or(dest_value.clone());
        let dest_parsed: Checkpoint = serde_json::from_value(dest_checkpoint_value).unwrap();

        assert_eq!(dest_parsed.stream, "users");
        assert_eq!(dest_parsed.kind, CheckpointKind::Dest);

        // Finally, test full correlation -> SQLite persistence
        let backend = SqliteStateBackend::in_memory().unwrap();
        let advanced =
            correlate_and_persist_cursors(&backend, "test_pipe", &[parsed], &[dest_parsed])
                .unwrap();
        assert_eq!(advanced, 1);

        let cursor = backend.get_cursor("test_pipe", "users").unwrap().unwrap();
        assert_eq!(cursor.cursor_value, Some("42".to_string()));
        assert_eq!(cursor.cursor_field, Some("id".to_string()));
    }
}

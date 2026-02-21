//! Wire protocol types shared between connectors and host runtime.

mod checkpoint;
mod cursor;
mod envelope;
mod stream;
mod summary;
mod wire;

pub use checkpoint::*;
pub use cursor::*;
pub use envelope::*;
pub use stream::*;
pub use summary::*;
pub use wire::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::ErrorCategory;

    #[test]
    fn test_sync_mode_cdc_roundtrip() {
        let mode = SyncMode::Cdc;
        let json = serde_json::to_string(&mode).expect("serialize");
        assert_eq!(json, "\"cdc\"");
        let back: SyncMode = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(mode, back);
    }

    #[test]
    fn test_protocol_version_roundtrip() {
        let v = ProtocolVersion::V2;
        let json = serde_json::to_string(&v).expect("serialize");
        assert_eq!(json, "\"2\"");
        let back: ProtocolVersion = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, ProtocolVersion::V2);
    }

    #[test]
    fn test_catalog_roundtrip() {
        let catalog = Catalog {
            streams: vec![Stream {
                name: "users".to_string(),
                schema: vec![ColumnSchema {
                    name: "id".to_string(),
                    data_type: ArrowDataType::Int64,
                    nullable: false,
                }],
                supported_sync_modes: vec![SyncMode::FullRefresh],
                source_defined_cursor: None,
                source_defined_primary_key: Some(vec!["id".to_string()]),
            }],
        };
        let json = serde_json::to_string(&catalog).expect("serialize");
        let back: Catalog = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(catalog, back);
    }

    #[test]
    fn test_write_mode_upsert_roundtrip() {
        let mode = WriteMode::Upsert {
            primary_key: vec!["id".to_string()],
        };
        let json = serde_json::to_string(&mode).expect("serialize");
        let back: WriteMode = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(mode, back);
    }

    #[test]
    fn test_feature_roundtrip() {
        let f = Feature::BulkLoadCopy;
        let json = serde_json::to_string(&f).expect("serialize");
        assert_eq!(json, "\"bulk_load_copy\"");
        let back: Feature = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(f, back);
    }

    #[test]
    fn test_connector_info_roundtrip() {
        let info = ConnectorInfo {
            protocol_version: ProtocolVersion::V2,
            features: vec![Feature::ExactlyOnce, Feature::BulkLoadCopy],
            default_max_batch_bytes: 64 * 1024 * 1024,
        };
        let json = serde_json::to_string(&info).expect("serialize");
        let back: ConnectorInfo = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(info, back);
    }

    #[test]
    fn test_schema_hint_columns_roundtrip() {
        let hint = SchemaHint::Columns(vec![ColumnSchema {
            name: "id".to_string(),
            data_type: ArrowDataType::Int64,
            nullable: false,
        }]);
        let json = serde_json::to_string(&hint).expect("serialize");
        let back: SchemaHint = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(hint, back);
    }

    #[test]
    fn test_cursor_value_roundtrip() {
        let vals = vec![
            CursorValue::Null,
            CursorValue::Int64(42),
            CursorValue::Utf8("abc".to_string()),
            CursorValue::TimestampMillis(1_700_000_000_000),
            CursorValue::TimestampMicros(1_700_000_000_000_000),
            CursorValue::Decimal {
                value: "123.45".to_string(),
                scale: 2,
            },
            CursorValue::Json(serde_json::json!({"key": "val"})),
        ];
        for v in vals {
            let json = serde_json::to_string(&v).expect("serialize");
            let back: CursorValue = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(v, back);
        }
    }

    #[test]
    fn test_stream_limits_default() {
        let limits = StreamLimits::default();
        assert_eq!(limits.max_batch_bytes, DEFAULT_MAX_BATCH_BYTES);
        assert_eq!(limits.max_record_bytes, DEFAULT_MAX_RECORD_BYTES);
        assert_eq!(limits.max_inflight_batches, DEFAULT_MAX_INFLIGHT_BATCHES);
        assert_eq!(limits.max_parallel_requests, DEFAULT_MAX_PARALLEL_REQUESTS);
        assert_eq!(
            limits.checkpoint_interval_bytes,
            DEFAULT_CHECKPOINT_INTERVAL_BYTES
        );
    }

    #[test]
    fn test_stream_policies_default() {
        let policies = StreamPolicies::default();
        assert_eq!(policies.on_data_error, DataErrorPolicy::Fail);
        assert_eq!(policies.schema_evolution.new_column, ColumnPolicy::Add);
        assert_eq!(
            policies.schema_evolution.removed_column,
            ColumnPolicy::Ignore
        );
        assert_eq!(
            policies.schema_evolution.type_change,
            TypeChangePolicy::Fail
        );
        assert_eq!(
            policies.schema_evolution.nullability_change,
            NullabilityPolicy::Allow
        );
    }

    #[test]
    fn test_stream_context_roundtrip() {
        let ctx = StreamContext {
            stream_name: "orders".to_string(),
            schema: SchemaHint::Columns(vec![ColumnSchema {
                name: "id".to_string(),
                data_type: ArrowDataType::Int64,
                nullable: false,
            }]),
            sync_mode: SyncMode::Incremental,
            cursor_info: Some(CursorInfo {
                cursor_field: "updated_at".to_string(),
                cursor_type: CursorType::TimestampMicros,
                last_value: Some(CursorValue::TimestampMicros(1_700_000_000_000_000)),
            }),
            limits: StreamLimits::default(),
            policies: StreamPolicies::default(),
            write_mode: Some(WriteMode::Append),
            selected_columns: None,
        };
        let json = serde_json::to_string(&ctx).expect("serialize");
        let back: StreamContext = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(ctx, back);
    }

    #[test]
    fn test_stream_context_with_selected_columns_roundtrip() {
        let ctx = StreamContext {
            stream_name: "users".to_string(),
            schema: SchemaHint::Columns(vec![]),
            sync_mode: SyncMode::FullRefresh,
            cursor_info: None,
            limits: StreamLimits::default(),
            policies: StreamPolicies::default(),
            write_mode: Some(WriteMode::Append),
            selected_columns: Some(vec!["id".into(), "name".into()]),
        };
        let json = serde_json::to_string(&ctx).expect("serialize");
        let back: StreamContext = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(ctx, back);
    }

    #[test]
    fn test_stream_context_backwards_compat_no_selected_columns() {
        let json = r#"{"stream_name":"users","schema":{"columns":[]},"sync_mode":"full_refresh","cursor_info":null,"limits":{"max_batch_bytes":67108864,"max_record_bytes":16777216,"max_inflight_batches":16,"max_parallel_requests":1,"checkpoint_interval_bytes":67108864,"checkpoint_interval_rows":0,"checkpoint_interval_seconds":0},"policies":{"on_data_error":"fail","schema_evolution":{"new_column":"add","removed_column":"ignore","type_change":"fail","nullability_change":"allow"}},"write_mode":"append"}"#;
        let ctx: StreamContext = serde_json::from_str(json).expect("deserialize");
        assert!(ctx.selected_columns.is_none());
    }

    #[test]
    fn test_checkpoint_roundtrip() {
        let cp = Checkpoint {
            id: 1,
            kind: CheckpointKind::Source,
            stream: "users".to_string(),
            cursor_field: Some("id".to_string()),
            cursor_value: Some(CursorValue::Int64(100)),
            records_processed: 500,
            bytes_processed: 65_536,
        };
        let json = serde_json::to_string(&cp).expect("serialize");
        let back: Checkpoint = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(cp, back);
    }

    #[test]
    fn test_state_scope_conversion() {
        assert_eq!(StateScope::try_from(0), Ok(StateScope::Pipeline));
        assert_eq!(StateScope::try_from(1), Ok(StateScope::Stream));
        assert_eq!(StateScope::try_from(2), Ok(StateScope::ConnectorInstance));
        assert_eq!(StateScope::try_from(99), Err(99));
        assert_eq!(StateScope::Pipeline.to_i32(), 0);
        assert_eq!(StateScope::Stream.to_i32(), 1);
        assert_eq!(StateScope::ConnectorInstance.to_i32(), 2);
    }

    #[test]
    fn test_metric_roundtrip() {
        let m = Metric {
            name: "rows_read".to_string(),
            value: MetricValue::Counter(42),
            labels: vec![("stream".to_string(), "users".to_string())],
        };
        let json = serde_json::to_string(&m).expect("serialize");
        let back: Metric = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(m, back);
    }

    #[test]
    fn test_read_summary_roundtrip() {
        let s = ReadSummary {
            records_read: 1000,
            bytes_read: 65_536,
            batches_emitted: 10,
            checkpoint_count: 2,
            records_skipped: 0,
            perf: None,
        };
        let json = serde_json::to_string(&s).expect("serialize");
        let back: ReadSummary = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(s, back);
    }

    #[test]
    fn test_write_summary_roundtrip() {
        let s = WriteSummary {
            records_written: 1000,
            bytes_written: 65_536,
            batches_written: 10,
            checkpoint_count: 2,
            records_failed: 0,
            perf: Some(WritePerf {
                connect_secs: 0.1,
                flush_secs: 1.5,
                commit_secs: 0.05,
                arrow_decode_secs: 0.12,
            }),
        };
        let json = serde_json::to_string(&s).expect("serialize");
        let back: WriteSummary = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(s, back);
    }

    #[test]
    fn test_dlq_record_roundtrip() {
        let record = DlqRecord {
            stream_name: "users".to_string(),
            record_json: "{\"id\":1}".to_string(),
            error_message: "bad record".to_string(),
            error_category: ErrorCategory::Data,
            failed_at: Iso8601Timestamp("2026-02-21T00:00:00Z".to_string()),
        };
        let json = serde_json::to_string(&record).expect("serialize");
        let back: DlqRecord = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(record, back);
    }

    #[test]
    fn test_payload_envelope_roundtrip() {
        let envelope = PayloadEnvelope {
            protocol_version: ProtocolVersion::V2,
            connector_id: "source-postgres".to_string(),
            stream_name: "users".to_string(),
            payload: ReadSummary {
                records_read: 100,
                bytes_read: 4096,
                batches_emitted: 1,
                checkpoint_count: 1,
                records_skipped: 0,
                perf: None,
            },
        };
        let json = serde_json::to_string(&envelope).expect("serialize");
        // Verify flatten works - fields should be at top level.
        let v: serde_json::Value = serde_json::from_str(&json).expect("parse");
        assert!(v.get("protocol_version").is_some());
        assert!(v.get("records_read").is_some());
        let back: PayloadEnvelope<ReadSummary> = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(envelope, back);
    }
}

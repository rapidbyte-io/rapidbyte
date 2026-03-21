use std::collections::HashSet;
use std::sync::Arc;

use rapidbyte_sdk::arrow::datatypes::Schema;
use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::schema::StreamSchema;
use rapidbyte_sdk::stream::SchemaEvolutionPolicy;

use crate::config::LoadMethod;
use crate::decode;

/// Immutable setup output for destination worker execution.
#[derive(Debug, Clone)]
pub struct WriteContract {
    pub target_schema: String,
    pub stream_name: String,
    pub effective_stream: String,
    pub qualified_table: String,
    pub effective_write_mode: Option<WriteMode>,
    pub schema_policy: SchemaEvolutionPolicy,
    pub needs_schema_ensure: bool,
    pub use_watermarks: bool,
    pub checkpoint: CheckpointConfig,
    pub copy_flush_bytes: Option<usize>,
    pub load_method: LoadMethod,
    pub is_replace: bool,
    pub watermark_records: u64,
    pub ignored_columns: HashSet<String>,
    pub type_null_columns: HashSet<String>,
}

/// Checkpoint threshold configuration extracted from `StreamLimits`.
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    pub bytes: u64,
    pub rows: u64,
    pub seconds: u64,
}

pub(crate) fn preflight_schema_from_stream_schema(
    schema: &StreamSchema,
) -> Result<Option<Arc<Schema>>, String> {
    use rapidbyte_sdk::arrow::datatypes::Field;

    if schema.fields.is_empty() {
        return Ok(None);
    }
    let fields: Result<Vec<Field>, String> = schema
        .fields
        .iter()
        .map(|f| {
            let dt = parse_arrow_type(&f.arrow_type).ok_or_else(|| {
                format!(
                    "unsupported schema type '{}' for field '{}'",
                    f.arrow_type, f.name
                )
            })?;
            Ok(Field::new(&f.name, dt, f.nullable))
        })
        .collect();
    Ok(Some(Arc::new(Schema::new(fields?))))
}

fn parse_arrow_type(s: &str) -> Option<rapidbyte_sdk::arrow::datatypes::DataType> {
    use rapidbyte_sdk::arrow::datatypes::{DataType, TimeUnit};
    use rapidbyte_sdk::arrow_types::ArrowDataType;

    Some(match ArrowDataType::from_schema_name(s)? {
        ArrowDataType::Boolean => DataType::Boolean,
        ArrowDataType::Int8 => DataType::Int8,
        ArrowDataType::Int16 => DataType::Int16,
        ArrowDataType::Int32 => DataType::Int32,
        ArrowDataType::Int64 => DataType::Int64,
        ArrowDataType::UInt8 => DataType::UInt8,
        ArrowDataType::UInt16 => DataType::UInt16,
        ArrowDataType::UInt32 => DataType::UInt32,
        ArrowDataType::UInt64 => DataType::UInt64,
        ArrowDataType::Float16 => DataType::Float16,
        ArrowDataType::Float32 => DataType::Float32,
        ArrowDataType::Float64 => DataType::Float64,
        ArrowDataType::Utf8 => DataType::Utf8,
        ArrowDataType::LargeUtf8 => DataType::LargeUtf8,
        ArrowDataType::Binary => DataType::Binary,
        ArrowDataType::LargeBinary => DataType::LargeBinary,
        ArrowDataType::Date32 => DataType::Date32,
        ArrowDataType::Date64 => DataType::Date64,
        ArrowDataType::TimestampMillis => {
            DataType::Timestamp(TimeUnit::Millisecond, None)
        }
        ArrowDataType::TimestampMicros => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        ArrowDataType::TimestampNanos => {
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        }
        ArrowDataType::Decimal128 => DataType::Decimal128(38, 9),
        ArrowDataType::Json => DataType::Utf8,
        _ => return None,
    })
}

pub(crate) fn schema_hint_has_shape(schema: &StreamSchema) -> bool {
    !schema.fields.is_empty()
}

/// Build a destination write contract for a stream.
pub(crate) fn prepare_stream_once(
    target_schema: &str,
    stream_name: &str,
    write_mode: Option<WriteMode>,
    _schema: &StreamSchema,
    use_watermarks: bool,
    schema_policy: SchemaEvolutionPolicy,
    checkpoint: CheckpointConfig,
    copy_flush_bytes: Option<usize>,
    load_method: LoadMethod,
) -> Result<WriteContract, String> {
    if stream_name.trim().is_empty() {
        return Err("stream name must not be empty".to_string());
    }

    let is_replace = matches!(write_mode, Some(WriteMode::Replace));
    let effective_write_mode = if is_replace {
        Some(WriteMode::Append)
    } else {
        write_mode
    };

    Ok(WriteContract {
        target_schema: target_schema.to_string(),
        stream_name: stream_name.to_string(),
        effective_stream: stream_name.to_string(),
        qualified_table: decode::qualified_name(target_schema, stream_name),
        effective_write_mode,
        schema_policy,
        needs_schema_ensure: true,
        use_watermarks,
        checkpoint,
        copy_flush_bytes,
        load_method,
        is_replace,
        watermark_records: 0,
        ignored_columns: HashSet::new(),
        type_null_columns: HashSet::new(),
    })
}

pub(crate) fn mark_contract_prepared(mut contract: WriteContract) -> WriteContract {
    contract.needs_schema_ensure = false;
    contract
}

#[cfg(test)]
mod tests {
    use rapidbyte_sdk::schema::{SchemaField, StreamSchema};

    use super::*;

    fn empty_stream_schema() -> StreamSchema {
        StreamSchema::default()
    }

    #[test]
    fn write_contract_clone_preserves_fields() {
        let contract = WriteContract {
            target_schema: "raw".to_string(),
            stream_name: "users".to_string(),
            effective_stream: "users".to_string(),
            qualified_table: "raw.users".to_string(),
            effective_write_mode: Some(WriteMode::Append),
            schema_policy: SchemaEvolutionPolicy::default(),
            needs_schema_ensure: true,
            use_watermarks: true,
            checkpoint: CheckpointConfig {
                bytes: 1024,
                rows: 100,
                seconds: 30,
            },
            copy_flush_bytes: Some(4 * 1024 * 1024),
            load_method: LoadMethod::Copy,
            is_replace: false,
            watermark_records: 0,
            ignored_columns: std::collections::HashSet::new(),
            type_null_columns: std::collections::HashSet::new(),
        };

        let cloned = contract.clone();
        assert_eq!(cloned.stream_name, "users");
        assert_eq!(cloned.qualified_table, "raw.users");
        assert_eq!(cloned.copy_flush_bytes, Some(4 * 1024 * 1024));
    }

    #[test]
    fn prepare_stream_once_requires_non_empty_stream_name() {
        let result = prepare_stream_once(
            "raw",
            "",
            Some(WriteMode::Append),
            &empty_stream_schema(),
            true,
            SchemaEvolutionPolicy::default(),
            CheckpointConfig {
                bytes: 0,
                rows: 0,
                seconds: 0,
            },
            None,
            LoadMethod::Copy,
        );

        assert!(result.is_err());
        assert!(result
            .expect_err("empty stream must fail")
            .contains("stream name"));
    }

    #[test]
    fn prepare_stream_once_disables_watermarks_for_partitioned_writes() {
        let contract = prepare_stream_once(
            "raw",
            "users",
            Some(WriteMode::Append),
            &empty_stream_schema(),
            false,
            SchemaEvolutionPolicy::default(),
            CheckpointConfig {
                bytes: 0,
                rows: 0,
                seconds: 0,
            },
            None,
            LoadMethod::Insert,
        )
        .expect("contract should build");

        assert!(!contract.use_watermarks);
    }

    #[test]
    fn preflight_schema_from_stream_schema_builds_arrow_schema() {
        let schema = StreamSchema {
            fields: vec![
                SchemaField::new("id", "int64", false),
                SchemaField::new("name", "utf8", true),
            ],
            primary_key: vec![],
            partition_keys: vec![],
            source_defined_cursor: None,
            schema_id: None,
        };

        let arrow = preflight_schema_from_stream_schema(&schema)
            .expect("schema parse should succeed")
            .expect("schema should be built");
        assert_eq!(arrow.fields().len(), 2);
        assert_eq!(arrow.field(0).name(), "id");
        assert_eq!(arrow.field(1).name(), "name");
    }

    #[test]
    fn preflight_schema_from_empty_stream_schema_returns_none() {
        let schema = empty_stream_schema();
        assert!(preflight_schema_from_stream_schema(&schema)
            .expect("empty schema should parse")
            .is_none());
    }

    #[test]
    fn preflight_schema_from_stream_schema_rejects_unknown_type() {
        let schema = StreamSchema {
            fields: vec![SchemaField::new("id", "made_up_type", false)],
            primary_key: vec![],
            partition_keys: vec![],
            source_defined_cursor: None,
            schema_id: None,
        };

        let err = preflight_schema_from_stream_schema(&schema)
            .expect_err("unknown type should fail");
        assert!(err.contains("made_up_type"));
    }
}

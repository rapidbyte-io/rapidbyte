use std::collections::{BTreeMap, HashMap};

use ::arrow::array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeStringArray, RecordBatch, StringArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use ::arrow::datatypes::DataType;
use ::arrow::util::display::array_value_to_string;
use bigdecimal::{num_bigint::BigInt, BigDecimal};
use rapidbyte_sdk::prelude::*;

use crate::config::{CompiledConfig, CompiledRule, NumericBound};

#[derive(Debug, Clone)]
struct RuleFailure {
    rule: &'static str,
    field: String,
    message: String,
}

#[derive(Debug, Clone)]
pub(crate) struct InvalidRow {
    pub(crate) row_index: usize,
    pub(crate) message: String,
}

#[derive(Debug, Clone)]
pub(crate) struct BatchEvaluation {
    pub(crate) valid_indices: Vec<u32>,
    pub(crate) invalid_rows: Vec<InvalidRow>,
    pub(crate) failure_counts: BTreeMap<(String, String), u64>,
}

enum NumericCellValue {
    Float(f64),
    Decimal(BigDecimal),
}

pub(crate) fn evaluate_batch(batch: &RecordBatch, config: &CompiledConfig) -> BatchEvaluation {
    let field_to_index = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| (field.name().clone(), idx))
        .collect::<HashMap<_, _>>();

    let unique_counts = build_unique_counts(batch, config, &field_to_index);

    let mut valid_indices = Vec::with_capacity(batch.num_rows());
    let mut invalid_rows = Vec::new();
    let mut failure_counts = BTreeMap::new();

    for row in 0..batch.num_rows() {
        let mut failures = Vec::with_capacity(config.rules.len());

        for rule in &config.rules {
            if let Some(failure) = evaluate_rule(rule, batch, row, &field_to_index, &unique_counts)
            {
                failures.push(failure);
            }
        }

        if failures.is_empty() {
            valid_indices.push(row as u32);
        } else {
            for failure in &failures {
                *failure_counts
                    .entry((failure.rule.to_string(), failure.field.clone()))
                    .or_default() += 1;
            }
            invalid_rows.push(InvalidRow {
                row_index: row,
                message: failures
                    .into_iter()
                    .map(|f| f.message)
                    .collect::<Vec<_>>()
                    .join("; "),
            });
        }
    }

    BatchEvaluation {
        valid_indices,
        invalid_rows,
        failure_counts,
    }
}

struct UniqueFieldState {
    keys: Vec<String>,
    counts: HashMap<String, usize>,
}

fn build_unique_counts(
    batch: &RecordBatch,
    config: &CompiledConfig,
    field_to_index: &HashMap<String, usize>,
) -> HashMap<String, UniqueFieldState> {
    let mut unique_counts: HashMap<String, UniqueFieldState> = HashMap::new();
    for rule in &config.rules {
        if let CompiledRule::Unique { field } = rule {
            let mut counts = HashMap::new();
            let mut keys = Vec::with_capacity(batch.num_rows());
            if let Some(index) = field_to_index.get(field) {
                let column = batch.column(*index);
                for row in 0..batch.num_rows() {
                    let key = unique_key(column.as_ref(), row);
                    *counts.entry(key.clone()).or_insert(0) += 1;
                    keys.push(key);
                }
            }
            unique_counts.insert(field.clone(), UniqueFieldState { keys, counts });
        }
    }
    unique_counts
}

fn evaluate_rule(
    rule: &CompiledRule,
    batch: &RecordBatch,
    row: usize,
    field_to_index: &HashMap<String, usize>,
    unique_counts: &HashMap<String, UniqueFieldState>,
) -> Option<RuleFailure> {
    match rule {
        CompiledRule::NotNull { field } => evaluate_not_null_rule(field, batch, row, field_to_index),
        CompiledRule::Regex {
            field,
            pattern,
            regex,
        } => evaluate_regex_rule(field, pattern, regex, batch, row, field_to_index),
        CompiledRule::Range { field, min, max } => {
            evaluate_range_rule(field, min, max, batch, row, field_to_index)
        }
        CompiledRule::Unique { field } => {
            evaluate_unique_rule(field, batch, row, field_to_index, unique_counts)
        }
    }
}

fn evaluate_not_null_rule(
    field: &str,
    batch: &RecordBatch,
    row: usize,
    field_to_index: &HashMap<String, usize>,
) -> Option<RuleFailure> {
    let Some(index) = field_to_index.get(field) else {
        return Some(RuleFailure {
            rule: "not_null",
            field: field.to_string(),
            message: format!("assert_not_null({field}) failed: column missing"),
        });
    };
    if batch.column(*index).is_null(row) {
        return Some(RuleFailure {
            rule: "not_null",
            field: field.to_string(),
            message: format!("assert_not_null({field}) failed: value is null"),
        });
    }
    None
}

fn evaluate_regex_rule(
    field: &str,
    pattern: &str,
    regex: &regex::Regex,
    batch: &RecordBatch,
    row: usize,
    field_to_index: &HashMap<String, usize>,
) -> Option<RuleFailure> {
    let Some(index) = field_to_index.get(field) else {
        return Some(RuleFailure {
            rule: "regex",
            field: field.to_string(),
            message: format!("assert_regex({field}, {pattern}) failed: column missing"),
        });
    };
    let column = batch.column(*index);
    if column.is_null(row) {
        return Some(RuleFailure {
            rule: "regex",
            field: field.to_string(),
            message: format!("assert_regex({field}, {pattern}) failed: value is null"),
        });
    }
    if !matches!(column.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
        return Some(RuleFailure {
            rule: "regex",
            field: field.to_string(),
            message: format!("assert_regex({field}, {pattern}) failed: field is not string"),
        });
    }
    let value = string_value(column.as_ref(), row).unwrap_or_default();
    if !regex.is_match(value) {
        return Some(RuleFailure {
            rule: "regex",
            field: field.to_string(),
            message: format!(
                "assert_regex({field}, {pattern}) failed: value '{value}' does not match"
            ),
        });
    }
    None
}

fn evaluate_range_rule(
    field: &str,
    min: &Option<NumericBound>,
    max: &Option<NumericBound>,
    batch: &RecordBatch,
    row: usize,
    field_to_index: &HashMap<String, usize>,
) -> Option<RuleFailure> {
    let Some(index) = field_to_index.get(field) else {
        return Some(RuleFailure {
            rule: "range",
            field: field.to_string(),
            message: format!("assert_range({field}) failed: column missing"),
        });
    };
    let column = batch.column(*index);
    if column.is_null(row) {
        return Some(RuleFailure {
            rule: "range",
            field: field.to_string(),
            message: format!("assert_range({field}) failed: value is null"),
        });
    }

    let Some(numeric_value) = numeric_value(column.as_ref(), row) else {
        return Some(RuleFailure {
            rule: "range",
            field: field.to_string(),
            message: format!("assert_range({field}) failed: field is not numeric"),
        });
    };
    match numeric_value {
        NumericCellValue::Float(value) => {
            if !value.is_finite() {
                return Some(RuleFailure {
                    rule: "range",
                    field: field.to_string(),
                    message: format!("assert_range({field}) failed: value is non-finite number"),
                });
            }
            if value_is_out_of_float_bounds(value, min, max) {
                return Some(RuleFailure {
                    rule: "range",
                    field: field.to_string(),
                    message: format!(
                        "assert_range({field}) failed: value {} outside bounds {}",
                        display_value(column.as_ref(), row),
                        format_bounds(min, max)
                    ),
                });
            }
        }
        NumericCellValue::Decimal(value) => {
            if value_is_out_of_decimal_bounds(&value, min, max) {
                return Some(RuleFailure {
                    rule: "range",
                    field: field.to_string(),
                    message: format!(
                        "assert_range({field}) failed: value {} outside bounds {}",
                        display_value(column.as_ref(), row),
                        format_bounds(min, max)
                    ),
                });
            }
        }
    }
    None
}

fn evaluate_unique_rule(
    field: &str,
    batch: &RecordBatch,
    row: usize,
    field_to_index: &HashMap<String, usize>,
    unique_counts: &HashMap<String, UniqueFieldState>,
) -> Option<RuleFailure> {
    let Some(index) = field_to_index.get(field) else {
        return Some(RuleFailure {
            rule: "unique",
            field: field.to_string(),
            message: format!("assert_unique({field}) failed: column missing"),
        });
    };
    let field_state = unique_counts.get(field)?;
    let key = &field_state.keys[row];
    if field_state
        .counts
        .get(key.as_str())
        .copied()
        .unwrap_or_default()
        > 1
    {
        let column = batch.column(*index);
        return Some(RuleFailure {
            rule: "unique",
            field: field.to_string(),
            message: format!(
                "assert_unique({field}) failed: duplicate value {}",
                display_value(column.as_ref(), row)
            ),
        });
    }
    None
}

pub(crate) fn emit_validation_metrics(ctx: &Context, evaluation: &BatchEvaluation) {
    for ((rule, field), count) in &evaluation.failure_counts {
        ctx.counter_with_labels(
            "validation_failures_total",
            *count,
            &[("rule", rule.as_str()), ("field", field.as_str())],
        );
    }
    ctx.counter(
        "validation_rows_valid_total",
        evaluation.valid_indices.len() as u64,
    );
    ctx.counter(
        "validation_rows_invalid_total",
        evaluation.invalid_rows.len() as u64,
    );
}

fn string_value<'a>(array: &'a dyn Array, row: usize) -> Option<&'a str> {
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        Some(values.value(row))
    } else if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        Some(values.value(row))
    } else {
        None
    }
}

fn numeric_value(array: &dyn Array, row: usize) -> Option<NumericCellValue> {
    match array.data_type() {
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|arr| NumericCellValue::Float(f64::from(arr.value(row)))),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|arr| NumericCellValue::Float(f64::from(arr.value(row)))),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|arr| NumericCellValue::Float(f64::from(arr.value(row)))),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|arr| NumericCellValue::Float(arr.value(row) as f64)),
        DataType::UInt8 => array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|arr| NumericCellValue::Float(f64::from(arr.value(row)))),
        DataType::UInt16 => array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|arr| NumericCellValue::Float(f64::from(arr.value(row)))),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|arr| NumericCellValue::Float(f64::from(arr.value(row)))),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|arr| NumericCellValue::Float(arr.value(row) as f64)),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|arr| NumericCellValue::Float(f64::from(arr.value(row)))),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|arr| NumericCellValue::Float(arr.value(row))),
        DataType::Decimal128(_, scale) => {
            let values = array.as_any().downcast_ref::<Decimal128Array>()?;
            Some(NumericCellValue::Decimal(decimal128_to_decimal(
                values.value(row),
                *scale,
            )))
        }
        DataType::Decimal256(_, _scale) => {
            let rendered = array_value_to_string(array, row).ok()?;
            rendered.parse::<BigDecimal>().ok().map(NumericCellValue::Decimal)
        }
        _ => None,
    }
}

fn decimal128_to_decimal(value: i128, scale: i8) -> BigDecimal {
    BigDecimal::new(BigInt::from(value), i64::from(scale))
}

fn value_is_out_of_float_bounds(
    value: f64,
    min: &Option<NumericBound>,
    max: &Option<NumericBound>,
) -> bool {
    min.as_ref().is_some_and(|bound| value < bound.as_f64)
        || max.as_ref().is_some_and(|bound| value > bound.as_f64)
}

fn value_is_out_of_decimal_bounds(
    value: &BigDecimal,
    min: &Option<NumericBound>,
    max: &Option<NumericBound>,
) -> bool {
    min.as_ref().is_some_and(|bound| value < &bound.decimal)
        || max.as_ref().is_some_and(|bound| value > &bound.decimal)
}

fn format_bounds(min: &Option<NumericBound>, max: &Option<NumericBound>) -> String {
    let min_bound = min
        .as_ref()
        .map_or_else(|| "-inf".to_string(), |bound| bound.literal.clone());
    let max_bound = max
        .as_ref()
        .map_or_else(|| "+inf".to_string(), |bound| bound.literal.clone());
    format!("[{min_bound}, {max_bound}]")
}

fn display_value(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        "null".to_string()
    } else {
        array_value_to_string(array, row).unwrap_or_else(|_| "<unprintable>".to_string())
    }
}

fn unique_key(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        "__NULL__".to_string()
    } else {
        display_value(array, row)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, Once, OnceLock};

    use ::arrow::array::{Decimal128Array, Decimal256Array, Float64Array, StringArray};
    use ::arrow::datatypes::{Field, Schema};
    use arrow_buffer::i256;
    use rapidbyte_sdk::checkpoint::StateScope;
    use rapidbyte_sdk::error::ErrorCategory;
    use rapidbyte_sdk::host_ffi::{self, HostImports};

    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    enum MetricCall {
        Counter {
            name: String,
            value: u64,
            labels_json: String,
        },
    }

    struct RecordingHostImports {
        metric_calls: Arc<Mutex<Vec<MetricCall>>>,
    }

    impl HostImports for RecordingHostImports {
        fn log(&self, _level: i32, _message: &str) {}
        fn frame_new(&self, _capacity: u64) -> Result<u64, PluginError> { Ok(1) }
        fn frame_write(&self, _handle: u64, _chunk: &[u8]) -> Result<u64, PluginError> { Ok(0) }
        fn frame_seal(&self, _handle: u64) -> Result<(), PluginError> { Ok(()) }
        fn frame_len(&self, _handle: u64) -> Result<u64, PluginError> { Ok(0) }
        fn frame_read(&self, _handle: u64, _offset: u64, _len: u64) -> Result<Vec<u8>, PluginError> {
            Ok(vec![])
        }
        fn frame_drop(&self, _handle: u64) {}
        fn emit_batch(&self, _handle: u64) -> Result<(), PluginError> { Ok(()) }
        fn next_batch(&self) -> Result<Option<u64>, PluginError> { Ok(None) }
        fn state_get(&self, _scope: StateScope, _key: &str) -> Result<Option<String>, PluginError> { Ok(None) }
        fn state_put(&self, _scope: StateScope, _key: &str, _value: &str) -> Result<(), PluginError> { Ok(()) }
        fn state_compare_and_set(
            &self,
            _scope: StateScope,
            _key: &str,
            _expected: Option<&str>,
            _new_value: &str,
        ) -> Result<bool, PluginError> { Ok(false) }
        fn checkpoint(&self, _plugin_id: &str, _stream_name: &str, _cp: &Checkpoint) -> Result<(), PluginError> {
            Ok(())
        }
        fn counter_add(&self, name: &str, value: u64, labels_json: &str) -> Result<(), PluginError> {
            self.metric_calls.lock().expect("metric calls lock poisoned").push(MetricCall::Counter {
                name: name.to_owned(),
                value,
                labels_json: labels_json.to_owned(),
            });
            Ok(())
        }
        fn gauge_set(&self, _name: &str, _value: f64, _labels_json: &str) -> Result<(), PluginError> { Ok(()) }
        fn histogram_record(&self, _name: &str, _value: f64, _labels_json: &str) -> Result<(), PluginError> { Ok(()) }
        fn emit_dlq_record(
            &self,
            _stream_name: &str,
            _record_json: &str,
            _error_message: &str,
            _error_category: ErrorCategory,
        ) -> Result<(), PluginError> { Ok(()) }
        fn connect_tcp(&self, _host: &str, _port: u16) -> Result<u64, PluginError> {
            Err(PluginError::internal("STUB", "No-op stub"))
        }
        fn socket_read(&self, _handle: u64, _len: u64) -> Result<host_ffi::SocketReadResult, PluginError> {
            Ok(host_ffi::SocketReadResult::Eof)
        }
        fn socket_write(&self, _handle: u64, data: &[u8]) -> Result<host_ffi::SocketWriteResult, PluginError> {
            Ok(host_ffi::SocketWriteResult::Written(data.len() as u64))
        }
        fn socket_close(&self, _handle: u64) {}
    }

    fn metric_calls() -> Arc<Mutex<Vec<MetricCall>>> {
        static INSTALL: Once = Once::new();
        static CALLS: OnceLock<Arc<Mutex<Vec<MetricCall>>>> = OnceLock::new();

        let calls = CALLS
            .get_or_init(|| Arc::new(Mutex::new(Vec::new())))
            .clone();
        INSTALL.call_once(|| {
            let installed = host_ffi::set_host_imports(Box::new(RecordingHostImports {
                metric_calls: calls.clone(),
            }));
            assert!(installed.is_ok(), "recording host imports should install");
        });
        calls.lock().expect("metric calls lock poisoned").clear();
        calls
    }

    fn bound(literal: &str) -> NumericBound {
        NumericBound {
            literal: literal.to_string(),
            decimal: literal.parse::<BigDecimal>().expect("bound should parse"),
            as_f64: literal.parse::<f64>().expect("bound should parse as f64"),
        }
    }

    #[test]
    fn evaluate_batch_collects_multiple_failures() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("email", DataType::Utf8, true),
            Field::new("age", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("x"), None])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(30.0), Some(300.0)])) as ArrayRef,
            ],
        )
        .expect("batch should build");

        let compiled = CompiledConfig {
            rules: vec![
                CompiledRule::NotNull {
                    field: "email".to_string(),
                },
                CompiledRule::Regex {
                    field: "email".to_string(),
                    pattern: "^.+@.+$".to_string(),
                    regex: regex::Regex::new("^.+@.+$").expect("regex should compile"),
                },
                CompiledRule::Range {
                    field: "age".to_string(),
                    min: Some(bound("0")),
                    max: Some(bound("150")),
                },
            ],
        };

        let evaluation = evaluate_batch(&batch, &compiled);
        assert_eq!(evaluation.valid_indices.len(), 0);
        assert_eq!(evaluation.invalid_rows.len(), 2);
        assert!(evaluation.invalid_rows[1].message.contains("assert_not_null"));
        assert!(evaluation.invalid_rows[1].message.contains("assert_regex"));
        assert!(evaluation.invalid_rows[1].message.contains("assert_range"));
    }

    #[test]
    fn evaluate_unique_flags_duplicate_nulls() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![None, None, Some("a"), Some("a")])) as ArrayRef],
        )
        .expect("batch should build");

        let compiled = CompiledConfig {
            rules: vec![CompiledRule::Unique {
                field: "id".to_string(),
            }],
        };

        let evaluation = evaluate_batch(&batch, &compiled);
        assert!(evaluation.valid_indices.is_empty());
        assert_eq!(evaluation.invalid_rows.len(), 4);
    }

    #[test]
    fn evaluate_decimal128_range_supports_bounds() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, 2),
            false,
        )]));
        let values = Decimal128Array::from(vec![1000_i128, 2500_i128, 7000_i128])
            .with_data_type(DataType::Decimal128(10, 2));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values) as ArrayRef])
            .expect("batch should build");

        let compiled = CompiledConfig {
            rules: vec![CompiledRule::Range {
                field: "amount".to_string(),
                min: Some(bound("10")),
                max: Some(bound("30")),
            }],
        };

        let evaluation = evaluate_batch(&batch, &compiled);
        assert_eq!(evaluation.valid_indices, vec![0, 1]);
        assert_eq!(evaluation.invalid_rows.len(), 1);
    }

    #[test]
    fn evaluate_decimal256_range_supports_bounds() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal256(20, 2),
            false,
        )]));
        let values = Decimal256Array::from(vec![
            i256::from_i128(1000),
            i256::from_i128(2500),
            i256::from_i128(7000),
        ])
        .with_data_type(DataType::Decimal256(20, 2));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values) as ArrayRef])
            .expect("batch should build");

        let compiled = CompiledConfig {
            rules: vec![CompiledRule::Range {
                field: "amount".to_string(),
                min: Some(bound("10")),
                max: Some(bound("30")),
            }],
        };

        let evaluation = evaluate_batch(&batch, &compiled);
        assert_eq!(evaluation.valid_indices, vec![0, 1]);
        assert_eq!(evaluation.invalid_rows.len(), 1);
    }

    #[test]
    fn evaluate_range_rejects_non_finite_float_values() {
        let schema = Arc::new(Schema::new(vec![Field::new("score", DataType::Float64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float64Array::from(vec![
                f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
                50.0,
            ])) as ArrayRef],
        )
        .expect("batch should build");

        let compiled = CompiledConfig {
            rules: vec![CompiledRule::Range {
                field: "score".to_string(),
                min: Some(bound("0")),
                max: Some(bound("100")),
            }],
        };

        let evaluation = evaluate_batch(&batch, &compiled);
        assert_eq!(evaluation.valid_indices, vec![3]);
        assert_eq!(evaluation.invalid_rows.len(), 3);
        assert!(evaluation
            .invalid_rows
            .iter()
            .any(|row| row.message.contains("non-finite number")));
    }

    #[test]
    fn evaluate_decimal128_range_allows_exact_bounds() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, 2),
            false,
        )]));
        let values = Decimal128Array::from(vec![1000_i128, 3000_i128])
            .with_data_type(DataType::Decimal128(10, 2));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values) as ArrayRef])
            .expect("batch should build");

        let compiled = CompiledConfig {
            rules: vec![CompiledRule::Range {
                field: "amount".to_string(),
                min: Some(bound("10")),
                max: Some(bound("30")),
            }],
        };

        let evaluation = evaluate_batch(&batch, &compiled);
        assert_eq!(evaluation.valid_indices, vec![0, 1]);
        assert!(evaluation.invalid_rows.is_empty());
    }

    #[test]
    fn evaluate_unique_and_metrics_count_all_duplicate_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![
                Some("dup"),
                Some("dup"),
                Some("dup"),
                Some("ok"),
            ])) as ArrayRef],
        )
        .expect("batch should build");

        let compiled = CompiledConfig {
            rules: vec![CompiledRule::Unique {
                field: "id".to_string(),
            }],
        };

        let evaluation = evaluate_batch(&batch, &compiled);
        assert_eq!(evaluation.valid_indices, vec![3]);
        assert_eq!(evaluation.invalid_rows.len(), 3);
        assert_eq!(
            evaluation
                .failure_counts
                .get(&("unique".to_string(), "id".to_string())),
            Some(&3)
        );
    }

    #[test]
    fn validation_failure_counts_include_rule_and_field_keys() {
        let evaluation = BatchEvaluation {
            valid_indices: vec![0, 2],
            invalid_rows: vec![InvalidRow {
                row_index: 1,
                message: "bad".to_string(),
            }],
            failure_counts: BTreeMap::from([(("regex".to_string(), "email".to_string()), 3)]),
        };

        assert_eq!(
            evaluation
                .failure_counts
                .get(&("regex".to_string(), "email".to_string())),
            Some(&3)
        );
        assert_eq!(evaluation.valid_indices.len(), 2);
        assert_eq!(evaluation.invalid_rows.len(), 1);
    }

    #[test]
    fn emit_validation_metrics_records_rule_and_field_labels() {
        let calls = metric_calls();
        let ctx = Context::new("validate", "users");
        let evaluation = BatchEvaluation {
            valid_indices: vec![0, 2],
            invalid_rows: vec![InvalidRow {
                row_index: 1,
                message: "bad".to_string(),
            }],
            failure_counts: BTreeMap::from([(("regex".to_string(), "email".to_string()), 3)]),
        };

        emit_validation_metrics(&ctx, &evaluation);

        let calls = calls.lock().expect("metric calls lock poisoned").clone();
        let MetricCall::Counter {
            name,
            value,
            labels_json,
        } = &calls[0];
        assert_eq!(name, "validation_failures_total");
        assert_eq!(*value, 3);
        let labels: serde_json::Value =
            serde_json::from_str(labels_json).expect("labels json should parse");
        assert_eq!(
            labels,
            serde_json::json!({
                "plugin": "validate",
                "stream": "users",
                "rule": "regex",
                "field": "email"
            })
        );
        assert_eq!(
            calls[1],
            MetricCall::Counter {
                name: "validation_rows_valid_total".to_string(),
                value: 2,
                labels_json: r#"{"plugin":"validate","stream":"users"}"#.to_string(),
            }
        );
        assert_eq!(
            calls[2],
            MetricCall::Counter {
                name: "validation_rows_invalid_total".to_string(),
                value: 1,
                labels_json: r#"{"plugin":"validate","stream":"users"}"#.to_string(),
            }
        );
    }

    #[test]
    fn evaluate_batch_reports_missing_columns_for_all_rules() {
        let schema = Arc::new(Schema::new(vec![Field::new("email", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef],
        )
        .expect("batch should build");

        let compiled = CompiledConfig {
            rules: vec![
                CompiledRule::NotNull {
                    field: "missing_not_null".to_string(),
                },
                CompiledRule::Regex {
                    field: "missing_regex".to_string(),
                    pattern: "^.+$".to_string(),
                    regex: regex::Regex::new("^.+$").expect("regex should compile"),
                },
                CompiledRule::Range {
                    field: "missing_range".to_string(),
                    min: Some(bound("1")),
                    max: Some(bound("2")),
                },
                CompiledRule::Unique {
                    field: "missing_unique".to_string(),
                },
            ],
        };

        let evaluation = evaluate_batch(&batch, &compiled);
        assert!(evaluation.valid_indices.is_empty());
        assert_eq!(evaluation.invalid_rows.len(), 1);
        let message = &evaluation.invalid_rows[0].message;
        assert!(message.contains("assert_not_null(missing_not_null) failed: column missing"));
        assert!(message.contains("assert_regex(missing_regex, ^.+$) failed: column missing"));
        assert!(message.contains("assert_range(missing_range) failed: column missing"));
        assert!(message.contains("assert_unique(missing_unique) failed: column missing"));
    }

    #[test]
    fn evaluate_decimal_range_uses_exact_decimal_bounds() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(38, 18),
            false,
        )]));
        let values = Decimal128Array::from(vec![
            100_000_000_000_000_000_i128,
            100_000_000_000_000_001_i128,
            99_999_999_999_999_999_i128,
        ])
        .with_data_type(DataType::Decimal128(38, 18));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values) as ArrayRef])
            .expect("batch should build");

        let compiled = CompiledConfig {
            rules: vec![CompiledRule::Range {
                field: "amount".to_string(),
                min: Some(bound("0.100000000000000000")),
                max: Some(bound("0.100000000000000000")),
            }],
        };

        let evaluation = evaluate_batch(&batch, &compiled);
        assert_eq!(evaluation.valid_indices, vec![0]);
        assert_eq!(evaluation.invalid_rows.len(), 2);
    }
}

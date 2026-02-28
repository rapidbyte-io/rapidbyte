use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use ::arrow::array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeStringArray, RecordBatch, StringArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use ::arrow::compute::take;
use ::arrow::datatypes::DataType;
use ::arrow::util::display::array_value_to_string;
use arrow_json::LineDelimitedWriter;
use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::stream::DataErrorPolicy;

use crate::config::{CompiledConfig, CompiledRule};

#[derive(Debug, Clone)]
struct RuleFailure {
    rule: &'static str,
    field: String,
    message: String,
}

pub async fn run(
    ctx: &Context,
    stream: &StreamContext,
    config: &CompiledConfig,
) -> Result<TransformSummary, ConnectorError> {
    let mut records_in: u64 = 0;
    let mut records_out: u64 = 0;
    let mut bytes_in: u64 = 0;
    let mut bytes_out: u64 = 0;
    let mut batches_processed: u64 = 0;

    while let Some((_schema, batches)) = ctx.next_batch(stream.limits.max_batch_bytes)? {
        for batch in &batches {
            if batch.num_rows() == 0 {
                continue;
            }
            batches_processed += 1;
            records_in += batch.num_rows() as u64;
            bytes_in += batch.get_array_memory_size() as u64;

            let evaluation = evaluate_batch(batch, config);
            emit_validation_metrics(ctx, &evaluation)?;

            if !evaluation.valid_indices.is_empty() {
                let filtered = select_rows(batch, &evaluation.valid_indices)?;
                records_out += filtered.num_rows() as u64;
                bytes_out += filtered.get_array_memory_size() as u64;
                ctx.emit_batch(&filtered)?;
            }

            if !evaluation.invalid_rows.is_empty() {
                match stream.policies.on_data_error {
                    DataErrorPolicy::Fail => {
                        let first = &evaluation.invalid_rows[0];
                        return Err(ConnectorError::data(
                            "VALIDATION_FAILED",
                            format!(
                                "validation failed for {} row(s); first failure at row {}: {}",
                                evaluation.invalid_rows.len(),
                                first.row_index,
                                first.message
                            ),
                        ));
                    }
                    DataErrorPolicy::Skip => {}
                    DataErrorPolicy::Dlq => {
                        for invalid in &evaluation.invalid_rows {
                            let record_json = row_to_json(batch, invalid.row_index)?;
                            ctx.emit_dlq_record(
                                &record_json,
                                &invalid.message,
                                rapidbyte_sdk::error::ErrorCategory::Data,
                            )?;
                        }
                    }
                }
            }
        }
    }

    ctx.log(
        LogLevel::Info,
        &format!(
            "Validation transform complete: {} rows in, {} rows out, {} batches",
            records_in, records_out, batches_processed
        ),
    );

    Ok(TransformSummary {
        records_in,
        records_out,
        bytes_in,
        bytes_out,
        batches_processed,
    })
}

struct InvalidRow {
    row_index: usize,
    message: String,
}

struct BatchEvaluation {
    valid_indices: Vec<u32>,
    invalid_rows: Vec<InvalidRow>,
    failure_counts: BTreeMap<(String, String), u64>,
}

fn evaluate_batch(batch: &RecordBatch, config: &CompiledConfig) -> BatchEvaluation {
    let field_to_index = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| (field.name().clone(), idx))
        .collect::<HashMap<_, _>>();

    let mut unique_counts: HashMap<String, HashMap<String, usize>> = HashMap::new();
    for rule in &config.rules {
        if let CompiledRule::Unique { field } = rule {
            let mut counts = HashMap::new();
            if let Some(index) = field_to_index.get(field) {
                let column = batch.column(*index);
                for row in 0..batch.num_rows() {
                    let key = unique_key(column.as_ref(), row);
                    *counts.entry(key).or_insert(0) += 1;
                }
            }
            unique_counts.insert(field.clone(), counts);
        }
    }

    let mut valid_indices = Vec::with_capacity(batch.num_rows());
    let mut invalid_rows = Vec::new();
    let mut failure_counts = BTreeMap::new();

    for row in 0..batch.num_rows() {
        let mut failures = Vec::with_capacity(config.rules.len());

        for rule in &config.rules {
            match rule {
                CompiledRule::NotNull { field } => {
                    if let Some(index) = field_to_index.get(field) {
                        if batch.column(*index).is_null(row) {
                            failures.push(RuleFailure {
                                rule: "not_null",
                                field: field.clone(),
                                message: format!("assert_not_null({field}) failed: value is null"),
                            });
                        }
                    } else {
                        failures.push(RuleFailure {
                            rule: "not_null",
                            field: field.clone(),
                            message: format!("assert_not_null({field}) failed: column missing"),
                        });
                    }
                }
                CompiledRule::Regex {
                    field,
                    pattern,
                    regex,
                } => {
                    if let Some(index) = field_to_index.get(field) {
                        let column = batch.column(*index);
                        if column.is_null(row) {
                            failures.push(RuleFailure {
                                rule: "regex",
                                field: field.clone(),
                                message: format!(
                                    "assert_regex({field}, {pattern}) failed: value is null"
                                ),
                            });
                        } else if !matches!(column.data_type(), DataType::Utf8 | DataType::LargeUtf8)
                        {
                            failures.push(RuleFailure {
                                rule: "regex",
                                field: field.clone(),
                                message: format!(
                                    "assert_regex({field}, {pattern}) failed: field is not string"
                                ),
                            });
                        } else {
                            let value = string_value(column.as_ref(), row).unwrap_or_default();
                            if !regex.is_match(value) {
                                failures.push(RuleFailure {
                                    rule: "regex",
                                    field: field.clone(),
                                    message: format!(
                                        "assert_regex({field}, {pattern}) failed: value '{value}' does not match"
                                    ),
                                });
                            }
                        }
                    } else {
                        failures.push(RuleFailure {
                            rule: "regex",
                            field: field.clone(),
                            message: format!("assert_regex({field}, {pattern}) failed: column missing"),
                        });
                    }
                }
                CompiledRule::Range { field, min, max } => {
                    if let Some(index) = field_to_index.get(field) {
                        let column = batch.column(*index);
                        if column.is_null(row) {
                            failures.push(RuleFailure {
                                rule: "range",
                                field: field.clone(),
                                message: format!("assert_range({field}) failed: value is null"),
                            });
                            continue;
                        }
                        match numeric_value(column.as_ref(), row) {
                            Some(value) => {
                                if !value.is_finite() {
                                    failures.push(RuleFailure {
                                        rule: "range",
                                        field: field.clone(),
                                        message: format!(
                                            "assert_range({field}) failed: value is non-finite number"
                                        ),
                                    });
                                } else if min.is_some_and(|lower| value < lower)
                                    || max.is_some_and(|upper| value > upper)
                                {
                                    failures.push(RuleFailure {
                                        rule: "range",
                                        field: field.clone(),
                                        message: format!(
                                            "assert_range({field}) failed: value {value} outside bounds [{:?}, {:?}]",
                                            min, max
                                        ),
                                    });
                                }
                            }
                            None => failures.push(RuleFailure {
                                rule: "range",
                                field: field.clone(),
                                message: format!(
                                    "assert_range({field}) failed: field is not numeric"
                                ),
                            }),
                        }
                    } else {
                        failures.push(RuleFailure {
                            rule: "range",
                            field: field.clone(),
                            message: format!("assert_range({field}) failed: column missing"),
                        });
                    }
                }
                CompiledRule::Unique { field } => {
                    if let Some(index) = field_to_index.get(field) {
                        let column = batch.column(*index);
                        let key = unique_key(column.as_ref(), row);
                        if unique_counts
                            .get(field)
                            .and_then(|counts| counts.get(&key))
                            .copied()
                            .unwrap_or_default()
                            > 1
                        {
                            failures.push(RuleFailure {
                                rule: "unique",
                                field: field.clone(),
                                message: format!(
                                    "assert_unique({field}) failed: duplicate value {}",
                                    display_value(column.as_ref(), row)
                                ),
                            });
                        }
                    } else {
                        failures.push(RuleFailure {
                            rule: "unique",
                            field: field.clone(),
                            message: format!("assert_unique({field}) failed: column missing"),
                        });
                    }
                }
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

fn emit_validation_metrics(ctx: &Context, evaluation: &BatchEvaluation) -> Result<(), ConnectorError> {
    for metric in build_validation_metrics(evaluation) {
        ctx.metric(&metric)?;
    }
    Ok(())
}

fn build_validation_metrics(evaluation: &BatchEvaluation) -> Vec<Metric> {
    let mut metrics = Vec::new();
    for ((rule, field), count) in &evaluation.failure_counts {
        metrics.push(Metric {
            name: "validation_failures_total".to_string(),
            value: MetricValue::Counter(*count),
            labels: vec![
                ("rule".to_string(), rule.clone()),
                ("field".to_string(), field.clone()),
            ],
        });
    }
    metrics.push(Metric {
        name: "validation_rows_valid_total".to_string(),
        value: MetricValue::Counter(evaluation.valid_indices.len() as u64),
        labels: Vec::new(),
    });
    metrics.push(Metric {
        name: "validation_rows_invalid_total".to_string(),
        value: MetricValue::Counter(evaluation.invalid_rows.len() as u64),
        labels: Vec::new(),
    });
    metrics
}

fn select_rows(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch, ConnectorError> {
    let idx = UInt32Array::from(indices.to_vec());
    let idx_ref = &idx as &dyn Array;
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for column in batch.columns() {
        let filtered = take(column.as_ref(), idx_ref, None).map_err(|e| {
            ConnectorError::internal("VALIDATE_FILTER", format!("failed to filter rows: {e}"))
        })?;
        arrays.push(filtered);
    }
    RecordBatch::try_new(Arc::clone(&batch.schema()), arrays).map_err(|e| {
        ConnectorError::internal(
            "VALIDATE_BATCH",
            format!("failed to build filtered record batch: {e}"),
        )
    })
}

fn row_to_json(batch: &RecordBatch, row: usize) -> Result<String, ConnectorError> {
    let single = select_rows(batch, &[row as u32])?;
    let mut writer = LineDelimitedWriter::new(Vec::new());
    writer.write_batches(&[&single]).map_err(|e| {
        ConnectorError::internal("VALIDATE_DLQ_JSON", format!("failed to encode DLQ row: {e}"))
    })?;
    writer.finish().map_err(|e| {
        ConnectorError::internal(
            "VALIDATE_DLQ_JSON",
            format!("failed to finalize DLQ row json: {e}"),
        )
    })?;
    let mut text = String::from_utf8(writer.into_inner()).map_err(|e| {
        ConnectorError::internal(
            "VALIDATE_DLQ_JSON",
            format!("dlq json was not utf8: {e}"),
        )
    })?;
    while text.ends_with('\n') {
        text.pop();
    }
    Ok(text)
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

fn numeric_value(array: &dyn Array, row: usize) -> Option<f64> {
    match array.data_type() {
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|arr| f64::from(arr.value(row))),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|arr| f64::from(arr.value(row))),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|arr| f64::from(arr.value(row))),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|arr| arr.value(row) as f64),
        DataType::UInt8 => array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|arr| f64::from(arr.value(row))),
        DataType::UInt16 => array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|arr| f64::from(arr.value(row))),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|arr| f64::from(arr.value(row))),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|arr| arr.value(row) as f64),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|arr| f64::from(arr.value(row))),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|arr| arr.value(row)),
        DataType::Decimal128(_, scale) => array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .map(|arr| decimal128_to_f64(arr.value(row), *scale)),
        DataType::Decimal256(_, _scale) => {
            let rendered = array_value_to_string(array, row).ok()?;
            rendered.parse::<f64>().ok()
        }
        _ => None,
    }
}

fn decimal128_to_f64(value: i128, scale: i8) -> f64 {
    if scale >= 0 {
        value as f64 / 10f64.powi(i32::from(scale))
    } else {
        value as f64 * 10f64.powi(-i32::from(scale))
    }
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
    use super::*;
    use ::arrow::array::{Decimal128Array, Decimal256Array, Float64Array, StringArray};
    use ::arrow::datatypes::{Field, Schema};
    use arrow_buffer::i256;

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
                    min: Some(0.0),
                    max: Some(150.0),
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
                min: Some(10.0),
                max: Some(30.0),
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
                min: Some(10.0),
                max: Some(30.0),
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
                min: Some(0.0),
                max: Some(100.0),
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
                min: Some(10.0),
                max: Some(30.0),
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

        let metrics = build_validation_metrics(&evaluation);
        let unique_failure = metrics
            .iter()
            .find(|m| {
                m.name == "validation_failures_total"
                    && m
                        .labels
                        .contains(&("rule".to_string(), "unique".to_string()))
                    && m.labels.contains(&("field".to_string(), "id".to_string()))
            })
            .expect("unique failure metric should exist");
        assert_eq!(unique_failure.value, MetricValue::Counter(3));
    }

    #[test]
    fn build_validation_metrics_includes_rule_and_field_labels() {
        let evaluation = BatchEvaluation {
            valid_indices: vec![0, 2],
            invalid_rows: vec![InvalidRow {
                row_index: 1,
                message: "bad".to_string(),
            }],
            failure_counts: BTreeMap::from([(("regex".to_string(), "email".to_string()), 3)]),
        };

        let metrics = build_validation_metrics(&evaluation);
        let failure = metrics
            .iter()
            .find(|m| m.name == "validation_failures_total")
            .expect("failure metric should exist");
        assert_eq!(
            failure.labels,
            vec![
                ("rule".to_string(), "regex".to_string()),
                ("field".to_string(), "email".to_string())
            ]
        );
    }
}

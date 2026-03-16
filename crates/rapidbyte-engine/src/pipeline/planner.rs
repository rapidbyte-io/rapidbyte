//! Stream context construction, parallelism computation, and execution planning.

use std::collections::HashSet;

use rapidbyte_runtime::{CompressionCodec, SandboxOverrides};
use rapidbyte_state::StateBackend;
use rapidbyte_types::catalog::SchemaHint;
use rapidbyte_types::cursor::{CursorInfo, CursorType, CursorValue};
use rapidbyte_types::manifest::{Permissions, PluginManifest};
use rapidbyte_types::state::{PipelineId, StreamName};
use rapidbyte_types::stream::{PartitionStrategy, StreamContext, StreamLimits, StreamPolicies};
use rapidbyte_types::wire::{Feature, SyncMode, WriteMode};

use crate::config::types::{parse_byte_size, PipelineConfig, PipelineParallelism};
use crate::error::PipelineError;

const SYSTEM_CORE_RESERVE_DIVISOR: u32 = 8;
const MIN_PIPELINE_COORDINATION_CORES: u32 = 1;
const MAX_PIPELINE_COORDINATION_CORES: u32 = 2;
const TRANSFORM_PENALTY_SLOPE: f64 = 0.05;
const MIN_TRANSFORM_FACTOR: f64 = 0.75;

pub(crate) struct ExecutionPlan {
    pub(crate) limits: StreamLimits,
    pub(crate) compression: Option<CompressionCodec>,
    pub(crate) stream_ctxs: Vec<StreamContext>,
}

/// Pipeline identity metadata.
pub(crate) struct PipelineIdentity {
    pub(crate) name: String,
    pub(crate) metric_run_label: String,
}

/// Identity, config, and sandbox for a single plugin.
pub(crate) struct PluginSpec {
    pub(crate) id: String,
    pub(crate) version: String,
    pub(crate) config: serde_json::Value,
    pub(crate) permissions: Option<Permissions>,
    pub(crate) overrides: Option<SandboxOverrides>,
}

/// Full execution parameters for a stream pipeline.
pub(crate) struct StreamExecutionParams {
    pub(crate) pipeline: PipelineIdentity,
    pub(crate) source: PluginSpec,
    pub(crate) destination: PluginSpec,
    pub(crate) transform_overrides: Vec<Option<SandboxOverrides>>,
    pub(crate) compression: Option<CompressionCodec>,
    pub(crate) channel_capacity: usize,
}

pub(crate) fn compute_pipeline_parallelism(
    config: &PipelineConfig,
    supports_partitioned_read: bool,
) -> u32 {
    match config.resources.parallelism {
        PipelineParallelism::Manual(value) => value.max(1),
        PipelineParallelism::Auto => {
            let available_cores = std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(1);
            let available_cores = u32::try_from(available_cores).unwrap_or(u32::MAX);
            compute_auto_parallelism(config, available_cores, supports_partitioned_read)
        }
    }
}

pub(crate) fn compute_auto_parallelism(
    config: &PipelineConfig,
    available_cores: u32,
    supports_partitioned_read: bool,
) -> u32 {
    let cap = compute_worker_core_budget(available_cores);

    let eligible_streams = if supports_partitioned_read {
        u32::try_from(
            config
                .source
                .streams
                .iter()
                .filter(|stream| stream.sync_mode == SyncMode::FullRefresh)
                .count(),
        )
        .unwrap_or(u32::MAX)
    } else {
        0
    };

    let base_target = if eligible_streams == 0 {
        1
    } else {
        let per_stream_budget = (cap / eligible_streams).max(1);
        (eligible_streams * per_stream_budget).min(cap)
    };

    #[allow(clippy::cast_precision_loss)]
    let num_transforms = config.transforms.len() as f64;
    let transform_factor = if num_transforms == 0.0 {
        1.0
    } else {
        (1.0 / (1.0 + (TRANSFORM_PENALTY_SLOPE * num_transforms))).max(MIN_TRANSFORM_FACTOR)
    };

    let scaled = (f64::from(base_target) * transform_factor).round();
    if scaled <= 1.0 {
        1
    } else if scaled >= f64::from(cap) {
        cap
    } else {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let scaled_u32 = scaled as u32;
        scaled_u32.clamp(1, cap)
    }
}

fn compute_worker_core_budget(available_cores: u32) -> u32 {
    let available_cores = available_cores.max(1);

    let system_reserve = if available_cores <= 2 {
        0
    } else {
        (available_cores / SYSTEM_CORE_RESERVE_DIVISOR).max(1)
    };
    let coordination_reserve = (available_cores / 4).clamp(
        MIN_PIPELINE_COORDINATION_CORES,
        MAX_PIPELINE_COORDINATION_CORES,
    );

    available_cores
        .saturating_sub(system_reserve)
        .saturating_sub(coordination_reserve)
        .max(1)
}

fn decode_incremental_last_value(raw: String, tie_breaker_field: Option<&str>) -> CursorValue {
    if tie_breaker_field.is_some() {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) {
            if value
                .as_object()
                .is_some_and(|object| object.contains_key("cursor"))
            {
                return CursorValue::Json { value };
            }
        }

        return CursorValue::Json {
            value: serde_json::json!({
                "cursor": {
                    "type": "utf8",
                    "value": raw,
                },
                "tie_breaker": {
                    "type": "null",
                }
            }),
        };
    }

    CursorValue::Utf8 { value: raw }
}

pub(crate) fn build_stream_contexts(
    config: &PipelineConfig,
    state: &dyn StateBackend,
    max_records: Option<u64>,
    source_manifest: Option<&PluginManifest>,
) -> Result<ExecutionPlan, PipelineError> {
    let supports_partitioned_read = source_manifest
        .is_some_and(|m: &PluginManifest| m.has_source_feature(Feature::PartitionedRead));
    let baseline_parallelism = compute_pipeline_parallelism(config, supports_partitioned_read);
    let autotune_decision = crate::autotune::compute_autotune_decision(
        config,
        baseline_parallelism,
        supports_partitioned_read,
    );
    let configured_parallelism = autotune_decision.parallelism;
    tracing::info!(
        autotune_enabled = autotune_decision.autotune_enabled,
        baseline_parallelism,
        configured_parallelism,
        partition_strategy = ?autotune_decision.partition_strategy,
        copy_flush_bytes_override = autotune_decision.copy_flush_bytes_override,
        "Autotune decision resolved"
    );
    let max_batch = parse_byte_size(&config.resources.max_batch_bytes).map_err(|e| {
        PipelineError::infra(format!(
            "Invalid max_batch_bytes '{}': {}",
            config.resources.max_batch_bytes, e
        ))
    })?;
    let checkpoint_interval = parse_byte_size(&config.resources.checkpoint_interval_bytes)
        .map_err(|e| {
            PipelineError::infra(format!(
                "Invalid checkpoint_interval_bytes '{}': {}",
                config.resources.checkpoint_interval_bytes, e
            ))
        })?;

    let limits = StreamLimits {
        max_batch_bytes: if max_batch > 0 {
            max_batch
        } else {
            StreamLimits::default().max_batch_bytes
        },
        checkpoint_interval_bytes: checkpoint_interval,
        checkpoint_interval_rows: config.resources.checkpoint_interval_rows,
        checkpoint_interval_seconds: config.resources.checkpoint_interval_seconds,
        max_inflight_batches: config.resources.max_inflight_batches,
        max_records,
        ..StreamLimits::default()
    };

    let pipeline_id = PipelineId::new(config.pipeline.clone());
    let should_partition = supports_partitioned_read && configured_parallelism > 1;
    let mut stream_ctxs = Vec::new();

    for s in &config.source.streams {
        let cursor_info = match s.sync_mode {
            SyncMode::Incremental => {
                if let Some(cursor_field) = &s.cursor_field {
                    let last_value = state
                        .get_cursor(&pipeline_id, &StreamName::new(s.name.clone()))
                        .map_err(|e| PipelineError::Infrastructure(e.into()))?
                        .and_then(|cs| cs.cursor_value)
                        .map(|v| decode_incremental_last_value(v, s.tie_breaker_field.as_deref()));
                    Some(CursorInfo {
                        cursor_field: cursor_field.clone(),
                        tie_breaker_field: s.tie_breaker_field.clone(),
                        cursor_type: CursorType::Utf8,
                        last_value,
                    })
                } else {
                    None
                }
            }
            SyncMode::Cdc => {
                let last_value = state
                    .get_cursor(&pipeline_id, &StreamName::new(s.name.clone()))
                    .map_err(|e| PipelineError::Infrastructure(e.into()))?
                    .and_then(|cs| cs.cursor_value)
                    .map(|v| CursorValue::Lsn { value: v });
                Some(CursorInfo {
                    cursor_field: "lsn".to_string(),
                    tie_breaker_field: None,
                    cursor_type: CursorType::Lsn,
                    last_value,
                })
            }
            SyncMode::FullRefresh => None,
        };

        let base_ctx = StreamContext {
            stream_name: s.name.clone(),
            source_stream_name: None,
            schema: SchemaHint::Columns(vec![]),
            sync_mode: s.sync_mode,
            cursor_info,
            limits: limits.clone(),
            policies: StreamPolicies {
                on_data_error: config.destination.on_data_error,
                schema_evolution: config.destination.schema_evolution.unwrap_or_default(),
            },
            write_mode: Some(
                config
                    .destination
                    .write_mode
                    .to_protocol(config.destination.primary_key.clone()),
            ),
            selected_columns: s.columns.clone(),
            partition_key: s.partition_key.clone(),
            partition_count: None,
            partition_index: None,
            effective_parallelism: Some(configured_parallelism),
            partition_strategy: None,
            copy_flush_bytes_override: autotune_decision.copy_flush_bytes_override,
        };

        if should_partition
            && s.sync_mode == SyncMode::FullRefresh
            && !matches!(base_ctx.write_mode, Some(WriteMode::Replace))
        {
            for shard in 0..configured_parallelism {
                let mut shard_ctx = base_ctx.clone();
                shard_ctx.source_stream_name = Some(s.name.clone());
                shard_ctx.partition_count = Some(configured_parallelism);
                shard_ctx.partition_index = Some(shard);
                shard_ctx.partition_strategy = Some(
                    autotune_decision
                        .partition_strategy
                        .unwrap_or(PartitionStrategy::Mod),
                );
                stream_ctxs.push(shard_ctx);
            }
        } else {
            stream_ctxs.push(base_ctx);
        }
    }

    Ok(ExecutionPlan {
        limits,
        compression: config.resources.compression,
        stream_ctxs,
    })
}

pub(crate) fn destination_preflight_streams(stream_ctxs: &[StreamContext]) -> Vec<StreamContext> {
    let mut seen = HashSet::new();
    let mut preflight = Vec::new();
    for stream_ctx in stream_ctxs {
        if matches!(stream_ctx.write_mode, Some(WriteMode::Replace)) {
            continue;
        }
        if seen.insert(stream_ctx.stream_name.clone()) {
            let mut preflight_ctx = stream_ctx.clone();
            // Preflight runs once per logical stream and should not carry shard identity.
            preflight_ctx.partition_count = None;
            preflight_ctx.partition_index = None;
            preflight.push(preflight_ctx);
        }
    }
    preflight
}

pub(crate) fn execution_parallelism(
    config: &PipelineConfig,
    stream_ctxs: &[StreamContext],
) -> usize {
    let resolved = stream_ctxs
        .iter()
        .filter_map(|ctx| ctx.effective_parallelism)
        .max()
        .unwrap_or_else(|| compute_pipeline_parallelism(config, false));

    usize::try_from(resolved.max(1)).unwrap_or(usize::MAX)
}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod stream_context_partition_tests {
    use super::*;
    use crate::config::types::PipelineConfig;
    use rapidbyte_state::SqliteStateBackend;
    use rapidbyte_types::manifest::{ResourceLimits, Roles, SourceCapabilities};
    use rapidbyte_types::wire::ProtocolVersion;

    fn test_manifest_with_partitioned_read() -> PluginManifest {
        PluginManifest {
            id: "test/pg".to_string(),
            name: "Test".to_string(),
            version: "0.1.0".to_string(),
            description: String::new(),
            author: None,
            license: None,
            protocol_version: ProtocolVersion::current(),
            roles: Roles {
                source: Some(SourceCapabilities {
                    supported_sync_modes: vec![SyncMode::FullRefresh],
                    features: vec![Feature::PartitionedRead],
                }),
                destination: None,
                transform: None,
            },
            permissions: Permissions::default(),
            limits: ResourceLimits::default(),
            config_schema: None,
        }
    }

    fn config_with_parallelism(parallelism: u32, sync_mode: &str) -> PipelineConfig {
        config_with_parallelism_and_write_mode(parallelism, sync_mode, "append")
    }

    fn config_with_parallelism_and_write_mode(
        parallelism: u32,
        sync_mode: &str,
        write_mode: &str,
    ) -> PipelineConfig {
        let yaml = format!(
            r#"
version: "1.0"
pipeline: test_partitioning
source:
  use: postgres
  config: {{}}
  streams:
    - name: bench_events
      sync_mode: {sync_mode}
destination:
  use: postgres
  config: {{}}
  write_mode: {write_mode}
resources:
  parallelism: {parallelism}
"#
        );
        serde_yaml::from_str(&yaml).expect("valid pipeline yaml")
    }

    fn config_with_parallelism_expr(
        parallelism: &str,
        source_use: &str,
        sync_mode: &str,
        write_mode: &str,
        num_transforms: usize,
    ) -> PipelineConfig {
        let transforms_yaml = if num_transforms == 0 {
            String::new()
        } else {
            let mut transforms = String::from("transforms:\n");
            for _ in 0..num_transforms {
                transforms.push_str("  - use: sql\n    config: {}\n");
            }
            transforms
        };

        let yaml = format!(
            r#"
version: "1.0"
pipeline: test_partitioning
source:
  use: {source_use}
  config: {{}}
  streams:
    - name: bench_events
      sync_mode: {sync_mode}
{transforms_yaml}destination:
  use: postgres
  config: {{}}
  write_mode: {write_mode}
resources:
  parallelism: {parallelism}
"#
        );
        serde_yaml::from_str(&yaml).expect("valid pipeline yaml")
    }

    #[test]
    fn full_refresh_with_parallelism_fans_out_stream_contexts() {
        let config = config_with_parallelism(4, "full_refresh");
        let state = SqliteStateBackend::in_memory().expect("in-memory state backend");
        let manifest = test_manifest_with_partitioned_read();

        let build = build_stream_contexts(&config, &state, None, Some(&manifest))
            .expect("stream contexts built");

        assert_eq!(build.stream_ctxs.len(), 4);
        assert_eq!(
            build
                .stream_ctxs
                .iter()
                .map(|ctx| ctx.stream_name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "bench_events",
                "bench_events",
                "bench_events",
                "bench_events"
            ]
        );
        for (idx, stream_ctx) in build.stream_ctxs.iter().enumerate() {
            assert_eq!(stream_ctx.sync_mode, SyncMode::FullRefresh);
            assert_eq!(
                stream_ctx.source_stream_name.as_deref(),
                Some("bench_events")
            );
            assert_eq!(stream_ctx.partition_count, Some(4));
            assert_eq!(
                stream_ctx.partition_index,
                Some(u32::try_from(idx).expect("partition index should fit in u32"))
            );
        }
    }

    #[test]
    fn incremental_streams_remain_unpartitioned() {
        let config = config_with_parallelism(4, "incremental");
        let state = SqliteStateBackend::in_memory().expect("in-memory state backend");
        let manifest = test_manifest_with_partitioned_read();

        let build = build_stream_contexts(&config, &state, None, Some(&manifest))
            .expect("stream contexts built");

        assert_eq!(build.stream_ctxs.len(), 1);
        let stream_ctx = &build.stream_ctxs[0];
        assert_eq!(stream_ctx.stream_name, "bench_events");
        assert_eq!(stream_ctx.source_stream_name, None);
        assert_eq!(stream_ctx.partition_count, None);
        assert_eq!(stream_ctx.partition_index, None);
    }

    #[test]
    fn replace_mode_streams_remain_unpartitioned() {
        let config = config_with_parallelism_and_write_mode(4, "full_refresh", "replace");
        let state = SqliteStateBackend::in_memory().expect("in-memory state backend");
        let manifest = test_manifest_with_partitioned_read();

        let build = build_stream_contexts(&config, &state, None, Some(&manifest))
            .expect("stream contexts built");

        assert_eq!(build.stream_ctxs.len(), 1);
        let stream_ctx = &build.stream_ctxs[0];
        assert_eq!(stream_ctx.stream_name, "bench_events");
        assert_eq!(stream_ctx.source_stream_name, None);
        assert_eq!(stream_ctx.partition_count, None);
        assert_eq!(stream_ctx.partition_index, None);
        assert_eq!(stream_ctx.write_mode, Some(WriteMode::Replace));
    }

    #[test]
    fn decode_incremental_last_value_wraps_legacy_scalar_for_tie_breaker_streams() {
        let value = decode_incremental_last_value("42".to_string(), Some("id"));

        match value {
            CursorValue::Json { value } => {
                assert_eq!(value["cursor"]["type"], "utf8");
                assert_eq!(value["cursor"]["value"], "42");
                assert_eq!(value["tie_breaker"]["type"], "null");
            }
            other => panic!("expected wrapped composite cursor, got {other:?}"),
        }
    }

    #[test]
    fn decode_incremental_last_value_preserves_composite_json() {
        let raw = r#"{"cursor":{"type":"utf8","value":"2024-01-01T00:00:00Z"},"tie_breaker":{"type":"int64","value":7}}"#;
        let value = decode_incremental_last_value(raw.to_string(), Some("id"));

        match value {
            CursorValue::Json { value } => {
                assert_eq!(value["cursor"]["value"], "2024-01-01T00:00:00Z");
                assert_eq!(value["tie_breaker"]["value"], 7);
            }
            other => panic!("expected composite cursor json, got {other:?}"),
        }
    }

    #[test]
    fn auto_parallelism_without_eligible_streams_resolves_to_one() {
        let config = config_with_parallelism_expr("auto", "postgres", "incremental", "append", 0);
        assert_eq!(compute_pipeline_parallelism(&config, true), 1);
    }

    #[test]
    fn auto_parallelism_uses_adaptive_core_budget() {
        let config = config_with_parallelism_expr("auto", "postgres", "full_refresh", "append", 0);

        assert_eq!(compute_auto_parallelism(&config, 16, true), 12);
        assert_eq!(compute_auto_parallelism(&config, 8, true), 5);
        assert_eq!(compute_auto_parallelism(&config, 4, true), 2);
    }

    #[test]
    fn manual_parallelism_override_is_honored() {
        let config = config_with_parallelism_expr("7", "postgres", "full_refresh", "append", 0);
        assert_eq!(compute_pipeline_parallelism(&config, true), 7);
    }

    #[test]
    fn transform_count_reduces_auto_parallelism() {
        let without_transforms =
            config_with_parallelism_expr("auto", "postgres", "full_refresh", "append", 0);
        let with_transforms =
            config_with_parallelism_expr("auto", "postgres", "full_refresh", "append", 3);

        assert!(
            compute_auto_parallelism(&with_transforms, 16, true)
                <= compute_auto_parallelism(&without_transforms, 16, true)
        );
    }

    #[test]
    fn auto_parallelism_scales_with_multiple_streams() {
        let yaml = r#"
version: "1.0"
pipeline: bench_pg
source:
  use: postgres
  config: {}
  streams:
    - name: a
      sync_mode: full_refresh
    - name: b
      sync_mode: full_refresh
    - name: c
      sync_mode: full_refresh
destination:
  use: postgres
  config: {}
  write_mode: append
resources:
  parallelism: auto
state:
  backend: sqlite
  connection: ":memory:"
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).expect("valid pipeline yaml");

        // 16 cores => 12 worker cores after reserves; split across 3 streams => 4 shards each.
        assert_eq!(compute_auto_parallelism(&config, 16, true), 12);
    }

    #[test]
    fn execution_parallelism_prefers_stream_context_override() {
        let config = config_with_parallelism_expr("2", "postgres", "full_refresh", "append", 0);
        let stream_ctxs = vec![StreamContext {
            stream_name: "users".to_string(),
            source_stream_name: Some("users".to_string()),
            schema: SchemaHint::Columns(vec![]),
            sync_mode: SyncMode::FullRefresh,
            cursor_info: None,
            limits: StreamLimits::default(),
            policies: StreamPolicies::default(),
            write_mode: Some(WriteMode::Append),
            selected_columns: None,
            partition_key: None,
            partition_count: Some(5),
            partition_index: Some(0),
            effective_parallelism: Some(5),
            partition_strategy: Some(rapidbyte_types::stream::PartitionStrategy::Mod),
            copy_flush_bytes_override: None,
        }];

        assert_eq!(execution_parallelism(&config, &stream_ctxs), 5);
    }

    #[test]
    fn execution_parallelism_falls_back_to_pipeline_setting() {
        let config = config_with_parallelism_expr("3", "postgres", "full_refresh", "append", 0);
        let stream_ctxs = vec![StreamContext {
            stream_name: "users".to_string(),
            source_stream_name: None,
            schema: SchemaHint::Columns(vec![]),
            sync_mode: SyncMode::FullRefresh,
            cursor_info: None,
            limits: StreamLimits::default(),
            policies: StreamPolicies::default(),
            write_mode: Some(WriteMode::Append),
            selected_columns: None,
            partition_key: None,
            partition_count: None,
            partition_index: None,
            effective_parallelism: None,
            partition_strategy: None,
            copy_flush_bytes_override: None,
        }];

        assert_eq!(execution_parallelism(&config, &stream_ctxs), 3);
    }

    #[test]
    fn destination_preflight_deduplicates_partitioned_streams() {
        let stream_ctxs = vec![
            StreamContext {
                stream_name: "bench_events".to_string(),
                source_stream_name: Some("bench_events".to_string()),
                schema: SchemaHint::Columns(vec![]),
                sync_mode: SyncMode::FullRefresh,
                cursor_info: None,
                limits: StreamLimits::default(),
                policies: StreamPolicies::default(),
                write_mode: None,
                selected_columns: None,
                partition_key: None,
                partition_count: Some(4),
                partition_index: Some(0),
                effective_parallelism: Some(4),
                partition_strategy: Some(rapidbyte_types::stream::PartitionStrategy::Mod),
                copy_flush_bytes_override: None,
            },
            StreamContext {
                stream_name: "bench_events".to_string(),
                source_stream_name: Some("bench_events".to_string()),
                schema: SchemaHint::Columns(vec![]),
                sync_mode: SyncMode::FullRefresh,
                cursor_info: None,
                limits: StreamLimits::default(),
                policies: StreamPolicies::default(),
                write_mode: None,
                selected_columns: None,
                partition_key: None,
                partition_count: Some(4),
                partition_index: Some(3),
                effective_parallelism: Some(4),
                partition_strategy: Some(rapidbyte_types::stream::PartitionStrategy::Mod),
                copy_flush_bytes_override: None,
            },
            StreamContext {
                stream_name: "users".to_string(),
                source_stream_name: None,
                schema: SchemaHint::Columns(vec![]),
                sync_mode: SyncMode::Incremental,
                cursor_info: None,
                limits: StreamLimits::default(),
                policies: StreamPolicies::default(),
                write_mode: None,
                selected_columns: None,
                partition_key: None,
                partition_count: None,
                partition_index: None,
                effective_parallelism: Some(1),
                partition_strategy: None,
                copy_flush_bytes_override: None,
            },
        ];

        let preflight = destination_preflight_streams(&stream_ctxs);
        assert_eq!(preflight.len(), 2);
        assert_eq!(preflight[0].stream_name, "bench_events");
        assert_eq!(preflight[0].partition_count, None);
        assert_eq!(preflight[0].partition_index, None);
        assert_eq!(preflight[1].stream_name, "users");
        assert_eq!(preflight[1].partition_count, None);
        assert_eq!(preflight[1].partition_index, None);
    }

    #[test]
    fn destination_preflight_skips_replace_mode_streams() {
        let stream_ctxs = vec![
            StreamContext {
                stream_name: "users_replace".to_string(),
                source_stream_name: None,
                schema: SchemaHint::Columns(vec![]),
                sync_mode: SyncMode::FullRefresh,
                cursor_info: None,
                limits: StreamLimits::default(),
                policies: StreamPolicies::default(),
                write_mode: Some(WriteMode::Replace),
                selected_columns: None,
                partition_key: None,
                partition_count: None,
                partition_index: None,
                effective_parallelism: Some(1),
                partition_strategy: None,
                copy_flush_bytes_override: None,
            },
            StreamContext {
                stream_name: "orders_append".to_string(),
                source_stream_name: None,
                schema: SchemaHint::Columns(vec![]),
                sync_mode: SyncMode::FullRefresh,
                cursor_info: None,
                limits: StreamLimits::default(),
                policies: StreamPolicies::default(),
                write_mode: Some(WriteMode::Append),
                selected_columns: None,
                partition_key: None,
                partition_count: None,
                partition_index: None,
                effective_parallelism: Some(1),
                partition_strategy: None,
                copy_flush_bytes_override: None,
            },
        ];

        let preflight = destination_preflight_streams(&stream_ctxs);
        assert_eq!(preflight.len(), 1);
        assert_eq!(preflight[0].stream_name, "orders_append");
    }
}

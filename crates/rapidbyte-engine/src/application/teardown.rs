//! Teardown-pipeline use case — clean up persistent resources.
//!
//! [`teardown_pipeline`] resolves source and destination plugins, then
//! invokes their teardown lifecycle method to drop replication slots,
//! staging tables, and other provisioned resources.

use rapidbyte_pipeline_config::PipelineConfig;
use rapidbyte_types::lifecycle::TeardownReport;
use rapidbyte_types::wire::PluginKind;

use crate::application::context::EngineContext;
use crate::application::{build_sandbox_overrides, extract_permissions, parse_plugin_id};
use crate::domain::error::PipelineError;
use crate::domain::ports::runner::TeardownParams;

/// Tear down persistent resources created by a pipeline.
///
/// Resolves source and destination plugins and calls their `teardown`
/// lifecycle method. The `reason` string is forwarded to each plugin
/// so it can log or condition its cleanup behavior.
///
/// # Errors
///
/// Returns `PipelineError` if plugin resolution or teardown fails.
pub async fn teardown_pipeline(
    ctx: &EngineContext,
    pipeline: &PipelineConfig,
    reason: &str,
) -> Result<TeardownReport, PipelineError> {
    let source_resolved = ctx
        .resolver
        .resolve(
            &pipeline.source.use_ref,
            PluginKind::Source,
            Some(&pipeline.source.config),
        )
        .await?;

    let dest_resolved = ctx
        .resolver
        .resolve(
            &pipeline.destination.use_ref,
            PluginKind::Destination,
            Some(&pipeline.destination.config),
        )
        .await?;

    let stream_names: Vec<String> = pipeline
        .source
        .streams
        .iter()
        .map(|s| s.name.clone())
        .collect();

    let mut all_actions = Vec::new();

    // Source teardown (e.g., drop replication slots)
    let (src_id, src_ver) = parse_plugin_id(&pipeline.source.use_ref);
    let src_permissions = extract_permissions(&source_resolved);
    let src_overrides = build_sandbox_overrides(
        pipeline.source.permissions.as_ref(),
        pipeline.source.limits.as_ref(),
        source_resolved.manifest.as_ref(),
    )?;

    let source_report = ctx
        .runner
        .teardown(&TeardownParams {
            wasm_path: source_resolved.wasm_path,
            kind: PluginKind::Source,
            plugin_id: src_id,
            plugin_version: src_ver,
            config: pipeline.source.config.clone(),
            streams: stream_names.clone(),
            reason: reason.to_string(),
            permissions: src_permissions,
            sandbox_overrides: src_overrides,
        })
        .await?;
    all_actions.extend(source_report.actions);

    // Destination teardown (e.g., drop staging tables)
    let (dst_id, dst_ver) = parse_plugin_id(&pipeline.destination.use_ref);
    let dst_permissions = extract_permissions(&dest_resolved);
    let dst_overrides = build_sandbox_overrides(
        pipeline.destination.permissions.as_ref(),
        pipeline.destination.limits.as_ref(),
        dest_resolved.manifest.as_ref(),
    )?;

    let dest_report = ctx
        .runner
        .teardown(&TeardownParams {
            wasm_path: dest_resolved.wasm_path,
            kind: PluginKind::Destination,
            plugin_id: dst_id,
            plugin_version: dst_ver,
            config: pipeline.destination.config.clone(),
            streams: stream_names,
            reason: reason.to_string(),
            permissions: dst_permissions,
            sandbox_overrides: dst_overrides,
        })
        .await?;
    all_actions.extend(dest_report.actions);

    Ok(TeardownReport {
        actions: all_actions,
    })
}

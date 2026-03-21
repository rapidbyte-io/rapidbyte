//! Check-pipeline use case.
//!
//! Resolves all plugins in a pipeline configuration and validates each
//! one, producing a [`CheckResult`] that summarizes the health of
//! every component.

use rapidbyte_pipeline_config::PipelineConfig;
use rapidbyte_types::schema::{FieldConstraint, FieldRequirement, StreamSchema};
use rapidbyte_types::validation::{ValidationReport, ValidationStatus};
use rapidbyte_types::wire::PluginKind;

use crate::application::context::EngineContext;
use crate::application::{build_sandbox_overrides, extract_permissions, parse_plugin_id};
use crate::domain::error::PipelineError;
use crate::domain::outcome::{CheckResult, CheckStatus, StreamNegotiationResult};
use crate::domain::ports::runner::{
    CheckComponentStatus, DiscoverParams, PrerequisitesParams, ValidateParams,
};

/// Resolve and validate all plugins in a pipeline configuration.
///
/// For each component (source, destination, transforms) this function:
/// 1. Resolves the plugin reference to a WASM module path.
/// 2. Checks for manifest presence.
/// 3. Validates the plugin configuration via the runner port.
///
/// The result aggregates validation statuses for every component so
/// callers can display a comprehensive health check.
///
/// # Errors
///
/// Returns `PipelineError` if plugin resolution fails for any
/// component (resolution failures are fatal). Individual validation
/// failures are captured in the returned `CheckResult` rather than
/// causing an early return.
#[allow(clippy::too_many_lines, clippy::missing_panics_doc)]
pub async fn check_pipeline(
    ctx: &EngineContext,
    pipeline: &PipelineConfig,
) -> Result<CheckResult, PipelineError> {
    // -- Source --
    let source_resolved = ctx
        .resolver
        .resolve(
            &pipeline.source.use_ref,
            PluginKind::Source,
            Some(&pipeline.source.config),
        )
        .await?;

    let source_manifest = source_resolved.manifest.as_ref().map(|_| CheckStatus {
        ok: true,
        message: String::new(),
    });

    let source_config = source_resolved
        .manifest
        .as_ref()
        .map(|manifest| config_check(&pipeline.source.use_ref, &pipeline.source.config, manifest));

    let (src_id, src_ver) = parse_plugin_id(&pipeline.source.use_ref);
    let source_permissions = extract_permissions(&source_resolved);
    let source_overrides = build_sandbox_overrides(
        pipeline.source.permissions.as_ref(),
        pipeline.source.limits.as_ref(),
        source_resolved.manifest.as_ref(),
    )?;

    let source_stream_names: Vec<String> = if pipeline.source.streams.is_empty() {
        vec!["check".to_string()]
    } else {
        pipeline
            .source
            .streams
            .iter()
            .map(|s| s.name.clone())
            .collect()
    };

    // Validate the source against every stream so stream-specific failures
    // are not missed.
    let mut source_validation = None;
    for stream_name in &source_stream_names {
        let validation = ctx
            .runner
            .validate_plugin(&ValidateParams {
                wasm_path: source_resolved.wasm_path.clone(),
                kind: PluginKind::Source,
                plugin_id: src_id.clone(),
                plugin_version: src_ver.clone(),
                config: pipeline.source.config.clone(),
                stream_name: stream_name.clone(),
                permissions: source_permissions.clone(),
                sandbox_overrides: source_overrides.clone(),
                upstream_schema: None,
            })
            .await?;

        // Keep the worst validation result across all streams
        let dominated = match source_validation.as_ref() {
            None => true,
            Some(prev) => severity(&validation) > severity(prev),
        };
        if dominated {
            source_validation = Some(validation);
        }
    }
    let source_validation = source_validation.expect("at least one stream name");

    // -- Destination --
    let dest_resolved = ctx
        .resolver
        .resolve(
            &pipeline.destination.use_ref,
            PluginKind::Destination,
            Some(&pipeline.destination.config),
        )
        .await?;

    let destination_manifest = dest_resolved.manifest.as_ref().map(|_| CheckStatus {
        ok: true,
        message: String::new(),
    });

    let destination_config = dest_resolved.manifest.as_ref().map(|manifest| {
        config_check(
            &pipeline.destination.use_ref,
            &pipeline.destination.config,
            manifest,
        )
    });

    let (dst_id, dst_ver) = parse_plugin_id(&pipeline.destination.use_ref);
    let dest_permissions = extract_permissions(&dest_resolved);
    let dest_overrides = build_sandbox_overrides(
        pipeline.destination.permissions.as_ref(),
        pipeline.destination.limits.as_ref(),
        dest_resolved.manifest.as_ref(),
    )?;

    // Validate the destination against every stream so stream-specific
    // failures are not missed.
    let mut destination_validation = None;
    for stream_name in &source_stream_names {
        let validation = ctx
            .runner
            .validate_plugin(&ValidateParams {
                wasm_path: dest_resolved.wasm_path.clone(),
                kind: PluginKind::Destination,
                plugin_id: dst_id.clone(),
                plugin_version: dst_ver.clone(),
                config: pipeline.destination.config.clone(),
                stream_name: stream_name.clone(),
                permissions: dest_permissions.clone(),
                sandbox_overrides: dest_overrides.clone(),
                upstream_schema: None,
            })
            .await?;

        // Keep the worst validation result across all streams
        let dominated = match destination_validation.as_ref() {
            None => true,
            Some(prev) => severity(&validation) > severity(prev),
        };
        if dominated {
            destination_validation = Some(validation);
        }
    }
    let mut destination_validation = destination_validation.expect("at least one stream name");

    // -- Transforms --
    let mut transform_configs = Vec::with_capacity(pipeline.transforms.len());
    let mut transform_validations = Vec::with_capacity(pipeline.transforms.len());

    for transform in &pipeline.transforms {
        let transform_resolved = ctx
            .resolver
            .resolve(
                &transform.use_ref,
                PluginKind::Transform,
                Some(&transform.config),
            )
            .await?;

        // Always push a result per transform to maintain positional alignment
        // with pipeline.transforms (CLI labels by index).
        transform_configs.push(match transform_resolved.manifest.as_ref() {
            Some(manifest) => config_check(&transform.use_ref, &transform.config, manifest),
            None => CheckStatus {
                ok: true,
                message: String::new(),
            },
        });

        let (t_id, t_ver) = parse_plugin_id(&transform.use_ref);
        let transform_permissions = extract_permissions(&transform_resolved);
        let transform_overrides = build_sandbox_overrides(
            transform.permissions.as_ref(),
            transform.limits.as_ref(),
            transform_resolved.manifest.as_ref(),
        )?;

        // Validate the transform against every source stream so that
        // stream-specific failures are not missed.
        let stream_names: Vec<String> = if pipeline.source.streams.is_empty() {
            vec!["check".to_string()]
        } else {
            pipeline
                .source
                .streams
                .iter()
                .map(|s| s.name.clone())
                .collect()
        };

        // Validate against all streams, keep the worst result per transform
        // to maintain 1:1 alignment with pipeline.transforms for CLI labeling.
        let mut worst_validation: Option<ValidationReport> = None;
        for stream_name in &stream_names {
            let validation = ctx
                .runner
                .validate_plugin(&ValidateParams {
                    wasm_path: transform_resolved.wasm_path.clone(),
                    kind: PluginKind::Transform,
                    plugin_id: t_id.clone(),
                    plugin_version: t_ver.clone(),
                    config: transform.config.clone(),
                    stream_name: stream_name.clone(),
                    permissions: transform_permissions.clone(),
                    sandbox_overrides: transform_overrides.clone(),
                    upstream_schema: None,
                })
                .await?;

            let mut result = validation.validation;
            if !result.message.is_empty() {
                result.message = format!("[stream: {}] {}", stream_name, result.message);
            }

            let dominated = worst_validation
                .as_ref()
                .is_none_or(|prev| validation_severity(&result) > validation_severity(prev));
            if dominated {
                worst_validation = Some(result);
            }
        }
        transform_validations.push(worst_validation.expect("at least one stream name"));
    }

    // -- Prerequisites --
    let source_prereqs = ctx
        .runner
        .prerequisites(&PrerequisitesParams {
            wasm_path: source_resolved.wasm_path.clone(),
            kind: PluginKind::Source,
            plugin_id: src_id.clone(),
            plugin_version: src_ver.clone(),
            config: pipeline.source.config.clone(),
            permissions: source_permissions.clone(),
            sandbox_overrides: source_overrides.clone(),
        })
        .await?;

    let dest_prereqs = ctx
        .runner
        .prerequisites(&PrerequisitesParams {
            wasm_path: dest_resolved.wasm_path.clone(),
            kind: PluginKind::Destination,
            plugin_id: dst_id.clone(),
            plugin_version: dst_ver.clone(),
            config: pipeline.destination.config.clone(),
            permissions: dest_permissions.clone(),
            sandbox_overrides: dest_overrides.clone(),
        })
        .await?;

    // -- Schema Negotiation --
    // Discover source schemas, then thread each stream's schema through
    // transforms and into the destination for field-level reconciliation.
    // Discovery failure is non-fatal: we skip negotiation but still report
    // the other check results.
    let mut schema_negotiation = Vec::new();
    let mut schema_aware_transform_validations = vec![None; pipeline.transforms.len()];
    let mut schema_aware_destination_validation: Option<ValidationReport> = None;

    let discovered = ctx
        .runner
        .discover(&DiscoverParams {
            wasm_path: source_resolved.wasm_path.clone(),
            plugin_id: src_id.clone(),
            plugin_version: src_ver.clone(),
            config: pipeline.source.config.clone(),
            permissions: source_permissions.clone(),
            sandbox_overrides: source_overrides.clone(),
        })
        .await;

    if let Ok(discovered_streams) = discovered {
        for stream_name in &source_stream_names {
            let source_schema = discovered_streams
                .iter()
                .find(|d| d.name == *stream_name)
                .map(|d| d.schema.clone());

            let mut current_schema = source_schema;

            // Thread through transforms
            let mut schema_validation_failed = false;
            for (transform_index, transform) in pipeline.transforms.iter().enumerate() {
                if current_schema.is_none() {
                    break;
                }
                let transform_resolved = ctx
                    .resolver
                    .resolve(
                        &transform.use_ref,
                        PluginKind::Transform,
                        Some(&transform.config),
                    )
                    .await?;

                let (t_id, t_ver) = parse_plugin_id(&transform.use_ref);
                let transform_permissions = extract_permissions(&transform_resolved);
                let transform_overrides = build_sandbox_overrides(
                    transform.permissions.as_ref(),
                    transform.limits.as_ref(),
                    transform_resolved.manifest.as_ref(),
                )?;

                let report = ctx
                    .runner
                    .validate_plugin(&ValidateParams {
                        wasm_path: transform_resolved.wasm_path.clone(),
                        kind: PluginKind::Transform,
                        plugin_id: t_id,
                        plugin_version: t_ver,
                        config: transform.config.clone(),
                        stream_name: stream_name.clone(),
                        permissions: transform_permissions,
                        sandbox_overrides: transform_overrides,
                        upstream_schema: current_schema.clone(),
                    })
                    .await?;

                let mut validation = report.validation;
                if !validation.message.is_empty() {
                    validation.message =
                        format!("[stream: {}] {}", stream_name, validation.message);
                }

                let update_transform_validation = schema_aware_transform_validations
                    .get(transform_index)
                    .and_then(Option::as_ref)
                    .is_none_or(|prev| {
                        validation_severity(&validation) > validation_severity(prev)
                    });
                if update_transform_validation {
                    schema_aware_transform_validations[transform_index] = Some(validation.clone());
                }

                if validation.status == ValidationStatus::Failed {
                    schema_validation_failed = true;
                    break;
                }

                if let Some(output) = validation.output_schema {
                    current_schema = Some(output);
                }
            }

            if schema_validation_failed {
                continue;
            }

            // Validate destination with final schema
            let dest_report = ctx
                .runner
                .validate_plugin(&ValidateParams {
                    wasm_path: dest_resolved.wasm_path.clone(),
                    kind: PluginKind::Destination,
                    plugin_id: dst_id.clone(),
                    plugin_version: dst_ver.clone(),
                    config: pipeline.destination.config.clone(),
                    stream_name: stream_name.clone(),
                    permissions: dest_permissions.clone(),
                    sandbox_overrides: dest_overrides.clone(),
                    upstream_schema: current_schema.clone(),
                })
                .await?;

            let mut dest_validation = dest_report.validation;
            if !dest_validation.message.is_empty() {
                dest_validation.message =
                    format!("[stream: {}] {}", stream_name, dest_validation.message);
            }
            let update_destination_validation = schema_aware_destination_validation
                .as_ref()
                .is_none_or(|prev| {
                    validation_severity(&dest_validation) > validation_severity(prev)
                });
            if update_destination_validation {
                schema_aware_destination_validation = Some(dest_validation.clone());
            }

            // Reconcile field requirements against the final schema
            if dest_validation.status != ValidationStatus::Failed {
                if let (Some(schema), Some(reqs)) =
                    (&current_schema, &dest_validation.field_requirements)
                {
                    let result = reconcile_schema(schema, reqs);
                    schema_negotiation.push(StreamNegotiationResult {
                        stream_name: stream_name.clone(),
                        passed: result.passed,
                        errors: result.errors,
                        warnings: result.warnings,
                    });
                }
            }
        }
    }

    for (index, schema_aware) in schema_aware_transform_validations.into_iter().enumerate() {
        if let Some(schema_aware) = schema_aware {
            let replace = validation_severity(&schema_aware)
                >= validation_severity(&transform_validations[index]);
            if replace {
                transform_validations[index] = schema_aware;
            }
        }
    }

    if let Some(schema_aware) = schema_aware_destination_validation {
        if validation_severity(&schema_aware)
            >= validation_severity(&destination_validation.validation)
        {
            destination_validation.validation = schema_aware;
        }
    }

    // -- State --
    // State connectivity is intentionally not validated during `check`.
    // The check use case validates plugin WASM modules and configuration
    // schemas, not infrastructure connectivity. State backend connectivity
    // is validated at run time when `build_run_context` connects to the
    // Postgres backend. The lightweight context used here has a no-op
    // state backend, so a real connectivity test is not possible.
    let state = CheckStatus {
        ok: true,
        message: "state backend not validated during check (validated at run time)".into(),
    };

    Ok(CheckResult {
        source_manifest,
        destination_manifest,
        source_config,
        destination_config,
        transform_configs,
        source_validation: source_validation.validation,
        destination_validation: destination_validation.validation,
        transform_validations,
        state,
        source_prerequisites: Some(source_prereqs),
        destination_prerequisites: Some(dest_prereqs),
        schema_negotiation,
    })
}

// ---------------------------------------------------------------------------
// Schema reconciliation
// ---------------------------------------------------------------------------

/// Result of reconciling a schema against a set of field requirements.
struct ReconciliationResult {
    passed: bool,
    errors: Vec<String>,
    warnings: Vec<String>,
}

/// Check whether a [`StreamSchema`] satisfies all [`FieldRequirement`]s.
///
/// - `FieldRequired` + missing field => error
/// - `FieldForbidden` + present field => error
/// - `TypeIncompatible` => always error (with accepted types)
/// - `FieldRecommended` + missing field => warning (non-fatal)
/// - `FieldOptional` or satisfied constraints => ok
fn reconcile_schema(
    schema: &StreamSchema,
    requirements: &[FieldRequirement],
) -> ReconciliationResult {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    for req in requirements {
        let field = schema.fields.iter().find(|f| f.name == req.field_name);
        match (&req.constraint, field) {
            (FieldConstraint::FieldRequired, None) => {
                errors.push(format!(
                    "Required field '{}' missing: {}",
                    req.field_name, req.reason
                ));
            }
            (FieldConstraint::FieldForbidden, Some(_)) => {
                errors.push(format!(
                    "Forbidden field '{}' present: {}",
                    req.field_name, req.reason
                ));
            }
            (FieldConstraint::TypeIncompatible, _) => {
                let accepted = req
                    .accepted_types
                    .as_ref()
                    .map(|t| t.join(", "))
                    .unwrap_or_default();
                errors.push(format!(
                    "Type incompatible for '{}': {} (accepted: {})",
                    req.field_name, req.reason, accepted
                ));
            }
            (FieldConstraint::FieldRecommended, None) => {
                warnings.push(format!(
                    "Recommended field '{}' missing: {}",
                    req.field_name, req.reason
                ));
            }
            _ => {} // FieldOptional, or constraint satisfied
        }
    }

    ReconciliationResult {
        passed: errors.is_empty(),
        errors,
        warnings,
    }
}

/// Return a numeric severity for a validation status so we can keep the
/// worst result across streams: Failed (2) > Warning (1) > Ok (0).
fn severity(status: &CheckComponentStatus) -> u8 {
    validation_severity(&status.validation)
}

fn validation_severity(result: &ValidationReport) -> u8 {
    match result.status {
        ValidationStatus::Failed => 2,
        ValidationStatus::Warning => 1,
        ValidationStatus::Success => 0,
    }
}

/// Validate plugin config against the manifest's JSON schema (if any).
fn config_check(
    plugin_ref: &str,
    config: &serde_json::Value,
    manifest: &rapidbyte_types::manifest::PluginManifest,
) -> CheckStatus {
    match crate::adapter::registry_resolver::validate_config_against_schema(
        plugin_ref, config, manifest,
    ) {
        Ok(()) => CheckStatus {
            ok: true,
            message: String::new(),
        },
        Err(e) => CheckStatus {
            ok: false,
            message: e.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::testing::{fake_context, test_resolved_plugin};
    use crate::domain::ports::runner::CheckComponentStatus;
    use rapidbyte_types::schema::SchemaField;
    use rapidbyte_types::validation::{ValidationReport, ValidationStatus};

    fn ok_validation() -> CheckComponentStatus {
        CheckComponentStatus {
            validation: ValidationReport::success(""),
        }
    }

    fn failed_validation(msg: &str) -> CheckComponentStatus {
        CheckComponentStatus {
            validation: ValidationReport::failed(msg),
        }
    }

    fn test_pipeline_config() -> PipelineConfig {
        serde_yaml::from_str(
            r#"
version: "1.0"
pipeline: test-pipeline
source:
    use: src
    config: {}
    streams:
        - name: users
          sync_mode: full_refresh
destination:
    use: dst
    config: {}
    write_mode: append
"#,
        )
        .expect("test yaml should parse")
    }

    fn test_pipeline_config_with_transform() -> PipelineConfig {
        serde_yaml::from_str(
            r#"
version: "1.0"
pipeline: test-pipeline-tx
source:
    use: src
    config: {}
    streams:
        - name: users
          sync_mode: full_refresh
transforms:
    - use: sql-transform
      config:
          query: "SELECT * FROM users"
destination:
    use: dst
    config: {}
    write_mode: append
"#,
        )
        .expect("test yaml should parse")
    }

    #[tokio::test]
    async fn check_validates_all_components() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner.enqueue_validate(Ok(ok_validation()));

        let pipeline = test_pipeline_config();
        let result = check_pipeline(&tc.ctx, &pipeline).await.unwrap();

        assert_eq!(result.source_validation.status, ValidationStatus::Success);
        assert_eq!(
            result.destination_validation.status,
            ValidationStatus::Success
        );
        assert!(result.transform_validations.is_empty());
        assert!(result.state.ok);
    }

    #[tokio::test]
    async fn check_includes_transforms() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        tc.resolver
            .register("sql-transform", test_resolved_plugin());
        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner.enqueue_validate(Ok(ok_validation()));

        let pipeline = test_pipeline_config_with_transform();
        let result = check_pipeline(&tc.ctx, &pipeline).await.unwrap();

        assert_eq!(result.transform_validations.len(), 1);
        assert_eq!(
            result.transform_validations[0].status,
            ValidationStatus::Success
        );
    }

    #[tokio::test]
    async fn check_returns_error_on_source_resolution_failure() {
        let tc = fake_context();
        // Don't register source plugin — resolver will fail.
        tc.resolver.register("dst", test_resolved_plugin());

        let pipeline = test_pipeline_config();
        let result = check_pipeline(&tc.ctx, &pipeline).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn check_returns_error_on_dest_resolution_failure() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.runner.enqueue_validate(Ok(ok_validation()));
        // Don't register destination plugin — resolver will fail.

        let pipeline = test_pipeline_config();
        let result = check_pipeline(&tc.ctx, &pipeline).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn check_captures_validation_failure() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        tc.runner
            .enqueue_validate(Ok(failed_validation("connection refused")));
        tc.runner.enqueue_validate(Ok(ok_validation()));

        let pipeline = test_pipeline_config();
        let result = check_pipeline(&tc.ctx, &pipeline).await.unwrap();

        assert_eq!(result.source_validation.status, ValidationStatus::Failed);
        assert_eq!(result.source_validation.message, "connection refused");
        assert_eq!(
            result.destination_validation.status,
            ValidationStatus::Success
        );
    }

    #[tokio::test]
    async fn check_no_manifest_means_none_for_manifest_and_config() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner.enqueue_validate(Ok(ok_validation()));

        let pipeline = test_pipeline_config();
        let result = check_pipeline(&tc.ctx, &pipeline).await.unwrap();

        // No manifest on the resolved plugin means these are None.
        assert!(result.source_manifest.is_none());
        assert!(result.destination_manifest.is_none());
        assert!(result.source_config.is_none());
        assert!(result.destination_config.is_none());
    }

    // -------------------------------------------------------------------
    // Test: Check with no transforms
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn check_with_no_transforms() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner.enqueue_validate(Ok(ok_validation()));

        // Pipeline with empty transforms (test_pipeline_config has none)
        let pipeline = test_pipeline_config();
        let result = check_pipeline(&tc.ctx, &pipeline).await.unwrap();

        assert!(result.transform_validations.is_empty());
        assert!(result.transform_configs.is_empty());
        assert_eq!(result.source_validation.status, ValidationStatus::Success);
        assert_eq!(
            result.destination_validation.status,
            ValidationStatus::Success
        );
        assert!(result.state.ok);
    }

    // -------------------------------------------------------------------
    // Test: Check transform resolution failure
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn check_transform_resolution_failure() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        // Don't register "sql-transform" — resolution will fail
        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner.enqueue_validate(Ok(ok_validation()));

        let pipeline = test_pipeline_config_with_transform();
        let result = check_pipeline(&tc.ctx, &pipeline).await;

        assert!(
            result.is_err(),
            "transform resolution failure should return error"
        );
    }

    // -------------------------------------------------------------------
    // Test: Check multiple transforms all validated
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn check_multiple_transforms_all_validated() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        tc.resolver.register("tx1", test_resolved_plugin());
        tc.resolver.register("tx2", test_resolved_plugin());
        tc.resolver.register("tx3", test_resolved_plugin());

        // source + destination + 3 transforms = 5 validate calls
        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner.enqueue_validate(Ok(ok_validation()));

        let pipeline: PipelineConfig = serde_yaml::from_str(
            r#"
version: "1.0"
pipeline: test-pipeline-multi-tx
source:
    use: src
    config: {}
    streams:
        - name: users
          sync_mode: full_refresh
transforms:
    - use: tx1
      config: {}
    - use: tx2
      config: {}
    - use: tx3
      config: {}
destination:
    use: dst
    config: {}
    write_mode: append
"#,
        )
        .expect("test yaml should parse");

        let result = check_pipeline(&tc.ctx, &pipeline).await.unwrap();

        assert_eq!(result.transform_validations.len(), 3);
        for v in &result.transform_validations {
            assert_eq!(v.status, ValidationStatus::Success);
        }
    }

    // -------------------------------------------------------------------
    // Test: Transform validation covers all streams
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn check_transform_validates_all_streams() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        tc.resolver
            .register("sql-transform", test_resolved_plugin());

        // source * 2 streams + destination * 2 streams + 1 transform * 2 streams = 6 validate calls
        tc.runner.enqueue_validate(Ok(ok_validation())); // source for stream 1
        tc.runner.enqueue_validate(Ok(ok_validation())); // source for stream 2
        tc.runner.enqueue_validate(Ok(ok_validation())); // destination for stream 1
        tc.runner.enqueue_validate(Ok(ok_validation())); // destination for stream 2
        tc.runner.enqueue_validate(Ok(ok_validation())); // transform for stream 1
        tc.runner
            .enqueue_validate(Ok(failed_validation("transform fails on orders"))); // transform for stream 2

        let pipeline: PipelineConfig = serde_yaml::from_str(
            r#"
version: "1.0"
pipeline: test-multi-stream-tx
source:
    use: src
    config: {}
    streams:
        - name: users
          sync_mode: full_refresh
        - name: orders
          sync_mode: full_refresh
transforms:
    - use: sql-transform
      config:
          query: "SELECT * FROM input"
destination:
    use: dst
    config: {}
    write_mode: append
"#,
        )
        .expect("test yaml should parse");

        let result = check_pipeline(&tc.ctx, &pipeline).await.unwrap();

        // 1 transform, worst-of-2-streams = 1 validation (keeps the failure)
        assert_eq!(result.transform_validations.len(), 1);
        assert_eq!(
            result.transform_validations[0].status,
            ValidationStatus::Failed
        );
        assert!(
            result.transform_validations[0]
                .message
                .contains("transform fails on orders"),
            "expected transform failure message, got: {}",
            result.transform_validations[0].message
        );
    }

    // -------------------------------------------------------------------
    // Test: N transforms × M streams with mixed outcomes per transform
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn check_multi_transform_multi_stream_keeps_worst_per_transform() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        tc.resolver.register("tx-a", test_resolved_plugin());
        tc.resolver.register("tx-b", test_resolved_plugin());

        // 2 streams × (source + dest + 2 transforms) = 2+2+4 = 8 validate calls
        // Source: ok, ok
        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner.enqueue_validate(Ok(ok_validation()));
        // Destination: ok, ok
        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner.enqueue_validate(Ok(ok_validation()));
        // Transform tx-a: FAILS on stream users, ok on stream orders
        tc.runner
            .enqueue_validate(Ok(failed_validation("tx-a fails on users")));
        tc.runner.enqueue_validate(Ok(ok_validation()));
        // Transform tx-b: ok on stream users, FAILS on stream orders
        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner
            .enqueue_validate(Ok(failed_validation("tx-b fails on orders")));

        let pipeline: PipelineConfig = serde_yaml::from_str(
            r#"
version: "1.0"
pipeline: test-n-transforms-m-streams
source:
    use: src
    config: {}
    streams:
        - name: users
          sync_mode: full_refresh
        - name: orders
          sync_mode: full_refresh
transforms:
    - use: tx-a
      config: {}
    - use: tx-b
      config: {}
destination:
    use: dst
    config: {}
    write_mode: append
"#,
        )
        .expect("test yaml should parse");

        let result = check_pipeline(&tc.ctx, &pipeline).await.unwrap();

        // 2 transforms, each keeping worst-of-2-streams
        assert_eq!(
            result.transform_validations.len(),
            2,
            "should have exactly 1 validation per transform"
        );

        // tx-a (index 0): failed on users → worst is Failed
        assert_eq!(
            result.transform_validations[0].status,
            ValidationStatus::Failed,
            "transform 0 (tx-a) should show failure from stream 'users'"
        );
        assert!(
            result.transform_validations[0]
                .message
                .contains("tx-a fails on users"),
            "transform 0 message should reference 'tx-a fails on users', got: {}",
            result.transform_validations[0].message
        );

        // tx-b (index 1): failed on orders → worst is Failed
        assert_eq!(
            result.transform_validations[1].status,
            ValidationStatus::Failed,
            "transform 1 (tx-b) should show failure from stream 'orders'"
        );
        assert!(
            result.transform_validations[1]
                .message
                .contains("tx-b fails on orders"),
            "transform 1 message should reference 'tx-b fails on orders', got: {}",
            result.transform_validations[1].message
        );
    }

    // -------------------------------------------------------------------
    // Test: Check mixed validation results
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn check_mixed_validation_results() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());

        // Source validation succeeds, destination validation fails
        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner
            .enqueue_validate(Ok(failed_validation("cannot connect to destination")));

        let pipeline = test_pipeline_config();
        let result = check_pipeline(&tc.ctx, &pipeline).await.unwrap();

        assert_eq!(result.source_validation.status, ValidationStatus::Success);
        assert_eq!(
            result.destination_validation.status,
            ValidationStatus::Failed
        );
        assert_eq!(
            result.destination_validation.message,
            "cannot connect to destination"
        );
    }

    // ===================================================================
    // Schema reconciliation unit tests
    // ===================================================================

    #[test]
    fn reconcile_all_satisfied() {
        let schema = StreamSchema {
            fields: vec![
                SchemaField::new("id", "int64", false),
                SchemaField::new("name", "utf8", true),
            ],
            primary_key: vec!["id".into()],
            ..Default::default()
        };
        let reqs = vec![FieldRequirement {
            field_name: "id".into(),
            constraint: FieldConstraint::FieldRequired,
            reason: "primary key".into(),
            accepted_types: None,
        }];
        let result = reconcile_schema(&schema, &reqs);
        assert!(result.passed);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn reconcile_required_field_missing() {
        let schema = StreamSchema {
            fields: vec![SchemaField::new("name", "utf8", true)],
            ..Default::default()
        };
        let reqs = vec![FieldRequirement {
            field_name: "id".into(),
            constraint: FieldConstraint::FieldRequired,
            reason: "primary key".into(),
            accepted_types: None,
        }];
        let result = reconcile_schema(&schema, &reqs);
        assert!(!result.passed);
        assert!(result.errors[0].contains("Required field 'id' missing"));
    }

    #[test]
    fn reconcile_forbidden_field_present() {
        let schema = StreamSchema {
            fields: vec![
                SchemaField::new("id", "int64", false),
                SchemaField::new("_internal", "utf8", true),
            ],
            ..Default::default()
        };
        let reqs = vec![FieldRequirement {
            field_name: "_internal".into(),
            constraint: FieldConstraint::FieldForbidden,
            reason: "reserved".into(),
            accepted_types: None,
        }];
        let result = reconcile_schema(&schema, &reqs);
        assert!(!result.passed);
    }

    #[test]
    fn reconcile_recommended_missing_is_warning() {
        let schema = StreamSchema {
            fields: vec![SchemaField::new("id", "int64", false)],
            ..Default::default()
        };
        let reqs = vec![FieldRequirement {
            field_name: "sort_key".into(),
            constraint: FieldConstraint::FieldRecommended,
            reason: "improves query perf".into(),
            accepted_types: None,
        }];
        let result = reconcile_schema(&schema, &reqs);
        assert!(result.passed); // Warnings don't fail
        assert_eq!(result.warnings.len(), 1);
    }

    #[test]
    fn reconcile_type_incompatible() {
        let schema = StreamSchema {
            fields: vec![SchemaField::new("age", "float64", true)],
            ..Default::default()
        };
        let reqs = vec![FieldRequirement {
            field_name: "age".into(),
            constraint: FieldConstraint::TypeIncompatible,
            reason: "float not supported".into(),
            accepted_types: Some(vec!["int32".into(), "int64".into()]),
        }];
        let result = reconcile_schema(&schema, &reqs);
        assert!(!result.passed);
        assert!(result.errors[0].contains("Type incompatible"));
        assert!(result.errors[0].contains("int32, int64"));
    }

    #[test]
    fn reconcile_optional_missing_is_ok() {
        let schema = StreamSchema {
            fields: vec![SchemaField::new("id", "int64", false)],
            ..Default::default()
        };
        let reqs = vec![FieldRequirement {
            field_name: "optional_field".into(),
            constraint: FieldConstraint::FieldOptional,
            reason: "not needed".into(),
            accepted_types: None,
        }];
        let result = reconcile_schema(&schema, &reqs);
        assert!(result.passed);
        assert!(result.errors.is_empty());
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn reconcile_mixed_constraints() {
        let schema = StreamSchema {
            fields: vec![
                SchemaField::new("id", "int64", false),
                SchemaField::new("_secret", "utf8", true),
            ],
            ..Default::default()
        };
        let reqs = vec![
            FieldRequirement {
                field_name: "id".into(),
                constraint: FieldConstraint::FieldRequired,
                reason: "pk".into(),
                accepted_types: None,
            },
            FieldRequirement {
                field_name: "email".into(),
                constraint: FieldConstraint::FieldRequired,
                reason: "needed for dedup".into(),
                accepted_types: None,
            },
            FieldRequirement {
                field_name: "_secret".into(),
                constraint: FieldConstraint::FieldForbidden,
                reason: "internal only".into(),
                accepted_types: None,
            },
            FieldRequirement {
                field_name: "sort_key".into(),
                constraint: FieldConstraint::FieldRecommended,
                reason: "perf".into(),
                accepted_types: None,
            },
        ];
        let result = reconcile_schema(&schema, &reqs);
        assert!(!result.passed);
        // 2 errors: email missing + _secret forbidden
        assert_eq!(result.errors.len(), 2);
        // 1 warning: sort_key recommended
        assert_eq!(result.warnings.len(), 1);
    }

    #[test]
    fn reconcile_empty_requirements() {
        let schema = StreamSchema {
            fields: vec![SchemaField::new("id", "int64", false)],
            ..Default::default()
        };
        let result = reconcile_schema(&schema, &[]);
        assert!(result.passed);
        assert!(result.errors.is_empty());
        assert!(result.warnings.is_empty());
    }

    // ===================================================================
    // Schema negotiation integration tests
    // ===================================================================

    #[tokio::test]
    async fn check_includes_prerequisites() {
        use rapidbyte_types::validation::PrerequisitesReport;

        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner
            .enqueue_prerequisites(Ok(PrerequisitesReport::passed()));
        tc.runner
            .enqueue_prerequisites(Ok(PrerequisitesReport::passed()));

        let pipeline = test_pipeline_config();
        let result = check_pipeline(&tc.ctx, &pipeline).await.unwrap();

        assert!(result.source_prerequisites.is_some());
        assert!(result.source_prerequisites.unwrap().passed);
        assert!(result.destination_prerequisites.is_some());
        assert!(result.destination_prerequisites.unwrap().passed);
    }

    #[tokio::test]
    async fn check_schema_negotiation_with_discover() {
        use crate::domain::ports::runner::DiscoveredStream;
        use rapidbyte_types::schema::SchemaField;

        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());

        // Source and destination validation (initial per-component pass)
        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner.enqueue_validate(Ok(ok_validation()));

        // Discover returns stream with schema
        let users_schema = StreamSchema {
            fields: vec![
                SchemaField::new("id", "int64", false),
                SchemaField::new("name", "utf8", true),
            ],
            primary_key: vec!["id".into()],
            ..Default::default()
        };
        tc.runner.enqueue_discover(Ok(vec![DiscoveredStream {
            name: "users".into(),
            schema: users_schema,
            supported_sync_modes: vec![rapidbyte_types::wire::SyncMode::FullRefresh],
            default_cursor_field: None,
            estimated_row_count: None,
            metadata_json: None,
        }]));

        // Destination validate with upstream_schema returns field requirements
        let dest_report = CheckComponentStatus {
            validation: ValidationReport::success("ok").with_field_requirements(vec![
                FieldRequirement {
                    field_name: "id".into(),
                    constraint: FieldConstraint::FieldRequired,
                    reason: "primary key".into(),
                    accepted_types: None,
                },
            ]),
        };
        tc.runner.enqueue_validate(Ok(dest_report));

        let pipeline = test_pipeline_config();
        let result = check_pipeline(&tc.ctx, &pipeline).await.unwrap();

        assert_eq!(result.schema_negotiation.len(), 1);
        assert!(result.schema_negotiation[0].passed);
        assert_eq!(result.schema_negotiation[0].stream_name, "users");
        assert!(result.schema_negotiation[0].errors.is_empty());
    }

    #[tokio::test]
    async fn check_schema_negotiation_detects_missing_required_field() {
        use crate::domain::ports::runner::DiscoveredStream;
        use rapidbyte_types::schema::SchemaField;

        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());

        // Source and destination validation (initial per-component pass)
        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner.enqueue_validate(Ok(ok_validation()));

        // Discover returns stream with schema missing "email"
        let users_schema = StreamSchema {
            fields: vec![SchemaField::new("id", "int64", false)],
            primary_key: vec!["id".into()],
            ..Default::default()
        };
        tc.runner.enqueue_discover(Ok(vec![DiscoveredStream {
            name: "users".into(),
            schema: users_schema,
            supported_sync_modes: vec![rapidbyte_types::wire::SyncMode::FullRefresh],
            default_cursor_field: None,
            estimated_row_count: None,
            metadata_json: None,
        }]));

        // Destination requires "email" field
        let dest_report = CheckComponentStatus {
            validation: ValidationReport::success("ok").with_field_requirements(vec![
                FieldRequirement {
                    field_name: "email".into(),
                    constraint: FieldConstraint::FieldRequired,
                    reason: "dedup key".into(),
                    accepted_types: None,
                },
            ]),
        };
        tc.runner.enqueue_validate(Ok(dest_report));

        let pipeline = test_pipeline_config();
        let result = check_pipeline(&tc.ctx, &pipeline).await.unwrap();

        assert_eq!(result.schema_negotiation.len(), 1);
        assert!(!result.schema_negotiation[0].passed);
        assert!(result.schema_negotiation[0].errors[0].contains("Required field 'email' missing"));
    }

    #[tokio::test]
    async fn check_schema_negotiation_surfaces_transform_failure() {
        use crate::domain::ports::runner::DiscoveredStream;
        use rapidbyte_types::schema::SchemaField;

        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        tc.resolver
            .register("sql-transform", test_resolved_plugin());

        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner.enqueue_validate(Ok(ok_validation()));

        let users_schema = StreamSchema {
            fields: vec![SchemaField::new("id", "int64", false)],
            primary_key: vec!["id".into()],
            ..Default::default()
        };
        tc.runner.enqueue_discover(Ok(vec![DiscoveredStream {
            name: "users".into(),
            schema: users_schema,
            supported_sync_modes: vec![rapidbyte_types::wire::SyncMode::FullRefresh],
            default_cursor_field: None,
            estimated_row_count: None,
            metadata_json: None,
        }]));

        tc.runner.enqueue_validate(Ok(CheckComponentStatus {
            validation: ValidationReport::failed("missing required field 'email'"),
        }));
        tc.runner.enqueue_validate(Ok(ok_validation()));

        let pipeline = test_pipeline_config_with_transform();
        let result = check_pipeline(&tc.ctx, &pipeline).await.unwrap();

        assert_eq!(result.transform_validations.len(), 1);
        assert_eq!(
            result.transform_validations[0].status,
            ValidationStatus::Failed
        );
        assert!(result.transform_validations[0]
            .message
            .contains("missing required field 'email'"));
    }

    #[tokio::test]
    async fn check_schema_negotiation_surfaces_destination_failure() {
        use crate::domain::ports::runner::DiscoveredStream;
        use rapidbyte_types::schema::SchemaField;

        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());

        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner.enqueue_validate(Ok(ok_validation()));

        let users_schema = StreamSchema {
            fields: vec![SchemaField::new("id", "int64", false)],
            primary_key: vec!["id".into()],
            ..Default::default()
        };
        tc.runner.enqueue_discover(Ok(vec![DiscoveredStream {
            name: "users".into(),
            schema: users_schema,
            supported_sync_modes: vec![rapidbyte_types::wire::SyncMode::FullRefresh],
            default_cursor_field: None,
            estimated_row_count: None,
            metadata_json: None,
        }]));

        tc.runner.enqueue_validate(Ok(CheckComponentStatus {
            validation: ValidationReport::failed("destination requires column 'email'"),
        }));

        let pipeline = test_pipeline_config();
        let result = check_pipeline(&tc.ctx, &pipeline).await.unwrap();

        assert_eq!(
            result.destination_validation.status,
            ValidationStatus::Failed
        );
        assert!(result
            .destination_validation
            .message
            .contains("destination requires column 'email'"));
    }

    #[tokio::test]
    async fn check_schema_negotiation_skipped_when_discover_fails() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());

        // Source and destination validation
        tc.runner.enqueue_validate(Ok(ok_validation()));
        tc.runner.enqueue_validate(Ok(ok_validation()));

        // Discover fails (no result enqueued; FakePluginRunner returns error)
        // The default prerequisites call returns passed, discover fails gracefully.

        let pipeline = test_pipeline_config();
        let result = check_pipeline(&tc.ctx, &pipeline).await.unwrap();

        // Schema negotiation should be empty when discover fails
        assert!(result.schema_negotiation.is_empty());
    }
}

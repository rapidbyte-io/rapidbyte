//! Check-pipeline use case.
//!
//! Resolves all plugins in a pipeline configuration and validates each
//! one, producing a [`CheckResult`] that summarizes the health of
//! every component.

use rapidbyte_pipeline_config::PipelineConfig;
use rapidbyte_types::error::ValidationStatus;
use rapidbyte_types::wire::PluginKind;

use crate::application::context::EngineContext;
use crate::application::{extract_permissions, parse_plugin_id};
use crate::domain::error::PipelineError;
use crate::domain::outcome::{CheckResult, CheckStatus};
use crate::domain::ports::runner::{CheckComponentStatus, ValidateParams};

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
    let destination_validation = destination_validation.expect("at least one stream name");

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

        if let Some(manifest) = transform_resolved.manifest.as_ref() {
            transform_configs.push(config_check(
                &transform.use_ref,
                &transform.config,
                manifest,
            ));
        }

        let (t_id, t_ver) = parse_plugin_id(&transform.use_ref);
        let transform_permissions = extract_permissions(&transform_resolved);

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

        for stream_name in stream_names {
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
                })
                .await?;

            // Issue 6: Include stream and transform name in the validation
            // message so the CLI can display which stream/transform pair
            // each entry belongs to (N_transforms x N_streams entries).
            let mut result = validation.validation;
            if result.message.is_empty() {
                result.message = format!("[{}/{}]", transform.use_ref, stream_name);
            } else {
                result.message =
                    format!("[{}/{}] {}", transform.use_ref, stream_name, result.message);
            }
            transform_validations.push(result);
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
    })
}

/// Return a numeric severity for a validation status so we can keep the
/// worst result across streams: Failed (2) > Warning (1) > Ok (0).
fn severity(status: &CheckComponentStatus) -> u8 {
    match status.validation.status {
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
    use rapidbyte_types::error::{ValidationResult, ValidationStatus};

    fn ok_validation() -> CheckComponentStatus {
        CheckComponentStatus {
            validation: ValidationResult {
                status: ValidationStatus::Success,
                message: String::new(),
                warnings: vec![],
            },
        }
    }

    fn failed_validation(msg: &str) -> CheckComponentStatus {
        CheckComponentStatus {
            validation: ValidationResult {
                status: ValidationStatus::Failed,
                message: msg.to_string(),
                warnings: vec![],
            },
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

        // 1 transform * 2 streams = 2 transform validations
        assert_eq!(result.transform_validations.len(), 2);
        assert_eq!(
            result.transform_validations[0].status,
            ValidationStatus::Success
        );
        assert_eq!(
            result.transform_validations[1].status,
            ValidationStatus::Failed
        );
        assert!(
            result.transform_validations[1]
                .message
                .contains("transform fails on orders"),
            "expected transform failure message, got: {}",
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
}

//! Check-pipeline use case.
//!
//! Resolves all plugins in a pipeline configuration and validates each
//! one, producing a [`CheckResult`] that summarizes the health of
//! every component.

use rapidbyte_pipeline_config::PipelineConfig;
use rapidbyte_types::wire::PluginKind;

use crate::application::context::EngineContext;
use crate::application::{extract_permissions, parse_plugin_id};
use crate::domain::error::PipelineError;
use crate::domain::outcome::{CheckResult, CheckStatus};
use crate::domain::ports::runner::ValidateParams;

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
#[allow(clippy::too_many_lines)]
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

    let source_validation = ctx
        .runner
        .validate_plugin(&ValidateParams {
            wasm_path: source_resolved.wasm_path,
            kind: PluginKind::Source,
            plugin_id: src_id,
            plugin_version: src_ver,
            config: pipeline.source.config.clone(),
            stream_name: first_stream_name(pipeline),
            permissions: source_permissions,
        })
        .await?;

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

    let destination_validation = ctx
        .runner
        .validate_plugin(&ValidateParams {
            wasm_path: dest_resolved.wasm_path,
            kind: PluginKind::Destination,
            plugin_id: dst_id,
            plugin_version: dst_ver,
            config: pipeline.destination.config.clone(),
            stream_name: first_stream_name(pipeline),
            permissions: dest_permissions,
        })
        .await?;

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

        let validation = ctx
            .runner
            .validate_plugin(&ValidateParams {
                wasm_path: transform_resolved.wasm_path,
                kind: PluginKind::Transform,
                plugin_id: t_id,
                plugin_version: t_ver,
                config: transform.config.clone(),
                stream_name: first_stream_name(pipeline),
                permissions: transform_permissions,
            })
            .await?;
        transform_validations.push(validation.validation);
    }

    // -- State (placeholder: always OK in this use case) --
    let state = CheckStatus {
        ok: true,
        message: String::new(),
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

/// Extract the first stream name from the pipeline, falling back to "check".
fn first_stream_name(pipeline: &PipelineConfig) -> String {
    pipeline
        .source
        .streams
        .first()
        .map_or_else(|| "check".to_string(), |s| s.name.clone())
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
}

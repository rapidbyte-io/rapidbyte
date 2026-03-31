//! Engine-backed [`PipelineInspector`] that delegates to `rapidbyte_engine`.

use async_trait::async_trait;
use rapidbyte_controller::domain::ports::pipeline_inspector::{
    CheckOutput, DiffOutput, InspectorError, PipelineInspector,
};
use rapidbyte_types::validation::ValidationStatus;

/// Engine-backed [`PipelineInspector`] that delegates to the engine runtime.
pub struct EnginePipelineInspector {
    registry_config: rapidbyte_registry::RegistryConfig,
}

impl EnginePipelineInspector {
    /// Create a new inspector with the given registry configuration.
    #[must_use]
    pub fn new(registry_config: rapidbyte_registry::RegistryConfig) -> Self {
        Self { registry_config }
    }
}

fn validation_ok(status: ValidationStatus) -> bool {
    matches!(
        status,
        ValidationStatus::Success | ValidationStatus::Warning
    )
}

#[async_trait]
impl PipelineInspector for EnginePipelineInspector {
    async fn check(&self, pipeline_yaml: &str) -> Result<CheckOutput, InspectorError> {
        let config: rapidbyte_pipeline_config::PipelineConfig = serde_yaml::from_str(pipeline_yaml)
            .map_err(|e| InspectorError::Pipeline(e.to_string()))?;

        let engine_ctx =
            rapidbyte_engine::build_lightweight_context(&self.registry_config, &config)
                .await
                .map_err(|e| InspectorError::Plugin(e.to_string()))?;

        let result = rapidbyte_engine::check_pipeline(&engine_ctx, &config)
            .await
            .map_err(|e| InspectorError::Pipeline(e.to_string()))?;

        // A check passes if all validation reports are non-failing and the
        // state check passed.
        let passed = result.state.ok
            && validation_ok(result.source_validation.status)
            && validation_ok(result.destination_validation.status)
            && result
                .transform_validations
                .iter()
                .all(|v| validation_ok(v.status))
            && result.schema_negotiation.iter().all(|s| s.passed);

        let checks = serde_json::json!({
            "state": { "ok": result.state.ok, "message": result.state.message },
            "source_manifest": result.source_manifest.as_ref().map(|s| serde_json::json!({ "ok": s.ok, "message": s.message })),
            "destination_manifest": result.destination_manifest.as_ref().map(|s| serde_json::json!({ "ok": s.ok, "message": s.message })),
        });

        Ok(CheckOutput { passed, checks })
    }

    async fn diff(&self, _pipeline_yaml: &str) -> Result<DiffOutput, InspectorError> {
        // Schema diff not yet implemented in engine
        Ok(DiffOutput { streams: vec![] })
    }
}

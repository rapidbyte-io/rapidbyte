use async_trait::async_trait;
use rapidbyte_controller::domain::ports::pipeline_inspector::{
    CheckOutput, DiffOutput, InspectorError, PipelineInspector,
};

/// Engine-backed [`PipelineInspector`] that delegates to the engine runtime.
///
/// This is a stub implementation — the real delegation to `check_pipeline()`
/// requires `EngineContext` which is not yet wired. The struct exists so it
/// can be imported and wired once `EngineContext` is available.
#[allow(dead_code)]
pub struct EnginePipelineInspector;

#[async_trait]
impl PipelineInspector for EnginePipelineInspector {
    async fn check(&self, _pipeline_yaml: &str) -> Result<CheckOutput, InspectorError> {
        // TODO: delegate to engine's check_pipeline() once EngineContext is wired
        Err(InspectorError::Plugin(
            "engine context not yet wired".into(),
        ))
    }

    async fn diff(&self, _pipeline_yaml: &str) -> Result<DiffOutput, InspectorError> {
        // TODO: delegate to engine's diff_pipeline() once EngineContext is wired
        Err(InspectorError::Plugin(
            "engine context not yet wired".into(),
        ))
    }
}

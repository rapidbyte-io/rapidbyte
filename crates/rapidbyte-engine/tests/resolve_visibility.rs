//! Verify that key hexagonal API types are publicly accessible.

use rapidbyte_engine::{
    check_pipeline, discover_plugin, run_pipeline, EngineConfig, EngineContext, PipelineError,
};

#[test]
fn hexagonal_api_types_are_public() {
    // Compile-time check: these types/functions are importable from outside the crate.
    let _ = check_pipeline;
    let _ = discover_plugin;
    let _ = run_pipeline;

    // Verify EngineContext fields are accessible.
    fn _assert_engine_context_fields(ctx: &EngineContext) {
        let _ = &ctx.runner;
        let _ = &ctx.resolver;
        let _ = &ctx.cursors;
        let _ = &ctx.runs;
        let _ = &ctx.dlq;
        let _ = &ctx.progress;
        let _ = &ctx.metrics;
        let _ = &ctx.config;
    }

    fn _assert_engine_config_fields(cfg: &EngineConfig) {
        let _ = cfg.max_retries;
        let _ = cfg.channel_capacity;
    }

    fn _assert_pipeline_error_variants(err: PipelineError) {
        match err {
            PipelineError::Plugin(_) => {}
            PipelineError::Infrastructure(_) => {}
            PipelineError::Cancelled => {}
        }
    }
}

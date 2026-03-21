//! Validation transform plugin for Rapidbyte.
//!
//! Applies rule-based data contract assertions (not-null, regex, range, unique)
//! to in-flight Arrow batches, filtering or failing rows that violate constraints.

mod config;
mod transform;
mod validate;

use rapidbyte_sdk::prelude::*;

#[rapidbyte_sdk::plugin(transform)]
pub struct TransformValidate {
    config: config::CompiledConfig,
}

impl Transform for TransformValidate {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<Self, PluginError> {
        let compiled = config.compile().map_err(|message| {
            PluginError::config("VALIDATE_CONFIG", format!("Invalid validation config: {message}"))
        })?;
        Ok(Self { config: compiled })
    }

    async fn validate(
        &self,
        _ctx: &Context,
        _upstream: Option<&StreamSchema>,
    ) -> Result<ValidationReport, PluginError> {
        // Config was already validated in init() — if we got here, it's valid
        Ok(ValidationReport::success("Validation transform config is valid"))
    }

    async fn transform(
        &self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<TransformSummary, PluginError> {
        transform::run(ctx, &stream, &self.config).await
    }
}

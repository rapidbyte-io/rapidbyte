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
    config: config::Config,
    compiled: std::sync::OnceLock<Result<config::CompiledConfig, String>>,
}

impl TransformValidate {
    fn compiled_config(&self) -> Result<&config::CompiledConfig, String> {
        match self.compiled.get_or_init(|| self.config.compile()) {
            Ok(compiled) => Ok(compiled),
            Err(message) => Err(message.clone()),
        }
    }
}

impl Transform for TransformValidate {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<Self, PluginError> {
        Ok(Self {
            config,
            compiled: std::sync::OnceLock::new(),
        })
    }

    async fn validate(
        &self,
        _ctx: &Context,
        _upstream: Option<&StreamSchema>,
    ) -> Result<ValidationReport, PluginError> {
        match self.compiled_config() {
            Ok(_) => Ok(ValidationReport::success(
                "Validation transform config is valid",
            )),
            Err(message) => Ok(ValidationReport::failed(&format!(
                "Invalid validation config: {message}"
            ))),
        }
    }

    async fn transform(
        &self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<TransformSummary, PluginError> {
        let compiled = self.compiled_config().map_err(|message| {
            PluginError::config("VALIDATE_CONFIG", format!("Invalid validation config: {message}"))
        })?;
        transform::run(ctx, &stream, compiled).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn invalid_regex_config() -> config::Config {
        serde_json::from_value(serde_json::json!({
            "rules": [{ "assert_regex": { "field": "email", "pattern": "(" } }]
        }))
        .expect("config should deserialize")
    }

    #[tokio::test]
    async fn init_accepts_config_that_requires_validation() {
        let result = TransformValidate::init(invalid_regex_config()).await;
        assert!(result.is_ok(), "init should not reject invalid config");
    }

    #[tokio::test]
    async fn invalid_config_reports_validation_failure() {
        let plugin = TransformValidate::init(invalid_regex_config())
            .await
            .expect("init should not reject invalid config");
        let ctx = Context::new("transform-validate", "users");
        let validation = plugin
            .validate(&ctx, None)
            .await
            .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Failed);
        assert!(validation.message.contains("Invalid validation config"));
    }
}

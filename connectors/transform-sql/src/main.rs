//! SQL transform connector powered by Apache DataFusion.

mod config;
mod transform;

use rapidbyte_sdk::prelude::*;

#[rapidbyte_sdk::connector(transform)]
pub struct TransformSql {
    config: config::Config,
}

impl Transform for TransformSql {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
        Ok((
            Self { config },
            ConnectorInfo {
                protocol_version: ProtocolVersion::V2,
                features: vec![],
                default_max_batch_bytes: StreamLimits::DEFAULT_MAX_BATCH_BYTES,
            },
        ))
    }

    async fn validate(
        config: &Self::Config,
        ctx: &Context,
    ) -> Result<ValidationResult, ConnectorError> {
        let _ = ctx;
        if config.query.trim().is_empty() {
            return Ok(ValidationResult {
                status: ValidationStatus::Failed,
                message: "SQL query must not be empty".to_string(),
            });
        }
        Ok(ValidationResult {
            status: ValidationStatus::Success,
            message: "SQL query configuration is valid".to_string(),
        })
    }

    async fn transform(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<TransformSummary, ConnectorError> {
        transform::run(ctx, &stream, &self.config).await
    }
}

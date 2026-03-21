//! SQL transform plugin powered by Apache DataFusion.

mod config;
mod references;
mod transform;

use datafusion::sql::parser::{DFParser, Statement as DfStatement};
use rapidbyte_sdk::prelude::*;

use crate::references::validate_query_for_stream_name;

fn normalize_and_parse_query(config: &config::Config) -> Result<String, String> {
    let query = config.normalized_query()?;
    let _ = parse_plannable_statement(&query)?;
    Ok(query)
}

fn parse_plannable_statement(query: &str) -> Result<DfStatement, String> {
    let mut statements =
        DFParser::parse_sql(query).map_err(|e| format!("failed to parse SQL query: {e}"))?;
    let statement = statements
        .pop_front()
        .ok_or_else(|| "SQL query must contain exactly one statement".to_string())?;
    if !statements.is_empty() {
        return Err("SQL query must contain exactly one statement".to_string());
    }
    Ok(statement)
}

#[rapidbyte_sdk::plugin(transform)]
pub struct TransformSql {
    config: config::Config,
    prepared: std::sync::OnceLock<Result<PreparedQuery, String>>,
}

struct PreparedQuery {
    query: String,
    statement: DfStatement,
}

impl TransformSql {
    fn prepared_query(&self) -> Result<&PreparedQuery, String> {
        match self.prepared.get_or_init(|| {
            let query = normalize_and_parse_query(&self.config)?;
            let statement = parse_plannable_statement(&query)?;
            Ok(PreparedQuery { query, statement })
        }) {
            Ok(prepared) => Ok(prepared),
            Err(message) => Err(message.clone()),
        }
    }
}

impl Transform for TransformSql {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<Self, PluginError> {
        Ok(Self {
            config,
            prepared: std::sync::OnceLock::new(),
        })
    }

    async fn validate(
        &self,
        ctx: &Context,
        _upstream: Option<&rapidbyte_sdk::schema::StreamSchema>,
    ) -> Result<ValidationReport, PluginError> {
        match self
            .prepared_query()
            .and_then(|prepared| validate_query_for_stream_name(&prepared.query, ctx.stream_name()))
        {
            Ok(()) => Ok(ValidationReport::success("SQL query configuration is valid")),
            Err(message) => Ok(ValidationReport::failed(&message)),
        }
    }

    async fn transform(
        &self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<TransformSummary, PluginError> {
        let prepared = self
            .prepared_query()
            .map_err(|message| PluginError::config("SQL_CONFIG", message))?;
        validate_query_for_stream_name(&prepared.query, ctx.stream_name())
            .map_err(|message| PluginError::config("SQL_CONFIG", message))?;
        transform::run(ctx, &stream, &self.config, &prepared.statement).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn validate_reports_empty_query_failure() {
        let plugin = TransformSql::init(config::Config {
            query: "   ".to_string(),
        })
        .await
        .expect("init should not reject invalid config");
        let ctx = Context::new("transform-sql", "users");
        let validation = plugin
            .validate(&ctx, None)
            .await
            .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Failed);
        assert!(validation.message.contains("empty"));
    }

    #[tokio::test]
    async fn validate_accepts_query_after_normalization() {
        let plugin = TransformSql::init(config::Config {
            query: "  SELECT * FROM users  ".to_string(),
        })
        .await
        .expect("init should succeed");
        let ctx = Context::new("transform-sql", "users");
        let validation = plugin
            .validate(&ctx, None)
            .await
            .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Success);
    }

    #[tokio::test]
    async fn init_accepts_query_without_stream_specific_validation() {
        let result = TransformSql::init(config::Config {
            query: "SELECT 1".to_string(),
        })
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn validate_fails_when_query_does_not_reference_current_stream_name() {
        let plugin = TransformSql::init(config::Config {
            query: "SELECT 1".to_string(),
        })
        .await
        .expect("init should succeed");
        let ctx = Context::new("transform-sql", "users");
        let validation = plugin
            .validate(&ctx, None)
            .await
            .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Failed);
    }

    #[tokio::test]
    async fn validate_succeeds_when_query_references_current_stream_name() {
        let plugin = TransformSql::init(config::Config {
            query: "SELECT id FROM users".to_string(),
        })
        .await
        .expect("init should succeed");
        let ctx = Context::new("transform-sql", "users");
        let validation = plugin
            .validate(&ctx, None)
            .await
            .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Success);
    }

    #[tokio::test]
    async fn validate_reports_parse_error_for_invalid_sql() {
        let plugin = TransformSql::init(config::Config {
            query: "SELECT FROM users".to_string(),
        })
        .await
        .expect("init should not reject invalid config");
        let ctx = Context::new("transform-sql", "users");
        let validation = plugin
            .validate(&ctx, None)
            .await
            .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Failed);
        assert!(validation.message.contains("failed to parse SQL query"));
    }

    #[tokio::test]
    async fn validate_reports_multiple_statements_failure() {
        let plugin = TransformSql::init(config::Config {
            query: "SELECT * FROM users; SELECT * FROM users".to_string(),
        })
        .await
        .expect("init should not reject invalid config");
        let ctx = Context::new("transform-sql", "users");
        let validation = plugin
            .validate(&ctx, None)
            .await
            .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Failed);
        assert!(validation.message.contains("exactly one statement"));
    }
}

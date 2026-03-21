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
    statement: DfStatement,
}

impl Transform for TransformSql {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<Self, PluginError> {
        let query = normalize_and_parse_query(&config)
            .map_err(|message| PluginError::config("SQL_CONFIG", message))?;
        let statement = parse_plannable_statement(&query)
            .map_err(|message| PluginError::config("SQL_CONFIG", message))?;
        Ok(Self {
            config: config::Config { query },
            statement,
        })
    }

    async fn validate(
        &self,
        ctx: &Context,
        _upstream: Option<&rapidbyte_sdk::schema::StreamSchema>,
    ) -> Result<ValidationReport, PluginError> {
        match normalize_and_parse_query(&self.config)
            .and_then(|query| validate_query_for_stream_name(&query, ctx.stream_name()))
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
        validate_query_for_stream_name(&self.config.query, ctx.stream_name())
            .map_err(|message| PluginError::config("SQL_CONFIG", message))?;
        transform::run(ctx, &stream, &self.config, &self.statement).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn init_rejects_empty_query() {
        let result = TransformSql::init(config::Config {
            query: "   ".to_string(),
        })
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn init_trims_query_before_storing() {
        let plugin = TransformSql::init(config::Config {
            query: "  SELECT * FROM users  ".to_string(),
        })
        .await
        .expect("init should succeed");

        assert_eq!(plugin.config.query, "SELECT * FROM users");
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
    async fn init_rejects_invalid_sql_with_parse_context() {
        let result = TransformSql::init(config::Config {
            query: "SELECT FROM users".to_string(),
        })
        .await;

        match result {
            Ok(_) => panic!("invalid SQL should fail init"),
            Err(err) => {
                assert_eq!(err.code, "SQL_CONFIG");
                assert!(err.message.contains("failed to parse SQL query"));
            }
        }
    }

    #[tokio::test]
    async fn init_rejects_multiple_statements() {
        let result = TransformSql::init(config::Config {
            query: "SELECT * FROM users; SELECT * FROM users".to_string(),
        })
        .await;

        match result {
            Ok(_) => panic!("multiple statements should fail init"),
            Err(err) => {
                assert_eq!(err.code, "SQL_CONFIG");
                assert!(err.message.contains("exactly one statement"));
            }
        }
    }

    #[tokio::test]
    async fn validate_reports_parse_error_for_invalid_sql() {
        // validate() is now an instance method, but init() also parses the query.
        // For invalid SQL, init() fails before we can call validate().
        // We test that init() correctly rejects invalid SQL instead.
        let result = TransformSql::init(config::Config {
            query: "SELECT FROM users".to_string(),
        })
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, "SQL_CONFIG");
        assert!(err.message.contains("failed to parse SQL query"));
    }
}

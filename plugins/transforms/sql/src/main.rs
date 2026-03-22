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

    async fn init(config: Self::Config, _input: InitInput<'_>) -> Result<Self, PluginError> {
        let _ = normalize_and_parse_query(&config)
            .map_err(|message| PluginError::config("SQL_CONFIG", message))?;
        Ok(Self {
            config,
            prepared: std::sync::OnceLock::new(),
        })
    }

    async fn validate(
        &self,
        input: ValidateInput<'_>,
    ) -> Result<ValidationReport, PluginError> {
        let prepared = self
            .prepared_query()
            .map_err(|message| PluginError::config("SQL_CONFIG", message))?;
        if let Some(stream_name) = input.stream_name {
            if let Err(message) = validate_query_for_stream_name(&prepared.query, stream_name) {
                return Ok(ValidationReport::failed(&message));
            }
        }
        Ok(ValidationReport::success("SQL query configuration is valid"))
    }

    async fn transform(
        &self,
        input: TransformInput<'_>,
    ) -> Result<TransformSummary, PluginError> {
        let prepared = self
            .prepared_query()
            .map_err(|message| PluginError::config("SQL_CONFIG", message))?;
        validate_query_for_stream_name(
            &prepared.query,
            input.stream.source_stream_or_stream_name(),
        )
        .map_err(|message| PluginError::config("SQL_CONFIG", message))?;
        transform::run(input, &self.config, &prepared.statement).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn init_rejects_empty_query() {
        let result = TransformSql::init(config::Config {
            query: "   ".to_string(),
        }, InitInput::new())
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn validate_accepts_query_after_normalization() {
        let plugin = TransformSql::init(config::Config {
            query: "  SELECT * FROM users  ".to_string(),
        }, InitInput::new())
        .await
        .expect("init should succeed");
        let validation = plugin
            .validate(ValidateInput::new(None))
            .await
            .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Success);
    }

    #[tokio::test]
    async fn validate_rejects_stream_specific_query_mismatch() {
        let plugin = TransformSql::init(
            config::Config {
                query: "SELECT * FROM public.orders".to_string(),
            },
            InitInput::new(),
        )
        .await
        .expect("init should succeed");

        let validation = plugin
            .validate(ValidateInput::new(None).with_stream_name(Some("public.users")))
            .await
            .expect("validate should return a report");

        assert_eq!(validation.status, ValidationStatus::Failed);
    }

    #[tokio::test]
    async fn init_accepts_query_without_stream_specific_validation() {
        let result = TransformSql::init(config::Config {
            query: "SELECT 1".to_string(),
        }, InitInput::new())
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn validate_reports_parse_error_for_invalid_sql() {
        let result = TransformSql::init(config::Config {
            query: "SELECT FROM users".to_string(),
        }, InitInput::new())
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
    async fn validate_reports_multiple_statements_failure() {
        let result = TransformSql::init(config::Config {
            query: "SELECT * FROM users; SELECT * FROM users".to_string(),
        }, InitInput::new())
        .await;

        match result {
            Ok(_) => panic!("multiple statements should fail init"),
            Err(err) => {
                assert_eq!(err.code, "SQL_CONFIG");
                assert!(err.message.contains("exactly one statement"));
            }
        }
    }
}

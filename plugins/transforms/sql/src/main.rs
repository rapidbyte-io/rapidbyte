//! SQL transform plugin powered by Apache DataFusion.

mod config;
mod transform;

use std::collections::{BTreeSet, HashSet};

use datafusion::sql::parser::{DFParser, Statement as DfStatement};
use datafusion::sql::sqlparser::ast::{Query, SetExpr, Statement, TableFactor, TableWithJoins};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use rapidbyte_sdk::prelude::*;

fn query_external_table_references(query: &str) -> Result<BTreeSet<String>, String> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, query)
        .map_err(|e| format!("failed to parse SQL query: {e}"))?;
    let mut references = BTreeSet::new();
    let ctes = HashSet::new();
    for statement in statements {
        statement_external_table_references(&statement, &ctes, &mut references);
    }
    Ok(references)
}

fn statement_external_table_references(
    statement: &Statement,
    ctes: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    match statement {
        Statement::Query(query) => {
            query_external_table_references_in_scope(query, ctes, references);
        }
        _ => {}
    }
}

fn query_external_table_references_in_scope(
    query: &Query,
    parent_ctes: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    let mut ctes = parent_ctes.clone();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            ctes.insert(cte.alias.name.value.to_ascii_lowercase());
        }
        for cte in &with.cte_tables {
            query_external_table_references_in_scope(&cte.query, &ctes, references);
        }
    }
    set_expr_external_table_references(&query.body, &ctes, references);
}

fn set_expr_external_table_references(
    set_expr: &SetExpr,
    ctes: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    match set_expr {
        SetExpr::Select(select) => {
            for table_with_joins in &select.from {
                table_with_joins_external_table_references(table_with_joins, ctes, references);
            }
        }
        SetExpr::Query(query) => query_external_table_references_in_scope(query, ctes, references),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_external_table_references(left, ctes, references);
            set_expr_external_table_references(right, ctes, references);
        }
        _ => {}
    }
}

fn table_with_joins_external_table_references(
    table_with_joins: &TableWithJoins,
    ctes: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    table_factor_external_table_references(&table_with_joins.relation, ctes, references);
    for join in &table_with_joins.joins {
        table_factor_external_table_references(&join.relation, ctes, references);
    }
}

fn table_factor_external_table_references(
    table_factor: &TableFactor,
    ctes: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    match table_factor {
        TableFactor::Table { name, .. } => {
            if name.0.len() == 1 {
                let table_name = &name.0[0].value;
                if ctes.contains(&table_name.to_ascii_lowercase()) {
                    return;
                }
            }
            references.insert(name.to_string());
        }
        TableFactor::Derived { subquery, .. } => {
            query_external_table_references_in_scope(subquery, ctes, references);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => table_with_joins_external_table_references(table_with_joins, ctes, references),
        _ => {}
    }
}

fn normalize_and_parse_query(config: &config::Config) -> Result<String, String> {
    let query = config.normalized_query()?;
    let _ = parse_plannable_statement(&query)?;
    Ok(query)
}

pub(crate) fn validate_query_for_stream_name(query: &str, stream_name: &str) -> Result<(), String> {
    let references = query_external_table_references(query)?;
    if references.is_empty() || !references.iter().any(|name| name.eq_ignore_ascii_case(stream_name))
    {
        return Err(format!(
            "SQL query must reference current stream table '{stream_name}'"
        ));
    }
    let invalid_references = references
        .into_iter()
        .filter(|name| !name.eq_ignore_ascii_case(stream_name))
        .collect::<Vec<_>>();
    if !invalid_references.is_empty() {
        return Err(format!(
            "SQL query may only reference current stream table '{stream_name}'; found: {}",
            invalid_references.join(", ")
        ));
    }
    Ok(())
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

    async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError> {
        let query = normalize_and_parse_query(&config)
            .map_err(|message| PluginError::config("SQL_CONFIG", message))?;
        let statement = parse_plannable_statement(&query)
            .map_err(|message| PluginError::config("SQL_CONFIG", message))?;
        Ok((
            Self {
                config: config::Config { query },
                statement,
            },
            PluginInfo {
                protocol_version: ProtocolVersion::V5,
                features: vec![],
                default_max_batch_bytes: StreamLimits::DEFAULT_MAX_BATCH_BYTES,
            },
        ))
    }

    async fn validate(
        config: &Self::Config,
        ctx: &Context,
    ) -> Result<ValidationResult, PluginError> {
        match normalize_and_parse_query(config)
            .and_then(|query| validate_query_for_stream_name(&query, ctx.stream_name()))
        {
            Ok(()) => Ok(ValidationResult {
                status: ValidationStatus::Success,
                message: "SQL query configuration is valid".to_string(),
                warnings: Vec::new(),
            }),
            Err(message) => Ok(ValidationResult {
                status: ValidationStatus::Failed,
                message,
                warnings: Vec::new(),
            }),
        }
    }

    async fn transform(
        &mut self,
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
        let (plugin, _info) = TransformSql::init(config::Config {
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
        let ctx = Context::new("transform-sql", "users");
        let validation = TransformSql::validate(
            &config::Config {
                query: "SELECT 1".to_string(),
            },
            &ctx,
        )
        .await
        .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Failed);
    }

    #[tokio::test]
    async fn validate_succeeds_when_query_references_current_stream_name() {
        let ctx = Context::new("transform-sql", "users");
        let validation = TransformSql::validate(
            &config::Config {
                query: "SELECT id FROM users".to_string(),
            },
            &ctx,
        )
        .await
        .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Success);
    }

    #[tokio::test]
    async fn validate_rejects_queries_that_reference_multiple_external_tables() {
        let ctx = Context::new("transform-sql", "users");
        let validation = TransformSql::validate(
            &config::Config {
                query: "SELECT users.id FROM users JOIN orders ON users.id = orders.user_id"
                    .to_string(),
            },
            &ctx,
        )
        .await
        .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Failed);
        assert!(validation.message.contains("may only reference current stream table"));
        assert!(validation.message.contains("orders"));
    }

    #[tokio::test]
    async fn validate_allows_ctes_derived_from_current_stream() {
        let ctx = Context::new("transform-sql", "users");
        let validation = TransformSql::validate(
            &config::Config {
                query: "WITH filtered AS (SELECT id FROM users) SELECT id FROM filtered"
                    .to_string(),
            },
            &ctx,
        )
        .await
        .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Success);
    }

    #[tokio::test]
    async fn validate_fails_for_legacy_input_table_name() {
        let ctx = Context::new("transform-sql", "users");
        let validation = TransformSql::validate(
            &config::Config {
                query: "SELECT id FROM input".to_string(),
            },
            &ctx,
        )
        .await
        .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Failed);
        assert!(validation.message.contains("users"));
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
        let ctx = Context::new("transform-sql", "users");
        let validation = TransformSql::validate(
            &config::Config {
                query: "SELECT FROM users".to_string(),
            },
            &ctx,
        )
        .await
        .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Failed);
        assert!(validation.message.contains("failed to parse SQL query"));
    }
}

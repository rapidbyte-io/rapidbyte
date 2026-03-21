//! Destination `PostgreSQL` prerequisite checks.

use tokio_postgres::Client;

use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::validation::{PrerequisiteCheck, PrerequisiteSeverity, PrerequisitesReport};

use crate::config::Config;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrerequisiteSnapshot {
    pub schema_exists: bool,
    pub can_create_schema: bool,
}

pub(crate) async fn prerequisites(
    config: &Config,
    ctx: &Context,
) -> Result<PrerequisitesReport, PluginError> {
    let client = crate::client::connect(config)
        .await
        .map_err(|e| PluginError::transient_network("CONNECTION_FAILED", e))?;

    let snapshot = snapshot(&client, config)
        .await
        .map_err(|e| PluginError::transient_db("PREREQUISITES_FAILED", e))?;

    ctx.log(
        LogLevel::Info,
        "dest-postgres: prerequisite checks complete",
    );

    Ok(build_prerequisites_report(config, snapshot))
}

pub(crate) async fn snapshot(
    client: &Client,
    config: &Config,
) -> Result<PrerequisiteSnapshot, String> {
    let schema_exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
            &[&config.schema],
        )
        .await
        .map_err(|e| format!("unable to query schema existence: {e}"))?
        .get(0);

    let can_create_schema: bool = if schema_exists {
        client
            .query_one(
                "SELECT has_schema_privilege(current_user, $1, 'CREATE')",
                &[&config.schema],
            )
            .await
            .map_err(|e| format!("unable to query schema privileges: {e}"))?
            .get(0)
    } else {
        client
            .query_one(
                "SELECT has_database_privilege(current_user, current_database(), 'CREATE')",
                &[],
            )
            .await
            .map_err(|e| format!("unable to query database privileges: {e}"))?
            .get(0)
    };

    Ok(PrerequisiteSnapshot {
        schema_exists,
        can_create_schema,
    })
}

pub(crate) fn build_prerequisites_report(
    config: &Config,
    snapshot: PrerequisiteSnapshot,
) -> PrerequisitesReport {
    let mut checks = Vec::with_capacity(3);
    checks.push(PrerequisiteCheck::passed(
        "db_connectivity",
        "connected to PostgreSQL",
    ));

    if snapshot.schema_exists {
        checks.push(PrerequisiteCheck {
            name: "target_schema_exists".to_string(),
            passed: true,
            severity: PrerequisiteSeverity::Info,
            message: format!("schema '{}' already exists", config.target_schema()),
            fix_hint: None,
        });
    } else {
        checks.push(PrerequisiteCheck {
            name: "target_schema_missing".to_string(),
            passed: true,
            severity: PrerequisiteSeverity::Info,
            message: format!(
                "schema '{}' does not exist yet; apply will create it",
                config.target_schema()
            ),
            fix_hint: None,
        });
    }

    if snapshot.can_create_schema {
        checks.push(PrerequisiteCheck {
            name: "target_schema_capability".to_string(),
            passed: true,
            severity: PrerequisiteSeverity::Info,
            message: format!(
                "current role can create tables in schema '{}'",
                config.target_schema()
            ),
            fix_hint: None,
        });
    } else {
        checks.push(
            PrerequisiteCheck::error(
                "target_schema_capability",
                &format!(
                    "current role cannot create schema or tables in '{}'",
                    config.target_schema()
                ),
            )
            .with_fix_hint(&format!(
                "Grant CREATE on the database or schema '{}' before applying the destination",
                config.target_schema()
            )),
        );
    }

    PrerequisitesReport::from_checks(checks)
}

#[cfg(test)]
mod tests {
    use rapidbyte_sdk::validation::PrerequisiteSeverity;
    use crate::config::Config;
    use super::{build_prerequisites_report, PrerequisiteSnapshot};

    fn base_config() -> Config {
        Config {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: String::new(),
            database: "postgres".to_string(),
            schema: "public".to_string(),
            load_method: crate::config::LoadMethod::Copy,
            copy_flush_bytes: None,
        }
    }

    #[test]
    fn prerequisites_report_marks_missing_schema_as_structured_info() {
        let report = build_prerequisites_report(
            &base_config(),
            PrerequisiteSnapshot {
                schema_exists: false,
                can_create_schema: true,
            },
        );

        assert!(report.passed);
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "target_schema_missing"));
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "target_schema_capability"
                && check.passed
                && check.severity == PrerequisiteSeverity::Info));
    }

    #[test]
    fn prerequisites_report_fails_when_schema_creation_capability_is_missing() {
        let report = build_prerequisites_report(
            &base_config(),
            PrerequisiteSnapshot {
                schema_exists: true,
                can_create_schema: false,
            },
        );

        assert!(!report.passed);
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "target_schema_capability" && !check.passed));
    }
}

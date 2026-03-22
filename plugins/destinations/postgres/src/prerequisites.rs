//! Destination `PostgreSQL` prerequisite checks.

use std::future::Future;

use pg_escape::quote_identifier;
use tokio_postgres::Client;

use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::validation::{PrerequisiteCheck, PrerequisiteSeverity, PrerequisitesReport};

use crate::config::Config;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrerequisiteSnapshot {
    pub schema_exists: bool,
    pub can_create_target_table: bool,
    pub can_comment_on_table: bool,
    pub can_write_watermarks: bool,
    pub can_write_existing_targets: bool,
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

    client
        .execute("BEGIN", &[])
        .await
        .map_err(|e| format!("unable to start prerequisite transaction: {e}"))?;

    let result = async {
        let backend_pid: i32 = client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .map_err(|e| format!("unable to query backend pid: {e}"))?
            .get(0);

        let probe_table_name = format!("__rb_prereq_probe_{backend_pid}");
        let probe_table = crate::decode::qualified_name(&config.schema, &probe_table_name);
        let probe_stream_name = format!("__rb_prereq_stream_{backend_pid}");

        let can_create_target_table = with_savepoint(client, "rb_prereq_create_table", || async {
            if !schema_exists {
                let create_schema =
                    format!("CREATE SCHEMA {}", quote_identifier(config.target_schema()));
                client.execute(&create_schema, &[]).await.map_err(|e| {
                    format!("unable to create schema '{}': {e}", config.target_schema())
                })?;
            }

            let create_table = format!(
                "CREATE UNLOGGED TABLE {} (
                    probe_id BIGINT PRIMARY KEY,
                    payload TEXT NOT NULL
                )",
                probe_table
            );
            client
                .execute(&create_table, &[])
                .await
                .map_err(|e| format!("unable to create probe table {probe_table}: {e}"))?;
            Ok(())
        })
        .await
        .is_ok();

        let can_comment_on_table = if can_create_target_table {
            with_savepoint(client, "rb_prereq_comment", || async {
                let comment = format!(
                    "COMMENT ON TABLE {} IS '{}'",
                    probe_table,
                    format!("dest-postgres prerequisite probe {backend_pid}").replace('\'', "''")
                );
                client
                    .execute(&comment, &[])
                    .await
                    .map_err(|e| format!("unable to COMMENT ON TABLE {probe_table}: {e}"))?;
                Ok(())
            })
            .await
            .is_ok()
        } else {
            false
        };

        let can_write_existing_targets = if can_create_target_table {
            with_savepoint(client, "rb_prereq_dml", || async {
                let insert_sql = format!(
                    "INSERT INTO {} (probe_id, payload) VALUES (1, 'probe')",
                    probe_table
                );
                client
                    .execute(&insert_sql, &[])
                    .await
                    .map_err(|e| format!("unable to INSERT into {probe_table}: {e}"))?;

                let update_sql = format!(
                    "UPDATE {} SET payload = 'probe-updated' WHERE probe_id = 1",
                    probe_table
                );
                client
                    .execute(&update_sql, &[])
                    .await
                    .map_err(|e| format!("unable to UPDATE {probe_table}: {e}"))?;
                Ok(())
            })
            .await
            .is_ok()
        } else {
            false
        };

        let can_write_watermarks = with_savepoint(client, "rb_prereq_watermarks", || async {
            crate::watermark::ensure_table(client, config.target_schema()).await?;
            crate::watermark::set(client, config.target_schema(), &probe_stream_name, 1, 1).await?;
            Ok(())
        })
        .await
        .is_ok();

        Ok(PrerequisiteSnapshot {
            schema_exists,
            can_create_target_table,
            can_comment_on_table,
            can_write_watermarks,
            can_write_existing_targets,
        })
    }
    .await;

    let _ = client.execute("ROLLBACK", &[]).await;
    result
}

pub(crate) fn build_prerequisites_report(
    config: &Config,
    snapshot: PrerequisiteSnapshot,
) -> PrerequisitesReport {
    let mut checks = Vec::with_capacity(7);
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
            message: if snapshot.can_create_target_table {
                format!(
                    "schema '{}' does not exist yet; transactional probe verified it can be created",
                    config.target_schema()
                )
            } else {
                format!(
                    "schema '{}' does not exist yet; transactional probe could not create it",
                    config.target_schema()
                )
            },
            fix_hint: None,
        });
    }

    if snapshot.can_create_target_table {
        checks.push(PrerequisiteCheck {
            name: "schema_table_ddl".to_string(),
            passed: true,
            severity: PrerequisiteSeverity::Info,
            message: format!(
                "current role can create and clean up target tables in schema '{}'",
                config.target_schema()
            ),
            fix_hint: None,
        });
    } else {
        checks.push(
            PrerequisiteCheck::error(
                "schema_table_ddl",
                &format!(
                    "current role cannot create or repair target tables in '{}'",
                    config.target_schema()
                ),
            )
            .with_fix_hint(&format!(
                "Grant the destination role CREATE on schema '{}' and allow it to create target tables",
                config.target_schema()
            )),
        );
    }

    if snapshot.can_comment_on_table {
        checks.push(PrerequisiteCheck::passed(
            "handoff_comment_persistence",
            &format!(
                "transactional probe verified COMMENT ON TABLE on a probe table in schema '{}'",
                config.target_schema()
            ),
        ));
    } else {
        checks.push(
            PrerequisiteCheck::error(
                "handoff_comment_persistence",
                &format!(
                    "transactional probe could not COMMENT ON TABLE in schema '{}'",
                    config.target_schema()
                ),
            )
            .with_fix_hint(&format!(
                "Ensure the destination role can run COMMENT ON TABLE for tables in '{}'",
                config.target_schema()
            )),
        );
    }

    if snapshot.can_write_watermarks {
        checks.push(PrerequisiteCheck::passed(
            "watermark_table_write",
            &format!(
                "current role can create and upsert watermark rows in schema '{}'",
                config.target_schema()
            ),
        ));
    } else {
        checks.push(
            PrerequisiteCheck::error(
                "watermark_table_write",
                &format!(
                    "current role cannot create or write the watermark table in schema '{}'",
                    config.target_schema()
                ),
            )
            .with_fix_hint(&format!(
                "Grant CREATE and INSERT/UPDATE on the watermark table in '{}'",
                config.target_schema()
            )),
        );
    }

    if snapshot.can_write_existing_targets {
        checks.push(PrerequisiteCheck::passed(
            "existing_target_dml",
            &format!(
                "transactional probe verified INSERT/UPDATE on a newly created probe table in schema '{}'",
                config.target_schema()
            ),
        ));
    } else {
        checks.push(
            PrerequisiteCheck::error(
                "existing_target_dml",
                &format!(
                    "transactional probe could not perform DML on a probe table in schema '{}'",
                    config.target_schema()
                ),
            )
            .with_fix_hint(&format!(
                "Grant INSERT and UPDATE privileges for destination tables in '{}'",
                config.target_schema()
            )),
        );
    }

    checks.push(PrerequisiteCheck {
        name: "existing_target_privileges_deferred".to_string(),
        passed: true,
        severity: PrerequisiteSeverity::Info,
        message: format!(
            "pre-provisioned target-table ownership and privileges are validated later by apply/write; this prerequisite probe only verifies capability on connector-created probe tables in '{}'",
            config.target_schema()
        ),
        fix_hint: None,
    });

    PrerequisitesReport::from_checks(checks)
}

async fn with_savepoint<F, Fut>(client: &Client, savepoint: &str, op: F) -> Result<(), String>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let savepoint_sql = format!("SAVEPOINT {savepoint}");
    client
        .execute(&savepoint_sql, &[])
        .await
        .map_err(|e| format!("unable to start prerequisite savepoint {savepoint}: {e}"))?;

    match op().await {
        Ok(()) => {
            let release_sql = format!("RELEASE SAVEPOINT {savepoint}");
            client.execute(&release_sql, &[]).await.map_err(|e| {
                format!("unable to release prerequisite savepoint {savepoint}: {e}")
            })?;
            Ok(())
        }
        Err(err) => {
            let rollback_sql = format!("ROLLBACK TO SAVEPOINT {savepoint}");
            let _ = client.execute(&rollback_sql, &[]).await;
            let release_sql = format!("RELEASE SAVEPOINT {savepoint}");
            let _ = client.execute(&release_sql, &[]).await;
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use rapidbyte_sdk::validation::PrerequisiteSeverity;
    use tokio_postgres::NoTls;

    use crate::config::Config;

    use super::{build_prerequisites_report, snapshot, PrerequisiteSnapshot};

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

    async fn connect() -> tokio_postgres::Client {
        let dsn = std::env::var("RAPIDBYTE_POSTGRES_TEST_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| {
                "host=127.0.0.1 port=33603 user=postgres password=postgres dbname=postgres"
                    .to_string()
            });

        let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
            .await
            .expect("connect to test postgres");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    #[test]
    fn prerequisites_report_marks_missing_schema_as_structured_info() {
        let report = build_prerequisites_report(
            &base_config(),
            PrerequisiteSnapshot {
                schema_exists: false,
                can_create_target_table: true,
                can_comment_on_table: true,
                can_write_watermarks: true,
                can_write_existing_targets: true,
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
            .any(|check| check.name == "schema_table_ddl"
                && check.passed
                && check.severity == PrerequisiteSeverity::Info));
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "handoff_comment_persistence" && check.passed));
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "watermark_table_write" && check.passed));
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "existing_target_dml" && check.passed));
    }

    #[test]
    fn prerequisites_report_fails_when_required_destination_capabilities_are_missing() {
        let report = build_prerequisites_report(
            &base_config(),
            PrerequisiteSnapshot {
                schema_exists: true,
                can_create_target_table: true,
                can_comment_on_table: false,
                can_write_watermarks: false,
                can_write_existing_targets: false,
            },
        );

        assert!(!report.passed);
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "handoff_comment_persistence" && !check.passed));
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "watermark_table_write" && !check.passed));
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "existing_target_dml" && !check.passed));
    }

    #[tokio::test]
    async fn snapshot_transactional_probe_reports_required_capabilities() {
        let config = base_config();
        let client = connect().await;

        let snapshot = snapshot(&client, &config)
            .await
            .expect("prerequisite snapshot");

        let _ = snapshot.schema_exists;
        assert!(snapshot.can_create_target_table);
        assert!(snapshot.can_comment_on_table);
        assert!(snapshot.can_write_watermarks);
        assert!(snapshot.can_write_existing_targets);
    }
}

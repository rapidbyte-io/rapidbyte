//! Source `PostgreSQL` prerequisite checks.

use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::validation::PrerequisiteCheck;
use rapidbyte_sdk::validation::PrerequisiteSeverity;
use tokio_postgres::Client;

use crate::config::Config;

const MIN_SERVER_VERSION_NUM: i32 = 100_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ServerVersionMetadata {
    Supported(i32),
    Unsupported { raw: String, reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrerequisiteSnapshot {
    pub server_version: ServerVersionMetadata,
}

pub(crate) async fn prerequisites(
    config: &Config,
    ctx: &Context,
) -> Result<PrerequisitesReport, PluginError> {
    let client = crate::client::connect(config)
        .await
        .map_err(|e| PluginError::transient_network("CONNECTION_FAILED", e))?;
    let snapshot = snapshot(&client)
        .await
        .map_err(|e| PluginError::transient_db("PREREQUISITES_FAILED", e))?;
    ctx.log(
        LogLevel::Info,
        "source-postgres: prerequisite checks complete",
    );
    Ok(build_prerequisites_report(config, snapshot))
}

pub(crate) fn build_prerequisites_report(
    _config: &Config,
    snapshot: PrerequisiteSnapshot,
) -> PrerequisitesReport {
    let mut checks = Vec::new();

    checks.push(check_server_version(&snapshot.server_version));
    checks.push(PrerequisiteCheck {
        name: "cdc_prerequisites_deferred".to_string(),
        passed: true,
        severity: PrerequisiteSeverity::Info,
        message: "Source-level prerequisites do not validate CDC-specific wal_level, replication privilege, publication, or slot readiness because stream sync mode is unavailable at this lifecycle boundary".to_string(),
        fix_hint: Some(
            "CDC readiness is not evaluated by source prerequisites until stream sync mode is available."
                .to_string(),
        ),
    });

    PrerequisitesReport::from_checks(checks)
}

pub(crate) fn check_server_version(server_version: &ServerVersionMetadata) -> PrerequisiteCheck {
    match server_version {
        ServerVersionMetadata::Supported(version) if *version >= MIN_SERVER_VERSION_NUM => {
            PrerequisiteCheck::passed("server_version", "supported PostgreSQL version")
        }
        ServerVersionMetadata::Supported(version) => PrerequisiteCheck::error(
            "server_version",
            &format!("PostgreSQL {version} is older than the minimum supported version"),
        )
        .with_fix_hint("Upgrade PostgreSQL to version 10.0 or newer."),
        ServerVersionMetadata::Unsupported { raw, reason } => PrerequisiteCheck::error(
            "server_version",
            &format!("unsupported PostgreSQL version metadata ({reason}): {raw}"),
        )
        .with_fix_hint("Ensure the server exposes a supported numeric server_version_num."),
    }
}

pub(crate) async fn snapshot(client: &Client) -> Result<PrerequisiteSnapshot, String> {
    let server_version = client
        .query_one("SHOW server_version_num", &[])
        .await
        .map_err(|e| format!("unable to query server_version_num: {e}"))?
        .get::<usize, String>(0);

    Ok(PrerequisiteSnapshot {
        server_version: server_version_metadata_from_raw(&server_version),
    })
}

pub(crate) fn server_version_metadata_from_raw(raw: &str) -> ServerVersionMetadata {
    match raw.trim().parse::<i32>() {
        Ok(version) => ServerVersionMetadata::Supported(version),
        Err(err) => ServerVersionMetadata::Unsupported {
            raw: raw.to_owned(),
            reason: format!("malformed server_version_num: {err}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_sdk::validation::PrerequisiteSeverity;

    fn base_config() -> Config {
        Config {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: String::new(),
            database: "test_db".to_string(),
            replication_slot: Some("slot_a".to_string()),
            publication: Some("pub_a".to_string()),
        }
    }

    fn base_snapshot() -> PrerequisiteSnapshot {
        PrerequisiteSnapshot {
            server_version: ServerVersionMetadata::Supported(MIN_SERVER_VERSION_NUM),
        }
    }

    #[test]
    fn prerequisites_report_rejects_unsupported_version_metadata() {
        let report = build_prerequisites_report(
            &base_config(),
            PrerequisiteSnapshot {
                server_version: ServerVersionMetadata::Unsupported {
                    raw: "server_version_num=unknown".to_string(),
                    reason: "version metadata unavailable".to_string(),
                },
                ..base_snapshot()
            },
        );

        let check = report
            .checks
            .iter()
            .find(|check| check.name == "server_version")
            .expect("server_version check");
        assert!(!check.passed);
        assert!(!report.passed);
    }

    #[test]
    fn server_version_metadata_from_raw_rejects_malformed_metadata() {
        let metadata = server_version_metadata_from_raw("not-a-number");

        assert_eq!(
            metadata,
            ServerVersionMetadata::Unsupported {
                raw: "not-a-number".to_string(),
                reason: "malformed server_version_num: invalid digit found in string".to_string()
            }
        );
    }

    #[test]
    fn prerequisites_report_defers_cdc_checks_without_sync_mode() {
        let report = build_prerequisites_report(&base_config(), base_snapshot());
        assert!(report.passed);
        assert_eq!(report.checks.len(), 2);
        let deferred = report
            .checks
            .iter()
            .find(|check| check.name == "cdc_prerequisites_deferred")
            .expect("cdc_prerequisites_deferred check");
        assert_eq!(deferred.severity, PrerequisiteSeverity::Info);
        assert!(deferred.message.contains("sync mode is unavailable"));
    }

    #[test]
    fn prerequisites_report_does_not_emit_cdc_warnings() {
        let report = build_prerequisites_report(&base_config(), base_snapshot());
        assert!(report
            .checks
            .iter()
            .all(|check| check.severity != PrerequisiteSeverity::Warning));
    }

    #[test]
    fn prerequisites_report_is_structured() {
        let report = build_prerequisites_report(&base_config(), base_snapshot());

        assert!(report.passed);
        assert_eq!(report.checks.len(), 2);
        assert_eq!(report.checks[0].name, "server_version");
        assert_eq!(report.checks[1].name, "cdc_prerequisites_deferred");
    }
}

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
    pub wal_level: Option<String>,
    pub has_replication_capability: bool,
    pub configured_publication_exists: Option<bool>,
    pub configured_slot: Option<ConfiguredSlotState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConfiguredSlotState {
    pub database: Option<String>,
    pub plugin: Option<String>,
    pub slot_type: String,
    pub active: bool,
}

pub(crate) async fn prerequisites(
    config: &Config,
    input: PrerequisitesInput<'_>,
) -> Result<PrerequisitesReport, PluginError> {
    let client = crate::client::connect(config)
        .await
        .map_err(|e| PluginError::transient_network("CONNECTION_FAILED", e))?;
    let snapshot = snapshot(&client, config)
        .await
        .map_err(|e| PluginError::transient_db("PREREQUISITES_FAILED", e))?;
    input
        .log
        .info("source-postgres: prerequisite checks complete");
    Ok(build_prerequisites_report(config, snapshot))
}

pub(crate) fn build_prerequisites_report(
    config: &Config,
    snapshot: PrerequisiteSnapshot,
) -> PrerequisitesReport {
    let mut checks = Vec::new();
    let has_explicit_cdc_slot = config.replication_slot.is_some();

    checks.push(check_server_version(&snapshot.server_version));
    if has_explicit_cdc_slot {
        checks.push(check_wal_level(snapshot.wal_level.as_deref()));
        checks.push(check_replication_capability(snapshot.has_replication_capability));
    } else {
        checks.push(PrerequisiteCheck {
            name: "cdc_source_capability_deferred".to_string(),
            passed: true,
            severity: PrerequisiteSeverity::Info,
            message: "Source-level prerequisites do not require wal_level=logical or replication capability unless an explicit CDC replication slot is configured".to_string(),
            fix_hint: Some(
                "Configured replication slots are validated here; CDC readiness for default object names or discovery-only publication filters is evaluated later when stream-level execution context is available."
                    .to_string(),
            ),
        });
    }

    if let Some(publication_name) = config.publication.as_deref() {
        checks.push(check_configured_publication(
            publication_name,
            snapshot.configured_publication_exists.unwrap_or(false),
        ));
    }

    if let Some(slot_name) = config.replication_slot.as_deref() {
        checks.push(check_configured_replication_slot(
            slot_name,
            &config.database,
            snapshot.configured_slot.as_ref(),
        ));
    } else {
        checks.push(PrerequisiteCheck {
            name: "cdc_prerequisites_deferred".to_string(),
            passed: true,
            severity: PrerequisiteSeverity::Info,
            message: "Source-level prerequisites cannot validate default CDC publication/slot readiness until stream sync mode and physical source identity are known".to_string(),
            fix_hint: Some(
                "Configured CDC object names are validated here; default CDC object readiness is deferred until stream-level execution context is available."
                    .to_string(),
            ),
        });
    }

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

pub(crate) async fn snapshot(client: &Client, config: &Config) -> Result<PrerequisiteSnapshot, String> {
    let server_version = client
        .query_one("SHOW server_version_num", &[])
        .await
        .map_err(|e| format!("unable to query server_version_num: {e}"))?
        .get::<usize, String>(0);

    let wal_level = client
        .query_one("SHOW wal_level", &[])
        .await
        .map_err(|e| format!("unable to query wal_level: {e}"))?
        .get::<usize, String>(0);

    let has_replication_capability = client
        .query_opt(
            "SELECT rolreplication FROM pg_catalog.pg_roles WHERE rolname = current_user",
            &[],
        )
        .await
        .map_err(|e| format!("unable to query replication capability: {e}"))?
        .and_then(|row| row.get::<usize, Option<bool>>(0))
        .unwrap_or(false);

    Ok(PrerequisiteSnapshot {
        server_version: server_version_metadata_from_raw(&server_version),
        wal_level: Some(wal_level),
        has_replication_capability,
        configured_publication_exists: match config.publication.as_deref() {
            Some(publication_name) => Some(configured_publication_exists(client, publication_name).await?),
            None => None,
        },
        configured_slot: match config.replication_slot.as_deref() {
            Some(slot_name) => configured_slot_state(client, slot_name).await?,
            None => None,
        },
    })
}

async fn configured_publication_exists(
    client: &Client,
    publication_name: &str,
) -> Result<bool, String> {
    client
        .query_opt(
            "SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = $1",
            &[&publication_name],
        )
        .await
        .map(|row| row.is_some())
        .map_err(|e| format!("unable to query configured publication '{publication_name}': {e}"))
}

async fn configured_slot_state(
    client: &Client,
    slot_name: &str,
) -> Result<Option<ConfiguredSlotState>, String> {
    client
        .query_opt(
            "SELECT database, plugin, slot_type, active FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .map_err(|e| format!("unable to query configured replication slot '{slot_name}': {e}"))
        .map(|row| {
            row.map(|row| ConfiguredSlotState {
                database: row.get::<usize, Option<String>>(0),
                plugin: row.get::<usize, Option<String>>(1),
                slot_type: row.get::<usize, String>(2),
                active: row.get::<usize, bool>(3),
            })
        })
}

fn check_wal_level(wal_level: Option<&str>) -> PrerequisiteCheck {
    match wal_level {
        Some("logical") => PrerequisiteCheck::passed("wal_level", "wal_level is logical"),
        Some(other) => PrerequisiteCheck::error(
            "wal_level",
            &format!("wal_level must be logical for CDC readiness, found '{other}'"),
        )
        .with_fix_hint("Set wal_level=logical before running CDC streams."),
        None => PrerequisiteCheck::error("wal_level", "unable to determine wal_level")
            .with_fix_hint("Verify the PostgreSQL server is reachable and exposes wal_level."),
    }
}

fn check_replication_capability(has_replication_capability: bool) -> PrerequisiteCheck {
    if has_replication_capability {
        PrerequisiteCheck::passed(
            "replication_capability",
            "current user has replication capability",
        )
    } else {
        PrerequisiteCheck::error(
            "replication_capability",
            "current user lacks REPLICATION capability required for CDC",
        )
        .with_fix_hint("Grant REPLICATION or use a role with replication capability.")
    }
}

fn check_configured_publication(publication_name: &str, exists: bool) -> PrerequisiteCheck {
    if exists {
        PrerequisiteCheck::passed(
            "configured_publication",
            &format!("configured publication '{publication_name}' exists"),
        )
    } else {
        PrerequisiteCheck::error(
            "configured_publication",
            &format!("configured publication '{publication_name}' does not exist"),
        )
        .with_fix_hint("Create the publication or correct the configured publication name.")
    }
}

fn check_configured_replication_slot(
    slot_name: &str,
    database_name: &str,
    slot: Option<&ConfiguredSlotState>,
) -> PrerequisiteCheck {
    let Some(slot) = slot else {
        return PrerequisiteCheck::error(
            "configured_replication_slot",
            &format!("configured replication slot '{slot_name}' does not exist"),
        )
        .with_fix_hint(
            "Create the slot, or remove the explicit slot name so the connector can create its default slot.",
        );
    };

    if slot.slot_type != "logical" {
        return PrerequisiteCheck::error(
            "configured_replication_slot",
            &format!(
                "configured replication slot '{slot_name}' is type '{}' instead of logical",
                slot.slot_type
            ),
        )
        .with_fix_hint("Use a logical slot created with pgoutput.");
    }

    match slot.plugin.as_deref() {
        Some("pgoutput") => {}
        Some(plugin) => {
            return PrerequisiteCheck::error(
                "configured_replication_slot",
                &format!(
                    "configured replication slot '{slot_name}' uses plugin '{plugin}' instead of pgoutput"
                ),
            )
            .with_fix_hint("Recreate the slot with the pgoutput output plugin.");
        }
        None => {
            return PrerequisiteCheck::error(
                "configured_replication_slot",
                &format!(
                    "configured replication slot '{slot_name}' does not expose a logical output plugin"
                ),
            )
            .with_fix_hint("Recreate the slot as a logical pgoutput slot.");
        }
    }

    if slot.active {
        return PrerequisiteCheck::error(
            "configured_replication_slot",
            &format!("configured replication slot '{slot_name}' is already active"),
        )
        .with_fix_hint("Stop the other replication session using this slot before running CDC.");
    }

    if slot.database.as_deref() != Some(database_name) {
        return PrerequisiteCheck::error(
            "configured_replication_slot",
            &format!(
                "configured replication slot '{slot_name}' belongs to database '{}' instead of '{}'",
                slot.database.as_deref().unwrap_or("<unknown>"),
                database_name
            ),
        )
        .with_fix_hint("Point the connector at the matching database or recreate the slot.");
    }

    PrerequisiteCheck::passed(
        "configured_replication_slot",
        &format!("configured replication slot '{slot_name}' is compatible"),
    )
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
    use rapidbyte_sdk::input::PrerequisitesInput;
    use rapidbyte_sdk::validation::PrerequisiteSeverity;

    fn base_config() -> Config {
        Config {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: String::new(),
            database: "test_db".to_string(),
            schema: None,
            replication_slot: Some("slot_a".to_string()),
            publication: Some("pub_a".to_string()),
        }
    }

    fn config_without_cdc_overrides() -> Config {
        Config {
            replication_slot: None,
            publication: None,
            ..base_config()
        }
    }

    fn base_snapshot() -> PrerequisiteSnapshot {
        PrerequisiteSnapshot {
            server_version: ServerVersionMetadata::Supported(MIN_SERVER_VERSION_NUM),
            wal_level: Some("logical".to_string()),
            has_replication_capability: true,
            configured_publication_exists: Some(true),
            configured_slot: Some(ConfiguredSlotState {
                database: Some("test_db".to_string()),
                plugin: Some("pgoutput".to_string()),
                slot_type: "logical".to_string(),
                active: false,
            }),
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
        let report = build_prerequisites_report(&config_without_cdc_overrides(), base_snapshot());
        assert!(report.passed);
        assert_eq!(report.checks.len(), 3);
        let deferred = report
            .checks
            .iter()
            .find(|check| check.name == "cdc_prerequisites_deferred")
            .expect("cdc_prerequisites_deferred check");
        assert_eq!(deferred.severity, PrerequisiteSeverity::Info);
        assert!(deferred.message.contains("cannot validate default CDC publication/slot readiness"));

        let capability = report
            .checks
            .iter()
            .find(|check| check.name == "cdc_source_capability_deferred")
            .expect("cdc_source_capability_deferred check");
        assert_eq!(capability.severity, PrerequisiteSeverity::Info);
        assert!(capability
            .message
            .contains("unless an explicit CDC replication slot is configured"));
    }

    #[test]
    fn prerequisites_report_does_not_emit_cdc_warnings() {
        let report = build_prerequisites_report(&config_without_cdc_overrides(), base_snapshot());
        assert!(report
            .checks
            .iter()
            .all(|check| check.severity != PrerequisiteSeverity::Warning));
    }

    #[test]
    fn prerequisites_report_is_structured() {
        let report = build_prerequisites_report(&base_config(), base_snapshot());

        assert!(report.passed);
        assert_eq!(report.checks.len(), 5);
        assert_eq!(report.checks[0].name, "server_version");
        assert_eq!(report.checks[1].name, "wal_level");
        assert_eq!(report.checks[2].name, "replication_capability");
        assert_eq!(report.checks[3].name, "configured_publication");
        assert_eq!(report.checks[4].name, "configured_replication_slot");
    }

    #[test]
    fn prerequisites_report_rejects_non_logical_wal_level() {
        let report = build_prerequisites_report(
            &base_config(),
            PrerequisiteSnapshot {
                wal_level: Some("replica".to_string()),
                ..base_snapshot()
            },
        );

        let check = report
            .checks
            .iter()
            .find(|check| check.name == "wal_level")
            .expect("wal_level check");
        assert!(!check.passed);
        assert!(!report.passed);
    }

    #[test]
    fn prerequisites_report_rejects_missing_replication_capability() {
        let report = build_prerequisites_report(
            &base_config(),
            PrerequisiteSnapshot {
                has_replication_capability: false,
                ..base_snapshot()
            },
        );

        let check = report
            .checks
            .iter()
            .find(|check| check.name == "replication_capability")
            .expect("replication_capability check");
        assert!(!check.passed);
        assert!(!report.passed);
    }

    #[test]
    fn prerequisites_report_rejects_missing_configured_publication() {
        let report = build_prerequisites_report(
            &base_config(),
            PrerequisiteSnapshot {
                configured_publication_exists: Some(false),
                ..base_snapshot()
            },
        );

        let check = report
            .checks
            .iter()
            .find(|check| check.name == "configured_publication")
            .expect("configured_publication check");
        assert!(!check.passed);
        assert!(!report.passed);
    }

    #[test]
    fn prerequisites_report_rejects_incompatible_configured_slot() {
        let report = build_prerequisites_report(
            &base_config(),
            PrerequisiteSnapshot {
                configured_slot: Some(ConfiguredSlotState {
                    database: Some("test_db".to_string()),
                    plugin: Some("decoderbufs".to_string()),
                    slot_type: "logical".to_string(),
                    active: false,
                }),
                ..base_snapshot()
            },
        );

        let check = report
            .checks
            .iter()
            .find(|check| check.name == "configured_replication_slot")
            .expect("configured_replication_slot check");
        assert!(!check.passed);
        assert!(!report.passed);
    }

    #[test]
    fn prerequisites_report_rejects_physical_configured_slot() {
        let report = build_prerequisites_report(
            &base_config(),
            PrerequisiteSnapshot {
                configured_slot: Some(ConfiguredSlotState {
                    database: None,
                    plugin: None,
                    slot_type: "physical".to_string(),
                    active: false,
                }),
                ..base_snapshot()
            },
        );

        let check = report
            .checks
            .iter()
            .find(|check| check.name == "configured_replication_slot")
            .expect("configured_replication_slot check");
        assert!(!check.passed);
        assert!(!report.passed);
    }

    #[test]
    fn prerequisites_helper_compiles_with_typed_input_without_context() {
        let config = base_config();
        // This is compile-shape coverage for the typed prerequisites helper.
        let _ = super::prerequisites(&config, PrerequisitesInput::new());
    }
}

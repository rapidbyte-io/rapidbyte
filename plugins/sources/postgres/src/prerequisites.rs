//! PostgreSQL prerequisite checks for source-postgres.

use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::validation::PrerequisiteCheck;

use crate::config::Config;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrerequisiteSnapshot {
    pub(crate) server_version: Option<String>,
    pub(crate) wal_level: Option<String>,
    pub(crate) replication_role: ReplicationRoleState,
    pub(crate) replication_slot: ResourceState,
    pub(crate) publication: ResourceState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReplicationRoleState {
    Granted,
    RdsGranted,
    Missing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResourceState {
    NotConfigured,
    Missing,
    Present {
        database: Option<String>,
    },
}

const MIN_POSTGRES_MAJOR: u32 = 10;

pub(crate) async fn prerequisites(config: &Config) -> Result<PrerequisitesReport, PluginError> {
    let client = crate::client::connect(config)
        .await
        .map_err(|e| PluginError::transient_network("CONNECTION_FAILED", e))?;
    let snapshot = collect_snapshot(&client, config)
        .await
        .map_err(|e| PluginError::transient_db("PREREQUISITES_FAILED", e))?;
    Ok(build_prerequisites_report(config, snapshot))
}

pub(crate) fn build_prerequisites_report(
    config: &Config,
    snapshot: PrerequisiteSnapshot,
) -> PrerequisitesReport {
    let mut checks = vec![
        check_server_version(snapshot.server_version),
        check_wal_level(snapshot.wal_level),
        check_replication_role(snapshot.replication_role),
    ];

    if let Some(slot_name) = config.configured_replication_slot() {
        checks.push(check_replication_slot(
            config,
            slot_name,
            snapshot.replication_slot,
        ));
    }

    if let Some(publication_name) = config.configured_publication() {
        checks.push(check_publication(publication_name, snapshot.publication));
    }

    PrerequisitesReport::from_checks(checks)
}

async fn collect_snapshot(
    client: &tokio_postgres::Client,
    config: &Config,
) -> Result<PrerequisiteSnapshot, String> {
    let server_version = query_scalar_text(client, "SHOW server_version").await?;
    let wal_level = query_scalar_text(client, "SHOW wal_level").await?;

    let replication_role = query_replication_role(client, &config.user).await?;
    let replication_slot = match config.configured_replication_slot() {
        Some(slot_name) => query_replication_slot(client, slot_name).await?,
        None => ResourceState::NotConfigured,
    };
    let publication = match config.configured_publication() {
        Some(publication_name) => query_publication(client, publication_name).await?,
        None => ResourceState::NotConfigured,
    };

    Ok(PrerequisiteSnapshot {
        server_version,
        wal_level,
        replication_role,
        replication_slot,
        publication,
    })
}

async fn query_scalar_text(
    client: &tokio_postgres::Client,
    sql: &str,
) -> Result<Option<String>, String> {
    let row = client.query_one(sql, &[]).await.map_err(|e| e.to_string())?;
    let value: String = row.get(0);
    Ok(Some(value))
}

async fn query_replication_role(
    client: &tokio_postgres::Client,
    user: &str,
) -> Result<ReplicationRoleState, String> {
    let row = client
        .query_one(
            "SELECT rolreplication FROM pg_catalog.pg_roles WHERE rolname = $1",
            &[&user],
        )
        .await
        .map_err(|e| e.to_string())?;
    let has_replication: bool = row.get(0);
    if has_replication {
        return Ok(ReplicationRoleState::Granted);
    }

    match client
        .query_one(
            "SELECT pg_has_role(current_user, 'rds_replication', 'member')",
            &[],
        )
        .await
    {
        Ok(row) if row.get::<_, bool>(0) => Ok(ReplicationRoleState::RdsGranted),
        Ok(_) => Ok(ReplicationRoleState::Missing),
        Err(_) => Ok(ReplicationRoleState::Missing),
    }
}

async fn query_replication_slot(
    client: &tokio_postgres::Client,
    slot_name: &str,
) -> Result<ResourceState, String> {
    let rows = client
        .query(
            "SELECT database FROM pg_catalog.pg_replication_slots \
             WHERE slot_name = $1 AND slot_type = 'logical'",
            &[&slot_name],
        )
        .await
        .map_err(|e| e.to_string())?;

    if rows.is_empty() {
        return Ok(ResourceState::Missing);
    }

    let database: Option<String> = rows[0].get(0);
    Ok(ResourceState::Present { database })
}

async fn query_publication(
    client: &tokio_postgres::Client,
    publication_name: &str,
) -> Result<ResourceState, String> {
    let rows = client
        .query(
            "SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = $1",
            &[&publication_name],
        )
        .await
        .map_err(|e| e.to_string())?;

    if rows.is_empty() {
        Ok(ResourceState::Missing)
    } else {
        Ok(ResourceState::Present { database: None })
    }
}

fn check_server_version(server_version: Option<String>) -> PrerequisiteCheck {
    let Some(raw) = server_version else {
        return PrerequisiteCheck::error(
            "server_version",
            "unable to determine PostgreSQL server version",
        )
        .with_fix_hint("Ensure the connection reaches a PostgreSQL server that reports a version string.");
    };

    match parse_major_minor(&raw) {
        Some((major, minor)) if major >= MIN_POSTGRES_MAJOR => {
            PrerequisiteCheck::passed("server_version", &format!("PostgreSQL {major}.{minor}"))
        }
        Some((major, minor)) => PrerequisiteCheck::error(
            "server_version",
            &format!(
                "PostgreSQL version {major}.{minor} is below the minimum supported version {MIN_POSTGRES_MAJOR}.0"
            ),
        )
        .with_fix_hint("Upgrade PostgreSQL to a supported version."),
        None => PrerequisiteCheck::error(
            "server_version",
            &format!("unsupported PostgreSQL version metadata: {raw}"),
        )
        .with_fix_hint("Verify SHOW server_version returns a standard PostgreSQL version string."),
    }
}

fn check_wal_level(wal_level: Option<String>) -> PrerequisiteCheck {
    match wal_level.as_deref().map(str::trim).map(str::to_ascii_lowercase) {
        Some(level) if level == "logical" => {
            PrerequisiteCheck::passed("wal_level", "wal_level is logical")
        }
        Some(level) => PrerequisiteCheck::error(
            "wal_level",
            &format!("wal_level is '{level}', but logical replication requires wal_level=logical"),
        )
        .with_fix_hint("Set wal_level = logical and restart PostgreSQL."),
        None => PrerequisiteCheck::error(
            "wal_level",
            "unable to determine wal_level",
        )
        .with_fix_hint("Query SHOW wal_level on the connected PostgreSQL instance."),
    }
}

fn check_replication_role(role: ReplicationRoleState) -> PrerequisiteCheck {
    match role {
        ReplicationRoleState::Granted => {
            PrerequisiteCheck::passed("replication_role", "user has REPLICATION")
        }
        ReplicationRoleState::RdsGranted => PrerequisiteCheck::passed(
            "replication_role",
            "user is a member of rds_replication",
        ),
        ReplicationRoleState::Missing => PrerequisiteCheck::error(
            "replication_role",
            "user must have the REPLICATION role or rds_replication membership",
        )
        .with_fix_hint("Grant REPLICATION to the database user or the provider-specific equivalent."),
    }
}

fn check_replication_slot(
    config: &Config,
    slot_name: &str,
    state: ResourceState,
) -> PrerequisiteCheck {
    match state {
        ResourceState::NotConfigured => {
            PrerequisiteCheck::passed("replication_slot", "no explicit replication slot configured")
        }
        ResourceState::Missing => PrerequisiteCheck::error(
            "replication_slot",
            &format!("replication slot '{slot_name}' does not exist"),
        )
        .with_fix_hint("Create the logical replication slot or configure the correct slot name."),
        ResourceState::Present { database } => {
            if let Some(database) = database {
                if database == config.database {
                    PrerequisiteCheck::passed(
                        "replication_slot",
                        &format!("replication slot '{slot_name}' exists in database '{database}'"),
                    )
                } else {
                    PrerequisiteCheck::error(
                        "replication_slot",
                        &format!(
                            "replication slot '{slot_name}' exists in database '{database}', but the configured database is '{}'",
                            config.database
                        ),
                    )
                    .with_fix_hint("Use a slot in the configured database or drop the existing slot.")
                }
            } else {
                PrerequisiteCheck::passed(
                    "replication_slot",
                    &format!("replication slot '{slot_name}' exists"),
                )
            }
        }
    }
}

fn check_publication(publication_name: &str, state: ResourceState) -> PrerequisiteCheck {
    match state {
        ResourceState::NotConfigured => {
            PrerequisiteCheck::passed("publication", "no explicit publication configured")
        }
        ResourceState::Missing => PrerequisiteCheck::error(
            "publication",
            &format!("publication '{publication_name}' does not exist"),
        )
        .with_fix_hint("Create the publication or configure the correct publication name."),
        ResourceState::Present { .. } => PrerequisiteCheck::passed(
            "publication",
            &format!("publication '{publication_name}' exists"),
        ),
    }
}

fn parse_major_minor(version: &str) -> Option<(u32, u32)> {
    let token = version.split_whitespace().next()?;
    let mut parts = token.split('.');
    let major = parts.next()?.parse::<u32>().ok()?;
    let minor = parts
        .next()
        .and_then(|part| {
            part.chars()
                .take_while(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse::<u32>()
                .ok()
        })
        .unwrap_or(0);
    Some((major, minor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn base_config() -> Config {
        Config {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: String::new(),
            database: "test".to_string(),
            replication_slot: None,
            publication: None,
        }
    }

    #[test]
    fn rejects_unsupported_version_metadata() {
        let report = build_prerequisites_report(
            &base_config(),
            PrerequisiteSnapshot {
                server_version: Some("PostgreSQL release candidate".to_string()),
                wal_level: Some("logical".to_string()),
                replication_role: ReplicationRoleState::Granted,
                replication_slot: ResourceState::NotConfigured,
                publication: ResourceState::NotConfigured,
            },
        );

        assert!(!report.passed);
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "server_version" && !check.passed));
    }

    #[test]
    fn rejects_non_logical_wal_level() {
        let report = build_prerequisites_report(
            &base_config(),
            PrerequisiteSnapshot {
                server_version: Some("15.3".to_string()),
                wal_level: Some("replica".to_string()),
                replication_role: ReplicationRoleState::Granted,
                replication_slot: ResourceState::NotConfigured,
                publication: ResourceState::NotConfigured,
            },
        );

        assert!(!report.passed);
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "wal_level" && !check.passed));
    }

    #[test]
    fn rejects_missing_replication_privilege() {
        let report = build_prerequisites_report(
            &base_config(),
            PrerequisiteSnapshot {
                server_version: Some("15.3".to_string()),
                wal_level: Some("logical".to_string()),
                replication_role: ReplicationRoleState::Missing,
                replication_slot: ResourceState::NotConfigured,
                publication: ResourceState::NotConfigured,
            },
        );

        assert!(!report.passed);
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "replication_role" && !check.passed));
    }

    #[test]
    fn rejects_invalid_publication_and_slot_state() {
        let mut config = base_config();
        config.replication_slot = Some("rapidbyte_users".to_string());
        config.publication = Some("rapidbyte_users".to_string());

        let report = build_prerequisites_report(
            &config,
            PrerequisiteSnapshot {
                server_version: Some("15.3".to_string()),
                wal_level: Some("logical".to_string()),
                replication_role: ReplicationRoleState::Granted,
                replication_slot: ResourceState::Present {
                    database: Some("otherdb".to_string()),
                },
                publication: ResourceState::Missing,
            },
        );

        assert!(!report.passed);
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "replication_slot" && !check.passed));
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "publication" && !check.passed));
    }

    #[test]
    fn produces_structured_prerequisites_report() {
        let report = build_prerequisites_report(
            &base_config(),
            PrerequisiteSnapshot {
                server_version: Some("15.3".to_string()),
                wal_level: Some("logical".to_string()),
                replication_role: ReplicationRoleState::Granted,
                replication_slot: ResourceState::NotConfigured,
                publication: ResourceState::NotConfigured,
            },
        );

        assert!(report.passed);
        assert!(report.checks.len() >= 3);
        assert!(report.checks.iter().all(|check| !check.name.is_empty()));
    }
}

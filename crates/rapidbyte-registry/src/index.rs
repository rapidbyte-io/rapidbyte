//! Searchable plugin index data types for the OCI plugin registry.
//!
//! The index is stored in the registry under the well-known repository
//! [`INDEX_REPOSITORY`] at tag [`INDEX_TAG`].  Callers read the index,
//! mutate it via [`PluginIndex::upsert`], and write it back as a JSON blob
//! wrapped in a single-layer OCI artifact.
//!
//! | Type              | Responsibility                                         |
//! |-------------------|--------------------------------------------------------|
//! | [`PluginIndex`]   | Ordered collection of entries + search/filter helpers |
//! | [`PluginIndexEntry`] | Metadata for a single plugin repository           |

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Try to parse a version string as (major, minor, patch) for comparison.
fn semver_parse(version: &str) -> Option<(u64, u64, u64)> {
    let v = version.strip_prefix('v').unwrap_or(version);
    let parts: Vec<&str> = v.split('.').collect();
    if parts.len() >= 3 {
        let major = parts[0].parse().ok()?;
        let minor = parts[1].parse().ok()?;
        // Strip pre-release suffix (e.g. "0-beta") for the patch number
        let patch_str = parts[2].split('-').next().unwrap_or(parts[2]);
        let patch = patch_str.parse().ok()?;
        Some((major, minor, patch))
    } else {
        None
    }
}

// ── Well-known index location ─────────────────────────────────────────────────

/// Well-known OCI repository used to store the plugin index.
pub const INDEX_REPOSITORY: &str = "rapidbyte-index";

/// Well-known OCI tag for the current plugin index.
pub const INDEX_TAG: &str = "latest";

// ── PluginIndexEntry ──────────────────────────────────────────────────────────

/// Metadata record for a single plugin repository stored in [`PluginIndex`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginIndexEntry {
    /// OCI repository path (e.g. `"source/postgres"`).
    pub repository: String,
    /// Human-readable plugin name.
    pub name: String,
    /// Short description shown in search results.
    pub description: String,
    /// Plugin category: `"source"`, `"destination"`, or `"transform"`.
    pub plugin_type: String,
    /// Latest published version tag.
    pub latest: String,
    /// All known version tags in the order they were upserted.
    pub versions: Vec<String>,
    /// Optional author string.
    pub author: Option<String>,
    /// Optional SPDX license identifier.
    pub license: Option<String>,
    /// Timestamp of the last metadata update.
    pub updated_at: DateTime<Utc>,
}

// ── PluginIndex ───────────────────────────────────────────────────────────────

/// A versioned, searchable collection of [`PluginIndexEntry`] records.
///
/// The `schema_version` field is always `1` for entries created by this
/// library version and can be used for forward-compatibility checks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginIndex {
    /// Schema version; always `1`.
    pub schema_version: u32,
    /// All known plugin entries.
    pub plugins: Vec<PluginIndexEntry>,
}

impl PluginIndex {
    /// Create an empty index with `schema_version = 1`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            schema_version: 1,
            plugins: Vec::new(),
        }
    }

    /// Insert or update an entry by [`PluginIndexEntry::repository`].
    ///
    /// - If no entry exists for the repository, the entry is appended as-is.
    /// - If an entry already exists:
    ///   - The `latest` version, `name`, `description`, `plugin_type`,
    ///     `author`, `license`, and `updated_at` fields are overwritten.
    ///   - Each version in `entry.versions` that is not already present is
    ///     appended to the existing `versions` list (no duplicates).
    pub fn upsert(&mut self, entry: PluginIndexEntry) {
        if let Some(existing) = self
            .plugins
            .iter_mut()
            .find(|e| e.repository == entry.repository)
        {
            // Update scalar metadata fields.
            existing.name = entry.name;
            existing.description = entry.description;
            existing.plugin_type = entry.plugin_type;
            existing.author = entry.author;
            existing.license = entry.license;
            existing.updated_at = entry.updated_at;

            // Only advance `latest` if the new version is semver-greater.
            // Fall back to always updating if either version isn't valid semver.
            let should_update_latest =
                match (semver_parse(&existing.latest), semver_parse(&entry.latest)) {
                    (Some(current), Some(new)) => new > current,
                    _ => true,
                };
            if should_update_latest {
                existing.latest = entry.latest;
            }

            // Merge versions without duplicates, preserving insertion order.
            for version in entry.versions {
                if !existing.versions.contains(&version) {
                    existing.versions.push(version);
                }
            }
        } else {
            self.plugins.push(entry);
        }
    }

    /// Return all entries whose `repository`, `name`, or `description`
    /// contain `query` (case-insensitive).
    ///
    /// An empty `query` matches every entry.  If `plugin_type` is `Some`,
    /// only entries whose `plugin_type` equals the given value are returned.
    #[must_use]
    pub fn search<'a>(
        &'a self,
        query: &str,
        plugin_type: Option<&str>,
    ) -> Vec<&'a PluginIndexEntry> {
        let needle = query.to_lowercase();

        self.plugins
            .iter()
            .filter(|e| {
                // Type filter.
                if let Some(pt) = plugin_type {
                    if e.plugin_type != pt {
                        return false;
                    }
                }

                // Text search (empty needle matches everything).
                if needle.is_empty() {
                    return true;
                }

                e.repository.to_lowercase().contains(&needle)
                    || e.name.to_lowercase().contains(&needle)
                    || e.description.to_lowercase().contains(&needle)
            })
            .collect()
    }
}

impl Default for PluginIndex {
    fn default() -> Self {
        Self::new()
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use chrono::TimeZone as _;

    use super::*;

    // ── helpers ───────────────────────────────────────────────────────────────

    fn ts(year: i32, month: u32, day: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, 0, 0, 0)
            .single()
            .expect("valid date")
    }

    fn make_entry(
        repository: &str,
        name: &str,
        description: &str,
        plugin_type: &str,
        latest: &str,
        versions: &[&str],
    ) -> PluginIndexEntry {
        PluginIndexEntry {
            repository: repository.to_owned(),
            name: name.to_owned(),
            description: description.to_owned(),
            plugin_type: plugin_type.to_owned(),
            latest: latest.to_owned(),
            versions: versions.iter().map(|v| (*v).to_owned()).collect(),
            author: None,
            license: None,
            updated_at: ts(2026, 1, 1),
        }
    }

    // ── new ───────────────────────────────────────────────────────────────────

    #[test]
    fn new_index_is_empty() {
        let idx = PluginIndex::new();
        assert_eq!(idx.schema_version, 1);
        assert!(idx.plugins.is_empty());
    }

    // ── upsert ────────────────────────────────────────────────────────────────

    #[test]
    fn upsert_adds_new_entry() {
        let mut idx = PluginIndex::new();
        let entry = make_entry(
            "source/postgres",
            "Postgres Source",
            "Read rows from Postgres",
            "source",
            "1.0.0",
            &["1.0.0"],
        );
        idx.upsert(entry);

        assert_eq!(idx.plugins.len(), 1);
        assert_eq!(idx.plugins[0].repository, "source/postgres");
        assert_eq!(idx.plugins[0].latest, "1.0.0");
        assert_eq!(idx.plugins[0].versions, vec!["1.0.0"]);
    }

    #[test]
    fn upsert_updates_existing_entry() {
        let mut idx = PluginIndex::new();

        // Insert v1.
        idx.upsert(make_entry(
            "source/postgres",
            "Postgres Source",
            "Read rows from Postgres",
            "source",
            "1.0.0",
            &["1.0.0"],
        ));

        // Upsert v2.
        let updated = PluginIndexEntry {
            repository: "source/postgres".to_owned(),
            name: "Postgres Source".to_owned(),
            description: "Read rows from Postgres (updated)".to_owned(),
            plugin_type: "source".to_owned(),
            latest: "2.0.0".to_owned(),
            versions: vec!["2.0.0".to_owned()],
            author: Some("alice".to_owned()),
            license: Some("Apache-2.0".to_owned()),
            updated_at: ts(2026, 6, 1),
        };
        idx.upsert(updated);

        assert_eq!(idx.plugins.len(), 1, "should not create a duplicate entry");

        let e = &idx.plugins[0];
        assert_eq!(e.latest, "2.0.0");
        assert_eq!(e.versions, vec!["1.0.0", "2.0.0"]);
        assert_eq!(e.description, "Read rows from Postgres (updated)");
        assert_eq!(e.author.as_deref(), Some("alice"));
        assert_eq!(e.updated_at, ts(2026, 6, 1));
    }

    #[test]
    fn upsert_does_not_duplicate_versions() {
        let mut idx = PluginIndex::new();

        idx.upsert(make_entry(
            "source/postgres",
            "Postgres Source",
            "desc",
            "source",
            "1.0.0",
            &["1.0.0"],
        ));

        // Push the same version a second time.
        idx.upsert(make_entry(
            "source/postgres",
            "Postgres Source",
            "desc",
            "source",
            "1.0.0",
            &["1.0.0"],
        ));

        assert_eq!(
            idx.plugins[0].versions,
            vec!["1.0.0"],
            "duplicate version must not be appended"
        );
    }

    // ── search ────────────────────────────────────────────────────────────────

    fn populated_index() -> PluginIndex {
        let mut idx = PluginIndex::new();
        idx.upsert(make_entry(
            "source/postgres",
            "Postgres Source",
            "Read rows from Postgres",
            "source",
            "1.0.0",
            &["1.0.0"],
        ));
        idx.upsert(make_entry(
            "destination/s3",
            "S3 Destination",
            "Write Parquet files to S3",
            "destination",
            "1.0.0",
            &["1.0.0"],
        ));
        idx.upsert(make_entry(
            "transform/sql",
            "SQL Transform",
            "Run arbitrary SQL over Arrow batches",
            "transform",
            "1.0.0",
            &["1.0.0"],
        ));
        idx
    }

    #[test]
    fn search_matches_name() {
        let idx = populated_index();
        let results = idx.search("SQL Transform", None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].repository, "transform/sql");
    }

    #[test]
    fn search_matches_description() {
        let idx = populated_index();
        let results = idx.search("Parquet files", None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].repository, "destination/s3");
    }

    #[test]
    fn search_case_insensitive() {
        let idx = populated_index();
        // Lower-case query against mixed-case data.
        let results = idx.search("postgres", None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].repository, "source/postgres");
    }

    #[test]
    fn search_filters_by_type() {
        let idx = populated_index();
        let results = idx.search("", Some("destination"));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].repository, "destination/s3");
    }

    #[test]
    fn search_empty_query_returns_all() {
        let idx = populated_index();
        let results = idx.search("", None);
        assert_eq!(results.len(), 3);
    }

    // ── serde ─────────────────────────────────────────────────────────────────

    #[test]
    fn serde_roundtrip() {
        let original = populated_index();
        let json = serde_json::to_string(&original).expect("serialization must succeed");
        let restored: PluginIndex =
            serde_json::from_str(&json).expect("deserialization must succeed");

        assert_eq!(restored.schema_version, original.schema_version);
        assert_eq!(restored.plugins.len(), original.plugins.len());

        for (a, b) in original.plugins.iter().zip(restored.plugins.iter()) {
            assert_eq!(a.repository, b.repository);
            assert_eq!(a.latest, b.latest);
            assert_eq!(a.versions, b.versions);
            assert_eq!(a.updated_at, b.updated_at);
        }
    }

    #[test]
    fn upsert_does_not_regress_latest_on_older_push() {
        let mut idx = PluginIndex::new();

        // Push 2.0.0 first
        idx.upsert(make_entry(
            "source/postgres",
            "Postgres",
            "desc",
            "source",
            "2.0.0",
            &["2.0.0"],
        ));
        assert_eq!(idx.plugins[0].latest, "2.0.0");

        // Push 1.0.0 after — latest should stay at 2.0.0
        idx.upsert(make_entry(
            "source/postgres",
            "Postgres",
            "desc",
            "source",
            "1.0.0",
            &["1.0.0"],
        ));
        assert_eq!(idx.plugins[0].latest, "2.0.0");
        assert!(idx.plugins[0].versions.contains(&"1.0.0".to_owned()));
        assert!(idx.plugins[0].versions.contains(&"2.0.0".to_owned()));
    }
}

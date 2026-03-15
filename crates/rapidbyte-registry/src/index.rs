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
use rapidbyte_types::wire::PluginKind;
use serde::{Deserialize, Serialize};

/// Deserialize `PluginKind` with a fallback for unknown/legacy values.
///
/// Existing index entries may contain free-form strings (e.g. `"unknown"`)
/// that predate the typed enum. Rather than failing the entire index
/// deserialization, fall back to `PluginKind::Transform` for unrecognized
/// values.
fn deserialize_plugin_kind<'de, D>(deserializer: D) -> Result<PluginKind, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.as_str() {
        "source" => Ok(PluginKind::Source),
        "destination" => Ok(PluginKind::Destination),
        // "transform" and any unknown/legacy values default to Transform.
        _ => Ok(PluginKind::Transform),
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
    /// Plugin category.
    #[serde(deserialize_with = "deserialize_plugin_kind")]
    pub plugin_type: PluginKind,
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
            // If current is valid semver but new is not, don't overwrite
            // (prevents non-semver tags like "nightly" from replacing "2.0.0").
            // If both are non-semver, always update (chronological ordering).
            let try_semver =
                |tag: &str| semver::Version::parse(tag.strip_prefix('v').unwrap_or(tag)).ok();
            let should_update_latest =
                match (try_semver(&existing.latest), try_semver(&entry.latest)) {
                    (Some(current), Some(new)) => new > current,
                    (Some(_), None) => false, // don't overwrite semver with non-semver
                    // semver replaces non-semver; both non-semver: last write wins
                    (None, _) => true,
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
        plugin_type: Option<PluginKind>,
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
        plugin_type: PluginKind,
        latest: &str,
        versions: &[&str],
    ) -> PluginIndexEntry {
        PluginIndexEntry {
            repository: repository.to_owned(),
            name: name.to_owned(),
            description: description.to_owned(),
            plugin_type,
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
            PluginKind::Source,
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
            PluginKind::Source,
            "1.0.0",
            &["1.0.0"],
        ));

        // Upsert v2.
        let updated = PluginIndexEntry {
            repository: "source/postgres".to_owned(),
            name: "Postgres Source".to_owned(),
            description: "Read rows from Postgres (updated)".to_owned(),
            plugin_type: PluginKind::Source,
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
            PluginKind::Source,
            "1.0.0",
            &["1.0.0"],
        ));

        // Push the same version a second time.
        idx.upsert(make_entry(
            "source/postgres",
            "Postgres Source",
            "desc",
            PluginKind::Source,
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
            PluginKind::Source,
            "1.0.0",
            &["1.0.0"],
        ));
        idx.upsert(make_entry(
            "destination/s3",
            "S3 Destination",
            "Write Parquet files to S3",
            PluginKind::Destination,
            "1.0.0",
            &["1.0.0"],
        ));
        idx.upsert(make_entry(
            "transform/sql",
            "SQL Transform",
            "Run arbitrary SQL over Arrow batches",
            PluginKind::Transform,
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
        let results = idx.search("", Some(PluginKind::Destination));
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
            PluginKind::Source,
            "2.0.0",
            &["2.0.0"],
        ));
        assert_eq!(idx.plugins[0].latest, "2.0.0");

        // Push 1.0.0 after — latest should stay at 2.0.0
        idx.upsert(make_entry(
            "source/postgres",
            "Postgres",
            "desc",
            PluginKind::Source,
            "1.0.0",
            &["1.0.0"],
        ));
        assert_eq!(idx.plugins[0].latest, "2.0.0");
        assert!(idx.plugins[0].versions.contains(&"1.0.0".to_owned()));
        assert!(idx.plugins[0].versions.contains(&"2.0.0".to_owned()));
    }

    #[test]
    fn upsert_advances_latest_from_prerelease_to_release() {
        let mut idx = PluginIndex::new();

        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "1.0.0-beta",
            &["1.0.0-beta"],
        ));
        assert_eq!(idx.plugins[0].latest, "1.0.0-beta");

        // Release 1.0.0 should advance latest past 1.0.0-beta
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "1.0.0",
            &["1.0.0"],
        ));
        assert_eq!(idx.plugins[0].latest, "1.0.0");
    }

    #[test]
    fn upsert_does_not_regress_release_to_prerelease() {
        let mut idx = PluginIndex::new();

        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "1.0.0",
            &["1.0.0"],
        ));

        // Pushing 1.0.1-rc1 should NOT regress latest back to a pre-release
        // at the same or lower version
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "1.0.0-rc1",
            &["1.0.0-rc1"],
        ));
        assert_eq!(idx.plugins[0].latest, "1.0.0");
    }

    #[test]
    fn upsert_advances_latest_for_newer_prerelease() {
        let mut idx = PluginIndex::new();

        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "1.0.0",
            &["1.0.0"],
        ));

        // 2.0.0-beta is a higher major version, should advance
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "2.0.0-beta",
            &["2.0.0-beta"],
        ));
        assert_eq!(idx.plugins[0].latest, "2.0.0-beta");
    }

    #[test]
    fn upsert_advances_prerelease_to_later_prerelease() {
        let mut idx = PluginIndex::new();

        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "1.0.0-beta",
            &["1.0.0-beta"],
        ));
        assert_eq!(idx.plugins[0].latest, "1.0.0-beta");

        // rc > beta lexicographically
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "1.0.0-rc",
            &["1.0.0-rc"],
        ));
        assert_eq!(idx.plugins[0].latest, "1.0.0-rc");
    }

    #[test]
    fn upsert_numeric_prerelease_ordering() {
        let mut idx = PluginIndex::new();

        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "1.0.0-beta.2",
            &["1.0.0-beta.2"],
        ));
        assert_eq!(idx.plugins[0].latest, "1.0.0-beta.2");

        // beta.10 > beta.2 numerically (not lexicographically)
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "1.0.0-beta.10",
            &["1.0.0-beta.10"],
        ));
        assert_eq!(idx.plugins[0].latest, "1.0.0-beta.10");

        // beta.3 should NOT regress from beta.10
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "1.0.0-beta.3",
            &["1.0.0-beta.3"],
        ));
        assert_eq!(idx.plugins[0].latest, "1.0.0-beta.10");
    }

    #[test]
    fn non_semver_versions_always_advance_latest() {
        let mut idx = PluginIndex::new();

        // 4-segment version is not semver — should always update
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "1.2.3.4",
            &["1.2.3.4"],
        ));
        assert_eq!(idx.plugins[0].latest, "1.2.3.4");

        // Pushing a 2-segment version (also not semver) should still update
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "1.2",
            &["1.2"],
        ));
        assert_eq!(idx.plugins[0].latest, "1.2");

        // Pushing a valid semver after non-semver should update
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "2.0.0",
            &["2.0.0"],
        ));
        assert_eq!(idx.plugins[0].latest, "2.0.0");
    }

    #[test]
    fn semver_rejects_non_standard_versions() {
        assert!(semver::Version::parse("1.2.3.4").is_err());
        assert!(semver::Version::parse("1.2").is_err());
    }

    #[test]
    fn semver_accepts_valid_versions() {
        assert!(semver::Version::parse("1.0.0").is_ok());
        assert!(semver::Version::parse("1.0.0-beta").is_ok());
        assert!(semver::Version::parse("2.1.3").is_ok());
        assert!(semver::Version::parse("1.0.0+build.5").is_ok());
    }

    #[test]
    fn upsert_handles_v_prefixed_tags() {
        let mut idx = PluginIndex::new();

        // v-prefixed tag treated as valid semver
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "v1.0.0",
            &["v1.0.0"],
        ));
        assert_eq!(idx.plugins[0].latest, "v1.0.0");

        // v2.0.0 > v1.0.0 even with prefix
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "v2.0.0",
            &["v2.0.0"],
        ));
        assert_eq!(idx.plugins[0].latest, "v2.0.0");

        // v1.5.0 should NOT regress from v2.0.0
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "v1.5.0",
            &["v1.5.0"],
        ));
        assert_eq!(idx.plugins[0].latest, "v2.0.0");

        // Mixed: non-prefixed 3.0.0 advances past v2.0.0
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "3.0.0",
            &["3.0.0"],
        ));
        assert_eq!(idx.plugins[0].latest, "3.0.0");

        // v2.5.0 should NOT regress from 3.0.0
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "v2.5.0",
            &["v2.5.0"],
        ));
        assert_eq!(idx.plugins[0].latest, "3.0.0");
    }

    #[test]
    fn build_metadata_ignored_for_comparison() {
        let mut idx = PluginIndex::new();
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "1.0.0+build.1",
            &["1.0.0+build.1"],
        ));
        assert_eq!(idx.plugins[0].latest, "1.0.0+build.1");

        // 1.0.1 > 1.0.0 regardless of build metadata
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "1.0.1",
            &["1.0.1"],
        ));
        assert_eq!(idx.plugins[0].latest, "1.0.1");

        // 1.0.0+build.99 should NOT regress from 1.0.1 (build metadata ignored)
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "1.0.0+build.99",
            &["1.0.0+build.99"],
        ));
        assert_eq!(idx.plugins[0].latest, "1.0.1");
    }

    #[test]
    fn non_semver_tag_does_not_overwrite_semver_latest() {
        let mut idx = PluginIndex::new();

        // Start with valid semver
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "2.0.0",
            &["2.0.0"],
        ));
        assert_eq!(idx.plugins[0].latest, "2.0.0");

        // Push non-semver tag — should NOT overwrite
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "nightly",
            &["nightly"],
        ));
        assert_eq!(idx.plugins[0].latest, "2.0.0");

        // Push 4-segment non-semver — should NOT overwrite
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "1.2.3.4",
            &["1.2.3.4"],
        ));
        assert_eq!(idx.plugins[0].latest, "2.0.0");

        // But valid higher semver SHOULD overwrite
        idx.upsert(make_entry(
            "p",
            "P",
            "d",
            PluginKind::Source,
            "3.0.0",
            &["3.0.0"],
        ));
        assert_eq!(idx.plugins[0].latest, "3.0.0");
    }

    #[test]
    fn deserialize_legacy_index_with_unknown_plugin_type() {
        let json = r#"{
            "schema_version": 1,
            "plugins": [{
                "repository": "test/legacy",
                "name": "Legacy Plugin",
                "description": "Has a non-standard plugin_type",
                "plugin_type": "unknown",
                "latest": "1.0.0",
                "versions": ["1.0.0"],
                "updated_at": "2026-01-01T00:00:00Z"
            }]
        }"#;
        let index: PluginIndex = serde_json::from_str(json)
            .expect("index with legacy plugin_type should still deserialize");
        assert_eq!(index.plugins.len(), 1);
        assert_eq!(index.plugins[0].plugin_type, PluginKind::Transform);
    }

    #[test]
    fn deserialize_index_with_standard_plugin_types() {
        let json = r#"{
            "schema_version": 1,
            "plugins": [
                {"repository":"a","name":"A","description":"","plugin_type":"source","latest":"1.0.0","versions":["1.0.0"],"updated_at":"2026-01-01T00:00:00Z"},
                {"repository":"b","name":"B","description":"","plugin_type":"destination","latest":"1.0.0","versions":["1.0.0"],"updated_at":"2026-01-01T00:00:00Z"},
                {"repository":"c","name":"C","description":"","plugin_type":"transform","latest":"1.0.0","versions":["1.0.0"],"updated_at":"2026-01-01T00:00:00Z"}
            ]
        }"#;
        let index: PluginIndex = serde_json::from_str(json).unwrap();
        assert_eq!(index.plugins[0].plugin_type, PluginKind::Source);
        assert_eq!(index.plugins[1].plugin_type, PluginKind::Destination);
        assert_eq!(index.plugins[2].plugin_type, PluginKind::Transform);
    }
}

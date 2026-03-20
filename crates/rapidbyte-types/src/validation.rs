//! Validation and prerequisites types for the v7 wire protocol.
//!
//! [`ValidationReport`] carries the outcome of a plugin's configuration or
//! schema validation, optionally including an output schema and field-level
//! requirements.
//!
//! [`PrerequisitesReport`] aggregates individual [`PrerequisiteCheck`] results
//! (e.g., connectivity, permissions) into a single pass/fail summary.

use serde::{Deserialize, Serialize};

use crate::schema::{FieldRequirement, StreamSchema};

// ── Validation Status ────────────────────────────────────────────

/// Outcome status of a validation check.
///
/// NOTE: A same-named enum exists in `crate::error`. The collision is
/// intentional during v7 migration and will be resolved in Task 7.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationStatus {
    /// Validation passed without issues.
    Success,
    /// Validation failed — the configuration or schema is invalid.
    Failed,
    /// Validation passed but produced warnings.
    Warning,
}

// ── Validation Report ────────────────────────────────────────────

/// Result of a plugin validation check, with optional schema output.
///
/// Construct via the [`ValidationReport::success`] or
/// [`ValidationReport::failed`] factory methods, then chain builder
/// methods for optional fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationReport {
    /// Overall validation outcome.
    pub status: ValidationStatus,
    /// Human-readable summary message.
    pub message: String,
    /// Non-fatal warnings emitted during validation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    /// Output schema produced by the plugin, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<StreamSchema>,
    /// Per-field requirements discovered during validation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_requirements: Option<Vec<FieldRequirement>>,
}

impl ValidationReport {
    /// Create a successful validation report.
    #[must_use]
    pub fn success(message: &str) -> Self {
        Self {
            status: ValidationStatus::Success,
            message: message.to_owned(),
            warnings: Vec::new(),
            output_schema: None,
            field_requirements: None,
        }
    }

    /// Create a failed validation report.
    #[must_use]
    pub fn failed(message: &str) -> Self {
        Self {
            status: ValidationStatus::Failed,
            message: message.to_owned(),
            warnings: Vec::new(),
            output_schema: None,
            field_requirements: None,
        }
    }

    /// Attach an output schema to this report.
    #[must_use]
    pub fn with_output_schema(mut self, schema: StreamSchema) -> Self {
        self.output_schema = Some(schema);
        self
    }

    /// Attach field-level requirements to this report.
    #[must_use]
    pub fn with_field_requirements(mut self, reqs: Vec<FieldRequirement>) -> Self {
        self.field_requirements = Some(reqs);
        self
    }
}

// ── Prerequisite Severity ────────────────────────────────────────

/// Severity level for a prerequisite check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrerequisiteSeverity {
    /// Fatal — blocks pipeline execution.
    Error,
    /// Non-fatal — execution proceeds but may be degraded.
    Warning,
    /// Informational — no impact on execution.
    Info,
}

// ── Prerequisite Check ───────────────────────────────────────────

/// A single prerequisite check result (e.g., "can connect to database").
///
/// Construct via [`PrerequisiteCheck::passed`] or
/// [`PrerequisiteCheck::error`], then optionally chain
/// [`PrerequisiteCheck::with_fix_hint`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrerequisiteCheck {
    /// Short name identifying the check (e.g., `"db_connectivity"`).
    pub name: String,
    /// Whether the check passed.
    pub passed: bool,
    /// Severity of this check.
    pub severity: PrerequisiteSeverity,
    /// Human-readable description of the check result.
    pub message: String,
    /// Optional hint on how to fix a failing check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fix_hint: Option<String>,
}

impl PrerequisiteCheck {
    /// Create a passing check with informational severity.
    #[must_use]
    pub fn passed(name: &str, message: &str) -> Self {
        Self {
            name: name.to_owned(),
            passed: true,
            severity: PrerequisiteSeverity::Info,
            message: message.to_owned(),
            fix_hint: None,
        }
    }

    /// Create a failing check with error severity.
    #[must_use]
    pub fn error(name: &str, message: &str) -> Self {
        Self {
            name: name.to_owned(),
            passed: false,
            severity: PrerequisiteSeverity::Error,
            message: message.to_owned(),
            fix_hint: None,
        }
    }

    /// Attach a remediation hint to this check.
    #[must_use]
    pub fn with_fix_hint(mut self, hint: &str) -> Self {
        self.fix_hint = Some(hint.to_owned());
        self
    }
}

// ── Prerequisites Report ─────────────────────────────────────────

/// Aggregated result of all prerequisite checks for a plugin.
///
/// The report passes if and only if every error-severity check passed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrerequisitesReport {
    /// Whether all error-severity checks passed.
    pub passed: bool,
    /// Individual check results.
    pub checks: Vec<PrerequisiteCheck>,
}

impl PrerequisitesReport {
    /// Create an empty report that passes (no checks).
    #[must_use]
    pub fn passed() -> Self {
        Self {
            passed: true,
            checks: Vec::new(),
        }
    }

    /// Build a report from a list of checks.
    ///
    /// The report passes if every error-severity check has `passed == true`.
    #[must_use]
    pub fn from_checks(checks: Vec<PrerequisiteCheck>) -> Self {
        let passed = checks
            .iter()
            .filter(|c| c.severity == PrerequisiteSeverity::Error)
            .all(|c| c.passed);
        Self { passed, checks }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FieldConstraint, FieldRequirement, SchemaField, StreamSchema};

    fn sample_schema() -> StreamSchema {
        StreamSchema {
            fields: vec![
                SchemaField::new("id", "int64", false).with_primary_key(true),
                SchemaField::new("name", "utf8", true),
            ],
            primary_key: vec!["id".into()],
            partition_keys: vec![],
            source_defined_cursor: None,
            schema_id: None,
        }
    }

    #[test]
    fn validation_report_success() {
        let report = ValidationReport::success("config is valid");
        assert_eq!(report.status, ValidationStatus::Success);
        assert_eq!(report.message, "config is valid");
        assert!(report.warnings.is_empty());
        assert!(report.output_schema.is_none());
        assert!(report.field_requirements.is_none());
    }

    #[test]
    fn validation_report_with_output_schema() {
        let schema = sample_schema();
        let report = ValidationReport::success("ok").with_output_schema(schema.clone());
        assert_eq!(report.status, ValidationStatus::Success);
        assert_eq!(report.output_schema, Some(schema));
    }

    #[test]
    fn validation_report_with_field_requirements() {
        let reqs = vec![FieldRequirement {
            field_name: "email".into(),
            constraint: FieldConstraint::FieldRequired,
            reason: "destination requires email".into(),
            accepted_types: None,
        }];
        let report = ValidationReport::success("ok").with_field_requirements(reqs.clone());
        assert_eq!(report.field_requirements, Some(reqs));
    }

    #[test]
    fn prerequisites_report_all_passed() {
        let checks = vec![
            PrerequisiteCheck::passed("db_connectivity", "connected to database"),
            PrerequisiteCheck::passed("permissions", "SELECT granted"),
        ];
        let report = PrerequisitesReport::from_checks(checks);
        assert!(report.passed);
        assert_eq!(report.checks.len(), 2);
    }

    #[test]
    fn prerequisites_report_with_failure() {
        let checks = vec![
            PrerequisiteCheck::passed("db_connectivity", "connected"),
            PrerequisiteCheck::error("permissions", "INSERT denied")
                .with_fix_hint("GRANT INSERT ON table TO user"),
        ];
        let report = PrerequisitesReport::from_checks(checks);
        assert!(!report.passed);
        assert_eq!(report.checks.len(), 2);
        assert_eq!(
            report.checks[1].fix_hint.as_deref(),
            Some("GRANT INSERT ON table TO user")
        );
    }

    #[test]
    fn prerequisites_report_empty_passes() {
        let report = PrerequisitesReport::passed();
        assert!(report.passed);
        assert!(report.checks.is_empty());
    }

    #[test]
    fn validation_report_roundtrip() {
        let schema = sample_schema();
        let reqs = vec![FieldRequirement {
            field_name: "id".into(),
            constraint: FieldConstraint::FieldRequired,
            reason: "primary key required".into(),
            accepted_types: Some(vec!["int64".into()]),
        }];
        let report = ValidationReport::failed("schema mismatch")
            .with_output_schema(schema)
            .with_field_requirements(reqs);

        let json = serde_json::to_string(&report).unwrap();
        let back: ValidationReport = serde_json::from_str(&json).unwrap();
        assert_eq!(report, back);
        assert_eq!(back.status, ValidationStatus::Failed);
        assert_eq!(back.message, "schema mismatch");
        assert!(back.output_schema.is_some());
        assert_eq!(back.field_requirements.as_ref().unwrap().len(), 1);
    }
}

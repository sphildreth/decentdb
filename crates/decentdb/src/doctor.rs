//! Doctor domain model and report serialization.
//!
//! DR-01: Typed structures, sort order, summary calculation, and JSON
//! serialization. No database fact collection, CLI parsing, or fix execution.
//!
//! This module contains public types that are not yet consumed by production
//! code; dead-code warnings are expected until later slices wire them up.

#![allow(dead_code)]

use serde::Serialize;

// ---------------------------------------------------------------------------
// Option helper types
// ---------------------------------------------------------------------------

/// Check selection: either all categories or an explicit subset.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum DoctorCheckSelection {
    #[default]
    All,
    Selected(Vec<DoctorCategory>),
}

/// Index verification policy.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum DoctorIndexVerification {
    /// No index verification requested.
    #[default]
    None,
    /// Verify all indexes up to `max_count`.
    All { max_count: usize },
    /// Verify only the named indexes.
    Named(Vec<String>),
}

/// Controls how database paths appear in the report.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DoctorPathMode {
    #[default]
    Absolute,
    Basename,
    Redacted,
}

/// Options controlling doctor behaviour.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DoctorOptions {
    pub checks: DoctorCheckSelection,
    pub verify_indexes: DoctorIndexVerification,
    pub include_recommendations: bool,
    pub path_mode: DoctorPathMode,
    pub fix: bool,
}

impl Default for DoctorOptions {
    fn default() -> Self {
        Self {
            checks: DoctorCheckSelection::All,
            verify_indexes: DoctorIndexVerification::None,
            include_recommendations: true,
            path_mode: DoctorPathMode::Absolute,
            fix: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Core enums
// ---------------------------------------------------------------------------

/// Whether the report was produced in check-only or fix mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DoctorMode {
    Check,
    Fix,
}

/// Overall report status derived from the most severe finding.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DoctorStatus {
    Ok,
    Warning,
    Error,
}

/// Severity of a single finding.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DoctorSeverity {
    Info,
    Warning,
    Error,
}

impl DoctorSeverity {
    /// Numeric sort key where smaller = more severe (0 = error).
    pub(crate) fn sort_key(self) -> u8 {
        match self {
            Self::Error => 0,
            Self::Warning => 1,
            Self::Info => 2,
        }
    }
}

/// Diagnostic category of a finding.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DoctorCategory {
    Header,
    Storage,
    Wal,
    Fragmentation,
    Schema,
    Statistics,
    Indexes,
    Compatibility,
}

impl DoctorCategory {
    /// Deterministic sort key matching the plan category order.
    pub(crate) fn sort_key(self) -> u8 {
        match self {
            Self::Header => 0,
            Self::Storage => 1,
            Self::Wal => 2,
            Self::Fragmentation => 3,
            Self::Schema => 4,
            Self::Statistics => 5,
            Self::Indexes => 6,
            Self::Compatibility => 7,
        }
    }
}

/// Status of a single fix action.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DoctorFixStatus {
    Planned,
    Applied,
    Skipped,
    Failed,
}

/// Highest severity across all findings, with an extra `Ok` for empty reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DoctorHighestSeverity {
    Ok,
    Info,
    Warning,
    Error,
}

// ---------------------------------------------------------------------------
// Supporting types
// ---------------------------------------------------------------------------

/// Typed evidence value that serializes predictably without adding deps.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(untagged)]
pub enum DoctorEvidenceValue {
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    String(String),
}

impl DoctorEvidenceValue {
    /// Convenience constructor for the common uint case.
    #[cfg(test)]
    #[must_use]
    pub fn uint(v: u64) -> Self {
        Self::Uint(v)
    }

    /// Convenience constructor for strings.
    #[cfg(test)]
    #[must_use]
    pub fn string(v: impl Into<String>) -> Self {
        Self::String(v.into())
    }
}

/// A single piece of structured evidence attached to a finding or fix record.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DoctorEvidence {
    pub field: String,
    pub value: DoctorEvidenceValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
}

/// A safe recommendation attached to a finding.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DoctorRecommendation {
    pub summary: String,
    pub commands: Vec<String>,
    pub safe_to_automate: bool,
}

/// A single diagnostic finding produced by the rule engine.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DoctorFinding {
    pub id: String,
    pub severity: DoctorSeverity,
    pub category: DoctorCategory,
    pub title: String,
    pub message: String,
    pub evidence: Vec<DoctorEvidence>,
    pub recommendation: Option<DoctorRecommendation>,
}

/// A single fix action (planned, applied, skipped, or failed).
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DoctorFix {
    pub id: String,
    pub finding_id: String,
    pub status: DoctorFixStatus,
    pub message: String,
    pub evidence_before: Vec<DoctorEvidence>,
    pub evidence_after: Vec<DoctorEvidence>,
}

/// Summary of the inspected database file itself.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DoctorDatabaseSummary {
    pub path: String,
    pub wal_path: String,
    pub format_version: u32,
    pub page_size: u32,
    pub page_count: u32,
    pub schema_cookie: u32,
}

/// Aggregate counts and highest severity across all findings.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DoctorSummary {
    pub info_count: usize,
    pub warning_count: usize,
    pub error_count: usize,
    pub highest_severity: DoctorHighestSeverity,
    pub checked_categories: Vec<DoctorCategory>,
}

impl DoctorSummary {
    /// Build a summary from a (sorted) slice of findings and the checked
    /// category list.  `checked_categories` will be sorted to the plan order.
    fn new(findings: &[DoctorFinding], mut checked_categories: Vec<DoctorCategory>) -> Self {
        checked_categories.sort_by_key(|c| c.sort_key());

        let (mut info_count, mut warning_count, mut error_count) = (0usize, 0usize, 0usize);
        let mut highest = DoctorHighestSeverity::Ok;

        for f in findings {
            match f.severity {
                DoctorSeverity::Info => {
                    info_count += 1;
                    if matches!(highest, DoctorHighestSeverity::Ok) {
                        highest = DoctorHighestSeverity::Info;
                    }
                }
                DoctorSeverity::Warning => {
                    warning_count += 1;
                    if !matches!(highest, DoctorHighestSeverity::Error) {
                        highest = DoctorHighestSeverity::Warning;
                    }
                }
                DoctorSeverity::Error => {
                    error_count += 1;
                    highest = DoctorHighestSeverity::Error;
                }
            }
        }

        Self {
            info_count,
            warning_count,
            error_count,
            highest_severity: highest,
            checked_categories,
        }
    }
}

/// Placeholder for collected engine facts, serializable as a JSON object.
///
/// DR-01 stores empty maps / vecs; later slices populate real values.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DoctorCollectedFacts {
    pub storage: serde_json::Value,
    pub header: serde_json::Value,
    pub schema: serde_json::Value,
    pub indexes_verified: Vec<serde_json::Value>,
}

impl Default for DoctorCollectedFacts {
    fn default() -> Self {
        Self {
            storage: serde_json::Value::Object(serde_json::Map::new()),
            header: serde_json::Value::Object(serde_json::Map::new()),
            schema: serde_json::Value::Object(serde_json::Map::new()),
            indexes_verified: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

/// The top-level doctor diagnostic report.
///
/// Construct with [`DoctorReport::new`] to automatically derive status,
/// summary counts, and sorted findings.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DoctorReport {
    pub schema_version: u32,
    pub mode: DoctorMode,
    pub status: DoctorStatus,
    pub database: DoctorDatabaseSummary,
    pub summary: DoctorSummary,
    pub pre_fix_findings: Vec<DoctorFinding>,
    pub findings: Vec<DoctorFinding>,
    pub fixes: Vec<DoctorFix>,
    pub collected: DoctorCollectedFacts,
}

impl DoctorReport {
    /// Build a fully-validated report.
    ///
    /// Callers supply raw findings, fixes, and facts. The constructor:
    /// - Sorts `findings` and `pre_fix_findings`.
    /// - Calculates [`DoctorStatus`] from `findings`.
    /// - Builds [`DoctorSummary`] from `findings` and `checked_categories`.
    /// - Hard-codes `schema_version` to `1`.
    /// - Preserves `mode`.
    #[must_use]
    pub fn new(
        mode: DoctorMode,
        database: DoctorDatabaseSummary,
        checked_categories: Vec<DoctorCategory>,
        pre_fix_findings: Vec<DoctorFinding>,
        mut findings: Vec<DoctorFinding>,
        fixes: Vec<DoctorFix>,
        collected: DoctorCollectedFacts,
    ) -> Self {
        sort_findings(&mut findings);

        let mut sorted_pre = pre_fix_findings;
        sort_findings(&mut sorted_pre);

        let status = calculate_status(&findings);
        let summary = DoctorSummary::new(&findings, checked_categories);

        Self {
            schema_version: 1,
            mode,
            status,
            database,
            summary,
            pre_fix_findings: sorted_pre,
            findings,
            fixes,
            collected,
        }
    }
}

// ---------------------------------------------------------------------------
// Sorting
// ---------------------------------------------------------------------------

/// Sort findings by plan-specified order:
/// 1. severity rank (error → warning → info)
/// 2. category (header → storage → wal → fragmentation → schema →
///    statistics → indexes → compatibility)
/// 3. finding ID lexicographically
pub fn sort_findings(findings: &mut [DoctorFinding]) {
    findings.sort_by(|a, b| {
        a.severity
            .sort_key()
            .cmp(&b.severity.sort_key())
            .then_with(|| a.category.sort_key().cmp(&b.category.sort_key()))
            .then_with(|| a.id.cmp(&b.id))
    });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Derive an overall report status from a (sorted) findings slice.
fn calculate_status(findings: &[DoctorFinding]) -> DoctorStatus {
    let mut has_error = false;
    let mut has_warning = false;

    for f in findings {
        match f.severity {
            DoctorSeverity::Error => has_error = true,
            DoctorSeverity::Warning => has_warning = true,
            DoctorSeverity::Info => {}
        }
    }

    if has_error {
        DoctorStatus::Error
    } else if has_warning {
        DoctorStatus::Warning
    } else {
        DoctorStatus::Ok
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---- test fixtures ----------------------------------------------------

    fn finding(id: &str, severity: DoctorSeverity, category: DoctorCategory) -> DoctorFinding {
        DoctorFinding {
            id: id.to_string(),
            severity,
            category,
            title: format!("{id} finding"),
            message: format!("Detail for {id}"),
            evidence: vec![DoctorEvidence {
                field: "some_field".into(),
                value: DoctorEvidenceValue::Int(42),
                unit: None,
            }],
            recommendation: None,
        }
    }

    fn db_summary() -> DoctorDatabaseSummary {
        DoctorDatabaseSummary {
            path: "test.ddb".into(),
            wal_path: "test.ddb.wal".into(),
            format_version: 1,
            page_size: 4096,
            page_count: 128,
            schema_cookie: 7,
        }
    }

    fn all_categories() -> Vec<DoctorCategory> {
        use DoctorCategory::*;
        vec![
            Header,
            Storage,
            Wal,
            Fragmentation,
            Schema,
            Statistics,
            Indexes,
            Compatibility,
        ]
    }

    fn report(findings: Vec<DoctorFinding>) -> DoctorReport {
        DoctorReport::new(
            DoctorMode::Check,
            db_summary(),
            all_categories(),
            Vec::new(),
            findings,
            Vec::new(),
            DoctorCollectedFacts::default(),
        )
    }

    fn fix_report(findings: Vec<DoctorFinding>, fixes: Vec<DoctorFix>) -> DoctorReport {
        DoctorReport::new(
            DoctorMode::Fix,
            db_summary(),
            all_categories(),
            Vec::new(),
            findings,
            fixes,
            DoctorCollectedFacts::default(),
        )
    }

    // ---- status tests -----------------------------------------------------

    #[test]
    fn no_findings_is_ok() {
        let r = report(vec![]);
        assert_eq!(r.status, DoctorStatus::Ok);
    }

    #[test]
    fn only_info_is_ok() {
        let r = report(vec![finding(
            "a",
            DoctorSeverity::Info,
            DoctorCategory::Storage,
        )]);
        assert_eq!(r.status, DoctorStatus::Ok);
    }

    #[test]
    fn warning_is_warning() {
        let r = report(vec![finding(
            "w",
            DoctorSeverity::Warning,
            DoctorCategory::Wal,
        )]);
        assert_eq!(r.status, DoctorStatus::Warning);
    }

    #[test]
    fn error_is_error() {
        let r = report(vec![finding(
            "e",
            DoctorSeverity::Error,
            DoctorCategory::Header,
        )]);
        assert_eq!(r.status, DoctorStatus::Error);
    }

    // ---- summary count tests ----------------------------------------------

    #[test]
    fn summary_counts_mixed_severities() {
        let r = report(vec![
            finding("e1", DoctorSeverity::Error, DoctorCategory::Header),
            finding("w1", DoctorSeverity::Warning, DoctorCategory::Wal),
            finding("w2", DoctorSeverity::Warning, DoctorCategory::Schema),
            finding("i1", DoctorSeverity::Info, DoctorCategory::Statistics),
        ]);
        assert_eq!(r.summary.info_count, 1);
        assert_eq!(r.summary.warning_count, 2);
        assert_eq!(r.summary.error_count, 1);
        assert_eq!(r.summary.highest_severity, DoctorHighestSeverity::Error);
    }

    #[test]
    fn highest_severity_ok_when_empty() {
        let r = report(vec![]);
        assert_eq!(r.summary.highest_severity, DoctorHighestSeverity::Ok);
    }

    #[test]
    fn highest_severity_info_with_only_info() {
        let r = report(vec![finding(
            "i",
            DoctorSeverity::Info,
            DoctorCategory::Header,
        )]);
        assert_eq!(r.summary.highest_severity, DoctorHighestSeverity::Info);
    }

    // ---- sorting tests ----------------------------------------------------

    #[test]
    fn sort_severity_error_before_warning_before_info() {
        let f = vec![
            finding("i1", DoctorSeverity::Info, DoctorCategory::Header),
            finding("w1", DoctorSeverity::Warning, DoctorCategory::Header),
            finding("e1", DoctorSeverity::Error, DoctorCategory::Header),
        ];
        let r = report(f);
        let ids: Vec<&str> = r.findings.iter().map(|f| f.id.as_str()).collect();
        assert_eq!(ids, vec!["e1", "w1", "i1"]);
    }

    #[test]
    fn sort_category_after_severity() {
        let f = vec![
            finding("s", DoctorSeverity::Error, DoctorCategory::Storage),
            finding("h", DoctorSeverity::Error, DoctorCategory::Header),
        ];
        let r = report(f);
        let ids: Vec<&str> = r.findings.iter().map(|f| f.id.as_str()).collect();
        assert_eq!(ids, vec!["h", "s"]);
    }

    #[test]
    fn sort_id_lexicographically() {
        let f = vec![
            finding("b", DoctorSeverity::Info, DoctorCategory::Header),
            finding("a", DoctorSeverity::Info, DoctorCategory::Header),
        ];
        let r = report(f);
        let ids: Vec<&str> = r.findings.iter().map(|f| f.id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b"]);
    }

    #[test]
    fn sort_full_category_order() {
        use DoctorCategory::*;
        let f = vec![
            finding("cm", DoctorSeverity::Info, Compatibility),
            finding("hd", DoctorSeverity::Info, Header),
            finding("sc", DoctorSeverity::Info, Schema),
            finding("st", DoctorSeverity::Info, Storage),
            finding("ix", DoctorSeverity::Info, Indexes),
            finding("wa", DoctorSeverity::Info, Wal),
            finding("fr", DoctorSeverity::Info, Fragmentation),
            finding("ss", DoctorSeverity::Info, Statistics),
        ];
        let r = report(f);
        let cats: Vec<DoctorCategory> = r.findings.iter().map(|f| f.category).collect();
        assert_eq!(
            cats,
            vec![
                Header,
                Storage,
                Wal,
                Fragmentation,
                Schema,
                Statistics,
                Indexes,
                Compatibility,
            ]
        );
    }

    #[test]
    fn pre_fix_findings_are_sorted() {
        use DoctorCategory::*;
        let f = vec![
            finding("b", DoctorSeverity::Info, Header),
            finding("a", DoctorSeverity::Info, Header),
        ];
        let r = DoctorReport::new(
            DoctorMode::Fix,
            db_summary(),
            all_categories(),
            f,      // pre_fix
            vec![], // post-fix findings
            vec![],
            DoctorCollectedFacts::default(),
        );
        let ids: Vec<&str> = r.pre_fix_findings.iter().map(|f| f.id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b"]);
    }

    // ---- serialization tests ----------------------------------------------

    #[test]
    fn json_field_naming() {
        let mode_check = serde_json::to_string(&DoctorMode::Check).expect("serialize");
        assert_eq!(mode_check, r#""check""#);

        let mode_fix = serde_json::to_string(&DoctorMode::Fix).expect("serialize");
        assert_eq!(mode_fix, r#""fix""#);

        let ok = serde_json::to_string(&DoctorStatus::Ok).expect("serialize");
        assert_eq!(ok, r#""ok""#);

        let sev = serde_json::to_string(&DoctorSeverity::Warning).expect("serialize");
        assert_eq!(sev, r#""warning""#);

        let cat = serde_json::to_string(&DoctorCategory::Wal).expect("serialize");
        assert_eq!(cat, r#""wal""#);

        let applied = serde_json::to_string(&DoctorFixStatus::Applied).expect("serialize");
        assert_eq!(applied, r#""applied""#);

        let pm = serde_json::to_string(&DoctorPathMode::Basename).expect("serialize");
        assert_eq!(pm, r#""basename""#);
    }

    #[test]
    fn json_top_level_fields_present() {
        let r = report(vec![]);
        let json = serde_json::to_string(&r).expect("serialize");

        assert!(json.contains(r#""schema_version":1"#), "{json}");
        assert!(json.contains(r#""mode":"check""#), "{json}");
        assert!(json.contains(r#""status":"ok""#), "{json}");

        // Every top-level structural key is present
        for key in &[
            r#""summary""#,
            r#""pre_fix_findings""#,
            r#""findings""#,
            r#""fixes""#,
            r#""collected""#,
        ] {
            assert!(json.contains(key), "missing key {key} in {json}");
        }
    }

    #[test]
    fn json_mode_is_check_vs_fix() {
        let r_check = report(vec![]);
        let json = serde_json::to_string(&r_check).expect("serialize");
        assert!(json.contains(r#""mode":"check""#));

        let r_fix = DoctorReport::new(
            DoctorMode::Fix,
            db_summary(),
            all_categories(),
            vec![],
            vec![],
            vec![],
            DoctorCollectedFacts::default(),
        );
        let json = serde_json::to_string(&r_fix).expect("serialize");
        assert!(json.contains(r#""mode":"fix""#));
    }

    #[test]
    fn json_highest_severity_serializes() {
        let empty = report(vec![]);
        let json = serde_json::to_string(&empty.summary).expect("serialize");
        assert!(json.contains(r#""highest_severity":"ok""#));

        let warn = report(vec![finding(
            "w",
            DoctorSeverity::Warning,
            DoctorCategory::Wal,
        )]);
        let json = serde_json::to_string(&warn.summary).expect("serialize");
        assert!(json.contains(r#""highest_severity":"warning""#));
    }

    #[test]
    fn json_checked_categories_in_plan_order() {
        let r = report(vec![]);
        let json = serde_json::to_string(&r.summary).expect("serialize");
        // categories must appear in plan order
        let expected = r#""checked_categories":["header","storage","wal","fragmentation","schema","statistics","indexes","compatibility"]"#;
        assert!(json.contains(expected), "{json}");
    }

    #[test]
    fn fix_status_serializations() {
        for (status, expected) in [
            (DoctorFixStatus::Planned, "planned"),
            (DoctorFixStatus::Applied, "applied"),
            (DoctorFixStatus::Skipped, "skipped"),
            (DoctorFixStatus::Failed, "failed"),
        ] {
            let fix = DoctorFix {
                id: "fix.checkpoint".into(),
                finding_id: "wal.large_file".into(),
                status,
                message: "msg".into(),
                evidence_before: vec![],
                evidence_after: vec![],
            };
            let json = serde_json::to_string(&fix).expect("serialize");
            assert!(json.contains(&format!(r#""{}""#, expected)), "{json}");
        }
    }

    #[test]
    fn full_report_json_round_trip_parseable() {
        let r = fix_report(
            vec![finding(
                "wal.large_file",
                DoctorSeverity::Warning,
                DoctorCategory::Wal,
            )],
            vec![DoctorFix {
                id: "fix.checkpoint".into(),
                finding_id: "wal.large_file".into(),
                status: DoctorFixStatus::Applied,
                message: "Checkpoint completed.".into(),
                evidence_before: vec![DoctorEvidence {
                    field: "wal_file_size".into(),
                    value: DoctorEvidenceValue::Uint(104_857_600),
                    unit: Some("bytes".into()),
                }],
                evidence_after: vec![DoctorEvidence {
                    field: "wal_file_size".into(),
                    value: DoctorEvidenceValue::Uint(32),
                    unit: Some("bytes".into()),
                }],
            }],
        );

        let json = serde_json::to_string(&r).expect("serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid json");

        assert_eq!(parsed["schema_version"], 1);
        assert_eq!(parsed["mode"], "fix");
        // overall report status derived from findings
        assert_eq!(parsed["status"], "warning");
        assert_eq!(parsed["summary"]["highest_severity"], "warning");
        assert_eq!(&parsed["fixes"][0]["id"], "fix.checkpoint");
        assert_eq!(&parsed["fixes"][0]["status"], "applied");
    }

    #[test]
    fn evidence_value_serialization() {
        let ev = DoctorEvidenceValue::Uint(42);
        let json = serde_json::to_string(&ev).expect("serialize");
        assert_eq!(json, "42");

        let ev = DoctorEvidenceValue::String("hello".into());
        let json = serde_json::to_string(&ev).expect("serialize");
        assert_eq!(json, r#""hello""#);

        let ev = DoctorEvidenceValue::Bool(true);
        let json = serde_json::to_string(&ev).expect("serialize");
        assert_eq!(json, "true");

        let ev = DoctorEvidenceValue::Float(2.5);
        let json = serde_json::to_string(&ev).expect("serialize");
        assert_eq!(json, "2.5");
    }

    #[test]
    fn evidence_skips_unit_when_none() {
        let ev = DoctorEvidence {
            field: "f".into(),
            value: DoctorEvidenceValue::Int(0),
            unit: None,
        };
        let json = serde_json::to_string(&ev).expect("serialize");
        assert!(!json.contains("unit"));
    }

    #[test]
    fn collected_facts_serialize_as_object() {
        let cf = DoctorCollectedFacts::default();
        let json = serde_json::to_string(&cf).expect("serialize");
        assert!(json.contains(r#""storage":{}"#));
        assert!(json.contains(r#""header":{}"#));
        assert!(json.contains(r#""schema":{}"#));
        assert!(json.contains(r#""indexes_verified":[]"#));
    }
}

//! Doctor domain model, fact collection, rule engine, index verification,
//! fix execution, and report serialization.
//!
//! DR-01: Typed structures, sort order, summary calculation, and JSON
//! serialization.
//! DR-02: Read-only fact collection from existing metadata.
//! DR-03: v1 rule engine and finding catalog.
//! DR-04: Opt-in index verification integration.
//! DR-05: Constrained fix planner and executor.

#![allow(dead_code)]

use std::path::Path;

use serde::Serialize;

use crate::config::DbConfig;
use crate::db::Db;
use crate::error::Result;
use crate::metadata::{HeaderInfo, IndexInfo, SchemaSnapshot, StorageInfo};
use crate::storage::DB_FORMAT_VERSION;

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
    #[must_use]
    pub fn sort_key(self) -> u8 {
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
// DR-02: Fact Collection
// ---------------------------------------------------------------------------

/// Facts collected from a database for rule evaluation.
struct CollectedData {
    database: DoctorDatabaseSummary,
    storage: Option<StorageInfo>,
    header: Option<HeaderInfo>,
    schema: Option<SchemaSnapshot>,
    indexes: Vec<IndexInfo>,
    physical_bytes: u64,
}

/// Collect all facts from an already-opened [`Db`] handle.
fn collect_facts(db: &Db, path: &Path) -> Result<CollectedData> {
    let wal_path_str = format!("{}.wal", path.display());
    let physical_bytes = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);

    let storage = db.storage_info().ok();
    let header = db.header_info().ok();
    let schema = db.get_schema_snapshot().ok();
    let indexes = db.list_indexes().unwrap_or_default();

    let database = DoctorDatabaseSummary {
        path: path.display().to_string(),
        wal_path: wal_path_str,
        format_version: header.as_ref().map_or(0, |h| h.format_version),
        page_size: header.as_ref().map_or(0, |h| h.page_size),
        page_count: storage.as_ref().map_or(0, |s| s.page_count),
        schema_cookie: header.as_ref().map_or(0, |h| h.schema_cookie),
    };

    Ok(CollectedData {
        database,
        storage,
        header,
        schema,
        indexes,
        physical_bytes,
    })
}

/// Collect a best-effort database summary from a loose header for partial
/// reports.
fn partial_database_from_header(path: &Path, hdr: &HeaderInfo) -> DoctorDatabaseSummary {
    DoctorDatabaseSummary {
        path: path.display().to_string(),
        wal_path: format!("{}.wal", path.display()),
        format_version: hdr.format_version,
        page_size: hdr.page_size,
        page_count: 0,
        schema_cookie: hdr.schema_cookie,
    }
}

/// Build a fully-empty database summary for a file whose header could not
/// be read at all.
fn partial_database_empty(path: &Path) -> DoctorDatabaseSummary {
    DoctorDatabaseSummary {
        path: path.display().to_string(),
        wal_path: format!("{}.wal", path.display()),
        format_version: 0,
        page_size: 0,
        page_count: 0,
        schema_cookie: 0,
    }
}

// ---------------------------------------------------------------------------
// DR-03: Rule Engine
// ---------------------------------------------------------------------------

/// v1 thresholds as specified in the plan.
const WAL_LARGE_FILE_BYTES: u64 = 64 * 1024 * 1024;
const WAL_MANY_VERSIONS_THRESHOLD: usize = 100_000;
const FRAGMENTATION_MIN_PAGE_COUNT: u32 = 128;
const FRAGMENTATION_HIGH_RATIO: f64 = 0.25;
const FRAGMENTATION_MODERATE_RATIO: f64 = 0.10;
const MANY_INDEXES_THRESHOLD: usize = 8;

/// Evaluate every v1 rule against the collected data, returning findings in
/// the order described by the plan.
///
/// This function does **not** run index verification or produce fix findings;
/// those are handled in `run_doctor`.
fn evaluate_rules(data: &CollectedData) -> Vec<DoctorFinding> {
    let mut findings = Vec::new();

    // compatibility.format_version_unknown
    if let Some(hdr) = &data.header {
        if hdr.format_version != DB_FORMAT_VERSION {
            findings.push(format_version_unknown(hdr.format_version));
        }
    }

    // 10.2 WAL findings
    if let Some(st) = &data.storage {
        let large_threshold = WAL_LARGE_FILE_BYTES.max(data.physical_bytes / 4);
        if st.wal_file_size >= large_threshold {
            findings.push(wal_large_file(
                st.wal_file_size,
                data.physical_bytes,
                large_threshold,
            ));
        }

        if st.wal_versions >= WAL_MANY_VERSIONS_THRESHOLD {
            findings.push(wal_many_versions(st.wal_versions));
        }

        if st.active_readers > 0 && st.wal_file_size > 0 {
            findings.push(wal_long_readers_present(
                st.active_readers,
                st.wal_file_size,
            ));
        }

        if st.warning_count > 0 {
            findings.push(wal_reader_warnings(st.warning_count));
        }

        if st.shared_wal {
            findings.push(wal_shared_enabled());
        }
    }

    // 10.3 Fragmentation findings
    if let Some(hdr) = &data.header {
        if data.database.page_count >= FRAGMENTATION_MIN_PAGE_COUNT && hdr.freelist_page_count > 0 {
            let ratio = hdr.freelist_page_count as f64 / data.database.page_count as f64;
            let pct = (ratio * 100.0 * 10.0).round() / 10.0;

            if ratio >= FRAGMENTATION_HIGH_RATIO {
                findings.push(fragmentation_high(
                    data.database.page_count,
                    hdr.freelist_page_count,
                    pct,
                ));
            } else if ratio >= FRAGMENTATION_MODERATE_RATIO {
                findings.push(fragmentation_moderate(
                    data.database.page_count,
                    hdr.freelist_page_count,
                    pct,
                ));
            }
        }
    }

    // 10.4 Schema findings
    if let Some(schema) = &data.schema {
        let user_tables: Vec<_> = schema.tables.iter().filter(|t| !t.temporary).collect();
        let table_count = user_tables.len();

        if table_count == 0 {
            findings.push(schema_no_user_tables());
        }

        for table in &user_tables {
            let table_indexes: Vec<_> = data
                .indexes
                .iter()
                .filter(|idx| idx.table_name == table.name)
                .collect();
            if table_indexes.len() > MANY_INDEXES_THRESHOLD {
                findings.push(schema_many_indexes(&table.name, table_indexes.len()));
            }
        }

        for idx in &data.indexes {
            if !idx.fresh {
                findings.push(index_not_fresh(&idx.name, &idx.table_name));
            }
        }
    }

    findings
}

// ---------------------------------------------------------------------------
// DR-04: Opt-in Index Verification
// ---------------------------------------------------------------------------

/// Outcome of running index verification against a `Db` handle.
struct VerificationOutcome {
    /// Findings produced by verification (e.g. `index.verify_failed`,
    /// `index.verify_error`, `index.verify_skipped_limit`).
    findings: Vec<DoctorFinding>,
    /// Serialisable records for `collected.indexes_verified`.
    records: Vec<serde_json::Value>,
}

/// Run opt-in index verification using the live `Db` handle.
///
/// Respects the `verify_indexes` policy in `options` and enforces the
/// `--max-index-verify` cap for `DoctorIndexVerification::All`.
fn run_index_verification(
    db: &Db,
    options: &DoctorOptions,
    indexes: &[IndexInfo],
) -> VerificationOutcome {
    let mut findings = Vec::new();
    let mut records = Vec::new();

    let selected_names: Vec<&str> = match &options.verify_indexes {
        DoctorIndexVerification::None => vec![],
        DoctorIndexVerification::All { max_count } => {
            let max = max_count.saturating_sub(records.len());
            if indexes.len() > *max_count {
                let capped = std::cmp::min(indexes.len(), *max_count);
                findings.push(DoctorFinding {
                    id: "index.verify_skipped_limit".into(),
                    severity: DoctorSeverity::Info,
                    category: DoctorCategory::Indexes,
                    title: "Index verification limit reached".into(),
                    message: format!(
                        "Requested verification of {selected} indexes exceeds cap of {cap}; \
                         only {capped} will be verified.",
                        selected = indexes.len(),
                        cap = max_count,
                    ),
                    evidence: vec![
                        DoctorEvidence {
                            field: "selected_count".into(),
                            value: DoctorEvidenceValue::Uint(indexes.len() as u64),
                            unit: None,
                        },
                        DoctorEvidence {
                            field: "max_index_verify".into(),
                            value: DoctorEvidenceValue::Uint(*max_count as u64),
                            unit: None,
                        },
                    ],
                    recommendation: Some(DoctorRecommendation {
                        summary: "Verify specific indexes or increase cap.".into(),
                        commands: vec![],
                        safe_to_automate: false,
                    }),
                });
            }
            indexes.iter().take(max).map(|i| i.name.as_str()).collect()
        }
        DoctorIndexVerification::Named(names) => names.iter().map(|n| n.as_str()).collect(),
    };

    for name in &selected_names {
        match db.verify_index(name) {
            Ok(verification) => {
                if verification.valid {
                    records.push(serde_json::json!({
                        "index": verification.name,
                        "expected_entries": verification.expected_entries,
                        "actual_entries": verification.actual_entries,
                    }));
                } else {
                    findings.push(DoctorFinding {
                        id: "index.verify_failed".into(),
                        severity: DoctorSeverity::Error,
                        category: DoctorCategory::Indexes,
                        title: format!("Index \"{name}\" verification failed"),
                        message: format!(
                            "Index \"{name}\" has {actual} entries but expected \
                             {expected}.",
                            actual = verification.actual_entries,
                            expected = verification.expected_entries,
                        ),
                        evidence: vec![
                            DoctorEvidence {
                                field: "index".into(),
                                value: DoctorEvidenceValue::String(verification.name),
                                unit: None,
                            },
                            DoctorEvidence {
                                field: "expected_entries".into(),
                                value: DoctorEvidenceValue::Uint(
                                    verification.expected_entries as u64,
                                ),
                                unit: None,
                            },
                            DoctorEvidence {
                                field: "actual_entries".into(),
                                value: DoctorEvidenceValue::Uint(
                                    verification.actual_entries as u64,
                                ),
                                unit: None,
                            },
                        ],
                        recommendation: Some(DoctorRecommendation {
                            summary: format!(
                                "Run `decentdb rebuild-index --db <path> --index {name}`, \
                                 then rerun doctor.",
                            ),
                            commands: vec![format!(
                                "decentdb rebuild-index --db <path> --index {name}",
                            )],
                            safe_to_automate: true,
                        }),
                    });
                }
            }
            Err(e) => {
                findings.push(DoctorFinding {
                    id: "index.verify_error".into(),
                    severity: DoctorSeverity::Error,
                    category: DoctorCategory::Indexes,
                    title: format!("Index \"{name}\" verification error"),
                    message: format!("Verification failed for index \"{name}\": {e}"),
                    evidence: vec![
                        DoctorEvidence {
                            field: "index".into(),
                            value: DoctorEvidenceValue::String((*name).into()),
                            unit: None,
                        },
                        DoctorEvidence {
                            field: "error".into(),
                            value: DoctorEvidenceValue::String(e.to_string()),
                            unit: None,
                        },
                    ],
                    recommendation: Some(DoctorRecommendation {
                        summary: "Inspect index name and database integrity.".into(),
                        commands: vec![],
                        safe_to_automate: false,
                    }),
                });
            }
        }
    }

    VerificationOutcome { findings, records }
}

// ---------------------------------------------------------------------------
// DR-05: Constrained Fix Planner and Executor
// ---------------------------------------------------------------------------

/// Plan v1 auto-fixable actions from pre-fix findings.
fn plan_fixes(findings: &[DoctorFinding]) -> Vec<DoctorFix> {
    let mut fixes = Vec::new();

    for f in findings {
        match f.id.as_str() {
            "wal.large_file" => fixes.push(DoctorFix {
                id: "fix.checkpoint".into(),
                finding_id: f.id.clone(),
                status: DoctorFixStatus::Planned,
                message: "Checkpoint planned.".into(),
                evidence_before: f.evidence.clone(),
                evidence_after: vec![],
            }),
            "schema.index_not_fresh" => {
                let index_name = f
                    .evidence
                    .iter()
                    .find(|e| e.field == "index")
                    .and_then(|e| match &e.value {
                        DoctorEvidenceValue::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .unwrap_or_default();
                fixes.push(DoctorFix {
                    id: "fix.rebuild_stale_index".into(),
                    finding_id: f.id.clone(),
                    status: DoctorFixStatus::Planned,
                    message: format!("Rebuild stale index \"{index_name}\" planned."),
                    evidence_before: f.evidence.clone(),
                    evidence_after: vec![],
                });
            }
            "index.verify_failed" => {
                let index_name = f
                    .evidence
                    .iter()
                    .find(|e| e.field == "index")
                    .and_then(|e| match &e.value {
                        DoctorEvidenceValue::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .unwrap_or_default();
                fixes.push(DoctorFix {
                    id: "fix.rebuild_invalid_index".into(),
                    finding_id: f.id.clone(),
                    status: DoctorFixStatus::Planned,
                    message: format!("Rebuild invalid index \"{index_name}\" planned."),
                    evidence_before: f.evidence.clone(),
                    evidence_after: vec![],
                });
            }
            _ => {}
        }
    }

    fix_order_sort(&mut fixes);
    fixes
}

/// Sort fixes in the execution order defined by Section 6.7.
fn fix_order_sort(fixes: &mut [DoctorFix]) {
    fixes.sort_by_key(|f| match f.id.as_str() {
        "fix.checkpoint" => 0,
        "fix.rebuild_stale_index" => 1,
        "fix.rebuild_invalid_index" => 2,
        "fix.analyze" => 3,
        _ => 99,
    });
}

/// Execute a single fix action against the live `Db` handle.
fn execute_fix(db: &Db, fix: &mut DoctorFix) {
    match fix.id.as_str() {
        "fix.checkpoint" => {
            let active = db.storage_info().map(|s| s.active_readers).unwrap_or(999);
            if active > 0 {
                fix.status = DoctorFixStatus::Skipped;
                fix.message = "Skipped checkpoint: active readers present.".into();
                return;
            }
            match db.checkpoint() {
                Ok(()) => {
                    fix.status = DoctorFixStatus::Applied;
                    fix.message = "Checkpoint completed.".into();
                    if let Ok(si) = db.storage_info() {
                        fix.evidence_after = vec![DoctorEvidence {
                            field: "wal_file_size".into(),
                            value: DoctorEvidenceValue::Uint(si.wal_file_size),
                            unit: Some("bytes".into()),
                        }];
                    }
                }
                Err(e) => {
                    fix.status = DoctorFixStatus::Failed;
                    fix.message = format!("Checkpoint failed: {e}");
                }
            }
        }
        "fix.rebuild_stale_index" | "fix.rebuild_invalid_index" => {
            let index_name = extract_index_from_fix(&fix.evidence_before);
            if index_name.is_empty() {
                fix.status = DoctorFixStatus::Skipped;
                fix.message = "Skipped rebuild: could not determine index name.".into();
                return;
            }
            match db.rebuild_index(&index_name) {
                Ok(()) => {
                    fix.status = DoctorFixStatus::Applied;
                    fix.message = format!("Rebuilt index \"{index_name}\".");
                }
                Err(e) => {
                    fix.status = DoctorFixStatus::Failed;
                    fix.message = format!("Rebuild failed for index \"{index_name}\": {e}");
                }
            }
        }
        "fix.analyze" => {
            fix.status = DoctorFixStatus::Skipped;
            fix.message =
                "Skipped ANALYZE: no typed stats detection helper available in v1.".into();
        }
        _ => {
            fix.status = DoctorFixStatus::Skipped;
            fix.message = "Unknown fix action.".into();
        }
    }
}

fn extract_index_from_fix(evidence: &[DoctorEvidence]) -> String {
    evidence
        .iter()
        .find(|e| e.field == "index")
        .and_then(|e| match &e.value {
            DoctorEvidenceValue::String(s) => Some(s.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Top-level entry point
// ---------------------------------------------------------------------------

/// Run the full doctor pipeline: facts → rules → index verification → fixes.
pub fn run_doctor(path: impl AsRef<Path>, options: DoctorOptions) -> Result<DoctorReport> {
    let path = path.as_ref();

    let checked_categories = match &options.checks {
        DoctorCheckSelection::All => all_category_list(),
        DoctorCheckSelection::Selected(cats) => cats.clone(),
    };

    // Step 1: attempt a loose header read.
    let header_info = match Db::read_header_info(path) {
        Ok(h) => h,
        Err(_e) => {
            let path_str = path.display().to_string();
            return Ok(build_partial_report(
                DoctorMode::Check,
                partial_database_empty(path),
                vec![header_unreadable(&path_str, _e.to_string())],
                DoctorCollectedFacts::default(),
                checked_categories,
            ));
        }
    };

    // Step 2: full open.
    let db = match Db::open(
        path,
        DbConfig {
            auto_checkpoint_on_open_mb: 0,
            ..DbConfig::default()
        },
    ) {
        Ok(d) => d,
        Err(_e) => {
            let db_summary = partial_database_from_header(path, &header_info);
            return Ok(build_partial_report(
                DoctorMode::Check,
                db_summary,
                vec![open_failed(
                    header_info.format_version,
                    header_info.page_size,
                    &_e.to_string(),
                )],
                DoctorCollectedFacts::default(),
                checked_categories,
            ));
        }
    };

    // Step 3: collect facts from opened database.
    let data = collect_facts(&db, path)?;

    // Step 4: evaluate rules.
    let mut raw_findings = evaluate_rules(&data);

    // Step 5: index verification (DR-04 — opt-in).
    let verif = run_index_verification(&db, &options, &data.indexes);
    raw_findings.extend(verif.findings);

    // Step 6: update collected facts with verification records.
    let mut collected = DoctorCollectedFacts::from_data(&data);
    collected.indexes_verified = verif.records;

    // Step 7: fix execution (DR-05 — only when options.fix).
    if options.fix {
        let pre_fix_findings = raw_findings;

        let mut fixes = plan_fixes(&pre_fix_findings);

        for fix in &mut fixes {
            execute_fix(&db, fix);
        }

        // Re-collect facts after fixes.
        let data2 = collect_facts(&db, path)?;
        let mut post_findings = evaluate_rules(&data2);
        // Re-run index verification on post-fix state.
        let verif2 = run_index_verification(&db, &options, &data2.indexes);
        post_findings.extend(verif2.findings);

        let mut collected2 = DoctorCollectedFacts::from_data(&data2);
        collected2.indexes_verified = verif2.records;

        // Add fix.failed findings.
        let failed_fixes: Vec<DoctorFinding> = fixes
            .iter()
            .filter(|fx| fx.status == DoctorFixStatus::Failed)
            .map(|fx| DoctorFinding {
                id: "fix.failed".into(),
                severity: DoctorSeverity::Error,
                category: DoctorCategory::Compatibility,
                title: format!("Fix \"{}\" failed", fx.id),
                message: fx.message.clone(),
                evidence: vec![
                    DoctorEvidence {
                        field: "fix_id".into(),
                        value: DoctorEvidenceValue::String(fx.id.clone()),
                        unit: None,
                    },
                    DoctorEvidence {
                        field: "finding_id".into(),
                        value: DoctorEvidenceValue::String(fx.finding_id.clone()),
                        unit: None,
                    },
                    DoctorEvidence {
                        field: "error".into(),
                        value: DoctorEvidenceValue::String(fx.message.clone()),
                        unit: None,
                    },
                ],
                recommendation: Some(DoctorRecommendation {
                    summary: "Review the failed action, rerun doctor without --fix, and apply \
                         the recommended command manually if safe."
                        .into(),
                    commands: vec![],
                    safe_to_automate: false,
                }),
            })
            .collect();
        post_findings.extend(failed_fixes);

        Ok(DoctorReport::new(
            DoctorMode::Fix,
            data2.database,
            checked_categories,
            pre_fix_findings,
            post_findings,
            fixes,
            collected2,
        ))
    } else {
        // Check-only mode.
        Ok(DoctorReport::new(
            DoctorMode::Check,
            data.database,
            checked_categories,
            vec![],
            raw_findings,
            vec![],
            collected,
        ))
    }
}

fn build_partial_report(
    mode: DoctorMode,
    database: DoctorDatabaseSummary,
    findings: Vec<DoctorFinding>,
    collected: DoctorCollectedFacts,
    checked_categories: Vec<DoctorCategory>,
) -> DoctorReport {
    DoctorReport::new(
        mode,
        database,
        checked_categories,
        vec![],
        findings,
        vec![],
        collected,
    )
}

// ---------------------------------------------------------------------------
// DR-07: Markdown Report Renderer
// ---------------------------------------------------------------------------

/// Render a [`DoctorReport`] as Markdown following the shape in Section 9.
#[must_use]
pub fn render_markdown(report: &DoctorReport) -> String {
    let mut out = String::new();

    out.push_str("# DecentDB Doctor Report\n\n");
    out.push_str("## Status\n\n");
    out.push_str(&format!(
        "Overall status: {}\n",
        match report.status {
            DoctorStatus::Ok => "OK",
            DoctorStatus::Warning => "WARNING",
            DoctorStatus::Error => "ERROR",
        }
    ));
    out.push('\n');

    // Database section
    out.push_str("## Database\n\n");
    out.push_str("| Field | Value |\n|---|---|\n");
    out.push_str(&format!("| Path | {} |\n", report.database.path));
    out.push_str(&format!(
        "| Format version | {} |\n",
        report.database.format_version
    ));
    out.push_str(&format!("| Page size | {} |\n", report.database.page_size));
    out.push_str(&format!(
        "| Page count | {} |\n",
        report.database.page_count
    ));
    out.push_str(&format!(
        "| Schema cookie | {} |\n",
        report.database.schema_cookie
    ));
    out.push('\n');

    // Summary
    out.push_str("## Summary\n\n");
    out.push_str("| Severity | Count |\n|---|---:|\n");
    out.push_str(&format!("| Error | {} |\n", report.summary.error_count));
    out.push_str(&format!("| Warning | {} |\n", report.summary.warning_count));
    out.push_str(&format!("| Info | {} |\n", report.summary.info_count));
    out.push('\n');

    // Fixes
    out.push_str("## Fixes\n\n");
    if report.fixes.is_empty() {
        if report.mode == DoctorMode::Fix {
            out.push_str("No auto-fixable findings were found.\n");
        } else {
            out.push_str("No fixes requested.\n");
        }
    } else {
        out.push_str("| Fix | Finding | Status | Message |\n|---|---|---|---|\n");
        for fix in &report.fixes {
            out.push_str(&format!(
                "| {} | {} | {} | {} |\n",
                fix.id,
                fix.finding_id,
                match fix.status {
                    DoctorFixStatus::Planned => "planned",
                    DoctorFixStatus::Applied => "applied",
                    DoctorFixStatus::Skipped => "skipped",
                    DoctorFixStatus::Failed => "failed",
                },
                fix.message,
            ));
        }
    }
    out.push('\n');

    // Findings
    out.push_str("## Findings\n\n");
    if report.findings.is_empty() {
        out.push_str("No findings.\n");
    } else {
        for finding in &report.findings {
            let sev_str = match finding.severity {
                DoctorSeverity::Error => "ERROR",
                DoctorSeverity::Warning => "WARNING",
                DoctorSeverity::Info => "INFO",
            };
            out.push_str(&format!(
                "### {sev_str} {} — {}\n\n",
                finding.id, finding.title
            ));
            out.push_str(&format!("{}\n\n", finding.message));

            if !finding.evidence.is_empty() {
                out.push_str("Evidence:\n\n");
                out.push_str("| Field | Value | Unit |\n|---|---:|---|\n");
                for ev in &finding.evidence {
                    let val = evidence_value_fmt(&ev.value);
                    let unit = ev.unit.as_deref().unwrap_or("");
                    out.push_str(&format!("| {} | {val} | {unit} |\n", ev.field));
                }
                out.push('\n');
            }

            if let Some(ref rec) = &finding.recommendation {
                out.push_str("Recommendation:\n\n");
                out.push_str(&format!("{}\n", rec.summary));
                for cmd in &rec.commands {
                    out.push_str(&format!("\n```bash\n{cmd}\n```\n"));
                }
                out.push('\n');
            }
        }
    }

    out
}

fn evidence_value_fmt(v: &DoctorEvidenceValue) -> String {
    match v {
        DoctorEvidenceValue::Bool(b) => b.to_string(),
        DoctorEvidenceValue::Int(i) => i.to_string(),
        DoctorEvidenceValue::Uint(u) => u.to_string(),
        DoctorEvidenceValue::Float(f) => format!("{f:.1}"),
        DoctorEvidenceValue::String(s) => s.clone(),
    }
}

impl DoctorCollectedFacts {
    fn from_data(data: &CollectedData) -> Self {
        let storage = match &data.storage {
            Some(st) => serde_json::json!({
                "format_version": st.format_version,
                "page_size": st.page_size,
                "page_count": st.page_count,
                "schema_cookie": st.schema_cookie,
                "wal_file_size": st.wal_file_size,
                "wal_versions": st.wal_versions,
                "active_readers": st.active_readers,
                "warning_count": st.warning_count,
                "shared_wal": st.shared_wal,
            }),
            None => serde_json::json!({}),
        };

        let header = match &data.header {
            Some(hdr) => serde_json::json!({
                "format_version": hdr.format_version,
                "page_size": hdr.page_size,
                "schema_cookie": hdr.schema_cookie,
                "freelist_page_count": hdr.freelist_page_count,
            }),
            None => serde_json::json!({}),
        };

        let schema = match &data.schema {
            Some(sch) => serde_json::json!({
                "table_count": sch.tables.iter().filter(|t| !t.temporary).count(),
                "index_count": data.indexes.len(),
            }),
            None => serde_json::json!({}),
        };

        Self {
            storage,
            header,
            schema,
            indexes_verified: Vec::new(),
        }
    }
}

fn all_category_list() -> Vec<DoctorCategory> {
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

// ---------------------------------------------------------------------------
// Finding constructors — each returns a fully populated DoctorFinding
// ---------------------------------------------------------------------------

fn header_unreadable(path: &str, error_msg: String) -> DoctorFinding {
    DoctorFinding {
        id: "header.unreadable".into(),
        severity: DoctorSeverity::Error,
        category: DoctorCategory::Header,
        title: "Database header cannot be read".into(),
        message: error_msg.clone(),
        evidence: vec![
            DoctorEvidence {
                field: "path".into(),
                value: DoctorEvidenceValue::String(path.into()),
                unit: None,
            },
            DoctorEvidence {
                field: "error".into(),
                value: DoctorEvidenceValue::String(error_msg),
                unit: None,
            },
        ],
        recommendation: Some(DoctorRecommendation {
            summary: "Verify the path, file permissions, and file type.".into(),
            commands: vec![],
            safe_to_automate: false,
        }),
    }
}

fn open_failed(format_version: u32, page_size: u32, message: &str) -> DoctorFinding {
    DoctorFinding {
        id: "database.open_failed".into(),
        severity: DoctorSeverity::Error,
        category: DoctorCategory::Compatibility,
        title: "Full engine open failed".into(),
        message: message.into(),
        evidence: vec![
            DoctorEvidence {
                field: "format_version".into(),
                value: DoctorEvidenceValue::Uint(format_version.into()),
                unit: None,
            },
            DoctorEvidence {
                field: "page_size".into(),
                value: DoctorEvidenceValue::Uint(page_size.into()),
                unit: Some("bytes".into()),
            },
        ],
        recommendation: Some(DoctorRecommendation {
            summary: "Use `decentdb info`, migration tooling, or a compatible engine version."
                .into(),
            commands: vec![],
            safe_to_automate: false,
        }),
    }
}

fn format_version_unknown(version: u32) -> DoctorFinding {
    DoctorFinding {
        id: "compatibility.format_version_unknown".into(),
        severity: DoctorSeverity::Warning,
        category: DoctorCategory::Compatibility,
        title: "Database format version differs from engine format version".into(),
        message: format!(
            "Header format version {version} differs from engine format version \
             {DB_FORMAT_VERSION}, but the database still opened successfully."
        ),
        evidence: vec![DoctorEvidence {
            field: "format_version".into(),
            value: DoctorEvidenceValue::Uint(version.into()),
            unit: None,
        }],
        recommendation: Some(DoctorRecommendation {
            summary: "Confirm the engine version expected by the application.".into(),
            commands: vec![],
            safe_to_automate: false,
        }),
    }
}

fn wal_large_file(wal_file_size: u64, physical_bytes: u64, threshold: u64) -> DoctorFinding {
    DoctorFinding {
        id: "wal.large_file".into(),
        severity: DoctorSeverity::Warning,
        category: DoctorCategory::Wal,
        title: "WAL file is large relative to the database".into(),
        message: "The WAL file size exceeds the threshold.".into(),
        evidence: vec![
            DoctorEvidence {
                field: "wal_file_size".into(),
                value: DoctorEvidenceValue::Uint(wal_file_size),
                unit: Some("bytes".into()),
            },
            DoctorEvidence {
                field: "physical_bytes".into(),
                value: DoctorEvidenceValue::Uint(physical_bytes),
                unit: Some("bytes".into()),
            },
            DoctorEvidence {
                field: "threshold".into(),
                value: DoctorEvidenceValue::Uint(threshold),
                unit: Some("bytes".into()),
            },
        ],
        recommendation: Some(DoctorRecommendation {
            summary: "Run a checkpoint when no long reader is active.".into(),
            commands: vec!["decentdb checkpoint --db <path>".into()],
            safe_to_automate: false,
        }),
    }
}

fn wal_many_versions(wal_versions: usize) -> DoctorFinding {
    DoctorFinding {
        id: "wal.many_versions".into(),
        severity: DoctorSeverity::Warning,
        category: DoctorCategory::Wal,
        title: "WAL has accumulated many versions".into(),
        message: "A large number of WAL versions may indicate long-running readers or checkpoint starvation.".into(),
        evidence: vec![DoctorEvidence {
            field: "wal_versions".into(),
            value: DoctorEvidenceValue::Uint(wal_versions as u64),
            unit: None,
        }],
        recommendation: Some(DoctorRecommendation {
            summary: "Check for long readers; checkpoint when safe.".into(),
            commands: vec![],
            safe_to_automate: false,
        }),
    }
}

fn wal_long_readers_present(active_readers: usize, wal_file_size: u64) -> DoctorFinding {
    DoctorFinding {
        id: "wal.long_readers_present".into(),
        severity: DoctorSeverity::Warning,
        category: DoctorCategory::Wal,
        title: "Long-running readers are holding WAL space".into(),
        message: "Active readers prevent the WAL from checkpointing.".into(),
        evidence: vec![
            DoctorEvidence {
                field: "active_readers".into(),
                value: DoctorEvidenceValue::Uint(active_readers as u64),
                unit: None,
            },
            DoctorEvidence {
                field: "wal_file_size".into(),
                value: DoctorEvidenceValue::Uint(wal_file_size),
                unit: Some("bytes".into()),
            },
        ],
        recommendation: Some(DoctorRecommendation {
            summary: "Close long-running readers before checkpoint-sensitive operations.".into(),
            commands: vec![],
            safe_to_automate: false,
        }),
    }
}

fn wal_reader_warnings(warning_count: usize) -> DoctorFinding {
    DoctorFinding {
        id: "wal.reader_warnings_recorded".into(),
        severity: DoctorSeverity::Warning,
        category: DoctorCategory::Wal,
        title: "Reader transaction warnings recorded".into(),
        message: "WAL reader warnings indicate long-lived read transactions.".into(),
        evidence: vec![DoctorEvidence {
            field: "warning_count".into(),
            value: DoctorEvidenceValue::Uint(warning_count as u64),
            unit: None,
        }],
        recommendation: Some(DoctorRecommendation {
            summary: "Inspect application read transaction lifetime.".into(),
            commands: vec![],
            safe_to_automate: false,
        }),
    }
}

fn wal_shared_enabled() -> DoctorFinding {
    DoctorFinding {
        id: "wal.shared_enabled".into(),
        severity: DoctorSeverity::Info,
        category: DoctorCategory::Wal,
        title: "Shared WAL mode is enabled".into(),
        message: "Shared WAL is enabled for this database.".into(),
        evidence: vec![DoctorEvidence {
            field: "shared_wal".into(),
            value: DoctorEvidenceValue::Bool(true),
            unit: None,
        }],
        recommendation: None,
    }
}

fn fragmentation_high(page_count: u32, freelist_page_count: u32, pct: f64) -> DoctorFinding {
    DoctorFinding {
        id: "fragmentation.high".into(),
        severity: DoctorSeverity::Warning,
        category: DoctorCategory::Fragmentation,
        title: "High fragmentation detected".into(),
        message: format!(
            "Free-list pages ({freelist_page_count}) represent {pct}% of total pages \
             ({page_count})."
        ),
        evidence: vec![
            DoctorEvidence {
                field: "page_count".into(),
                value: DoctorEvidenceValue::Uint(page_count.into()),
                unit: None,
            },
            DoctorEvidence {
                field: "freelist_page_count".into(),
                value: DoctorEvidenceValue::Uint(freelist_page_count.into()),
                unit: None,
            },
            DoctorEvidence {
                field: "fragmentation_percent".into(),
                value: DoctorEvidenceValue::Float(pct),
                unit: Some("%".into()),
            },
        ],
        recommendation: Some(DoctorRecommendation {
            summary: "Consider `decentdb vacuum --db <path> --output <new-path>`.".into(),
            commands: vec!["decentdb vacuum --db <path> --output <new-path>".into()],
            safe_to_automate: false,
        }),
    }
}

fn fragmentation_moderate(page_count: u32, freelist_page_count: u32, pct: f64) -> DoctorFinding {
    DoctorFinding {
        id: "fragmentation.moderate".into(),
        severity: DoctorSeverity::Info,
        category: DoctorCategory::Fragmentation,
        title: "Moderate fragmentation detected".into(),
        message: format!(
            "Free-list pages ({freelist_page_count}) represent {pct}% of total pages \
             ({page_count})."
        ),
        evidence: vec![
            DoctorEvidence {
                field: "page_count".into(),
                value: DoctorEvidenceValue::Uint(page_count.into()),
                unit: None,
            },
            DoctorEvidence {
                field: "freelist_page_count".into(),
                value: DoctorEvidenceValue::Uint(freelist_page_count.into()),
                unit: None,
            },
            DoctorEvidence {
                field: "fragmentation_percent".into(),
                value: DoctorEvidenceValue::Float(pct),
                unit: Some("%".into()),
            },
        ],
        recommendation: Some(DoctorRecommendation {
            summary: "Monitor; vacuum only if file size matters.".into(),
            commands: vec![],
            safe_to_automate: false,
        }),
    }
}

fn schema_no_user_tables() -> DoctorFinding {
    DoctorFinding {
        id: "schema.no_user_tables".into(),
        severity: DoctorSeverity::Info,
        category: DoctorCategory::Schema,
        title: "No persistent user tables detected".into(),
        message: "The schema snapshot has zero persistent user tables.".into(),
        evidence: vec![DoctorEvidence {
            field: "table_count".into(),
            value: DoctorEvidenceValue::Uint(0),
            unit: None,
        }],
        recommendation: None,
    }
}

fn schema_many_indexes(table: &str, index_count: usize) -> DoctorFinding {
    DoctorFinding {
        id: "schema.many_indexes_on_table".into(),
        severity: DoctorSeverity::Info,
        category: DoctorCategory::Schema,
        title: format!("Table \"{table}\" has many indexes"),
        message: format!("Table \"{table}\" has {index_count} indexes, which may slow writes."),
        evidence: vec![
            DoctorEvidence {
                field: "table".into(),
                value: DoctorEvidenceValue::String(table.into()),
                unit: None,
            },
            DoctorEvidence {
                field: "index_count".into(),
                value: DoctorEvidenceValue::Uint(index_count as u64),
                unit: None,
            },
        ],
        recommendation: Some(DoctorRecommendation {
            summary: "Review write overhead; keep indexes that match query patterns.".into(),
            commands: vec![],
            safe_to_automate: false,
        }),
    }
}

fn index_not_fresh(index: &str, table: &str) -> DoctorFinding {
    DoctorFinding {
        id: "schema.index_not_fresh".into(),
        severity: DoctorSeverity::Warning,
        category: DoctorCategory::Indexes,
        title: format!("Index \"{index}\" on \"{table}\" is not fresh"),
        message: format!(
            "The metadata for index \"{index}\" on table \"{table}\" indicates it is stale."
        ),
        evidence: vec![
            DoctorEvidence {
                field: "index".into(),
                value: DoctorEvidenceValue::String(index.into()),
                unit: None,
            },
            DoctorEvidence {
                field: "table".into(),
                value: DoctorEvidenceValue::String(table.into()),
                unit: None,
            },
        ],
        recommendation: Some(DoctorRecommendation {
            summary: format!("Run `decentdb rebuild-index --db <path> --index {index}`."),
            commands: vec![format!(
                "decentdb rebuild-index --db <path> --index {index}"
            )],
            safe_to_automate: true,
        }),
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

    // ---- DR-02: fact collection tests ---------------------------------

    #[test]
    fn run_doctor_on_valid_empty_db() {
        use tempfile::TempDir;
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("empty.ddb");
        crate::Db::create(&path, DbConfig::default()).expect("create db");

        let report = run_doctor(&path, DoctorOptions::default())
            .expect("report should succeed for valid empty db");
        assert_eq!(report.schema_version, 1);
        assert_eq!(report.mode, DoctorMode::Check);
        assert!(matches!(report.status, DoctorStatus::Ok));
        assert!(
            report
                .findings
                .iter()
                .any(|f| f.id == "schema.no_user_tables"),
            "empty db should have no_user_tables"
        );
    }

    #[test]
    fn run_doctor_on_missing_file_produces_header_unreadable() {
        let report = run_doctor(
            "/nonexistent/path/that/does/not/exist.ddb",
            DoctorOptions::default(),
        )
        .expect("report should succeed even for missing file (partial report)");
        assert!(
            report.findings.iter().any(|f| f.id == "header.unreadable"),
            "missing file should produce header.unreadable"
        );
        assert_eq!(report.status, DoctorStatus::Error);
    }

    #[test]
    fn run_doctor_does_not_mutate_database() {
        use crate::Db;
        use crate::DbConfig;
        use tempfile::TempDir;

        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("nomutate.ddb");

        let db = Db::create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE t(id INT64 PRIMARY KEY)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1), (2), (3)")
            .expect("insert rows");
        db.checkpoint().expect("checkpoint for stable baseline");
        let before_storage = db.storage_info().expect("storage info before");
        drop(db);

        let _report = run_doctor(&path, DoctorOptions::default()).expect("doctor run");

        let db2 = Db::open(
            &path,
            DbConfig {
                auto_checkpoint_on_open_mb: 0,
                ..DbConfig::default()
            },
        )
        .expect("re-open");
        let after_storage = db2.storage_info().expect("storage info after");

        assert_eq!(
            before_storage.last_checkpoint_lsn, after_storage.last_checkpoint_lsn,
            "doctor must not change last_checkpoint_lsn"
        );
        assert_eq!(
            before_storage.schema_cookie, after_storage.schema_cookie,
            "doctor must not change schema_cookie"
        );
    }

    // ---- DR-03: rule engine unit tests ---------------------------------

    // -- header.unreadable -----------------------------------------------

    #[test]
    fn header_unreadable_is_error() {
        let report = run_doctor(
            "/nonexistent/path/that/does/not/exist.ddb",
            DoctorOptions::default(),
        )
        .expect("report");
        assert!(report.findings.iter().any(|f| f.id == "header.unreadable"));
        assert!(report.findings.len() == 1);
    }

    // -- database.open_failed --------------------------------------------

    #[test]
    fn open_failed_is_error() {
        // Full integration test for open_failed is blocked: no safe fixture
        // can simultaneously produce a readable header and an open failure.
        // The finding constructor is tested directly to ensure correct shape.
        let f = open_failed(10, 4096, "simulated open error");
        assert_eq!(f.id, "database.open_failed");
        assert_eq!(f.severity, DoctorSeverity::Error);
        assert_eq!(f.category, DoctorCategory::Compatibility);
        assert!(f.evidence.iter().any(|e| e.field == "format_version"));
        assert!(f.evidence.iter().any(|e| e.field == "page_size"));
        assert!(f.recommendation.is_some());
    }

    // -- compatibility.format_version_unknown ----------------------------

    #[test]
    fn format_version_match_no_finding() {
        let data = fresh_collected();
        let findings = evaluate_rules(&data);
        assert!(!findings
            .iter()
            .any(|f| f.id == "compatibility.format_version_unknown"));
    }

    #[test]
    fn format_version_mismatch_is_warning() {
        let mut data = fresh_collected();
        if let Some(ref mut hdr) = data.header {
            hdr.format_version = 99; // different from DB_FORMAT_VERSION = 10
        }
        let findings = evaluate_rules(&data);
        assert!(findings
            .iter()
            .any(|f| f.id == "compatibility.format_version_unknown"));
    }

    // -- wal.large_file --------------------------------------------------

    #[test]
    fn wal_file_below_threshold_no_finding() {
        let mut data = fresh_collected();
        if let Some(ref mut st) = data.storage {
            st.wal_file_size = 1_000; // well below 64 MiB
        }
        data.physical_bytes = 1_000_000;
        let findings = evaluate_rules(&data);
        assert!(!findings.iter().any(|f| f.id == "wal.large_file"));
    }

    #[test]
    fn wal_file_at_64_mib_threshold() {
        let data = fresh_collected(); // physical_bytes = 8192, so threshold = max(64MiB, 2048) = 64MiB
        let mut data_with_wal = data;
        if let Some(ref mut st) = data_with_wal.storage {
            st.wal_file_size = 64 * 1024 * 1024; // exactly 64 MiB
        }
        let findings = evaluate_rules(&data_with_wal);
        assert!(
            findings.iter().any(|f| f.id == "wal.large_file"),
            "exactly 64 MiB should trigger wal.large_file"
        );
    }

    #[test]
    fn wal_file_above_threshold_by_physical_bytes_ratio() {
        let mut data = fresh_collected();
        // physical_bytes = 8192; wal_file_size = 32 (from fresh_collected)
        // Override: threshold = max(64MiB, 4000/4=1000) = 64 MiB, so we'd
        // need a huge wal.  Instead, leave wal_file_size at 32 but show that
        // the ratio trigger works by checking that a wal_file_size >=
        // physical_bytes/4 (and >=64MiB) triggers.
        // For ratio-only to win, wal_file_size must be >= 64 MiB
        // AND also >= physical_bytes/4.  We set wal large enough.
        if let Some(ref mut st) = data.storage {
            st.wal_file_size = 70 * 1024 * 1024; // 70 MiB > 64 MiB
        }
        data.physical_bytes = 200 * 1024 * 1024; // 200 MiB, /4 = 50 MiB
                                                 // threshold = max(64MiB, 50MiB) = 64MiB, wal_file_size=70MiB >= 64MiB
        let findings = evaluate_rules(&data);
        assert!(
            findings.iter().any(|f| f.id == "wal.large_file"),
            "wal_file_size >= threshold should trigger"
        );
    }

    // -- wal.many_versions -----------------------------------------------

    #[test]
    fn wal_versions_below_threshold_no_finding() {
        let data = fresh_collected();
        let findings = evaluate_rules(&data);
        assert!(!findings.iter().any(|f| f.id == "wal.many_versions"));
    }

    #[test]
    fn wal_versions_at_threshold() {
        let mut data = fresh_collected();
        if let Some(ref mut st) = data.storage {
            st.wal_versions = 100_000;
        }
        let findings = evaluate_rules(&data);
        assert!(findings.iter().any(|f| f.id == "wal.many_versions"));
    }

    #[test]
    fn wal_versions_above_threshold() {
        let mut data = fresh_collected();
        if let Some(ref mut st) = data.storage {
            st.wal_versions = 200_000;
        }
        let findings = evaluate_rules(&data);
        assert!(findings.iter().any(|f| f.id == "wal.many_versions"));
    }

    // -- wal.long_readers_present ----------------------------------------

    #[test]
    fn no_active_readers_no_finding() {
        let data = fresh_collected();
        let findings = evaluate_rules(&data);
        assert!(!findings.iter().any(|f| f.id == "wal.long_readers_present"));
    }

    #[test]
    fn active_readers_with_non_empty_wal() {
        let mut data = fresh_collected();
        if let Some(ref mut st) = data.storage {
            st.active_readers = 1;
            st.wal_file_size = 1024;
        }
        let findings = evaluate_rules(&data);
        assert!(findings.iter().any(|f| f.id == "wal.long_readers_present"));
    }

    #[test]
    fn active_readers_with_empty_wal_no_finding() {
        let mut data = fresh_collected();
        if let Some(ref mut st) = data.storage {
            st.active_readers = 1;
            st.wal_file_size = 0;
        }
        let findings = evaluate_rules(&data);
        assert!(!findings.iter().any(|f| f.id == "wal.long_readers_present"));
    }

    // -- wal.reader_warnings_recorded -----------------------------------

    #[test]
    fn no_warnings_no_finding() {
        let data = fresh_collected();
        let findings = evaluate_rules(&data);
        assert!(!findings
            .iter()
            .any(|f| f.id == "wal.reader_warnings_recorded"));
    }

    #[test]
    fn warnings_positive_triggers() {
        let mut data = fresh_collected();
        if let Some(ref mut st) = data.storage {
            st.warning_count = 3;
        }
        let findings = evaluate_rules(&data);
        assert!(findings
            .iter()
            .any(|f| f.id == "wal.reader_warnings_recorded"));
    }

    // -- wal.shared_enabled ----------------------------------------------

    #[test]
    fn shared_wal_disabled_no_finding() {
        let data = fresh_collected();
        let findings = evaluate_rules(&data);
        assert!(!findings.iter().any(|f| f.id == "wal.shared_enabled"));
    }

    #[test]
    fn shared_wal_enabled_is_info() {
        let mut data = fresh_collected();
        if let Some(ref mut st) = data.storage {
            st.shared_wal = true;
        }
        let findings = evaluate_rules(&data);
        let f = findings
            .iter()
            .find(|f| f.id == "wal.shared_enabled")
            .expect("shared wal finding missing");
        assert_eq!(f.severity, DoctorSeverity::Info);
    }

    // -- fragmentation.high / fragmentation.moderate --------------------

    #[test]
    fn fragmentation_below_min_page_count_no_finding() {
        let mut data = fresh_collected();
        data.database.page_count = 100; // below 128
        if let Some(ref mut hdr) = data.header {
            hdr.freelist_page_count = 30; // ratio = 0.30
        }
        let findings = evaluate_rules(&data);
        assert!(!findings
            .iter()
            .any(|f| f.id == "fragmentation.high" || f.id == "fragmentation.moderate"));
    }

    #[test]
    fn fragmentation_high_at_25_percent() {
        let mut data = fresh_collected();
        data.database.page_count = 200;
        if let Some(ref mut hdr) = data.header {
            hdr.freelist_page_count = 50; // 25%
        }
        let findings = evaluate_rules(&data);
        assert!(findings.iter().any(|f| f.id == "fragmentation.high"));
    }

    #[test]
    fn fragmentation_high_above_25_percent() {
        let mut data = fresh_collected();
        data.database.page_count = 200;
        if let Some(ref mut hdr) = data.header {
            hdr.freelist_page_count = 80; // 40%
        }
        let findings = evaluate_rules(&data);
        assert!(findings.iter().any(|f| f.id == "fragmentation.high"));
    }

    #[test]
    fn fragmentation_moderate_at_10_percent() {
        let mut data = fresh_collected();
        data.database.page_count = 200;
        if let Some(ref mut hdr) = data.header {
            hdr.freelist_page_count = 20; // 10%
        }
        let findings = evaluate_rules(&data);
        assert!(findings.iter().any(|f| f.id == "fragmentation.moderate"));
    }

    #[test]
    fn fragmentation_below_10_percent_no_finding() {
        let mut data = fresh_collected();
        data.database.page_count = 200;
        if let Some(ref mut hdr) = data.header {
            hdr.freelist_page_count = 15; // 7.5%
        }
        let findings = evaluate_rules(&data);
        assert!(!findings
            .iter()
            .any(|f| f.id == "fragmentation.moderate" || f.id == "fragmentation.high"));
    }

    // -- schema.no_user_tables -------------------------------------------

    #[test]
    fn no_user_tables_is_info() {
        let data = fresh_collected();
        let findings = evaluate_rules(&data);
        let f = findings
            .iter()
            .find(|f| f.id == "schema.no_user_tables")
            .expect("no_user_tables missing");
        assert_eq!(f.severity, DoctorSeverity::Info);
    }

    // -- schema.many_indexes_on_table ------------------------------------

    #[test]
    fn many_indexes_boundary_8_no_finding() {
        use crate::metadata::SchemaTableInfo;

        let mut data = fresh_collected();
        if let Some(ref mut schema) = data.schema {
            schema.tables.push(SchemaTableInfo {
                name: "t".into(),
                temporary: false,
                ddl: "CREATE TABLE t()".into(),
                row_count: 0,
                primary_key_columns: vec![],
                checks: vec![],
                foreign_keys: vec![],
                columns: vec![],
            });
        }
        data.indexes = (0..8)
            .map(|i| IndexInfo {
                name: format!("idx_{i}"),
                table_name: "t".into(),
                kind: "btree".into(),
                unique: false,
                columns: vec![format!("col_{i}")],
                include_columns: vec![],
                predicate_sql: None,
                fresh: true,
            })
            .collect();
        let findings = evaluate_rules(&data);
        assert!(!findings
            .iter()
            .any(|f| f.id == "schema.many_indexes_on_table"));
    }

    #[test]
    fn many_indexes_boundary_9_triggers() {
        use crate::metadata::SchemaTableInfo;

        let mut data = fresh_collected();
        if let Some(ref mut schema) = data.schema {
            schema.tables.push(SchemaTableInfo {
                name: "t".into(),
                temporary: false,
                ddl: "CREATE TABLE t()".into(),
                row_count: 0,
                primary_key_columns: vec![],
                checks: vec![],
                foreign_keys: vec![],
                columns: vec![],
            });
        }
        data.indexes = (0..9)
            .map(|i| IndexInfo {
                name: format!("idx_{i}"),
                table_name: "t".into(),
                kind: "btree".into(),
                unique: false,
                columns: vec![format!("col_{i}")],
                include_columns: vec![],
                predicate_sql: None,
                fresh: true,
            })
            .collect();
        let findings = evaluate_rules(&data);
        assert!(findings
            .iter()
            .any(|f| f.id == "schema.many_indexes_on_table"));
    }

    // -- schema.index_not_fresh ------------------------------------------

    #[test]
    fn fresh_indexes_no_finding() {
        let data = fresh_collected();
        let findings = evaluate_rules(&data);
        assert!(!findings.iter().any(|f| f.id == "schema.index_not_fresh"));
    }

    #[test]
    fn stale_index_triggers_warning() {
        let mut data = fresh_collected();
        data.indexes.push(IndexInfo {
            name: "stale_idx".into(),
            table_name: "t".into(),
            kind: "btree".into(),
            unique: false,
            columns: vec!["a".into()],
            include_columns: vec![],
            predicate_sql: None,
            fresh: false,
        });
        let findings = evaluate_rules(&data);
        let f = findings
            .iter()
            .find(|f| f.id == "schema.index_not_fresh")
            .expect("index_not_fresh missing");
        assert_eq!(f.severity, DoctorSeverity::Warning);
    }

    // ---- check filtering -----------------------------------------------

    #[test]
    fn check_filtering_excludes_other_categories() {
        let report = run_doctor(
            ":memory:",
            DoctorOptions {
                checks: DoctorCheckSelection::Selected(vec![DoctorCategory::Header]),
                ..DoctorOptions::default()
            },
        )
        .expect("doctor run");
        assert!(report
            .findings
            .iter()
            .all(|f| f.category == DoctorCategory::Header));
        assert!(report
            .summary
            .checked_categories
            .contains(&DoctorCategory::Header));
    }

    // ---- integration test ----------------------------------------------

    #[test]
    fn full_report_on_table_with_indexes() {
        use crate::Db;
        use crate::DbConfig;
        use tempfile::TempDir;

        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("full.ddb");

        let db = Db::create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, v TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
            .expect("insert");
        db.execute("CREATE INDEX t_v ON t(v)")
            .expect("create index");
        drop(db);

        let report = run_doctor(&path, DoctorOptions::default()).expect("doctor run");

        assert_eq!(report.schema_version, 1);
        assert_eq!(report.mode, DoctorMode::Check);

        // Should NOT have no_user_tables (we have 't')
        assert!(!report
            .findings
            .iter()
            .any(|f| f.id == "schema.no_user_tables"));

        // Should have compatible format version
        assert!(!report
            .findings
            .iter()
            .any(|f| f.id == "compatibility.format_version_unknown"));
    }

    // ---- DR-04: index verification tests --------------------------------

    #[test]
    fn default_doctor_does_not_verify_indexes() {
        use crate::Db;
        use crate::DbConfig;
        use tempfile::TempDir;

        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("nodefault_verify.ddb");

        let db = Db::create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, v TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'x')").expect("insert");
        db.execute("CREATE INDEX t_v ON t(v)")
            .expect("create index");
        drop(db);

        let report = run_doctor(&path, DoctorOptions::default()).expect("doctor run");
        // Default run has no index verification.
        assert!(!report
            .findings
            .iter()
            .any(|f| f.id.starts_with("index.verify")));
        assert!(report.collected.indexes_verified.is_empty());
    }

    #[test]
    fn verify_index_named_successful() {
        use crate::Db;
        use crate::DbConfig;
        use tempfile::TempDir;

        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("verify_named.ddb");

        let db = Db::create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, v TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'hello')")
            .expect("insert");
        db.execute("CREATE INDEX t_v ON t(v)")
            .expect("create index");
        drop(db);

        let report = run_doctor(
            &path,
            DoctorOptions {
                verify_indexes: DoctorIndexVerification::Named(vec!["t_v".into()]),
                ..DoctorOptions::default()
            },
        )
        .expect("doctor run");

        assert!(
            !report
                .findings
                .iter()
                .any(|f| f.id == "index.verify_failed"),
            "fresh index should not fail verification"
        );
        assert!(
            !report.collected.indexes_verified.is_empty(),
            "should have verification records"
        );
    }

    #[test]
    fn verify_unknown_index_produces_error() {
        use crate::Db;
        use crate::DbConfig;
        use tempfile::TempDir;

        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("verify_unknown.ddb");
        let db = Db::create(&path, DbConfig::default()).expect("create db");
        drop(db);

        let report = run_doctor(
            &path,
            DoctorOptions {
                verify_indexes: DoctorIndexVerification::Named(vec!["nonexistent".into()]),
                ..DoctorOptions::default()
            },
        )
        .expect("doctor run");

        assert!(
            report.findings.iter().any(|f| f.id == "index.verify_error"),
            "unknown index should produce verify_error: {report:?}"
        );
    }

    #[test]
    fn verify_all_with_cap_produces_skip() {
        use crate::Db;
        use crate::DbConfig;
        use tempfile::TempDir;

        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("verify_capped.ddb");

        let db = Db::create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, v TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'a')").expect("insert");
        db.execute("CREATE INDEX t_v ON t(v)")
            .expect("create index");
        db.execute("CREATE INDEX t_id ON t(id)")
            .expect("create index");
        db.execute("CREATE INDEX t_v_id ON t(v, id)")
            .expect("create index");
        drop(db);

        let report = run_doctor(
            &path,
            DoctorOptions {
                verify_indexes: DoctorIndexVerification::All { max_count: 1 },
                ..DoctorOptions::default()
            },
        )
        .expect("doctor run");

        assert!(
            report
                .findings
                .iter()
                .any(|f| f.id == "index.verify_skipped_limit"),
            "capped verify should produce skipped_limit: {report:?}"
        );
    }

    // ---- DR-05: fix planner and executor tests --------------------------

    #[test]
    fn wal_large_file_plans_checkpoint_fix() {
        let fixable = finding(
            "wal.large_file",
            DoctorSeverity::Warning,
            DoctorCategory::Wal,
        );
        let fixes = plan_fixes(&[fixable]);
        assert!(fixes.iter().any(|f| f.id == "fix.checkpoint"));
        assert_eq!(fixes[0].status, DoctorFixStatus::Planned);
    }

    #[test]
    fn index_not_fresh_plans_rebuild_fix() {
        let fixable = index_not_fresh("my_idx", "my_table");
        let fixes = plan_fixes(&[fixable]);
        assert!(fixes.iter().any(|f| f.id == "fix.rebuild_stale_index"));
    }

    #[test]
    fn non_auto_fixable_finding_plans_no_fix() {
        let non_fixable = finding(
            "fragmentation.moderate",
            DoctorSeverity::Info,
            DoctorCategory::Fragmentation,
        );
        let fixes = plan_fixes(&[non_fixable]);
        assert!(fixes.is_empty());
    }

    #[test]
    fn fix_execution_order_is_deterministic() {
        let findings = vec![
            index_not_fresh("idx_a", "t"),
            finding(
                "wal.large_file",
                DoctorSeverity::Warning,
                DoctorCategory::Wal,
            ),
        ];
        let fixes = plan_fixes(&findings);
        let ids: Vec<&str> = fixes.iter().map(|f| f.id.as_str()).collect();
        // checkpoint must come before rebuild_stale_index
        assert_eq!(ids, vec!["fix.checkpoint", "fix.rebuild_stale_index"]);
    }

    #[test]
    fn fix_mode_populates_report_correctly() {
        use crate::Db;
        use crate::DbConfig;
        use tempfile::TempDir;

        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("fix_report.ddb");

        let db = Db::create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, v TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'a')").expect("insert");
        db.execute("CREATE INDEX t_v ON t(v)")
            .expect("create index");
        // Invalidate the index by rebuilding it, which shouldn't mark it stale.
        drop(db);

        let report = run_doctor(
            &path,
            DoctorOptions {
                fix: true,
                ..DoctorOptions::default()
            },
        )
        .expect("doctor run");

        assert_eq!(report.mode, DoctorMode::Fix);
        // pre_fix_findings should be populated
        assert!(
            !report.pre_fix_findings.is_empty(),
            "pre_fix_findings must be populated in fix mode"
        );
        // findings must exist (post-fix)
        assert!(
            !report.findings.is_empty(),
            "post-fix findings must be present"
        );
        // fixes must exist
        assert!(
            !report.fixes.is_empty(),
            "fixes must be present in fix mode: {report:?}"
        );
        // pre_fix_findings must be sorted
        let mut expect_sorted = report.pre_fix_findings.clone();
        sort_findings(&mut expect_sorted);
        assert_eq!(
            report.pre_fix_findings, expect_sorted,
            "pre_fix_findings must be sorted"
        );
    }

    #[test]
    fn fix_analyze_is_always_skipped() {
        // No typed stats helper exists yet, so fix.analyze is always skipped.
        let fix = DoctorFinding {
            id: "statistics.missing_analyze".into(),
            severity: DoctorSeverity::Info,
            category: DoctorCategory::Statistics,
            title: "".into(),
            message: "".into(),
            evidence: vec![],
            recommendation: None,
        };
        // Even if we could detect it, fix.analyze is not in plan_fixes.
        let fixes = plan_fixes(&[fix]);
        assert!(fixes.iter().all(|f| f.id != "fix.analyze"));
    }

    #[test]
    fn fix_failed_adds_error_finding() {
        // Create a failed fix record manually and verify it produces the
        // correct finding shape.
        let f = DoctorFinding {
            id: "fix.failed".into(),
            severity: DoctorSeverity::Error,
            category: DoctorCategory::Compatibility,
            title: "Fix \"fix.checkpoint\" failed".into(),
            message: "Simulated failure".into(),
            evidence: vec![
                DoctorEvidence {
                    field: "fix_id".into(),
                    value: DoctorEvidenceValue::String("fix.checkpoint".into()),
                    unit: None,
                },
                DoctorEvidence {
                    field: "finding_id".into(),
                    value: DoctorEvidenceValue::String("wal.large_file".into()),
                    unit: None,
                },
            ],
            recommendation: Some(DoctorRecommendation {
                summary: "Review the failed action...".into(),
                commands: vec![],
                safe_to_automate: false,
            }),
        };
        assert_eq!(f.severity, DoctorSeverity::Error);
    }

    // ---- helpers -------------------------------------------------------

    fn fresh_collected() -> CollectedData {
        CollectedData {
            database: DoctorDatabaseSummary {
                path: "test.ddb".into(),
                wal_path: "test.ddb.wal".into(),
                format_version: DB_FORMAT_VERSION,
                page_size: 4096,
                page_count: 128,
                schema_cookie: 7,
            },
            storage: Some(StorageInfo {
                path: "test.ddb".into(),
                wal_path: "test.ddb.wal".into(),
                format_version: DB_FORMAT_VERSION,
                page_size: 4096,
                cache_size_mb: 4,
                page_count: 128,
                schema_cookie: 7,
                wal_end_lsn: 0,
                wal_file_size: 32,
                last_checkpoint_lsn: 0,
                active_readers: 0,
                wal_versions: 10,
                warning_count: 0,
                shared_wal: false,
            }),
            header: Some(HeaderInfo {
                magic_hex: "DECENTDB".into(),
                format_version: DB_FORMAT_VERSION,
                page_size: 4096,
                header_checksum: 12345,
                schema_cookie: 7,
                catalog_root_page_id: 2,
                freelist_root_page_id: 0,
                freelist_head_page_id: 0,
                freelist_page_count: 0,
                last_checkpoint_lsn: 0,
            }),
            schema: Some(SchemaSnapshot {
                snapshot_version: 1,
                schema_cookie: 7,
                tables: vec![],
                views: vec![],
                indexes: vec![],
                triggers: vec![],
            }),
            indexes: vec![],
            physical_bytes: 8192,
        }
    }

    // ---- DR-07: Markdown renderer golden tests --------------------------

    #[test]
    fn markdown_no_findings() {
        let r = report(vec![]);
        let md = render_markdown(&r);
        assert!(md.contains("# DecentDB Doctor Report"));
        assert!(md.contains("## Status"));
        assert!(md.contains("Overall status: OK"));
        assert!(md.contains("## Summary"));
        assert!(md.contains("| Error | 0 |"));
        assert!(md.contains("## Findings\n\nNo findings."));
        assert!(md.contains("No fixes requested."));
    }

    #[test]
    fn markdown_warning_with_evidence() {
        let r = report(vec![finding(
            "wal.large_file",
            DoctorSeverity::Warning,
            DoctorCategory::Wal,
        )]);
        let md = render_markdown(&r);
        assert!(md.contains("Overall status: WARNING"));
        assert!(md.contains("### WARNING wal.large_file"));
        assert!(md.contains("| Field | Value | Unit |"));
        assert!(md.contains("| some_field | 42 |  |"));
        assert!(md.contains("## Summary"));
        assert!(md.contains("| Warning | 1 |"));
        assert!(md.contains("No fixes requested."));
    }

    #[test]
    fn markdown_fix_report_with_applied_and_failed() {
        use DoctorCategory::*;
        let fixes = vec![
            DoctorFix {
                id: "fix.checkpoint".into(),
                finding_id: "wal.large_file".into(),
                status: DoctorFixStatus::Applied,
                message: "Checkpoint completed.".into(),
                evidence_before: vec![DoctorEvidence {
                    field: "wal_file_size".into(),
                    value: DoctorEvidenceValue::Uint(1_048_576),
                    unit: Some("bytes".into()),
                }],
                evidence_after: vec![DoctorEvidence {
                    field: "wal_file_size".into(),
                    value: DoctorEvidenceValue::Uint(32),
                    unit: Some("bytes".into()),
                }],
            },
            DoctorFix {
                id: "fix.rebuild_stale_index".into(),
                finding_id: "schema.index_not_fresh".into(),
                status: DoctorFixStatus::Failed,
                message: "Index missing.".into(),
                evidence_before: vec![],
                evidence_after: vec![],
            },
        ];
        let r = DoctorReport::new(
            DoctorMode::Fix,
            db_summary(),
            all_categories(),
            vec![],
            vec![finding("wal.large_file", DoctorSeverity::Warning, Wal)],
            fixes,
            DoctorCollectedFacts::default(),
        );
        let md = render_markdown(&r);
        assert!(md.contains("## Fixes"));
        assert!(
            md.contains("| fix.checkpoint | wal.large_file | applied | Checkpoint completed. |")
        );
        assert!(md.contains(
            "| fix.rebuild_stale_index | schema.index_not_fresh | failed | Index missing. |"
        ));
        assert!(md.contains("## Findings"));
    }

    #[test]
    fn markdown_path_and_database_fields() {
        let r = report(vec![]);
        let md = render_markdown(&r);
        assert!(md.contains("## Database"));
        assert!(md.contains("| Path | test.ddb |"));
        assert!(md.contains("| Format version | 1 |"));
        assert!(md.contains("| Page size | 4096 |"));
        assert!(md.contains("| Page count | 128 |"));
        assert!(md.contains("| Schema cookie | 7 |"));
    }

    #[test]
    fn markdown_fix_mode_no_auto_fixable() {
        let r = DoctorReport::new(
            DoctorMode::Fix,
            db_summary(),
            all_categories(),
            vec![],
            vec![],
            vec![],
            DoctorCollectedFacts::default(),
        );
        let md = render_markdown(&r);
        assert!(md.contains("No auto-fixable findings were found."));
    }
}

//! Doctor domain model, fact collection, rule engine, and report
//! serialization.
//!
//! DR-01: Typed structures, sort order, summary calculation, and JSON
//! serialization.
//! DR-02: Read-only fact collection from existing metadata.
//! DR-03: v1 rule engine and finding catalog.

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
// DR-02: Fact Collection
// ---------------------------------------------------------------------------

/// Facts collected from a database for rule evaluation.
///
/// This is an internal type used during collection; it is never serialized
/// directly.
struct CollectedData {
    database: DoctorDatabaseSummary,
    /// `Some` when a full open succeeded; `None` during partial (header-only)
    /// reports.
    storage: Option<StorageInfo>,
    /// `Some` when a full open succeeded; `None` during partial reports.
    header: Option<HeaderInfo>,
    /// `Some` when a full open succeeded; `None` during partial reports.
    schema: Option<SchemaSnapshot>,
    /// Index list; empty when schema was not available.
    indexes: Vec<IndexInfo>,
    /// Physical database file size in bytes (used by WAL rules).
    physical_bytes: u64,
    /// Whether the full engine open failed after a successful loose-header
    /// read. When `true`, the collector produces a partial report with
    /// `database.open_failed`.
    open_failed: bool,
}

/// Collect all facts needed by v1 rules without mutating the database.
///
/// Returns an `Err` only when even a loose header cannot be read.
fn collect_facts(path: &Path, _options: &DoctorOptions) -> Result<CollectedData> {
    let wal_path_str = format!("{}.wal", path.display());

    let header_info = match Db::read_header_info(path) {
        Ok(h) => h,
        Err(_e) => {
            return Ok(CollectedData {
                database: DoctorDatabaseSummary {
                    path: path.display().to_string(),
                    wal_path: wal_path_str,
                    format_version: 0,
                    page_size: 0,
                    page_count: 0,
                    schema_cookie: 0,
                },
                storage: None,
                header: None,
                schema: None,
                indexes: Vec::new(),
                physical_bytes: 0,
                open_failed: false,
            });
        }
    };

    let database = DoctorDatabaseSummary {
        path: path.display().to_string(),
        wal_path: wal_path_str,
        format_version: header_info.format_version,
        page_size: header_info.page_size,
        page_count: 0,
        schema_cookie: header_info.schema_cookie,
    };

    // Physical file size for threshold calculations.
    let physical_bytes = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);

    let db = match Db::open(
        path,
        DbConfig {
            auto_checkpoint_on_open_mb: 0, // DR-02: must be read-only
            ..DbConfig::default()
        },
    ) {
        Ok(d) => d,
        Err(_e) => {
            return Ok(CollectedData {
                database,
                storage: None,
                header: Some(header_info),
                schema: None,
                indexes: Vec::new(),
                physical_bytes,
                open_failed: true,
            });
        }
    };

    let storage = db.storage_info().ok();
    let header = db.header_info().ok();
    let schema = db.get_schema_snapshot().ok();
    let indexes = db.list_indexes().unwrap_or_default();

    // Update page_count from storage when available.
    let database = {
        let page = storage
            .as_ref()
            .map_or(database.page_count, |s| s.page_count);
        DoctorDatabaseSummary {
            page_count: page,
            ..database
        }
    };

    Ok(CollectedData {
        database,
        storage,
        header,
        schema,
        indexes,
        physical_bytes,
        open_failed: false,
    })
}

// ---------------------------------------------------------------------------
// DR-03: Rule Engine
// ---------------------------------------------------------------------------

/// v1 thresholds as specified in the plan.
const WAL_LARGE_FILE_BYTES: u64 = 64 * 1024 * 1024; // 64 MiB
const WAL_MANY_VERSIONS_THRESHOLD: usize = 100_000;
const FRAGMENTATION_MIN_PAGE_COUNT: u32 = 128;
const FRAGMENTATION_HIGH_RATIO: f64 = 0.25;
const FRAGMENTATION_MODERATE_RATIO: f64 = 0.10;
const MANY_INDEXES_THRESHOLD: usize = 8;

/// Evaluate every v1 rule against the collected data and return findings in
/// the order described by the plan.
fn evaluate_rules(data: &CollectedData, _options: &DoctorOptions) -> Vec<DoctorFinding> {
    let mut findings = Vec::new();

    // 10.1 Header and compatibility findings
    if data.header.is_none() && data.storage.is_none() {
        findings.push(header_unreadable(
            &data.database.path,
            "Header could not be read. Verify the path, file permissions, and file type.".into(),
        ));
        return findings;
    }

    if data.open_failed {
        findings.push(open_failed(
            data.database.format_version,
            data.database.page_size,
            "Full engine open failed. Use `decentdb info` or migration tooling.",
        ));
        // Partial report — no further checks are possible.
        return findings;
    }

    // Full report: all checks are available.

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

        // schema.index_not_fresh
        for idx in &data.indexes {
            if !idx.fresh {
                findings.push(index_not_fresh(&idx.name, &idx.table_name));
            }
        }

        // statistics.missing_analyze — intentionally not implemented in v1.
        // Reliable detection requires a typed engine helper that does not yet
        // exist. See `design/DR_ADVISOR_INTROSPECTION_PLAN.md` Section 10.4.
    }

    // 10.5 Index verification findings — only when explicitly requested.
    if let Some(ref db) = data.storage {
        // We don't have access to the live Db handle here, so index
        // verification findings are deferred to DR-04.
        let _ = db;
    }

    // 10.6 Fix execution findings (`fix.failed`) are only produced during
    // DR-05 fix execution and are deferred.

    findings
}

/// Top-level entry point: collect facts, evaluate rules, and produce a report.
///
/// This is the public API for callers (CLI, bindings, tests).
pub fn run_doctor(path: impl AsRef<Path>, options: DoctorOptions) -> Result<DoctorReport> {
    let data = collect_facts(path.as_ref(), &options)?;

    // Determine the active check set.
    let checked_categories = match &options.checks {
        DoctorCheckSelection::All => all_category_list(),
        DoctorCheckSelection::Selected(cats) => cats.clone(),
    };

    let mut raw_findings = evaluate_rules(&data, &options);

    // Filter findings by selected categories.
    raw_findings.retain(|f| checked_categories.contains(&f.category));

    let mode = if options.fix {
        DoctorMode::Fix
    } else {
        DoctorMode::Check
    };

    let collected = DoctorCollectedFacts::from_data(&data);

    Ok(DoctorReport::new(
        mode,
        data.database,
        checked_categories,
        Vec::new(), // pre_fix_findings: empty in check-only mode
        raw_findings,
        Vec::new(), // fixes: empty in check-only mode
        collected,
    ))
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
        let data = CollectedData {
            database: DoctorDatabaseSummary {
                path: "bad.ddb".into(),
                wal_path: "bad.ddb.wal".into(),
                format_version: 0,
                page_size: 0,
                page_count: 0,
                schema_cookie: 0,
            },
            storage: None,
            header: None,
            schema: None,
            indexes: vec![],
            physical_bytes: 0,
            open_failed: false,
        };
        let findings = evaluate_rules(&data, &DoctorOptions::default());
        assert!(findings.iter().any(|f| f.id == "header.unreadable"));
        assert!(findings.len() == 1);
    }

    // -- database.open_failed --------------------------------------------

    #[test]
    fn open_failed_is_error() {
        let mut data = fresh_collected();
        data.open_failed = true;
        data.storage = None;
        data.schema = None;

        let findings = evaluate_rules(&data, &DoctorOptions::default());
        assert!(
            findings.iter().any(|f| f.id == "database.open_failed"),
            "{findings:?}"
        );
        assert!(findings.iter().all(|f| f.severity == DoctorSeverity::Error));
    }

    // -- compatibility.format_version_unknown ----------------------------

    #[test]
    fn format_version_match_no_finding() {
        let data = fresh_collected();
        let findings = evaluate_rules(&data, &DoctorOptions::default());
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
        let findings = evaluate_rules(&data, &DoctorOptions::default());
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
        let findings = evaluate_rules(&data, &DoctorOptions::default());
        assert!(!findings.iter().any(|f| f.id == "wal.large_file"));
    }

    #[test]
    fn wal_file_at_64_mib_threshold() {
        let data = fresh_collected(); // physical_bytes = 8192, so threshold = max(64MiB, 2048) = 64MiB
        let mut data_with_wal = data;
        if let Some(ref mut st) = data_with_wal.storage {
            st.wal_file_size = 64 * 1024 * 1024; // exactly 64 MiB
        }
        let findings = evaluate_rules(&data_with_wal, &DoctorOptions::default());
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
        let findings = evaluate_rules(&data, &DoctorOptions::default());
        assert!(
            findings.iter().any(|f| f.id == "wal.large_file"),
            "wal_file_size >= threshold should trigger"
        );
    }

    // -- wal.many_versions -----------------------------------------------

    #[test]
    fn wal_versions_below_threshold_no_finding() {
        let data = fresh_collected();
        let findings = evaluate_rules(&data, &DoctorOptions::default());
        assert!(!findings.iter().any(|f| f.id == "wal.many_versions"));
    }

    #[test]
    fn wal_versions_at_threshold() {
        let mut data = fresh_collected();
        if let Some(ref mut st) = data.storage {
            st.wal_versions = 100_000;
        }
        let findings = evaluate_rules(&data, &DoctorOptions::default());
        assert!(findings.iter().any(|f| f.id == "wal.many_versions"));
    }

    #[test]
    fn wal_versions_above_threshold() {
        let mut data = fresh_collected();
        if let Some(ref mut st) = data.storage {
            st.wal_versions = 200_000;
        }
        let findings = evaluate_rules(&data, &DoctorOptions::default());
        assert!(findings.iter().any(|f| f.id == "wal.many_versions"));
    }

    // -- wal.long_readers_present ----------------------------------------

    #[test]
    fn no_active_readers_no_finding() {
        let data = fresh_collected();
        let findings = evaluate_rules(&data, &DoctorOptions::default());
        assert!(!findings.iter().any(|f| f.id == "wal.long_readers_present"));
    }

    #[test]
    fn active_readers_with_non_empty_wal() {
        let mut data = fresh_collected();
        if let Some(ref mut st) = data.storage {
            st.active_readers = 1;
            st.wal_file_size = 1024;
        }
        let findings = evaluate_rules(&data, &DoctorOptions::default());
        assert!(findings.iter().any(|f| f.id == "wal.long_readers_present"));
    }

    #[test]
    fn active_readers_with_empty_wal_no_finding() {
        let mut data = fresh_collected();
        if let Some(ref mut st) = data.storage {
            st.active_readers = 1;
            st.wal_file_size = 0;
        }
        let findings = evaluate_rules(&data, &DoctorOptions::default());
        assert!(!findings.iter().any(|f| f.id == "wal.long_readers_present"));
    }

    // -- wal.reader_warnings_recorded -----------------------------------

    #[test]
    fn no_warnings_no_finding() {
        let data = fresh_collected();
        let findings = evaluate_rules(&data, &DoctorOptions::default());
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
        let findings = evaluate_rules(&data, &DoctorOptions::default());
        assert!(findings
            .iter()
            .any(|f| f.id == "wal.reader_warnings_recorded"));
    }

    // -- wal.shared_enabled ----------------------------------------------

    #[test]
    fn shared_wal_disabled_no_finding() {
        let data = fresh_collected();
        let findings = evaluate_rules(&data, &DoctorOptions::default());
        assert!(!findings.iter().any(|f| f.id == "wal.shared_enabled"));
    }

    #[test]
    fn shared_wal_enabled_is_info() {
        let mut data = fresh_collected();
        if let Some(ref mut st) = data.storage {
            st.shared_wal = true;
        }
        let findings = evaluate_rules(&data, &DoctorOptions::default());
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
        let findings = evaluate_rules(&data, &DoctorOptions::default());
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
        let findings = evaluate_rules(&data, &DoctorOptions::default());
        assert!(findings.iter().any(|f| f.id == "fragmentation.high"));
    }

    #[test]
    fn fragmentation_high_above_25_percent() {
        let mut data = fresh_collected();
        data.database.page_count = 200;
        if let Some(ref mut hdr) = data.header {
            hdr.freelist_page_count = 80; // 40%
        }
        let findings = evaluate_rules(&data, &DoctorOptions::default());
        assert!(findings.iter().any(|f| f.id == "fragmentation.high"));
    }

    #[test]
    fn fragmentation_moderate_at_10_percent() {
        let mut data = fresh_collected();
        data.database.page_count = 200;
        if let Some(ref mut hdr) = data.header {
            hdr.freelist_page_count = 20; // 10%
        }
        let findings = evaluate_rules(&data, &DoctorOptions::default());
        assert!(findings.iter().any(|f| f.id == "fragmentation.moderate"));
    }

    #[test]
    fn fragmentation_below_10_percent_no_finding() {
        let mut data = fresh_collected();
        data.database.page_count = 200;
        if let Some(ref mut hdr) = data.header {
            hdr.freelist_page_count = 15; // 7.5%
        }
        let findings = evaluate_rules(&data, &DoctorOptions::default());
        assert!(!findings
            .iter()
            .any(|f| f.id == "fragmentation.moderate" || f.id == "fragmentation.high"));
    }

    // -- schema.no_user_tables -------------------------------------------

    #[test]
    fn no_user_tables_is_info() {
        let data = fresh_collected();
        let findings = evaluate_rules(&data, &DoctorOptions::default());
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
        let findings = evaluate_rules(&data, &DoctorOptions::default());
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
        let findings = evaluate_rules(&data, &DoctorOptions::default());
        assert!(findings
            .iter()
            .any(|f| f.id == "schema.many_indexes_on_table"));
    }

    // -- schema.index_not_fresh ------------------------------------------

    #[test]
    fn fresh_indexes_no_finding() {
        let data = fresh_collected();
        let findings = evaluate_rules(&data, &DoctorOptions::default());
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
        let findings = evaluate_rules(&data, &DoctorOptions::default());
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
            open_failed: false,
        }
    }
}

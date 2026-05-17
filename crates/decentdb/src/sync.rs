use std::collections::{BTreeSet, HashSet};
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::catalog::{identifiers_equal, TableSchema};
use crate::error::{DbError, Result};
use crate::exec::EngineRuntime;
use crate::record::value::Value;
use crate::sql::ast::{BinaryOp, Expr};
use crate::sql::parser::parse_expression_sql;
use crate::vfs::{self, FileKind, OpenMode, VfsFile, VfsHandle};

pub(crate) const METADATA_TABLE: &str = "__decentdb_sync_metadata";

pub(crate) const METADATA_TABLE_DDL: &str =
    "CREATE TABLE IF NOT EXISTS __decentdb_sync_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)";

pub(crate) const CONFLICTS_TABLE: &str = "__decentdb_sync_conflicts";

pub(crate) const CONFLICTS_TABLE_DDL: &str = "CREATE TABLE IF NOT EXISTS __decentdb_sync_conflicts (conflict_id INT64 PRIMARY KEY, batch_id TEXT NOT NULL, remote_replica_id TEXT NOT NULL, remote_sequence INT64 NOT NULL, table_name TEXT NOT NULL, operation TEXT NOT NULL, conflict_type TEXT NOT NULL, message TEXT NOT NULL, primary_key_json TEXT NOT NULL, remote_record_json TEXT NOT NULL, local_row_json TEXT, created_at_micros INT64 NOT NULL, resolved INT64 NOT NULL)";

pub(crate) const PEERS_TABLE: &str = "__decentdb_sync_peers";

pub(crate) const PEERS_TABLE_DDL: &str = "CREATE TABLE IF NOT EXISTS __decentdb_sync_peers (name TEXT PRIMARY KEY, endpoint TEXT NOT NULL, token_env TEXT, created_at_micros INT64 NOT NULL, updated_at_micros INT64 NOT NULL)";

pub(crate) const SESSIONS_TABLE: &str = "__decentdb_sync_sessions";

pub(crate) const SESSIONS_TABLE_DDL: &str = "CREATE TABLE IF NOT EXISTS __decentdb_sync_sessions (session_id INT64 PRIMARY KEY, peer_name TEXT NOT NULL, direction TEXT NOT NULL, remote_replica_id TEXT, started_at_micros INT64 NOT NULL, ended_at_micros INT64, status TEXT NOT NULL, error TEXT, pushed_batch_id TEXT, pulled_batch_id TEXT, pushed_seen INT64 NOT NULL, pushed_applied INT64 NOT NULL, pushed_skipped INT64 NOT NULL, pushed_conflicted INT64 NOT NULL, pulled_seen INT64 NOT NULL, pulled_applied INT64 NOT NULL, pulled_skipped INT64 NOT NULL, pulled_conflicted INT64 NOT NULL, retry_count INT64 NOT NULL)";

pub(crate) const SCOPES_TABLE: &str = "__decentdb_sync_scopes";

pub(crate) const SCOPES_TABLE_DDL: &str = "CREATE TABLE IF NOT EXISTS __decentdb_sync_scopes (name TEXT PRIMARY KEY, include_tables_json TEXT NOT NULL, row_filter TEXT, filter_columns_json TEXT NOT NULL, created_at_micros INT64 NOT NULL, updated_at_micros INT64 NOT NULL)";

pub(crate) const PEER_SCOPES_TABLE: &str = "__decentdb_sync_peer_scopes";

pub(crate) const PEER_SCOPES_TABLE_DDL: &str = "CREATE TABLE IF NOT EXISTS __decentdb_sync_peer_scopes (peer_name TEXT PRIMARY KEY, scope_name TEXT NOT NULL, created_at_micros INT64 NOT NULL, updated_at_micros INT64 NOT NULL)";

pub(crate) fn is_internal_table_name(name: &str) -> bool {
    name.starts_with("__decentdb_")
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncJournalRecord {
    #[serde(rename = "schema_version")]
    pub schema_version: u32,
    pub sequence: u64,
    pub replica_id: String,
    pub transaction_lsn: u64,
    pub table: String,
    pub operation: String,
    pub primary_key: serde_json::Value,
    pub after: Option<serde_json::Value>,
    pub schema_cookie: u32,
    pub committed_at_micros: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncChangeBatch {
    pub protocol_version: u32,
    pub batch_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_replica_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_high_watermark: Option<u64>,
    pub record_count: usize,
    #[serde(default)]
    pub records: Vec<SyncJournalRecord>,
}

impl SyncChangeBatch {
    const PROTOCOL_VERSION: u32 = 1;

    #[must_use]
    pub fn empty() -> Self {
        Self {
            protocol_version: Self::PROTOCOL_VERSION,
            batch_id: "sync-batch:v1:empty:0:0:0".to_string(),
            source_replica_id: None,
            first_sequence: None,
            last_sequence: None,
            source_high_watermark: None,
            record_count: 0,
            records: Vec::new(),
        }
    }

    pub fn from_records(records: Vec<SyncJournalRecord>) -> Result<Self> {
        if records.is_empty() {
            return Ok(Self::empty());
        }

        let source_replica_id = Some(records[0].replica_id.clone());
        let first_sequence = Some(records[0].sequence);
        let last_sequence = Some(
            records
                .last()
                .map(|record| record.sequence)
                .unwrap_or(records[0].sequence),
        );
        let batch = Self {
            protocol_version: Self::PROTOCOL_VERSION,
            batch_id: Self::deterministic_batch_id(
                source_replica_id.as_deref(),
                first_sequence,
                last_sequence,
                records.len(),
            ),
            source_replica_id,
            first_sequence,
            last_sequence,
            source_high_watermark: None,
            record_count: records.len(),
            records,
        };
        batch.validate()?;
        Ok(batch)
    }

    pub(crate) fn scoped_from_records(
        records: Vec<SyncJournalRecord>,
        source_replica_id: Option<String>,
        source_high_watermark: Option<u64>,
    ) -> Result<Self> {
        let mut batch = Self::from_records(records)?;
        if batch.source_replica_id.is_none() {
            batch.source_replica_id = source_replica_id;
        }
        batch.source_high_watermark = source_high_watermark;
        batch.validate()?;
        Ok(batch)
    }

    pub fn validate(&self) -> Result<()> {
        if self.protocol_version != Self::PROTOCOL_VERSION {
            return Err(DbError::sql(format!(
                "unsupported sync batch protocol version {}",
                self.protocol_version
            )));
        }

        if self.record_count != self.records.len() {
            return Err(DbError::sql(format!(
                "sync batch record_count {} does not match {} record(s)",
                self.record_count,
                self.records.len()
            )));
        }

        if self.records.is_empty() {
            if self.batch_id != "sync-batch:v1:empty:0:0:0" {
                return Err(DbError::sql(format!(
                    "sync batch_id mismatch for empty batch: expected sync-batch:v1:empty:0:0:0, got {}",
                    self.batch_id
                )));
            }
            if self.first_sequence.is_some() || self.last_sequence.is_some() {
                return Err(DbError::sql(
                    "empty sync batch must not declare sequence bounds",
                ));
            }
            if self.source_high_watermark.is_some() && self.source_replica_id.is_none() {
                return Err(DbError::sql(
                    "empty sync batch with source_high_watermark must include source_replica_id",
                ));
            }
            return Ok(());
        }

        let source_replica_id = self
            .source_replica_id
            .as_deref()
            .ok_or_else(|| DbError::sql("non-empty sync batch must include source_replica_id"))?;
        let first_sequence = self
            .first_sequence
            .ok_or_else(|| DbError::sql("non-empty sync batch must include first_sequence"))?;
        let last_sequence = self
            .last_sequence
            .ok_or_else(|| DbError::sql("non-empty sync batch must include last_sequence"))?;
        let expected_batch_id = Self::deterministic_batch_id(
            Some(source_replica_id),
            Some(first_sequence),
            Some(last_sequence),
            self.record_count,
        );
        if self.batch_id != expected_batch_id {
            return Err(DbError::sql(format!(
                "sync batch_id mismatch: expected {}, got {}",
                expected_batch_id, self.batch_id
            )));
        }

        if self.records.first().map(|record| record.sequence) != Some(first_sequence) {
            return Err(DbError::sql(format!(
                "sync batch first_sequence {} does not match first record sequence {}",
                first_sequence,
                self.records
                    .first()
                    .map(|record| record.sequence)
                    .unwrap_or(0)
            )));
        }
        if self.records.last().map(|record| record.sequence) != Some(last_sequence) {
            return Err(DbError::sql(format!(
                "sync batch last_sequence {} does not match last record sequence {}",
                last_sequence,
                self.records
                    .last()
                    .map(|record| record.sequence)
                    .unwrap_or(0)
            )));
        }
        if self
            .source_high_watermark
            .is_some_and(|watermark| watermark < last_sequence)
        {
            return Err(DbError::sql(format!(
                "sync batch source_high_watermark must be >= last_sequence {last_sequence}"
            )));
        }

        for record in &self.records {
            if record.replica_id != source_replica_id {
                return Err(DbError::sql(format!(
                    "mixed source_replica_id values in sync batch: expected {}, got {}",
                    source_replica_id, record.replica_id
                )));
            }
        }

        Ok(())
    }

    fn deterministic_batch_id(
        source_replica_id: Option<&str>,
        first_sequence: Option<u64>,
        last_sequence: Option<u64>,
        record_count: usize,
    ) -> String {
        match (
            source_replica_id,
            first_sequence,
            last_sequence,
            record_count,
        ) {
            (_, None, None, 0) => "sync-batch:v1:empty:0:0:0".to_string(),
            (Some(source_replica_id), Some(first_sequence), Some(last_sequence), record_count) => {
                format!(
                    "sync-batch:v1:{source_replica_id}:{first_sequence}:{last_sequence}:{record_count}"
                )
            }
            _ => "sync-batch:v1:empty:0:0:0".to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct SyncStatus {
    pub enabled: bool,
    pub replica_id: Option<String>,
    pub next_sequence: u64,
    pub journal_path: Option<String>,
    pub journal_size_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SyncRunDirection {
    Push,
    Pull,
    Both,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SyncConflictPolicy {
    #[serde(alias = "record")]
    Record,
    #[serde(alias = "stop")]
    Stop,
    #[serde(alias = "last-writer-wins")]
    LastWriterWins,
    #[serde(alias = "origin-priority")]
    OriginPriority,
}

impl SyncConflictPolicy {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Record => "record",
            Self::Stop => "stop",
            Self::LastWriterWins => "last_writer_wins",
            Self::OriginPriority => "origin_priority",
        }
    }
}

impl Display for SyncConflictPolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SyncConflictPolicy {
    type Err = DbError;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "record" => Ok(Self::Record),
            "stop" => Ok(Self::Stop),
            "last_writer_wins" | "last-writer-wins" => Ok(Self::LastWriterWins),
            "origin_priority" | "origin-priority" => Ok(Self::OriginPriority),
            other => Err(DbError::sql(format!(
                "unsupported sync conflict policy '{other}'"
            ))),
        }
    }
}

impl Default for SyncConflictPolicy {
    fn default() -> Self {
        Self::Record
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncConflictPolicyConfig {
    #[serde(default)]
    pub default_policy: SyncConflictPolicy,
    pub origin_priority: Vec<String>,
}

impl Default for SyncConflictPolicyConfig {
    fn default() -> Self {
        Self {
            default_policy: SyncConflictPolicy::Record,
            origin_priority: Vec::new(),
        }
    }
}

impl SyncRunDirection {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Push => "push",
            Self::Pull => "pull",
            Self::Both => "both",
        }
    }
}

impl Display for SyncRunDirection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SyncRunDirection {
    type Err = DbError;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "push" => Ok(Self::Push),
            "pull" => Ok(Self::Pull),
            "both" => Ok(Self::Both),
            other => Err(DbError::sql(format!(
                "unsupported sync run direction '{other}'"
            ))),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncPeer {
    pub name: String,
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_env: Option<String>,
    pub created_at_micros: i64,
    pub updated_at_micros: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncScope {
    pub name: String,
    pub include_tables: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_filter: Option<String>,
    pub filter_columns: Vec<String>,
    pub created_at_micros: i64,
    pub updated_at_micros: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncPeerScopeBinding {
    pub peer_name: String,
    pub scope_name: String,
    pub created_at_micros: i64,
    pub updated_at_micros: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncSession {
    pub session_id: i64,
    pub peer_name: String,
    pub direction: SyncRunDirection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_replica_id: Option<String>,
    pub started_at_micros: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at_micros: Option<i64>,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pushed_batch_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pulled_batch_id: Option<String>,
    pub pushed_seen: i64,
    pub pushed_applied: i64,
    pub pushed_skipped: i64,
    pub pushed_conflicted: i64,
    pub pulled_seen: i64,
    pub pulled_applied: i64,
    pub pulled_skipped: i64,
    pub pulled_conflicted: i64,
    pub retry_count: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncHandshake {
    pub protocol_version: u32,
    pub engine_version: String,
    pub replica_id: Option<String>,
    pub capabilities: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncRunSummary {
    pub peer_name: String,
    pub direction: SyncRunDirection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_replica_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pushed: Option<SyncImportSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pulled: Option<SyncImportSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pushed_batch_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pulled_batch_id: Option<String>,
    pub retry_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SyncConflict {
    pub conflict_id: i64,
    pub batch_id: String,
    pub remote_replica_id: String,
    pub remote_sequence: i64,
    pub table_name: String,
    pub operation: String,
    pub conflict_type: String,
    pub message: String,
    pub primary_key_json: serde_json::Value,
    pub remote_record_json: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_at_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution_note: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_record_json: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_row_json: Option<serde_json::Value>,
    pub created_at_micros: i64,
    pub resolved: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncDoctorSeverity {
    #[default]
    Info,
    Warning,
    Error,
}

impl SyncDoctorSeverity {
    fn as_rank(&self) -> u8 {
        match self {
            Self::Info => 0,
            Self::Warning => 1,
            Self::Error => 2,
        }
    }

    fn max(self, other: Self) -> Self {
        if self.as_rank() >= other.as_rank() {
            self
        } else {
            other
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct SyncJournalIssue {
    pub line_number: usize,
    pub sequence: Option<u64>,
    pub severity: SyncDoctorSeverity,
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct SyncJournalIntegrityReport {
    pub total_records: usize,
    pub first_sequence: Option<u64>,
    pub last_sequence: Option<u64>,
    pub highest_severity: SyncDoctorSeverity,
    pub issues: Vec<SyncJournalIssue>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncImportSummary {
    pub seen: usize,
    pub applied: usize,
    pub skipped: usize,
    pub conflicted: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SyncMutation {
    pub table: String,
    pub operation: SyncOperation,
    pub primary_key: serde_json::Value,
    pub after: Option<serde_json::Value>,
    pub schema_cookie: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SyncOperation {
    Insert,
    Update,
    Delete,
}

impl SyncOperation {
    fn as_str(&self) -> &'static str {
        match self {
            SyncOperation::Insert => "insert",
            SyncOperation::Update => "update",
            SyncOperation::Delete => "delete",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SyncScopeDefinition {
    pub(crate) name: String,
    pub(crate) include_tables: Vec<String>,
    pub(crate) row_filter: Option<String>,
    pub(crate) filter_columns: Vec<String>,
}

pub(crate) fn validate_sync_scope_definition(
    runtime: &EngineRuntime,
    name: &str,
    include_tables: &[&str],
    row_filter: Option<&str>,
) -> Result<SyncScopeDefinition> {
    let name = name.trim();
    if name.is_empty() {
        return Err(DbError::sql("sync scope name must not be empty"));
    }

    if include_tables.is_empty() {
        return Err(DbError::sql("sync scope include_tables must not be empty"));
    }

    let mut canonical_tables = Vec::with_capacity(include_tables.len());
    let mut seen_tables = BTreeSet::new();
    for table_name in include_tables {
        let table_name = table_name.trim();
        if table_name.is_empty() {
            return Err(DbError::sql("sync scope table name must not be empty"));
        }
        if is_internal_table_name(table_name) {
            return Err(DbError::sql(format!(
                "sync scope cannot include internal table '{table_name}'"
            )));
        }
        if !seen_tables.insert(table_name.to_ascii_lowercase()) {
            return Err(DbError::sql(format!(
                "sync scope include_tables contains duplicate table '{table_name}'"
            )));
        }
        let table = runtime.catalog.table(table_name).ok_or_else(|| {
            DbError::sql(format!("sync scope table '{table_name}' does not exist"))
        })?;
        if table.primary_key_columns.is_empty() {
            return Err(DbError::sql(format!(
                "sync scope table '{}' must have a stable primary key",
                table.name
            )));
        }
        canonical_tables.push(table.name.clone());
    }
    canonical_tables.sort_unstable();

    let row_filter = row_filter.map(str::trim).map(str::to_string);
    if row_filter
        .as_deref()
        .is_some_and(|filter| filter.is_empty())
    {
        return Err(DbError::sql("sync scope row_filter must not be empty"));
    }

    let filter_columns = if let Some(filter_sql) = row_filter.as_deref() {
        let expr = parse_expression_sql(filter_sql).map_err(|error| {
            DbError::sql(format!(
                "invalid row filter for sync scope '{name}': {error}"
            ))
        })?;
        validate_sync_scope_filter_expr(runtime, &canonical_tables, &expr)?
    } else {
        Vec::new()
    };

    Ok(SyncScopeDefinition {
        name: name.to_string(),
        include_tables: canonical_tables,
        row_filter,
        filter_columns,
    })
}

fn validate_sync_scope_filter_expr(
    runtime: &EngineRuntime,
    include_tables: &[String],
    expr: &Expr,
) -> Result<Vec<String>> {
    let mut referenced_columns = BTreeSet::new();
    validate_sync_scope_filter_expr_inner(expr, &mut referenced_columns)?;
    let mut columns = Vec::with_capacity(referenced_columns.len());
    for column in referenced_columns {
        let mut canonical_column: Option<String> = None;
        for table_name in include_tables {
            let table = runtime
                .catalog
                .table(table_name)
                .expect("validated table must exist");
            let Some(table_column) = table
                .columns
                .iter()
                .find(|candidate| identifiers_equal(&candidate.name, &column))
            else {
                return Err(DbError::sql(format!(
                    "sync scope row filter column '{column}' is missing from table '{}'",
                    table.name
                )));
            };
            if !table
                .primary_key_columns
                .iter()
                .any(|pk| identifiers_equal(pk, &table_column.name))
            {
                return Err(DbError::sql(format!(
                    "sync scope row filter column '{column}' must be part of the primary key for table '{}'",
                    table.name
                )));
            }
            if canonical_column.is_none() {
                canonical_column = Some(table_column.name.clone());
            }
        }
        columns.push(canonical_column.expect("column canonicalized"));
    }
    columns.sort_unstable();
    Ok(columns)
}

fn validate_sync_scope_filter_expr_inner(
    expr: &Expr,
    referenced_columns: &mut BTreeSet<String>,
) -> Result<()> {
    match expr {
        Expr::Binary { left, op, right } => match op {
            BinaryOp::And => {
                validate_sync_scope_filter_expr_inner(left, referenced_columns)?;
                validate_sync_scope_filter_expr_inner(right, referenced_columns)
            }
            BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq => {
                if let Some(error) = sync_scope_filter_disallowed_operand(left)
                    .or_else(|| sync_scope_filter_disallowed_operand(right))
                {
                    return Err(error);
                }
                let column = sync_scope_filter_column_name(left, right).ok_or_else(|| {
                    DbError::sql("sync scope predicates must compare a column against a literal")
                })?;
                sync_scope_filter_literal(right)?;
                referenced_columns.insert(column);
                Ok(())
            }
            BinaryOp::Or => Err(DbError::sql(
                "OR is not supported in sync row filters; use AND only",
            )),
            BinaryOp::Add
            | BinaryOp::Sub
            | BinaryOp::Mul
            | BinaryOp::Div
            | BinaryOp::Mod
            | BinaryOp::Concat => Err(DbError::sql(
                "arithmetic and concatenation are not supported in sync row filters",
            )),
            BinaryOp::JsonExtract | BinaryOp::JsonExtractText => Err(DbError::sql(
                "JSON path operators are not supported in sync row filters",
            )),
            BinaryOp::IsDistinctFrom | BinaryOp::IsNotDistinctFrom => Err(DbError::sql(
                "IS DISTINCT FROM is not supported in sync row filters",
            )),
            BinaryOp::RegexMatch
            | BinaryOp::RegexMatchCaseInsensitive
            | BinaryOp::RegexNotMatch
            | BinaryOp::RegexNotMatchCaseInsensitive => Err(DbError::sql(
                "regular expression operators are not supported in sync row filters",
            )),
        },
        Expr::InList {
            expr,
            items,
            negated,
        } => {
            if *negated {
                return Err(DbError::sql("NOT IN is not supported in sync row filters"));
            }
            if let Some(error) = sync_scope_filter_disallowed_operand(expr) {
                return Err(error);
            }
            sync_scope_filter_column(expr)?;
            if items.is_empty() {
                return Err(DbError::sql(
                    "IN lists in sync row filters must contain at least one literal",
                ));
            }
            for item in items {
                sync_scope_filter_literal(item)?;
            }
            if let Expr::Column {
                column,
                table: None,
            } = expr.as_ref()
            {
                referenced_columns.insert(column.clone());
                Ok(())
            } else {
                Err(DbError::sql(
                    "sync row filters must compare a single column against literal values",
                ))
            }
        }
        Expr::IsNull { expr, .. } => {
            if let Some(error) = sync_scope_filter_disallowed_operand(expr) {
                return Err(error);
            }
            sync_scope_filter_column(expr)?;
            if let Expr::Column {
                column,
                table: None,
            } = expr.as_ref()
            {
                referenced_columns.insert(column.clone());
                Ok(())
            } else {
                Err(DbError::sql(
                    "sync row filters must reference a single unqualified column",
                ))
            }
        }
        Expr::Column {
            table: Some(table),
            column,
        } => Err(DbError::sql(format!(
            "dotted column references are not supported in sync row filters: {table}.{column}"
        ))),
        Expr::Column {
            table: None,
            column,
        } => Err(DbError::sql(format!(
            "column '{column}' must be compared against a literal in sync row filters"
        ))),
        Expr::Literal(_) => Err(DbError::sql(
            "sync row filters must compare columns against literals",
        )),
        Expr::Unary { op, .. } => Err(DbError::sql(format!(
            "unary operator '{op:?}' is not supported in sync row filters"
        ))),
        Expr::Between { .. } => Err(DbError::sql("BETWEEN is not supported in sync row filters")),
        Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_) => Err(DbError::sql(
            "subqueries and EXISTS are not supported in sync row filters",
        )),
        Expr::Like { .. } => Err(DbError::sql("LIKE is not supported in sync row filters")),
        Expr::Function { name, .. } => Err(DbError::sql(format!(
            "functions are not supported in sync row filters: {name}()"
        ))),
        Expr::Aggregate { name, .. } => Err(DbError::sql(format!(
            "aggregate-like function calls are not supported in sync row filters: {name}"
        ))),
        Expr::RowNumber { .. } | Expr::WindowFunction { .. } => Err(DbError::sql(
            "window functions are not supported in sync row filters",
        )),
        Expr::Case { .. } => Err(DbError::sql(
            "CASE expressions are not supported in sync row filters",
        )),
        Expr::Cast { .. } => Err(DbError::sql("CAST is not supported in sync row filters")),
        Expr::Row(_) => Err(DbError::sql(
            "row values are not supported in sync row filters",
        )),
        Expr::Parameter(_) => Err(DbError::sql(
            "parameters are not supported in sync row filters",
        )),
    }
}

fn sync_scope_filter_column_name(left: &Expr, right: &Expr) -> Option<String> {
    match (left, right) {
        (
            Expr::Column {
                table: None,
                column,
            },
            Expr::Literal(_),
        ) => Some(column.clone()),
        _ => None,
    }
}

fn sync_scope_filter_column(expr: &Expr) -> Result<()> {
    match expr {
        Expr::Column { table: None, .. } => Ok(()),
        Expr::Column {
            table: Some(table),
            column,
        } => Err(DbError::sql(format!(
            "dotted column references are not supported in sync row filters: {table}.{column}"
        ))),
        _ => Err(DbError::sql(
            "sync row filter predicates must reference a single column",
        )),
    }
}

fn sync_scope_filter_disallowed_operand(expr: &Expr) -> Option<DbError> {
    match expr {
        Expr::Column {
            table: Some(table),
            column,
        } => Some(DbError::sql(format!(
            "dotted column references are not supported in sync row filters: {table}.{column}"
        ))),
        Expr::Function { name, .. } => Some(DbError::sql(format!(
            "functions are not supported in sync row filters: {name}()"
        ))),
        Expr::Aggregate { name, .. } => Some(DbError::sql(format!(
            "aggregate-like function calls are not supported in sync row filters: {name}"
        ))),
        Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_) => Some(DbError::sql(
            "subqueries and EXISTS are not supported in sync row filters",
        )),
        Expr::Binary { op, .. } => match op {
            BinaryOp::Or => Some(DbError::sql(
                "OR is not supported in sync row filters; use AND only",
            )),
            BinaryOp::Add
            | BinaryOp::Sub
            | BinaryOp::Mul
            | BinaryOp::Div
            | BinaryOp::Mod
            | BinaryOp::Concat => Some(DbError::sql(
                "arithmetic and concatenation are not supported in sync row filters",
            )),
            BinaryOp::JsonExtract | BinaryOp::JsonExtractText => Some(DbError::sql(
                "JSON path operators are not supported in sync row filters",
            )),
            BinaryOp::IsDistinctFrom | BinaryOp::IsNotDistinctFrom => Some(DbError::sql(
                "IS DISTINCT FROM is not supported in sync row filters",
            )),
            BinaryOp::RegexMatch
            | BinaryOp::RegexMatchCaseInsensitive
            | BinaryOp::RegexNotMatch
            | BinaryOp::RegexNotMatchCaseInsensitive => Some(DbError::sql(
                "regular expression operators are not supported in sync row filters",
            )),
            BinaryOp::And
            | BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq => None,
        },
        Expr::Unary { op, .. } => Some(DbError::sql(format!(
            "unary operator '{op:?}' is not supported in sync row filters"
        ))),
        Expr::Between { .. } => Some(DbError::sql("BETWEEN is not supported in sync row filters")),
        Expr::Like { .. } => Some(DbError::sql("LIKE is not supported in sync row filters")),
        Expr::RowNumber { .. } | Expr::WindowFunction { .. } => Some(DbError::sql(
            "window functions are not supported in sync row filters",
        )),
        Expr::Case { .. } => Some(DbError::sql(
            "CASE expressions are not supported in sync row filters",
        )),
        Expr::Cast { .. } => Some(DbError::sql("CAST is not supported in sync row filters")),
        Expr::InList { .. } => None,
        Expr::IsNull { .. } => None,
        Expr::Row(_) => Some(DbError::sql(
            "row values are not supported in sync row filters",
        )),
        Expr::Parameter(_) => Some(DbError::sql(
            "parameters are not supported in sync row filters",
        )),
        Expr::Column { table: None, .. } | Expr::Literal(_) => None,
    }
}

fn sync_scope_filter_literal(expr: &Expr) -> Result<()> {
    match expr {
        Expr::Literal(Value::Int64(_))
        | Expr::Literal(Value::Text(_))
        | Expr::Literal(Value::Null)
        | Expr::Literal(Value::Bool(_)) => Ok(()),
        Expr::Literal(_) => Err(DbError::sql(
            "sync row filters only support integer, text, NULL, true, and false literals",
        )),
        Expr::Column {
            table: Some(table),
            column,
        } => Err(DbError::sql(format!(
            "dotted column references are not supported in sync row filters: {table}.{column}"
        ))),
        Expr::Column {
            table: None,
            column,
        } => Err(DbError::sql(format!(
            "column '{column}' must be compared against a literal in sync row filters"
        ))),
        other => {
            if let Some(error) = sync_scope_filter_disallowed_operand(other) {
                Err(error)
            } else {
                Err(DbError::sql(
                    "sync row filter predicates must compare columns against literals",
                ))
            }
        }
    }
}

pub(crate) struct SyncContext {
    enabled: AtomicBool,
    replica_id: Mutex<Option<String>>,
    capture_enabled: AtomicBool,
    next_sequence: AtomicU64,
    journal_file: Mutex<Option<Arc<dyn VfsFile>>>,
    journal_write_offset: Mutex<u64>,
    journal_path: PathBuf,
    pub(crate) pending_mutations: Mutex<Vec<SyncMutation>>,
}

pub(crate) fn current_time_micros() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_micros() as i64)
        .unwrap_or(0)
}

pub(crate) struct SyncJournalCaptureScope<'a> {
    sync_ctx: &'a SyncContext,
    capture_enabled: bool,
}

impl<'a> Drop for SyncJournalCaptureScope<'a> {
    fn drop(&mut self) {
        self.sync_ctx
            .capture_enabled
            .store(self.capture_enabled, Ordering::Release);
    }
}

impl std::fmt::Debug for SyncContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncContext")
            .field("enabled", &self.enabled)
            .field("replica_id", &self.replica_id)
            .field("next_sequence", &self.next_sequence)
            .field("journal_path", &self.journal_path)
            .finish()
    }
}

impl SyncContext {
    pub(crate) fn new(db_path: &Path) -> Self {
        let journal_path = journal_path_for(db_path);
        Self {
            enabled: AtomicBool::new(false),
            replica_id: Mutex::new(None),
            capture_enabled: AtomicBool::new(true),
            next_sequence: AtomicU64::new(1),
            journal_file: Mutex::new(None),
            journal_write_offset: Mutex::new(0),
            journal_path,
            pending_mutations: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    pub(crate) fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Release);
        if !enabled {
            if let Ok(mut guard) = self.journal_file.lock() {
                *guard = None;
            }
            if let Ok(mut off) = self.journal_write_offset.lock() {
                *off = 0;
            }
        }
    }

    pub(crate) fn ensure_journal_open(&self, vfs: &VfsHandle) -> Result<()> {
        let mut guard = self
            .journal_file
            .lock()
            .map_err(|_| DbError::internal("sync journal lock poisoned"))?;
        if guard.is_some() {
            return Ok(());
        }
        let file = vfs.open(
            &self.journal_path,
            OpenMode::OpenOrCreate,
            FileKind::SyncJournal,
        )?;
        let file_size = file.file_size()?;
        if let Ok(mut off) = self.journal_write_offset.lock() {
            *off = file_size;
        }
        *guard = Some(file);
        Ok(())
    }

    pub(crate) fn set_replica_id(&self, replica_id: &str) {
        if let Ok(mut guard) = self.replica_id.lock() {
            *guard = Some(replica_id.to_string());
        }
    }

    pub(crate) fn suppress_capture(&self) -> SyncJournalCaptureScope<'_> {
        let capture_enabled = self.capture_enabled.swap(false, Ordering::AcqRel);
        SyncJournalCaptureScope {
            sync_ctx: self,
            capture_enabled,
        }
    }

    pub(crate) fn capture_enabled(&self) -> bool {
        self.capture_enabled.load(Ordering::Acquire)
    }

    pub(crate) fn replica_id(&self) -> Option<String> {
        self.replica_id.lock().ok()?.clone()
    }

    pub(crate) fn next_sequence(&self) -> u64 {
        self.next_sequence.load(Ordering::Acquire)
    }

    pub(crate) fn set_next_sequence(&self, seq: u64) {
        self.next_sequence.store(seq, Ordering::Release);
    }

    pub(crate) fn journal_path(&self) -> &Path {
        &self.journal_path
    }

    pub(crate) fn journal_size_bytes(&self) -> u64 {
        self.journal_write_offset.lock().map(|g| *g).unwrap_or(0)
    }

    pub(crate) fn journal_file_handle(&self) -> Result<Option<Arc<dyn VfsFile>>> {
        let guard = self
            .journal_file
            .lock()
            .map_err(|_| DbError::internal("sync journal lock poisoned"))?;
        Ok(guard.as_ref().cloned())
    }

    pub(crate) fn set_journal_write_offset(&self, offset: u64) -> Result<()> {
        *self
            .journal_write_offset
            .lock()
            .map_err(|_| DbError::internal("sync journal offset lock poisoned"))? = offset;
        Ok(())
    }

    pub(crate) fn flush_journal(&self, vfs: &VfsHandle, transaction_lsn: u64) -> Result<()> {
        if !self.is_enabled() || !self.capture_enabled() {
            return Ok(());
        }
        let replica_id = match self.replica_id() {
            Some(id) => id,
            None => return Ok(()),
        };
        let mut mutations: Vec<SyncMutation> = {
            let mut guard = self
                .pending_mutations
                .lock()
                .map_err(|_| DbError::internal("sync pending mutations lock poisoned"))?;
            std::mem::take(&mut *guard)
        };
        if mutations.is_empty() {
            return Ok(());
        }

        self.ensure_journal_open(vfs)?;
        let now_micros = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);

        let mut buffer = Vec::new();
        let mut seq = self.next_sequence();

        for mutation in &mut mutations {
            let record = SyncJournalRecord {
                schema_version: 1,
                sequence: seq,
                replica_id: replica_id.clone(),
                transaction_lsn,
                table: mutation.table.clone(),
                operation: mutation.operation.as_str().to_string(),
                primary_key: std::mem::take(&mut mutation.primary_key),
                after: std::mem::take(&mut mutation.after),
                schema_cookie: mutation.schema_cookie,
                committed_at_micros: now_micros,
            };
            serde_json::to_writer(&mut buffer, &record).map_err(|e| {
                DbError::internal(format!("failed to serialize sync journal record: {e}"))
            })?;
            buffer.push(b'\n');
            seq += 1;
        }

        let guard = self
            .journal_file
            .lock()
            .map_err(|_| DbError::internal("sync journal lock poisoned"))?;
        let journal_file = guard
            .as_ref()
            .ok_or_else(|| DbError::internal("sync journal file not open"))?;
        let mut offset = *self
            .journal_write_offset
            .lock()
            .map_err(|_| DbError::internal("sync journal offset lock poisoned"))?;

        vfs::write_all_at(journal_file.as_ref(), offset, &buffer)?;
        journal_file.sync_data()?;

        offset += buffer.len() as u64;
        *self
            .journal_write_offset
            .lock()
            .map_err(|_| DbError::internal("sync journal offset lock poisoned"))? = offset;
        self.set_next_sequence(seq);

        Ok(())
    }
}

fn journal_path_for(db_path: &Path) -> PathBuf {
    let mut name = db_path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "decentdb.sync-journal".to_string());
    name.push_str(".sync-journal");
    if let Some(parent) = db_path.parent() {
        parent.join(&name)
    } else {
        PathBuf::from(name)
    }
}

pub(crate) fn build_after_json(schema: &TableSchema, values: &[Value]) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (col, val) in schema.columns.iter().zip(values) {
        map.insert(col.name.clone(), value_to_json(val));
    }
    serde_json::Value::Object(map)
}

pub(crate) fn build_primary_key_json(schema: &TableSchema, values: &[Value]) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for pk_col in &schema.primary_key_columns {
        if let Some(pos) = schema.columns.iter().position(|c| &c.name == pk_col) {
            if let Some(val) = values.get(pos) {
                map.insert(pk_col.clone(), value_to_json(val));
            }
        }
    }
    serde_json::Value::Object(map)
}

fn value_to_json(val: &Value) -> serde_json::Value {
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Int64(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
        Value::Float64(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Blob(b) => {
            let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
            serde_json::Value::String(hex)
        }
        Value::Decimal { scaled, scale } => {
            serde_json::Value::String(decimal_to_json_text(*scaled, *scale))
        }
        Value::Uuid(u) => {
            let s = format!(
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                u[0], u[1], u[2], u[3], u[4], u[5], u[6], u[7],
                u[8], u[9], u[10], u[11], u[12], u[13], u[14], u[15]
            );
            serde_json::Value::String(s)
        }
        Value::TimestampMicros(ts) => serde_json::Value::Number(serde_json::Number::from(*ts)),
    }
}

fn decimal_to_json_text(scaled: i64, scale: u8) -> String {
    if scale == 0 {
        return scaled.to_string();
    }
    let negative = scaled < 0;
    let digits = scaled.unsigned_abs().to_string();
    let scale = usize::from(scale);
    let padded = if digits.len() <= scale {
        format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
    } else {
        digits
    };
    let split = padded.len() - scale;
    let mut decimal = format!("{}.{}", &padded[..split], &padded[split..]);
    if negative {
        decimal.insert(0, '-');
    }
    decimal
}

pub(crate) fn read_journal_records(
    journal_path: &Path,
    vfs: &VfsHandle,
    since_seq: u64,
    limit: usize,
) -> Result<Vec<SyncJournalRecord>> {
    if !vfs.file_exists(journal_path)? {
        return Ok(Vec::new());
    }
    let file = vfs.open(journal_path, OpenMode::OpenExisting, FileKind::SyncJournal)?;
    let file_size = file.file_size()?;
    if file_size == 0 {
        return Ok(Vec::new());
    }

    let mut buf = vec![0u8; file_size as usize];
    let mut offset: u64 = 0;
    while (offset as usize) < buf.len() {
        let read = file.read_at(offset, &mut buf[offset as usize..])?;
        if read == 0 {
            break;
        }
        offset += read as u64;
    }
    let content = String::from_utf8_lossy(&buf[..offset as usize]);

    let mut records = Vec::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let record: SyncJournalRecord = serde_json::from_str(trimmed).map_err(|e| {
            DbError::corruption(format!("failed to parse sync journal record: {e}"))
        })?;
        if record.sequence > since_seq {
            records.push(record);
        }
    }
    if records.len() > limit {
        records.truncate(limit);
    }
    Ok(records)
}

pub(crate) fn inspect_journal_integrity(
    journal_path: &Path,
    vfs: &VfsHandle,
    local_replica_id: Option<&str>,
) -> Result<SyncJournalIntegrityReport> {
    if !vfs.file_exists(journal_path)? {
        return Ok(SyncJournalIntegrityReport {
            total_records: 0,
            first_sequence: None,
            last_sequence: None,
            highest_severity: SyncDoctorSeverity::Info,
            issues: Vec::new(),
        });
    }

    let file = vfs.open(journal_path, OpenMode::OpenExisting, FileKind::SyncJournal)?;
    let file_size = file.file_size()?;
    if file_size == 0 {
        return Ok(SyncJournalIntegrityReport {
            total_records: 0,
            first_sequence: None,
            last_sequence: None,
            highest_severity: SyncDoctorSeverity::Info,
            issues: Vec::new(),
        });
    }

    let mut buf = vec![0u8; file_size as usize];
    let mut offset = 0;
    while (offset as usize) < buf.len() {
        let read = file.read_at(offset, &mut buf[offset as usize..])?;
        if read == 0 {
            break;
        }
        offset += read as u64;
    }
    let content = String::from_utf8_lossy(&buf[..offset as usize]);

    let mut issues = Vec::new();
    let mut seen = HashSet::new();
    let mut first_sequence: Option<u64> = None;
    let mut last_sequence: Option<u64> = None;
    let mut highest = SyncDoctorSeverity::Info;
    let mut total_records = 0usize;

    for (line_index, line) in content.lines().enumerate() {
        let line_number = line_index + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        total_records += 1;

        let record: SyncJournalRecord = match serde_json::from_str(trimmed) {
            Ok(record) => record,
            Err(error) => {
                highest = highest.max(SyncDoctorSeverity::Error);
                issues.push(SyncJournalIssue {
                    line_number,
                    sequence: None,
                    severity: SyncDoctorSeverity::Error,
                    code: "malformed_record".to_string(),
                    message: format!("malformed sync journal record: {error}"),
                });
                continue;
            }
        };

        let sequence = record.sequence;
        if sequence == 0 {
            highest = highest.max(SyncDoctorSeverity::Error);
            issues.push(SyncJournalIssue {
                line_number,
                sequence: Some(sequence),
                severity: SyncDoctorSeverity::Error,
                code: "invalid_sequence".to_string(),
                message: "sequence must be positive".to_string(),
            });
        }

        if first_sequence.is_none() && sequence > 1 {
            highest = highest.max(SyncDoctorSeverity::Error);
            issues.push(SyncJournalIssue {
                line_number,
                sequence: Some(sequence),
                severity: SyncDoctorSeverity::Error,
                code: "sequence_gap".to_string(),
                message: format!("expected first sequence to be 1, got {sequence}"),
            });
        }

        if let Some(previous) = last_sequence {
            if sequence <= previous {
                highest = highest.max(SyncDoctorSeverity::Error);
                issues.push(SyncJournalIssue {
                    line_number,
                    sequence: Some(sequence),
                    severity: SyncDoctorSeverity::Error,
                    code: "non_monotonic_sequence".to_string(),
                    message: "sequence values are not monotonic".to_string(),
                });
            }
            if sequence > previous + 1 {
                highest = highest.max(SyncDoctorSeverity::Error);
                issues.push(SyncJournalIssue {
                    line_number,
                    sequence: Some(sequence),
                    severity: SyncDoctorSeverity::Error,
                    code: "sequence_gap".to_string(),
                    message: format!("sequence gap after {previous}, expected {}", previous + 1),
                });
            }
        }

        if !seen.insert(sequence) {
            highest = highest.max(SyncDoctorSeverity::Error);
            issues.push(SyncJournalIssue {
                line_number,
                sequence: Some(sequence),
                severity: SyncDoctorSeverity::Error,
                code: "duplicate_sequence".to_string(),
                message: format!("duplicate sequence value {sequence}"),
            });
        }

        if let Some(local_replica_id) = local_replica_id {
            if record.replica_id != local_replica_id {
                highest = highest.max(SyncDoctorSeverity::Error);
                issues.push(SyncJournalIssue {
                    line_number,
                    sequence: Some(sequence),
                    severity: SyncDoctorSeverity::Error,
                    code: "replica_id_mismatch".to_string(),
                    message: format!(
                        "record replica '{}' does not match local replica '{}'",
                        record.replica_id, local_replica_id
                    ),
                });
            }
        }

        if record.schema_version != 1 {
            highest = highest.max(SyncDoctorSeverity::Error);
            issues.push(SyncJournalIssue {
                line_number,
                sequence: Some(sequence),
                severity: SyncDoctorSeverity::Error,
                code: "unsupported_schema_version".to_string(),
                message: format!("unsupported schema version {}", record.schema_version),
            });
        }

        if first_sequence.is_none() {
            first_sequence = Some(sequence);
        }
        last_sequence = Some(sequence);
    }

    Ok(SyncJournalIntegrityReport {
        total_records,
        first_sequence,
        last_sequence,
        highest_severity: highest,
        issues,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fs;
    use std::sync::{Mutex, OnceLock};

    use super::{
        SyncChangeBatch, SyncImportSummary, SyncJournalRecord, SyncRunDirection, SyncRunSummary,
    };
    use crate::Db;
    use crate::SyncDoctorSeverity;
    use crate::Value;

    fn sync_failpoint_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct FailpointReset;

    impl Drop for FailpointReset {
        fn drop(&mut self) {
            let _ = Db::clear_failpoints();
        }
    }

    fn temp_db() -> (tempfile::TempDir, Db) {
        let dir = tempfile::TempDir::with_prefix("decentdb-sync-test").unwrap();
        let path = dir.path().join("test.ddb");
        let db = Db::create(&path, crate::config::DbConfig::default()).unwrap();
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        (dir, db)
    }

    fn scoped_temp_db() -> (tempfile::TempDir, Db) {
        let dir = tempfile::TempDir::with_prefix("decentdb-sync-scope-test").unwrap();
        let path = dir.path().join("test.ddb");
        let db = Db::create(&path, crate::config::DbConfig::default()).unwrap();
        db.execute(
            "CREATE TABLE tenant_items (tenant_id INTEGER, id INTEGER, value TEXT, PRIMARY KEY (tenant_id, id))",
        )
        .unwrap();
        db.execute(
            "CREATE TABLE tenant_logs (tenant_id INTEGER, id INTEGER, value TEXT, PRIMARY KEY (tenant_id, id))",
        )
        .unwrap();
        db.sync_init_replica("node-a").unwrap();
        (dir, db)
    }

    fn users_sync_record(
        seed: &SyncJournalRecord,
        sequence: u64,
        operation: &str,
        primary_key: serde_json::Value,
        after: Option<serde_json::Value>,
    ) -> SyncJournalRecord {
        SyncJournalRecord {
            schema_version: seed.schema_version,
            sequence,
            replica_id: seed.replica_id.clone(),
            transaction_lsn: sequence,
            table: seed.table.clone(),
            operation: operation.to_string(),
            primary_key,
            after,
            schema_cookie: seed.schema_cookie,
            committed_at_micros: sequence as i64,
        }
    }

    #[test]
    fn sync_starts_disabled() {
        let (_dir, db) = temp_db();
        assert!(!db.sync_is_enabled().unwrap());
    }

    #[test]
    fn init_replica_enables_sync() {
        let (_dir, db) = temp_db();
        db.sync_init_replica("node-1").unwrap();
        assert!(db.sync_is_enabled().unwrap());
        let status = db.sync_status().unwrap();
        assert!(status.enabled);
        assert_eq!(status.replica_id.as_deref(), Some("node-1"));
        assert_eq!(status.next_sequence, 1);
    }

    #[test]
    fn insert_creates_journal_record() {
        let (_dir, db) = temp_db();
        db.sync_init_replica("node-1").unwrap();
        db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        let records = db.sync_pending_changes(0, 10).unwrap();
        assert_eq!(records.len(), 1);
        let r = &records[0];
        assert_eq!(r.schema_version, 1);
        assert_eq!(r.sequence, 1);
        assert_eq!(r.replica_id, "node-1");
        assert_eq!(r.table, "users");
        assert_eq!(r.operation, "insert");
        assert!(r.primary_key.is_object());
        assert!(r.after.is_some());
        let after = r.after.as_ref().unwrap();
        assert_eq!(after["id"], serde_json::json!(1));
        assert_eq!(after["name"], serde_json::json!("Alice"));
    }

    #[test]
    fn update_creates_journal_record() {
        let (_dir, db) = temp_db();
        db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        db.sync_init_replica("node-1").unwrap();
        db.execute("UPDATE users SET name = 'Bob' WHERE id = 1")
            .unwrap();
        let records = db.sync_pending_changes(0, 10).unwrap();
        assert_eq!(records.len(), 1);
        let r = &records[0];
        assert_eq!(r.operation, "update");
        assert_eq!(r.table, "users");
        let after = r.after.as_ref().unwrap();
        assert_eq!(after["name"], serde_json::json!("Bob"));
    }

    #[test]
    fn delete_creates_journal_record() {
        let (_dir, db) = temp_db();
        db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        db.sync_init_replica("node-1").unwrap();
        db.execute("DELETE FROM users WHERE id = 1").unwrap();
        let records = db.sync_pending_changes(0, 10).unwrap();
        assert_eq!(records.len(), 1);
        let r = &records[0];
        assert_eq!(r.operation, "delete");
        assert_eq!(r.table, "users");
        assert!(r.after.is_none());
        assert!(r.primary_key.is_object());
    }

    #[test]
    fn rollback_creates_no_records() {
        let (_dir, db) = temp_db();
        db.sync_init_replica("node-1").unwrap();
        db.begin_transaction().unwrap();
        db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        db.rollback_transaction().unwrap();
        let records = db.sync_pending_changes(0, 10).unwrap();
        assert_eq!(records.len(), 0);
    }

    #[test]
    fn savepoint_rollback_no_records() {
        let (_dir, db) = temp_db();
        db.sync_init_replica("node-1").unwrap();
        db.begin_transaction().unwrap();
        db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        db.create_savepoint("sp1").unwrap();
        db.execute("UPDATE users SET name = 'Bob' WHERE id = 1")
            .unwrap();
        db.rollback_to_savepoint("sp1").unwrap();
        db.commit_transaction().unwrap();
        let records = db.sync_pending_changes(0, 10).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].operation, "insert");
    }

    #[test]
    fn temp_table_mutations_ignored() {
        let (_dir, db) = temp_db();
        db.execute("CREATE TEMP TABLE tmp (x INTEGER)").unwrap();
        db.sync_init_replica("node-1").unwrap();
        db.execute("INSERT INTO tmp VALUES (1)").unwrap();
        let records = db.sync_pending_changes(0, 10).unwrap();
        assert_eq!(records.len(), 0);
    }

    #[test]
    fn sync_metadata_table_mutations_ignored() {
        let (_dir, db) = temp_db();
        db.sync_init_replica("node-1").unwrap();
        let records = db.sync_pending_changes(0, 10).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn reopen_preserves_metadata() {
        let dir = tempfile::TempDir::with_prefix("decentdb-sync-reopen").unwrap();
        let path = dir.path().join("test.ddb");
        {
            let db = Db::create(&path, crate::config::DbConfig::default()).unwrap();
            db.execute("CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT)")
                .unwrap();
            db.sync_init_replica("node-a").unwrap();
            db.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
        }
        {
            let db = Db::open(&path, crate::config::DbConfig::default()).unwrap();
            assert!(db.sync_is_enabled().unwrap());
            let status = db.sync_status().unwrap();
            assert_eq!(status.replica_id.as_deref(), Some("node-a"));
            assert!(status.enabled);
            let records = db.sync_pending_changes(0, 10).unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].replica_id, "node-a");
        }
    }

    #[test]
    fn reopened_db_records_new_changes_and_continues_sequence() {
        let dir = tempfile::TempDir::with_prefix("decentdb-sync-reopen-seq").unwrap();
        let path = dir.path().join("test.ddb");
        {
            let db = Db::create(&path, crate::config::DbConfig::default()).unwrap();
            db.execute("CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT)")
                .unwrap();
            db.sync_init_replica("node-a").unwrap();
            db.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
            assert_eq!(db.sync_status().unwrap().next_sequence, 2);
        }
        {
            let db = Db::open(&path, crate::config::DbConfig::default()).unwrap();
            assert_eq!(db.sync_status().unwrap().next_sequence, 2);
            db.execute("INSERT INTO t VALUES (2, 'y')").unwrap();
            let records = db.sync_pending_changes(0, 10).unwrap();
            assert_eq!(records.len(), 2);
            assert_eq!(records[0].sequence, 1);
            assert_eq!(records[1].sequence, 2);
            assert_eq!(db.sync_status().unwrap().next_sequence, 3);
        }
        {
            let db = Db::open(&path, crate::config::DbConfig::default()).unwrap();
            assert_eq!(db.sync_status().unwrap().next_sequence, 3);
        }
    }

    #[test]
    fn committed_sync_journal_records_survive_reopen_and_are_incrementally_enumerable() {
        let dir = tempfile::TempDir::with_prefix("decentdb-sync-journal-reopen").unwrap();
        let path = dir.path().join("test.ddb");
        {
            let db = Db::create(&path, crate::config::DbConfig::default()).unwrap();
            db.execute("CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT)")
                .unwrap();
            db.sync_init_replica("node-a").unwrap();
            db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
            db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
            db.execute("INSERT INTO t VALUES (3, 'c')").unwrap();

            let records = db.sync_pending_changes(0, 10).unwrap();
            assert_eq!(
                records
                    .iter()
                    .map(|record| record.sequence)
                    .collect::<Vec<_>>(),
                vec![1, 2, 3]
            );
        }
        {
            let db = Db::open(&path, crate::config::DbConfig::default()).unwrap();
            assert_eq!(db.sync_status().unwrap().next_sequence, 4);

            let first = db.sync_pending_changes(0, 1).unwrap();
            assert_eq!(first.len(), 1);
            assert_eq!(first[0].sequence, 1);

            let second = db.sync_pending_changes(1, 1).unwrap();
            assert_eq!(second.len(), 1);
            assert_eq!(second[0].sequence, 2);

            let third = db.sync_pending_changes(2, 10).unwrap();
            assert_eq!(
                third
                    .iter()
                    .map(|record| record.sequence)
                    .collect::<Vec<_>>(),
                vec![3]
            );
        }
    }

    fn sync_journal_failure_surfaces_error(label: &str, expect_empty_after_reopen: bool) {
        let _guard = sync_failpoint_lock().lock().unwrap();
        let _reset = FailpointReset;
        Db::clear_failpoints().unwrap();

        let dir = tempfile::TempDir::with_prefix("decentdb-sync-failure").unwrap();
        let path = dir.path().join("test.ddb");
        let db = Db::create(&path, crate::config::DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        db.sync_init_replica("node-a").unwrap();

        Db::install_failpoint(label, "error", 1, 0).unwrap();
        let err = db
            .execute("INSERT INTO t VALUES (1, 'a')")
            .expect_err("sync journal failure must surface to the caller");
        assert!(
            err.to_string()
                .contains(&format!("fault injected at {label}")),
            "unexpected error: {err}"
        );

        Db::clear_failpoints().unwrap();
        drop(db);

        let reopened = Db::open(&path, crate::config::DbConfig::default()).unwrap();
        if expect_empty_after_reopen {
            assert!(
                reopened.sync_pending_changes(0, 10).unwrap().is_empty(),
                "write failure should not leave a committed sync journal record"
            );
        }
    }

    #[test]
    fn sync_journal_write_failure_is_returned_to_caller() {
        sync_journal_failure_surfaces_error("sync.write", true);
    }

    #[test]
    fn sync_journal_fsync_failure_is_returned_to_caller() {
        sync_journal_failure_surfaces_error("sync.fsync", false);
    }

    #[test]
    fn pending_changes_respects_since_and_limit() {
        let (_dir, db) = temp_db();
        db.sync_init_replica("node-1").unwrap();
        for i in 1..=5 {
            db.execute(&format!("INSERT INTO users VALUES ({i}, 'user{i}')"))
                .unwrap();
        }
        let all = db.sync_pending_changes(0, 10).unwrap();
        assert_eq!(all.len(), 5);
        let from3 = db.sync_pending_changes(3, 10).unwrap();
        assert_eq!(from3.len(), 2);
        let limited = db.sync_pending_changes(0, 2).unwrap();
        assert_eq!(limited.len(), 2);
    }

    #[test]
    fn enable_disable_sync() {
        let (_dir, db) = temp_db();
        assert!(!db.sync_is_enabled().unwrap());
        db.sync_set_enabled(true).unwrap();
        assert!(db.sync_is_enabled().unwrap());
        db.sync_set_enabled(false).unwrap();
        assert!(!db.sync_is_enabled().unwrap());
    }

    #[test]
    fn list_tables_filters_internal() {
        let (_dir, db) = temp_db();
        db.sync_init_replica("node-1").unwrap();
        let tables = db.list_tables().unwrap();
        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"users"));
        assert!(!names.contains(&"__decentdb_sync_metadata"));
    }

    #[test]
    fn multiple_inserts_in_transaction() {
        let (_dir, db) = temp_db();
        db.sync_init_replica("node-1").unwrap();
        db.begin_transaction().unwrap();
        db.execute("INSERT INTO users VALUES (1, 'a')").unwrap();
        db.execute("INSERT INTO users VALUES (2, 'b')").unwrap();
        db.commit_transaction().unwrap();
        let records = db.sync_pending_changes(0, 10).unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn sync_disabled_no_journal_writes() {
        let (_dir, db) = temp_db();
        db.execute("INSERT INTO users VALUES (1, 'x')").unwrap();
        let status = db.sync_status().unwrap();
        assert!(!status.enabled);
        assert_eq!(status.journal_size_bytes, 0);
    }

    #[test]
    fn journal_sequence_monotonic() {
        let (_dir, db) = temp_db();
        db.sync_init_replica("node-1").unwrap();
        for i in 1..=10 {
            db.execute(&format!("INSERT INTO users VALUES ({i}, 'user{i}')"))
                .unwrap();
        }
        let records = db.sync_pending_changes(0, 20).unwrap();
        assert_eq!(records.len(), 10);
        for (i, r) in records.iter().enumerate() {
            assert_eq!(r.sequence, (i + 1) as u64);
        }
    }

    fn sync_journal_lines() -> Vec<String> {
        vec![
            r#"{"schema_version":1,"sequence":1,"replica_id":"node-a","transaction_lsn":1,"table":"users","operation":"insert","primary_key":{"id":1},"after":{"id":1,"name":"Alice"},"schema_cookie":1,"committed_at_micros":0}"#.to_string(),
            r#"{"schema_version":1,"sequence":3,"replica_id":"node-a","transaction_lsn":1,"table":"users","operation":"insert","primary_key":{"id":1},"after":{"id":1,"name":"Alice"},"schema_cookie":1,"committed_at_micros":0}"#.to_string(),
            r#"{"schema_version":1,"sequence":3,"replica_id":"node-a","transaction_lsn":1,"table":"users","operation":"insert","primary_key":{"id":1},"after":{"id":1,"name":"Alice"},"schema_cookie":1,"committed_at_micros":0}"#.to_string(),
            r#"{"schema_version":1,"sequence":2,"replica_id":"node-b","transaction_lsn":1,"table":"users","operation":"insert","primary_key":{"id":1},"after":{"id":1,"name":"Alice"},"schema_cookie":1,"committed_at_micros":0}"#.to_string(),
            r#"{"schema_version":2,"sequence":4,"replica_id":"node-a","transaction_lsn":1,"table":"users","operation":"insert","primary_key":{"id":1},"after":{"id":1,"name":"Alice"},"schema_cookie":1,"committed_at_micros":0}"#.to_string(),
        ]
    }

    #[test]
    fn sync_import_roundtrip_apply_insert_update_delete() {
        let (_dir_a, db_a) = temp_db();
        db_a.sync_init_replica("node-a").unwrap();
        db_a.execute("INSERT INTO users VALUES (1, 'alice')")
            .unwrap();
        db_a.execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
        db_a.execute("UPDATE users SET name = 'alice v2' WHERE id = 1")
            .unwrap();
        db_a.execute("DELETE FROM users WHERE id = 2").unwrap();

        let records = db_a.sync_pending_changes(0, 100).unwrap();
        let (_dir_b, db_b) = temp_db();
        db_b.sync_init_replica("node-b").unwrap();
        let summary = db_b.sync_import_records(&records).unwrap();
        assert_eq!(summary.seen, 4);
        assert_eq!(summary.applied, 4);
        assert_eq!(summary.skipped, 0);
        assert_eq!(summary.conflicted, 0);

        let rows = db_b
            .execute("SELECT id, name FROM users ORDER BY id")
            .unwrap();
        assert_eq!(rows.rows().len(), 1);
        assert_eq!(
            rows.rows()[0].values(),
            &[Value::Int64(1), Value::Text("alice v2".to_string())]
        );
        assert_eq!(db_b.sync_pending_changes(0, 100).unwrap().len(), 0);
    }

    #[test]
    fn sync_import_roundtrip_preserves_decimal_payloads() {
        let (_dir_a, db_a) = temp_db();
        db_a.execute("CREATE TABLE money (id INTEGER PRIMARY KEY, amount DECIMAL(10, 3))")
            .unwrap();
        db_a.sync_init_replica("node-a").unwrap();
        db_a.execute_with_params(
            "INSERT INTO money VALUES (1, $1)",
            &[Value::Decimal {
                scaled: 1999,
                scale: 2,
            }],
        )
        .unwrap();
        let records = db_a.sync_pending_changes(0, 100).unwrap();

        let (_dir_b, db_b) = temp_db();
        db_b.execute("CREATE TABLE money (id INTEGER PRIMARY KEY, amount DECIMAL(10, 3))")
            .unwrap();
        db_b.sync_init_replica("node-b").unwrap();
        let summary = db_b.sync_import_records(&records).unwrap();
        assert_eq!(summary.applied, 1);
        assert_eq!(summary.conflicted, 0);

        let rows = db_b
            .execute("SELECT amount FROM money WHERE id = 1")
            .unwrap();
        assert_eq!(
            rows.rows()[0].values()[0],
            Value::Decimal {
                scaled: 1999,
                scale: 2
            }
        );
    }

    #[test]
    fn sync_import_reimport_skips_already_applied_records() {
        let (_dir_a, db_a) = temp_db();
        db_a.sync_init_replica("node-a").unwrap();
        db_a.execute("INSERT INTO users VALUES (1, 'alice')")
            .unwrap();
        let records = db_a.sync_pending_changes(0, 100).unwrap();

        let (_dir_b, db_b) = temp_db();
        db_b.sync_init_replica("node-b").unwrap();
        assert!(db_b.sync_import_records(&records).unwrap().applied == 1);
        let reimported = db_b.sync_import_records(&records).unwrap();
        assert_eq!(reimported.seen, 1);
        assert_eq!(reimported.applied, 0);
        assert_eq!(reimported.skipped, 1);
        assert_eq!(reimported.conflicted, 0);
        let rows = db_b
            .execute("SELECT id, name FROM users ORDER BY id")
            .unwrap();
        assert_eq!(
            rows.rows()[0].values(),
            &[Value::Int64(1), Value::Text("alice".to_string())]
        );
    }

    #[test]
    fn sync_export_batch_has_deterministic_identity_and_validates_ranges() {
        let (_dir, db) = temp_db();
        db.sync_init_replica("node-a").unwrap();
        db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        db.execute("INSERT INTO users VALUES (2, 'Bob')").unwrap();

        let batch = db.sync_export_batch(0, 2).unwrap();
        assert_eq!(batch.protocol_version, 1);
        assert_eq!(batch.batch_id, "sync-batch:v1:node-a:1:2:2");
        assert_eq!(batch.source_replica_id.as_deref(), Some("node-a"));
        assert_eq!(batch.first_sequence, Some(1));
        assert_eq!(batch.last_sequence, Some(2));
        assert_eq!(batch.record_count, 2);
        batch.validate().unwrap();

        let mut bad_count = batch.clone();
        bad_count.record_count = 3;
        assert!(bad_count.validate().is_err());

        let mut bad_source = batch.clone();
        bad_source.source_replica_id = Some("node-b".to_string());
        assert!(bad_source.validate().is_err());

        let mut bad_range = batch.clone();
        bad_range.first_sequence = Some(2);
        assert!(bad_range.validate().is_err());
    }

    #[test]
    fn sync_batches_exchange_bidirectionally_without_echo_and_advance_watermarks() {
        let (_dir_a, db_a) = temp_db();
        let (_dir_b, db_b) = temp_db();
        db_a.sync_init_replica("node-a").unwrap();
        db_b.sync_init_replica("node-b").unwrap();

        db_a.execute("INSERT INTO users VALUES (1, 'alice')")
            .unwrap();
        let batch_a = db_a.sync_export_batch(0, 100).unwrap();
        assert_eq!(batch_a.record_count, 1);
        assert_eq!(batch_a.records[0].primary_key, serde_json::json!({"id": 1}));

        let imported_a = db_b.sync_import_batch(&batch_a).unwrap();
        assert_eq!(imported_a.applied, 1);
        assert_eq!(imported_a.skipped, 0);
        assert_eq!(imported_a.conflicted, 0);
        assert_eq!(db_b.sync_peer_watermark("node-a").unwrap(), Some(1));

        db_b.execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
        let batch_b = db_b.sync_export_batch(0, 100).unwrap();
        assert_eq!(batch_b.record_count, 1);
        assert_eq!(batch_b.source_replica_id.as_deref(), Some("node-b"));
        assert_eq!(batch_b.records[0].primary_key, serde_json::json!({"id": 2}));

        let imported_b = db_a.sync_import_batch(&batch_b).unwrap();
        assert_eq!(imported_b.applied, 1);
        assert_eq!(imported_b.skipped, 0);
        assert_eq!(imported_b.conflicted, 0);
        assert_eq!(db_a.sync_peer_watermark("node-b").unwrap(), Some(1));

        let replay_a = db_b.sync_import_batch(&batch_a).unwrap();
        assert_eq!(replay_a.seen, 1);
        assert_eq!(replay_a.applied, 0);
        assert_eq!(replay_a.skipped, 1);
        assert_eq!(replay_a.conflicted, 0);

        let replay_b = db_a.sync_import_batch(&batch_b).unwrap();
        assert_eq!(replay_b.seen, 1);
        assert_eq!(replay_b.applied, 0);
        assert_eq!(replay_b.skipped, 1);
        assert_eq!(replay_b.conflicted, 0);
    }

    #[test]
    fn sync_import_conflicting_insert_records_visible_conflict() {
        let (_dir_a, db_a) = temp_db();
        let (_dir_b, db_b) = temp_db();
        db_a.sync_init_replica("node-a").unwrap();
        db_b.sync_init_replica("node-b").unwrap();

        db_a.execute("INSERT INTO users VALUES (1, 'alice')")
            .unwrap();
        db_b.execute("INSERT INTO users VALUES (1, 'existing')")
            .unwrap();

        let batch = db_a.sync_export_batch(0, 10).unwrap();
        let summary = db_b.sync_import_batch(&batch).unwrap();
        assert_eq!(summary.seen, 1);
        assert_eq!(summary.applied, 0);
        assert_eq!(summary.skipped, 0);
        assert_eq!(summary.conflicted, 1);

        let conflicts = db_b.sync_conflicts().unwrap();
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].conflict_type, "constraint_error");
        assert!(!conflicts[0].message.is_empty());
    }

    #[test]
    fn sync_import_missing_target_records_conflict() {
        let (_dir_src, db_src) = temp_db();
        db_src.sync_init_replica("node-a").unwrap();
        db_src
            .execute("INSERT INTO users VALUES (99, 'seed')")
            .unwrap();
        let seed_record = db_src.sync_export_batch(0, 1).unwrap().records[0].clone();

        let (_dir, db) = temp_db();
        db.sync_init_replica("node-b").unwrap();

        let batch = SyncChangeBatch {
            protocol_version: 1,
            batch_id: "sync-batch:v1:node-a:1:2:2".to_string(),
            source_replica_id: Some("node-a".to_string()),
            first_sequence: Some(1),
            last_sequence: Some(2),
            source_high_watermark: None,
            record_count: 2,
            records: vec![
                users_sync_record(
                    &seed_record,
                    1,
                    "update",
                    serde_json::json!({"id": 42}),
                    Some(serde_json::json!({"id": 42, "name": "ghost"})),
                ),
                users_sync_record(
                    &seed_record,
                    2,
                    "delete",
                    serde_json::json!({"id": 43}),
                    None,
                ),
            ],
        };
        batch.validate().unwrap();

        let summary = db.sync_import_batch(&batch).unwrap();
        assert_eq!(summary.seen, 2);
        assert_eq!(summary.applied, 0);
        assert_eq!(summary.skipped, 0);
        assert_eq!(summary.conflicted, 2);

        let conflicts = db.sync_conflicts().unwrap();
        assert_eq!(conflicts.len(), 2);
        assert!(conflicts
            .iter()
            .all(|conflict| conflict.conflict_type == "missing_target"));
    }

    #[test]
    fn sync_prune_refuses_when_peer_watermark_is_too_low_and_succeeds_after_advancing() {
        let (_dir_src, db_src) = temp_db();
        let (_dir_dst, db_dst) = temp_db();
        db_src.sync_init_replica("node-a").unwrap();
        db_dst.sync_init_replica("node-b").unwrap();

        for id in 1..=3 {
            db_src
                .execute(&format!("INSERT INTO users VALUES ({id}, 'src{id}')"))
                .unwrap();
        }
        for id in 100..=102 {
            db_dst
                .execute(&format!("INSERT INTO users VALUES ({id}, 'dst{id}')"))
                .unwrap();
        }

        let first_batch = db_src.sync_export_batch(0, 2).unwrap();
        assert_eq!(first_batch.record_count, 2);
        db_dst.sync_import_batch(&first_batch).unwrap();
        assert_eq!(db_dst.sync_peer_watermark("node-a").unwrap(), Some(2));

        let err = db_dst
            .sync_prune_journal_through(2)
            .expect_err("pruning through the watermark must be rejected");
        assert!(err.to_string().contains("lowest peer watermark is 2"));

        let second_batch = db_src.sync_export_batch(2, 1).unwrap();
        assert_eq!(second_batch.record_count, 1);
        db_dst.sync_import_batch(&second_batch).unwrap();
        assert_eq!(db_dst.sync_peer_watermark("node-a").unwrap(), Some(3));

        let pruned = db_dst.sync_prune_journal_through(2).unwrap();
        assert_eq!(pruned, 2);

        let remaining = db_dst.sync_pending_changes(0, 10).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].sequence, 3);
    }

    #[test]
    fn sync_import_rejects_local_replica_records() {
        let (_dir_a, db_a) = temp_db();
        db_a.sync_init_replica("node-a").unwrap();
        db_a.execute("INSERT INTO users VALUES (1, 'alice')")
            .unwrap();
        let records = db_a.sync_pending_changes(0, 100).unwrap();

        let (_dir_b, db_b) = temp_db();
        db_b.sync_init_replica("node-a").unwrap();
        let err = db_b
            .sync_import_records(&records)
            .expect_err("same-replica records must be rejected");
        assert!(err
            .to_string()
            .contains("cannot import batch from same replica"));
    }

    #[test]
    fn sync_integrity_report_passes_for_normal_journal() {
        let (_dir, db) = temp_db();
        db.sync_init_replica("node-a").unwrap();
        db.execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        db.execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
        let report = db.sync_integrity_report().unwrap();
        assert_eq!(report.total_records, 2);
        assert_eq!(report.first_sequence, Some(1));
        assert_eq!(report.last_sequence, Some(2));
        assert!(report.issues.is_empty());
        assert_eq!(report.highest_severity, SyncDoctorSeverity::Info);
    }

    #[test]
    fn sync_integrity_report_detects_corruption_and_gaps() {
        let (_dir, db) = temp_db();
        db.sync_init_replica("node-a").unwrap();
        let status = db.sync_status().unwrap();
        let journal_path = status.journal_path.expect("journal path");
        let lines = sync_journal_lines();
        let mut content = lines.join("\n");
        content.push('\n');
        content.push_str("not-json");
        fs::write(&journal_path, content).unwrap();

        let report = db.sync_integrity_report().unwrap();
        assert_eq!(report.total_records, 6);
        let codes: HashSet<_> = report
            .issues
            .iter()
            .map(|issue| issue.code.as_str())
            .collect();
        assert!(codes.contains("malformed_record"));
        assert!(codes.contains("sequence_gap"));
        assert!(codes.contains("duplicate_sequence"));
        assert!(codes.contains("non_monotonic_sequence"));
        assert!(codes.contains("replica_id_mismatch"));
        assert!(codes.contains("unsupported_schema_version"));
        assert_eq!(report.highest_severity, SyncDoctorSeverity::Error);
    }

    #[test]
    fn sync_peers_persist_update_remove_and_internal_tables_stay_hidden() {
        let (_dir, db) = temp_db();

        db.sync_add_peer("beta", "https://beta.example.com", Some("BETA_TOKEN"))
            .unwrap();
        db.sync_add_peer("alpha", "https://alpha.example.com", None)
            .unwrap();

        let tables = db.list_tables().unwrap();
        assert!(tables
            .iter()
            .all(|table| !table.name.starts_with("__decentdb_sync_")));

        let peers = db.sync_peers().unwrap();
        assert_eq!(
            peers
                .iter()
                .map(|peer| peer.name.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "beta"]
        );
        assert_eq!(peers[1].token_env.as_deref(), Some("BETA_TOKEN"));

        let created_at = peers[1].created_at_micros;
        db.sync_add_peer("beta", "https://beta2.example.com", Some("BETA_TOKEN_2"))
            .unwrap();
        let updated = db.sync_peer("beta").unwrap().expect("peer");
        assert_eq!(updated.created_at_micros, created_at);
        assert_eq!(updated.endpoint, "https://beta2.example.com");
        assert_eq!(updated.token_env.as_deref(), Some("BETA_TOKEN_2"));

        let view = db
            .execute("SELECT * FROM sys_sync_peers ORDER BY name")
            .unwrap();
        assert_eq!(
            view.columns(),
            &[
                "name".to_string(),
                "endpoint".to_string(),
                "token_env".to_string(),
                "created_at_micros".to_string(),
                "updated_at_micros".to_string(),
            ]
        );
        assert_eq!(view.rows().len(), 2);

        assert!(db.sync_remove_peer("alpha").unwrap());
        assert!(!db.sync_remove_peer("alpha").unwrap());
        let remaining = db.sync_peers().unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].name, "beta");
    }

    #[test]
    fn sync_scopes_round_trip_and_views_stay_deterministic() {
        let (_dir, db) = temp_db();

        db.sync_create_scope("tenant_1", &["users"], None).unwrap();
        db.sync_create_scope("tenant_2", &["users"], Some("id >= 10"))
            .unwrap();

        let scope = db.sync_scope("tenant_1").unwrap().expect("scope");
        assert_eq!(scope.name, "tenant_1");
        assert_eq!(scope.include_tables, vec!["users".to_string()]);
        assert!(scope.row_filter.is_none());
        assert_eq!(scope.filter_columns, Vec::<String>::new());

        let scopes = db.sync_scopes().unwrap();
        assert_eq!(
            scopes
                .iter()
                .map(|scope| scope.name.as_str())
                .collect::<Vec<_>>(),
            vec!["tenant_1", "tenant_2"]
        );

        let scopes_view = db
            .execute("SELECT * FROM sys_sync_scopes ORDER BY name")
            .unwrap();
        assert_eq!(
            scopes_view.columns(),
            &[
                "name",
                "include_tables_json",
                "row_filter",
                "filter_columns_json",
                "created_at_micros",
                "updated_at_micros",
            ]
        );
        assert_eq!(scopes_view.rows().len(), 2);

        let scope_tables_view = db
            .execute("SELECT * FROM sys_sync_scope_tables ORDER BY scope_name, table_name")
            .unwrap();
        assert_eq!(scope_tables_view.columns(), &["scope_name", "table_name"]);
        assert_eq!(scope_tables_view.rows().len(), 2);
        assert_eq!(
            scope_tables_view.rows()[0].values(),
            &[
                Value::Text("tenant_1".to_string()),
                Value::Text("users".to_string()),
            ]
        );

        assert!(db.sync_drop_scope("tenant_2").unwrap());
        assert!(!db.sync_drop_scope("tenant_2").unwrap());
        assert_eq!(db.sync_scopes().unwrap().len(), 1);
    }

    #[test]
    fn sync_scope_validation_rejects_invalid_definitions() {
        let (_dir, db) = temp_db();
        db.execute("CREATE TABLE no_pk (id INTEGER, value TEXT)")
            .unwrap();
        db.execute(
            "CREATE TABLE tenant_docs (tenant_id INTEGER, id INTEGER PRIMARY KEY, value TEXT)",
        )
        .unwrap();
        db.execute(
            "CREATE TABLE tenant_items (tenant_id INTEGER, id INTEGER, value TEXT, PRIMARY KEY (tenant_id, id))",
        )
        .unwrap();

        let missing_table = db
            .sync_create_scope("missing", &["does_not_exist"], None)
            .expect_err("missing table should fail");
        assert!(missing_table.to_string().contains("does_not_exist"));

        let duplicate = db
            .sync_create_scope("duplicate", &["users", "users"], None)
            .expect_err("duplicate include should fail");
        assert!(duplicate.to_string().contains("duplicate"));

        let internal = db
            .sync_create_scope("internal", &["__decentdb_sync_metadata"], None)
            .expect_err("internal table should fail");
        assert!(internal.to_string().contains("internal table"));

        let no_pk = db
            .sync_create_scope("no_pk", &["no_pk"], None)
            .expect_err("table without pk should fail");
        assert!(no_pk.to_string().contains("stable primary key"));

        let missing_column = db
            .sync_create_scope("missing_column", &["users"], Some("tenant_id = 1"))
            .expect_err("missing filter column should fail");
        assert!(missing_column.to_string().contains("tenant_id"));

        let not_in_pk = db
            .sync_create_scope("not_in_pk", &["tenant_docs"], Some("tenant_id = 1"))
            .expect_err("filter column not in pk should fail");
        assert!(not_in_pk.to_string().contains("primary key"));

        for (name, filter, needle) in [
            ("or", Some("id = 1 OR id = 2"), "OR"),
            (
                "subquery",
                Some("id IN (SELECT id FROM users)"),
                "subqueries",
            ),
            ("function", Some("random() = 1"), "functions"),
            ("dotted", Some("users.id = 1"), "dotted"),
            ("decimal_literal", Some("id = 1.5"), "literals"),
        ] {
            let err = db
                .sync_create_scope(name, &["users"], filter)
                .expect_err("invalid filter should fail");
            assert!(err.to_string().contains(needle), "{err}");
        }

        db.sync_create_scope("pk_filter", &["tenant_items"], Some("tenant_id = 1"))
            .unwrap();
        let pk_filter = db.sync_scope("pk_filter").unwrap().unwrap();
        assert_eq!(pk_filter.filter_columns, vec!["tenant_id".to_string()]);
    }

    #[test]
    fn sync_scoped_export_import_and_peer_binding_round_trip() {
        let (_dir_src, src) = scoped_temp_db();
        let (_dir_dst, dst) = scoped_temp_db();
        dst.sync_init_replica("node-b").unwrap();

        src.execute("INSERT INTO tenant_items VALUES (1, 10, 't1-a')")
            .unwrap();
        src.execute("INSERT INTO tenant_items VALUES (2, 20, 't2-a')")
            .unwrap();
        src.execute("UPDATE tenant_items SET value = 't1-b' WHERE tenant_id = 1 AND id = 10")
            .unwrap();
        src.execute("DELETE FROM tenant_items WHERE tenant_id = 2 AND id = 20")
            .unwrap();
        src.execute("INSERT INTO tenant_logs VALUES (1, 1, 'log-1')")
            .unwrap();
        src.execute("INSERT INTO tenant_logs VALUES (2, 2, 'log-2')")
            .unwrap();

        src.sync_create_scope(
            "tenant_1",
            &["tenant_items", "tenant_logs"],
            Some("tenant_id = 1"),
        )
        .unwrap();
        dst.sync_create_scope(
            "tenant_1",
            &["tenant_items", "tenant_logs"],
            Some("tenant_id = 1"),
        )
        .unwrap();

        let scoped_batch = src.sync_export_batch_for_scope("tenant_1", 0, 100).unwrap();
        assert_eq!(scoped_batch.records.len(), 3);
        assert_eq!(scoped_batch.last_sequence, Some(5));
        assert_eq!(scoped_batch.source_high_watermark, Some(6));
        assert!(scoped_batch.records.iter().all(|record| {
            record
                .primary_key
                .get("tenant_id")
                .is_some_and(|value| value == &serde_json::json!(1))
        }));

        let err = dst
            .sync_import_batch_for_scope("tenant_1", &src.sync_export_batch(0, 100).unwrap())
            .expect_err("out of scope records must be rejected");
        assert!(err.to_string().contains("does not match scope"));

        let summary = dst
            .sync_import_batch_for_scope("tenant_1", &scoped_batch)
            .unwrap();
        assert_eq!(summary.applied, 3);
        assert_eq!(dst.sync_peer_watermark("node-a").unwrap(), Some(6));

        let rows = dst
            .execute("SELECT tenant_id, id, value FROM tenant_items ORDER BY tenant_id, id")
            .unwrap();
        assert_eq!(rows.rows().len(), 1);
        assert_eq!(
            rows.rows()[0].values(),
            &[
                Value::Int64(1),
                Value::Int64(10),
                Value::Text("t1-b".to_string()),
            ]
        );

        dst.sync_add_peer("relay", "https://relay.example.com", None)
            .unwrap();
        dst.sync_bind_peer_scope("relay", "tenant_1").unwrap();
        let binding = dst.sync_peer_scope("relay").unwrap().expect("binding");
        assert_eq!(binding.scope_name, "tenant_1");
        let definition = dst
            .sync_peer_scope_definition("relay")
            .unwrap()
            .expect("definition");
        assert_eq!(definition.name, "tenant_1");
        assert!(dst.sync_unbind_peer_scope("relay").unwrap());
        assert!(dst.sync_peer_scope("relay").unwrap().is_none());
        assert!(dst.sync_peer_scope_definition("relay").unwrap().is_none());
    }

    #[test]
    fn sync_sql_inspection_views_cover_scopes_tables_and_bindings() {
        let (_dir, db) = temp_db();
        db.sync_add_peer("relay", "https://relay.example.com", None)
            .unwrap();
        db.sync_create_scope("tenant_1", &["users"], Some("id = 1"))
            .unwrap();
        db.sync_bind_peer_scope("relay", "tenant_1").unwrap();

        let peer_scopes = db
            .execute("SELECT * FROM sys_sync_peer_scopes ORDER BY peer_name")
            .unwrap();
        assert_eq!(
            peer_scopes.columns(),
            &[
                "peer_name",
                "scope_name",
                "created_at_micros",
                "updated_at_micros",
            ]
        );
        assert_eq!(peer_scopes.rows().len(), 1);
        assert_eq!(
            peer_scopes.rows()[0].values(),
            &[
                Value::Text("relay".to_string()),
                Value::Text("tenant_1".to_string()),
                peer_scopes.rows()[0].values()[2].clone(),
                peer_scopes.rows()[0].values()[3].clone(),
            ]
        );
    }

    #[test]
    fn sync_session_helpers_record_success_failure_and_views_expose_columns() {
        let (_dir, db) = temp_db();
        db.sync_init_replica("node-a").unwrap();

        let success_id = db
            .sync_start_session("peer-a", SyncRunDirection::Both, Some("remote-a"))
            .unwrap();
        let success_summary = SyncRunSummary {
            peer_name: "peer-a".to_string(),
            direction: SyncRunDirection::Both,
            remote_replica_id: Some("remote-a".to_string()),
            pushed: Some(SyncImportSummary {
                seen: 2,
                applied: 1,
                skipped: 1,
                conflicted: 0,
            }),
            pulled: Some(SyncImportSummary {
                seen: 1,
                applied: 1,
                skipped: 0,
                conflicted: 0,
            }),
            pushed_batch_id: Some("push-batch".to_string()),
            pulled_batch_id: Some("pull-batch".to_string()),
            retry_count: 0,
        };
        db.sync_finish_session_success(success_id, &success_summary)
            .unwrap();

        let failed_id = db
            .sync_start_session("peer-b", SyncRunDirection::Pull, Some("remote-b"))
            .unwrap();
        let failed_summary = SyncRunSummary {
            peer_name: "peer-b".to_string(),
            direction: SyncRunDirection::Pull,
            remote_replica_id: Some("remote-b".to_string()),
            pushed: None,
            pulled: Some(SyncImportSummary {
                seen: 3,
                applied: 2,
                skipped: 1,
                conflicted: 0,
            }),
            pushed_batch_id: None,
            pulled_batch_id: Some("pull-batch-2".to_string()),
            retry_count: 0,
        };
        db.sync_finish_session_failed(failed_id, &failed_summary, "boom")
            .unwrap();

        let sessions = db.sync_sessions().unwrap();
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].status, "success");
        assert_eq!(sessions[0].pushed_batch_id.as_deref(), Some("push-batch"));
        assert_eq!(sessions[1].status, "failed");
        assert_eq!(sessions[1].error.as_deref(), Some("boom"));

        let view = db
            .execute("SELECT * FROM sys_sync_sessions ORDER BY session_id")
            .unwrap();
        assert_eq!(
            view.columns(),
            &[
                "session_id".to_string(),
                "peer_name".to_string(),
                "direction".to_string(),
                "remote_replica_id".to_string(),
                "started_at_micros".to_string(),
                "ended_at_micros".to_string(),
                "status".to_string(),
                "error".to_string(),
                "pushed_batch_id".to_string(),
                "pulled_batch_id".to_string(),
                "pushed_seen".to_string(),
                "pushed_applied".to_string(),
                "pushed_skipped".to_string(),
                "pushed_conflicted".to_string(),
                "pulled_seen".to_string(),
                "pulled_applied".to_string(),
                "pulled_skipped".to_string(),
                "pulled_conflicted".to_string(),
                "retry_count".to_string(),
            ]
        );
        assert_eq!(view.rows().len(), 2);
    }
}

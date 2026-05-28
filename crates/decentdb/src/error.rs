//! Structured engine error taxonomy.

use std::collections::BTreeMap;
use std::path::Path;

use serde::Serialize;
use serde::Serializer;
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const DIAGNOSTIC_VERSION: u16 = 1;
const MAX_DETAIL_KEYS: usize = 9;
const MAX_STRING_DETAIL_LEN: usize = 64;
const REDACTED_VALUE: &str = "<redacted>";

#[allow(dead_code)]
const _PHASE1_SUBCODES: &[&str] = &[
    SUBCODE_SQL_UNKNOWN,
    SUBCODE_SQL_SYNTAX,
    SUBCODE_SQL_RELATION_NOT_FOUND,
    SUBCODE_SQL_COLUMN_NOT_FOUND,
    SUBCODE_SQL_AMBIGUOUS_COLUMN,
    SUBCODE_SQL_PARAMETER_MISSING,
    SUBCODE_SQL_PARAMETER_TYPE_MISMATCH,
    SUBCODE_SQL_UNSUPPORTED_FEATURE,
    SUBCODE_CONSTRAINT_UNKNOWN,
    SUBCODE_CONSTRAINT_UNIQUE,
    SUBCODE_CONSTRAINT_NOT_NULL,
    SUBCODE_CONSTRAINT_CHECK,
    SUBCODE_CONSTRAINT_FOREIGN_KEY,
    SUBCODE_TRANSACTION_UNKNOWN,
    SUBCODE_TRANSACTION_NO_ACTIVE,
    SUBCODE_TRANSACTION_INVALID_STATE,
    SUBCODE_QUEUE_WRITE_TIMEOUT,
    SUBCODE_QUEUE_CANCELED,
    SUBCODE_QUEUE_FULL,
    SUBCODE_QUEUE_CLOSED,
    SUBCODE_BUSY_UNKNOWN,
    SUBCODE_BUSY_WRITER_LOCK,
    SUBCODE_BUSY_READER_CONFLICT,
    SUBCODE_COORDINATION_LOCK_TIMEOUT,
    SUBCODE_COORDINATION_SIDECAR_UNAVAILABLE,
    SUBCODE_IO_UNKNOWN,
    SUBCODE_IO_PERMISSION_DENIED,
    SUBCODE_IO_DISK_FULL,
    SUBCODE_IO_NOT_FOUND,
    SUBCODE_FORMAT_UNSUPPORTED_VERSION,
    SUBCODE_CORRUPTION_UNKNOWN,
    SUBCODE_CORRUPTION_DATABASE_HEADER,
    SUBCODE_CORRUPTION_PAGE_CHECKSUM,
    SUBCODE_CORRUPTION_WAL_FRAME,
    SUBCODE_CORRUPTION_WAL_REPLAY,
    SUBCODE_SECURITY_POLICY_DENIED,
    SUBCODE_SECURITY_MASK_EXPRESSION_INVALID,
    SUBCODE_TDE_KEY_REQUIRED,
    SUBCODE_TDE_KEY_MISMATCH,
    SUBCODE_SYNC_SCOPE_NOT_FOUND,
    SUBCODE_SYNC_CHANGESET_CONFLICT,
    SUBCODE_SYNC_RETENTION_BLOCKED,
    SUBCODE_BRANCH_NOT_FOUND,
    SUBCODE_BRANCH_MERGE_CONFLICT,
    SUBCODE_EXTENSION_UNTRUSTED_PACKAGE,
    SUBCODE_INTERNAL_UNKNOWN,
    SUBCODE_INTERNAL_PANIC_CAPTURED,
    SUBCODE_INTERNAL_INVARIANT,
];

// ---------------------------------------------------------------------------
// Error subcodes (stable identifiers)
// ---------------------------------------------------------------------------

pub const SUBCODE_SQL_UNKNOWN: &str = "sql.unknown";
pub const SUBCODE_SQL_SYNTAX: &str = "sql.syntax";
pub const SUBCODE_SQL_RELATION_NOT_FOUND: &str = "sql.relation_not_found";
pub const SUBCODE_SQL_COLUMN_NOT_FOUND: &str = "sql.column_not_found";
pub const SUBCODE_SQL_AMBIGUOUS_COLUMN: &str = "sql.ambiguous_column";
pub const SUBCODE_SQL_PARAMETER_MISSING: &str = "sql.parameter_missing";
pub const SUBCODE_SQL_PARAMETER_TYPE_MISMATCH: &str = "sql.parameter_type_mismatch";
pub const SUBCODE_SQL_UNSUPPORTED_FEATURE: &str = "sql.unsupported_feature";
pub const SUBCODE_CONSTRAINT_UNKNOWN: &str = "constraint.unknown";
pub const SUBCODE_CONSTRAINT_UNIQUE: &str = "constraint.unique";
pub const SUBCODE_CONSTRAINT_NOT_NULL: &str = "constraint.not_null";
pub const SUBCODE_CONSTRAINT_CHECK: &str = "constraint.check";
pub const SUBCODE_CONSTRAINT_FOREIGN_KEY: &str = "constraint.foreign_key";
pub const SUBCODE_TRANSACTION_UNKNOWN: &str = "transaction.unknown";
pub const SUBCODE_TRANSACTION_NO_ACTIVE: &str = "transaction.no_active_transaction";
pub const SUBCODE_TRANSACTION_INVALID_STATE: &str = "transaction.invalid_state";
pub const SUBCODE_QUEUE_WRITE_TIMEOUT: &str = "queue.write_timeout";
pub const SUBCODE_QUEUE_CANCELED: &str = "queue.canceled";
pub const SUBCODE_QUEUE_FULL: &str = "queue.full";
pub const SUBCODE_QUEUE_CLOSED: &str = "queue.closed";
pub const SUBCODE_BUSY_UNKNOWN: &str = "busy.unknown";
pub const SUBCODE_BUSY_WRITER_LOCK: &str = "busy.writer_lock";
pub const SUBCODE_BUSY_READER_CONFLICT: &str = "busy.reader_conflict";
pub const SUBCODE_COORDINATION_LOCK_TIMEOUT: &str = "coordination.lock_timeout";
pub const SUBCODE_COORDINATION_SIDECAR_UNAVAILABLE: &str = "coordination.sidecar_unavailable";
pub const SUBCODE_IO_UNKNOWN: &str = "io.unknown";
pub const SUBCODE_IO_PERMISSION_DENIED: &str = "io.permission_denied";
pub const SUBCODE_IO_DISK_FULL: &str = "io.disk_full";
pub const SUBCODE_IO_NOT_FOUND: &str = "io.not_found";
pub const SUBCODE_FORMAT_UNSUPPORTED_VERSION: &str = "format.unsupported_version";
pub const SUBCODE_CORRUPTION_UNKNOWN: &str = "corruption.unknown";
pub const SUBCODE_CORRUPTION_DATABASE_HEADER: &str = "corruption.database_header";
pub const SUBCODE_CORRUPTION_PAGE_CHECKSUM: &str = "corruption.page_checksum";
pub const SUBCODE_CORRUPTION_WAL_FRAME: &str = "corruption.wal_frame";
pub const SUBCODE_CORRUPTION_WAL_REPLAY: &str = "corruption.wal_replay";
pub const SUBCODE_SECURITY_POLICY_DENIED: &str = "security.policy_denied";
pub const SUBCODE_SECURITY_MASK_EXPRESSION_INVALID: &str = "security.mask_expression_invalid";
pub const SUBCODE_TDE_KEY_REQUIRED: &str = "tde.key_required";
pub const SUBCODE_TDE_KEY_MISMATCH: &str = "tde.key_mismatch";
pub const SUBCODE_SYNC_SCOPE_NOT_FOUND: &str = "sync.scope_not_found";
pub const SUBCODE_SYNC_CHANGESET_CONFLICT: &str = "sync.changeset_conflict";
pub const SUBCODE_SYNC_RETENTION_BLOCKED: &str = "sync.retention_blocked";
pub const SUBCODE_BRANCH_NOT_FOUND: &str = "branch.not_found";
pub const SUBCODE_BRANCH_MERGE_CONFLICT: &str = "branch.merge_conflict";
pub const SUBCODE_EXTENSION_UNTRUSTED_PACKAGE: &str = "extension.untrusted_package";
pub const SUBCODE_INTERNAL_UNKNOWN: &str = "internal.unknown";
pub const SUBCODE_INTERNAL_PANIC_CAPTURED: &str = "internal.panic_captured";
pub const SUBCODE_INTERNAL_INVARIANT: &str = "internal.invariant";

fn short_hex_sha256(input: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(input.as_bytes());
    let bytes = digest.finalize();
    let hex = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let prefix = &hex[..12];
    format!("sha256:{prefix}")
}

#[allow(dead_code)]
fn is_sensitive_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    lower.contains("key")
        || lower.contains("token")
        || lower.contains("secret")
        || lower.contains("password")
        || lower.contains("credential")
        || lower.contains("auth")
}

fn sanitize_detail_value(value: Value) -> Option<Value> {
    match value {
        Value::Bool(_) | Value::Number(_) => Some(value),
        Value::String(raw) => {
            let truncated = raw.chars().take(MAX_STRING_DETAIL_LEN).collect::<String>();
            Some(Value::String(truncated))
        }
        _ => None,
    }
}

#[allow(dead_code)]
fn sanitize_audit_or_option_value(key: &str, value: &str) -> String {
    if is_sensitive_key(key) {
        REDACTED_VALUE.to_string()
    } else {
        value.to_string()
    }
}

fn clean_identifier(raw: &str) -> String {
    raw.trim_matches(|ch: char| {
        ch.is_whitespace()
            || ch == '\''
            || ch == '"'
            || ch == '`'
            || ch == ','
            || ch == '.'
            || ch == ':'
    })
    .to_string()
}

fn token_after(message: &str, marker: &str) -> Option<String> {
    let lower = message.to_ascii_lowercase();
    let start = lower.find(marker)? + marker.len();
    message[start..]
        .split_whitespace()
        .next()
        .map(clean_identifier)
        .filter(|value| !value.is_empty())
}

fn quoted_after(message: &str, marker: &str) -> Option<String> {
    let lower = message.to_ascii_lowercase();
    let start = lower.find(marker)? + marker.len();
    let rest = &message[start..];
    let quote = rest.find(['\'', '"', '`'])?;
    let quote_char = rest[quote..].chars().next()?;
    let value_start = quote + quote_char.len_utf8();
    let value_end = rest[value_start..].find(quote_char)? + value_start;
    let value = clean_identifier(&rest[value_start..value_end]);
    (!value.is_empty()).then_some(value)
}

fn first_parameter_index(message: &str) -> Option<u32> {
    let bytes = message.as_bytes();
    for index in 0..bytes.len() {
        if bytes[index] != b'$' {
            continue;
        }
        let mut end = index + 1;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
        if end > index + 1 {
            if let Ok(value) = message[index + 1..end].parse::<u32>() {
                return Some(value);
            }
        }
    }
    None
}

fn diagnostic_doctor(
    kind: DbDoctorHandoffKind,
    command: &'static str,
    sql: &[&'static str],
) -> DbDoctorHandoff {
    DbDoctorHandoff {
        kind,
        command: command.to_string(),
        sql: sql
            .iter()
            .map(|statement| (*statement).to_string())
            .collect(),
    }
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn serialize_error_code<S>(code: &DbErrorCode, s: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_u32(code.as_u32())
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DbDiagnosticRedaction {
    Default,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DbDiagnosticPathKind {
    Database,
    Wal,
    CoordinationSidecar,
    SyncJournal,
    BackupDestination,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DbDiagnosticPath {
    pub kind: DbDiagnosticPathKind,
    pub display: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
}

impl DbDiagnosticPath {
    #[must_use]
    pub fn redacted(path: &str, kind: DbDiagnosticPathKind) -> Self {
        Self::redacted_with_display(path, kind, None)
    }

    #[must_use]
    pub fn redacted_with_display(
        path: &str,
        kind: DbDiagnosticPathKind,
        display: Option<&str>,
    ) -> Self {
        let display = display
            .map(str::to_owned)
            .or_else(|| {
                Path::new(path)
                    .file_name()
                    .and_then(|part| part.to_str())
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| "path".to_owned());
        Self {
            kind,
            display,
            fingerprint: Some(short_hex_sha256(path)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DbDiagnosticParameter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_name: Option<String>,
}

impl DbDiagnosticParameter {
    #[must_use]
    pub fn new(index: Option<u32>, name: Option<&str>, type_name: Option<&str>) -> Self {
        Self {
            index,
            name: name.map(str::to_owned),
            type_name: type_name.map(str::to_owned),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
#[allow(dead_code)]
pub struct DbDiagnosticOpenOptions {
    options: BTreeMap<String, String>,
}

impl DbDiagnosticOpenOptions {
    #[allow(dead_code)]
    #[must_use]
    pub fn from_raw(raw: &str) -> Self {
        let mut options = BTreeMap::new();
        for fragment in raw.split([';', ',']) {
            let mut parts = fragment.splitn(2, '=');
            let key = match parts.next() {
                Some(raw_key) => raw_key.trim().to_lowercase(),
                None => continue,
            };
            if key.is_empty() {
                continue;
            }
            let value = parts.next().unwrap_or_default().trim();
            let redacted = sanitize_audit_or_option_value(&key, value);
            options.insert(key, redacted);
        }
        Self { options }
    }

    #[allow(dead_code)]
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.options
            .get(&key.to_ascii_lowercase())
            .map(String::as_str)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
#[allow(dead_code)]
pub struct DbDiagnosticAuditContext {
    fields: BTreeMap<String, String>,
}

impl DbDiagnosticAuditContext {
    #[allow(dead_code)]
    #[must_use]
    pub fn from_pairs<I>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let mut fields = BTreeMap::new();
        for (key, value) in pairs {
            let redacted = sanitize_audit_or_option_value(&key, &value);
            fields.insert(key, redacted);
        }
        Self { fields }
    }

    #[allow(dead_code)]
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.fields.get(key).map(String::as_str)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub struct DbDiagnosticSyncToken {
    value: &'static str,
}

impl DbDiagnosticSyncToken {
    #[allow(dead_code)]
    #[must_use]
    pub fn redacted(_token: &str) -> Self {
        Self {
            value: REDACTED_VALUE,
        }
    }

    #[allow(dead_code)]
    #[must_use]
    pub fn value(&self) -> &'static str {
        self.value
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DbDoctorHandoffKind {
    ProcessCoordination,
    Sync,
    Wal,
    Corruption,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DbDoctorHandoff {
    pub kind: DbDoctorHandoffKind,
    pub command: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sql: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct DbDiagnosticContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constraint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync_scope: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync_peer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changeset_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process_owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<DbDiagnosticPath>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameter: Option<DbDiagnosticParameter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<BTreeMap<String, Value>>,
}

impl DbDiagnosticContext {
    #[must_use]
    pub fn with_relation(mut self, relation: impl Into<String>) -> Self {
        self.relation = Some(relation.into());
        self
    }

    #[must_use]
    pub fn with_column(mut self, column: impl Into<String>) -> Self {
        self.column = Some(column.into());
        self
    }

    #[must_use]
    pub fn with_constraint(mut self, constraint: impl Into<String>) -> Self {
        self.constraint = Some(constraint.into());
        self
    }

    #[must_use]
    pub fn with_index(mut self, index: impl Into<String>) -> Self {
        self.index = Some(index.into());
        self
    }

    #[must_use]
    pub fn with_policy(mut self, policy: impl Into<String>) -> Self {
        self.policy = Some(policy.into());
        self
    }

    #[must_use]
    pub fn with_branch(mut self, branch: impl Into<String>) -> Self {
        self.branch = Some(branch.into());
        self
    }

    #[must_use]
    pub fn with_sync_scope(mut self, sync_scope: impl Into<String>) -> Self {
        self.sync_scope = Some(sync_scope.into());
        self
    }

    #[must_use]
    pub fn with_sync_peer(mut self, sync_peer: impl Into<String>) -> Self {
        self.sync_peer = Some(sync_peer.into());
        self
    }

    #[must_use]
    pub fn with_changeset(mut self, changeset_id: impl Into<String>) -> Self {
        self.changeset_id = Some(changeset_id.into());
        self
    }

    #[must_use]
    pub fn with_path(mut self, path: DbDiagnosticPath) -> Self {
        self.path = Some(path);
        self
    }

    #[must_use]
    pub fn with_format(mut self, format: impl Into<String>) -> Self {
        self.format = Some(format.into());
        self
    }

    #[must_use]
    pub fn with_process_owner(mut self, process_owner: impl Into<String>) -> Self {
        self.process_owner = Some(process_owner.into());
        self
    }

    #[must_use]
    pub fn with_wal(mut self, wal: impl Into<String>) -> Self {
        self.wal = Some(wal.into());
        self
    }

    #[must_use]
    pub fn with_parameter(mut self, parameter: DbDiagnosticParameter) -> Self {
        self.parameter = Some(parameter);
        self
    }

    #[must_use]
    pub fn with_detail(mut self, key: impl Into<String>, value: Value) -> Self {
        if self.details.is_none() {
            self.details = Some(BTreeMap::new());
        }
        if let Some(details) = &mut self.details {
            if details.len() < MAX_DETAIL_KEYS {
                if let Some(safe) = sanitize_detail_value(value) {
                    details.insert(key.into(), safe);
                }
            }
        }
        self
    }

    #[must_use]
    pub fn detail_count(&self) -> usize {
        self.details.as_ref().map_or(0, BTreeMap::len)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DbDiagnostic {
    pub version: u16,
    #[serde(serialize_with = "serialize_error_code")]
    pub code: DbErrorCode,
    pub code_name: &'static str,
    pub subcode: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sqlstate: Option<&'static str>,
    pub message: String,
    pub retryable: bool,
    pub permanent: bool,
    pub redaction: DbDiagnosticRedaction,
    #[serde(flatten)]
    pub context: DbDiagnosticContext,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub docs: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doctor: Option<DbDoctorHandoff>,
}

impl DbDiagnostic {
    #[must_use]
    pub fn new(
        code: DbErrorCode,
        subcode: &'static str,
        message: impl Into<String>,
        retryable: bool,
        permanent: bool,
    ) -> Self {
        Self {
            version: DIAGNOSTIC_VERSION,
            code,
            code_name: code.code_name(),
            subcode,
            sqlstate: None,
            message: message.into(),
            retryable,
            permanent,
            redaction: DbDiagnosticRedaction::Default,
            context: DbDiagnosticContext::default(),
            hint: None,
            docs: None,
            doctor: None,
        }
    }

    #[must_use]
    pub fn with_sqlstate(mut self, sqlstate: &'static str) -> Self {
        self.sqlstate = Some(sqlstate);
        self
    }

    #[must_use]
    pub fn with_hint(mut self, hint: &'static str) -> Self {
        self.hint = Some(hint);
        self
    }

    #[must_use]
    pub fn with_docs(mut self, docs: &'static str) -> Self {
        self.docs = Some(docs);
        self
    }

    #[must_use]
    pub fn with_doctor(mut self, doctor: DbDoctorHandoff) -> Self {
        self.doctor = Some(doctor);
        self
    }

    #[must_use]
    pub fn with_context(mut self, context: DbDiagnosticContext) -> Self {
        self.context = context;
        self
    }

    pub fn to_json(&self) -> serde_json::Result<String> {
        serde_json::to_string(self)
    }
}

/// Stable numeric error codes for the DecentDB engine.
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DbErrorCode {
    Io = 1,
    Corruption = 2,
    Constraint = 3,
    Transaction = 4,
    Sql = 5,
    Internal = 6,
    Panic = 7,
    UnsupportedFormatVersion = 8,
    Busy = 9,
    Timeout = 10,
    Canceled = 11,
    QueueFull = 12,
    QueueClosed = 13,
}

impl DbErrorCode {
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self as u32
    }

    #[must_use]
    pub const fn code_name(self) -> &'static str {
        match self {
            Self::Io => "ERR_IO",
            Self::Corruption => "ERR_CORRUPTION",
            Self::Constraint => "ERR_CONSTRAINT",
            Self::Transaction => "ERR_TRANSACTION",
            Self::Sql => "ERR_SQL",
            Self::Internal => "ERR_INTERNAL",
            Self::Panic => "ERR_PANIC",
            Self::UnsupportedFormatVersion => "ERR_UNSUPPORTED_FORMAT_VERSION",
            Self::Busy => "ERR_BUSY",
            Self::Timeout => "ERR_TIMEOUT",
            Self::Canceled => "ERR_CANCELED",
            Self::QueueFull => "ERR_QUEUE_FULL",
            Self::QueueClosed => "ERR_QUEUE_CLOSED",
        }
    }
}

/// Canonical engine result type.
pub type Result<T> = std::result::Result<T, DbError>;

/// Canonical engine error type.
#[derive(Debug, Error)]
pub enum DbError {
    #[error("I/O error: {context}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
    #[error("database corruption: {message}")]
    Corruption { message: String },
    #[error("constraint violation: {message}")]
    Constraint { message: String },
    #[error("transaction error: {message}")]
    Transaction { message: String },
    #[error("SQL error: {message}")]
    Sql { message: String },
    #[error("internal engine error: {message}")]
    Internal { message: String },
    #[error("panic captured at boundary: {message}")]
    Panic { message: String },
    #[error("unsupported database format version: {version}")]
    UnsupportedFormatVersion { version: u32 },
    #[error("busy: {message}")]
    Busy { message: String },
    #[error("operation timed out: {message}")]
    Timeout { message: String },
    #[error("operation canceled: {message}")]
    Canceled { message: String },
    #[error("write queue full: {message}")]
    QueueFull { message: String },
    #[error("write queue closed: {message}")]
    QueueClosed { message: String },
    #[error("{message}")]
    Structured {
        message: String,
        diagnostic: Box<DbDiagnostic>,
    },
}

impl DbError {
    fn sql_legacy_diagnostic(message: &str) -> DbDiagnostic {
        let lower = message.to_ascii_lowercase();
        if lower.contains("mask expression") && lower.contains("parse") {
            return DbDiagnostic::new(
                DbErrorCode::Sql,
                SUBCODE_SECURITY_MASK_EXPRESSION_INVALID,
                message,
                false,
                true,
            )
            .with_sqlstate("42601")
            .with_docs("errors/security-mask-expression-invalid");
        }
        if lower.contains("policy denied") || lower.contains("permission denied by policy") {
            return DbDiagnostic::new(
                DbErrorCode::Sql,
                SUBCODE_SECURITY_POLICY_DENIED,
                message,
                false,
                true,
            )
            .with_sqlstate("42501")
            .with_docs("errors/security-policy-denied");
        }
        if lower.contains("extension package is unsigned")
            || lower.contains("extension signature verification failed")
            || lower.contains("untrusted extension")
        {
            return DbDiagnostic::new(
                DbErrorCode::Sql,
                SUBCODE_EXTENSION_UNTRUSTED_PACKAGE,
                message,
                false,
                true,
            )
            .with_docs("errors/extension-untrusted-package");
        }
        if lower.contains("branch") && (lower.contains("not found") || lower.contains("missing")) {
            let branch = quoted_after(message, "branch")
                .or_else(|| token_after(message, "branch"))
                .unwrap_or_else(|| "unknown".to_string());
            return DbDiagnostic::new(
                DbErrorCode::Sql,
                SUBCODE_BRANCH_NOT_FOUND,
                message,
                false,
                true,
            )
            .with_context(DbDiagnosticContext::default().with_branch(branch))
            .with_docs("errors/branch-not-found");
        }
        if lower.contains("sync scope")
            && (lower.contains("not found") || lower.contains("unknown"))
        {
            let sync_scope = quoted_after(message, "sync scope")
                .or_else(|| token_after(message, "sync scope"))
                .unwrap_or_else(|| "unknown".to_string());
            return DbDiagnostic::new(
                DbErrorCode::Sql,
                SUBCODE_SYNC_SCOPE_NOT_FOUND,
                message,
                false,
                true,
            )
            .with_context(DbDiagnosticContext::default().with_sync_scope(sync_scope))
            .with_docs("errors/sync-scope-not-found");
        }
        if lower.contains("ambiguous column") {
            let column = token_after(message, "ambiguous column reference")
                .or_else(|| token_after(message, "ambiguous column"))
                .unwrap_or_else(|| "unknown".to_string());
            return DbDiagnostic::new(
                DbErrorCode::Sql,
                SUBCODE_SQL_AMBIGUOUS_COLUMN,
                message,
                false,
                true,
            )
            .with_sqlstate("42702")
            .with_context(DbDiagnosticContext::default().with_column(column))
            .with_docs("errors/sql-ambiguous-column");
        }
        if lower.contains("unknown column")
            || (lower.contains("column") && lower.contains("not found"))
            || (lower.contains("column") && lower.contains("does not exist"))
        {
            let column = token_after(message, "unknown column")
                .or_else(|| quoted_after(message, "column"))
                .or_else(|| token_after(message, "column"))
                .unwrap_or_else(|| "unknown".to_string());
            return DbDiagnostic::new(
                DbErrorCode::Sql,
                SUBCODE_SQL_COLUMN_NOT_FOUND,
                message,
                false,
                true,
            )
            .with_sqlstate("42703")
            .with_context(DbDiagnosticContext::default().with_column(column))
            .with_docs("errors/sql-column-not-found");
        }
        if lower.contains("unknown table")
            || lower.contains("unknown relation")
            || lower.contains("unknown table or view")
            || (lower.contains("table") && lower.contains("not found"))
        {
            let relation = token_after(message, "unknown table or view")
                .or_else(|| token_after(message, "unknown table"))
                .or_else(|| token_after(message, "unknown relation"))
                .or_else(|| token_after(message, "table"))
                .unwrap_or_else(|| "unknown".to_string());
            return DbDiagnostic::new(
                DbErrorCode::Sql,
                SUBCODE_SQL_RELATION_NOT_FOUND,
                message,
                false,
                true,
            )
            .with_sqlstate("42P01")
            .with_context(DbDiagnosticContext::default().with_relation(relation))
            .with_docs("errors/sql-relation-not-found");
        }
        if lower.contains("parameter")
            && (lower.contains("not provided")
                || lower.contains("missing")
                || lower.contains("was not bound"))
        {
            return DbDiagnostic::new(
                DbErrorCode::Sql,
                SUBCODE_SQL_PARAMETER_MISSING,
                message,
                false,
                true,
            )
            .with_sqlstate("07002")
            .with_context(DbDiagnosticContext::default().with_parameter(
                DbDiagnosticParameter::new(first_parameter_index(message), None, None),
            ))
            .with_docs("errors/sql-parameter-missing");
        }
        if lower.contains("parameter") && lower.contains("type") {
            return DbDiagnostic::new(
                DbErrorCode::Sql,
                SUBCODE_SQL_PARAMETER_TYPE_MISMATCH,
                message,
                false,
                true,
            )
            .with_sqlstate("42804")
            .with_context(DbDiagnosticContext::default().with_parameter(
                DbDiagnosticParameter::new(first_parameter_index(message), None, None),
            ))
            .with_docs("errors/sql-parameter-type-mismatch");
        }
        if lower.contains("unsupported") || lower.contains("not supported") {
            return DbDiagnostic::new(
                DbErrorCode::Sql,
                SUBCODE_SQL_UNSUPPORTED_FEATURE,
                message,
                false,
                true,
            )
            .with_sqlstate("0A000")
            .with_docs("errors/sql-unsupported-feature");
        }
        if lower.contains("syntax")
            || lower.contains("parse")
            || lower.contains("parser")
            || lower.contains("expected")
            || lower.contains("invalid sql")
        {
            return DbDiagnostic::new(DbErrorCode::Sql, SUBCODE_SQL_SYNTAX, message, false, true)
                .with_sqlstate("42601")
                .with_docs("errors/sql-syntax");
        }
        Self::legacy_diagnostic(DbErrorCode::Sql, message)
    }

    fn constraint_legacy_diagnostic(message: &str) -> DbDiagnostic {
        let lower = message.to_ascii_lowercase();
        if lower.contains("foreign key") {
            return DbDiagnostic::new(
                DbErrorCode::Constraint,
                SUBCODE_CONSTRAINT_FOREIGN_KEY,
                message,
                false,
                true,
            )
            .with_sqlstate("23503")
            .with_docs("errors/constraint-foreign-key");
        }
        if lower.contains("not null") || lower.contains("must not be null") {
            return DbDiagnostic::new(
                DbErrorCode::Constraint,
                SUBCODE_CONSTRAINT_NOT_NULL,
                message,
                false,
                true,
            )
            .with_sqlstate("23502")
            .with_docs("errors/constraint-not-null");
        }
        if lower.contains("check") {
            return DbDiagnostic::new(
                DbErrorCode::Constraint,
                SUBCODE_CONSTRAINT_CHECK,
                message,
                false,
                true,
            )
            .with_sqlstate("23514")
            .with_docs("errors/constraint-check");
        }
        if lower.contains("unique")
            || lower.contains("duplicate")
            || lower.contains("primary key")
            || lower.contains("already exists")
        {
            return DbDiagnostic::new(
                DbErrorCode::Constraint,
                SUBCODE_CONSTRAINT_UNIQUE,
                message,
                false,
                true,
            )
            .with_sqlstate("23505")
            .with_docs("errors/constraint-unique");
        }
        Self::legacy_diagnostic(DbErrorCode::Constraint, message)
    }

    fn transaction_legacy_diagnostic(message: &str) -> DbDiagnostic {
        let lower = message.to_ascii_lowercase();
        if lower.contains("no active") {
            return DbDiagnostic::new(
                DbErrorCode::Transaction,
                SUBCODE_TRANSACTION_NO_ACTIVE,
                message,
                false,
                true,
            )
            .with_sqlstate("25000")
            .with_docs("errors/transaction-no-active-transaction");
        }
        DbDiagnostic::new(
            DbErrorCode::Transaction,
            SUBCODE_TRANSACTION_INVALID_STATE,
            message,
            false,
            true,
        )
        .with_sqlstate("25000")
        .with_docs("errors/transaction-invalid-state")
    }

    fn io_legacy_diagnostic(context: &str, source: &std::io::Error) -> DbDiagnostic {
        let raw_os_error = source.raw_os_error();
        let (subcode, retryable, docs) = if source.kind() == std::io::ErrorKind::PermissionDenied {
            (
                SUBCODE_IO_PERMISSION_DENIED,
                false,
                "errors/io-permission-denied",
            )
        } else if source.kind() == std::io::ErrorKind::NotFound {
            (SUBCODE_IO_NOT_FOUND, false, "errors/io-not-found")
        } else if matches!(raw_os_error, Some(28) | Some(112)) {
            (SUBCODE_IO_DISK_FULL, true, "errors/io-disk-full")
        } else {
            (SUBCODE_IO_UNKNOWN, false, "errors/io-unknown")
        };
        DbDiagnostic::new(
            DbErrorCode::Io,
            subcode,
            format!("I/O error: {context}"),
            retryable,
            true,
        )
        .with_context(
            DbDiagnosticContext::default().with_path(DbDiagnosticPath::redacted(
                context,
                DbDiagnosticPathKind::Unknown,
            )),
        )
        .with_docs(docs)
    }

    fn corruption_legacy_diagnostic(message: &str) -> DbDiagnostic {
        let lower = message.to_ascii_lowercase();
        let (subcode, docs, kind) = if lower.contains("wal") && lower.contains("replay") {
            (
                SUBCODE_CORRUPTION_WAL_REPLAY,
                "errors/corruption-wal-replay",
                DbDoctorHandoffKind::Wal,
            )
        } else if lower.contains("wal") || lower.contains("frame") {
            (
                SUBCODE_CORRUPTION_WAL_FRAME,
                "errors/corruption-wal-frame",
                DbDoctorHandoffKind::Wal,
            )
        } else if lower.contains("checksum") || lower.contains("page") {
            (
                SUBCODE_CORRUPTION_PAGE_CHECKSUM,
                "errors/corruption-page-checksum",
                DbDoctorHandoffKind::Corruption,
            )
        } else if lower.contains("header") || lower.contains("magic") {
            (
                SUBCODE_CORRUPTION_DATABASE_HEADER,
                "errors/corruption-database-header",
                DbDoctorHandoffKind::Corruption,
            )
        } else {
            (
                SUBCODE_CORRUPTION_UNKNOWN,
                "errors/corruption-unknown",
                DbDoctorHandoffKind::Corruption,
            )
        };
        DbDiagnostic::new(DbErrorCode::Corruption, subcode, message, false, true)
            .with_docs(docs)
            .with_doctor(diagnostic_doctor(
                kind,
                "decentdb doctor --db <redacted> --format=json",
                &[],
            ))
    }

    fn busy_legacy_diagnostic(message: &str) -> DbDiagnostic {
        let lower = message.to_ascii_lowercase();
        let subcode = if lower.contains("reader") {
            SUBCODE_BUSY_READER_CONFLICT
        } else {
            SUBCODE_BUSY_WRITER_LOCK
        };
        DbDiagnostic::new(DbErrorCode::Busy, subcode, message, true, true)
            .with_sqlstate("55P03")
            .with_doctor(diagnostic_doctor(
                DbDoctorHandoffKind::ProcessCoordination,
                "decentdb doctor --db <redacted> --format=json",
                &[
                    "SELECT * FROM sys.process_coordination",
                    "SELECT * FROM sys.process_lock_metrics",
                ],
            ))
            .with_docs(if subcode == SUBCODE_BUSY_READER_CONFLICT {
                "errors/busy-reader-conflict"
            } else {
                "errors/busy-writer-lock"
            })
    }

    fn timeout_legacy_diagnostic(message: &str) -> DbDiagnostic {
        let lower = message.to_ascii_lowercase();
        if lower.contains("writer lock") || lower.contains("process") {
            return DbDiagnostic::new(
                DbErrorCode::Timeout,
                SUBCODE_COORDINATION_LOCK_TIMEOUT,
                message,
                true,
                true,
            )
            .with_sqlstate("55P03")
            .with_doctor(diagnostic_doctor(
                DbDoctorHandoffKind::ProcessCoordination,
                "decentdb doctor --db <redacted> --format=json",
                &[
                    "SELECT * FROM sys.process_coordination",
                    "SELECT * FROM sys.process_lock_metrics",
                ],
            ))
            .with_docs("errors/coordination-lock-timeout");
        }
        DbDiagnostic::new(
            DbErrorCode::Timeout,
            SUBCODE_QUEUE_WRITE_TIMEOUT,
            message,
            true,
            true,
        )
        .with_sqlstate("HYT00")
        .with_docs("errors/queue-write-timeout")
    }

    fn legacy_diagnostic(code: DbErrorCode, message: impl Into<String>) -> DbDiagnostic {
        let message = message.into();
        let (subcode, retryable, permanent) = match code {
            DbErrorCode::Sql => (SUBCODE_SQL_UNKNOWN, false, true),
            DbErrorCode::Constraint => (SUBCODE_CONSTRAINT_UNKNOWN, false, true),
            DbErrorCode::Transaction => (SUBCODE_TRANSACTION_UNKNOWN, false, true),
            DbErrorCode::Io => (SUBCODE_IO_UNKNOWN, false, true),
            DbErrorCode::Corruption => (SUBCODE_CORRUPTION_UNKNOWN, false, true),
            DbErrorCode::Internal => (SUBCODE_INTERNAL_UNKNOWN, false, true),
            DbErrorCode::Panic => (SUBCODE_INTERNAL_PANIC_CAPTURED, false, true),
            DbErrorCode::UnsupportedFormatVersion => {
                (SUBCODE_FORMAT_UNSUPPORTED_VERSION, false, true)
            }
            DbErrorCode::Busy => (SUBCODE_BUSY_UNKNOWN, true, true),
            DbErrorCode::Timeout => (SUBCODE_QUEUE_WRITE_TIMEOUT, true, true),
            DbErrorCode::Canceled => (SUBCODE_QUEUE_CANCELED, false, false),
            DbErrorCode::QueueFull => (SUBCODE_QUEUE_FULL, true, true),
            DbErrorCode::QueueClosed => (SUBCODE_QUEUE_CLOSED, false, true),
        };

        DbDiagnostic::new(code, subcode, message, retryable, permanent)
    }

    #[allow(clippy::too_many_arguments)]
    fn structured(
        code: DbErrorCode,
        subcode: &'static str,
        message: impl Into<String>,
        retryable: bool,
        permanent: bool,
        context: DbDiagnosticContext,
        sqlstate: Option<&'static str>,
        hint: Option<&'static str>,
        docs: Option<&'static str>,
    ) -> Self {
        let message = message.into();
        let mut diagnostic =
            DbDiagnostic::new(code, subcode, message.clone(), retryable, permanent)
                .with_context(context);
        if let Some(sqlstate) = sqlstate {
            diagnostic = diagnostic.with_sqlstate(sqlstate);
        }
        if let Some(hint) = hint {
            diagnostic = diagnostic.with_hint(hint);
        }
        if let Some(docs) = docs {
            diagnostic = diagnostic.with_docs(docs);
        }
        Self::Structured {
            message,
            diagnostic: Box::new(diagnostic),
        }
    }

    #[must_use]
    pub fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }

    #[must_use]
    pub fn corruption(message: impl Into<String>) -> Self {
        Self::Corruption {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn constraint(message: impl Into<String>) -> Self {
        Self::Constraint {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn transaction(message: impl Into<String>) -> Self {
        Self::Transaction {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn sql(message: impl Into<String>) -> Self {
        Self::Sql {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn panic(message: impl Into<String>) -> Self {
        Self::Panic {
            message: message.into(),
        }
    }

    /// Structured variant for internal invariant violations.
    #[must_use]
    pub fn internal_invariant(message: impl Into<String>) -> Self {
        Self::structured(
            DbErrorCode::Internal,
            SUBCODE_INTERNAL_INVARIANT,
            message,
            false,
            true,
            DbDiagnosticContext::default(),
            Some("XX000"),
            None,
            Some("errors/internal-invariant"),
        )
    }

    #[must_use]
    pub fn unsupported_format_version(version: u32) -> Self {
        Self::UnsupportedFormatVersion { version }
    }

    #[must_use]
    pub fn busy(message: impl Into<String>) -> Self {
        Self::Busy {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn timeout(message: impl Into<String>) -> Self {
        Self::Timeout {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn canceled(message: impl Into<String>) -> Self {
        Self::Canceled {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn queue_full(message: impl Into<String>) -> Self {
        Self::QueueFull {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn queue_closed(message: impl Into<String>) -> Self {
        Self::QueueClosed {
            message: message.into(),
        }
    }

    /// Structured variant for SQL syntax errors.
    #[must_use]
    pub fn sql_syntax(message: impl Into<String>) -> Self {
        Self::structured(
            DbErrorCode::Sql,
            SUBCODE_SQL_SYNTAX,
            message,
            false,
            true,
            DbDiagnosticContext::default(),
            Some("42601"),
            None,
            Some("errors/sql-syntax"),
        )
    }

    /// Structured variant for SQL relation-not-found path.
    #[must_use]
    pub fn sql_relation_not_found(relation: impl Into<String>, message: impl Into<String>) -> Self {
        Self::structured(
            DbErrorCode::Sql,
            SUBCODE_SQL_RELATION_NOT_FOUND,
            message,
            false,
            true,
            DbDiagnosticContext::default().with_relation(relation),
            Some("42P01"),
            None,
            Some("errors/sql-relation-not-found"),
        )
    }

    /// Structured variant for SQL column-not-found errors.
    #[must_use]
    pub fn sql_column_not_found(
        relation: impl Into<String>,
        column: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::structured(
            DbErrorCode::Sql,
            SUBCODE_SQL_COLUMN_NOT_FOUND,
            message,
            false,
            true,
            DbDiagnosticContext::default()
                .with_relation(relation)
                .with_column(column),
            Some("42703"),
            None,
            Some("errors/sql-column-not-found"),
        )
    }

    /// Structured variant for unique constraints with object context.
    #[must_use]
    pub fn constraint_unique(
        relation: impl Into<String>,
        column: impl Into<String>,
        constraint: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::structured(
            DbErrorCode::Constraint,
            SUBCODE_CONSTRAINT_UNIQUE,
            message,
            false,
            true,
            DbDiagnosticContext::default()
                .with_relation(relation)
                .with_column(column)
                .with_constraint(constraint),
            Some("23505"),
            None,
            Some("errors/constraint-unique"),
        )
    }

    /// Structured variant for not-null constraints.
    #[must_use]
    pub fn constraint_not_null(
        relation: impl Into<String>,
        column: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::structured(
            DbErrorCode::Constraint,
            SUBCODE_CONSTRAINT_NOT_NULL,
            message,
            false,
            true,
            DbDiagnosticContext::default()
                .with_relation(relation)
                .with_column(column),
            Some("23502"),
            None,
            Some("errors/constraint-not-null"),
        )
    }

    /// Structured variant for check constraints.
    #[must_use]
    pub fn constraint_check(relation: impl Into<String>, message: impl Into<String>) -> Self {
        Self::structured(
            DbErrorCode::Constraint,
            SUBCODE_CONSTRAINT_CHECK,
            message,
            false,
            true,
            DbDiagnosticContext::default().with_relation(relation),
            Some("23514"),
            None,
            Some("errors/constraint-check"),
        )
    }

    /// Structured variant for foreign-key constraints.
    #[must_use]
    pub fn constraint_foreign_key(
        relation: impl Into<String>,
        column: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::structured(
            DbErrorCode::Constraint,
            SUBCODE_CONSTRAINT_FOREIGN_KEY,
            message,
            false,
            true,
            DbDiagnosticContext::default()
                .with_relation(relation)
                .with_column(column),
            Some("23503"),
            None,
            Some("errors/constraint-foreign-key"),
        )
    }

    /// Structured variant for writer lock contention.
    #[must_use]
    pub fn busy_writer_lock(message: impl Into<String>) -> Self {
        Self::structured(
            DbErrorCode::Busy,
            SUBCODE_BUSY_WRITER_LOCK,
            message,
            true,
            true,
            DbDiagnosticContext::default(),
            None,
            Some("writer lock is busy"),
            Some("errors/busy-writer-lock"),
        )
    }

    /// Structured variant for queue full context and shape.
    #[must_use]
    pub fn queue_full_with_capacity(capacity: u64, message: impl Into<String>) -> Self {
        Self::structured(
            DbErrorCode::QueueFull,
            SUBCODE_QUEUE_FULL,
            message,
            true,
            true,
            DbDiagnosticContext::default().with_detail("capacity", capacity.into()),
            Some("HYT00"),
            Some("retry after backoff when queue pressure drops"),
            Some("errors/queue-full"),
        )
    }

    /// Structured variant for a bounded path redaction example.
    #[must_use]
    pub fn io_not_found(path: impl Into<String>, message: impl Into<String>) -> Self {
        let path = path.into();
        Self::structured(
            DbErrorCode::Io,
            SUBCODE_IO_NOT_FOUND,
            message,
            false,
            true,
            DbDiagnosticContext::default().with_path(DbDiagnosticPath::redacted(
                &path,
                DbDiagnosticPathKind::Unknown,
            )),
            None,
            None,
            Some("errors/io-not-found"),
        )
    }

    /// Return the diagnostic object for this error.
    #[must_use]
    pub fn diagnostic(&self) -> DbDiagnostic {
        match self {
            Self::Structured { diagnostic, .. } => diagnostic.as_ref().clone(),
            Self::Io { context, source } => Self::io_legacy_diagnostic(context, source),
            Self::Corruption { message } => Self::corruption_legacy_diagnostic(message),
            Self::Constraint { message } => Self::constraint_legacy_diagnostic(message),
            Self::Transaction { message } => Self::transaction_legacy_diagnostic(message),
            Self::Sql { message } => Self::sql_legacy_diagnostic(message),
            Self::Internal { message } => Self::legacy_diagnostic(DbErrorCode::Internal, message),
            Self::Panic { message } => Self::legacy_diagnostic(DbErrorCode::Panic, message),
            Self::UnsupportedFormatVersion { version } => {
                let mut diagnostic = DbDiagnostic::new(
                    DbErrorCode::UnsupportedFormatVersion,
                    SUBCODE_FORMAT_UNSUPPORTED_VERSION,
                    format!("unsupported database format version: {version}"),
                    false,
                    true,
                );
                diagnostic.context = diagnostic.context.with_format(version.to_string());
                diagnostic
            }
            Self::Busy { message } => Self::busy_legacy_diagnostic(message),
            Self::Timeout { message } => Self::timeout_legacy_diagnostic(message),
            Self::Canceled { message } => Self::legacy_diagnostic(DbErrorCode::Canceled, message),
            Self::QueueFull { message } => Self::legacy_diagnostic(DbErrorCode::QueueFull, message),
            Self::QueueClosed { message } => {
                Self::legacy_diagnostic(DbErrorCode::QueueClosed, message)
            }
        }
    }

    #[must_use]
    pub fn code(&self) -> DbErrorCode {
        match self {
            Self::Io { .. } => DbErrorCode::Io,
            Self::Corruption { .. } => DbErrorCode::Corruption,
            Self::Constraint { .. } => DbErrorCode::Constraint,
            Self::Transaction { .. } => DbErrorCode::Transaction,
            Self::Sql { .. } => DbErrorCode::Sql,
            Self::Internal { .. } => DbErrorCode::Internal,
            Self::Panic { .. } => DbErrorCode::Panic,
            Self::UnsupportedFormatVersion { .. } => DbErrorCode::UnsupportedFormatVersion,
            Self::Busy { .. } => DbErrorCode::Busy,
            Self::Timeout { .. } => DbErrorCode::Timeout,
            Self::Canceled { .. } => DbErrorCode::Canceled,
            Self::QueueFull { .. } => DbErrorCode::QueueFull,
            Self::QueueClosed { .. } => DbErrorCode::QueueClosed,
            Self::Structured { diagnostic, .. } => diagnostic.code,
        }
    }

    #[must_use]
    pub fn numeric_code(&self) -> u32 {
        self.code().as_u32()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Error;

    use super::{
        DbDiagnostic, DbDiagnosticAuditContext, DbDiagnosticContext, DbDiagnosticOpenOptions,
        DbDiagnosticPath, DbDiagnosticPathKind, DbDiagnosticSyncToken, DbError, DbErrorCode,
        MAX_DETAIL_KEYS, REDACTED_VALUE, SUBCODE_BUSY_WRITER_LOCK, SUBCODE_CONSTRAINT_UNIQUE,
        SUBCODE_QUEUE_CANCELED, SUBCODE_QUEUE_FULL, SUBCODE_SQL_RELATION_NOT_FOUND,
        SUBCODE_SQL_SYNTAX, SUBCODE_SQL_UNKNOWN,
    };
    use serde_json::json;
    use serde_json::Value;

    #[test]
    fn error_categories_map_to_stable_numeric_codes() {
        let cases = [
            (DbError::io("disk", Error::other("disk")), DbErrorCode::Io),
            (DbError::corruption("bad header"), DbErrorCode::Corruption),
            (
                DbError::constraint("duplicate key"),
                DbErrorCode::Constraint,
            ),
            (DbError::transaction("busy"), DbErrorCode::Transaction),
            (DbError::sql("syntax"), DbErrorCode::Sql),
            (DbError::internal("broken invariant"), DbErrorCode::Internal),
            (DbError::panic("panic payload"), DbErrorCode::Panic),
            (
                DbError::unsupported_format_version(7),
                DbErrorCode::UnsupportedFormatVersion,
            ),
            (DbError::busy("writer busy"), DbErrorCode::Busy),
            (DbError::timeout("queue wait"), DbErrorCode::Timeout),
            (DbError::canceled("before run"), DbErrorCode::Canceled),
            (
                DbError::queue_full("capacity reached"),
                DbErrorCode::QueueFull,
            ),
            (DbError::queue_closed("shutdown"), DbErrorCode::QueueClosed),
        ];

        for (error, expected_code) in cases {
            assert_eq!(error.code(), expected_code);
            assert_eq!(error.numeric_code(), expected_code.as_u32());
        }
    }

    #[test]
    fn error_display_includes_message() {
        let err = DbError::corruption("test corruption detail");
        let msg = err.to_string();
        assert!(msg.contains("corruption"));
        assert!(msg.contains("test corruption detail"));
    }

    #[test]
    fn diagnostic_schema_omits_absent_optionals_and_is_deterministic() {
        let diagnostic = DbError::sql("generic engine failure").diagnostic();
        assert_eq!(diagnostic.subcode, SUBCODE_SQL_UNKNOWN);
        let actual = diagnostic.to_json().expect("diagnostic json");
        assert_eq!(actual, "{\"version\":1,\"code\":5,\"code_name\":\"ERR_SQL\",\"subcode\":\"sql.unknown\",\"message\":\"generic engine failure\",\"retryable\":false,\"permanent\":true,\"redaction\":\"default\"}");
    }

    #[test]
    fn diagnostic_context_shape_and_details_limits() {
        let context = DbDiagnosticContext::default()
            .with_relation("users")
            .with_column("id")
            .with_detail("timeout_ms", 30.into())
            .with_detail("state", "active".into())
            .with_detail("rejected", json!(["not", "serialized"]))
            .with_detail("a", Value::from(1))
            .with_detail("b", Value::from(2))
            .with_detail("c", Value::from(3))
            .with_detail("d", Value::from(4))
            .with_detail("e", Value::from(5))
            .with_detail("f", Value::from(6))
            .with_detail("g", Value::from(7))
            .with_detail("h", Value::from(8))
            .with_detail("i", Value::from(9))
            .with_detail("j", Value::from(10));

        assert_eq!(context.detail_count(), MAX_DETAIL_KEYS);
        let diagnostic = DbDiagnostic::new(
            DbErrorCode::QueueFull,
            SUBCODE_QUEUE_FULL,
            "test queue full",
            true,
            true,
        )
        .with_context(context);

        let json = diagnostic.to_json().expect("diagnostic json");
        let value = serde_json::from_str::<Value>(&json).expect("diagnostic parsed");
        let details = value
            .get("details")
            .expect("details present")
            .as_object()
            .expect("details object");
        assert_eq!(details.len(), MAX_DETAIL_KEYS);
        assert!(!details.contains_key("rejected"));
    }

    #[test]
    fn diagnostic_subcode_and_retry_permanent_semantics() {
        let syntax = DbError::sql_syntax("invalid syntax");
        let diagnostic = syntax.diagnostic();
        assert_eq!(diagnostic.subcode, SUBCODE_SQL_SYNTAX);
        assert!(!diagnostic.retryable);
        assert!(diagnostic.permanent);

        let busy = DbError::busy("lock busy");
        let diagnostic = busy.diagnostic();
        assert_eq!(diagnostic.subcode, SUBCODE_BUSY_WRITER_LOCK);
        assert!(diagnostic.retryable);
        assert!(diagnostic.permanent);

        let queued = DbError::queue_full("full");
        let diagnostic = queued.diagnostic();
        assert_eq!(diagnostic.subcode, SUBCODE_QUEUE_FULL);
        assert!(diagnostic.retryable);
        assert!(diagnostic.permanent);

        let canceled = DbError::canceled("caller canceled");
        let diagnostic = canceled.diagnostic();
        assert_eq!(diagnostic.subcode, SUBCODE_QUEUE_CANCELED);
        assert!(!diagnostic.retryable);
        assert!(!diagnostic.permanent);
    }

    #[test]
    fn redaction_helpers_hide_sensitive_context() {
        let path =
            DbDiagnosticPath::redacted("/tmp/decentdb/main.ddb", DbDiagnosticPathKind::Database);
        assert_eq!(path.display, "main.ddb");
        assert!(path.fingerprint.is_some());
        assert!(!path
            .fingerprint
            .as_ref()
            .expect("fingerprint")
            .contains('/'));

        let options = DbDiagnosticOpenOptions::from_raw(
            "journal_mode=wal;encryption_key_hex=deadcafe;cache_size=256MB",
        );
        assert_eq!(options.get("encryption_key_hex"), Some(REDACTED_VALUE));

        let audit = DbDiagnosticAuditContext::from_pairs(vec![
            ("principal".to_string(), "alice".to_string()),
            ("sync_token".to_string(), "token-abc".to_string()),
        ]);
        assert_eq!(audit.get("sync_token"), Some(REDACTED_VALUE));

        let token = DbDiagnosticSyncToken::redacted("s3cr3t");
        assert_eq!(token.value(), REDACTED_VALUE);
    }

    #[test]
    fn subcode_helpers_cover_expected_variants() {
        let relation = DbError::sql_relation_not_found("users", "missing relation");
        let relation_diag = relation.diagnostic();
        assert_eq!(relation_diag.subcode, SUBCODE_SQL_RELATION_NOT_FOUND);
        assert_eq!(relation_diag.context.relation.as_deref(), Some("users"));
        assert!(relation_diag.context.details.is_none());

        let unique =
            DbError::constraint_unique("users", "email", "users_email_key", "email duplicate");
        let unique_diag = unique.diagnostic();
        assert_eq!(unique_diag.subcode, SUBCODE_CONSTRAINT_UNIQUE);
        assert_eq!(unique_diag.context.relation.as_deref(), Some("users"));
        assert_eq!(unique_diag.context.column.as_deref(), Some("email"));
        assert_eq!(
            unique_diag.context.constraint.as_deref(),
            Some("users_email_key")
        );
    }
}

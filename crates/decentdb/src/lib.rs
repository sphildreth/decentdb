#![deny(unsafe_op_in_unsafe_fn)]
#![deny(unused_must_use)]
//! DecentDB core engine.
//!
//! Phase 0 establishes the stable top-level API surface and the bootstrap
//! database file format entry points used by later storage slices.

#[cfg(feature = "bench-internals")]
pub mod benchmark;
mod branch;
#[cfg(any(all(target_arch = "wasm32", target_os = "unknown"), test))]
mod browser_result;
mod btree;
mod c_api;
mod catalog;
mod config;
mod db;
mod doctor;
mod error;
mod exec;
mod json;
#[cfg(test)]
mod json_tests;
mod metadata;
mod planner;
mod record;
mod search;
pub(crate) mod spatial;
mod sql;
mod storage;
mod sync;
mod tooling;
mod vfs;
mod wal;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod wasm;

pub use crate::branch::{
    BranchDiffReport, BranchInfo, BranchLogEntry, BranchMergeChange, BranchMergeConflict,
    BranchMergeOperation, BranchMergeReport, BranchRestoreReport, BranchRowDiff, BranchTableDiff,
    BranchTableDiffStatus, NamedSnapshot,
};
pub use crate::config::{DbConfig, WalSyncMode};
pub use crate::db::{evict_shared_wal, Db, PreparedStatement, SqlTransaction};
pub use crate::doctor::{
    render_markdown, run_doctor, sort_findings, DoctorCategory, DoctorCheckSelection,
    DoctorCollectedFacts, DoctorDatabaseSummary, DoctorEvidence, DoctorEvidenceValue,
    DoctorFinding, DoctorFix, DoctorFixStatus, DoctorHighestSeverity, DoctorIndexVerification,
    DoctorMode, DoctorOptions, DoctorPathMode, DoctorRecommendation, DoctorReport, DoctorSeverity,
    DoctorStatus, DoctorSummary,
};
pub use crate::error::{DbError, DbErrorCode, Result};
pub use crate::exec::{BulkLoadOptions, QueryResult, QueryRow};
pub use crate::metadata::{
    CheckConstraintInfo, ColumnInfo, ForeignKeyInfo, HeaderInfo, IndexInfo, IndexVerification,
    QueryContract, QueryParameterInfo, QueryResultColumnInfo, SchemaColumnInfo, SchemaIndexInfo,
    SchemaSnapshot, SchemaTableInfo, SchemaTriggerInfo, SchemaViewInfo, StorageInfo, TableInfo,
    ToolingCapabilities, ToolingColumnTypeMetadata, ToolingMetadata, ToolingSpatialTypeInfo,
    ToolingTypeInfo, TriggerInfo, ViewInfo,
};
pub use crate::record::value::Value;
pub use crate::storage::DB_FORMAT_VERSION;
pub use crate::sync::{
    SyncChangeBatch, SyncConflict, SyncConflictPolicy, SyncConflictPolicyConfig,
    SyncDoctorSeverity, SyncHandshake, SyncImportSummary, SyncJournalIntegrityReport,
    SyncJournalIssue, SyncJournalRecord, SyncOperationalDoctorReport, SyncPeer, SyncPeerLag,
    SyncPeerScopeBinding, SyncPruneSummary, SyncRetentionReport, SyncRunDirection, SyncRunSummary,
    SyncScope, SyncSession, SyncStatus,
};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use crate::wasm::WebDb;

/// Returns the DecentDB crate version.
#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::version;

    #[test]
    fn test_version() {
        assert!(!version().is_empty());
    }
}

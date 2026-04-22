#![deny(unsafe_op_in_unsafe_fn)]
#![deny(unused_must_use)]
//! DecentDB core engine.
//!
//! Phase 0 establishes the stable top-level API surface and the bootstrap
//! database file format entry points used by later storage slices.

mod alloc;
#[cfg(feature = "bench-internals")]
pub mod benchmark;
mod btree;
mod c_api;
mod catalog;
mod config;
mod db;
mod error;
mod exec;
mod json;
#[cfg(test)]
mod json_tests;
mod metadata;
mod planner;
mod record;
mod search;
mod sql;
mod storage;
mod vfs;
mod wal;

pub use crate::config::{DbConfig, WalSyncMode};
pub use crate::db::{evict_shared_wal, Db, PreparedStatement, SqlTransaction};
pub use crate::error::{DbError, DbErrorCode, Result};
pub use crate::exec::{BulkLoadOptions, QueryResult, QueryRow};
pub use crate::metadata::{
    CheckConstraintInfo, ColumnInfo, ForeignKeyInfo, HeaderInfo, IndexInfo, IndexVerification,
    SchemaColumnInfo, SchemaIndexInfo, SchemaSnapshot, SchemaTableInfo, SchemaTriggerInfo,
    SchemaViewInfo, StorageInfo, TableInfo, TriggerInfo, ViewInfo,
};
pub use crate::record::value::Value;
pub use crate::storage::DB_FORMAT_VERSION;

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

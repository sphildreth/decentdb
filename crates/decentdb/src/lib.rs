#![deny(unsafe_op_in_unsafe_fn)]
#![deny(unused_must_use)]
//! DecentDB core engine.
//!
//! Phase 0 establishes the stable top-level API surface and the bootstrap
//! database file format entry points used by later storage slices.

mod btree;
mod catalog;
mod c_api;
mod config;
mod db;
mod error;
mod exec;
mod metadata;
mod planner;
mod record;
mod search;
mod sql;
mod storage;
mod vfs;
mod wal;

pub use crate::config::{DbConfig, WalSyncMode};
pub use crate::db::{evict_shared_wal, Db};
pub use crate::error::{DbError, DbErrorCode, Result};
pub use crate::exec::{BulkLoadOptions, QueryResult, QueryRow};
pub use crate::metadata::{
    ColumnInfo, ForeignKeyInfo, HeaderInfo, IndexInfo, IndexVerification, StorageInfo, TableInfo,
    TriggerInfo, ViewInfo,
};
pub use crate::record::value::Value;

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

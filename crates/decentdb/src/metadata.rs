//! Public metadata and inspection types for the Rust, CLI, and FFI surfaces.

use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageInfo {
    pub path: PathBuf,
    pub wal_path: PathBuf,
    pub page_size: u32,
    pub cache_size_mb: usize,
    pub page_count: u32,
    pub schema_cookie: u32,
    pub wal_end_lsn: u64,
    pub wal_file_size: u64,
    pub last_checkpoint_lsn: u64,
    pub active_readers: usize,
    pub wal_versions: usize,
    pub warning_count: usize,
    pub shared_wal: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderInfo {
    pub magic_hex: String,
    pub format_version: u32,
    pub page_size: u32,
    pub header_checksum: u32,
    pub schema_cookie: u32,
    pub catalog_root_page_id: u32,
    pub freelist_root_page_id: u32,
    pub freelist_head_page_id: u32,
    pub freelist_page_count: u32,
    pub last_checkpoint_lsn: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForeignKeyInfo {
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
    pub on_delete: String,
    pub on_update: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnInfo {
    pub name: String,
    pub column_type: String,
    pub nullable: bool,
    pub default_sql: Option<String>,
    pub primary_key: bool,
    pub unique: bool,
    pub auto_increment: bool,
    pub checks: Vec<String>,
    pub foreign_key: Option<ForeignKeyInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableInfo {
    pub name: String,
    pub temporary: bool,
    pub columns: Vec<ColumnInfo>,
    pub checks: Vec<String>,
    pub foreign_keys: Vec<ForeignKeyInfo>,
    pub primary_key_columns: Vec<String>,
    pub row_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexInfo {
    pub name: String,
    pub table_name: String,
    pub kind: String,
    pub unique: bool,
    pub columns: Vec<String>,
    pub include_columns: Vec<String>,
    pub predicate_sql: Option<String>,
    pub fresh: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ViewInfo {
    pub name: String,
    pub temporary: bool,
    pub sql_text: String,
    pub column_names: Vec<String>,
    pub dependencies: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TriggerInfo {
    pub name: String,
    pub target_name: String,
    pub kind: String,
    pub event: String,
    pub on_view: bool,
    pub action_sql: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexVerification {
    pub name: String,
    pub valid: bool,
    pub expected_entries: usize,
    pub actual_entries: usize,
}

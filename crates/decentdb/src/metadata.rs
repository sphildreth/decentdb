//! Public metadata and inspection types for the Rust, CLI, and FFI surfaces.

use std::path::PathBuf;

use serde::Serialize;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageInfo {
    pub path: PathBuf,
    pub wal_path: PathBuf,
    pub format_version: u32,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ForeignKeyInfo {
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
    pub on_delete: String,
    pub on_update: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TableInfo {
    pub name: String,
    pub temporary: bool,
    pub columns: Vec<ColumnInfo>,
    pub checks: Vec<String>,
    pub foreign_keys: Vec<ForeignKeyInfo>,
    pub primary_key_columns: Vec<String>,
    pub row_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ViewInfo {
    pub name: String,
    pub temporary: bool,
    pub sql_text: String,
    pub column_names: Vec<String>,
    pub dependencies: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CheckConstraintInfo {
    pub name: Option<String>,
    pub expression_sql: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SchemaColumnInfo {
    pub name: String,
    pub column_type: String,
    pub nullable: bool,
    pub default_sql: Option<String>,
    pub primary_key: bool,
    pub unique: bool,
    pub auto_increment: bool,
    pub generated_sql: Option<String>,
    pub generated_stored: bool,
    pub checks: Vec<CheckConstraintInfo>,
    pub foreign_key: Option<ForeignKeyInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SchemaTableInfo {
    pub name: String,
    pub temporary: bool,
    pub ddl: String,
    pub row_count: usize,
    pub primary_key_columns: Vec<String>,
    pub checks: Vec<CheckConstraintInfo>,
    pub foreign_keys: Vec<ForeignKeyInfo>,
    pub columns: Vec<SchemaColumnInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SchemaViewInfo {
    pub name: String,
    pub temporary: bool,
    pub sql_text: String,
    pub column_names: Vec<String>,
    pub dependencies: Vec<String>,
    pub ddl: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SchemaIndexInfo {
    pub name: String,
    pub table_name: String,
    pub kind: String,
    pub unique: bool,
    pub columns: Vec<String>,
    pub include_columns: Vec<String>,
    pub predicate_sql: Option<String>,
    pub fresh: bool,
    pub temporary: bool,
    pub ddl: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SchemaTriggerInfo {
    pub name: String,
    pub target_name: String,
    pub target_kind: String,
    pub timing: String,
    pub events: Vec<String>,
    pub events_mask: u32,
    pub for_each_row: bool,
    pub temporary: bool,
    pub action_sql: String,
    pub ddl: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SchemaSnapshot {
    pub snapshot_version: u32,
    pub schema_cookie: u32,
    pub tables: Vec<SchemaTableInfo>,
    pub views: Vec<SchemaViewInfo>,
    pub indexes: Vec<SchemaIndexInfo>,
    pub triggers: Vec<SchemaTriggerInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ToolingMetadata {
    pub metadata_version: u32,
    pub engine_version: String,
    pub database_format_version: u32,
    pub schema_cookie: u32,
    pub temp_schema_cookie: u32,
    pub schema_fingerprint: String,
    pub schema_fingerprint_algorithm: String,
    pub schema: SchemaSnapshot,
    pub column_type_metadata: Vec<ToolingColumnTypeMetadata>,
    pub capabilities: ToolingCapabilities,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ToolingCapabilities {
    pub query_contract_version: u32,
    pub query_describe: bool,
    pub deterministic_json: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ToolingColumnTypeMetadata {
    pub table_name: String,
    pub column_name: String,
    pub column_type: String,
    pub type_info: ToolingTypeInfo,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ToolingTypeInfo {
    pub type_name: String,
    pub value_kind: String,
    pub c_value_tag: u32,
    pub spatial: Option<ToolingSpatialTypeInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ToolingSpatialTypeInfo {
    pub subtype: String,
    pub dimensions: String,
    pub srid: i32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QueryContract {
    pub contract_version: u32,
    pub sql: String,
    pub statement_kind: String,
    pub read_only: bool,
    pub schema_cookie: u32,
    pub temp_schema_cookie: u32,
    pub schema_fingerprint: String,
    pub parameters: Vec<QueryParameterInfo>,
    pub result_columns: Vec<QueryResultColumnInfo>,
    pub diagnostics: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QueryParameterInfo {
    pub position: usize,
    pub name: String,
    pub type_name: Option<String>,
    pub nullable: Option<bool>,
    pub source: String,
    pub source_table: Option<String>,
    pub source_column: Option<String>,
    pub diagnostics: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QueryResultColumnInfo {
    pub ordinal: usize,
    pub name: String,
    pub type_name: Option<String>,
    pub nullable: Option<bool>,
    pub source: String,
    pub source_table: Option<String>,
    pub source_column: Option<String>,
    pub expression_sql: Option<String>,
    pub diagnostics: Vec<String>,
}

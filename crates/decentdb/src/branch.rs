#![allow(dead_code)] // Branch catalog metadata keeps diagnostic fields used by API/JSON surfaces.

use serde::{Deserialize, Serialize};

use crate::db::Db;
use crate::error::{DbError, Result};
use crate::record::value::Value;
use crate::sync::current_time_micros;

pub(crate) const DEFAULT_BRANCH_NAME: &str = "main";
pub(crate) const DEFAULT_BRANCH_ID: &str = "branch:main";
pub(crate) const DEFAULT_HEAD_ID: &str = "head:main:bootstrap";
pub(crate) const DEFAULT_ROOT_MANIFEST_ID: &str = "root:main:bootstrap";

pub(crate) const BRANCHES_TABLE: &str = "__decentdb_branches";
pub(crate) const BRANCH_HEADS_TABLE: &str = "__decentdb_branch_heads";
pub(crate) const SNAPSHOTS_TABLE: &str = "__decentdb_snapshots";
pub(crate) const ROOT_MANIFESTS_TABLE: &str = "__decentdb_root_manifests";
pub(crate) const BRANCH_SQL_LOG_TABLE: &str = "__decentdb_branch_sql_log";

const BRANCHES_TABLE_DDL: &str = "CREATE TABLE IF NOT EXISTS __decentdb_branches (branch_id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, current_head_id TEXT, base_head_id TEXT, created_at_micros INT64 NOT NULL, updated_at_micros INT64 NOT NULL, deleted_at_micros INT64)";
const BRANCH_HEADS_TABLE_DDL: &str = "CREATE TABLE IF NOT EXISTS __decentdb_branch_heads (head_id TEXT PRIMARY KEY, branch_id TEXT NOT NULL, parent_head_id TEXT, root_manifest_id TEXT NOT NULL, commit_lsn INT64 NOT NULL, message TEXT, created_at_micros INT64 NOT NULL)";
const SNAPSHOTS_TABLE_DDL: &str = "CREATE TABLE IF NOT EXISTS __decentdb_snapshots (snapshot_id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, branch_id TEXT NOT NULL, head_id TEXT NOT NULL, snapshot_lsn INT64 NOT NULL, created_at_micros INT64 NOT NULL)";
const ROOT_MANIFESTS_TABLE_DDL: &str = "CREATE TABLE IF NOT EXISTS __decentdb_root_manifests (manifest_id TEXT PRIMARY KEY, schema_cookie INT64 NOT NULL, catalog_root_page_id INT64, table_roots_json TEXT NOT NULL, index_roots_json TEXT NOT NULL, sequence_state_json TEXT NOT NULL, metadata_version INT64 NOT NULL, commit_lsn INT64 NOT NULL, created_at_micros INT64 NOT NULL)";
const BRANCH_SQL_LOG_TABLE_DDL: &str = "CREATE TABLE IF NOT EXISTS __decentdb_branch_sql_log (log_id TEXT PRIMARY KEY, head_id TEXT NOT NULL UNIQUE, branch_id TEXT NOT NULL, sequence INT64 NOT NULL, sql TEXT NOT NULL, created_at_micros INT64 NOT NULL)";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BranchInfo {
    pub branch_id: String,
    pub name: String,
    pub current_head_id: Option<String>,
    pub base_head_id: Option<String>,
    pub created_at_micros: i64,
    pub updated_at_micros: i64,
    pub deleted_at_micros: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct BranchHeadMetadata {
    pub head_id: String,
    pub branch_id: String,
    pub parent_head_id: Option<String>,
    pub root_manifest_id: String,
    pub commit_lsn: i64,
    pub message: Option<String>,
    pub created_at_micros: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BranchSqlLogEntry {
    pub head_id: String,
    pub branch_id: String,
    pub sequence: i64,
    pub sql: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BranchLogEntry {
    pub head_id: String,
    pub branch_id: String,
    pub parent_head_id: Option<String>,
    pub message: Option<String>,
    pub created_at_micros: i64,
    pub sql: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BranchDiffReport {
    pub left_ref: String,
    pub right_ref: String,
    pub table_count: usize,
    pub changed_table_count: usize,
    pub added_row_count: usize,
    pub updated_row_count: usize,
    pub deleted_row_count: usize,
    pub tables: Vec<BranchTableDiff>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BranchTableDiff {
    pub table: String,
    pub status: BranchTableDiffStatus,
    pub schema_changed: bool,
    pub added: Vec<BranchRowDiff>,
    pub updated: Vec<BranchRowDiff>,
    pub deleted: Vec<BranchRowDiff>,
    pub message: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BranchTableDiffStatus {
    Unchanged,
    Added,
    Removed,
    Changed,
    Unsupported,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BranchRowDiff {
    pub primary_key: Vec<String>,
    pub before: Option<Vec<String>>,
    pub after: Option<Vec<String>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BranchRestoreReport {
    pub branch: String,
    pub target_ref: String,
    pub dry_run: bool,
    pub previous_head_id: Option<String>,
    pub target_head_id: String,
    pub new_head_id: Option<String>,
    pub changed_table_count: usize,
    pub added_row_count: usize,
    pub updated_row_count: usize,
    pub deleted_row_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BranchMergeReport {
    pub source: String,
    pub target: String,
    pub dry_run: bool,
    pub clean: bool,
    pub base_head_id: String,
    pub table_count: usize,
    pub applied_change_count: usize,
    pub conflict_count: usize,
    pub applied: Vec<BranchMergeChange>,
    pub conflicts: Vec<BranchMergeConflict>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BranchMergeChange {
    pub table: String,
    pub primary_key: Vec<String>,
    pub operation: BranchMergeOperation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BranchMergeOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BranchMergeConflict {
    pub table: String,
    pub primary_key: Vec<String>,
    pub conflict_type: String,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct RootManifestPrototype {
    pub manifest_id: String,
    pub schema_cookie: u32,
    pub catalog_root_page_id: Option<i64>,
    pub table_roots_json: String,
    pub index_roots_json: String,
    pub sequence_state_json: String,
    pub metadata_version: i64,
    pub commit_lsn: i64,
    pub created_at_micros: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NamedSnapshot {
    pub snapshot_id: String,
    pub name: String,
    pub branch_id: String,
    pub head_id: String,
    pub snapshot_lsn: u64,
    pub created_at_micros: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BranchBootstrapSummary {
    pub created: bool,
    pub branch: BranchInfo,
    pub head: BranchHeadMetadata,
    pub root_manifest: RootManifestPrototype,
}

pub(crate) fn ensure_branch_catalog(db: &Db) -> Result<()> {
    for ddl in [
        BRANCHES_TABLE_DDL,
        BRANCH_HEADS_TABLE_DDL,
        SNAPSHOTS_TABLE_DDL,
        ROOT_MANIFESTS_TABLE_DDL,
        BRANCH_SQL_LOG_TABLE_DDL,
    ] {
        db.execute(ddl)?;
    }
    Ok(())
}

pub(crate) fn bootstrap_main_branch(db: &Db) -> Result<BranchBootstrapSummary> {
    ensure_branch_catalog(db)?;
    if let Some(branch) = branch_by_name(db, DEFAULT_BRANCH_NAME)? {
        let current_head_id = branch
            .current_head_id
            .as_deref()
            .ok_or_else(|| DbError::internal("main branch bootstrap is missing a current head"))?;
        let head = branch_head_by_id(db, current_head_id)?.ok_or_else(|| {
            DbError::internal("main branch bootstrap references a missing branch head")
        })?;
        let root_manifest = root_manifest_by_id(db, &head.root_manifest_id)?.ok_or_else(|| {
            DbError::internal("main branch bootstrap references a missing root manifest")
        })?;
        return Ok(BranchBootstrapSummary {
            created: false,
            branch,
            head,
            root_manifest,
        });
    }

    let created_at_micros = current_time_micros();
    let schema_cookie = db.schema_cookie()?;
    let manifest = RootManifestPrototype {
        manifest_id: DEFAULT_ROOT_MANIFEST_ID.to_string(),
        schema_cookie,
        catalog_root_page_id: None,
        table_roots_json: "{}".to_string(),
        index_roots_json: "{}".to_string(),
        sequence_state_json: "{}".to_string(),
        metadata_version: 1,
        commit_lsn: 0,
        created_at_micros,
    };
    let head = BranchHeadMetadata {
        head_id: DEFAULT_HEAD_ID.to_string(),
        branch_id: DEFAULT_BRANCH_ID.to_string(),
        parent_head_id: None,
        root_manifest_id: manifest.manifest_id.clone(),
        commit_lsn: manifest.commit_lsn,
        message: Some("bootstrap main branch".to_string()),
        created_at_micros,
    };
    let branch = BranchInfo {
        branch_id: DEFAULT_BRANCH_ID.to_string(),
        name: DEFAULT_BRANCH_NAME.to_string(),
        current_head_id: Some(head.head_id.clone()),
        base_head_id: None,
        created_at_micros,
        updated_at_micros: created_at_micros,
        deleted_at_micros: None,
    };

    insert_root_manifest(db, &manifest)?;
    insert_branch_head(db, &head)?;
    insert_branch(db, &branch)?;

    Ok(BranchBootstrapSummary {
        created: true,
        branch,
        head,
        root_manifest: manifest,
    })
}

pub(crate) fn list_branches(db: &Db) -> Result<Vec<BranchInfo>> {
    ensure_branch_catalog(db)?;
    let result = db.execute(&format!(
        "SELECT branch_id, name, current_head_id, base_head_id, created_at_micros, updated_at_micros, deleted_at_micros FROM {BRANCHES_TABLE} WHERE deleted_at_micros IS NULL ORDER BY name"
    ))?;
    result.rows().iter().map(branch_info_from_row).collect()
}

pub(crate) fn main_branch_head(db: &Db) -> Result<Option<BranchHeadMetadata>> {
    ensure_branch_catalog(db)?;
    let Some(branch) = branch_by_name(db, DEFAULT_BRANCH_NAME)? else {
        return Ok(None);
    };
    let Some(head_id) = branch.current_head_id else {
        return Ok(None);
    };
    branch_head_by_id(db, &head_id)
}

pub(crate) fn create_branch(
    db: &Db,
    name: &str,
    source_lsn: u64,
    schema_cookie: u32,
    parent_head_id: Option<&str>,
) -> Result<BranchInfo> {
    let name = validate_user_name(name, "branch")?;
    if name == DEFAULT_BRANCH_NAME {
        return Err(DbError::sql("branch 'main' already exists"));
    }
    bootstrap_main_branch(db)?;
    if branch_by_name(db, name)?.is_some() {
        return Err(DbError::sql(format!("branch '{name}' already exists")));
    }

    let created_at_micros = current_time_micros();
    let source_lsn_i64 = u64_to_i64(source_lsn, "source_lsn")?;
    let id_suffix = format!("{}:{created_at_micros}", stable_id_component(name));
    let manifest = RootManifestPrototype {
        manifest_id: format!("root:branch:{id_suffix}"),
        schema_cookie,
        catalog_root_page_id: None,
        table_roots_json: "{}".to_string(),
        index_roots_json: "{}".to_string(),
        sequence_state_json: "{}".to_string(),
        metadata_version: 1,
        commit_lsn: source_lsn_i64,
        created_at_micros,
    };
    let head = BranchHeadMetadata {
        head_id: format!("head:branch:{id_suffix}"),
        branch_id: format!("branch:{id_suffix}"),
        parent_head_id: parent_head_id.map(ToString::to_string),
        root_manifest_id: manifest.manifest_id.clone(),
        commit_lsn: source_lsn_i64,
        message: Some(format!("create branch {name}")),
        created_at_micros,
    };
    let branch = BranchInfo {
        branch_id: head.branch_id.clone(),
        name: name.to_string(),
        current_head_id: Some(head.head_id.clone()),
        base_head_id: Some(head.head_id.clone()),
        created_at_micros,
        updated_at_micros: created_at_micros,
        deleted_at_micros: None,
    };

    insert_root_manifest(db, &manifest)?;
    insert_branch_head(db, &head)?;
    insert_branch(db, &branch)?;
    Ok(branch)
}

pub(crate) fn branch_lsn_by_name(db: &Db, name: &str) -> Result<Option<u64>> {
    ensure_branch_catalog(db)?;
    let Some(branch) = branch_by_name(db, name)? else {
        return Ok(None);
    };
    let Some(head_id) = branch.current_head_id else {
        return Ok(None);
    };
    branch_head_lsn_by_id(db, &head_id)
}

pub(crate) fn delete_branch(db: &Db, name: &str) -> Result<bool> {
    ensure_branch_catalog(db)?;
    let name = validate_user_name(name, "branch")?;
    if name == DEFAULT_BRANCH_NAME {
        return Err(DbError::sql("cannot delete the main branch"));
    }
    let result = db.execute(&format!(
        "DELETE FROM {BRANCHES_TABLE} WHERE name = {}",
        sql_text_literal(name)
    ))?;
    Ok(result.affected_rows() > 0)
}

pub(crate) fn rename_branch(db: &Db, old_name: &str, new_name: &str) -> Result<bool> {
    ensure_branch_catalog(db)?;
    let old_name = validate_user_name(old_name, "branch")?;
    let new_name = validate_user_name(new_name, "branch")?;
    if old_name == DEFAULT_BRANCH_NAME {
        return Err(DbError::sql("cannot rename the main branch"));
    }
    if new_name == DEFAULT_BRANCH_NAME {
        return Err(DbError::sql("branch 'main' already exists"));
    }
    if branch_by_name(db, new_name)?.is_some() {
        return Err(DbError::sql(format!("branch '{new_name}' already exists")));
    }
    let updated_at_micros = current_time_micros();
    let result = db.execute(&format!(
        "UPDATE {BRANCHES_TABLE} SET name = {new_name}, updated_at_micros = {updated_at_micros} WHERE name = {old_name}",
        new_name = sql_text_literal(new_name),
        old_name = sql_text_literal(old_name),
    ))?;
    Ok(result.affected_rows() > 0)
}

pub(crate) fn branch_sql_log(db: &Db, branch_id: &str) -> Result<Vec<BranchSqlLogEntry>> {
    ensure_branch_catalog(db)?;
    let result = db.execute(&format!(
        "SELECT head_id, branch_id, sequence, sql FROM {BRANCH_SQL_LOG_TABLE} WHERE branch_id = {} ORDER BY sequence",
        sql_text_literal(branch_id)
    ))?;
    result
        .rows()
        .iter()
        .map(branch_sql_log_entry_from_row)
        .collect()
}

pub(crate) fn branch_sql_log_for_head(
    db: &Db,
    current_head_id: &str,
) -> Result<Vec<BranchSqlLogEntry>> {
    ensure_branch_catalog(db)?;
    let mut entries = Vec::new();
    let mut next_head_id = Some(current_head_id.to_string());
    while let Some(head_id) = next_head_id {
        let head = branch_head_by_id(db, &head_id)?
            .ok_or_else(|| DbError::corruption(format!("branch head '{head_id}' is missing")))?;
        if let Some(entry) = branch_sql_log_entry_by_head_id(db, &head.head_id)? {
            entries.push(entry);
        }
        next_head_id = head.parent_head_id;
    }
    entries.reverse();
    Ok(entries)
}

pub(crate) fn branch_log(db: &Db, branch_name: &str) -> Result<Vec<BranchLogEntry>> {
    ensure_branch_catalog(db)?;
    let branch = branch_by_name(db, branch_name)?
        .ok_or_else(|| DbError::transaction(format!("unknown branch '{branch_name}'")))?;
    let mut entries = Vec::new();
    let mut next_head_id = branch.current_head_id.clone();
    while let Some(head_id) = next_head_id {
        let head = branch_head_by_id(db, &head_id)?
            .ok_or_else(|| DbError::corruption(format!("branch head '{head_id}' is missing")))?;
        let sql = branch_sql_log_entry_by_head_id(db, &head.head_id)?.map(|entry| entry.sql);
        next_head_id = head.parent_head_id.clone();
        entries.push(BranchLogEntry {
            head_id: head.head_id,
            branch_id: head.branch_id,
            parent_head_id: head.parent_head_id,
            message: head.message,
            created_at_micros: head.created_at_micros,
            sql,
        });
    }
    Ok(entries)
}

pub(crate) fn commit_branch(
    db: &Db,
    branch: &BranchInfo,
    message: &str,
) -> Result<BranchHeadMetadata> {
    ensure_branch_catalog(db)?;
    let message = validate_message(message)?;
    let current_head_id = branch
        .current_head_id
        .as_deref()
        .ok_or_else(|| DbError::internal("branch is missing a current head"))?;
    let current_head = branch_head_by_id(db, current_head_id)?
        .ok_or_else(|| DbError::internal("branch current head is missing"))?;
    let created_at_micros = current_time_micros();
    let head_id = format!(
        "head:branch-commit:{}:{created_at_micros}",
        stable_id_component(&branch.name)
    );
    let head = BranchHeadMetadata {
        head_id,
        branch_id: branch.branch_id.clone(),
        parent_head_id: Some(current_head.head_id),
        root_manifest_id: current_head.root_manifest_id,
        commit_lsn: current_head.commit_lsn,
        message: Some(message.to_string()),
        created_at_micros,
    };
    let batch = format!(
        "BEGIN; INSERT INTO {heads_table} (head_id, branch_id, parent_head_id, root_manifest_id, commit_lsn, message, created_at_micros) VALUES ({head_id}, {branch_id}, {parent_head_id}, {root_manifest_id}, {commit_lsn}, {message}, {created_at_micros}); UPDATE {branches_table} SET current_head_id = {head_id}, updated_at_micros = {created_at_micros} WHERE branch_id = {branch_id}; COMMIT",
        heads_table = BRANCH_HEADS_TABLE,
        branches_table = BRANCHES_TABLE,
        head_id = sql_text_literal(&head.head_id),
        branch_id = sql_text_literal(&head.branch_id),
        parent_head_id = sql_nullable_text_literal(head.parent_head_id.as_deref()),
        root_manifest_id = sql_text_literal(&head.root_manifest_id),
        commit_lsn = head.commit_lsn,
        message = sql_nullable_text_literal(head.message.as_deref()),
        created_at_micros = head.created_at_micros,
    );
    db.execute_batch(&batch)?;
    branch_head_by_id(db, &head.head_id)?
        .ok_or_else(|| DbError::internal("inserted branch commit head is missing"))
}

pub(crate) fn restore_branch_head(
    db: &Db,
    branch: &BranchInfo,
    target_head: &BranchHeadMetadata,
    target_ref: &str,
) -> Result<BranchHeadMetadata> {
    ensure_branch_catalog(db)?;
    let current_head_id = branch
        .current_head_id
        .as_deref()
        .ok_or_else(|| DbError::internal("branch is missing a current head"))?;
    let created_at_micros = current_time_micros();
    let head_id = format!(
        "head:branch-restore:{}:{created_at_micros}",
        stable_id_component(&branch.name)
    );
    let head = BranchHeadMetadata {
        head_id,
        branch_id: branch.branch_id.clone(),
        parent_head_id: Some(target_head.head_id.clone()),
        root_manifest_id: target_head.root_manifest_id.clone(),
        commit_lsn: target_head.commit_lsn,
        message: Some(format!("restore {} to {target_ref}", branch.name)),
        created_at_micros,
    };
    let batch = format!(
        "BEGIN; INSERT INTO {heads_table} (head_id, branch_id, parent_head_id, root_manifest_id, commit_lsn, message, created_at_micros) VALUES ({head_id}, {branch_id}, {parent_head_id}, {root_manifest_id}, {commit_lsn}, {message}, {created_at_micros}); UPDATE {branches_table} SET current_head_id = {head_id}, updated_at_micros = {created_at_micros} WHERE branch_id = {branch_id} AND current_head_id = {current_head_id}; COMMIT",
        heads_table = BRANCH_HEADS_TABLE,
        branches_table = BRANCHES_TABLE,
        head_id = sql_text_literal(&head.head_id),
        branch_id = sql_text_literal(&head.branch_id),
        parent_head_id = sql_nullable_text_literal(head.parent_head_id.as_deref()),
        root_manifest_id = sql_text_literal(&head.root_manifest_id),
        commit_lsn = head.commit_lsn,
        message = sql_nullable_text_literal(head.message.as_deref()),
        created_at_micros = head.created_at_micros,
        current_head_id = sql_text_literal(current_head_id),
    );
    db.execute_batch(&batch)?;
    branch_head_by_id(db, &head.head_id)?
        .ok_or_else(|| DbError::internal("inserted branch restore head is missing"))
}

pub(crate) fn append_branch_sql_log(
    db: &Db,
    branch: &BranchInfo,
    sql: &str,
) -> Result<BranchHeadMetadata> {
    ensure_branch_catalog(db)?;
    let current_head_id = branch
        .current_head_id
        .as_deref()
        .ok_or_else(|| DbError::internal("branch is missing a current head"))?;
    let current_head = branch_head_by_id(db, current_head_id)?
        .ok_or_else(|| DbError::internal("branch current head is missing"))?;
    let log = branch_sql_log(db, &branch.branch_id)?;
    let sequence = log
        .last()
        .map_or(1, |entry| entry.sequence.saturating_add(1));
    let created_at_micros = current_time_micros();
    let head_id = format!(
        "head:branch-log:{}:{created_at_micros}",
        stable_id_component(&branch.name)
    );
    let log_id = format!("log:{}:{sequence}", branch.branch_id);
    let message = format!("branch SQL batch {sequence}");
    let batch = format!(
        "BEGIN; INSERT INTO {heads_table} (head_id, branch_id, parent_head_id, root_manifest_id, commit_lsn, message, created_at_micros) VALUES ({head_id}, {branch_id}, {parent_head_id}, {root_manifest_id}, {commit_lsn}, {message}, {created_at_micros}); INSERT INTO {log_table} (log_id, head_id, branch_id, sequence, sql, created_at_micros) VALUES ({log_id}, {head_id}, {branch_id}, {sequence}, {sql}, {created_at_micros}); UPDATE {branches_table} SET current_head_id = {head_id}, updated_at_micros = {created_at_micros} WHERE branch_id = {branch_id}; COMMIT",
        log_table = BRANCH_SQL_LOG_TABLE,
        heads_table = BRANCH_HEADS_TABLE,
        branches_table = BRANCHES_TABLE,
        log_id = sql_text_literal(&log_id),
        branch_id = sql_text_literal(&branch.branch_id),
        sequence = sequence,
        sql = sql_text_literal(sql),
        created_at_micros = created_at_micros,
        head_id = sql_text_literal(&head_id),
        parent_head_id = sql_text_literal(&current_head.head_id),
        root_manifest_id = sql_text_literal(&current_head.root_manifest_id),
        commit_lsn = current_head.commit_lsn,
        message = sql_text_literal(&message),
    );
    db.execute_batch(&batch)?;
    branch_head_by_id(db, &head_id)?
        .ok_or_else(|| DbError::internal("inserted branch head is missing"))
}

pub(crate) fn create_named_snapshot(
    db: &Db,
    name: &str,
    snapshot_lsn: u64,
    schema_cookie: u32,
) -> Result<NamedSnapshot> {
    let name = validate_user_name(name, "snapshot")?;
    let bootstrap = bootstrap_main_branch(db)?;
    if snapshot_by_name(db, name)?.is_some() {
        return Err(DbError::sql(format!("snapshot '{name}' already exists")));
    }

    let created_at_micros = current_time_micros();
    let snapshot_lsn_i64 = u64_to_i64(snapshot_lsn, "snapshot_lsn")?;
    let id_suffix = format!("{}:{created_at_micros}", stable_id_component(name));
    let manifest = RootManifestPrototype {
        manifest_id: format!("root:main:snapshot:{id_suffix}"),
        schema_cookie,
        catalog_root_page_id: None,
        table_roots_json: "{}".to_string(),
        index_roots_json: "{}".to_string(),
        sequence_state_json: "{}".to_string(),
        metadata_version: 1,
        commit_lsn: snapshot_lsn_i64,
        created_at_micros,
    };
    let head = BranchHeadMetadata {
        head_id: format!("head:main:snapshot:{id_suffix}"),
        branch_id: DEFAULT_BRANCH_ID.to_string(),
        parent_head_id: bootstrap.branch.current_head_id.clone(),
        root_manifest_id: manifest.manifest_id.clone(),
        commit_lsn: snapshot_lsn_i64,
        message: Some(format!("snapshot {name}")),
        created_at_micros,
    };
    let snapshot = NamedSnapshot {
        snapshot_id: format!("snapshot:main:{id_suffix}"),
        name: name.to_string(),
        branch_id: DEFAULT_BRANCH_ID.to_string(),
        head_id: head.head_id.clone(),
        snapshot_lsn,
        created_at_micros,
    };

    insert_root_manifest(db, &manifest)?;
    insert_branch_head(db, &head)?;
    insert_snapshot(db, &snapshot)?;

    Ok(snapshot)
}

pub(crate) fn list_named_snapshots(db: &Db) -> Result<Vec<NamedSnapshot>> {
    ensure_branch_catalog(db)?;
    let result = db.execute(&format!(
        "SELECT snapshot_id, name, branch_id, head_id, snapshot_lsn, created_at_micros FROM {SNAPSHOTS_TABLE} ORDER BY created_at_micros, name"
    ))?;
    result.rows().iter().map(snapshot_from_row).collect()
}

pub(crate) fn snapshot_by_name(db: &Db, name: &str) -> Result<Option<NamedSnapshot>> {
    ensure_branch_catalog(db)?;
    let result = db.execute(&format!(
        "SELECT snapshot_id, name, branch_id, head_id, snapshot_lsn, created_at_micros FROM {SNAPSHOTS_TABLE} WHERE name = {}",
        sql_text_literal(name)
    ))?;
    result.rows().first().map(snapshot_from_row).transpose()
}

pub(crate) fn branch_head_lsn_by_id(db: &Db, head_id: &str) -> Result<Option<u64>> {
    ensure_branch_catalog(db)?;
    branch_head_by_id(db, head_id)?
        .map(|head| i64_to_u64(head.commit_lsn, "commit_lsn"))
        .transpose()
}

pub(crate) fn delete_named_snapshot(db: &Db, name: &str) -> Result<bool> {
    ensure_branch_catalog(db)?;
    let name = validate_user_name(name, "snapshot")?;
    let result = db.execute(&format!(
        "DELETE FROM {SNAPSHOTS_TABLE} WHERE name = {}",
        sql_text_literal(name)
    ))?;
    Ok(result.affected_rows() > 0)
}

pub(crate) fn retained_snapshot_lsn(db: &Db) -> Result<Option<u64>> {
    let mut retained_lsn = None;
    if !db.internal_table_exists(SNAPSHOTS_TABLE)? {
        // Continue below: active non-main branches can also retain history.
    } else {
        let result = db.execute(&format!(
            "SELECT snapshot_lsn FROM {SNAPSHOTS_TABLE} ORDER BY snapshot_lsn LIMIT 1"
        ))?;
        retained_lsn = result
            .rows()
            .first()
            .map(|row| {
                let value = expect_i64(row.values().first(), "snapshot_lsn")?;
                i64_to_u64(value, "snapshot_lsn")
            })
            .transpose()?;
    }
    if db.internal_table_exists(BRANCHES_TABLE)? && db.internal_table_exists(BRANCH_HEADS_TABLE)? {
        let result = db.execute(&format!(
            "SELECT h.commit_lsn FROM {BRANCHES_TABLE} b, {BRANCH_HEADS_TABLE} h WHERE b.current_head_id = h.head_id AND b.name != {main} ORDER BY h.commit_lsn LIMIT 1",
            main = sql_text_literal(DEFAULT_BRANCH_NAME),
        ))?;
        let branch_lsn = result
            .rows()
            .first()
            .map(|row| {
                let value = expect_i64(row.values().first(), "commit_lsn")?;
                i64_to_u64(value, "commit_lsn")
            })
            .transpose()?;
        retained_lsn = match (retained_lsn, branch_lsn) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        };
    }
    Ok(retained_lsn)
}

pub(crate) fn named_snapshot_count(db: &Db) -> Result<usize> {
    if !db.internal_table_exists(SNAPSHOTS_TABLE)? {
        return Ok(0);
    }
    let result = db.execute(&format!("SELECT COUNT(*) FROM {SNAPSHOTS_TABLE}"))?;
    let count = result
        .rows()
        .first()
        .map(|row| expect_i64(row.values().first(), "snapshot_count"))
        .transpose()?
        .unwrap_or(0);
    usize::try_from(count)
        .map_err(|_| DbError::corruption("snapshot_count must be non-negative in branch catalog"))
}

fn insert_branch(db: &Db, branch: &BranchInfo) -> Result<()> {
    db.execute(&format!(
        "INSERT INTO {BRANCHES_TABLE} (branch_id, name, current_head_id, base_head_id, created_at_micros, updated_at_micros, deleted_at_micros) VALUES ({branch_id}, {name}, {current_head_id}, {base_head_id}, {created_at_micros}, {updated_at_micros}, {deleted_at_micros})",
        branch_id = sql_text_literal(&branch.branch_id),
        name = sql_text_literal(&branch.name),
        current_head_id = sql_nullable_text_literal(branch.current_head_id.as_deref()),
        base_head_id = sql_nullable_text_literal(branch.base_head_id.as_deref()),
        created_at_micros = branch.created_at_micros,
        updated_at_micros = branch.updated_at_micros,
        deleted_at_micros = sql_nullable_i64_literal(branch.deleted_at_micros),
    ))?;
    Ok(())
}

fn insert_branch_head(db: &Db, head: &BranchHeadMetadata) -> Result<()> {
    db.execute(&format!(
        "INSERT INTO {BRANCH_HEADS_TABLE} (head_id, branch_id, parent_head_id, root_manifest_id, commit_lsn, message, created_at_micros) VALUES ({head_id}, {branch_id}, {parent_head_id}, {root_manifest_id}, {commit_lsn}, {message}, {created_at_micros})",
        head_id = sql_text_literal(&head.head_id),
        branch_id = sql_text_literal(&head.branch_id),
        parent_head_id = sql_nullable_text_literal(head.parent_head_id.as_deref()),
        root_manifest_id = sql_text_literal(&head.root_manifest_id),
        commit_lsn = head.commit_lsn,
        message = sql_nullable_text_literal(head.message.as_deref()),
        created_at_micros = head.created_at_micros,
    ))?;
    Ok(())
}

fn insert_root_manifest(db: &Db, manifest: &RootManifestPrototype) -> Result<()> {
    db.execute(&format!(
        "INSERT INTO {ROOT_MANIFESTS_TABLE} (manifest_id, schema_cookie, catalog_root_page_id, table_roots_json, index_roots_json, sequence_state_json, metadata_version, commit_lsn, created_at_micros) VALUES ({manifest_id}, {schema_cookie}, {catalog_root_page_id}, {table_roots_json}, {index_roots_json}, {sequence_state_json}, {metadata_version}, {commit_lsn}, {created_at_micros})",
        manifest_id = sql_text_literal(&manifest.manifest_id),
        schema_cookie = manifest.schema_cookie,
        catalog_root_page_id = sql_nullable_i64_literal(manifest.catalog_root_page_id),
        table_roots_json = sql_text_literal(&manifest.table_roots_json),
        index_roots_json = sql_text_literal(&manifest.index_roots_json),
        sequence_state_json = sql_text_literal(&manifest.sequence_state_json),
        metadata_version = manifest.metadata_version,
        commit_lsn = manifest.commit_lsn,
        created_at_micros = manifest.created_at_micros,
    ))?;
    Ok(())
}

fn insert_snapshot(db: &Db, snapshot: &NamedSnapshot) -> Result<()> {
    db.execute(&format!(
        "INSERT INTO {SNAPSHOTS_TABLE} (snapshot_id, name, branch_id, head_id, snapshot_lsn, created_at_micros) VALUES ({snapshot_id}, {name}, {branch_id}, {head_id}, {snapshot_lsn}, {created_at_micros})",
        snapshot_id = sql_text_literal(&snapshot.snapshot_id),
        name = sql_text_literal(&snapshot.name),
        branch_id = sql_text_literal(&snapshot.branch_id),
        head_id = sql_text_literal(&snapshot.head_id),
        snapshot_lsn = u64_to_i64(snapshot.snapshot_lsn, "snapshot_lsn")?,
        created_at_micros = snapshot.created_at_micros,
    ))?;
    Ok(())
}

pub(crate) fn branch_by_name(db: &Db, name: &str) -> Result<Option<BranchInfo>> {
    ensure_branch_catalog(db)?;
    let result = db.execute(&format!(
        "SELECT branch_id, name, current_head_id, base_head_id, created_at_micros, updated_at_micros, deleted_at_micros FROM {BRANCHES_TABLE} WHERE name = {} AND deleted_at_micros IS NULL",
        sql_text_literal(name)
    ))?;
    result.rows().first().map(branch_info_from_row).transpose()
}

pub(crate) fn branch_head_by_id(db: &Db, head_id: &str) -> Result<Option<BranchHeadMetadata>> {
    let result = db.execute(&format!(
        "SELECT head_id, branch_id, parent_head_id, root_manifest_id, commit_lsn, message, created_at_micros FROM {BRANCH_HEADS_TABLE} WHERE head_id = {}",
        sql_text_literal(head_id)
    ))?;
    result
        .rows()
        .first()
        .map(branch_head_metadata_from_row)
        .transpose()
}

fn root_manifest_by_id(db: &Db, manifest_id: &str) -> Result<Option<RootManifestPrototype>> {
    let result = db.execute(&format!(
        "SELECT manifest_id, schema_cookie, catalog_root_page_id, table_roots_json, index_roots_json, sequence_state_json, metadata_version, commit_lsn, created_at_micros FROM {ROOT_MANIFESTS_TABLE} WHERE manifest_id = {}",
        sql_text_literal(manifest_id)
    ))?;
    result
        .rows()
        .first()
        .map(root_manifest_from_row)
        .transpose()
}

fn branch_info_from_row(row: &crate::exec::QueryRow) -> Result<BranchInfo> {
    let values = row.values();
    Ok(BranchInfo {
        branch_id: expect_text(values.first(), "branch_id")?,
        name: expect_text(values.get(1), "name")?,
        current_head_id: optional_text(values.get(2), "current_head_id")?,
        base_head_id: optional_text(values.get(3), "base_head_id")?,
        created_at_micros: expect_i64(values.get(4), "created_at_micros")?,
        updated_at_micros: expect_i64(values.get(5), "updated_at_micros")?,
        deleted_at_micros: optional_i64(values.get(6), "deleted_at_micros")?,
    })
}

fn branch_head_metadata_from_row(row: &crate::exec::QueryRow) -> Result<BranchHeadMetadata> {
    let values = row.values();
    Ok(BranchHeadMetadata {
        head_id: expect_text(values.first(), "head_id")?,
        branch_id: expect_text(values.get(1), "branch_id")?,
        parent_head_id: optional_text(values.get(2), "parent_head_id")?,
        root_manifest_id: expect_text(values.get(3), "root_manifest_id")?,
        commit_lsn: expect_i64(values.get(4), "commit_lsn")?,
        message: optional_text(values.get(5), "message")?,
        created_at_micros: expect_i64(values.get(6), "created_at_micros")?,
    })
}

fn branch_sql_log_entry_by_head_id(db: &Db, head_id: &str) -> Result<Option<BranchSqlLogEntry>> {
    let result = db.execute(&format!(
        "SELECT head_id, branch_id, sequence, sql FROM {BRANCH_SQL_LOG_TABLE} WHERE head_id = {}",
        sql_text_literal(head_id)
    ))?;
    result
        .rows()
        .first()
        .map(branch_sql_log_entry_from_row)
        .transpose()
}

fn branch_sql_log_entry_from_row(row: &crate::exec::QueryRow) -> Result<BranchSqlLogEntry> {
    let values = row.values();
    Ok(BranchSqlLogEntry {
        head_id: expect_text(values.first(), "head_id")?,
        branch_id: expect_text(values.get(1), "branch_id")?,
        sequence: expect_i64(values.get(2), "sequence")?,
        sql: expect_text(values.get(3), "sql")?,
    })
}

fn root_manifest_from_row(row: &crate::exec::QueryRow) -> Result<RootManifestPrototype> {
    let values = row.values();
    let schema_cookie = u32::try_from(expect_i64(values.get(1), "schema_cookie")?)
        .map_err(|_| DbError::corruption("root manifest schema_cookie out of range"))?;
    Ok(RootManifestPrototype {
        manifest_id: expect_text(values.first(), "manifest_id")?,
        schema_cookie,
        catalog_root_page_id: optional_i64(values.get(2), "catalog_root_page_id")?,
        table_roots_json: expect_text(values.get(3), "table_roots_json")?,
        index_roots_json: expect_text(values.get(4), "index_roots_json")?,
        sequence_state_json: expect_text(values.get(5), "sequence_state_json")?,
        metadata_version: expect_i64(values.get(6), "metadata_version")?,
        commit_lsn: expect_i64(values.get(7), "commit_lsn")?,
        created_at_micros: expect_i64(values.get(8), "created_at_micros")?,
    })
}

fn snapshot_from_row(row: &crate::exec::QueryRow) -> Result<NamedSnapshot> {
    let values = row.values();
    let snapshot_lsn = expect_i64(values.get(4), "snapshot_lsn")?;
    Ok(NamedSnapshot {
        snapshot_id: expect_text(values.first(), "snapshot_id")?,
        name: expect_text(values.get(1), "name")?,
        branch_id: expect_text(values.get(2), "branch_id")?,
        head_id: expect_text(values.get(3), "head_id")?,
        snapshot_lsn: i64_to_u64(snapshot_lsn, "snapshot_lsn")?,
        created_at_micros: expect_i64(values.get(5), "created_at_micros")?,
    })
}

fn expect_text(value: Option<&Value>, field_name: &str) -> Result<String> {
    optional_text(value, field_name)?
        .ok_or_else(|| DbError::corruption(format!("{field_name} must not be NULL")))
}

fn optional_text(value: Option<&Value>, field_name: &str) -> Result<Option<String>> {
    match value {
        Some(Value::Text(value)) => Ok(Some(value.clone())),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(DbError::corruption(format!(
            "{field_name} must be stored as TEXT"
        ))),
        None => Err(DbError::corruption(format!(
            "{field_name} missing from row"
        ))),
    }
}

fn expect_i64(value: Option<&Value>, field_name: &str) -> Result<i64> {
    optional_i64(value, field_name)?
        .ok_or_else(|| DbError::corruption(format!("{field_name} must not be NULL")))
}

fn optional_i64(value: Option<&Value>, field_name: &str) -> Result<Option<i64>> {
    match value {
        Some(Value::Int64(value)) => Ok(Some(*value)),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(DbError::corruption(format!(
            "{field_name} must be stored as INT64"
        ))),
        None => Err(DbError::corruption(format!(
            "{field_name} missing from row"
        ))),
    }
}

fn sql_text_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_nullable_text_literal(value: Option<&str>) -> String {
    value
        .map(sql_text_literal)
        .unwrap_or_else(|| "NULL".to_string())
}

fn sql_nullable_i64_literal(value: Option<i64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "NULL".to_string())
}

fn validate_user_name<'a>(name: &'a str, kind: &str) -> Result<&'a str> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(DbError::sql(format!("{kind} name must not be empty")));
    }
    if trimmed != name {
        return Err(DbError::sql(format!(
            "{kind} name must not contain leading or trailing whitespace"
        )));
    }
    Ok(trimmed)
}

fn validate_message(message: &str) -> Result<&str> {
    let trimmed = message.trim();
    if trimmed.is_empty() {
        return Err(DbError::sql("branch commit message must not be empty"));
    }
    Ok(trimmed)
}

fn stable_id_component(name: &str) -> String {
    name.chars()
        .map(|ch| match ch {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' => ch,
            _ => '_',
        })
        .collect()
}

fn u64_to_i64(value: u64, field_name: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| DbError::internal(format!("{field_name} overflowed INT64")))
}

fn i64_to_u64(value: i64, field_name: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| {
        DbError::corruption(format!(
            "{field_name} must be non-negative in branch catalog"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Db, DbConfig};

    fn temp_db() -> (tempfile::TempDir, Db) {
        let dir = tempfile::TempDir::with_prefix("decentdb-branch-test").unwrap();
        let path = dir.path().join("branch.ddb");
        let db = Db::open_or_create(&path, DbConfig::default()).unwrap();
        (dir, db)
    }

    #[test]
    fn bootstrap_main_branch_is_idempotent() {
        let (_dir, db) = temp_db();

        let first = bootstrap_main_branch(&db).unwrap();
        assert!(first.created);
        assert_eq!(first.branch.name, DEFAULT_BRANCH_NAME);
        assert_eq!(
            first.branch.current_head_id.as_deref(),
            Some(DEFAULT_HEAD_ID)
        );
        assert_eq!(first.head.root_manifest_id, DEFAULT_ROOT_MANIFEST_ID);

        let second = bootstrap_main_branch(&db).unwrap();
        assert!(!second.created);
        assert_eq!(second.branch, first.branch);
        assert_eq!(second.head, first.head);
        assert_eq!(second.root_manifest, first.root_manifest);

        let branches = list_branches(&db).unwrap();
        assert_eq!(branches, vec![first.branch]);
        let head = main_branch_head(&db).unwrap().unwrap();
        assert_eq!(head, first.head);
    }

    #[test]
    fn bootstrap_main_branch_persists_after_reopen() {
        let dir = tempfile::TempDir::with_prefix("decentdb-branch-reopen").unwrap();
        let path = dir.path().join("branch.ddb");

        let first = {
            let db = Db::open_or_create(&path, DbConfig::default()).unwrap();
            bootstrap_main_branch(&db).unwrap()
        };
        let reopened = Db::open_or_create(&path, DbConfig::default()).unwrap();
        let second = bootstrap_main_branch(&reopened).unwrap();

        assert!(!second.created);
        assert_eq!(second.branch, first.branch);
        assert_eq!(second.head, first.head);
        assert_eq!(second.root_manifest, first.root_manifest);
    }

    #[test]
    fn branch_catalog_tables_are_hidden_from_user_listing() {
        let (_dir, db) = temp_db();

        bootstrap_main_branch(&db).unwrap();

        let table_names: Vec<String> = db
            .list_tables()
            .unwrap()
            .into_iter()
            .map(|table| table.name)
            .collect();
        assert!(!table_names.iter().any(|name| name == BRANCHES_TABLE));
        assert!(!table_names.iter().any(|name| name == BRANCH_HEADS_TABLE));
        assert!(!table_names.iter().any(|name| name == SNAPSHOTS_TABLE));
        assert!(!table_names.iter().any(|name| name == ROOT_MANIFESTS_TABLE));
    }

    #[test]
    fn named_snapshot_preserves_read_only_time_travel_after_main_advances() {
        let (_dir, db) = temp_db();
        db.execute("CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO items (id, name) VALUES (1, 'before')")
            .unwrap();

        let snapshot = db.snapshot_create("before-update").unwrap();
        assert_eq!(snapshot.name, "before-update");
        assert_eq!(db.snapshot_list().unwrap(), vec![snapshot.clone()]);
        assert_eq!(
            db.snapshot_lsn_for_ref(&snapshot.head_id).unwrap(),
            Some(snapshot.snapshot_lsn)
        );

        db.execute("UPDATE items SET name = 'after' WHERE id = 1")
            .unwrap();

        let result = db
            .execute_batch_at_snapshot("SELECT name FROM items WHERE id = 1", "before-update")
            .unwrap();
        assert_eq!(
            result[0].rows()[0].values()[0],
            Value::Text("before".to_string())
        );

        let write_error = db
            .execute_batch_at_snapshot(
                "INSERT INTO items (id, name) VALUES (2, 'blocked')",
                "before-update",
            )
            .unwrap_err();
        assert!(
            write_error.to_string().contains("read-only"),
            "unexpected error: {write_error}"
        );
    }

    #[test]
    fn named_snapshot_retains_history_across_checkpoint_until_deleted() {
        let (_dir, db) = temp_db();
        db.execute("CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO items (id, name) VALUES (1, 'before')")
            .unwrap();
        db.snapshot_create("before-checkpoint").unwrap();
        db.execute("UPDATE items SET name = 'after' WHERE id = 1")
            .unwrap();

        db.checkpoint_wal().unwrap();

        let historical = db
            .execute_batch_at_snapshot("SELECT name FROM items WHERE id = 1", "before-checkpoint")
            .unwrap();
        assert_eq!(
            historical[0].rows()[0].values()[0],
            Value::Text("before".to_string())
        );
        let latest = db.execute("SELECT name FROM items WHERE id = 1").unwrap();
        assert_eq!(
            latest.rows()[0].values()[0],
            Value::Text("after".to_string())
        );

        assert!(db.snapshot_delete("before-checkpoint").unwrap());
        assert!(db.snapshot_list().unwrap().is_empty());
    }

    #[test]
    fn branch_reads_source_state_while_main_advances() {
        let (_dir, db) = temp_db();
        db.execute("CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO items (id, name) VALUES (1, 'before')")
            .unwrap();

        let branch = db.branch_create("work", None).unwrap();
        assert_eq!(branch.name, "work");

        db.execute("UPDATE items SET name = 'after' WHERE id = 1")
            .unwrap();

        let branch_result = db
            .execute_batch_on_branch("SELECT name FROM items WHERE id = 1", "work")
            .unwrap();
        assert_eq!(
            branch_result[0].rows()[0].values()[0],
            Value::Text("before".to_string())
        );

        let main_result = db.execute("SELECT name FROM items WHERE id = 1").unwrap();
        assert_eq!(
            main_result.rows()[0].values()[0],
            Value::Text("after".to_string())
        );

        db.execute_batch_on_branch("UPDATE items SET name = 'branch' WHERE id = 1", "work")
            .unwrap();
        let branch_after_write = db
            .execute_batch_on_branch("SELECT name FROM items WHERE id = 1", "work")
            .unwrap();
        assert_eq!(
            branch_after_write[0].rows()[0].values()[0],
            Value::Text("branch".to_string())
        );
        let main_after_branch_write = db.execute("SELECT name FROM items WHERE id = 1").unwrap();
        assert_eq!(
            main_after_branch_write.rows()[0].values()[0],
            Value::Text("after".to_string())
        );
    }

    #[test]
    fn branch_local_writes_survive_reopen_and_can_be_forked() {
        let dir = tempfile::TempDir::with_prefix("decentdb-branch-reopen-test").unwrap();
        let path = dir.path().join("branch.ddb");
        let db = Db::open_or_create(&path, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO items (id, name) VALUES (1, 'base')")
            .unwrap();
        db.branch_create("work", None).unwrap();
        db.execute_batch_on_branch("UPDATE items SET name = 'work' WHERE id = 1", "work")
            .unwrap();
        db.branch_create("child", Some("work")).unwrap();
        drop(db);

        let reopened = Db::open_or_create(&path, DbConfig::default()).unwrap();
        let work_result = reopened
            .execute_batch_on_branch("SELECT name FROM items WHERE id = 1", "work")
            .unwrap();
        assert_eq!(
            work_result[0].rows()[0].values()[0],
            Value::Text("work".to_string())
        );
        let child_result = reopened
            .execute_batch_on_branch("SELECT name FROM items WHERE id = 1", "child")
            .unwrap();
        assert_eq!(
            child_result[0].rows()[0].values()[0],
            Value::Text("work".to_string())
        );
        let main_result = reopened
            .execute("SELECT name FROM items WHERE id = 1")
            .unwrap();
        assert_eq!(
            main_result.rows()[0].values()[0],
            Value::Text("base".to_string())
        );
    }

    #[test]
    fn branch_commit_marker_appears_in_head_log() {
        let (_dir, db) = temp_db();
        db.execute("CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO items (id, name) VALUES (1, 'base')")
            .unwrap();
        db.branch_create("work", None).unwrap();
        db.execute_batch_on_branch("UPDATE items SET name = 'work' WHERE id = 1", "work")
            .unwrap();
        db.branch_commit("work", "reviewed branch changes").unwrap();

        let log = db.branch_log("work").unwrap();
        assert_eq!(
            log.first().and_then(|entry| entry.message.as_deref()),
            Some("reviewed branch changes")
        );
        assert!(log.iter().any(
            |entry| entry.sql.as_deref() == Some("UPDATE items SET name = 'work' WHERE id = 1")
        ));
    }

    #[test]
    fn branch_diff_reports_primary_key_row_changes() {
        let (_dir, db) = temp_db();
        db.execute("CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO items (id, name) VALUES (1, 'base'), (2, 'deleted')")
            .unwrap();
        db.branch_create("work", None).unwrap();
        db.execute_batch_on_branch(
            "UPDATE items SET name = 'branch' WHERE id = 1; \
             DELETE FROM items WHERE id = 2; \
             INSERT INTO items (id, name) VALUES (3, 'added')",
            "work",
        )
        .unwrap();

        let diff = db.branch_diff("main", "work").unwrap();
        assert_eq!(diff.changed_table_count, 1);
        assert_eq!(diff.added_row_count, 1);
        assert_eq!(diff.updated_row_count, 1);
        assert_eq!(diff.deleted_row_count, 1);
        let table = diff
            .tables
            .iter()
            .find(|table| table.table == "items")
            .unwrap();
        assert_eq!(table.status, BranchTableDiffStatus::Changed);
        assert_eq!(table.added[0].primary_key, vec!["3"]);
        assert_eq!(table.updated[0].primary_key, vec!["1"]);
        assert_eq!(table.deleted[0].primary_key, vec!["2"]);
    }

    #[test]
    fn branch_restore_moves_head_to_snapshot_after_dry_run() {
        let (_dir, db) = temp_db();
        db.execute("CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO items (id, name) VALUES (1, 'base')")
            .unwrap();
        let snapshot = db.snapshot_create("base-snapshot").unwrap();
        db.branch_create("work", None).unwrap();
        db.execute_batch_on_branch("UPDATE items SET name = 'work' WHERE id = 1", "work")
            .unwrap();

        let dry_run = db.branch_restore("work", "base-snapshot", true).unwrap();
        assert!(dry_run.dry_run);
        assert_eq!(dry_run.new_head_id, None);
        assert_eq!(dry_run.target_head_id, snapshot.head_id);
        let still_work = db
            .execute_batch_on_branch("SELECT name FROM items WHERE id = 1", "work")
            .unwrap();
        assert_eq!(
            still_work[0].rows()[0].values()[0],
            Value::Text("work".to_string())
        );

        let restored = db.branch_restore("work", "base-snapshot", false).unwrap();
        assert!(!restored.dry_run);
        assert!(restored.new_head_id.is_some());
        let result = db
            .execute_batch_on_branch("SELECT name FROM items WHERE id = 1", "work")
            .unwrap();
        assert_eq!(
            result[0].rows()[0].values()[0],
            Value::Text("base".to_string())
        );
    }

    #[test]
    fn branch_merge_applies_clean_row_changes_into_main() {
        let (_dir, db) = temp_db();
        db.execute("CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO items (id, name) VALUES (1, 'base')")
            .unwrap();
        db.branch_create("work", None).unwrap();
        db.execute_batch_on_branch("UPDATE items SET name = 'branch' WHERE id = 1", "work")
            .unwrap();

        let dry_run = db.branch_merge("work", "main", true).unwrap();
        assert!(dry_run.clean);
        assert_eq!(dry_run.applied_change_count, 0);
        assert_eq!(dry_run.conflict_count, 0);

        let merged = db.branch_merge("work", "main", false).unwrap();
        assert!(merged.clean);
        assert_eq!(merged.applied_change_count, 1);
        let main = db.execute("SELECT name FROM items WHERE id = 1").unwrap();
        assert_eq!(
            main.rows()[0].values()[0],
            Value::Text("branch".to_string())
        );
    }

    #[test]
    fn branch_merge_reports_update_update_conflict() {
        let (_dir, db) = temp_db();
        db.execute("CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO items (id, name) VALUES (1, 'base')")
            .unwrap();
        db.branch_create("work", None).unwrap();
        db.execute_batch_on_branch("UPDATE items SET name = 'branch' WHERE id = 1", "work")
            .unwrap();
        db.execute("UPDATE items SET name = 'main' WHERE id = 1")
            .unwrap();

        let report = db.branch_merge("work", "main", false).unwrap();
        assert!(!report.clean);
        assert_eq!(report.applied_change_count, 0);
        assert_eq!(report.conflict_count, 1);
        assert_eq!(report.conflicts[0].conflict_type, "update_update");
        let main = db.execute("SELECT name FROM items WHERE id = 1").unwrap();
        assert_eq!(main.rows()[0].values()[0], Value::Text("main".to_string()));
    }

    #[test]
    fn branch_list_rename_and_delete_update_metadata() {
        let (_dir, db) = temp_db();
        db.execute("CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO items (id, name) VALUES (1, 'before')")
            .unwrap();

        db.branch_create("work", None).unwrap();
        let names = db
            .branch_list()
            .unwrap()
            .into_iter()
            .map(|branch| branch.name)
            .collect::<Vec<_>>();
        assert!(names.contains(&"main".to_string()));
        assert!(names.contains(&"work".to_string()));

        assert!(db.branch_rename("work", "review").unwrap());
        assert!(db.branch_lsn("work").unwrap().is_none());
        assert!(db.branch_lsn("review").unwrap().is_some());

        assert!(db.branch_delete("review").unwrap());
        assert!(db.branch_lsn("review").unwrap().is_none());
    }
}

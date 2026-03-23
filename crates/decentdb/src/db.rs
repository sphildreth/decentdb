//! Stable database owner and bootstrap lifecycle entry points.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::catalog::{
    CatalogHandle, ColumnSchema, ForeignKeyAction, ForeignKeyConstraint, IndexColumn, IndexKind,
    IndexSchema, TableSchema, TriggerEvent, TriggerKind, TriggerSchema, ViewSchema,
};
use crate::config::DbConfig;
use crate::error::{DbError, Result};
use crate::exec::{
    statement_is_read_only, BulkLoadOptions, EngineRuntime, QueryResult, RuntimeIndex,
};
use crate::metadata::{
    ColumnInfo, ForeignKeyInfo, HeaderInfo, IndexInfo, IndexVerification, StorageInfo, TableInfo,
    TriggerInfo, ViewInfo,
};
use crate::record::value::Value;
use crate::sql::ast::Statement as SqlStatement;
use crate::sql::parser::parse_sql_statement;
use crate::storage::page::{self, PageId};
use crate::storage::{self, DatabaseHeader, PagerHandle};
use crate::vfs::faulty::{self, FailAction, Failpoint};
use crate::vfs::{is_memory_path, write_all_at, FileKind, OpenMode, VfsHandle};
use crate::wal::reader_registry::ReaderGuard;
use crate::wal::savepoint::StatementSavepoint;
use crate::wal::WalHandle;

/// Stable engine owner used across later storage, SQL, and FFI slices.
#[derive(Clone, Debug)]
pub struct Db {
    inner: Arc<DbInner>,
}

#[derive(Debug)]
struct DbInner {
    path: PathBuf,
    config: DbConfig,
    _vfs: VfsHandle,
    pager: PagerHandle,
    wal: WalHandle,
    catalog: CatalogHandle,
    engine: RwLock<EngineRuntime>,
    last_runtime_lsn: AtomicU64,
    sql_write_lock: Mutex<()>,
    sql_txn: Mutex<Option<SqlTxnState>>,
    write_txn: Mutex<WriteTxn>,
    statement_cache: Mutex<StatementCache>,
    held_snapshots: Mutex<HashMap<u64, ReaderGuard>>,
}

#[derive(Debug, Default)]
struct WriteTxn {
    active: bool,
    staged_pages: BTreeMap<PageId, Vec<u8>>,
}

#[derive(Debug)]
struct SqlTxnState {
    runtime: EngineRuntime,
    base_lsn: u64,
    savepoints: Vec<SqlSavepoint>,
}

#[derive(Clone, Debug)]
struct SqlSavepoint {
    name: String,
    runtime: EngineRuntime,
}

const STATEMENT_CACHE_CAPACITY: usize = 128;

#[derive(Debug)]
struct StatementCache {
    entries: HashMap<String, Arc<SqlStatement>>,
    order: VecDeque<String>,
    capacity: usize,
}

impl Default for StatementCache {
    fn default() -> Self {
        Self::with_capacity(STATEMENT_CACHE_CAPACITY)
    }
}

impl StatementCache {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    fn get_or_parse(&mut self, sql: &str) -> Result<Arc<SqlStatement>> {
        if let Some(statement) = self.entries.get(sql) {
            return Ok(Arc::clone(statement));
        }

        let statement = Arc::new(parse_sql_statement(sql)?);
        if self.capacity == 0 {
            return Ok(statement);
        }

        while self.entries.len() >= self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            } else {
                break;
            }
        }

        let key = sql.to_string();
        self.order.push_back(key.clone());
        self.entries.insert(key, Arc::clone(&statement));
        Ok(statement)
    }
}

impl Db {
    /// Creates a brand new database file with an initialized page-1 header and
    /// reserved catalog root page.
    pub fn create(path: impl AsRef<Path>, config: DbConfig) -> Result<Self> {
        let path = path.as_ref();
        config.validate_for_create()?;
        let vfs = VfsHandle::for_path(path);
        let open_mode = if vfs.is_memory() {
            OpenMode::OpenOrCreate
        } else {
            OpenMode::CreateNew
        };
        let file = vfs.open(path, open_mode, FileKind::Database)?;
        let header = DatabaseHeader::new(config.page_size);
        storage::write_database_bootstrap_vfs(file.as_ref(), &header)?;
        file.sync_metadata()?;

        Self::open_with_vfs(path.to_path_buf(), config, vfs)
    }

    /// Opens an existing database file and validates its fixed header.
    pub fn open(path: impl AsRef<Path>, config: DbConfig) -> Result<Self> {
        let path = path.as_ref();
        let vfs = VfsHandle::for_path(path);
        let mode = if vfs.is_memory() {
            OpenMode::OpenOrCreate
        } else {
            OpenMode::OpenExisting
        };
        let file = vfs.open(path, mode, FileKind::Database)?;
        if vfs.is_memory() && file.file_size()? == 0 {
            let header = DatabaseHeader::new(config.page_size);
            storage::write_database_bootstrap_vfs(file.as_ref(), &header)?;
            file.sync_metadata()?;
        }
        Self::open_with_vfs(path.to_path_buf(), config, vfs)
    }

    /// Opens an existing database or creates a new one when the path does not
    /// yet exist.
    pub fn open_or_create(path: impl AsRef<Path>, config: DbConfig) -> Result<Self> {
        let path = path.as_ref();
        let vfs = VfsHandle::for_path(path);
        if vfs.is_memory() || vfs.file_exists(path)? {
            Self::open(path, config)
        } else {
            Self::create(path, config)
        }
    }

    /// Begins an explicit SQL transaction on this database handle.
    pub fn begin_transaction(&self) -> Result<()> {
        self.refresh_engine_from_storage()?;
        let runtime = self.engine_snapshot()?;
        let base_lsn = self.inner.last_runtime_lsn.load(Ordering::Acquire);
        let mut txn = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        if txn.is_some() {
            return Err(DbError::transaction(
                "SQL transaction is already active on this handle",
            ));
        }
        *txn = Some(SqlTxnState {
            runtime,
            base_lsn,
            savepoints: Vec::new(),
        });
        Ok(())
    }

    /// Commits the current explicit SQL transaction.
    pub fn commit_transaction(&self) -> Result<u64> {
        let state = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?
            .take()
            .ok_or_else(|| DbError::transaction("no active SQL transaction to commit"))?;
        self.persist_runtime_if_latest(state.runtime, Some(state.base_lsn))
    }

    /// Rolls back the current explicit SQL transaction.
    pub fn rollback_transaction(&self) -> Result<()> {
        let mut txn = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        if txn.is_none() {
            return Err(DbError::transaction(
                "no active SQL transaction to roll back",
            ));
        }
        *txn = None;
        Ok(())
    }

    /// Returns whether this handle currently has an explicit SQL transaction.
    pub fn in_transaction(&self) -> Result<bool> {
        self.inner
            .sql_txn
            .lock()
            .map(|txn| txn.is_some())
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))
    }

    /// Creates a named savepoint inside the current explicit SQL transaction.
    pub fn create_savepoint(&self, name: &str) -> Result<()> {
        let mut txn = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        let state = txn
            .as_mut()
            .ok_or_else(|| DbError::transaction("SAVEPOINT requires an active SQL transaction"))?;
        state.savepoints.push(SqlSavepoint {
            name: canonical_savepoint_name(name),
            runtime: state.runtime.clone(),
        });
        Ok(())
    }

    /// Releases a named savepoint and any nested savepoints created after it.
    pub fn release_savepoint(&self, name: &str) -> Result<()> {
        let mut txn = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        let state = txn.as_mut().ok_or_else(|| {
            DbError::transaction("RELEASE SAVEPOINT requires an active SQL transaction")
        })?;
        let target = canonical_savepoint_name(name);
        let index = state
            .savepoints
            .iter()
            .rposition(|savepoint| savepoint.name == target)
            .ok_or_else(|| DbError::transaction(format!("savepoint {name} does not exist")))?;
        state.savepoints.truncate(index);
        Ok(())
    }

    /// Rolls the current explicit SQL transaction back to a named savepoint.
    pub fn rollback_to_savepoint(&self, name: &str) -> Result<()> {
        let mut txn = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        let state = txn.as_mut().ok_or_else(|| {
            DbError::transaction("ROLLBACK TO SAVEPOINT requires an active SQL transaction")
        })?;
        let target = canonical_savepoint_name(name);
        let index = state
            .savepoints
            .iter()
            .rposition(|savepoint| savepoint.name == target)
            .ok_or_else(|| DbError::transaction(format!("savepoint {name} does not exist")))?;
        state.runtime = state.savepoints[index].runtime.clone();
        state.savepoints.truncate(index + 1);
        Ok(())
    }

    /// Returns a structured snapshot of the current storage state.
    pub fn storage_info(&self) -> Result<StorageInfo> {
        let header = self.inner.pager.header_snapshot()?;
        Ok(StorageInfo {
            path: self.path().to_path_buf(),
            wal_path: self.inner.wal.file_path().to_path_buf(),
            page_size: self.inner.config.page_size,
            cache_size_mb: self.inner.config.cache_size_mb,
            page_count: self.inner.pager.on_disk_page_count()?,
            schema_cookie: header.schema_cookie,
            wal_end_lsn: self.inner.wal.latest_snapshot(),
            wal_file_size: self.inner.wal.file_size()?,
            last_checkpoint_lsn: header.last_checkpoint_lsn,
            active_readers: self.inner.wal.active_reader_count()?,
            wal_versions: self.inner.wal.version_count()?,
            warning_count: self.inner.wal.warnings()?.len(),
            shared_wal: self.inner.wal.is_shared(),
        })
    }

    /// Returns the decoded page-1 database header fields.
    pub fn header_info(&self) -> Result<HeaderInfo> {
        let header = self.inner.pager.header_snapshot()?;
        Ok(HeaderInfo {
            magic_hex: hex_encode(&header.magic),
            format_version: header.format_version,
            page_size: header.page_size,
            header_checksum: header.header_checksum,
            schema_cookie: header.schema_cookie,
            catalog_root_page_id: header.catalog_root_page_id,
            freelist_root_page_id: header.freelist.root_page_id,
            freelist_head_page_id: header.freelist.head_page_id,
            freelist_page_count: header.freelist.page_count,
            last_checkpoint_lsn: header.last_checkpoint_lsn,
        })
    }

    /// Writes a checkpointed snapshot of the database into a new destination file.
    pub fn save_as(&self, dest: impl AsRef<Path>) -> Result<()> {
        let dest = dest.as_ref();
        if is_memory_path(dest) {
            return Err(DbError::transaction(
                "save_as destination must be an on-disk path",
            ));
        }

        self.checkpoint()?;

        let vfs = VfsHandle::for_path(dest);
        if vfs.file_exists(dest)? {
            return Err(DbError::io(
                format!("destination {} already exists", dest.display()),
                std::io::Error::new(std::io::ErrorKind::AlreadyExists, "destination exists"),
            ));
        }

        let file = vfs.open(dest, OpenMode::CreateNew, FileKind::Database)?;
        let page_size = self.inner.config.page_size;
        let page_count = self.inner.pager.on_disk_page_count()?;
        for page_id in 1..=page_count {
            let page = self.read_page(page_id)?;
            write_all_at(file.as_ref(), page::page_offset(page_id, page_size), &page)?;
        }
        file.set_len(page::page_offset(page_count.saturating_add(1), page_size))?;
        file.sync_metadata()?;
        Ok(())
    }

    /// Begins a single-connection write transaction.
    pub fn begin_write(&self) -> Result<()> {
        let mut txn = self
            .inner
            .write_txn
            .lock()
            .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
        if txn.active {
            return Err(DbError::transaction("write transaction is already active"));
        }
        txn.active = true;
        txn.staged_pages.clear();
        Ok(())
    }

    /// Stages a full-page image inside the current write transaction.
    pub fn write_page(&self, page_id: u32, data: &[u8]) -> Result<()> {
        page::validate_page_id(page_id)?;
        if data.len() != self.inner.config.page_size as usize {
            return Err(DbError::internal(format!(
                "page {page_id} write length {} does not match configured page size {}",
                data.len(),
                self.inner.config.page_size
            )));
        }
        let mut txn = self
            .inner
            .write_txn
            .lock()
            .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
        if !txn.active {
            return Err(DbError::transaction(
                "write_page requires an active write transaction",
            ));
        }
        txn.staged_pages.insert(page_id, data.to_vec());
        Ok(())
    }

    /// Allocates a new page from the freelist or file tail.
    pub fn allocate_page(&self) -> Result<u32> {
        let txn = self
            .inner
            .write_txn
            .lock()
            .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
        if !txn.active {
            return Err(DbError::transaction(
                "allocate_page requires an active write transaction",
            ));
        }
        drop(txn);
        self.inner.pager.allocate_page()
    }

    /// Frees an existing page back to the freelist.
    pub fn free_page(&self, page_id: u32) -> Result<()> {
        let txn = self
            .inner
            .write_txn
            .lock()
            .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
        if !txn.active {
            return Err(DbError::transaction(
                "free_page requires an active write transaction",
            ));
        }
        drop(txn);
        self.inner.pager.free_page(page_id)
    }

    /// Commits the current write transaction to the WAL.
    pub fn commit(&self) -> Result<u64> {
        let pages = {
            let mut txn = self
                .inner
                .write_txn
                .lock()
                .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
            if !txn.active {
                return Err(DbError::transaction(
                    "no active write transaction to commit",
                ));
            }
            txn.active = false;
            std::mem::take(&mut txn.staged_pages)
        };

        let pages: Vec<_> = pages.into_iter().collect();
        let max_page_id = pages.iter().map(|(page_id, _)| *page_id).max().unwrap_or(0);
        let max_page_count = self
            .inner
            .pager
            .on_disk_page_count()?
            .max(self.inner.wal.max_page_count())
            .max(max_page_id);
        self.inner.wal.commit_pages(&pages, max_page_count)
    }

    fn commit_if_latest(&self, expected_latest_lsn: u64) -> Result<u64> {
        let pages = {
            let mut txn = self
                .inner
                .write_txn
                .lock()
                .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
            if !txn.active {
                return Err(DbError::transaction(
                    "no active write transaction to commit",
                ));
            }
            txn.active = false;
            std::mem::take(&mut txn.staged_pages)
        };

        let pages: Vec<_> = pages.into_iter().collect();
        let max_page_id = pages.iter().map(|(page_id, _)| *page_id).max().unwrap_or(0);
        let max_page_count = self
            .inner
            .pager
            .on_disk_page_count()?
            .max(self.inner.wal.max_page_count())
            .max(max_page_id);
        self.inner
            .wal
            .commit_pages_if_latest(&pages, max_page_count, expected_latest_lsn)
    }

    /// Rolls back the current write transaction.
    pub fn rollback(&self) -> Result<()> {
        let mut txn = self
            .inner
            .write_txn
            .lock()
            .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
        txn.active = false;
        txn.staged_pages.clear();
        Ok(())
    }

    /// Reads the latest visible version of a page.
    pub fn read_page(&self, page_id: u32) -> Result<Vec<u8>> {
        page::validate_page_id(page_id)?;
        if let Some(staged) = self
            .inner
            .write_txn
            .lock()
            .map_err(|_| DbError::internal("write transaction lock poisoned"))?
            .staged_pages
            .get(&page_id)
            .cloned()
        {
            return Ok(staged);
        }

        let snapshot_lsn = self.inner.wal.latest_snapshot();
        if let Some(wal_page) = self
            .inner
            .wal
            .read_page_at_snapshot(page_id, snapshot_lsn)?
        {
            return Ok(wal_page);
        }
        self.inner.pager.read_page(page_id)
    }

    /// Performs a reader-aware checkpoint.
    pub fn checkpoint(&self) -> Result<()> {
        self.inner
            .wal
            .checkpoint(&self.inner.pager, self.inner.config.checkpoint_timeout_sec)
    }

    /// Executes a single SQL statement without parameters.
    pub fn execute(&self, sql: &str) -> Result<QueryResult> {
        self.execute_with_params(sql, &[])
    }

    /// Executes a single SQL statement with positional `$n` parameters.
    pub fn execute_with_params(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        let mut results = self.execute_batch_with_params(sql, params)?;
        if results.len() != 1 {
            return Err(DbError::sql(format!(
                "expected exactly one SQL statement, got {}",
                results.len()
            )));
        }
        Ok(results.remove(0))
    }

    /// Executes one or more semicolon-delimited SQL statements.
    pub fn execute_batch(&self, sql: &str) -> Result<Vec<QueryResult>> {
        self.execute_batch_with_params(sql, &[])
    }

    /// Executes one or more semicolon-delimited SQL statements with `$n` parameters.
    pub fn execute_batch_with_params(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<QueryResult>> {
        let mut results = Vec::new();
        for statement_sql in split_sql_batch(sql) {
            let trimmed = statement_sql.trim();
            if trimmed.is_empty() {
                continue;
            }

            if let Some(control) = parse_transaction_control(trimmed) {
                match control {
                    TransactionControl::Begin => self.begin_transaction()?,
                    TransactionControl::Commit => {
                        self.commit_transaction()?;
                    }
                    TransactionControl::Rollback => self.rollback_transaction()?,
                    TransactionControl::Savepoint(name) => self.create_savepoint(&name)?,
                    TransactionControl::ReleaseSavepoint(name) => {
                        self.release_savepoint(&name)?;
                    }
                    TransactionControl::RollbackToSavepoint(name) => {
                        self.rollback_to_savepoint(&name)?;
                    }
                }
                results.push(QueryResult::with_affected_rows(0));
                continue;
            }

            let statement = self.parsed_statement(trimmed)?;
            let result = if statement_is_read_only(&statement) {
                self.execute_read_statement(&statement, params)?
            } else {
                self.execute_write_statement(&statement, params)?
            };
            results.push(result);
        }
        Ok(results)
    }

    /// Loads rows into a table as a single writer-held bulk operation.
    pub fn bulk_load_rows(
        &self,
        table_name: &str,
        columns: &[&str],
        rows: &[Vec<Value>],
        options: BulkLoadOptions,
    ) -> Result<u64> {
        let _writer = self
            .inner
            .sql_write_lock
            .lock()
            .map_err(|_| DbError::internal("SQL writer lock poisoned"))?;
        let mut working = self.engine_snapshot()?;
        let inserted = working.bulk_load_rows(
            table_name,
            columns,
            rows,
            options,
            self.inner.config.page_size,
        )?;
        self.persist_runtime(working)?;
        if options.checkpoint_on_complete {
            self.checkpoint()?;
        }
        Ok(inserted)
    }

    /// Rebuilds a single named index from the persisted table state.
    pub fn rebuild_index(&self, name: &str) -> Result<()> {
        let _writer = self
            .inner
            .sql_write_lock
            .lock()
            .map_err(|_| DbError::internal("SQL writer lock poisoned"))?;
        let mut working = self.engine_snapshot()?;
        working.rebuild_index(name, self.inner.config.page_size)?;
        self.persist_runtime(working).map(|_| ())
    }

    /// Rebuilds all indexes from the persisted table state.
    pub fn rebuild_indexes(&self) -> Result<()> {
        let _writer = self
            .inner
            .sql_write_lock
            .lock()
            .map_err(|_| DbError::internal("SQL writer lock poisoned"))?;
        let mut working = self.engine_snapshot()?;
        working.rebuild_indexes(self.inner.config.page_size)?;
        self.persist_runtime(working).map(|_| ())
    }

    /// Holds a snapshot open until `release_snapshot` is called.
    pub fn hold_snapshot(&self) -> Result<u64> {
        let guard = self.inner.wal.begin_reader()?;
        let token = guard.id();
        self.inner
            .held_snapshots
            .lock()
            .map_err(|_| DbError::internal("snapshot registry lock poisoned"))?
            .insert(token, guard);
        Ok(token)
    }

    /// Releases a snapshot previously acquired with `hold_snapshot`.
    pub fn release_snapshot(&self, token: u64) -> Result<()> {
        self.inner
            .held_snapshots
            .lock()
            .map_err(|_| DbError::internal("snapshot registry lock poisoned"))?
            .remove(&token)
            .ok_or_else(|| DbError::transaction(format!("unknown snapshot token {token}")))?;
        Ok(())
    }

    /// Reads a page using a previously held snapshot token.
    pub fn read_page_for_snapshot(&self, token: u64, page_id: u32) -> Result<Vec<u8>> {
        page::validate_page_id(page_id)?;
        let snapshot_lsn = self
            .inner
            .held_snapshots
            .lock()
            .map_err(|_| DbError::internal("snapshot registry lock poisoned"))?
            .get(&token)
            .map(|guard| guard.snapshot_lsn())
            .ok_or_else(|| DbError::transaction(format!("unknown snapshot token {token}")))?;

        if let Some(wal_page) = self
            .inner
            .wal
            .read_page_at_snapshot(page_id, snapshot_lsn)?
        {
            return Ok(wal_page);
        }
        self.inner.pager.read_page(page_id)
    }

    /// Returns a deterministic JSON summary of storage state for the harness.
    pub fn inspect_storage_state_json(&self) -> Result<String> {
        let header = self.inner.pager.header_snapshot()?;
        let warnings = self.inner.wal.warnings()?;
        Ok(format!(
            "{{\"path\":\"{}\",\"page_size\":{},\"page_count\":{},\"schema_cookie\":{},\"wal_end_lsn\":{},\"wal_file_size\":{},\"wal_path\":\"{}\",\"last_checkpoint_lsn\":{},\"active_readers\":{},\"wal_versions\":{},\"warning_count\":{},\"shared_wal\":{}}}",
            json_escape(self.path().display().to_string()),
            self.inner.config.page_size,
            self.inner.pager.on_disk_page_count()?,
            header.schema_cookie,
            self.inner.wal.latest_snapshot(),
            self.inner.wal.file_size()?,
            json_escape(self.inner.wal.file_path().display().to_string()),
            header.last_checkpoint_lsn,
            self.inner.wal.active_reader_count()?,
            self.inner.wal.version_count()?,
            warnings.len(),
            if self.inner.wal.is_shared() { "true" } else { "false" }
        ))
    }

    /// Returns all table definitions with row-count metadata.
    pub fn list_tables(&self) -> Result<Vec<TableInfo>> {
        let runtime = self.runtime_for_inspection()?;
        Ok(runtime
            .catalog
            .tables
            .values()
            .map(|table| {
                table_info(
                    table,
                    runtime
                        .tables
                        .get(&table.name)
                        .map_or(0, |data| data.rows.len()),
                )
            })
            .collect())
    }

    /// Returns a single table definition by name.
    pub fn describe_table(&self, name: &str) -> Result<TableInfo> {
        let runtime = self.runtime_for_inspection()?;
        let table = runtime
            .catalog
            .tables
            .get(name)
            .ok_or_else(|| DbError::sql(format!("unknown table {name}")))?;
        Ok(table_info(
            table,
            runtime.tables.get(name).map_or(0, |data| data.rows.len()),
        ))
    }

    /// Returns canonical `CREATE TABLE` SQL for a named table.
    pub fn table_ddl(&self, name: &str) -> Result<String> {
        let runtime = self.runtime_for_inspection()?;
        let table = runtime
            .catalog
            .tables
            .get(name)
            .ok_or_else(|| DbError::sql(format!("unknown table {name}")))?;
        Ok(render_create_table(table))
    }

    /// Returns all index definitions.
    pub fn list_indexes(&self) -> Result<Vec<IndexInfo>> {
        let runtime = self.runtime_for_inspection()?;
        Ok(runtime.catalog.indexes.values().map(index_info).collect())
    }

    /// Returns all view definitions.
    pub fn list_views(&self) -> Result<Vec<ViewInfo>> {
        let runtime = self.runtime_for_inspection()?;
        Ok(runtime.catalog.views.values().map(view_info).collect())
    }

    /// Returns canonical `CREATE VIEW` SQL for a named view.
    pub fn view_ddl(&self, name: &str) -> Result<String> {
        let runtime = self.runtime_for_inspection()?;
        let view = runtime
            .catalog
            .views
            .get(name)
            .ok_or_else(|| DbError::sql(format!("unknown view {name}")))?;
        Ok(render_create_view(view))
    }

    /// Returns all trigger definitions.
    pub fn list_triggers(&self) -> Result<Vec<TriggerInfo>> {
        let runtime = self.runtime_for_inspection()?;
        Ok(runtime
            .catalog
            .triggers
            .values()
            .map(trigger_info)
            .collect())
    }

    /// Verifies that a named index can be rebuilt logically from the persisted table state.
    pub fn verify_index(&self, name: &str) -> Result<IndexVerification> {
        let runtime = self.runtime_for_inspection()?;
        let existing = runtime
            .indexes
            .get(name)
            .map_or(0, runtime_index_entry_count);

        let mut rebuilt = self.runtime_for_inspection()?;
        rebuilt.rebuild_index(name, self.inner.config.page_size)?;
        let actual = rebuilt
            .indexes
            .get(name)
            .map_or(0, runtime_index_entry_count);

        Ok(IndexVerification {
            name: name.to_string(),
            valid: existing == actual,
            expected_entries: existing,
            actual_entries: actual,
        })
    }

    /// Dumps the current catalog and table contents as deterministic SQL.
    pub fn dump_sql(&self) -> Result<String> {
        let runtime = self.runtime_for_inspection()?;
        Ok(render_runtime_dump(&runtime))
    }

    /// Installs a global FaultyVfs failpoint used by the storage harness.
    pub fn install_failpoint(
        label: &str,
        action: &str,
        trigger_on: u64,
        value: usize,
    ) -> Result<()> {
        let action = match action {
            "error" => FailAction::Error,
            "partial_read" => FailAction::PartialRead { bytes: value },
            "partial_write" => FailAction::PartialWrite { bytes: value },
            "drop_sync" => FailAction::DropSync,
            _ => {
                return Err(DbError::internal(format!(
                    "unsupported failpoint action {action}"
                )))
            }
        };
        faulty::install_failpoint(Failpoint {
            label: label.to_string(),
            trigger_on,
            action,
        })
    }

    /// Clears all globally installed storage failpoints.
    pub fn clear_failpoints() -> Result<()> {
        faulty::clear_failpoints()
    }

    /// Returns the failpoint decision log as deterministic JSON.
    pub fn failpoint_log_json() -> Result<String> {
        let logs = faulty::failpoint_logs()?;
        let entries = logs
            .into_iter()
            .map(|entry| {
                format!(
                    "{{\"label\":\"{}\",\"hit\":{},\"outcome\":\"{}\"}}",
                    json_escape(entry.label),
                    entry.hit,
                    json_escape(entry.outcome)
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        Ok(format!("[{entries}]"))
    }

    fn open_with_vfs(path: PathBuf, config: DbConfig, vfs: VfsHandle) -> Result<Self> {
        let file = vfs.open(
            &path,
            if vfs.is_memory() {
                OpenMode::OpenOrCreate
            } else {
                OpenMode::OpenExisting
            },
            FileKind::Database,
        )?;
        let header = storage::read_database_header_vfs(file.as_ref())?;
        let mut effective_config = config;
        effective_config.page_size = header.page_size;
        let schema_cookie = header.schema_cookie;

        let pager = PagerHandle::open(Arc::clone(&file), header, effective_config.cache_size_mb)?;
        let wal = WalHandle::acquire(
            &vfs,
            &path,
            effective_config.page_size,
            effective_config.wal_sync_mode,
        )?;
        wal.set_max_page_count(pager.on_disk_page_count()?);
        let runtime = EngineRuntime::load_from_storage(&pager, &wal, schema_cookie)?;
        let catalog = CatalogHandle::new(runtime.catalog.clone());
        let last_runtime_lsn = wal.latest_snapshot();

        Ok(Self {
            inner: Arc::new(DbInner {
                path,
                config: effective_config,
                _vfs: vfs,
                pager,
                wal,
                catalog,
                engine: RwLock::new(runtime),
                last_runtime_lsn: AtomicU64::new(last_runtime_lsn),
                sql_write_lock: Mutex::new(()),
                sql_txn: Mutex::new(None),
                write_txn: Mutex::new(WriteTxn::default()),
                statement_cache: Mutex::new(StatementCache::default()),
                held_snapshots: Mutex::new(HashMap::new()),
            }),
        })
    }

    #[must_use]
    pub fn config(&self) -> &DbConfig {
        &self.inner.config
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    pub fn schema_cookie(&self) -> Result<u32> {
        self.inner.catalog.schema_cookie()
    }

    pub(crate) fn set_schema_cookie(&self, schema_cookie: u32) -> Result<()> {
        self.inner.pager.set_schema_cookie(schema_cookie)
    }

    fn execute_read_statement(
        &self,
        statement: &crate::sql::ast::Statement,
        params: &[Value],
    ) -> Result<QueryResult> {
        if let Some(runtime) = self.transaction_runtime_snapshot()? {
            return runtime.execute_read_statement(statement, params, self.inner.config.page_size);
        }

        self.refresh_engine_from_storage()?;
        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        runtime.execute_read_statement(statement, params, self.inner.config.page_size)
    }

    fn execute_write_statement(
        &self,
        statement: &crate::sql::ast::Statement,
        params: &[Value],
    ) -> Result<QueryResult> {
        if self.in_transaction()? {
            let mut txn = self
                .inner
                .sql_txn
                .lock()
                .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
            let state = txn
                .as_mut()
                .ok_or_else(|| DbError::transaction("no active SQL transaction"))?;
            if matches!(
                statement,
                crate::sql::ast::Statement::Insert(insert)
                    if state.runtime.can_execute_insert_in_place(insert)
            ) {
                return state
                    .runtime
                    .execute_statement(statement, params, self.inner.config.page_size);
            }
            let mut working = state.runtime.clone();
            working.rebuild_stale_indexes(self.inner.config.page_size)?;
            let result =
                working.execute_statement(statement, params, self.inner.config.page_size)?;
            state.runtime = working;
            return Ok(result);
        }

        let _writer = self
            .inner
            .sql_write_lock
            .lock()
            .map_err(|_| DbError::internal("SQL writer lock poisoned"))?;
        self.refresh_engine_from_storage()?;
        let _savepoint = StatementSavepoint::new(self.inner.wal.latest_snapshot());
        if matches!(
            statement,
            crate::sql::ast::Statement::Insert(insert)
                if self
                    .inner
                    .engine
                    .read()
                    .map_err(|_| DbError::internal("engine runtime lock poisoned"))?
                    .can_execute_insert_in_place(insert)
        ) {
            return self.execute_autocommit_insert_in_place(statement, params);
        }
        let mut working = self.engine_snapshot()?;
        let result = working.execute_statement(statement, params, self.inner.config.page_size)?;
        self.persist_runtime(working)?;
        Ok(result)
    }

    fn execute_autocommit_insert_in_place(
        &self,
        statement: &crate::sql::ast::Statement,
        params: &[Value],
    ) -> Result<QueryResult> {
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        let result = match runtime.execute_statement(statement, params, self.inner.config.page_size) {
            Ok(result) => result,
            Err(error) => {
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
        self.begin_write()?;
        if let Err(error) = runtime.persist_to_db(self) {
            let _ = self.rollback();
            self.restore_runtime_from_storage(&mut runtime)?;
            return Err(error);
        }
        let committed_lsn = match self.commit() {
            Ok(lsn) => lsn,
            Err(error) => {
                let _ = self.rollback();
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        self.inner.catalog.replace(runtime.catalog.clone())?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        Ok(result)
    }

    fn engine_snapshot(&self) -> Result<EngineRuntime> {
        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        let mut snapshot = runtime.clone();
        snapshot.rebuild_stale_indexes(self.inner.config.page_size)?;
        Ok(snapshot)
    }

    fn parsed_statement(&self, sql: &str) -> Result<Arc<SqlStatement>> {
        self.inner
            .statement_cache
            .lock()
            .map_err(|_| DbError::internal("statement cache lock poisoned"))?
            .get_or_parse(sql)
    }

    fn persist_runtime(&self, runtime: EngineRuntime) -> Result<u64> {
        self.persist_runtime_if_latest(runtime, None)
    }

    fn persist_runtime_if_latest(
        &self,
        runtime: EngineRuntime,
        expected_latest_lsn: Option<u64>,
    ) -> Result<u64> {
        let mut runtime = runtime;
        runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
        self.begin_write()?;
        if let Err(error) = runtime.persist_to_db(self) {
            let _ = self.rollback();
            return Err(error);
        }
        let committed_lsn = match expected_latest_lsn {
            Some(expected) => self.commit_if_latest(expected),
            None => self.commit(),
        };
        let committed_lsn = match committed_lsn {
            Ok(lsn) => lsn,
            Err(error) => {
                let _ = self.rollback();
                return Err(error);
            }
        };
        self.inner.catalog.replace(runtime.catalog.clone())?;
        let mut guard = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        *guard = runtime;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        Ok(committed_lsn)
    }

    fn runtime_for_inspection(&self) -> Result<EngineRuntime> {
        if let Some(runtime) = self.transaction_runtime_snapshot()? {
            return Ok(runtime);
        }
        self.refresh_engine_from_storage()?;
        self.engine_snapshot()
    }

    fn transaction_runtime_snapshot(&self) -> Result<Option<EngineRuntime>> {
        let txn = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        let Some(state) = txn.as_ref() else {
            return Ok(None);
        };

        let mut snapshot = state.runtime.clone();
        snapshot.rebuild_stale_indexes(self.inner.config.page_size)?;
        Ok(Some(snapshot))
    }

    fn restore_runtime_from_storage(&self, runtime: &mut EngineRuntime) -> Result<()> {
        let schema_cookie = self.current_schema_cookie()?;
        let restored =
            EngineRuntime::load_from_storage(&self.inner.pager, &self.inner.wal, schema_cookie)?;
        self.inner.catalog.replace(restored.catalog.clone())?;
        *runtime = restored;
        self.inner
            .last_runtime_lsn
            .store(self.inner.wal.latest_snapshot(), Ordering::Release);
        Ok(())
    }

    fn refresh_engine_from_storage(&self) -> Result<()> {
        let latest_lsn = self.inner.wal.latest_snapshot();
        if latest_lsn <= self.inner.last_runtime_lsn.load(Ordering::Acquire) {
            return Ok(());
        }

        let schema_cookie = self.current_schema_cookie()?;
        let runtime =
            EngineRuntime::load_from_storage(&self.inner.pager, &self.inner.wal, schema_cookie)?;
        self.inner.catalog.replace(runtime.catalog.clone())?;
        let mut guard = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        *guard = runtime;
        self.inner
            .last_runtime_lsn
            .store(latest_lsn, Ordering::Release);
        Ok(())
    }

    fn current_schema_cookie(&self) -> Result<u32> {
        let page = self.read_page(page::HEADER_PAGE_ID)?;
        let mut bytes = [0_u8; storage::header::DB_HEADER_SIZE];
        bytes.copy_from_slice(&page[..storage::header::DB_HEADER_SIZE]);
        Ok(DatabaseHeader::decode(&bytes)?.schema_cookie)
    }
}

/// Evicts the shared WAL registry entry for an on-disk database path.
pub fn evict_shared_wal(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    let vfs = VfsHandle::for_path(path);
    WalHandle::evict(&vfs, path)
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TransactionControl {
    Begin,
    Commit,
    Rollback,
    Savepoint(String),
    ReleaseSavepoint(String),
    RollbackToSavepoint(String),
}

fn parse_transaction_control(sql: &str) -> Option<TransactionControl> {
    let normalized = normalized_control_sql(sql);
    let upper = normalized.to_ascii_uppercase();

    match upper.as_str() {
        "BEGIN"
        | "BEGIN TRANSACTION"
        | "BEGIN DEFERRED"
        | "BEGIN DEFERRED TRANSACTION"
        | "BEGIN IMMEDIATE"
        | "BEGIN IMMEDIATE TRANSACTION"
        | "BEGIN EXCLUSIVE"
        | "BEGIN EXCLUSIVE TRANSACTION" => Some(TransactionControl::Begin),
        "COMMIT" | "END" | "END TRANSACTION" => Some(TransactionControl::Commit),
        "ROLLBACK" | "ROLLBACK TRANSACTION" => Some(TransactionControl::Rollback),
        _ => parse_savepoint_control(&normalized),
    }
}

fn normalized_control_sql(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn parse_savepoint_control(sql: &str) -> Option<TransactionControl> {
    if let Some(name) = strip_control_prefix(sql, "SAVEPOINT ") {
        return Some(TransactionControl::Savepoint(name.to_string()));
    }
    if let Some(name) = strip_control_prefix(sql, "RELEASE SAVEPOINT ") {
        return Some(TransactionControl::ReleaseSavepoint(name.to_string()));
    }
    if let Some(name) = strip_control_prefix(sql, "RELEASE ") {
        return Some(TransactionControl::ReleaseSavepoint(name.to_string()));
    }
    if let Some(name) = strip_control_prefix(sql, "ROLLBACK TO SAVEPOINT ") {
        return Some(TransactionControl::RollbackToSavepoint(name.to_string()));
    }
    if let Some(name) = strip_control_prefix(sql, "ROLLBACK TRANSACTION TO SAVEPOINT ") {
        return Some(TransactionControl::RollbackToSavepoint(name.to_string()));
    }
    if let Some(name) = strip_control_prefix(sql, "ROLLBACK TO ") {
        return Some(TransactionControl::RollbackToSavepoint(name.to_string()));
    }
    None
}

fn strip_control_prefix<'a>(sql: &'a str, prefix: &str) -> Option<&'a str> {
    if !sql.get(..prefix.len())?.eq_ignore_ascii_case(prefix) {
        return None;
    }
    let remainder = sql[prefix.len()..].trim();
    if remainder.is_empty() {
        None
    } else {
        Some(remainder)
    }
}

fn canonical_savepoint_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

fn split_sql_batch(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while let Some(ch) = chars.next() {
        if in_line_comment {
            current.push(ch);
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }

        if in_block_comment {
            current.push(ch);
            if ch == '*' && matches!(chars.peek(), Some('/')) {
                current.push(chars.next().expect("comment terminator"));
                in_block_comment = false;
            }
            continue;
        }

        if in_single {
            current.push(ch);
            if ch == '\'' {
                if matches!(chars.peek(), Some('\'')) {
                    current.push(chars.next().expect("escaped quote"));
                } else {
                    in_single = false;
                }
            }
            continue;
        }

        if in_double {
            current.push(ch);
            if ch == '"' {
                if matches!(chars.peek(), Some('"')) {
                    current.push(chars.next().expect("escaped quote"));
                } else {
                    in_double = false;
                }
            }
            continue;
        }

        match ch {
            '\'' => {
                in_single = true;
                current.push(ch);
            }
            '"' => {
                in_double = true;
                current.push(ch);
            }
            '-' if matches!(chars.peek(), Some('-')) => {
                current.push(ch);
                current.push(chars.next().expect("line comment start"));
                in_line_comment = true;
            }
            '/' if matches!(chars.peek(), Some('*')) => {
                current.push(ch);
                current.push(chars.next().expect("block comment start"));
                in_block_comment = true;
            }
            ';' => {
                if !current.trim().is_empty() {
                    statements.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        statements.push(current);
    }
    statements
}

fn table_info(table: &TableSchema, row_count: usize) -> TableInfo {
    TableInfo {
        name: table.name.clone(),
        columns: table.columns.iter().map(column_info).collect(),
        checks: table
            .checks
            .iter()
            .map(|check| check.expression_sql.clone())
            .collect(),
        foreign_keys: table.foreign_keys.iter().map(foreign_key_info).collect(),
        primary_key_columns: table.primary_key_columns.clone(),
        row_count,
    }
}

fn column_info(column: &ColumnSchema) -> ColumnInfo {
    ColumnInfo {
        name: column.name.clone(),
        column_type: column.column_type.as_str().to_string(),
        nullable: column.nullable,
        default_sql: column.default_sql.clone(),
        primary_key: column.primary_key,
        unique: column.unique,
        auto_increment: column.auto_increment,
        checks: column
            .checks
            .iter()
            .map(|check| check.expression_sql.clone())
            .collect(),
        foreign_key: column.foreign_key.as_ref().map(foreign_key_info),
    }
}

fn foreign_key_info(foreign_key: &ForeignKeyConstraint) -> ForeignKeyInfo {
    ForeignKeyInfo {
        name: foreign_key.name.clone(),
        columns: foreign_key.columns.clone(),
        referenced_table: foreign_key.referenced_table.clone(),
        referenced_columns: foreign_key.referenced_columns.clone(),
        on_delete: foreign_key_action_name(foreign_key.on_delete).to_string(),
        on_update: foreign_key_action_name(foreign_key.on_update).to_string(),
    }
}

fn index_info(index: &IndexSchema) -> IndexInfo {
    IndexInfo {
        name: index.name.clone(),
        table_name: index.table_name.clone(),
        kind: match index.kind {
            IndexKind::Btree => "btree",
            IndexKind::Trigram => "trigram",
        }
        .to_string(),
        unique: index.unique,
        columns: index.columns.iter().map(index_column_name).collect(),
        predicate_sql: index.predicate_sql.clone(),
        fresh: index.fresh,
    }
}

fn view_info(view: &ViewSchema) -> ViewInfo {
    ViewInfo {
        name: view.name.clone(),
        sql_text: view.sql_text.clone(),
        column_names: view.column_names.clone(),
        dependencies: view.dependencies.clone(),
    }
}

fn trigger_info(trigger: &TriggerSchema) -> TriggerInfo {
    TriggerInfo {
        name: trigger.name.clone(),
        target_name: trigger.target_name.clone(),
        kind: trigger_kind_name(trigger.kind).to_string(),
        event: trigger_event_name(trigger.event).to_string(),
        on_view: trigger.on_view,
        action_sql: trigger.action_sql.clone(),
    }
}

fn index_column_name(column: &IndexColumn) -> String {
    if let Some(name) = &column.column_name {
        name.clone()
    } else if let Some(expression) = &column.expression_sql {
        expression.clone()
    } else {
        "<expr>".to_string()
    }
}

fn foreign_key_action_name(action: ForeignKeyAction) -> &'static str {
    match action {
        ForeignKeyAction::NoAction => "NO ACTION",
        ForeignKeyAction::Restrict => "RESTRICT",
        ForeignKeyAction::Cascade => "CASCADE",
        ForeignKeyAction::SetNull => "SET NULL",
    }
}

fn trigger_kind_name(kind: TriggerKind) -> &'static str {
    match kind {
        TriggerKind::After => "AFTER",
        TriggerKind::InsteadOf => "INSTEAD OF",
    }
}

fn trigger_event_name(event: TriggerEvent) -> &'static str {
    match event {
        TriggerEvent::Insert => "INSERT",
        TriggerEvent::Update => "UPDATE",
        TriggerEvent::Delete => "DELETE",
    }
}

fn runtime_index_entry_count(index: &RuntimeIndex) -> usize {
    match index {
        RuntimeIndex::Btree { keys } => keys.values().map(Vec::len).sum(),
        RuntimeIndex::Trigram { index } => index.entry_count(),
    }
}

fn render_runtime_dump(runtime: &EngineRuntime) -> String {
    let mut lines = Vec::new();

    for table in runtime.catalog.tables.values() {
        lines.push(render_create_table(table));
    }
    for (table_name, table_data) in &runtime.tables {
        if let Some(table) = runtime.catalog.tables.get(table_name) {
            for row in &table_data.rows {
                lines.push(render_insert(table, &row.values));
            }
        }
    }
    for view in runtime.catalog.views.values() {
        lines.push(render_create_view(view));
    }
    for index in runtime.catalog.indexes.values() {
        lines.push(render_create_index(index));
    }
    for trigger in runtime.catalog.triggers.values() {
        lines.push(render_create_trigger(trigger));
    }

    lines.join("\n")
}

fn render_create_table(table: &TableSchema) -> String {
    let mut definitions = Vec::new();
    for column in &table.columns {
        let mut definition = format!(
            "{} {}",
            sql_identifier(&column.name),
            column.column_type.as_str()
        );
        if !column.nullable {
            definition.push_str(" NOT NULL");
        }
        if column.primary_key {
            definition.push_str(" PRIMARY KEY");
        }
        if column.unique {
            definition.push_str(" UNIQUE");
        }
        if column.auto_increment {
            definition.push_str(" AUTOINCREMENT");
        }
        if let Some(default_sql) = &column.default_sql {
            definition.push_str(" DEFAULT ");
            definition.push_str(default_sql);
        }
        for check in &column.checks {
            definition.push_str(" CHECK (");
            definition.push_str(&check.expression_sql);
            definition.push(')');
        }
        if let Some(foreign_key) = &column.foreign_key {
            definition.push(' ');
            definition.push_str(&render_foreign_key(foreign_key));
        }
        definitions.push(definition);
    }

    if table.primary_key_columns.len() > 1 {
        definitions.push(format!(
            "PRIMARY KEY ({})",
            table
                .primary_key_columns
                .iter()
                .map(|name| sql_identifier(name))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    for foreign_key in &table.foreign_keys {
        definitions.push(render_foreign_key(foreign_key));
    }
    for check in &table.checks {
        definitions.push(format!("CHECK ({})", check.expression_sql));
    }

    format!(
        "CREATE TABLE {} ({});",
        sql_identifier(&table.name),
        definitions.join(", ")
    )
}

fn render_foreign_key(foreign_key: &ForeignKeyConstraint) -> String {
    let mut sql = String::new();
    if let Some(name) = &foreign_key.name {
        sql.push_str("CONSTRAINT ");
        sql.push_str(&sql_identifier(name));
        sql.push(' ');
    }
    sql.push_str("FOREIGN KEY (");
    sql.push_str(
        &foreign_key
            .columns
            .iter()
            .map(|name| sql_identifier(name))
            .collect::<Vec<_>>()
            .join(", "),
    );
    sql.push_str(") REFERENCES ");
    sql.push_str(&sql_identifier(&foreign_key.referenced_table));
    sql.push_str(" (");
    sql.push_str(
        &foreign_key
            .referenced_columns
            .iter()
            .map(|name| sql_identifier(name))
            .collect::<Vec<_>>()
            .join(", "),
    );
    sql.push(')');
    if foreign_key.on_delete != ForeignKeyAction::NoAction {
        sql.push_str(" ON DELETE ");
        sql.push_str(foreign_key_action_name(foreign_key.on_delete));
    }
    if foreign_key.on_update != ForeignKeyAction::NoAction {
        sql.push_str(" ON UPDATE ");
        sql.push_str(foreign_key_action_name(foreign_key.on_update));
    }
    sql
}

fn render_insert(table: &TableSchema, values: &[Value]) -> String {
    let columns = table
        .columns
        .iter()
        .map(|column| sql_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let values = values
        .iter()
        .map(render_value_sql)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {} ({columns}) VALUES ({values});",
        sql_identifier(&table.name)
    )
}

fn render_create_view(view: &ViewSchema) -> String {
    let columns = if view.column_names.is_empty() {
        String::new()
    } else {
        format!(
            " ({})",
            view.column_names
                .iter()
                .map(|name| sql_identifier(name))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    format!(
        "CREATE VIEW {}{columns} AS {};",
        sql_identifier(&view.name),
        view.sql_text
    )
}

fn render_create_index(index: &IndexSchema) -> String {
    let unique = if index.unique { "UNIQUE " } else { "" };
    let using = match index.kind {
        IndexKind::Btree => String::new(),
        IndexKind::Trigram => " USING trigram".to_string(),
    };
    let columns = index
        .columns
        .iter()
        .map(index_column_name)
        .collect::<Vec<_>>()
        .join(", ");
    let predicate = index
        .predicate_sql
        .as_ref()
        .map(|predicate| format!(" WHERE {predicate}"))
        .unwrap_or_default();
    format!(
        "CREATE {unique}INDEX {} ON {}{using} ({columns}){predicate};",
        sql_identifier(&index.name),
        sql_identifier(&index.table_name)
    )
}

fn render_create_trigger(trigger: &TriggerSchema) -> String {
    format!(
        "CREATE TRIGGER {} {} {} ON {} FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql({});",
        sql_identifier(&trigger.name),
        trigger_kind_name(trigger.kind),
        trigger_event_name(trigger.event),
        sql_identifier(&trigger.target_name),
        render_value_sql(&Value::Text(trigger.action_sql.clone()))
    )
}

fn render_value_sql(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Float64(value) => {
            if value.is_finite() {
                value.to_string()
            } else {
                "NULL".to_string()
            }
        }
        Value::Bool(value) => {
            if *value {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Value::Text(value) => format!("'{}'", value.replace('\'', "''")),
        Value::Blob(value) => format!("X'{}'", hex_encode(value)),
        Value::Decimal { scaled, scale } => {
            if *scale == 0 {
                scaled.to_string()
            } else {
                let negative = *scaled < 0;
                let digits = scaled.unsigned_abs().to_string();
                let scale = usize::from(*scale);
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
        }
        Value::Uuid(value) => format!("X'{}'", hex_encode(value)),
        Value::TimestampMicros(value) => value.to_string(),
    }
}

fn sql_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;

        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn json_escape(input: String) -> String {
    input
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::StatementCache;

    #[test]
    fn statement_cache_reuses_parsed_statement() {
        let mut cache = StatementCache::with_capacity(4);
        let first = cache.get_or_parse("SELECT 1").expect("parse");
        let second = cache.get_or_parse("SELECT 1").expect("cache hit");
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn statement_cache_evicts_oldest_entry_when_full() {
        let mut cache = StatementCache::with_capacity(1);
        let first = cache.get_or_parse("SELECT 1").expect("parse first");
        let _second = cache.get_or_parse("SELECT 2").expect("parse second");
        let first_again = cache.get_or_parse("SELECT 1").expect("reparse evicted");
        assert!(!Arc::ptr_eq(&first, &first_again));
    }
}

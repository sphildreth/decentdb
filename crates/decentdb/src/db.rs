//! Stable database owner and bootstrap lifecycle entry points.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};

use crate::catalog::{
    identifiers_equal, CatalogHandle, CheckConstraint, ColumnSchema, ForeignKeyAction,
    ForeignKeyConstraint, IndexColumn, IndexKind, IndexSchema, TableSchema, TriggerEvent,
    TriggerKind, TriggerSchema, ViewSchema,
};
use crate::config::DbConfig;
use crate::error::{DbError, Result};
use crate::exec::dml::{
    PreparedInsertValueSource, PreparedSimpleDelete, PreparedSimpleInsert, PreparedSimpleUpdate,
};
use crate::exec::{
    statement_is_read_only, BulkLoadOptions, EngineRuntime, QueryResult, QueryRow, RuntimeIndex,
    TableData,
};
use crate::metadata::{
    CheckConstraintInfo, ColumnInfo, ForeignKeyInfo, HeaderInfo, IndexInfo, IndexVerification,
    SchemaColumnInfo, SchemaIndexInfo, SchemaSnapshot, SchemaTableInfo, SchemaTriggerInfo,
    SchemaViewInfo, StorageInfo, TableInfo, TriggerInfo, ViewInfo,
};
use crate::record::value::Value;
use crate::sql::ast::Statement as SqlStatement;
use crate::sql::parser::{parse_sql_statement, rewrite_legacy_trigger_body};
use crate::storage::freelist::{decode_freelist_next, encode_freelist_page};
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

/// Reusable single-statement execution handle bound to the current schema.
///
/// Prepared statements become invalid after schema changes and must be
/// re-prepared. Data changes remain visible across executions.
#[derive(Clone, Debug)]
pub struct PreparedStatement {
    db: Db,
    schema_cookie: u32,
    temp_schema_cookie: u32,
    statement: Arc<SqlStatement>,
    prepared_insert: Option<Arc<PreparedSimpleInsert>>,
    prepared_update: Option<Arc<PreparedSimpleUpdate>>,
    prepared_delete: Option<Arc<PreparedSimpleDelete>>,
    read_only: bool,
    in_state_without_clone: bool,
}

/// Exclusive SQL transaction handle that keeps mutable runtime state reserved.
///
/// While this handle is active, callers should use it for all SQL work on the
/// same `Db` handle until `commit` or `rollback`.
#[derive(Debug)]
pub struct SqlTransaction<'a> {
    db: &'a Db,
    state: Option<ExclusiveSqlTxnState<'a>>,
}

impl PreparedStatement {
    /// Executes the prepared statement with the provided positional `$n`
    /// parameters.
    pub fn execute(&self, params: &[Value]) -> Result<QueryResult> {
        self.db.execute_prepared_statement(self, params)
    }

    /// Executes the prepared statement inside an active [`SqlTransaction`].
    pub fn execute_in(
        &self,
        txn: &mut SqlTransaction<'_>,
        params: &[Value],
    ) -> Result<QueryResult> {
        txn.execute_prepared(self, params)
    }
}

impl SqlTransaction<'_> {
    /// Prepares a single SQL statement against this transaction's current schema.
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        let state = self
            .state
            .as_ref()
            .ok_or_else(|| DbError::transaction("SQL transaction handle is no longer active"))?;
        self.db.prepare_with_runtime(sql, &state.runtime)
    }

    /// Executes a prepared statement inside this transaction without per-row
    /// `Db` transaction lock churn.
    pub fn execute_prepared(
        &mut self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<QueryResult> {
        let state = self
            .state
            .as_mut()
            .ok_or_else(|| DbError::transaction("SQL transaction handle is no longer active"))?;
        self.db
            .execute_prepared_in_exclusive_state(prepared, params, state)
    }

    /// Commits this transaction's reserved runtime into the WAL-backed database.
    pub fn commit(mut self) -> Result<u64> {
        let state = self
            .state
            .take()
            .ok_or_else(|| DbError::transaction("SQL transaction handle is no longer active"))?;
        let result = self.db.commit_exclusive_sql_txn(state);
        let release = self.deactivate();
        match (result, release) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(lsn), Ok(())) => Ok(lsn),
        }
    }

    /// Rolls this transaction back and releases the handle.
    pub fn rollback(mut self) -> Result<()> {
        let state = self
            .state
            .take()
            .ok_or_else(|| DbError::transaction("SQL transaction handle is no longer active"))?;
        let result = self.db.rollback_exclusive_sql_txn(state);
        self.deactivate().and(result)
    }

    fn deactivate(&mut self) -> Result<()> {
        let mut txn = self
            .db
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        *txn = SqlTxnSlot::None;
        self.db.inner.sql_txn_active.store(false, Ordering::Release);
        Ok(())
    }
}

impl Drop for SqlTransaction<'_> {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            let _ = self.db.rollback_exclusive_sql_txn(state);
        }
        if let Ok(mut txn) = self.db.inner.sql_txn.lock() {
            *txn = SqlTxnSlot::None;
            self.db.inner.sql_txn_active.store(false, Ordering::Release);
        }
    }
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
    writer_last_commit_lsn: AtomicU64,
    last_seen_checkpoint_epoch: AtomicU64,
    sql_write_lock: Mutex<()>,
    sql_txn: Mutex<SqlTxnSlot>,
    sql_txn_active: AtomicBool,
    write_txn: Mutex<WriteTxn>,
    temp_state: Mutex<TempSchemaState>,
    statement_cache: Mutex<StatementCache>,
    prepared_insert_cache: Mutex<PreparedInsertCache>,
    held_snapshots: Mutex<HashMap<u64, ReaderGuard>>,
}

impl Drop for DbInner {
    fn drop(&mut self) {
        if self.wal.latest_snapshot() == 0 {
            return;
        }
        self.wal.set_checkpoint_pending(true);
        if self.wal.strong_handle_count() != 1 {
            self.wal.set_checkpoint_pending(false);
            return;
        }
        if self.write_txn.lock().map(|txn| txn.active).unwrap_or(true) {
            self.wal.set_checkpoint_pending(false);
            return;
        }
        if self
            .sql_txn
            .lock()
            .map(|txn| !matches!(*txn, SqlTxnSlot::None))
            .unwrap_or(true)
        {
            self.wal.set_checkpoint_pending(false);
            return;
        }
        if let Ok(mut held_snapshots) = self.held_snapshots.lock() {
            held_snapshots.clear();
        }
        let _ = self
            .wal
            .checkpoint(&self.pager, self.config.checkpoint_timeout_sec);
    }
}

#[derive(Debug, Default)]
struct WriteTxn {
    active: bool,
    staged_pages: BTreeMap<PageId, Vec<u8>>,
}

#[derive(Clone, Debug, Default)]
struct TempSchemaState {
    schema_cookie: u32,
    tables: Arc<BTreeMap<String, TableSchema>>,
    table_data: Arc<BTreeMap<String, Arc<TableData>>>,
    views: Arc<BTreeMap<String, ViewSchema>>,
    indexes: Arc<BTreeMap<String, IndexSchema>>,
}

impl TempSchemaState {
    fn apply_to_runtime(&self, runtime: &mut EngineRuntime) {
        runtime.temp_schema_cookie = self.schema_cookie;
        runtime.temp_tables = Arc::clone(&self.tables);
        runtime.temp_table_data = Arc::clone(&self.table_data);
        runtime.temp_views = Arc::clone(&self.views);
        runtime.temp_indexes = Arc::clone(&self.indexes);
    }

    fn update_from_runtime(&mut self, runtime: &EngineRuntime) {
        self.schema_cookie = runtime.temp_schema_cookie;
        self.tables = Arc::clone(&runtime.temp_tables);
        self.table_data = Arc::clone(&runtime.temp_table_data);
        self.views = Arc::clone(&runtime.temp_views);
        self.indexes = Arc::clone(&runtime.temp_indexes);
    }
}

#[derive(Debug)]
struct SqlTxnState {
    runtime: EngineRuntime,
    base_lsn: u64,
    base_checkpoint_epoch: u64,
    persistent_changed: bool,
    indexes_maybe_stale: bool,
    savepoints: Vec<SqlSavepoint>,
}

#[derive(Debug)]
struct ExclusiveSqlTxnState<'a> {
    runtime: RwLockWriteGuard<'a, EngineRuntime>,
    base_lsn: u64,
    base_checkpoint_epoch: u64,
    persistent_changed: bool,
    indexes_maybe_stale: bool,
}

#[derive(Debug)]
enum SqlTxnSlot {
    None,
    Shared(Box<SqlTxnState>),
    Exclusive,
}

#[derive(Clone, Debug)]
struct SqlSavepoint {
    name: String,
    runtime: EngineRuntime,
    persistent_changed: bool,
    indexes_maybe_stale: bool,
}

const STATEMENT_CACHE_CAPACITY: usize = 128;
const PREPARED_INSERT_CACHE_CAPACITY: usize = 128;

#[derive(Debug)]
struct StatementCache {
    entries: HashMap<String, Arc<SqlStatement>>,
    order: VecDeque<String>,
    capacity: usize,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PreparedInsertKey {
    schema_cookie: u32,
    temp_schema_cookie: u32,
    sql: String,
}

#[derive(Debug)]
struct PreparedInsertCache {
    entries: HashMap<PreparedInsertKey, Arc<PreparedSimpleInsert>>,
    order: VecDeque<PreparedInsertKey>,
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

impl Default for PreparedInsertCache {
    fn default() -> Self {
        Self::with_capacity(PREPARED_INSERT_CACHE_CAPACITY)
    }
}

impl PreparedInsertCache {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    fn get_or_prepare<F>(
        &mut self,
        sql: &str,
        schema_cookie: u32,
        temp_schema_cookie: u32,
        build: F,
    ) -> Result<Option<Arc<PreparedSimpleInsert>>>
    where
        F: FnOnce() -> Result<Option<PreparedSimpleInsert>>,
    {
        let key = PreparedInsertKey {
            schema_cookie,
            temp_schema_cookie,
            sql: sql.to_string(),
        };
        if let Some(plan) = self.entries.get(&key) {
            return Ok(Some(Arc::clone(plan)));
        }

        let Some(plan) = build()? else {
            return Ok(None);
        };
        let plan = Arc::new(plan);
        if self.capacity == 0 {
            return Ok(Some(plan));
        }

        while self.entries.len() >= self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            } else {
                break;
            }
        }

        self.order.push_back(key.clone());
        self.entries.insert(key, Arc::clone(&plan));
        Ok(Some(plan))
    }
}

impl Db {
    /// Reads the raw database header without opening the entire database engine
    /// or validating the format version. This is useful for inspection utilities
    /// and pre-flight format validation.
    pub fn read_header_info(path: impl AsRef<Path>) -> Result<HeaderInfo> {
        let path = path.as_ref();
        let vfs = VfsHandle::for_path(path);
        let file = vfs.open(path, OpenMode::OpenExisting, FileKind::Database)?;
        let header = storage::read_database_header_vfs_loose(file.as_ref())?;
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

    /// Begins an exclusive SQL transaction handle that reserves mutable runtime
    /// state until commit or rollback.
    pub fn transaction(&self) -> Result<SqlTransaction<'_>> {
        let state = self.build_exclusive_sql_txn_state()?;
        let mut txn = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        if !matches!(*txn, SqlTxnSlot::None) {
            return Err(DbError::transaction(
                "SQL transaction is already active on this handle",
            ));
        }
        *txn = SqlTxnSlot::Exclusive;
        self.inner.sql_txn_active.store(true, Ordering::Release);
        Ok(SqlTransaction {
            db: self,
            state: Some(state),
        })
    }

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
        let state = self.build_sql_txn_state()?;
        let mut txn = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        if !matches!(*txn, SqlTxnSlot::None) {
            return Err(DbError::transaction(
                "SQL transaction is already active on this handle",
            ));
        }
        *txn = SqlTxnSlot::Shared(Box::new(state));
        self.inner.sql_txn_active.store(true, Ordering::Release);
        Ok(())
    }

    /// Commits the current explicit SQL transaction.
    pub fn commit_transaction(&self) -> Result<u64> {
        let state = {
            let mut txn = self
                .inner
                .sql_txn
                .lock()
                .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
            match std::mem::replace(&mut *txn, SqlTxnSlot::None) {
                SqlTxnSlot::Shared(state) => {
                    self.inner.sql_txn_active.store(false, Ordering::Release);
                    *state
                }
                SqlTxnSlot::Exclusive => {
                    *txn = SqlTxnSlot::Exclusive;
                    self.inner.sql_txn_active.store(true, Ordering::Release);
                    return Err(self.exclusive_sql_txn_error());
                }
                SqlTxnSlot::None => {
                    self.inner.sql_txn_active.store(false, Ordering::Release);
                    return Err(DbError::transaction("no active SQL transaction to commit"));
                }
            }
        };
        if !state.persistent_changed {
            self.install_temp_runtime(state.runtime)?;
            return Ok(state.base_lsn);
        }
        self.persist_runtime_if_latest(
            state.runtime,
            Some((state.base_lsn, state.base_checkpoint_epoch)),
            state.indexes_maybe_stale,
        )
    }

    /// Rolls back the current explicit SQL transaction.
    pub fn rollback_transaction(&self) -> Result<()> {
        let mut txn = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        match *txn {
            SqlTxnSlot::Shared(_) => {
                *txn = SqlTxnSlot::None;
                self.inner.sql_txn_active.store(false, Ordering::Release);
                Ok(())
            }
            SqlTxnSlot::Exclusive => Err(self.exclusive_sql_txn_error()),
            SqlTxnSlot::None => Err(DbError::transaction(
                "no active SQL transaction to roll back",
            )),
        }
    }

    /// Returns whether this handle currently has an explicit SQL transaction.
    pub fn in_transaction(&self) -> Result<bool> {
        if !self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Ok(false);
        }
        self.inner
            .sql_txn
            .lock()
            .map(|txn| !matches!(*txn, SqlTxnSlot::None))
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))
    }

    /// Creates a named savepoint inside the current explicit SQL transaction.
    pub fn create_savepoint(&self, name: &str) -> Result<()> {
        let mut txn = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        let state = match &mut *txn {
            SqlTxnSlot::Shared(state) => state,
            SqlTxnSlot::Exclusive => return Err(self.exclusive_sql_txn_error()),
            SqlTxnSlot::None => {
                return Err(DbError::transaction(
                    "SAVEPOINT requires an active SQL transaction",
                ));
            }
        };
        state.savepoints.push(SqlSavepoint {
            name: canonical_savepoint_name(name),
            runtime: state.runtime.clone(),
            persistent_changed: state.persistent_changed,
            indexes_maybe_stale: state.indexes_maybe_stale,
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
        let state = match &mut *txn {
            SqlTxnSlot::Shared(state) => state,
            SqlTxnSlot::Exclusive => return Err(self.exclusive_sql_txn_error()),
            SqlTxnSlot::None => {
                return Err(DbError::transaction(
                    "RELEASE SAVEPOINT requires an active SQL transaction",
                ));
            }
        };
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
        let state = match &mut *txn {
            SqlTxnSlot::Shared(state) => state,
            SqlTxnSlot::Exclusive => return Err(self.exclusive_sql_txn_error()),
            SqlTxnSlot::None => {
                return Err(DbError::transaction(
                    "ROLLBACK TO SAVEPOINT requires an active SQL transaction",
                ));
            }
        };
        let target = canonical_savepoint_name(name);
        let index = state
            .savepoints
            .iter()
            .rposition(|savepoint| savepoint.name == target)
            .ok_or_else(|| DbError::transaction(format!("savepoint {name} does not exist")))?;
        state.runtime = state.savepoints[index].runtime.clone();
        state.persistent_changed = state.savepoints[index].persistent_changed;
        state.indexes_maybe_stale = state.savepoints[index].indexes_maybe_stale;
        state.savepoints.truncate(index + 1);
        Ok(())
    }

    /// Returns a structured snapshot of the current storage state.
    pub fn storage_info(&self) -> Result<StorageInfo> {
        let header = self.inner.pager.header_snapshot()?;
        Ok(StorageInfo {
            path: self.path().to_path_buf(),
            wal_path: self.inner.wal.file_path().to_path_buf(),
            format_version: header.format_version,
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

    pub(crate) fn write_page_owned(&self, page_id: u32, data: Vec<u8>) -> Result<()> {
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
        txn.staged_pages.insert(page_id, data);
        Ok(())
    }

    /// Allocates a new page from the freelist or file tail.
    pub fn allocate_page(&self) -> Result<u32> {
        let mut txn = self
            .inner
            .write_txn
            .lock()
            .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
        if !txn.active {
            return Err(DbError::transaction(
                "allocate_page requires an active write transaction",
            ));
        }

        let mut header = self.write_txn_header(&txn)?;
        if header.freelist.head_page_id != 0 {
            let page_id = header.freelist.head_page_id;
            let freelist_page = self.write_txn_visible_page(&txn, page_id)?;
            let next = decode_freelist_next(&freelist_page)?;
            header.freelist.head_page_id = next;
            header.freelist.page_count = header.freelist.page_count.saturating_sub(1);
            self.stage_write_txn_header(&mut txn, &header);
            txn.staged_pages
                .insert(page_id, page::zeroed_page(self.inner.config.page_size));
            return Ok(page_id);
        }

        let max_staged_page_id = txn.staged_pages.keys().copied().max().unwrap_or(0);
        let next_page_id = self
            .inner
            .pager
            .on_disk_page_count()?
            .max(self.inner.wal.max_page_count())
            .max(max_staged_page_id)
            .saturating_add(1);
        txn.staged_pages
            .entry(next_page_id)
            .or_insert_with(|| page::zeroed_page(self.inner.config.page_size));
        Ok(next_page_id)
    }

    /// Frees an existing page back to the freelist.
    pub fn free_page(&self, page_id: u32) -> Result<()> {
        page::validate_page_id(page_id)?;
        if page_id <= page::CATALOG_ROOT_PAGE_ID {
            return Err(DbError::transaction(format!(
                "page {page_id} is reserved and cannot be freed"
            )));
        }
        let mut txn = self
            .inner
            .write_txn
            .lock()
            .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
        if !txn.active {
            return Err(DbError::transaction(
                "free_page requires an active write transaction",
            ));
        }

        let mut header = self.write_txn_header(&txn)?;
        let page_bytes =
            encode_freelist_page(self.inner.config.page_size, header.freelist.head_page_id);
        txn.staged_pages.insert(page_id, page_bytes);
        header.freelist.head_page_id = page_id;
        header.freelist.page_count = header.freelist.page_count.saturating_add(1);
        self.stage_write_txn_header(&mut txn, &header);
        Ok(())
    }

    /// Commits the current write transaction to the WAL.
    pub fn commit(&self) -> Result<u64> {
        let (max_page_id, pages) = {
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
            let max_page_id = txn
                .staged_pages
                .last_key_value()
                .map_or(0, |(page_id, _)| *page_id);
            (max_page_id, std::mem::take(&mut txn.staged_pages))
        };

        let pages: Vec<_> = pages.into_iter().collect();
        let max_page_count = self.inner.wal.max_page_count().max(max_page_id);
        self.inner
            .wal
            .commit_pages(&self.inner.pager, pages, max_page_count)
    }

    fn commit_if_latest(
        &self,
        expected_latest_lsn: u64,
        expected_checkpoint_epoch: u64,
    ) -> Result<u64> {
        let (max_page_id, pages) = {
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
            let max_page_id = txn
                .staged_pages
                .last_key_value()
                .map_or(0, |(page_id, _)| *page_id);
            (max_page_id, std::mem::take(&mut txn.staged_pages))
        };

        let pages: Vec<_> = pages.into_iter().collect();
        let max_page_count = self.inner.wal.max_page_count().max(max_page_id);
        self.inner.wal.commit_pages_if_latest(
            &self.inner.pager,
            pages,
            max_page_count,
            expected_latest_lsn,
            expected_checkpoint_epoch,
        )
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

    fn write_txn_visible_page(&self, txn: &WriteTxn, page_id: PageId) -> Result<Arc<[u8]>> {
        if let Some(staged) = txn.staged_pages.get(&page_id).cloned() {
            return Ok(Arc::from(staged));
        }

        let reader = self.inner.wal.begin_reader()?;
        let snapshot_lsn = reader.snapshot_lsn();
        if let Some(wal_page) = self
            .inner
            .wal
            .read_page_at_snapshot(page_id, snapshot_lsn)?
        {
            return Ok(wal_page);
        }
        self.inner.pager.read_page(page_id)
    }

    fn write_txn_header(&self, txn: &WriteTxn) -> Result<DatabaseHeader> {
        let page = self.write_txn_visible_page(txn, page::HEADER_PAGE_ID)?;
        let mut bytes = [0_u8; storage::header::DB_HEADER_SIZE];
        bytes.copy_from_slice(&page[..storage::header::DB_HEADER_SIZE]);
        DatabaseHeader::decode(&bytes)
    }

    fn stage_write_txn_header(&self, txn: &mut WriteTxn, header: &DatabaseHeader) {
        let mut page = page::zeroed_page(self.inner.config.page_size);
        page[..storage::header::DB_HEADER_SIZE].copy_from_slice(&header.encode());
        txn.staged_pages.insert(page::HEADER_PAGE_ID, page);
    }

    /// Reads the latest visible version of a page.
    pub fn read_page(&self, page_id: u32) -> Result<Arc<[u8]>> {
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
            return Ok(Arc::from(staged));
        }

        let reader = self.inner.wal.begin_reader()?;
        let snapshot_lsn = reader.snapshot_lsn();
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
        self.compact_persisted_payloads_before_checkpoint()?;
        self.inner
            .wal
            .checkpoint(&self.inner.pager, self.inner.config.checkpoint_timeout_sec)
    }

    /// Blocks until every commit acknowledged before this call is durable on
    /// disk.
    ///
    /// For the default [`crate::WalSyncMode::Full`] mode (and `Normal`), every
    /// commit is already synchronously durable when it returns, so this is a
    /// cheap no-op. Under [`crate::WalSyncMode::AsyncCommit`] it forces the
    /// background flusher to run and waits until the WAL is on stable storage.
    ///
    /// See `design/adr/0135-async-commit-wal-group-commit.md`.
    pub fn sync(&self) -> Result<()> {
        self.inner.wal.flush_to_durable()
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
            if let Some(pragma) = parse_pragma_command(trimmed)? {
                let result = self.execute_pragma_command(pragma)?;
                results.push(result);
                continue;
            }

            let statement = self.parsed_statement(trimmed)?;
            let result = if statement_is_read_only(&statement) {
                self.execute_read_statement(&statement, params)?
            } else {
                self.execute_write_statement(trimmed, &statement, params)?
            };
            results.push(result);
        }
        Ok(results)
    }

    /// Prepares a single SQL statement for repeated execution.
    ///
    /// Prepared statements are bound to the current schema cookie. If the schema
    /// changes, the handle must be recreated before it can be executed again.
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        let runtime = self.runtime_for_prepare()?;
        self.prepare_with_runtime(sql, &runtime)
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
        self.refresh_and_ensure_all_tables_loaded()?;
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
        self.refresh_and_ensure_all_tables_loaded()?;
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
        self.refresh_and_ensure_all_tables_loaded()?;
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
    pub fn read_page_for_snapshot(&self, token: u64, page_id: u32) -> Result<Arc<[u8]>> {
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
        // ADR 0143 Phase A: surface per-runtime row residency so callers can
        // verify that Phases B/C/D close the gap between db_file_bytes and
        // tables_in_memory_bytes.
        let (rows_total, bytes_total, table_count, deferred_count) = {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            runtime.table_memory_totals()
        };
        Ok(format!(
            "{{\"path\":\"{}\",\"page_size\":{},\"page_count\":{},\"schema_cookie\":{},\"wal_end_lsn\":{},\"wal_file_size\":{},\"wal_path\":\"{}\",\"last_checkpoint_lsn\":{},\"active_readers\":{},\"wal_versions\":{},\"warning_count\":{},\"shared_wal\":{},\"tables_in_memory_bytes\":{},\"rows_in_memory_count\":{},\"loaded_table_count\":{},\"deferred_table_count\":{}}}",
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
            if self.inner.wal.is_shared() { "true" } else { "false" },
            bytes_total,
            rows_total,
            table_count,
            deferred_count,
        ))
    }

    /// Returns all table definitions with row-count metadata.
    pub fn list_tables(&self) -> Result<Vec<TableInfo>> {
        let runtime = self.runtime_for_inspection()?;
        let mut tables = runtime
            .catalog
            .tables
            .values()
            .map(|table| {
                table_info(
                    table,
                    runtime
                        .table_data(&table.name)
                        .map_or(0, |data| data.rows.len()),
                )
            })
            .collect::<Vec<_>>();
        tables.extend(runtime.temp_tables.values().map(|table| {
            table_info(
                table,
                runtime
                    .temp_table_data
                    .get(&table.name)
                    .map_or(0, |data| data.rows.len()),
            )
        }));
        Ok(tables)
    }

    /// Returns a single table definition by name.
    pub fn describe_table(&self, name: &str) -> Result<TableInfo> {
        let runtime = self.runtime_for_inspection()?;
        if runtime.temp_views.contains_key(name) && !runtime.temp_tables.contains_key(name) {
            return Err(DbError::sql(format!("unknown table {name}")));
        }
        let (table, row_count) = if let Some(table) = runtime.temp_tables.get(name) {
            (
                table,
                runtime
                    .temp_table_data
                    .get(name)
                    .map_or(0, |data| data.rows.len()),
            )
        } else {
            let table = runtime
                .catalog
                .tables
                .get(name)
                .ok_or_else(|| DbError::sql(format!("unknown table {name}")))?;
            (
                table,
                runtime.table_data(name).map_or(0, |data| data.rows.len()),
            )
        };
        Ok(table_info(table, row_count))
    }

    /// Returns canonical `CREATE TABLE` SQL for a named table.
    pub fn table_ddl(&self, name: &str) -> Result<String> {
        let runtime = self.runtime_for_inspection()?;
        if runtime.temp_views.contains_key(name) && !runtime.temp_tables.contains_key(name) {
            return Err(DbError::sql(format!("unknown table {name}")));
        }
        let table = runtime
            .temp_tables
            .get(name)
            .or_else(|| runtime.catalog.tables.get(name))
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
        let mut views = runtime
            .catalog
            .views
            .values()
            .map(view_info)
            .collect::<Vec<_>>();
        views.extend(runtime.temp_views.values().map(view_info));
        Ok(views)
    }

    /// Returns canonical `CREATE VIEW` SQL for a named view.
    pub fn view_ddl(&self, name: &str) -> Result<String> {
        let runtime = self.runtime_for_inspection()?;
        if runtime.temp_tables.contains_key(name) && !runtime.temp_views.contains_key(name) {
            return Err(DbError::sql(format!("unknown view {name}")));
        }
        let view = runtime
            .temp_views
            .get(name)
            .or_else(|| runtime.catalog.views.get(name))
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

    /// Returns the authoritative rich schema snapshot for bindings and tooling.
    pub fn get_schema_snapshot(&self) -> Result<SchemaSnapshot> {
        let runtime = self.runtime_for_inspection()?;
        Ok(schema_snapshot(&runtime))
    }

    /// Verifies that a named index can be rebuilt logically from the persisted table state.
    pub fn verify_index(&self, name: &str) -> Result<IndexVerification> {
        let runtime = self.runtime_for_inspection()?;
        let existing = runtime.index(name).map_or(0, runtime_index_entry_count);

        let mut rebuilt = self.runtime_for_inspection()?;
        rebuilt.rebuild_index(name, self.inner.config.page_size)?;
        let actual = rebuilt.index(name).map_or(0, runtime_index_entry_count);

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
        let wal = WalHandle::acquire(&vfs, &path, &effective_config, &pager)?;
        wal.set_max_page_count(pager.on_disk_page_count()?);

        // ADR 0143 engine-memory plan: drop the in-memory WAL page-version
        // index before loading the runtime when the on-disk WAL is large.
        // Without this, re-opening a database with an uncheckpointed
        // multi-hundred-MB WAL leaves the entire page-version chain
        // resident (~one Arc<[u8]> per WAL frame), which is what the
        // 2026-04-22 memory probes were reporting as `wal_versions=82191`
        // / 336 MB of pure index. The synchronous checkpoint does the
        // copyback into the data file, then truncates the WAL header so
        // downstream reads service straight from the page cache.
        let on_open_threshold_bytes =
            u64::from(effective_config.auto_checkpoint_on_open_mb) * 1024 * 1024;
        if on_open_threshold_bytes > 0 {
            let wal_size = wal
                .latest_snapshot()
                .saturating_sub(crate::wal::format::WAL_HEADER_SIZE);
            if wal_size > on_open_threshold_bytes {
                // Best-effort: a checkpoint failure here is not fatal,
                // because the runtime load below will still succeed
                // against the existing WAL state. Surface it as a
                // warning-shaped log so embedders can investigate.
                if let Err(_e) = wal.checkpoint(&pager, effective_config.checkpoint_timeout_sec) {}
            }
        }

        let (runtime, runtime_lsn) =
            EngineRuntime::load_from_storage(&pager, &wal, schema_cookie, &effective_config)?;
        let catalog = CatalogHandle::new(runtime.catalog.as_ref().clone());
        let last_seen_checkpoint_epoch = wal.checkpoint_epoch();

        let db = Self {
            inner: Arc::new(DbInner {
                path,
                config: effective_config,
                _vfs: vfs,
                pager,
                wal,
                catalog,
                engine: RwLock::new(runtime),
                last_runtime_lsn: AtomicU64::new(runtime_lsn),
                writer_last_commit_lsn: AtomicU64::new(0),
                last_seen_checkpoint_epoch: AtomicU64::new(last_seen_checkpoint_epoch),
                sql_write_lock: Mutex::new(()),
                sql_txn: Mutex::new(SqlTxnSlot::None),
                sql_txn_active: AtomicBool::new(false),
                write_txn: Mutex::new(WriteTxn::default()),
                temp_state: Mutex::new(TempSchemaState::default()),
                statement_cache: Mutex::new(StatementCache::default()),
                prepared_insert_cache: Mutex::new(PreparedInsertCache::default()),
                held_snapshots: Mutex::new(HashMap::new()),
            }),
        };
        db.backfill_missing_persistent_pk_indexes()?;
        Ok(db)
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

        self.execute_nontransaction_read_statement(statement, params, None)
    }

    fn execute_nontransaction_read_statement(
        &self,
        statement: &SqlStatement,
        params: &[Value],
        prepared: Option<&PreparedStatement>,
    ) -> Result<QueryResult> {
        if !self.inner.config.defer_table_materialization {
            self.refresh_engine_from_storage()?;
            self.ensure_all_tables_loaded()?;
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            self.validate_prepared_against_runtime(prepared, &runtime)?;
            return runtime.execute_read_statement(statement, params, self.inner.config.page_size);
        }

        let reader = self.inner.wal.begin_reader()?;
        let snapshot_lsn = reader.snapshot_lsn();
        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        if let SqlStatement::Query(query) = statement {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            self.validate_prepared_against_runtime(prepared, &runtime)?;
            if let Some(result) = runtime.try_execute_simple_deferred_indexed_projection_query(
                query,
                params,
                &self.inner.pager,
                &self.inner.wal,
                snapshot_lsn,
                self.inner.config.persistent_pk_index,
            )? {
                return Ok(result);
            }
            if let Some(result) = runtime.try_execute_simple_deferred_paged_query(
                query,
                params,
                &self.inner.pager,
                &self.inner.wal,
                snapshot_lsn,
            )? {
                return Ok(result);
            }
        }

        let targeted_ok =
            self.ensure_tables_loaded_for_statement_at_snapshot(statement, Some(snapshot_lsn))?;
        if !targeted_ok {
            self.ensure_all_tables_loaded_at_snapshot(Some(snapshot_lsn))?;
        }
        drop(reader);

        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        self.validate_prepared_against_runtime(prepared, &runtime)?;
        runtime.execute_read_statement(statement, params, self.inner.config.page_size)
    }

    fn validate_prepared_against_runtime(
        &self,
        prepared: Option<&PreparedStatement>,
        runtime: &EngineRuntime,
    ) -> Result<()> {
        if let Some(prepared) = prepared {
            self.validate_prepared_schema_cookie(
                prepared,
                runtime.catalog.schema_cookie,
                runtime.temp_schema_cookie,
            )?;
        }
        Ok(())
    }

    fn execute_pragma_command(&self, command: PragmaCommand) -> Result<QueryResult> {
        match command {
            PragmaCommand::Query(PragmaName::PageSize) => Ok(QueryResult::with_rows(
                vec!["page_size".to_string()],
                vec![QueryRow::new(vec![Value::Int64(i64::from(
                    self.inner.config.page_size,
                ))])],
            )),
            PragmaCommand::Query(PragmaName::CacheSize) => Ok(QueryResult::with_rows(
                vec!["cache_size".to_string()],
                vec![QueryRow::new(vec![Value::Int64(cache_size_pages(
                    &self.inner.config,
                ))])],
            )),
            PragmaCommand::Query(PragmaName::DatabaseList) => {
                let file_name = if is_memory_path(&self.inner.path) {
                    ":memory:".to_string()
                } else {
                    self.inner.path.display().to_string()
                };
                Ok(QueryResult::with_rows(
                    vec!["seq".to_string(), "name".to_string(), "file".to_string()],
                    vec![QueryRow::new(vec![
                        Value::Int64(0),
                        Value::Text("main".to_string()),
                        Value::Text(file_name),
                    ])],
                ))
            }
            PragmaCommand::Query(PragmaName::TableInfo) => Err(DbError::sql(
                "PRAGMA table_info(table_name) requires a table name argument",
            )),
            PragmaCommand::TableInfo(table_name) => {
                let info = self.describe_table(&table_name)?;
                let rows = info
                    .columns
                    .into_iter()
                    .enumerate()
                    .map(|(cid, column)| {
                        QueryRow::new(vec![
                            Value::Int64(i64::try_from(cid).unwrap_or(i64::MAX)),
                            Value::Text(column.name),
                            Value::Text(column.column_type),
                            Value::Int64(if column.nullable { 0 } else { 1 }),
                            column.default_sql.map_or(Value::Null, Value::Text),
                            Value::Int64(if column.primary_key { 1 } else { 0 }),
                        ])
                    })
                    .collect();
                Ok(QueryResult::with_rows(
                    vec![
                        "cid".to_string(),
                        "name".to_string(),
                        "type".to_string(),
                        "notnull".to_string(),
                        "dflt_value".to_string(),
                        "pk".to_string(),
                    ],
                    rows,
                ))
            }
            PragmaCommand::Query(PragmaName::IntegrityCheck) => {
                let mut runtime = self.runtime_for_inspection()?;
                runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
                let mut errors = Vec::new();
                for (table_name, table) in &runtime.catalog.tables {
                    let Some(data) = runtime.table_data(table_name) else {
                        errors.push(format!("table {} is missing row storage", table_name));
                        continue;
                    };
                    if let Some((row_id, row_len)) = data.rows.iter().find_map(|row| {
                        (row.values.len() != table.columns.len())
                            .then_some((row.row_id, row.values.len()))
                    }) {
                        errors.push(format!(
                            "table {} row {} has {} values but schema defines {} columns",
                            table_name,
                            row_id,
                            row_len,
                            table.columns.len()
                        ));
                    }
                }
                for index in runtime.catalog.indexes.values() {
                    if runtime.catalog.table(&index.table_name).is_none() {
                        errors.push(format!(
                            "index {} references missing table {}",
                            index.name, index.table_name
                        ));
                    }
                    if !runtime.indexes.contains_key(&index.name) {
                        errors.push(format!("runtime index {} is missing", index.name));
                    }
                }
                if errors.is_empty() {
                    Ok(QueryResult::with_rows(
                        vec!["integrity_check".to_string()],
                        vec![QueryRow::new(vec![Value::Text("ok".to_string())])],
                    ))
                } else {
                    Ok(QueryResult::with_rows(
                        vec!["integrity_check".to_string()],
                        errors
                            .into_iter()
                            .map(|error| QueryRow::new(vec![Value::Text(error)]))
                            .collect(),
                    ))
                }
            }
            PragmaCommand::Set(name, value) => self.execute_pragma_set(name, value),
        }
    }

    fn execute_pragma_set(&self, name: PragmaName, value: i64) -> Result<QueryResult> {
        match name {
            PragmaName::PageSize => {
                if value == i64::from(self.inner.config.page_size) {
                    Ok(QueryResult::with_affected_rows(0))
                } else {
                    Err(DbError::sql(
                        "PRAGMA page_size cannot be changed on an open database; reopen with DbConfig::page_size",
                    ))
                }
            }
            PragmaName::CacheSize => {
                if value == cache_size_pages(&self.inner.config) {
                    Ok(QueryResult::with_affected_rows(0))
                } else {
                    Err(DbError::sql(
                        "PRAGMA cache_size cannot be changed on an open connection; reopen with DbConfig::cache_size_mb",
                    ))
                }
            }
            PragmaName::IntegrityCheck | PragmaName::DatabaseList | PragmaName::TableInfo => {
                Err(DbError::sql(format!(
                    "PRAGMA {} does not support assignment",
                    pragma_name_sql(name)
                )))
            }
        }
    }

    fn execute_prepared_statement(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<QueryResult> {
        if prepared.read_only {
            self.execute_prepared_read_statement(prepared, params)
        } else {
            self.execute_prepared_write_statement(prepared, params)
        }
    }

    pub(crate) fn execute_prepared_batch_with_builder<F>(
        &self,
        prepared: &PreparedStatement,
        row_count: usize,
        param_count: usize,
        mut build_params: F,
    ) -> Result<u64>
    where
        F: FnMut(usize, &mut [Value]) -> Result<()>,
    {
        let mut params = vec![Value::Null; param_count];
        if row_count == 0 {
            return Ok(0);
        }

        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            let mut txn = self
                .inner
                .sql_txn
                .lock()
                .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
            match &mut *txn {
                SqlTxnSlot::Shared(state) => {
                    self.validate_prepared_schema_cookie(
                        prepared,
                        state.runtime.catalog.schema_cookie,
                        state.runtime.temp_schema_cookie,
                    )?;
                    let mut total_affected = 0_u64;
                    if prepared.read_only {
                        for row_index in 0..row_count {
                            build_params(row_index, &mut params)?;
                            let result = state.runtime.execute_read_statement(
                                prepared.statement.as_ref(),
                                &params,
                                self.inner.config.page_size,
                            )?;
                            total_affected = total_affected.saturating_add(result.affected_rows());
                        }
                        return Ok(total_affected);
                    }

                    if let Some(prepared_insert) = prepared
                        .prepared_insert
                        .as_deref()
                        .filter(|plan| state.runtime.can_reuse_prepared_simple_insert(plan))
                    {
                        if Self::prepared_insert_uses_direct_positional_params(
                            prepared_insert,
                            param_count,
                        ) {
                            for row_index in 0..row_count {
                                build_params(row_index, &mut params)?;
                                let affected = state
                                    .runtime
                                    .execute_prepared_simple_insert_positional_params_in_place(
                                        prepared_insert,
                                        &mut params,
                                        self.inner.config.page_size,
                                    )?;
                                total_affected = total_affected.saturating_add(affected);
                            }
                            return Ok(total_affected);
                        }

                        for row_index in 0..row_count {
                            build_params(row_index, &mut params)?;
                            let result = state.runtime.execute_prepared_simple_insert(
                                prepared_insert,
                                &params,
                                self.inner.config.page_size,
                            )?;
                            total_affected = total_affected.saturating_add(result.affected_rows());
                        }
                        return Ok(total_affected);
                    }

                    for row_index in 0..row_count {
                        build_params(row_index, &mut params)?;
                        let result = self.execute_prepared_in_state(prepared, &params, state)?;
                        total_affected = total_affected.saturating_add(result.affected_rows());
                    }
                    return Ok(total_affected);
                }
                SqlTxnSlot::Exclusive => return Err(self.exclusive_sql_txn_error()),
                SqlTxnSlot::None => {}
            }
        }

        let mut total_affected = 0_u64;
        for row_index in 0..row_count {
            build_params(row_index, &mut params)?;
            let result = self.execute_prepared_statement(prepared, &params)?;
            total_affected = total_affected.saturating_add(result.affected_rows());
        }
        Ok(total_affected)
    }

    fn prepared_insert_uses_direct_positional_params(
        prepared_insert: &PreparedSimpleInsert,
        param_count: usize,
    ) -> bool {
        if prepared_insert.value_sources.len() != param_count
            || prepared_insert.columns.len() != param_count
        {
            return false;
        }

        prepared_insert
            .value_sources
            .iter()
            .enumerate()
            .all(|(index, source)| {
                matches!(
                    source,
                    PreparedInsertValueSource::Parameter(position) if *position == index + 1
                )
            })
    }

    fn execute_prepared_read_statement(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<QueryResult> {
        if let Some(runtime) = self.transaction_runtime_snapshot()? {
            self.validate_prepared_schema_cookie(
                prepared,
                runtime.catalog.schema_cookie,
                runtime.temp_schema_cookie,
            )?;
            return runtime.execute_read_statement(
                prepared.statement.as_ref(),
                params,
                self.inner.config.page_size,
            );
        }

        self.execute_nontransaction_read_statement(
            prepared.statement.as_ref(),
            params,
            Some(prepared),
        )
    }

    fn execute_prepared_write_statement(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<QueryResult> {
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            let mut txn = self
                .inner
                .sql_txn
                .lock()
                .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
            match &mut *txn {
                SqlTxnSlot::Shared(state) => {
                    return self.execute_prepared_in_state(prepared, params, state);
                }
                SqlTxnSlot::Exclusive => return Err(self.exclusive_sql_txn_error()),
                SqlTxnSlot::None => {}
            }
        }

        let _writer = self
            .inner
            .sql_write_lock
            .lock()
            .map_err(|_| DbError::internal("SQL writer lock poisoned"))?;
        self.refresh_and_ensure_all_tables_loaded()?;
        let temp_only = {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            self.validate_prepared_schema_cookie(
                prepared,
                runtime.catalog.schema_cookie,
                runtime.temp_schema_cookie,
            )?;
            self.statement_is_temp_only(&runtime, prepared.statement.as_ref())
        };
        let _savepoint = StatementSavepoint::new(self.inner.wal.latest_snapshot());
        if temp_only {
            let mut working = self.engine_snapshot()?;
            let result = working.execute_statement(
                prepared.statement.as_ref(),
                params,
                self.inner.config.page_size,
            )?;
            self.install_temp_runtime(working)?;
            return Ok(result);
        }
        if let Some(prepared_insert) = prepared.prepared_insert.as_deref() {
            if let Some(result) = self.try_execute_autocommit_prepared_insert_in_place(
                prepared,
                prepared_insert,
                params,
            )? {
                return Ok(result);
            }
        }
        if let Some(prepared_update) = prepared.prepared_update.as_deref() {
            if let Some(result) = self.try_execute_autocommit_prepared_update_in_place(
                prepared,
                prepared_update,
                params,
            )? {
                return Ok(result);
            }
        }
        if let Some(prepared_delete) = prepared.prepared_delete.as_deref() {
            if let Some(result) = self.try_execute_autocommit_prepared_delete_in_place(
                prepared,
                prepared_delete,
                params,
            )? {
                return Ok(result);
            }
        }
        {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            self.validate_prepared_schema_cookie(
                prepared,
                runtime.catalog.schema_cookie,
                runtime.temp_schema_cookie,
            )?;
            if matches!(
                prepared.statement.as_ref(),
                crate::sql::ast::Statement::Insert(insert)
                    if runtime.can_execute_insert_in_place(insert)
            ) {
                drop(runtime);
                return self
                    .execute_autocommit_insert_in_place(prepared.statement.as_ref(), params);
            }
        }
        self.execute_autocommit_in_place(|runtime| {
            runtime.execute_statement(
                prepared.statement.as_ref(),
                params,
                self.inner.config.page_size,
            )
        })
    }

    fn execute_write_statement(
        &self,
        sql: &str,
        statement: &crate::sql::ast::Statement,
        params: &[Value],
    ) -> Result<QueryResult> {
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            let mut txn = self
                .inner
                .sql_txn
                .lock()
                .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
            match &mut *txn {
                SqlTxnSlot::Shared(state) => {
                    return self.execute_statement_in_state(sql, statement, params, state);
                }
                SqlTxnSlot::Exclusive => return Err(self.exclusive_sql_txn_error()),
                SqlTxnSlot::None => {}
            }
        }

        let _writer = self
            .inner
            .sql_write_lock
            .lock()
            .map_err(|_| DbError::internal("SQL writer lock poisoned"))?;
        self.refresh_and_ensure_all_tables_loaded()?;
        let temp_only = {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            self.statement_is_temp_only(&runtime, statement)
        };
        let _savepoint = StatementSavepoint::new(self.inner.wal.latest_snapshot());
        if temp_only {
            let mut working = self.engine_snapshot()?;
            let result =
                working.execute_statement(statement, params, self.inner.config.page_size)?;
            self.install_temp_runtime(working)?;
            return Ok(result);
        }
        if let crate::sql::ast::Statement::Insert(insert) = statement {
            let prepared = {
                let runtime = self
                    .inner
                    .engine
                    .read()
                    .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
                self.prepared_simple_insert(sql, insert, &runtime)?
            };
            if let Some(prepared) = prepared {
                return self.execute_autocommit_prepared_insert_in_place(prepared.as_ref(), params);
            }
        }
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
        self.execute_autocommit_in_place(|runtime| {
            runtime.execute_statement(statement, params, self.inner.config.page_size)
        })
    }

    fn execute_autocommit_insert_in_place(
        &self,
        statement: &crate::sql::ast::Statement,
        params: &[Value],
    ) -> Result<QueryResult> {
        self.execute_autocommit_in_place(|runtime| {
            runtime.execute_statement(statement, params, self.inner.config.page_size)
        })
    }

    fn execute_autocommit_prepared_insert_in_place(
        &self,
        prepared: &PreparedSimpleInsert,
        params: &[Value],
    ) -> Result<QueryResult> {
        self.execute_autocommit_in_place(|runtime| {
            runtime.execute_prepared_simple_insert(prepared, params, self.inner.config.page_size)
        })
    }

    fn try_execute_autocommit_prepared_insert_in_place(
        &self,
        prepared_statement: &PreparedStatement,
        prepared_insert: &PreparedSimpleInsert,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        self.validate_prepared_schema_cookie(
            prepared_statement,
            runtime.catalog.schema_cookie,
            runtime.temp_schema_cookie,
        )?;
        if !runtime.can_reuse_prepared_simple_insert(prepared_insert) {
            return Ok(None);
        }
        let result = match runtime.execute_prepared_simple_insert(
            prepared_insert,
            params,
            self.inner.config.page_size,
        ) {
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
        let runtime_schema_cookie = runtime.catalog.schema_cookie;
        if self.inner.catalog.schema_cookie()? != runtime_schema_cookie {
            self.inner
                .catalog
                .replace(runtime.catalog.as_ref().clone())?;
        }
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        Ok(Some(result))
    }

    fn try_execute_autocommit_prepared_update_in_place(
        &self,
        prepared_statement: &PreparedStatement,
        prepared_update: &PreparedSimpleUpdate,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        self.validate_prepared_schema_cookie(
            prepared_statement,
            runtime.catalog.schema_cookie,
            runtime.temp_schema_cookie,
        )?;
        if !runtime.can_reuse_prepared_simple_update(prepared_update) {
            return Ok(None);
        }
        let result = match runtime.execute_prepared_simple_update(prepared_update, params) {
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
        let runtime_schema_cookie = runtime.catalog.schema_cookie;
        if self.inner.catalog.schema_cookie()? != runtime_schema_cookie {
            self.inner
                .catalog
                .replace(runtime.catalog.as_ref().clone())?;
        }
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        Ok(Some(result))
    }

    fn try_execute_autocommit_prepared_delete_in_place(
        &self,
        prepared_statement: &PreparedStatement,
        prepared_delete: &PreparedSimpleDelete,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        self.validate_prepared_schema_cookie(
            prepared_statement,
            runtime.catalog.schema_cookie,
            runtime.temp_schema_cookie,
        )?;
        if !runtime.can_reuse_prepared_simple_delete(prepared_delete) {
            return Ok(None);
        }
        let result = match runtime.execute_prepared_simple_delete(prepared_delete, params) {
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
        let runtime_schema_cookie = runtime.catalog.schema_cookie;
        if self.inner.catalog.schema_cookie()? != runtime_schema_cookie {
            self.inner
                .catalog
                .replace(runtime.catalog.as_ref().clone())?;
        }
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        Ok(Some(result))
    }

    fn execute_autocommit_in_place<F>(&self, apply: F) -> Result<QueryResult>
    where
        F: FnOnce(&mut EngineRuntime) -> Result<QueryResult>,
    {
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        let result = match apply(&mut runtime) {
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
        let runtime_schema_cookie = runtime.catalog.schema_cookie;
        if self.inner.catalog.schema_cookie()? != runtime_schema_cookie {
            self.inner
                .catalog
                .replace(runtime.catalog.as_ref().clone())?;
        }
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        Ok(result)
    }

    fn backfill_missing_persistent_pk_indexes(&self) -> Result<()> {
        if !self.inner.config.persistent_pk_index {
            return Ok(());
        }

        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        let needs_backfill = runtime.catalog.tables.iter().any(|(table_name, table)| {
            table.pk_index_root.is_none()
                && runtime
                    .persisted_tables
                    .get(table_name)
                    .is_some_and(|state| state.pointer.head_page_id != 0)
        });
        if !needs_backfill {
            return Ok(());
        }

        self.begin_write()?;
        let changed = match runtime.backfill_missing_persistent_pk_indexes(self) {
            Ok(changed) => changed,
            Err(error) => {
                let _ = self.rollback();
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        if !changed {
            self.rollback()?;
            return Ok(());
        }
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
        self.inner
            .catalog
            .replace(runtime.catalog.as_ref().clone())?;
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        Ok(())
    }

    fn compact_persisted_payloads_before_checkpoint(&self) -> Result<()> {
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Ok(());
        }
        self.refresh_and_ensure_all_tables_loaded()?;
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        if !runtime.has_checkpoint_compaction_candidates() {
            return Ok(());
        }
        self.begin_write()?;
        let changed = match runtime.compact_persisted_payloads_for_checkpoint(self) {
            Ok(changed) => changed,
            Err(error) => {
                let _ = self.rollback();
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        if !changed {
            self.rollback()?;
            return Ok(());
        }
        let committed_lsn = match self.commit() {
            Ok(lsn) => lsn,
            Err(error) => {
                let _ = self.rollback();
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        Ok(())
    }

    fn engine_snapshot(&self) -> Result<EngineRuntime> {
        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        let mut snapshot = runtime.clone();
        self.apply_temp_state_to_runtime(&mut snapshot)?;
        snapshot.rebuild_stale_indexes(self.inner.config.page_size)?;
        Ok(snapshot)
    }

    #[cfg(test)]
    pub(crate) fn debug_engine_snapshot(&self) -> Result<EngineRuntime> {
        self.engine_snapshot()
    }

    fn apply_temp_state_to_runtime(&self, runtime: &mut EngineRuntime) -> Result<()> {
        self.inner
            .temp_state
            .lock()
            .map_err(|_| DbError::internal("temp schema lock poisoned"))?
            .apply_to_runtime(runtime);
        Ok(())
    }

    fn sync_temp_state_from_runtime(&self, runtime: &EngineRuntime) -> Result<()> {
        self.inner
            .temp_state
            .lock()
            .map_err(|_| DbError::internal("temp schema lock poisoned"))?
            .update_from_runtime(runtime);
        Ok(())
    }

    fn install_temp_runtime(&self, runtime: EngineRuntime) -> Result<()> {
        self.sync_temp_state_from_runtime(&runtime)?;
        let mut guard = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        *guard = runtime;
        Ok(())
    }

    fn statement_is_temp_only(
        &self,
        runtime: &EngineRuntime,
        statement: &crate::sql::ast::Statement,
    ) -> bool {
        let temp_has_table = |name: &str| {
            runtime
                .temp_tables
                .keys()
                .any(|entry| identifiers_equal(entry, name))
        };
        let temp_has_view = |name: &str| {
            runtime
                .temp_views
                .keys()
                .any(|entry| identifiers_equal(entry, name))
        };
        let temp_has_name = |name: &str| temp_has_table(name) || temp_has_view(name);
        match statement {
            crate::sql::ast::Statement::CreateTable(statement) => statement.temporary,
            crate::sql::ast::Statement::CreateTableAs(statement) => statement.temporary,
            crate::sql::ast::Statement::CreateView(statement) => statement.temporary,
            crate::sql::ast::Statement::DropTable { name, .. } => temp_has_table(name),
            crate::sql::ast::Statement::DropView { name, .. } => temp_has_view(name),
            crate::sql::ast::Statement::Insert(statement) => temp_has_name(&statement.table_name),
            crate::sql::ast::Statement::Update(statement) => temp_has_name(&statement.table_name),
            crate::sql::ast::Statement::Delete(statement) => temp_has_name(&statement.table_name),
            _ => false,
        }
    }

    fn parsed_statement(&self, sql: &str) -> Result<Arc<SqlStatement>> {
        self.inner
            .statement_cache
            .lock()
            .map_err(|_| DbError::internal("statement cache lock poisoned"))?
            .get_or_parse(sql)
    }

    fn prepare_with_runtime(
        &self,
        sql: &str,
        runtime: &EngineRuntime,
    ) -> Result<PreparedStatement> {
        let prepared_sql = prepared_statement_sql(sql)?;
        let statement = self.parsed_statement(&prepared_sql)?;
        let read_only = statement_is_read_only(statement.as_ref());
        let (prepared_insert, prepared_update, prepared_delete) = match statement.as_ref() {
            SqlStatement::Insert(insert) => (
                self.prepared_simple_insert(&prepared_sql, insert, runtime)?,
                None,
                None,
            ),
            SqlStatement::Update(update) => (
                None,
                runtime.prepare_simple_update(update)?.map(Arc::new),
                None,
            ),
            SqlStatement::Delete(delete) => (
                None,
                None,
                runtime.prepare_simple_delete(delete)?.map(Arc::new),
            ),
            _ => (None, None, None),
        };
        Ok(PreparedStatement {
            db: self.clone(),
            schema_cookie: runtime.catalog.schema_cookie,
            temp_schema_cookie: runtime.temp_schema_cookie,
            statement: Arc::clone(&statement),
            prepared_insert,
            prepared_update,
            prepared_delete,
            read_only,
            in_state_without_clone: !read_only
                && runtime.can_execute_statement_in_state_without_clone(statement.as_ref()),
        })
    }

    fn prepared_simple_insert(
        &self,
        sql: &str,
        statement: &crate::sql::ast::InsertStatement,
        runtime: &EngineRuntime,
    ) -> Result<Option<Arc<PreparedSimpleInsert>>> {
        self.inner
            .prepared_insert_cache
            .lock()
            .map_err(|_| DbError::internal("prepared insert cache lock poisoned"))?
            .get_or_prepare(
                sql,
                runtime.catalog.schema_cookie,
                runtime.temp_schema_cookie,
                || runtime.prepare_simple_insert(statement),
            )
    }

    fn persist_runtime(&self, runtime: EngineRuntime) -> Result<u64> {
        self.persist_runtime_if_latest(runtime, None, true)
    }

    fn build_exclusive_sql_txn_state(&self) -> Result<ExclusiveSqlTxnState<'_>> {
        self.refresh_and_ensure_all_tables_loaded()?;
        let current_lsn = self.inner.last_runtime_lsn.load(Ordering::Acquire);
        let current_epoch = self.inner.wal.checkpoint_epoch();
        let runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        Ok(ExclusiveSqlTxnState {
            runtime,
            base_lsn: current_lsn,
            base_checkpoint_epoch: current_epoch,
            persistent_changed: false,
            indexes_maybe_stale: false,
        })
    }

    fn commit_exclusive_sql_txn(&self, mut state: ExclusiveSqlTxnState<'_>) -> Result<u64> {
        if !state.persistent_changed {
            self.sync_temp_state_from_runtime(&state.runtime)?;
            return Ok(state.base_lsn);
        }

        let runtime_schema_cookie = state.runtime.catalog.schema_cookie;
        if state.indexes_maybe_stale {
            state
                .runtime
                .rebuild_stale_indexes(self.inner.config.page_size)?;
        }
        self.begin_write()?;
        if let Err(error) = state.runtime.persist_to_db(self) {
            let _ = self.rollback();
            self.restore_runtime_from_storage(&mut state.runtime)?;
            return Err(error);
        }
        let committed_lsn = match self.commit_if_latest(state.base_lsn, state.base_checkpoint_epoch)
        {
            Ok(lsn) => lsn,
            Err(error) => {
                let _ = self.rollback();
                self.restore_runtime_from_storage(&mut state.runtime)?;
                return Err(error);
            }
        };
        if self.inner.catalog.schema_cookie()? != runtime_schema_cookie {
            self.inner
                .catalog
                .replace(state.runtime.catalog.as_ref().clone())?;
        }
        self.sync_temp_state_from_runtime(&state.runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        Ok(committed_lsn)
    }

    fn rollback_exclusive_sql_txn(&self, mut state: ExclusiveSqlTxnState<'_>) -> Result<()> {
        self.restore_runtime_from_storage(&mut state.runtime)
    }

    fn persist_runtime_if_latest(
        &self,
        runtime: EngineRuntime,
        expected_latest: Option<(u64, u64)>,
        rebuild_stale_indexes: bool,
    ) -> Result<u64> {
        let mut runtime = runtime;
        let runtime_schema_cookie = runtime.catalog.schema_cookie;
        if rebuild_stale_indexes {
            runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
        }
        self.begin_write()?;
        if let Err(error) = runtime.persist_to_db(self) {
            let _ = self.rollback();
            return Err(error);
        }
        let committed_lsn = match expected_latest {
            Some((lsn, epoch)) => self.commit_if_latest(lsn, epoch),
            None => self.commit(),
        };
        let committed_lsn = match committed_lsn {
            Ok(lsn) => lsn,
            Err(error) => {
                let _ = self.rollback();
                return Err(error);
            }
        };
        if self.inner.catalog.schema_cookie()? != runtime_schema_cookie {
            self.inner
                .catalog
                .replace(runtime.catalog.as_ref().clone())?;
        }
        self.sync_temp_state_from_runtime(&runtime)?;
        let mut guard = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        *guard = runtime;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        drop(guard);

        Ok(committed_lsn)
    }

    fn runtime_for_inspection(&self) -> Result<EngineRuntime> {
        if let Some(runtime) = self.transaction_runtime_snapshot()? {
            return Ok(runtime);
        }
        self.refresh_and_ensure_all_tables_loaded()?;
        self.engine_snapshot()
    }

    fn runtime_for_prepare(&self) -> Result<EngineRuntime> {
        if let Some(runtime) = self.transaction_runtime_snapshot()? {
            return Ok(runtime);
        }
        self.refresh_engine_from_storage()?;
        // ADR 0143 Phase B: prepare() only needs catalog/schema metadata
        // to plan a statement. Skip the eager all-tables materialization
        // so applications that prepare a large number of statements at
        // startup don't fault every persisted table into memory just to
        // get a `PreparedStatement` handle. Row data is loaded on first
        // execution by the read/write paths.
        self.engine_snapshot()
    }

    fn transaction_runtime_snapshot(&self) -> Result<Option<EngineRuntime>> {
        if !self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Ok(None);
        }
        let txn = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        let state = match &*txn {
            SqlTxnSlot::Shared(state) => state,
            SqlTxnSlot::Exclusive => return Err(self.exclusive_sql_txn_error()),
            SqlTxnSlot::None => return Ok(None),
        };

        let mut snapshot = state.runtime.clone();
        if state.indexes_maybe_stale {
            snapshot.rebuild_stale_indexes(self.inner.config.page_size)?;
        }
        Ok(Some(snapshot))
    }

    fn restore_runtime_from_storage(&self, runtime: &mut EngineRuntime) -> Result<()> {
        let schema_cookie = self.current_schema_cookie()?;
        let (mut restored, restored_lsn) = EngineRuntime::load_from_storage(
            &self.inner.pager,
            &self.inner.wal,
            schema_cookie,
            &self.inner.config,
        )?;
        self.apply_temp_state_to_runtime(&mut restored)?;
        self.inner
            .catalog
            .replace(restored.catalog.as_ref().clone())?;
        *runtime = restored;
        self.inner
            .last_runtime_lsn
            .store(restored_lsn, Ordering::Release);
        Ok(())
    }

    fn refresh_engine_from_snapshot(&self, snapshot_lsn: u64) -> Result<()> {
        let latest_checkpoint_epoch = self.inner.wal.checkpoint_epoch();
        let last_seen_checkpoint_epoch = self
            .inner
            .last_seen_checkpoint_epoch
            .load(Ordering::Acquire);
        let last_runtime_lsn = self.inner.last_runtime_lsn.load(Ordering::Acquire);
        if latest_checkpoint_epoch != last_seen_checkpoint_epoch {
            let cached_header = self.inner.pager.header_snapshot()?;
            let on_disk_header = self.inner.pager.header_from_disk()?;
            if on_disk_header.last_checkpoint_lsn != cached_header.last_checkpoint_lsn {
                self.inner.pager.refresh_from_disk(on_disk_header)?;
            }
            self.inner
                .last_seen_checkpoint_epoch
                .store(latest_checkpoint_epoch, Ordering::Release);
        }
        if snapshot_lsn == last_runtime_lsn && latest_checkpoint_epoch == last_seen_checkpoint_epoch
        {
            return Ok(());
        }

        let writer_last_commit_lsn = self.inner.writer_last_commit_lsn.load(Ordering::Acquire);
        if last_runtime_lsn > 0 && snapshot_lsn <= writer_last_commit_lsn {
            self.inner
                .last_runtime_lsn
                .store(snapshot_lsn, Ordering::Release);
            self.inner
                .last_seen_checkpoint_epoch
                .store(latest_checkpoint_epoch, Ordering::Release);
            return Ok(());
        }

        let schema_cookie = self.current_schema_cookie_at_snapshot(snapshot_lsn)?;
        let mut runtime = EngineRuntime::load_from_storage_at_snapshot(
            &self.inner.pager,
            &self.inner.wal,
            schema_cookie,
            &self.inner.config,
            snapshot_lsn,
        )?;
        self.apply_temp_state_to_runtime(&mut runtime)?;
        self.inner
            .catalog
            .replace(runtime.catalog.as_ref().clone())?;
        let mut guard = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        *guard = runtime;
        self.inner
            .last_runtime_lsn
            .store(snapshot_lsn, Ordering::Release);
        self.inner
            .last_seen_checkpoint_epoch
            .store(latest_checkpoint_epoch, Ordering::Release);
        Ok(())
    }

    fn refresh_engine_from_storage(&self) -> Result<()> {
        let latest_lsn = self.inner.wal.latest_snapshot();
        let latest_checkpoint_epoch = self.inner.wal.checkpoint_epoch();
        let last_runtime_lsn = self.inner.last_runtime_lsn.load(Ordering::Acquire);
        let last_seen_checkpoint_epoch = self
            .inner
            .last_seen_checkpoint_epoch
            .load(Ordering::Acquire);

        if latest_lsn <= last_runtime_lsn && latest_checkpoint_epoch == last_seen_checkpoint_epoch {
            return Ok(());
        }

        if latest_checkpoint_epoch != last_seen_checkpoint_epoch {
            let cached_header = self.inner.pager.header_snapshot()?;
            let on_disk_header = self.inner.pager.header_from_disk()?;
            if on_disk_header.last_checkpoint_lsn != cached_header.last_checkpoint_lsn {
                self.inner.pager.refresh_from_disk(on_disk_header)?;
            }
            self.inner
                .last_seen_checkpoint_epoch
                .store(latest_checkpoint_epoch, Ordering::Release);
        }

        let writer_last_commit_lsn = self.inner.writer_last_commit_lsn.load(Ordering::Acquire);
        if last_runtime_lsn > 0 && latest_lsn <= writer_last_commit_lsn {
            // A checkpoint can legally fold this handle's last committed WAL
            // history back into the database file and reset the live WAL end
            // (often to 0 after truncation). The in-memory runtime is still
            // current, but future OCC writes must compare against the new live
            // WAL end rather than the pre-checkpoint commit LSN.
            self.inner
                .last_runtime_lsn
                .store(latest_lsn, Ordering::Release);
            return Ok(());
        }

        let schema_cookie = self.current_schema_cookie()?;
        let (mut runtime, runtime_lsn) = EngineRuntime::load_from_storage(
            &self.inner.pager,
            &self.inner.wal,
            schema_cookie,
            &self.inner.config,
        )?;
        self.apply_temp_state_to_runtime(&mut runtime)?;
        self.inner
            .catalog
            .replace(runtime.catalog.as_ref().clone())?;
        let mut guard = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        *guard = runtime;
        self.inner
            .last_runtime_lsn
            .store(runtime_lsn, Ordering::Release);
        Ok(())
    }

    fn refresh_and_ensure_all_tables_loaded(&self) -> Result<()> {
        if !self.inner.config.defer_table_materialization {
            self.refresh_engine_from_storage()?;
            self.ensure_all_tables_loaded()?;
            return Ok(());
        }

        let reader = self.inner.wal.begin_reader()?;
        let snapshot_lsn = reader.snapshot_lsn();
        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        self.ensure_all_tables_loaded_at_snapshot(Some(snapshot_lsn))?;
        drop(reader);
        Ok(())
    }

    /// Materializes deferred tables specified by name.
    ///
    /// Fast path (no matching deferred tables): one read-lock check.
    /// Slow path: drops the read lock, takes a write lock, loads only the
    /// specified tables and rebuilds their indexes, then releases.
    ///
    /// This enables per-table on-demand loading for ADR 0143 Phase B.
    fn ensure_tables_loaded_at_snapshot(
        &self,
        names: &[&str],
        snapshot_lsn: Option<u64>,
    ) -> Result<()> {
        use std::collections::BTreeSet;
        let filter: BTreeSet<String> = names.iter().map(|s| s.to_string()).collect();
        {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            let has_deferred = runtime.has_deferred_tables();
            let has_match = names.iter().any(|name| {
                runtime
                    .deferred_table_names()
                    .any(|dt| dt.eq_ignore_ascii_case(name))
            });
            if !has_deferred || !has_match {
                return Ok(());
            }
        }
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        if let Some(snapshot_lsn) = snapshot_lsn {
            runtime.load_deferred_tables_filtered_at_snapshot(
                &self.inner.pager,
                &self.inner.wal,
                self.inner.config.page_size,
                &filter,
                snapshot_lsn,
            )
        } else {
            runtime.load_deferred_tables_filtered(
                &self.inner.pager,
                &self.inner.wal,
                self.inner.config.page_size,
                &filter,
            )
        }
    }

    /// Attempts to materialize *only* the tables referenced by `statement`.
    ///
    /// Returns `Ok(true)` when statement analysis was conservatively
    /// exhaustive and the targeted load succeeded — the caller can then
    /// safely skip `ensure_all_tables_loaded()`. Returns `Ok(false)` when
    /// the statement contains shapes the analyzer can't fully resolve
    /// (CTEs, subqueries, set operations, INSERT…SELECT, DDL, …); the
    /// caller must fall back to loading all tables.
    ///
    /// Per ADR 0143 Phase B + the rubber-duck plan critique on
    /// 2026-04-22: only a strict whitelist is treated as targeted-safe.
    fn ensure_tables_loaded_for_statement_at_snapshot(
        &self,
        statement: &SqlStatement,
        snapshot_lsn: Option<u64>,
    ) -> Result<bool> {
        use crate::sql::ast::safe_referenced_tables;
        let Some(tables) = safe_referenced_tables(statement) else {
            return Ok(false);
        };
        if tables.is_empty() {
            // Statement is safely analyzed but references no tables
            // (e.g. `SELECT 1`); nothing to load.
            return Ok(true);
        }
        // ADR 0143 Phase B: if any reference is to something the catalog
        // does not recognize as a base table (most commonly a view, but
        // also a not-yet-attached temp table), the statement may pull in
        // additional underlying tables we cannot enumerate here. Fall
        // back to loading everything to keep correctness.
        {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            for name in &tables {
                let known = runtime
                    .catalog
                    .tables
                    .keys()
                    .any(|k| k.eq_ignore_ascii_case(name));
                if !known {
                    return Ok(false);
                }
            }
        }
        let names: Vec<String> = tables.into_iter().collect();
        let names_refs: Vec<&str> = names.iter().map(|s: &String| s.as_str()).collect();
        self.ensure_tables_loaded_at_snapshot(&names_refs, snapshot_lsn)?;
        Ok(true)
    }

    /// Materializes all tables that were deferred during `Db::open`.
    ///
    /// Fast path (no deferred tables): one read-lock check on the engine.
    /// Slow path: drops the read lock, takes a write lock, loads all deferred
    /// tables and rebuilds indexes, then releases.
    fn ensure_all_tables_loaded(&self) -> Result<()> {
        self.ensure_all_tables_loaded_at_snapshot(None)
    }

    fn ensure_all_tables_loaded_at_snapshot(&self, snapshot_lsn: Option<u64>) -> Result<()> {
        {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            if !runtime.has_deferred_tables() {
                return Ok(());
            }
        }
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        if let Some(snapshot_lsn) = snapshot_lsn {
            runtime.load_deferred_tables_at_snapshot(
                &self.inner.pager,
                &self.inner.wal,
                self.inner.config.page_size,
                snapshot_lsn,
            )
        } else {
            runtime.load_deferred_tables(
                &self.inner.pager,
                &self.inner.wal,
                self.inner.config.page_size,
            )
        }
    }

    fn current_schema_cookie(&self) -> Result<u32> {
        let page = self.read_page(page::HEADER_PAGE_ID)?;
        let mut bytes = [0_u8; storage::header::DB_HEADER_SIZE];
        bytes.copy_from_slice(&page[..storage::header::DB_HEADER_SIZE]);
        Ok(DatabaseHeader::decode(&bytes)?.schema_cookie)
    }

    fn current_schema_cookie_at_snapshot(&self, snapshot_lsn: u64) -> Result<u32> {
        let page = if let Some(wal_page) = self
            .inner
            .wal
            .read_page_at_snapshot(page::HEADER_PAGE_ID, snapshot_lsn)?
        {
            wal_page
        } else {
            self.inner.pager.read_page(page::HEADER_PAGE_ID)?
        };
        let mut bytes = [0_u8; storage::header::DB_HEADER_SIZE];
        bytes.copy_from_slice(&page[..storage::header::DB_HEADER_SIZE]);
        Ok(DatabaseHeader::decode(&bytes)?.schema_cookie)
    }

    fn validate_prepared_schema_cookie(
        &self,
        prepared: &PreparedStatement,
        schema_cookie: u32,
        temp_schema_cookie: u32,
    ) -> Result<()> {
        if schema_cookie == prepared.schema_cookie
            && temp_schema_cookie == prepared.temp_schema_cookie
        {
            return Ok(());
        }
        Err(DbError::sql(
            "prepared statement is no longer valid because the schema changed",
        ))
    }

    fn build_sql_txn_state(&self) -> Result<SqlTxnState> {
        self.refresh_and_ensure_all_tables_loaded()?;
        let current_lsn = self.inner.last_runtime_lsn.load(Ordering::Acquire);
        let current_epoch = self.inner.wal.checkpoint_epoch();

        let mut runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?
            .clone();
        self.apply_temp_state_to_runtime(&mut runtime)?;
        Ok(SqlTxnState {
            runtime,
            base_lsn: current_lsn,
            base_checkpoint_epoch: current_epoch,
            persistent_changed: false,
            indexes_maybe_stale: false,
            savepoints: Vec::new(),
        })
    }

    fn execute_prepared_in_state(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
        state: &mut SqlTxnState,
    ) -> Result<QueryResult> {
        if !Arc::ptr_eq(&self.inner, &prepared.db.inner) {
            return Err(DbError::transaction(
                "prepared statement belongs to a different database handle",
            ));
        }
        self.validate_prepared_schema_cookie(
            prepared,
            state.runtime.catalog.schema_cookie,
            state.runtime.temp_schema_cookie,
        )?;
        if prepared.read_only {
            return state.runtime.execute_read_statement(
                prepared.statement.as_ref(),
                params,
                self.inner.config.page_size,
            );
        }
        if matches!(
            prepared.statement.as_ref(),
            crate::sql::ast::Statement::Analyze { .. }
        ) {
            return Err(DbError::transaction(
                "ANALYZE is not supported inside an explicit SQL transaction",
            ));
        }

        let page_size = self.inner.config.page_size;
        // Fast paths for simple prepared DML. These fast paths already verify
        // that the target table is not temporary, so we can set
        // persistent_changed = true directly without a separate
        // statement_is_temp_only check (which would touch cold cache lines).
        if let Some(prepared_update) = prepared
            .prepared_update
            .as_deref()
            .filter(|plan| state.runtime.can_reuse_prepared_simple_update(plan))
        {
            let result = state
                .runtime
                .execute_prepared_simple_update(prepared_update, params)?;
            state.persistent_changed = true;
            return Ok(result);
        }
        if let Some(prepared_delete) = prepared
            .prepared_delete
            .as_deref()
            .filter(|plan| state.runtime.can_reuse_prepared_simple_delete(plan))
        {
            let result = state
                .runtime
                .execute_prepared_simple_delete(prepared_delete, params)?;
            state.persistent_changed = true;
            return Ok(result);
        }
        if let Some(prepared_insert) = prepared
            .prepared_insert
            .as_deref()
            .filter(|plan| state.runtime.can_reuse_prepared_simple_insert(plan))
        {
            let result =
                state
                    .runtime
                    .execute_prepared_simple_insert(prepared_insert, params, page_size)?;
            state.persistent_changed = true;
            return Ok(result);
        }
        let temp_only = self.statement_is_temp_only(&state.runtime, prepared.statement.as_ref());
        if matches!(
            prepared.statement.as_ref(),
            crate::sql::ast::Statement::Insert(insert)
                if state.runtime.can_execute_insert_in_place(insert)
        ) {
            let result =
                state
                    .runtime
                    .execute_statement(prepared.statement.as_ref(), params, page_size)?;
            state.persistent_changed |= !temp_only;
            return Ok(result);
        }
        if prepared.in_state_without_clone {
            let result =
                state
                    .runtime
                    .execute_statement(prepared.statement.as_ref(), params, page_size)?;
            state.persistent_changed |= !temp_only;
            return Ok(result);
        }
        let mut working = state.runtime.clone();
        working.rebuild_stale_indexes(page_size)?;
        let result = working.execute_statement(prepared.statement.as_ref(), params, page_size)?;
        state.runtime = working;
        state.persistent_changed |= !temp_only;
        state.indexes_maybe_stale = true;
        Ok(result)
    }

    fn execute_prepared_in_exclusive_state(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
        state: &mut ExclusiveSqlTxnState<'_>,
    ) -> Result<QueryResult> {
        if !Arc::ptr_eq(&self.inner, &prepared.db.inner) {
            return Err(DbError::transaction(
                "prepared statement belongs to a different database handle",
            ));
        }
        self.validate_prepared_schema_cookie(
            prepared,
            state.runtime.catalog.schema_cookie,
            state.runtime.temp_schema_cookie,
        )?;
        if prepared.read_only {
            return state.runtime.execute_read_statement(
                prepared.statement.as_ref(),
                params,
                self.inner.config.page_size,
            );
        }
        if matches!(
            prepared.statement.as_ref(),
            crate::sql::ast::Statement::Analyze { .. }
        ) {
            return Err(DbError::transaction(
                "ANALYZE is not supported inside an explicit SQL transaction",
            ));
        }

        let page_size = self.inner.config.page_size;
        if let Some(prepared_update) = prepared
            .prepared_update
            .as_deref()
            .filter(|plan| state.runtime.can_reuse_prepared_simple_update(plan))
        {
            let result = state
                .runtime
                .execute_prepared_simple_update(prepared_update, params)?;
            state.persistent_changed = true;
            return Ok(result);
        }
        if let Some(prepared_delete) = prepared
            .prepared_delete
            .as_deref()
            .filter(|plan| state.runtime.can_reuse_prepared_simple_delete(plan))
        {
            let result = state
                .runtime
                .execute_prepared_simple_delete(prepared_delete, params)?;
            state.persistent_changed = true;
            return Ok(result);
        }
        if let Some(prepared_insert) = prepared
            .prepared_insert
            .as_deref()
            .filter(|plan| state.runtime.can_reuse_prepared_simple_insert(plan))
        {
            let result =
                state
                    .runtime
                    .execute_prepared_simple_insert(prepared_insert, params, page_size)?;
            state.persistent_changed = true;
            return Ok(result);
        }
        let temp_only = self.statement_is_temp_only(&state.runtime, prepared.statement.as_ref());
        if matches!(
            prepared.statement.as_ref(),
            crate::sql::ast::Statement::Insert(insert)
                if state.runtime.can_execute_insert_in_place(insert)
        ) {
            let result =
                state
                    .runtime
                    .execute_statement(prepared.statement.as_ref(), params, page_size)?;
            state.persistent_changed |= !temp_only;
            return Ok(result);
        }
        if prepared.in_state_without_clone {
            let result =
                state
                    .runtime
                    .execute_statement(prepared.statement.as_ref(), params, page_size)?;
            state.persistent_changed |= !temp_only;
            return Ok(result);
        }

        let mut working = state.runtime.clone();
        working.rebuild_stale_indexes(page_size)?;
        let result = working.execute_statement(prepared.statement.as_ref(), params, page_size)?;
        *state.runtime = working;
        state.persistent_changed |= !temp_only;
        state.indexes_maybe_stale = true;
        Ok(result)
    }

    fn execute_statement_in_state(
        &self,
        sql: &str,
        statement: &crate::sql::ast::Statement,
        params: &[Value],
        state: &mut SqlTxnState,
    ) -> Result<QueryResult> {
        if matches!(statement, crate::sql::ast::Statement::Analyze { .. }) {
            return Err(DbError::transaction(
                "ANALYZE is not supported inside an explicit SQL transaction",
            ));
        }
        let temp_only = self.statement_is_temp_only(&state.runtime, statement);
        if let crate::sql::ast::Statement::Insert(insert) = statement {
            if let Some(prepared) = self.prepared_simple_insert(sql, insert, &state.runtime)? {
                let result = state.runtime.execute_prepared_simple_insert(
                    prepared.as_ref(),
                    params,
                    self.inner.config.page_size,
                )?;
                state.persistent_changed |= !temp_only;
                return Ok(result);
            }
        }
        if matches!(
            statement,
            crate::sql::ast::Statement::Insert(insert)
                if state.runtime.can_execute_insert_in_place(insert)
        ) {
            let result =
                state
                    .runtime
                    .execute_statement(statement, params, self.inner.config.page_size)?;
            state.persistent_changed |= !temp_only;
            return Ok(result);
        }
        if state
            .runtime
            .can_execute_statement_in_state_without_clone(statement)
        {
            let result =
                state
                    .runtime
                    .execute_statement(statement, params, self.inner.config.page_size)?;
            state.persistent_changed |= !temp_only;
            return Ok(result);
        }
        let mut working = state.runtime.clone();
        working.rebuild_stale_indexes(self.inner.config.page_size)?;
        let result = working.execute_statement(statement, params, self.inner.config.page_size)?;
        state.runtime = working;
        state.persistent_changed |= !temp_only;
        state.indexes_maybe_stale = true;
        Ok(result)
    }

    fn exclusive_sql_txn_error(&self) -> DbError {
        DbError::transaction(
            "a SQL transaction handle is active on this database handle; use it until commit or rollback",
        )
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum PragmaCommand {
    Query(PragmaName),
    TableInfo(String),
    Set(PragmaName, i64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PragmaName {
    PageSize,
    CacheSize,
    IntegrityCheck,
    DatabaseList,
    TableInfo,
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

fn parse_pragma_command(sql: &str) -> Result<Option<PragmaCommand>> {
    let trimmed = sql.trim();
    let Some(_) = trimmed
        .get(..6)
        .filter(|prefix| prefix.eq_ignore_ascii_case("PRAGMA"))
    else {
        return Ok(None);
    };
    let body = trimmed[6..].trim();
    if body.is_empty() {
        return Err(DbError::sql("PRAGMA requires a name"));
    }
    let body = body.trim_end_matches(';').trim();
    if body.is_empty() {
        return Err(DbError::sql("PRAGMA requires a name"));
    }

    if let Some(open_paren) = body.find('(') {
        let close_paren = body
            .rfind(')')
            .ok_or_else(|| DbError::sql("PRAGMA call is missing closing ')'"))?;
        if close_paren <= open_paren {
            return Err(DbError::sql("PRAGMA call has invalid parentheses"));
        }
        let name = body[..open_paren].trim();
        let argument = body[open_paren + 1..close_paren].trim();
        let trailing = body[close_paren + 1..].trim();
        if !trailing.is_empty() {
            return Err(DbError::sql("PRAGMA call has unexpected trailing content"));
        }
        let name = parse_pragma_name(name)?;
        if argument.is_empty() {
            return Err(DbError::sql("PRAGMA call requires an argument"));
        }
        return match name {
            PragmaName::TableInfo => {
                let table_name = parse_pragma_table_argument(argument)?;
                Ok(Some(PragmaCommand::TableInfo(table_name)))
            }
            _ => Err(DbError::sql(format!(
                "PRAGMA {} does not accept call syntax",
                pragma_name_sql(name)
            ))),
        };
    }

    let (name, value) = if let Some(eq_index) = body.find('=') {
        let name = body[..eq_index].trim();
        let value = body[eq_index + 1..].trim();
        if name.is_empty() || value.is_empty() {
            return Err(DbError::sql("PRAGMA assignment requires a name and value"));
        }
        (name, Some(value))
    } else {
        (body, None)
    };
    let pragma_name = parse_pragma_name(name)?;
    let command = if let Some(value) = value {
        let value = value.parse::<i64>().map_err(|_| {
            DbError::sql(format!(
                "PRAGMA {} expects an integer value",
                name.to_ascii_lowercase()
            ))
        })?;
        PragmaCommand::Set(pragma_name, value)
    } else {
        PragmaCommand::Query(pragma_name)
    };
    Ok(Some(command))
}

fn parse_pragma_name(name: &str) -> Result<PragmaName> {
    let normalized = name.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "page_size" => Ok(PragmaName::PageSize),
        "cache_size" => Ok(PragmaName::CacheSize),
        "integrity_check" => Ok(PragmaName::IntegrityCheck),
        "database_list" => Ok(PragmaName::DatabaseList),
        "table_info" => Ok(PragmaName::TableInfo),
        _ => Err(DbError::sql(format!("unsupported PRAGMA {}", normalized))),
    }
}

fn pragma_name_sql(name: PragmaName) -> &'static str {
    match name {
        PragmaName::PageSize => "page_size",
        PragmaName::CacheSize => "cache_size",
        PragmaName::IntegrityCheck => "integrity_check",
        PragmaName::DatabaseList => "database_list",
        PragmaName::TableInfo => "table_info",
    }
}

fn parse_pragma_table_argument(argument: &str) -> Result<String> {
    let trimmed = argument.trim();
    if trimmed.is_empty() {
        return Err(DbError::sql("PRAGMA table_info requires a table name"));
    }
    if trimmed.starts_with('\"') {
        if !trimmed.ends_with('\"') || trimmed.len() < 2 {
            return Err(DbError::sql(
                "PRAGMA table_info has invalid quoted table name",
            ));
        }
        let inner = &trimmed[1..trimmed.len() - 1];
        return Ok(inner.replace("\"\"", "\""));
    }
    if trimmed.starts_with('\'') {
        if !trimmed.ends_with('\'') || trimmed.len() < 2 {
            return Err(DbError::sql(
                "PRAGMA table_info has invalid quoted table name",
            ));
        }
        let inner = &trimmed[1..trimmed.len() - 1];
        return Ok(inner.replace("''", "'"));
    }
    Ok(trimmed.to_string())
}

fn cache_size_pages(config: &DbConfig) -> i64 {
    let bytes = config.cache_size_mb.saturating_mul(1024 * 1024);
    let pages = (bytes / config.page_size as usize).max(1);
    i64::try_from(pages).unwrap_or(i64::MAX)
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
    let mut statement_tokens = Vec::new();
    let mut trigger_body_depth = 0usize;

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
            _ if ch.is_ascii_alphanumeric() || ch == '_' => {
                current.push(ch);
                let mut token = ch.to_ascii_uppercase().to_string();
                while let Some(next) = chars.peek().copied() {
                    if !(next.is_ascii_alphanumeric() || next == '_') {
                        break;
                    }
                    let next = chars.next().expect("peeked token char");
                    current.push(next);
                    token.push(next.to_ascii_uppercase());
                }
                if statement_tokens.len() < 2 {
                    statement_tokens.push(token.clone());
                }
                if statement_tokens.as_slice() == ["CREATE", "TRIGGER"] {
                    if token == "BEGIN" {
                        trigger_body_depth += 1;
                    } else if token == "END" && trigger_body_depth > 0 {
                        trigger_body_depth -= 1;
                    }
                }
            }
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
                if trigger_body_depth > 0 {
                    current.push(ch);
                } else if !current.trim().is_empty() {
                    statements.push(rewrite_legacy_trigger_body(current.trim()).into_owned());
                    current.clear();
                    statement_tokens.clear();
                    trigger_body_depth = 0;
                }
            }
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        statements.push(rewrite_legacy_trigger_body(current.trim()).into_owned());
    }
    statements
}

fn prepared_statement_sql(sql: &str) -> Result<String> {
    let statements = split_sql_batch(sql)
        .into_iter()
        .map(|statement| statement.trim().to_string())
        .filter(|statement| !statement.is_empty())
        .collect::<Vec<_>>();
    if statements.len() != 1 {
        return Err(DbError::sql(format!(
            "expected exactly one SQL statement, got {}",
            statements.len()
        )));
    }
    let statement = statements
        .into_iter()
        .next()
        .ok_or_else(|| DbError::sql("expected exactly one SQL statement, got 0"))?;
    if parse_transaction_control(&statement).is_some() {
        return Err(DbError::sql(
            "prepared statements do not support transaction control",
        ));
    }
    Ok(statement)
}

fn table_info(table: &TableSchema, row_count: usize) -> TableInfo {
    TableInfo {
        name: table.name.clone(),
        temporary: table.temporary,
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
        include_columns: index.include_columns.clone(),
        predicate_sql: index.predicate_sql.clone(),
        fresh: index.fresh,
    }
}

fn view_info(view: &ViewSchema) -> ViewInfo {
    ViewInfo {
        name: view.name.clone(),
        temporary: view.temporary,
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

fn schema_snapshot(runtime: &EngineRuntime) -> SchemaSnapshot {
    let mut tables = runtime
        .catalog
        .tables
        .values()
        .map(|table| {
            schema_table_info(
                table,
                runtime
                    .table_data(&table.name)
                    .map_or(0, |data| data.rows.len()),
            )
        })
        .collect::<Vec<_>>();
    tables.extend(runtime.temp_tables.values().map(|table| {
        schema_table_info(
            table,
            runtime
                .temp_table_data
                .get(&table.name)
                .map_or(0, |data| data.rows.len()),
        )
    }));
    tables.sort_by(|left, right| left.name.cmp(&right.name));

    let mut views = runtime
        .catalog
        .views
        .values()
        .map(schema_view_info)
        .collect::<Vec<_>>();
    views.extend(runtime.temp_views.values().map(schema_view_info));
    views.sort_by(|left, right| left.name.cmp(&right.name));

    let mut indexes = runtime
        .catalog
        .indexes
        .values()
        .map(schema_index_info)
        .collect::<Vec<_>>();
    indexes.sort_by(|left, right| left.name.cmp(&right.name));

    let mut triggers = runtime
        .catalog
        .triggers
        .values()
        .map(schema_trigger_info)
        .collect::<Vec<_>>();
    triggers.sort_by(|left, right| left.name.cmp(&right.name));

    SchemaSnapshot {
        snapshot_version: 1,
        schema_cookie: runtime.catalog.schema_cookie,
        tables,
        views,
        indexes,
        triggers,
    }
}

fn schema_table_info(table: &TableSchema, row_count: usize) -> SchemaTableInfo {
    SchemaTableInfo {
        name: table.name.clone(),
        temporary: table.temporary,
        ddl: render_create_table(table),
        row_count,
        primary_key_columns: table.primary_key_columns.clone(),
        checks: table.checks.iter().map(check_constraint_info).collect(),
        foreign_keys: table.foreign_keys.iter().map(foreign_key_info).collect(),
        columns: table.columns.iter().map(schema_column_info).collect(),
    }
}

fn schema_column_info(column: &ColumnSchema) -> SchemaColumnInfo {
    SchemaColumnInfo {
        name: column.name.clone(),
        column_type: column.column_type.as_str().to_string(),
        nullable: column.nullable,
        default_sql: column.default_sql.clone(),
        primary_key: column.primary_key,
        unique: column.unique,
        auto_increment: column.auto_increment,
        generated_sql: column.generated_sql.clone(),
        generated_stored: column.generated_stored,
        checks: column.checks.iter().map(check_constraint_info).collect(),
        foreign_key: column.foreign_key.as_ref().map(foreign_key_info),
    }
}

fn check_constraint_info(check: &CheckConstraint) -> CheckConstraintInfo {
    CheckConstraintInfo {
        name: check.name.clone(),
        expression_sql: check.expression_sql.clone(),
    }
}

fn schema_view_info(view: &ViewSchema) -> SchemaViewInfo {
    SchemaViewInfo {
        name: view.name.clone(),
        temporary: view.temporary,
        sql_text: view.sql_text.clone(),
        column_names: view.column_names.clone(),
        dependencies: view.dependencies.clone(),
        ddl: render_create_view(view),
    }
}

fn schema_index_info(index: &IndexSchema) -> SchemaIndexInfo {
    SchemaIndexInfo {
        name: index.name.clone(),
        table_name: index.table_name.clone(),
        kind: match index.kind {
            IndexKind::Btree => "btree",
            IndexKind::Trigram => "trigram",
        }
        .to_string(),
        unique: index.unique,
        columns: index.columns.iter().map(index_column_name).collect(),
        include_columns: index.include_columns.clone(),
        predicate_sql: index.predicate_sql.clone(),
        fresh: index.fresh,
        temporary: false,
        ddl: render_create_index(index),
    }
}

fn schema_trigger_info(trigger: &TriggerSchema) -> SchemaTriggerInfo {
    let event = trigger_event_name(trigger.event).to_ascii_lowercase();
    SchemaTriggerInfo {
        name: trigger.name.clone(),
        target_name: trigger.target_name.clone(),
        target_kind: if trigger.on_view {
            "view".to_string()
        } else {
            "table".to_string()
        },
        timing: match trigger.kind {
            TriggerKind::After => "after".to_string(),
            TriggerKind::InsteadOf => "instead_of".to_string(),
        },
        events: vec![event],
        events_mask: trigger_event_mask(trigger.event),
        for_each_row: true,
        temporary: false,
        action_sql: trigger.action_sql.clone(),
        ddl: render_create_trigger(trigger),
    }
}

fn trigger_event_mask(event: TriggerEvent) -> u32 {
    match event {
        TriggerEvent::Insert => 1,
        TriggerEvent::Update => 2,
        TriggerEvent::Delete => 4,
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
        RuntimeIndex::Btree { keys } => keys.total_row_id_count(),
        RuntimeIndex::Trigram { index } => index.entry_count(),
    }
}

fn render_runtime_dump(runtime: &EngineRuntime) -> String {
    let mut lines = Vec::new();

    for table in runtime.catalog.tables.values() {
        lines.push(render_create_table(table));
    }
    for (table_name, table_data) in runtime.loaded_tables() {
        if let Some(table) = runtime.catalog.tables.get(table_name) {
            for row in &table_data.rows {
                lines.push(render_insert(table, &row.values));
            }
        }
    }
    for view in runtime.catalog.views.values() {
        lines.push(render_create_view(view));
    }
    for table in runtime.temp_tables.values() {
        lines.push(render_create_table(table));
    }
    for (table_name, table_data) in runtime.temp_table_data.iter() {
        if let Some(table) = runtime.temp_tables.get(table_name) {
            for row in &table_data.rows {
                lines.push(render_insert(table, &row.values));
            }
        }
    }
    for view in runtime.temp_views.values() {
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
        if let Some(generated_sql) = &column.generated_sql {
            definition.push_str(" GENERATED ALWAYS AS (");
            definition.push_str(generated_sql);
            if column.generated_stored {
                definition.push_str(") STORED");
            } else {
                definition.push_str(") VIRTUAL");
            }
        } else if let Some(default_sql) = &column.default_sql {
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
        "CREATE {}TABLE {} ({});",
        if table.temporary { "TEMP " } else { "" },
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
        "CREATE {}VIEW {}{columns} AS {};",
        if view.temporary { "TEMP " } else { "" },
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
    let include = if index.include_columns.is_empty() {
        String::new()
    } else {
        format!(
            " INCLUDE ({})",
            index
                .include_columns
                .iter()
                .map(|column| sql_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let predicate = index
        .predicate_sql
        .as_ref()
        .map(|predicate| format!(" WHERE {predicate}"))
        .unwrap_or_default();
    format!(
        "CREATE {unique}INDEX {} ON {}{using} ({columns}){include}{predicate};",
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
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    use crate::catalog::{
        ColumnSchema, ColumnType, IndexKind, IndexSchema, TableSchema, ViewSchema,
    };
    use crate::config::DbConfig;
    use crate::exec::{EngineRuntime, TableData};
    use std::sync::Arc;

    use crate::exec::dml::{PreparedInsertColumn, PreparedInsertValueSource, PreparedSimpleInsert};
    use crate::{Db, Value};

    use super::{split_sql_batch, PreparedInsertCache, StatementCache, TempSchemaState};

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

    #[test]
    fn split_sql_batch_preserves_legacy_trigger_body_statement() {
        let statements = split_sql_batch(
            "CREATE TRIGGER log_insert AFTER INSERT ON users
             FOR EACH ROW BEGIN
               SELECT decentdb_exec_sql('INSERT INTO audit_log (msg) VALUES (''user added'')');
             END;
             INSERT INTO users VALUES (1, 'Ada');",
        );
        assert_eq!(statements.len(), 2);
        assert_eq!(
            statements[0],
            "CREATE TRIGGER log_insert AFTER INSERT ON users
             FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit_log (msg) VALUES (''user added'')')"
        );
        assert_eq!(statements[1], "INSERT INTO users VALUES (1, 'Ada')");
    }

    #[test]
    fn temp_schema_apply_is_shallow_when_unmutated() {
        let table = TableSchema {
            name: "temp_docs".to_string(),
            temporary: true,
            columns: vec![ColumnSchema {
                name: "id".to_string(),
                column_type: ColumnType::Int64,
                nullable: false,
                default_sql: None,
                generated_sql: None,
                generated_stored: false,
                primary_key: true,
                unique: true,
                auto_increment: false,
                checks: Vec::new(),
                foreign_key: None,
            }],
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            primary_key_columns: vec!["id".to_string()],
            next_row_id: 1,
            pk_index_root: None,
        };
        let view = ViewSchema {
            name: "temp_view".to_string(),
            temporary: true,
            sql_text: "SELECT id FROM temp_docs".to_string(),
            column_names: vec!["id".to_string()],
            dependencies: vec!["temp_docs".to_string()],
        };
        let index = IndexSchema {
            name: "temp_docs_pk".to_string(),
            table_name: "temp_docs".to_string(),
            kind: IndexKind::Btree,
            unique: true,
            columns: Vec::new(),
            include_columns: Vec::new(),
            predicate_sql: None,
            fresh: false,
        };
        let state = TempSchemaState {
            schema_cookie: 7,
            tables: Arc::new(BTreeMap::from([(table.name.clone(), table)])),
            table_data: Arc::new(BTreeMap::from([(
                "temp_docs".to_string(),
                Arc::new(TableData::default()),
            )])),
            views: Arc::new(BTreeMap::from([(view.name.clone(), view)])),
            indexes: Arc::new(BTreeMap::from([(index.name.clone(), index)])),
        };
        let mut runtime = EngineRuntime::empty(1);

        state.apply_to_runtime(&mut runtime);

        assert_eq!(runtime.temp_schema_cookie, 7);
        assert!(Arc::ptr_eq(&state.tables, &runtime.temp_tables));
        assert!(Arc::ptr_eq(&state.table_data, &runtime.temp_table_data));
        assert!(Arc::ptr_eq(&state.views, &runtime.temp_views));
        assert!(Arc::ptr_eq(&state.indexes, &runtime.temp_indexes));
    }

    #[test]
    fn pragma_page_size_query() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        let result = db.execute("PRAGMA page_size").expect("pragma page_size");
        assert_eq!(result.columns(), &["page_size".to_string()]);
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(i64::from(db.config().page_size))]
        );
    }

    #[test]
    fn pragma_cache_size_query() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        let result = db.execute("PRAGMA cache_size").expect("pragma cache_size");
        assert_eq!(result.columns(), &["cache_size".to_string()]);
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(super::cache_size_pages(db.config()))]
        );
    }

    #[test]
    fn pragma_integrity_check_query() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, val TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'a')")
            .expect("insert row");
        let result = db
            .execute("PRAGMA integrity_check")
            .expect("pragma integrity_check");
        assert_eq!(result.columns(), &["integrity_check".to_string()]);
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values(), &[Value::Text("ok".to_string())]);
    }

    #[test]
    fn pragma_database_list_query() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        let result = db
            .execute("PRAGMA database_list")
            .expect("pragma database_list");
        assert_eq!(
            result.columns(),
            &["seq".to_string(), "name".to_string(), "file".to_string()]
        );
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(0),
                Value::Text("main".to_string()),
                Value::Text(":memory:".to_string())
            ]
        );
    }

    #[test]
    fn pragma_table_info_query() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, name TEXT DEFAULT 'anon')")
            .expect("create table");

        let result = db.execute("PRAGMA table_info(t)").expect("table_info");
        assert_eq!(
            result.columns(),
            &[
                "cid".to_string(),
                "name".to_string(),
                "type".to_string(),
                "notnull".to_string(),
                "dflt_value".to_string(),
                "pk".to_string()
            ]
        );
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(0),
                Value::Text("id".to_string()),
                Value::Text("INT64".to_string()),
                Value::Int64(1),
                Value::Null,
                Value::Int64(1)
            ]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[
                Value::Int64(1),
                Value::Text("name".to_string()),
                Value::Text("TEXT".to_string()),
                Value::Int64(0),
                Value::Text("'anon'".to_string()),
                Value::Int64(0)
            ]
        );
    }

    #[test]
    fn pragma_table_info_assignment_is_rejected() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        let error = db
            .execute("PRAGMA table_info = 1")
            .expect_err("assignment should fail");
        assert!(
            error.to_string().contains("does not support assignment"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn pragma_assignments_are_limited() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        let page_size_noop = db.execute("PRAGMA page_size = 4096");
        assert!(
            page_size_noop.is_ok(),
            "expected no-op assignment to succeed"
        );

        let cache_size_error = db
            .execute("PRAGMA cache_size = 8")
            .expect_err("cache_size assignment should fail");
        assert!(cache_size_error
            .to_string()
            .contains("cannot be changed on an open connection"));

        let integrity_assignment_error = db
            .execute("PRAGMA integrity_check = 1")
            .expect_err("integrity_check assignment should fail");
        assert!(integrity_assignment_error
            .to_string()
            .contains("does not support assignment"));
    }

    #[test]
    fn unsupported_pragma_reports_sql_error() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        let error = db
            .execute("PRAGMA foreign_keys")
            .expect_err("unsupported pragma should fail");
        assert!(error.to_string().contains("unsupported PRAGMA"));
    }

    #[test]
    fn prepared_insert_cache_is_scoped_by_schema_cookie() {
        let mut cache = PreparedInsertCache::with_capacity(4);
        let first = cache
            .get_or_prepare("INSERT INTO users (id) VALUES ($1)", 1, 0, || {
                Ok(Some(dummy_prepared_insert("users_v1")))
            })
            .expect("prepare first")
            .expect("prepared plan");
        let cached = cache
            .get_or_prepare("INSERT INTO users (id) VALUES ($1)", 1, 0, || {
                Ok(Some(dummy_prepared_insert("users_v1_new")))
            })
            .expect("prepare cached")
            .expect("cached plan");
        assert!(Arc::ptr_eq(&first, &cached));

        let second_schema = cache
            .get_or_prepare("INSERT INTO users (id) VALUES ($1)", 2, 0, || {
                Ok(Some(dummy_prepared_insert("users_v2")))
            })
            .expect("prepare second schema")
            .expect("prepared plan");
        assert!(!Arc::ptr_eq(&first, &second_schema));
        assert_eq!(second_schema.table_name, "users_v2");
    }

    #[test]
    fn exclusive_transaction_commit_persists_prepared_writes() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("create table");

        let mut txn = db.transaction().expect("begin exclusive txn");
        let insert = txn
            .prepare("INSERT INTO t VALUES ($1, $2)")
            .expect("prepare insert");
        for i in 0_i64..32_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[Value::Int64(i), Value::Text(format!("value-{i}"))],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit txn");

        assert_eq!(
            scalar_i64(&db.execute("SELECT COUNT(*) FROM t").expect("count rows")),
            32
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT id FROM t WHERE val = 'value-17'")
                    .expect("lookup committed row")
            ),
            17
        );
    }

    #[test]
    fn exclusive_transaction_rollback_discards_persistent_changes() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'seed')")
            .expect("seed row");

        let mut txn = db.transaction().expect("begin exclusive txn");
        let insert = txn
            .prepare("INSERT INTO t VALUES ($1, $2)")
            .expect("prepare insert");
        insert
            .execute_in(
                &mut txn,
                &[Value::Int64(2), Value::Text("transient".to_string())],
            )
            .expect("insert transient row");
        txn.rollback().expect("rollback txn");

        assert_eq!(
            scalar_i64(&db.execute("SELECT COUNT(*) FROM t").expect("count rows")),
            1
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM t WHERE id = 2")
                    .expect("count rolled back row")
            ),
            0
        );
    }

    #[test]
    fn checkpoint_compacts_large_persisted_payloads() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("checkpoint-compacts-large-payloads.ddb");
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create docs table");

        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin exclusive txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[Value::Int64(i), Value::Text(large_body.clone())],
                )
                .expect("insert large row");
        }
        txn.commit().expect("commit rows");

        let runtime_before = db
            .runtime_for_inspection()
            .expect("runtime before checkpoint");
        let docs_before = runtime_before
            .persisted_tables
            .get("docs")
            .expect("persisted docs table before checkpoint");
        assert!(
            docs_before.pointer.logical_len >= 64 * 1024,
            "test setup did not create a large enough payload"
        );
        assert!(
            !docs_before.pointer.is_compressed(),
            "normal commits should leave table payloads uncompressed"
        );

        db.checkpoint().expect("checkpoint");

        let runtime_after = db
            .runtime_for_inspection()
            .expect("runtime after checkpoint");
        let docs_after = runtime_after
            .persisted_tables
            .get("docs")
            .expect("persisted docs table after checkpoint");
        assert!(
            docs_after.pointer.is_compressed(),
            "checkpoint should compact large persisted payloads"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM docs")
                    .expect("count docs rows")
            ),
            96
        );
    }

    #[test]
    fn checkpoint_preserves_large_payloads_with_persistent_pk_index() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("checkpoint-keeps-pk-payloads.ddb");
        let config = DbConfig {
            persistent_pk_index: true,
            ..DbConfig::default()
        };
        let db = Db::open_or_create(&path, config).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create docs table");

        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin exclusive txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[Value::Int64(i), Value::Text(large_body.clone())],
                )
                .expect("insert large row");
        }
        txn.commit().expect("commit rows");

        let runtime_before = db
            .runtime_for_inspection()
            .expect("runtime before checkpoint");
        let docs_before = runtime_before
            .persisted_tables
            .get("docs")
            .expect("persisted docs before checkpoint");
        assert!(
            docs_before.pointer.logical_len >= 64 * 1024,
            "test setup did not create a large enough payload"
        );
        assert!(
            !docs_before.pointer.is_compressed(),
            "persistent pk writes should keep payload uncompressed"
        );
        assert!(
            docs_before.pk_index_root.is_some(),
            "persistent pk writes should record a locator tree root"
        );

        db.checkpoint().expect("checkpoint");

        let runtime_after = db
            .runtime_for_inspection()
            .expect("runtime after checkpoint");
        let docs_after = runtime_after
            .persisted_tables
            .get("docs")
            .expect("persisted docs after checkpoint");
        assert!(
            !docs_after.pointer.is_compressed(),
            "checkpoint should not compact payloads with persistent pk roots"
        );
        assert!(
            docs_after.pk_index_root.is_some(),
            "checkpoint should preserve the persistent pk locator tree"
        );
    }

    #[test]
    fn persistent_pk_index_backfills_compressed_tables_and_keeps_point_lookup_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("persistent-pk-backfill.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");

            let runtime = db
                .runtime_for_inspection()
                .expect("runtime after checkpoint");
            let seeded = runtime
                .persisted_tables
                .get("seeded")
                .expect("persisted seeded after checkpoint");
            assert!(
                seeded.pointer.is_compressed(),
                "legacy checkpoint should compact the large payload before backfill"
            );
            assert!(
                seeded.pk_index_root.is_none(),
                "legacy checkpoint should not have a persistent pk root"
            );
        }

        let config = DbConfig {
            persistent_pk_index: true,
            ..DbConfig::default()
        };
        let db = Db::open_or_create(&path, config).expect("reopen with persistent pk index");
        {
            let runtime = db.inner.engine.read().expect("engine runtime lock");
            let seeded = runtime
                .persisted_tables
                .get("seeded")
                .expect("persisted seeded after backfill");
            assert!(
                !seeded.pointer.is_compressed(),
                "backfill should rewrite the large payload uncompressed"
            );
            assert!(
                seeded.pk_index_root.is_some(),
                "backfill should attach a persistent pk locator tree"
            );
            assert!(
                runtime
                    .catalog
                    .tables
                    .get("seeded")
                    .and_then(|table| table.pk_index_root)
                    .is_some(),
                "catalog state should retain the persistent pk locator root"
            );
        }

        let json_open = db.inspect_storage_state_json().expect("json snapshot");
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected backfilled table to stay deferred at open, got: {json_open}"
        );

        let result = db
            .execute("SELECT n FROM seeded WHERE id = 17")
            .expect("point lookup");
        assert_eq!(scalar_i64(&result), 17);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after point lookup");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected persistent pk point lookup to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected seeded to remain deferred after point lookup, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after persistent pk point lookup, got: {json_after}"
        );
    }

    #[test]
    fn reader_handle_refreshes_after_external_checkpoint() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("checkpoint-refresh.ddb");
        let config = DbConfig::default();

        let setup = Db::open_or_create(&path, config.clone()).expect("open setup");
        setup
            .execute("CREATE TABLE t (id INTEGER)")
            .expect("create table");
        setup.begin_transaction().expect("begin seed txn");
        for i in 0_i64..100_i64 {
            setup
                .execute_with_params("INSERT INTO t VALUES ($1)", &[Value::Int64(i)])
                .expect("seed insert");
        }
        setup.commit_transaction().expect("commit seed");
        drop(setup);

        let reader = Db::open_or_create(&path, config.clone()).expect("open reader");
        assert_eq!(
            scalar_i64(
                &reader
                    .execute("SELECT COUNT(*) FROM t")
                    .expect("reader count before")
            ),
            100
        );

        let writer = Db::open_or_create(&path, config).expect("open writer");
        writer.begin_transaction().expect("begin writer txn");
        for i in 100_i64..200_i64 {
            writer
                .execute_with_params("INSERT INTO t VALUES ($1)", &[Value::Int64(i)])
                .expect("writer insert");
        }
        writer.commit_transaction().expect("commit writer");
        writer.checkpoint().expect("checkpoint writer");

        assert_eq!(
            scalar_i64(
                &reader
                    .execute("SELECT COUNT(*) FROM t")
                    .expect("reader count after")
            ),
            200
        );
    }

    #[test]
    fn same_handle_explicit_txn_can_commit_after_checkpoint_truncates_wal() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("checkpoint-explicit-txn-rebase.ddb");
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'before-checkpoint')")
            .expect("insert before checkpoint");
        db.checkpoint().expect("checkpoint");
        assert_eq!(
            db.inner.wal.latest_snapshot(),
            0,
            "checkpoint should truncate WAL"
        );

        db.begin_transaction().expect("begin transaction");
        db.execute("INSERT INTO t VALUES (2, 'after-checkpoint')")
            .expect("insert after checkpoint");
        db.commit_transaction()
            .expect("commit transaction after checkpoint");

        assert_eq!(
            scalar_i64(&db.execute("SELECT COUNT(*) FROM t").expect("count rows")),
            2
        );
    }

    #[test]
    fn same_handle_prepared_explicit_txn_can_commit_after_checkpoint_truncates_wal() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("checkpoint-prepared-explicit-txn-rebase.ddb");
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'before-checkpoint')")
            .expect("insert before checkpoint");
        db.checkpoint().expect("checkpoint");
        assert_eq!(
            db.inner.wal.latest_snapshot(),
            0,
            "checkpoint should truncate WAL"
        );

        db.begin_transaction().expect("begin transaction");
        let prepared = db
            .prepare("INSERT INTO t VALUES ($1, $2)")
            .expect("prepare insert");
        prepared
            .execute(&[Value::Int64(2), Value::Text("after-checkpoint".to_string())])
            .expect("execute prepared insert after checkpoint");
        db.commit_transaction()
            .expect("commit prepared transaction after checkpoint");

        assert_eq!(
            scalar_i64(&db.execute("SELECT COUNT(*) FROM t").expect("count rows")),
            2
        );
    }

    #[test]
    fn shared_wal_registry_entry_is_removed_when_last_handle_drops() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("shared-wal-registry-cleanup.ddb");
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE t (id INTEGER)")
            .expect("create table");

        let canonical_path = crate::vfs::VfsHandle::for_path(&path)
            .canonicalize_path(&path)
            .expect("canonicalize path");
        assert!(crate::wal::shared::has_registry_entry_for_tests(
            &canonical_path
        ));

        drop(db);

        assert!(!crate::wal::shared::has_registry_entry_for_tests(
            &canonical_path
        ));
    }

    #[test]
    fn checkpoint_preserves_unchanged_table_payload_pages() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("checkpoint-unchanged-table-payload.ddb");
        let config = DbConfig::default();

        let setup = Db::open_or_create(&path, config.clone()).expect("open setup");
        setup
            .execute("CREATE TABLE initial (id INTEGER, val TEXT)")
            .expect("create initial table");
        setup
            .execute_with_params(
                "INSERT INTO initial VALUES ($1, $2)",
                &[Value::Int64(1), Value::Text("seed".to_string())],
            )
            .expect("seed initial row");
        setup.checkpoint().expect("checkpoint setup");
        drop(setup);

        let writer = Db::open_or_create(&path, config).expect("open writer");
        writer
            .execute("CREATE TABLE new_table (id INTEGER, val TEXT)")
            .expect("create new table");
        writer.begin_transaction().expect("begin writer txn");
        for i in 0_i64..100_i64 {
            writer
                .execute_with_params(
                    "INSERT INTO new_table VALUES ($1, $2)",
                    &[Value::Int64(i), Value::Text(format!("value-{i}"))],
                )
                .expect("insert new row");
        }
        writer.commit_transaction().expect("commit writer");
        writer.checkpoint().expect("checkpoint writer");
        drop(writer);

        let reopened = Db::open_or_create(&path, DbConfig::default()).expect("reopen database");
        assert_eq!(
            scalar_i64(
                &reopened
                    .execute("SELECT COUNT(*) FROM initial")
                    .expect("count initial rows after checkpoint")
            ),
            1
        );
        assert_eq!(
            scalar_i64(
                &reopened
                    .execute("SELECT COUNT(*) FROM new_table")
                    .expect("count new rows after checkpoint")
            ),
            100
        );
    }

    #[test]
    fn schema_snapshot_projects_rich_schema_metadata() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE parent (id INT PRIMARY KEY)")
            .expect("create parent table");
        db.execute(
            "CREATE TABLE child (
                id INT PRIMARY KEY,
                parent_id INT REFERENCES parent(id) ON DELETE CASCADE ON UPDATE SET NULL,
                qty INT CHECK (qty > 0),
                price FLOAT64 NOT NULL,
                total_stored FLOAT64 GENERATED ALWAYS AS (price * qty) STORED,
                total_virtual FLOAT64 GENERATED ALWAYS AS (price * qty) VIRTUAL,
                CONSTRAINT child_parent_positive CHECK (parent_id IS NULL OR parent_id > 0),
                CHECK (qty < 1000)
            )",
        )
        .expect("create child table");
        db.execute("INSERT INTO parent VALUES (1)")
            .expect("insert parent row");
        db.execute("INSERT INTO child (id, parent_id, qty, price) VALUES (1, 1, 5, 3.0)")
            .expect("insert child row");

        db.execute("CREATE TEMP TABLE temp_data (id INT PRIMARY KEY)")
            .expect("create temp table");
        db.execute("INSERT INTO temp_data VALUES (7)")
            .expect("insert temp row");
        db.execute("CREATE VIEW child_view AS SELECT id, parent_id FROM child")
            .expect("create view");
        db.execute("CREATE TEMP VIEW temp_child_ids AS SELECT id FROM temp_data")
            .expect("create temp view");
        db.execute(
            "CREATE INDEX child_parent_partial ON child(parent_id) WHERE parent_id IS NOT NULL",
        )
        .expect("create partial index");
        db.execute(
            "CREATE TRIGGER child_after_insert AFTER INSERT ON child FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO parent VALUES (999)')",
        )
        .expect("create table trigger");
        db.execute(
            "CREATE TRIGGER child_view_insert INSTEAD OF INSERT ON child_view FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO child (id, parent_id, qty, price) VALUES (2000, NULL, 1, 1.0)')",
        )
        .expect("create view trigger");

        let snapshot = db.get_schema_snapshot().expect("schema snapshot");
        assert_eq!(snapshot.snapshot_version, 1);

        let child = snapshot
            .tables
            .iter()
            .find(|table| table.name == "child")
            .expect("child table in snapshot");
        assert!(!child.temporary);
        assert_eq!(child.row_count, 1);
        assert!(child.ddl.contains("CREATE TABLE \"child\""));
        assert!(child
            .checks
            .iter()
            .any(|check| check.name.as_deref() == Some("child_parent_positive")));
        assert!(child.checks.iter().any(|check| check.name.is_none()));
        assert!(child
            .foreign_keys
            .iter()
            .any(|fk| fk.on_delete == "CASCADE" && fk.on_update == "SET NULL"));

        let qty_column = child
            .columns
            .iter()
            .find(|column| column.name == "qty")
            .expect("qty column");
        assert!(qty_column.checks.iter().any(|check| check.name.is_none()));

        let stored = child
            .columns
            .iter()
            .find(|column| column.name == "total_stored")
            .expect("stored generated column");
        assert_eq!(stored.generated_sql.as_deref(), Some("(price * qty)"));
        assert!(stored.generated_stored);

        let virtual_column = child
            .columns
            .iter()
            .find(|column| column.name == "total_virtual")
            .expect("virtual generated column");
        assert_eq!(
            virtual_column.generated_sql.as_deref(),
            Some("(price * qty)")
        );
        assert!(!virtual_column.generated_stored);

        let temp_table = snapshot
            .tables
            .iter()
            .find(|table| table.name == "temp_data")
            .expect("temp table in snapshot");
        assert!(temp_table.temporary);
        assert_eq!(temp_table.row_count, 1);
        assert!(temp_table.ddl.contains("CREATE TEMP TABLE"));

        let temp_view = snapshot
            .views
            .iter()
            .find(|view| view.name == "temp_child_ids")
            .expect("temp view in snapshot");
        assert!(temp_view.temporary);
        assert!(temp_view.ddl.contains("CREATE TEMP VIEW"));

        let partial_index = snapshot
            .indexes
            .iter()
            .find(|index| index.name == "child_parent_partial")
            .expect("partial index in snapshot");
        assert_eq!(
            partial_index.predicate_sql.as_deref(),
            Some("parent_id IS NOT NULL")
        );
        assert!(partial_index.ddl.contains("WHERE parent_id IS NOT NULL"));

        let table_trigger = snapshot
            .triggers
            .iter()
            .find(|trigger| trigger.name == "child_after_insert")
            .expect("table trigger in snapshot");
        assert_eq!(table_trigger.target_kind, "table");
        assert_eq!(table_trigger.timing, "after");
        assert_eq!(table_trigger.events, vec!["insert".to_string()]);
        assert_eq!(table_trigger.events_mask, 1);
        assert!(table_trigger.for_each_row);
        assert!(!table_trigger.temporary);
        assert!(table_trigger.ddl.contains("CREATE TRIGGER"));

        let view_trigger = snapshot
            .triggers
            .iter()
            .find(|trigger| trigger.name == "child_view_insert")
            .expect("view trigger in snapshot");
        assert_eq!(view_trigger.target_kind, "view");
        assert_eq!(view_trigger.timing, "instead_of");
    }

    #[test]
    fn schema_snapshot_orders_top_level_collections_by_name() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE z_table (id INT PRIMARY KEY)")
            .expect("create z_table");
        db.execute("CREATE TABLE a_table (id INT PRIMARY KEY)")
            .expect("create a_table");
        db.execute("CREATE TEMP TABLE m_table (id INT PRIMARY KEY)")
            .expect("create m_table");
        db.execute("CREATE VIEW z_view AS SELECT id FROM z_table")
            .expect("create z_view");
        db.execute("CREATE VIEW a_view AS SELECT id FROM a_table")
            .expect("create a_view");
        db.execute("CREATE TEMP VIEW m_view AS SELECT id FROM m_table")
            .expect("create m_view");
        db.execute("CREATE INDEX z_index ON z_table(id)")
            .expect("create z_index");
        db.execute("CREATE INDEX a_index ON a_table(id)")
            .expect("create a_index");
        db.execute(
            "CREATE TRIGGER z_trigger AFTER INSERT ON z_table FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO z_table VALUES (1000)')",
        )
        .expect("create z_trigger");
        db.execute(
            "CREATE TRIGGER a_trigger AFTER INSERT ON a_table FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO a_table VALUES (1000)')",
        )
        .expect("create a_trigger");

        let snapshot = db.get_schema_snapshot().expect("schema snapshot");

        let table_names = snapshot
            .tables
            .iter()
            .map(|table| table.name.clone())
            .collect::<Vec<_>>();
        assert_sorted_names(&table_names);

        let view_names = snapshot
            .views
            .iter()
            .map(|view| view.name.clone())
            .collect::<Vec<_>>();
        assert_sorted_names(&view_names);

        let index_names = snapshot
            .indexes
            .iter()
            .map(|index| index.name.clone())
            .collect::<Vec<_>>();
        assert_sorted_names(&index_names);

        let trigger_names = snapshot
            .triggers
            .iter()
            .map(|trigger| trigger.name.clone())
            .collect::<Vec<_>>();
        assert_sorted_names(&trigger_names);
    }

    #[test]
    fn write_transaction_page_allocation_stays_off_main_file_until_commit() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("txn-page-allocation.ddb");
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");

        let initial_page_count = db
            .inner
            .pager
            .on_disk_page_count()
            .expect("initial page count");

        db.begin_write().expect("begin write txn");
        let page_id = db.allocate_page().expect("allocate staged page");
        assert!(page_id > initial_page_count);
        assert_eq!(
            db.inner
                .pager
                .on_disk_page_count()
                .expect("page count after staged allocation"),
            initial_page_count
        );
        assert_eq!(
            db.read_page(page_id).expect("read staged page").to_vec(),
            vec![0_u8; db.config().page_size as usize]
        );
        db.rollback().expect("rollback write txn");

        let reopened = Db::open_or_create(&path, DbConfig::default()).expect("reopen db");
        assert_eq!(
            reopened
                .inner
                .pager
                .on_disk_page_count()
                .expect("page count after rollback"),
            initial_page_count
        );
    }

    #[test]
    fn freed_pages_are_reused_after_commit() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("freelist-reuse.ddb");
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");

        db.begin_write().expect("begin allocate txn");
        let page_id = db.allocate_page().expect("allocate page");
        db.commit().expect("commit allocated page");

        db.begin_write().expect("begin free txn");
        db.free_page(page_id).expect("free page");
        db.commit().expect("commit freed page");

        db.begin_write().expect("begin reuse txn");
        let reused = db.allocate_page().expect("reuse page");
        assert_eq!(reused, page_id);
        db.rollback().expect("rollback reuse txn");
    }

    #[test]
    fn checkpoint_truncates_tail_freelist_pages_and_resets_allocator_state() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("checkpoint-tail-truncation.ddb");
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");

        db.begin_write().expect("begin allocate txn");
        let page3 = db.allocate_page().expect("allocate page 3");
        let page4 = db.allocate_page().expect("allocate page 4");
        let page5 = db.allocate_page().expect("allocate page 5");
        assert_eq!((page3, page4, page5), (3, 4, 5));
        db.commit().expect("commit allocated pages");

        db.begin_write().expect("begin free txn");
        db.free_page(page5).expect("free page 5");
        db.free_page(page4).expect("free page 4");
        db.commit().expect("commit freed tail pages");
        db.checkpoint().expect("checkpoint freed pages");

        assert_eq!(
            db.inner
                .pager
                .on_disk_page_count()
                .expect("page count after truncation"),
            3
        );
        assert_eq!(
            db.inner
                .pager
                .header_snapshot()
                .expect("header snapshot after truncation")
                .freelist
                .page_count,
            0
        );

        db.begin_write().expect("begin allocate after truncation");
        let reused = db.allocate_page().expect("allocate page after truncation");
        assert_eq!(reused, 4);
        db.rollback().expect("rollback post-truncation allocation");
    }

    fn dummy_prepared_insert(table_name: &str) -> PreparedSimpleInsert {
        PreparedSimpleInsert {
            table_name: table_name.to_string(),
            columns: vec![PreparedInsertColumn {
                name: "id".to_string(),
                column_type: crate::catalog::ColumnType::Int64,
                auto_increment: false,
            }],
            primary_auto_row_id_column_index: None,
            value_sources: vec![PreparedInsertValueSource::Null],
            required_columns: Vec::new(),
            foreign_keys: Vec::new(),
            unique_indexes: Vec::new(),
            insert_indexes: Vec::new(),
            use_generic_validation: false,
            use_generic_index_updates: false,
            compiled_index_state_epoch: 0,
        }
    }

    fn assert_sorted_names(names: &[String]) {
        let mut sorted = names.to_vec();
        sorted.sort();
        assert_eq!(names, sorted);
    }

    fn scalar_i64(result: &crate::QueryResult) -> i64 {
        match result.rows()[0].values()[0] {
            Value::Int64(value) => value,
            ref other => panic!("expected INT64 scalar, got {other:?}"),
        }
    }

    /// ADR 0143 Phase A: `inspect_storage_state_json` exposes per-runtime
    /// table residency so callers can verify lazy-load progress.
    #[test]
    fn inspect_storage_state_json_reports_table_memory_totals() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE m (id INTEGER PRIMARY KEY, label TEXT)")
            .expect("create m");
        for i in 0..32 {
            db.execute(&format!(
                "INSERT INTO m (id, label) VALUES ({i}, 'value-{i}-with-some-text')"
            ))
            .expect("insert m");
        }
        let json = db
            .inspect_storage_state_json()
            .expect("inspect storage state");
        assert!(
            json.contains("\"tables_in_memory_bytes\":"),
            "missing tables_in_memory_bytes: {json}"
        );
        assert!(
            json.contains("\"rows_in_memory_count\":"),
            "missing rows_in_memory_count: {json}"
        );
        assert!(
            json.contains("\"loaded_table_count\":"),
            "missing loaded_table_count: {json}"
        );
        assert!(
            json.contains("\"deferred_table_count\":0"),
            "expected zero deferred tables on a fresh in-memory db: {json}"
        );
        // 32 inserted rows must show up in the residency total.
        assert!(
            json.contains("\"rows_in_memory_count\":32"),
            "expected rows_in_memory_count=32, got: {json}"
        );
    }

    /// ADR 0143 Phase B: by default, re-opening a DB leaves persisted
    /// tables in the deferred set until the first SQL statement runs,
    /// then materializes them.
    #[test]
    fn default_defer_table_materialization_skips_eager_load_at_open() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("defer-load.ddb");

        // Seed a DB with a persisted table and close.
        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create seeded");
            for i in 0..50 {
                db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                    .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with default defer");

        // At open, the table should not yet be loaded.
        let json_open = db.inspect_storage_state_json().expect("json snapshot");
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected 1 deferred table at open, got: {json_open}"
        );
        assert!(
            json_open.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows at open, got: {json_open}"
        );

        // The first SELECT triggers materialization.
        let result = db
            .execute("SELECT COUNT(*) FROM seeded")
            .expect("count after defer");
        assert_eq!(scalar_i64(&result), 50);

        let json_after = db.inspect_storage_state_json().expect("json after");
        assert!(
            json_after.contains("\"deferred_table_count\":0"),
            "expected zero deferred tables after first query, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":50"),
            "expected 50 resident rows after materialize, got: {json_after}"
        );
    }

    #[test]
    fn row_id_point_lookup_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-row-id.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create seeded");
            for i in 0..50 {
                db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                    .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let json_open = db.inspect_storage_state_json().expect("json snapshot");
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected deferred table at open, got: {json_open}"
        );

        let result = db
            .execute("SELECT n FROM seeded WHERE id = 17")
            .expect("point lookup");
        assert_eq!(scalar_i64(&result), 17);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after point lookup");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected point lookup to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after deferred point lookup, got: {json_after}"
        );
    }

    #[test]
    fn prepared_row_id_point_lookup_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("prepared-deferred-row-id.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create seeded");
            for i in 0..50 {
                db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                    .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let prepared = db
            .prepare("SELECT n FROM seeded WHERE id = $1")
            .expect("prepare point lookup");
        let result = prepared
            .execute(&[Value::Int64(17)])
            .expect("execute prepared point lookup");
        assert_eq!(scalar_i64(&result), 17);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after prepared point lookup");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected prepared point lookup to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after prepared deferred point lookup, got: {json_after}"
        );
    }

    #[test]
    fn simple_table_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-simple-projection.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create seeded");
            for i in 0..50 {
                db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                    .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute("SELECT n FROM seeded")
            .expect("simple projection");
        assert_eq!(result.rows().len(), 50);
        assert_eq!(result.rows()[0].values(), &[Value::Int64(0)]);
        assert_eq!(result.rows()[49].values(), &[Value::Int64(49)]);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after simple projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected simple projection to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after simple projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after simple projection, got: {json_after}"
        );
    }

    #[test]
    fn simple_filtered_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-filtered-projection.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create seeded");
            for i in 0..50 {
                db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                    .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute("SELECT n FROM seeded WHERE n >= 10 AND n <= 12 ORDER BY n")
            .expect("filtered projection");
        assert_eq!(result.rows().len(), 3);
        assert_eq!(result.rows()[0].values(), &[Value::Int64(10)]);
        assert_eq!(result.rows()[1].values(), &[Value::Int64(11)]);
        assert_eq!(result.rows()[2].values(), &[Value::Int64(12)]);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after filtered projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected filtered projection to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after filtered projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after filtered projection, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_numeric_aggregate_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-grouped-aggregate.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
                .expect("create seeded");
            for i in 0..10 {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n) VALUES ({i}, {}, {i})",
                    i % 2
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp, COUNT(*), SUM(n) FROM seeded WHERE n >= 2 AND n <= 7 GROUP BY grp",
            )
            .expect("grouped aggregate");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(0), Value::Int64(3), Value::Int64(12)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(1), Value::Int64(3), Value::Int64(15)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped aggregate");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped aggregate to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped aggregate, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped aggregate, got: {json_after}"
        );
    }

    #[test]
    fn defer_table_materialization_false_preserves_eager_load_at_open() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("eager-load.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create seeded");
            for i in 0..50 {
                db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                    .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(
            &path,
            DbConfig {
                defer_table_materialization: false,
                ..DbConfig::default()
            },
        )
        .expect("reopen with eager load");

        let json_open = db.inspect_storage_state_json().expect("json snapshot");
        assert!(
            json_open.contains("\"deferred_table_count\":0"),
            "expected zero deferred tables at open, got: {json_open}"
        );
        assert!(
            json_open.contains("\"rows_in_memory_count\":50"),
            "expected eager load to materialize rows at open, got: {json_open}"
        );
    }

    #[test]
    fn per_table_load_matches_mixed_case_identifiers_case_insensitively() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("mixed-case.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE \"ArtistStaging\" (id INTEGER PRIMARY KEY, name TEXT)")
                .expect("create mixed-case table");
            db.execute("INSERT INTO \"ArtistStaging\" VALUES (1, 'alpha')")
                .expect("insert seed row");
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen");
        let json_open = db.inspect_storage_state_json().expect("json at open");
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected deferred table at open, got: {json_open}"
        );

        let result = db
            .execute("SELECT COUNT(*) FROM ArtistStaging")
            .expect("count mixed-case table from unquoted SQL");
        assert_eq!(scalar_i64(&result), 1);

        let json_after = db.inspect_storage_state_json().expect("json after query");
        assert!(
            json_after.contains("\"loaded_table_count\":1,"),
            "expected mixed-case table to load on first query, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":0}"),
            "expected no deferred tables after load, got: {json_after}"
        );
    }

    /// ADR 0143 Phase B: per-table on-demand load - query only small table
    /// should not materialize the large table.
    #[test]
    fn per_table_load_skips_large_table() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("per-table.ddb");

        // Seed the DB with both tables - small and large
        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE small (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create small");
            db.execute("CREATE TABLE large (id INTEGER PRIMARY KEY, data TEXT)")
                .expect("create large");

            for i in 0..10 {
                db.execute(&format!("INSERT INTO small (id, n) VALUES ({i}, {i})"))
                    .expect("insert small");
            }
            db.checkpoint().expect("checkpoint small");
            for i in 0..1000 {
                let data = format!("data-{}", i);
                db.execute(&format!(
                    "INSERT INTO large (id, data) VALUES ({i}, '{}')",
                    data
                ))
                .expect("insert large");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        // Re-open with deferred materialization
        let cfg = DbConfig {
            defer_table_materialization: true,
            ..DbConfig::default()
        };
        let db = Db::open_or_create(&path, cfg).expect("reopen with defer");

        // At open, both tables should be deferred
        let json_open = db.inspect_storage_state_json().expect("json at open");
        assert!(
            json_open.contains("\"deferred_table_count\":2"),
            "expected 2 deferred tables at open, got: {json_open}"
        );

        // Query only the small table
        let result = db
            .execute("SELECT COUNT(*) FROM small")
            .expect("query small");
        assert_eq!(scalar_i64(&result), 10);

        // After query: per-table on-demand load must have materialized
        // ONLY `small`, leaving `large` deferred. Use precise field
        // assertions (with trailing comma) to avoid the substring-match
        // bug where `:10` matches `:1010`.
        let json_after = db.inspect_storage_state_json().expect("json after query");
        assert!(
            json_after.contains("\"deferred_table_count\":1}"),
            "expected exactly 1 deferred table after small-only query, got: {json_after}"
        );
        assert!(
            json_after.contains("\"loaded_table_count\":1,"),
            "expected exactly 1 loaded table after small-only query, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":10,"),
            "expected exactly 10 resident rows (small only), got: {json_after}"
        );
    }

    /// ADR 0143 Phase B: when statement analysis is uncertain (subquery
    /// in WHERE), the per-table gate must conservatively fall back to
    /// loading all tables.
    #[test]
    fn per_table_load_falls_back_for_subquery() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("subq.ddb");
        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create a");
            db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create b");
            for i in 0..3 {
                db.execute(&format!("INSERT INTO a VALUES ({i},{i})"))
                    .expect("ins a");
                db.execute(&format!("INSERT INTO b VALUES ({i},{i})"))
                    .expect("ins b");
            }
            db.checkpoint().expect("checkpoint");
        }
        let cfg = DbConfig {
            defer_table_materialization: true,
            ..DbConfig::default()
        };
        let db = Db::open_or_create(&path, cfg).expect("reopen");
        // EXISTS subquery references `b`, but `safe_referenced_tables`
        // returns None for any subquery shape, so both tables must load.
        db.execute("SELECT COUNT(*) FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.n = a.n)")
            .expect("query with subquery");
        let json = db.inspect_storage_state_json().expect("json");
        assert!(
            json.contains("\"deferred_table_count\":0}"),
            "expected zero deferred tables after subquery fallback, got: {json}"
        );
        assert!(
            json.contains("\"loaded_table_count\":2,"),
            "expected both tables loaded after subquery fallback, got: {json}"
        );
    }
}

//! Stable database owner and bootstrap lifecycle entry points.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::catalog::CatalogHandle;
use crate::config::DbConfig;
use crate::error::{DbError, Result};
use crate::exec::{statement_is_read_only, BulkLoadOptions, EngineRuntime, QueryResult};
use crate::record::value::Value;
use crate::sql::parser::parse_sql_batch;
use crate::storage::page::{self, PageId};
use crate::storage::{self, DatabaseHeader, PagerHandle};
use crate::vfs::faulty::{self, FailAction, Failpoint};
use crate::vfs::{FileKind, OpenMode, VfsHandle};
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
    write_txn: Mutex<WriteTxn>,
    held_snapshots: Mutex<HashMap<u64, ReaderGuard>>,
}

#[derive(Debug, Default)]
struct WriteTxn {
    active: bool,
    staged_pages: BTreeMap<PageId, Vec<u8>>,
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
        let statements = parse_sql_batch(sql)?;
        let mut results = Vec::with_capacity(statements.len());
        for statement in statements {
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
        self.persist_runtime(working)
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
        self.persist_runtime(working)
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
                write_txn: Mutex::new(WriteTxn::default()),
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
        let _writer = self
            .inner
            .sql_write_lock
            .lock()
            .map_err(|_| DbError::internal("SQL writer lock poisoned"))?;
        self.refresh_engine_from_storage()?;
        let _savepoint = StatementSavepoint::new(self.inner.wal.latest_snapshot());
        let mut working = self.engine_snapshot()?;
        let result = working.execute_statement(statement, params, self.inner.config.page_size)?;
        self.persist_runtime(working)?;
        Ok(result)
    }

    fn engine_snapshot(&self) -> Result<EngineRuntime> {
        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        let mut snapshot = runtime.clone();
        snapshot.rebuild_indexes(self.inner.config.page_size)?;
        Ok(snapshot)
    }

    fn persist_runtime(&self, runtime: EngineRuntime) -> Result<()> {
        self.begin_write()?;
        if let Err(error) = runtime.persist_to_db(self) {
            let _ = self.rollback();
            return Err(error);
        }
        let committed_lsn = match self.commit() {
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

fn json_escape(input: String) -> String {
    input
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

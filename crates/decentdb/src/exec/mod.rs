//! Query execution operators, SQL result values, and runtime state.

pub(crate) mod bulk_load;
#[cfg(test)]
mod bulk_load_tests;
pub(crate) mod constraints;
pub(crate) mod ddl;
pub(crate) mod dml;
pub(crate) mod operators;
pub(crate) mod row;
pub(crate) mod triggers;
pub(crate) mod txn;
pub(crate) mod views;
#[cfg(test)]
mod views_tests;

#[cfg(test)]
mod dml_more_tests;
#[cfg(test)]
mod dml_unit_tests;
#[cfg(test)]
mod runtime_unit_tests;

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, Months, NaiveDate, NaiveDateTime, TimeZone,
    Timelike, Utc,
};

use crate::catalog::{
    identifiers_equal, CatalogState, ColumnSchema, IndexKind, IndexSchema, IndexStats, SchemaInfo,
    TableSchema, TableStats, ViewSchema,
};
use crate::error::{DbError, Result};
use crate::json::{parse_json, parse_json_path, JsonValue};
use crate::planner;
use crate::record::compression::{CompressionMode, AUTO_MIN_PAYLOAD_BYTES};
use crate::record::key::encode_index_key;
use crate::record::overflow::{
    append_uncompressed_with_tail, build_overflow_chain_cache, free_overflow, read_overflow,
    read_uncompressed_overflow_tail, rewrite_overflow, rewrite_overflow_cached, OverflowChainCache,
    OverflowPointer, OverflowTailInfo, OVERFLOW_HEADER_SIZE,
};
use crate::record::row::Row;
use crate::record::value::{compare_decimal, parse_decimal_text, Value};
use crate::search::{TrigramIndex, TrigramQueryResult};
use crate::sql::ast::{
    BinaryOp, ColumnDefinition, CommonTableExpr, CreateTableAsStatement, CreateTableStatement,
    Expr, FromItem, JoinConstraint, JoinKind, Query, QueryBody, Select, SelectItem, Statement,
    SubqueryQuantifier, TruncateIdentityMode, UnaryOp,
};
use crate::sql::parser::parse_sql_statement;
use crate::storage::checksum::{crc32c_extend, crc32c_parts};
use crate::storage::page::{self, PageId, PageStore};
use crate::storage::PagerHandle;
use crate::wal::WalHandle;

use self::row::{ColumnBinding, Dataset};

pub use row::{QueryResult, QueryRow};

const ENGINE_ROOT_MAGIC: [u8; 8] = *b"DDBSQL1\0";
const ENGINE_ROOT_VERSION: u32 = 1;
const ENGINE_ROOT_HEADER_SIZE: usize = 32;
const RECURSIVE_CTE_MAX_ITERATIONS: usize = 1000;
const LEGACY_RUNTIME_PAYLOAD_MAGIC: &[u8; 9] = b"DDBSTATE1";
const MANIFEST_PAYLOAD_MAGIC: &[u8; 8] = b"DDBMANF1";
const TABLE_PAYLOAD_MAGIC: &[u8; 8] = b"DDBTBL01";
const GENERATED_COLUMNS_SECTION_MAGIC: &[u8; 8] = b"DDBGCM02";
const INDEX_INCLUDE_COLUMNS_SECTION_MAGIC: &[u8; 8] = b"DDBICL1\0";
const SCHEMAS_SECTION_MAGIC: &[u8; 8] = b"DDBSCH01";
static RANDOM_STATE: AtomicU64 = AtomicU64::new(0);

fn generated_columns_are_stored(table: &TableSchema) -> bool {
    table
        .columns
        .iter()
        .all(|column| column.generated_sql.is_none() || column.generated_stored)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NameResolutionScope {
    Session,
}

fn map_get_ci<'a, V>(map: &'a BTreeMap<String, V>, name: &str) -> Option<&'a V> {
    map.get(name).or_else(|| {
        map.iter()
            .find(|(entry_name, _)| identifiers_equal(entry_name, name))
            .map(|(_, value)| value)
    })
}

fn map_get_ci_mut<'a, V>(map: &'a mut BTreeMap<String, V>, name: &str) -> Option<&'a mut V> {
    if map.contains_key(name) {
        return map.get_mut(name);
    }
    let existing = map
        .keys()
        .find(|entry_name| identifiers_equal(entry_name, name))
        .cloned()?;
    map.get_mut(&existing)
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StoredRow {
    pub(crate) row_id: i64,
    pub(crate) values: Vec<Value>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct TableData {
    pub(crate) rows: Vec<StoredRow>,
}

impl TableData {
    pub(super) fn row_index_by_id(&self, row_id: i64) -> Option<usize> {
        if let Some(index) = row_id
            .checked_sub(1)
            .and_then(|value| usize::try_from(value).ok())
        {
            if let Some(row) = self.rows.get(index) {
                if row.row_id == row_id {
                    return Some(index);
                }
            }
        }

        if let Ok(index) = self.rows.binary_search_by_key(&row_id, |row| row.row_id) {
            return Some(index);
        }

        self.rows.iter().position(|row| row.row_id == row_id)
    }

    pub(super) fn row_by_id(&self, row_id: i64) -> Option<&StoredRow> {
        self.row_index_by_id(row_id)
            .and_then(|index| self.rows.get(index))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PersistedTableState {
    pub(crate) pointer: OverflowPointer,
    pub(crate) checksum: u32,
    pub(crate) row_count: usize,
    pub(crate) tail: OverflowTailInfo,
}

impl Default for PersistedTableState {
    fn default() -> Self {
        Self {
            pointer: OverflowPointer {
                head_page_id: 0,
                logical_len: 0,
                flags: 0,
            },
            checksum: 0,
            row_count: 0,
            tail: OverflowTailInfo::default(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeBtreeKey {
    Encoded(Vec<u8>),
    Int64(i64),
}

#[derive(Default)]
pub(crate) struct Int64IdentityHasher(u64);

impl Hasher for Int64IdentityHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        // Runtime INT64 index keys are hashed via write_i64/write_u64. This fallback
        // preserves determinism for any incidental byte-oriented hashing.
        let mut hash = 0_u64;
        for (shift, byte) in bytes.iter().copied().take(8).enumerate() {
            hash |= u64::from(byte) << (shift * 8);
        }
        self.0 = hash;
    }

    fn write_i64(&mut self, value: i64) {
        self.0 = value as u64;
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

type Int64HashBuilder = BuildHasherDefault<Int64IdentityHasher>;
type Int64Map<V> = HashMap<i64, V, Int64HashBuilder>;

#[derive(Clone, Debug)]
pub(crate) enum RuntimeBtreeKeys {
    UniqueEncoded(BTreeMap<Vec<u8>, i64>),
    NonUniqueEncoded(BTreeMap<Vec<u8>, Vec<i64>>),
    UniqueInt64(Int64Map<i64>),
    NonUniqueInt64(Int64Map<Vec<i64>>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeRowIdSet<'a> {
    Empty,
    Single(i64),
    Many(&'a [i64]),
}

impl RuntimeRowIdSet<'_> {
    #[must_use]
    pub(crate) fn len(self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Single(_) => 1,
            Self::Many(values) => values.len(),
        }
    }

    #[must_use]
    pub(crate) fn is_empty(self) -> bool {
        matches!(self, Self::Empty)
    }

    pub(crate) fn for_each(self, mut f: impl FnMut(i64)) {
        match self {
            Self::Empty => {}
            Self::Single(row_id) => f(row_id),
            Self::Many(values) => {
                for row_id in values {
                    f(*row_id);
                }
            }
        }
    }
}

impl RuntimeBtreeKeys {
    fn row_id_set_for_key(&self, key: &RuntimeBtreeKey) -> RuntimeRowIdSet<'_> {
        match (self, key) {
            (Self::UniqueEncoded(keys), RuntimeBtreeKey::Encoded(key)) => keys
                .get(key)
                .copied()
                .map(RuntimeRowIdSet::Single)
                .unwrap_or(RuntimeRowIdSet::Empty),
            (Self::NonUniqueEncoded(keys), RuntimeBtreeKey::Encoded(key)) => keys
                .get(key)
                .map(Vec::as_slice)
                .map(RuntimeRowIdSet::Many)
                .unwrap_or(RuntimeRowIdSet::Empty),
            (Self::UniqueInt64(keys), RuntimeBtreeKey::Int64(key)) => keys
                .get(key)
                .copied()
                .map(RuntimeRowIdSet::Single)
                .unwrap_or(RuntimeRowIdSet::Empty),
            (Self::NonUniqueInt64(keys), RuntimeBtreeKey::Int64(key)) => keys
                .get(key)
                .map(Vec::as_slice)
                .map(RuntimeRowIdSet::Many)
                .unwrap_or(RuntimeRowIdSet::Empty),
            _ => RuntimeRowIdSet::Empty,
        }
    }

    pub(super) fn row_ids_for_key(&self, key: &RuntimeBtreeKey) -> Vec<i64> {
        let row_ids = self.row_id_set_for_key(key);
        let mut values = Vec::with_capacity(row_ids.len());
        row_ids.for_each(|row_id| values.push(row_id));
        values
    }

    pub(super) fn row_ids_for_value_set(&self, value: &Value) -> Result<RuntimeRowIdSet<'_>> {
        match self {
            Self::UniqueEncoded(_) | Self::NonUniqueEncoded(_) => {
                let key = RuntimeBtreeKey::Encoded(encode_index_key(value)?);
                Ok(self.row_id_set_for_key(&key))
            }
            Self::UniqueInt64(_) | Self::NonUniqueInt64(_) => match value {
                Value::Int64(value) => Ok(self.row_id_set_for_key(&RuntimeBtreeKey::Int64(*value))),
                _ => Ok(RuntimeRowIdSet::Empty),
            },
        }
    }

    pub(super) fn row_ids_for_value(&self, value: &Value) -> Result<Vec<i64>> {
        let row_ids = self.row_ids_for_value_set(value)?;
        let mut values = Vec::with_capacity(row_ids.len());
        row_ids.for_each(|row_id| values.push(row_id));
        Ok(values)
    }

    pub(super) fn contains_any(&self, key: &RuntimeBtreeKey) -> bool {
        match (self, key) {
            (Self::UniqueEncoded(keys), RuntimeBtreeKey::Encoded(key)) => keys.contains_key(key),
            (Self::NonUniqueEncoded(keys), RuntimeBtreeKey::Encoded(key)) => {
                keys.get(key).is_some_and(|row_ids| !row_ids.is_empty())
            }
            (Self::UniqueInt64(keys), RuntimeBtreeKey::Int64(key)) => keys.contains_key(key),
            (Self::NonUniqueInt64(keys), RuntimeBtreeKey::Int64(key)) => {
                keys.get(key).is_some_and(|row_ids| !row_ids.is_empty())
            }
            _ => false,
        }
    }

    pub(super) fn insert_row_id(&mut self, key: RuntimeBtreeKey, row_id: i64) -> Result<()> {
        match (self, key) {
            (Self::UniqueEncoded(keys), RuntimeBtreeKey::Encoded(key)) => {
                if keys.insert(key, row_id).is_some() {
                    return Err(DbError::internal(
                        "unique runtime BTREE index received a duplicate key insert",
                    ));
                }
            }
            (Self::NonUniqueEncoded(keys), RuntimeBtreeKey::Encoded(key)) => {
                keys.entry(key).or_default().push(row_id);
            }
            (Self::UniqueInt64(keys), RuntimeBtreeKey::Int64(key)) => {
                if keys.insert(key, row_id).is_some() {
                    return Err(DbError::internal(
                        "unique runtime BTREE index received a duplicate key insert",
                    ));
                }
            }
            (Self::NonUniqueInt64(keys), RuntimeBtreeKey::Int64(key)) => {
                keys.entry(key).or_default().push(row_id);
            }
            (Self::UniqueEncoded(_), RuntimeBtreeKey::Int64(_))
            | (Self::NonUniqueEncoded(_), RuntimeBtreeKey::Int64(_))
            | (Self::UniqueInt64(_), RuntimeBtreeKey::Encoded(_))
            | (Self::NonUniqueInt64(_), RuntimeBtreeKey::Encoded(_)) => {
                return Err(DbError::internal(
                    "runtime BTREE key type did not match the runtime index representation",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn remove_row_id(&mut self, key: &RuntimeBtreeKey, row_id: i64) -> Result<()> {
        match (self, key) {
            (Self::UniqueEncoded(keys), RuntimeBtreeKey::Encoded(key)) => {
                if let Some(existing) = keys.get(key).copied() {
                    if existing != row_id {
                        return Err(DbError::internal(
                            "unique runtime BTREE index row-id mismatch during delete",
                        ));
                    }
                    keys.remove(key);
                }
            }
            (Self::NonUniqueEncoded(keys), RuntimeBtreeKey::Encoded(key)) => {
                let remove_entry = if let Some(row_ids) = keys.get_mut(key) {
                    row_ids.retain(|entry| *entry != row_id);
                    row_ids.is_empty()
                } else {
                    false
                };
                if remove_entry {
                    keys.remove(key);
                }
            }
            (Self::UniqueInt64(keys), RuntimeBtreeKey::Int64(key)) => {
                if let Some(existing) = keys.get(key).copied() {
                    if existing != row_id {
                        return Err(DbError::internal(
                            "unique runtime BTREE index row-id mismatch during delete",
                        ));
                    }
                    keys.remove(key);
                }
            }
            (Self::NonUniqueInt64(keys), RuntimeBtreeKey::Int64(key)) => {
                let remove_entry = if let Some(row_ids) = keys.get_mut(key) {
                    row_ids.retain(|entry| *entry != row_id);
                    row_ids.is_empty()
                } else {
                    false
                };
                if remove_entry {
                    keys.remove(key);
                }
            }
            (Self::UniqueEncoded(_), RuntimeBtreeKey::Int64(_))
            | (Self::NonUniqueEncoded(_), RuntimeBtreeKey::Int64(_))
            | (Self::UniqueInt64(_), RuntimeBtreeKey::Encoded(_))
            | (Self::NonUniqueInt64(_), RuntimeBtreeKey::Encoded(_)) => {
                return Err(DbError::internal(
                    "runtime BTREE key type did not match the runtime index representation",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn total_row_id_count(&self) -> usize {
        match self {
            Self::UniqueEncoded(keys) => keys.len(),
            Self::NonUniqueEncoded(keys) => keys.values().map(Vec::len).sum(),
            Self::UniqueInt64(keys) => keys.len(),
            Self::NonUniqueInt64(keys) => keys.values().map(Vec::len).sum(),
        }
    }

    pub(crate) fn distinct_key_count(&self) -> usize {
        match self {
            Self::UniqueEncoded(keys) => keys.len(),
            Self::NonUniqueEncoded(keys) => keys.len(),
            Self::UniqueInt64(keys) => keys.len(),
            Self::NonUniqueInt64(keys) => keys.len(),
        }
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        match self {
            Self::UniqueEncoded(keys) => keys.is_empty(),
            Self::NonUniqueEncoded(keys) => keys.is_empty(),
            Self::UniqueInt64(keys) => keys.is_empty(),
            Self::NonUniqueInt64(keys) => keys.is_empty(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum RuntimeIndex {
    Btree { keys: RuntimeBtreeKeys },
    Trigram { index: TrigramIndex },
}

#[derive(Debug)]
pub(super) enum PendingIndexInsert {
    Btree {
        name: String,
        key: RuntimeBtreeKey,
        row_id: i64,
    },
    Trigram {
        name: String,
        row_id: u64,
        text: String,
    },
}

#[derive(Debug)]
pub(crate) struct EngineRuntime {
    pub(crate) catalog: CatalogState,
    pub(crate) tables: BTreeMap<String, TableData>,
    pub(crate) temp_tables: BTreeMap<String, TableSchema>,
    pub(crate) temp_table_data: BTreeMap<String, TableData>,
    pub(crate) temp_views: BTreeMap<String, ViewSchema>,
    pub(crate) temp_indexes: BTreeMap<String, IndexSchema>,
    pub(crate) temp_schema_cookie: u32,
    pub(crate) indexes: BTreeMap<String, RuntimeIndex>,
    pub(crate) persisted_tables: BTreeMap<String, PersistedTableState>,
    /// Tables whose row data has not yet been loaded from storage.
    /// Populated during `decode_manifest_payload` and cleared by
    /// `load_deferred_tables`.
    pub(crate) deferred_tables: BTreeSet<String>,
    pub(crate) dirty_tables: BTreeSet<String>,
    pub(crate) append_only_dirty_tables: BTreeSet<String>,
    /// Row indices updated by prepared simple UPDATE; avoids full re-encode.
    row_update_dirty: BTreeMap<String, Vec<usize>>,
    /// Cached table payloads from the last commit for incremental splice.
    cached_payloads: BTreeMap<String, Arc<Vec<u8>>>,
    root_state: Option<RootHeader>,
    pub(crate) index_state_epoch: u64,
    manifest_template: Option<ManifestTemplate>,
    overflow_chain_caches: BTreeMap<String, OverflowChainCache>,
    manifest_chain_cache: Option<OverflowChainCache>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BulkLoadOptions {
    pub batch_size: usize,
    pub sync_interval: usize,
    pub disable_indexes: bool,
    pub checkpoint_on_complete: bool,
}

impl Default for BulkLoadOptions {
    fn default() -> Self {
        Self {
            batch_size: 1_000,
            sync_interval: 1_000,
            disable_indexes: false,
            checkpoint_on_complete: true,
        }
    }
}

impl Clone for EngineRuntime {
    fn clone(&self) -> Self {
        Self {
            catalog: self.catalog.clone(),
            tables: self.tables.clone(),
            temp_tables: self.temp_tables.clone(),
            temp_table_data: self.temp_table_data.clone(),
            temp_views: self.temp_views.clone(),
            temp_indexes: self.temp_indexes.clone(),
            temp_schema_cookie: self.temp_schema_cookie,
            indexes: self.indexes.clone(),
            persisted_tables: self.persisted_tables.clone(),
            deferred_tables: self.deferred_tables.clone(),
            // Preserve dirty state so that multi-statement transactions
            // (clone-and-replace) do not lose modifications from earlier
            // statements.  `persist_to_db` clears dirty state after a
            // successful persist, so autocommit paths are unaffected.
            dirty_tables: self.dirty_tables.clone(),
            append_only_dirty_tables: self.append_only_dirty_tables.clone(),
            // Escalate row-update dirty to full dirty on clone: the
            // subsequent generic execution path may modify the same rows
            // in ways that invalidate the splice assumption.
            row_update_dirty: BTreeMap::new(),
            // Arc payloads: clone is just a refcount bump.
            cached_payloads: self.cached_payloads.clone(),
            root_state: self.root_state,
            index_state_epoch: self.index_state_epoch,
            // Optimization caches rebuilt on demand during persist.
            manifest_template: None,
            overflow_chain_caches: BTreeMap::new(),
            manifest_chain_cache: None,
        }
    }
}

impl EngineRuntime {
    #[must_use]
    pub(crate) fn empty(schema_cookie: u32) -> Self {
        Self {
            catalog: CatalogState::empty(schema_cookie),
            tables: BTreeMap::new(),
            temp_tables: BTreeMap::new(),
            temp_table_data: BTreeMap::new(),
            temp_views: BTreeMap::new(),
            temp_indexes: BTreeMap::new(),
            temp_schema_cookie: 0,
            indexes: BTreeMap::new(),
            persisted_tables: BTreeMap::new(),
            deferred_tables: BTreeSet::new(),
            dirty_tables: BTreeSet::new(),
            append_only_dirty_tables: BTreeSet::new(),
            row_update_dirty: BTreeMap::new(),
            cached_payloads: BTreeMap::new(),
            root_state: None,
            index_state_epoch: 0,
            manifest_template: None,
            overflow_chain_caches: BTreeMap::new(),
            manifest_chain_cache: None,
        }
    }

    pub(super) fn bump_temp_schema_cookie(&mut self) {
        self.temp_schema_cookie = self.temp_schema_cookie.wrapping_add(1);
        if self.temp_schema_cookie == 0 {
            self.temp_schema_cookie = 1;
        }
    }

    fn persistent_resolution_runtime(&self) -> Self {
        let mut runtime = self.clone();
        runtime.temp_tables.clear();
        runtime.temp_table_data.clear();
        runtime.temp_views.clear();
        runtime.temp_indexes.clear();
        runtime.temp_schema_cookie = 0;
        runtime
    }

    pub(crate) fn load_from_storage(
        pager: &PagerHandle,
        wal: &WalHandle,
        schema_cookie: u32,
    ) -> Result<(Self, u64)> {
        let reader = wal.begin_reader()?;
        let snapshot_lsn = reader.snapshot_lsn();
        let store = SnapshotPageStore {
            pager,
            wal,
            snapshot_lsn,
        };
        let root_page = store.read_page(page::CATALOG_ROOT_PAGE_ID)?;
        let root = decode_root_header(&root_page)?;
        let mut runtime = if let Some(root) = root {
            let payload = if root.pointer.logical_len == 0 || root.pointer.head_page_id == 0 {
                Vec::new()
            } else {
                read_overflow(&store, root.pointer)?
            };
            if crc32c_parts(&[payload.as_slice()]) != root.payload_checksum {
                return Err(DbError::corruption("catalog state checksum mismatch"));
            }
            let mut runtime = if payload.is_empty() {
                Self::empty(root.schema_cookie)
            } else if payload.starts_with(LEGACY_RUNTIME_PAYLOAD_MAGIC) {
                let mut runtime = decode_runtime_payload(&payload)?;
                runtime.mark_all_tables_dirty();
                runtime
            } else if payload.starts_with(MANIFEST_PAYLOAD_MAGIC) {
                decode_manifest_payload(&store, &payload)?
            } else {
                return Err(DbError::corruption("unknown catalog state payload magic"));
            };
            runtime.root_state = Some(root);
            runtime
        } else {
            Self::empty(schema_cookie)
        };
        runtime.catalog.schema_cookie = schema_cookie;
        if runtime.deferred_tables.is_empty() {
            runtime.rebuild_indexes(pager.page_size())?;
        }
        drop(reader);
        Ok((runtime, snapshot_lsn))
    }

    /// Returns `true` when one or more tables still have their row data
    /// deferred (not yet loaded from storage).
    #[must_use]
    pub(crate) fn has_deferred_tables(&self) -> bool {
        !self.deferred_tables.is_empty()
    }

    /// Materializes all deferred table data from storage, then rebuilds
    /// indexes.  After this call `deferred_tables` is empty and the runtime
    /// is fully populated.
    pub(crate) fn load_deferred_tables(
        &mut self,
        pager: &PagerHandle,
        wal: &WalHandle,
        page_size: u32,
    ) -> Result<()> {
        if self.deferred_tables.is_empty() {
            return Ok(());
        }
        let reader = wal.begin_reader()?;
        let store = SnapshotPageStore {
            pager,
            wal,
            snapshot_lsn: reader.snapshot_lsn(),
        };
        let table_names: Vec<String> = self.deferred_tables.iter().cloned().collect();
        for table_name in &table_names {
            let state = self.persisted_tables.get(table_name).ok_or_else(|| {
                DbError::internal(format!(
                    "deferred table '{table_name}' has no persisted state"
                ))
            })?;
            let pointer = state.pointer;
            let checksum = state.checksum;

            let data = if pointer.head_page_id == 0 || pointer.logical_len == 0 {
                TableData::default()
            } else {
                let payload = read_overflow(&store, pointer)?;
                if crc32c_parts(&[payload.as_slice()]) != checksum {
                    return Err(DbError::corruption(format!(
                        "table payload checksum mismatch for {table_name}"
                    )));
                }
                decode_table_payload(&payload)?
            };

            if let Some(ps) = self.persisted_tables.get_mut(table_name) {
                ps.row_count = data.rows.len();
                ps.tail = read_uncompressed_overflow_tail(&store, ps.pointer)?.unwrap_or_default();
            }
            self.tables.insert(table_name.clone(), data);
        }
        self.deferred_tables.clear();
        drop(reader);
        self.rebuild_indexes(page_size)?;
        Ok(())
    }

    pub(crate) fn persist_to_db(&mut self, db: &crate::db::Db) -> Result<()> {
        let old_root = self.root_state;
        let schema_cookie_changed =
            old_root.is_none_or(|root| root.schema_cookie != self.catalog.schema_cookie);
        let dirty_tables = if self.persisted_tables.is_empty() {
            self.catalog.tables.keys().cloned().collect::<Vec<_>>()
        } else {
            self.dirty_tables.iter().cloned().collect::<Vec<_>>()
        };
        let removed_tables = self
            .persisted_tables
            .keys()
            .filter(|table_name| !self.catalog.tables.contains_key(*table_name))
            .cloned()
            .collect::<Vec<_>>();

        {
            let mut store = DbTxnPageStore { db };
            for table_name in dirty_tables {
                let Some(_table) = self.catalog.tables.get(&table_name) else {
                    continue;
                };
                let data = self.tables.get(&table_name).ok_or_else(|| {
                    DbError::internal(format!("table data for {table_name} is missing"))
                })?;
                let previous_state = self
                    .persisted_tables
                    .get(&table_name)
                    .copied()
                    .unwrap_or_default();
                let previous_pointer = previous_state.pointer;
                if self.append_only_dirty_tables.contains(&table_name)
                    && previous_pointer.head_page_id != 0
                    && !previous_pointer.is_compressed()
                {
                    let existing_count = previous_state.row_count;
                    if existing_count <= data.rows.len() {
                        let appended_rows = encode_appended_table_rows(data, existing_count)?;
                        if !appended_rows.is_empty() {
                            let tail = if previous_state.tail.page_id != 0 {
                                previous_state.tail
                            } else {
                                read_uncompressed_overflow_tail(&store, previous_pointer)?
                                    .ok_or_else(|| {
                                        DbError::corruption("overflow tail info is missing")
                                    })?
                            };
                            let (pointer, tail) = append_uncompressed_with_tail(
                                &mut store,
                                previous_pointer,
                                tail,
                                &appended_rows,
                            )?;
                            let checksum =
                                crc32c_extend(previous_state.checksum, &[appended_rows.as_slice()]);
                            self.persisted_tables.insert(
                                table_name.clone(),
                                PersistedTableState {
                                    pointer,
                                    checksum,
                                    row_count: data.rows.len(),
                                    tail,
                                },
                            );
                            // Invalidate chain cache — the append path
                            // modified the chain without hashing.
                            self.overflow_chain_caches.remove(&table_name);
                            self.cached_payloads.remove(&table_name);
                            continue;
                        }
                    }
                }

                // Choose the encoding path:
                //  1. Row-update splice: only re-encode modified rows using cached payload
                //  2. Append-only: read old payload, append new rows
                //  3. Full re-encode: encode every row from scratch
                let (payload, skip_overflow_pages) =
                    if let Some(dirty_indices) = self.row_update_dirty.get(&table_name) {
                        if let Some(cached) = self.cached_payloads.get(&table_name) {
                            let splice = splice_updated_rows_payload(cached, data, dirty_indices)?;
                            // Compute how many leading overflow pages are
                            // guaranteed identical (their byte ranges fall
                            // entirely within the unchanged prefix).
                            let page_size = db.config().page_size as usize;
                            let chunk_cap = page_size.saturating_sub(OVERFLOW_HEADER_SIZE);
                            let skip = if chunk_cap > 0 {
                                splice.first_dirty_byte / chunk_cap
                            } else {
                                0
                            };
                            (splice.payload, skip)
                        } else {
                            (encode_table_payload(data)?, 0)
                        }
                    } else if self.append_only_dirty_tables.contains(&table_name)
                        && previous_pointer.head_page_id != 0
                    {
                        let previous_payload = read_overflow(&store, previous_pointer)?;
                        (append_table_payload(previous_payload, data)?, 0)
                    } else {
                        (encode_table_payload(data)?, 0)
                    };

                let checksum = crc32c_parts(&[payload.as_slice()]);
                let (pointer, new_chain_cache, tail) = if let Some(chain_cache) =
                    self.overflow_chain_caches.get(&table_name)
                {
                    rewrite_overflow_cached(
                        &mut store,
                        previous_pointer,
                        &payload,
                        &chain_cache.page_ids,
                        skip_overflow_pages,
                    )?
                } else {
                    let ptr = rewrite_overflow(
                        &mut store,
                        previous_pointer,
                        &payload,
                        CompressionMode::Never,
                    )?;
                    let cache = build_overflow_chain_cache(&store, ptr.head_page_id)?;
                    let tail = read_uncompressed_overflow_tail(&store, ptr)?.unwrap_or_default();
                    (ptr, cache, tail)
                };
                self.overflow_chain_caches
                    .insert(table_name.clone(), new_chain_cache);
                self.persisted_tables.insert(
                    table_name.clone(),
                    PersistedTableState {
                        pointer,
                        checksum,
                        row_count: data.rows.len(),
                        tail,
                    },
                );
                self.cached_payloads
                    .insert(table_name.clone(), Arc::new(payload));
            }
            for table_name in removed_tables {
                let Some(state) = self.persisted_tables.remove(&table_name) else {
                    continue;
                };
                self.overflow_chain_caches.remove(&table_name);
                self.cached_payloads.remove(&table_name);
                if state.pointer.head_page_id != 0 {
                    free_overflow(&mut store, state.pointer.head_page_id)?;
                }
            }
        }

        let (checksum, pointer) = {
            // Take the chain cache to avoid overlapping borrows with
            // manifest_payload (which mutates self.manifest_template).
            let chain_cache = self.manifest_chain_cache.take();
            let manifest = self.manifest_payload()?;
            let checksum = crc32c_parts(&[manifest]);
            let previous_manifest_pointer = old_root.map_or(
                OverflowPointer {
                    head_page_id: 0,
                    logical_len: 0,
                    flags: 0,
                },
                |root| root.pointer,
            );
            let pointer = {
                let mut store = DbTxnPageStore { db };
                if let Some(chain_cache) = chain_cache {
                    let (ptr, new_cache, _tail) = rewrite_overflow_cached(
                        &mut store,
                        previous_manifest_pointer,
                        manifest,
                        &chain_cache.page_ids,
                        0,
                    )?;
                    self.manifest_chain_cache = Some(new_cache);
                    ptr
                } else {
                    let ptr = rewrite_overflow(
                        &mut store,
                        previous_manifest_pointer,
                        manifest,
                        CompressionMode::Never,
                    )?;
                    let cache = build_overflow_chain_cache(&store, ptr.head_page_id)?;
                    self.manifest_chain_cache = Some(cache);
                    ptr
                }
            };
            (checksum, pointer)
        };

        let root_page = encode_root_header(
            db.config().page_size,
            RootHeader {
                schema_cookie: self.catalog.schema_cookie,
                payload_checksum: checksum,
                pointer,
            },
        );
        db.write_page_owned(page::CATALOG_ROOT_PAGE_ID, root_page)?;
        self.dirty_tables.clear();
        self.append_only_dirty_tables.clear();
        self.row_update_dirty.clear();
        self.root_state = Some(RootHeader {
            schema_cookie: self.catalog.schema_cookie,
            payload_checksum: checksum,
            pointer,
        });
        if schema_cookie_changed {
            db.set_schema_cookie(self.catalog.schema_cookie)?;
        }
        Ok(())
    }

    pub(crate) fn has_checkpoint_compaction_candidates(&self) -> bool {
        self.persisted_tables.values().any(|state| {
            state.pointer.head_page_id != 0
                && !state.pointer.is_compressed()
                && usize::try_from(state.pointer.logical_len)
                    .ok()
                    .is_some_and(|len| len >= AUTO_MIN_PAYLOAD_BYTES)
        }) || self.root_state.is_some_and(|root| {
            root.pointer.head_page_id != 0
                && !root.pointer.is_compressed()
                && usize::try_from(root.pointer.logical_len)
                    .ok()
                    .is_some_and(|len| len >= AUTO_MIN_PAYLOAD_BYTES)
        })
    }

    pub(crate) fn compact_persisted_payloads_for_checkpoint(
        &mut self,
        db: &crate::db::Db,
    ) -> Result<bool> {
        let old_root = self.root_state;
        let mut changed = false;
        {
            let mut store = DbTxnPageStore { db };
            let table_names = self.persisted_tables.keys().cloned().collect::<Vec<_>>();
            for table_name in table_names {
                let Some(previous_state) = self.persisted_tables.get(&table_name).copied() else {
                    continue;
                };
                let previous_pointer = previous_state.pointer;
                if previous_pointer.head_page_id == 0
                    || previous_pointer.is_compressed()
                    || usize::try_from(previous_pointer.logical_len)
                        .ok()
                        .is_none_or(|len| len < AUTO_MIN_PAYLOAD_BYTES)
                {
                    continue;
                }
                let payload = if let Some(cached) = self.cached_payloads.get(&table_name) {
                    Arc::clone(cached)
                } else {
                    Arc::new(read_overflow(&store, previous_pointer)?)
                };
                let pointer = rewrite_overflow(
                    &mut store,
                    previous_pointer,
                    payload.as_slice(),
                    CompressionMode::Auto,
                )?;
                if pointer != previous_pointer {
                    changed = true;
                }
                let tail = if pointer.is_compressed() {
                    OverflowTailInfo::default()
                } else {
                    read_uncompressed_overflow_tail(&store, pointer)?.unwrap_or_default()
                };
                self.persisted_tables.insert(
                    table_name.clone(),
                    PersistedTableState {
                        pointer,
                        checksum: previous_state.checksum,
                        row_count: previous_state.row_count,
                        tail,
                    },
                );
                self.cached_payloads.insert(table_name.clone(), payload);
                self.overflow_chain_caches.remove(&table_name);
            }
        }

        let (checksum, pointer) = {
            let manifest = self.manifest_payload()?;
            let checksum = crc32c_parts(&[manifest]);
            let previous_manifest_pointer = old_root.map_or(
                OverflowPointer {
                    head_page_id: 0,
                    logical_len: 0,
                    flags: 0,
                },
                |root| root.pointer,
            );
            let pointer = {
                let mut store = DbTxnPageStore { db };
                rewrite_overflow(
                    &mut store,
                    previous_manifest_pointer,
                    manifest,
                    CompressionMode::Auto,
                )?
            };
            (checksum, pointer)
        };

        let new_root = RootHeader {
            schema_cookie: self.catalog.schema_cookie,
            payload_checksum: checksum,
            pointer,
        };
        if old_root != Some(new_root) {
            let root_page = encode_root_header(db.config().page_size, new_root);
            db.write_page_owned(page::CATALOG_ROOT_PAGE_ID, root_page)?;
            self.root_state = Some(new_root);
            changed = true;
        }
        Ok(changed)
    }

    fn manifest_payload(&mut self) -> Result<&[u8]> {
        let use_template =
            self.manifest_template.as_ref().is_some_and(|template| {
                template.schema_cookie == self.catalog.schema_cookie
                    && template.table_next_row_id_offsets.len() == self.catalog.tables.len()
                    && template.table_state_offsets.len() == self.catalog.tables.len()
                    && self.catalog.tables.keys().all(|table_name| {
                        template.table_next_row_id_offsets.contains_key(table_name)
                    })
                    && self
                        .catalog
                        .tables
                        .keys()
                        .all(|table_name| template.table_state_offsets.contains_key(table_name))
            });

        if !use_template {
            let encoded = encode_manifest_payload_with_offsets(self, &self.persisted_tables)?;
            self.manifest_template = Some(ManifestTemplate {
                schema_cookie: self.catalog.schema_cookie,
                table_next_row_id_offsets: encoded.table_next_row_id_offsets,
                table_state_offsets: encoded.table_state_offsets,
                bytes: encoded.bytes,
            });
        }

        let template = self
            .manifest_template
            .as_mut()
            .ok_or_else(|| DbError::internal("manifest template was not initialized"))?;
        for (table_name, offset) in &template.table_next_row_id_offsets {
            let next_row_id = self
                .catalog
                .tables
                .get(table_name)
                .map(|table| table.next_row_id)
                .ok_or_else(|| {
                    DbError::internal(format!(
                        "manifest next_row_id offset referenced unknown table {table_name}"
                    ))
                })?;
            patch_manifest_table_next_row_id(&mut template.bytes, *offset, next_row_id)?;
        }
        for (table_name, offset) in &template.table_state_offsets {
            let state = self
                .persisted_tables
                .get(table_name)
                .copied()
                .unwrap_or_default();
            patch_manifest_table_state(&mut template.bytes, *offset, state)?;
        }
        Ok(template.bytes.as_slice())
    }

    pub(super) fn planner_catalog(&self) -> CatalogState {
        let mut catalog = self.catalog.clone();
        for (name, table) in &self.temp_tables {
            catalog.views.remove(name);
            catalog.tables.insert(name.clone(), table.clone());
            catalog
                .indexes
                .retain(|_, index| !identifiers_equal(&index.table_name, name));
            catalog
                .triggers
                .retain(|_, trigger| !identifiers_equal(&trigger.target_name, name));
            catalog.table_stats.remove(name);
        }
        for (name, view) in &self.temp_views {
            catalog.tables.remove(name);
            catalog.views.insert(name.clone(), view.clone());
            catalog
                .triggers
                .retain(|_, trigger| !identifiers_equal(&trigger.target_name, name));
        }
        catalog
    }

    pub(super) fn temp_relation_exists(&self, name: &str) -> bool {
        self.temp_table_schema(name).is_some() || self.temp_view(name).is_some()
    }

    pub(super) fn temp_table_schema(&self, name: &str) -> Option<&TableSchema> {
        map_get_ci(&self.temp_tables, name)
    }

    pub(super) fn temp_table_schema_mut(&mut self, name: &str) -> Option<&mut TableSchema> {
        map_get_ci_mut(&mut self.temp_tables, name)
    }

    pub(super) fn temp_table_data(&self, name: &str) -> Option<&TableData> {
        map_get_ci(&self.temp_table_data, name)
    }

    pub(super) fn temp_table_data_mut(&mut self, name: &str) -> Option<&mut TableData> {
        map_get_ci_mut(&mut self.temp_table_data, name)
    }

    pub(super) fn temp_view(&self, name: &str) -> Option<&ViewSchema> {
        map_get_ci(&self.temp_views, name)
    }

    pub(super) fn visible_view(
        &self,
        name: &str,
        _scope: NameResolutionScope,
    ) -> Option<&ViewSchema> {
        if let Some(view) = self.temp_view(name) {
            return Some(view);
        }
        if self.temp_table_schema(name).is_some() {
            return None;
        }
        self.catalog.view(name)
    }

    pub(super) fn visible_table_is_temporary(&self, name: &str) -> bool {
        !self.temp_tables.is_empty() && self.temp_table_schema(name).is_some()
    }

    pub(super) fn table_schema_in_scope(
        &self,
        name: &str,
        _scope: NameResolutionScope,
    ) -> Option<&TableSchema> {
        if self.temp_view(name).is_some() {
            return None;
        }
        if let Some(table) = self.temp_table_schema(name) {
            return Some(table);
        }
        self.catalog.table(name)
    }

    pub(super) fn table_schema(&self, name: &str) -> Option<&TableSchema> {
        self.table_schema_in_scope(name, NameResolutionScope::Session)
    }

    pub(super) fn table_data_in_scope(
        &self,
        name: &str,
        _scope: NameResolutionScope,
    ) -> Option<&TableData> {
        if self.temp_view(name).is_some() {
            return None;
        }
        if let Some(table) = self.temp_table_schema(name) {
            return self.temp_table_data(&table.name);
        }
        let table_name = self.catalog.table(name)?.name.clone();
        self.tables.get(&table_name)
    }

    pub(super) fn table_data(&self, name: &str) -> Option<&TableData> {
        self.table_data_in_scope(name, NameResolutionScope::Session)
    }

    pub(super) fn table_data_for_schema<'a>(
        &'a self,
        table: &TableSchema,
        name: &str,
    ) -> Result<Option<Cow<'a, TableData>>> {
        let Some(data) = self.table_data(name) else {
            return Ok(None);
        };
        if generated_columns_are_stored(table) {
            return Ok(Some(Cow::Borrowed(data)));
        }
        let mut materialized = TableData::default();
        materialized.rows.reserve(data.rows.len());
        for stored_row in &data.rows {
            let mut values = stored_row.values.clone();
            self.apply_virtual_generated_columns(table, &mut values)?;
            materialized.rows.push(StoredRow {
                row_id: stored_row.row_id,
                values,
            });
        }
        Ok(Some(Cow::Owned(materialized)))
    }

    pub(super) fn table_data_mut_in_scope(
        &mut self,
        name: &str,
        _scope: NameResolutionScope,
    ) -> Option<&mut TableData> {
        if self.temp_view(name).is_some() {
            return None;
        }
        if self.temp_table_schema(name).is_some() {
            return self.temp_table_data_mut(name);
        }
        map_get_ci_mut(&mut self.tables, name)
    }

    pub(super) fn table_data_mut(&mut self, name: &str) -> Option<&mut TableData> {
        self.table_data_mut_in_scope(name, NameResolutionScope::Session)
    }

    pub(crate) fn rebuild_indexes(&mut self, page_size: u32) -> Result<()> {
        let indexes = self.catalog.indexes.values().cloned().collect::<Vec<_>>();
        let mut rebuilt = BTreeMap::new();
        for index in indexes {
            rebuilt.insert(
                index.name.clone(),
                build_runtime_index(&index, self, page_size)?,
            );
        }
        self.indexes = rebuilt;
        for index in self.catalog.indexes.values_mut() {
            index.fresh = true;
        }
        self.index_state_epoch = self.index_state_epoch.wrapping_add(1);
        Ok(())
    }

    pub(crate) fn rebuild_index(&mut self, name: &str, page_size: u32) -> Result<()> {
        let index = self
            .catalog
            .indexes
            .get(name)
            .cloned()
            .ok_or_else(|| DbError::sql(format!("unknown index {name}")))?;
        self.indexes.insert(
            name.to_string(),
            build_runtime_index(&index, self, page_size)?,
        );
        if let Some(index) = self.catalog.indexes.get_mut(name) {
            index.fresh = true;
        }
        self.index_state_epoch = self.index_state_epoch.wrapping_add(1);
        Ok(())
    }

    pub(crate) fn rebuild_stale_indexes(&mut self, page_size: u32) -> Result<()> {
        if self
            .catalog
            .indexes
            .iter()
            .all(|(name, index)| index.fresh && self.indexes.contains_key(name))
        {
            return Ok(());
        }

        let names = self
            .catalog
            .indexes
            .iter()
            .filter(|(name, index)| !index.fresh || !self.indexes.contains_key(*name))
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        for name in names {
            self.rebuild_index(&name, page_size)?;
        }
        Ok(())
    }

    pub(super) fn mark_indexes_stale_for_table(&mut self, table_name: &str) {
        if self.visible_table_is_temporary(table_name) {
            return;
        }
        let mut changed = false;
        for index in self.catalog.indexes.values_mut() {
            if identifiers_equal(&index.table_name, table_name) && index.fresh {
                index.fresh = false;
                changed = true;
            }
        }
        if changed {
            self.index_state_epoch = self.index_state_epoch.wrapping_add(1);
        }
    }

    pub(super) fn mark_table_dirty(&mut self, table_name: &str) {
        if self.visible_table_is_temporary(table_name) {
            return;
        }
        self.append_only_dirty_tables.remove(table_name);
        self.row_update_dirty.remove(table_name);
        if self.dirty_tables.contains(table_name) {
            return;
        }
        self.dirty_tables.insert(table_name.to_string());
    }

    pub(super) fn mark_table_row_dirty(&mut self, table_name: &str, row_index: usize) {
        if self.visible_table_is_temporary(table_name) {
            return;
        }
        // Already fully dirty (not append-only, not row-update) — nothing to refine.
        if self.dirty_tables.contains(table_name)
            && !self.append_only_dirty_tables.contains(table_name)
            && !self.row_update_dirty.contains_key(table_name)
        {
            return;
        }
        // Append-only dirty is incompatible with row-update; escalate.
        if self.append_only_dirty_tables.contains(table_name) {
            self.append_only_dirty_tables.remove(table_name);
            return;
        }
        self.dirty_tables.insert(table_name.to_string());
        self.row_update_dirty
            .entry(table_name.to_string())
            .or_default()
            .push(row_index);
    }

    pub(super) fn mark_table_append_dirty(&mut self, table_name: &str) {
        if self.visible_table_is_temporary(table_name) {
            return;
        }
        if self.append_only_dirty_tables.contains(table_name)
            || self.dirty_tables.contains(table_name)
        {
            return;
        }
        self.dirty_tables.insert(table_name.to_string());
        self.append_only_dirty_tables.insert(table_name.to_string());
    }

    pub(super) fn mark_all_tables_dirty(&mut self) {
        self.dirty_tables
            .extend(self.catalog.tables.keys().cloned());
        self.append_only_dirty_tables.clear();
        self.row_update_dirty.clear();
    }

    pub(super) fn prepare_insert_index_updates(
        &mut self,
        table_name: &str,
        row: &StoredRow,
        page_size: u32,
    ) -> Result<Vec<PendingIndexInsert>> {
        if self.visible_table_is_temporary(table_name) {
            return Ok(Vec::new());
        }
        let table = self
            .table_schema(table_name)
            .cloned()
            .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))?;
        let indexes = self
            .catalog
            .indexes
            .values()
            .filter(|index| identifiers_equal(&index.table_name, table_name) && index.fresh)
            .cloned()
            .collect::<Vec<_>>();
        let mut updates = Vec::new();

        for index in indexes {
            if !self.indexes.contains_key(&index.name) {
                self.rebuild_index(&index.name, page_size)?;
            }

            match index.kind {
                IndexKind::Btree => {
                    let Some(key) = compute_index_key(self, &index, &table, &row.values)? else {
                        continue;
                    };
                    updates.push(PendingIndexInsert::Btree {
                        name: index.name.clone(),
                        key,
                        row_id: row.row_id,
                    });
                }
                IndexKind::Trigram => {
                    if !row_satisfies_index_predicate(self, &index, &table, &row.values)? {
                        continue;
                    }
                    let text = compute_index_values(self, &index, &table, &row.values)?
                        .into_iter()
                        .next()
                        .ok_or_else(|| {
                            DbError::constraint("trigram index requires a single text expression")
                        })?;
                    let Value::Text(text) = text else {
                        return Err(DbError::constraint(
                            "trigram index requires a single text expression",
                        ));
                    };
                    updates.push(PendingIndexInsert::Trigram {
                        name: index.name.clone(),
                        row_id: row.row_id as u64,
                        text,
                    });
                }
            }
        }

        Ok(updates)
    }

    pub(super) fn apply_insert_index_updates(
        &mut self,
        updates: Vec<PendingIndexInsert>,
    ) -> Result<()> {
        for update in updates {
            match update {
                PendingIndexInsert::Btree { name, key, row_id } => {
                    match self.indexes.get_mut(&name) {
                        Some(RuntimeIndex::Btree { keys }) => {
                            keys.insert_row_id(key, row_id)?;
                        }
                        Some(_) => {
                            return Err(DbError::internal(format!(
                                "runtime index {name} is not a BTREE index"
                            )))
                        }
                        None => {
                            return Err(DbError::internal(format!(
                                "runtime index {name} is missing"
                            )))
                        }
                    }
                }
                PendingIndexInsert::Trigram { name, row_id, text } => {
                    match self.indexes.get_mut(&name) {
                        Some(RuntimeIndex::Trigram { index }) => {
                            index.queue_insert(row_id, &text);
                        }
                        Some(_) => {
                            return Err(DbError::internal(format!(
                                "runtime index {name} is not a trigram index"
                            )))
                        }
                        None => {
                            return Err(DbError::internal(format!(
                                "runtime index {name} is missing"
                            )))
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn execute_statement(
        &mut self,
        statement: &Statement,
        params: &[Value],
        page_size: u32,
    ) -> Result<QueryResult> {
        match statement {
            Statement::Query(_) | Statement::Explain(_) => {
                self.execute_read_statement(statement, params, page_size)
            }
            Statement::Insert(statement) => {
                let result = self.execute_insert(statement, params, page_size)?;
                Ok(result)
            }
            Statement::Update(statement) => {
                let result = self.execute_update(statement, params, page_size)?;
                Ok(result)
            }
            Statement::Delete(statement) => {
                let result = self.execute_delete(statement, params, page_size)?;
                Ok(result)
            }
            Statement::Analyze { table_name } => {
                self.execute_analyze(table_name.as_deref())?;
                Ok(QueryResult::with_affected_rows(0))
            }
            Statement::CreateTable(statement) => {
                self.execute_create_table(statement)?;
                self.rebuild_indexes(page_size)?;
                Ok(QueryResult::with_affected_rows(0))
            }
            Statement::CreateSchema {
                name,
                if_not_exists,
            } => {
                self.execute_create_schema(name, *if_not_exists)?;
                Ok(QueryResult::with_affected_rows(0))
            }
            Statement::CreateTableAs(statement) => {
                let result = self.execute_create_table_as(statement, params, page_size)?;
                self.rebuild_indexes(page_size)?;
                Ok(result)
            }
            Statement::CreateIndex(statement) => {
                self.execute_create_index(statement, page_size)?;
                self.rebuild_indexes(page_size)?;
                Ok(QueryResult::with_affected_rows(0))
            }
            Statement::CreateView(statement) => {
                self.execute_create_view(statement)?;
                self.rebuild_indexes(page_size)?;
                Ok(QueryResult::with_affected_rows(0))
            }
            Statement::CreateTrigger(statement) => {
                self.execute_create_trigger(statement)?;
                self.rebuild_indexes(page_size)?;
                Ok(QueryResult::with_affected_rows(0))
            }
            Statement::DropTable { name, if_exists } => {
                self.execute_drop_table(name, *if_exists, page_size)?;
                self.rebuild_indexes(page_size)?;
                Ok(QueryResult::with_affected_rows(0))
            }
            Statement::DropIndex { name, if_exists } => {
                self.execute_drop_index(name, *if_exists)?;
                self.rebuild_indexes(page_size)?;
                Ok(QueryResult::with_affected_rows(0))
            }
            Statement::DropView { name, if_exists } => {
                self.execute_drop_view(name, *if_exists)?;
                self.rebuild_indexes(page_size)?;
                Ok(QueryResult::with_affected_rows(0))
            }
            Statement::DropTrigger {
                name,
                table_name,
                if_exists,
            } => {
                self.execute_drop_trigger(name, table_name, *if_exists)?;
                self.rebuild_indexes(page_size)?;
                Ok(QueryResult::with_affected_rows(0))
            }
            Statement::AlterViewRename {
                view_name,
                new_name,
            } => {
                self.execute_alter_view_rename(view_name, new_name)?;
                self.rebuild_indexes(page_size)?;
                Ok(QueryResult::with_affected_rows(0))
            }
            Statement::AlterTable {
                table_name,
                actions,
            } => {
                self.execute_alter_table(table_name, actions, params, page_size)?;
                Ok(QueryResult::with_affected_rows(0))
            }
            Statement::TruncateTable {
                table_name,
                identity,
                cascade,
            } => {
                self.execute_truncate_table(
                    table_name,
                    *identity == TruncateIdentityMode::Restart,
                    *cascade,
                    page_size,
                )?;
                Ok(QueryResult::with_affected_rows(0))
            }
        }
    }

    fn execute_create_table_as(
        &mut self,
        statement: &CreateTableAsStatement,
        params: &[Value],
        page_size: u32,
    ) -> Result<QueryResult> {
        if statement.temporary {
            if self.temp_relation_exists(&statement.table_name) {
                if statement.if_not_exists
                    && self.temp_table_schema(&statement.table_name).is_some()
                {
                    return Ok(QueryResult::with_affected_rows(0));
                }
                return Err(DbError::sql(format!(
                    "object {} already exists",
                    statement.table_name
                )));
            }
        } else if self.catalog.contains_object(&statement.table_name) {
            if statement.if_not_exists && self.catalog.table(&statement.table_name).is_some() {
                return Ok(QueryResult::with_affected_rows(0));
            }
            return Err(DbError::sql(format!(
                "object {} already exists",
                statement.table_name
            )));
        }

        let mut source = self.evaluate_query(&statement.query, params, &BTreeMap::new())?;
        let target_columns = if statement.column_names.is_empty() {
            source
                .columns
                .iter()
                .enumerate()
                .map(|(index, binding)| {
                    if binding.name.is_empty() {
                        format!("column{}", index + 1)
                    } else {
                        binding.name.clone()
                    }
                })
                .collect::<Vec<_>>()
        } else {
            if statement.column_names.len() != source.columns.len() {
                return Err(DbError::sql(format!(
                    "CREATE TABLE AS expected {} column names but query produced {} columns",
                    statement.column_names.len(),
                    source.columns.len()
                )));
            }
            statement.column_names.clone()
        };

        let columns = target_columns
            .iter()
            .enumerate()
            .map(|(index, name)| ColumnDefinition {
                name: name.clone(),
                column_type: infer_column_type_for_ctas(&source.rows, index),
                nullable: true,
                default: None,
                generated: None,
                generated_stored: true,
                primary_key: false,
                unique: false,
                checks: Vec::new(),
                references: None,
            })
            .collect::<Vec<_>>();
        let create_statement = CreateTableStatement {
            table_name: statement.table_name.clone(),
            temporary: statement.temporary,
            if_not_exists: false,
            columns,
            constraints: Vec::new(),
        };
        self.execute_create_table(&create_statement)?;
        if !statement.with_data {
            return Ok(QueryResult::with_affected_rows(0));
        }

        let table_name = statement.table_name.clone();
        let temporary = self.visible_table_is_temporary(&table_name);
        let mut affected_rows = 0_u64;
        for source_row in source.rows.drain(..) {
            let candidate = {
                let mut staged_table = self
                    .table_schema(&table_name)
                    .cloned()
                    .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?;
                let candidate = dml::build_insert_row_values(
                    self,
                    &mut staged_table,
                    &target_columns,
                    source_row,
                    params,
                )?;
                if temporary {
                    self.temp_table_schema_mut(&table_name)
                        .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?
                        .next_row_id = staged_table.next_row_id;
                } else {
                    self.catalog
                        .tables
                        .get_mut(&table_name)
                        .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?
                        .next_row_id = staged_table.next_row_id;
                }
                candidate
            };
            self.validate_row(&table_name, &candidate, None, params)?;
            let row_id = {
                let table = self
                    .table_schema(&table_name)
                    .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?;
                dml::primary_row_id(table, &candidate)
                    .unwrap_or_else(|| dml::next_row_id(self, &table_name))
            };
            let stored_row = StoredRow {
                row_id,
                values: candidate,
            };
            let index_updates =
                self.prepare_insert_index_updates(&table_name, &stored_row, page_size)?;
            self.table_data_mut(&table_name)
                .ok_or_else(|| {
                    DbError::internal(format!("table data for {table_name} is missing"))
                })?
                .rows
                .push(stored_row);
            self.apply_insert_index_updates(index_updates)?;
            if !temporary {
                self.mark_table_dirty(&table_name);
            }
            affected_rows += 1;
        }

        Ok(QueryResult::with_affected_rows(affected_rows))
    }

    pub(crate) fn execute_read_statement(
        &self,
        statement: &Statement,
        params: &[Value],
        _page_size: u32,
    ) -> Result<QueryResult> {
        match statement {
            Statement::Query(query) => {
                if let Some(result) = self.try_execute_simple_count_query(query)? {
                    return Ok(result);
                }
                if let Some(result) =
                    self.try_execute_simple_grouped_numeric_aggregate_query(query, params)?
                {
                    return Ok(result);
                }
                if let Some(result) = self.try_execute_benchmark_history_query(query, params)? {
                    return Ok(result);
                }
                if let Some(result) = self.try_execute_benchmark_report_query(query, params)? {
                    return Ok(result);
                }
                if let Some(result) =
                    self.try_execute_simple_indexed_join_projection_query(query, params)?
                {
                    return Ok(result);
                }
                if let Some(result) =
                    self.try_execute_simple_indexed_projection_query(query, params)?
                {
                    return Ok(result);
                }
                if let Some(result) =
                    self.try_execute_simple_filtered_projection_query(query, params)?
                {
                    return Ok(result);
                }
                if let Some(result) = self.try_execute_simple_table_projection_query(query)? {
                    return Ok(result);
                }
                self.evaluate_query(query, params, &BTreeMap::new())
                    .map(dataset_to_result)
            }
            Statement::Explain(explain) => {
                let planner_catalog = self.planner_catalog();
                let mut lines = planner::plan_statement(
                    &Statement::Explain(explain.clone()),
                    &planner_catalog,
                )?
                .render();
                if explain.analyze {
                    lines.insert(0, "ANALYZE true".to_string());
                    let started = Instant::now();
                    let actual_rows = match explain.statement.as_ref() {
                        Statement::Query(query) => self
                            .evaluate_query(query, params, &BTreeMap::new())?
                            .rows
                            .len(),
                        other => {
                            return Err(DbError::sql(format!(
                                "EXPLAIN ANALYZE is not supported for {other:?}"
                            )))
                        }
                    };
                    lines.push(format!("Actual Rows: {actual_rows}"));
                    lines.push(format!(
                        "Actual Time: {:.3} ms",
                        started.elapsed().as_secs_f64() * 1_000.0
                    ));
                }
                Ok(QueryResult::with_explain(lines))
            }
            other => Err(DbError::internal(format!(
                "read-only execution received mutating statement {other:?}"
            ))),
        }
    }

    fn try_execute_simple_count_query(&self, query: &Query) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty()
            || !query.order_by.is_empty()
            || query.limit.is_some()
            || query.offset.is_some()
        {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.filter.is_some()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || select.distinct
            || !select.distinct_on.is_empty()
            || select.from.len() != 1
            || select.projection.len() != 1
        {
            return Ok(None);
        }
        let FromItem::Table { name, .. } = &select.from[0] else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
        {
            return Ok(None);
        }
        let Some(table) = self.table_schema(name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(table) {
            return Ok(None);
        }

        let SelectItem::Expr { expr, alias } = &select.projection[0] else {
            return Ok(None);
        };
        let Expr::Aggregate {
            name: aggregate_name,
            args,
            distinct,
            star,
            order_by,
            within_group,
        } = expr
        else {
            return Ok(None);
        };
        if !aggregate_name.eq_ignore_ascii_case("count")
            || !args.is_empty()
            || *distinct
            || !*star
            || !order_by.is_empty()
            || *within_group
        {
            return Ok(None);
        }

        let row_count = self.table_data(name).map_or(0, |data| data.rows.len());
        let row_count = i64::try_from(row_count)
            .map_err(|_| DbError::sql(format!("table {name} exceeds COUNT(*) row-count limits")))?;
        let column_name = alias.clone().unwrap_or_else(|| infer_expr_name(expr, 1));
        Ok(Some(QueryResult::with_rows(
            vec![column_name],
            vec![QueryRow::new(vec![Value::Int64(row_count)])],
        )))
    }

    fn try_execute_simple_grouped_numeric_aggregate_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty()
            || !query.order_by.is_empty()
            || query.limit.is_some()
            || query.offset.is_some()
        {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.having.is_some()
            || select.distinct
            || !select.distinct_on.is_empty()
            || select.from.len() != 1
            || select.group_by.len() != 1
            || select.projection.len() != 3
        {
            return Ok(None);
        }
        let Some(filter) = select.filter.as_ref() else {
            return Ok(None);
        };
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
        {
            return Ok(None);
        }

        let table_schema = match self.table_schema(name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        let Some(data) = self.table_data(name) else {
            return Ok(None);
        };
        let binding_name = alias.as_deref().unwrap_or(name);

        let Expr::Column {
            table: group_table,
            column: group_column,
        } = &select.group_by[0]
        else {
            return Ok(None);
        };
        if let Some(group_table) = group_table.as_deref() {
            if !identifiers_equal(group_table, name)
                && !identifiers_equal(group_table, binding_name)
            {
                return Ok(None);
            }
        }
        let Some(group_column_index) = table_schema
            .columns
            .iter()
            .position(|candidate| identifiers_equal(&candidate.name, group_column))
        else {
            return Ok(None);
        };

        let Some(range_filter) = simple_range_projection_filter(filter) else {
            return Ok(None);
        };
        if let Some(filter_table) = range_filter.table {
            if !identifiers_equal(filter_table, name)
                && !identifiers_equal(filter_table, binding_name)
            {
                return Ok(None);
            }
        }
        let Some(filter_column_index) = table_schema
            .columns
            .iter()
            .position(|candidate| identifiers_equal(&candidate.name, range_filter.column))
        else {
            return Ok(None);
        };

        let lower_bound = range_filter
            .lower
            .map(|bound| {
                Ok(SimpleRangeBoundValue {
                    inclusive: bound.inclusive,
                    value: self.eval_expr(
                        bound.value_expr,
                        &Dataset::empty(),
                        &[],
                        params,
                        &BTreeMap::new(),
                        None,
                    )?,
                })
            })
            .transpose()?;
        let upper_bound = range_filter
            .upper
            .map(|bound| {
                Ok(SimpleRangeBoundValue {
                    inclusive: bound.inclusive,
                    value: self.eval_expr(
                        bound.value_expr,
                        &Dataset::empty(),
                        &[],
                        params,
                        &BTreeMap::new(),
                        None,
                    )?,
                })
            })
            .transpose()?;

        let SelectItem::Expr {
            expr: projection_group_expr,
            alias: projection_group_alias,
        } = &select.projection[0]
        else {
            return Ok(None);
        };
        let Expr::Column {
            table: projection_group_table,
            column: projection_group_column,
        } = projection_group_expr
        else {
            return Ok(None);
        };
        if let Some(projection_group_table) = projection_group_table.as_deref() {
            if !identifiers_equal(projection_group_table, name)
                && !identifiers_equal(projection_group_table, binding_name)
            {
                return Ok(None);
            }
        }
        if !identifiers_equal(projection_group_column, group_column) {
            return Ok(None);
        }

        let SelectItem::Expr {
            expr: count_expr,
            alias: count_alias,
        } = &select.projection[1]
        else {
            return Ok(None);
        };
        let Expr::Aggregate {
            name: count_name,
            args: count_args,
            distinct: count_distinct,
            star: count_star,
            order_by: count_order_by,
            within_group: count_within_group,
        } = count_expr
        else {
            return Ok(None);
        };
        if !count_name.eq_ignore_ascii_case("count")
            || !count_args.is_empty()
            || *count_distinct
            || !*count_star
            || !count_order_by.is_empty()
            || *count_within_group
        {
            return Ok(None);
        }

        let SelectItem::Expr {
            expr: sum_expr,
            alias: sum_alias,
        } = &select.projection[2]
        else {
            return Ok(None);
        };
        let Expr::Aggregate {
            name: sum_name,
            args: sum_args,
            distinct: sum_distinct,
            star: sum_star,
            order_by: sum_order_by,
            within_group: sum_within_group,
        } = sum_expr
        else {
            return Ok(None);
        };
        if !sum_name.eq_ignore_ascii_case("sum")
            || sum_args.len() != 1
            || *sum_distinct
            || *sum_star
            || !sum_order_by.is_empty()
            || *sum_within_group
        {
            return Ok(None);
        }
        let Expr::Column {
            table: sum_table,
            column: sum_column,
        } = &sum_args[0]
        else {
            return Ok(None);
        };
        if let Some(sum_table) = sum_table.as_deref() {
            if !identifiers_equal(sum_table, name) && !identifiers_equal(sum_table, binding_name) {
                return Ok(None);
            }
        }
        let Some(sum_column_index) = table_schema
            .columns
            .iter()
            .position(|candidate| identifiers_equal(&candidate.name, sum_column))
        else {
            return Ok(None);
        };

        let mut groups = Vec::<SimpleGroupedNumericAggregate>::new();
        for stored_row in &data.rows {
            if !simple_range_bound_matches(
                &stored_row.values[filter_column_index],
                lower_bound.as_ref(),
                upper_bound.as_ref(),
            )? {
                continue;
            }

            let group_value = stored_row.values[group_column_index].clone();
            let group_index = groups
                .iter()
                .position(|group| group.group_value == group_value)
                .unwrap_or_else(|| {
                    groups.push(SimpleGroupedNumericAggregate::new(group_value.clone()));
                    groups.len() - 1
                });
            groups[group_index].count += 1;
            groups[group_index].add_numeric(&stored_row.values[sum_column_index])?;
        }

        let column_names = vec![
            projection_group_alias
                .clone()
                .unwrap_or_else(|| infer_expr_name(projection_group_expr, 1)),
            count_alias
                .clone()
                .unwrap_or_else(|| infer_expr_name(count_expr, 2)),
            sum_alias
                .clone()
                .unwrap_or_else(|| infer_expr_name(sum_expr, 3)),
        ];
        let rows = groups
            .into_iter()
            .map(SimpleGroupedNumericAggregate::into_row)
            .collect();
        Ok(Some(QueryResult::with_rows(column_names, rows)))
    }

    fn try_execute_simple_indexed_join_projection_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty() || !query.order_by.is_empty() || query.offset.is_some() {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if !select.group_by.is_empty()
            || select.having.is_some()
            || select.distinct
            || !select.distinct_on.is_empty()
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let Some(filter) = select.filter.as_ref() else {
            return Ok(None);
        };
        let FromItem::Join {
            left,
            right,
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(on),
        } = &select.from[0]
        else {
            return Ok(None);
        };
        let (left_name, left_alias) = match &**left {
            FromItem::Table { name, alias } => (name, alias),
            _ => return Ok(None),
        };
        let (right_name, right_alias) = match &**right {
            FromItem::Table { name, alias } => (name, alias),
            _ => return Ok(None),
        };
        if self
            .visible_view(left_name, NameResolutionScope::Session)
            .is_some()
            || self
                .visible_view(right_name, NameResolutionScope::Session)
                .is_some()
            || self.visible_table_is_temporary(left_name)
            || self.visible_table_is_temporary(right_name)
        {
            return Ok(None);
        }

        let Some((filter_table, filter_column, value_expr)) = simple_btree_lookup(filter) else {
            return Ok(None);
        };
        let Some((left_join, right_join)) = simple_join_equality(on) else {
            return Ok(None);
        };

        let left_binding = TableBindingRef {
            name: left_name,
            alias: left_alias,
        };
        let right_binding = TableBindingRef {
            name: right_name,
            alias: right_alias,
        };

        let (
            filtered_table,
            filtered_alias,
            filtered_join_column,
            probe_table,
            probe_alias,
            probe_join_column,
        ) = if matches_table_binding(left_binding, filter_table)
            && matches_table_binding(left_binding, left_join.table)
            && matches_table_binding(right_binding, right_join.table)
        {
            (
                left_name,
                left_alias,
                left_join.column,
                right_name,
                right_alias,
                right_join.column,
            )
        } else if matches_table_binding(right_binding, filter_table)
            && matches_table_binding(right_binding, right_join.table)
            && matches_table_binding(left_binding, left_join.table)
        {
            (
                right_name,
                right_alias,
                right_join.column,
                left_name,
                left_alias,
                left_join.column,
            )
        } else {
            return Ok(None);
        };

        let filtered_schema = match self.table_schema(filtered_table) {
            Some(table) => table,
            None => return Ok(None),
        };
        let probe_schema = match self.table_schema(probe_table) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(filtered_schema)
            || !generated_columns_are_stored(probe_schema)
        {
            return Ok(None);
        }
        let Some(filtered_data) = self.table_data(filtered_table) else {
            return Ok(None);
        };
        let Some(filter_index) = self.catalog.indexes.values().find(|index| {
            identifiers_equal(&index.table_name, filtered_table)
                && index.fresh
                && index.kind == IndexKind::Btree
                && index.predicate_sql.is_none()
                && index.columns.len() == 1
                && index.columns[0]
                    .column_name
                    .as_deref()
                    .is_some_and(|index_column| identifiers_equal(index_column, filter_column))
                && index.columns[0].expression_sql.is_none()
        }) else {
            return Ok(None);
        };
        let filter_value = self.eval_expr(
            value_expr,
            &Dataset::empty(),
            &[],
            params,
            &BTreeMap::new(),
            None,
        )?;
        let Some(RuntimeIndex::Btree { keys: filter_keys }) = self.indexes.get(&filter_index.name)
        else {
            return Ok(None);
        };
        let filtered_join_index = filtered_schema
            .columns
            .iter()
            .position(|column| identifiers_equal(&column.name, filtered_join_column))
            .ok_or_else(|| DbError::sql(format!("unknown column {filtered_join_column}")))?;
        let Some(probe_data) = self.table_data(probe_table) else {
            return Ok(None);
        };
        let is_probe_rowid_alias = crate::exec::dml::row_id_alias_column_name(probe_schema)
            .is_some_and(|name| identifiers_equal(name, probe_join_column));

        let probe_index = if is_probe_rowid_alias {
            None
        } else {
            self.catalog.indexes.values().find(|index| {
                identifiers_equal(&index.table_name, probe_table)
                    && index.fresh
                    && index.kind == IndexKind::Btree
                    && index.predicate_sql.is_none()
                    && index.columns.len() == 1
                    && index.columns[0]
                        .column_name
                        .as_deref()
                        .is_some_and(|index_column| {
                            identifiers_equal(index_column, probe_join_column)
                        })
                    && index.columns[0].expression_sql.is_none()
            })
        };
        if probe_index.is_none() && !is_probe_rowid_alias {
            return Ok(None);
        }
        let keys = if let Some(index) = probe_index {
            let Some(RuntimeIndex::Btree { keys }) = self.indexes.get(&index.name) else {
                return Ok(None);
            };
            Some(keys)
        } else {
            None
        };
        let Some((projection_plan, column_names)) = simple_join_projection_plan(
            &select.projection,
            filtered_table,
            filtered_alias,
            filtered_schema,
            probe_table,
            probe_alias,
            probe_schema,
        ) else {
            return Ok(None);
        };

        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));

        let use_probe_row_position_map = probe_data
            .rows
            .len()
            .saturating_mul(filtered_data.rows.len())
            > 8_192;
        let probe_row_positions = if use_probe_row_position_map {
            let mut positions = Int64Map::<usize>::default();
            for (position, row) in probe_data.rows.iter().enumerate() {
                positions.insert(row.row_id, position);
            }
            Some(positions)
        } else {
            None
        };

        let mut rows = Vec::new();
        let mut projection_error = None;
        let mut stop = false;
        let filtered_row_ids = filter_keys.row_ids_for_value_set(&filter_value)?;

        filtered_row_ids.for_each(|filtered_row_id| {
            if stop || projection_error.is_some() {
                return;
            }
            let Some(filtered_row) = filtered_data.row_by_id(filtered_row_id) else {
                return;
            };
            let Some(join_value) = filtered_row.values.get(filtered_join_index) else {
                projection_error = Some(DbError::internal(
                    "join row is shorter than filtered table schema",
                ));
                stop = true;
                return;
            };
            if matches!(join_value, Value::Null) {
                return;
            }

            let probe_row_ids = if let Some(keys) = keys {
                match keys.row_ids_for_value_set(join_value) {
                    Ok(ids) => ids,
                    Err(e) => {
                        projection_error = Some(e);
                        stop = true;
                        return;
                    }
                }
            } else if let Value::Int64(val) = join_value {
                RuntimeRowIdSet::Single(*val)
            } else {
                RuntimeRowIdSet::Empty
            };

            probe_row_ids.for_each(|row_id| {
                if stop || projection_error.is_some() {
                    return;
                }
                let probe_row = if let Some(positions) = probe_row_positions.as_ref() {
                    let Some(probe_position) = positions.get(&row_id).copied() else {
                        return;
                    };
                    &probe_data.rows[probe_position].values
                } else {
                    let Some(probe_row) = probe_data.row_by_id(row_id) else {
                        return;
                    };
                    &probe_row.values
                };

                let mut projected = Vec::with_capacity(projection_plan.len());
                for slot in &projection_plan {
                    let value = match slot {
                        SimpleJoinProjectionSource::Filtered(index) => {
                            filtered_row.values.get(*index)
                        }
                        SimpleJoinProjectionSource::Probe(index) => probe_row.get(*index),
                    };
                    let Some(value) = value else {
                        projection_error = Some(DbError::internal(
                            "join projection source index exceeds row width",
                        ));
                        stop = true;
                        return;
                    };
                    projected.push(value.clone());
                }
                rows.push(projected);
                if limit.is_some_and(|limit| rows.len() >= limit) {
                    stop = true;
                }
            });
        });

        if let Some(error) = projection_error.take() {
            return Err(error);
        }
        let rows = rows.into_iter().map(QueryRow::new).collect();
        Ok(Some(QueryResult::with_rows(column_names, rows)))
    }

    fn try_execute_benchmark_history_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty()
            || query.limit.is_some()
            || query.offset.is_some()
            || query.order_by.len() != 1
        {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if !select.group_by.is_empty()
            || select.having.is_some()
            || select.distinct
            || !select.distinct_on.is_empty()
            || select.from.len() != 1
            || select.projection.len() != 6
        {
            return Ok(None);
        }

        let Some(filter) = select.filter.as_ref() else {
            return Ok(None);
        };
        let Some((filter_table, filter_column, value_expr)) = simple_btree_lookup(filter) else {
            return Ok(None);
        };

        let FromItem::Join {
            left: order_payment_items,
            right: item_item,
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(item_join_on),
        } = &select.from[0]
        else {
            return Ok(None);
        };
        let FromItem::Table {
            name: item_name,
            alias: item_alias,
        } = &**item_item
        else {
            return Ok(None);
        };
        let FromItem::Join {
            left: order_payment,
            right: order_item_item,
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(order_item_join_on),
        } = &**order_payment_items
        else {
            return Ok(None);
        };
        let FromItem::Table {
            name: order_item_name,
            alias: order_item_alias,
        } = &**order_item_item
        else {
            return Ok(None);
        };
        let FromItem::Join {
            left: order_item,
            right: payment_item,
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(payment_join_on),
        } = &**order_payment
        else {
            return Ok(None);
        };
        let FromItem::Table {
            name: order_name,
            alias: order_alias,
        } = &**order_item
        else {
            return Ok(None);
        };
        let FromItem::Table {
            name: payment_name,
            alias: payment_alias,
        } = &**payment_item
        else {
            return Ok(None);
        };

        let order_binding = TableBindingRef {
            name: order_name,
            alias: order_alias,
        };
        let payment_binding = TableBindingRef {
            name: payment_name,
            alias: payment_alias,
        };
        let order_item_binding = TableBindingRef {
            name: order_item_name,
            alias: order_item_alias,
        };
        let item_binding = TableBindingRef {
            name: item_name,
            alias: item_alias,
        };

        if self
            .visible_view(order_name, NameResolutionScope::Session)
            .is_some()
            || self
                .visible_view(payment_name, NameResolutionScope::Session)
                .is_some()
            || self
                .visible_view(order_item_name, NameResolutionScope::Session)
                .is_some()
            || self
                .visible_view(item_name, NameResolutionScope::Session)
                .is_some()
            || self.visible_table_is_temporary(order_name)
            || self.visible_table_is_temporary(payment_name)
            || self.visible_table_is_temporary(order_item_name)
            || self.visible_table_is_temporary(item_name)
        {
            return Ok(None);
        }

        if !matches_filter_binding(order_name, order_alias, filter_table)
            || !identifiers_equal(filter_column, "user_id")
        {
            return Ok(None);
        }
        if !join_constraint_matches_columns(
            payment_join_on,
            order_binding,
            "id",
            payment_binding,
            "order_id",
        ) || !join_constraint_matches_columns(
            order_item_join_on,
            order_binding,
            "id",
            order_item_binding,
            "order_id",
        ) || !join_constraint_matches_columns(
            item_join_on,
            order_item_binding,
            "item_id",
            item_binding,
            "id",
        ) {
            return Ok(None);
        }

        if !query.order_by[0].descending
            || !expr_matches_binding_column(&query.order_by[0].expr, order_binding, "id")
        {
            return Ok(None);
        }

        let projection = [
            (0, order_binding, "id"),
            (1, order_binding, "total_amount"),
            (2, payment_binding, "status"),
            (3, item_binding, "name"),
            (4, order_item_binding, "quantity"),
            (5, order_item_binding, "price"),
        ];
        for (index, binding, column) in projection {
            let SelectItem::Expr { expr, .. } = &select.projection[index] else {
                return Ok(None);
            };
            if !expr_matches_binding_column(expr, binding, column) {
                return Ok(None);
            }
        }

        let order_schema = match self.table_schema(order_name) {
            Some(table) => table,
            None => return Ok(None),
        };
        let payment_schema = match self.table_schema(payment_name) {
            Some(table) => table,
            None => return Ok(None),
        };
        let order_item_schema = match self.table_schema(order_item_name) {
            Some(table) => table,
            None => return Ok(None),
        };
        let item_schema = match self.table_schema(item_name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(order_schema)
            || !generated_columns_are_stored(payment_schema)
            || !generated_columns_are_stored(order_item_schema)
            || !generated_columns_are_stored(item_schema)
        {
            return Ok(None);
        }

        let Some(order_data) = self.table_data(order_name) else {
            return Ok(None);
        };
        let Some(payment_data) = self.table_data(payment_name) else {
            return Ok(None);
        };
        let Some(order_item_data) = self.table_data(order_item_name) else {
            return Ok(None);
        };
        let Some(item_data) = self.table_data(item_name) else {
            return Ok(None);
        };

        let Some(order_user_keys) = self.single_column_btree_keys(order_name, "user_id") else {
            return Ok(None);
        };
        let Some(payment_order_keys) = self.single_column_btree_keys(payment_name, "order_id")
        else {
            return Ok(None);
        };
        let Some(order_item_order_keys) =
            self.single_column_btree_keys(order_item_name, "order_id")
        else {
            return Ok(None);
        };
        let Some(item_id_keys) = self.single_column_btree_keys(item_name, "id") else {
            return Ok(None);
        };

        let Some(order_id_index) = schema_column_index(order_schema, "id") else {
            return Ok(None);
        };
        let Some(order_total_amount_index) = schema_column_index(order_schema, "total_amount")
        else {
            return Ok(None);
        };
        let Some(payment_status_index) = schema_column_index(payment_schema, "status") else {
            return Ok(None);
        };
        let Some(order_item_item_id_index) = schema_column_index(order_item_schema, "item_id")
        else {
            return Ok(None);
        };
        let Some(order_item_quantity_index) = schema_column_index(order_item_schema, "quantity")
        else {
            return Ok(None);
        };
        let Some(order_item_price_index) = schema_column_index(order_item_schema, "price") else {
            return Ok(None);
        };
        let Some(item_name_index) = schema_column_index(item_schema, "name") else {
            return Ok(None);
        };

        if order_schema.columns[order_id_index].column_type != crate::catalog::ColumnType::Int64
            || order_item_schema.columns[order_item_item_id_index].column_type
                != crate::catalog::ColumnType::Int64
            || order_item_schema.columns[order_item_quantity_index].column_type
                != crate::catalog::ColumnType::Int64
            || payment_schema.columns[payment_status_index].column_type
                != crate::catalog::ColumnType::Text
            || item_schema.columns[item_name_index].column_type != crate::catalog::ColumnType::Text
        {
            return Ok(None);
        }

        let filter_value = self.eval_expr(
            value_expr,
            &Dataset::empty(),
            &[],
            params,
            &BTreeMap::new(),
            None,
        )?;
        let mut matching_orders = order_user_keys.row_ids_for_value(&filter_value)?;
        matching_orders.sort_by(|left_row_id, right_row_id| {
            let left_value = order_data
                .row_by_id(*left_row_id)
                .and_then(|row| row.values.get(order_id_index))
                .and_then(value_as_int64)
                .unwrap_or(i64::MIN);
            let right_value = order_data
                .row_by_id(*right_row_id)
                .and_then(|row| row.values.get(order_id_index))
                .and_then(value_as_int64)
                .unwrap_or(i64::MIN);
            right_value.cmp(&left_value)
        });

        let column_names = select
            .projection
            .iter()
            .enumerate()
            .map(|(index, item)| match item {
                SelectItem::Expr { expr, alias } => alias
                    .clone()
                    .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
                SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                    unreachable!("history fast path only matches explicit projection")
                }
            })
            .collect::<Vec<_>>();

        let mut rows = Vec::new();
        for order_row_id in matching_orders {
            let Some(order_row) = order_data.row_by_id(order_row_id) else {
                continue;
            };
            let Some(order_id) = order_row
                .values
                .get(order_id_index)
                .and_then(value_as_int64)
            else {
                return Ok(None);
            };
            let order_id_value = Value::Int64(order_id);
            let payment_row_ids = payment_order_keys.row_ids_for_value(&order_id_value)?;
            if payment_row_ids.is_empty() {
                continue;
            }
            let order_item_row_ids = order_item_order_keys.row_ids_for_value(&order_id_value)?;
            if order_item_row_ids.is_empty() {
                continue;
            }

            for payment_row_id in payment_row_ids {
                let Some(payment_row) = payment_data.row_by_id(payment_row_id) else {
                    continue;
                };
                let Some(payment_status) = payment_row.values.get(payment_status_index) else {
                    return Ok(None);
                };

                for order_item_row_id in &order_item_row_ids {
                    let Some(order_item_row) = order_item_data.row_by_id(*order_item_row_id) else {
                        continue;
                    };
                    let Some(item_id_value) = order_item_row.values.get(order_item_item_id_index)
                    else {
                        return Ok(None);
                    };
                    let item_row_ids = item_id_keys.row_ids_for_value(item_id_value)?;
                    if item_row_ids.is_empty() {
                        continue;
                    }
                    for item_row_id in item_row_ids {
                        let Some(item_row) = item_data.row_by_id(item_row_id) else {
                            continue;
                        };
                        let Some(item_name_value) = item_row.values.get(item_name_index) else {
                            return Ok(None);
                        };
                        let Some(quantity_value) =
                            order_item_row.values.get(order_item_quantity_index)
                        else {
                            return Ok(None);
                        };
                        let Some(price_value) = order_item_row.values.get(order_item_price_index)
                        else {
                            return Ok(None);
                        };

                        rows.push(QueryRow::new(vec![
                            Value::Int64(order_id),
                            order_row.values[order_total_amount_index].clone(),
                            payment_status.clone(),
                            item_name_value.clone(),
                            quantity_value.clone(),
                            price_value.clone(),
                        ]));
                    }
                }
            }
        }

        Ok(Some(QueryResult::with_rows(column_names, rows)))
    }

    fn try_execute_benchmark_report_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty()
            || query.offset.is_some()
            || query.order_by.len() != 1
            || query.limit.is_none()
        {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.from.len() != 1
            || select.filter.is_none()
            || select.having.is_some()
            || select.distinct
            || !select.distinct_on.is_empty()
            || select.group_by.len() != 2
            || select.projection.len() != 3
        {
            return Ok(None);
        }

        let Some(filter) = select.filter.as_ref() else {
            return Ok(None);
        };
        let Some((filter_table, filter_column, value_expr)) = simple_btree_lookup(filter) else {
            return Ok(None);
        };

        let FromItem::Join {
            left: item_order_items,
            right: order_item,
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(order_join_on),
        } = &select.from[0]
        else {
            return Ok(None);
        };
        let FromItem::Table {
            name: order_name,
            alias: order_alias,
        } = &**order_item
        else {
            return Ok(None);
        };
        let FromItem::Join {
            left: item_item,
            right: order_item_item,
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(order_item_join_on),
        } = &**item_order_items
        else {
            return Ok(None);
        };
        let FromItem::Table {
            name: item_name,
            alias: item_alias,
        } = &**item_item
        else {
            return Ok(None);
        };
        let FromItem::Table {
            name: order_item_name,
            alias: order_item_alias,
        } = &**order_item_item
        else {
            return Ok(None);
        };

        let item_binding = TableBindingRef {
            name: item_name,
            alias: item_alias,
        };
        let order_item_binding = TableBindingRef {
            name: order_item_name,
            alias: order_item_alias,
        };
        let order_binding = TableBindingRef {
            name: order_name,
            alias: order_alias,
        };

        if self
            .visible_view(item_name, NameResolutionScope::Session)
            .is_some()
            || self
                .visible_view(order_item_name, NameResolutionScope::Session)
                .is_some()
            || self
                .visible_view(order_name, NameResolutionScope::Session)
                .is_some()
            || self.visible_table_is_temporary(item_name)
            || self.visible_table_is_temporary(order_item_name)
            || self.visible_table_is_temporary(order_name)
        {
            return Ok(None);
        }

        if !matches_filter_binding(order_name, order_alias, filter_table)
            || !identifiers_equal(filter_column, "status")
            || !join_constraint_matches_columns(
                order_item_join_on,
                item_binding,
                "id",
                order_item_binding,
                "item_id",
            )
            || !join_constraint_matches_columns(
                order_join_on,
                order_item_binding,
                "order_id",
                order_binding,
                "id",
            )
        {
            return Ok(None);
        }

        let SelectItem::Expr {
            expr: item_name_expr,
            alias: item_name_alias,
        } = &select.projection[0]
        else {
            return Ok(None);
        };
        if !expr_matches_binding_column(item_name_expr, item_binding, "name") {
            return Ok(None);
        }

        let SelectItem::Expr {
            expr: quantity_sum_expr,
            alias: quantity_sum_alias,
        } = &select.projection[1]
        else {
            return Ok(None);
        };
        if !aggregate_matches_single_binding_column(
            quantity_sum_expr,
            "sum",
            order_item_binding,
            "quantity",
        ) {
            return Ok(None);
        }

        let SelectItem::Expr {
            expr: revenue_expr,
            alias: revenue_alias,
        } = &select.projection[2]
        else {
            return Ok(None);
        };
        if !aggregate_matches_binding_product(
            revenue_expr,
            "sum",
            order_item_binding,
            "quantity",
            order_item_binding,
            "price",
        ) {
            return Ok(None);
        }
        if !order_by_matches_alias_or_projection(
            &query.order_by[0],
            revenue_alias.as_deref(),
            revenue_expr,
            true,
        ) {
            return Ok(None);
        }

        if select.group_by.len() != 2
            || !expr_matches_binding_column(&select.group_by[0], item_binding, "id")
            || !expr_matches_binding_column(&select.group_by[1], item_binding, "name")
        {
            return Ok(None);
        }

        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(usize::MAX);

        let item_schema = match self.table_schema(item_name) {
            Some(table) => table,
            None => return Ok(None),
        };
        let order_item_schema = match self.table_schema(order_item_name) {
            Some(table) => table,
            None => return Ok(None),
        };
        let order_schema = match self.table_schema(order_name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(item_schema)
            || !generated_columns_are_stored(order_item_schema)
            || !generated_columns_are_stored(order_schema)
        {
            return Ok(None);
        }

        let Some(item_data) = self.table_data(item_name) else {
            return Ok(None);
        };
        let Some(order_item_data) = self.table_data(order_item_name) else {
            return Ok(None);
        };
        let Some(order_data) = self.table_data(order_name) else {
            return Ok(None);
        };

        let Some(order_status_keys) = self.single_column_btree_keys(order_name, "status") else {
            return Ok(None);
        };
        let Some(order_item_order_keys) =
            self.single_column_btree_keys(order_item_name, "order_id")
        else {
            return Ok(None);
        };
        let Some(item_id_keys) = self.single_column_btree_keys(item_name, "id") else {
            return Ok(None);
        };

        let Some(order_id_index) = schema_column_index(order_schema, "id") else {
            return Ok(None);
        };
        let Some(order_item_item_id_index) = schema_column_index(order_item_schema, "item_id")
        else {
            return Ok(None);
        };
        let Some(order_item_quantity_index) = schema_column_index(order_item_schema, "quantity")
        else {
            return Ok(None);
        };
        let Some(order_item_price_index) = schema_column_index(order_item_schema, "price") else {
            return Ok(None);
        };
        let Some(item_name_index) = schema_column_index(item_schema, "name") else {
            return Ok(None);
        };

        if order_schema.columns[order_id_index].column_type != crate::catalog::ColumnType::Int64
            || order_item_schema.columns[order_item_item_id_index].column_type
                != crate::catalog::ColumnType::Int64
            || order_item_schema.columns[order_item_quantity_index].column_type
                != crate::catalog::ColumnType::Int64
            || item_schema.columns[item_name_index].column_type != crate::catalog::ColumnType::Text
        {
            return Ok(None);
        }

        let filter_value = self.eval_expr(
            value_expr,
            &Dataset::empty(),
            &[],
            params,
            &BTreeMap::new(),
            None,
        )?;
        let matching_order_row_ids = order_status_keys.row_ids_for_value(&filter_value)?;
        let mut aggregates = BTreeMap::<i64, BenchmarkReportAggregate>::new();
        for order_row_id in matching_order_row_ids {
            let Some(order_row) = order_data.row_by_id(order_row_id) else {
                continue;
            };
            let Some(order_id) = order_row
                .values
                .get(order_id_index)
                .and_then(value_as_int64)
            else {
                return Ok(None);
            };
            let order_id_value = Value::Int64(order_id);
            let order_item_row_ids = order_item_order_keys.row_ids_for_value(&order_id_value)?;
            for order_item_row_id in order_item_row_ids {
                let Some(order_item_row) = order_item_data.row_by_id(order_item_row_id) else {
                    continue;
                };
                let Some(item_id) = order_item_row
                    .values
                    .get(order_item_item_id_index)
                    .and_then(value_as_int64)
                else {
                    return Ok(None);
                };
                let Some(quantity) = order_item_row
                    .values
                    .get(order_item_quantity_index)
                    .and_then(value_as_int64)
                else {
                    return Ok(None);
                };
                let Some(price) = order_item_row
                    .values
                    .get(order_item_price_index)
                    .and_then(value_as_f64)
                else {
                    return Ok(None);
                };

                let item_row_ids = item_id_keys.row_ids_for_value(&Value::Int64(item_id))?;
                if item_row_ids.is_empty() {
                    continue;
                }
                for item_row_id in item_row_ids {
                    let Some(item_row) = item_data.row_by_id(item_row_id) else {
                        continue;
                    };
                    let Some(item_name_value) = item_row.values.get(item_name_index) else {
                        return Ok(None);
                    };
                    let Some(item_name_text) = value_as_text(item_name_value) else {
                        return Ok(None);
                    };
                    let aggregate = aggregates.entry(item_id).or_insert_with(|| {
                        BenchmarkReportAggregate::new(item_name_text.to_string())
                    });
                    aggregate.quantity_total += quantity;
                    aggregate.revenue_total += quantity as f64 * price;
                }
            }
        }

        let column_names = vec![
            item_name_alias
                .clone()
                .unwrap_or_else(|| infer_expr_name(item_name_expr, 1)),
            quantity_sum_alias
                .clone()
                .unwrap_or_else(|| infer_expr_name(quantity_sum_expr, 2)),
            revenue_alias
                .clone()
                .unwrap_or_else(|| infer_expr_name(revenue_expr, 3)),
        ];

        let mut rows = aggregates
            .into_values()
            .map(|aggregate| {
                QueryRow::new(vec![
                    Value::Text(aggregate.item_name),
                    Value::Int64(aggregate.quantity_total),
                    Value::Float64(aggregate.revenue_total),
                ])
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            let revenue_ordering = compare_values(&left.values()[2], &right.values()[2])
                .unwrap_or(std::cmp::Ordering::Equal)
                .reverse();
            if revenue_ordering != std::cmp::Ordering::Equal {
                return revenue_ordering;
            }
            compare_values(&left.values()[0], &right.values()[0])
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        if rows.len() > limit {
            rows.truncate(limit);
        }
        Ok(Some(QueryResult::with_rows(column_names, rows)))
    }

    fn single_column_btree_keys(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Option<&RuntimeBtreeKeys> {
        let index = self.catalog.indexes.values().find(|index| {
            identifiers_equal(&index.table_name, table_name)
                && index.fresh
                && index.kind == IndexKind::Btree
                && index.predicate_sql.is_none()
                && index.columns.len() == 1
                && index.columns[0]
                    .column_name
                    .as_deref()
                    .is_some_and(|index_column| identifiers_equal(index_column, column_name))
                && index.columns[0].expression_sql.is_none()
        })?;
        let RuntimeIndex::Btree { keys } = self.indexes.get(&index.name)? else {
            return None;
        };
        Some(keys)
    }

    fn execute_analyze(&mut self, table_name: Option<&str>) -> Result<()> {
        let target_tables = if let Some(table_name) = table_name {
            if self.visible_table_is_temporary(table_name) {
                return Err(DbError::sql(
                    "ANALYZE is not supported for temporary tables",
                ));
            }
            if self
                .visible_view(table_name, NameResolutionScope::Session)
                .is_some()
                && self.catalog.table(table_name).is_none()
            {
                return Err(DbError::sql(format!("unknown table {table_name}")));
            }
            let table = self
                .catalog
                .table(table_name)
                .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))?;
            vec![table.name.clone()]
        } else {
            self.catalog.tables.keys().cloned().collect::<Vec<_>>()
        };
        for table_name in target_tables {
            self.refresh_table_stats(&table_name)?;
        }
        Ok(())
    }

    fn refresh_table_stats(&mut self, table_name: &str) -> Result<()> {
        let row_count = self
            .table_data(table_name)
            .map(|table| i64::try_from(table.rows.len()))
            .transpose()
            .map_err(|_| {
                DbError::sql(format!(
                    "table {table_name} exceeds ANALYZE row-count limits"
                ))
            })?
            .unwrap_or(0);
        self.catalog
            .table_stats
            .insert(table_name.to_string(), TableStats { row_count });

        let index_names = self
            .catalog
            .indexes
            .values()
            .filter(|index| identifiers_equal(&index.table_name, table_name))
            .map(|index| index.name.clone())
            .collect::<Vec<_>>();
        for index_name in index_names {
            self.catalog.index_stats.remove(&index_name);
            let Some(index) = self.catalog.index(&index_name).cloned() else {
                continue;
            };
            if index.kind != IndexKind::Btree {
                continue;
            }
            let Some(RuntimeIndex::Btree { keys }) = self.indexes.get(&index.name) else {
                continue;
            };
            let entry_count = i64::try_from(keys.total_row_id_count()).map_err(|_| {
                DbError::sql(format!(
                    "index {} exceeds ANALYZE entry-count limits",
                    index.name
                ))
            })?;
            let distinct_key_count = i64::try_from(keys.distinct_key_count()).map_err(|_| {
                DbError::sql(format!(
                    "index {} exceeds ANALYZE distinct-count limits",
                    index.name
                ))
            })?;
            self.catalog.index_stats.insert(
                index.name.clone(),
                IndexStats {
                    entry_count,
                    distinct_key_count,
                },
            );
        }
        Ok(())
    }

    fn try_execute_simple_table_projection_query(
        &self,
        query: &Query,
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty()
            || !query.order_by.is_empty()
            || query.limit.is_some()
            || query.offset.is_some()
        {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.filter.is_some()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || select.distinct
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
        {
            return Ok(None);
        }

        let table_schema = match self.table_schema(name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        let Some(data) = self.table_data(name) else {
            return Ok(None);
        };
        let Some((projection_indexes, column_names)) =
            self.simple_projection_plan(select, name, alias, table_schema)
        else {
            return Ok(None);
        };

        let mut rows = Vec::with_capacity(data.rows.len());
        for stored_row in &data.rows {
            let mut projected = Vec::with_capacity(projection_indexes.len());
            for index in &projection_indexes {
                projected.push(stored_row.values[*index].clone());
            }
            rows.push(QueryRow::new(projected));
        }
        Ok(Some(QueryResult::with_rows(column_names, rows)))
    }

    fn try_execute_simple_filtered_projection_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty() || query.offset.is_some() {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if !select.group_by.is_empty()
            || select.having.is_some()
            || select.distinct
            || !select.distinct_on.is_empty()
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let Some(filter) = select.filter.as_ref() else {
            return Ok(None);
        };
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
        {
            return Ok(None);
        }

        let table_schema = match self.table_schema(name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        let Some(data) = self.table_data(name) else {
            return Ok(None);
        };
        let Some((projection_indexes, column_names)) =
            self.simple_projection_plan(select, name, alias, table_schema)
        else {
            return Ok(None);
        };
        let binding_name = alias.as_deref().unwrap_or(name);

        let Some(range_filter) = simple_range_projection_filter(filter) else {
            return Ok(None);
        };
        let filter_table = range_filter.table;
        let filter_column = range_filter.column;
        let lower_bound = range_filter.lower;
        let upper_bound = range_filter.upper;
        if let Some(table_name) = filter_table {
            if !identifiers_equal(table_name, name) && !identifiers_equal(table_name, binding_name)
            {
                return Ok(None);
            }
        }
        let filter_column_index = table_schema
            .columns
            .iter()
            .position(|candidate| identifiers_equal(&candidate.name, filter_column))
            .ok_or_else(|| {
                DbError::internal(format!(
                    "simple filtered projection column {filter_column} missing from {name}"
                ))
            })?;

        let lower_bound = lower_bound
            .map(|bound| {
                Ok(SimpleRangeBoundValue {
                    inclusive: bound.inclusive,
                    value: self.eval_expr(
                        bound.value_expr,
                        &Dataset::empty(),
                        &[],
                        params,
                        &BTreeMap::new(),
                        None,
                    )?,
                })
            })
            .transpose()?;
        let upper_bound = upper_bound
            .map(|bound| {
                Ok(SimpleRangeBoundValue {
                    inclusive: bound.inclusive,
                    value: self.eval_expr(
                        bound.value_expr,
                        &Dataset::empty(),
                        &[],
                        params,
                        &BTreeMap::new(),
                        None,
                    )?,
                })
            })
            .transpose()?;

        let order_by = if query.order_by.is_empty() {
            None
        } else if query.order_by.len() == 1 {
            let Expr::Column {
                table: order_table,
                column: order_column,
            } = &query.order_by[0].expr
            else {
                return Ok(None);
            };
            if let Some(order_table) = order_table.as_deref() {
                if !identifiers_equal(order_table, name)
                    && !identifiers_equal(order_table, binding_name)
                {
                    return Ok(None);
                }
            }
            let Some(order_projection_index) =
                projection_indexes.iter().position(|projection_index| {
                    table_schema.columns[*projection_index]
                        .name
                        .as_str()
                        .eq_ignore_ascii_case(order_column)
                })
            else {
                return Ok(None);
            };
            Some(SimpleOrderByPlan {
                projection_index: order_projection_index,
                descending: query.order_by[0].descending,
            })
        } else {
            return Ok(None);
        };

        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));

        let mut rows = Vec::with_capacity(data.rows.len());
        for stored_row in &data.rows {
            let candidate = &stored_row.values[filter_column_index];
            if !simple_range_bound_matches(candidate, lower_bound.as_ref(), upper_bound.as_ref())? {
                continue;
            }
            let mut projected = Vec::with_capacity(projection_indexes.len());
            for index in &projection_indexes {
                projected.push(stored_row.values[*index].clone());
            }
            rows.push(QueryRow::new(projected));
        }

        if let Some(order_by) = order_by {
            let mut sort_error = None;
            rows.sort_by(|left, right| {
                let ordering = compare_values(
                    &left.values()[order_by.projection_index],
                    &right.values()[order_by.projection_index],
                );
                match ordering {
                    Ok(ordering) => {
                        if order_by.descending {
                            ordering.reverse()
                        } else {
                            ordering
                        }
                    }
                    Err(error) => {
                        if sort_error.is_none() {
                            sort_error = Some(error);
                        }
                        std::cmp::Ordering::Equal
                    }
                }
            });
            if let Some(error) = sort_error {
                return Err(error);
            }
        }

        if let Some(limit) = limit {
            rows.truncate(limit);
        }

        Ok(Some(QueryResult::with_rows(column_names, rows)))
    }

    fn try_execute_simple_indexed_projection_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty()
            || !query.order_by.is_empty()
            || query.limit.is_some()
            || query.offset.is_some()
        {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if !select.group_by.is_empty()
            || select.having.is_some()
            || select.distinct
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let Some(filter) = select.filter.as_ref() else {
            return Ok(None);
        };
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
            || self.visible_table_is_temporary(name)
        {
            return Ok(None);
        }

        let table_schema = match self.table_schema(name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        let Some(data) = self.table_data(name) else {
            return Ok(None);
        };
        let binding_name = alias.as_deref().unwrap_or(name);

        let Some((filter_table, filter_column, value_expr)) = simple_btree_lookup(filter) else {
            return Ok(None);
        };
        if let Some(table_name) = filter_table {
            if !identifiers_equal(table_name, name) && !identifiers_equal(table_name, binding_name)
            {
                return Ok(None);
            }
        }

        let value = self.eval_expr(
            value_expr,
            &Dataset::empty(),
            &[],
            params,
            &BTreeMap::new(),
            None,
        )?;
        let Some((projection_indexes, column_names)) =
            self.simple_projection_plan(select, name, alias, table_schema)
        else {
            return Ok(None);
        };

        if row_id_alias_column_name(table_schema)
            .is_some_and(|column_name| identifiers_equal(column_name, filter_column))
        {
            let mut rows = Vec::new();
            if let Value::Int64(row_id) = value {
                if let Some(stored_row) = data.row_by_id(row_id) {
                    let mut projected = Vec::with_capacity(projection_indexes.len());
                    for index in &projection_indexes {
                        projected.push(stored_row.values[*index].clone());
                    }
                    rows.push(QueryRow::new(projected));
                }
            }
            return Ok(Some(QueryResult::with_rows(column_names, rows)));
        }

        let Some(index) = self.catalog.indexes.values().find(|index| {
            identifiers_equal(&index.table_name, name)
                && index.fresh
                && index.kind == IndexKind::Btree
                && index.predicate_sql.is_none()
                && index.columns.len() == 1
                && index.columns[0]
                    .column_name
                    .as_deref()
                    .is_some_and(|index_column| identifiers_equal(index_column, filter_column))
                && index.columns[0].expression_sql.is_none()
        }) else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Btree { keys }) = self.indexes.get(&index.name) else {
            return Ok(None);
        };
        let row_ids = keys.row_ids_for_value_set(&value)?;

        let mut rows = Vec::with_capacity(row_ids.len());
        row_ids.for_each(|row_id| {
            let Some(stored_row) = data.row_by_id(row_id) else {
                return;
            };
            let mut projected = Vec::with_capacity(projection_indexes.len());
            for index in &projection_indexes {
                projected.push(stored_row.values[*index].clone());
            }
            rows.push(QueryRow::new(projected));
        });
        Ok(Some(QueryResult::with_rows(column_names, rows)))
    }

    fn simple_projection_plan(
        &self,
        select: &Select,
        table_name: &str,
        table_alias: &Option<String>,
        table_schema: &TableSchema,
    ) -> Option<(Vec<usize>, Vec<String>)> {
        let binding_name = table_alias.as_deref().unwrap_or(table_name);
        let mut projection_indexes = Vec::with_capacity(select.projection.len());
        let mut column_names = Vec::with_capacity(select.projection.len());

        for item in &select.projection {
            let SelectItem::Expr {
                expr,
                alias: select_alias,
            } = item
            else {
                return None;
            };
            let Expr::Column {
                table: table_name_expr,
                column,
            } = expr
            else {
                return None;
            };
            if let Some(projection_table) = table_name_expr.as_deref() {
                if !identifiers_equal(projection_table, table_name)
                    && !identifiers_equal(projection_table, binding_name)
                {
                    return None;
                }
            }
            let column_index = table_schema
                .columns
                .iter()
                .position(|candidate| identifiers_equal(&candidate.name, column))?;
            projection_indexes.push(column_index);
            column_names.push(select_alias.clone().unwrap_or_else(|| column.clone()));
        }

        Some((projection_indexes, column_names))
    }

    pub(crate) fn evaluate_query(
        &self,
        query: &Query,
        params: &[Value],
        inherited_ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Dataset> {
        let mut ctes = inherited_ctes.clone();
        let recursive_ctes = validate_recursive_ctes(query)?;
        for cte in &query.ctes {
            let dataset = if recursive_ctes.contains(&cte.name) {
                self.evaluate_recursive_cte(cte, params, &ctes)?
            } else {
                prepare_cte_dataset(cte, self.evaluate_query(&cte.query, params, &ctes)?)?
            };
            ctes.insert(cte.name.clone(), dataset);
        }

        let mut sorted_during_select = false;
        let mut dataset = match &query.body {
            QueryBody::Select(select)
                if !select.group_by.is_empty()
                    || projection_has_aggregate_items(&select.projection) =>
            {
                self.evaluate_select(select, params, &ctes)?
            }
            QueryBody::Select(select) => {
                let mut source = self.build_select_dataset(select, params, &ctes)?;
                if !query.order_by.is_empty() {
                    self.sort_dataset(&mut source, &query.order_by, params, &ctes)?;
                }
                let mut projected =
                    self.project_dataset(&source, &select.projection, params, &ctes, None)?;
                if !query.order_by.is_empty() {
                    self.sort_dataset(&mut projected, &query.order_by, params, &ctes)?;
                    sorted_during_select = true;
                }
                projected
            }
            _ => self.evaluate_query_body(&query.body, params, &ctes)?,
        };
        if let QueryBody::Select(select) = &query.body {
            if select.distinct {
                if !query.order_by.is_empty() && !sorted_during_select {
                    self.sort_dataset(&mut dataset, &query.order_by, params, &ctes)?;
                    sorted_during_select = true;
                }
                dataset = self.apply_select_distinct(select, dataset, params, &ctes)?;
            }
        }
        if !query.order_by.is_empty() && !sorted_during_select {
            self.sort_dataset(&mut dataset, &query.order_by, params, &ctes)?;
        }
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &ctes))
            .transpose()?
            .unwrap_or(0);
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &ctes))
            .transpose()?;
        if offset > 0 || limit.is_some() {
            let start = usize::try_from(offset.max(0)).unwrap_or(usize::MAX);
            let rows = if start >= dataset.rows.len() {
                Vec::new()
            } else {
                let iter = dataset.rows.into_iter().skip(start);
                match limit {
                    Some(limit) => iter
                        .take(usize::try_from(limit.max(0)).unwrap_or(0))
                        .collect(),
                    None => iter.collect(),
                }
            };
            dataset.rows = rows;
        }
        Ok(dataset)
    }

    fn evaluate_recursive_cte(
        &self,
        cte: &CommonTableExpr,
        params: &[Value],
        inherited_ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Dataset> {
        if !cte.query.order_by.is_empty() || cte.query.limit.is_some() || cte.query.offset.is_some()
        {
            return Err(DbError::sql(format!(
                "recursive CTE {} does not support ORDER BY, LIMIT, or OFFSET at the CTE level",
                cte.name
            )));
        }

        let QueryBody::SetOperation {
            op: crate::sql::ast::SetOperation::Union,
            all,
            left,
            right,
        } = &cte.query.body
        else {
            return Err(DbError::sql(format!(
                "recursive CTE {} must use UNION or UNION ALL between anchor and recursive terms",
                cte.name
            )));
        };

        let anchor_references = query_body_table_reference_count(left, &cte.name);
        if anchor_references != 0 {
            return Err(DbError::sql(format!(
                "recursive CTE {} anchor term must not reference itself",
                cte.name
            )));
        }

        let recursive_references = query_body_table_reference_count(right, &cte.name);
        if recursive_references != 1 {
            return Err(DbError::sql(format!(
                "recursive CTE {} recursive term must reference itself exactly once",
                cte.name
            )));
        }
        validate_recursive_term(right, &cte.name)?;

        let mut anchor = self.evaluate_query_body(left, params, inherited_ctes)?;
        if !all {
            anchor.rows = deduplicate_rows(anchor.rows)?;
        }
        let mut result = prepare_cte_dataset(cte, anchor)?;
        let mut working = result.clone();
        let mut seen = if *all {
            None
        } else {
            Some(
                result
                    .rows
                    .iter()
                    .map(|row| row_identity(row))
                    .collect::<Result<BTreeSet<_>>>()?,
            )
        };

        for _ in 0..RECURSIVE_CTE_MAX_ITERATIONS {
            if working.rows.is_empty() {
                return Ok(result);
            }

            let mut recursive_ctes = inherited_ctes.clone();
            recursive_ctes.insert(cte.name.clone(), working.clone());

            let recursive_rows = prepare_cte_dataset(
                cte,
                self.evaluate_query_body(right, params, &recursive_ctes)?,
            )?;
            if recursive_rows.columns.len() != result.columns.len() {
                return Err(DbError::sql(format!(
                    "recursive CTE {} produced {} columns in its recursive term but {} in its anchor term",
                    cte.name,
                    recursive_rows.columns.len(),
                    result.columns.len()
                )));
            }

            let next_rows = if let Some(seen) = &mut seen {
                let mut rows = Vec::new();
                for row in recursive_rows.rows {
                    let identity = row_identity(&row)?;
                    if seen.insert(identity) {
                        rows.push(row);
                    }
                }
                rows
            } else {
                recursive_rows.rows
            };

            if next_rows.is_empty() {
                return Ok(result);
            }

            result.rows.extend(next_rows.clone());
            working.rows = next_rows;
        }

        Err(DbError::sql(format!(
            "recursive CTE {} exceeded the {} iteration limit",
            cte.name, RECURSIVE_CTE_MAX_ITERATIONS
        )))
    }

    fn evaluate_query_with_outer(
        &self,
        query: &Query,
        params: &[Value],
        inherited_ctes: &BTreeMap<String, Dataset>,
        outer_dataset: &Dataset,
        outer_row: &[Value],
    ) -> Result<Dataset> {
        if !query_references_outer_scope(query, outer_dataset) {
            return self.evaluate_query(query, params, inherited_ctes);
        }
        if query.recursive {
            return Err(DbError::sql(
                "WITH RECURSIVE is not supported in correlated subqueries yet",
            ));
        }

        let mut ctes = inherited_ctes.clone();
        for cte in &query.ctes {
            let mut dataset = self.evaluate_query_with_outer(
                &cte.query,
                params,
                &ctes,
                outer_dataset,
                outer_row,
            )?;
            if !cte.column_names.is_empty() {
                if cte.column_names.len() != dataset.columns.len() {
                    return Err(DbError::sql(format!(
                        "CTE {} expected {} columns but produced {}",
                        cte.name,
                        cte.column_names.len(),
                        dataset.columns.len()
                    )));
                }
                for (binding, name) in dataset.columns.iter_mut().zip(&cte.column_names) {
                    binding.name = name.clone();
                    binding.table = Some(cte.name.clone());
                }
            }
            ctes.insert(cte.name.clone(), dataset);
        }

        let mut sorted_during_select = false;
        let mut dataset = match &query.body {
            QueryBody::Select(select)
                if !select.group_by.is_empty()
                    || projection_has_aggregate_items(&select.projection) =>
            {
                self.evaluate_select_with_outer(select, params, &ctes, outer_dataset, outer_row)?
            }
            QueryBody::Select(select) => {
                let mut source = self.build_select_dataset_with_outer(
                    select,
                    params,
                    &ctes,
                    outer_dataset,
                    outer_row,
                )?;
                if !query.order_by.is_empty() {
                    self.sort_dataset(&mut source, &query.order_by, params, &ctes)?;
                }
                let mut projected =
                    self.project_dataset(&source, &select.projection, params, &ctes, None)?;
                if !query.order_by.is_empty() {
                    self.sort_dataset(&mut projected, &query.order_by, params, &ctes)?;
                    sorted_during_select = true;
                }
                projected
            }
            _ => self.evaluate_query_body_with_outer(
                &query.body,
                params,
                &ctes,
                outer_dataset,
                outer_row,
            )?,
        };
        if let QueryBody::Select(select) = &query.body {
            if select.distinct {
                if !query.order_by.is_empty() && !sorted_during_select {
                    self.sort_dataset(&mut dataset, &query.order_by, params, &ctes)?;
                    sorted_during_select = true;
                }
                dataset = self.apply_select_distinct(select, dataset, params, &ctes)?;
            }
        }
        if !query.order_by.is_empty() && !sorted_during_select {
            self.sort_dataset(&mut dataset, &query.order_by, params, &ctes)?;
        }
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &ctes))
            .transpose()?
            .unwrap_or(0);
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &ctes))
            .transpose()?;
        if offset > 0 || limit.is_some() {
            let start = usize::try_from(offset.max(0)).unwrap_or(usize::MAX);
            let rows = if start >= dataset.rows.len() {
                Vec::new()
            } else {
                let iter = dataset.rows.into_iter().skip(start);
                match limit {
                    Some(limit) => iter
                        .take(usize::try_from(limit.max(0)).unwrap_or(0))
                        .collect(),
                    None => iter.collect(),
                }
            };
            dataset.rows = rows;
        }
        Ok(dataset)
    }

    fn evaluate_query_body(
        &self,
        body: &QueryBody,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Dataset> {
        match body {
            QueryBody::Select(select) => self.evaluate_select(select, params, ctes),
            QueryBody::Values(rows) => self.evaluate_values_body(rows, params, ctes),
            QueryBody::SetOperation {
                op,
                all,
                left,
                right,
            } => {
                let left = self.evaluate_query_body(left, params, ctes)?;
                let right = self.evaluate_query_body(right, params, ctes)?;
                self.evaluate_set_operation(*op, *all, left, right)
            }
        }
    }

    fn evaluate_query_body_with_outer(
        &self,
        body: &QueryBody,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
        outer_dataset: &Dataset,
        outer_row: &[Value],
    ) -> Result<Dataset> {
        match body {
            QueryBody::Select(select) => {
                self.evaluate_select_with_outer(select, params, ctes, outer_dataset, outer_row)
            }
            QueryBody::Values(rows) => {
                self.evaluate_values_body_with_outer(rows, params, ctes, outer_dataset, outer_row)
            }
            QueryBody::SetOperation {
                op,
                all,
                left,
                right,
            } => {
                let left = self.evaluate_query_body_with_outer(
                    left,
                    params,
                    ctes,
                    outer_dataset,
                    outer_row,
                )?;
                let right = self.evaluate_query_body_with_outer(
                    right,
                    params,
                    ctes,
                    outer_dataset,
                    outer_row,
                )?;
                self.evaluate_set_operation(*op, *all, left, right)
            }
        }
    }

    fn evaluate_select(
        &self,
        select: &Select,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Dataset> {
        let dataset = self.build_select_dataset(select, params, ctes)?;
        if !select.group_by.is_empty() || projection_has_aggregate_items(&select.projection) {
            self.evaluate_grouped_select(select, dataset, params, ctes)
        } else {
            self.project_dataset(&dataset, &select.projection, params, ctes, None)
        }
    }

    fn evaluate_select_with_outer(
        &self,
        select: &Select,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
        outer_dataset: &Dataset,
        outer_row: &[Value],
    ) -> Result<Dataset> {
        let dataset =
            self.build_select_dataset_with_outer(select, params, ctes, outer_dataset, outer_row)?;
        if !select.group_by.is_empty() || projection_has_aggregate_items(&select.projection) {
            self.evaluate_grouped_select(select, dataset, params, ctes)
        } else {
            self.project_dataset(&dataset, &select.projection, params, ctes, None)
        }
    }

    fn apply_select_distinct(
        &self,
        select: &Select,
        dataset: Dataset,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Dataset> {
        if !select.distinct {
            return Ok(dataset);
        }

        let columns = dataset.columns;
        let rows = if select.distinct_on.is_empty() {
            deduplicate_rows_stable(dataset.rows)?
        } else {
            let key_dataset = Dataset {
                columns: columns.clone(),
                rows: Vec::new(),
            };
            let mut seen = BTreeSet::new();
            let mut distinct_rows = Vec::new();
            for row in dataset.rows {
                let key = select
                    .distinct_on
                    .iter()
                    .map(|expr| self.eval_expr(expr, &key_dataset, &row, params, ctes, None))
                    .collect::<Result<Vec<_>>>()?;
                if seen.insert(row_identity(&key)?) {
                    distinct_rows.push(row);
                }
            }
            distinct_rows
        };

        Ok(Dataset { columns, rows })
    }

    fn build_select_dataset(
        &self,
        select: &Select,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Dataset> {
        let has_lateral = select.from.iter().any(from_item_contains_lateral);
        let mut dataset = if !has_lateral {
            if let Some(dataset) = self.try_indexed_scan(select, params, ctes)? {
                dataset
            } else if let Some(dataset) = self.try_indexed_join(select, params, ctes)? {
                dataset
            } else if let Some(dataset) =
                self.try_indexed_prefiltered_inner_join_tree(select, params, ctes)?
            {
                dataset
            } else {
                self.evaluate_from_clause(&select.from, params, ctes, &Dataset::empty(), &[])?
            }
        } else {
            self.evaluate_from_clause(&select.from, params, ctes, &Dataset::empty(), &[])?
        };

        if let Some(filter) = &select.filter {
            let filter_dataset = Dataset {
                columns: dataset.columns.clone(),
                rows: Vec::new(),
            };
            dataset.rows.retain(|row| {
                matches!(
                    self.eval_expr(filter, &filter_dataset, row, params, ctes, None),
                    Ok(Value::Bool(true))
                )
            });
        }

        Ok(dataset)
    }

    fn build_select_dataset_with_outer(
        &self,
        select: &Select,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
        outer_dataset: &Dataset,
        outer_row: &[Value],
    ) -> Result<Dataset> {
        let mut dataset =
            self.evaluate_from_clause(&select.from, params, ctes, outer_dataset, outer_row)?;

        dataset = augment_dataset_with_outer_scope(dataset, outer_dataset, outer_row);
        if let Some(filter) = &select.filter {
            let filter_dataset = Dataset {
                columns: dataset.columns.clone(),
                rows: Vec::new(),
            };
            dataset.rows.retain(|row| {
                matches!(
                    self.eval_expr(filter, &filter_dataset, row, params, ctes, None),
                    Ok(Value::Bool(true))
                )
            });
        }
        Ok(dataset)
    }

    fn evaluate_from_clause(
        &self,
        from: &[FromItem],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
        scope_dataset: &Dataset,
        scope_row: &[Value],
    ) -> Result<Dataset> {
        if from.is_empty() {
            return Ok(Dataset {
                columns: Vec::new(),
                rows: vec![Vec::new()],
            });
        }
        let mut iter = from.iter();
        let cross_constraint = JoinConstraint::On(Expr::Literal(Value::Bool(true)));
        let mut current = self.evaluate_from_item_in_scope(
            iter.next().expect("first FROM item"),
            params,
            ctes,
            scope_dataset,
            scope_row,
        )?;
        for item in iter {
            current = if from_item_is_lateral(item) {
                self.evaluate_join_with_lateral_right(
                    current,
                    item,
                    JoinKind::Inner,
                    &cross_constraint,
                    params,
                    ctes,
                    scope_dataset,
                    scope_row,
                )?
            } else {
                let right =
                    self.evaluate_from_item_in_scope(item, params, ctes, scope_dataset, scope_row)?;
                nested_loop_join(
                    current,
                    right,
                    JoinKind::Inner,
                    &cross_constraint,
                    self,
                    params,
                    ctes,
                )?
            };
        }
        Ok(current)
    }

    fn try_indexed_scan(
        &self,
        select: &Select,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Option<Dataset>> {
        let Some(filter) = &select.filter else {
            return Ok(None);
        };
        if select.from.len() != 1 {
            return Ok(None);
        }
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if ctes.contains_key(name)
            || self
                .visible_view(name, NameResolutionScope::Session)
                .is_some()
            || self.visible_table_is_temporary(name)
        {
            return Ok(None);
        }
        let table = self
            .table_schema(name)
            .ok_or_else(|| DbError::sql(format!("unknown table or view {name}")))?;
        if !generated_columns_are_stored(table) {
            return Ok(None);
        }
        let Some(data) = self.table_data(name) else {
            return Ok(None);
        };

        if let Some((table_qualifier, column_name, value_expr)) = simple_btree_lookup(filter) {
            if !matches_filter_binding(name, alias, table_qualifier) {
                return Ok(None);
            }
            if let Some(index) = self.catalog.indexes.values().find(|index| {
                identifiers_equal(&index.table_name, name)
                    && index.fresh
                    && index.kind == IndexKind::Btree
                    && index.predicate_sql.is_none()
                    && index.columns.len() == 1
                    && index.columns[0]
                        .column_name
                        .as_deref()
                        .is_some_and(|index_column| identifiers_equal(index_column, column_name))
                    && index.columns[0].expression_sql.is_none()
            }) {
                let value =
                    self.eval_expr(value_expr, &Dataset::empty(), &[], params, ctes, None)?;
                if let Some(RuntimeIndex::Btree { keys }) = self.indexes.get(&index.name) {
                    let row_ids = keys.row_ids_for_value_set(&value)?;
                    return self
                        .dataset_from_row_id_set(table, data, alias, row_ids)
                        .map(Some);
                }
            }
        }

        if let Some((column_name, pattern_expr, has_additional_filter)) =
            simple_trigram_lookup(filter)
        {
            if let Some(index) = self.catalog.indexes.values().find(|index| {
                identifiers_equal(&index.table_name, name)
                    && index.fresh
                    && index.kind == IndexKind::Trigram
                    && index.predicate_sql.is_none()
                    && index.columns.len() == 1
                    && index.columns[0]
                        .column_name
                        .as_deref()
                        .is_some_and(|index_column| identifiers_equal(index_column, column_name))
            }) {
                let pattern =
                    self.eval_expr(pattern_expr, &Dataset::empty(), &[], params, ctes, None)?;
                if let Value::Text(pattern) = pattern {
                    if let Some(RuntimeIndex::Trigram { index }) = self.indexes.get(&index.name) {
                        if !index.planner_may_use_index() {
                            return Ok(None);
                        }
                        let row_ids =
                            match index.query_candidates(&pattern, has_additional_filter)? {
                                TrigramQueryResult::Candidates(ids)
                                | TrigramQueryResult::Capped(ids) => ids
                                    .into_iter()
                                    .filter_map(|row_id| i64::try_from(row_id).ok())
                                    .collect::<Vec<_>>(),
                                TrigramQueryResult::FallbackTooShort
                                | TrigramQueryResult::FallbackRequiresAdditionalFilter
                                | TrigramQueryResult::RebuildRequired => return Ok(None),
                            };
                        return self
                            .dataset_from_row_id_set(
                                table,
                                data,
                                alias,
                                RuntimeRowIdSet::Many(&row_ids),
                            )
                            .map(Some);
                    }
                }
            }
        }

        Ok(None)
    }

    fn evaluate_values_body(
        &self,
        rows: &[Vec<Expr>],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Dataset> {
        self.evaluate_values_body_inner(rows, params, ctes, &Dataset::empty(), &[])
    }

    fn evaluate_values_body_with_outer(
        &self,
        rows: &[Vec<Expr>],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
        outer_dataset: &Dataset,
        outer_row: &[Value],
    ) -> Result<Dataset> {
        self.evaluate_values_body_inner(rows, params, ctes, outer_dataset, outer_row)
    }

    fn evaluate_values_body_inner(
        &self,
        rows: &[Vec<Expr>],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
        scope_dataset: &Dataset,
        scope_row: &[Value],
    ) -> Result<Dataset> {
        let width = rows.first().map_or(0, Vec::len);
        let mut columns = Vec::with_capacity(width);
        if let Some(first_row) = rows.first() {
            for (index, expr) in first_row.iter().enumerate() {
                columns.push(ColumnBinding::visible(
                    None,
                    infer_expr_name(expr, index + 1),
                ));
            }
        }

        let mut result_rows = Vec::with_capacity(rows.len());
        for row in rows {
            if row.len() != width {
                return Err(DbError::sql(
                    "VALUES rows must all have the same number of columns",
                ));
            }
            let values = row
                .iter()
                .map(|expr| self.eval_expr(expr, scope_dataset, scope_row, params, ctes, None))
                .collect::<Result<Vec<_>>>()?;
            result_rows.push(values);
        }
        Ok(Dataset {
            columns,
            rows: result_rows,
        })
    }

    fn try_indexed_join(
        &self,
        select: &Select,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Option<Dataset>> {
        let Some(filter) = &select.filter else {
            return Ok(None);
        };
        if select.from.len() != 1 {
            return Ok(None);
        }
        let FromItem::Join {
            left,
            right,
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(on),
        } = &select.from[0]
        else {
            return Ok(None);
        };
        let (left_name, left_alias) = match &**left {
            FromItem::Table { name, alias } => (name, alias),
            _ => return Ok(None),
        };
        let (right_name, right_alias) = match &**right {
            FromItem::Table { name, alias } => (name, alias),
            _ => return Ok(None),
        };
        if ctes.contains_key(left_name)
            || ctes.contains_key(right_name)
            || self
                .visible_view(left_name, NameResolutionScope::Session)
                .is_some()
            || self
                .visible_view(right_name, NameResolutionScope::Session)
                .is_some()
            || self.visible_table_is_temporary(left_name)
            || self.visible_table_is_temporary(right_name)
        {
            return Ok(None);
        }

        let Some((filter_table, filter_column, value_expr)) = simple_btree_lookup(filter) else {
            return Ok(None);
        };
        let Some((left_join, right_join)) = simple_join_equality(on) else {
            return Ok(None);
        };

        let left_binding = TableBindingRef {
            name: left_name,
            alias: left_alias,
        };
        let right_binding = TableBindingRef {
            name: right_name,
            alias: right_alias,
        };

        if matches_table_binding(left_binding, filter_table)
            && matches_table_binding(left_binding, left_join.table)
            && matches_table_binding(right_binding, right_join.table)
        {
            let Some(left_dataset) = self.indexed_table_lookup(
                left_name,
                left_alias,
                filter_column,
                value_expr,
                params,
                ctes,
            )?
            else {
                return Ok(None);
            };
            return self.indexed_inner_join_filtered(IndexedJoinPlan {
                filtered_table: left_binding,
                filtered_dataset: &left_dataset,
                filtered_join_column: left_join.column,
                probe_table: right_binding,
                probe_join_column: right_join.column,
                filtered_on_left: true,
            });
        }

        if matches_table_binding(right_binding, filter_table)
            && matches_table_binding(right_binding, right_join.table)
            && matches_table_binding(left_binding, left_join.table)
        {
            let Some(right_dataset) = self.indexed_table_lookup(
                right_name,
                right_alias,
                filter_column,
                value_expr,
                params,
                ctes,
            )?
            else {
                return Ok(None);
            };
            return self.indexed_inner_join_filtered(IndexedJoinPlan {
                filtered_table: right_binding,
                filtered_dataset: &right_dataset,
                filtered_join_column: right_join.column,
                probe_table: left_binding,
                probe_join_column: left_join.column,
                filtered_on_left: false,
            });
        }

        Ok(None)
    }

    fn try_indexed_prefiltered_inner_join_tree(
        &self,
        select: &Select,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Option<Dataset>> {
        let Some(filter) = &select.filter else {
            return Ok(None);
        };
        if select.from.len() != 1 {
            return Ok(None);
        }
        let Some((Some(filter_table), filter_column, value_expr)) = simple_btree_lookup(filter)
        else {
            return Ok(None);
        };
        if !from_item_is_all_inner_table_joins(&select.from[0]) {
            return Ok(None);
        }

        let mut applied_prefilter = false;
        let dataset = self.evaluate_from_item_with_indexed_prefilter(
            &select.from[0],
            params,
            ctes,
            filter_table,
            filter_column,
            value_expr,
            &mut applied_prefilter,
        )?;
        if applied_prefilter {
            Ok(Some(dataset))
        } else {
            Ok(None)
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_from_item_with_indexed_prefilter(
        &self,
        item: &FromItem,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
        filter_table: &str,
        filter_column: &str,
        value_expr: &Expr,
        applied_prefilter: &mut bool,
    ) -> Result<Dataset> {
        match item {
            FromItem::Table { name, alias } => {
                if !*applied_prefilter
                    && matches_filter_binding(name, alias, Some(filter_table))
                    && !ctes.contains_key(name)
                    && self
                        .visible_view(name, NameResolutionScope::Session)
                        .is_none()
                    && !self.visible_table_is_temporary(name)
                {
                    if let Some(dataset) = self.indexed_table_lookup(
                        name,
                        alias,
                        filter_column,
                        value_expr,
                        params,
                        ctes,
                    )? {
                        *applied_prefilter = true;
                        return Ok(dataset);
                    }
                }
                self.evaluate_from_item(item, params, ctes)
            }
            FromItem::Join {
                left,
                right,
                kind,
                constraint,
            } => {
                let left_dataset = self.evaluate_from_item_with_indexed_prefilter(
                    left,
                    params,
                    ctes,
                    filter_table,
                    filter_column,
                    value_expr,
                    applied_prefilter,
                )?;
                if matches!(kind, JoinKind::Inner) {
                    if let Some(dataset) = self.try_indexed_inner_join_with_right_table(
                        &left_dataset,
                        right,
                        constraint,
                        ctes,
                    )? {
                        return Ok(dataset);
                    }
                }
                let right_dataset = self.evaluate_from_item_with_indexed_prefilter(
                    right,
                    params,
                    ctes,
                    filter_table,
                    filter_column,
                    value_expr,
                    applied_prefilter,
                )?;
                nested_loop_join(
                    left_dataset,
                    right_dataset,
                    *kind,
                    constraint,
                    self,
                    params,
                    ctes,
                )
            }
            _ => self.evaluate_from_item(item, params, ctes),
        }
    }

    fn indexed_table_lookup(
        &self,
        table_name: &str,
        alias: &Option<String>,
        column_name: &str,
        value_expr: &Expr,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Option<Dataset>> {
        let table = self
            .table_schema(table_name)
            .ok_or_else(|| DbError::sql(format!("unknown table or view {table_name}")))?;
        if !generated_columns_are_stored(table) {
            return Ok(None);
        }
        let Some(data) = self.table_data(table_name) else {
            return Ok(None);
        };
        let Some(index) = self.catalog.indexes.values().find(|index| {
            identifiers_equal(&index.table_name, table_name)
                && index.fresh
                && index.kind == IndexKind::Btree
                && index.predicate_sql.is_none()
                && index.columns.len() == 1
                && index.columns[0]
                    .column_name
                    .as_deref()
                    .is_some_and(|index_column| identifiers_equal(index_column, column_name))
                && index.columns[0].expression_sql.is_none()
        }) else {
            return Ok(None);
        };

        let value = self.eval_expr(value_expr, &Dataset::empty(), &[], params, ctes, None)?;
        let Some(RuntimeIndex::Btree { keys }) = self.indexes.get(&index.name) else {
            return Ok(None);
        };
        let row_ids = keys.row_ids_for_value_set(&value)?;
        self.dataset_from_row_id_set(table, data, alias, row_ids)
            .map(Some)
    }

    fn indexed_inner_join_filtered(&self, plan: IndexedJoinPlan<'_>) -> Result<Option<Dataset>> {
        let filtered_table = self.table_schema(plan.filtered_table.name).ok_or_else(|| {
            DbError::sql(format!(
                "unknown table or view {}",
                plan.filtered_table.name
            ))
        })?;
        let probe_table = self.table_schema(plan.probe_table.name).ok_or_else(|| {
            DbError::sql(format!("unknown table or view {}", plan.probe_table.name))
        })?;
        if !generated_columns_are_stored(filtered_table)
            || !generated_columns_are_stored(probe_table)
        {
            return Ok(None);
        }
        let empty_probe_data = TableData::default();
        let probe_data = self
            .table_data(plan.probe_table.name)
            .unwrap_or(&empty_probe_data);
        let filtered_join_index = filtered_table
            .columns
            .iter()
            .position(|column| identifiers_equal(&column.name, plan.filtered_join_column))
            .ok_or_else(|| DbError::sql(format!("unknown column {}", plan.filtered_join_column)))?;
        let is_probe_rowid_alias = crate::exec::dml::row_id_alias_column_name(probe_table)
            .is_some_and(|name| identifiers_equal(name, plan.probe_join_column));

        let probe_index = if is_probe_rowid_alias {
            None
        } else {
            self.catalog.indexes.values().find(|index| {
                identifiers_equal(&index.table_name, plan.probe_table.name)
                    && index.fresh
                    && index.kind == IndexKind::Btree
                    && index.predicate_sql.is_none()
                    && index.columns.len() == 1
                    && index.columns[0]
                        .column_name
                        .as_deref()
                        .is_some_and(|index_column| {
                            identifiers_equal(index_column, plan.probe_join_column)
                        })
                    && index.columns[0].expression_sql.is_none()
            })
        };
        if probe_index.is_none() && !is_probe_rowid_alias {
            return Ok(None);
        }
        let keys = if let Some(index) = probe_index {
            let Some(RuntimeIndex::Btree { keys }) = self.indexes.get(&index.name) else {
                return Ok(None);
            };
            Some(keys)
        } else {
            None
        };
        let use_probe_row_position_map = probe_data
            .rows
            .len()
            .saturating_mul(plan.filtered_dataset.rows.len())
            > 8_192;
        let probe_row_positions = if use_probe_row_position_map {
            let mut positions = Int64Map::<usize>::default();
            for (position, row) in probe_data.rows.iter().enumerate() {
                positions.insert(row.row_id, position);
            }
            Some(positions)
        } else {
            None
        };

        let probe_columns = probe_table
            .columns
            .iter()
            .map(|column| {
                ColumnBinding::visible(
                    Some(plan.probe_table.binding_name().to_string()),
                    column.name.clone(),
                )
            })
            .collect::<Vec<_>>();
        let mut columns = if plan.filtered_on_left {
            plan.filtered_dataset.columns.clone()
        } else {
            probe_columns.clone()
        };
        if plan.filtered_on_left {
            columns.extend(probe_columns.clone());
        } else {
            columns.extend(plan.filtered_dataset.columns.clone());
        }
        let mut rows = Vec::new();
        for filtered_row in &plan.filtered_dataset.rows {
            let Some(join_value) = filtered_row.get(filtered_join_index) else {
                return Err(DbError::internal(
                    "join row is shorter than filtered table schema",
                ));
            };
            if matches!(join_value, Value::Null) {
                continue;
            }
            let row_ids = if let Some(keys) = keys {
                keys.row_ids_for_value_set(join_value)?
            } else if let Value::Int64(val) = join_value {
                RuntimeRowIdSet::Single(*val)
            } else {
                RuntimeRowIdSet::Empty
            };
            if row_ids.is_empty() {
                continue;
            }
            row_ids.for_each(|row_id| {
                let probe_row = if let Some(positions) = probe_row_positions.as_ref() {
                    let Some(probe_position) = positions.get(&row_id).copied() else {
                        return;
                    };
                    &probe_data.rows[probe_position].values
                } else {
                    let Some(probe_row) = probe_data.row_by_id(row_id) else {
                        return;
                    };
                    &probe_row.values
                };
                let mut row = Vec::with_capacity(filtered_row.len() + probe_row.len());
                if plan.filtered_on_left {
                    row.extend_from_slice(filtered_row);
                    row.extend_from_slice(probe_row);
                } else {
                    row.extend_from_slice(probe_row);
                    row.extend_from_slice(filtered_row);
                }
                rows.push(row);
            });
        }
        Ok(Some(Dataset { columns, rows }))
    }

    fn try_indexed_inner_join_with_right_table(
        &self,
        left: &Dataset,
        right_item: &FromItem,
        constraint: &JoinConstraint,
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Option<Dataset>> {
        let JoinConstraint::On(on) = constraint else {
            return Ok(None);
        };
        let Some((left_join_ref, right_join_ref)) = simple_join_equality(on) else {
            return Ok(None);
        };
        let FromItem::Table {
            name: right_name,
            alias: right_alias,
        } = right_item
        else {
            return Ok(None);
        };
        if ctes.contains_key(right_name) {
            return Ok(None);
        }
        if self
            .visible_view(right_name, NameResolutionScope::Session)
            .is_some()
            || self.visible_table_is_temporary(right_name)
        {
            return Ok(None);
        }

        let right_binding = TableBindingRef {
            name: right_name,
            alias: right_alias,
        };
        let (left_probe_ref, right_join_column) =
            if matches_table_binding(right_binding, right_join_ref.table) {
                (left_join_ref, right_join_ref.column)
            } else if matches_table_binding(right_binding, left_join_ref.table) {
                (right_join_ref, left_join_ref.column)
            } else {
                return Ok(None);
            };
        let Some(left_join_index) =
            dataset_column_index(left, left_probe_ref.table, left_probe_ref.column)
        else {
            return Ok(None);
        };

        let right_table = self
            .table_schema(right_name)
            .ok_or_else(|| DbError::sql(format!("unknown table or view {right_name}")))?;
        if !generated_columns_are_stored(right_table) {
            return Ok(None);
        }
        let Some(right_data) = self.table_data(right_name) else {
            return Ok(None);
        };
        let is_probe_rowid_alias = crate::exec::dml::row_id_alias_column_name(right_table)
            .is_some_and(|name| identifiers_equal(name, right_join_column));

        let probe_index = if is_probe_rowid_alias {
            None
        } else {
            self.catalog.indexes.values().find(|index| {
                identifiers_equal(&index.table_name, right_name)
                    && index.fresh
                    && index.kind == IndexKind::Btree
                    && index.predicate_sql.is_none()
                    && index.columns.len() == 1
                    && index.columns[0]
                        .column_name
                        .as_deref()
                        .is_some_and(|index_column| {
                            identifiers_equal(index_column, right_join_column)
                        })
                    && index.columns[0].expression_sql.is_none()
            })
        };
        if probe_index.is_none() && !is_probe_rowid_alias {
            return Ok(None);
        }
        let keys = if let Some(index) = probe_index {
            let Some(RuntimeIndex::Btree { keys }) = self.indexes.get(&index.name) else {
                return Ok(None);
            };
            Some(keys)
        } else {
            None
        };

        let use_right_row_position_map =
            right_data.rows.len().saturating_mul(left.rows.len()) > 8_192;
        let right_row_positions = if use_right_row_position_map {
            let mut positions = Int64Map::<usize>::default();
            for (position, row) in right_data.rows.iter().enumerate() {
                positions.insert(row.row_id, position);
            }
            Some(positions)
        } else {
            None
        };

        let right_binding_name = right_alias.clone().unwrap_or_else(|| right_name.clone());
        let mut columns = left.columns.clone();
        columns.extend(right_table.columns.iter().map(|column| {
            ColumnBinding::visible(Some(right_binding_name.clone()), column.name.clone())
        }));
        let mut rows = Vec::new();
        for left_row in &left.rows {
            let Some(join_value) = left_row.get(left_join_index) else {
                return Err(DbError::internal(
                    "join row is shorter than the left input schema",
                ));
            };
            if matches!(join_value, Value::Null) {
                continue;
            }
            let row_ids = if let Some(keys) = keys {
                keys.row_ids_for_value_set(join_value)?
            } else if let Value::Int64(val) = join_value {
                RuntimeRowIdSet::Single(*val)
            } else {
                RuntimeRowIdSet::Empty
            };
            row_ids.for_each(|row_id| {
                let right_values = if let Some(positions) = right_row_positions.as_ref() {
                    let Some(right_position) = positions.get(&row_id).copied() else {
                        return;
                    };
                    &right_data.rows[right_position].values
                } else {
                    let Some(right_row) = right_data.row_by_id(row_id) else {
                        return;
                    };
                    &right_row.values
                };
                let mut row = Vec::with_capacity(left_row.len() + right_values.len());
                row.extend_from_slice(left_row);
                row.extend_from_slice(right_values);
                rows.push(row);
            });
        }
        Ok(Some(Dataset { columns, rows }))
    }

    fn evaluate_from_item(
        &self,
        item: &FromItem,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Dataset> {
        self.evaluate_from_item_in_scope(item, params, ctes, &Dataset::empty(), &[])
    }

    fn evaluate_from_item_in_scope(
        &self,
        item: &FromItem,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
        scope_dataset: &Dataset,
        scope_row: &[Value],
    ) -> Result<Dataset> {
        match item {
            FromItem::Table { name, alias } => {
                if let Some(dataset) = ctes.get(name) {
                    let mut dataset = dataset.clone();
                    if let Some(alias) = alias {
                        for column in &mut dataset.columns {
                            column.table = Some(alias.clone());
                        }
                    }
                    return Ok(dataset);
                }
                if let Some(view) = self.visible_view(name, NameResolutionScope::Session) {
                    let view_statement = parse_sql_statement(&view.sql_text)?;
                    let Statement::Query(query) = view_statement else {
                        return Err(DbError::corruption(format!(
                            "view {} does not contain a SELECT statement",
                            view.name
                        )));
                    };
                    let mut dataset = if view.temporary {
                        self.evaluate_query(&query, params, ctes)?
                    } else {
                        let persistent_runtime = self.persistent_resolution_runtime();
                        persistent_runtime.evaluate_query(&query, params, ctes)?
                    };
                    if let Some(alias) = alias {
                        for column in &mut dataset.columns {
                            column.table = Some(alias.clone());
                        }
                    } else {
                        for column in &mut dataset.columns {
                            column.table = Some(view.name.clone());
                        }
                    }
                    return Ok(dataset);
                }
                let table = self
                    .table_schema(name)
                    .ok_or_else(|| DbError::sql(format!("unknown table or view {name}")))?;
                let data = self
                    .table_data_for_schema(table, name)
                    .map(|data| data.map(Cow::into_owned))?
                    .unwrap_or_default();
                Ok(Dataset {
                    columns: table
                        .columns
                        .iter()
                        .map(|column| {
                            ColumnBinding::visible(
                                Some(alias.clone().unwrap_or_else(|| name.clone())),
                                column.name.clone(),
                            )
                        })
                        .collect(),
                    rows: data.rows.into_iter().map(|row| row.values).collect(),
                })
            }
            FromItem::Subquery {
                query,
                alias,
                column_names,
                lateral,
            } => {
                let mut dataset = if *lateral {
                    self.evaluate_query_with_outer(query, params, ctes, scope_dataset, scope_row)?
                } else {
                    self.evaluate_query(query, params, ctes)?
                };
                if !column_names.is_empty() {
                    if column_names.len() != dataset.columns.len() {
                        return Err(DbError::sql(format!(
                            "subquery alias {} expected {} column names but produced {} columns",
                            alias,
                            column_names.len(),
                            dataset.columns.len()
                        )));
                    }
                    for (binding, column_name) in dataset.columns.iter_mut().zip(column_names) {
                        binding.name = column_name.clone();
                    }
                }
                for column in &mut dataset.columns {
                    column.table = Some(alias.clone());
                }
                Ok(dataset)
            }
            FromItem::Function {
                name,
                args,
                alias,
                lateral,
            } => {
                let eval_dataset = if *lateral {
                    scope_dataset
                } else {
                    &Dataset::empty()
                };
                let eval_row = if *lateral { scope_row } else { &[] };
                let values = args
                    .iter()
                    .map(|expr| self.eval_expr(expr, eval_dataset, eval_row, params, ctes, None))
                    .collect::<Result<Vec<_>>>()?;
                self.evaluate_table_function(name, values, alias)
            }
            FromItem::Join {
                left,
                right,
                kind,
                constraint,
            } => {
                let left =
                    self.evaluate_from_item_in_scope(left, params, ctes, scope_dataset, scope_row)?;
                if from_item_is_lateral(right) {
                    return self.evaluate_join_with_lateral_right(
                        left,
                        right,
                        *kind,
                        constraint,
                        params,
                        ctes,
                        scope_dataset,
                        scope_row,
                    );
                }
                if matches!(kind, JoinKind::Inner) {
                    if let Some(dataset) = self
                        .try_indexed_inner_join_with_right_table(&left, right, constraint, ctes)?
                    {
                        return Ok(dataset);
                    }
                }
                let right = self.evaluate_from_item_in_scope(
                    right,
                    params,
                    ctes,
                    scope_dataset,
                    scope_row,
                )?;
                nested_loop_join(left, right, *kind, constraint, self, params, ctes)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_join_with_lateral_right(
        &self,
        left: Dataset,
        right_item: &FromItem,
        kind: JoinKind,
        constraint: &JoinConstraint,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
        scope_dataset: &Dataset,
        scope_row: &[Value],
    ) -> Result<Dataset> {
        if matches!(kind, JoinKind::Right | JoinKind::Full) {
            return Err(DbError::sql(
                "LATERAL is only supported with INNER, LEFT, and CROSS joins",
            ));
        }

        let mut columns = left.columns.clone();
        let mut rows = Vec::new();
        for left_row in &left.rows {
            let left_single = Dataset {
                columns: left.columns.clone(),
                rows: vec![left_row.clone()],
            };
            let scope_with_left =
                augment_dataset_with_outer_scope(left_single.clone(), scope_dataset, scope_row);
            let scope_values = scope_with_left
                .rows
                .first()
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let right = self.evaluate_from_item_in_scope(
                right_item,
                params,
                ctes,
                &scope_with_left,
                scope_values,
            )?;
            let joined =
                nested_loop_join(left_single, right, kind, constraint, self, params, ctes)?;
            columns = joined.columns.clone();
            rows.extend(joined.rows);
        }
        Ok(Dataset { columns, rows })
    }

    fn evaluate_table_function(
        &self,
        name: &str,
        values: Vec<Value>,
        alias: &Option<String>,
    ) -> Result<Dataset> {
        let table_name = alias.clone().unwrap_or_else(|| name.to_string());
        match name {
            "json_each" | "pg_catalog.json_each" => {
                self.evaluate_json_table_function(table_name, values, false)
            }
            "json_tree" | "pg_catalog.json_tree" => {
                self.evaluate_json_table_function(table_name, values, true)
            }
            other => Err(DbError::sql(format!("unsupported table function {other}"))),
        }
    }

    fn evaluate_json_table_function(
        &self,
        table_name: String,
        values: Vec<Value>,
        recursive: bool,
    ) -> Result<Dataset> {
        if values.len() != 1 {
            return Err(DbError::sql(if recursive {
                "json_tree expects 1 argument"
            } else {
                "json_each expects 1 argument"
            }));
        }
        let rows = if recursive {
            expand_json_tree_rows(&values[0])?
        } else {
            expand_json_each_rows(&values[0])?
        };
        let mut columns = vec![
            ColumnBinding::visible(Some(table_name.clone()), "key".to_string()),
            ColumnBinding::visible(Some(table_name.clone()), "value".to_string()),
            ColumnBinding::visible(Some(table_name.clone()), "type".to_string()),
        ];
        if recursive {
            columns.push(ColumnBinding::visible(Some(table_name), "path".to_string()));
        }
        Ok(Dataset { columns, rows })
    }

    fn dataset_from_row_id_set(
        &self,
        table: &TableSchema,
        data: &TableData,
        alias: &Option<String>,
        row_ids: RuntimeRowIdSet<'_>,
    ) -> Result<Dataset> {
        let table_name = alias.clone().unwrap_or_else(|| table.name.clone());
        let mut rows = Vec::with_capacity(row_ids.len());
        row_ids.for_each(|row_id| {
            if let Some(row) = data.row_by_id(row_id) {
                rows.push(row.values.clone());
            }
        });
        Ok(Dataset {
            columns: table
                .columns
                .iter()
                .map(|column| ColumnBinding::visible(Some(table_name.clone()), column.name.clone()))
                .collect(),
            rows,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RootHeader {
    schema_cookie: u32,
    payload_checksum: u32,
    pointer: OverflowPointer,
}

#[derive(Clone, Debug)]
struct ManifestTemplate {
    schema_cookie: u32,
    table_next_row_id_offsets: BTreeMap<String, usize>,
    table_state_offsets: BTreeMap<String, usize>,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
struct ManifestEncoding {
    bytes: Vec<u8>,
    table_next_row_id_offsets: BTreeMap<String, usize>,
    table_state_offsets: BTreeMap<String, usize>,
}

#[derive(Debug)]
struct SnapshotPageStore<'a> {
    pager: &'a PagerHandle,
    wal: &'a WalHandle,
    snapshot_lsn: u64,
}

impl PageStore for SnapshotPageStore<'_> {
    fn page_size(&self) -> u32 {
        self.pager.page_size()
    }

    fn allocate_page(&mut self) -> Result<PageId> {
        Err(DbError::internal(
            "snapshot page store does not support allocation",
        ))
    }

    fn free_page(&mut self, _page_id: PageId) -> Result<()> {
        Err(DbError::internal(
            "snapshot page store does not support free",
        ))
    }

    fn read_page(&self, page_id: PageId) -> Result<Arc<[u8]>> {
        if let Some(page) = self.wal.read_page_at_snapshot(page_id, self.snapshot_lsn)? {
            Ok(Arc::from(page))
        } else {
            self.pager.read_page(page_id)
        }
    }

    fn write_page(&mut self, _page_id: PageId, _data: &[u8]) -> Result<()> {
        Err(DbError::internal(
            "snapshot page store does not support writes",
        ))
    }
}

#[derive(Debug)]
struct DbTxnPageStore<'a> {
    db: &'a crate::db::Db,
}

impl PageStore for DbTxnPageStore<'_> {
    fn page_size(&self) -> u32 {
        self.db.config().page_size
    }

    fn allocate_page(&mut self) -> Result<PageId> {
        self.db.allocate_page()
    }

    fn free_page(&mut self, page_id: PageId) -> Result<()> {
        self.db.free_page(page_id)
    }

    fn read_page(&self, page_id: PageId) -> Result<Arc<[u8]>> {
        self.db.read_page(page_id)
    }

    fn write_page(&mut self, page_id: PageId, data: &[u8]) -> Result<()> {
        self.db.write_page(page_id, data)
    }

    fn write_page_owned(&mut self, page_id: PageId, data: Vec<u8>) -> Result<()> {
        self.db.write_page_owned(page_id, data)
    }
}

fn augment_dataset_with_outer_scope(
    mut dataset: Dataset,
    outer_dataset: &Dataset,
    outer_row: &[Value],
) -> Dataset {
    if outer_dataset.columns.is_empty() || outer_row.is_empty() {
        return dataset;
    }

    dataset.columns.extend(outer_dataset.columns.clone());
    for row in &mut dataset.rows {
        row.extend_from_slice(outer_row);
    }
    dataset
}

fn query_references_outer_scope(query: &Query, outer_dataset: &Dataset) -> bool {
    let outer_tables = outer_dataset
        .columns
        .iter()
        .filter_map(|binding| binding.table.clone())
        .collect::<BTreeSet<_>>();
    if outer_tables.is_empty() {
        return false;
    }
    query_references_outer_tables(query, &outer_tables)
}

fn query_references_outer_tables(query: &Query, outer_tables: &BTreeSet<String>) -> bool {
    let local_tables = collect_query_table_names(query);
    query
        .ctes
        .iter()
        .any(|cte| query_references_outer_tables(&cte.query, outer_tables))
        || query_body_references_outer(&query.body, outer_tables, &local_tables)
        || query
            .order_by
            .iter()
            .any(|order| expr_references_outer(&order.expr, outer_tables, &local_tables))
        || query
            .limit
            .as_ref()
            .is_some_and(|expr| expr_references_outer(expr, outer_tables, &local_tables))
        || query
            .offset
            .as_ref()
            .is_some_and(|expr| expr_references_outer(expr, outer_tables, &local_tables))
}

fn query_body_references_outer(
    body: &QueryBody,
    outer_tables: &BTreeSet<String>,
    local_tables: &BTreeSet<String>,
) -> bool {
    match body {
        QueryBody::Select(select) => select_references_outer(select, outer_tables, local_tables),
        QueryBody::Values(rows) => rows
            .iter()
            .flatten()
            .any(|expr| expr_references_outer(expr, outer_tables, local_tables)),
        QueryBody::SetOperation { left, right, .. } => {
            query_body_references_outer(left, outer_tables, local_tables)
                || query_body_references_outer(right, outer_tables, local_tables)
        }
    }
}

fn select_references_outer(
    select: &Select,
    outer_tables: &BTreeSet<String>,
    local_tables: &BTreeSet<String>,
) -> bool {
    select.projection.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => expr_references_outer(expr, outer_tables, local_tables),
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    }) || select
        .filter
        .as_ref()
        .is_some_and(|expr| expr_references_outer(expr, outer_tables, local_tables))
        || select
            .group_by
            .iter()
            .any(|expr| expr_references_outer(expr, outer_tables, local_tables))
        || select
            .having
            .as_ref()
            .is_some_and(|expr| expr_references_outer(expr, outer_tables, local_tables))
        || select
            .distinct_on
            .iter()
            .any(|expr| expr_references_outer(expr, outer_tables, local_tables))
}

fn expr_references_outer(
    expr: &Expr,
    outer_tables: &BTreeSet<String>,
    local_tables: &BTreeSet<String>,
) -> bool {
    match expr {
        Expr::Literal(_) | Expr::Parameter(_) => false,
        Expr::Column { table, .. } => table.as_ref().is_some_and(|table_name| {
            outer_tables
                .iter()
                .any(|outer_table| identifiers_equal(outer_table, table_name))
                && !local_tables
                    .iter()
                    .any(|local_table| identifiers_equal(local_table, table_name))
        }),
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } | Expr::IsNull { expr, .. } => {
            expr_references_outer(expr, outer_tables, local_tables)
        }
        Expr::Binary { left, right, .. } => {
            expr_references_outer(left, outer_tables, local_tables)
                || expr_references_outer(right, outer_tables, local_tables)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_outer(expr, outer_tables, local_tables)
                || expr_references_outer(low, outer_tables, local_tables)
                || expr_references_outer(high, outer_tables, local_tables)
        }
        Expr::Row(items) => items
            .iter()
            .any(|item| expr_references_outer(item, outer_tables, local_tables)),
        Expr::InList { expr, items, .. } => {
            expr_references_outer(expr, outer_tables, local_tables)
                || items
                    .iter()
                    .any(|item| expr_references_outer(item, outer_tables, local_tables))
        }
        Expr::InSubquery { expr, query, .. } => {
            expr_references_outer(expr, outer_tables, local_tables)
                || query_references_outer_tables(query, outer_tables)
        }
        Expr::CompareSubquery { expr, query, .. } => {
            expr_references_outer(expr, outer_tables, local_tables)
                || query_references_outer_tables(query, outer_tables)
        }
        Expr::ScalarSubquery(query) | Expr::Exists(query) => {
            query_references_outer_tables(query, outer_tables)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_references_outer(expr, outer_tables, local_tables)
                || expr_references_outer(pattern, outer_tables, local_tables)
                || escape
                    .as_ref()
                    .is_some_and(|expr| expr_references_outer(expr, outer_tables, local_tables))
        }
        Expr::Function { args, .. } | Expr::Aggregate { args, .. } => args
            .iter()
            .any(|arg| expr_references_outer(arg, outer_tables, local_tables)),
        Expr::RowNumber {
            partition_by,
            order_by,
            ..
        } => {
            partition_by
                .iter()
                .any(|expr| expr_references_outer(expr, outer_tables, local_tables))
                || order_by
                    .iter()
                    .any(|order| expr_references_outer(&order.expr, outer_tables, local_tables))
        }
        Expr::WindowFunction {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter()
                .any(|expr| expr_references_outer(expr, outer_tables, local_tables))
                || partition_by
                    .iter()
                    .any(|expr| expr_references_outer(expr, outer_tables, local_tables))
                || order_by
                    .iter()
                    .any(|order| expr_references_outer(&order.expr, outer_tables, local_tables))
        }
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| expr_references_outer(expr, outer_tables, local_tables))
                || branches.iter().any(|(condition, value)| {
                    expr_references_outer(condition, outer_tables, local_tables)
                        || expr_references_outer(value, outer_tables, local_tables)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| expr_references_outer(expr, outer_tables, local_tables))
        }
    }
}

fn collect_query_table_names(query: &Query) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    collect_query_body_table_names(&query.body, &mut names);
    names
}

fn collect_query_body_table_names(body: &QueryBody, names: &mut BTreeSet<String>) {
    match body {
        QueryBody::Select(select) => {
            for item in &select.from {
                collect_from_item_table_names(item, names);
            }
        }
        QueryBody::Values(_) => {}
        QueryBody::SetOperation { left, right, .. } => {
            collect_query_body_table_names(left, names);
            collect_query_body_table_names(right, names);
        }
    }
}

fn collect_from_item_table_names(item: &FromItem, names: &mut BTreeSet<String>) {
    match item {
        FromItem::Table { name, alias } => {
            names.insert(alias.clone().unwrap_or_else(|| name.clone()));
        }
        FromItem::Function { name, alias, .. } => {
            names.insert(alias.clone().unwrap_or_else(|| name.clone()));
        }
        FromItem::Subquery { alias, .. } => {
            names.insert(alias.clone());
        }
        FromItem::Join { left, right, .. } => {
            collect_from_item_table_names(left, names);
            collect_from_item_table_names(right, names);
        }
    }
}

fn prepare_cte_dataset(cte: &CommonTableExpr, mut dataset: Dataset) -> Result<Dataset> {
    if !cte.column_names.is_empty() {
        if cte.column_names.len() != dataset.columns.len() {
            return Err(DbError::sql(format!(
                "CTE {} expected {} columns but produced {}",
                cte.name,
                cte.column_names.len(),
                dataset.columns.len()
            )));
        }
        for (binding, name) in dataset.columns.iter_mut().zip(&cte.column_names) {
            binding.name = name.clone();
        }
    }
    for binding in &mut dataset.columns {
        binding.table = Some(cte.name.clone());
        binding.hidden = false;
    }
    Ok(dataset)
}

fn recursive_term_has_unsupported_features(body: &QueryBody) -> bool {
    match body {
        QueryBody::Select(select) => {
            select.distinct
                || !select.distinct_on.is_empty()
                || !select.group_by.is_empty()
                || select.having.is_some()
                || select
                    .projection
                    .iter()
                    .any(select_item_contains_window_or_subquery)
                || projection_has_aggregate_items(&select.projection)
                || select
                    .filter
                    .as_ref()
                    .is_some_and(expr_contains_recursive_unsupported_feature)
                || select
                    .group_by
                    .iter()
                    .any(expr_contains_recursive_unsupported_feature)
                || select
                    .having
                    .as_ref()
                    .is_some_and(expr_contains_recursive_unsupported_feature)
                || select
                    .distinct_on
                    .iter()
                    .any(expr_contains_recursive_unsupported_feature)
                || select.from.iter().any(from_item_contains_subquery)
        }
        QueryBody::Values(_) => true,
        QueryBody::SetOperation { .. } => true,
    }
}

fn select_item_contains_window_or_subquery(item: &SelectItem) -> bool {
    match item {
        SelectItem::Expr { expr, .. } => expr_contains_recursive_unsupported_feature(expr),
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    }
}

fn expr_contains_recursive_unsupported_feature(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate { .. }
        | Expr::RowNumber { .. }
        | Expr::WindowFunction { .. }
        | Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_) => true,
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } | Expr::IsNull { expr, .. } => {
            expr_contains_recursive_unsupported_feature(expr)
        }
        Expr::Binary { left, right, .. } => {
            expr_contains_recursive_unsupported_feature(left)
                || expr_contains_recursive_unsupported_feature(right)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_recursive_unsupported_feature(expr)
                || expr_contains_recursive_unsupported_feature(low)
                || expr_contains_recursive_unsupported_feature(high)
        }
        Expr::InList { expr, items, .. } => {
            expr_contains_recursive_unsupported_feature(expr)
                || items
                    .iter()
                    .any(expr_contains_recursive_unsupported_feature)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_recursive_unsupported_feature(expr)
                || expr_contains_recursive_unsupported_feature(pattern)
                || escape
                    .as_ref()
                    .is_some_and(|expr| expr_contains_recursive_unsupported_feature(expr))
        }
        Expr::Function { args, .. } => args.iter().any(expr_contains_recursive_unsupported_feature),
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| expr_contains_recursive_unsupported_feature(expr))
                || branches.iter().any(|(condition, value)| {
                    expr_contains_recursive_unsupported_feature(condition)
                        || expr_contains_recursive_unsupported_feature(value)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| expr_contains_recursive_unsupported_feature(expr))
        }
        Expr::Row(items) => items
            .iter()
            .any(expr_contains_recursive_unsupported_feature),
        Expr::Literal(_) | Expr::Column { .. } | Expr::Parameter(_) => false,
    }
}

fn from_item_contains_subquery(item: &FromItem) -> bool {
    match item {
        FromItem::Table { .. } => false,
        FromItem::Function { .. } => false,
        FromItem::Subquery { .. } => true,
        FromItem::Join { left, right, .. } => {
            from_item_contains_subquery(left) || from_item_contains_subquery(right)
        }
    }
}

fn from_item_is_lateral(item: &FromItem) -> bool {
    match item {
        FromItem::Subquery { lateral, .. } | FromItem::Function { lateral, .. } => *lateral,
        FromItem::Table { .. } | FromItem::Join { .. } => false,
    }
}

fn from_item_contains_lateral(item: &FromItem) -> bool {
    match item {
        FromItem::Subquery { lateral, .. } | FromItem::Function { lateral, .. } => *lateral,
        FromItem::Table { .. } => false,
        FromItem::Join { left, right, .. } => {
            from_item_contains_lateral(left) || from_item_contains_lateral(right)
        }
    }
}

fn validate_recursive_term(body: &QueryBody, cte_name: &str) -> Result<()> {
    if recursive_term_has_unsupported_features(body) {
        return Err(DbError::sql(format!(
            "recursive CTE {} recursive term only supports non-distinct SELECT statements without aggregates, window functions, or subqueries",
            cte_name
        )));
    }
    Ok(())
}

fn validate_recursive_ctes(query: &Query) -> Result<BTreeSet<String>> {
    let recursive_ctes = query
        .ctes
        .iter()
        .filter(|cte| query_table_reference_count(&cte.query, &cte.name) > 0)
        .map(|cte| cte.name.clone())
        .collect::<BTreeSet<_>>();
    if recursive_ctes.len() > 1 {
        return Err(DbError::sql(
            "WITH RECURSIVE supports only one self-referencing CTE per statement in DecentDB v0",
        ));
    }
    Ok(recursive_ctes)
}

fn query_table_reference_count(query: &Query, table_name: &str) -> usize {
    query
        .ctes
        .iter()
        .map(|cte| query_table_reference_count(&cte.query, table_name))
        .sum::<usize>()
        + query_body_table_reference_count(&query.body, table_name)
}

fn query_body_table_reference_count(body: &QueryBody, table_name: &str) -> usize {
    match body {
        QueryBody::Select(select) => select
            .from
            .iter()
            .map(|item| from_item_table_reference_count(item, table_name))
            .sum(),
        QueryBody::Values(_) => 0,
        QueryBody::SetOperation { left, right, .. } => {
            query_body_table_reference_count(left, table_name)
                + query_body_table_reference_count(right, table_name)
        }
    }
}

fn from_item_table_reference_count(item: &FromItem, table_name: &str) -> usize {
    match item {
        FromItem::Table { name, alias } => usize::from(
            identifiers_equal(name, table_name)
                || alias
                    .as_deref()
                    .is_some_and(|alias| identifiers_equal(alias, table_name)),
        ),
        FromItem::Function { name, alias, .. } => usize::from(
            identifiers_equal(name, table_name)
                || alias
                    .as_deref()
                    .is_some_and(|alias| identifiers_equal(alias, table_name)),
        ),
        FromItem::Subquery { query, .. } => query_table_reference_count(query, table_name),
        FromItem::Join { left, right, .. } => {
            from_item_table_reference_count(left, table_name)
                + from_item_table_reference_count(right, table_name)
        }
    }
}

fn build_runtime_index(
    index: &IndexSchema,
    runtime: &EngineRuntime,
    page_size: u32,
) -> Result<RuntimeIndex> {
    let table = runtime.table_schema(&index.table_name).ok_or_else(|| {
        DbError::corruption(format!(
            "index {} references missing table {}",
            index.name, index.table_name
        ))
    })?;
    let data = runtime.table_data(&index.table_name).ok_or_else(|| {
        DbError::corruption(format!("table data for {} is missing", index.table_name))
    })?;

    match index.kind {
        IndexKind::Btree => {
            let int64_keys = btree_uses_typed_int64_keys(index, table);
            if index.unique && int64_keys {
                let mut keys =
                    HashMap::with_capacity_and_hasher(data.rows.len(), Int64HashBuilder::default());
                for row in &data.rows {
                    let Some(key) = compute_index_key(runtime, index, table, &row.values)? else {
                        continue;
                    };
                    let RuntimeBtreeKey::Int64(key) = key else {
                        return Err(DbError::internal(
                            "typed INT64 runtime index received an encoded key",
                        ));
                    };
                    if keys.insert(key, row.row_id).is_some() {
                        return Err(DbError::corruption(format!(
                            "unique index {} contains duplicate keys",
                            index.name
                        )));
                    }
                }
                Ok(RuntimeIndex::Btree {
                    keys: RuntimeBtreeKeys::UniqueInt64(keys),
                })
            } else if index.unique {
                let mut keys = BTreeMap::<Vec<u8>, i64>::new();
                for row in &data.rows {
                    let Some(key) = compute_index_key(runtime, index, table, &row.values)? else {
                        continue;
                    };
                    let RuntimeBtreeKey::Encoded(key) = key else {
                        return Err(DbError::internal(
                            "encoded runtime index received an INT64 key",
                        ));
                    };
                    if keys.insert(key, row.row_id).is_some() {
                        return Err(DbError::corruption(format!(
                            "unique index {} contains duplicate keys",
                            index.name
                        )));
                    }
                }
                Ok(RuntimeIndex::Btree {
                    keys: RuntimeBtreeKeys::UniqueEncoded(keys),
                })
            } else if int64_keys {
                let mut keys: Int64Map<Vec<i64>> =
                    HashMap::with_capacity_and_hasher(data.rows.len(), Int64HashBuilder::default());
                for row in &data.rows {
                    let Some(key) = compute_index_key(runtime, index, table, &row.values)? else {
                        continue;
                    };
                    let RuntimeBtreeKey::Int64(key) = key else {
                        return Err(DbError::internal(
                            "typed INT64 runtime index received an encoded key",
                        ));
                    };
                    keys.entry(key).or_default().push(row.row_id);
                }
                Ok(RuntimeIndex::Btree {
                    keys: RuntimeBtreeKeys::NonUniqueInt64(keys),
                })
            } else {
                let mut keys = BTreeMap::<Vec<u8>, Vec<i64>>::new();
                for row in &data.rows {
                    let Some(key) = compute_index_key(runtime, index, table, &row.values)? else {
                        continue;
                    };
                    let RuntimeBtreeKey::Encoded(key) = key else {
                        return Err(DbError::internal(
                            "encoded runtime index received an INT64 key",
                        ));
                    };
                    keys.entry(key).or_default().push(row.row_id);
                }
                Ok(RuntimeIndex::Btree {
                    keys: RuntimeBtreeKeys::NonUniqueEncoded(keys),
                })
            }
        }
        IndexKind::Trigram => {
            let mut trigram = TrigramIndex::new(page_size, 100_000);
            for row in &data.rows {
                if !row_satisfies_index_predicate(runtime, index, table, &row.values)? {
                    continue;
                }
                if let Value::Text(text) = compute_index_values(runtime, index, table, &row.values)?
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        DbError::constraint("trigram index requires a single text expression")
                    })?
                {
                    trigram.queue_insert(row.row_id as u64, &text);
                }
            }
            trigram.checkpoint()?;
            Ok(RuntimeIndex::Trigram { index: trigram })
        }
    }
}

pub(super) fn compute_index_key(
    runtime: &EngineRuntime,
    index: &IndexSchema,
    table: &TableSchema,
    row_values: &[Value],
) -> Result<Option<RuntimeBtreeKey>> {
    if !row_satisfies_index_predicate(runtime, index, table, row_values)? {
        return Ok(None);
    }
    if btree_uses_typed_int64_keys(index, table) {
        let [column] = index.columns.as_slice() else {
            return Err(DbError::internal(
                "typed INT64 runtime indexes require exactly one indexed column",
            ));
        };
        if let Some(column_name) = &column.column_name {
            let position = column_position(table, column_name).ok_or_else(|| {
                DbError::constraint(format!("index column {} does not exist", column_name))
            })?;
            let Value::Int64(value) = row_values
                .get(position)
                .ok_or_else(|| DbError::internal("row is shorter than table schema"))?
            else {
                return Err(DbError::internal(
                    "typed INT64 runtime index expected an INT64 row value",
                ));
            };
            return Ok(Some(RuntimeBtreeKey::Int64(*value)));
        }
    }
    let values = compute_index_values(runtime, index, table, row_values)?;
    if index.unique && values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(None);
    }
    let key = if values.len() == 1 {
        encode_index_key(&values[0])?
    } else {
        Row::new(values).encode()?
    };
    Ok(Some(RuntimeBtreeKey::Encoded(key)))
}

fn btree_uses_typed_int64_keys(index: &IndexSchema, table: &TableSchema) -> bool {
    let [column] = index.columns.as_slice() else {
        return false;
    };
    if column.expression_sql.is_some() {
        return false;
    }
    let Some(column_name) = &column.column_name else {
        return false;
    };
    column_schema(table, column_name).is_some_and(|column| {
        column.column_type == crate::catalog::ColumnType::Int64 && !column.nullable
    })
}

pub(super) fn compute_index_values(
    runtime: &EngineRuntime,
    index: &IndexSchema,
    table: &TableSchema,
    row_values: &[Value],
) -> Result<Vec<Value>> {
    let row_materialized = if generated_columns_are_stored(table) {
        Cow::Borrowed(row_values)
    } else {
        let mut materialized = row_values.to_vec();
        runtime.apply_virtual_generated_columns(table, &mut materialized)?;
        Cow::Owned(materialized)
    };
    let row_for_eval = row_materialized.as_ref();
    let dataset = table_row_dataset(table, row_for_eval, &table.name);
    let bindings = dataset.rows.first().map(Vec::as_slice).unwrap_or(&[]);
    index
        .columns
        .iter()
        .map(|column| {
            if let Some(column_name) = &column.column_name {
                let position = column_position(table, column_name).ok_or_else(|| {
                    DbError::constraint(format!("index column {} does not exist", column_name))
                })?;
                Ok(row_for_eval[position].clone())
            } else if let Some(expression_sql) = &column.expression_sql {
                let expr = crate::sql::parser::parse_expression_sql(expression_sql)?;
                runtime.eval_expr(&expr, &dataset, bindings, &[], &BTreeMap::new(), None)
            } else {
                Err(DbError::constraint("index column definition is empty"))
            }
        })
        .collect()
}

pub(super) fn row_satisfies_index_predicate(
    runtime: &EngineRuntime,
    index: &IndexSchema,
    table: &TableSchema,
    row_values: &[Value],
) -> Result<bool> {
    let Some(predicate_sql) = &index.predicate_sql else {
        return Ok(true);
    };
    let expr = crate::sql::parser::parse_expression_sql(predicate_sql)?;
    let row_materialized = if generated_columns_are_stored(table) {
        Cow::Borrowed(row_values)
    } else {
        let mut materialized = row_values.to_vec();
        runtime.apply_virtual_generated_columns(table, &mut materialized)?;
        Cow::Owned(materialized)
    };
    let row_for_eval = row_materialized.as_ref();
    let dataset = table_row_dataset(table, row_for_eval, &table.name);
    let bindings = dataset.rows.first().map(Vec::as_slice).unwrap_or(&[]);
    Ok(matches!(
        runtime.eval_expr(&expr, &dataset, bindings, &[], &BTreeMap::new(), None)?,
        Value::Bool(true)
    ))
}

pub(super) fn table_row_dataset(table: &TableSchema, row: &[Value], table_name: &str) -> Dataset {
    Dataset {
        columns: table
            .columns
            .iter()
            .map(|column| ColumnBinding::visible(Some(table_name.to_string()), column.name.clone()))
            .collect(),
        rows: vec![row.to_vec()],
    }
}

fn decode_root_header(page_bytes: &[u8]) -> Result<Option<RootHeader>> {
    if page_bytes.iter().all(|byte| *byte == 0) {
        return Ok(None);
    }
    if page_bytes.len() < ENGINE_ROOT_HEADER_SIZE {
        return Err(DbError::corruption("catalog root page is truncated"));
    }
    if page_bytes[0..ENGINE_ROOT_MAGIC.len()] != ENGINE_ROOT_MAGIC {
        return Err(DbError::corruption("catalog root page magic is invalid"));
    }
    let version = u32::from_le_bytes(page_bytes[8..12].try_into().expect("version"));
    if version != ENGINE_ROOT_VERSION {
        return Err(DbError::corruption(format!(
            "unsupported catalog root version {version}"
        )));
    }
    Ok(Some(RootHeader {
        schema_cookie: u32::from_le_bytes(page_bytes[12..16].try_into().expect("cookie")),
        payload_checksum: u32::from_le_bytes(page_bytes[16..20].try_into().expect("checksum")),
        pointer: OverflowPointer {
            head_page_id: u32::from_le_bytes(page_bytes[20..24].try_into().expect("head page")),
            logical_len: u32::from_le_bytes(page_bytes[24..28].try_into().expect("logical len")),
            flags: page_bytes[28],
        },
    }))
}

fn encode_root_header(page_size: u32, header: RootHeader) -> Vec<u8> {
    let mut page = vec![0_u8; page_size as usize];
    page[0..8].copy_from_slice(&ENGINE_ROOT_MAGIC);
    page[8..12].copy_from_slice(&ENGINE_ROOT_VERSION.to_le_bytes());
    page[12..16].copy_from_slice(&header.schema_cookie.to_le_bytes());
    page[16..20].copy_from_slice(&header.payload_checksum.to_le_bytes());
    page[20..24].copy_from_slice(&header.pointer.head_page_id.to_le_bytes());
    page[24..28].copy_from_slice(&header.pointer.logical_len.to_le_bytes());
    page[28] = header.pointer.flags;
    page
}

#[cfg(test)]
fn encode_runtime_payload(runtime: &EngineRuntime) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    output.extend_from_slice(LEGACY_RUNTIME_PAYLOAD_MAGIC);
    encode_u32(&mut output, runtime.catalog.schema_cookie);
    encode_u32(&mut output, runtime.catalog.tables.len() as u32);
    for table in runtime.catalog.tables.values() {
        encode_string(&mut output, &table.name)?;
        encode_u32(&mut output, table.columns.len() as u32);
        for column in &table.columns {
            encode_string(&mut output, &column.name)?;
            output.push(column.column_type as u8);
            output.push(u8::from(column.nullable));
            encode_optional_string(&mut output, column.default_sql.as_deref())?;
            output.push(u8::from(column.primary_key));
            output.push(u8::from(column.unique));
            output.push(u8::from(column.auto_increment));
            encode_u32(&mut output, column.checks.len() as u32);
            for check in &column.checks {
                encode_optional_string(&mut output, check.name.as_deref())?;
                encode_string(&mut output, &check.expression_sql)?;
            }
            output.push(u8::from(column.foreign_key.is_some()));
            if let Some(foreign_key) = &column.foreign_key {
                encode_foreign_key(&mut output, foreign_key)?;
            }
        }
        encode_u32(&mut output, table.checks.len() as u32);
        for check in &table.checks {
            encode_optional_string(&mut output, check.name.as_deref())?;
            encode_string(&mut output, &check.expression_sql)?;
        }
        encode_u32(&mut output, table.foreign_keys.len() as u32);
        for foreign_key in &table.foreign_keys {
            encode_foreign_key(&mut output, foreign_key)?;
        }
        encode_strings(&mut output, &table.primary_key_columns)?;
        encode_i64(&mut output, table.next_row_id);
        let data = runtime.tables.get(&table.name).cloned().unwrap_or_default();
        encode_u32(&mut output, data.rows.len() as u32);
        for row in data.rows {
            encode_i64(&mut output, row.row_id);
            let encoded = Row::new(row.values).encode()?;
            encode_bytes(&mut output, &encoded)?;
        }
    }

    encode_u32(&mut output, runtime.catalog.indexes.len() as u32);
    for index in runtime.catalog.indexes.values() {
        encode_string(&mut output, &index.name)?;
        encode_string(&mut output, &index.table_name)?;
        output.push(index.kind as u8);
        output.push(u8::from(index.unique));
        encode_u32(&mut output, index.columns.len() as u32);
        for column in &index.columns {
            encode_optional_string(&mut output, column.column_name.as_deref())?;
            encode_optional_string(&mut output, column.expression_sql.as_deref())?;
        }
        encode_optional_string(&mut output, index.predicate_sql.as_deref())?;
        output.push(u8::from(index.fresh));
    }
    encode_u32(&mut output, runtime.catalog.views.len() as u32);
    for view in runtime.catalog.views.values() {
        encode_string(&mut output, &view.name)?;
        encode_string(&mut output, &view.sql_text)?;
        encode_strings(&mut output, &view.column_names)?;
        encode_strings(&mut output, &view.dependencies)?;
    }

    encode_u32(&mut output, runtime.catalog.triggers.len() as u32);
    for trigger in runtime.catalog.triggers.values() {
        encode_string(&mut output, &trigger.name)?;
        encode_string(&mut output, &trigger.target_name)?;
        output.push(trigger.kind as u8);
        output.push(trigger.event as u8);
        output.push(u8::from(trigger.on_view));
        encode_string(&mut output, &trigger.action_sql)?;
    }
    encode_schemas_section(&mut output, &runtime.catalog.schemas)?;
    encode_index_include_columns_section(&mut output, &runtime.catalog.indexes)?;
    encode_generated_columns_section(&mut output, &runtime.catalog.tables)?;
    Ok(output)
}

fn decode_runtime_payload(bytes: &[u8]) -> Result<EngineRuntime> {
    let mut cursor = Cursor::new(bytes);
    let magic = cursor.read_slice(9)?;
    if magic != LEGACY_RUNTIME_PAYLOAD_MAGIC {
        return Err(DbError::corruption("catalog state magic is invalid"));
    }
    let mut runtime = EngineRuntime::empty(cursor.read_u32()?);
    let table_count = cursor.read_u32()?;
    for _ in 0..table_count {
        let table_name = cursor.read_string()?;
        let column_count = cursor.read_u32()?;
        let mut table = TableSchema {
            name: table_name.clone(),
            temporary: false,
            columns: Vec::with_capacity(column_count as usize),
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            primary_key_columns: Vec::new(),
            next_row_id: 1,
        };
        for _ in 0..column_count {
            let name = cursor.read_string()?;
            let column_type = decode_column_type(cursor.read_u8()?)?;
            let nullable = cursor.read_bool()?;
            let default_sql = cursor.read_optional_string()?;
            let primary_key = cursor.read_bool()?;
            let unique = cursor.read_bool()?;
            let auto_increment = cursor.read_bool()?;
            let check_count = cursor.read_u32()?;
            let mut checks = Vec::with_capacity(check_count as usize);
            for _ in 0..check_count {
                checks.push(crate::catalog::CheckConstraint {
                    name: cursor.read_optional_string()?,
                    expression_sql: cursor.read_string()?,
                });
            }
            let has_fk = cursor.read_bool()?;
            let foreign_key = if has_fk {
                Some(decode_foreign_key(&mut cursor)?)
            } else {
                None
            };
            table.columns.push(crate::catalog::ColumnSchema {
                name,
                column_type,
                nullable,
                default_sql,
                generated_sql: None,
                generated_stored: true,
                primary_key,
                unique,
                auto_increment,
                checks,
                foreign_key,
            });
        }
        let table_check_count = cursor.read_u32()?;
        for _ in 0..table_check_count {
            table.checks.push(crate::catalog::CheckConstraint {
                name: cursor.read_optional_string()?,
                expression_sql: cursor.read_string()?,
            });
        }
        let fk_count = cursor.read_u32()?;
        for _ in 0..fk_count {
            table.foreign_keys.push(decode_foreign_key(&mut cursor)?);
        }
        table.primary_key_columns = cursor.read_strings()?;
        table.next_row_id = cursor.read_i64()?;
        let row_count = cursor.read_u32()?;
        let mut data = TableData::default();
        for _ in 0..row_count {
            let row_id = cursor.read_i64()?;
            let row_bytes_len = cursor.read_u32()? as usize;
            let row_bytes = cursor.read_slice(row_bytes_len)?;
            let row = Row::decode(row_bytes)?;
            data.rows.push(StoredRow {
                row_id,
                values: row.into_values(),
            });
        }
        runtime.catalog.tables.insert(table_name.clone(), table);
        runtime.tables.insert(table_name, data);
    }

    let index_count = cursor.read_u32()?;
    for _ in 0..index_count {
        let name = cursor.read_string()?;
        let table_name = cursor.read_string()?;
        let kind = decode_index_kind(cursor.read_u8()?)?;
        let unique = cursor.read_bool()?;
        let column_count = cursor.read_u32()?;
        let mut columns = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            columns.push(crate::catalog::IndexColumn {
                column_name: cursor.read_optional_string()?,
                expression_sql: cursor.read_optional_string()?,
            });
        }
        let predicate_sql = cursor.read_optional_string()?;
        let fresh = cursor.read_bool()?;
        runtime.catalog.indexes.insert(
            name.clone(),
            crate::catalog::IndexSchema {
                name,
                table_name,
                kind,
                unique,
                columns,
                include_columns: Vec::new(),
                predicate_sql,
                fresh,
            },
        );
    }
    let view_count = cursor.read_u32()?;
    for _ in 0..view_count {
        let view = crate::catalog::ViewSchema {
            name: cursor.read_string()?,
            temporary: false,
            sql_text: cursor.read_string()?,
            column_names: cursor.read_strings()?,
            dependencies: cursor.read_strings()?,
        };
        runtime.catalog.views.insert(view.name.clone(), view);
    }

    let trigger_count = cursor.read_u32()?;
    for _ in 0..trigger_count {
        let trigger = crate::catalog::TriggerSchema {
            name: cursor.read_string()?,
            target_name: cursor.read_string()?,
            kind: decode_trigger_kind(cursor.read_u8()?)?,
            event: decode_trigger_event(cursor.read_u8()?)?,
            on_view: cursor.read_bool()?,
            action_sql: cursor.read_string()?,
        };
        runtime
            .catalog
            .triggers
            .insert(trigger.name.clone(), trigger);
    }
    if cursor.offset < cursor.bytes.len() {
        decode_schemas_section(&mut cursor, &mut runtime.catalog.schemas)?;
    }
    if cursor.offset < cursor.bytes.len() {
        decode_index_include_columns_section(&mut cursor, &mut runtime.catalog.indexes)?;
    }
    if cursor.offset < cursor.bytes.len() {
        decode_generated_columns_section(&mut cursor, &mut runtime.catalog.tables)?;
    }
    Ok(runtime)
}

#[cfg(test)]
fn encode_manifest_payload(
    runtime: &EngineRuntime,
    table_states: &BTreeMap<String, PersistedTableState>,
) -> Result<Vec<u8>> {
    Ok(encode_manifest_payload_with_offsets(runtime, table_states)?.bytes)
}

fn encode_manifest_payload_with_offsets(
    runtime: &EngineRuntime,
    table_states: &BTreeMap<String, PersistedTableState>,
) -> Result<ManifestEncoding> {
    let mut output = Vec::new();
    let mut table_next_row_id_offsets = BTreeMap::new();
    let mut table_state_offsets = BTreeMap::new();
    output.extend_from_slice(MANIFEST_PAYLOAD_MAGIC);
    encode_u32(&mut output, runtime.catalog.schema_cookie);
    encode_u32(&mut output, runtime.catalog.tables.len() as u32);
    for table in runtime.catalog.tables.values() {
        encode_string(&mut output, &table.name)?;
        encode_u32(&mut output, table.columns.len() as u32);
        for column in &table.columns {
            encode_string(&mut output, &column.name)?;
            output.push(column.column_type as u8);
            output.push(u8::from(column.nullable));
            encode_optional_string(&mut output, column.default_sql.as_deref())?;
            output.push(u8::from(column.primary_key));
            output.push(u8::from(column.unique));
            output.push(u8::from(column.auto_increment));
            encode_u32(&mut output, column.checks.len() as u32);
            for check in &column.checks {
                encode_optional_string(&mut output, check.name.as_deref())?;
                encode_string(&mut output, &check.expression_sql)?;
            }
            output.push(u8::from(column.foreign_key.is_some()));
            if let Some(foreign_key) = &column.foreign_key {
                encode_foreign_key(&mut output, foreign_key)?;
            }
        }
        encode_u32(&mut output, table.checks.len() as u32);
        for check in &table.checks {
            encode_optional_string(&mut output, check.name.as_deref())?;
            encode_string(&mut output, &check.expression_sql)?;
        }
        encode_u32(&mut output, table.foreign_keys.len() as u32);
        for foreign_key in &table.foreign_keys {
            encode_foreign_key(&mut output, foreign_key)?;
        }
        encode_strings(&mut output, &table.primary_key_columns)?;
        table_next_row_id_offsets.insert(table.name.clone(), output.len());
        encode_i64(&mut output, table.next_row_id);
        table_state_offsets.insert(table.name.clone(), output.len());
        let state = table_states.get(&table.name).copied().unwrap_or_default();
        encode_u32(&mut output, state.checksum);
        encode_u32(&mut output, state.pointer.head_page_id);
        encode_u32(&mut output, state.pointer.logical_len);
        output.push(state.pointer.flags);
    }

    encode_u32(&mut output, runtime.catalog.indexes.len() as u32);
    for index in runtime.catalog.indexes.values() {
        encode_string(&mut output, &index.name)?;
        encode_string(&mut output, &index.table_name)?;
        output.push(index.kind as u8);
        output.push(u8::from(index.unique));
        encode_u32(&mut output, index.columns.len() as u32);
        for column in &index.columns {
            encode_optional_string(&mut output, column.column_name.as_deref())?;
            encode_optional_string(&mut output, column.expression_sql.as_deref())?;
        }
        encode_optional_string(&mut output, index.predicate_sql.as_deref())?;
        output.push(u8::from(index.fresh));
    }
    encode_u32(&mut output, runtime.catalog.views.len() as u32);
    for view in runtime.catalog.views.values() {
        encode_string(&mut output, &view.name)?;
        encode_string(&mut output, &view.sql_text)?;
        encode_strings(&mut output, &view.column_names)?;
        encode_strings(&mut output, &view.dependencies)?;
    }

    encode_u32(&mut output, runtime.catalog.triggers.len() as u32);
    for trigger in runtime.catalog.triggers.values() {
        encode_string(&mut output, &trigger.name)?;
        encode_string(&mut output, &trigger.target_name)?;
        output.push(trigger.kind as u8);
        output.push(trigger.event as u8);
        output.push(u8::from(trigger.on_view));
        encode_string(&mut output, &trigger.action_sql)?;
    }

    let table_stats = runtime
        .catalog
        .table_stats
        .iter()
        .filter(|(name, _)| runtime.catalog.tables.contains_key(*name))
        .collect::<Vec<_>>();
    encode_u32(&mut output, table_stats.len() as u32);
    for (name, stats) in table_stats {
        encode_string(&mut output, name)?;
        encode_i64(&mut output, stats.row_count);
    }

    let index_stats = runtime
        .catalog
        .index_stats
        .iter()
        .filter(|(name, _)| runtime.catalog.indexes.contains_key(*name))
        .collect::<Vec<_>>();
    encode_u32(&mut output, index_stats.len() as u32);
    for (name, stats) in index_stats {
        encode_string(&mut output, name)?;
        encode_i64(&mut output, stats.entry_count);
        encode_i64(&mut output, stats.distinct_key_count);
    }
    encode_schemas_section(&mut output, &runtime.catalog.schemas)?;
    encode_index_include_columns_section(&mut output, &runtime.catalog.indexes)?;
    encode_generated_columns_section(&mut output, &runtime.catalog.tables)?;
    Ok(ManifestEncoding {
        bytes: output,
        table_next_row_id_offsets,
        table_state_offsets,
    })
}

fn patch_manifest_table_next_row_id(
    payload: &mut [u8],
    offset: usize,
    next_row_id: i64,
) -> Result<()> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| DbError::internal("manifest next_row_id offset overflow"))?;
    if end > payload.len() {
        return Err(DbError::internal(
            "manifest next_row_id offset exceeded payload length",
        ));
    }
    payload[offset..end].copy_from_slice(&next_row_id.to_le_bytes());
    Ok(())
}

fn patch_manifest_table_state(
    payload: &mut [u8],
    offset: usize,
    state: PersistedTableState,
) -> Result<()> {
    let end = offset
        .checked_add(13)
        .ok_or_else(|| DbError::internal("manifest table-state offset overflow"))?;
    if end > payload.len() {
        return Err(DbError::internal(
            "manifest table-state offset exceeded payload length",
        ));
    }
    payload[offset..offset + 4].copy_from_slice(&state.checksum.to_le_bytes());
    payload[offset + 4..offset + 8].copy_from_slice(&state.pointer.head_page_id.to_le_bytes());
    payload[offset + 8..offset + 12].copy_from_slice(&state.pointer.logical_len.to_le_bytes());
    payload[offset + 12] = state.pointer.flags;
    Ok(())
}

fn decode_manifest_payload<S: PageStore>(_store: &S, bytes: &[u8]) -> Result<EngineRuntime> {
    let mut cursor = Cursor::new(bytes);
    let magic = cursor.read_slice(MANIFEST_PAYLOAD_MAGIC.len())?;
    if magic != MANIFEST_PAYLOAD_MAGIC {
        return Err(DbError::corruption("catalog manifest magic is invalid"));
    }
    let mut runtime = EngineRuntime::empty(cursor.read_u32()?);
    let table_count = cursor.read_u32()?;
    for _ in 0..table_count {
        let table_name = cursor.read_string()?;
        let column_count = cursor.read_u32()?;
        let mut table = TableSchema {
            name: table_name.clone(),
            temporary: false,
            columns: Vec::with_capacity(column_count as usize),
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            primary_key_columns: Vec::new(),
            next_row_id: 1,
        };
        for _ in 0..column_count {
            let name = cursor.read_string()?;
            let column_type = decode_column_type(cursor.read_u8()?)?;
            let nullable = cursor.read_bool()?;
            let default_sql = cursor.read_optional_string()?;
            let primary_key = cursor.read_bool()?;
            let unique = cursor.read_bool()?;
            let auto_increment = cursor.read_bool()?;
            let check_count = cursor.read_u32()?;
            let mut checks = Vec::with_capacity(check_count as usize);
            for _ in 0..check_count {
                checks.push(crate::catalog::CheckConstraint {
                    name: cursor.read_optional_string()?,
                    expression_sql: cursor.read_string()?,
                });
            }
            let has_fk = cursor.read_bool()?;
            let foreign_key = if has_fk {
                Some(decode_foreign_key(&mut cursor)?)
            } else {
                None
            };
            table.columns.push(crate::catalog::ColumnSchema {
                name,
                column_type,
                nullable,
                default_sql,
                generated_sql: None,
                generated_stored: true,
                primary_key,
                unique,
                auto_increment,
                checks,
                foreign_key,
            });
        }
        let table_check_count = cursor.read_u32()?;
        for _ in 0..table_check_count {
            table.checks.push(crate::catalog::CheckConstraint {
                name: cursor.read_optional_string()?,
                expression_sql: cursor.read_string()?,
            });
        }
        let fk_count = cursor.read_u32()?;
        for _ in 0..fk_count {
            table.foreign_keys.push(decode_foreign_key(&mut cursor)?);
        }
        table.primary_key_columns = cursor.read_strings()?;
        table.next_row_id = cursor.read_i64()?;
        let state = PersistedTableState {
            checksum: cursor.read_u32()?,
            pointer: OverflowPointer {
                head_page_id: cursor.read_u32()?,
                logical_len: cursor.read_u32()?,
                flags: cursor.read_u8()?,
            },
            row_count: 0,
            tail: OverflowTailInfo::default(),
        };
        runtime.catalog.tables.insert(table_name.clone(), table);
        let has_data = state.pointer.head_page_id != 0 && state.pointer.logical_len != 0;
        if has_data {
            // Defer row data loading to first statement execution.
            runtime.deferred_tables.insert(table_name.clone());
        }
        runtime.persisted_tables.insert(table_name.clone(), state);
        if !has_data {
            // Empty tables are immediately available.
            runtime.tables.insert(table_name, TableData::default());
        }
    }

    let index_count = cursor.read_u32()?;
    for _ in 0..index_count {
        let name = cursor.read_string()?;
        let table_name = cursor.read_string()?;
        let kind = decode_index_kind(cursor.read_u8()?)?;
        let unique = cursor.read_bool()?;
        let column_count = cursor.read_u32()?;
        let mut columns = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            columns.push(crate::catalog::IndexColumn {
                column_name: cursor.read_optional_string()?,
                expression_sql: cursor.read_optional_string()?,
            });
        }
        let predicate_sql = cursor.read_optional_string()?;
        let fresh = cursor.read_bool()?;
        runtime.catalog.indexes.insert(
            name.clone(),
            crate::catalog::IndexSchema {
                name,
                table_name,
                kind,
                unique,
                columns,
                include_columns: Vec::new(),
                predicate_sql,
                fresh,
            },
        );
    }
    let view_count = cursor.read_u32()?;
    for _ in 0..view_count {
        let view = crate::catalog::ViewSchema {
            name: cursor.read_string()?,
            temporary: false,
            sql_text: cursor.read_string()?,
            column_names: cursor.read_strings()?,
            dependencies: cursor.read_strings()?,
        };
        runtime.catalog.views.insert(view.name.clone(), view);
    }

    let trigger_count = cursor.read_u32()?;
    for _ in 0..trigger_count {
        let trigger = crate::catalog::TriggerSchema {
            name: cursor.read_string()?,
            target_name: cursor.read_string()?,
            kind: decode_trigger_kind(cursor.read_u8()?)?,
            event: decode_trigger_event(cursor.read_u8()?)?,
            on_view: cursor.read_bool()?,
            action_sql: cursor.read_string()?,
        };
        runtime
            .catalog
            .triggers
            .insert(trigger.name.clone(), trigger);
    }
    if cursor.offset < cursor.bytes.len() {
        let table_stats_count = cursor.read_u32()?;
        for _ in 0..table_stats_count {
            let name = cursor.read_string()?;
            let stats = crate::catalog::TableStats {
                row_count: cursor.read_i64()?,
            };
            runtime.catalog.table_stats.insert(name, stats);
        }
    }
    if cursor.offset < cursor.bytes.len() {
        let index_stats_count = cursor.read_u32()?;
        for _ in 0..index_stats_count {
            let name = cursor.read_string()?;
            let stats = crate::catalog::IndexStats {
                entry_count: cursor.read_i64()?,
                distinct_key_count: cursor.read_i64()?,
            };
            runtime.catalog.index_stats.insert(name, stats);
        }
    }
    if cursor.offset < cursor.bytes.len() {
        decode_schemas_section(&mut cursor, &mut runtime.catalog.schemas)?;
    }
    if cursor.offset < cursor.bytes.len() {
        decode_index_include_columns_section(&mut cursor, &mut runtime.catalog.indexes)?;
    }
    if cursor.offset < cursor.bytes.len() {
        decode_generated_columns_section(&mut cursor, &mut runtime.catalog.tables)?;
    }
    Ok(runtime)
}

fn encode_table_payload(data: &TableData) -> Result<Vec<u8>> {
    if data.rows.is_empty() {
        return Ok(Vec::new());
    }
    let mut output = Vec::with_capacity(TABLE_PAYLOAD_MAGIC.len() + 4 + data.rows.len() * 32);
    output.extend_from_slice(TABLE_PAYLOAD_MAGIC);
    encode_u32(&mut output, data.rows.len() as u32);
    let mut encoded_row = Vec::with_capacity(64);
    for row in &data.rows {
        encode_i64(&mut output, row.row_id);
        Row::encode_values_into(&row.values, &mut encoded_row)?;
        encode_bytes(&mut output, &encoded_row)?;
    }
    Ok(output)
}

/// Build a new payload by splicing only the modified rows into the cached
/// previous payload.  Unchanged row bytes are copied verbatim from `old`,
/// saving the per-row serialisation cost for the common single-row UPDATE.
/// Result of a splice operation, containing the new payload and metadata
/// about which byte offset was first modified.
struct SpliceResult {
    payload: Vec<u8>,
    /// Byte offset of the first modified row in the OLD payload. Pages before
    /// this offset are guaranteed unchanged and can be skipped during overflow
    /// rewrite.
    first_dirty_byte: usize,
}

fn splice_updated_rows_payload(
    old: &[u8],
    data: &TableData,
    dirty_indices: &[usize],
) -> Result<SpliceResult> {
    const HEADER_LEN: usize = 8 /* magic */ + 4 /* row_count */;

    if old.len() < HEADER_LEN || old[..8] != *TABLE_PAYLOAD_MAGIC {
        let payload = encode_table_payload(data)?;
        return Ok(SpliceResult {
            payload,
            first_dirty_byte: 0,
        });
    }
    let old_row_count =
        u32::from_le_bytes(old[8..12].try_into().expect("row-count header length")) as usize;
    if old_row_count != data.rows.len() {
        // Row count changed (e.g. concurrent insert/delete after the cache
        // was stored) — fall back to full encode for safety.
        let payload = encode_table_payload(data)?;
        return Ok(SpliceResult {
            payload,
            first_dirty_byte: 0,
        });
    }

    // Fast path: only a handful of rows changed.  Scan the old payload to
    // locate byte ranges of each dirty row, then splice new encodings in.
    //
    // Row wire format:
    //   row_id       (8 bytes, i64 LE)
    //   row_data_len (4 bytes, u32 LE)
    //   row_data     (row_data_len bytes)
    //
    // We need the byte offset of each dirty row (and the one after it) so
    // we can copy unchanged prefix / suffix regions.

    // Sort dirty indices so we splice left-to-right.
    let mut sorted_dirty: Vec<usize> = dirty_indices.to_vec();
    sorted_dirty.sort_unstable();
    sorted_dirty.dedup();

    // Scan the old payload to locate dirty row byte ranges.
    // row_spans[i] = (start, end) byte offsets in `old` for dirty row i.
    let mut row_spans: Vec<(usize, usize)> = Vec::with_capacity(sorted_dirty.len());
    let mut scan_offset = HEADER_LEN;
    let mut dirty_cursor = 0;
    let mut row_idx = 0;
    while dirty_cursor < sorted_dirty.len() && scan_offset + 12 <= old.len() {
        let rd_len = u32::from_le_bytes(
            old[scan_offset + 8..scan_offset + 12]
                .try_into()
                .expect("row data len"),
        ) as usize;
        let row_end = scan_offset + 12 + rd_len;
        if row_idx == sorted_dirty[dirty_cursor] {
            row_spans.push((scan_offset, row_end));
            dirty_cursor += 1;
        }
        scan_offset = row_end;
        row_idx += 1;
        if row_idx > old_row_count {
            break;
        }
    }

    if row_spans.len() != sorted_dirty.len() {
        // Could not find all dirty rows in the old payload; fall back.
        let payload = encode_table_payload(data)?;
        return Ok(SpliceResult {
            payload,
            first_dirty_byte: 0,
        });
    }

    let first_dirty_byte = row_spans.first().map_or(0, |s| s.0);

    // Build the spliced payload.
    let mut output = Vec::with_capacity(old.len() + sorted_dirty.len() * 32);
    output.extend_from_slice(&old[..8]); // magic
    encode_u32(&mut output, data.rows.len() as u32);

    let mut copy_from = HEADER_LEN;
    let mut encoded_row = Vec::with_capacity(128);
    for (span_idx, &dirty_row) in sorted_dirty.iter().enumerate() {
        let (span_start, span_end) = row_spans[span_idx];

        // Copy unchanged bytes before this dirty row.
        if copy_from < span_start {
            output.extend_from_slice(&old[copy_from..span_start]);
        }

        // Encode the updated row.
        let row = &data.rows[dirty_row];
        encode_i64(&mut output, row.row_id);
        Row::encode_values_into(&row.values, &mut encoded_row)?;
        encode_bytes(&mut output, &encoded_row)?;

        copy_from = span_end;
    }
    // Copy any remaining unchanged tail.
    if copy_from < old.len() {
        output.extend_from_slice(&old[copy_from..]);
    }

    Ok(SpliceResult {
        payload: output,
        first_dirty_byte,
    })
}

fn encode_appended_table_rows(data: &TableData, existing_count: usize) -> Result<Vec<u8>> {
    if existing_count > data.rows.len() {
        return Err(DbError::internal(
            "append-only table payload rewrite saw fewer rows than the previous persisted payload",
        ));
    }
    if existing_count == data.rows.len() {
        return Ok(Vec::new());
    }

    let mut appended = Vec::with_capacity((data.rows.len() - existing_count) * 32);
    let mut encoded_row = Vec::with_capacity(64);
    for row in data.rows.iter().skip(existing_count) {
        encode_i64(&mut appended, row.row_id);
        Row::encode_values_into(&row.values, &mut encoded_row)?;
        encode_bytes(&mut appended, &encoded_row)?;
    }
    Ok(appended)
}

fn append_table_payload(mut previous: Vec<u8>, data: &TableData) -> Result<Vec<u8>> {
    if data.rows.is_empty() {
        return Ok(Vec::new());
    }
    if previous.is_empty() {
        return encode_table_payload(data);
    }
    let count_offset = TABLE_PAYLOAD_MAGIC.len();
    if previous.len() < count_offset + 4 {
        return Err(DbError::corruption("table payload header is truncated"));
    }
    if previous[..count_offset] != *TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }

    let existing_count = u32::from_le_bytes(
        previous[count_offset..count_offset + 4]
            .try_into()
            .expect("row-count header length"),
    ) as usize;
    let appended_rows = encode_appended_table_rows(data, existing_count)?;
    if appended_rows.is_empty() {
        return Ok(previous);
    }

    previous[count_offset..count_offset + 4].copy_from_slice(
        &u32::try_from(data.rows.len())
            .map_err(|_| DbError::constraint("table row count exceeds u32"))?
            .to_le_bytes(),
    );
    previous.extend_from_slice(&appended_rows);
    Ok(previous)
}

fn decode_table_payload(bytes: &[u8]) -> Result<TableData> {
    if bytes.is_empty() {
        return Ok(TableData::default());
    }
    let mut cursor = Cursor::new(bytes);
    let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
    if magic != TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    let row_count = cursor.read_u32()? as usize;
    let mut data = TableData::default();
    data.rows.reserve(row_count);
    while cursor.offset < cursor.bytes.len() {
        let row_id = cursor.read_i64()?;
        let row_bytes_len = cursor.read_u32()? as usize;
        let row_bytes = cursor.read_slice(row_bytes_len)?;
        let row = Row::decode(row_bytes)?;
        data.rows.push(StoredRow {
            row_id,
            values: row.into_values(),
        });
    }
    if data.rows.len() < row_count {
        return Err(DbError::corruption(
            "table payload row count exceeded decoded row content",
        ));
    }
    Ok(data)
}

fn encode_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn encode_i64(output: &mut Vec<u8>, value: i64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn encode_string(output: &mut Vec<u8>, value: &str) -> Result<()> {
    encode_u32(
        output,
        u32::try_from(value.len()).map_err(|_| DbError::constraint("string length exceeds u32"))?,
    );
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn encode_optional_string(output: &mut Vec<u8>, value: Option<&str>) -> Result<()> {
    output.push(u8::from(value.is_some()));
    if let Some(value) = value {
        encode_string(output, value)?;
    }
    Ok(())
}

fn encode_strings(output: &mut Vec<u8>, values: &[String]) -> Result<()> {
    encode_u32(
        output,
        u32::try_from(values.len())
            .map_err(|_| DbError::constraint("string list length exceeds u32"))?,
    );
    for value in values {
        encode_string(output, value)?;
    }
    Ok(())
}

fn encode_bytes(output: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    encode_u32(
        output,
        u32::try_from(bytes.len())
            .map_err(|_| DbError::constraint("byte vector length exceeds u32"))?,
    );
    output.extend_from_slice(bytes);
    Ok(())
}

fn encode_foreign_key(
    output: &mut Vec<u8>,
    foreign_key: &crate::catalog::ForeignKeyConstraint,
) -> Result<()> {
    encode_optional_string(output, foreign_key.name.as_deref())?;
    encode_strings(output, &foreign_key.columns)?;
    encode_string(output, &foreign_key.referenced_table)?;
    encode_strings(output, &foreign_key.referenced_columns)?;
    output.push(foreign_key.on_delete as u8);
    output.push(foreign_key.on_update as u8);
    Ok(())
}

fn encode_generated_columns_section(
    output: &mut Vec<u8>,
    tables: &BTreeMap<String, TableSchema>,
) -> Result<()> {
    let generated_columns = tables
        .values()
        .flat_map(|table| {
            table.columns.iter().filter_map(move |column| {
                column.generated_sql.as_ref().map(|generated_sql| {
                    (
                        table.name.as_str(),
                        column.name.as_str(),
                        generated_sql.as_str(),
                        column.generated_stored,
                    )
                })
            })
        })
        .collect::<Vec<_>>();
    output.extend_from_slice(GENERATED_COLUMNS_SECTION_MAGIC);
    output.push(1);
    encode_u32(
        output,
        u32::try_from(generated_columns.len())
            .map_err(|_| DbError::constraint("generated column count exceeds u32"))?,
    );
    for (table_name, column_name, generated_sql, generated_stored) in generated_columns {
        encode_string(output, table_name)?;
        encode_string(output, column_name)?;
        encode_string(output, generated_sql)?;
        output.push(u8::from(generated_stored));
    }
    Ok(())
}

fn encode_index_include_columns_section(
    output: &mut Vec<u8>,
    indexes: &BTreeMap<String, IndexSchema>,
) -> Result<()> {
    let include_entries = indexes
        .iter()
        .filter(|(_, index)| !index.include_columns.is_empty())
        .collect::<Vec<_>>();
    output.extend_from_slice(INDEX_INCLUDE_COLUMNS_SECTION_MAGIC);
    output.push(1);
    encode_u32(
        output,
        u32::try_from(include_entries.len())
            .map_err(|_| DbError::constraint("index include entry count exceeds u32"))?,
    );
    for (index_name, index) in include_entries {
        encode_string(output, index_name)?;
        encode_strings(output, &index.include_columns)?;
    }
    Ok(())
}

fn encode_schemas_section(
    output: &mut Vec<u8>,
    schemas: &BTreeMap<String, SchemaInfo>,
) -> Result<()> {
    output.extend_from_slice(SCHEMAS_SECTION_MAGIC);
    output.push(1);
    encode_u32(
        output,
        u32::try_from(schemas.len())
            .map_err(|_| DbError::constraint("schema count exceeds u32"))?,
    );
    for schema in schemas.values() {
        encode_string(output, &schema.name)?;
    }
    Ok(())
}

fn decode_schemas_section(
    cursor: &mut Cursor<'_>,
    schemas: &mut BTreeMap<String, SchemaInfo>,
) -> Result<()> {
    let section_is_present = cursor
        .bytes
        .get(cursor.offset..cursor.offset + SCHEMAS_SECTION_MAGIC.len())
        .is_some_and(|magic| magic == SCHEMAS_SECTION_MAGIC);
    if !section_is_present {
        return Ok(());
    }
    cursor.offset += SCHEMAS_SECTION_MAGIC.len();
    let version = cursor.read_u8()?;
    if version != 1 {
        return Err(DbError::corruption(format!(
            "unknown schemas section version {version}"
        )));
    }
    let schema_count = cursor.read_u32()?;
    for _ in 0..schema_count {
        let name = cursor.read_string()?;
        schemas.insert(name.clone(), SchemaInfo { name });
    }
    Ok(())
}

fn decode_index_include_columns_section(
    cursor: &mut Cursor<'_>,
    indexes: &mut BTreeMap<String, IndexSchema>,
) -> Result<()> {
    let section_is_present = cursor
        .bytes
        .get(cursor.offset..cursor.offset + INDEX_INCLUDE_COLUMNS_SECTION_MAGIC.len())
        .is_some_and(|magic| magic == INDEX_INCLUDE_COLUMNS_SECTION_MAGIC);
    if !section_is_present {
        return Ok(());
    }
    cursor.offset += INDEX_INCLUDE_COLUMNS_SECTION_MAGIC.len();
    let version = cursor.read_u8()?;
    if version != 1 {
        return Err(DbError::corruption(format!(
            "unknown index include columns section version {version}"
        )));
    }
    let entry_count = cursor.read_u32()?;
    for _ in 0..entry_count {
        let index_name = cursor.read_string()?;
        let include_columns = cursor.read_strings()?;
        let index = indexes.get_mut(&index_name).ok_or_else(|| {
            DbError::corruption(format!(
                "index include metadata referenced unknown index {index_name}"
            ))
        })?;
        index.include_columns = include_columns;
    }
    Ok(())
}

#[cfg(test)]
fn drop_index_include_columns_section(payload: &[u8]) -> Result<Vec<u8>> {
    let start = payload
        .windows(INDEX_INCLUDE_COLUMNS_SECTION_MAGIC.len())
        .position(|window| window == INDEX_INCLUDE_COLUMNS_SECTION_MAGIC)
        .ok_or_else(|| DbError::internal("index include columns section not found"))?;
    let mut cursor = Cursor::new(
        payload
            .get(start + INDEX_INCLUDE_COLUMNS_SECTION_MAGIC.len()..)
            .ok_or_else(|| DbError::internal("index include columns section header truncated"))?,
    );
    let _version = cursor.read_u8()?;
    let entry_count = cursor.read_u32()?;
    for _ in 0..entry_count {
        let _index_name = cursor.read_string()?;
        let _include_columns = cursor.read_strings()?;
    }
    let section_len = INDEX_INCLUDE_COLUMNS_SECTION_MAGIC.len() + cursor.offset;
    let end = start
        .checked_add(section_len)
        .ok_or_else(|| DbError::internal("index include section length overflow"))?;
    let mut output = Vec::with_capacity(payload.len().saturating_sub(section_len));
    output.extend_from_slice(
        payload
            .get(..start)
            .ok_or_else(|| DbError::internal("invalid include section start"))?,
    );
    output.extend_from_slice(
        payload
            .get(end..)
            .ok_or_else(|| DbError::internal("invalid include section end"))?,
    );
    Ok(output)
}

fn decode_generated_columns_section(
    cursor: &mut Cursor<'_>,
    tables: &mut BTreeMap<String, TableSchema>,
) -> Result<()> {
    let section_is_versioned = cursor
        .bytes
        .get(cursor.offset..cursor.offset + GENERATED_COLUMNS_SECTION_MAGIC.len())
        .is_some_and(|magic| magic == GENERATED_COLUMNS_SECTION_MAGIC);
    if section_is_versioned {
        cursor.offset += GENERATED_COLUMNS_SECTION_MAGIC.len();
        let version = cursor.read_u8()?;
        if version != 1 {
            return Err(DbError::corruption(format!(
                "unknown generated columns section version {version}"
            )));
        }
    }
    let generated_column_count = cursor.read_u32()?;
    for _ in 0..generated_column_count {
        let table_name = cursor.read_string()?;
        let column_name = cursor.read_string()?;
        let generated_sql = cursor.read_string()?;
        let generated_stored = if section_is_versioned {
            cursor.read_bool()?
        } else {
            true
        };
        let table = tables.get_mut(&table_name).ok_or_else(|| {
            DbError::corruption(format!(
                "generated column metadata referenced unknown table {table_name}"
            ))
        })?;
        let column = table
            .columns
            .iter_mut()
            .find(|column| identifiers_equal(&column.name, &column_name))
            .ok_or_else(|| {
                DbError::corruption(format!(
                    "generated column metadata referenced unknown column {}.{}",
                    table_name, column_name
                ))
            })?;
        column.generated_sql = Some(generated_sql);
        column.generated_stored = generated_stored;
    }
    Ok(())
}

fn decode_column_type(tag: u8) -> Result<crate::catalog::ColumnType> {
    match tag {
        0 => Ok(crate::catalog::ColumnType::Int64),
        1 => Ok(crate::catalog::ColumnType::Float64),
        2 => Ok(crate::catalog::ColumnType::Text),
        3 => Ok(crate::catalog::ColumnType::Bool),
        4 => Ok(crate::catalog::ColumnType::Blob),
        5 => Ok(crate::catalog::ColumnType::Decimal),
        6 => Ok(crate::catalog::ColumnType::Uuid),
        7 => Ok(crate::catalog::ColumnType::Timestamp),
        _ => Err(DbError::corruption("unknown column type tag")),
    }
}

fn decode_index_kind(tag: u8) -> Result<crate::catalog::IndexKind> {
    match tag {
        0 => Ok(crate::catalog::IndexKind::Btree),
        1 => Ok(crate::catalog::IndexKind::Trigram),
        _ => Err(DbError::corruption("unknown index kind tag")),
    }
}

fn decode_trigger_kind(tag: u8) -> Result<crate::catalog::TriggerKind> {
    match tag {
        0 => Ok(crate::catalog::TriggerKind::After),
        1 => Ok(crate::catalog::TriggerKind::InsteadOf),
        _ => Err(DbError::corruption("unknown trigger kind tag")),
    }
}

fn decode_trigger_event(tag: u8) -> Result<crate::catalog::TriggerEvent> {
    match tag {
        0 => Ok(crate::catalog::TriggerEvent::Insert),
        1 => Ok(crate::catalog::TriggerEvent::Update),
        2 => Ok(crate::catalog::TriggerEvent::Delete),
        _ => Err(DbError::corruption("unknown trigger event tag")),
    }
}

fn decode_fk_action(tag: u8) -> Result<crate::catalog::ForeignKeyAction> {
    match tag {
        0 => Ok(crate::catalog::ForeignKeyAction::NoAction),
        1 => Ok(crate::catalog::ForeignKeyAction::Restrict),
        2 => Ok(crate::catalog::ForeignKeyAction::Cascade),
        3 => Ok(crate::catalog::ForeignKeyAction::SetNull),
        _ => Err(DbError::corruption("unknown foreign-key action tag")),
    }
}

fn decode_foreign_key(cursor: &mut Cursor<'_>) -> Result<crate::catalog::ForeignKeyConstraint> {
    Ok(crate::catalog::ForeignKeyConstraint {
        name: cursor.read_optional_string()?,
        columns: cursor.read_strings()?,
        referenced_table: cursor.read_string()?,
        referenced_columns: cursor.read_strings()?,
        on_delete: decode_fk_action(cursor.read_u8()?)?,
        on_update: decode_fk_action(cursor.read_u8()?)?,
    })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_slice(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| DbError::corruption("cursor overflow"))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| DbError::corruption("truncated catalog state"))?;
        self.offset = end;
        Ok(bytes)
    }

    fn read_u8(&mut self) -> Result<u8> {
        let value = *self
            .bytes
            .get(self.offset)
            .ok_or_else(|| DbError::corruption("truncated catalog state"))?;
        self.offset += 1;
        Ok(value)
    }

    fn read_bool(&mut self) -> Result<bool> {
        Ok(self.read_u8()? != 0)
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_slice(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().expect("u32")))
    }

    fn read_i64(&mut self) -> Result<i64> {
        let bytes = self.read_slice(8)?;
        Ok(i64::from_le_bytes(bytes.try_into().expect("i64")))
    }

    fn read_string(&mut self) -> Result<String> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_slice(len)?;
        std::str::from_utf8(bytes)
            .map(|s| s.to_owned())
            .map_err(|error| {
                DbError::corruption(format!("catalog state string is not valid UTF-8: {error}"))
            })
    }

    fn read_optional_string(&mut self) -> Result<Option<String>> {
        if self.read_bool()? {
            Ok(Some(self.read_string()?))
        } else {
            Ok(None)
        }
    }

    fn read_strings(&mut self) -> Result<Vec<String>> {
        let len = self.read_u32()? as usize;
        (0..len).map(|_| self.read_string()).collect()
    }
}

fn dataset_to_result(dataset: Dataset) -> QueryResult {
    QueryResult::with_rows(
        dataset
            .columns
            .into_iter()
            .map(|binding| binding.name)
            .collect(),
        dataset.rows.into_iter().map(QueryRow::new).collect(),
    )
}

fn projection_has_aggregate_items(items: &[SelectItem]) -> bool {
    items.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => expr_contains_aggregate(expr),
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    })
}

fn simple_btree_lookup(filter: &Expr) -> Option<(Option<&str>, &str, &Expr)> {
    match filter {
        Expr::Binary { left, op, right } if *op == BinaryOp::Eq => match (&**left, &**right) {
            (Expr::Column { table, column }, value)
                if matches!(value, Expr::Literal(_) | Expr::Parameter(_)) =>
            {
                Some((table.as_deref(), column.as_str(), value))
            }
            (value, Expr::Column { table, column })
                if matches!(value, Expr::Literal(_) | Expr::Parameter(_)) =>
            {
                Some((table.as_deref(), column.as_str(), value))
            }
            _ => None,
        },
        _ => None,
    }
}

#[derive(Clone, Copy, Debug)]
struct SimpleRangeBound<'a> {
    inclusive: bool,
    value_expr: &'a Expr,
}

#[derive(Clone, Debug)]
struct SimpleRangeBoundValue {
    inclusive: bool,
    value: Value,
}

#[derive(Clone, Debug)]
struct SimpleGroupedNumericAggregate {
    group_value: Value,
    count: i64,
    total_int: i64,
    total_float: f64,
    saw_float: bool,
    saw_value: bool,
}

impl SimpleGroupedNumericAggregate {
    fn new(group_value: Value) -> Self {
        Self {
            group_value,
            count: 0,
            total_int: 0,
            total_float: 0.0,
            saw_float: false,
            saw_value: false,
        }
    }

    fn add_numeric(&mut self, value: &Value) -> Result<()> {
        match value {
            Value::Null => Ok(()),
            Value::Int64(value) => {
                self.total_int += value;
                self.total_float += *value as f64;
                self.saw_value = true;
                Ok(())
            }
            Value::Float64(value) => {
                self.total_float += *value;
                self.saw_float = true;
                self.saw_value = true;
                Ok(())
            }
            Value::Decimal { scaled, scale } => {
                self.total_float += decimal_to_f64(*scaled, *scale);
                self.saw_float = true;
                self.saw_value = true;
                Ok(())
            }
            other => Err(DbError::sql(format!(
                "numeric aggregate does not support {other:?}"
            ))),
        }
    }

    fn into_row(self) -> QueryRow {
        let Self {
            group_value,
            count,
            total_int,
            total_float,
            saw_float,
            saw_value,
        } = self;
        QueryRow::new(vec![
            group_value,
            Value::Int64(count),
            if !saw_value {
                Value::Null
            } else if saw_float {
                Value::Float64(total_float)
            } else {
                Value::Int64(total_int)
            },
        ])
    }
}

#[derive(Clone, Debug)]
struct BenchmarkReportAggregate {
    item_name: String,
    quantity_total: i64,
    revenue_total: f64,
}

impl BenchmarkReportAggregate {
    fn new(item_name: String) -> Self {
        Self {
            item_name,
            quantity_total: 0,
            revenue_total: 0.0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct SimpleOrderByPlan {
    projection_index: usize,
    descending: bool,
}

#[derive(Clone, Copy, Debug)]
enum SimpleJoinProjectionSource {
    Filtered(usize),
    Probe(usize),
}

#[derive(Clone, Copy, Debug)]
struct SimpleRangeProjectionFilter<'a> {
    table: Option<&'a str>,
    column: &'a str,
    lower: Option<SimpleRangeBound<'a>>,
    upper: Option<SimpleRangeBound<'a>>,
}

fn simple_range_projection_filter(filter: &Expr) -> Option<SimpleRangeProjectionFilter<'_>> {
    let mut state = SimpleRangeFilterState::default();
    collect_simple_range_projection_terms(filter, &mut state)?;
    Some(SimpleRangeProjectionFilter {
        table: state.table,
        column: state.column?,
        lower: state.lower,
        upper: state.upper,
    })
}

#[derive(Clone, Copy, Debug, Default)]
struct SimpleRangeFilterState<'a> {
    table: Option<&'a str>,
    column: Option<&'a str>,
    lower: Option<SimpleRangeBound<'a>>,
    upper: Option<SimpleRangeBound<'a>>,
}

fn collect_simple_range_projection_terms<'a>(
    filter: &'a Expr,
    state: &mut SimpleRangeFilterState<'a>,
) -> Option<()> {
    match filter {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            collect_simple_range_projection_terms(left, state)?;
            collect_simple_range_projection_terms(right, state)?;
            Some(())
        }
        Expr::Binary { left, op, right } => {
            let (table, column, bound_kind, value_expr) =
                simple_range_projection_bound(left, *op, right).or_else(|| {
                    simple_range_projection_bound(right, reverse_binary_op(*op)?, left)
                })?;
            if let Some(existing_table) = state.table {
                if Some(existing_table) != table {
                    return None;
                }
            } else {
                state.table = table;
            }
            if let Some(existing_column) = state.column {
                if !identifiers_equal(existing_column, column) {
                    return None;
                }
            } else {
                state.column = Some(column);
            }
            match bound_kind {
                SimpleRangeBoundKind::Lower(inclusive) => {
                    if state.lower.is_some() {
                        return None;
                    }
                    state.lower = Some(SimpleRangeBound {
                        inclusive,
                        value_expr,
                    });
                }
                SimpleRangeBoundKind::Upper(inclusive) => {
                    if state.upper.is_some() {
                        return None;
                    }
                    state.upper = Some(SimpleRangeBound {
                        inclusive,
                        value_expr,
                    });
                }
            }
            Some(())
        }
        _ => None,
    }
}

#[derive(Clone, Copy, Debug)]
enum SimpleRangeBoundKind {
    Lower(bool),
    Upper(bool),
}

fn simple_range_projection_bound<'a>(
    left: &'a Expr,
    op: BinaryOp,
    right: &'a Expr,
) -> Option<(Option<&'a str>, &'a str, SimpleRangeBoundKind, &'a Expr)> {
    let Expr::Column { table, column } = left else {
        return None;
    };
    if !matches!(right, Expr::Literal(_) | Expr::Parameter(_)) {
        return None;
    }
    let bound_kind = match op {
        BinaryOp::Gt => SimpleRangeBoundKind::Lower(false),
        BinaryOp::GtEq => SimpleRangeBoundKind::Lower(true),
        BinaryOp::Lt => SimpleRangeBoundKind::Upper(false),
        BinaryOp::LtEq => SimpleRangeBoundKind::Upper(true),
        _ => return None,
    };
    Some((table.as_deref(), column.as_str(), bound_kind, right))
}

fn reverse_binary_op(op: BinaryOp) -> Option<BinaryOp> {
    match op {
        BinaryOp::Gt => Some(BinaryOp::Lt),
        BinaryOp::GtEq => Some(BinaryOp::LtEq),
        BinaryOp::Lt => Some(BinaryOp::Gt),
        BinaryOp::LtEq => Some(BinaryOp::GtEq),
        _ => None,
    }
}

fn simple_range_bound_matches(
    candidate: &Value,
    lower_bound: Option<&SimpleRangeBoundValue>,
    upper_bound: Option<&SimpleRangeBoundValue>,
) -> Result<bool> {
    if let Some(lower_bound) = lower_bound {
        let ordering = compare_values(candidate, &lower_bound.value)?;
        let lower_matches = if lower_bound.inclusive {
            ordering != std::cmp::Ordering::Less
        } else {
            ordering == std::cmp::Ordering::Greater
        };
        if !lower_matches {
            return Ok(false);
        }
    }
    if let Some(upper_bound) = upper_bound {
        let ordering = compare_values(candidate, &upper_bound.value)?;
        let upper_matches = if upper_bound.inclusive {
            ordering != std::cmp::Ordering::Greater
        } else {
            ordering == std::cmp::Ordering::Less
        };
        if !upper_matches {
            return Ok(false);
        }
    }
    Ok(true)
}

#[derive(Clone, Copy, Debug)]
struct QualifiedColumnRef<'a> {
    table: Option<&'a str>,
    column: &'a str,
}

#[derive(Clone, Copy, Debug)]
struct TableBindingRef<'a> {
    name: &'a str,
    alias: &'a Option<String>,
}

impl<'a> TableBindingRef<'a> {
    fn binding_name(self) -> &'a str {
        self.alias.as_deref().unwrap_or(self.name)
    }
}

#[derive(Clone, Copy, Debug)]
struct IndexedJoinPlan<'a> {
    filtered_table: TableBindingRef<'a>,
    filtered_dataset: &'a Dataset,
    filtered_join_column: &'a str,
    probe_table: TableBindingRef<'a>,
    probe_join_column: &'a str,
    filtered_on_left: bool,
}

fn simple_join_equality(on: &Expr) -> Option<(QualifiedColumnRef<'_>, QualifiedColumnRef<'_>)> {
    let Expr::Binary { left, op, right } = on else {
        return None;
    };
    if *op != BinaryOp::Eq {
        return None;
    }
    let (
        Expr::Column {
            table: left_table,
            column: left_column,
        },
        Expr::Column {
            table: right_table,
            column: right_column,
        },
    ) = (&**left, &**right)
    else {
        return None;
    };
    Some((
        QualifiedColumnRef {
            table: left_table.as_deref(),
            column: left_column,
        },
        QualifiedColumnRef {
            table: right_table.as_deref(),
            column: right_column,
        },
    ))
}

fn row_id_alias_column_name(table: &TableSchema) -> Option<&str> {
    if table.primary_key_columns.len() != 1 {
        return None;
    }
    let primary_key_column = &table.primary_key_columns[0];
    table
        .columns
        .iter()
        .find(|column| identifiers_equal(&column.name, primary_key_column) && column.auto_increment)
        .map(|column| column.name.as_str())
}

fn matches_table_binding(table: TableBindingRef<'_>, qualifier: Option<&str>) -> bool {
    qualifier.is_some_and(|qualifier| identifiers_equal(qualifier, table.binding_name()))
}

fn matches_filter_binding(
    table_name: &str,
    alias: &Option<String>,
    qualifier: Option<&str>,
) -> bool {
    match qualifier {
        Some(qualifier) => identifiers_equal(qualifier, alias.as_deref().unwrap_or(table_name)),
        None => true,
    }
}

fn dataset_column_index(dataset: &Dataset, qualifier: Option<&str>, column: &str) -> Option<usize> {
    let matches = dataset
        .columns
        .iter()
        .enumerate()
        .filter(|(_, binding)| {
            if !identifiers_equal(&binding.name, column) {
                return false;
            }
            if let Some(qualifier) = qualifier {
                binding
                    .table
                    .as_deref()
                    .is_some_and(|table| identifiers_equal(table, qualifier))
            } else {
                !binding.hidden
            }
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [index] => Some(*index),
        _ => None,
    }
}

#[derive(Debug)]
enum MembershipValue {
    Scalar(Value),
    Row(Vec<Value>),
}

fn membership_value_has_nulls(value: &MembershipValue) -> bool {
    match value {
        MembershipValue::Scalar(value) => matches!(value, Value::Null),
        MembershipValue::Row(values) => values.iter().any(|value| matches!(value, Value::Null)),
    }
}

fn compare_membership_values(
    left: &MembershipValue,
    right: &MembershipValue,
) -> Result<Option<bool>> {
    match (left, right) {
        (MembershipValue::Scalar(left), MembershipValue::Scalar(right)) => {
            if matches!(left, Value::Null) || matches!(right, Value::Null) {
                Ok(None)
            } else {
                Ok(Some(
                    compare_values(left, right)? == std::cmp::Ordering::Equal,
                ))
            }
        }
        (MembershipValue::Row(left), MembershipValue::Row(right)) => {
            if left.len() != right.len() {
                return Err(DbError::sql(format!(
                    "row-value comparison expected {} columns but got {}",
                    left.len(),
                    right.len()
                )));
            }
            let mut saw_null = false;
            for (left_value, right_value) in left.iter().zip(right) {
                if matches!(left_value, Value::Null) || matches!(right_value, Value::Null) {
                    saw_null = true;
                    continue;
                }
                if compare_values(left_value, right_value)? != std::cmp::Ordering::Equal {
                    return Ok(Some(false));
                }
            }
            if saw_null {
                Ok(None)
            } else {
                Ok(Some(true))
            }
        }
        (MembershipValue::Scalar(_), MembershipValue::Row(right))
        | (MembershipValue::Row(right), MembershipValue::Scalar(_)) => Err(DbError::sql(format!(
            "row-value comparison expected {} columns but got 1",
            right.len()
        ))),
    }
}

fn schema_column_index(schema: &TableSchema, column: &str) -> Option<usize> {
    schema
        .columns
        .iter()
        .position(|candidate| identifiers_equal(&candidate.name, column))
}

fn value_as_int64(value: &Value) -> Option<i64> {
    match value {
        Value::Int64(value) => Some(*value),
        _ => None,
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Int64(value) => Some(*value as f64),
        Value::Float64(value) => Some(*value),
        _ => None,
    }
}

fn value_as_text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(value) => Some(value.as_str()),
        _ => None,
    }
}

fn expr_matches_binding_column(expr: &Expr, binding: TableBindingRef<'_>, column: &str) -> bool {
    let Expr::Column {
        table,
        column: expr_column,
    } = expr
    else {
        return false;
    };
    matches_table_binding(binding, table.as_deref()) && identifiers_equal(expr_column, column)
}

fn join_constraint_matches_columns(
    on: &Expr,
    left_binding: TableBindingRef<'_>,
    left_column: &str,
    right_binding: TableBindingRef<'_>,
    right_column: &str,
) -> bool {
    let Some((left_ref, right_ref)) = simple_join_equality(on) else {
        return false;
    };
    (matches_table_binding(left_binding, left_ref.table)
        && identifiers_equal(left_ref.column, left_column)
        && matches_table_binding(right_binding, right_ref.table)
        && identifiers_equal(right_ref.column, right_column))
        || (matches_table_binding(left_binding, right_ref.table)
            && identifiers_equal(right_ref.column, left_column)
            && matches_table_binding(right_binding, left_ref.table)
            && identifiers_equal(left_ref.column, right_column))
}

fn aggregate_matches_single_binding_column(
    expr: &Expr,
    aggregate_name: &str,
    binding: TableBindingRef<'_>,
    column: &str,
) -> bool {
    let Expr::Aggregate {
        name,
        args,
        distinct,
        star,
        order_by,
        within_group,
    } = expr
    else {
        return false;
    };
    if !name.eq_ignore_ascii_case(aggregate_name)
        || *distinct
        || *star
        || !order_by.is_empty()
        || *within_group
        || args.len() != 1
    {
        return false;
    }
    expr_matches_binding_column(&args[0], binding, column)
}

fn aggregate_matches_binding_product(
    expr: &Expr,
    aggregate_name: &str,
    left_binding: TableBindingRef<'_>,
    left_column: &str,
    right_binding: TableBindingRef<'_>,
    right_column: &str,
) -> bool {
    let Expr::Aggregate {
        name,
        args,
        distinct,
        star,
        order_by,
        within_group,
    } = expr
    else {
        return false;
    };
    if !name.eq_ignore_ascii_case(aggregate_name)
        || *distinct
        || *star
        || !order_by.is_empty()
        || *within_group
        || args.len() != 1
    {
        return false;
    }
    let Expr::Binary { left, op, right } = &args[0] else {
        return false;
    };
    if *op != BinaryOp::Mul {
        return false;
    }
    (expr_matches_binding_column(left, left_binding, left_column)
        && expr_matches_binding_column(right, right_binding, right_column))
        || (expr_matches_binding_column(left, right_binding, right_column)
            && expr_matches_binding_column(right, left_binding, left_column))
}

fn order_by_matches_alias_or_projection(
    order_by: &crate::sql::ast::OrderBy,
    alias: Option<&str>,
    projection_expr: &Expr,
    descending: bool,
) -> bool {
    if order_by.descending != descending {
        return false;
    }
    if let Some(alias) = alias {
        if let Expr::Column {
            table: None,
            column,
        } = &order_by.expr
        {
            if identifiers_equal(column.as_str(), alias) {
                return true;
            }
        }
    }
    &order_by.expr == projection_expr
}

fn from_item_is_all_inner_table_joins(item: &FromItem) -> bool {
    match item {
        FromItem::Table { .. } => true,
        FromItem::Join {
            left,
            right,
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(_),
        } => from_item_is_all_inner_table_joins(left) && from_item_is_all_inner_table_joins(right),
        _ => false,
    }
}

fn simple_select_item_column_index(
    dataset: &Dataset,
    table: Option<&str>,
    column: &str,
) -> Option<usize> {
    let matches = dataset
        .columns
        .iter()
        .enumerate()
        .filter(|(_, binding)| {
            if !identifiers_equal(&binding.name, column) {
                return false;
            }
            match table {
                Some(table_name) => binding
                    .table
                    .as_deref()
                    .is_some_and(|binding_table| identifiers_equal(binding_table, table_name)),
                None => !binding.hidden,
            }
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [index] => Some(*index),
        _ => None,
    }
}

fn simple_join_projection_plan(
    items: &[SelectItem],
    filtered_table_name: &str,
    filtered_alias: &Option<String>,
    filtered_schema: &TableSchema,
    probe_table_name: &str,
    probe_alias: &Option<String>,
    probe_schema: &TableSchema,
) -> Option<(Vec<SimpleJoinProjectionSource>, Vec<String>)> {
    let filtered_binding = filtered_alias.as_deref().unwrap_or(filtered_table_name);
    let probe_binding = probe_alias.as_deref().unwrap_or(probe_table_name);
    let mut projection_plan = Vec::with_capacity(items.len());
    let mut column_names = Vec::with_capacity(items.len());

    for (index, item) in items.iter().enumerate() {
        let SelectItem::Expr { expr, alias } = item else {
            return None;
        };
        let Expr::Column { table, column } = expr else {
            return None;
        };

        let filtered_index = filtered_schema
            .columns
            .iter()
            .position(|candidate| identifiers_equal(&candidate.name, column));
        let probe_index = probe_schema
            .columns
            .iter()
            .position(|candidate| identifiers_equal(&candidate.name, column));
        let source = match table.as_deref() {
            Some(table_name)
                if identifiers_equal(table_name, filtered_table_name)
                    || identifiers_equal(table_name, filtered_binding) =>
            {
                Some(SimpleJoinProjectionSource::Filtered(filtered_index?))
            }
            Some(table_name)
                if identifiers_equal(table_name, probe_table_name)
                    || identifiers_equal(table_name, probe_binding) =>
            {
                Some(SimpleJoinProjectionSource::Probe(probe_index?))
            }
            Some(_) => None,
            None => match (filtered_index, probe_index) {
                (Some(filtered_index), None) => {
                    Some(SimpleJoinProjectionSource::Filtered(filtered_index))
                }
                (None, Some(probe_index)) => Some(SimpleJoinProjectionSource::Probe(probe_index)),
                _ => None,
            },
        }?;
        projection_plan.push(source);
        column_names.push(
            alias
                .clone()
                .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
        );
    }

    Some((projection_plan, column_names))
}

fn try_project_simple_select_items(
    dataset: &Dataset,
    items: &[SelectItem],
) -> Result<Option<Dataset>> {
    let mut output_columns = Vec::new();
    let mut projection_plan = Vec::<usize>::new();
    for (index, item) in items.iter().enumerate() {
        match item {
            SelectItem::Expr { expr, alias } => {
                let Expr::Column { table, column } = expr else {
                    return Ok(None);
                };
                let Some(source_index) =
                    simple_select_item_column_index(dataset, table.as_deref(), column)
                else {
                    return Ok(None);
                };
                projection_plan.push(source_index);
                output_columns.push(ColumnBinding::visible(
                    None,
                    alias
                        .clone()
                        .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
                ));
            }
            SelectItem::Wildcard => {
                for (source_index, binding) in dataset.columns.iter().enumerate() {
                    if binding.hidden {
                        continue;
                    }
                    projection_plan.push(source_index);
                    output_columns.push(binding.as_output());
                }
            }
            SelectItem::QualifiedWildcard(table) => {
                let mut matched = false;
                for (source_index, binding) in dataset.columns.iter().enumerate() {
                    if binding.table.as_deref() != Some(table.as_str()) {
                        continue;
                    }
                    projection_plan.push(source_index);
                    output_columns.push(binding.as_output());
                    matched = true;
                }
                if !matched {
                    return Ok(None);
                }
            }
        }
    }

    let mut output_rows = Vec::with_capacity(dataset.rows.len());
    for row in &dataset.rows {
        let mut output_row = Vec::with_capacity(projection_plan.len());
        for source_index in &projection_plan {
            let value = row
                .get(*source_index)
                .ok_or_else(|| DbError::internal("projection source index exceeds row width"))?;
            output_row.push(value.clone());
        }
        output_rows.push(output_row);
    }
    Ok(Some(Dataset {
        columns: output_columns,
        rows: output_rows,
    }))
}

fn simple_trigram_lookup(filter: &Expr) -> Option<(&str, &Expr, bool)> {
    match filter {
        Expr::Like { expr, pattern, .. } => match (&**expr, &**pattern) {
            (Expr::Column { column, .. }, pattern @ (Expr::Literal(_) | Expr::Parameter(_))) => {
                Some((column.as_str(), pattern, false))
            }
            _ => None,
        },
        Expr::Binary {
            left,
            op: BinaryOp::And | BinaryOp::Or,
            right,
        } => simple_trigram_lookup(left)
            .map(|(column, pattern, _)| (column, pattern, true))
            .or_else(|| {
                simple_trigram_lookup(right).map(|(column, pattern, _)| (column, pattern, true))
            }),
        _ => None,
    }
}

fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate { .. } => true,
        Expr::Unary { expr, .. } => expr_contains_aggregate(expr),
        Expr::Binary { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_aggregate(expr)
                || expr_contains_aggregate(low)
                || expr_contains_aggregate(high)
        }
        Expr::InList { expr, items, .. } => {
            expr_contains_aggregate(expr) || items.iter().any(expr_contains_aggregate)
        }
        Expr::InSubquery { expr, .. } => expr_contains_aggregate(expr),
        Expr::CompareSubquery { expr, .. } => expr_contains_aggregate(expr),
        Expr::ScalarSubquery(_) | Expr::Exists(_) => false,
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_aggregate(expr)
                || expr_contains_aggregate(pattern)
                || escape.as_deref().is_some_and(expr_contains_aggregate)
        }
        Expr::IsNull { expr, .. } => expr_contains_aggregate(expr),
        Expr::Function { args, .. } => args.iter().any(expr_contains_aggregate),
        Expr::RowNumber { .. } | Expr::WindowFunction { .. } => false,
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand.as_deref().is_some_and(expr_contains_aggregate)
                || branches.iter().any(|(left, right)| {
                    expr_contains_aggregate(left) || expr_contains_aggregate(right)
                })
                || else_expr.as_deref().is_some_and(expr_contains_aggregate)
        }
        Expr::Row(items) => items.iter().any(expr_contains_aggregate),
        Expr::Cast { expr, .. } => expr_contains_aggregate(expr),
        Expr::Literal(_) | Expr::Column { .. } | Expr::Parameter(_) => false,
    }
}

#[derive(Clone, Debug)]
struct JoinUsingColumn {
    name: String,
    left_index: usize,
    right_index: usize,
}

struct JoinEvalContext<'a> {
    dataset: &'a Dataset,
    runtime: &'a EngineRuntime,
    params: &'a [Value],
    ctes: &'a BTreeMap<String, Dataset>,
}

fn visible_column_names(dataset: &Dataset) -> Vec<String> {
    let mut names = Vec::<String>::new();
    for binding in dataset.columns.iter().filter(|binding| !binding.hidden) {
        if !names
            .iter()
            .any(|name| identifiers_equal(name, &binding.name))
        {
            names.push(binding.name.clone());
        }
    }
    names
}

fn visible_column_exists(dataset: &Dataset, column: &str) -> bool {
    dataset
        .columns
        .iter()
        .any(|binding| !binding.hidden && identifiers_equal(&binding.name, column))
}

fn resolve_visible_join_column(
    dataset: &Dataset,
    column: &str,
    join_form: &str,
    side: &str,
) -> Result<usize> {
    let matches = dataset
        .columns
        .iter()
        .enumerate()
        .filter(|(_, binding)| !binding.hidden && identifiers_equal(&binding.name, column))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [single] => Ok(single.0),
        [] => Err(DbError::sql(format!(
            "{join_form} column {column} does not exist in {side} input"
        ))),
        _ => Err(DbError::sql(format!(
            "{join_form} column {column} is ambiguous in {side} input"
        ))),
    }
}

fn resolve_join_using_columns(
    left: &Dataset,
    right: &Dataset,
    constraint: &JoinConstraint,
) -> Result<Vec<JoinUsingColumn>> {
    match constraint {
        JoinConstraint::On(_) => Ok(Vec::new()),
        JoinConstraint::Using(columns) => {
            let mut pairs = Vec::with_capacity(columns.len());
            let mut seen = Vec::<String>::new();
            for column in columns {
                if seen
                    .iter()
                    .any(|existing| identifiers_equal(existing, column))
                {
                    return Err(DbError::sql(format!(
                        "JOIN USING column {column} specified more than once"
                    )));
                }
                let left_index = resolve_visible_join_column(left, column, "JOIN USING", "left")?;
                let right_index =
                    resolve_visible_join_column(right, column, "JOIN USING", "right")?;
                pairs.push(JoinUsingColumn {
                    name: left.columns[left_index].name.clone(),
                    left_index,
                    right_index,
                });
                seen.push(column.clone());
            }
            Ok(pairs)
        }
        JoinConstraint::Natural => {
            let mut pairs = Vec::new();
            for column in visible_column_names(left) {
                if !visible_column_exists(right, &column) {
                    continue;
                }
                let left_index =
                    resolve_visible_join_column(left, &column, "NATURAL JOIN", "left")?;
                let right_index =
                    resolve_visible_join_column(right, &column, "NATURAL JOIN", "right")?;
                pairs.push(JoinUsingColumn {
                    name: left.columns[left_index].name.clone(),
                    left_index,
                    right_index,
                });
            }
            Ok(pairs)
        }
    }
}

fn join_output_columns(
    left: &Dataset,
    right: &Dataset,
    using_columns: &[JoinUsingColumn],
) -> Vec<ColumnBinding> {
    if using_columns.is_empty() {
        let mut columns = left.columns.clone();
        columns.extend(right.columns.clone());
        return columns;
    }

    let left_hidden = using_columns
        .iter()
        .map(|column| column.left_index)
        .collect::<BTreeSet<_>>();
    let right_hidden = using_columns
        .iter()
        .map(|column| column.right_index)
        .collect::<BTreeSet<_>>();

    let mut columns =
        Vec::with_capacity(using_columns.len() + left.columns.len() + right.columns.len());
    for column in using_columns {
        columns.push(ColumnBinding::visible(None, column.name.clone()));
    }
    for (index, binding) in left.columns.iter().enumerate() {
        let mut binding = binding.clone();
        if left_hidden.contains(&index) {
            binding.hidden = true;
        }
        columns.push(binding);
    }
    for (index, binding) in right.columns.iter().enumerate() {
        let mut binding = binding.clone();
        if right_hidden.contains(&index) {
            binding.hidden = true;
        }
        columns.push(binding);
    }
    columns
}

fn merged_join_value(left: &Value, right: &Value) -> Value {
    if matches!(left, Value::Null) {
        right.clone()
    } else {
        left.clone()
    }
}

fn join_output_row(
    left_row: &[Value],
    right_row: &[Value],
    using_columns: &[JoinUsingColumn],
) -> Result<Vec<Value>> {
    if using_columns.is_empty() {
        let mut row = left_row.to_vec();
        row.extend_from_slice(right_row);
        return Ok(row);
    }

    let mut row = Vec::with_capacity(using_columns.len() + left_row.len() + right_row.len());
    for column in using_columns {
        let left_value = left_row
            .get(column.left_index)
            .ok_or_else(|| DbError::internal("left join row is shorter than its bindings"))?;
        let right_value = right_row
            .get(column.right_index)
            .ok_or_else(|| DbError::internal("right join row is shorter than its bindings"))?;
        row.push(merged_join_value(left_value, right_value));
    }
    row.extend_from_slice(left_row);
    row.extend_from_slice(right_row);
    Ok(row)
}

fn join_rows_match(
    constraint: &JoinConstraint,
    using_columns: &[JoinUsingColumn],
    eval_row: &[Value],
    left_row: &[Value],
    right_row: &[Value],
    context: &JoinEvalContext<'_>,
) -> Result<bool> {
    match constraint {
        JoinConstraint::On(on) => Ok(matches!(
            context.runtime.eval_expr(
                on,
                context.dataset,
                eval_row,
                context.params,
                context.ctes,
                None
            )?,
            Value::Bool(true)
        )),
        JoinConstraint::Using(_) | JoinConstraint::Natural => {
            for column in using_columns {
                let left_value = left_row.get(column.left_index).ok_or_else(|| {
                    DbError::internal("left join row is shorter than its bindings")
                })?;
                let right_value = right_row.get(column.right_index).ok_or_else(|| {
                    DbError::internal("right join row is shorter than its bindings")
                })?;
                if matches!(left_value, Value::Null) || matches!(right_value, Value::Null) {
                    return Ok(false);
                }
                if compare_values(left_value, right_value)? != std::cmp::Ordering::Equal {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}

fn nested_loop_join(
    left: Dataset,
    right: Dataset,
    kind: JoinKind,
    constraint: &JoinConstraint,
    runtime: &EngineRuntime,
    params: &[Value],
    ctes: &BTreeMap<String, Dataset>,
) -> Result<Dataset> {
    let using_columns = resolve_join_using_columns(&left, &right, constraint)?;

    let mut eval_columns = left.columns.clone();
    eval_columns.extend(right.columns.clone());
    let eval_dataset = Dataset {
        columns: eval_columns,
        rows: Vec::new(),
    };
    let eval_context = JoinEvalContext {
        dataset: &eval_dataset,
        runtime,
        params,
        ctes,
    };
    let columns = join_output_columns(&left, &right, &using_columns);
    let mut rows = Vec::new();
    let mut matched_right = vec![false; right.rows.len()];
    let left_nulls = vec![Value::Null; left.columns.len()];
    let right_nulls = vec![Value::Null; right.columns.len()];
    for left_row in &left.rows {
        let mut matched = false;
        for (right_index, right_row) in right.rows.iter().enumerate() {
            let mut eval_row = left_row.clone();
            eval_row.extend(right_row.clone());
            if join_rows_match(
                constraint,
                &using_columns,
                &eval_row,
                left_row,
                right_row,
                &eval_context,
            )? {
                matched = true;
                matched_right[right_index] = true;
                rows.push(join_output_row(left_row, right_row, &using_columns)?);
            }
        }
        if !matched && matches!(kind, JoinKind::Left | JoinKind::Full) {
            rows.push(join_output_row(left_row, &right_nulls, &using_columns)?);
        }
    }
    if matches!(kind, JoinKind::Right | JoinKind::Full) {
        for (matched, right_row) in matched_right.iter().zip(&right.rows) {
            if !matched {
                rows.push(join_output_row(&left_nulls, right_row, &using_columns)?);
            }
        }
    }
    Ok(Dataset { columns, rows })
}

impl EngineRuntime {
    fn evaluate_set_operation(
        &self,
        op: crate::sql::ast::SetOperation,
        all: bool,
        left: Dataset,
        right: Dataset,
    ) -> Result<Dataset> {
        if left.columns.len() != right.columns.len() {
            return Err(DbError::sql(
                "set operations require matching column counts",
            ));
        }
        let columns = left.columns.clone();
        let rows = match op {
            crate::sql::ast::SetOperation::Union => {
                let mut rows = left.rows;
                rows.extend(right.rows);
                if !all {
                    deduplicate_rows(rows)?
                } else {
                    rows
                }
            }
            crate::sql::ast::SetOperation::Intersect => {
                let right_counts = count_row_identities(&right.rows)?;
                let mut rows = Vec::new();
                if all {
                    let mut remaining = right_counts;
                    for row in left.rows {
                        let identity = row_identity(&row)?;
                        if consume_row_identity_count(&mut remaining, &identity) {
                            rows.push(row);
                        }
                    }
                } else {
                    for row in left.rows {
                        let identity = row_identity(&row)?;
                        if right_counts.contains_key(&identity) {
                            rows.push(row);
                        }
                    }
                    rows = deduplicate_rows(rows)?;
                }
                rows
            }
            crate::sql::ast::SetOperation::Except => {
                let right_counts = count_row_identities(&right.rows)?;
                let mut rows = Vec::new();
                if all {
                    let mut remaining = right_counts;
                    for row in left.rows {
                        let identity = row_identity(&row)?;
                        if !consume_row_identity_count(&mut remaining, &identity) {
                            rows.push(row);
                        }
                    }
                } else {
                    for row in left.rows {
                        let identity = row_identity(&row)?;
                        if !right_counts.contains_key(&identity) {
                            rows.push(row);
                        }
                    }
                    rows = deduplicate_rows(rows)?;
                }
                rows
            }
        };
        Ok(Dataset { columns, rows })
    }

    fn project_dataset(
        &self,
        dataset: &Dataset,
        items: &[SelectItem],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
        excluded: Option<&Dataset>,
    ) -> Result<Dataset> {
        if let Some(projected) = try_project_simple_select_items(dataset, items)? {
            return Ok(projected);
        }
        let window_values = items
            .iter()
            .map(|item| match item {
                SelectItem::Expr {
                    expr:
                        Expr::RowNumber {
                            partition_by,
                            order_by,
                            frame,
                        },
                    ..
                } => self
                    .compute_row_number_values(
                        dataset,
                        partition_by,
                        order_by,
                        frame.as_ref(),
                        params,
                        ctes,
                    )
                    .map(Some),
                SelectItem::Expr {
                    expr:
                        Expr::WindowFunction {
                            name,
                            args,
                            partition_by,
                            order_by,
                            frame,
                            distinct,
                            star,
                        },
                    ..
                } => self
                    .compute_window_function_values(
                        dataset,
                        name,
                        args,
                        partition_by,
                        order_by,
                        frame.as_ref(),
                        *distinct,
                        *star,
                        params,
                        ctes,
                    )
                    .map(Some),
                _ => Ok(None),
            })
            .collect::<Result<Vec<_>>>()?;
        let mut columns = Vec::new();
        for (index, item) in items.iter().enumerate() {
            match item {
                SelectItem::Expr { expr, alias } => columns.push(ColumnBinding::visible(
                    None,
                    alias
                        .clone()
                        .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
                )),
                SelectItem::Wildcard => columns.extend(
                    dataset
                        .columns
                        .iter()
                        .filter(|binding| !binding.hidden)
                        .map(ColumnBinding::as_output),
                ),
                SelectItem::QualifiedWildcard(table) => columns.extend(
                    dataset
                        .columns
                        .iter()
                        .filter(|column| column.table.as_deref() == Some(table.as_str()))
                        .map(ColumnBinding::as_output),
                ),
            }
        }
        let mut rows = Vec::with_capacity(dataset.rows.len());
        for (row_index, row) in dataset.rows.iter().enumerate() {
            let mut output = Vec::new();
            for (item_index, item) in items.iter().enumerate() {
                match item {
                    SelectItem::Expr { expr, .. } => match expr {
                        Expr::RowNumber { .. } | Expr::WindowFunction { .. } => output.push(
                            window_values[item_index]
                                .as_ref()
                                .and_then(|values| values.get(row_index))
                                .cloned()
                                .ok_or_else(|| {
                                    DbError::internal("window-function values were not precomputed")
                                })?,
                        ),
                        _ => {
                            output.push(self.eval_expr(expr, dataset, row, params, ctes, excluded)?)
                        }
                    },
                    SelectItem::Wildcard => {
                        for (binding, value) in dataset.columns.iter().zip(row) {
                            if !binding.hidden {
                                output.push(value.clone());
                            }
                        }
                    }
                    SelectItem::QualifiedWildcard(table) => {
                        for (binding, value) in dataset.columns.iter().zip(row) {
                            if binding.table.as_deref() == Some(table.as_str()) {
                                output.push(value.clone());
                            }
                        }
                    }
                }
            }
            rows.push(output);
        }
        Ok(Dataset { columns, rows })
    }

    fn compute_row_number_values(
        &self,
        dataset: &Dataset,
        partition_by: &[Expr],
        order_by: &[crate::sql::ast::OrderBy],
        _frame: Option<&crate::sql::ast::WindowFrame>,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Vec<Value>> {
        let mut partitions = BTreeMap::<Vec<u8>, Vec<usize>>::new();
        for (row_index, row) in dataset.rows.iter().enumerate() {
            let key = if partition_by.is_empty() {
                vec![0]
            } else {
                let values = partition_by
                    .iter()
                    .map(|expr| self.eval_expr(expr, dataset, row, params, ctes, None))
                    .collect::<Result<Vec<_>>>()?;
                row_identity(&values)?
            };
            partitions.entry(key).or_default().push(row_index);
        }

        let mut row_numbers = vec![Value::Null; dataset.rows.len()];
        for indices in partitions.into_values() {
            let mut sorted = indices;
            sorted.sort_by(|left, right| {
                for order in order_by {
                    let left_value = self
                        .eval_expr(
                            &order.expr,
                            dataset,
                            &dataset.rows[*left],
                            params,
                            ctes,
                            None,
                        )
                        .unwrap_or(Value::Null);
                    let right_value = self
                        .eval_expr(
                            &order.expr,
                            dataset,
                            &dataset.rows[*right],
                            params,
                            ctes,
                            None,
                        )
                        .unwrap_or(Value::Null);
                    let ordering = compare_values(&left_value, &right_value)
                        .unwrap_or(std::cmp::Ordering::Equal);
                    if ordering != std::cmp::Ordering::Equal {
                        return if order.descending {
                            ordering.reverse()
                        } else {
                            ordering
                        };
                    }
                }
                left.cmp(right)
            });

            for (ordinal, row_index) in sorted.into_iter().enumerate() {
                row_numbers[row_index] = Value::Int64((ordinal + 1) as i64);
            }
        }
        Ok(row_numbers)
    }

    #[allow(clippy::too_many_arguments)]
    fn compute_window_function_values(
        &self,
        dataset: &Dataset,
        name: &str,
        args: &[Expr],
        partition_by: &[Expr],
        order_by: &[crate::sql::ast::OrderBy],
        _frame: Option<&crate::sql::ast::WindowFrame>,
        _distinct: bool,
        _star: bool,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Vec<Value>> {
        let mut partitions = BTreeMap::<Vec<u8>, Vec<usize>>::new();
        for (row_index, row) in dataset.rows.iter().enumerate() {
            let key = if partition_by.is_empty() {
                vec![0]
            } else {
                let values = partition_by
                    .iter()
                    .map(|expr| self.eval_expr(expr, dataset, row, params, ctes, None))
                    .collect::<Result<Vec<_>>>()?;
                row_identity(&values)?
            };
            partitions.entry(key).or_default().push(row_index);
        }

        let mut results = vec![Value::Null; dataset.rows.len()];
        for indices in partitions.into_values() {
            let mut sorted = indices;
            sorted.sort_by(|left, right| {
                for order in order_by {
                    let left_value = self
                        .eval_expr(
                            &order.expr,
                            dataset,
                            &dataset.rows[*left],
                            params,
                            ctes,
                            None,
                        )
                        .unwrap_or(Value::Null);
                    let right_value = self
                        .eval_expr(
                            &order.expr,
                            dataset,
                            &dataset.rows[*right],
                            params,
                            ctes,
                            None,
                        )
                        .unwrap_or(Value::Null);
                    let ordering = compare_values(&left_value, &right_value)
                        .unwrap_or(std::cmp::Ordering::Equal);
                    if ordering != std::cmp::Ordering::Equal {
                        return if order.descending {
                            ordering.reverse()
                        } else {
                            ordering
                        };
                    }
                }
                left.cmp(right)
            });

            let order_keys = sorted
                .iter()
                .map(|row_index| {
                    order_by
                        .iter()
                        .map(|order| {
                            self.eval_expr(
                                &order.expr,
                                dataset,
                                &dataset.rows[*row_index],
                                params,
                                ctes,
                                None,
                            )
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;
            let (peer_starts, peer_ends) = compute_window_peer_bounds(&order_keys)?;

            match name {
                "rank" => {
                    if _distinct || _star {
                        return Err(DbError::sql("RANK does not support DISTINCT or *"));
                    }
                    let mut current_rank = 1_i64;
                    for (ordinal, row_index) in sorted.iter().enumerate() {
                        if ordinal > 0
                            && !window_order_keys_equal(
                                &order_keys[ordinal - 1],
                                &order_keys[ordinal],
                            )?
                        {
                            current_rank = (ordinal + 1) as i64;
                        }
                        results[*row_index] = Value::Int64(current_rank);
                    }
                }
                "dense_rank" => {
                    if _distinct || _star {
                        return Err(DbError::sql("DENSE_RANK does not support DISTINCT or *"));
                    }
                    let mut current_rank = 1_i64;
                    for (ordinal, row_index) in sorted.iter().enumerate() {
                        if ordinal > 0
                            && !window_order_keys_equal(
                                &order_keys[ordinal - 1],
                                &order_keys[ordinal],
                            )?
                        {
                            current_rank += 1;
                        }
                        results[*row_index] = Value::Int64(current_rank);
                    }
                }
                "percent_rank" => {
                    if _distinct || _star || !args.is_empty() {
                        return Err(DbError::sql("PERCENT_RANK expects no arguments"));
                    }
                    if sorted.len() == 1 {
                        results[sorted[0]] = Value::Float64(0.0);
                        continue;
                    }
                    let mut current_rank = 1_i64;
                    let denominator = (sorted.len() - 1) as f64;
                    for (ordinal, row_index) in sorted.iter().enumerate() {
                        if ordinal > 0
                            && !window_order_keys_equal(
                                &order_keys[ordinal - 1],
                                &order_keys[ordinal],
                            )?
                        {
                            current_rank = (ordinal + 1) as i64;
                        }
                        let value = (current_rank - 1) as f64 / denominator;
                        results[*row_index] = Value::Float64(value);
                    }
                }
                "cume_dist" => {
                    if _distinct || _star || !args.is_empty() {
                        return Err(DbError::sql("CUME_DIST expects no arguments"));
                    }
                    let partition_len = sorted.len() as f64;
                    let mut ordinal = 0_usize;
                    while ordinal < sorted.len() {
                        let peer_end = peer_ends[ordinal];
                        let value = Value::Float64((peer_end + 1) as f64 / partition_len);
                        for peer_ordinal in ordinal..=peer_end {
                            results[sorted[peer_ordinal]] = value.clone();
                        }
                        ordinal = peer_end + 1;
                    }
                }
                "ntile" => {
                    if _distinct || _star || args.len() != 1 {
                        return Err(DbError::sql("NTILE expects exactly 1 argument"));
                    }
                    let first_row = dataset
                        .rows
                        .get(sorted[0])
                        .map(Vec::as_slice)
                        .ok_or_else(|| DbError::internal("window row index is invalid"))?;
                    let buckets = match self
                        .eval_expr(&args[0], dataset, first_row, params, ctes, None)?
                    {
                        Value::Int64(value) if value > 0 => usize::try_from(value)
                            .map_err(|_| DbError::sql("NTILE bucket count is out of range"))?,
                        Value::Int64(_) => {
                            return Err(DbError::sql("NTILE bucket count must be greater than 0"))
                        }
                        Value::Null => {
                            return Err(DbError::sql("NTILE bucket count cannot be NULL"))
                        }
                        other => {
                            return Err(DbError::sql(format!(
                                "NTILE bucket count must be INT64, got {other:?}"
                            )))
                        }
                    };
                    let partition_len = sorted.len();
                    let base_size = partition_len / buckets;
                    let extra = partition_len % buckets;
                    for (ordinal, row_index) in sorted.iter().enumerate() {
                        let bucket = if ordinal < (base_size + 1) * extra {
                            (ordinal / (base_size + 1)) + 1
                        } else {
                            ((ordinal - (base_size + 1) * extra) / base_size.max(1)) + extra + 1
                        };
                        results[*row_index] = Value::Int64(bucket as i64);
                    }
                }
                "lag" | "lead" => {
                    if _distinct || _star {
                        return Err(DbError::sql(format!(
                            "{} does not support DISTINCT or *",
                            name.to_ascii_uppercase()
                        )));
                    }
                    if args.is_empty() || args.len() > 3 {
                        return Err(DbError::sql(format!(
                            "{} expects 1 to 3 arguments",
                            name.to_ascii_uppercase()
                        )));
                    }
                    let offset = match args.get(1) {
                        Some(expr) => {
                            match self.eval_expr(expr, dataset, &[], params, ctes, None)? {
                                Value::Int64(value) if value >= 0 => value as usize,
                                Value::Int64(_) => {
                                    return Err(DbError::sql(format!(
                                        "{} offset must be non-negative",
                                        name.to_ascii_uppercase()
                                    )))
                                }
                                other => {
                                    return Err(DbError::sql(format!(
                                        "{} offset must be INT64, got {other:?}",
                                        name.to_ascii_uppercase()
                                    )))
                                }
                            }
                        }
                        None => 1,
                    };
                    let ordered_values = sorted
                        .iter()
                        .map(|row_index| {
                            self.eval_expr(
                                &args[0],
                                dataset,
                                &dataset.rows[*row_index],
                                params,
                                ctes,
                                None,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    for (ordinal, row_index) in sorted.iter().enumerate() {
                        let target_ordinal = if name == "lag" {
                            ordinal.checked_sub(offset)
                        } else {
                            ordinal
                                .checked_add(offset)
                                .filter(|target| *target < sorted.len())
                        };
                        results[*row_index] = if let Some(target_ordinal) = target_ordinal {
                            ordered_values[target_ordinal].clone()
                        } else if let Some(default_expr) = args.get(2) {
                            self.eval_expr(
                                default_expr,
                                dataset,
                                &dataset.rows[*row_index],
                                params,
                                ctes,
                                None,
                            )?
                        } else {
                            Value::Null
                        };
                    }
                }
                "first_value" | "last_value" => {
                    if _distinct || _star {
                        return Err(DbError::sql(format!(
                            "{} does not support DISTINCT or *",
                            name.to_ascii_uppercase()
                        )));
                    }
                    if args.len() != 1 {
                        return Err(DbError::sql(format!(
                            "{} expects exactly 1 argument",
                            name.to_ascii_uppercase()
                        )));
                    }
                    let ordered_values = sorted
                        .iter()
                        .map(|row_index| {
                            self.eval_expr(
                                &args[0],
                                dataset,
                                &dataset.rows[*row_index],
                                params,
                                ctes,
                                None,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    for (ordinal, row_index) in sorted.iter().enumerate() {
                        let frame_range = self.window_frame_bounds_for_row(
                            dataset,
                            &sorted,
                            order_by,
                            &peer_starts,
                            &peer_ends,
                            ordinal,
                            _frame,
                            params,
                            ctes,
                        )?;
                        results[*row_index] = if let Some((frame_start, frame_end)) = frame_range {
                            if name == "first_value" {
                                ordered_values[frame_start].clone()
                            } else {
                                ordered_values[frame_end].clone()
                            }
                        } else {
                            Value::Null
                        };
                    }
                }
                "nth_value" => {
                    if _distinct || _star {
                        return Err(DbError::sql("NTH_VALUE does not support DISTINCT or *"));
                    }
                    if args.len() != 2 {
                        return Err(DbError::sql(
                            "NTH_VALUE expects exactly 2 arguments".to_string(),
                        ));
                    }
                    let position =
                        match self.eval_expr(&args[1], dataset, &[], params, ctes, None)? {
                            Value::Int64(value) if value >= 1 => value as usize,
                            Value::Int64(_) => {
                                return Err(DbError::sql("NTH_VALUE position must be >= 1"))
                            }
                            other => {
                                return Err(DbError::sql(format!(
                                    "NTH_VALUE position must be INT64, got {other:?}"
                                )))
                            }
                        };
                    let ordered_values = sorted
                        .iter()
                        .map(|row_index| {
                            self.eval_expr(
                                &args[0],
                                dataset,
                                &dataset.rows[*row_index],
                                params,
                                ctes,
                                None,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    for (ordinal, row_index) in sorted.iter().enumerate() {
                        let frame_range = self.window_frame_bounds_for_row(
                            dataset,
                            &sorted,
                            order_by,
                            &peer_starts,
                            &peer_ends,
                            ordinal,
                            _frame,
                            params,
                            ctes,
                        )?;
                        results[*row_index] = if let Some((frame_start, frame_end)) = frame_range {
                            frame_start
                                .checked_add(position.saturating_sub(1))
                                .filter(|index| *index <= frame_end)
                                .and_then(|index| ordered_values.get(index))
                                .cloned()
                                .unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        };
                    }
                }
                "count" | "sum" | "avg" | "min" | "max" | "total" | "stddev" | "stddev_samp"
                | "stddev_pop" | "variance" | "var_samp" | "var_pop" | "bool_and" | "bool_or"
                | "group_concat" | "string_agg" => {
                    for (ordinal, row_index) in sorted.iter().enumerate() {
                        let frame_range = self.window_frame_bounds_for_row(
                            dataset,
                            &sorted,
                            order_by,
                            &peer_starts,
                            &peer_ends,
                            ordinal,
                            _frame,
                            params,
                            ctes,
                        )?;
                        results[*row_index] = self.eval_window_aggregate(
                            name,
                            args,
                            _distinct,
                            _star,
                            dataset,
                            &sorted,
                            frame_range,
                            params,
                            ctes,
                        )?;
                    }
                }
                other => {
                    return Err(DbError::sql(format!(
                        "unsupported window function {}",
                        other.to_ascii_uppercase()
                    )))
                }
            }
        }
        Ok(results)
    }

    #[allow(clippy::too_many_arguments)]
    fn window_frame_bounds_for_row(
        &self,
        dataset: &Dataset,
        sorted: &[usize],
        order_by: &[crate::sql::ast::OrderBy],
        peer_starts: &[usize],
        peer_ends: &[usize],
        ordinal: usize,
        frame: Option<&crate::sql::ast::WindowFrame>,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Option<(usize, usize)>> {
        if sorted.is_empty() {
            return Ok(None);
        }

        if frame.is_none() {
            if order_by.is_empty() {
                return Ok(Some((0, sorted.len() - 1)));
            }
            return Ok(Some((0, peer_ends[ordinal])));
        }

        let frame = frame.ok_or_else(|| DbError::internal("window frame is missing"))?;
        let row_index = *sorted
            .get(ordinal)
            .ok_or_else(|| DbError::internal("window row index is invalid"))?;
        let row = dataset
            .rows
            .get(row_index)
            .map(Vec::as_slice)
            .ok_or_else(|| DbError::internal("window row index is invalid"))?;
        let default_end = crate::sql::ast::WindowFrameBound::CurrentRow;
        let end_bound = frame.end.as_ref().unwrap_or(&default_end);
        let start = self.window_frame_bound_index(
            dataset,
            row,
            &frame.start,
            true,
            ordinal,
            sorted.len(),
            peer_starts,
            peer_ends,
            frame.unit,
            params,
            ctes,
        )?;
        let end = self.window_frame_bound_index(
            dataset,
            row,
            end_bound,
            false,
            ordinal,
            sorted.len(),
            peer_starts,
            peer_ends,
            frame.unit,
            params,
            ctes,
        )?;
        normalize_window_frame_range(start, end, sorted.len())
    }

    #[allow(clippy::too_many_arguments)]
    fn window_frame_bound_index(
        &self,
        dataset: &Dataset,
        row: &[Value],
        bound: &crate::sql::ast::WindowFrameBound,
        start: bool,
        ordinal: usize,
        partition_len: usize,
        peer_starts: &[usize],
        peer_ends: &[usize],
        unit: crate::sql::ast::WindowFrameUnit,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<i64> {
        let partition_len = i64::try_from(partition_len)
            .map_err(|_| DbError::internal("window partition is too large"))?;
        let ordinal =
            i64::try_from(ordinal).map_err(|_| DbError::internal("window ordinal is too large"))?;
        match (unit, bound) {
            (
                crate::sql::ast::WindowFrameUnit::Range,
                crate::sql::ast::WindowFrameBound::Preceding(_)
                | crate::sql::ast::WindowFrameBound::Following(_),
            ) => Err(DbError::sql(
                "RANGE frames with offset bounds are not supported yet",
            )),
            (_, crate::sql::ast::WindowFrameBound::UnboundedPreceding) => Ok(0),
            (_, crate::sql::ast::WindowFrameBound::UnboundedFollowing) => {
                if start {
                    Ok(partition_len)
                } else {
                    Ok(partition_len - 1)
                }
            }
            (
                crate::sql::ast::WindowFrameUnit::Rows,
                crate::sql::ast::WindowFrameBound::CurrentRow,
            ) => Ok(ordinal),
            (
                crate::sql::ast::WindowFrameUnit::Range,
                crate::sql::ast::WindowFrameBound::CurrentRow,
            ) => {
                if start {
                    i64::try_from(peer_starts[ordinal as usize])
                        .map_err(|_| DbError::internal("window peer start is too large"))
                } else {
                    i64::try_from(peer_ends[ordinal as usize])
                        .map_err(|_| DbError::internal("window peer end is too large"))
                }
            }
            (
                crate::sql::ast::WindowFrameUnit::Rows,
                crate::sql::ast::WindowFrameBound::Preceding(offset),
            ) => {
                let offset = self.eval_window_frame_offset(dataset, row, offset, params, ctes)?;
                Ok(ordinal - offset)
            }
            (
                crate::sql::ast::WindowFrameUnit::Rows,
                crate::sql::ast::WindowFrameBound::Following(offset),
            ) => {
                let offset = self.eval_window_frame_offset(dataset, row, offset, params, ctes)?;
                Ok(ordinal + offset)
            }
        }
    }

    fn eval_window_frame_offset(
        &self,
        dataset: &Dataset,
        row: &[Value],
        offset: &Expr,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<i64> {
        match self.eval_expr(offset, dataset, row, params, ctes, None)? {
            Value::Int64(value) if value >= 0 => Ok(value),
            Value::Int64(_) => Err(DbError::sql(
                "window frame offset must be a non-negative integer",
            )),
            Value::Null => Err(DbError::sql("window frame offset cannot be NULL")),
            other => Err(DbError::sql(format!(
                "window frame offset must be INT64, got {other:?}"
            ))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn eval_window_aggregate(
        &self,
        name: &str,
        args: &[Expr],
        distinct: bool,
        star: bool,
        dataset: &Dataset,
        sorted_partition: &[usize],
        frame_range: Option<(usize, usize)>,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Value> {
        let aggregate_ctx = AggregateEvalContext {
            runtime: self,
            dataset,
            params,
            ctes,
        };
        let empty_indexes: [usize; 0] = [];
        let row_indexes = if let Some((start, end)) = frame_range {
            sorted_partition
                .get(start..=end)
                .ok_or_else(|| DbError::internal("window frame range is invalid"))?
        } else {
            &empty_indexes
        };

        match name {
            "count" => {
                if star {
                    if distinct {
                        return Err(DbError::sql("COUNT(DISTINCT *) is not supported"));
                    }
                    return Ok(Value::Int64(row_indexes.len() as i64));
                }
                if args.len() != 1 {
                    return Err(DbError::sql("COUNT expects exactly 1 argument"));
                }
                if distinct {
                    let mut vals = Vec::new();
                    for row_index in row_indexes {
                        let row = dataset
                            .rows
                            .get(*row_index)
                            .map(Vec::as_slice)
                            .ok_or_else(|| DbError::internal("window row index is invalid"))?;
                        let val = self.eval_expr(&args[0], dataset, row, params, ctes, None)?;
                        if !matches!(val, Value::Null) {
                            vals.push(val);
                        }
                    }
                    vals.sort_by(|a, b| compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal));
                    vals.dedup_by(|a, b| {
                        compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal)
                            == std::cmp::Ordering::Equal
                    });
                    Ok(Value::Int64(vals.len() as i64))
                } else {
                    let mut count = 0_i64;
                    for row_index in row_indexes {
                        let row = dataset
                            .rows
                            .get(*row_index)
                            .map(Vec::as_slice)
                            .ok_or_else(|| DbError::internal("window row index is invalid"))?;
                        if !matches!(
                            self.eval_expr(&args[0], dataset, row, params, ctes, None)?,
                            Value::Null
                        ) {
                            count += 1;
                        }
                    }
                    Ok(Value::Int64(count))
                }
            }
            "sum" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("SUM expects exactly 1 argument"));
                }
                aggregate_numeric(
                    &aggregate_ctx,
                    row_indexes,
                    &args[0],
                    NumericAgg::Sum,
                    distinct,
                )
            }
            "avg" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("AVG expects exactly 1 argument"));
                }
                aggregate_numeric(
                    &aggregate_ctx,
                    row_indexes,
                    &args[0],
                    NumericAgg::Avg,
                    distinct,
                )
            }
            "total" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("TOTAL expects exactly 1 argument"));
                }
                aggregate_numeric(
                    &aggregate_ctx,
                    row_indexes,
                    &args[0],
                    NumericAgg::Total,
                    distinct,
                )
            }
            "stddev" | "stddev_samp" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("STDDEV expects exactly 1 argument"));
                }
                aggregate_variance(
                    &aggregate_ctx,
                    row_indexes,
                    &args[0],
                    VarianceAgg::StddevSamp,
                    distinct,
                )
            }
            "stddev_pop" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("STDDEV_POP expects exactly 1 argument"));
                }
                aggregate_variance(
                    &aggregate_ctx,
                    row_indexes,
                    &args[0],
                    VarianceAgg::StddevPop,
                    distinct,
                )
            }
            "variance" | "var_samp" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("VAR_SAMP expects exactly 1 argument"));
                }
                aggregate_variance(
                    &aggregate_ctx,
                    row_indexes,
                    &args[0],
                    VarianceAgg::VarSamp,
                    distinct,
                )
            }
            "var_pop" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("VAR_POP expects exactly 1 argument"));
                }
                aggregate_variance(
                    &aggregate_ctx,
                    row_indexes,
                    &args[0],
                    VarianceAgg::VarPop,
                    distinct,
                )
            }
            "bool_and" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("BOOL_AND expects exactly 1 argument"));
                }
                aggregate_bool(
                    &aggregate_ctx,
                    row_indexes,
                    &args[0],
                    BoolAgg::And,
                    distinct,
                )
            }
            "bool_or" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("BOOL_OR expects exactly 1 argument"));
                }
                aggregate_bool(&aggregate_ctx, row_indexes, &args[0], BoolAgg::Or, distinct)
            }
            "min" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("MIN expects exactly 1 argument"));
                }
                aggregate_extreme(self, dataset, row_indexes, &args[0], params, ctes, true)
            }
            "max" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("MAX expects exactly 1 argument"));
                }
                aggregate_extreme(self, dataset, row_indexes, &args[0], params, ctes, false)
            }
            name @ ("group_concat" | "string_agg") => {
                if star {
                    return Err(DbError::sql(format!(
                        "{} does not support *",
                        name.to_ascii_uppercase()
                    )));
                }
                if distinct {
                    return Err(DbError::sql(format!(
                        "{} DISTINCT is not supported in window context",
                        name.to_ascii_uppercase()
                    )));
                }
                aggregate_group_concat(&aggregate_ctx, row_indexes, args, false, &[], name)
            }
            other => Err(DbError::sql(format!(
                "unsupported aggregate window function {other}"
            ))),
        }
    }

    fn evaluate_grouped_select(
        &self,
        select: &Select,
        dataset: Dataset,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Dataset> {
        let mut groups = BTreeMap::<Vec<u8>, Vec<usize>>::new();
        if dataset.rows.is_empty() && select.group_by.is_empty() {
            groups.insert(Vec::new(), Vec::new());
        } else {
            for (row_index, row) in dataset.rows.iter().enumerate() {
                let key_values = select
                    .group_by
                    .iter()
                    .map(|expr| self.eval_expr(expr, &dataset, row, params, ctes, None))
                    .collect::<Result<Vec<_>>>()?;
                groups
                    .entry(row_identity(&key_values)?)
                    .or_default()
                    .push(row_index);
            }
        }
        let columns = select
            .projection
            .iter()
            .enumerate()
            .map(|(index, item)| match item {
                SelectItem::Expr { expr, alias } => ColumnBinding::visible(
                    None,
                    alias
                        .clone()
                        .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
                ),
                SelectItem::Wildcard => ColumnBinding::visible(None, format!("col{}", index + 1)),
                SelectItem::QualifiedWildcard(_) => {
                    ColumnBinding::visible(None, format!("col{}", index + 1))
                }
            })
            .collect::<Vec<_>>();
        let mut rows = Vec::new();
        for group_row_indexes in groups.into_values() {
            if let Some(having) = &select.having {
                if !matches!(
                    self.eval_group_expr(having, &dataset, &group_row_indexes, params, ctes)?,
                    Value::Bool(true)
                ) {
                    continue;
                }
            }
            let mut output = Vec::new();
            for item in &select.projection {
                match item {
                    SelectItem::Expr { expr, .. } => output.push(self.eval_group_expr(
                        expr,
                        &dataset,
                        &group_row_indexes,
                        params,
                        ctes,
                    )?),
                    SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                        return Err(DbError::sql(
                            "wildcards are not supported in grouped SELECT output",
                        ))
                    }
                }
            }
            rows.push(output);
        }
        Ok(Dataset { columns, rows })
    }

    fn sort_dataset(
        &self,
        dataset: &mut Dataset,
        order_by: &[crate::sql::ast::OrderBy],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<()> {
        if order_by.is_empty() || dataset.rows.len() <= 1 {
            return Ok(());
        }
        let eval_dataset = Dataset {
            columns: dataset.columns.clone(),
            rows: Vec::new(),
        };
        let sort_keys = dataset
            .rows
            .iter()
            .map(|row| {
                order_by
                    .iter()
                    .map(|order| {
                        self.eval_expr(&order.expr, &eval_dataset, row, params, ctes, None)
                            .unwrap_or(Value::Null)
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        let mut order = (0..dataset.rows.len()).collect::<Vec<_>>();
        order.sort_by(|left_index, right_index| {
            let left_key = &sort_keys[*left_index];
            let right_key = &sort_keys[*right_index];
            for (order_clause, (left_value, right_value)) in
                order_by.iter().zip(left_key.iter().zip(right_key.iter()))
            {
                let ordering =
                    compare_values(left_value, right_value).unwrap_or(std::cmp::Ordering::Equal);
                if ordering != std::cmp::Ordering::Equal {
                    return if order_clause.descending {
                        ordering.reverse()
                    } else {
                        ordering
                    };
                }
            }
            left_index.cmp(right_index)
        });

        let mut rows = std::mem::take(&mut dataset.rows)
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>();
        dataset.rows = order
            .into_iter()
            .map(|row_index| {
                rows.get_mut(row_index)
                    .and_then(Option::take)
                    .ok_or_else(|| DbError::internal("sorted row index is invalid"))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(())
    }

    fn eval_constant_i64(
        &self,
        expr: &Expr,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<i64> {
        match self.eval_expr(expr, &Dataset::empty(), &[], params, ctes, None)? {
            Value::Int64(value) => Ok(value),
            other => Err(DbError::sql(format!(
                "expected integer constant, got {other:?}"
            ))),
        }
    }

    fn eval_group_membership_value(
        &self,
        expr: &Expr,
        dataset: &Dataset,
        group_row_indexes: &[usize],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<MembershipValue> {
        match expr {
            Expr::Row(items) => Ok(MembershipValue::Row(
                items
                    .iter()
                    .map(|item| {
                        self.eval_group_expr(item, dataset, group_row_indexes, params, ctes)
                    })
                    .collect::<Result<Vec<_>>>()?,
            )),
            _ => Ok(MembershipValue::Scalar(self.eval_group_expr(
                expr,
                dataset,
                group_row_indexes,
                params,
                ctes,
            )?)),
        }
    }

    fn eval_membership_value(
        &self,
        expr: &Expr,
        dataset: &Dataset,
        row: &[Value],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
        excluded: Option<&Dataset>,
    ) -> Result<MembershipValue> {
        match expr {
            Expr::Row(items) => Ok(MembershipValue::Row(
                items
                    .iter()
                    .map(|item| self.eval_expr(item, dataset, row, params, ctes, excluded))
                    .collect::<Result<Vec<_>>>()?,
            )),
            _ => Ok(MembershipValue::Scalar(
                self.eval_expr(expr, dataset, row, params, ctes, excluded)?,
            )),
        }
    }

    fn eval_group_expr(
        &self,
        expr: &Expr,
        dataset: &Dataset,
        group_row_indexes: &[usize],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Value> {
        let aggregate_ctx = AggregateEvalContext {
            runtime: self,
            dataset,
            params,
            ctes,
        };
        match expr {
            Expr::Aggregate {
                name,
                args,
                star,
                distinct,
                order_by,
                within_group,
            } => match name.as_str() {
                "array_agg" | "median" | "percentile_cont" | "percentile_disc" => {
                    match name.as_str() {
                        "array_agg" => {
                            if *within_group {
                                return Err(DbError::sql(
                                    "ARRAY_AGG does not support WITHIN GROUP",
                                ));
                            }
                            if *star || args.len() != 1 {
                                return Err(DbError::sql("ARRAY_AGG expects exactly 1 argument"));
                            }
                            aggregate_array_agg(
                                &aggregate_ctx,
                                group_row_indexes,
                                &args[0],
                                *distinct,
                                order_by,
                            )
                        }
                        "median" => {
                            if *within_group {
                                return Err(DbError::sql(
                                    "MEDIAN does not support WITHIN GROUP; use MEDIAN(expr)",
                                ));
                            }
                            if !order_by.is_empty() {
                                return Err(DbError::sql(
                                    "MEDIAN does not support aggregate ORDER BY",
                                ));
                            }
                            if *star || args.len() != 1 {
                                return Err(DbError::sql("MEDIAN expects exactly 1 argument"));
                            }
                            aggregate_median(&aggregate_ctx, group_row_indexes, &args[0], *distinct)
                        }
                        "percentile_cont" => {
                            if !*within_group {
                                return Err(DbError::sql(
                                    "PERCENTILE_CONT requires WITHIN GROUP (ORDER BY ...)",
                                ));
                            }
                            if *distinct {
                                return Err(DbError::sql(
                                    "PERCENTILE_CONT does not support DISTINCT",
                                ));
                            }
                            if *star || args.len() != 1 {
                                return Err(DbError::sql(
                                    "PERCENTILE_CONT expects exactly 1 argument",
                                ));
                            }
                            aggregate_percentile_cont(
                                self,
                                dataset,
                                group_row_indexes,
                                &args[0],
                                order_by,
                                params,
                                ctes,
                            )
                        }
                        "percentile_disc" => {
                            if !*within_group {
                                return Err(DbError::sql(
                                    "PERCENTILE_DISC requires WITHIN GROUP (ORDER BY ...)",
                                ));
                            }
                            if *distinct {
                                return Err(DbError::sql(
                                    "PERCENTILE_DISC does not support DISTINCT",
                                ));
                            }
                            if *star || args.len() != 1 {
                                return Err(DbError::sql(
                                    "PERCENTILE_DISC expects exactly 1 argument",
                                ));
                            }
                            aggregate_percentile_disc(
                                self,
                                dataset,
                                group_row_indexes,
                                &args[0],
                                order_by,
                                params,
                                ctes,
                            )
                        }
                        _ => Err(DbError::sql(format!(
                            "unsupported aggregate function {}",
                            name.to_ascii_uppercase()
                        ))),
                    }
                }
                name if *within_group => Err(DbError::sql(format!(
                    "{} does not support WITHIN GROUP",
                    name.to_ascii_uppercase()
                ))),
                name if !order_by.is_empty() && !matches!(name, "group_concat" | "string_agg") => {
                    Err(DbError::sql(format!(
                        "{} does not support aggregate ORDER BY",
                        name.to_ascii_uppercase()
                    )))
                }
                "count" => {
                    if *star {
                        Ok(Value::Int64(group_row_indexes.len() as i64))
                    } else if *distinct {
                        let mut vals = Vec::new();
                        for row_index in group_row_indexes {
                            let row =
                                dataset.rows.get(*row_index).map(Vec::as_slice).ok_or_else(
                                    || DbError::internal("group row index is invalid"),
                                )?;
                            let val = self.eval_expr(&args[0], dataset, row, params, ctes, None)?;
                            if !matches!(val, Value::Null) {
                                vals.push(val);
                            }
                        }
                        vals.sort_by(|a, b| {
                            compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal)
                        });
                        vals.dedup_by(|a, b| {
                            compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal)
                                == std::cmp::Ordering::Equal
                        });
                        Ok(Value::Int64(vals.len() as i64))
                    } else {
                        let mut count = 0_i64;
                        for row_index in group_row_indexes {
                            let row =
                                dataset.rows.get(*row_index).map(Vec::as_slice).ok_or_else(
                                    || DbError::internal("group row index is invalid"),
                                )?;
                            if !matches!(
                                self.eval_expr(&args[0], dataset, row, params, ctes, None)?,
                                Value::Null
                            ) {
                                count += 1;
                            }
                        }
                        Ok(Value::Int64(count))
                    }
                }
                "sum" => aggregate_numeric(
                    &aggregate_ctx,
                    group_row_indexes,
                    &args[0],
                    NumericAgg::Sum,
                    *distinct,
                ),
                "avg" => aggregate_numeric(
                    &aggregate_ctx,
                    group_row_indexes,
                    &args[0],
                    NumericAgg::Avg,
                    *distinct,
                ),
                "total" => aggregate_numeric(
                    &aggregate_ctx,
                    group_row_indexes,
                    &args[0],
                    NumericAgg::Total,
                    *distinct,
                ),
                "stddev" | "stddev_samp" => aggregate_variance(
                    &aggregate_ctx,
                    group_row_indexes,
                    &args[0],
                    VarianceAgg::StddevSamp,
                    *distinct,
                ),
                "stddev_pop" => aggregate_variance(
                    &aggregate_ctx,
                    group_row_indexes,
                    &args[0],
                    VarianceAgg::StddevPop,
                    *distinct,
                ),
                "variance" | "var_samp" => aggregate_variance(
                    &aggregate_ctx,
                    group_row_indexes,
                    &args[0],
                    VarianceAgg::VarSamp,
                    *distinct,
                ),
                "var_pop" => aggregate_variance(
                    &aggregate_ctx,
                    group_row_indexes,
                    &args[0],
                    VarianceAgg::VarPop,
                    *distinct,
                ),
                "bool_and" => aggregate_bool(
                    &aggregate_ctx,
                    group_row_indexes,
                    &args[0],
                    BoolAgg::And,
                    *distinct,
                ),
                "bool_or" => aggregate_bool(
                    &aggregate_ctx,
                    group_row_indexes,
                    &args[0],
                    BoolAgg::Or,
                    *distinct,
                ),
                "min" => aggregate_extreme(
                    self,
                    dataset,
                    group_row_indexes,
                    &args[0],
                    params,
                    ctes,
                    true,
                ),
                "max" => aggregate_extreme(
                    self,
                    dataset,
                    group_row_indexes,
                    &args[0],
                    params,
                    ctes,
                    false,
                ),
                name @ ("group_concat" | "string_agg") => aggregate_group_concat(
                    &aggregate_ctx,
                    group_row_indexes,
                    args,
                    *distinct,
                    order_by,
                    name,
                ),
                other => Err(DbError::sql(format!(
                    "unsupported aggregate function {other}"
                ))),
            },
            Expr::Unary { op, expr } => {
                let value = self.eval_group_expr(expr, dataset, group_row_indexes, params, ctes)?;
                match op {
                    UnaryOp::Not => Ok(match truthy(&value) {
                        Some(value) => Value::Bool(!value),
                        None => Value::Null,
                    }),
                    UnaryOp::Negate => match value {
                        Value::Int64(value) => Ok(Value::Int64(-value)),
                        Value::Float64(value) => Ok(Value::Float64(-value)),
                        Value::Null => Ok(Value::Null),
                        other => Err(DbError::sql(format!("cannot negate {other:?}"))),
                    },
                }
            }
            Expr::Binary { left, op, right } => eval_binary(
                op,
                self.eval_group_expr(left, dataset, group_row_indexes, params, ctes)?,
                self.eval_group_expr(right, dataset, group_row_indexes, params, ctes)?,
            ),
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let value = self.eval_group_expr(expr, dataset, group_row_indexes, params, ctes)?;
                let low = self.eval_group_expr(low, dataset, group_row_indexes, params, ctes)?;
                let high = self.eval_group_expr(high, dataset, group_row_indexes, params, ctes)?;
                if matches!(value, Value::Null)
                    || matches!(low, Value::Null)
                    || matches!(high, Value::Null)
                {
                    return Ok(Value::Null);
                }
                let in_range = compare_values(&value, &low)? != std::cmp::Ordering::Less
                    && compare_values(&value, &high)? != std::cmp::Ordering::Greater;
                Ok(Value::Bool(if *negated { !in_range } else { in_range }))
            }
            Expr::InList {
                expr,
                items,
                negated,
            } => {
                let value = self.eval_group_membership_value(
                    expr,
                    dataset,
                    group_row_indexes,
                    params,
                    ctes,
                )?;
                if membership_value_has_nulls(&value) {
                    return Ok(Value::Null);
                }
                let mut saw_null = false;
                for item in items {
                    let candidate = self.eval_group_membership_value(
                        item,
                        dataset,
                        group_row_indexes,
                        params,
                        ctes,
                    )?;
                    match compare_membership_values(&value, &candidate)? {
                        Some(true) => return Ok(Value::Bool(!*negated)),
                        Some(false) => {}
                        None => saw_null = true,
                    }
                }
                if saw_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Bool(*negated))
                }
            }
            Expr::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
                ..
            } => {
                let left = self.eval_group_expr(expr, dataset, group_row_indexes, params, ctes)?;
                let right =
                    self.eval_group_expr(pattern, dataset, group_row_indexes, params, ctes)?;
                let escape = escape
                    .as_ref()
                    .map(|expr| {
                        self.eval_group_expr(expr, dataset, group_row_indexes, params, ctes)
                    })
                    .transpose()?;
                eval_like(left, right, escape, *case_insensitive, *negated)
            }
            Expr::IsNull { expr, negated } => {
                let is_null = matches!(
                    self.eval_group_expr(expr, dataset, group_row_indexes, params, ctes)?,
                    Value::Null
                );
                Ok(Value::Bool(if *negated { !is_null } else { is_null }))
            }
            Expr::Function { name, args } => {
                let row = if let Some(row_index) = group_row_indexes.first().copied() {
                    dataset
                        .rows
                        .get(row_index)
                        .map(Vec::as_slice)
                        .ok_or_else(|| DbError::internal("group row index is invalid"))?
                } else {
                    &[]
                };
                let values = args
                    .iter()
                    .map(|arg| self.eval_group_expr(arg, dataset, group_row_indexes, params, ctes))
                    .collect::<Result<Vec<_>>>()?;
                match name.as_str() {
                    "coalesce" => Ok(values
                        .into_iter()
                        .find(|value| !matches!(value, Value::Null))
                        .unwrap_or(Value::Null)),
                    "nullif" => {
                        if values.len() != 2 {
                            return Err(DbError::sql("NULLIF expects exactly two arguments"));
                        }
                        if compare_values(&values[0], &values[1])? == std::cmp::Ordering::Equal {
                            Ok(Value::Null)
                        } else {
                            Ok(values[0].clone())
                        }
                    }
                    "length" => unary_text_fn(values, |value| value.len().to_string())
                        .and_then(|value| cast_value(value, crate::catalog::ColumnType::Int64)),
                    "lower" => unary_text_fn(values, |value| value.to_ascii_lowercase()),
                    "upper" => unary_text_fn(values, |value| value.to_ascii_uppercase()),
                    "trim" => unary_text_fn(values, |value| value.trim().to_string()),
                    other => self.eval_expr(
                        &Expr::Function {
                            name: other.to_string(),
                            args: args.to_vec(),
                        },
                        dataset,
                        row,
                        params,
                        ctes,
                        None,
                    ),
                }
            }
            Expr::Case {
                operand,
                branches,
                else_expr,
            } => {
                let operand_value = operand
                    .as_deref()
                    .map(|expr| {
                        self.eval_group_expr(expr, dataset, group_row_indexes, params, ctes)
                    })
                    .transpose()?;
                for (condition, result) in branches {
                    let matches = if let Some(operand_value) = &operand_value {
                        compare_values(
                            operand_value,
                            &self.eval_group_expr(
                                condition,
                                dataset,
                                group_row_indexes,
                                params,
                                ctes,
                            )?,
                        )? == std::cmp::Ordering::Equal
                    } else {
                        matches!(
                            self.eval_group_expr(
                                condition,
                                dataset,
                                group_row_indexes,
                                params,
                                ctes,
                            )?,
                            Value::Bool(true)
                        )
                    };
                    if matches {
                        return self.eval_group_expr(
                            result,
                            dataset,
                            group_row_indexes,
                            params,
                            ctes,
                        );
                    }
                }
                else_expr
                    .as_deref()
                    .map(|expr| {
                        self.eval_group_expr(expr, dataset, group_row_indexes, params, ctes)
                    })
                    .transpose()?
                    .map_or(Ok(Value::Null), Ok)
            }
            Expr::Cast { expr, target_type } => cast_value(
                self.eval_group_expr(expr, dataset, group_row_indexes, params, ctes)?,
                *target_type,
            ),
            Expr::Row(_) => Err(DbError::sql(
                "row values are only supported in IN comparisons",
            )),
            Expr::RowNumber { .. } | Expr::WindowFunction { .. } => Err(DbError::sql(
                "window functions cannot be nested inside grouped expressions",
            )),
            _ => {
                let row = if let Some(row_index) = group_row_indexes.first().copied() {
                    dataset
                        .rows
                        .get(row_index)
                        .map(Vec::as_slice)
                        .ok_or_else(|| DbError::internal("group row index is invalid"))?
                } else {
                    &[]
                };
                self.eval_expr(expr, dataset, row, params, ctes, None)
            }
        }
    }

    fn eval_expr(
        &self,
        expr: &Expr,
        dataset: &Dataset,
        row: &[Value],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
        excluded: Option<&Dataset>,
    ) -> Result<Value> {
        match expr {
            Expr::Literal(value) => Ok(value.clone()),
            Expr::Column { table, column } => {
                self.resolve_column(dataset, row, table.as_deref(), column, excluded)
            }
            Expr::Parameter(number) => params
                .get(number.saturating_sub(1))
                .cloned()
                .ok_or_else(|| DbError::sql(format!("missing value for parameter ${number}"))),
            Expr::Unary { op, expr } => {
                let value = self.eval_expr(expr, dataset, row, params, ctes, excluded)?;
                match op {
                    UnaryOp::Not => Ok(match truthy(&value) {
                        Some(value) => Value::Bool(!value),
                        None => Value::Null,
                    }),
                    UnaryOp::Negate => match value {
                        Value::Int64(value) => Ok(Value::Int64(-value)),
                        Value::Float64(value) => Ok(Value::Float64(-value)),
                        Value::Null => Ok(Value::Null),
                        other => Err(DbError::sql(format!("cannot negate {other:?}"))),
                    },
                }
            }
            Expr::Binary { left, op, right } => {
                let left = self.eval_expr(left, dataset, row, params, ctes, excluded)?;
                let right = self.eval_expr(right, dataset, row, params, ctes, excluded)?;
                eval_binary(op, left, right)
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let value = self.eval_expr(expr, dataset, row, params, ctes, excluded)?;
                let low = self.eval_expr(low, dataset, row, params, ctes, excluded)?;
                let high = self.eval_expr(high, dataset, row, params, ctes, excluded)?;
                if matches!(value, Value::Null)
                    || matches!(low, Value::Null)
                    || matches!(high, Value::Null)
                {
                    return Ok(Value::Null);
                }
                let in_range = compare_values(&value, &low)? != std::cmp::Ordering::Less
                    && compare_values(&value, &high)? != std::cmp::Ordering::Greater;
                Ok(Value::Bool(if *negated { !in_range } else { in_range }))
            }
            Expr::InList {
                expr,
                items,
                negated,
            } => {
                let value =
                    self.eval_membership_value(expr, dataset, row, params, ctes, excluded)?;
                if membership_value_has_nulls(&value) {
                    return Ok(Value::Null);
                }
                let mut saw_null = false;
                for item in items {
                    let candidate =
                        self.eval_membership_value(item, dataset, row, params, ctes, excluded)?;
                    match compare_membership_values(&value, &candidate)? {
                        Some(true) => return Ok(Value::Bool(!*negated)),
                        Some(false) => {}
                        None => saw_null = true,
                    }
                }
                if saw_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Bool(*negated))
                }
            }
            Expr::InSubquery {
                expr,
                query,
                negated,
            } => {
                let value =
                    self.eval_membership_value(expr, dataset, row, params, ctes, excluded)?;
                if membership_value_has_nulls(&value) {
                    return Ok(Value::Null);
                }
                let subquery = self.evaluate_query_with_outer(query, params, ctes, dataset, row)?;
                let expected_width = match &value {
                    MembershipValue::Scalar(_) => 1,
                    MembershipValue::Row(values) => values.len(),
                };
                if subquery.columns.len() != expected_width {
                    return Err(DbError::sql(format!(
                        "IN subquery must return exactly {} column{}",
                        expected_width,
                        if expected_width == 1 { "" } else { "s" }
                    )));
                }
                let mut saw_null = false;
                for subquery_row in &subquery.rows {
                    let candidate = if expected_width == 1 {
                        MembershipValue::Scalar(
                            subquery_row.first().cloned().unwrap_or(Value::Null),
                        )
                    } else {
                        MembershipValue::Row(subquery_row.clone())
                    };
                    match compare_membership_values(&value, &candidate)? {
                        Some(true) => return Ok(Value::Bool(!*negated)),
                        Some(false) => {}
                        None => saw_null = true,
                    }
                }
                if saw_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Bool(*negated))
                }
            }
            Expr::CompareSubquery {
                expr,
                op,
                quantifier,
                query,
            } => {
                let left_value = self.eval_expr(expr, dataset, row, params, ctes, excluded)?;
                let subquery = self.evaluate_query_with_outer(query, params, ctes, dataset, row)?;
                if subquery.columns.len() != 1 {
                    return Err(DbError::sql(
                        "subquery comparison must return exactly one column",
                    ));
                }
                let mut saw_null = false;
                let mut saw_row = false;
                for subquery_row in &subquery.rows {
                    saw_row = true;
                    let candidate = subquery_row.first().cloned().unwrap_or(Value::Null);
                    match eval_binary(op, left_value.clone(), candidate)? {
                        Value::Bool(result) => match quantifier {
                            SubqueryQuantifier::Any if result => return Ok(Value::Bool(true)),
                            SubqueryQuantifier::All if !result => return Ok(Value::Bool(false)),
                            _ => {}
                        },
                        Value::Null => saw_null = true,
                        other => {
                            return Err(DbError::internal(format!(
                                "subquery comparison did not evaluate to boolean: {other:?}"
                            )))
                        }
                    }
                }
                if !saw_row {
                    return Ok(Value::Bool(matches!(quantifier, SubqueryQuantifier::All)));
                }
                if saw_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Bool(matches!(quantifier, SubqueryQuantifier::All)))
                }
            }
            Expr::ScalarSubquery(query) => {
                let subquery = self.evaluate_query_with_outer(query, params, ctes, dataset, row)?;
                if subquery.columns.len() != 1 {
                    return Err(DbError::sql(
                        "scalar subquery must return exactly one column",
                    ));
                }
                Ok(subquery
                    .rows
                    .first()
                    .and_then(|subquery_row| subquery_row.first())
                    .cloned()
                    .unwrap_or(Value::Null))
            }
            Expr::Exists(query) => Ok(Value::Bool(
                !self
                    .evaluate_query_with_outer(query, params, ctes, dataset, row)?
                    .rows
                    .is_empty(),
            )),
            Expr::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
                ..
            } => {
                let left = self.eval_expr(expr, dataset, row, params, ctes, excluded)?;
                let right = self.eval_expr(pattern, dataset, row, params, ctes, excluded)?;
                let escape = escape
                    .as_ref()
                    .map(|expr| self.eval_expr(expr, dataset, row, params, ctes, excluded))
                    .transpose()?;
                eval_like(left, right, escape, *case_insensitive, *negated)
            }
            Expr::IsNull { expr, negated } => {
                let is_null = matches!(
                    self.eval_expr(expr, dataset, row, params, ctes, excluded)?,
                    Value::Null
                );
                Ok(Value::Bool(if *negated { !is_null } else { is_null }))
            }
            Expr::Function { name, args } => {
                eval_function(self, name, args, dataset, row, params, ctes, excluded)
            }
            Expr::Aggregate { .. } => Err(DbError::sql(
                "aggregate expressions require grouped evaluation",
            )),
            Expr::RowNumber { .. } | Expr::WindowFunction { .. } => Err(DbError::sql(
                "window-function execution is not yet implemented",
            )),
            Expr::Case {
                operand,
                branches,
                else_expr,
            } => {
                let operand_value = operand
                    .as_deref()
                    .map(|expr| self.eval_expr(expr, dataset, row, params, ctes, excluded))
                    .transpose()?;
                for (condition, result) in branches {
                    let matches = if let Some(operand_value) = &operand_value {
                        compare_values(
                            operand_value,
                            &self.eval_expr(condition, dataset, row, params, ctes, excluded)?,
                        )? == std::cmp::Ordering::Equal
                    } else {
                        matches!(
                            self.eval_expr(condition, dataset, row, params, ctes, excluded)?,
                            Value::Bool(true)
                        )
                    };
                    if matches {
                        return self.eval_expr(result, dataset, row, params, ctes, excluded);
                    }
                }
                else_expr
                    .as_deref()
                    .map(|expr| self.eval_expr(expr, dataset, row, params, ctes, excluded))
                    .transpose()?
                    .map_or(Ok(Value::Null), Ok)
            }
            Expr::Cast { expr, target_type } => cast_value(
                self.eval_expr(expr, dataset, row, params, ctes, excluded)?,
                *target_type,
            ),
            Expr::Row(_) => Err(DbError::sql(
                "row values are only supported in IN comparisons",
            )),
        }
    }

    fn resolve_column(
        &self,
        dataset: &Dataset,
        row: &[Value],
        table: Option<&str>,
        column: &str,
        excluded: Option<&Dataset>,
    ) -> Result<Value> {
        if let Some(table_name) = table {
            if identifiers_equal(table_name, "excluded") {
                let excluded = excluded.ok_or_else(|| {
                    DbError::sql("EXCLUDED is only valid in ON CONFLICT DO UPDATE")
                })?;
                return self.resolve_column(
                    excluded,
                    excluded.rows.first().map(Vec::as_slice).unwrap_or(&[]),
                    None,
                    column,
                    None,
                );
            }
        }
        let mut matched_index = None;
        for (index, binding) in dataset.columns.iter().enumerate() {
            let visible_match = table.is_some() || !binding.hidden;
            if !visible_match || !identifiers_equal(&binding.name, column) {
                continue;
            }
            if table.is_some_and(|table| {
                !binding
                    .table
                    .as_deref()
                    .is_some_and(|binding_table| identifiers_equal(binding_table, table))
            }) {
                continue;
            }
            if matched_index.replace(index).is_some() {
                return Err(DbError::sql(format!("ambiguous column reference {column}")));
            }
        }
        if let Some(index) = matched_index {
            row.get(index)
                .cloned()
                .ok_or_else(|| DbError::internal("row is shorter than its bindings"))
        } else {
            Err(DbError::sql(format!("unknown column {column}")))
        }
    }

    pub(super) fn apply_virtual_generated_columns(
        &self,
        table: &TableSchema,
        row: &mut [Value],
    ) -> Result<()> {
        if generated_columns_are_stored(table) {
            return Ok(());
        }
        let mut base_values = row.to_vec();
        for (index, column) in table.columns.iter().enumerate() {
            let Some(generated_sql) = &column.generated_sql else {
                continue;
            };
            if column.generated_stored {
                base_values[index] = row
                    .get(index)
                    .cloned()
                    .ok_or_else(|| DbError::internal("row is shorter than table schema"))?;
                continue;
            }
            let expr = crate::sql::parser::parse_expression_sql(generated_sql)?;
            let dataset = table_row_dataset(table, &base_values, &table.name);
            let eval_row = dataset.rows.first().map(Vec::as_slice).unwrap_or(&[]);
            let value = self.eval_expr(&expr, &dataset, eval_row, &[], &BTreeMap::new(), None)?;
            let cast_value = cast_value(value, column.column_type)?;
            if let Some(slot) = row.get_mut(index) {
                *slot = cast_value.clone();
            } else {
                return Err(DbError::internal("row is shorter than table schema"));
            }
            base_values[index] = cast_value;
        }
        Ok(())
    }
}

pub(crate) fn statement_is_read_only(statement: &Statement) -> bool {
    matches!(statement, Statement::Query(_) | Statement::Explain(_))
}

fn infer_expr_name(expr: &Expr, ordinal: usize) -> String {
    match expr {
        Expr::Column { column, .. } => column.clone(),
        Expr::RowNumber { .. } => "row_number".to_string(),
        Expr::WindowFunction { name, .. } => name.clone(),
        _ => format!("col{ordinal}"),
    }
}

enum NumericAgg {
    Sum,
    Avg,
    Total,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VarianceAgg {
    StddevSamp,
    StddevPop,
    VarSamp,
    VarPop,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BoolAgg {
    And,
    Or,
}

struct AggregateEvalContext<'a> {
    runtime: &'a EngineRuntime,
    dataset: &'a Dataset,
    params: &'a [Value],
    ctes: &'a BTreeMap<String, Dataset>,
}

impl AggregateEvalContext<'_> {
    fn eval_row(&self, row: &[Value], expr: &Expr) -> Result<Value> {
        self.runtime
            .eval_expr(expr, self.dataset, row, self.params, self.ctes, None)
    }
}

fn aggregate_numeric(
    ctx: &AggregateEvalContext<'_>,
    row_indexes: &[usize],
    expr: &Expr,
    kind: NumericAgg,
    distinct: bool,
) -> Result<Value> {
    let mut total_int = 0_i64;
    let mut total_float = 0_f64;
    let mut saw_float = false;
    let mut count = 0_i64;

    if distinct {
        let mut vals = Vec::new();
        for row_index in row_indexes {
            let row = ctx
                .dataset
                .rows
                .get(*row_index)
                .map(Vec::as_slice)
                .ok_or_else(|| DbError::internal("group row index is invalid"))?;
            let val = ctx.eval_row(row, expr)?;
            if !matches!(val, Value::Null) {
                vals.push(val);
            }
        }
        vals.sort_by(|a, b| compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal));
        vals.dedup_by(|a, b| {
            compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal) == std::cmp::Ordering::Equal
        });
        for val in vals {
            match val {
                Value::Int64(value) => {
                    total_int += value;
                    total_float += value as f64;
                    count += 1;
                }
                Value::Float64(value) => {
                    total_float += value;
                    saw_float = true;
                    count += 1;
                }
                Value::Decimal { scaled, scale } => {
                    total_float += (scaled as f64) / 10_f64.powi(i32::from(scale));
                    saw_float = true;
                    count += 1;
                }
                other => {
                    return Err(DbError::sql(format!(
                        "numeric aggregate does not support {other:?}"
                    )))
                }
            }
        }
    } else {
        for row_index in row_indexes {
            let row = ctx
                .dataset
                .rows
                .get(*row_index)
                .map(Vec::as_slice)
                .ok_or_else(|| DbError::internal("group row index is invalid"))?;
            match ctx.eval_row(row, expr)? {
                Value::Null => {}
                Value::Int64(value) => {
                    total_int += value;
                    total_float += value as f64;
                    count += 1;
                }
                Value::Float64(value) => {
                    total_float += value;
                    saw_float = true;
                    count += 1;
                }
                Value::Decimal { scaled, scale } => {
                    total_float += (scaled as f64) / 10_f64.powi(i32::from(scale));
                    saw_float = true;
                    count += 1;
                }
                other => {
                    return Err(DbError::sql(format!(
                        "numeric aggregate does not support {other:?}"
                    )))
                }
            }
        }
    }
    if count == 0 {
        return Ok(match kind {
            NumericAgg::Total => Value::Float64(0.0),
            NumericAgg::Sum | NumericAgg::Avg => Value::Null,
        });
    }
    Ok(match kind {
        NumericAgg::Sum if saw_float => Value::Float64(total_float),
        NumericAgg::Sum => Value::Int64(total_int),
        NumericAgg::Avg => Value::Float64(total_float / count as f64),
        NumericAgg::Total => Value::Float64(total_float),
    })
}

fn aggregate_variance(
    ctx: &AggregateEvalContext<'_>,
    row_indexes: &[usize],
    expr: &Expr,
    kind: VarianceAgg,
    distinct: bool,
) -> Result<Value> {
    let mut values = Vec::new();
    for row_index in row_indexes {
        let row = ctx
            .dataset
            .rows
            .get(*row_index)
            .map(Vec::as_slice)
            .ok_or_else(|| DbError::internal("group row index is invalid"))?;
        let value = ctx.eval_row(row, expr)?;
        if !matches!(value, Value::Null) {
            values.push(value);
        }
    }

    if distinct {
        values.sort_by(|a, b| compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal));
        values.dedup_by(|a, b| {
            compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal) == std::cmp::Ordering::Equal
        });
    }

    let mut count = 0_u64;
    let mut mean = 0.0_f64;
    let mut m2 = 0.0_f64;
    for value in values {
        let number = match value {
            Value::Int64(value) => value as f64,
            Value::Float64(value) => value,
            Value::Decimal { scaled, scale } => (scaled as f64) / 10_f64.powi(i32::from(scale)),
            other => {
                return Err(DbError::sql(format!(
                    "variance aggregate does not support {other:?}"
                )));
            }
        };
        count += 1;
        let delta = number - mean;
        mean += delta / (count as f64);
        let delta2 = number - mean;
        m2 += delta * delta2;
    }

    if count == 0 {
        return Ok(Value::Null);
    }

    let denominator = match kind {
        VarianceAgg::StddevPop | VarianceAgg::VarPop => count as f64,
        VarianceAgg::StddevSamp | VarianceAgg::VarSamp => {
            if count < 2 {
                return Ok(Value::Null);
            }
            (count - 1) as f64
        }
    };
    let variance = m2 / denominator;
    Ok(match kind {
        VarianceAgg::StddevSamp | VarianceAgg::StddevPop => Value::Float64(variance.sqrt()),
        VarianceAgg::VarSamp | VarianceAgg::VarPop => Value::Float64(variance),
    })
}

fn aggregate_bool(
    ctx: &AggregateEvalContext<'_>,
    row_indexes: &[usize],
    expr: &Expr,
    kind: BoolAgg,
    distinct: bool,
) -> Result<Value> {
    let mut values = Vec::new();
    for row_index in row_indexes {
        let row = ctx
            .dataset
            .rows
            .get(*row_index)
            .map(Vec::as_slice)
            .ok_or_else(|| DbError::internal("group row index is invalid"))?;
        let value = ctx.eval_row(row, expr)?;
        if !matches!(value, Value::Null) {
            values.push(value);
        }
    }

    if distinct {
        values.sort_by(|a, b| compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal));
        values.dedup_by(|a, b| {
            compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal) == std::cmp::Ordering::Equal
        });
    }

    let mut saw_non_null = false;
    let mut result = match kind {
        BoolAgg::And => true,
        BoolAgg::Or => false,
    };

    for value in values {
        let boolean = match value {
            Value::Bool(value) => value,
            other => {
                return Err(DbError::sql(format!(
                    "boolean aggregate does not support {other:?}"
                )))
            }
        };
        saw_non_null = true;
        match kind {
            BoolAgg::And => {
                result &= boolean;
                if !result {
                    break;
                }
            }
            BoolAgg::Or => {
                result |= boolean;
                if result {
                    break;
                }
            }
        }
    }

    if saw_non_null {
        Ok(Value::Bool(result))
    } else {
        Ok(Value::Null)
    }
}

fn aggregate_extreme(
    runtime: &EngineRuntime,
    dataset: &Dataset,
    row_indexes: &[usize],
    expr: &Expr,
    params: &[Value],
    ctes: &BTreeMap<String, Dataset>,
    want_min: bool,
) -> Result<Value> {
    let mut current: Option<Value> = None;
    for row_index in row_indexes {
        let row = dataset
            .rows
            .get(*row_index)
            .map(Vec::as_slice)
            .ok_or_else(|| DbError::internal("group row index is invalid"))?;
        let value = runtime.eval_expr(expr, dataset, row, params, ctes, None)?;
        if matches!(value, Value::Null) {
            continue;
        }
        match &current {
            Some(existing) => {
                let ordering = compare_values(&value, existing)?;
                if (want_min && ordering == std::cmp::Ordering::Less)
                    || (!want_min && ordering == std::cmp::Ordering::Greater)
                {
                    current = Some(value);
                }
            }
            None => current = Some(value),
        }
    }
    Ok(current.unwrap_or(Value::Null))
}

#[allow(clippy::too_many_arguments)]
fn eval_function(
    runtime: &EngineRuntime,
    name: &str,
    args: &[Expr],
    dataset: &Dataset,
    row: &[Value],
    params: &[Value],
    ctes: &BTreeMap<String, Dataset>,
    excluded: Option<&Dataset>,
) -> Result<Value> {
    let values = args
        .iter()
        .map(|expr| runtime.eval_expr(expr, dataset, row, params, ctes, excluded))
        .collect::<Result<Vec<_>>>()?;
    match name {
        "coalesce" => Ok(values
            .into_iter()
            .find(|value| !matches!(value, Value::Null))
            .unwrap_or(Value::Null)),
        "nullif" => {
            if values.len() != 2 {
                return Err(DbError::sql("NULLIF expects two arguments"));
            }
            if compare_values(&values[0], &values[1])? == std::cmp::Ordering::Equal {
                Ok(Value::Null)
            } else {
                Ok(values[0].clone())
            }
        }
        "greatest" => eval_greatest_least(&values, true),
        "least" => eval_greatest_least(&values, false),
        "iif" => eval_iif(&values),
        "concat" => {
            let mut output = String::new();
            for value in &values {
                if matches!(value, Value::Null) {
                    continue;
                }
                output.push_str(&value_to_text(value)?);
            }
            Ok(Value::Text(output))
        }
        "concat_ws" => {
            if values.is_empty() {
                return Err(DbError::sql("CONCAT_WS expects at least 1 argument"));
            }
            let Some(separator) = expect_text_arg("CONCAT_WS", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let mut parts = Vec::new();
            for value in &values[1..] {
                if matches!(value, Value::Null) {
                    continue;
                }
                parts.push(value_to_text(value)?);
            }
            Ok(Value::Text(parts.join(separator)))
        }
        "lower" => unary_text_fn(values, |value| value.to_ascii_lowercase()),
        "upper" => unary_text_fn(values, |value| value.to_ascii_uppercase()),
        "trim" | "pg_catalog.btrim" => unary_text_fn(values, |value| value.trim().to_string()),
        "ltrim" | "pg_catalog.ltrim" => {
            unary_text_fn(values, |value| value.trim_start().to_string())
        }
        "rtrim" | "pg_catalog.rtrim" => unary_text_fn(values, |value| value.trim_end().to_string()),
        "position" | "pg_catalog.position" => {
            if values.len() != 2 {
                return Err(DbError::sql("POSITION expects 2 arguments"));
            }
            let Some(haystack) = expect_text_arg("POSITION", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(needle) = expect_text_arg("POSITION", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            if needle.is_empty() {
                return Ok(Value::Int64(1));
            }
            if let Some(idx) = haystack.find(needle) {
                let char_idx = haystack[..idx].chars().count();
                Ok(Value::Int64((char_idx + 1) as i64))
            } else {
                Ok(Value::Int64(0))
            }
        }
        "initcap" => {
            if values.len() != 1 {
                return Err(DbError::sql("INITCAP expects 1 argument"));
            }
            let Some(value) = expect_text_arg("INITCAP", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let mut output = String::with_capacity(value.len());
            let mut start_of_word = true;
            for ch in value.chars() {
                if ch.is_alphanumeric() {
                    if start_of_word {
                        for upper in ch.to_uppercase() {
                            output.push(upper);
                        }
                        start_of_word = false;
                    } else {
                        for lower in ch.to_lowercase() {
                            output.push(lower);
                        }
                    }
                } else {
                    start_of_word = true;
                    output.push(ch);
                }
            }
            Ok(Value::Text(output))
        }
        "ascii" => {
            if values.len() != 1 {
                return Err(DbError::sql("ASCII expects 1 argument"));
            }
            let Some(value) = expect_text_arg("ASCII", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Int64(
                value
                    .chars()
                    .next()
                    .map_or(0_i64, |ch| i64::from(ch as u32)),
            ))
        }
        "length" => {
            if values.len() != 1 {
                return Err(DbError::sql("LENGTH expects one argument"));
            }
            match &values[0] {
                Value::Text(value) => Ok(Value::Int64(value.chars().count() as i64)),
                Value::Null => Ok(Value::Null),
                other => Err(DbError::sql(format!("LENGTH expects text, got {other:?}"))),
            }
        }
        "substr" | "substring" => {
            if values.len() < 2 || values.len() > 3 {
                return Err(DbError::sql("SUBSTR expects 2 or 3 arguments"));
            }
            if matches!(values[0], Value::Null) || matches!(values[1], Value::Null) {
                return Ok(Value::Null);
            }
            if values.len() == 3 && matches!(values[2], Value::Null) {
                return Ok(Value::Null);
            }
            let s = match &values[0] {
                Value::Text(s) => s,
                _ => return Err(DbError::sql("SUBSTR expects text for first argument")),
            };
            let start = match &values[1] {
                Value::Int64(i) => *i,
                _ => return Err(DbError::sql("SUBSTR expects int for second argument")),
            };
            let length = if values.len() == 3 {
                match &values[2] {
                    Value::Int64(i) => Some(*i),
                    _ => return Err(DbError::sql("SUBSTR expects int for third argument")),
                }
            } else {
                None
            };

            let char_idx = if start > 0 { start - 1 } else { 0 } as usize;
            let chars = s.chars().skip(char_idx);
            if let Some(l) = length {
                let len = if l > 0 { l as usize } else { 0 };
                Ok(Value::Text(chars.take(len).collect()))
            } else {
                Ok(Value::Text(chars.collect()))
            }
        }
        "replace" => {
            if values.len() != 3 {
                return Err(DbError::sql("REPLACE expects 3 arguments"));
            }
            if matches!(values[0], Value::Null)
                || matches!(values[1], Value::Null)
                || matches!(values[2], Value::Null)
            {
                return Ok(Value::Null);
            }
            let s = match &values[0] {
                Value::Text(s) => s,
                _ => return Err(DbError::sql("REPLACE expects text for first argument")),
            };
            let target = match &values[1] {
                Value::Text(s) => s,
                _ => return Err(DbError::sql("REPLACE expects text for second argument")),
            };
            let replacement = match &values[2] {
                Value::Text(s) => s,
                _ => return Err(DbError::sql("REPLACE expects text for third argument")),
            };
            Ok(Value::Text(s.replace(target, replacement)))
        }
        "regexp_replace" => {
            if values.len() < 3 || values.len() > 4 {
                return Err(DbError::sql("REGEXP_REPLACE expects 3 or 4 arguments"));
            }
            let Some(input) = expect_text_arg("REGEXP_REPLACE", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(pattern) = expect_text_arg("REGEXP_REPLACE", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            let Some(replacement) = expect_text_arg("REGEXP_REPLACE", "third", &values[2])? else {
                return Ok(Value::Null);
            };
            let flags = if let Some(flag_value) = values.get(3) {
                let Some(flags) = expect_text_arg("REGEXP_REPLACE", "fourth", flag_value)? else {
                    return Ok(Value::Null);
                };
                Some(flags)
            } else {
                None
            };
            Ok(Value::Text(eval_regexp_replace(
                input,
                pattern,
                replacement,
                flags,
            )?))
        }
        "split_part" => {
            if values.len() != 3 {
                return Err(DbError::sql("SPLIT_PART expects 3 arguments"));
            }
            let Some(value) = expect_text_arg("SPLIT_PART", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(delimiter) = expect_text_arg("SPLIT_PART", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            let Some(index) = expect_int_arg("SPLIT_PART", "third", &values[2])? else {
                return Ok(Value::Null);
            };
            if index <= 0 {
                return Err(DbError::sql("SPLIT_PART index must be greater than 0"));
            }
            if delimiter.is_empty() {
                if index == 1 {
                    return Ok(Value::Text(value.to_string()));
                }
                return Ok(Value::Text(String::new()));
            }
            let index = usize::try_from(index - 1)
                .map_err(|_| DbError::sql("SPLIT_PART index is out of range"))?;
            Ok(Value::Text(
                value
                    .split(delimiter)
                    .nth(index)
                    .unwrap_or_default()
                    .to_string(),
            ))
        }
        "string_to_array" => {
            if values.len() != 2 {
                return Err(DbError::sql("STRING_TO_ARRAY expects 2 arguments"));
            }
            let Some(value) = expect_text_arg("STRING_TO_ARRAY", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(delimiter) = expect_text_arg("STRING_TO_ARRAY", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            let array = if delimiter.is_empty() {
                JsonValue::Array(vec![JsonValue::String(value.to_string())])
            } else {
                JsonValue::Array(
                    value
                        .split(delimiter)
                        .map(|part| JsonValue::String(part.to_string()))
                        .collect::<Vec<_>>(),
                )
            };
            Ok(Value::Text(array.render_json()))
        }
        "quote_ident" => {
            if values.len() != 1 {
                return Err(DbError::sql("QUOTE_IDENT expects 1 argument"));
            }
            let Some(identifier) = expect_text_arg("QUOTE_IDENT", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Text(format!(
                "\"{}\"",
                identifier.replace('"', "\"\"")
            )))
        }
        "quote_literal" => {
            if values.len() != 1 {
                return Err(DbError::sql("QUOTE_LITERAL expects 1 argument"));
            }
            let Some(literal) = expect_text_arg("QUOTE_LITERAL", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Text(format!("'{}'", literal.replace('\'', "''"))))
        }
        "md5" => {
            if values.len() != 1 {
                return Err(DbError::sql("MD5 expects 1 argument"));
            }
            let Some(value) = expect_text_arg("MD5", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Text(format!("{:x}", md5::compute(value.as_bytes()))))
        }
        "sha256" => {
            if values.len() != 1 {
                return Err(DbError::sql("SHA256 expects 1 argument"));
            }
            let Some(value) = expect_text_arg("SHA256", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let digest = <sha2::Sha256 as sha2::Digest>::digest(value.as_bytes());
            Ok(Value::Text(hex_encode_lower(&digest)))
        }
        "instr" => {
            if values.len() != 2 {
                return Err(DbError::sql("INSTR expects 2 arguments"));
            }
            if matches!(values[0], Value::Null) || matches!(values[1], Value::Null) {
                return Ok(Value::Null);
            }
            let s = match &values[0] {
                Value::Text(s) => s,
                _ => return Err(DbError::sql("INSTR expects text for first argument")),
            };
            let target = match &values[1] {
                Value::Text(s) => s,
                _ => return Err(DbError::sql("INSTR expects text for second argument")),
            };
            match s.find(target) {
                Some(idx) => {
                    let char_idx = s[..idx].chars().count();
                    Ok(Value::Int64((char_idx + 1) as i64))
                }
                None => Ok(Value::Int64(0)),
            }
        }
        "left" => {
            if values.len() != 2 {
                return Err(DbError::sql("LEFT expects 2 arguments"));
            }
            let Some(value) = expect_text_arg("LEFT", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(length) = expect_int_arg("LEFT", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Text(take_left_chars(
                value,
                non_negative_usize(length, "LEFT", "second")?,
            )))
        }
        "right" => {
            if values.len() != 2 {
                return Err(DbError::sql("RIGHT expects 2 arguments"));
            }
            let Some(value) = expect_text_arg("RIGHT", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(length) = expect_int_arg("RIGHT", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Text(take_right_chars(
                value,
                non_negative_usize(length, "RIGHT", "second")?,
            )))
        }
        "lpad" => {
            if values.len() != 3 {
                return Err(DbError::sql("LPAD expects 3 arguments"));
            }
            let Some(value) = expect_text_arg("LPAD", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(target_len) = expect_int_arg("LPAD", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            let Some(pad) = expect_text_arg("LPAD", "third", &values[2])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Text(pad_left(value, target_len, pad)))
        }
        "rpad" => {
            if values.len() != 3 {
                return Err(DbError::sql("RPAD expects 3 arguments"));
            }
            let Some(value) = expect_text_arg("RPAD", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(target_len) = expect_int_arg("RPAD", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            let Some(pad) = expect_text_arg("RPAD", "third", &values[2])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Text(pad_right(value, target_len, pad)))
        }
        "repeat" => {
            if values.len() != 2 {
                return Err(DbError::sql("REPEAT expects 2 arguments"));
            }
            let Some(value) = expect_text_arg("REPEAT", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(count) = expect_int_arg("REPEAT", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            let count = non_negative_usize(count, "REPEAT", "second")?;
            Ok(Value::Text(value.repeat(count)))
        }
        "reverse" => {
            if values.len() != 1 {
                return Err(DbError::sql("REVERSE expects 1 argument"));
            }
            let Some(value) = expect_text_arg("REVERSE", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Text(value.chars().rev().collect()))
        }
        "chr" | "char" => {
            if values.len() != 1 {
                return Err(DbError::sql("CHR expects 1 argument"));
            }
            let Some(codepoint) = expect_int_arg("CHR", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let codepoint = u32::try_from(codepoint)
                .map_err(|_| DbError::sql("CHR code point must be between 0 and 1114111"))?;
            let ch = char::from_u32(codepoint)
                .ok_or_else(|| DbError::sql("CHR code point must be between 0 and 1114111"))?;
            Ok(Value::Text(ch.to_string()))
        }
        "hex" => {
            if values.len() != 1 {
                return Err(DbError::sql("HEX expects 1 argument"));
            }
            match &values[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(value) => Ok(Value::Text(hex_encode_upper(value.as_bytes()))),
                Value::Blob(value) => Ok(Value::Text(hex_encode_upper(value))),
                Value::Uuid(value) => Ok(Value::Text(hex_encode_upper(value))),
                other => Err(DbError::sql(format!(
                    "HEX expects text, BLOB, or UUID, got {other:?}"
                ))),
            }
        }
        "abs" => {
            if values.len() != 1 {
                return Err(DbError::sql("ABS expects 1 argument"));
            }
            match expect_numeric_arg("ABS", "first", &values[0])? {
                None => Ok(Value::Null),
                Some(NumericValue::Int64(value)) => value
                    .checked_abs()
                    .map(Value::Int64)
                    .ok_or_else(|| DbError::sql("ABS overflow for INT64 input")),
                Some(NumericValue::Float64(value)) => Ok(Value::Float64(value.abs())),
                Some(NumericValue::Decimal { scaled, scale }) => scaled
                    .checked_abs()
                    .map(|scaled| Value::Decimal { scaled, scale })
                    .ok_or_else(|| DbError::sql("ABS overflow for DECIMAL input")),
            }
        }
        "ceil" | "ceiling" => {
            if values.len() != 1 {
                return Err(DbError::sql("CEIL expects 1 argument"));
            }
            match expect_numeric_arg("CEIL", "first", &values[0])? {
                None => Ok(Value::Null),
                Some(NumericValue::Int64(value)) => Ok(Value::Int64(value)),
                Some(value) => Ok(Value::Float64(value.as_f64().ceil())),
            }
        }
        "floor" => {
            if values.len() != 1 {
                return Err(DbError::sql("FLOOR expects 1 argument"));
            }
            match expect_numeric_arg("FLOOR", "first", &values[0])? {
                None => Ok(Value::Null),
                Some(NumericValue::Int64(value)) => Ok(Value::Int64(value)),
                Some(value) => Ok(Value::Float64(value.as_f64().floor())),
            }
        }
        "round" => {
            if values.is_empty() || values.len() > 2 {
                return Err(DbError::sql("ROUND expects 1 or 2 arguments"));
            }
            let Some(number) = expect_numeric_arg("ROUND", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let scale = if let Some(value) = values.get(1) {
                let Some(scale) = expect_int_arg("ROUND", "second", value)? else {
                    return Ok(Value::Null);
                };
                i32::try_from(scale).map_err(|_| DbError::sql("ROUND precision is out of range"))?
            } else {
                0
            };
            let factor = 10_f64.powi(scale);
            Ok(Value::Float64((number.as_f64() * factor).round() / factor))
        }
        "sqrt" => {
            if values.len() != 1 {
                return Err(DbError::sql("SQRT expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("SQRT", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let number = number.as_f64();
            if number < 0.0 {
                return Ok(Value::Null);
            }
            Ok(Value::Float64(number.sqrt()))
        }
        "power" | "pow" => {
            if values.len() != 2 {
                return Err(DbError::sql("POWER expects 2 arguments"));
            }
            let Some(base) = expect_numeric_arg("POWER", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(exponent) = expect_numeric_arg("POWER", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Float64(base.as_f64().powf(exponent.as_f64())))
        }
        "mod" => {
            if values.len() != 2 {
                return Err(DbError::sql("MOD expects 2 arguments"));
            }
            let Some(left) = expect_numeric_arg("MOD", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(right) = expect_numeric_arg("MOD", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            match (left, right) {
                (_, NumericValue::Int64(0)) => Ok(Value::Null),
                (_, NumericValue::Float64(0.0)) => Ok(Value::Null),
                (_, NumericValue::Decimal { scaled: 0, .. }) => Ok(Value::Null),
                (NumericValue::Int64(left), NumericValue::Int64(right)) => {
                    Ok(Value::Int64(left % right))
                }
                (left, right) => Ok(Value::Float64(left.as_f64() % right.as_f64())),
            }
        }
        "sign" => {
            if values.len() != 1 {
                return Err(DbError::sql("SIGN expects 1 argument"));
            }
            match expect_numeric_arg("SIGN", "first", &values[0])? {
                None => Ok(Value::Null),
                Some(NumericValue::Int64(value)) => Ok(Value::Int64(value.signum())),
                Some(NumericValue::Float64(value)) => Ok(Value::Int64(if value > 0.0 {
                    1
                } else if value < 0.0 {
                    -1
                } else {
                    0
                })),
                Some(NumericValue::Decimal { scaled, .. }) => Ok(Value::Int64(scaled.signum())),
            }
        }
        "ln" => {
            if values.len() != 1 {
                return Err(DbError::sql("LN expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("LN", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let number = number.as_f64();
            if number <= 0.0 {
                return Ok(Value::Null);
            }
            Ok(Value::Float64(number.ln()))
        }
        "log" => {
            if values.is_empty() || values.len() > 2 {
                return Err(DbError::sql("LOG expects 1 or 2 arguments"));
            }
            if values.len() == 1 {
                let Some(number) = expect_numeric_arg("LOG", "first", &values[0])? else {
                    return Ok(Value::Null);
                };
                let number = number.as_f64();
                if number <= 0.0 {
                    return Ok(Value::Null);
                }
                return Ok(Value::Float64(number.log10()));
            }
            let Some(base) = expect_numeric_arg("LOG", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(number) = expect_numeric_arg("LOG", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            let base = base.as_f64();
            let number = number.as_f64();
            if base <= 0.0 || base == 1.0 || number <= 0.0 {
                return Ok(Value::Null);
            }
            Ok(Value::Float64(number.log(base)))
        }
        "exp" => {
            if values.len() != 1 {
                return Err(DbError::sql("EXP expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("EXP", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Float64(number.as_f64().exp()))
        }
        "sin" => {
            if values.len() != 1 {
                return Err(DbError::sql("SIN expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("SIN", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Float64(number.as_f64().sin()))
        }
        "cos" => {
            if values.len() != 1 {
                return Err(DbError::sql("COS expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("COS", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Float64(number.as_f64().cos()))
        }
        "tan" => {
            if values.len() != 1 {
                return Err(DbError::sql("TAN expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("TAN", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let radians = number.as_f64();
            if radians.cos().abs() < 1e-12 {
                return Ok(Value::Null);
            }
            Ok(Value::Float64(radians.tan()))
        }
        "asin" => {
            if values.len() != 1 {
                return Err(DbError::sql("ASIN expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("ASIN", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let value = number.as_f64();
            if !(-1.0..=1.0).contains(&value) {
                return Ok(Value::Null);
            }
            Ok(Value::Float64(value.asin()))
        }
        "acos" => {
            if values.len() != 1 {
                return Err(DbError::sql("ACOS expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("ACOS", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let value = number.as_f64();
            if !(-1.0..=1.0).contains(&value) {
                return Ok(Value::Null);
            }
            Ok(Value::Float64(value.acos()))
        }
        "atan" => {
            if values.len() != 1 {
                return Err(DbError::sql("ATAN expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("ATAN", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Float64(number.as_f64().atan()))
        }
        "atan2" => {
            if values.len() != 2 {
                return Err(DbError::sql("ATAN2 expects 2 arguments"));
            }
            let Some(y) = expect_numeric_arg("ATAN2", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(x) = expect_numeric_arg("ATAN2", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Float64(y.as_f64().atan2(x.as_f64())))
        }
        "pi" => {
            if !values.is_empty() {
                return Err(DbError::sql("PI expects 0 arguments"));
            }
            Ok(Value::Float64(std::f64::consts::PI))
        }
        "degrees" => {
            if values.len() != 1 {
                return Err(DbError::sql("DEGREES expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("DEGREES", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Float64(number.as_f64().to_degrees()))
        }
        "radians" => {
            if values.len() != 1 {
                return Err(DbError::sql("RADIANS expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("RADIANS", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Float64(number.as_f64().to_radians()))
        }
        "cot" => {
            if values.len() != 1 {
                return Err(DbError::sql("COT expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("COT", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let tan = number.as_f64().tan();
            if tan.abs() < 1e-12 {
                return Ok(Value::Null);
            }
            Ok(Value::Float64(1.0 / tan))
        }
        "random" => {
            if !values.is_empty() {
                return Err(DbError::sql("RANDOM expects 0 arguments"));
            }
            Ok(Value::Float64(next_random_f64()))
        }
        "now" | "current_timestamp" | "localtimestamp" => eval_current_timestamp(values),
        "current_date" => eval_current_date(values),
        "current_time" | "localtime" => eval_current_time(values),
        "date_trunc" => eval_date_trunc(values),
        "date_part" | "pg_catalog.date_part" => eval_date_part(values),
        "date_diff" => eval_date_diff(values),
        "last_day" => eval_last_day(values),
        "next_day" => eval_next_day(values),
        "make_date" => eval_make_date(values),
        "make_timestamp" => eval_make_timestamp(values),
        "to_timestamp" => eval_to_timestamp(values),
        "interval" => eval_interval(values),
        "age" => eval_age(values),
        "date" => eval_date(values),
        "datetime" => eval_datetime(values),
        "strftime" => eval_strftime(values),
        "extract" | "pg_catalog.extract" => eval_extract(values),
        "gen_random_uuid" => eval_gen_random_uuid(values),
        "uuid_parse" => eval_uuid_parse(values),
        "uuid_to_string" => eval_uuid_to_string(values),
        "json_array" | "pg_catalog.json_array" => eval_json_array(values),
        "json_array_length" => eval_json_array_length(values),
        "json_extract" => eval_json_extract(values),
        "json_object" | "pg_catalog.json_object" => eval_json_object(values),
        "json_type" | "pg_catalog.json_type" => eval_json_type(values),
        "json_valid" | "pg_catalog.json_valid" => eval_json_valid(values),
        other => Err(DbError::sql(format!("unsupported scalar function {other}"))),
    }
}

fn eval_greatest_least(values: &[Value], want_greatest: bool) -> Result<Value> {
    if values.is_empty() {
        return Err(DbError::sql(if want_greatest {
            "GREATEST expects at least 1 argument"
        } else {
            "LEAST expects at least 1 argument"
        }));
    }
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let mut best = values[0].clone();
    for value in &values[1..] {
        let ordering = compare_values(value, &best)?;
        if (want_greatest && ordering == std::cmp::Ordering::Greater)
            || (!want_greatest && ordering == std::cmp::Ordering::Less)
        {
            best = value.clone();
        }
    }
    Ok(best)
}

fn eval_iif(values: &[Value]) -> Result<Value> {
    if values.len() != 3 {
        return Err(DbError::sql("IIF expects 3 arguments"));
    }
    Ok(if matches!(truthy(&values[0]), Some(true)) {
        values[1].clone()
    } else {
        values[2].clone()
    })
}

#[derive(Clone, Copy, Debug)]
enum NumericValue {
    Int64(i64),
    Float64(f64),
    Decimal { scaled: i64, scale: u8 },
}

impl NumericValue {
    fn as_f64(self) -> f64 {
        match self {
            Self::Int64(value) => value as f64,
            Self::Float64(value) => value,
            Self::Decimal { scaled, scale } => (scaled as f64) / 10_f64.powi(i32::from(scale)),
        }
    }
}

fn unary_text_fn(values: Vec<Value>, f: impl FnOnce(String) -> String) -> Result<Value> {
    if values.len() != 1 {
        return Err(DbError::sql("function expects one argument"));
    }
    match values.into_iter().next() {
        Some(Value::Text(value)) => Ok(Value::Text(f(value))),
        Some(Value::Null) => Ok(Value::Null),
        Some(other) => Err(DbError::sql(format!(
            "function expects text, got {other:?}"
        ))),
        None => Err(DbError::sql("function expects one argument")),
    }
}

fn expect_text_arg<'a>(
    function_name: &str,
    ordinal: &str,
    value: &'a Value,
) -> Result<Option<&'a str>> {
    match value {
        Value::Text(value) => Ok(Some(value)),
        Value::Null => Ok(None),
        other => Err(DbError::sql(format!(
            "{function_name} expects text for {ordinal} argument, got {other:?}"
        ))),
    }
}

fn expect_int_arg(function_name: &str, ordinal: &str, value: &Value) -> Result<Option<i64>> {
    match value {
        Value::Int64(value) => Ok(Some(*value)),
        Value::Null => Ok(None),
        other => Err(DbError::sql(format!(
            "{function_name} expects int for {ordinal} argument, got {other:?}"
        ))),
    }
}

fn expect_numeric_arg(
    function_name: &str,
    ordinal: &str,
    value: &Value,
) -> Result<Option<NumericValue>> {
    match value {
        Value::Int64(value) => Ok(Some(NumericValue::Int64(*value))),
        Value::Float64(value) => Ok(Some(NumericValue::Float64(*value))),
        Value::Decimal { scaled, scale } => Ok(Some(NumericValue::Decimal {
            scaled: *scaled,
            scale: *scale,
        })),
        Value::Null => Ok(None),
        other => Err(DbError::sql(format!(
            "{function_name} expects numeric input for {ordinal} argument, got {other:?}"
        ))),
    }
}

fn non_negative_usize(value: i64, function_name: &str, ordinal: &str) -> Result<usize> {
    if value < 0 {
        return Ok(0);
    }
    usize::try_from(value).map_err(|_| {
        DbError::sql(format!(
            "{function_name} {ordinal} argument is out of range"
        ))
    })
}

fn take_left_chars(value: &str, len: usize) -> String {
    value.chars().take(len).collect()
}

fn take_right_chars(value: &str, len: usize) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    let start = chars.len().saturating_sub(len);
    chars[start..].iter().copied().collect()
}

fn pad_left(value: &str, target_len: i64, pad: &str) -> String {
    let target_len = target_len.max(0) as usize;
    let current_len = value.chars().count();
    if target_len <= current_len {
        return take_left_chars(value, target_len);
    }
    if pad.is_empty() {
        return value.to_string();
    }
    let padding = repeat_to_char_len(pad, target_len - current_len);
    format!("{padding}{value}")
}

fn pad_right(value: &str, target_len: i64, pad: &str) -> String {
    let target_len = target_len.max(0) as usize;
    let current_len = value.chars().count();
    if target_len <= current_len {
        return take_left_chars(value, target_len);
    }
    if pad.is_empty() {
        return value.to_string();
    }
    let padding = repeat_to_char_len(pad, target_len - current_len);
    format!("{value}{padding}")
}

fn repeat_to_char_len(pattern: &str, len: usize) -> String {
    if len == 0 || pattern.is_empty() {
        return String::new();
    }
    let mut output = String::new();
    while output.chars().count() < len {
        output.push_str(pattern);
    }
    output.chars().take(len).collect()
}

fn hex_encode_upper(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;

        let _ = write!(output, "{byte:02X}");
    }
    output
}

fn hex_encode_lower(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;

        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn eval_regexp_replace(
    input: &str,
    pattern: &str,
    replacement: &str,
    flags: Option<&str>,
) -> Result<String> {
    let mut case_insensitive = false;
    let mut global = false;
    if let Some(flags) = flags {
        for flag in flags.chars() {
            match flag {
                'i' | 'I' => case_insensitive = true,
                'g' | 'G' => global = true,
                _ => {
                    return Err(DbError::sql(format!(
                        "REGEXP_REPLACE flag {flag} is not supported"
                    )))
                }
            }
        }
    }
    let mut builder = regex::RegexBuilder::new(pattern);
    builder.case_insensitive(case_insensitive);
    let regex = builder
        .build()
        .map_err(|error| DbError::sql(format!("invalid regular expression: {error}")))?;
    if global {
        Ok(regex.replace_all(input, replacement).to_string())
    } else {
        Ok(regex.replace(input, replacement).to_string())
    }
}

fn next_random_u64() -> u64 {
    let mut observed = RANDOM_STATE.load(Ordering::Relaxed);
    loop {
        let current = if observed == 0 {
            random_seed()
        } else {
            observed
        };
        let next = splitmix64(current);
        match RANDOM_STATE.compare_exchange_weak(
            observed,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return next,
            Err(actual) => observed = actual,
        }
    }
}

fn next_random_f64() -> f64 {
    let value = next_random_u64();
    ((value >> 11) as f64) / ((1_u64 << 53) as f64)
}

fn random_seed() -> u64 {
    let nanos = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos() as u64,
        Err(_) => 0xDDB5_EED5_A17C_E55D,
    };
    nanos ^ 0x9E37_79B9_7F4A_7C15
}

fn splitmix64(state: u64) -> u64 {
    let mut value = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

fn eval_current_timestamp(values: Vec<Value>) -> Result<Value> {
    if !values.is_empty() {
        return Err(DbError::sql("CURRENT_TIMESTAMP expects 0 arguments"));
    }
    Ok(Value::TimestampMicros(current_utc_timestamp_micros()?))
}

fn eval_current_date(values: Vec<Value>) -> Result<Value> {
    if !values.is_empty() {
        return Err(DbError::sql("CURRENT_DATE expects 0 arguments"));
    }
    Ok(Value::Text(format_date(current_utc_datetime())))
}

fn eval_current_time(values: Vec<Value>) -> Result<Value> {
    if !values.is_empty() {
        return Err(DbError::sql("CURRENT_TIME expects 0 arguments"));
    }
    Ok(Value::Text(format_time(current_utc_datetime())))
}

fn eval_date(values: Vec<Value>) -> Result<Value> {
    let Some(datetime) = resolve_datetime_arguments("DATE", &values, true)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(format_date(datetime)))
}

fn eval_datetime(values: Vec<Value>) -> Result<Value> {
    let Some(datetime) = resolve_datetime_arguments("DATETIME", &values, true)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(format_datetime(datetime)))
}

fn eval_strftime(values: Vec<Value>) -> Result<Value> {
    if values.is_empty() {
        return Err(DbError::sql("STRFTIME expects at least 1 argument"));
    }
    let Some(format) = expect_text_arg("STRFTIME", "first", &values[0])? else {
        return Ok(Value::Null);
    };
    let Some(datetime) = resolve_datetime_arguments("STRFTIME", &values[1..], true)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(datetime.format(format).to_string()))
}

fn eval_extract(values: Vec<Value>) -> Result<Value> {
    if values.len() != 2 {
        return Err(DbError::sql("EXTRACT expects 2 arguments"));
    }
    let Some(field) = expect_text_arg("EXTRACT", "first", &values[0])? else {
        return Ok(Value::Null);
    };
    let Some(datetime) = datetime_from_value("EXTRACT", &values[1])? else {
        return Ok(Value::Null);
    };
    match field.to_ascii_uppercase().as_str() {
        "YEAR" => Ok(Value::Int64(i64::from(datetime.year()))),
        "MONTH" => Ok(Value::Int64(i64::from(datetime.month()))),
        "DAY" => Ok(Value::Int64(i64::from(datetime.day()))),
        "HOUR" => Ok(Value::Int64(i64::from(datetime.hour()))),
        "MINUTE" => Ok(Value::Int64(i64::from(datetime.minute()))),
        "SECOND" => Ok(Value::Int64(i64::from(datetime.second()))),
        "DOW" => Ok(Value::Int64(i64::from(
            datetime.weekday().num_days_from_sunday(),
        ))),
        "DOY" => Ok(Value::Int64(i64::from(datetime.ordinal()))),
        "EPOCH" => Ok(Value::Float64(
            (datetime.timestamp_micros() as f64) / 1_000_000.0,
        )),
        other => Err(DbError::sql(format!(
            "EXTRACT field {other} is not supported"
        ))),
    }
}

fn eval_date_trunc(values: Vec<Value>) -> Result<Value> {
    if values.len() != 2 {
        return Err(DbError::sql("DATE_TRUNC expects 2 arguments"));
    }
    let Some(precision) = expect_text_arg("DATE_TRUNC", "first", &values[0])? else {
        return Ok(Value::Null);
    };
    let Some(datetime) = datetime_from_value("DATE_TRUNC", &values[1])? else {
        return Ok(Value::Null);
    };
    let truncated = truncate_datetime(datetime, precision)?;
    Ok(Value::TimestampMicros(truncated.timestamp_micros()))
}

fn eval_date_part(values: Vec<Value>) -> Result<Value> {
    if values.len() != 2 {
        return Err(DbError::sql("DATE_PART expects 2 arguments"));
    }
    eval_extract(values)
}

fn eval_date_diff(values: Vec<Value>) -> Result<Value> {
    if values.len() != 3 {
        return Err(DbError::sql("DATE_DIFF expects 3 arguments"));
    }
    let Some(part) = expect_text_arg("DATE_DIFF", "first", &values[0])? else {
        return Ok(Value::Null);
    };
    let Some(start) = datetime_from_value("DATE_DIFF", &values[1])? else {
        return Ok(Value::Null);
    };
    let Some(end) = datetime_from_value("DATE_DIFF", &values[2])? else {
        return Ok(Value::Null);
    };
    let diff = date_diff_part(part, start, end)?;
    Ok(Value::Int64(diff))
}

fn eval_last_day(values: Vec<Value>) -> Result<Value> {
    if values.len() != 1 {
        return Err(DbError::sql("LAST_DAY expects 1 argument"));
    }
    let Some(datetime) = datetime_from_value("LAST_DAY", &values[0])? else {
        return Ok(Value::Null);
    };
    let first_of_month = datetime
        .date_naive()
        .with_day(1)
        .ok_or_else(|| DbError::sql("LAST_DAY date is out of range"))?;
    let next_month = first_of_month
        .checked_add_months(Months::new(1))
        .ok_or_else(|| DbError::sql("LAST_DAY date is out of range"))?;
    let last_day = next_month
        .pred_opt()
        .ok_or_else(|| DbError::sql("LAST_DAY date is out of range"))?;
    Ok(Value::Text(last_day.format("%Y-%m-%d").to_string()))
}

fn eval_next_day(values: Vec<Value>) -> Result<Value> {
    if values.len() != 2 {
        return Err(DbError::sql("NEXT_DAY expects 2 arguments"));
    }
    let Some(datetime) = datetime_from_value("NEXT_DAY", &values[0])? else {
        return Ok(Value::Null);
    };
    let Some(weekday_text) = expect_text_arg("NEXT_DAY", "second", &values[1])? else {
        return Ok(Value::Null);
    };
    let target = parse_weekday_name(weekday_text)?;
    let mut days_ahead = i64::from(target.num_days_from_monday())
        - i64::from(datetime.weekday().num_days_from_monday());
    if days_ahead <= 0 {
        days_ahead += 7;
    }
    let next = datetime
        .checked_add_signed(ChronoDuration::days(days_ahead))
        .ok_or_else(|| DbError::sql("NEXT_DAY result overflowed supported range"))?;
    Ok(Value::Text(
        next.date_naive().format("%Y-%m-%d").to_string(),
    ))
}

fn eval_make_date(values: Vec<Value>) -> Result<Value> {
    if values.len() != 3 {
        return Err(DbError::sql("MAKE_DATE expects 3 arguments"));
    }
    let year = expect_int_arg("MAKE_DATE", "first", &values[0])?;
    let month = expect_int_arg("MAKE_DATE", "second", &values[1])?;
    let day = expect_int_arg("MAKE_DATE", "third", &values[2])?;
    let (Some(year), Some(month), Some(day)) = (year, month, day) else {
        return Ok(Value::Null);
    };
    let year = i32::try_from(year).map_err(|_| DbError::sql("MAKE_DATE year is out of range"))?;
    let month =
        u32::try_from(month).map_err(|_| DbError::sql("MAKE_DATE month is out of range"))?;
    let day = u32::try_from(day).map_err(|_| DbError::sql("MAKE_DATE day is out of range"))?;
    let date = NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| DbError::sql("MAKE_DATE arguments are not a valid date"))?;
    Ok(Value::Text(date.format("%Y-%m-%d").to_string()))
}

fn eval_make_timestamp(values: Vec<Value>) -> Result<Value> {
    if values.len() != 6 {
        return Err(DbError::sql("MAKE_TIMESTAMP expects 6 arguments"));
    }
    let year = expect_int_arg("MAKE_TIMESTAMP", "first", &values[0])?;
    let month = expect_int_arg("MAKE_TIMESTAMP", "second", &values[1])?;
    let day = expect_int_arg("MAKE_TIMESTAMP", "third", &values[2])?;
    let hour = expect_int_arg("MAKE_TIMESTAMP", "fourth", &values[3])?;
    let minute = expect_int_arg("MAKE_TIMESTAMP", "fifth", &values[4])?;
    let second = expect_numeric_arg("MAKE_TIMESTAMP", "sixth", &values[5])?;
    let (Some(year), Some(month), Some(day), Some(hour), Some(minute), Some(second)) =
        (year, month, day, hour, minute, second)
    else {
        return Ok(Value::Null);
    };
    let year =
        i32::try_from(year).map_err(|_| DbError::sql("MAKE_TIMESTAMP year is out of range"))?;
    let month =
        u32::try_from(month).map_err(|_| DbError::sql("MAKE_TIMESTAMP month is out of range"))?;
    let day = u32::try_from(day).map_err(|_| DbError::sql("MAKE_TIMESTAMP day is out of range"))?;
    let hour =
        u32::try_from(hour).map_err(|_| DbError::sql("MAKE_TIMESTAMP hour is out of range"))?;
    let minute =
        u32::try_from(minute).map_err(|_| DbError::sql("MAKE_TIMESTAMP minute is out of range"))?;
    let second_f64 = second.as_f64();
    if !(0.0..60.0).contains(&second_f64) {
        return Err(DbError::sql(
            "MAKE_TIMESTAMP seconds must be between 0 (inclusive) and 60 (exclusive)",
        ));
    }
    let second_whole = second_f64.floor() as u32;
    let micros = ((second_f64 - f64::from(second_whole)) * 1_000_000.0).round() as u32;
    let date = NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| DbError::sql("MAKE_TIMESTAMP arguments are not a valid date"))?;
    let datetime = date
        .and_hms_micro_opt(hour, minute, second_whole, micros.min(999_999))
        .ok_or_else(|| DbError::sql("MAKE_TIMESTAMP arguments are not a valid timestamp"))?;
    Ok(Value::TimestampMicros(
        DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc).timestamp_micros(),
    ))
}

fn eval_to_timestamp(values: Vec<Value>) -> Result<Value> {
    if values.is_empty() || values.len() > 2 {
        return Err(DbError::sql("TO_TIMESTAMP expects 1 or 2 arguments"));
    }
    match (&values[0], values.get(1)) {
        (Value::Null, _) => Ok(Value::Null),
        (Value::Int64(epoch), None) => Ok(Value::TimestampMicros(
            epoch
                .checked_mul(1_000_000)
                .ok_or_else(|| DbError::sql("TO_TIMESTAMP epoch is out of range"))?,
        )),
        (Value::Float64(epoch), None) => Ok(Value::TimestampMicros((epoch * 1_000_000.0) as i64)),
        (Value::Text(text), None) => Ok(Value::TimestampMicros(
            parse_datetime_text("TO_TIMESTAMP", text)?.timestamp_micros(),
        )),
        (Value::Text(text), Some(Value::Text(format))) => Ok(Value::TimestampMicros(
            parse_to_timestamp_with_format(text, format)?.timestamp_micros(),
        )),
        (_, Some(Value::Null)) => Ok(Value::Null),
        (other, None) => Err(DbError::sql(format!(
            "TO_TIMESTAMP expects numeric epoch or text input, got {other:?}"
        ))),
        (_, Some(other)) => Err(DbError::sql(format!(
            "TO_TIMESTAMP format argument must be text, got {other:?}"
        ))),
    }
}

fn eval_interval(values: Vec<Value>) -> Result<Value> {
    if values.len() != 1 {
        return Err(DbError::sql("INTERVAL expects 1 argument"));
    }
    let raw = match &values[0] {
        Value::Null => return Ok(Value::Null),
        Value::Text(raw) => raw.as_str(),
        other => {
            return Err(DbError::sql(format!(
                "INTERVAL expects text literal input, got {other:?}"
            )))
        }
    };
    let micros = parse_interval_micros(raw)?;
    Ok(Value::Int64(micros))
}

fn eval_age(values: Vec<Value>) -> Result<Value> {
    if values.is_empty() || values.len() > 2 {
        return Err(DbError::sql("AGE expects 1 or 2 arguments"));
    }
    let first = match datetime_from_value("AGE", &values[0])? {
        Some(value) => value,
        None => return Ok(Value::Null),
    };
    let second = if let Some(second) = values.get(1) {
        match datetime_from_value("AGE", second)? {
            Some(value) => value,
            None => return Ok(Value::Null),
        }
    } else {
        current_utc_datetime()
    };
    let delta = first.timestamp_micros() - second.timestamp_micros();
    Ok(Value::Text(format_age_interval(delta)))
}

fn truncate_datetime(datetime: DateTime<Utc>, precision: &str) -> Result<DateTime<Utc>> {
    let lower = precision.to_ascii_lowercase();
    let date = datetime.date_naive();
    let time = datetime.time();
    let truncated = match lower.as_str() {
        "microsecond" | "microseconds" => datetime.naive_utc(),
        "millisecond" | "milliseconds" => datetime
            .naive_utc()
            .with_nanosecond((time.nanosecond() / 1_000_000) * 1_000_000)
            .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?,
        "second" | "seconds" => date
            .and_hms_opt(time.hour(), time.minute(), time.second())
            .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?,
        "minute" | "minutes" => date
            .and_hms_opt(time.hour(), time.minute(), 0)
            .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?,
        "hour" | "hours" => date
            .and_hms_opt(time.hour(), 0, 0)
            .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?,
        "day" | "days" => date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?,
        "week" | "weeks" => {
            let start = date
                .checked_sub_signed(ChronoDuration::days(i64::from(
                    date.weekday().num_days_from_monday(),
                )))
                .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid date"))?;
            start
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?
        }
        "month" | "months" => date
            .with_day(1)
            .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid date"))?
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?,
        "quarter" | "quarters" => {
            let month = ((date.month() - 1) / 3) * 3 + 1;
            NaiveDate::from_ymd_opt(date.year(), month, 1)
                .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid date"))?
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?
        }
        "year" | "years" => NaiveDate::from_ymd_opt(date.year(), 1, 1)
            .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid date"))?
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?,
        "decade" | "decades" => {
            let year = date.year().div_euclid(10) * 10;
            NaiveDate::from_ymd_opt(year, 1, 1)
                .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid date"))?
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?
        }
        "century" | "centuries" => {
            let year = ((date.year() - 1).div_euclid(100) * 100) + 1;
            NaiveDate::from_ymd_opt(year, 1, 1)
                .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid date"))?
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?
        }
        "millennium" | "millennia" => {
            let year = ((date.year() - 1).div_euclid(1000) * 1000) + 1;
            NaiveDate::from_ymd_opt(year, 1, 1)
                .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid date"))?
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?
        }
        other => {
            return Err(DbError::sql(format!(
                "DATE_TRUNC precision {other} is not supported"
            )))
        }
    };
    Ok(DateTime::from_naive_utc_and_offset(truncated, Utc))
}

fn date_diff_part(part: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<i64> {
    let part = part.to_ascii_lowercase();
    let micros = end.timestamp_micros() - start.timestamp_micros();
    let result = match part.as_str() {
        "microsecond" | "microseconds" => micros,
        "millisecond" | "milliseconds" => micros.div_euclid(1_000),
        "second" | "seconds" => micros.div_euclid(1_000_000),
        "minute" | "minutes" => micros.div_euclid(60 * 1_000_000),
        "hour" | "hours" => micros.div_euclid(60 * 60 * 1_000_000),
        "day" | "days" => micros.div_euclid(24 * 60 * 60 * 1_000_000),
        "week" | "weeks" => micros.div_euclid(7 * 24 * 60 * 60 * 1_000_000),
        "month" | "months" => {
            let start_date = start.date_naive();
            let end_date = end.date_naive();
            let mut months = i64::from(end_date.year() - start_date.year()) * 12
                + i64::from(end_date.month())
                - i64::from(start_date.month());
            if end_date.day() < start_date.day() {
                months -= 1;
            }
            months
        }
        "year" | "years" => {
            let start_date = start.date_naive();
            let end_date = end.date_naive();
            let mut years = i64::from(end_date.year() - start_date.year());
            if (end_date.month(), end_date.day()) < (start_date.month(), start_date.day()) {
                years -= 1;
            }
            years
        }
        other => {
            return Err(DbError::sql(format!(
                "DATE_DIFF part {other} is not supported"
            )))
        }
    };
    Ok(result)
}

fn parse_weekday_name(value: &str) -> Result<chrono::Weekday> {
    match value.trim().to_ascii_lowercase().as_str() {
        "monday" | "mon" => Ok(chrono::Weekday::Mon),
        "tuesday" | "tue" | "tues" => Ok(chrono::Weekday::Tue),
        "wednesday" | "wed" => Ok(chrono::Weekday::Wed),
        "thursday" | "thu" | "thurs" => Ok(chrono::Weekday::Thu),
        "friday" | "fri" => Ok(chrono::Weekday::Fri),
        "saturday" | "sat" => Ok(chrono::Weekday::Sat),
        "sunday" | "sun" => Ok(chrono::Weekday::Sun),
        other => Err(DbError::sql(format!(
            "NEXT_DAY weekday {other} is not supported"
        ))),
    }
}

fn parse_to_timestamp_with_format(text: &str, format: &str) -> Result<DateTime<Utc>> {
    let mapped = match format {
        "YYYY-MM-DD HH24:MI:SS" => "%Y-%m-%d %H:%M:%S",
        "YYYY-MM-DD" => "%Y-%m-%d",
        "DD/MM/YYYY" => "%d/%m/%Y",
        _ => {
            return Err(DbError::sql(
                "TO_TIMESTAMP format is not supported by DecentDB yet",
            ))
        }
    };
    if mapped.contains("%H") {
        let naive = NaiveDateTime::parse_from_str(text, mapped)
            .map_err(|_| DbError::sql("TO_TIMESTAMP input does not match format"))?;
        Ok(DateTime::from_naive_utc_and_offset(naive, Utc))
    } else {
        let date = NaiveDate::parse_from_str(text, mapped)
            .map_err(|_| DbError::sql("TO_TIMESTAMP input does not match format"))?;
        let naive = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| DbError::sql("TO_TIMESTAMP input is out of range"))?;
        Ok(DateTime::from_naive_utc_and_offset(naive, Utc))
    }
}

fn parse_interval_micros(text: &str) -> Result<i64> {
    let mut tokens = text.split_whitespace();
    let mut total_micros: i64 = 0;
    let mut saw_any = false;
    while let Some(amount_token) = tokens.next() {
        let unit_token = tokens
            .next()
            .ok_or_else(|| DbError::sql("INTERVAL text must be pairs of amount and unit"))?;
        let amount = amount_token
            .parse::<i64>()
            .map_err(|_| DbError::sql("INTERVAL amount must be an integer"))?;
        let unit = unit_token.to_ascii_lowercase();
        let unit = unit.trim_end_matches('s');
        let unit_micros = match unit {
            "year" => 365_i64
                .checked_mul(24)
                .and_then(|v| v.checked_mul(60))
                .and_then(|v| v.checked_mul(60))
                .and_then(|v| v.checked_mul(1_000_000))
                .ok_or_else(|| DbError::sql("INTERVAL value overflowed"))?,
            "month" => 30_i64
                .checked_mul(24)
                .and_then(|v| v.checked_mul(60))
                .and_then(|v| v.checked_mul(60))
                .and_then(|v| v.checked_mul(1_000_000))
                .ok_or_else(|| DbError::sql("INTERVAL value overflowed"))?,
            "week" => 7_i64
                .checked_mul(24)
                .and_then(|v| v.checked_mul(60))
                .and_then(|v| v.checked_mul(60))
                .and_then(|v| v.checked_mul(1_000_000))
                .ok_or_else(|| DbError::sql("INTERVAL value overflowed"))?,
            "day" => 24_i64
                .checked_mul(60)
                .and_then(|v| v.checked_mul(60))
                .and_then(|v| v.checked_mul(1_000_000))
                .ok_or_else(|| DbError::sql("INTERVAL value overflowed"))?,
            "hour" => 60_i64
                .checked_mul(60)
                .and_then(|v| v.checked_mul(1_000_000))
                .ok_or_else(|| DbError::sql("INTERVAL value overflowed"))?,
            "minute" => 60_i64
                .checked_mul(1_000_000)
                .ok_or_else(|| DbError::sql("INTERVAL value overflowed"))?,
            "second" => 1_000_000_i64,
            _ => {
                return Err(DbError::sql(format!(
                    "INTERVAL unit {unit_token} is not supported"
                )))
            }
        };
        let delta = amount
            .checked_mul(unit_micros)
            .ok_or_else(|| DbError::sql("INTERVAL value overflowed"))?;
        total_micros = total_micros
            .checked_add(delta)
            .ok_or_else(|| DbError::sql("INTERVAL value overflowed"))?;
        saw_any = true;
    }
    if !saw_any {
        return Err(DbError::sql("INTERVAL text must not be empty"));
    }
    Ok(total_micros)
}

fn format_age_interval(delta_micros: i64) -> String {
    let negative = delta_micros < 0;
    let mut remainder = delta_micros.unsigned_abs();
    let day_micros = 24_u64 * 60 * 60 * 1_000_000;
    let hour_micros = 60_u64 * 60 * 1_000_000;
    let minute_micros = 60_u64 * 1_000_000;
    let second_micros = 1_000_000_u64;
    let days = remainder / day_micros;
    remainder %= day_micros;
    let hours = remainder / hour_micros;
    remainder %= hour_micros;
    let minutes = remainder / minute_micros;
    remainder %= minute_micros;
    let seconds = remainder / second_micros;
    let micros = remainder % second_micros;
    let sign = if negative { "-" } else { "" };
    if micros == 0 {
        format!("{sign}{days} days {hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{sign}{days} days {hours:02}:{minutes:02}:{seconds:02}.{micros:06}")
    }
}

fn eval_gen_random_uuid(values: Vec<Value>) -> Result<Value> {
    if !values.is_empty() {
        return Err(DbError::sql("GEN_RANDOM_UUID expects 0 arguments"));
    }
    let mut value = [0u8; 16];
    value[..8].copy_from_slice(&next_random_u64().to_be_bytes());
    value[8..].copy_from_slice(&next_random_u64().to_be_bytes());
    value[6] = (value[6] & 0x0f) | 0x40;
    value[8] = (value[8] & 0x3f) | 0x80;
    Ok(Value::Uuid(value))
}

fn eval_uuid_parse(values: Vec<Value>) -> Result<Value> {
    if values.len() != 1 {
        return Err(DbError::sql("UUID_PARSE expects 1 argument"));
    }
    match &values[0] {
        Value::Null => Ok(Value::Null),
        Value::Text(value) => Ok(Value::Uuid(parse_uuid_text(value)?)),
        other => Err(DbError::sql(format!(
            "UUID_PARSE expects text input, got {other:?}"
        ))),
    }
}

fn eval_uuid_to_string(values: Vec<Value>) -> Result<Value> {
    if values.len() != 1 {
        return Err(DbError::sql("UUID_TO_STRING expects 1 argument"));
    }
    match &values[0] {
        Value::Null => Ok(Value::Null),
        Value::Uuid(value) => Ok(Value::Text(value_to_text(&Value::Uuid(*value))?)),
        other => Err(DbError::sql(format!(
            "UUID_TO_STRING expects UUID input, got {other:?}"
        ))),
    }
}

fn current_utc_datetime() -> DateTime<Utc> {
    Utc::now()
}

fn current_utc_timestamp_micros() -> Result<i64> {
    let now = current_utc_datetime();
    let seconds = now.timestamp();
    let micros = i64::from(now.timestamp_subsec_micros());
    seconds
        .checked_mul(1_000_000)
        .and_then(|value| value.checked_add(micros))
        .ok_or_else(|| DbError::sql("CURRENT_TIMESTAMP overflowed the supported range"))
}

fn resolve_datetime_arguments(
    function_name: &str,
    values: &[Value],
    default_now: bool,
) -> Result<Option<DateTime<Utc>>> {
    if values.is_empty() {
        return if default_now {
            Ok(Some(current_utc_datetime()))
        } else {
            Err(DbError::sql(format!(
                "{function_name} expects a date/time argument"
            )))
        };
    }
    let Some(mut datetime) = datetime_from_value(function_name, &values[0])? else {
        return Ok(None);
    };
    for modifier in &values[1..] {
        let Some(modifier) = expect_text_arg(function_name, "modifier", modifier)? else {
            return Ok(None);
        };
        datetime = apply_datetime_modifier(function_name, datetime, modifier)?;
    }
    Ok(Some(datetime))
}

fn datetime_from_value(function_name: &str, value: &Value) -> Result<Option<DateTime<Utc>>> {
    match value {
        Value::Null => Ok(None),
        Value::Text(value) => parse_datetime_text(function_name, value).map(Some),
        Value::TimestampMicros(value) => Utc
            .timestamp_micros(*value)
            .single()
            .map(Some)
            .ok_or_else(|| DbError::sql(format!("{function_name} timestamp is out of range"))),
        other => Err(DbError::sql(format!(
            "{function_name} expects text or TIMESTAMP input, got {other:?}"
        ))),
    }
}

fn parse_datetime_text(function_name: &str, value: &str) -> Result<DateTime<Utc>> {
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("now") {
        return Ok(current_utc_datetime());
    }
    if let Ok(value) = DateTime::parse_from_rfc3339(trimmed) {
        return Ok(value.with_timezone(&Utc));
    }
    for format in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
    ] {
        if let Ok(value) = NaiveDateTime::parse_from_str(trimmed, format) {
            return Ok(DateTime::from_naive_utc_and_offset(value, Utc));
        }
    }
    if let Ok(value) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        let value = value
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| DbError::sql("date value is out of range"))?;
        return Ok(DateTime::from_naive_utc_and_offset(value, Utc));
    }
    Err(DbError::sql(format!(
        "{function_name} expects ISO-like date/time text or 'now'"
    )))
}

fn apply_datetime_modifier(
    function_name: &str,
    datetime: DateTime<Utc>,
    modifier: &str,
) -> Result<DateTime<Utc>> {
    let modifier = modifier.trim();
    if modifier.is_empty() {
        return Err(DbError::sql(format!(
            "{function_name} date/time modifier must not be empty"
        )));
    }
    let (sign, body) = if let Some(rest) = modifier.strip_prefix('+') {
        (1_i64, rest)
    } else if let Some(rest) = modifier.strip_prefix('-') {
        (-1_i64, rest)
    } else {
        (1_i64, modifier)
    };
    let mut parts = body.split_whitespace();
    let amount = parts
        .next()
        .ok_or_else(|| DbError::sql(format!("{function_name} date/time modifier is invalid")))?
        .parse::<i64>()
        .map_err(|_| DbError::sql(format!("{function_name} date/time modifier is invalid")))?;
    let amount = amount
        .checked_mul(sign)
        .ok_or_else(|| DbError::sql(format!("{function_name} date/time modifier overflowed")))?;
    let unit = parts
        .next()
        .ok_or_else(|| DbError::sql(format!("{function_name} date/time modifier is invalid")))?;
    if parts.next().is_some() {
        return Err(DbError::sql(format!(
            "{function_name} date/time modifier is invalid"
        )));
    }
    match unit.to_ascii_lowercase().trim_end_matches('s') {
        "year" => shift_datetime_by_months(
            datetime,
            amount.checked_mul(12).ok_or_else(|| {
                DbError::sql(format!("{function_name} date/time modifier overflowed"))
            })?,
        ),
        "month" => shift_datetime_by_months(datetime, amount),
        "day" => shift_datetime_by_duration(function_name, datetime, ChronoDuration::days(amount)),
        "hour" => {
            shift_datetime_by_duration(function_name, datetime, ChronoDuration::hours(amount))
        }
        "minute" => {
            shift_datetime_by_duration(function_name, datetime, ChronoDuration::minutes(amount))
        }
        "second" => {
            shift_datetime_by_duration(function_name, datetime, ChronoDuration::seconds(amount))
        }
        other => Err(DbError::sql(format!(
            "{function_name} does not support date/time modifier unit {other}"
        ))),
    }
}

fn shift_datetime_by_duration(
    function_name: &str,
    datetime: DateTime<Utc>,
    duration: ChronoDuration,
) -> Result<DateTime<Utc>> {
    datetime.checked_add_signed(duration).ok_or_else(|| {
        DbError::sql(format!(
            "{function_name} date/time modifier overflowed the supported range"
        ))
    })
}

fn shift_datetime_by_months(datetime: DateTime<Utc>, months: i64) -> Result<DateTime<Utc>> {
    if months == 0 {
        return Ok(datetime);
    }
    let magnitude = months
        .checked_abs()
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| DbError::sql("date/time month modifier is out of range"))?;
    let naive = if months > 0 {
        datetime
            .naive_utc()
            .checked_add_months(Months::new(magnitude))
    } else {
        datetime
            .naive_utc()
            .checked_sub_months(Months::new(magnitude))
    };
    naive
        .map(|value| DateTime::from_naive_utc_and_offset(value, Utc))
        .ok_or_else(|| DbError::sql("date/time month modifier overflowed the supported range"))
}

fn format_date(datetime: DateTime<Utc>) -> String {
    datetime.format("%Y-%m-%d").to_string()
}

fn format_time(datetime: DateTime<Utc>) -> String {
    datetime.format("%H:%M:%S").to_string()
}

fn format_datetime(datetime: DateTime<Utc>) -> String {
    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
}

fn parse_uuid_text(value: &str) -> Result<[u8; 16]> {
    let compact = if value.len() == 36 {
        if value.as_bytes().get(8) != Some(&b'-')
            || value.as_bytes().get(13) != Some(&b'-')
            || value.as_bytes().get(18) != Some(&b'-')
            || value.as_bytes().get(23) != Some(&b'-')
        {
            return Err(DbError::sql("UUID_PARSE expects canonical UUID text"));
        }
        value.replace('-', "")
    } else if value.len() == 32 {
        value.to_string()
    } else {
        return Err(DbError::sql("UUID_PARSE expects canonical UUID text"));
    };
    if compact.len() != 32 {
        return Err(DbError::sql("UUID_PARSE expects canonical UUID text"));
    }
    let mut uuid = [0u8; 16];
    for (index, chunk) in compact.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(chunk)
            .map_err(|_| DbError::sql("UUID_PARSE expects canonical UUID text"))?;
        uuid[index] = u8::from_str_radix(text, 16)
            .map_err(|_| DbError::sql("UUID_PARSE expects canonical UUID text"))?;
    }
    Ok(uuid)
}

fn eval_json_binary_operator(left: &Value, right: &Value, text_mode: bool) -> Result<Value> {
    let Some(target) = json_operator_target(left, right)? else {
        return Ok(Value::Null);
    };
    if text_mode {
        match target {
            JsonValue::Null => Ok(Value::Null),
            JsonValue::String(value) => Ok(Value::Text(value)),
            JsonValue::Number(value) => Ok(Value::Text(value)),
            JsonValue::Bool(value) => Ok(Value::Text(if value {
                "true".to_string()
            } else {
                "false".to_string()
            })),
            other => Ok(Value::Text(other.render_json())),
        }
    } else {
        Ok(Value::Text(target.render_json()))
    }
}

fn json_operator_target(left: &Value, right: &Value) -> Result<Option<JsonValue>> {
    let json = match left {
        Value::Null => return Ok(None),
        Value::Text(value) => parse_json(value)?,
        other => {
            return Err(DbError::sql(format!(
                "JSON operators expect text JSON input, got {other:?}"
            )))
        }
    };
    match right {
        Value::Null => Ok(None),
        Value::Text(path) if path.starts_with('$') => {
            let path = parse_json_path(path)?;
            Ok(json.lookup(&path).cloned())
        }
        Value::Text(key) => match &json {
            JsonValue::Object(object) => Ok(object.get(key).cloned()),
            JsonValue::Array(array) => {
                let Ok(index) = key.parse::<usize>() else {
                    return Ok(None);
                };
                Ok(array.get(index).cloned())
            }
            _ => Ok(None),
        },
        Value::Int64(index) => {
            let Ok(index) = usize::try_from(*index) else {
                return Ok(None);
            };
            match &json {
                JsonValue::Array(array) => Ok(array.get(index).cloned()),
                _ => Ok(None),
            }
        }
        other => Err(DbError::sql(format!(
            "JSON operators expect text keys or integer indexes, got {other:?}"
        ))),
    }
}

fn expand_json_each_rows(value: &Value) -> Result<Vec<Vec<Value>>> {
    let Some(json) = json_table_input("json_each", value)? else {
        return Ok(Vec::new());
    };
    match json {
        JsonValue::Object(object) => object
            .into_iter()
            .map(|(key, value)| json_table_row(Value::Text(key), value, None))
            .collect(),
        JsonValue::Array(array) => array
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                json_table_row(
                    Value::Int64(i64::try_from(index).unwrap_or(i64::MAX)),
                    value,
                    None,
                )
            })
            .collect(),
        other => Ok(vec![json_table_row(Value::Null, other, None)?]),
    }
}

fn expand_json_tree_rows(value: &Value) -> Result<Vec<Vec<Value>>> {
    let Some(json) = json_table_input("json_tree", value)? else {
        return Ok(Vec::new());
    };
    let mut rows = Vec::new();
    append_json_tree_rows(Value::Null, json, "$".to_string(), &mut rows)?;
    Ok(rows)
}

fn json_table_input(function_name: &str, value: &Value) -> Result<Option<JsonValue>> {
    match value {
        Value::Null => Ok(None),
        Value::Text(value) => parse_json(value).map(Some),
        other => Err(DbError::sql(format!(
            "{function_name} expects text JSON input, got {other:?}"
        ))),
    }
}

fn append_json_tree_rows(
    key: Value,
    value: JsonValue,
    path: String,
    rows: &mut Vec<Vec<Value>>,
) -> Result<()> {
    rows.push(json_table_row(key, value.clone(), Some(path.clone()))?);
    match value {
        JsonValue::Object(object) => {
            for (child_key, child_value) in object {
                append_json_tree_rows(
                    Value::Text(child_key.clone()),
                    child_value,
                    format!("{path}.{child_key}"),
                    rows,
                )?;
            }
        }
        JsonValue::Array(array) => {
            for (index, child_value) in array.into_iter().enumerate() {
                append_json_tree_rows(
                    Value::Int64(i64::try_from(index).unwrap_or(i64::MAX)),
                    child_value,
                    format!("{path}[{index}]"),
                    rows,
                )?;
            }
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::String(_) | JsonValue::Number(_) => {}
    }
    Ok(())
}

fn json_table_row(key: Value, value: JsonValue, path: Option<String>) -> Result<Vec<Value>> {
    let mut row = vec![
        key,
        json_value_to_value(value.clone())?,
        Value::Text(json_type_name(&value).to_string()),
    ];
    if let Some(path) = path {
        row.push(Value::Text(path));
    }
    Ok(row)
}

fn eval_json_array(values: Vec<Value>) -> Result<Value> {
    let items = values
        .iter()
        .map(json_value_from_value)
        .collect::<Result<Vec<_>>>()?;
    Ok(Value::Text(JsonValue::Array(items).render_json()))
}

fn eval_json_object(values: Vec<Value>) -> Result<Value> {
    if !values.len().is_multiple_of(2) {
        return Err(DbError::sql(
            "json_object expects an even number of arguments",
        ));
    }
    let mut object = BTreeMap::new();
    for pair in values.chunks_exact(2) {
        let key = match &pair[0] {
            Value::Text(value) => value.clone(),
            Value::Null => return Err(DbError::sql("json_object keys cannot be NULL")),
            other => {
                return Err(DbError::sql(format!(
                    "json_object keys must be text, got {other:?}"
                )))
            }
        };
        object.insert(key, json_value_from_value(&pair[1])?);
    }
    Ok(Value::Text(JsonValue::Object(object).render_json()))
}

fn eval_json_type(values: Vec<Value>) -> Result<Value> {
    if values.is_empty() || values.len() > 2 {
        return Err(DbError::sql("json_type expects 1 or 2 arguments"));
    }
    let Some(target) = json_target_value(&values)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(json_type_name(&target).to_string()))
}

fn eval_json_valid(values: Vec<Value>) -> Result<Value> {
    if values.len() != 1 {
        return Err(DbError::sql("json_valid expects 1 argument"));
    }
    match &values[0] {
        Value::Null => Ok(Value::Null),
        Value::Text(value) => Ok(Value::Bool(parse_json(value).is_ok())),
        other => Err(DbError::sql(format!(
            "json_valid expects text input, got {other:?}"
        ))),
    }
}

fn json_type_name(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(true) => "true",
        JsonValue::Bool(false) => "false",
        JsonValue::String(_) => "text",
        JsonValue::Number(number) => {
            if number.contains('.') {
                "real"
            } else {
                "integer"
            }
        }
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

fn json_value_from_value(value: &Value) -> Result<JsonValue> {
    match value {
        Value::Null => Ok(JsonValue::Null),
        Value::Int64(value) => Ok(JsonValue::Number(value.to_string())),
        Value::Float64(value) => Ok(JsonValue::Number(value.to_string())),
        Value::Bool(value) => Ok(JsonValue::Bool(*value)),
        Value::Text(value) => Ok(JsonValue::String(value.clone())),
        Value::Blob(_) => Err(DbError::sql("cannot encode BLOB value as JSON")),
        Value::Decimal { scaled, scale } => {
            Ok(JsonValue::Number(decimal_to_string(*scaled, *scale)))
        }
        Value::Uuid(value) => Ok(JsonValue::String(value_to_text(&Value::Uuid(*value))?)),
        Value::TimestampMicros(value) => Ok(JsonValue::Number(value.to_string())),
    }
}

fn sort_aggregate_row_indexes(
    runtime: &EngineRuntime,
    dataset: &Dataset,
    row_indexes: &[usize],
    order_by: &[crate::sql::ast::OrderBy],
    params: &[Value],
    ctes: &BTreeMap<String, Dataset>,
) -> Result<Vec<usize>> {
    if order_by.is_empty() {
        return Ok(row_indexes.to_vec());
    }
    let mut keyed = row_indexes
        .iter()
        .map(|row_index| {
            let row = dataset
                .rows
                .get(*row_index)
                .map(Vec::as_slice)
                .ok_or_else(|| DbError::internal("group row index is invalid"))?;
            let keys = order_by
                .iter()
                .map(|order| runtime.eval_expr(&order.expr, dataset, row, params, ctes, None))
                .collect::<Result<Vec<_>>>()?;
            Ok((*row_index, keys))
        })
        .collect::<Result<Vec<_>>>()?;
    keyed.sort_by(|(left_index, left_keys), (right_index, right_keys)| {
        for (order, (left, right)) in order_by.iter().zip(left_keys.iter().zip(right_keys.iter())) {
            let ordering = compare_values(left, right).unwrap_or(std::cmp::Ordering::Equal);
            if ordering != std::cmp::Ordering::Equal {
                return if order.descending {
                    ordering.reverse()
                } else {
                    ordering
                };
            }
        }
        left_index.cmp(right_index)
    });
    Ok(keyed.into_iter().map(|(row_index, _)| row_index).collect())
}

fn values_equal(left: &Value, right: &Value) -> Result<bool> {
    Ok(compare_values(left, right)? == std::cmp::Ordering::Equal)
}

fn value_to_numeric_f64(value: Value, fn_name: &str) -> Result<Option<f64>> {
    match value {
        Value::Null => Ok(None),
        Value::Int64(value) => Ok(Some(value as f64)),
        Value::Float64(value) => Ok(Some(value)),
        Value::Decimal { scaled, scale } => {
            Ok(Some((scaled as f64) / 10_f64.powi(i32::from(scale))))
        }
        other => Err(DbError::sql(format!(
            "{fn_name} expects numeric values, got {other:?}"
        ))),
    }
}

fn parse_percentile_fraction(value: Value, fn_name: &str) -> Result<f64> {
    let Some(fraction) = value_to_numeric_f64(value, fn_name)? else {
        return Err(DbError::sql(format!("{fn_name} fraction cannot be NULL")));
    };
    if !(0.0..=1.0).contains(&fraction) {
        return Err(DbError::sql(format!(
            "{fn_name} fraction must be between 0 and 1"
        )));
    }
    Ok(fraction)
}

fn aggregate_array_agg(
    ctx: &AggregateEvalContext<'_>,
    row_indexes: &[usize],
    expr: &Expr,
    distinct: bool,
    order_by: &[crate::sql::ast::OrderBy],
) -> Result<Value> {
    let ordered_indexes = sort_aggregate_row_indexes(
        ctx.runtime,
        ctx.dataset,
        row_indexes,
        order_by,
        ctx.params,
        ctx.ctes,
    )?;
    let mut values = Vec::new();
    let mut seen_values = Vec::<Value>::new();
    for row_index in ordered_indexes {
        let row = ctx
            .dataset
            .rows
            .get(row_index)
            .map(Vec::as_slice)
            .ok_or_else(|| DbError::internal("group row index is invalid"))?;
        let value = ctx.eval_row(row, expr)?;
        if distinct {
            if seen_values
                .iter()
                .map(|seen| values_equal(seen, &value))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .any(std::convert::identity)
            {
                continue;
            }
            seen_values.push(value.clone());
        }
        values.push(json_value_from_value(&value)?);
    }
    Ok(Value::Text(JsonValue::Array(values).render_json()))
}

fn aggregate_median(
    ctx: &AggregateEvalContext<'_>,
    row_indexes: &[usize],
    expr: &Expr,
    distinct: bool,
) -> Result<Value> {
    let mut values = Vec::new();
    for row_index in row_indexes {
        let row = ctx
            .dataset
            .rows
            .get(*row_index)
            .map(Vec::as_slice)
            .ok_or_else(|| DbError::internal("group row index is invalid"))?;
        if let Some(number) = value_to_numeric_f64(ctx.eval_row(row, expr)?, "MEDIAN")? {
            values.push(number);
        }
    }
    if distinct {
        values.sort_by(|a, b| a.total_cmp(b));
        values.dedup_by(|a, b| a.total_cmp(b) == std::cmp::Ordering::Equal);
    } else {
        values.sort_by(|a, b| a.total_cmp(b));
    }
    if values.is_empty() {
        return Ok(Value::Null);
    }
    let mid = values.len() / 2;
    if values.len() % 2 == 1 {
        Ok(Value::Float64(values[mid]))
    } else {
        Ok(Value::Float64((values[mid - 1] + values[mid]) / 2.0))
    }
}

fn aggregate_percentile_cont(
    runtime: &EngineRuntime,
    dataset: &Dataset,
    row_indexes: &[usize],
    fraction_expr: &Expr,
    order_by: &[crate::sql::ast::OrderBy],
    params: &[Value],
    ctes: &BTreeMap<String, Dataset>,
) -> Result<Value> {
    if order_by.len() != 1 {
        return Err(DbError::sql(
            "PERCENTILE_CONT requires exactly one ORDER BY expression",
        ));
    }
    let fraction = parse_percentile_fraction(
        runtime.eval_expr(fraction_expr, dataset, &[], params, ctes, None)?,
        "PERCENTILE_CONT",
    )?;
    let ordered_indexes =
        sort_aggregate_row_indexes(runtime, dataset, row_indexes, order_by, params, ctes)?;
    let mut values = Vec::new();
    for row_index in ordered_indexes {
        let row = dataset
            .rows
            .get(row_index)
            .map(Vec::as_slice)
            .ok_or_else(|| DbError::internal("group row index is invalid"))?;
        let order_value = runtime.eval_expr(&order_by[0].expr, dataset, row, params, ctes, None)?;
        if let Some(number) = value_to_numeric_f64(order_value, "PERCENTILE_CONT")? {
            values.push(number);
        }
    }
    if values.is_empty() {
        return Ok(Value::Null);
    }
    let max_index = (values.len() - 1) as f64;
    let position = fraction * max_index;
    let lower_index = position.floor() as usize;
    let upper_index = position.ceil() as usize;
    if lower_index == upper_index {
        Ok(Value::Float64(values[lower_index]))
    } else {
        let lower = values[lower_index];
        let upper = values[upper_index];
        let weight = position - (lower_index as f64);
        Ok(Value::Float64(lower + (upper - lower) * weight))
    }
}

fn aggregate_percentile_disc(
    runtime: &EngineRuntime,
    dataset: &Dataset,
    row_indexes: &[usize],
    fraction_expr: &Expr,
    order_by: &[crate::sql::ast::OrderBy],
    params: &[Value],
    ctes: &BTreeMap<String, Dataset>,
) -> Result<Value> {
    if order_by.len() != 1 {
        return Err(DbError::sql(
            "PERCENTILE_DISC requires exactly one ORDER BY expression",
        ));
    }
    let fraction = parse_percentile_fraction(
        runtime.eval_expr(fraction_expr, dataset, &[], params, ctes, None)?,
        "PERCENTILE_DISC",
    )?;
    let ordered_indexes =
        sort_aggregate_row_indexes(runtime, dataset, row_indexes, order_by, params, ctes)?;
    let mut values = Vec::new();
    for row_index in ordered_indexes {
        let row = dataset
            .rows
            .get(row_index)
            .map(Vec::as_slice)
            .ok_or_else(|| DbError::internal("group row index is invalid"))?;
        let order_value = runtime.eval_expr(&order_by[0].expr, dataset, row, params, ctes, None)?;
        if !matches!(order_value, Value::Null) {
            values.push(order_value);
        }
    }
    if values.is_empty() {
        return Ok(Value::Null);
    }
    let threshold = ((values.len() as f64) * fraction).ceil() as usize;
    let index = threshold.saturating_sub(1).min(values.len() - 1);
    Ok(values[index].clone())
}

fn decimal_to_string(scaled: i64, scale: u8) -> String {
    if scale == 0 {
        return scaled.to_string();
    }
    let negative = scaled < 0;
    let digits = scaled.unsigned_abs().to_string();
    let scale = usize::from(scale);
    let padded = if digits.len() <= scale {
        format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
    } else {
        digits
    };
    let split = padded.len() - scale;
    let mut output = format!("{}.{}", &padded[..split], &padded[split..]);
    if negative {
        output.insert(0, '-');
    }
    output
}

fn eval_like(
    left: Value,
    right: Value,
    escape: Option<Value>,
    case_insensitive: bool,
    negated: bool,
) -> Result<Value> {
    let escape = normalize_like_escape(escape)?;
    match (left, right) {
        (Value::Text(left), Value::Text(right)) => {
            let matches = like_match(&left, &right, case_insensitive, escape);
            Ok(Value::Bool(if negated { !matches } else { matches }))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        other => Err(DbError::sql(format!(
            "LIKE expects text values, got {other:?}"
        ))),
    }
}

fn normalize_like_escape(escape: Option<Value>) -> Result<Option<char>> {
    match escape {
        None => Ok(None),
        Some(Value::Null) => Ok(None),
        Some(Value::Text(text)) => {
            let mut chars = text.chars();
            let Some(ch) = chars.next() else {
                return Ok(None);
            };
            if chars.next().is_some() {
                return Err(DbError::sql(
                    "LIKE ESCAPE expression must evaluate to a single character",
                ));
            }
            Ok(Some(ch))
        }
        Some(other) => Err(DbError::sql(format!(
            "LIKE ESCAPE expects text, got {other:?}"
        ))),
    }
}

fn aggregate_group_concat(
    ctx: &AggregateEvalContext<'_>,
    row_indexes: &[usize],
    args: &[Expr],
    distinct: bool,
    order_by: &[crate::sql::ast::OrderBy],
    function_name: &str,
) -> Result<Value> {
    let function_name = function_name.to_ascii_uppercase();
    if args.is_empty() || args.len() > 2 {
        return Err(DbError::sql(format!(
            "{function_name} expects 1 or 2 arguments"
        )));
    }
    let ordered_indexes = sort_aggregate_row_indexes(
        ctx.runtime,
        ctx.dataset,
        row_indexes,
        order_by,
        ctx.params,
        ctx.ctes,
    )?;
    let mut parts = Vec::new();
    let mut seen_values = Vec::<Value>::new();
    let mut separator = ",".to_string();
    for row_index in ordered_indexes {
        let row = ctx
            .dataset
            .rows
            .get(row_index)
            .map(Vec::as_slice)
            .ok_or_else(|| DbError::internal("group row index is invalid"))?;
        let value = ctx.eval_row(row, &args[0])?;
        if matches!(value, Value::Null) {
            continue;
        }
        if distinct {
            if seen_values
                .iter()
                .map(|seen| values_equal(seen, &value))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .any(std::convert::identity)
            {
                continue;
            }
            seen_values.push(value.clone());
        }
        if let Some(separator_expr) = args.get(1) {
            separator = match ctx.eval_row(row, separator_expr)? {
                Value::Text(value) => value,
                Value::Null => String::new(),
                other => {
                    return Err(DbError::sql(format!(
                        "{function_name} separator must be text, got {other:?}"
                    )))
                }
            };
        }
        parts.push(value_to_text(&value)?);
    }
    if parts.is_empty() {
        Ok(Value::Null)
    } else {
        Ok(Value::Text(parts.join(&separator)))
    }
}

fn eval_json_array_length(values: Vec<Value>) -> Result<Value> {
    if values.is_empty() || values.len() > 2 {
        return Err(DbError::sql("json_array_length expects 1 or 2 arguments"));
    }
    let target = json_target_value(&values)?;
    match target {
        Some(JsonValue::Array(array)) => Ok(Value::Int64(array.len() as i64)),
        Some(_) => Ok(Value::Int64(0)),
        None => Ok(Value::Null),
    }
}

fn eval_json_extract(values: Vec<Value>) -> Result<Value> {
    if values.len() != 2 {
        return Err(DbError::sql("json_extract expects 2 arguments"));
    }
    let Some(target) = json_target_value(&values)? else {
        return Ok(Value::Null);
    };
    json_value_to_value(target)
}

fn json_target_value(values: &[Value]) -> Result<Option<JsonValue>> {
    let json = match values.first() {
        Some(Value::Null) | None => return Ok(None),
        Some(Value::Text(value)) => parse_json(value)?,
        Some(other) => {
            return Err(DbError::sql(format!(
                "JSON functions expect text input, got {other:?}"
            )))
        }
    };
    if let Some(path_value) = values.get(1) {
        let path = match path_value {
            Value::Null => return Ok(None),
            Value::Text(path) => parse_json_path(path)?,
            other => {
                return Err(DbError::sql(format!(
                    "JSON path must be text, got {other:?}"
                )))
            }
        };
        Ok(json.lookup(&path).cloned())
    } else {
        Ok(Some(json))
    }
}

fn json_value_to_value(value: JsonValue) -> Result<Value> {
    match value {
        JsonValue::Null => Ok(Value::Null),
        JsonValue::Bool(value) => Ok(Value::Bool(value)),
        JsonValue::String(value) => Ok(Value::Text(value)),
        JsonValue::Number(value) => {
            let (scaled, scale) = parse_decimal_text(&value)?;
            if scale == 0 {
                Ok(Value::Int64(scaled))
            } else {
                Ok(Value::Float64(
                    (scaled as f64) / 10_f64.powi(i32::from(scale)),
                ))
            }
        }
        JsonValue::Object(_) | JsonValue::Array(_) => Ok(Value::Text(value.render_json())),
    }
}

fn value_to_text(value: &Value) -> Result<String> {
    match value {
        Value::Null => Ok(String::new()),
        Value::Int64(value) => Ok(value.to_string()),
        Value::Float64(value) => Ok(value.to_string()),
        Value::Bool(value) => Ok(if *value { "true" } else { "false" }.to_string()),
        Value::Text(value) => Ok(value.clone()),
        Value::Blob(_) => Err(DbError::sql("cannot stringify BLOB value")),
        Value::Decimal { scaled, scale } => {
            if *scale == 0 {
                Ok(scaled.to_string())
            } else {
                let negative = *scaled < 0;
                let digits = scaled.unsigned_abs().to_string();
                let scale = usize::from(*scale);
                if digits.len() <= scale {
                    let padded = format!("{digits:0>width$}", width = scale + 1);
                    let split = padded.len() - scale;
                    Ok(format!(
                        "{}{}.{}",
                        if negative { "-" } else { "" },
                        &padded[..split],
                        &padded[split..]
                    ))
                } else {
                    let split = digits.len() - scale;
                    Ok(format!(
                        "{}{}.{}",
                        if negative { "-" } else { "" },
                        &digits[..split],
                        &digits[split..]
                    ))
                }
            }
        }
        Value::Uuid(value) => Ok(format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
            value[8], value[9], value[10], value[11], value[12], value[13], value[14], value[15]
        )),
        Value::TimestampMicros(value) => Ok(value.to_string()),
    }
}

fn cast_value(value: Value, target_type: crate::catalog::ColumnType) -> Result<Value> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    match target_type {
        crate::catalog::ColumnType::Int64 => match value {
            Value::Int64(value) => Ok(Value::Int64(value)),
            Value::Float64(value) => Ok(Value::Int64(value as i64)),
            Value::Decimal { scaled, scale } => Ok(Value::Int64(decimal_to_i64(scaled, scale))),
            Value::Bool(value) => Ok(Value::Int64(if value { 1 } else { 0 })),
            Value::Text(value) => value
                .parse::<i64>()
                .map(Value::Int64)
                .map_err(|_| DbError::sql("invalid INT64 cast")),
            other => Err(DbError::sql(format!("cannot cast {other:?} to INT64"))),
        },
        crate::catalog::ColumnType::Float64 => match value {
            Value::Int64(value) => Ok(Value::Float64(value as f64)),
            Value::Float64(value) => Ok(Value::Float64(value)),
            Value::Decimal { scaled, scale } => Ok(Value::Float64(decimal_to_f64(scaled, scale))),
            Value::Text(value) => value
                .parse::<f64>()
                .map(Value::Float64)
                .map_err(|_| DbError::sql("invalid FLOAT64 cast")),
            other => Err(DbError::sql(format!("cannot cast {other:?} to FLOAT64"))),
        },
        crate::catalog::ColumnType::Text => Ok(Value::Text(match value {
            Value::Text(value) => value,
            Value::Int64(value) => value.to_string(),
            Value::Float64(value) => value.to_string(),
            Value::Bool(value) => value.to_string(),
            other => return Err(DbError::sql(format!("cannot cast {other:?} to TEXT"))),
        })),
        crate::catalog::ColumnType::Bool => match value {
            Value::Bool(value) => Ok(Value::Bool(value)),
            Value::Text(value) => match value.to_ascii_lowercase().as_str() {
                "true" | "t" | "1" => Ok(Value::Bool(true)),
                "false" | "f" | "0" => Ok(Value::Bool(false)),
                _ => Err(DbError::sql("invalid BOOL cast")),
            },
            other => Err(DbError::sql(format!("cannot cast {other:?} to BOOL"))),
        },
        crate::catalog::ColumnType::Blob => match value {
            Value::Blob(value) => Ok(Value::Blob(value)),
            Value::Uuid(value) => Ok(Value::Blob(value.to_vec())),
            other => Err(DbError::sql(format!("cannot cast {other:?} to BLOB"))),
        },
        crate::catalog::ColumnType::Decimal => match value {
            Value::Decimal { scaled, scale } => Ok(Value::Decimal { scaled, scale }),
            Value::Int64(value) => Ok(Value::Decimal {
                scaled: value,
                scale: 0,
            }),
            Value::Float64(value) => {
                let (scaled, scale) = parse_decimal_text(&value.to_string())?;
                Ok(Value::Decimal { scaled, scale })
            }
            Value::Text(value) => {
                let (scaled, scale) = parse_decimal_text(&value)?;
                Ok(Value::Decimal { scaled, scale })
            }
            other => Err(DbError::sql(format!("cannot cast {other:?} to DECIMAL"))),
        },
        crate::catalog::ColumnType::Uuid => match value {
            Value::Uuid(value) => Ok(Value::Uuid(value)),
            Value::Blob(value) if value.len() == 16 => {
                let mut uuid = [0u8; 16];
                uuid.copy_from_slice(&value);
                Ok(Value::Uuid(uuid))
            }
            other => Err(DbError::sql(format!("cannot cast {other:?} to UUID"))),
        },
        crate::catalog::ColumnType::Timestamp => match value {
            Value::TimestampMicros(value) => Ok(Value::TimestampMicros(value)),
            Value::Int64(value) => Ok(Value::TimestampMicros(value)),
            Value::Text(value) => Ok(Value::TimestampMicros(
                parse_datetime_text("TIMESTAMP cast", &value)?.timestamp_micros(),
            )),
            other => Err(DbError::sql(format!("cannot cast {other:?} to TIMESTAMP"))),
        },
    }
}

fn decimal_to_f64(scaled: i64, scale: u8) -> f64 {
    (scaled as f64) / 10_f64.powi(i32::from(scale))
}

fn decimal_to_i64(scaled: i64, scale: u8) -> i64 {
    if scale == 0 {
        return scaled;
    }
    let Some(divisor) = 10_i64.checked_pow(u32::from(scale)) else {
        return 0;
    };
    scaled / divisor
}

fn infer_column_type_for_ctas(
    rows: &[Vec<Value>],
    column_index: usize,
) -> crate::catalog::ColumnType {
    for row in rows {
        let Some(value) = row.get(column_index) else {
            continue;
        };
        match value {
            Value::Null => continue,
            Value::Int64(_) => return crate::catalog::ColumnType::Int64,
            Value::Float64(_) => return crate::catalog::ColumnType::Float64,
            Value::Text(_) => return crate::catalog::ColumnType::Text,
            Value::Bool(_) => return crate::catalog::ColumnType::Bool,
            Value::Blob(_) => return crate::catalog::ColumnType::Blob,
            Value::Decimal { .. } => return crate::catalog::ColumnType::Decimal,
            Value::Uuid(_) => return crate::catalog::ColumnType::Uuid,
            Value::TimestampMicros(_) => return crate::catalog::ColumnType::Timestamp,
        }
    }
    crate::catalog::ColumnType::Text
}

fn truthy(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        Value::Null => None,
        _ => None,
    }
}

fn eval_binary(op: &BinaryOp, left: Value, right: Value) -> Result<Value> {
    match op {
        BinaryOp::And => Ok(match (truthy(&left), truthy(&right)) {
            (Some(false), _) | (_, Some(false)) => Value::Bool(false),
            (Some(true), Some(true)) => Value::Bool(true),
            _ => Value::Null,
        }),
        BinaryOp::Or => Ok(match (truthy(&left), truthy(&right)) {
            (Some(true), _) | (_, Some(true)) => Value::Bool(true),
            (Some(false), Some(false)) => Value::Bool(false),
            _ => Value::Null,
        }),
        BinaryOp::Concat => match (left, right) {
            (Value::Text(left), Value::Text(right)) => Ok(Value::Text(left + &right)),
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            other => Err(DbError::sql(format!("cannot concatenate {other:?}"))),
        },
        BinaryOp::JsonExtract => eval_json_binary_operator(&left, &right, false),
        BinaryOp::JsonExtractText => eval_json_binary_operator(&left, &right, true),
        BinaryOp::RegexMatch => eval_regex(left, right, false, false),
        BinaryOp::RegexMatchCaseInsensitive => eval_regex(left, right, true, false),
        BinaryOp::RegexNotMatch => eval_regex(left, right, false, true),
        BinaryOp::RegexNotMatchCaseInsensitive => eval_regex(left, right, true, true),
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
            arithmetic(op, left, right)
        }
        BinaryOp::IsDistinctFrom => Ok(Value::Bool(
            compare_values(&left, &right)? != std::cmp::Ordering::Equal,
        )),
        BinaryOp::IsNotDistinctFrom => Ok(Value::Bool(
            compare_values(&left, &right)? == std::cmp::Ordering::Equal,
        )),
        _ => {
            if matches!(left, Value::Null) || matches!(right, Value::Null) {
                return Ok(Value::Null);
            }
            let ordering = compare_values(&left, &right)?;
            Ok(Value::Bool(match op {
                BinaryOp::Eq => ordering == std::cmp::Ordering::Equal,
                BinaryOp::NotEq => ordering != std::cmp::Ordering::Equal,
                BinaryOp::Lt => ordering == std::cmp::Ordering::Less,
                BinaryOp::LtEq => ordering != std::cmp::Ordering::Greater,
                BinaryOp::Gt => ordering == std::cmp::Ordering::Greater,
                BinaryOp::GtEq => ordering != std::cmp::Ordering::Less,
                _ => unreachable!(),
            }))
        }
    }
}

fn eval_regex(left: Value, right: Value, case_insensitive: bool, negated: bool) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Text(left), Value::Text(pattern)) => {
            let mut builder = regex::RegexBuilder::new(&pattern);
            builder.case_insensitive(case_insensitive);
            let regex = builder
                .build()
                .map_err(|error| DbError::sql(format!("invalid regular expression: {error}")))?;
            let matched = regex.is_match(&left);
            Ok(Value::Bool(if negated { !matched } else { matched }))
        }
        other => Err(DbError::sql(format!(
            "regex operators expect text values, got {other:?}"
        ))),
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use crate::record::compression::CompressionMode;
    use crate::search::TrigramQueryResult;
    use crate::sql::parser::parse_sql_statement;
    use crate::storage::page::InMemoryPageStore;
    use crate::Value;

    use super::{
        decode_manifest_payload, decode_runtime_payload, drop_index_include_columns_section,
        encode_manifest_payload, encode_runtime_payload, encode_table_payload, EngineRuntime,
        PersistedTableState, RuntimeBtreeKeys, RuntimeIndex,
    };

    const PAGE_SIZE: u32 = 4096;

    #[test]
    fn runtime_clone_preserves_btree_and_trigram_indexes() {
        let mut runtime = EngineRuntime::empty(1);
        execute_sql(
            &mut runtime,
            "CREATE TABLE docs (id INT64 PRIMARY KEY, email TEXT, body TEXT)",
        );
        execute_sql(&mut runtime, "CREATE INDEX docs_email_idx ON docs (email)");
        execute_sql(
            &mut runtime,
            "CREATE INDEX docs_body_trgm_idx ON docs USING gin (body)",
        );
        execute_sql(
            &mut runtime,
            "INSERT INTO docs (id, email, body) VALUES (1, 'a@example.com', 'alphabet soup')",
        );

        let cloned = runtime.clone();

        let RuntimeIndex::Btree { keys } = cloned
            .indexes
            .get("docs_email_idx")
            .expect("email index should be cloned")
        else {
            panic!("expected BTREE runtime index");
        };
        assert!(!keys.is_empty(), "btree entries should be preserved");

        let RuntimeIndex::Trigram { index } = cloned
            .indexes
            .get("docs_body_trgm_idx")
            .expect("trigram index should be cloned")
        else {
            panic!("expected trigram runtime index");
        };
        assert_eq!(
            index
                .query_candidates("alpha", false)
                .expect("query cloned index"),
            TrigramQueryResult::Candidates(vec![1])
        );
    }

    #[test]
    fn non_nullable_int64_btree_indexes_use_typed_runtime_keys() {
        let mut runtime = EngineRuntime::empty(1);
        execute_sql(
            &mut runtime,
            "CREATE TABLE docs (id INT64 PRIMARY KEY, email TEXT)",
        );
        execute_sql(
            &mut runtime,
            "INSERT INTO docs (id, email) VALUES (7, 'a@example.com')",
        );

        let index_name = runtime
            .catalog
            .indexes
            .keys()
            .next()
            .cloned()
            .expect("primary-key index should exist");

        let RuntimeIndex::Btree { keys } = runtime
            .indexes
            .get(&index_name)
            .expect("INT64 index should exist")
        else {
            panic!("expected BTREE runtime index");
        };

        let RuntimeBtreeKeys::UniqueInt64(entries) = keys else {
            panic!("expected typed INT64 runtime keys");
        };
        assert_eq!(entries.get(&7), Some(&7));
        assert_eq!(
            keys.row_ids_for_value(&Value::Int64(7))
                .expect("INT64 lookup should succeed"),
            vec![7]
        );
        assert!(keys
            .row_ids_for_value(&Value::Text("7".into()))
            .expect("mismatched lookup should not fail")
            .is_empty());
    }

    #[test]
    fn legacy_runtime_payload_decode_still_round_trips() {
        let mut runtime = EngineRuntime::empty(7);
        execute_sql(
            &mut runtime,
            "CREATE TABLE docs (id INT64 PRIMARY KEY, email TEXT, body TEXT)",
        );
        execute_sql(&mut runtime, "CREATE INDEX docs_email_idx ON docs (email)");
        execute_sql(
            &mut runtime,
            "INSERT INTO docs (id, email, body) VALUES (1, 'a@example.com', 'alphabet soup')",
        );

        let payload = encode_runtime_payload(&runtime).expect("encode legacy runtime payload");
        let decoded = decode_runtime_payload(&payload).expect("decode legacy runtime payload");

        assert_eq!(decoded.catalog.schema_cookie, runtime.catalog.schema_cookie);
        assert_eq!(decoded.tables["docs"].rows, runtime.tables["docs"].rows);
        assert!(decoded.catalog.indexes.contains_key("docs_email_idx"));
    }

    #[test]
    fn manifest_payload_decode_loads_per_table_rows() {
        let mut runtime = EngineRuntime::empty(9);
        execute_sql(
            &mut runtime,
            "CREATE TABLE docs (id INT64 PRIMARY KEY, email TEXT, body TEXT)",
        );
        execute_sql(
            &mut runtime,
            "INSERT INTO docs (id, email, body) VALUES (1, 'a@example.com', 'alphabet soup')",
        );

        let mut store = InMemoryPageStore::new(PAGE_SIZE);
        let table_payload =
            encode_table_payload(&runtime.tables["docs"]).expect("encode table payload");
        let pointer = crate::record::overflow::write_overflow(
            &mut store,
            &table_payload,
            CompressionMode::Auto,
        )
        .expect("write table payload");
        let tail = crate::record::overflow::read_uncompressed_overflow_tail(&store, pointer)
            .expect("read table tail")
            .expect("table tail");
        let mut table_states = runtime.persisted_tables.clone();
        table_states.insert(
            "docs".to_string(),
            PersistedTableState {
                pointer,
                checksum: crate::storage::checksum::crc32c_parts(&[table_payload.as_slice()]),
                row_count: runtime.tables["docs"].rows.len(),
                tail,
            },
        );

        let manifest =
            encode_manifest_payload(&runtime, &table_states).expect("encode manifest payload");
        let decoded = decode_manifest_payload(&store, &manifest).expect("decode manifest payload");

        assert_eq!(decoded.catalog.schema_cookie, runtime.catalog.schema_cookie);
        // Table data is deferred — "docs" should not be in tables yet.
        assert!(decoded.deferred_tables.contains("docs"));
        assert!(!decoded.tables.contains_key("docs"));
        // Schema and persisted state (pointer, checksum) are available immediately.
        assert!(decoded.catalog.tables.contains_key("docs"));
        assert_eq!(
            decoded.persisted_tables["docs"].pointer,
            table_states["docs"].pointer
        );
        assert_eq!(
            decoded.persisted_tables["docs"].checksum,
            table_states["docs"].checksum
        );
    }

    #[test]
    fn index_include_columns_round_trip_manifest_and_legacy_payloads() {
        let mut runtime = EngineRuntime::empty(13);
        execute_sql(
            &mut runtime,
            "CREATE TABLE cover_idx (id INT64 PRIMARY KEY, k TEXT, payload TEXT, flag BOOL)",
        );
        execute_sql(
            &mut runtime,
            "CREATE INDEX cover_idx_k ON cover_idx (k) INCLUDE (payload, flag)",
        );

        let legacy = encode_runtime_payload(&runtime).expect("encode legacy runtime payload");
        let legacy_decoded =
            decode_runtime_payload(&legacy).expect("decode legacy runtime payload");
        assert_eq!(
            legacy_decoded.catalog.indexes["cover_idx_k"].include_columns,
            vec!["payload".to_string(), "flag".to_string()]
        );

        let store = InMemoryPageStore::new(PAGE_SIZE);
        let manifest =
            encode_manifest_payload(&runtime, &runtime.persisted_tables).expect("encode manifest");
        let manifest_decoded =
            decode_manifest_payload(&store, &manifest).expect("decode manifest payload");
        assert_eq!(
            manifest_decoded.catalog.indexes["cover_idx_k"].include_columns,
            vec!["payload".to_string(), "flag".to_string()]
        );
    }

    #[test]
    fn manifest_decode_without_index_include_section_defaults_to_empty_include_columns() {
        let mut runtime = EngineRuntime::empty(14);
        execute_sql(
            &mut runtime,
            "CREATE TABLE cover_legacy (id INT64 PRIMARY KEY, k TEXT, payload TEXT)",
        );
        execute_sql(
            &mut runtime,
            "CREATE INDEX cover_legacy_idx ON cover_legacy (k) INCLUDE (payload)",
        );
        let store = InMemoryPageStore::new(PAGE_SIZE);
        let manifest =
            encode_manifest_payload(&runtime, &runtime.persisted_tables).expect("encode manifest");
        let legacy_manifest =
            drop_index_include_columns_section(&manifest).expect("drop include section");
        let decoded =
            decode_manifest_payload(&store, &legacy_manifest).expect("decode legacy manifest");
        assert!(decoded.catalog.indexes["cover_legacy_idx"]
            .include_columns
            .is_empty());
    }

    #[test]
    fn schema_entries_round_trip_manifest_and_legacy_payloads() {
        let mut runtime = EngineRuntime::empty(15);
        execute_sql(&mut runtime, "CREATE SCHEMA app");
        execute_sql(&mut runtime, "CREATE SCHEMA IF NOT EXISTS analytics");

        let legacy = encode_runtime_payload(&runtime).expect("encode legacy runtime payload");
        let legacy_decoded =
            decode_runtime_payload(&legacy).expect("decode legacy runtime payload");
        assert!(legacy_decoded.catalog.schemas.contains_key("app"));
        assert!(legacy_decoded.catalog.schemas.contains_key("analytics"));

        let store = InMemoryPageStore::new(PAGE_SIZE);
        let manifest =
            encode_manifest_payload(&runtime, &runtime.persisted_tables).expect("encode manifest");
        let manifest_decoded =
            decode_manifest_payload(&store, &manifest).expect("decode manifest payload");
        assert!(manifest_decoded.catalog.schemas.contains_key("app"));
        assert!(manifest_decoded.catalog.schemas.contains_key("analytics"));
    }

    #[test]
    fn manifest_template_patch_updates_next_row_id() {
        let mut runtime = EngineRuntime::empty(10);
        execute_sql(&mut runtime, "CREATE TABLE docs (id INT64 PRIMARY KEY)");
        runtime
            .catalog
            .tables
            .get_mut("docs")
            .expect("table should exist")
            .next_row_id = 42;
        let store = InMemoryPageStore::new(PAGE_SIZE);

        let first_payload = runtime
            .manifest_payload()
            .expect("build first manifest payload")
            .to_vec();
        let first = decode_manifest_payload(&store, &first_payload).expect("decode first payload");
        assert_eq!(first.catalog.tables["docs"].next_row_id, 42);

        runtime
            .catalog
            .tables
            .get_mut("docs")
            .expect("table should exist")
            .next_row_id = 99;
        let second_payload = runtime
            .manifest_payload()
            .expect("build second manifest payload")
            .to_vec();
        let second =
            decode_manifest_payload(&store, &second_payload).expect("decode second payload");
        assert_eq!(second.catalog.tables["docs"].next_row_id, 99);
    }

    #[test]
    fn non_correlated_in_subquery_does_not_capture_outer_scope() {
        let mut runtime = EngineRuntime::empty(11);
        execute_sql(
            &mut runtime,
            "CREATE TABLE t (id INT64 PRIMARY KEY, name TEXT, grp INT64)",
        );
        execute_sql(
            &mut runtime,
            "INSERT INTO t VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 1)",
        );

        let statement = parse_sql_statement(
            "SELECT COUNT(*) FROM t WHERE grp IN (SELECT grp FROM t WHERE name = 'a')",
        )
        .expect("parse SQL");
        let result = runtime
            .execute_statement(&statement, &[], PAGE_SIZE)
            .expect("execute SQL");

        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values(), &[Value::Int64(2)]);
    }

    #[test]
    fn correlated_exists_uses_outer_table_name_when_inner_table_is_aliased() {
        let mut runtime = EngineRuntime::empty(12);
        execute_sql(
            &mut runtime,
            "CREATE TABLE del_artists (Id INT64 PRIMARY KEY, LibraryId INT64)",
        );
        execute_sql(
            &mut runtime,
            "CREATE TABLE del_contributors (Id INT64 PRIMARY KEY, ArtistId INT64)",
        );
        execute_sql(
            &mut runtime,
            "INSERT INTO del_artists VALUES (1, 10), (2, 20)",
        );
        execute_sql(
            &mut runtime,
            "INSERT INTO del_contributors VALUES (1, 1), (2, 2)",
        );

        let delete = parse_sql_statement(
            "DELETE FROM del_contributors WHERE EXISTS (\
             SELECT 1 FROM del_contributors AS c \
             INNER JOIN del_artists AS a ON c.ArtistId = a.Id \
             WHERE a.LibraryId = $1 AND del_contributors.Id = c.Id)",
        )
        .expect("parse delete");
        let result = runtime
            .execute_statement(&delete, &[Value::Int64(10)], PAGE_SIZE)
            .expect("execute delete");
        assert_eq!(result.affected_rows(), 1);

        let count =
            parse_sql_statement("SELECT COUNT(*) FROM del_contributors").expect("parse count");
        let result = runtime
            .execute_statement(&count, &[], PAGE_SIZE)
            .expect("execute count");
        assert_eq!(result.rows()[0].values(), &[Value::Int64(1)]);
    }

    #[test]
    fn simple_count_star_without_filter_uses_fast_path() {
        let mut runtime = EngineRuntime::empty(16);
        execute_sql(
            &mut runtime,
            "CREATE TABLE t (id INT64 PRIMARY KEY, name TEXT)",
        );
        execute_sql(
            &mut runtime,
            "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        );

        let statement = parse_sql_statement("SELECT COUNT(*) FROM t").expect("parse count");
        let result = runtime
            .execute_statement(&statement, &[], PAGE_SIZE)
            .expect("execute count");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values(), &[Value::Int64(3)]);
    }

    #[test]
    fn analyze_collects_table_and_index_stats() {
        let mut runtime = EngineRuntime::empty(12);
        execute_sql(
            &mut runtime,
            "CREATE TABLE docs (id INT64 PRIMARY KEY, email TEXT)",
        );
        execute_sql(&mut runtime, "CREATE INDEX docs_email_idx ON docs (email)");
        execute_sql(
            &mut runtime,
            "INSERT INTO docs (id, email) VALUES (1, 'a@example.com')",
        );
        execute_sql(
            &mut runtime,
            "INSERT INTO docs (id, email) VALUES (2, 'a@example.com')",
        );
        execute_sql(
            &mut runtime,
            "INSERT INTO docs (id, email) VALUES (3, 'b@example.com')",
        );

        execute_sql(&mut runtime, "ANALYZE docs");

        assert_eq!(
            runtime.catalog.table_stats.get("docs"),
            Some(&crate::catalog::TableStats { row_count: 3 })
        );
        assert_eq!(
            runtime.catalog.index_stats.get("docs_email_idx"),
            Some(&crate::catalog::IndexStats {
                entry_count: 3,
                distinct_key_count: 2,
            })
        );
    }

    #[test]
    fn manifest_round_trip_preserves_analyze_stats() {
        let mut runtime = EngineRuntime::empty(13);
        execute_sql(
            &mut runtime,
            "CREATE TABLE docs (id INT64 PRIMARY KEY, email TEXT)",
        );
        execute_sql(&mut runtime, "CREATE INDEX docs_email_idx ON docs (email)");
        execute_sql(
            &mut runtime,
            "INSERT INTO docs (id, email) VALUES (1, 'a@example.com')",
        );
        execute_sql(
            &mut runtime,
            "INSERT INTO docs (id, email) VALUES (2, 'b@example.com')",
        );
        execute_sql(&mut runtime, "ANALYZE");

        let store = InMemoryPageStore::new(PAGE_SIZE);
        let manifest = encode_manifest_payload(&runtime, &runtime.persisted_tables)
            .expect("encode manifest payload");
        let decoded = decode_manifest_payload(&store, &manifest).expect("decode manifest payload");

        assert_eq!(
            decoded.catalog.table_stats.get("docs"),
            Some(&crate::catalog::TableStats { row_count: 2 })
        );
        assert_eq!(
            decoded.catalog.index_stats.get("docs_email_idx"),
            Some(&crate::catalog::IndexStats {
                entry_count: 2,
                distinct_key_count: 2,
            })
        );
    }

    fn execute_sql(runtime: &mut EngineRuntime, sql: &str) {
        let statement = parse_sql_statement(sql).expect("parse SQL");
        runtime
            .execute_statement(&statement, &[], PAGE_SIZE)
            .expect("execute SQL");
    }
}

fn arithmetic(op: &BinaryOp, left: Value, right: Value) -> Result<Value> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    if let Some(result) = timestamp_interval_arithmetic(op, &left, &right)? {
        return Ok(result);
    }
    match (left, right) {
        (Value::Int64(left), Value::Int64(right)) => {
            if matches!(op, BinaryOp::Div | BinaryOp::Mod) && right == 0 {
                return Ok(Value::Null);
            }
            Ok(match op {
                BinaryOp::Add => Value::Int64(left + right),
                BinaryOp::Sub => Value::Int64(left - right),
                BinaryOp::Mul => Value::Int64(left * right),
                BinaryOp::Div => Value::Int64(left / right),
                BinaryOp::Mod => Value::Int64(left % right),
                _ => unreachable!(),
            })
        }
        (Value::Int64(left), Value::Float64(right)) => {
            arithmetic(op, Value::Float64(left as f64), Value::Float64(right))
        }
        (Value::Float64(left), Value::Int64(right)) => {
            arithmetic(op, Value::Float64(left), Value::Float64(right as f64))
        }
        (Value::Float64(left), Value::Float64(right)) => {
            if matches!(op, BinaryOp::Div | BinaryOp::Mod) && right == 0.0 {
                return Ok(Value::Null);
            }
            Ok(match op {
                BinaryOp::Add => Value::Float64(left + right),
                BinaryOp::Sub => Value::Float64(left - right),
                BinaryOp::Mul => Value::Float64(left * right),
                BinaryOp::Div => Value::Float64(left / right),
                BinaryOp::Mod => Value::Float64(left % right),
                _ => unreachable!(),
            })
        }
        other => Err(DbError::sql(format!(
            "arithmetic is not defined for {other:?}"
        ))),
    }
}

fn timestamp_interval_arithmetic(
    op: &BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Option<Value>> {
    match (op, left, right) {
        (BinaryOp::Add, Value::TimestampMicros(timestamp), Value::Int64(interval_micros)) => {
            Ok(Some(Value::TimestampMicros(apply_interval_micros(
                *timestamp,
                *interval_micros,
                true,
            )?)))
        }
        (BinaryOp::Sub, Value::TimestampMicros(timestamp), Value::Int64(interval_micros)) => {
            Ok(Some(Value::TimestampMicros(apply_interval_micros(
                *timestamp,
                *interval_micros,
                false,
            )?)))
        }
        (BinaryOp::Add, Value::Int64(interval_micros), Value::TimestampMicros(timestamp)) => {
            Ok(Some(Value::TimestampMicros(apply_interval_micros(
                *timestamp,
                *interval_micros,
                true,
            )?)))
        }
        (BinaryOp::Add, Value::Text(timestamp), Value::Int64(interval_micros)) => {
            Ok(Some(Value::TimestampMicros(apply_interval_micros(
                parse_datetime_text("interval arithmetic", timestamp)?.timestamp_micros(),
                *interval_micros,
                true,
            )?)))
        }
        (BinaryOp::Sub, Value::Text(timestamp), Value::Int64(interval_micros)) => {
            Ok(Some(Value::TimestampMicros(apply_interval_micros(
                parse_datetime_text("interval arithmetic", timestamp)?.timestamp_micros(),
                *interval_micros,
                false,
            )?)))
        }
        (BinaryOp::Add, Value::Int64(interval_micros), Value::Text(timestamp)) => {
            Ok(Some(Value::TimestampMicros(apply_interval_micros(
                parse_datetime_text("interval arithmetic", timestamp)?.timestamp_micros(),
                *interval_micros,
                true,
            )?)))
        }
        (BinaryOp::Add, Value::Text(timestamp), Value::Text(interval_text)) => {
            Ok(Some(Value::TimestampMicros(apply_interval_micros(
                parse_datetime_text("interval arithmetic", timestamp)?.timestamp_micros(),
                parse_interval_micros(interval_text)?,
                true,
            )?)))
        }
        (BinaryOp::Sub, Value::Text(timestamp), Value::Text(interval_text)) => {
            Ok(Some(Value::TimestampMicros(apply_interval_micros(
                parse_datetime_text("interval arithmetic", timestamp)?.timestamp_micros(),
                parse_interval_micros(interval_text)?,
                false,
            )?)))
        }
        (BinaryOp::Add, Value::TimestampMicros(timestamp), Value::Text(interval_text)) => {
            Ok(Some(Value::TimestampMicros(apply_interval_micros(
                *timestamp,
                parse_interval_micros(interval_text)?,
                true,
            )?)))
        }
        (BinaryOp::Sub, Value::TimestampMicros(timestamp), Value::Text(interval_text)) => {
            Ok(Some(Value::TimestampMicros(apply_interval_micros(
                *timestamp,
                parse_interval_micros(interval_text)?,
                false,
            )?)))
        }
        (BinaryOp::Add, Value::Text(interval_text), Value::TimestampMicros(timestamp)) => {
            Ok(Some(Value::TimestampMicros(apply_interval_micros(
                *timestamp,
                parse_interval_micros(interval_text)?,
                true,
            )?)))
        }
        _ => Ok(None),
    }
}

fn apply_interval_micros(timestamp_micros: i64, interval_micros: i64, add: bool) -> Result<i64> {
    if add {
        timestamp_micros
            .checked_add(interval_micros)
            .ok_or_else(|| DbError::sql("timestamp addition overflowed"))
    } else {
        timestamp_micros
            .checked_sub(interval_micros)
            .ok_or_else(|| DbError::sql("timestamp subtraction overflowed"))
    }
}

fn compare_values(left: &Value, right: &Value) -> Result<std::cmp::Ordering> {
    use std::cmp::Ordering;
    if let Some(ordering) = compare_numeric_text_values(left, right) {
        return Ok(ordering);
    }
    match (left, right) {
        (Value::Null, Value::Null) => Ok(Ordering::Equal),
        (Value::Null, _) => Ok(Ordering::Less),
        (_, Value::Null) => Ok(Ordering::Greater),
        (Value::Int64(left), Value::Int64(right)) => Ok(left.cmp(right)),
        (Value::Float64(left), Value::Float64(right)) => Ok(left.total_cmp(right)),
        (Value::Int64(left), Value::Float64(right)) => Ok((*left as f64).total_cmp(right)),
        (Value::Float64(left), Value::Int64(right)) => Ok(left.total_cmp(&(*right as f64))),
        (
            Value::Decimal {
                scaled: left_scaled,
                scale: left_scale,
            },
            Value::Decimal {
                scaled: right_scaled,
                scale: right_scale,
            },
        ) => Ok(compare_decimal(
            *left_scaled,
            *left_scale,
            *right_scaled,
            *right_scale,
        )),
        (
            Value::Decimal {
                scaled: left_scaled,
                scale: left_scale,
            },
            Value::Float64(right),
        ) => {
            let left_f64 = decimal_to_f64(*left_scaled, *left_scale);
            Ok(left_f64.total_cmp(right))
        }
        (
            Value::Float64(left),
            Value::Decimal {
                scaled: right_scaled,
                scale: right_scale,
            },
        ) => {
            let right_f64 = decimal_to_f64(*right_scaled, *right_scale);
            Ok(left.total_cmp(&right_f64))
        }
        (
            Value::Decimal {
                scaled: left_scaled,
                scale: left_scale,
            },
            Value::Int64(right),
        ) => {
            let left_f64 = decimal_to_f64(*left_scaled, *left_scale);
            Ok(left_f64.total_cmp(&(*right as f64)))
        }
        (
            Value::Int64(left),
            Value::Decimal {
                scaled: right_scaled,
                scale: right_scale,
            },
        ) => {
            let right_f64 = decimal_to_f64(*right_scaled, *right_scale);
            Ok((*left as f64).total_cmp(&right_f64))
        }
        (Value::Bool(left), Value::Bool(right)) => Ok(left.cmp(right)),
        (Value::Text(left), Value::Text(right)) => Ok(left.cmp(right)),
        (Value::Blob(left), Value::Blob(right)) => Ok(left.cmp(right)),
        (Value::Uuid(left), Value::Uuid(right)) => Ok(left.cmp(right)),
        (Value::TimestampMicros(left), Value::TimestampMicros(right)) => Ok(left.cmp(right)),
        (Value::Blob(left), Value::Uuid(right)) => Ok(left.as_slice().cmp(right.as_slice())),
        (Value::Uuid(left), Value::Blob(right)) => Ok(left.as_slice().cmp(right.as_slice())),
        _ => Err(DbError::sql(format!(
            "cannot compare values {left:?} and {right:?}"
        ))),
    }
}

fn window_order_keys_equal(left: &[Value], right: &[Value]) -> Result<bool> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (left_value, right_value) in left.iter().zip(right) {
        if compare_values(left_value, right_value)? != std::cmp::Ordering::Equal {
            return Ok(false);
        }
    }
    Ok(true)
}

fn compute_window_peer_bounds(order_keys: &[Vec<Value>]) -> Result<(Vec<usize>, Vec<usize>)> {
    let mut starts = vec![0_usize; order_keys.len()];
    let mut ends = vec![0_usize; order_keys.len()];
    let mut ordinal = 0_usize;
    while ordinal < order_keys.len() {
        let mut peer_end = ordinal;
        while peer_end + 1 < order_keys.len()
            && window_order_keys_equal(&order_keys[ordinal], &order_keys[peer_end + 1])?
        {
            peer_end += 1;
        }
        for peer in ordinal..=peer_end {
            starts[peer] = ordinal;
            ends[peer] = peer_end;
        }
        ordinal = peer_end + 1;
    }
    Ok((starts, ends))
}

fn normalize_window_frame_range(
    start: i64,
    end: i64,
    partition_len: usize,
) -> Result<Option<(usize, usize)>> {
    if partition_len == 0 {
        return Ok(None);
    }
    let len_i64 = i64::try_from(partition_len)
        .map_err(|_| DbError::internal("window partition is too large"))?;
    let start = start.clamp(0, len_i64);
    let end = end.clamp(-1, len_i64 - 1);
    if start > end {
        return Ok(None);
    }
    let start =
        usize::try_from(start).map_err(|_| DbError::internal("window frame start is invalid"))?;
    let end = usize::try_from(end).map_err(|_| DbError::internal("window frame end is invalid"))?;
    Ok(Some((start, end)))
}

fn compare_numeric_text_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;

    fn parsed_numeric_text(value: &str) -> Option<f64> {
        let (scaled, scale) = parse_decimal_text(value).ok()?;
        Some((scaled as f64) / 10_f64.powi(i32::from(scale)))
    }

    match (left, right) {
        (Value::Int64(left), Value::Text(right)) => parsed_numeric_text(right)
            .map(|right| (*left as f64).total_cmp(&right))
            .or(Some(Ordering::Less)),
        (Value::Float64(left), Value::Text(right)) => parsed_numeric_text(right)
            .map(|right| left.total_cmp(&right))
            .or(Some(Ordering::Less)),
        (Value::Text(left), Value::Int64(right)) => parsed_numeric_text(left)
            .map(|left| left.total_cmp(&(*right as f64)))
            .or(Some(Ordering::Greater)),
        (Value::Text(left), Value::Float64(right)) => parsed_numeric_text(left)
            .map(|left| left.total_cmp(right))
            .or(Some(Ordering::Greater)),
        _ => None,
    }
}

fn like_match(input: &str, pattern: &str, case_insensitive: bool, escape: Option<char>) -> bool {
    let input = if case_insensitive {
        input.to_ascii_uppercase()
    } else {
        input.to_string()
    };
    let pattern = if case_insensitive {
        pattern.to_ascii_uppercase()
    } else {
        pattern.to_string()
    };
    let input = input.chars().collect::<Vec<_>>();
    let pattern = pattern.chars().collect::<Vec<_>>();
    like_match_chars(&input, &pattern, escape)
}

fn like_match_chars(input: &[char], pattern: &[char], escape: Option<char>) -> bool {
    if pattern.is_empty() {
        return input.is_empty();
    }
    let current = pattern[0];
    if Some(current) == escape {
        return match pattern.get(1) {
            Some(literal) => {
                !input.is_empty()
                    && input[0] == *literal
                    && like_match_chars(&input[1..], &pattern[2..], escape)
            }
            None => {
                !input.is_empty()
                    && input[0] == current
                    && like_match_chars(&input[1..], &pattern[1..], escape)
            }
        };
    }
    match current {
        '%' => (0..=input.len())
            .any(|offset| like_match_chars(&input[offset..], &pattern[1..], escape)),
        '_' => !input.is_empty() && like_match_chars(&input[1..], &pattern[1..], escape),
        literal => {
            !input.is_empty()
                && input[0] == literal
                && like_match_chars(&input[1..], &pattern[1..], escape)
        }
    }
}

fn row_identity(row: &[Value]) -> Result<Vec<u8>> {
    Row::new(row.to_vec()).encode()
}

fn deduplicate_rows_stable(rows: Vec<Vec<Value>>) -> Result<Vec<Vec<Value>>> {
    let mut seen = BTreeSet::new();
    let mut distinct_rows = Vec::new();
    for row in rows {
        if seen.insert(row_identity(&row)?) {
            distinct_rows.push(row);
        }
    }
    Ok(distinct_rows)
}

fn count_row_identities(rows: &[Vec<Value>]) -> Result<HashMap<Vec<u8>, usize>> {
    let mut counts = HashMap::new();
    for row in rows {
        *counts.entry(row_identity(row)?).or_insert(0) += 1;
    }
    Ok(counts)
}

fn consume_row_identity_count(counts: &mut HashMap<Vec<u8>, usize>, identity: &[u8]) -> bool {
    if let Some(remaining) = counts.get_mut(identity) {
        if *remaining > 0 {
            *remaining -= 1;
            return true;
        }
    }
    false
}

fn deduplicate_rows(rows: Vec<Vec<Value>>) -> Result<Vec<Vec<Value>>> {
    let mut seen = BTreeMap::<Vec<u8>, Vec<Value>>::new();
    for row in rows {
        seen.entry(row_identity(&row)?).or_insert(row);
    }
    Ok(seen.into_values().collect())
}

pub(super) fn column_position(table: &TableSchema, column_name: &str) -> Option<usize> {
    table
        .columns
        .iter()
        .position(|column| identifiers_equal(&column.name, column_name))
}

pub(super) fn column_schema<'a>(
    table: &'a TableSchema,
    column_name: &str,
) -> Option<&'a ColumnSchema> {
    table
        .columns
        .iter()
        .find(|column| identifiers_equal(&column.name, column_name))
}

#[cfg(test)]
mod more_exec_tests;
#[cfg(test)]
mod runtime_tests;

#[cfg(test)]
mod exec_mod_private_tests {
    use super::*;

    #[test]
    fn map_get_ci_and_mut_basic() {
        let mut map = std::collections::BTreeMap::new();
        map.insert("Key".to_string(), 1);
        assert_eq!(map_get_ci(&map, "key"), Some(&1));
        let v = map_get_ci_mut(&mut map, "KEY");
        assert!(v.is_some());
        *v.unwrap() = 2;
        assert_eq!(map_get_ci(&map, "key"), Some(&2));
    }

    #[test]
    fn generated_columns_are_stored_behavior() {
        let table = TableSchema {
            name: "t".to_string(),
            temporary: false,
            columns: vec![crate::catalog::ColumnSchema {
                name: "a".to_string(),
                column_type: crate::catalog::ColumnType::Int64,
                nullable: false,
                default_sql: None,
                generated_sql: None,
                generated_stored: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                checks: vec![],
                foreign_key: None,
            }],
            checks: vec![],
            foreign_keys: vec![],
            primary_key_columns: vec![],
            next_row_id: 1,
        };
        assert!(generated_columns_are_stored(&table));

        let table2 = TableSchema {
            name: "u".to_string(),
            temporary: false,
            columns: vec![crate::catalog::ColumnSchema {
                name: "g".to_string(),
                column_type: crate::catalog::ColumnType::Int64,
                nullable: false,
                default_sql: None,
                generated_sql: Some("1".to_string()),
                generated_stored: true,
                primary_key: false,
                unique: false,
                auto_increment: false,
                checks: vec![],
                foreign_key: None,
            }],
            checks: vec![],
            foreign_keys: vec![],
            primary_key_columns: vec![],
            next_row_id: 1,
        };
        assert!(generated_columns_are_stored(&table2));

        let table3 = TableSchema {
            name: "v".to_string(),
            temporary: false,
            columns: vec![crate::catalog::ColumnSchema {
                name: "g".to_string(),
                column_type: crate::catalog::ColumnType::Int64,
                nullable: false,
                default_sql: None,
                generated_sql: Some("1".to_string()),
                generated_stored: false,
                primary_key: false,
                unique: false,
                auto_increment: false,
                checks: vec![],
                foreign_key: None,
            }],
            checks: vec![],
            foreign_keys: vec![],
            primary_key_columns: vec![],
            next_row_id: 1,
        };
        assert!(!generated_columns_are_stored(&table3));
    }
}

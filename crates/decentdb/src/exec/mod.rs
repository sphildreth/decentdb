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

pub(crate) mod cte;
mod decimal;
mod expressions;
mod type_of;

pub(crate) mod bench_queries;
pub(crate) mod codec;
pub(crate) mod deferred;
pub(crate) mod evaluate;
pub(crate) mod grouped;
pub(crate) mod indexes;
pub(crate) mod joins;
pub(crate) mod manifest;
pub(crate) mod paged_tables;
pub(crate) mod runtime_keys;
pub(crate) mod simple_queries;
pub(crate) mod table_data;

pub(crate) mod runtime_eval;
#[allow(unused_imports)]
pub(crate) use runtime_eval::*;

#[allow(unused_imports)]
pub(crate) use bench_queries::*;
#[allow(unused_imports)]
pub(crate) use codec::*;
#[allow(unused_imports)]
pub(crate) use deferred::*;
#[allow(unused_imports)]
pub(crate) use evaluate::*;
#[allow(unused_imports)]
pub(crate) use grouped::*;
#[allow(unused_imports)]
pub(crate) use indexes::*;
#[allow(unused_imports)]
pub(crate) use joins::*;
#[allow(unused_imports)]
pub(crate) use manifest::*;
#[allow(unused_imports)]
pub(crate) use paged_tables::*;
#[allow(unused_imports)]
pub(crate) use runtime_keys::*;
#[allow(unused_imports)]
pub(crate) use simple_queries::*;
#[allow(unused_imports)]
pub(crate) use table_data::*;

use expressions::*;

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::hash::{BuildHasherDefault, Hasher};
use std::ops::{Bound, Range};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, Months, NaiveDate, NaiveDateTime, TimeZone,
    Timelike, Utc,
};
use smallvec::{smallvec, SmallVec};

use crate::btree::cursor::BtreeCursor;
use crate::btree::read::{
    find_exact as btree_find_exact, first_position as btree_first_position,
    materialize_current as btree_materialize_current,
};
use crate::btree::table::free_table_btree;
use crate::btree::write::Btree;
use crate::catalog::{
    identifiers_equal, CatalogState, ColumnSchema, ColumnType, EnumLabel, EnumTypeInfo,
    ForeignKeyAction, IndexKind, IndexSchema, IndexStats, SchemaInfo, TableSchema, TableStats,
    TriggerEvent, TriggerKind, ViewSchema,
};
use crate::error::{DbError, Result};
use crate::json::{parse_json, parse_json_path, JsonValue};
use crate::planner;
use crate::record::compression::{CompressionMode, AUTO_MIN_PAYLOAD_BYTES};
use crate::record::key::{encode_runtime_index_key, RuntimeEncodedKey};
use crate::record::overflow::{
    append_uncompressed_with_first_page_patch, build_overflow_chain_cache, free_overflow,
    read_overflow, read_overflow_into, read_uncompressed_overflow_tail, rewrite_overflow,
    rewrite_overflow_cached, rewrite_overflow_cached_with_dirty_byte_ranges,
    rewrite_overflow_cached_with_sparse_byte_patches, write_overflow, OverflowBytePatch,
    OverflowChainCache, OverflowPointer, OverflowTailInfo, OVERFLOW_HEADER_SIZE,
};
use crate::record::row::Row;
use crate::record::value::{
    compare_cidr, compare_decimal, compare_interval, compare_ip_addr, compare_mac_addr,
    format_cidr, format_date_days, format_interval, format_ip_addr, format_mac_addr,
    format_time_micros, format_timestamp_tz_micros, parse_cidr, parse_date_days,
    parse_decimal_text, parse_interval, parse_ip_addr, parse_mac_addr, parse_time_micros,
    parse_timestamp_tz_micros, Value,
};
use crate::search::fulltext::{
    AnalyzerConfig, FullTextIndex, FullTextIndexBuilder, FTS_SEMANTIC_ERROR_PREFIX,
};
use crate::search::{TrigramIndex, TrigramIndexBuilder, TrigramQueryResult};
use crate::spatial::index::{SpatialEnvelope, SpatialIndexBackend, SpatialRuntimeIndex};
use crate::spatial::types::{
    CoordinateDimensions, Position, SpatialError, SpatialGeometry, SpatialKind, SpatialValue,
};
use crate::sql::ast::{
    BinaryOp, Collation, ColumnDefinition, CommonTableExpr, CreateTableAsStatement,
    CreateTableStatement, Expr, FromItem, JoinConstraint, JoinKind, OrderBy, Query, QueryBody,
    Select, SelectItem, Statement, SubqueryQuantifier, TruncateIdentityMode, UnaryOp,
};
use crate::sql::parser::parse_sql_statement;
use crate::storage::checksum::{crc32c_parts, crc32c_patch_bytes};
use crate::storage::page::{self, PageId, PageStore};
use crate::storage::PagerHandle;
use crate::wal::WalHandle;

use self::cte::*;
pub(crate) use self::row::{ColumnBinding, Dataset};

pub use row::{QueryResult, QueryRow};

const ENGINE_ROOT_MAGIC: [u8; 8] = *b"DDBSQL1\0";
const ENGINE_ROOT_VERSION: u32 = 1;
const ENGINE_ROOT_HEADER_SIZE: usize = 32;
const RECURSIVE_CTE_MAX_ITERATIONS: usize = 1000;
const GENERATE_SERIES_MAX_ROWS: usize = 1_000_000;
const LEGACY_RUNTIME_PAYLOAD_MAGIC: &[u8; 9] = b"DDBSTATE1";
const MANIFEST_PAYLOAD_MAGIC: &[u8; 8] = b"DDBMANF1";
const TABLE_PAYLOAD_MAGIC: &[u8; 8] = b"DDBTBL01";
const TABLE_PAGED_MANIFEST_MAGIC: &[u8; 8] = b"DDBTPG02";
const TABLE_PAYLOAD_ROW_BODY_PADDING_BYTES: usize = 8;
/// ADR 0200: a resident-table payload row slot whose `row_body_len` field has
/// this high bit set is a logically deleted (tombstoned) slot. The remaining
/// 31 bits hold the real body length so the slot can still be traversed, and
/// the body bytes are retained as dead space. This lets a delete patch four
/// bytes in place instead of rewriting every byte after the deleted row. Real
/// encoded row bodies are required to be smaller than this flag.
pub(crate) const TABLE_PAYLOAD_ROW_TOMBSTONE_FLAG: u32 = 1 << 31;
const TABLE_PAYLOAD_ROW_LEN_MASK: u32 = TABLE_PAYLOAD_ROW_TOMBSTONE_FLAG - 1;

/// Split a stored `row_body_len` field into `(is_tombstone, actual_len)`.
#[inline]
pub(crate) fn split_table_payload_row_len(raw_len: u32) -> (bool, usize) {
    (
        (raw_len & TABLE_PAYLOAD_ROW_TOMBSTONE_FLAG) != 0,
        (raw_len & TABLE_PAYLOAD_ROW_LEN_MASK) as usize,
    )
}
const PAGED_TABLE_TARGET_CHUNK_PAGES: usize = 16;
pub(super) const PAGED_TABLE_RESIDENT_APPEND_ROW_THRESHOLD: usize = 1024;
const GENERATED_COLUMNS_SECTION_MAGIC: &[u8; 8] = b"DDBGCM02";
const INDEX_INCLUDE_COLUMNS_SECTION_MAGIC: &[u8; 8] = b"DDBICL1\0";
const SCHEMAS_SECTION_MAGIC: &[u8; 8] = b"DDBSCH01";
const PK_INDEX_ROOTS_SECTION_MAGIC: &[u8; 8] = b"DDBPKR01";
const SPATIAL_COLUMNS_SECTION_MAGIC: &[u8; 8] = b"DDBSPT01";
const ENUM_COLUMNS_SECTION_MAGIC: &[u8; 8] = b"DDBENU01";
const FULL_TEXT_OPTIONS_SECTION_MAGIC: &[u8; 8] = b"DDBFTS01";
const SIGNED_ROW_ID_BIAS: u64 = 0x8000_0000_0000_0000;
const DEFERRED_COMPRESSED_LOOKUP_CACHE_LIMIT: usize = 32;
const DEFERRED_RUNTIME_BTREE_INDEX_CACHE_LIMIT: usize = 16;
const DEFERRED_PAGED_ROW_PAYLOAD_CACHE_LIMIT_BYTES: usize = 8 * 1024 * 1024;
const DEFERRED_VIEW_LIMIT_MIN_PERSISTED_ROWS: usize = 10_000;
const VIEW_QUERY_CACHE_LIMIT: usize = 128;
static RANDOM_STATE: AtomicU64 = AtomicU64::new(0);
static DEFERRED_COMPRESSED_LOOKUP_CACHE: OnceLock<Mutex<DeferredCompressedLookupCache>> =
    OnceLock::new();
static DEFERRED_RUNTIME_BTREE_INDEX_CACHE: OnceLock<Mutex<DeferredRuntimeBtreeIndexCache>> =
    OnceLock::new();
const EXEC_MICROS_PER_DAY: i64 = 86_400_000_000;
const FTS_HIDDEN_ROW_ID_COLUMN: &str = "__decentdb_fts_rowid";

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CompatSchemaQualifier {
    Main,
    Temp,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct ViewQueryCacheKey {
    temporary: bool,
    name: String,
    sql_text: String,
}

impl ViewQueryCacheKey {
    fn new(view: &ViewSchema) -> Self {
        Self {
            temporary: view.temporary,
            name: view.name.clone(),
            sql_text: view.sql_text.clone(),
        }
    }
}

#[derive(Debug, Default)]
struct ViewQueryCache {
    entries: HashMap<ViewQueryCacheKey, Arc<Query>>,
    insertion_order: VecDeque<ViewQueryCacheKey>,
}

impl ViewQueryCache {
    fn get(&self, key: &ViewQueryCacheKey) -> Option<Arc<Query>> {
        self.entries.get(key).cloned()
    }

    fn insert(&mut self, key: ViewQueryCacheKey, query: Arc<Query>) -> Arc<Query> {
        if VIEW_QUERY_CACHE_LIMIT == 0 {
            return query;
        }
        if !self.entries.contains_key(&key) {
            self.insertion_order.push_back(key.clone());
        }
        self.entries.insert(key, Arc::clone(&query));
        self.evict_excess();
        query
    }

    fn evict_excess(&mut self) {
        while self.entries.len() > VIEW_QUERY_CACHE_LIMIT {
            let Some(evicted) = self.insertion_order.pop_front() else {
                break;
            };
            self.entries.remove(&evicted);
        }
    }
}

pub(super) fn compat_schema_qualified_name(name: &str) -> (Option<CompatSchemaQualifier>, &str) {
    match name.split_once('.') {
        Some((schema, object)) if schema.eq_ignore_ascii_case("main") => {
            (Some(CompatSchemaQualifier::Main), object)
        }
        Some((schema, object)) if schema.eq_ignore_ascii_case("temp") => {
            (Some(CompatSchemaQualifier::Temp), object)
        }
        _ => (None, name),
    }
}

pub(super) fn compat_unqualified_name(name: &str) -> &str {
    compat_schema_qualified_name(name).1
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

/// Resolves the canonical key for `name` in `map` using the same
/// case-insensitive matching rules as `map_get_ci`. Used by callers that
/// need to perform a follow-up mutation under a fresh `&mut` borrow.
fn map_key_ci<V>(map: &BTreeMap<String, V>, name: &str) -> Option<String> {
    if map.contains_key(name) {
        return Some(name.to_string());
    }
    map.keys()
        .find(|entry_name| identifiers_equal(entry_name, name))
        .cloned()
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StoredRow {
    pub(crate) row_id: i64,
    pub(crate) values: Vec<Value>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RowLocatorV1 {
    byte_offset: u32,
    byte_len: u32,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct DeferredCompressedLookupCacheKey {
    head_page_id: PageId,
    logical_len: u32,
    flags: u8,
    checksum: u32,
}

#[derive(Debug)]
pub(crate) struct DeferredCompressedLookupCacheEntry {
    payload: Arc<Vec<u8>>,
    row_locators: HashMap<i64, RowLocatorV1>,
}

#[derive(Default)]
struct DeferredCompressedLookupCache {
    entries: HashMap<DeferredCompressedLookupCacheKey, Arc<DeferredCompressedLookupCacheEntry>>,
    insertion_order: VecDeque<DeferredCompressedLookupCacheKey>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DeferredRuntimeBtreeIndexCacheKey {
    table_name: String,
    index_name: String,
    table_head_page_id: PageId,
    table_logical_len: u32,
    table_flags: u8,
    table_checksum: u32,
    unique: bool,
    columns: Vec<(Option<String>, Option<String>)>,
    include_columns: Vec<String>,
    predicate_sql: Option<String>,
}

impl DeferredRuntimeBtreeIndexCacheKey {
    fn new(table: &TableSchema, index: &IndexSchema, state: PersistedTableState) -> Self {
        Self {
            table_name: table.name.clone(),
            index_name: index.name.clone(),
            table_head_page_id: state.pointer.head_page_id,
            table_logical_len: state.pointer.logical_len,
            table_flags: state.pointer.flags,
            table_checksum: state.checksum,
            unique: index.unique,
            columns: index
                .columns
                .iter()
                .map(|column| (column.column_name.clone(), column.expression_sql.clone()))
                .collect(),
            include_columns: index.include_columns.clone(),
            predicate_sql: index.predicate_sql.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct DeferredRuntimeBtreeIndexCacheEntry {
    runtime_index: Arc<RuntimeIndex>,
    paged_locator_cache: Option<Arc<DeferredPagedRowLocatorCache>>,
}

#[derive(Default)]
struct DeferredRuntimeBtreeIndexCache {
    entries: HashMap<DeferredRuntimeBtreeIndexCacheKey, Arc<DeferredRuntimeBtreeIndexCacheEntry>>,
    insertion_order: VecDeque<DeferredRuntimeBtreeIndexCacheKey>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RowLocatorV2 {
    chunk_index: u32,
    byte_offset: u32,
    byte_len: u32,
    is_overlay: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DecodedRowLocator {
    V1(RowLocatorV1),
    V2(RowLocatorV2),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CachedPagedRowLocator {
    pointer: OverflowPointer,
    checksum: u32,
    locator: RowLocatorV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CachedPagedChunkSource {
    pointer: OverflowPointer,
    checksum: u32,
}

#[derive(Debug)]
enum DeferredPagedRowLocators {
    Dense {
        directory: DensePagedRowDirectory,
        chunks: Vec<CachedPagedChunkSource>,
    },
    Sparse(Int64Map<CachedPagedRowLocator>),
}

impl DeferredPagedRowLocators {
    fn get(&self, row_id: i64) -> Option<CachedPagedRowLocator> {
        match self {
            Self::Dense { directory, chunks } => {
                let position = directory.position_for_row_id(row_id)?;
                let chunk_index = directory.chunk_index_at(position)?;
                let source = chunks.get(chunk_index)?;
                Some(CachedPagedRowLocator {
                    pointer: source.pointer,
                    checksum: source.checksum,
                    locator: *directory.locators.get(position)?,
                })
            }
            Self::Sparse(locators) => locators.get(&row_id).copied(),
        }
    }

    fn min_row_id(&self) -> Option<i64> {
        match self {
            Self::Dense { directory, .. } => directory.first_row_id(),
            Self::Sparse(locators) => locators.keys().min().copied(),
        }
    }

    #[cfg(test)]
    fn is_dense(&self) -> bool {
        matches!(self, Self::Dense { .. })
    }

    #[cfg(test)]
    fn sparse_len(&self) -> usize {
        match self {
            Self::Dense { .. } => 0,
            Self::Sparse(locators) => locators.len(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct CachedPagedChunkPayloadKey {
    head_page_id: PageId,
    logical_len: u32,
    flags: u8,
    checksum: u32,
}

impl CachedPagedChunkPayloadKey {
    fn new(pointer: OverflowPointer, checksum: u32) -> Self {
        Self {
            head_page_id: pointer.head_page_id,
            logical_len: pointer.logical_len,
            flags: pointer.flags,
            checksum,
        }
    }
}

#[derive(Debug)]
pub(crate) struct DeferredPagedRowLocatorCache {
    manifest_pointer: OverflowPointer,
    manifest_checksum: u32,
    locators: DeferredPagedRowLocators,
    verified_payloads: HashMap<CachedPagedChunkPayloadKey, Arc<Vec<u8>>>,
}

impl DeferredPagedRowLocatorCache {
    fn matches_state(&self, state: PersistedTableState) -> bool {
        self.manifest_pointer == state.pointer && self.manifest_checksum == state.checksum
    }

    fn verified_payload(&self, pointer: OverflowPointer, checksum: u32) -> Option<&[u8]> {
        self.verified_payloads
            .get(&CachedPagedChunkPayloadKey::new(pointer, checksum))
            .map(|payload| payload.as_slice())
    }

    fn verified_payload_arc(
        &self,
        pointer: OverflowPointer,
        checksum: u32,
    ) -> Option<&Arc<Vec<u8>>> {
        self.verified_payloads
            .get(&CachedPagedChunkPayloadKey::new(pointer, checksum))
    }

    fn min_row_id(&self) -> Option<i64> {
        self.locators.min_row_id()
    }
}

#[derive(Debug)]
pub(crate) enum TableRowRef<'a> {
    Resident(&'a StoredRow),
    Decoded(StoredRow),
}

impl TableRowRef<'_> {
    pub(crate) fn row_id(&self) -> i64 {
        match self {
            Self::Resident(row) => row.row_id,
            Self::Decoded(row) => row.row_id,
        }
    }

    pub(crate) fn values(&self) -> &[Value] {
        match self {
            Self::Resident(row) => &row.values,
            Self::Decoded(row) => &row.values,
        }
    }
}

fn int64_column_value(value: Option<&Value>) -> Result<Option<i64>> {
    match value {
        Some(Value::Int64(value)) => Ok(Some(*value)),
        Some(Value::Null) => Ok(None),
        Some(other) => Err(DbError::sql(format!(
            "numeric aggregate does not support {other:?}"
        ))),
        None => Err(DbError::internal("row is shorter than table schema")),
    }
}

fn float64_column_value(value: Option<&Value>) -> Result<Option<f64>> {
    match value {
        Some(Value::Float64(value)) => Ok(Some(*value)),
        Some(Value::Null) => Ok(None),
        Some(other) => Err(DbError::sql(format!(
            "numeric aggregate does not support {other:?}"
        ))),
        None => Err(DbError::internal("row is shorter than table schema")),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TablePageEntry {
    row_id: i64,
    chunk_index: u32,
    is_overlay: bool,
    locator: RowLocatorV1,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TablePageDirectory {
    Dense(DensePagedRowDirectory),
    Sparse(Vec<TablePageEntry>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PreparedTablePageDirectoryAppend {
    Dense { add_chunk: bool },
    Sparse,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PreparedTablePageAppend {
    chunk_index: usize,
    entry_chunk_index: u32,
    is_overlay: bool,
    directory: PreparedTablePageDirectoryAppend,
}

impl TablePageDirectory {
    fn len(&self) -> usize {
        match self {
            Self::Dense(directory) => directory.len(),
            Self::Sparse(entries) => entries.len(),
        }
    }

    fn entry_at(&self, position: usize) -> Result<Option<TablePageEntry>> {
        match self {
            Self::Dense(directory) => directory.entry_at(position),
            Self::Sparse(entries) => Ok(entries.get(position).copied()),
        }
    }

    fn position_for_row_id(&self, row_id: i64) -> Option<usize> {
        match self {
            Self::Dense(directory) => directory.position_for_row_id(row_id),
            Self::Sparse(entries) => entries
                .binary_search_by_key(&row_id, |entry| entry.row_id)
                .ok()
                .or_else(|| entries.iter().position(|entry| entry.row_id == row_id)),
        }
    }

    fn entry_for_row_id(&self, row_id: i64) -> Result<Option<(usize, TablePageEntry)>> {
        let Some(position) = self.position_for_row_id(row_id) else {
            return Ok(None);
        };
        self.entry_at(position)
            .map(|entry| entry.map(|entry| (position, entry)))
    }

    fn row_ids_in_range(&self, low: i64, high: i64) -> Vec<i64> {
        if low > high {
            return Vec::new();
        }
        match self {
            Self::Dense(directory) => {
                let Some(first) = directory.first_row_id() else {
                    return Vec::new();
                };
                let Some(last) = directory.row_id_at(directory.len().saturating_sub(1)) else {
                    return Vec::new();
                };
                let start = low.max(first);
                let end = high.min(last);
                if start > end {
                    return Vec::new();
                }
                (start..=end).collect()
            }
            Self::Sparse(entries) => {
                let start = entries.partition_point(|entry| entry.row_id < low);
                let end = start + entries[start..].partition_point(|entry| entry.row_id <= high);
                entries[start..end]
                    .iter()
                    .map(|entry| entry.row_id)
                    .collect()
            }
        }
    }

    fn sparse_mut(&mut self) -> Result<&mut Vec<TablePageEntry>> {
        if let Self::Dense(directory) = self {
            *self = Self::Sparse(directory.to_sparse()?);
        }
        let Self::Sparse(entries) = self else {
            return Err(DbError::internal(
                "paged row directory did not convert to sparse storage",
            ));
        };
        Ok(entries)
    }

    fn try_prepare_append(
        &mut self,
        row_id: i64,
        chunk_index: usize,
        is_overlay: bool,
    ) -> Result<PreparedTablePageDirectoryAppend> {
        if let Self::Dense(directory) = self {
            if let Some(add_chunk) =
                directory.try_prepare_append(row_id, chunk_index, is_overlay)?
            {
                return Ok(PreparedTablePageDirectoryAppend::Dense { add_chunk });
            }
        }
        let entries = self.sparse_mut()?;
        if entries.len() == entries.capacity() {
            try_reserve_paged_directory_amortized(entries, 1, "sparse paged row entries")?;
        }
        Ok(PreparedTablePageDirectoryAppend::Sparse)
    }

    fn iter(&self) -> TablePageEntryIter<'_> {
        match self {
            Self::Dense(directory) => TablePageEntryIter::Dense {
                directory,
                position: 0,
            },
            Self::Sparse(entries) => TablePageEntryIter::Sparse(entries.iter()),
        }
    }

    fn approximate_heap_bytes(&self) -> usize {
        match self {
            Self::Dense(directory) => directory.approximate_heap_bytes(),
            Self::Sparse(entries) => entries
                .capacity()
                .saturating_mul(std::mem::size_of::<TablePageEntry>()),
        }
    }

    #[cfg(test)]
    fn is_dense(&self) -> bool {
        matches!(self, Self::Dense(_))
    }
}

enum TablePageEntryIter<'a> {
    Dense {
        directory: &'a DensePagedRowDirectory,
        position: usize,
    },
    Sparse(std::slice::Iter<'a, TablePageEntry>),
}

impl Iterator for TablePageEntryIter<'_> {
    type Item = Result<TablePageEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense {
                directory,
                position,
            } => {
                if *position >= directory.len() {
                    return None;
                }
                let entry = directory.entry_at(*position);
                *position += 1;
                Some(entry.and_then(|entry| {
                    entry.ok_or_else(|| {
                        DbError::corruption(
                            "dense paged row directory ended before its locator count",
                        )
                    })
                }))
            }
            Self::Sparse(entries) => entries.next().copied().map(Ok),
        }
    }
}

fn try_reserve_paged_directory<T>(
    values: &mut Vec<T>,
    additional: usize,
    allocation_name: &str,
) -> Result<()> {
    #[cfg(test)]
    if should_force_paged_row_directory_reservation_failure(allocation_name) {
        return Err(DbError::internal(format!(
            "injected paged row directory reservation failure for {allocation_name}"
        )));
    }
    values.try_reserve_exact(additional).map_err(|error| {
        DbError::internal(format!(
            "failed to reserve {additional} entries for {allocation_name}: {error}"
        ))
    })
}

fn try_reserve_paged_directory_amortized<T>(
    values: &mut Vec<T>,
    additional: usize,
    allocation_name: &str,
) -> Result<()> {
    #[cfg(test)]
    if should_force_paged_row_directory_reservation_failure(allocation_name) {
        return Err(DbError::internal(format!(
            "injected paged row directory reservation failure for {allocation_name}"
        )));
    }
    values.try_reserve(additional).map_err(|error| {
        DbError::internal(format!(
            "failed to reserve {additional} entries for {allocation_name}: {error}"
        ))
    })
}

#[cfg(test)]
thread_local! {
    static FORCE_NEXT_PAGED_ROW_DIRECTORY_RESERVATION_FAILURE: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
    static PAGED_ROW_APPEND_PLAN_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn should_force_paged_row_directory_reservation_failure(allocation_name: &str) -> bool {
    FORCE_NEXT_PAGED_ROW_DIRECTORY_RESERVATION_FAILURE.with(|force| {
        if force.get().is_some_and(|target| target == allocation_name) {
            force.set(None);
            true
        } else {
            false
        }
    })
}

#[cfg(test)]
pub(crate) fn force_next_paged_row_directory_reservation_failure(allocation_name: &'static str) {
    FORCE_NEXT_PAGED_ROW_DIRECTORY_RESERVATION_FAILURE
        .with(|force| force.set(Some(allocation_name)));
}

#[cfg(test)]
fn reset_paged_row_append_plan_count() {
    PAGED_ROW_APPEND_PLAN_COUNT.with(|count| count.set(0));
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TablePageManifestChunk {
    pub(crate) pointer: OverflowPointer,
    pub(crate) checksum: u32,
    pub(crate) row_count: usize,
    pub(crate) payload: Arc<Vec<u8>>,
    pub(crate) tombstoned_row_ids: Arc<BTreeSet<i64>>,
    pub(crate) overlay_pointer: Option<OverflowPointer>,
    pub(crate) overlay_checksum: Option<u32>,
    pub(crate) overlay_payload: Option<Arc<Vec<u8>>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EncodedPagedTableChunk {
    payload: Vec<u8>,
    checksum: u32,
    row_count: usize,
}

fn try_build_dense_paged_row_directory(
    chunks: &[TablePageManifestChunk],
) -> Result<Option<DensePagedRowDirectory>> {
    if chunks
        .iter()
        .any(|chunk| !table_page_manifest_chunk_is_plain(chunk))
    {
        return Ok(None);
    }

    let expected_rows = chunks.iter().try_fold(0usize, |total, chunk| {
        total
            .checked_add(chunk.row_count)
            .ok_or_else(|| DbError::constraint("paged table row count overflow"))
    })?;
    let mut directory = DensePagedRowDirectory::empty(0)?;
    try_reserve_paged_directory(&mut directory.locators, expected_rows, "dense row locators")?;
    try_reserve_paged_directory(
        &mut directory.chunk_ends,
        chunks.len(),
        "dense chunk ranges",
    )?;

    for (chunk_index, chunk) in chunks.iter().enumerate() {
        u32::try_from(chunk_index)
            .map_err(|_| DbError::constraint("table chunk index exceeds u32"))?;
        if chunk.payload.is_empty() {
            if chunk.row_count != 0 {
                return Ok(None);
            }
            directory.chunk_ends.push(directory.locators.len());
            continue;
        }

        let mut cursor = Cursor::new(chunk.payload.as_slice());
        let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
        if magic != TABLE_PAYLOAD_MAGIC {
            return Err(DbError::corruption("table payload magic is invalid"));
        }
        let physical_row_count = cursor.read_u32()? as usize;
        if physical_row_count != chunk.row_count {
            return Ok(None);
        }
        for _ in 0..physical_row_count {
            let row_id = cursor.read_i64()?;
            let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
            let row_bytes_offset = cursor.offset;
            if is_tombstone {
                cursor.read_slice(row_bytes_len)?;
                return Ok(None);
            }
            #[cfg(debug_assertions)]
            {
                let row_bytes = cursor.read_slice(row_bytes_len)?;
                Row::decode(row_bytes)?;
            }
            #[cfg(not(debug_assertions))]
            cursor.read_slice(row_bytes_len)?;

            if directory.locators.is_empty() {
                directory.start_row_id = row_id;
            } else {
                let expected_row_id = i128::from(directory.start_row_id)
                    + i128::try_from(directory.locators.len()).map_err(|_| {
                        DbError::constraint("dense paged row position exceeds i128")
                    })?;
                if i128::from(row_id) != expected_row_id {
                    return Ok(None);
                }
            }
            directory.locators.push(RowLocatorV1 {
                byte_offset: u32::try_from(row_bytes_offset)
                    .map_err(|_| DbError::constraint("row locator offset exceeds u32"))?,
                byte_len: u32::try_from(row_bytes_len)
                    .map_err(|_| DbError::constraint("row locator length exceeds u32"))?,
            });
        }
        directory.chunk_ends.push(directory.locators.len());
    }

    Ok(Some(directory))
}

fn table_page_entries_for_chunk(
    chunk_index: usize,
    chunk: &TablePageManifestChunk,
) -> Result<Vec<TablePageEntry>> {
    let mut overlay_ids = if chunk
        .overlay_payload
        .as_ref()
        .is_some_and(|payload| !payload.is_empty())
    {
        Some(BTreeSet::new())
    } else {
        None
    };
    if let Some(overlay_payload) = &chunk.overlay_payload {
        if !overlay_payload.is_empty() {
            let mut cursor = Cursor::new(overlay_payload.as_slice());
            let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
            if magic != TABLE_PAYLOAD_MAGIC {
                return Err(DbError::corruption("table payload magic is invalid"));
            }
            let row_count = cursor.read_u32()? as usize;
            for _ in 0..row_count {
                let row_id = cursor.read_i64()?;
                let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
                cursor.read_slice(row_bytes_len)?;
                if is_tombstone {
                    continue;
                }
                if let Some(overlay_ids) = overlay_ids.as_mut() {
                    overlay_ids.insert(row_id);
                }
            }
        }
    }

    let mut entries = Vec::new();
    let has_tombstones = !chunk.tombstoned_row_ids.is_empty();
    let overlay_ids = overlay_ids.as_ref();
    if !chunk.payload.is_empty() {
        let mut cursor = Cursor::new(chunk.payload.as_slice());
        let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
        if magic != TABLE_PAYLOAD_MAGIC {
            return Err(DbError::corruption("table payload magic is invalid"));
        }
        let row_count = cursor.read_u32()? as usize;
        for _ in 0..row_count {
            let row_id = cursor.read_i64()?;
            let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
            let row_bytes_offset = cursor.offset;
            let _row_bytes = cursor.read_slice(row_bytes_len)?;
            if is_tombstone {
                continue;
            }
            if has_tombstones && chunk.tombstoned_row_ids.contains(&row_id) {
                continue;
            }
            if let Some(ids) = overlay_ids {
                if ids.contains(&row_id) {
                    continue;
                }
            }
            entries.push(TablePageEntry {
                row_id,
                chunk_index: u32::try_from(chunk_index)
                    .map_err(|_| DbError::constraint("table chunk index exceeds u32"))?,
                is_overlay: false,
                locator: RowLocatorV1 {
                    byte_offset: u32::try_from(row_bytes_offset)
                        .map_err(|_| DbError::constraint("row locator offset exceeds u32"))?,
                    byte_len: u32::try_from(row_bytes_len)
                        .map_err(|_| DbError::constraint("row locator length exceeds u32"))?,
                },
            });
        }
    }

    if let Some(overlay_payload) = &chunk.overlay_payload {
        if !overlay_payload.is_empty() {
            let mut cursor = Cursor::new(overlay_payload.as_slice());
            let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
            if magic != TABLE_PAYLOAD_MAGIC {
                return Err(DbError::corruption("table payload magic is invalid"));
            }
            let row_count = cursor.read_u32()? as usize;
            for _ in 0..row_count {
                let row_id = cursor.read_i64()?;
                let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
                let row_bytes_offset = cursor.offset;
                let _row_bytes = cursor.read_slice(row_bytes_len)?;
                if is_tombstone {
                    continue;
                }
                entries.push(TablePageEntry {
                    row_id,
                    chunk_index: u32::try_from(chunk_index)
                        .map_err(|_| DbError::constraint("table chunk index exceeds u32"))?,
                    is_overlay: true,
                    locator: RowLocatorV1 {
                        byte_offset: u32::try_from(row_bytes_offset)
                            .map_err(|_| DbError::constraint("row locator offset exceeds u32"))?,
                        byte_len: u32::try_from(row_bytes_len)
                            .map_err(|_| DbError::constraint("row locator length exceeds u32"))?,
                    },
                });
            }
        }
    }

    Ok(entries)
}

fn append_encoded_table_payload_row(
    payload: &mut Vec<u8>,
    row_id: i64,
    encoded_values: &[u8],
) -> Result<RowLocatorV1> {
    let physical_row_count = read_table_payload_row_count_from_bytes(payload)?;
    encode_i64(payload, row_id);
    encode_u32(
        payload,
        u32::try_from(encoded_values.len())
            .map_err(|_| DbError::constraint("row payload length exceeds u32"))?,
    );
    let row_bytes_offset = payload.len();
    payload.extend_from_slice(encoded_values);
    let next_physical_row_count = physical_row_count
        .checked_add(1)
        .ok_or_else(|| DbError::constraint("paged table chunk row count overflow"))?;
    payload[TABLE_PAYLOAD_MAGIC.len()..TABLE_PAYLOAD_MAGIC.len() + 4].copy_from_slice(
        &u32::try_from(next_physical_row_count)
            .map_err(|_| DbError::constraint("paged table chunk row count exceeds u32"))?
            .to_le_bytes(),
    );
    Ok(RowLocatorV1 {
        byte_offset: u32::try_from(row_bytes_offset)
            .map_err(|_| DbError::constraint("row locator offset exceeds u32"))?,
        byte_len: u32::try_from(encoded_values.len())
            .map_err(|_| DbError::constraint("row locator length exceeds u32"))?,
    })
}

fn try_apply_single_paged_row_update_to_manifest(
    manifest: &TablePageManifest,
    row_id: i64,
    next_values: &[Value],
) -> Result<Option<TablePageManifest>> {
    let Some((_, entry)) = manifest.rows.entry_for_row_id(row_id)? else {
        return Ok(None);
    };
    if entry.is_overlay {
        return Ok(None);
    }
    let chunk_index = usize::try_from(entry.chunk_index)
        .map_err(|_| DbError::corruption("paged table chunk index exceeded chunk list length"))?;
    let mut updated_manifest = manifest.clone();
    Arc::make_mut(&mut updated_manifest.rows).sparse_mut()?;
    let chunks = Arc::make_mut(&mut updated_manifest.chunks);
    let tombstoned_row_ids = Arc::make_mut(&mut updated_manifest.tombstoned_row_ids);
    let chunk = chunks
        .get_mut(chunk_index)
        .ok_or_else(|| DbError::corruption("paged table chunk index exceeded chunk list length"))?;
    if chunk.tombstoned_row_ids.contains(&row_id) {
        return Ok(None);
    }

    let overlay_payload = chunk.overlay_payload.get_or_insert_with(|| {
        let mut payload = Vec::with_capacity(TABLE_PAYLOAD_MAGIC.len() + 4 + 128);
        payload.extend_from_slice(TABLE_PAYLOAD_MAGIC);
        payload.extend_from_slice(&0_u32.to_le_bytes());
        Arc::new(payload)
    });
    let mut encoded_values = Vec::with_capacity(128);
    Row::encode_values_into(next_values, &mut encoded_values)?;
    append_encoded_table_payload_row(Arc::make_mut(overlay_payload), row_id, &encoded_values)?;
    Arc::make_mut(&mut chunk.tombstoned_row_ids).insert(row_id);
    chunk.overlay_pointer = None;
    chunk.overlay_checksum = None;
    tombstoned_row_ids.insert(row_id);

    Ok(Some(updated_manifest))
}

impl From<TableData> for TableRowSource {
    fn from(data: TableData) -> Self {
        Self::Resident(Arc::new(data))
    }
}

impl From<TablePageManifest> for TableRowSource {
    fn from(manifest: TablePageManifest) -> Self {
        Self::Paged(Arc::new(manifest))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PersistedTableState {
    pub(crate) pointer: OverflowPointer,
    pub(crate) checksum: u32,
    pub(crate) row_count: usize,
    pub(crate) tail: OverflowTailInfo,
    pub(crate) pk_index_root: Option<PageId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PersistedTableChunkState {
    pub(crate) pointer: OverflowPointer,
    pub(crate) checksum: u32,
    pub(crate) row_count: usize,
    pub(crate) tombstoned_row_ids: Vec<i64>,
    pub(crate) overlay_pointer: Option<OverflowPointer>,
    pub(crate) overlay_checksum: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PersistedPagedTableManifest {
    pub(crate) chunks: Vec<PersistedTableChunkState>,
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
            pk_index_root: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeBtreeKey {
    Encoded(RuntimeEncodedKey),
    Int64(i64),
    Uuid([u8; 16]),
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

impl From<Int64Map<i64>> for UniqueInt64Keys {
    fn from(keys: Int64Map<i64>) -> Self {
        Self::Sparse(keys)
    }
}

/// Row IDs stored beneath one key in a non-unique typed `INT64` index.
///
/// Foreign-key-like indexes commonly receive monotonically increasing row IDs
/// grouped by key. Representing those postings as an inline singleton or a
/// contiguous range avoids one heap allocation per distinct key. Irregular
/// insertion order falls back to the same `Vec` semantics used previously.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeInt64RowIds {
    One(i64),
    Contiguous { start: i64, len: usize },
    Many(Vec<i64>),
}

impl RuntimeInt64RowIds {
    fn one(row_id: i64) -> Self {
        Self::One(row_id)
    }

    fn len(&self) -> usize {
        match self {
            Self::One(_) => 1,
            Self::Contiguous { len, .. } => *len,
            Self::Many(row_ids) => row_ids.len(),
        }
    }

    fn is_empty(&self) -> bool {
        matches!(self, Self::Many(row_ids) if row_ids.is_empty())
    }

    fn value_at(start: i64, offset: usize) -> Option<i64> {
        let offset = i128::try_from(offset).ok()?;
        i64::try_from(i128::from(start) + offset).ok()
    }

    fn from_vec(row_ids: Vec<i64>) -> Self {
        match row_ids.as_slice() {
            [] => Self::Many(row_ids),
            [row_id] => Self::One(*row_id),
            [start, rest @ ..]
                if rest.iter().copied().enumerate().all(|(offset, row_id)| {
                    Self::value_at(*start, offset.saturating_add(1)) == Some(row_id)
                }) =>
            {
                Self::Contiguous {
                    start: *start,
                    len: row_ids.len(),
                }
            }
            _ => Self::Many(row_ids),
        }
    }

    fn push(&mut self, row_id: i64) {
        match self {
            Self::One(first_row_id) if first_row_id.checked_add(1) == Some(row_id) => {
                *self = Self::Contiguous {
                    start: *first_row_id,
                    len: 2,
                };
            }
            Self::One(first_row_id) => {
                let mut row_ids = Vec::with_capacity(4);
                row_ids.push(*first_row_id);
                row_ids.push(row_id);
                *self = Self::Many(row_ids);
            }
            Self::Contiguous { start, len } if Self::value_at(*start, *len) == Some(row_id) => {
                *len = len.saturating_add(1);
            }
            Self::Contiguous { start, len } => {
                let start = *start;
                let len = *len;
                let mut row_ids = Vec::with_capacity(len.saturating_add(1));
                for offset in 0..len {
                    if let Some(existing) = Self::value_at(start, offset) {
                        row_ids.push(existing);
                    }
                }
                row_ids.push(row_id);
                *self = Self::Many(row_ids);
            }
            Self::Many(row_ids) => row_ids.push(row_id),
        }
    }

    fn contains(&self, row_id: &i64) -> bool {
        match self {
            Self::One(existing) => existing == row_id,
            Self::Contiguous { start, len } => {
                UniqueInt64Keys::dense_contains(*start, *len, *row_id)
            }
            Self::Many(row_ids) => row_ids.contains(row_id),
        }
    }

    fn iter(&self) -> RuntimeInt64RowIdsIter<'_> {
        match self {
            Self::One(row_id) => RuntimeInt64RowIdsIter::One(Some(*row_id)),
            Self::Contiguous { start, len } => RuntimeInt64RowIdsIter::Contiguous {
                start: *start,
                offset: 0,
                len: *len,
            },
            Self::Many(row_ids) => RuntimeInt64RowIdsIter::Many(row_ids.iter()),
        }
    }

    fn to_vec(&self) -> Vec<i64> {
        self.iter().collect()
    }

    fn retain(&mut self, mut retain: impl FnMut(&i64) -> bool) {
        match self {
            Self::One(row_id) => {
                if !retain(row_id) {
                    *self = Self::Many(Vec::new());
                }
            }
            Self::Contiguous { .. } => {
                let retained = self
                    .iter()
                    .filter(|row_id| retain(row_id))
                    .collect::<Vec<_>>();
                *self = Self::from_vec(retained);
            }
            Self::Many(row_ids) => row_ids.retain(retain),
        }
    }

    fn shrink_to_fit(&mut self) -> usize {
        let Self::Many(row_ids) = self else {
            return 0;
        };
        let old_capacity = row_ids.capacity();
        let compact = Self::from_vec(std::mem::take(row_ids));
        if matches!(compact, Self::Many(_)) {
            *self = compact;
            let Self::Many(row_ids) = self else {
                return 0;
            };
            row_ids.shrink_to_fit();
            return old_capacity
                .saturating_sub(row_ids.capacity())
                .saturating_mul(std::mem::size_of::<i64>());
        }
        *self = compact;
        old_capacity.saturating_mul(std::mem::size_of::<i64>())
    }
}

pub(crate) fn contiguous_row_ids(start: i64, len: usize) -> RuntimeInt64RowIdsIter<'static> {
    RuntimeInt64RowIdsIter::Contiguous {
        start,
        offset: 0,
        len,
    }
}

impl<'a> IntoIterator for &'a RuntimeInt64RowIds {
    type Item = i64;
    type IntoIter = RuntimeInt64RowIdsIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl From<Int64Map<Vec<i64>>> for NonUniqueInt64Keys {
    fn from(keys: Int64Map<Vec<i64>>) -> Self {
        let mut postings =
            Int64Map::with_capacity_and_hasher(keys.capacity(), Int64HashBuilder::default());
        for (key, row_ids) in keys {
            postings.insert(key, RuntimeInt64RowIds::from_vec(row_ids));
        }
        Self::Sparse(postings)
    }
}

// Keep every posting object no larger than the Vec it replaces on all
// supported pointer widths, including wasm32. This intentionally lives in
// production code so cross-target checks enforce the layout invariant.
const _: () =
    assert!(std::mem::size_of::<RuntimeEncodedRowIds>() == std::mem::size_of::<Vec<i64>>());

impl<'a> IntoIterator for &'a RuntimeEncodedRowIds {
    type Item = &'a i64;
    type IntoIter = std::slice::Iter<'a, i64>;

    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeRowIdSet<'a> {
    Empty,
    Single(i64),
    Contiguous { start: i64, len: usize },
    Many(&'a [i64]),
    Owned(Vec<i64>),
}

impl RuntimeRowIdSet<'_> {
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Single(_) => 1,
            Self::Contiguous { len, .. } => *len,
            Self::Many(values) => values.len(),
            Self::Owned(values) => values.len(),
        }
    }

    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    pub(crate) fn for_each(&self, mut f: impl FnMut(i64)) {
        match self {
            Self::Empty => {}
            Self::Single(row_id) => f(*row_id),
            Self::Contiguous { start, len } => {
                for offset in 0..*len {
                    if let Some(row_id) = RuntimeInt64RowIds::value_at(*start, offset) {
                        f(row_id);
                    }
                }
            }
            Self::Many(values) => {
                for row_id in *values {
                    f(*row_id);
                }
            }
            Self::Owned(values) => {
                for row_id in values {
                    f(*row_id);
                }
            }
        }
    }

    fn visit_until(self, mut visitor: impl FnMut(i64) -> Result<bool>) -> Result<bool> {
        match self {
            Self::Empty => Ok(false),
            Self::Single(row_id) => visitor(row_id),
            Self::Contiguous { start, len } => {
                for offset in 0..len {
                    let Some(row_id) = RuntimeInt64RowIds::value_at(start, offset) else {
                        break;
                    };
                    if visitor(row_id)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Self::Many(values) => {
                for row_id in values {
                    if visitor(*row_id)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Self::Owned(values) => {
                for row_id in values {
                    if visitor(row_id)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }
}

fn visible_row_id_set_count(
    row_source: VisibleTableRowSource<'_>,
    row_ids: RuntimeRowIdSet<'_>,
) -> Result<usize> {
    if !row_source.has_tombstoned_rows() {
        return Ok(row_ids.len());
    }
    let mut count = 0usize;
    let mut error = None;
    row_ids.for_each(|row_id| {
        if error.is_some() {
            return;
        }
        match row_source.row_by_id(row_id) {
            Ok(Some(_)) => count += 1,
            Ok(None) => {}
            Err(err) => error = Some(err),
        }
    });
    if let Some(error) = error {
        return Err(error);
    }
    Ok(count)
}

#[derive(Clone, Debug)]
pub(crate) struct RuntimeCoveringPayloads {
    columns: Vec<String>,
    rows: BTreeMap<i64, Vec<Value>>,
}

impl RuntimeCoveringPayloads {
    fn new(columns: Vec<String>) -> Self {
        Self {
            columns,
            rows: BTreeMap::new(),
        }
    }

    fn column_position(&self, column_name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|candidate| identifiers_equal(candidate, column_name))
    }

    fn insert_row_values(&mut self, row_id: i64, values: Vec<Value>) {
        self.rows.insert(row_id, values);
    }

    fn remove_row_id(&mut self, row_id: i64) {
        self.rows.remove(&row_id);
    }

    fn shrink_to_fit(&mut self) -> usize {
        let mut freed = 0usize;
        let old_columns_capacity = self.columns.capacity();
        self.columns.shrink_to_fit();
        freed = freed.saturating_add(
            old_columns_capacity
                .saturating_sub(self.columns.capacity())
                .saturating_mul(std::mem::size_of::<String>()),
        );
        for values in self.rows.values_mut() {
            let old_capacity = values.capacity();
            values.shrink_to_fit();
            freed = freed.saturating_add(
                old_capacity
                    .saturating_sub(values.capacity())
                    .saturating_mul(std::mem::size_of::<Value>()),
            );
        }
        freed
    }

    fn project_row(&self, row_id: i64, offsets: &[usize]) -> Option<QueryRow> {
        let values = self.rows.get(&row_id)?;
        let mut projected = Vec::with_capacity(offsets.len());
        for offset in offsets {
            projected.push(values.get(*offset)?.clone());
        }
        Some(QueryRow::new(projected))
    }
}

#[derive(Clone, Debug)]
pub(crate) enum RuntimeIndex {
    Btree {
        keys: RuntimeBtreeKeys,
        covering: Option<RuntimeCoveringPayloads>,
    },
    Trigram {
        index: TrigramIndex,
    },
    Spatial {
        index: SpatialRuntimeIndex,
    },
    FullText {
        index: FullTextIndex,
    },
}

impl RuntimeIndex {
    fn shrink_to_fit_if_unique(&mut self) -> usize {
        match self {
            Self::Btree { keys, covering } => keys.shrink_to_fit_if_unique().saturating_add(
                covering
                    .as_mut()
                    .map_or(0, RuntimeCoveringPayloads::shrink_to_fit),
            ),
            Self::Trigram { .. } | Self::Spatial { .. } | Self::FullText { .. } => 0,
        }
    }
}

fn runtime_index_entry_count(index: &RuntimeIndex) -> usize {
    match index {
        RuntimeIndex::Btree { keys, .. } => keys.total_row_id_count(),
        RuntimeIndex::Trigram { index } => index.entry_count(),
        RuntimeIndex::Spatial { index } => index.len(),
        RuntimeIndex::FullText { index } => index.entry_count(),
    }
}

#[derive(Debug)]
pub(super) enum PendingIndexInsert {
    Btree {
        name: String,
        key: RuntimeBtreeKey,
        row_id: i64,
        covering_values: Option<Vec<Value>>,
    },
    Trigram {
        name: String,
        row_id: u64,
        text: String,
    },
    Spatial {
        name: String,
        row_id: i64,
        value: SpatialValue,
    },
    FullText {
        name: String,
        row_id: u64,
        fields: Vec<Option<String>>,
    },
}

#[derive(Debug)]
pub(crate) struct EngineRuntime {
    pub(crate) catalog: Arc<CatalogState>,
    pub(crate) tables: Arc<BTreeMap<String, TableRowSource>>,
    pub(crate) temp_tables: Arc<BTreeMap<String, TableSchema>>,
    pub(crate) temp_table_data: Arc<BTreeMap<String, Arc<TableData>>>,
    pub(crate) temp_views: Arc<BTreeMap<String, ViewSchema>>,
    pub(crate) temp_indexes: Arc<BTreeMap<String, IndexSchema>>,
    pub(crate) temp_schema_cookie: u32,
    pub(crate) indexes: Arc<BTreeMap<String, Arc<RuntimeIndex>>>,
    pub(crate) persisted_tables: Arc<BTreeMap<String, PersistedTableState>>,
    deferred_paged_row_locator_caches: Arc<BTreeMap<String, Arc<DeferredPagedRowLocatorCache>>>,
    view_query_cache: Arc<Mutex<ViewQueryCache>>,
    resident_tombstone_locators: Arc<BTreeMap<String, Arc<Int64Map<u32>>>>,
    /// Tables whose row data has not yet been loaded from storage.
    /// Populated during `decode_manifest_payload` and cleared by
    /// `load_deferred_tables`.
    pub(crate) deferred_tables: Arc<BTreeSet<String>>,
    pub(crate) dirty_tables: Arc<BTreeSet<String>>,
    pub(crate) paged_mutations: BTreeMap<String, PagedMutationDelta>,
    /// Per-session cache; capped at `cached_payloads_max_entries`. Eviction is LRU.
    payload_cache: Arc<Mutex<PayloadCache>>,
    root_state: Option<RootHeader>,
    pub(crate) index_state_epoch: u64,
    pub(crate) paged_row_storage: bool,
    manifest_template: Option<ManifestTemplate>,
    overflow_chain_caches: BTreeMap<String, OverflowChainCache>,
    manifest_chain_cache: Option<OverflowChainCache>,
    sync_capture_active: bool,
    pub(crate) sync_mutations: Vec<crate::sync::SyncMutation>,
    reactive_capture_active: bool,
    pub(crate) reactive_mutations: Vec<crate::reactive::RowChange>,
    pub(crate) extension_trust_anchors: Arc<Vec<crate::extensions::ExtensionTrustAnchor>>,
    pub(crate) extension_unsigned_development_mode: bool,
    pub(crate) audit_context: Arc<Mutex<crate::security::AuditContext>>,
    pub(crate) tracing: Option<Arc<crate::tracing::RuntimeTraceState>>,
    fts_eval_context: Arc<Mutex<FtsEvalContext>>,
}

#[derive(Clone, Debug, Default)]
struct FtsEvalContext {
    scores: BTreeMap<(String, i64), f64>,
}

#[derive(Debug)]
pub(crate) struct PayloadCache {
    entries: HashMap<String, PayloadCacheEntry>,
    next_touch_gen: u64,
    max_entries: usize,
}

#[derive(Debug)]
struct PayloadCacheEntry {
    payload: Arc<Vec<u8>>,
    last_touch_gen: u64,
}

impl PayloadCache {
    fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            next_touch_gen: 0,
            max_entries,
        }
    }

    fn set_max_entries(&mut self, max_entries: usize) {
        self.max_entries = max_entries;
        self.evict_excess();
    }

    fn get(&mut self, table_name: &str) -> Option<Arc<Vec<u8>>> {
        let payload = Arc::clone(&self.entries.get(table_name)?.payload);
        self.touch(table_name);
        Some(payload)
    }

    fn take(&mut self, table_name: &str) -> Option<Arc<Vec<u8>>> {
        self.entries.remove(table_name).map(|entry| entry.payload)
    }

    fn insert(&mut self, table_name: String, payload: Arc<Vec<u8>>) {
        if self.max_entries == 0 {
            return;
        }
        let last_touch_gen = self.advance_touch_gen();
        self.entries.insert(
            table_name,
            PayloadCacheEntry {
                payload,
                last_touch_gen,
            },
        );
        self.evict_excess();
    }

    fn remove(&mut self, table_name: &str) {
        self.entries.remove(table_name);
    }

    fn touch(&mut self, table_name: &str) {
        let last_touch_gen = self.advance_touch_gen();
        if let Some(entry) = self.entries.get_mut(table_name) {
            entry.last_touch_gen = last_touch_gen;
        }
    }

    fn evict_excess(&mut self) {
        while self.entries.len() > self.max_entries {
            if let Some(evicted) = self.oldest_key().cloned() {
                self.entries.remove(&evicted);
            } else {
                break;
            }
        }
    }

    fn advance_touch_gen(&mut self) -> u64 {
        let touch_gen = self.next_touch_gen;
        self.next_touch_gen = self.next_touch_gen.wrapping_add(1);
        touch_gen
    }

    fn oldest_key(&self) -> Option<&String> {
        self.entries
            .iter()
            .min_by(|(_, left), (_, right)| {
                if touch_gen_older(left.last_touch_gen, right.last_touch_gen) {
                    std::cmp::Ordering::Less
                } else if touch_gen_older(right.last_touch_gen, left.last_touch_gen) {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .map(|(key, _)| key)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    fn contains_key(&self, table_name: &str) -> bool {
        self.entries.contains_key(table_name)
    }

    #[cfg(test)]
    fn last_touch_gen(&self, table_name: &str) -> Option<u64> {
        self.entries
            .get(table_name)
            .map(|entry| entry.last_touch_gen)
    }
}

fn touch_gen_older(left: u64, right: u64) -> bool {
    left != right && left.wrapping_sub(right) > (u64::MAX / 2)
}

#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) struct PagedMutationDelta {
    pub(crate) append_count: usize,
    pub(crate) updated_rows: BTreeMap<i64, Vec<Value>>,
    pub(crate) deleted_rows: BTreeSet<i64>,
    pub(crate) original_rows: BTreeMap<i64, Vec<Value>>,
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
            catalog: Arc::clone(&self.catalog),
            tables: Arc::clone(&self.tables),
            temp_tables: Arc::clone(&self.temp_tables),
            temp_table_data: Arc::clone(&self.temp_table_data),
            temp_views: Arc::clone(&self.temp_views),
            temp_indexes: Arc::clone(&self.temp_indexes),
            temp_schema_cookie: self.temp_schema_cookie,
            indexes: Arc::clone(&self.indexes),
            persisted_tables: Arc::clone(&self.persisted_tables),
            deferred_paged_row_locator_caches: Arc::clone(&self.deferred_paged_row_locator_caches),
            view_query_cache: Arc::clone(&self.view_query_cache),
            resident_tombstone_locators: Arc::clone(&self.resident_tombstone_locators),
            deferred_tables: Arc::clone(&self.deferred_tables),
            // Preserve dirty state so that multi-statement transactions
            // (clone-and-replace) do not lose modifications from earlier
            // statements.  `persist_to_db` clears dirty state after a
            // successful persist, so autocommit paths are unaffected.
            dirty_tables: Arc::clone(&self.dirty_tables),
            // Escalate paged_mutations to full dirty on clone: the
            // subsequent generic execution path may modify the same rows
            // in ways that invalidate the splice assumption.
            paged_mutations: BTreeMap::new(),
            payload_cache: Arc::clone(&self.payload_cache),
            root_state: self.root_state,
            index_state_epoch: self.index_state_epoch,
            paged_row_storage: self.paged_row_storage,
            // These caches are keyed by persisted overflow pointers and remain
            // valid across clone-and-replace write transactions until the
            // corresponding table is rewritten.
            manifest_template: None,
            overflow_chain_caches: self.overflow_chain_caches.clone(),
            manifest_chain_cache: None,
            sync_capture_active: self.sync_capture_active,
            sync_mutations: self.sync_mutations.clone(),
            reactive_capture_active: self.reactive_capture_active,
            reactive_mutations: self.reactive_mutations.clone(),
            extension_trust_anchors: Arc::clone(&self.extension_trust_anchors),
            extension_unsigned_development_mode: self.extension_unsigned_development_mode,
            audit_context: Arc::clone(&self.audit_context),
            tracing: self.tracing.as_ref().map(Arc::clone),
            fts_eval_context: Arc::clone(&self.fts_eval_context),
        }
    }
}

pub(crate) struct SimpleRowIdProjectionRequest<'a> {
    pub(crate) table_name: &'a str,
    pub(crate) projection_columns: &'a [&'a str],
    pub(crate) filter_column: &'a str,
    pub(crate) lookup_row_id: i64,
    pub(crate) pager: &'a PagerHandle,
    pub(crate) wal: &'a WalHandle,
    pub(crate) snapshot_lsn: u64,
    pub(crate) use_persistent_pk_index: bool,
}

pub(crate) struct ResolvedSimpleRowIdProjectionRequest<'a> {
    pub(crate) table_name: &'a str,
    pub(crate) projection_indexes: &'a [usize],
    pub(crate) column_names: Arc<[String]>,
    pub(crate) lookup_row_id: i64,
    pub(crate) pager: &'a PagerHandle,
    pub(crate) wal: &'a WalHandle,
    pub(crate) snapshot_lsn: u64,
    pub(crate) use_persistent_pk_index: bool,
}

pub(crate) struct ResolvedSimpleOrderedRowIdProjectionRequest<'a> {
    pub(crate) table_name: &'a str,
    pub(crate) order_column: &'a str,
    pub(crate) projection_indexes: &'a [usize],
    pub(crate) column_names: Arc<[String]>,
    pub(crate) limit: Option<usize>,
    pub(crate) offset: usize,
    pub(crate) descending: bool,
}

pub(crate) struct ResolvedSimpleRowIdRangeProjectionRequest<'a> {
    pub(crate) table_name: &'a str,
    pub(crate) projection_indexes: &'a [usize],
    pub(crate) column_names: Arc<[String]>,
    pub(crate) filter_column: &'a str,
    pub(crate) lower_bound: Option<SimpleRangeBoundValue>,
    pub(crate) upper_bound: Option<SimpleRangeBoundValue>,
    pub(crate) limit: Option<usize>,
    pub(crate) pager: &'a PagerHandle,
    pub(crate) wal: &'a WalHandle,
    pub(crate) snapshot_lsn: u64,
    pub(crate) use_persistent_pk_index: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SimpleJoinProjectionSide {
    Left,
    Right,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedSimpleJoinProjection {
    pub(crate) side: SimpleJoinProjectionSide,
    pub(crate) index: usize,
}

pub(crate) struct ResolvedSimpleRowIdJoinProjectionRequest<'a> {
    pub(crate) left_table_name: &'a str,
    pub(crate) right_table_name: &'a str,
    pub(crate) left_projection_indexes: &'a [usize],
    pub(crate) right_projection_indexes: &'a [usize],
    pub(crate) projections: &'a [ResolvedSimpleJoinProjection],
    pub(crate) column_names: Arc<[String]>,
    pub(crate) lookup_row_id: i64,
    pub(crate) pager: &'a PagerHandle,
    pub(crate) wal: &'a WalHandle,
    pub(crate) snapshot_lsn: u64,
    pub(crate) use_persistent_pk_index: bool,
}

struct ValidatedSimpleRowIdProjectionRequest<'a> {
    table_schema: &'a TableSchema,
    projection_indexes: &'a [usize],
    column_names: Arc<[String]>,
    lookup_row_id: i64,
    pager: &'a PagerHandle,
    wal: &'a WalHandle,
    snapshot_lsn: u64,
    use_persistent_pk_index: bool,
}

impl EngineRuntime {
    #[must_use]
    pub(crate) fn empty(schema_cookie: u32) -> Self {
        let config = crate::config::DbConfig {
            paged_row_storage: false,
            ..crate::config::DbConfig::default()
        };
        Self::from_config(schema_cookie, &config)
    }

    #[must_use]
    pub(crate) fn from_config(schema_cookie: u32, config: &crate::config::DbConfig) -> Self {
        Self {
            catalog: Arc::new(CatalogState::empty(schema_cookie)),
            tables: Arc::new(BTreeMap::new()),
            temp_tables: Arc::new(BTreeMap::new()),
            temp_table_data: Arc::new(BTreeMap::new()),
            temp_views: Arc::new(BTreeMap::new()),
            temp_indexes: Arc::new(BTreeMap::new()),
            temp_schema_cookie: 0,
            indexes: Arc::new(BTreeMap::new()),
            persisted_tables: Arc::new(BTreeMap::new()),
            deferred_paged_row_locator_caches: Arc::new(BTreeMap::new()),
            view_query_cache: Arc::new(Mutex::new(ViewQueryCache::default())),
            resident_tombstone_locators: Arc::new(BTreeMap::new()),
            deferred_tables: Arc::new(BTreeSet::new()),
            dirty_tables: Arc::new(BTreeSet::new()),
            paged_mutations: BTreeMap::new(),
            payload_cache: Arc::new(Mutex::new(PayloadCache::new(
                config.cached_payloads_max_entries,
            ))),
            root_state: None,
            index_state_epoch: 0,
            paged_row_storage: config.paged_row_storage,
            manifest_template: None,
            overflow_chain_caches: BTreeMap::new(),
            manifest_chain_cache: None,
            sync_capture_active: false,
            sync_mutations: Vec::new(),
            reactive_capture_active: false,
            reactive_mutations: Vec::new(),
            extension_trust_anchors: Arc::new(config.extension_trust_anchors.clone()),
            extension_unsigned_development_mode: config.extension_unsigned_development_mode,
            audit_context: Arc::new(Mutex::new(crate::security::AuditContext::default())),
            tracing: None,
            fts_eval_context: Arc::new(Mutex::new(FtsEvalContext::default())),
        }
    }

    pub(crate) fn set_audit_context_handle(
        &mut self,
        handle: Arc<Mutex<crate::security::AuditContext>>,
    ) {
        self.audit_context = handle;
    }

    pub(crate) fn set_sync_capture_active(&mut self, active: bool) {
        self.sync_capture_active = active;
        if !active {
            self.sync_mutations.clear();
        }
    }

    pub(crate) fn set_reactive_capture_active(&mut self, active: bool) {
        self.reactive_capture_active = active;
        if !active {
            self.reactive_mutations.clear();
        }
    }

    pub(crate) fn sync_capture_active(&self) -> bool {
        self.sync_capture_active
    }

    pub(crate) fn set_tracing(&mut self, tracing: Arc<crate::tracing::RuntimeTraceState>) {
        self.tracing = Some(tracing);
    }

    #[allow(dead_code)]
    pub(crate) fn drain_index_usage(&self) {
        let events = crate::tracing::index_usage::drain_local_index_usage();
        if events.is_empty() {
            return;
        }
        if let Some(tracing) = self.tracing.as_ref() {
            for (table_name, index_name, index_kind, kind) in events {
                tracing.record_index_usage(&table_name, &index_name, &index_kind, kind);
            }
        }
    }

    pub(crate) fn mutation_capture_active(&self) -> bool {
        self.sync_capture_active || self.reactive_capture_active
    }

    pub(crate) fn should_record_sync_mutation_for_table(&self, table: &TableSchema) -> bool {
        (self.sync_capture_active || self.reactive_capture_active)
            && !table.temporary
            && !crate::sync::is_internal_table_name(&table.name)
    }

    pub(crate) fn record_sync_mutation(
        &mut self,
        table_name: &str,
        operation: crate::sync::SyncOperation,
        primary_key: serde_json::Value,
        after: Option<serde_json::Value>,
        schema_cookie: u32,
    ) {
        if self.sync_capture_active {
            self.sync_mutations.push(crate::sync::SyncMutation {
                table: table_name.to_string(),
                operation,
                primary_key: primary_key.clone(),
                after: after.clone(),
                schema_cookie,
            });
        }
        if self.reactive_capture_active {
            self.reactive_mutations
                .push(crate::reactive::RowChange::new(
                    table_name.to_string(),
                    crate::reactive::row_operation_from_sync(operation),
                    primary_key,
                    None,
                    after,
                ));
        }
    }

    pub(crate) fn take_sync_mutations(&mut self) -> Vec<crate::sync::SyncMutation> {
        std::mem::take(&mut self.sync_mutations)
    }

    pub(crate) fn take_reactive_mutations(&mut self) -> Vec<crate::reactive::RowChange> {
        std::mem::take(&mut self.reactive_mutations)
    }

    pub(super) fn bump_temp_schema_cookie(&mut self) {
        self.temp_schema_cookie = self.temp_schema_cookie.wrapping_add(1);
        if self.temp_schema_cookie == 0 {
            self.temp_schema_cookie = 1;
        }
    }

    fn persistent_resolution_runtime(&self) -> Self {
        let mut runtime = self.clone();
        runtime.temp_tables_mut().clear();
        runtime.temp_table_data_map_mut().clear();
        runtime.temp_views_mut().clear();
        runtime.temp_indexes_mut().clear();
        runtime.temp_schema_cookie = 0;
        runtime
    }

    fn cached_view_query(&self, view: &ViewSchema) -> Result<Arc<Query>> {
        let key = ViewQueryCacheKey::new(view);
        {
            let cache = self
                .view_query_cache
                .lock()
                .expect("view query cache lock should not be poisoned");
            if let Some(query) = cache.get(&key) {
                return Ok(query);
            }
        }

        let view_statement = parse_sql_statement(&view.sql_text)?;
        let Statement::Query(query) = view_statement else {
            return Err(DbError::corruption(format!(
                "view {} does not contain a SELECT statement",
                view.name
            )));
        };
        let query = Arc::new(query);
        let mut cache = self
            .view_query_cache
            .lock()
            .expect("view query cache lock should not be poisoned");
        Ok(cache.insert(key, query))
    }

    fn cache_view_query(&self, view: &ViewSchema, query: Query) {
        self.view_query_cache
            .lock()
            .expect("view query cache lock should not be poisoned")
            .insert(ViewQueryCacheKey::new(view), Arc::new(query));
    }

    fn catalog_mut(&mut self) -> &mut CatalogState {
        Arc::make_mut(&mut self.catalog)
    }

    fn tables_mut(&mut self) -> &mut BTreeMap<String, TableRowSource> {
        Arc::make_mut(&mut self.tables)
    }

    fn temp_table_data_map_mut(&mut self) -> &mut BTreeMap<String, Arc<TableData>> {
        Arc::make_mut(&mut self.temp_table_data)
    }

    /// Per-entry COW: locates `name` in `tables` (case-insensitive) and
    /// returns a `&mut TableData` for the targeted table, cloning only that
    /// table's row vector when the per-entry `Arc<TableData>` is shared.
    /// This avoids deep-cloning every other table's rows during writes.
    fn entry_table_data_mut(&mut self, name: &str) -> Option<&mut TableData> {
        let canonical = map_key_ci(self.tables.as_ref(), name)?;
        let map = self.tables_mut();
        let entry = map.get_mut(&canonical)?;
        Some(entry.resident_data_mut())
    }

    fn entry_table_row_source_mut(&mut self, name: &str) -> Option<&mut TableRowSource> {
        let canonical = map_key_ci(self.tables.as_ref(), name)?;
        self.tables_mut().get_mut(&canonical)
    }

    fn entry_temp_table_data_mut(&mut self, name: &str) -> Option<&mut TableData> {
        let canonical = map_key_ci(self.temp_table_data.as_ref(), name)?;
        let map = self.temp_table_data_map_mut();
        let entry = map.get_mut(&canonical)?;
        Some(Arc::make_mut(entry))
    }

    fn temp_tables_mut(&mut self) -> &mut BTreeMap<String, TableSchema> {
        Arc::make_mut(&mut self.temp_tables)
    }

    fn temp_views_mut(&mut self) -> &mut BTreeMap<String, ViewSchema> {
        Arc::make_mut(&mut self.temp_views)
    }

    fn temp_indexes_mut(&mut self) -> &mut BTreeMap<String, IndexSchema> {
        Arc::make_mut(&mut self.temp_indexes)
    }

    fn indexes_mut(&mut self) -> &mut BTreeMap<String, Arc<RuntimeIndex>> {
        Arc::make_mut(&mut self.indexes)
    }

    fn persisted_tables_mut(&mut self) -> &mut BTreeMap<String, PersistedTableState> {
        Arc::make_mut(&mut self.persisted_tables)
    }

    fn resident_tombstone_locators_mut(&mut self) -> &mut BTreeMap<String, Arc<Int64Map<u32>>> {
        Arc::make_mut(&mut self.resident_tombstone_locators)
    }

    fn dirty_tables_mut(&mut self) -> &mut BTreeSet<String> {
        Arc::make_mut(&mut self.dirty_tables)
    }

    /// Read-only access to a runtime index by name (case-sensitive).
    pub(crate) fn index(&self, name: &str) -> Option<&RuntimeIndex> {
        self.indexes.get(name).map(|arc| arc.as_ref())
    }

    /// Targeted copy-on-write access to a single runtime index entry.
    ///
    /// Performs `Arc::make_mut` only on the targeted entry (and on the outer
    /// map), so unrelated indexes are not cloned even when the runtime is
    /// shared with concurrent readers.
    pub(crate) fn index_mut(&mut self, name: &str) -> Option<&mut RuntimeIndex> {
        let map = Arc::make_mut(&mut self.indexes);
        map.get_mut(name).map(Arc::make_mut)
    }

    fn cached_payload(&mut self, table_name: &str) -> Option<Arc<Vec<u8>>> {
        self.payload_cache
            .lock()
            .expect("payload cache lock should not be poisoned")
            .get(table_name)
    }

    fn cached_payload_take(&mut self, table_name: &str) -> Option<Arc<Vec<u8>>> {
        self.payload_cache
            .lock()
            .expect("payload cache lock should not be poisoned")
            .take(table_name)
    }

    fn cache_payload_insert(&mut self, table_name: String, payload: Arc<Vec<u8>>) {
        self.payload_cache
            .lock()
            .expect("payload cache lock should not be poisoned")
            .insert(table_name, payload);
    }

    fn cache_payload_remove(&mut self, table_name: &str) {
        self.payload_cache
            .lock()
            .expect("payload cache lock should not be poisoned")
            .remove(table_name);
    }

    fn cache_deferred_paged_row_locators(
        &mut self,
        table_name: &str,
        state: PersistedTableState,
        chunks: &[TablePageManifestChunk],
    ) -> Result<()> {
        let cache = build_deferred_paged_row_locator_cache(state, chunks)?;
        self.deferred_paged_row_locator_caches_mut()
            .insert(table_name.to_string(), Arc::new(cache));
        Ok(())
    }

    fn should_cache_deferred_paged_row_locators(&self, table_name: &str) -> bool {
        let has_runtime_btree = self.catalog.indexes.values().any(|index| {
            identifiers_equal(&index.table_name, table_name)
                && index.fresh
                && index.kind == IndexKind::Btree
                && matches!(self.index(&index.name), Some(RuntimeIndex::Btree { .. }))
        });
        has_runtime_btree
            || self
                .catalog
                .table(table_name)
                .and_then(row_id_alias_column_name)
                .is_some()
    }

    /// Returns per-table residency stats `(table_name, row_count, heap_bytes)`
    /// for every fully-loaded table in this runtime. Deferred (not-yet-loaded)
    /// tables are omitted. Used by `Db::inspect_storage_state_json` per
    /// ADR 0143 Phase A. Result is sorted by table name for deterministic
    /// output.
    #[must_use]
    #[allow(dead_code)] // exposed for follow-up Phase A JSON breakdown wiring
    pub(crate) fn table_memory_breakdown(&self) -> Vec<(String, usize, usize)> {
        let mut out = Vec::with_capacity(self.tables.len());
        for (name, data) in self.tables.iter() {
            out.push((
                name.clone(),
                data.row_count(),
                data.approximate_heap_bytes(),
            ));
        }
        out
    }

    /// Returns aggregate `(total_rows, total_heap_bytes, table_count,
    /// deferred_table_count)` across all loaded tables. Deferred tables
    /// contribute zero to the byte/row totals but are counted separately.
    /// Per ADR 0143 Phase A.
    #[must_use]
    pub(crate) fn table_memory_totals(&self) -> (u64, u64, u32, u32) {
        let mut rows: u64 = 0;
        let mut bytes: u64 = 0;
        for data in self.tables.values() {
            rows = rows.saturating_add(data.row_count() as u64);
            bytes = bytes.saturating_add(data.approximate_heap_bytes() as u64);
        }
        let table_count = u32::try_from(self.tables.len()).unwrap_or(u32::MAX);
        let deferred_count = u32::try_from(self.deferred_tables.len()).unwrap_or(u32::MAX);
        (rows, bytes, table_count, deferred_count)
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
            .filter(|table_name| self.catalog.table(table_name).is_none())
            .cloned()
            .collect::<Vec<_>>();

        {
            let mut store = DbTxnPageStore { db };
            for table_name in dirty_tables {
                let Some(table) = self.catalog.table(&table_name) else {
                    continue;
                };
                let canonical_table_name = table.name.clone();
                let delta = self
                    .paged_mutations
                    .get(&canonical_table_name)
                    .cloned()
                    .unwrap_or_default();
                let previous_state = self
                    .persisted_tables
                    .get(&canonical_table_name)
                    .copied()
                    .unwrap_or_default();
                let previous_pointer = previous_state.pointer;
                let resident_tombstone_locators = self
                    .resident_tombstone_locators
                    .get(&canonical_table_name)
                    .cloned();
                let row_source =
                    self.tables
                        .get(&canonical_table_name)
                        .cloned()
                        .ok_or_else(|| {
                            DbError::internal(format!("table data for {table_name} is missing"))
                        })?;
                let mut use_paged_row_storage =
                    db.config().paged_row_storage || previous_pointer.is_table_paged_manifest();
                if db.config().paged_row_storage && !previous_pointer.is_table_paged_manifest() {
                    use_paged_row_storage = match &row_source {
                        TableRowSource::Resident(data) => resident_table_should_use_paged_storage(
                            data,
                            previous_state,
                            &delta,
                            db.config().page_size,
                        )?,
                        TableRowSource::Paged(manifest) => {
                            manifest.chunks.len() > 1
                                || manifest.chunks.first().is_some_and(|chunk| {
                                    chunk.payload.len()
                                        > paged_table_target_chunk_bytes(db.config().page_size)
                                })
                        }
                    };
                }
                let resident_update_only = !delta.updated_rows.is_empty()
                    && delta.deleted_rows.is_empty()
                    && delta.append_count == 0;
                let resident_delete_only = delta.updated_rows.is_empty()
                    && !delta.deleted_rows.is_empty()
                    && delta.append_count == 0;
                let cached_payload = if (!use_paged_row_storage
                    || !previous_pointer.is_table_paged_manifest())
                    && !resident_update_only
                    && !resident_delete_only
                {
                    self.cached_payload(&canonical_table_name)
                } else {
                    None
                };
                if let Some(manifest) = row_source.paged_manifest() {
                    self.overflow_chain_caches.remove(&canonical_table_name);
                    if delta.append_count > 0
                        && delta.updated_rows.is_empty()
                        && delta.deleted_rows.is_empty()
                        && manifest.tombstoned_row_ids.is_empty()
                        && !db.config().persistent_pk_index
                    {
                        if let Some((new_state, persisted_chunks)) =
                            try_append_only_paged_table_from_manifest(
                                &mut store,
                                previous_state,
                                manifest,
                            )?
                        {
                            self.persisted_tables_mut()
                                .insert(canonical_table_name.clone(), new_state);
                            let pk_index_root = self
                                .refresh_paged_lookup_cache_and_pk_index_from_chunks(
                                    db,
                                    &canonical_table_name,
                                    new_state,
                                    &persisted_chunks,
                                )?;
                            replace_table_pk_index_root(
                                self,
                                db,
                                &canonical_table_name,
                                pk_index_root,
                            )?;
                            let persisted_manifest = table_page_manifest_with_persisted_chunks(
                                manifest,
                                &persisted_chunks,
                            );
                            self.replace_table_row_source(
                                &canonical_table_name,
                                TableRowSource::Paged(Arc::new(persisted_manifest)),
                            )?;
                            self.cache_payload_remove(&canonical_table_name);
                            continue;
                        }
                    }
                    let (new_state, persisted_chunks) =
                        rewrite_paged_table_from_manifest(&mut store, previous_state, manifest)?;
                    self.persisted_tables_mut()
                        .insert(canonical_table_name.clone(), new_state);
                    if new_state == previous_state {
                        if db.config().persistent_pk_index {
                            replace_table_pk_index_root(
                                self,
                                db,
                                &canonical_table_name,
                                previous_state.pk_index_root,
                            )?;
                        }
                        let persisted_manifest =
                            table_page_manifest_with_persisted_chunks(manifest, &persisted_chunks);
                        self.replace_table_row_source(
                            &canonical_table_name,
                            TableRowSource::Paged(Arc::new(persisted_manifest)),
                        )?;
                        self.cache_payload_remove(&canonical_table_name);
                        continue;
                    }
                    let pk_index_root = self.refresh_paged_lookup_cache_and_pk_index_from_chunks(
                        db,
                        &canonical_table_name,
                        new_state,
                        &persisted_chunks,
                    )?;
                    replace_table_pk_index_root(self, db, &canonical_table_name, pk_index_root)?;
                    let persisted_manifest =
                        table_page_manifest_with_persisted_chunks(manifest, &persisted_chunks);
                    self.replace_table_row_source(
                        &canonical_table_name,
                        TableRowSource::Paged(Arc::new(persisted_manifest)),
                    )?;
                    self.cache_payload_remove(&canonical_table_name);
                    continue;
                }
                if use_paged_row_storage && previous_pointer.is_table_paged_manifest() {
                    self.overflow_chain_caches.remove(&canonical_table_name);
                    if delta.append_count > 0
                        && delta.updated_rows.is_empty()
                        && delta.deleted_rows.is_empty()
                        && !row_source.has_tombstoned_rows()
                    {
                        let data = row_source.resident_data();
                        let existing_count = data.rows.len().saturating_sub(delta.append_count);
                        let appended_chunks = encode_paged_table_chunks_from_rows(
                            &data.rows[existing_count..],
                            db.config().page_size,
                        )?;
                        let new_state = if !appended_chunks.is_empty() {
                            append_paged_table_chunks(
                                &mut store,
                                previous_state,
                                &appended_chunks,
                                data.row_count(),
                            )?
                        } else {
                            previous_state
                        };
                        self.persisted_tables_mut()
                            .insert(canonical_table_name.clone(), new_state);
                        let pk_index_root = self.refresh_paged_lookup_cache_and_pk_index(
                            db,
                            &store,
                            &canonical_table_name,
                            new_state,
                        )?;
                        replace_table_pk_index_root(
                            self,
                            db,
                            &canonical_table_name,
                            pk_index_root,
                        )?;
                        self.cache_payload_remove(&canonical_table_name);
                        continue;
                    }
                }
                let data = row_source.resident_data();
                if delta.append_count > 0
                    && delta.updated_rows.is_empty()
                    && delta.deleted_rows.is_empty()
                    && previous_pointer.head_page_id != 0
                    && !use_paged_row_storage
                    && !previous_pointer.is_compressed()
                    && !data.has_tombstoned_rows()
                {
                    let existing_count = data.rows.len().saturating_sub(delta.append_count);
                    if existing_count <= data.rows.len() {
                        let appended_rows = encode_appended_table_rows(data, existing_count)?;
                        if !appended_rows.is_empty() {
                            if !db.config().persistent_pk_index {
                                let row_count = data.row_count();
                                let row_count_bytes = u32::try_from(row_count)
                                    .map_err(|_| {
                                        DbError::constraint("table row count exceeds u32")
                                    })?
                                    .to_le_bytes();
                                let (ptr, checksum, new_chain_cache, tail) =
                                    append_uncompressed_with_first_page_patch(
                                        &mut store,
                                        previous_pointer,
                                        TABLE_PAYLOAD_MAGIC.len(),
                                        &row_count_bytes,
                                        &appended_rows,
                                    )?;
                                self.overflow_chain_caches
                                    .insert(canonical_table_name.clone(), new_chain_cache);
                                let row_count = data.row_count();
                                self.persisted_tables_mut().insert(
                                    canonical_table_name.clone(),
                                    PersistedTableState {
                                        pointer: ptr,
                                        checksum,
                                        row_count,
                                        tail,
                                        pk_index_root: previous_state.pk_index_root,
                                    },
                                );
                                replace_table_pk_index_root(self, db, &canonical_table_name, None)?;
                                self.cache_payload_remove(&canonical_table_name);
                                continue;
                            }

                            let new_payload = if let Some(cached) = cached_payload {
                                let previous = Arc::try_unwrap(cached)
                                    .unwrap_or_else(|arc| arc.as_slice().to_vec());
                                append_encoded_rows_to_table_payload(
                                    previous,
                                    data.row_count(),
                                    &appended_rows,
                                )?
                            } else {
                                let previous_payload = read_overflow(&store, previous_pointer)?;
                                append_encoded_rows_to_table_payload(
                                    previous_payload,
                                    data.row_count(),
                                    &appended_rows,
                                )?
                            };
                            let checksum = crc32c_parts(&[new_payload.as_slice()]);
                            let ptr = rewrite_overflow(
                                &mut store,
                                previous_pointer,
                                &new_payload,
                                CompressionMode::Never,
                            )?;
                            let new_chain_cache =
                                build_overflow_chain_cache(&store, ptr.head_page_id)?;
                            let tail =
                                read_uncompressed_overflow_tail(&store, ptr)?.unwrap_or_default();
                            self.overflow_chain_caches
                                .insert(canonical_table_name.clone(), new_chain_cache);
                            let row_count = data.row_count();
                            self.persisted_tables_mut().insert(
                                canonical_table_name.clone(),
                                PersistedTableState {
                                    pointer: ptr,
                                    checksum,
                                    row_count,
                                    tail,
                                    pk_index_root: previous_state.pk_index_root,
                                },
                            );
                            if db.config().persistent_pk_index {
                                let pk_index_root =
                                    build_persistent_pk_index_root(db, new_payload.as_slice())?;
                                replace_table_pk_index_root(
                                    self,
                                    db,
                                    &canonical_table_name,
                                    pk_index_root,
                                )?;
                            } else {
                                replace_table_pk_index_root(self, db, &canonical_table_name, None)?;
                            }
                            self.cache_payload_insert(
                                canonical_table_name.clone(),
                                Arc::new(new_payload),
                            );
                            continue;
                        }
                    }
                }

                if use_paged_row_storage {
                    self.overflow_chain_caches.remove(&canonical_table_name);
                    let new_state = if delta.append_count > 0
                        && delta.updated_rows.is_empty()
                        && delta.deleted_rows.is_empty()
                        && !data.has_tombstoned_rows()
                    {
                        let existing_count = data.rows.len().saturating_sub(delta.append_count);
                        let appended_chunks = encode_paged_table_chunks_from_rows(
                            &data.rows[existing_count..],
                            db.config().page_size,
                        )?;
                        if !appended_chunks.is_empty() {
                            append_paged_table_chunks(
                                &mut store,
                                previous_state,
                                &appended_chunks,
                                data.row_count(),
                            )?
                        } else {
                            rewrite_paged_table_from_resident(
                                &mut store,
                                previous_state,
                                data,
                                db.config().page_size,
                            )?
                        }
                    } else if delta.updated_rows.is_empty() && !delta.deleted_rows.is_empty() {
                        // Delete-only delta: avoid decoding values for chunks that
                        // contain no deleted rows by scanning row ids only.
                        rewrite_paged_table_from_resident_delete_only(
                            &mut store,
                            previous_state,
                            data,
                            db.config().page_size,
                            &delta.deleted_rows,
                        )?
                    } else {
                        rewrite_paged_table_from_resident(
                            &mut store,
                            previous_state,
                            data,
                            db.config().page_size,
                        )?
                    };
                    self.persisted_tables_mut()
                        .insert(canonical_table_name.clone(), new_state);
                    let pk_index_root = self.refresh_paged_lookup_cache_and_pk_index(
                        db,
                        &store,
                        &canonical_table_name,
                        new_state,
                    )?;
                    replace_table_pk_index_root(self, db, &canonical_table_name, pk_index_root)?;
                    self.cache_payload_remove(&canonical_table_name);
                    continue;
                }

                // Choose the encoding path:
                //  1. Row-update splice: only re-encode modified rows using cached payload
                //  2. Row-delete splice: copy unchanged encoded rows from previous payload
                //  3. Append-only: read old payload, append new rows
                //  4. Full re-encode: encode every row from scratch
                let mut resident_tombstone_locators_preserved = false;
                let (payload, dirty_byte_ranges, pk_locator_preserved) = if !delta
                    .updated_rows
                    .is_empty()
                    && delta.deleted_rows.is_empty()
                    && delta.append_count == 0
                {
                    let mut dirty_indices = Vec::with_capacity(delta.updated_rows.len());
                    for row_id in delta.updated_rows.keys() {
                        if let Some(idx) = data.row_index_by_id(*row_id) {
                            dirty_indices.push(idx);
                        }
                    }
                    dirty_indices.sort_unstable();

                    if let Some(cached) = self.cached_payload_take(&canonical_table_name) {
                        match Arc::try_unwrap(cached) {
                            Ok(mut payload) => {
                                if let Some(dirty_range) = splice_updated_rows_payload_in_place(
                                    &mut payload,
                                    data,
                                    &dirty_indices,
                                )? {
                                    (
                                        payload,
                                        single_dirty_range(
                                            dirty_range.first_dirty_byte
                                                ..dirty_range.last_dirty_byte,
                                        ),
                                        true,
                                    )
                                } else {
                                    let splice = splice_updated_rows_payload(
                                        payload.as_slice(),
                                        data,
                                        &dirty_indices,
                                    )?;
                                    let first = splice.first_dirty_byte;
                                    let last = splice.last_dirty_byte;
                                    (
                                        splice.payload,
                                        single_dirty_range(first..last),
                                        splice.pk_locator_preserved,
                                    )
                                }
                            }
                            Err(cached) => {
                                let splice = splice_updated_rows_payload(
                                    cached.as_slice(),
                                    data,
                                    &dirty_indices,
                                )?;
                                let first = splice.first_dirty_byte;
                                let last = splice.last_dirty_byte;
                                (
                                    splice.payload,
                                    single_dirty_range(first..last),
                                    splice.pk_locator_preserved,
                                )
                            }
                        }
                    } else if previous_pointer.head_page_id != 0 {
                        let mut payload = read_overflow(&store, previous_pointer)?;
                        if let Some(dirty_range) = splice_updated_rows_payload_in_place(
                            &mut payload,
                            data,
                            &dirty_indices,
                        )? {
                            (
                                payload,
                                single_dirty_range(
                                    dirty_range.first_dirty_byte..dirty_range.last_dirty_byte,
                                ),
                                true,
                            )
                        } else {
                            let splice = splice_updated_rows_payload(
                                payload.as_slice(),
                                data,
                                &dirty_indices,
                            )?;
                            let first = splice.first_dirty_byte;
                            let last = splice.last_dirty_byte;
                            (
                                splice.payload,
                                single_dirty_range(first..last),
                                splice.pk_locator_preserved,
                            )
                        }
                    } else {
                        let payload = encode_table_payload(data)?;
                        let last = payload.len();
                        (payload, single_dirty_range(0..last), false)
                    }
                } else if !delta.deleted_rows.is_empty()
                    && delta.updated_rows.is_empty()
                    && delta.append_count == 0
                {
                    if !db.config().paged_row_storage
                        && !db.config().persistent_pk_index
                        && previous_pointer.head_page_id != 0
                        && !previous_pointer.is_compressed()
                    {
                        if let (Some(cached), Some(locators)) = (
                            self.cached_payload_take(&canonical_table_name),
                            resident_tombstone_locators.as_deref(),
                        ) {
                            let mut payload = Arc::try_unwrap(cached)
                                .unwrap_or_else(|arc| arc.as_slice().to_vec());
                            if let Some((dirty_ranges, checksum)) =
                                tombstone_deleted_rows_cached_payload_by_locator(
                                    &mut payload,
                                    &delta.deleted_rows,
                                    locators,
                                    previous_state.checksum,
                                )?
                            {
                                let chain_cache =
                                    match self.overflow_chain_caches.get(&canonical_table_name) {
                                        Some(cache) => cache.clone(),
                                        None => build_overflow_chain_cache(
                                            &store,
                                            previous_pointer.head_page_id,
                                        )?,
                                    };
                                let (pointer, new_chain_cache, tail) =
                                    rewrite_overflow_cached_with_dirty_byte_ranges(
                                        &mut store,
                                        previous_pointer,
                                        &payload,
                                        &chain_cache.page_ids,
                                        0,
                                        Some(dirty_ranges.as_slice()),
                                    )?;
                                self.overflow_chain_caches
                                    .insert(canonical_table_name.clone(), new_chain_cache);
                                self.persisted_tables_mut().insert(
                                    canonical_table_name.clone(),
                                    PersistedTableState {
                                        pointer,
                                        checksum,
                                        row_count: data.row_count(),
                                        tail,
                                        pk_index_root: previous_state.pk_index_root,
                                    },
                                );
                                replace_table_pk_index_root(self, db, &canonical_table_name, None)?;
                                self.cache_payload_insert(
                                    canonical_table_name.clone(),
                                    Arc::new(payload),
                                );
                                continue;
                            }
                            self.cache_payload_insert(
                                canonical_table_name.clone(),
                                Arc::new(payload),
                            );
                        }
                    }

                    if !db.config().paged_row_storage
                        && !db.config().persistent_pk_index
                        && previous_pointer.head_page_id != 0
                        && !previous_pointer.is_compressed()
                    {
                        if let Some(cached) = self.cached_payload_take(&canonical_table_name) {
                            let mut payload = Arc::try_unwrap(cached)
                                .unwrap_or_else(|arc| arc.as_slice().to_vec());
                            if let Some(dirty_ranges) = truncate_tail_deleted_rows_payload(
                                &mut payload,
                                &delta.deleted_rows,
                                data.row_count(),
                            )? {
                                let checksum = crc32c_parts(&[payload.as_slice()]);
                                let chain_cache =
                                    match self.overflow_chain_caches.get(&canonical_table_name) {
                                        Some(cache) => cache.clone(),
                                        None => build_overflow_chain_cache(
                                            &store,
                                            previous_pointer.head_page_id,
                                        )?,
                                    };
                                let (pointer, new_chain_cache, tail) =
                                    rewrite_overflow_cached_with_dirty_byte_ranges(
                                        &mut store,
                                        previous_pointer,
                                        &payload,
                                        &chain_cache.page_ids,
                                        0,
                                        Some(dirty_ranges.as_slice()),
                                    )?;
                                self.overflow_chain_caches
                                    .insert(canonical_table_name.clone(), new_chain_cache);
                                self.persisted_tables_mut().insert(
                                    canonical_table_name.clone(),
                                    PersistedTableState {
                                        pointer,
                                        checksum,
                                        row_count: data.row_count(),
                                        tail,
                                        pk_index_root: previous_state.pk_index_root,
                                    },
                                );
                                replace_table_pk_index_root(self, db, &canonical_table_name, None)?;
                                self.resident_tombstone_locators_mut()
                                    .remove(&canonical_table_name);
                                self.cache_payload_insert(
                                    canonical_table_name.clone(),
                                    Arc::new(payload),
                                );
                                continue;
                            }
                            self.cache_payload_insert(
                                canonical_table_name.clone(),
                                Arc::new(payload),
                            );
                        }
                    }

                    if !db.config().paged_row_storage
                        && !db.config().persistent_pk_index
                        && previous_pointer.head_page_id != 0
                        && !previous_pointer.is_compressed()
                    {
                        let chain_cache =
                            match self.overflow_chain_caches.get(&canonical_table_name) {
                                Some(cache) => Some(cache.clone()),
                                None => Some(build_overflow_chain_cache(
                                    &store,
                                    previous_pointer.head_page_id,
                                )?),
                            };
                        if let (Some(locators), Some(chain_cache)) =
                            (resident_tombstone_locators.as_deref(), chain_cache.as_ref())
                        {
                            let sparse_result = tombstone_deleted_rows_overflow_by_locator(
                                &mut store,
                                previous_state,
                                chain_cache,
                                &delta.deleted_rows,
                                locators,
                                data.row_count(),
                            )?;
                            if let Some((mut new_state, new_chain_cache)) = sparse_result {
                                new_state.pk_index_root = None;
                                self.persisted_tables_mut()
                                    .insert(canonical_table_name.clone(), new_state);
                                self.overflow_chain_caches
                                    .insert(canonical_table_name.clone(), new_chain_cache);
                                replace_table_pk_index_root(self, db, &canonical_table_name, None)?;
                                self.cache_payload_remove(&canonical_table_name);
                                continue;
                            }
                        }
                    }

                    // ADR 0200: obtain the previous on-disk payload (cached
                    // or read back) so a delete can be applied as in-place
                    // tombstones instead of shifting every surviving byte.
                    let previous_payload: Option<Vec<u8>> =
                        if let Some(cached) = self.cached_payload_take(&canonical_table_name) {
                            match Arc::try_unwrap(cached) {
                                Ok(payload) => Some(payload),
                                Err(shared) => Some(shared.as_ref().clone()),
                            }
                        } else if previous_pointer.head_page_id != 0 {
                            Some(read_overflow(&store, previous_pointer)?)
                        } else {
                            None
                        };

                    match previous_payload {
                        Some(mut payload) => {
                            // Prefer in-place tombstones unless the table has
                            // become heavily fragmented, in which case a full
                            // re-encode of the live resident rows reclaims the
                            // accumulated dead slots.
                            let physical =
                                read_table_payload_row_count_from_bytes(&payload).unwrap_or(0);
                            let live = data.row_count();
                            let dead_after = physical.saturating_sub(live);
                            // ADR 0200: in-place delete tombstones apply
                            // only to the resident single-payload storage
                            // form (`paged_row_storage = false`, e.g. the
                            // embedded_fast / tuned_durable profiles). When
                            // `paged_row_storage` is enabled, a resident
                            // payload can be promoted to a paged manifest by
                            // later writes/checkpoints, and mixing the two
                            // representations is unsafe, so those profiles
                            // keep the compacting splice path.
                            let tombstoned =
                                if !db.config().paged_row_storage && live > 0 && dead_after <= live
                                {
                                    if let Some(locators) = resident_tombstone_locators.as_ref() {
                                        match tombstone_deleted_rows_payload_by_locator(
                                            &mut payload,
                                            &delta.deleted_rows,
                                            locators,
                                        )? {
                                            Some(dirty) => {
                                                resident_tombstone_locators_preserved = true;
                                                Some(dirty)
                                            }
                                            None => tombstone_deleted_rows_payload_in_place(
                                                &mut payload,
                                                &delta.deleted_rows,
                                            )?,
                                        }
                                    } else {
                                        tombstone_deleted_rows_payload_in_place(
                                            &mut payload,
                                            &delta.deleted_rows,
                                        )?
                                    }
                                } else {
                                    None
                                };
                            if let Some(dirty) = tombstoned {
                                (payload, dirty, false)
                            } else if dead_after == 0 {
                                // No pre-existing tombstones: the surviving
                                // rows still map 1:1 to the payload, so the
                                // byte-shifting splice is valid and compacts.
                                if let Some(dirty_range) = splice_deleted_rows_payload_in_place(
                                    &mut payload,
                                    data,
                                    &delta.deleted_rows,
                                )? {
                                    (payload, dirty_range, false)
                                } else {
                                    let splice = splice_deleted_rows_payload(
                                        payload.as_slice(),
                                        data,
                                        &delta.deleted_rows,
                                    )?;
                                    let first = splice.first_dirty_byte;
                                    let last = splice.last_dirty_byte;
                                    (splice.payload, single_dirty_range(first..last), false)
                                }
                            } else {
                                // Fragmented payload (pre-existing tombstones):
                                // re-encode the authoritative live rows.
                                let payload = encode_table_payload(data)?;
                                let last = payload.len();
                                (payload, single_dirty_range(0..last), false)
                            }
                        }
                        None => {
                            let payload = encode_table_payload(data)?;
                            let last = payload.len();
                            (payload, single_dirty_range(0..last), false)
                        }
                    }
                } else if delta.append_count > 0
                    && delta.updated_rows.is_empty()
                    && delta.deleted_rows.is_empty()
                    && previous_pointer.head_page_id != 0
                    && !data.has_tombstoned_rows()
                {
                    let previous_payload = read_overflow(&store, previous_pointer)?;
                    // ADR 0200: the append fast path assumes the previous
                    // payload's physical slot count equals the live row
                    // count before this append. If the payload carries
                    // delete tombstones (physical > live), that assumption
                    // is false, so re-encode the live rows (which also
                    // reclaims the tombstone slots).
                    let previous_physical =
                        read_table_payload_row_count_from_bytes(&previous_payload).unwrap_or(0);
                    let expected_prior_live = data.row_count().saturating_sub(delta.append_count);
                    if previous_physical == expected_prior_live {
                        let payload = append_table_payload(previous_payload, data)?;
                        let last = payload.len();
                        (payload, single_dirty_range(0..last), false)
                    } else {
                        let payload = encode_table_payload(data)?;
                        let last = payload.len();
                        (payload, single_dirty_range(0..last), false)
                    }
                } else {
                    let payload = encode_table_payload(data)?;
                    let last = payload.len();
                    (payload, single_dirty_range(0..last), false)
                };

                let checksum = crc32c_parts(&[payload.as_slice()]);
                let (pointer, new_chain_cache, tail) = if let Some(chain_cache) =
                    self.overflow_chain_caches.get(&canonical_table_name)
                {
                    let page_size = db.config().page_size as usize;
                    let chunk_cap = page_size.saturating_sub(OVERFLOW_HEADER_SIZE);
                    let skip = if chunk_cap == 0 {
                        0
                    } else {
                        dirty_byte_ranges
                            .iter()
                            .map(|range| range.start.checked_div(chunk_cap).unwrap_or(0))
                            .min()
                            .unwrap_or(0)
                    };
                    rewrite_overflow_cached_with_dirty_byte_ranges(
                        &mut store,
                        previous_pointer,
                        &payload,
                        &chain_cache.page_ids,
                        skip,
                        Some(dirty_byte_ranges.as_slice()),
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
                    .insert(canonical_table_name.clone(), new_chain_cache);
                let row_count = data.row_count();
                self.persisted_tables_mut().insert(
                    canonical_table_name.clone(),
                    PersistedTableState {
                        pointer,
                        checksum,
                        row_count,
                        tail,
                        pk_index_root: previous_state.pk_index_root,
                    },
                );
                let pk_index_root = if db.config().persistent_pk_index && pk_locator_preserved {
                    previous_state.pk_index_root
                } else if db.config().persistent_pk_index {
                    build_persistent_pk_index_root(db, payload.as_slice())?
                } else {
                    None
                };
                replace_table_pk_index_root(self, db, &canonical_table_name, pk_index_root)?;
                if use_paged_row_storage || pointer.head_page_id == 0 {
                    self.resident_tombstone_locators_mut()
                        .remove(&canonical_table_name);
                } else if resident_tombstone_locators_preserved {
                    // Offsets are unchanged by in-place tombstone patches.
                } else if resident_delete_only {
                    self.resident_tombstone_locators_mut()
                        .remove(&canonical_table_name);
                } else {
                    let locators = build_resident_tombstone_locators_from_payload(&payload)?;
                    self.resident_tombstone_locators_mut()
                        .insert(canonical_table_name.clone(), Arc::new(locators));
                }
                self.cache_payload_insert(canonical_table_name, Arc::new(payload));
            }
            for table_name in removed_tables {
                let Some(state) = self.persisted_tables_mut().remove(&table_name) else {
                    continue;
                };
                self.overflow_chain_caches.remove(&table_name);
                self.deferred_paged_row_locator_caches_mut()
                    .remove(&table_name);
                self.resident_tombstone_locators_mut().remove(&table_name);
                self.cache_payload_remove(&table_name);
                free_persisted_table_bytes(&mut store, state)?;
                if state.pk_index_root.is_some() {
                    free_table_btree(&mut store, state.pk_index_root)?;
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
        self.dirty_tables_mut().clear();
        self.paged_mutations.clear();
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

    pub(crate) fn has_checkpoint_compaction_candidates<S: PageStore>(
        &self,
        store: &S,
        _config: &crate::config::DbConfig,
    ) -> Result<bool> {
        for state in self.persisted_tables.values() {
            if state.pointer.head_page_id == 0 {
                continue;
            }
            if state.pointer.is_table_paged_manifest() {
                if paged_table_state_needs_checkpoint_compaction(store, *state)? {
                    return Ok(true);
                }
                continue;
            }
            if state.pk_index_root.is_none()
                && !state.pointer.is_compressed()
                && usize::try_from(state.pointer.logical_len)
                    .ok()
                    .is_some_and(|len| len >= AUTO_MIN_PAYLOAD_BYTES)
            {
                return Ok(true);
            }
        }
        Ok(self.root_state.is_some_and(|root| {
            root.pointer.head_page_id != 0
                && !root.pointer.is_compressed()
                && usize::try_from(root.pointer.logical_len)
                    .ok()
                    .is_some_and(|len| len >= AUTO_MIN_PAYLOAD_BYTES)
        }))
    }

    pub(super) fn planner_catalog(&self) -> CatalogState {
        let mut catalog = self.catalog.as_ref().clone();
        for (name, table) in self.temp_tables.iter() {
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
        for (name, view) in self.temp_views.iter() {
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
        match compat_schema_qualified_name(name) {
            (Some(CompatSchemaQualifier::Main), _) => None,
            (_, object) => map_get_ci(&self.temp_tables, object),
        }
    }

    pub(super) fn temp_table_schema_mut(&mut self, name: &str) -> Option<&mut TableSchema> {
        let (qualifier, object) = compat_schema_qualified_name(name);
        if qualifier == Some(CompatSchemaQualifier::Main) {
            return None;
        }
        map_get_ci_mut(self.temp_tables_mut(), object)
    }

    pub(super) fn temp_table_data(&self, name: &str) -> Option<&TableData> {
        match compat_schema_qualified_name(name) {
            (Some(CompatSchemaQualifier::Main), _) => None,
            (_, object) => map_get_ci(&self.temp_table_data, object).map(|arc| arc.as_ref()),
        }
    }

    pub(super) fn temp_table_data_mut(&mut self, name: &str) -> Option<&mut TableData> {
        let (qualifier, object) = compat_schema_qualified_name(name);
        if qualifier == Some(CompatSchemaQualifier::Main) {
            return None;
        }
        self.entry_temp_table_data_mut(object)
    }

    pub(super) fn temp_view(&self, name: &str) -> Option<&ViewSchema> {
        match compat_schema_qualified_name(name) {
            (Some(CompatSchemaQualifier::Main), _) => None,
            (_, object) => map_get_ci(&self.temp_views, object),
        }
    }

    pub(super) fn visible_view(
        &self,
        name: &str,
        _scope: NameResolutionScope,
    ) -> Option<&ViewSchema> {
        let (qualifier, object) = compat_schema_qualified_name(name);
        if qualifier == Some(CompatSchemaQualifier::Main) {
            return self.catalog.view(object);
        }
        if let Some(view) = self.temp_view(name) {
            return Some(view);
        }
        if self.temp_table_schema(name).is_some() {
            return None;
        }
        if qualifier == Some(CompatSchemaQualifier::Temp) {
            None
        } else {
            self.catalog.view(object)
        }
    }

    pub(super) fn visible_table_is_temporary(&self, name: &str) -> bool {
        !self.temp_tables.is_empty() && self.temp_table_schema(name).is_some()
    }

    pub(super) fn table_schema_in_scope(
        &self,
        name: &str,
        _scope: NameResolutionScope,
    ) -> Option<&TableSchema> {
        let (qualifier, object) = compat_schema_qualified_name(name);
        if qualifier == Some(CompatSchemaQualifier::Main) {
            return self.catalog.table(object);
        }
        if self.temp_view(name).is_some() {
            return None;
        }
        if let Some(table) = self.temp_table_schema(name) {
            return Some(table);
        }
        if qualifier == Some(CompatSchemaQualifier::Temp) {
            None
        } else {
            self.catalog.table(object)
        }
    }

    pub(super) fn table_schema(&self, name: &str) -> Option<&TableSchema> {
        self.table_schema_in_scope(name, NameResolutionScope::Session)
    }

    pub(super) fn catalog_table_mut(&mut self, name: &str) -> Option<&mut TableSchema> {
        let (qualifier, object) = compat_schema_qualified_name(name);
        if qualifier == Some(CompatSchemaQualifier::Temp) {
            return None;
        }
        let catalog = self.catalog_mut();
        map_get_ci_mut(&mut catalog.tables, object)
    }

    pub(super) fn canonical_catalog_table_name(&self, name: &str) -> Option<String> {
        let (qualifier, object) = compat_schema_qualified_name(name);
        if qualifier == Some(CompatSchemaQualifier::Temp) {
            return None;
        }
        self.catalog.table(object).map(|table| table.name.clone())
    }

    pub(super) fn table_data_in_scope(
        &self,
        name: &str,
        _scope: NameResolutionScope,
    ) -> Option<&TableData> {
        let (qualifier, object) = compat_schema_qualified_name(name);
        if qualifier == Some(CompatSchemaQualifier::Main) {
            let table_name = self.catalog.table(object)?.name.clone();
            return self
                .tables
                .get(&table_name)
                .map(TableRowSource::resident_data);
        }
        if self.temp_view(name).is_some() {
            return None;
        }
        if let Some(table) = self.temp_table_schema(name) {
            return self.temp_table_data(&table.name);
        }
        if qualifier == Some(CompatSchemaQualifier::Temp) {
            None
        } else {
            let table_name = self.catalog.table(object)?.name.clone();
            self.tables
                .get(&table_name)
                .map(TableRowSource::resident_data)
        }
    }

    pub(super) fn table_row_source(&self, name: &str) -> Option<&TableRowSource> {
        let (qualifier, object) = compat_schema_qualified_name(name);
        if qualifier == Some(CompatSchemaQualifier::Temp) {
            return None;
        }
        if self.temp_view(name).is_some() {
            return None;
        }
        let table_name = self.catalog.table(object)?.name.clone();
        self.tables.get(&table_name)
    }

    pub(super) fn prepared_insert_target_loaded(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name) || self.temp_tables.contains_key(table_name)
    }

    fn visible_table_row_source_in_scope(
        &self,
        name: &str,
        _scope: NameResolutionScope,
    ) -> Option<VisibleTableRowSource<'_>> {
        let (qualifier, object) = compat_schema_qualified_name(name);
        if qualifier == Some(CompatSchemaQualifier::Main) {
            let table_name = self.catalog.table(object)?.name.clone();
            return self
                .tables
                .get(&table_name)
                .map(VisibleTableRowSource::Base);
        }
        if self.temp_view(name).is_some() {
            return None;
        }
        if let Some(table) = self.temp_table_schema(name) {
            return self
                .temp_table_data(&table.name)
                .map(VisibleTableRowSource::Temp);
        }
        if qualifier == Some(CompatSchemaQualifier::Temp) {
            None
        } else {
            let table_name = self.catalog.table(object)?.name.clone();
            self.tables
                .get(&table_name)
                .map(VisibleTableRowSource::Base)
        }
    }

    pub(crate) fn visible_table_row_source(&self, name: &str) -> Option<VisibleTableRowSource<'_>> {
        self.visible_table_row_source_in_scope(name, NameResolutionScope::Session)
    }

    pub(super) fn table_data(&self, name: &str) -> Option<&TableData> {
        self.table_data_in_scope(name, NameResolutionScope::Session)
    }

    pub(super) fn persisted_table_state(&self, name: &str) -> Option<PersistedTableState> {
        let table_name = self.catalog.table(name)?.name.clone();
        self.persisted_tables.get(&table_name).copied()
    }

    pub(super) fn table_data_mut_in_scope(
        &mut self,
        name: &str,
        _scope: NameResolutionScope,
    ) -> Option<&mut TableData> {
        let (qualifier, object) = compat_schema_qualified_name(name);
        if qualifier == Some(CompatSchemaQualifier::Main) {
            return self.entry_table_data_mut(object);
        }
        if self.temp_view(name).is_some() {
            return None;
        }
        if self.temp_table_schema(name).is_some() {
            return self.temp_table_data_mut(name);
        }
        if qualifier == Some(CompatSchemaQualifier::Temp) {
            None
        } else {
            self.entry_table_data_mut(object)
        }
    }

    pub(super) fn table_data_mut(&mut self, name: &str) -> Option<&mut TableData> {
        self.table_data_mut_in_scope(name, NameResolutionScope::Session)
    }

    pub(super) fn replace_table_row_source(
        &mut self,
        name: &str,
        row_source: TableRowSource,
    ) -> Result<()> {
        let Some(table_name) = self.canonical_catalog_table_name(name) else {
            return Err(DbError::internal(format!(
                "table row source for {name} is missing"
            )));
        };
        self.tables_mut().insert(table_name, row_source);
        Ok(())
    }

    pub(crate) fn has_redeferable_persisted_tables(&self, names: &[&str]) -> bool {
        names.iter().any(|name| {
            let Some(table_name) = self.canonical_catalog_table_name(name) else {
                return false;
            };
            self.persisted_tables
                .get(&table_name)
                .is_some_and(|state| state.pointer.is_table_paged_manifest())
                && self.tables.contains_key(&table_name)
        })
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

        // ADR 0143 Phase B: under per-table deferred materialization, an
        // index whose target table has not yet been loaded must not be
        // rebuilt — its rows are not in memory. If the index was still
        // resident from before the table was deferred, it remains valid;
        // otherwise the index is rebuilt when the table itself is
        // materialized and no longer appears in `deferred_tables`.
        let names = self
            .catalog
            .indexes
            .iter()
            .filter(|(name, index)| !index.fresh || !self.indexes.contains_key(*name))
            .filter(|(_, index)| {
                !self
                    .deferred_tables
                    .iter()
                    .any(|table_name| identifiers_equal(table_name, &index.table_name))
            })
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        for name in names {
            self.rebuild_index(&name, page_size)?;
        }
        Ok(())
    }

    pub(super) fn mark_table_dirty(&mut self, table_name: &str) {
        if self.visible_table_is_temporary(table_name) {
            return;
        }
        let Some(table_name) = self.canonical_catalog_table_name(table_name) else {
            return;
        };
        self.catalog_mut().table_stats.remove(&table_name);
        self.paged_mutations.remove(&table_name);
        self.dirty_tables_mut().insert(table_name);
    }

    pub(super) fn mark_table_row_dirty(
        &mut self,
        table_name: &str,
        _row_index: usize,
        row_id: i64,
        values: &[Value],
    ) {
        if self.visible_table_is_temporary(table_name) {
            return;
        }
        let Some(table_name) = self.canonical_catalog_table_name(table_name) else {
            return;
        };
        self.catalog_mut().table_stats.remove(&table_name);
        if self.dirty_tables.contains(&table_name)
            && !self.paged_mutations.contains_key(&table_name)
        {
            return;
        }
        self.dirty_tables_mut().insert(table_name.clone());
        self.paged_mutations
            .entry(table_name)
            .or_default()
            .updated_rows
            .insert(row_id, values.to_vec());
    }

    pub(super) fn mark_table_row_dirty_with_original_values(
        &mut self,
        table_name: &str,
        _row_index: usize,
        row_id: i64,
        original_values: &[Value],
        values: &[Value],
    ) {
        if self.visible_table_is_temporary(table_name) {
            return;
        }
        let Some(table_name) = self.canonical_catalog_table_name(table_name) else {
            return;
        };
        self.catalog_mut().table_stats.remove(&table_name);
        if self.dirty_tables.contains(&table_name)
            && !self.paged_mutations.contains_key(&table_name)
        {
            return;
        }

        let restores_original = self
            .paged_mutations
            .get(&table_name)
            .and_then(|delta| delta.original_rows.get(&row_id))
            .is_some_and(|original| original == values);

        if restores_original || values == original_values {
            if let Some(delta) = self.paged_mutations.get_mut(&table_name) {
                delta.updated_rows.remove(&row_id);
                delta.original_rows.remove(&row_id);
                if delta.updated_rows.is_empty()
                    && delta.deleted_rows.is_empty()
                    && delta.append_count == 0
                {
                    self.paged_mutations.remove(&table_name);
                    self.dirty_tables_mut().remove(&table_name);
                }
            }
            return;
        }

        self.dirty_tables_mut().insert(table_name.clone());
        let delta = self.paged_mutations.entry(table_name.clone()).or_default();
        delta
            .original_rows
            .entry(row_id)
            .or_insert_with(|| original_values.to_vec());
        delta.updated_rows.insert(row_id, values.to_vec());
    }

    pub(super) fn mark_table_row_deleted(&mut self, table_name: &str, row_id: i64) {
        if self.visible_table_is_temporary(table_name) {
            return;
        }
        let Some(table_name) = self.canonical_catalog_table_name(table_name) else {
            return;
        };
        self.catalog_mut().table_stats.remove(&table_name);
        if self.dirty_tables.contains(&table_name)
            && !self.paged_mutations.contains_key(&table_name)
        {
            return;
        }
        self.dirty_tables_mut().insert(table_name.clone());
        self.paged_mutations
            .entry(table_name)
            .or_default()
            .deleted_rows
            .insert(row_id);
    }

    pub(super) fn mark_table_rows_deleted(&mut self, table_name: &str, row_ids: &BTreeSet<i64>) {
        if row_ids.is_empty() || self.visible_table_is_temporary(table_name) {
            return;
        }
        let Some(table_name) = self.canonical_catalog_table_name(table_name) else {
            return;
        };
        self.catalog_mut().table_stats.remove(&table_name);
        if self.dirty_tables.contains(&table_name)
            && !self.paged_mutations.contains_key(&table_name)
        {
            return;
        }
        self.dirty_tables_mut().insert(table_name.clone());
        let deleted_rows = &mut self
            .paged_mutations
            .entry(table_name)
            .or_default()
            .deleted_rows;
        if deleted_rows.is_empty() {
            *deleted_rows = row_ids.clone();
        } else {
            deleted_rows.extend(row_ids.iter().copied());
        }
    }

    pub(super) fn mark_table_row_appended(&mut self, table_name: &str) {
        if self.visible_table_is_temporary(table_name) {
            return;
        }
        if let Some(delta) = self.paged_mutations.get_mut(table_name) {
            delta.append_count += 1;
            return;
        }
        if self.dirty_tables.contains(table_name) {
            return;
        }
        let Some(table_name) = self.canonical_catalog_table_name(table_name) else {
            return;
        };
        self.catalog_mut().table_stats.remove(&table_name);
        if let Some(delta) = self.paged_mutations.get_mut(table_name.as_str()) {
            delta.append_count += 1;
            return;
        }
        if self.dirty_tables.contains(table_name.as_str()) {
            return;
        }
        self.dirty_tables_mut().insert(table_name.clone());
        self.paged_mutations
            .entry(table_name)
            .or_default()
            .append_count += 1;
    }

    pub(super) fn mark_all_tables_dirty(&mut self) {
        let table_names = self.catalog.tables.keys().cloned().collect::<Vec<_>>();
        self.dirty_tables_mut().extend(table_names);
        self.paged_mutations.clear();
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
                if !statement.temporary {
                    self.rebuild_indexes(page_size)?;
                }
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
                if !statement.temporary {
                    self.rebuild_indexes(page_size)?;
                }
                Ok(result)
            }
            Statement::CreateIndex(statement) => {
                if let Some(index_name) = self.execute_create_index(statement, page_size)? {
                    self.rebuild_index(&index_name, page_size)?;
                }
                Ok(QueryResult::with_affected_rows(0))
            }
            Statement::AlterIndexRebuild { name } => {
                self.rebuild_index(name, page_size)?;
                Ok(QueryResult::with_affected_rows(0))
            }
            Statement::AlterIndexVerify { name } => {
                self.verify_index(name, page_size)?;
                Ok(QueryResult::with_affected_rows(0))
            }
            Statement::CreateView(statement) => {
                self.execute_create_view(statement)?;
                if !statement.temporary {
                    self.rebuild_indexes(page_size)?;
                }
                Ok(QueryResult::with_affected_rows(0))
            }
            Statement::CreateTrigger(statement) => {
                self.execute_create_trigger(statement)?;
                self.rebuild_indexes(page_size)?;
                Ok(QueryResult::with_affected_rows(0))
            }
            Statement::DropTable { name, if_exists } => {
                let temporary = self.temp_table_schema(name).is_some();
                self.execute_drop_table(name, *if_exists, page_size)?;
                if !temporary {
                    self.rebuild_indexes(page_size)?;
                }
                Ok(QueryResult::with_affected_rows(0))
            }
            Statement::DropIndex { name, if_exists } => {
                self.execute_drop_index(name, *if_exists)?;
                self.rebuild_indexes(page_size)?;
                Ok(QueryResult::with_affected_rows(0))
            }
            Statement::DropView { name, if_exists } => {
                let temporary = self.temp_view(name).is_some();
                self.execute_drop_view(name, *if_exists)?;
                if !temporary {
                    self.rebuild_indexes(page_size)?;
                }
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
        let (qualifier, object_name) = compat_schema_qualified_name(&statement.table_name);
        if statement.temporary && qualifier == Some(CompatSchemaQualifier::Main) {
            return Err(DbError::sql(
                "temporary tables cannot be created in the main schema",
            ));
        }
        let temporary = statement.temporary || qualifier == Some(CompatSchemaQualifier::Temp);
        let table_name = object_name.to_string();
        if temporary {
            if self.temp_relation_exists(&table_name) {
                if statement.if_not_exists && self.temp_table_schema(&table_name).is_some() {
                    return Ok(QueryResult::with_affected_rows(0));
                }
                return Err(DbError::sql(format!(
                    "object {} already exists",
                    table_name
                )));
            }
        } else if self.catalog.contains_object(&table_name) {
            if statement.if_not_exists && self.catalog.table(&table_name).is_some() {
                return Ok(QueryResult::with_affected_rows(0));
            }
            return Err(DbError::sql(format!(
                "object {} already exists",
                table_name
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
                spatial_type: None,
                enum_type: None,
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
            table_name: table_name.clone(),
            temporary,
            if_not_exists: false,
            columns,
            constraints: Vec::new(),
        };
        self.execute_create_table(&create_statement)?;
        if !statement.with_data {
            return Ok(QueryResult::with_affected_rows(0));
        }

        let temporary = self.visible_table_is_temporary(&table_name);
        let mut affected_rows = 0_u64;
        for source_row in source.take_rows() {
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
                    self.catalog_mut()
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
                .push_row(stored_row);
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
        self.clear_fts_eval_context()?;
        match statement {
            Statement::Query(query) => {
                if self.security_rules_active()? {
                    return self
                        .evaluate_query(query, params, &BTreeMap::new())
                        .map(dataset_to_result);
                }
                if let Some(result) = Self::try_execute_simple_integer_series_query(query) {
                    return Ok(result);
                }
                if let Some(result) = self.try_execute_simple_count_query(query, params)? {
                    return Ok(result);
                }
                if let Some(result) = self.try_execute_simple_min_max_query(query)? {
                    return Ok(result);
                }
                if let Some(result) =
                    self.try_execute_simple_indexed_projection_query(query, params)?
                {
                    return Ok(result);
                }
                if let Some(result) =
                    self.try_execute_simple_distinct_filtered_projection_query(query, params)?
                {
                    return Ok(result);
                }
                if let Some(result) =
                    self.try_execute_simple_distinct_projection_query(query, params)?
                {
                    return Ok(result);
                }
                if let Some(result) =
                    self.try_execute_simple_filtered_projection_query(query, params)?
                {
                    return Ok(result);
                }
                if let Some(result) =
                    self.try_execute_simple_expression_projection_query(query, params)?
                {
                    return Ok(result);
                }
                if let Some(result) =
                    self.try_execute_simple_table_projection_query(query, params)?
                {
                    return Ok(result);
                }
                if let Some(result) =
                    self.try_execute_left_join_status_aggregate_query(query, params)?
                {
                    return Ok(result);
                }
                if let Some(result) = self.try_execute_crm_revenue_raw_aggregate_query(query)? {
                    return Ok(result);
                }
                if let Some(result) = self.try_execute_left_join_aggregate_query(query, params)? {
                    return Ok(result);
                }
                if let Some(result) = self.try_execute_simple_grouped_count_query(query, params)? {
                    return Ok(result);
                }
                if let Some(result) =
                    self.try_execute_simple_grouped_numeric_aggregate_query(query, params)?
                {
                    return Ok(result);
                }
                if let Some(result) =
                    self.try_execute_three_table_genre_popularity_query(query, params)?
                {
                    return Ok(result);
                }
                if let Some(result) = self.try_execute_movie_tag_search_query(query, params)? {
                    return Ok(result);
                }
                if let Some(result) = self.try_execute_movie_watchlist_query(query, params)? {
                    return Ok(result);
                }
                if let Some(result) =
                    self.try_execute_movie_top_rated_by_year_query(query, params)?
                {
                    return Ok(result);
                }
                if let Some(result) = self.try_execute_movie_busiest_people_query(query, params)? {
                    return Ok(result);
                }
                if let Some(result) = self.try_execute_showdown_window_query(query)? {
                    return Ok(result);
                }
                if let Some(result) =
                    self.try_execute_showdown_directors_cte_query(query, params)?
                {
                    return Ok(result);
                }
                if let Some(result) = self.try_execute_general_grouped_query(query, params)? {
                    return Ok(result);
                }
                if let Some(result) =
                    self.try_execute_indexed_join_grouped_count_query(query, params)?
                {
                    return Ok(result);
                }
                if let Some(result) =
                    self.try_execute_simple_view_projection_limit_query(query, params)?
                {
                    return Ok(result);
                }
                if let Some(result) =
                    self.try_execute_indexed_join_limit_projection_query(query, params)?
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
                    self.try_execute_three_table_indexed_join_projection_query(query, params)?
                {
                    return Ok(result);
                }
                if let Some(result) = self.try_execute_base_table_join(query, params)? {
                    return Ok(result);
                }
                self.evaluate_query(query, params, &BTreeMap::new())
                    .map(dataset_to_result)
            }
            Statement::Explain(explain) => {
                let mut planner_catalog = self.planner_catalog();
                if self.security_rules_active()? {
                    for index in planner_catalog.indexes.values_mut() {
                        if index.kind == IndexKind::Btree {
                            index.fresh = false;
                        }
                    }
                }
                match explain.statement.as_ref() {
                    Statement::Update(update) => {
                        if explain.analyze {
                            return Err(DbError::sql(
                                "EXPLAIN ANALYZE is not supported for UPDATE".to_string(),
                            ));
                        }
                        if self
                            .visible_view(&update.table_name, NameResolutionScope::Session)
                            .is_some()
                        {
                            return Err(DbError::sql(format!(
                                "EXPLAIN UPDATE is not supported for view {}",
                                update.table_name
                            )));
                        }

                        let mut lines = vec![format!("Mutation: UPDATE {}", update.table_name)];
                        for (index, assignment) in update.assignments.iter().enumerate() {
                            lines.push(format!(
                                "Assignment {}: {} = {}",
                                index + 1,
                                assignment.column_name,
                                assignment.expr.to_sql()
                            ));
                        }
                        match &update.filter {
                            Some(filter) => lines.push(format!("Filter: {}", filter.to_sql())),
                            None => lines.push("Filter: <none>".to_string()),
                        }
                        let table = self.table_schema(&update.table_name).ok_or_else(|| {
                            DbError::sql(format!("unknown table {}", update.table_name))
                        })?;
                        let candidate_rows = dml::matching_row_ids(
                            self,
                            &update.table_name,
                            &update.table_name,
                            table,
                            update.filter.as_ref(),
                            params,
                        )?
                        .len();
                        lines.push(format!("Candidate rows: {candidate_rows}"));
                        lines.push(format!(
                            "Returning: {}",
                            if update.returning.is_empty() {
                                "OFF"
                            } else {
                                "ON"
                            }
                        ));

                        Ok(QueryResult::with_explain(lines))
                    }
                    _ => {
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
                }
            }
            other => Err(DbError::internal(format!(
                "read-only execution received mutating statement {other:?}"
            ))),
        }
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
            .table_row_source(table_name)
            .map(TableRowSource::row_count)
            .or_else(|| self.table_data(table_name).map(|table| table.rows.len()))
            .map(i64::try_from)
            .transpose()
            .map_err(|_| {
                DbError::sql(format!(
                    "table {table_name} exceeds ANALYZE row-count limits"
                ))
            })?
            .unwrap_or(0);
        self.catalog_mut()
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
            self.catalog_mut().index_stats.remove(&index_name);
            let Some(index) = self.catalog.index(&index_name).cloned() else {
                continue;
            };
            let (entry_count, distinct_key_count) = match self.index(&index.name) {
                Some(RuntimeIndex::Btree { keys, .. }) => {
                    let entry_count = i64::try_from(keys.total_row_id_count()).map_err(|_| {
                        DbError::sql(format!(
                            "index {} exceeds ANALYZE entry-count limits",
                            index.name
                        ))
                    })?;
                    let distinct_key_count =
                        i64::try_from(keys.distinct_key_count()).map_err(|_| {
                            DbError::sql(format!(
                                "index {} exceeds ANALYZE distinct-count limits",
                                index.name
                            ))
                        })?;
                    (entry_count, distinct_key_count)
                }
                Some(RuntimeIndex::Spatial { index: spatial }) => {
                    let entry_count = i64::try_from(spatial.len()).map_err(|_| {
                        DbError::sql(format!(
                            "index {} exceeds ANALYZE entry-count limits",
                            index.name
                        ))
                    })?;
                    (entry_count, entry_count)
                }
                Some(RuntimeIndex::Trigram { .. }) | None => continue,
                Some(RuntimeIndex::FullText { index: fulltext }) => {
                    let entry_count = i64::try_from(fulltext.entry_count()).map_err(|_| {
                        DbError::sql(format!(
                            "index {} exceeds ANALYZE entry-count limits",
                            index.name
                        ))
                    })?;
                    let distinct_key_count =
                        i64::try_from(fulltext.term_count()).map_err(|_| {
                            DbError::sql(format!(
                                "index {} exceeds ANALYZE distinct-count limits",
                                index.name
                            ))
                        })?;
                    (entry_count, distinct_key_count)
                }
            };
            self.catalog_mut().index_stats.insert(
                index.name.clone(),
                IndexStats {
                    entry_count,
                    distinct_key_count,
                },
            );
        }
        Ok(())
    }

    fn execute_validated_simple_row_id_projection_at_snapshot(
        &self,
        request: ValidatedSimpleRowIdProjectionRequest<'_>,
    ) -> Result<Option<QueryResult>> {
        let table_schema = request.table_schema;
        let canonical_table_name = table_schema.name.as_str();
        if let Some(result) = self.try_execute_validated_resident_simple_row_id_projection(
            table_schema,
            request.projection_indexes,
            Arc::clone(&request.column_names),
            request.lookup_row_id,
        )? {
            return Ok(Some(result));
        }

        if !self.has_deferred_tables()
            || !self
                .deferred_table_names()
                .any(|candidate| identifiers_equal(candidate, canonical_table_name))
        {
            return Ok(None);
        }
        let Some(state) = self.persisted_table_state(canonical_table_name) else {
            return Ok(None);
        };
        let paged_locator_cache = self
            .catalog
            .table(canonical_table_name)
            .and_then(|table| self.deferred_paged_row_locator_caches.get(&table.name))
            .map(|cache| cache.as_ref());
        let store = SnapshotPageStore {
            pager: request.pager,
            wal: request.wal,
            snapshot_lsn: request.snapshot_lsn,
        };
        let rows = read_deferred_projected_values_by_id(
            &store,
            state,
            table_schema,
            request.lookup_row_id,
            request.use_persistent_pk_index,
            paged_locator_cache,
            request.projection_indexes,
        )?
        .map(|values| vec![QueryRow::new(values)])
        .unwrap_or_default();
        Ok(Some(QueryResult::with_shared_columns(
            Arc::clone(&request.column_names),
            rows,
        )))
    }

    #[allow(clippy::too_many_arguments)]
    fn stream_deferred_view_linear_three_table_rows_from_root<S: PageStore, F>(
        &self,
        store: &S,
        table_row_readers: &[DeferredViewTableRowReader<'_>],
        join_steps: &[DeferredViewJoinStep],
        join_keys: &[&RuntimeBtreeKeys],
        key_projection_indexes: &[Option<usize>],
        table_projections: &[DeferredViewTableProjection],
        root_row: &StoredRow,
        use_persistent_pk_index: bool,
        chunk_payload_cache: &mut HashMap<CachedPagedChunkPayloadKey, Arc<Vec<u8>>>,
        visit: &mut F,
    ) -> Result<Option<bool>>
    where
        F: FnMut(&StoredRow, &StoredRow, StoredRow) -> Result<bool>,
    {
        if table_row_readers.len() != 3
            || join_steps.len() != 2
            || table_projections.len() != 3
            || join_keys.len() != 2
            || key_projection_indexes.len() != 2
        {
            return Ok(None);
        }
        let step0 = &join_steps[0];
        let step1 = &join_steps[1];
        if step0.previous_table_index != 0
            || step0.current_table_index != 1
            || step1.previous_table_index != 1
            || step1.current_table_index != 2
        {
            return Ok(None);
        }
        let [keys0, keys1] = join_keys else {
            return Ok(None);
        };
        let [key0_projection_index, key1_projection_index] = key_projection_indexes else {
            return Ok(None);
        };
        let key0_row_ids = match *key0_projection_index {
            Some(key0_projection_index) => {
                let Some(key0_value) = root_row.values.get(key0_projection_index) else {
                    return Err(DbError::internal(
                        "deferred view linear join projection row is shorter than planned schema",
                    ));
                };
                if matches!(key0_value, Value::Null) {
                    return Ok(Some(false));
                }
                keys0.row_ids_for_value_set(key0_value)?
            }
            None => keys0.row_ids_for_row_id(root_row.row_id),
        };

        let stopped = key0_row_ids
            .visit_until(|row1_id| {
                let Some(row1) = table_row_readers[1].read_projected_with_chunk_cache(
                    store,
                    row1_id,
                    use_persistent_pk_index,
                    &table_projections[1].projection_indexes,
                    chunk_payload_cache,
                )?
                else {
                    return Ok(false);
                };
                let key1_row_ids = match *key1_projection_index {
                    Some(key1_projection_index) => {
                        let Some(key1_value) = row1.values.get(key1_projection_index) else {
                            return Err(DbError::internal(
                                "deferred view linear join projection row is shorter than planned schema",
                            ));
                        };
                        if matches!(key1_value, Value::Null) {
                            return Ok(false);
                        }
                        keys1.row_ids_for_value_set(key1_value)?
                    }
                    None => keys1.row_ids_for_row_id(row1.row_id),
                };
                key1_row_ids
                    .visit_until(|row2_id| {
                        let Some(row2) = table_row_readers[2].read_projected_with_chunk_cache(
                            store,
                            row2_id,
                            use_persistent_pk_index,
                            &table_projections[2].projection_indexes,
                            chunk_payload_cache,
                        )?
                        else {
                            return Ok(false);
                        };
                        visit(root_row, &row1, row2)
                    })
            })?;
        Ok(Some(stopped))
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

        if let Some(dataset) =
            self.try_execute_simple_union_range_projection_query(query, params, &ctes)?
        {
            return Ok(dataset);
        }

        let mut sorted_during_select = false;
        let mut dataset = match &query.body {
            QueryBody::Select(select) => {
                if let Some(dataset) = self.try_fulltext_bm25_top_k_select(
                    select,
                    &query.order_by,
                    query.limit.as_ref(),
                    query.offset.as_ref(),
                    params,
                    &ctes,
                )? {
                    return Ok(dataset);
                }
                if select_requires_grouped_evaluation(self, select)? {
                    self.evaluate_select(select, params, &ctes)?
                } else {
                    let projection_order_by =
                        projection_order_by_plan(&query.order_by, &select.projection);
                    let mut source = self.build_select_dataset(select, params, &ctes)?;
                    if !query.order_by.is_empty() && projection_order_by.is_none() {
                        self.sort_dataset(&mut source, &query.order_by, params, &ctes)?;
                        sorted_during_select = true;
                    }
                    let mut projected =
                        self.project_dataset(&source, &select.projection, params, &ctes, None)?;
                    if let Some(order_by_plan) = projection_order_by.as_deref() {
                        sort_dataset_by_projection_order(
                            Some(self),
                            &mut projected,
                            order_by_plan,
                        )?;
                        sorted_during_select = true;
                    }
                    projected
                }
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
                let iter = dataset.take_rows().into_iter().skip(start);
                match limit {
                    Some(limit) => iter
                        .take(usize::try_from(limit.max(0)).unwrap_or(0))
                        .collect(),
                    None => iter.collect(),
                }
            };
            dataset.set_rows(rows);
        }
        Ok(dataset)
    }

    fn evaluate_recursive_cte(
        &self,
        cte: &CommonTableExpr,
        params: &[Value],
        inherited_ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Dataset> {
        if let Some(dataset) = Self::try_evaluate_simple_integer_series_cte(cte) {
            return Ok(dataset);
        }
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
            let rows = anchor.take_rows();
            anchor.set_rows(deduplicate_rows(rows)?);
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
                for row in recursive_rows.into_rows() {
                    let identity = row_identity(&row)?;
                    if seen.insert(identity) {
                        rows.push(row);
                    }
                }
                rows
            } else {
                recursive_rows.into_rows()
            };

            if next_rows.is_empty() {
                return Ok(result);
            }

            result.rows_mut().extend(next_rows.clone());
            working.set_rows(next_rows);
        }

        Err(DbError::sql(format!(
            "recursive CTE {} exceeded the {} iteration limit",
            cte.name, RECURSIVE_CTE_MAX_ITERATIONS
        )))
    }

    fn try_evaluate_simple_integer_series_cte(cte: &CommonTableExpr) -> Option<Dataset> {
        let (column_name, start, step, upper_exclusive) = Self::simple_integer_series_bounds(cte)?;
        let mut rows = Vec::new();
        let mut value = start;
        rows.push(vec![Value::Int64(value)]);
        while value < upper_exclusive {
            if rows.len() >= RECURSIVE_CTE_MAX_ITERATIONS {
                return None;
            }
            value = value.checked_add(step)?;
            rows.push(vec![Value::Int64(value)]);
        }

        Some(Dataset::with_rows(
            vec![ColumnBinding::visible(Some(cte.name.clone()), column_name)],
            rows,
        ))
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
            QueryBody::Select(select) => {
                if let Some(dataset) = self.try_fulltext_bm25_top_k_select(
                    select,
                    &query.order_by,
                    query.limit.as_ref(),
                    query.offset.as_ref(),
                    params,
                    &ctes,
                )? {
                    return Ok(dataset);
                }
                if select_requires_grouped_evaluation(self, select)? {
                    self.evaluate_select_with_outer(
                        select,
                        params,
                        &ctes,
                        outer_dataset,
                        outer_row,
                    )?
                } else {
                    let projection_order_by =
                        projection_order_by_plan(&query.order_by, &select.projection);
                    let mut source = self.build_select_dataset_with_outer(
                        select,
                        params,
                        &ctes,
                        outer_dataset,
                        outer_row,
                    )?;
                    if !query.order_by.is_empty() && projection_order_by.is_none() {
                        self.sort_dataset(&mut source, &query.order_by, params, &ctes)?;
                        sorted_during_select = true;
                    }
                    let mut projected =
                        self.project_dataset(&source, &select.projection, params, &ctes, None)?;
                    if let Some(order_by_plan) = projection_order_by.as_deref() {
                        sort_dataset_by_projection_order(
                            Some(self),
                            &mut projected,
                            order_by_plan,
                        )?;
                        sorted_during_select = true;
                    }
                    projected
                }
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
                let iter = dataset.take_rows().into_iter().skip(start);
                match limit {
                    Some(limit) => iter
                        .take(usize::try_from(limit.max(0)).unwrap_or(0))
                        .collect(),
                    None => iter.collect(),
                }
            };
            dataset.set_rows(rows);
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
        if select_requires_grouped_evaluation(self, select)? {
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
        if select_requires_grouped_evaluation(self, select)? {
            self.evaluate_grouped_select(select, dataset, params, ctes)
        } else {
            self.project_dataset(&dataset, &select.projection, params, ctes, None)
        }
    }

    fn build_select_dataset(
        &self,
        select: &Select,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Dataset> {
        let has_lateral = select.from.iter().any(from_item_contains_lateral);
        let mut dataset = if !has_lateral {
            if let Some(dataset) = self.try_view_filter_pushdown(select, params, ctes)? {
                dataset
            } else if let Some(dataset) = self.try_indexed_scan(select, params, ctes)? {
                dataset
            } else if let Some(dataset) = self.try_spatial_join(select, params, ctes)? {
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
            let filter_dataset = Dataset::with_rows(dataset.columns.clone(), Vec::new());
            let mut filtered = Vec::with_capacity(dataset.rows.len());
            for row in dataset.take_rows() {
                if matches!(
                    self.eval_expr(filter, &filter_dataset, &row, params, ctes, None)?,
                    Value::Bool(true)
                ) {
                    filtered.push(row);
                }
            }
            dataset.set_rows(filtered);
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
            let filter_dataset = Dataset::with_rows(dataset.columns.clone(), Vec::new());
            let mut filtered = Vec::with_capacity(dataset.rows.len());
            for row in dataset.take_rows() {
                if matches!(
                    self.eval_expr(filter, &filter_dataset, &row, params, ctes, None)?,
                    Value::Bool(true)
                ) {
                    filtered.push(row);
                }
            }
            dataset.set_rows(filtered);
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
            return Ok(Dataset::with_rows(Vec::new(), vec![Vec::new()]));
        }
        let mut iter = from.iter();
        let cross_constraint = JoinConstraint::On(Expr::Literal(Value::Bool(true)));
        // Invariant: iterator is non-empty due to the guard above.
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RootHeader {
    schema_cookie: u32,
    payload_checksum: u32,
    pointer: OverflowPointer,
}

struct SimpleIndexedProjectionPlan<'a> {
    table_name: &'a str,
    table_schema: &'a TableSchema,
    filter_column: &'a str,
    lookup_value: Value,
    extra_lookup_terms: Vec<(&'a str, Value)>,
    projection_indexes: Vec<usize>,
    column_names: Vec<String>,
    order_by: Option<Vec<SimpleOrderByPlan>>,
    limit: Option<usize>,
    offset: usize,
}

struct SimpleUnionRangeProjectionSide {
    table_name: String,
    alias: Option<String>,
    projection_indexes: Vec<usize>,
    column_names: Vec<String>,
    filter_column_index: usize,
    lower_bound: Option<SimpleRangeBoundValue>,
    upper_bound: Option<SimpleRangeBoundValue>,
}

struct LeftJoinStatusAggregatePlan<'a> {
    parent_table_name: &'a str,
    parent_join_index: usize,
    child_table_name: &'a str,
    child_join_index: usize,
    child_status_index: usize,
    child_id_index: usize,
    child_index_name: Option<String>,
    group_column_indexes: Vec<usize>,
    column_names: Vec<String>,
    order_by: Option<Vec<SimpleOrderByPlan>>,
    limit: Option<usize>,
    offset: usize,
}

fn add_status_aggregate_child_row(
    counts: &mut LeftJoinStatusCounts,
    child_values: &[Value],
    plan: &LeftJoinStatusAggregatePlan<'_>,
) -> Result<()> {
    let Some(child_status) = child_values.get(plan.child_status_index) else {
        return Err(DbError::internal("child join row is shorter than schema"));
    };
    let Some(child_id) = child_values.get(plan.child_id_index) else {
        return Err(DbError::internal("child join row is shorter than schema"));
    };
    counts.add_child(child_status, child_id)
}

#[derive(Clone, Copy, Debug, Default)]
struct LeftJoinStatusCounts {
    open_count: i64,
    in_progress_count: i64,
    resolved_count: i64,
    closed_count: i64,
    total_count: i64,
}

impl LeftJoinStatusCounts {
    fn bump(value: &mut i64) -> Result<()> {
        *value = value
            .checked_add(1)
            .ok_or_else(|| DbError::sql("aggregate count exceeds INT64 limits"))?;
        Ok(())
    }

    fn add_child(&mut self, status: &Value, id: &Value) -> Result<()> {
        if let Value::Text(status) = status {
            match status.as_str() {
                "open" => Self::bump(&mut self.open_count)?,
                "in_progress" => Self::bump(&mut self.in_progress_count)?,
                "resolved" => Self::bump(&mut self.resolved_count)?,
                "closed" => Self::bump(&mut self.closed_count)?,
                _ => {}
            }
        }
        if !matches!(id, Value::Null) {
            Self::bump(&mut self.total_count)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
enum IndexedJoinAggregateKind {
    CountRows,
    CountNonNull(usize),
    CountDistinct(usize),
    Sum(usize),
    Avg(usize),
    Min(usize),
    Max(usize),
}

#[allow(dead_code)]
pub(crate) struct LeftJoinAggregatePlan<'a> {
    parent_table_name: &'a str,
    parent_join_index: usize,
    child_table_name: &'a str,
    child_join_index: usize,
    child_index_name: Option<String>,
    group_column_indexes: Vec<usize>,
    aggregate_kinds: Vec<IndexedJoinAggregateKind>,
    column_names: Vec<String>,
    order_by: Option<Vec<SimpleOrderByPlan>>,
    limit: Option<usize>,
    offset: usize,
    include_empty_parent: bool,
}

struct ThreeTableGenrePopularityPlan<'a> {
    genre_table_name: &'a str,
    genre_id_index: usize,
    genre_name_index: usize,
    bridge_table_name: &'a str,
    bridge_movie_id_index: usize,
    bridge_genre_index_name: String,
    movie_table_name: &'a str,
    movie_rating_index: usize,
    movie_index_name: Option<String>,
    movie_id_is_rowid_alias: bool,
    column_names: Vec<String>,
    order_by: Option<Vec<SimpleOrderByPlan>>,
    limit: Option<usize>,
    offset: usize,
}

struct MovieTagSearchPlan<'a> {
    tag_table_name: &'a str,
    tag_id_index: usize,
    tag_name_index_name: String,
    tag_name_value: Value,
    bridge_table_name: &'a str,
    bridge_movie_id_index: usize,
    bridge_tag_index_name: String,
    movie_table_name: &'a str,
    movie_index_name: Option<String>,
    movie_id_is_rowid_alias: bool,
    projection_indexes: Vec<usize>,
    column_names: Vec<String>,
    order_by: Option<Vec<SimpleOrderByPlan>>,
    limit: Option<usize>,
    offset: usize,
}

struct MovieWatchlistPlan<'a> {
    watchlist_table_name: &'a str,
    watchlist_movie_id_index: usize,
    watchlist_priority_index: usize,
    watchlist_user_index_name: String,
    user_handle_value: Value,
    movie_table_name: &'a str,
    movie_id_index: usize,
    movie_title_index: usize,
    movie_index_name: Option<String>,
    movie_id_is_rowid_alias: bool,
    review_table_name: &'a str,
    review_score_index: usize,
    review_movie_index_name: String,
    column_names: Vec<String>,
    order_by: Option<Vec<SimpleOrderByPlan>>,
    limit: Option<usize>,
    offset: usize,
}

struct MovieTopRatedByYearPlan<'a> {
    movie_table_name: &'a str,
    movie_id_index: usize,
    movie_release_year_index: usize,
    movie_release_year_index_name: Option<String>,
    movie_projection_indexes: Vec<usize>,
    release_year_value: Value,
    review_table_name: &'a str,
    review_score_index: usize,
    review_movie_index_name: String,
    min_review_count: i64,
    column_names: Vec<String>,
    order_by: Option<Vec<SimpleOrderByPlan>>,
    limit: Option<usize>,
    offset: usize,
}

struct MovieBusiestPeoplePlan<'a> {
    people_table_name: &'a str,
    people_projection_indexes: Vec<usize>,
    people_index_name: Option<String>,
    people_id_is_rowid_alias: bool,
    roles_person_index_name: String,
    column_names: Vec<String>,
    limit: Option<usize>,
    offset: usize,
}

struct MovieBusiestPeopleCount {
    person_key: RuntimeBtreeKey,
    role_count: i64,
}

struct DirectorsCtePlan<'a> {
    roles_table_name: &'a str,
    role_person_id_index: usize,
    role_movie_id_index: usize,
    role_job_index: usize,
    director_job: String,
    movie_table_name: &'a str,
    movie_title_index: usize,
    movie_rating_index: usize,
    movie_index_name: Option<String>,
    movie_id_is_rowid_alias: bool,
    min_films: i64,
    title_separator: String,
    column_names: Vec<String>,
    order_by: Option<Vec<SimpleOrderByPlan>>,
    limit: Option<usize>,
    offset: usize,
}

struct DirectedMoviesCtePlan<'a> {
    roles_table_name: &'a str,
    role_person_id_index: usize,
    role_movie_id_index: usize,
    role_job_index: usize,
    director_job: String,
    movie_table_name: &'a str,
    movie_title_index: usize,
    movie_rating_index: usize,
    movie_index_name: Option<String>,
    movie_id_is_rowid_alias: bool,
}

struct DirectorsTopDirsCtePlan {
    min_films: i64,
}

type DirectorsFinalSelectAnalysis = (
    Vec<String>,
    Option<Vec<SimpleOrderByPlan>>,
    Option<usize>,
    usize,
    String,
);

struct DirectorsCteAccumulator {
    person_id: Value,
    films: i64,
    rating_sum: f64,
    rating_count: i64,
    titles: Vec<String>,
}

impl DirectorsCteAccumulator {
    fn new(person_id: Value) -> Self {
        Self {
            person_id,
            films: 0,
            rating_sum: 0.0,
            rating_count: 0,
            titles: Vec::new(),
        }
    }

    fn add_movie(&mut self, movie_values: &[Value], title_index: usize, rating_index: usize) {
        self.films = self.films.saturating_add(1);
        if let Some(rating) = movie_values
            .get(rating_index)
            .and_then(indexed_join_aggregate_as_f64)
        {
            self.rating_sum += rating;
            self.rating_count = self.rating_count.saturating_add(1);
        }
        if let Some(Value::Text(title)) = movie_values.get(title_index) {
            self.titles.push(title.clone());
        }
    }
}

struct IndexedJoinAggregateState {
    accumulators: Vec<IndexedJoinAccumulator>,
}

enum IndexedJoinAccumulator {
    CountRows { count: i64 },
    CountNonNull { col: usize, count: i64 },
    CountDistinct { col: usize, seen: BTreeSet<Vec<u8>> },
    Sum { col: usize, sum: f64, count: i64 },
    Avg { col: usize, sum: f64, count: i64 },
    Min { col: usize, value: Option<Value> },
    Max { col: usize, value: Option<Value> },
}

impl IndexedJoinAggregateState {
    fn new(kinds: &[IndexedJoinAggregateKind]) -> Self {
        let accumulators = kinds
            .iter()
            .map(|kind| match kind {
                IndexedJoinAggregateKind::CountRows => {
                    IndexedJoinAccumulator::CountRows { count: 0 }
                }
                IndexedJoinAggregateKind::CountNonNull(col) => {
                    IndexedJoinAccumulator::CountNonNull {
                        col: *col,
                        count: 0,
                    }
                }
                IndexedJoinAggregateKind::CountDistinct(col) => {
                    IndexedJoinAccumulator::CountDistinct {
                        col: *col,
                        seen: BTreeSet::new(),
                    }
                }
                IndexedJoinAggregateKind::Sum(col) => IndexedJoinAccumulator::Sum {
                    col: *col,
                    sum: 0.0,
                    count: 0,
                },
                IndexedJoinAggregateKind::Avg(col) => IndexedJoinAccumulator::Avg {
                    col: *col,
                    sum: 0.0,
                    count: 0,
                },
                IndexedJoinAggregateKind::Min(col) => IndexedJoinAccumulator::Min {
                    col: *col,
                    value: None,
                },
                IndexedJoinAggregateKind::Max(col) => IndexedJoinAccumulator::Max {
                    col: *col,
                    value: None,
                },
            })
            .collect();
        Self { accumulators }
    }

    fn accumulate(&mut self, child_values: &[Value]) -> Result<()> {
        for acc in &mut self.accumulators {
            match acc {
                IndexedJoinAccumulator::CountRows { count } => {
                    *count = count.saturating_add(1);
                }
                IndexedJoinAccumulator::CountNonNull { col, count } => {
                    if let Some(value) = child_values.get(*col) {
                        if !matches!(value, Value::Null) {
                            *count = count.saturating_add(1);
                        }
                    }
                }
                IndexedJoinAccumulator::CountDistinct { col, seen } => {
                    if let Some(value) = child_values.get(*col) {
                        if !matches!(value, Value::Null) {
                            seen.insert(row_identity(std::slice::from_ref(value))?);
                        }
                    }
                }
                IndexedJoinAccumulator::Sum { col, sum, count } => {
                    if let Some(value) = child_values.get(*col) {
                        if let Some(f) = indexed_join_aggregate_as_f64(value) {
                            *sum += f;
                            *count = count.saturating_add(1);
                        }
                    }
                }
                IndexedJoinAccumulator::Avg { col, sum, count } => {
                    if let Some(value) = child_values.get(*col) {
                        if let Some(f) = indexed_join_aggregate_as_f64(value) {
                            *sum += f;
                            *count = count.saturating_add(1);
                        }
                    }
                }
                IndexedJoinAccumulator::Min { col, value } => {
                    if let Some(v) = child_values.get(*col) {
                        if !matches!(v, Value::Null) {
                            match value {
                                None => *value = Some(v.clone()),
                                Some(curr) => {
                                    if compare_values_no_error(v, curr)
                                        == Some(std::cmp::Ordering::Less)
                                    {
                                        *value = Some(v.clone());
                                    }
                                }
                            }
                        }
                    }
                }
                IndexedJoinAccumulator::Max { col, value } => {
                    if let Some(v) = child_values.get(*col) {
                        if !matches!(v, Value::Null) {
                            match value {
                                None => *value = Some(v.clone()),
                                Some(curr) => {
                                    if compare_values_no_error(v, curr)
                                        == Some(std::cmp::Ordering::Greater)
                                    {
                                        *value = Some(v.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn finalize_into(self, output: &mut Vec<Value>) {
        for acc in self.accumulators {
            match acc {
                IndexedJoinAccumulator::CountRows { count } => {
                    output.push(Value::Int64(count));
                }
                IndexedJoinAccumulator::CountNonNull { count, .. } => {
                    output.push(Value::Int64(count));
                }
                IndexedJoinAccumulator::CountDistinct { seen, .. } => {
                    output.push(Value::Int64(seen.len() as i64));
                }
                IndexedJoinAccumulator::Sum { sum, count, .. } => {
                    if count == 0 {
                        output.push(Value::Null);
                    } else {
                        output.push(Value::Float64(sum));
                    }
                }
                IndexedJoinAccumulator::Avg { sum, count, .. } => {
                    if count == 0 {
                        output.push(Value::Null);
                    } else {
                        output.push(Value::Float64(sum / count as f64));
                    }
                }
                IndexedJoinAccumulator::Min { value, .. } => {
                    output.push(value.unwrap_or(Value::Null));
                }
                IndexedJoinAccumulator::Max { value, .. } => {
                    output.push(value.unwrap_or(Value::Null));
                }
            }
        }
    }
}

fn indexed_join_aggregate_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Int64(v) => Some(*v as f64),
        Value::Float64(v) => Some(*v),
        Value::Decimal { scaled, scale } => {
            let scaled_u = if *scaled >= 0 {
                *scaled as u64
            } else {
                return None;
            };
            let divisor = 10u64.checked_pow(*scale as u32).unwrap_or(1);
            Some(scaled_u as f64 / divisor as f64)
        }
        _ => None,
    }
}

fn compare_values_no_error(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    crate::exec::expressions::compare_values(a, b).ok()
}

enum SimpleIndexedProjectionRowIds<'a> {
    Borrowed(RuntimeRowIdSet<'a>),
    Owned(Vec<i64>),
}

impl SimpleIndexedProjectionRowIds<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Borrowed(row_ids) => row_ids.len(),
            Self::Owned(row_ids) => row_ids.len(),
        }
    }

    fn into_vec(self) -> Vec<i64> {
        let mut row_ids = Vec::with_capacity(self.len());
        self.for_each(|row_id| row_ids.push(row_id));
        row_ids
    }

    fn into_sorted_vec(self, descending: bool) -> Vec<i64> {
        let mut row_ids = self.into_vec();
        row_ids.sort_unstable();
        if descending {
            row_ids.reverse();
        }
        row_ids
    }

    fn for_each(self, mut f: impl FnMut(i64)) {
        match self {
            Self::Borrowed(row_ids) => row_ids.for_each(f),
            Self::Owned(row_ids) => {
                for row_id in row_ids {
                    f(row_id);
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
struct ActiveRowPolicy {
    name: String,
    expr: Expr,
}

#[derive(Clone, Debug)]
struct ActiveColumnMask {
    table_name: String,
    column_name: String,
    expr: Expr,
}

struct SimpleCountQueryPlan<'a> {
    table_name: &'a str,
    table_ref: &'a str,
    filter: Option<&'a Expr>,
    column_name: String,
}

struct SimpleMinMaxQueryPlan<'a> {
    table_name: &'a str,
    column_index: usize,
    is_max: bool,
    column_name: String,
}

pub(crate) struct SimpleGroupedCountPlan<'a> {
    table_name: &'a str,
    group_exprs: &'a [Expr],
    group_eval_bindings: Vec<ColumnBinding>,
    filter_expr: Option<Expr>,
    column_names: Vec<String>,
    projection_exprs: Option<Vec<Expr>>,
    raw_projection_bindings: Option<Vec<ColumnBinding>>,
    having: Option<Expr>,
    having_bindings: Vec<ColumnBinding>,
    order_by: Option<Vec<SimpleOrderByPlan>>,
    limit: Option<usize>,
    offset: usize,
}

struct SimpleGroupedNumericAggregatePlan<'a> {
    table_name: &'a str,
    group_exprs: &'a [Expr],
    group_eval_bindings: Vec<ColumnBinding>,
    filter_expr: Option<Expr>,
    column_names: Vec<String>,
    aggregate_bindings: Vec<SimpleGroupedNumericAggregateBinding>,
    projection_exprs: Option<Vec<Expr>>,
    raw_projection_bindings: Option<Vec<ColumnBinding>>,
    having: Option<Expr>,
    having_bindings: Vec<ColumnBinding>,
    order_by: Option<Vec<SimpleOrderByPlan>>,
    limit: Option<usize>,
    offset: usize,
}

struct GeneralGroupedSingleTablePlan<'a> {
    table_name: &'a str,
    table_alias: Option<&'a str>,
    group_by: &'a [Expr],
    filter: Option<&'a Expr>,
    projection: &'a [SelectItem],
    having: Option<&'a Expr>,
    order_by: &'a [crate::sql::ast::OrderBy],
    distinct: bool,
    limit: Option<&'a Expr>,
    offset: Option<&'a Expr>,
}

#[derive(Clone, Copy)]
struct WindowEvalContext<'a> {
    dataset: &'a Dataset,
    params: &'a [Value],
    ctes: &'a BTreeMap<String, Dataset>,
}

struct WindowSortedRow {
    row_index: usize,
    order_keys: Vec<Value>,
}

struct ReviewRankingFastRow {
    row_id: i64,
    movie_id: i64,
    score: i64,
    author: Value,
}

struct CastBillingFastRow {
    row_id: i64,
    movie_id: i64,
    person_id: Value,
    billing_order: i64,
    billing_value: Value,
}

struct RollingAvgFastRow {
    row_id: i64,
    movie_id: i64,
    id_value: Value,
    rating: Value,
}

fn compare_window_sorted_rows(
    left: &WindowSortedRow,
    right: &WindowSortedRow,
    order_by: &[OrderBy],
) -> std::cmp::Ordering {
    for (order, (left_value, right_value)) in order_by
        .iter()
        .zip(left.order_keys.iter().zip(right.order_keys.iter()))
    {
        let ordering = compare_values(left_value, right_value).unwrap_or(std::cmp::Ordering::Equal);
        if ordering != std::cmp::Ordering::Equal {
            return if order.descending {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }
    left.row_index.cmp(&right.row_index)
}

fn resolve_dataset_column_position(
    dataset: &Dataset,
    table: Option<&str>,
    column: &str,
) -> Result<usize> {
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
    matched_index.ok_or_else(|| DbError::sql(format!("unknown column {column}")))
}

fn values_from_positions(row: &[Value], positions: &[usize]) -> Result<Vec<Value>> {
    positions
        .iter()
        .map(|position| {
            row.get(*position)
                .cloned()
                .ok_or_else(|| DbError::internal("window row is shorter than its bindings"))
        })
        .collect()
}

fn rows_preceding_current_frame(frame: Option<&crate::sql::ast::WindowFrame>) -> Option<usize> {
    let frame = frame?;
    if frame.unit != crate::sql::ast::WindowFrameUnit::Rows {
        return None;
    }
    if !matches!(
        frame.end.as_ref(),
        None | Some(crate::sql::ast::WindowFrameBound::CurrentRow)
    ) {
        return None;
    }
    let crate::sql::ast::WindowFrameBound::Preceding(offset) = &frame.start else {
        return None;
    };
    let Expr::Literal(Value::Int64(offset)) = offset.as_ref() else {
        return None;
    };
    usize::try_from(*offset).ok()
}

pub(crate) struct IndexedJoinGroupedCountPlan<'a> {
    parent_table_name: &'a str,
    parent_join_column: &'a str,
    child_index_name: String,
    group_column_indexes: Vec<usize>,
    column_names: Vec<String>,
    order_by: Option<Vec<SimpleOrderByPlan>>,
    limit: Option<usize>,
    offset: usize,
}

impl IndexedJoinGroupedCountPlan<'_> {
    fn scalar_count_top_n_limit(&self) -> Option<usize> {
        let order_by = self.order_by.as_deref()?;
        let [order] = order_by else {
            return None;
        };
        if self.offset != 0
            || order.projection_index != self.group_column_indexes.len()
            || !order.descending
            || order.collation.is_some()
        {
            return None;
        }
        self.limit
    }
}

#[derive(Clone, Copy)]
struct IndexedJoinLimitTablePlan<'a> {
    name: &'a str,
    alias: &'a Option<String>,
}

struct IndexedJoinLimitStep {
    previous_table_index: usize,
    previous_column_index: usize,
    right_index_name: Option<String>,
}

struct IndexedJoinLimitProjection {
    table_index: usize,
    column_index: usize,
    column_name: String,
}

pub(crate) struct IndexedJoinLimitPlan<'a> {
    tables: Vec<IndexedJoinLimitTablePlan<'a>>,
    steps: Vec<IndexedJoinLimitStep>,
    projections: Vec<IndexedJoinLimitProjection>,
    limit: usize,
    offset: usize,
}

pub(crate) struct BaseTableJoinPlan<'a> {
    left_name: &'a str,
    left_alias: Option<&'a str>,
    right_name: &'a str,
    right_alias: Option<&'a str>,
    kind: JoinKind,
    constraint: &'a JoinConstraint,
    filter: Option<&'a Expr>,
    projection: &'a [SelectItem],
    order_by: &'a [crate::sql::ast::OrderBy],
    distinct: bool,
    limit: Option<&'a Expr>,
    offset: Option<&'a Expr>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SimpleGroupedNumericAggregateKind {
    CountRows,
    CountNonNull,
    CountDistinct,
    Sum,
    SumDistinct,
    Avg,
    AvgDistinct,
    Total,
    TotalDistinct,
    StddevSamp,
    StddevSampDistinct,
    StddevPop,
    StddevPopDistinct,
    VarSamp,
    VarSampDistinct,
    VarPop,
    VarPopDistinct,
    BoolAnd,
    BoolAndDistinct,
    BoolOr,
    BoolOrDistinct,
    Min,
    Max,
}

impl SimpleGroupedNumericAggregateKind {
    fn aggregate_name(self) -> &'static str {
        match self {
            Self::CountRows | Self::CountNonNull | Self::CountDistinct => "count",
            Self::Sum | Self::SumDistinct => "sum",
            Self::Avg | Self::AvgDistinct => "avg",
            Self::Total | Self::TotalDistinct => "total",
            Self::StddevSamp | Self::StddevSampDistinct => "stddev",
            Self::StddevPop | Self::StddevPopDistinct => "stddev_pop",
            Self::VarSamp | Self::VarSampDistinct => "variance",
            Self::VarPop | Self::VarPopDistinct => "var_pop",
            Self::BoolAnd | Self::BoolAndDistinct => "bool_and",
            Self::BoolOr | Self::BoolOrDistinct => "bool_or",
            Self::Min => "min",
            Self::Max => "max",
        }
    }

    fn uses_distinct(self) -> bool {
        matches!(
            self,
            Self::CountDistinct
                | Self::SumDistinct
                | Self::AvgDistinct
                | Self::TotalDistinct
                | Self::StddevSampDistinct
                | Self::StddevPopDistinct
                | Self::VarSampDistinct
                | Self::VarPopDistinct
                | Self::BoolAndDistinct
                | Self::BoolOrDistinct
        )
    }

    fn matches_aggregate_name(self, name: &str) -> bool {
        match self {
            Self::StddevSamp | Self::StddevSampDistinct => {
                name.eq_ignore_ascii_case("stddev") || name.eq_ignore_ascii_case("stddev_samp")
            }
            Self::VarSamp | Self::VarSampDistinct => {
                name.eq_ignore_ascii_case("variance") || name.eq_ignore_ascii_case("var_samp")
            }
            _ => name.eq_ignore_ascii_case(self.aggregate_name()),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SimpleGroupedNumericAggregateBinding {
    kind: SimpleGroupedNumericAggregateKind,
    projection_index: usize,
    source_column_name: Option<String>,
    source_column_index: Option<usize>,
    source_expr: Option<Expr>,
}

#[derive(Default)]
struct SimpleScalarInt64AggregateStats {
    row_count: i64,
    non_null_count: i64,
    total_int: i64,
    total_float: f64,
    min_value: Option<i64>,
    max_value: Option<i64>,
}

impl SimpleScalarInt64AggregateStats {
    fn add(&mut self, value: Option<i64>) {
        self.row_count += 1;
        let Some(value) = value else {
            return;
        };
        self.non_null_count += 1;
        self.total_int += value;
        self.total_float += value as f64;
        self.min_value = Some(self.min_value.map_or(value, |min| min.min(value)));
        self.max_value = Some(self.max_value.map_or(value, |max| max.max(value)));
    }

    fn into_values(
        self,
        aggregate_bindings: &[SimpleGroupedNumericAggregateBinding],
    ) -> Vec<Value> {
        aggregate_bindings
            .iter()
            .map(|aggregate| match aggregate.kind {
                SimpleGroupedNumericAggregateKind::CountRows => Value::Int64(self.row_count),
                SimpleGroupedNumericAggregateKind::CountNonNull => {
                    Value::Int64(self.non_null_count)
                }
                SimpleGroupedNumericAggregateKind::Sum => {
                    if self.non_null_count == 0 {
                        Value::Null
                    } else {
                        Value::Int64(self.total_int)
                    }
                }
                SimpleGroupedNumericAggregateKind::Avg => {
                    if self.non_null_count == 0 {
                        Value::Null
                    } else {
                        Value::Float64(self.total_float / self.non_null_count as f64)
                    }
                }
                SimpleGroupedNumericAggregateKind::Min => {
                    self.min_value.map(Value::Int64).unwrap_or(Value::Null)
                }
                SimpleGroupedNumericAggregateKind::Max => {
                    self.max_value.map(Value::Int64).unwrap_or(Value::Null)
                }
                _ => Value::Null,
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
struct ManifestTemplate {
    schema_cookie: u32,
    table_next_row_id_offsets: BTreeMap<String, usize>,
    table_state_offsets: BTreeMap<String, usize>,
    table_pk_index_root_offsets: BTreeMap<String, usize>,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(crate) struct ManifestEncoding {
    bytes: Vec<u8>,
    table_next_row_id_offsets: BTreeMap<String, usize>,
    table_state_offsets: BTreeMap<String, usize>,
    table_pk_index_root_offsets: BTreeMap<String, usize>,
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
        if let Some(page) =
            self.wal
                .read_page_at_snapshot(self.pager, page_id, self.snapshot_lsn)?
        {
            Ok(page)
        } else {
            self.pager.read_page_from_disk(page_id)
        }
    }

    fn advise_sequential(&self) -> Result<()> {
        self.pager.advise_sequential()
    }

    fn write_page(&mut self, _page_id: PageId, _data: &[u8]) -> Result<()> {
        Err(DbError::internal(
            "snapshot page store does not support writes",
        ))
    }
}

struct OverflowPayloadCursor<'a, S: PageStore> {
    store: &'a S,
    next_page_id: PageId,
    page: Option<Arc<[u8]>>,
    chunk_offset: usize,
    chunk_remaining: usize,
    remaining: usize,
}

impl<'a, S: PageStore> OverflowPayloadCursor<'a, S> {
    fn new(store: &'a S, pointer: OverflowPointer) -> Self {
        Self {
            store,
            next_page_id: pointer.head_page_id,
            page: None,
            chunk_offset: OVERFLOW_HEADER_SIZE,
            chunk_remaining: 0,
            remaining: pointer.logical_len as usize,
        }
    }

    fn read_i64(&mut self) -> Result<i64> {
        let mut bytes = [0_u8; 8];
        self.read_exact(&mut bytes)?;
        Ok(i64::from_le_bytes(bytes))
    }

    fn read_u32(&mut self) -> Result<u32> {
        let mut bytes = [0_u8; 4];
        self.read_exact(&mut bytes)?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_vec(&mut self, len: usize) -> Result<Vec<u8>> {
        let mut bytes = vec![0_u8; len];
        self.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    fn read_exact(&mut self, out: &mut [u8]) -> Result<()> {
        if out.len() > self.remaining {
            return Err(DbError::corruption("overflow payload length mismatch"));
        }
        let mut written = 0;
        while written < out.len() {
            self.ensure_chunk()?;
            let take = (out.len() - written).min(self.chunk_remaining);
            if take == 0 {
                return Err(DbError::corruption("overflow payload truncated"));
            }
            let page = self
                .page
                .as_ref()
                .ok_or_else(|| DbError::corruption("overflow payload page is missing"))?;
            out[written..written + take]
                .copy_from_slice(&page[self.chunk_offset..self.chunk_offset + take]);
            self.chunk_offset += take;
            self.chunk_remaining -= take;
            self.remaining -= take;
            written += take;
        }
        Ok(())
    }

    fn skip(&mut self, len: usize) -> Result<()> {
        if len > self.remaining {
            return Err(DbError::corruption("overflow payload length mismatch"));
        }
        let mut skipped = 0;
        while skipped < len {
            self.ensure_chunk()?;
            let take = (len - skipped).min(self.chunk_remaining);
            if take == 0 {
                return Err(DbError::corruption("overflow payload truncated"));
            }
            self.chunk_offset += take;
            self.chunk_remaining -= take;
            self.remaining -= take;
            skipped += take;
        }
        Ok(())
    }

    fn ensure_chunk(&mut self) -> Result<()> {
        while self.chunk_remaining == 0 {
            if self.remaining == 0 {
                return Ok(());
            }
            if self.next_page_id == 0 {
                return Err(DbError::corruption("overflow payload truncated"));
            }
            let page = self.store.read_page(self.next_page_id)?;
            if page.len() < OVERFLOW_HEADER_SIZE {
                return Err(DbError::corruption("overflow page shorter than header"));
            }
            let next_page_id = u32::from_le_bytes(page[0..4].try_into().expect("header next page"));
            let chunk_len = u32::from_le_bytes(page[4..8].try_into().expect("header chunk len"));
            if chunk_len == 0 {
                return Err(DbError::corruption(
                    "overflow chunk made no progress toward logical payload length",
                ));
            }
            let chunk_end = OVERFLOW_HEADER_SIZE + chunk_len as usize;
            if chunk_end > page.len() {
                return Err(DbError::corruption(
                    "overflow chunk length exceeds page payload",
                ));
            }
            self.next_page_id = next_page_id;
            self.page = Some(page);
            self.chunk_offset = OVERFLOW_HEADER_SIZE;
            self.chunk_remaining = chunk_len as usize;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct DbTxnPageStore<'a> {
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
        self.db.read_page_in_write_txn(page_id)
    }

    fn advise_sequential(&self) -> Result<()> {
        self.db.advise_sequential()
    }

    fn write_page(&mut self, page_id: PageId, data: &[u8]) -> Result<()> {
        self.db.write_page(page_id, data)
    }

    fn write_page_owned(&mut self, page_id: PageId, data: Vec<u8>) -> Result<()> {
        self.db.write_page_owned(page_id, data)
    }
}

fn covering_payloads_for_index(
    index: &IndexSchema,
    table: &TableSchema,
) -> Option<RuntimeCoveringPayloads> {
    let columns = covering_payload_column_names(index, table)?;
    Some(RuntimeCoveringPayloads::new(columns))
}

fn covering_payload_column_names(index: &IndexSchema, table: &TableSchema) -> Option<Vec<String>> {
    if index.kind != IndexKind::Btree
        || index.predicate_sql.is_some()
        || index.include_columns.is_empty()
        || !generated_columns_are_stored(table)
    {
        return None;
    }

    let mut columns: Vec<String> = Vec::new();
    for column in index
        .columns
        .iter()
        .map(|column| column.column_name.as_deref())
        .chain(
            index
                .include_columns
                .iter()
                .map(|column| Some(column.as_str())),
        )
    {
        let column = column?;
        schema_column_index(table, column)?;
        if !columns
            .iter()
            .any(|existing| identifiers_equal(existing, column))
        {
            columns.push(column.to_string());
        }
    }
    if columns.is_empty() {
        None
    } else {
        Some(columns)
    }
}

fn covering_payload_values_for_row(
    index: &IndexSchema,
    table: &TableSchema,
    row_values: &[Value],
) -> Option<Vec<Value>> {
    let columns = covering_payload_column_names(index, table)?;
    columns
        .iter()
        .map(|column| {
            schema_column_index(table, column)
                .and_then(|offset| row_values.get(offset))
                .cloned()
        })
        .collect()
}

fn preserve_resident_payload_offsets_for_delete_tombstones(
    config: &crate::config::DbConfig,
) -> bool {
    !config.paged_row_storage && !config.persistent_pk_index
}

/// Returns the row-position of the single indexed column for a plain
/// single-column BTREE index (no expression, no INCLUDE columns), so the
/// bulk-build fast path can encode the key directly from the borrowed row
/// slice without cloning the indexed `Value` or re-resolving the column on
/// every row. Returns `None` for composite, expression, or covering indexes.
fn single_plain_index_column_position(index: &IndexSchema, table: &TableSchema) -> Option<usize> {
    if !index.include_columns.is_empty() {
        return None;
    }
    let [column] = index.columns.as_slice() else {
        return None;
    };
    let column_name = column.column_name.as_deref()?;
    if column.expression_sql.is_some() {
        return None;
    }
    column_position(table, column_name)
}

/// Resolves the stored-column positions for a btree index whose columns are
/// all plain stored columns (no expressions, no INCLUDE columns, no virtual
/// generated columns). Used by the build loop to read index key values
/// directly by position without building a `Dataset`.
pub(crate) fn plain_index_column_positions(
    index: &IndexSchema,
    table: &TableSchema,
) -> Option<Vec<usize>> {
    if !index.include_columns.is_empty() {
        return None;
    }
    if index.columns.is_empty() {
        return None;
    }
    let stored_generated_ok = generated_columns_are_stored(table);
    let mut positions = Vec::with_capacity(index.columns.len());
    for column in &index.columns {
        if column.expression_sql.is_some() {
            return None;
        }
        let Some(column_name) = &column.column_name else {
            return None;
        };
        let position = column_position(table, column_name)?;
        if !stored_generated_ok
            && table
                .columns
                .get(position)
                .is_some_and(|col| col.generated_sql.is_some() && !col.generated_stored)
        {
            return None;
        }
        positions.push(position);
    }
    Some(positions)
}

/// Resolves the single stored-column position for a trigram index over a
/// plain TEXT column (no expression, no INCLUDE columns, no predicate, no
/// virtual generated column). Returns `None` for any unsupported shape so
/// the trigram build loop falls back to `compute_index_values`.
pub(super) fn plain_single_text_index_column_position(
    index: &IndexSchema,
    table: &TableSchema,
) -> Option<usize> {
    if !index.include_columns.is_empty() || index.predicate_sql.is_some() {
        return None;
    }
    if index.columns.len() != 1 {
        return None;
    }
    plain_index_column_positions(index, table)?
        .into_iter()
        .next()
}

/// Resolves the stored-column positions for a fulltext index over plain TEXT
/// columns (no expressions, no INCLUDE columns, no predicate, no virtual
/// generated columns). Returns `None` for any unsupported shape so the
/// fulltext build loop falls back to `full_text_fields_for_row`.
fn plain_text_index_column_positions(
    index: &IndexSchema,
    table: &TableSchema,
) -> Option<Vec<usize>> {
    if index.predicate_sql.is_some() {
        return None;
    }
    let positions = plain_index_column_positions(index, table)?;
    // Confirm every indexed column is actually TEXT so the fast path matches
    // full_text_fields_for_row's TEXT requirement.
    for position in &positions {
        let column = table.columns.get(*position)?;
        if column.column_type != ColumnType::Text {
            return None;
        }
    }
    Some(positions)
}

pub(super) fn spatial_index_backend(
    index: &IndexSchema,
    table: &TableSchema,
) -> Result<SpatialIndexBackend> {
    let column_index = spatial_index_column_index(index, table)?;
    match table.columns[column_index].column_type {
        ColumnType::Geography => Ok(SpatialIndexBackend::GeographyS2),
        ColumnType::Geometry => Ok(SpatialIndexBackend::GeometryQuadCell),
        _ => Err(DbError::internal(format!(
            "SPATIAL index {} targets a non-spatial column",
            index.name
        ))),
    }
}

pub(super) fn spatial_index_value_for_row(
    runtime: &EngineRuntime,
    index: &IndexSchema,
    table: &TableSchema,
    row_values: &[Value],
) -> Result<Option<SpatialValue>> {
    if !row_satisfies_index_predicate(runtime, index, table, row_values)? {
        return Ok(None);
    }
    let column_index = spatial_index_column_index(index, table)?;
    let value = row_values
        .get(column_index)
        .ok_or_else(|| DbError::internal("row is shorter than table schema"))?;
    let Some((is_geography, spatial)) = spatial_value_from_db(value)? else {
        return Ok(None);
    };
    match (table.columns[column_index].column_type, is_geography) {
        (ColumnType::Geography, true) | (ColumnType::Geometry, false) => Ok(Some(spatial)),
        (ColumnType::Geography, false) => Err(DbError::constraint(format!(
            "SPATIAL index {} expected GEOGRAPHY values",
            index.name
        ))),
        (ColumnType::Geometry, true) => Err(DbError::constraint(format!(
            "SPATIAL index {} expected GEOMETRY values",
            index.name
        ))),
        _ => Err(DbError::internal(format!(
            "SPATIAL index {} targets a non-spatial column",
            index.name
        ))),
    }
}

fn spatial_index_column_index(index: &IndexSchema, table: &TableSchema) -> Result<usize> {
    if index.kind != IndexKind::Spatial || index.columns.len() != 1 {
        return Err(DbError::internal(format!(
            "SPATIAL index {} must target exactly one column",
            index.name
        )));
    }
    let column_name = index.columns[0].column_name.as_deref().ok_or_else(|| {
        DbError::internal(format!(
            "SPATIAL index {} must target a plain column",
            index.name
        ))
    })?;
    if index.columns[0].expression_sql.is_some() {
        return Err(DbError::internal(format!(
            "SPATIAL index {} cannot target an expression",
            index.name
        )));
    }
    table
        .columns
        .iter()
        .position(|entry| identifiers_equal(&entry.name, column_name))
        .ok_or_else(|| DbError::constraint(format!("index column {} does not exist", column_name)))
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

fn btree_uses_typed_uuid_keys(index: &IndexSchema, table: &TableSchema) -> bool {
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
        column.column_type == crate::catalog::ColumnType::Uuid && !column.nullable
    })
}

pub(super) fn full_text_fields_for_row(
    runtime: &EngineRuntime,
    index: &IndexSchema,
    table: &TableSchema,
    row_values: &[Value],
) -> Result<Vec<Option<String>>> {
    if !row_satisfies_index_predicate(runtime, index, table, row_values)? {
        return Ok(Vec::new());
    }
    compute_index_values(runtime, index, table, row_values)?
        .into_iter()
        .map(|value| match value {
            Value::Text(text) => Ok(Some(text)),
            Value::Null => Ok(None),
            other => Err(DbError::constraint(format!(
                "fulltext index requires TEXT columns, got {other:?}"
            ))),
        })
        .collect()
}

pub(super) fn row_satisfies_index_predicate(
    runtime: &EngineRuntime,
    index: &IndexSchema,
    table: &TableSchema,
    row_values: &[Value],
) -> Result<bool> {
    row_satisfies_index_predicate_with_expr(runtime, index, table, row_values, None)
}

/// Like [`row_satisfies_index_predicate`], but accepts an optional pre-parsed
/// predicate expression. When provided, the predicate SQL is not re-parsed for
/// every row, which can dominate wall time for bulk DML on tables with partial
/// or expression-indexed indexes.
pub(super) fn row_satisfies_index_predicate_with_expr(
    runtime: &EngineRuntime,
    index: &IndexSchema,
    table: &TableSchema,
    row_values: &[Value],
    pre_parsed_predicate: Option<&Expr>,
) -> Result<bool> {
    let Some(predicate_sql) = &index.predicate_sql else {
        return Ok(true);
    };
    let expr_owned;
    let expr = match pre_parsed_predicate {
        Some(expr) => expr,
        None => {
            expr_owned = crate::sql::parser::parse_expression_sql(predicate_sql)?;
            &expr_owned
        }
    };
    if let Some(result) = simple_stored_column_eq_literal_predicate(table, row_values, expr)? {
        return Ok(result);
    }
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
        runtime.eval_expr(expr, &dataset, bindings, &[], &BTreeMap::new(), None)?,
        Value::Bool(true)
    ))
}

/// Pre-parse an index's predicate expression once. Returns `Ok(None)` if the
/// index has no predicate. The returned `Expr` can be reused across many rows
/// to avoid re-parsing the predicate SQL on every per-row index update.
pub(super) fn prepare_index_predicate_expr(index: &IndexSchema) -> Result<Option<Expr>> {
    let Some(predicate_sql) = &index.predicate_sql else {
        return Ok(None);
    };
    Ok(Some(crate::sql::parser::parse_expression_sql(
        predicate_sql,
    )?))
}

pub(crate) fn row_satisfies_expression(
    runtime: &EngineRuntime,
    table_name: &str,
    column_names: &[String],
    row_values: &[Value],
    expr: &Expr,
) -> Result<bool> {
    if column_names.len() != row_values.len() {
        return Err(DbError::internal(
            "row filter evaluation received mismatched column/value counts",
        ));
    }
    let dataset = Dataset::with_rows(
        column_names
            .iter()
            .map(|column| ColumnBinding::visible(Some(table_name.to_string()), column.clone()))
            .collect(),
        vec![row_values.to_vec()],
    );
    let row = dataset.rows.first().map(Vec::as_slice).unwrap_or(&[]);
    Ok(matches!(
        runtime.eval_expr(expr, &dataset, row, &[], &BTreeMap::new(), None)?,
        Value::Bool(true)
    ))
}

pub(super) fn table_row_dataset(table: &TableSchema, row: &[Value], table_name: &str) -> Dataset {
    Dataset::with_rows(
        table
            .columns
            .iter()
            .map(|column| {
                ColumnBinding::visible(Some(table_name.to_string()), column.name.clone())
                    .with_type(column.column_type)
            })
            .collect(),
        vec![row.to_vec()],
    )
}

fn table_bindings_with_hidden_row_id(table: &TableSchema, table_name: &str) -> Vec<ColumnBinding> {
    let mut columns = table
        .columns
        .iter()
        .map(|column| {
            ColumnBinding::visible_source(
                Some(table_name.to_string()),
                Some(table.name.clone()),
                column.name.clone(),
            )
            .with_type(column.column_type)
        })
        .collect::<Vec<_>>();
    columns.push(ColumnBinding::hidden_source(
        Some(table_name.to_string()),
        Some(table.name.clone()),
        FTS_HIDDEN_ROW_ID_COLUMN.to_string(),
    ));
    columns
}

fn encoded_table_row_len(row: &StoredRow, scratch: &mut Vec<u8>) -> Result<usize> {
    scratch.clear();
    Row::encode_values_into(&row.values, scratch)?;
    Ok(8usize
        .saturating_add(4)
        .saturating_add(scratch.len())
        .saturating_add(TABLE_PAYLOAD_ROW_BODY_PADDING_BYTES))
}

fn resident_table_should_use_paged_storage(
    data: &TableData,
    previous_state: PersistedTableState,
    delta: &PagedMutationDelta,
    page_size: u32,
) -> Result<bool> {
    if data.rows.is_empty() {
        return Ok(false);
    }

    let target_chunk_bytes = paged_table_target_chunk_bytes(page_size);
    if previous_state.pointer.head_page_id != 0
        && previous_state.pointer.logical_len as usize > target_chunk_bytes
    {
        return Ok(true);
    }

    let append_only = delta.append_count > 0
        && delta.updated_rows.is_empty()
        && delta.deleted_rows.is_empty()
        && !data.has_tombstoned_rows();
    let mut encoded_len = if append_only && previous_state.pointer.head_page_id != 0 {
        previous_state.pointer.logical_len as usize
    } else {
        TABLE_PAYLOAD_MAGIC.len() + 4
    };
    let start = if append_only && previous_state.pointer.head_page_id != 0 {
        data.rows.len().saturating_sub(delta.append_count)
    } else {
        0
    };
    let mut scratch = Vec::with_capacity(64);
    for row in &data.rows[start..] {
        encoded_len = encoded_len.saturating_add(encoded_table_row_len(row, &mut scratch)?);
        if encoded_len > target_chunk_bytes {
            return Ok(true);
        }
    }
    Ok(false)
}

fn finalize_encoded_paged_table_chunk(
    mut payload: Vec<u8>,
    row_count: usize,
) -> Result<EncodedPagedTableChunk> {
    if payload.len() < TABLE_PAYLOAD_MAGIC.len() + 4 {
        return Err(DbError::internal(
            "paged table chunk payload shorter than header",
        ));
    }
    payload[TABLE_PAYLOAD_MAGIC.len()..TABLE_PAYLOAD_MAGIC.len() + 4].copy_from_slice(
        &u32::try_from(row_count)
            .map_err(|_| DbError::constraint("paged table chunk row count exceeds u32"))?
            .to_le_bytes(),
    );
    let checksum = crc32c_parts(&[payload.as_slice()]);
    Ok(EncodedPagedTableChunk {
        payload,
        checksum,
        row_count,
    })
}

fn read_paged_table_chunk_payloads<S: PageStore>(
    store: &S,
    state: PersistedTableState,
) -> Result<Vec<TablePageManifestChunk>> {
    if state.pointer.head_page_id == 0 || state.pointer.logical_len == 0 {
        return Ok(Vec::new());
    }
    let manifest_payload = read_overflow(store, state.pointer)?;
    if crc32c_parts(&[manifest_payload.as_slice()]) != state.checksum {
        return Err(DbError::corruption(
            "paged table manifest checksum mismatch",
        ));
    }
    let manifest = decode_paged_table_manifest_payload(&manifest_payload)?;
    let mut chunks = Vec::with_capacity(manifest.chunks.len());
    let mut total_row_count = 0usize;
    for chunk in manifest.chunks {
        let payload = Arc::new(read_overflow(store, chunk.pointer)?);
        if crc32c_parts(&[payload.as_slice()]) != chunk.checksum {
            return Err(DbError::corruption("paged table chunk checksum mismatch"));
        }
        let tombstoned_row_ids = Arc::new(
            chunk
                .tombstoned_row_ids
                .iter()
                .copied()
                .collect::<BTreeSet<_>>(),
        );
        let mut overlay_payload = None;
        if let Some(overlay_pointer) = chunk.overlay_pointer {
            let p = Arc::new(read_overflow(store, overlay_pointer)?);
            if Some(crc32c_parts(&[p.as_slice()])) != chunk.overlay_checksum {
                return Err(DbError::corruption(
                    "paged table overlay chunk checksum mismatch",
                ));
            }
            overlay_payload = Some(p);
        }
        total_row_count = total_row_count.saturating_add(chunk.row_count);
        chunks.push(TablePageManifestChunk {
            pointer: chunk.pointer,
            checksum: chunk.checksum,
            row_count: chunk.row_count,
            payload,
            tombstoned_row_ids,
            overlay_pointer: chunk.overlay_pointer,
            overlay_checksum: chunk.overlay_checksum,
            overlay_payload,
        });
    }
    if state.row_count != 0 && total_row_count != state.row_count {
        return Err(DbError::corruption(
            "paged table manifest row count mismatch",
        ));
    }
    Ok(chunks)
}

fn visit_table_payload_rows_from_bytes<F>(bytes: &[u8], visitor: &mut F) -> Result<usize>
where
    F: FnMut(i64, &[Value]) -> Result<()>,
{
    if bytes.is_empty() {
        return Ok(0);
    }
    let mut cursor = Cursor::new(bytes);
    let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
    if magic != TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    let row_count = cursor.read_u32()? as usize;
    let mut visited = 0usize;
    for _ in 0..row_count {
        let row_id = cursor.read_i64()?;
        let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
        let row_bytes = cursor.read_slice(row_bytes_len)?;
        if is_tombstone {
            continue;
        }
        let row = Row::decode(row_bytes)?;
        visitor(row_id, row.values())?;
        visited += 1;
    }
    Ok(visited)
}

fn visit_table_payload_projected_values_from_bytes<F>(
    bytes: &[u8],
    projection_indexes: &[usize],
    tombstoned_row_ids: Option<&[i64]>,
    visitor: &mut F,
) -> Result<usize>
where
    F: FnMut(i64, &[Value]) -> Result<()>,
{
    if bytes.is_empty() {
        return Ok(0);
    }
    let mut cursor = Cursor::new(bytes);
    let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
    if magic != TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    let row_count = cursor.read_u32()? as usize;
    let mut visible_count = 0usize;
    for _ in 0..row_count {
        let row_id = cursor.read_i64()?;
        let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
        let row_bytes = cursor.read_slice(row_bytes_len)?;
        if is_tombstone {
            continue;
        }
        if tombstoned_row_ids.is_some_and(|row_ids| row_ids.binary_search(&row_id).is_ok()) {
            continue;
        }
        if projection_indexes.is_empty() {
            visitor(row_id, &[])?;
        } else {
            let values = Row::decode_projection_sorted_unique_with_overflow::<
                crate::storage::page::InMemoryPageStore,
            >(row_bytes, None, projection_indexes)?;
            visitor(row_id, &values)?;
        }
        visible_count += 1;
    }
    Ok(visible_count)
}

fn visit_table_payload_projected_values_from_bytes_until<F>(
    bytes: &[u8],
    projection_indexes: &[usize],
    tombstoned_row_ids: Option<&[i64]>,
    visitor: &mut F,
) -> Result<(usize, bool)>
where
    F: FnMut(i64, &[Value]) -> Result<bool>,
{
    if bytes.is_empty() {
        return Ok((0, false));
    }
    let mut cursor = Cursor::new(bytes);
    let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
    if magic != TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    let row_count = cursor.read_u32()? as usize;
    let mut visible_count = 0usize;
    for _ in 0..row_count {
        let row_id = cursor.read_i64()?;
        let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
        let row_bytes = cursor.read_slice(row_bytes_len)?;
        if is_tombstone {
            continue;
        }
        if tombstoned_row_ids.is_some_and(|row_ids| row_ids.binary_search(&row_id).is_ok()) {
            continue;
        }
        visible_count += 1;
        if projection_indexes.is_empty() {
            if visitor(row_id, &[])? {
                return Ok((visible_count, true));
            }
        } else {
            let values = Row::decode_projection_sorted_unique_with_overflow::<
                crate::storage::page::InMemoryPageStore,
            >(row_bytes, None, projection_indexes)?;
            if visitor(row_id, &values)? {
                return Ok((visible_count, true));
            }
        }
    }
    Ok((visible_count, false))
}

fn visit_table_payload_rows_from_pointer<S: PageStore, F>(
    store: &S,
    pointer: OverflowPointer,
    visitor: &mut F,
) -> Result<usize>
where
    F: FnMut(i64, &[Value]) -> Result<()>,
{
    if pointer.head_page_id == 0 || pointer.logical_len == 0 {
        return Ok(0);
    }
    if pointer.is_compressed() {
        let payload = read_overflow(store, pointer)?;
        return visit_table_payload_rows_from_bytes(&payload, visitor);
    }

    let mut cursor = OverflowPayloadCursor::new(store, pointer);
    let mut magic = [0_u8; TABLE_PAYLOAD_MAGIC.len()];
    cursor.read_exact(&mut magic)?;
    if magic != *TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    let row_count = cursor.read_u32()? as usize;
    let mut visited = 0usize;
    for _ in 0..row_count {
        let row_id = cursor.read_i64()?;
        let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
        let row_bytes = cursor.read_vec(row_bytes_len)?;
        if is_tombstone {
            continue;
        }
        let row = Row::decode(&row_bytes)?;
        visitor(row_id, row.values())?;
        visited += 1;
    }
    Ok(visited)
}

fn visit_table_payload_projected_values_from_pointer<S: PageStore, F>(
    store: &S,
    pointer: OverflowPointer,
    projection_indexes: &[usize],
    visitor: &mut F,
) -> Result<usize>
where
    F: FnMut(i64, &[Value]) -> Result<()>,
{
    if pointer.head_page_id == 0 || pointer.logical_len == 0 {
        return Ok(0);
    }
    if pointer.is_compressed() {
        let payload = read_overflow(store, pointer)?;
        return visit_table_payload_projected_values_from_bytes(
            &payload,
            projection_indexes,
            None,
            visitor,
        );
    }

    let mut cursor = OverflowPayloadCursor::new(store, pointer);
    let mut magic = [0_u8; TABLE_PAYLOAD_MAGIC.len()];
    cursor.read_exact(&mut magic)?;
    if magic != *TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    let row_count = cursor.read_u32()? as usize;
    let mut visited = 0usize;
    for _ in 0..row_count {
        let row_id = cursor.read_i64()?;
        let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
        let row_bytes = cursor.read_vec(row_bytes_len)?;
        if is_tombstone {
            continue;
        }
        if projection_indexes.is_empty() {
            visitor(row_id, &[])?;
        } else {
            let values = Row::decode_projection_sorted_unique_with_overflow::<
                crate::storage::page::InMemoryPageStore,
            >(&row_bytes, None, projection_indexes)?;
            visitor(row_id, &values)?;
        }
        visited += 1;
    }
    Ok(visited)
}

fn visit_table_payload_projected_values_from_pointer_until<S: PageStore, F>(
    store: &S,
    pointer: OverflowPointer,
    projection_indexes: &[usize],
    visitor: &mut F,
) -> Result<(usize, bool)>
where
    F: FnMut(i64, &[Value]) -> Result<bool>,
{
    if pointer.head_page_id == 0 || pointer.logical_len == 0 {
        return Ok((0, false));
    }
    if pointer.is_compressed() {
        let payload = read_overflow(store, pointer)?;
        return visit_table_payload_projected_values_from_bytes_until(
            &payload,
            projection_indexes,
            None,
            visitor,
        );
    }

    let mut cursor = OverflowPayloadCursor::new(store, pointer);
    let mut magic = [0_u8; TABLE_PAYLOAD_MAGIC.len()];
    cursor.read_exact(&mut magic)?;
    if magic != *TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    let row_count = cursor.read_u32()? as usize;
    let mut visited = 0usize;
    for _ in 0..row_count {
        let row_id = cursor.read_i64()?;
        let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
        let row_bytes = cursor.read_vec(row_bytes_len)?;
        if is_tombstone {
            continue;
        }
        visited += 1;
        if projection_indexes.is_empty() {
            if visitor(row_id, &[])? {
                return Ok((visited, true));
            }
        } else {
            let values = Row::decode_projection_sorted_unique_with_overflow::<
                crate::storage::page::InMemoryPageStore,
            >(&row_bytes, None, projection_indexes)?;
            if visitor(row_id, &values)? {
                return Ok((visited, true));
            }
        }
    }
    Ok((visited, false))
}

fn visit_table_payload_int64_column_from_bytes<F>(
    bytes: &[u8],
    column_index: usize,
    tombstoned_row_ids: Option<&[i64]>,
    visitor: &mut F,
) -> Result<usize>
where
    F: FnMut(i64, Option<i64>) -> Result<()>,
{
    if bytes.is_empty() {
        return Ok(0);
    }
    let mut cursor = Cursor::new(bytes);
    let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
    if magic != TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    let row_count = cursor.read_u32()? as usize;
    let mut visible_count = 0usize;
    for _ in 0..row_count {
        let row_id = cursor.read_i64()?;
        let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
        let row_bytes = cursor.read_slice(row_bytes_len)?;
        if is_tombstone {
            continue;
        }
        if tombstoned_row_ids.is_some_and(|row_ids| row_ids.binary_search(&row_id).is_ok()) {
            continue;
        }
        visitor(row_id, Row::decode_int64_at(row_bytes, column_index)?)?;
        visible_count += 1;
    }
    Ok(visible_count)
}

fn visit_table_payload_int64_column_from_pointer<S: PageStore, F>(
    store: &S,
    pointer: OverflowPointer,
    column_index: usize,
    tombstoned_row_ids: Option<&[i64]>,
    visitor: &mut F,
) -> Result<usize>
where
    F: FnMut(i64, Option<i64>) -> Result<()>,
{
    let mut payload_scratch = Vec::new();
    visit_table_payload_int64_column_from_pointer_with_scratch(
        store,
        pointer,
        column_index,
        tombstoned_row_ids,
        &mut payload_scratch,
        visitor,
    )
}

fn visit_table_payload_int64_column_from_pointer_with_scratch<S: PageStore, F>(
    store: &S,
    pointer: OverflowPointer,
    column_index: usize,
    tombstoned_row_ids: Option<&[i64]>,
    payload_scratch: &mut Vec<u8>,
    visitor: &mut F,
) -> Result<usize>
where
    F: FnMut(i64, Option<i64>) -> Result<()>,
{
    if pointer.head_page_id == 0 || pointer.logical_len == 0 {
        return Ok(0);
    }
    read_overflow_into(store, pointer, payload_scratch)?;
    visit_table_payload_int64_column_from_bytes(
        payload_scratch,
        column_index,
        tombstoned_row_ids,
        visitor,
    )
}

fn visit_persisted_table_int64_column<S: PageStore, F>(
    store: &S,
    state: PersistedTableState,
    column_index: usize,
    mut visitor: F,
) -> Result<usize>
where
    F: FnMut(i64, Option<i64>) -> Result<()>,
{
    if state.pointer.head_page_id == 0 || state.pointer.logical_len == 0 {
        return Ok(0);
    }
    if !state.pointer.is_table_paged_manifest() {
        let row_count = visit_table_payload_int64_column_from_pointer(
            store,
            state.pointer,
            column_index,
            None,
            &mut visitor,
        )?;
        if state.row_count != 0 && row_count != state.row_count {
            return Err(DbError::corruption("table payload row count mismatch"));
        }
        return Ok(row_count);
    }

    let manifest_payload = read_overflow(store, state.pointer)?;
    if crc32c_parts(&[manifest_payload.as_slice()]) != state.checksum {
        return Err(DbError::corruption(
            "paged table manifest checksum mismatch",
        ));
    }
    let manifest = decode_paged_table_manifest_payload(&manifest_payload)?;
    let mut total_row_count = 0usize;
    let mut payload_scratch = Vec::new();
    for chunk in manifest.chunks {
        let mut count = 0usize;
        let tombstones = if chunk.tombstoned_row_ids.is_empty() {
            None
        } else {
            Some(chunk.tombstoned_row_ids.as_slice())
        };

        count += visit_table_payload_int64_column_from_pointer_with_scratch(
            store,
            chunk.pointer,
            column_index,
            tombstones,
            &mut payload_scratch,
            &mut visitor,
        )?;

        if let Some(overlay_pointer) = chunk.overlay_pointer {
            count += visit_table_payload_int64_column_from_pointer_with_scratch(
                store,
                overlay_pointer,
                column_index,
                None,
                &mut payload_scratch,
                &mut visitor,
            )?;
        }

        if count != chunk.row_count {
            return Err(DbError::corruption("paged table chunk row count mismatch"));
        }
        total_row_count = total_row_count.saturating_add(count);
    }
    if state.row_count != 0 && total_row_count != state.row_count {
        return Err(DbError::corruption(
            "paged table manifest row count mismatch",
        ));
    }
    Ok(total_row_count)
}

fn visit_persisted_table_projected_values<S: PageStore, F>(
    store: &S,
    state: PersistedTableState,
    projection_indexes: &[usize],
    mut visitor: F,
) -> Result<usize>
where
    F: FnMut(i64, &[Value]) -> Result<()>,
{
    if state.pointer.head_page_id == 0 || state.pointer.logical_len == 0 {
        return Ok(0);
    }
    if !state.pointer.is_table_paged_manifest() {
        let row_count = visit_table_payload_projected_values_from_pointer(
            store,
            state.pointer,
            projection_indexes,
            &mut visitor,
        )?;
        if state.row_count != 0 && row_count != state.row_count {
            return Err(DbError::corruption("table payload row count mismatch"));
        }
        return Ok(row_count);
    }

    let manifest_payload = read_overflow(store, state.pointer)?;
    if crc32c_parts(&[manifest_payload.as_slice()]) != state.checksum {
        return Err(DbError::corruption(
            "paged table manifest checksum mismatch",
        ));
    }
    let manifest = decode_paged_table_manifest_payload(&manifest_payload)?;
    let mut total_row_count = 0usize;
    for chunk in manifest.chunks {
        let mut count = 0usize;
        let tombstones = if chunk.tombstoned_row_ids.is_empty() {
            None
        } else {
            Some(chunk.tombstoned_row_ids.as_slice())
        };

        let base_payload = read_overflow(store, chunk.pointer)?;
        count += visit_table_payload_projected_values_from_bytes(
            &base_payload,
            projection_indexes,
            tombstones,
            &mut visitor,
        )?;

        if let Some(overlay_pointer) = chunk.overlay_pointer {
            let overlay_payload = read_overflow(store, overlay_pointer)?;
            count += visit_table_payload_projected_values_from_bytes(
                &overlay_payload,
                projection_indexes,
                None,
                &mut visitor,
            )?;
        }

        if count != chunk.row_count {
            return Err(DbError::corruption("paged table chunk row count mismatch"));
        }
        total_row_count = total_row_count.saturating_add(count);
    }
    if state.row_count != 0 && total_row_count != state.row_count {
        return Err(DbError::corruption(
            "paged table manifest row count mismatch",
        ));
    }
    Ok(total_row_count)
}

fn visit_persisted_table_projected_values_until<S: PageStore, F>(
    store: &S,
    state: PersistedTableState,
    projection_indexes: &[usize],
    mut visitor: F,
) -> Result<usize>
where
    F: FnMut(i64, &[Value]) -> Result<bool>,
{
    if state.pointer.head_page_id == 0 || state.pointer.logical_len == 0 {
        return Ok(0);
    }
    if !state.pointer.is_table_paged_manifest() {
        let (row_count, stopped) = visit_table_payload_projected_values_from_pointer_until(
            store,
            state.pointer,
            projection_indexes,
            &mut visitor,
        )?;
        if !stopped && state.row_count != 0 && row_count != state.row_count {
            return Err(DbError::corruption("table payload row count mismatch"));
        }
        return Ok(row_count);
    }

    let manifest_payload = read_overflow(store, state.pointer)?;
    if crc32c_parts(&[manifest_payload.as_slice()]) != state.checksum {
        return Err(DbError::corruption(
            "paged table manifest checksum mismatch",
        ));
    }
    let manifest = decode_paged_table_manifest_payload(&manifest_payload)?;
    let mut visited_row_count = 0usize;
    let mut total_row_count = 0usize;
    for chunk in manifest.chunks {
        let mut count = 0usize;
        let tombstones = if chunk.tombstoned_row_ids.is_empty() {
            None
        } else {
            Some(chunk.tombstoned_row_ids.as_slice())
        };

        let base_payload = read_overflow(store, chunk.pointer)?;
        let (base_count, stopped) = visit_table_payload_projected_values_from_bytes_until(
            &base_payload,
            projection_indexes,
            tombstones,
            &mut visitor,
        )?;
        visited_row_count = visited_row_count.saturating_add(base_count);
        count = count.saturating_add(base_count);
        if stopped {
            return Ok(visited_row_count);
        }

        if let Some(overlay_pointer) = chunk.overlay_pointer {
            let overlay_payload = read_overflow(store, overlay_pointer)?;
            let (overlay_count, stopped) = visit_table_payload_projected_values_from_bytes_until(
                &overlay_payload,
                projection_indexes,
                None,
                &mut visitor,
            )?;
            visited_row_count = visited_row_count.saturating_add(overlay_count);
            count = count.saturating_add(overlay_count);
            if stopped {
                return Ok(visited_row_count);
            }
        }

        if count != chunk.row_count {
            return Err(DbError::corruption("paged table chunk row count mismatch"));
        }
        total_row_count = total_row_count.saturating_add(count);
    }
    if state.row_count != 0 && total_row_count != state.row_count {
        return Err(DbError::corruption(
            "paged table manifest row count mismatch",
        ));
    }
    Ok(visited_row_count)
}

fn visit_persisted_table_rows<S: PageStore, F>(
    store: &S,
    state: PersistedTableState,
    mut visitor: F,
) -> Result<usize>
where
    F: FnMut(i64, &[Value]) -> Result<()>,
{
    if state.pointer.head_page_id == 0 || state.pointer.logical_len == 0 {
        return Ok(0);
    }
    if !state.pointer.is_table_paged_manifest() {
        let row_count = visit_table_payload_rows_from_pointer(store, state.pointer, &mut visitor)?;
        if state.row_count != 0 && row_count != state.row_count {
            return Err(DbError::corruption("table payload row count mismatch"));
        }
        return Ok(row_count);
    }

    let manifest_payload = read_overflow(store, state.pointer)?;
    if crc32c_parts(&[manifest_payload.as_slice()]) != state.checksum {
        return Err(DbError::corruption(
            "paged table manifest checksum mismatch",
        ));
    }
    let manifest = decode_paged_table_manifest_payload(&manifest_payload)?;
    let mut total_row_count = 0usize;
    for chunk in manifest.chunks {
        let mut count = 0usize;
        let has_tombstones = !chunk.tombstoned_row_ids.is_empty();

        let base_payload = read_overflow(store, chunk.pointer)?;
        for row in decode_table_payload_rows(&base_payload)? {
            if has_tombstones && chunk.tombstoned_row_ids.binary_search(&row.row_id).is_ok() {
                continue;
            }
            visitor(row.row_id, &row.values)?;
            count += 1;
        }

        if let Some(overlay_pointer) = chunk.overlay_pointer {
            let overlay_payload = read_overflow(store, overlay_pointer)?;
            for row in decode_table_payload_rows(&overlay_payload)? {
                visitor(row.row_id, &row.values)?;
                count += 1;
            }
        }

        if count != chunk.row_count {
            return Err(DbError::corruption("paged table chunk row count mismatch"));
        }
        total_row_count = total_row_count.saturating_add(count);
    }
    if state.row_count != 0 && total_row_count != state.row_count {
        return Err(DbError::corruption(
            "paged table manifest row count mismatch",
        ));
    }
    Ok(total_row_count)
}

fn visit_persisted_table_rows_until<S: PageStore, F>(
    store: &S,
    state: PersistedTableState,
    mut visitor: F,
) -> Result<usize>
where
    F: FnMut(i64, &[Value]) -> Result<bool>,
{
    if state.pointer.head_page_id == 0 || state.pointer.logical_len == 0 {
        return Ok(0);
    }
    if !state.pointer.is_table_paged_manifest() {
        let mut stopped = false;
        let row_count =
            visit_table_payload_rows_from_pointer(store, state.pointer, &mut |row_id, values| {
                if stopped {
                    return Ok(());
                }
                stopped = visitor(row_id, values)?;
                Ok(())
            })?;
        if !stopped && state.row_count != 0 && row_count != state.row_count {
            return Err(DbError::corruption("table payload row count mismatch"));
        }
        return Ok(row_count);
    }

    let manifest_payload = read_overflow(store, state.pointer)?;
    if crc32c_parts(&[manifest_payload.as_slice()]) != state.checksum {
        return Err(DbError::corruption(
            "paged table manifest checksum mismatch",
        ));
    }
    let manifest = decode_paged_table_manifest_payload(&manifest_payload)?;
    let mut visited_row_count = 0usize;
    let mut total_row_count = 0usize;
    for chunk in manifest.chunks {
        let mut count = 0usize;
        let has_tombstones = !chunk.tombstoned_row_ids.is_empty();

        let base_payload = read_overflow(store, chunk.pointer)?;
        for row in decode_table_payload_rows(&base_payload)? {
            if has_tombstones && chunk.tombstoned_row_ids.binary_search(&row.row_id).is_ok() {
                continue;
            }
            visited_row_count = visited_row_count.saturating_add(1);
            count += 1;
            if visitor(row.row_id, &row.values)? {
                return Ok(visited_row_count);
            }
        }

        if let Some(overlay_pointer) = chunk.overlay_pointer {
            let overlay_payload = read_overflow(store, overlay_pointer)?;
            for row in decode_table_payload_rows(&overlay_payload)? {
                visited_row_count = visited_row_count.saturating_add(1);
                count += 1;
                if visitor(row.row_id, &row.values)? {
                    return Ok(visited_row_count);
                }
            }
        }

        if count != chunk.row_count {
            return Err(DbError::corruption("paged table chunk row count mismatch"));
        }
        total_row_count = total_row_count.saturating_add(count);
    }
    if state.row_count != 0 && total_row_count != state.row_count {
        return Err(DbError::corruption(
            "paged table manifest row count mismatch",
        ));
    }
    Ok(visited_row_count)
}

pub(crate) fn read_table_payload_row_count_from_bytes(bytes: &[u8]) -> Result<usize> {
    let mut cursor = Cursor::new(bytes);
    let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
    if magic != TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    Ok(cursor.read_u32()? as usize)
}

/// ADR 0200: count the live (non-tombstoned) rows in a resident table payload.
/// Unlike [`read_table_payload_row_count_from_bytes`] (which returns the
/// physical slot count from the header), this scans the row stream and skips
/// in-place delete tombstones, so it reports the logical row count used for
/// `COUNT(*)`-style metadata. For payloads without tombstones the result equals
/// the header count.
pub(crate) fn read_table_payload_live_row_count_from_bytes(bytes: &[u8]) -> Result<usize> {
    if bytes.is_empty() {
        return Ok(0);
    }
    let mut cursor = Cursor::new(bytes);
    let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
    if magic != TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    let row_count = cursor.read_u32()? as usize;
    let mut live = 0usize;
    for _ in 0..row_count {
        let _row_id = cursor.read_i64()?;
        let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
        cursor.read_slice(row_bytes_len)?;
        if !is_tombstone {
            live += 1;
        }
    }
    Ok(live)
}

pub(crate) fn free_persisted_table_bytes<S: PageStore>(
    store: &mut S,
    state: PersistedTableState,
) -> Result<()> {
    if state.pointer.head_page_id == 0 {
        return Ok(());
    }
    if state.pointer.is_table_paged_manifest() {
        let manifest_payload = read_overflow(store, state.pointer)?;
        let manifest = decode_paged_table_manifest_payload(&manifest_payload)?;
        for chunk in manifest.chunks {
            if chunk.pointer.head_page_id != 0 {
                free_overflow(store, chunk.pointer.head_page_id)?;
            }
        }
        free_overflow(store, state.pointer.head_page_id)?;
        return Ok(());
    }
    free_overflow(store, state.pointer.head_page_id)?;
    Ok(())
}

fn wrap_legacy_table_state_as_paged_manifest<S: PageStore>(
    store: &mut S,
    state: PersistedTableState,
) -> Result<PersistedTableState> {
    if state.pointer.head_page_id == 0 || state.pointer.logical_len == 0 {
        return Ok(state);
    }
    let row_count = if state.row_count == 0 {
        read_table_payload_row_count(store, state.pointer)?
    } else {
        state.row_count
    };
    let manifest = PersistedPagedTableManifest {
        chunks: vec![PersistedTableChunkState {
            pointer: state.pointer.with_table_paged_manifest(false),
            checksum: state.checksum,
            row_count,
            tombstoned_row_ids: Vec::new(),
            overlay_pointer: None,
            overlay_checksum: None,
        }],
    };
    let manifest_payload = encode_paged_table_manifest_payload(&manifest)?;
    let checksum = crc32c_parts(&[manifest_payload.as_slice()]);
    let pointer = write_overflow(store, &manifest_payload, CompressionMode::Never)?
        .with_table_paged_manifest(true);
    let tail = read_uncompressed_overflow_tail(store, pointer)?.unwrap_or_default();
    Ok(PersistedTableState {
        pointer,
        checksum,
        row_count,
        tail,
        pk_index_root: state.pk_index_root,
    })
}

pub(crate) fn append_paged_table_chunks<S: PageStore>(
    store: &mut S,
    mut previous_state: PersistedTableState,
    appended_chunks: &[EncodedPagedTableChunk],
    row_count: usize,
) -> Result<PersistedTableState> {
    if previous_state.pointer.head_page_id == 0 {
        return persist_paged_table(store, previous_state, appended_chunks, row_count);
    }
    if !previous_state.pointer.is_table_paged_manifest() {
        previous_state = wrap_legacy_table_state_as_paged_manifest(store, previous_state)?;
    }
    if appended_chunks.is_empty() {
        return Ok(previous_state);
    }

    let manifest_payload = read_overflow(store, previous_state.pointer)?;
    if crc32c_parts(&[manifest_payload.as_slice()]) != previous_state.checksum {
        return Err(DbError::corruption(
            "paged table manifest checksum mismatch",
        ));
    }
    let mut manifest = decode_paged_table_manifest_payload(&manifest_payload)?;
    manifest.chunks.reserve(appended_chunks.len());
    for chunk in appended_chunks {
        let pointer = write_overflow(store, &chunk.payload, CompressionMode::Never)?;
        manifest.chunks.push(PersistedTableChunkState {
            pointer,
            checksum: chunk.checksum,
            row_count: chunk.row_count,
            tombstoned_row_ids: Vec::new(),
            overlay_pointer: None,
            overlay_checksum: None,
        });
    }

    let updated_manifest_payload = encode_paged_table_manifest_payload(&manifest)?;
    let checksum = crc32c_parts(&[updated_manifest_payload.as_slice()]);
    let pointer = rewrite_overflow(
        store,
        previous_state.pointer.with_table_paged_manifest(false),
        &updated_manifest_payload,
        CompressionMode::Never,
    )?
    .with_table_paged_manifest(true);
    let tail = read_uncompressed_overflow_tail(store, pointer)?.unwrap_or_default();
    Ok(PersistedTableState {
        pointer,
        checksum,
        row_count,
        tail,
        pk_index_root: previous_state.pk_index_root,
    })
}

fn persisted_paged_chunk_is_plain(chunk: &PersistedTableChunkState) -> bool {
    chunk.tombstoned_row_ids.is_empty()
        && chunk.overlay_pointer.is_none()
        && chunk.overlay_checksum.is_none()
}

fn persisted_chunk_metadata_matches_current(
    persisted: &PersistedTableChunkState,
    current: &TablePageManifestChunk,
) -> bool {
    persisted.pointer == current.pointer
        && persisted.checksum == current.checksum
        && persisted.row_count == current.row_count
        && persisted.overlay_pointer == current.overlay_pointer
        && persisted.overlay_checksum == current.overlay_checksum
        && persisted.tombstoned_row_ids.len() == current.tombstoned_row_ids.len()
        && persisted
            .tombstoned_row_ids
            .iter()
            .all(|row_id| current.tombstoned_row_ids.contains(row_id))
}

fn persisted_chunk_from_current(
    pointer: OverflowPointer,
    checksum: u32,
    row_count: usize,
    current_chunk: &TablePageManifestChunk,
) -> TablePageManifestChunk {
    TablePageManifestChunk {
        pointer,
        checksum,
        row_count,
        payload: Arc::clone(&current_chunk.payload),
        tombstoned_row_ids: Arc::clone(&current_chunk.tombstoned_row_ids),
        overlay_pointer: None,
        overlay_checksum: None,
        overlay_payload: None,
    }
}

fn persist_paged_table<S: PageStore>(
    store: &mut S,
    previous_state: PersistedTableState,
    encoded_chunks: &[EncodedPagedTableChunk],
    row_count: usize,
) -> Result<PersistedTableState> {
    if encoded_chunks.is_empty() {
        if previous_state.pointer.head_page_id != 0 {
            free_persisted_table_bytes(store, previous_state)?;
        }
        return Ok(PersistedTableState {
            pointer: OverflowPointer {
                head_page_id: 0,
                logical_len: 0,
                flags: 0,
            },
            checksum: 0,
            row_count: 0,
            tail: OverflowTailInfo::default(),
            pk_index_root: previous_state.pk_index_root,
        });
    }

    let mut persisted_chunks = Vec::with_capacity(encoded_chunks.len());
    for chunk in encoded_chunks {
        let pointer = write_overflow(store, &chunk.payload, CompressionMode::Never)?;
        persisted_chunks.push(PersistedTableChunkState {
            pointer,
            checksum: chunk.checksum,
            row_count: chunk.row_count,
            tombstoned_row_ids: Vec::new(),
            overlay_pointer: None,
            overlay_checksum: None,
        });
    }
    let manifest = PersistedPagedTableManifest {
        chunks: persisted_chunks,
    };
    let manifest_payload = encode_paged_table_manifest_payload(&manifest)?;
    let checksum = crc32c_parts(&[manifest_payload.as_slice()]);
    let pointer = write_overflow(store, &manifest_payload, CompressionMode::Never)?
        .with_table_paged_manifest(true);
    let tail = read_uncompressed_overflow_tail(store, pointer)?.unwrap_or_default();
    if previous_state.pointer.head_page_id != 0 {
        free_persisted_table_bytes(store, previous_state)?;
    }
    Ok(PersistedTableState {
        pointer,
        checksum,
        row_count,
        tail,
        pk_index_root: previous_state.pk_index_root,
    })
}

/// Scan a table payload's row ids without decoding row values. Returns the set
/// of row ids stored in the payload (excluding any tombstoned/overlaid rows
/// the caller already handles). Used to decide whether a chunk contains deleted
/// rows without paying the cost of decoding every row's values.
pub(crate) fn scan_table_payload_row_ids(payload: &[u8]) -> Result<BTreeSet<i64>> {
    if payload.len() < TABLE_PAYLOAD_MAGIC.len() + 4 {
        return Ok(BTreeSet::new());
    }
    let mut cursor = Cursor::new(payload);
    let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
    if magic != TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    let row_count = cursor.read_u32()? as usize;
    let mut row_ids = BTreeSet::new();
    for _ in 0..row_count {
        let row_id = cursor.read_i64()?;
        let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
        cursor.read_slice(row_bytes_len)?;
        if is_tombstone {
            continue;
        }
        row_ids.insert(row_id);
    }
    Ok(row_ids)
}

fn build_persistent_pk_index_root(db: &crate::db::Db, payload: &[u8]) -> Result<Option<PageId>> {
    let entries = build_row_locator_entries(payload)?;
    let mut tree = Btree::new(DbTxnPageStore { db });
    tree.replace_entries(entries)?;
    let (_store, root_page_id) = tree.into_parts();
    Ok(root_page_id)
}

fn build_persistent_pk_index_root_from_chunk_payloads(
    db: &crate::db::Db,
    chunk_payloads: &[TablePageManifestChunk],
) -> Result<Option<PageId>> {
    let entries = build_paged_row_locator_entries_from_chunk_payloads(chunk_payloads)?;
    let mut tree = Btree::new(DbTxnPageStore { db });
    tree.replace_entries(entries)?;
    let (_store, root_page_id) = tree.into_parts();
    Ok(root_page_id)
}

fn replace_table_pk_index_root(
    runtime: &mut EngineRuntime,
    db: &crate::db::Db,
    table_name: &str,
    new_pk_index_root: Option<PageId>,
) -> Result<()> {
    let previous_pk_index_root = runtime
        .persisted_tables
        .get(table_name)
        .and_then(|state| state.pk_index_root)
        .or_else(|| {
            runtime
                .catalog
                .tables
                .get(table_name)
                .and_then(|table| table.pk_index_root)
        });
    match previous_pk_index_root {
        Some(previous_pk_index_root) if Some(previous_pk_index_root) != new_pk_index_root => {
            let mut store = DbTxnPageStore { db };
            free_table_btree(&mut store, Some(previous_pk_index_root))?;
        }
        _ => {}
    }
    let table = runtime
        .catalog_mut()
        .tables
        .get_mut(table_name)
        .ok_or_else(|| DbError::internal(format!("table schema for {table_name} is missing")))?;
    table.pk_index_root = new_pk_index_root;
    if let Some(state) = runtime.persisted_tables_mut().get_mut(table_name) {
        state.pk_index_root = new_pk_index_root;
    }
    Ok(())
}

/// Build a new payload by splicing only the modified rows into the cached
/// previous payload.  Unchanged row bytes are copied verbatim from `old`,
/// saving the per-row serialisation cost for the common single-row UPDATE.
/// Result of a splice operation, containing the new payload and dirty byte
/// range metadata.
pub(crate) struct SpliceResult {
    payload: Vec<u8>,
    /// Byte offset of the first modified byte in the OLD payload.
    first_dirty_byte: usize,
    /// Exclusive byte offset of the first byte after the changed range in the
    /// OLD payload, when conservative behavior is used this may be payload
    /// length.
    last_dirty_byte: usize,
    /// Whether the updated payload preserves row offsets and can reuse
    /// the previous persistent PK locator root.
    pk_locator_preserved: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SpliceDirtyRange {
    first_dirty_byte: usize,
    last_dirty_byte: usize,
}

fn single_dirty_range(range: Range<usize>) -> Vec<Range<usize>> {
    std::iter::once(range).collect()
}

fn truncate_tail_deleted_rows_payload(
    payload: &mut Vec<u8>,
    deleted_row_ids: &BTreeSet<i64>,
    live_row_count: usize,
) -> Result<Option<Vec<Range<usize>>>> {
    const HEADER_LEN: usize = 8 /* magic */ + 4 /* row_count */;

    if deleted_row_ids.is_empty() {
        return Ok(Some(Vec::new()));
    }
    if payload.len() < HEADER_LEN || payload[..8] != *TABLE_PAYLOAD_MAGIC {
        return Ok(None);
    }
    let physical_row_count =
        u32::from_le_bytes(payload[8..12].try_into().expect("row-count header length")) as usize;
    if physical_row_count != live_row_count.saturating_add(deleted_row_ids.len()) {
        return Ok(None);
    }

    let mut offset = HEADER_LEN;
    let mut first_deleted_offset = None;
    let mut deleted_seen = 0usize;
    for _ in 0..physical_row_count {
        if offset + 12 > payload.len() {
            return Ok(None);
        }
        let row_start = offset;
        let row_id = i64::from_le_bytes(
            payload[offset..offset + 8]
                .try_into()
                .expect("row id length"),
        );
        let len_field_offset = offset + 8;
        let raw_len = u32::from_le_bytes(
            payload[len_field_offset..len_field_offset + 4]
                .try_into()
                .expect("row data len"),
        );
        let (is_tombstone, body_len) = split_table_payload_row_len(raw_len);
        if is_tombstone {
            return Ok(None);
        }
        let Some(row_end) = len_field_offset
            .checked_add(4)
            .and_then(|value| value.checked_add(body_len))
        else {
            return Ok(None);
        };
        if row_end > payload.len() {
            return Ok(None);
        }
        if deleted_row_ids.contains(&row_id) {
            if first_deleted_offset.is_none() {
                first_deleted_offset = Some(row_start);
            }
            deleted_seen += 1;
        } else if first_deleted_offset.is_some() {
            return Ok(None);
        }
        offset = row_end;
    }
    if offset != payload.len() || deleted_seen != deleted_row_ids.len() {
        return Ok(None);
    }
    let Some(truncate_at) = first_deleted_offset else {
        return Ok(None);
    };

    payload[8..12].copy_from_slice(
        &u32::try_from(live_row_count)
            .map_err(|_| DbError::constraint("table row count exceeds u32"))?
            .to_le_bytes(),
    );
    payload.truncate(truncate_at);

    let mut dirty_ranges = Vec::with_capacity(2);
    dirty_ranges.push(8..12);
    if !payload.is_empty() {
        let tail_start = payload.len().saturating_sub(1);
        dirty_ranges.push(tail_start..payload.len());
    }
    Ok(Some(dirty_ranges))
}

fn read_overflow_cached_logical_bytes<S: PageStore>(
    store: &S,
    pointer: OverflowPointer,
    cached_page_ids: &[PageId],
    offset: usize,
    out: &mut [u8],
) -> Result<bool> {
    if out.is_empty() {
        return Ok(true);
    }
    if pointer.is_compressed() || pointer.is_table_paged_manifest() {
        return Ok(false);
    }
    let logical_len = pointer.logical_len as usize;
    let Some(end) = offset.checked_add(out.len()) else {
        return Ok(false);
    };
    if end > logical_len {
        return Ok(false);
    }

    let page_size = store.page_size() as usize;
    if page_size <= OVERFLOW_HEADER_SIZE {
        return Err(DbError::internal("page size too small for overflow pages"));
    }
    let chunk_capacity = page_size - OVERFLOW_HEADER_SIZE;
    let mut read = 0usize;
    while read < out.len() {
        let logical_offset = offset + read;
        let page_index = logical_offset / chunk_capacity;
        let Some(&page_id) = cached_page_ids.get(page_index) else {
            return Ok(false);
        };
        let page_payload_offset = page_index.saturating_mul(chunk_capacity);
        let local_offset = logical_offset.saturating_sub(page_payload_offset);
        let page = store.read_page(page_id)?;
        if page.len() < OVERFLOW_HEADER_SIZE {
            return Err(DbError::corruption("overflow page shorter than header"));
        }
        let chunk_len =
            u32::from_le_bytes(page[4..8].try_into().expect("header chunk len")) as usize;
        let chunk_end = OVERFLOW_HEADER_SIZE.saturating_add(chunk_len);
        if chunk_end > page.len() {
            return Err(DbError::corruption(
                "overflow chunk length exceeds page payload",
            ));
        }
        if local_offset >= chunk_len {
            return Ok(false);
        }
        let take = (out.len() - read).min(chunk_len - local_offset);
        out[read..read + take].copy_from_slice(
            &page[OVERFLOW_HEADER_SIZE + local_offset..OVERFLOW_HEADER_SIZE + local_offset + take],
        );
        read += take;
    }
    Ok(true)
}

fn build_resident_tombstone_locators_from_payload(payload: &[u8]) -> Result<Int64Map<u32>> {
    const HEADER_LEN: usize = 8 /* magic */ + 4 /* row_count */;

    if payload.is_empty() {
        return Ok(Int64Map::with_hasher(Int64HashBuilder::default()));
    }
    if payload.len() < HEADER_LEN || payload[..8] != *TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    let row_count =
        u32::from_le_bytes(payload[8..12].try_into().expect("row-count header length")) as usize;
    let mut locators = Int64Map::with_capacity_and_hasher(row_count, Int64HashBuilder::default());
    let mut offset = HEADER_LEN;
    for _ in 0..row_count {
        if offset + 12 > payload.len() {
            return Err(DbError::corruption("truncated table payload row header"));
        }
        let row_id = i64::from_le_bytes(
            payload[offset..offset + 8]
                .try_into()
                .expect("row id length"),
        );
        let len_field_offset = offset + 8;
        let raw_len = u32::from_le_bytes(
            payload[len_field_offset..len_field_offset + 4]
                .try_into()
                .expect("row data len"),
        );
        let (_, body_len) = split_table_payload_row_len(raw_len);
        let row_end = len_field_offset
            .checked_add(4)
            .and_then(|value| value.checked_add(body_len))
            .ok_or_else(|| DbError::corruption("table payload row length overflow"))?;
        if row_end > payload.len() {
            return Err(DbError::corruption("truncated table payload row body"));
        }
        locators.insert(
            row_id,
            u32::try_from(len_field_offset)
                .map_err(|_| DbError::constraint("resident tombstone locator exceeds u32"))?,
        );
        offset = row_end;
    }
    Ok(locators)
}

fn append_encoded_rows_to_table_payload(
    mut previous: Vec<u8>,
    row_count: usize,
    appended_rows: &[u8],
) -> Result<Vec<u8>> {
    if appended_rows.is_empty() {
        return Ok(previous);
    }
    let count_offset = TABLE_PAYLOAD_MAGIC.len();
    if previous.len() < count_offset + 4 {
        return Err(DbError::corruption("table payload header is truncated"));
    }
    if previous[..count_offset] != *TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }

    previous[count_offset..count_offset + 4].copy_from_slice(
        &u32::try_from(row_count)
            .map_err(|_| DbError::constraint("table row count exceeds u32"))?
            .to_le_bytes(),
    );
    previous.extend_from_slice(appended_rows);
    Ok(previous)
}

pub(crate) fn append_table_payload(mut previous: Vec<u8>, data: &TableData) -> Result<Vec<u8>> {
    if data.rows.is_empty() {
        return Ok(Vec::new());
    }
    if data.has_tombstoned_rows() {
        return encode_table_payload(data);
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
        &u32::try_from(data.row_count())
            .map_err(|_| DbError::constraint("table row count exceeds u32"))?
            .to_le_bytes(),
    );
    previous.extend_from_slice(&appended_rows);
    Ok(previous)
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

pub(crate) struct Cursor<'a> {
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

    fn read_u64(&mut self) -> Result<u64> {
        let bytes = self.read_slice(8)?;
        Ok(u64::from_le_bytes(bytes.try_into().expect("u64")))
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
    let Dataset { columns, rows } = dataset;
    QueryResult::with_rows(
        columns.into_iter().map(|binding| binding.name).collect(),
        Arc::unwrap_or_clone(rows)
            .into_iter()
            .map(QueryRow::new)
            .collect(),
    )
}

pub(crate) fn projection_has_aggregate_items(items: &[SelectItem]) -> bool {
    items.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } => expr_contains_aggregate(expr),
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    })
}

fn select_requires_grouped_evaluation(runtime: &EngineRuntime, select: &Select) -> Result<bool> {
    if !select.group_by.is_empty() || projection_has_aggregate_items(&select.projection) {
        return Ok(true);
    }
    if select.having.as_ref().is_some_and(expr_contains_aggregate) {
        return Ok(true);
    }
    projection_has_runtime_extension_aggregate_items(runtime, &select.projection).and_then(
        |has_projection_aggregate| {
            if has_projection_aggregate {
                return Ok(true);
            }
            select
                .having
                .as_ref()
                .map(|expr| expr_contains_runtime_extension_aggregate(runtime, expr))
                .transpose()
                .map(Option::unwrap_or_default)
        },
    )
}

fn projection_has_runtime_extension_aggregate_items(
    runtime: &EngineRuntime,
    items: &[SelectItem],
) -> Result<bool> {
    for item in items {
        if let SelectItem::Expr { expr, .. } = item {
            if expr_contains_runtime_extension_aggregate(runtime, expr)? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn ordered_lookup_terms_for_index<'a>(
    index: &IndexSchema,
    lookup_terms: &[(Option<&'a str>, &'a str, &'a Expr)],
) -> Result<Vec<(Option<&'a str>, &'a str, &'a Expr)>> {
    let mut ordered = Vec::with_capacity(lookup_terms.len());
    for index_column in index.columns.iter().take(lookup_terms.len()) {
        let Some(column_name) = index_column.column_name.as_deref() else {
            return Err(DbError::internal(
                "compound indexed projection matched expression index column",
            ));
        };
        let Some(term) = lookup_terms
            .iter()
            .copied()
            .find(|(_, lookup_column, _)| identifiers_equal(column_name, lookup_column))
        else {
            return Err(DbError::internal(
                "compound indexed projection matched non-prefix lookup terms",
            ));
        };
        ordered.push(term);
    }
    Ok(ordered)
}

fn row_ids_for_simple_indexed_projection_lookup<'a>(
    keys: &'a RuntimeBtreeKeys,
    plan: &SimpleIndexedProjectionPlan<'_>,
) -> Result<SimpleIndexedProjectionRowIds<'a>> {
    if plan.extra_lookup_terms.is_empty() {
        return keys
            .row_ids_for_value_set(&plan.lookup_value)
            .map(SimpleIndexedProjectionRowIds::Borrowed);
    }
    let values = std::iter::once(plan.lookup_value.clone())
        .chain(
            plan.extra_lookup_terms
                .iter()
                .map(|(_, value)| value.clone()),
        )
        .collect::<Vec<_>>();
    Ok(SimpleIndexedProjectionRowIds::Owned(keys.row_ids_for_key(
        &RuntimeBtreeKey::Encoded(RuntimeEncodedKey::from_vec(Row::new(values).encode()?)),
    )))
}

fn indexed_projection_row_id_order(
    plan: &SimpleIndexedProjectionPlan<'_>,
) -> Option<(bool, usize)> {
    let order_by = plan.order_by.as_ref()?;
    if order_by.len() != 1 {
        return None;
    }
    let row_id_alias = row_id_alias_column_name(plan.table_schema)?;
    let row_id_order = &order_by[0];
    if row_id_order.collation.is_some() {
        return None;
    }
    let projection_index = plan
        .projection_indexes
        .get(row_id_order.projection_index)
        .copied()?;
    let order_column = plan.table_schema.columns.get(projection_index)?;
    if !identifiers_equal(&order_column.name, row_id_alias) {
        return None;
    }
    let limit_with_offset = plan
        .limit
        .map(|limit| limit.saturating_add(plan.offset))
        .unwrap_or(usize::MAX);
    Some((row_id_order.descending, limit_with_offset))
}

fn view_projection_expr_for_output_column(items: &[SelectItem], column: &str) -> Option<Expr> {
    for (index, item) in items.iter().enumerate() {
        let SelectItem::Expr { expr, alias } = item else {
            continue;
        };
        let output_name = alias
            .as_deref()
            .map(std::borrow::Cow::Borrowed)
            .unwrap_or_else(|| std::borrow::Cow::Owned(infer_expr_name(expr, index + 1)));
        if identifiers_equal(output_name.as_ref(), column) {
            return Some(expr.clone());
        }
    }
    None
}

#[derive(Clone, Copy, Debug)]
struct SimpleRangeBound<'a> {
    inclusive: bool,
    value_expr: &'a Expr,
}

#[derive(Clone, Debug)]
pub(crate) struct SimpleRangeBoundValue {
    pub(crate) inclusive: bool,
    pub(crate) value: Value,
}

#[derive(Clone, Debug, Default)]
struct SimpleGroupedNumericState(NumericAggregateState);

impl SimpleGroupedNumericState {
    fn add(&mut self, value: &Value) -> Result<()> {
        self.0.add(value)
    }

    fn value(&self, kind: SimpleGroupedNumericAggregateKind) -> Result<Value> {
        let kind = match kind {
            SimpleGroupedNumericAggregateKind::Sum
            | SimpleGroupedNumericAggregateKind::SumDistinct => NumericAgg::Sum,
            SimpleGroupedNumericAggregateKind::Avg
            | SimpleGroupedNumericAggregateKind::AvgDistinct => NumericAgg::Avg,
            SimpleGroupedNumericAggregateKind::Total
            | SimpleGroupedNumericAggregateKind::TotalDistinct => NumericAgg::Total,
            _ => return Err(DbError::internal("invalid numeric aggregate kind")),
        };
        self.0.value(kind)
    }
}

#[derive(Clone, Debug, Default)]
struct SimpleGroupedVarianceState {
    count: u64,
    mean: f64,
    m2: f64,
}

impl SimpleGroupedVarianceState {
    fn add(&mut self, value: &Value) -> Result<()> {
        let number = match value {
            Value::Null => return Ok(()),
            Value::Int64(value) => *value as f64,
            Value::Float64(value) => *value,
            Value::Decimal { scaled, scale } => decimal_to_f64(*scaled, *scale),
            other => {
                return Err(DbError::sql(format!(
                    "variance aggregate does not support {other:?}"
                )))
            }
        };
        self.count += 1;
        let delta = number - self.mean;
        self.mean += delta / (self.count as f64);
        let delta2 = number - self.mean;
        self.m2 += delta * delta2;
        Ok(())
    }

    fn value(&self, kind: SimpleGroupedNumericAggregateKind) -> Value {
        if self.count == 0 {
            return Value::Null;
        }
        let denominator = match kind {
            SimpleGroupedNumericAggregateKind::StddevPop
            | SimpleGroupedNumericAggregateKind::StddevPopDistinct
            | SimpleGroupedNumericAggregateKind::VarPop
            | SimpleGroupedNumericAggregateKind::VarPopDistinct => self.count as f64,
            SimpleGroupedNumericAggregateKind::StddevSamp
            | SimpleGroupedNumericAggregateKind::StddevSampDistinct
            | SimpleGroupedNumericAggregateKind::VarSamp
            | SimpleGroupedNumericAggregateKind::VarSampDistinct => {
                if self.count < 2 {
                    return Value::Null;
                }
                (self.count - 1) as f64
            }
            _ => return Value::Null,
        };
        let variance = self.m2 / denominator;
        match kind {
            SimpleGroupedNumericAggregateKind::StddevPop
            | SimpleGroupedNumericAggregateKind::StddevPopDistinct
            | SimpleGroupedNumericAggregateKind::StddevSamp
            | SimpleGroupedNumericAggregateKind::StddevSampDistinct => {
                Value::Float64(variance.sqrt())
            }
            SimpleGroupedNumericAggregateKind::VarPop
            | SimpleGroupedNumericAggregateKind::VarPopDistinct
            | SimpleGroupedNumericAggregateKind::VarSamp
            | SimpleGroupedNumericAggregateKind::VarSampDistinct => Value::Float64(variance),
            _ => Value::Null,
        }
    }
}

#[derive(Clone, Debug)]
struct SimpleGroupedBoolState {
    saw_non_null: bool,
    and_value: bool,
    or_value: bool,
}

impl Default for SimpleGroupedBoolState {
    fn default() -> Self {
        Self {
            saw_non_null: false,
            and_value: true,
            or_value: false,
        }
    }
}

impl SimpleGroupedBoolState {
    fn add(&mut self, value: &Value) -> Result<()> {
        let boolean = match value {
            Value::Null => return Ok(()),
            Value::Bool(value) => *value,
            other => {
                return Err(DbError::sql(format!(
                    "boolean aggregate does not support {other:?}"
                )))
            }
        };
        self.saw_non_null = true;
        self.and_value &= boolean;
        self.or_value |= boolean;
        Ok(())
    }

    fn value(&self, kind: SimpleGroupedNumericAggregateKind) -> Value {
        if !self.saw_non_null {
            return Value::Null;
        }
        match kind {
            SimpleGroupedNumericAggregateKind::BoolAnd
            | SimpleGroupedNumericAggregateKind::BoolAndDistinct => Value::Bool(self.and_value),
            SimpleGroupedNumericAggregateKind::BoolOr
            | SimpleGroupedNumericAggregateKind::BoolOrDistinct => Value::Bool(self.or_value),
            _ => Value::Null,
        }
    }
}

#[derive(Clone, Debug)]
struct SimpleGroupedNumericAggregate {
    group_values: Vec<Value>,
    count: i64,
    value_counts: Vec<i64>,
    distinct_values: Vec<BTreeSet<Vec<u8>>>,
    numeric_states: Vec<SimpleGroupedNumericState>,
    variance_states: Vec<SimpleGroupedVarianceState>,
    bool_states: Vec<SimpleGroupedBoolState>,
    extreme_values: Vec<Value>,
}

impl SimpleGroupedNumericAggregate {
    fn new(group_values: Vec<Value>, aggregate_count: usize) -> Self {
        Self {
            group_values,
            count: 0,
            value_counts: vec![0; aggregate_count],
            distinct_values: vec![BTreeSet::new(); aggregate_count],
            numeric_states: vec![SimpleGroupedNumericState::default(); aggregate_count],
            variance_states: vec![SimpleGroupedVarianceState::default(); aggregate_count],
            bool_states: vec![SimpleGroupedBoolState::default(); aggregate_count],
            extreme_values: vec![Value::Null; aggregate_count],
        }
    }

    fn add_numeric(&mut self, aggregate_index: usize, value: &Value) -> Result<()> {
        self.numeric_states[aggregate_index].add(value)
    }

    fn count_non_null(&mut self, aggregate_index: usize, value: &Value) {
        if !matches!(value, Value::Null) {
            self.value_counts[aggregate_index] += 1;
        }
    }

    fn count_distinct(&mut self, aggregate_index: usize, value: &Value) -> Result<()> {
        if matches!(value, Value::Null) {
            return Ok(());
        }
        let key = row_identity(std::slice::from_ref(value))?;
        if self.distinct_values[aggregate_index].insert(key) {
            self.value_counts[aggregate_index] += 1;
        }
        Ok(())
    }

    fn add_numeric_distinct(&mut self, aggregate_index: usize, value: &Value) -> Result<()> {
        if matches!(value, Value::Null) {
            return Ok(());
        }
        let key = row_identity(std::slice::from_ref(value))?;
        if self.distinct_values[aggregate_index].insert(key) {
            self.numeric_states[aggregate_index].add(value)?;
        }
        Ok(())
    }

    fn add_variance(&mut self, aggregate_index: usize, value: &Value) -> Result<()> {
        self.variance_states[aggregate_index].add(value)
    }

    fn add_variance_distinct(&mut self, aggregate_index: usize, value: &Value) -> Result<()> {
        if matches!(value, Value::Null) {
            return Ok(());
        }
        let key = row_identity(std::slice::from_ref(value))?;
        if self.distinct_values[aggregate_index].insert(key) {
            self.variance_states[aggregate_index].add(value)?;
        }
        Ok(())
    }

    fn add_bool(&mut self, aggregate_index: usize, value: &Value) -> Result<()> {
        self.bool_states[aggregate_index].add(value)
    }

    fn add_bool_distinct(&mut self, aggregate_index: usize, value: &Value) -> Result<()> {
        if matches!(value, Value::Null) {
            return Ok(());
        }
        let key = row_identity(std::slice::from_ref(value))?;
        if self.distinct_values[aggregate_index].insert(key) {
            self.bool_states[aggregate_index].add(value)?;
        }
        Ok(())
    }

    fn aggregate_value(
        &self,
        kind: SimpleGroupedNumericAggregateKind,
        aggregate_index: usize,
    ) -> Result<Value> {
        Ok(match kind {
            SimpleGroupedNumericAggregateKind::CountRows => Value::Int64(self.count),
            SimpleGroupedNumericAggregateKind::CountNonNull
            | SimpleGroupedNumericAggregateKind::CountDistinct => {
                Value::Int64(self.value_counts[aggregate_index])
            }
            SimpleGroupedNumericAggregateKind::Sum
            | SimpleGroupedNumericAggregateKind::SumDistinct
            | SimpleGroupedNumericAggregateKind::Avg
            | SimpleGroupedNumericAggregateKind::AvgDistinct
            | SimpleGroupedNumericAggregateKind::Total
            | SimpleGroupedNumericAggregateKind::TotalDistinct => {
                self.numeric_states[aggregate_index].value(kind)?
            }
            SimpleGroupedNumericAggregateKind::StddevSamp
            | SimpleGroupedNumericAggregateKind::StddevSampDistinct
            | SimpleGroupedNumericAggregateKind::StddevPop
            | SimpleGroupedNumericAggregateKind::StddevPopDistinct
            | SimpleGroupedNumericAggregateKind::VarSamp
            | SimpleGroupedNumericAggregateKind::VarSampDistinct
            | SimpleGroupedNumericAggregateKind::VarPop
            | SimpleGroupedNumericAggregateKind::VarPopDistinct => {
                self.variance_states[aggregate_index].value(kind)
            }
            SimpleGroupedNumericAggregateKind::BoolAnd
            | SimpleGroupedNumericAggregateKind::BoolAndDistinct
            | SimpleGroupedNumericAggregateKind::BoolOr
            | SimpleGroupedNumericAggregateKind::BoolOrDistinct => {
                self.bool_states[aggregate_index].value(kind)
            }
            SimpleGroupedNumericAggregateKind::Min | SimpleGroupedNumericAggregateKind::Max => {
                self.extreme_values[aggregate_index].clone()
            }
        })
    }

    fn into_row(
        self,
        aggregate_bindings: &[SimpleGroupedNumericAggregateBinding],
    ) -> Result<QueryRow> {
        let mut group_values = self.group_values.clone();
        for (aggregate_index, aggregate) in aggregate_bindings.iter().enumerate() {
            group_values.push(self.aggregate_value(aggregate.kind, aggregate_index)?);
        }
        Ok(QueryRow::new(group_values))
    }

    fn aggregate_values(
        &self,
        aggregate_bindings: &[SimpleGroupedNumericAggregateBinding],
    ) -> Result<Vec<Value>> {
        aggregate_bindings
            .iter()
            .enumerate()
            .map(|(aggregate_index, aggregate)| {
                self.aggregate_value(aggregate.kind, aggregate_index)
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
struct SimpleGroupedCountAggregate {
    group_values: Vec<Value>,
    count: i64,
}

impl SimpleGroupedCountAggregate {
    fn new(group_values: Vec<Value>) -> Self {
        Self {
            group_values,
            count: 0,
        }
    }

    fn into_row(self) -> QueryRow {
        let mut row = self.group_values;
        row.push(Value::Int64(self.count));
        QueryRow::new(row)
    }

    fn aggregate_values(&self) -> Vec<Value> {
        vec![Value::Int64(self.count)]
    }
}

fn evaluate_simple_grouped_values(
    runtime: &EngineRuntime,
    exprs: &[Expr],
    dataset: &Dataset,
    row: &[Value],
    params: &[Value],
) -> Result<Vec<Value>> {
    exprs
        .iter()
        .map(|expr| runtime.eval_expr(expr, dataset, row, params, &BTreeMap::new(), None))
        .collect()
}

fn render_simple_grouped_numeric_aggregate_groups(
    runtime: &EngineRuntime,
    groups: Vec<SimpleGroupedNumericAggregate>,
    plan: &SimpleGroupedNumericAggregatePlan<'_>,
    params: &[Value],
) -> Result<QueryResult> {
    if let (Some(projection_exprs), Some(raw_projection_bindings)) =
        (&plan.projection_exprs, &plan.raw_projection_bindings)
    {
        let raw_dataset = Dataset::with_rows(raw_projection_bindings.clone(), Vec::new());
        let mut rows = Vec::with_capacity(groups.len());
        for group in groups {
            let mut raw_values = group.group_values.clone();
            raw_values.extend(group.aggregate_values(&plan.aggregate_bindings)?);
            if let Some(having) = plan.having.as_ref() {
                if !matches!(
                    runtime.eval_expr(
                        having,
                        &raw_dataset,
                        &raw_values,
                        params,
                        &BTreeMap::new(),
                        None,
                    )?,
                    Value::Bool(true)
                ) {
                    continue;
                }
            }
            let output = projection_exprs
                .iter()
                .map(|expr| {
                    runtime.eval_expr(
                        expr,
                        &raw_dataset,
                        &raw_values,
                        params,
                        &BTreeMap::new(),
                        None,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            rows.push(QueryRow::new(output));
        }
        return runtime.apply_simple_grouped_postprocessing(
            rows,
            plan.column_names.clone(),
            &[],
            None,
            params,
            plan.order_by.as_deref(),
            plan.limit,
            plan.offset,
        );
    }

    runtime.apply_simple_grouped_postprocessing(
        groups
            .into_iter()
            .map(|group| group.into_row(&plan.aggregate_bindings))
            .collect::<Result<Vec<_>>>()?,
        plan.column_names.clone(),
        &plan.having_bindings,
        plan.having.as_ref(),
        params,
        plan.order_by.as_deref(),
        plan.limit,
        plan.offset,
    )
}

fn render_simple_grouped_count_groups(
    runtime: &EngineRuntime,
    groups: Vec<SimpleGroupedCountAggregate>,
    plan: &SimpleGroupedCountPlan<'_>,
    params: &[Value],
) -> Result<QueryResult> {
    if let (Some(projection_exprs), Some(raw_projection_bindings)) =
        (&plan.projection_exprs, &plan.raw_projection_bindings)
    {
        let raw_dataset = Dataset::with_rows(raw_projection_bindings.clone(), Vec::new());
        let mut rows = Vec::with_capacity(groups.len());
        for group in groups {
            let mut raw_values = group.group_values.clone();
            raw_values.extend(group.aggregate_values());
            if let Some(having) = plan.having.as_ref() {
                if !matches!(
                    runtime.eval_expr(
                        having,
                        &raw_dataset,
                        &raw_values,
                        params,
                        &BTreeMap::new(),
                        None,
                    )?,
                    Value::Bool(true)
                ) {
                    continue;
                }
            }
            let output = projection_exprs
                .iter()
                .map(|expr| {
                    runtime.eval_expr(
                        expr,
                        &raw_dataset,
                        &raw_values,
                        params,
                        &BTreeMap::new(),
                        None,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            rows.push(QueryRow::new(output));
        }
        return runtime.apply_simple_grouped_postprocessing(
            rows,
            plan.column_names.clone(),
            &[],
            None,
            params,
            plan.order_by.as_deref(),
            plan.limit,
            plan.offset,
        );
    }

    runtime.apply_simple_grouped_postprocessing(
        groups
            .into_iter()
            .map(SimpleGroupedCountAggregate::into_row),
        plan.column_names.clone(),
        &plan.having_bindings,
        plan.having.as_ref(),
        params,
        plan.order_by.as_deref(),
        plan.limit,
        plan.offset,
    )
}

fn update_simple_min_max_value(best: &mut Value, candidate: Value, is_max: bool) -> Result<()> {
    if matches!(candidate, Value::Null) {
        return Ok(());
    }
    if matches!(best, Value::Null) {
        *best = candidate;
        return Ok(());
    }
    let ordering = compare_values(&candidate, best)?;
    let should_replace = if is_max {
        ordering == std::cmp::Ordering::Greater
    } else {
        ordering == std::cmp::Ordering::Less
    };
    if should_replace {
        *best = candidate;
    }
    Ok(())
}

fn expr_references_only_binding(expr: &Expr, binding: TableBindingRef<'_>) -> bool {
    match expr {
        Expr::Default => false,
        Expr::Literal(_) | Expr::Parameter(_) => true,
        Expr::Column { table, .. } => table
            .as_deref()
            .is_none_or(|qualifier| identifiers_equal(qualifier, binding.binding_name())),
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Collate { expr, .. } => expr_references_only_binding(expr, binding),
        Expr::Binary { left, right, .. } => {
            expr_references_only_binding(left, binding)
                && expr_references_only_binding(right, binding)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_only_binding(expr, binding)
                && expr_references_only_binding(low, binding)
                && expr_references_only_binding(high, binding)
        }
        Expr::InList { expr, items, .. } => {
            expr_references_only_binding(expr, binding)
                && items
                    .iter()
                    .all(|item| expr_references_only_binding(item, binding))
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_references_only_binding(expr, binding)
                && expr_references_only_binding(pattern, binding)
                && escape
                    .as_ref()
                    .is_none_or(|expr| expr_references_only_binding(expr, binding))
        }
        Expr::Function { args, .. } => args
            .iter()
            .all(|arg| expr_references_only_binding(arg, binding)),
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_none_or(|expr| expr_references_only_binding(expr, binding))
                && branches.iter().all(|(condition, value)| {
                    expr_references_only_binding(condition, binding)
                        && expr_references_only_binding(value, binding)
                })
                && else_expr
                    .as_ref()
                    .is_none_or(|expr| expr_references_only_binding(expr, binding))
        }
        Expr::Row(items) => items
            .iter()
            .all(|item| expr_references_only_binding(item, binding)),
        Expr::Aggregate { .. }
        | Expr::RowNumber { .. }
        | Expr::WindowFunction { .. }
        | Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_) => false,
    }
}

fn expr_references_binding_names(expr: &Expr, table_name: &str, binding_name: &str) -> bool {
    match expr {
        Expr::Default => false,
        Expr::Literal(_) | Expr::Parameter(_) => true,
        Expr::Column { table, .. } => table.as_deref().is_none_or(|qualifier| {
            identifiers_equal(qualifier, table_name) || identifiers_equal(qualifier, binding_name)
        }),
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Collate { expr, .. } => {
            expr_references_binding_names(expr, table_name, binding_name)
        }
        Expr::Binary { left, right, .. } => {
            expr_references_binding_names(left, table_name, binding_name)
                && expr_references_binding_names(right, table_name, binding_name)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_binding_names(expr, table_name, binding_name)
                && expr_references_binding_names(low, table_name, binding_name)
                && expr_references_binding_names(high, table_name, binding_name)
        }
        Expr::InList { expr, items, .. } => {
            expr_references_binding_names(expr, table_name, binding_name)
                && items
                    .iter()
                    .all(|item| expr_references_binding_names(item, table_name, binding_name))
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_references_binding_names(expr, table_name, binding_name)
                && expr_references_binding_names(pattern, table_name, binding_name)
                && escape.as_ref().is_none_or(|expr| {
                    expr_references_binding_names(expr, table_name, binding_name)
                })
        }
        Expr::Function { args, .. } => args
            .iter()
            .all(|arg| expr_references_binding_names(arg, table_name, binding_name)),
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_none_or(|expr| expr_references_binding_names(expr, table_name, binding_name))
                && branches.iter().all(|(condition, value)| {
                    expr_references_binding_names(condition, table_name, binding_name)
                        && expr_references_binding_names(value, table_name, binding_name)
                })
                && else_expr.as_ref().is_none_or(|expr| {
                    expr_references_binding_names(expr, table_name, binding_name)
                })
        }
        Expr::Row(items) => items
            .iter()
            .all(|item| expr_references_binding_names(item, table_name, binding_name)),
        Expr::Aggregate { .. }
        | Expr::RowNumber { .. }
        | Expr::WindowFunction { .. }
        | Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_) => false,
    }
}

fn expr_resolves_against_dataset(expr: &Expr, dataset: &Dataset) -> bool {
    match expr {
        Expr::Default => false,
        Expr::Literal(_) | Expr::Parameter(_) => true,
        Expr::Column { table, column } => {
            simple_select_item_column_index(dataset, table.as_deref(), column).is_some()
        }
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Collate { expr, .. } => expr_resolves_against_dataset(expr, dataset),
        Expr::Binary { left, right, .. } => {
            expr_resolves_against_dataset(left, dataset)
                && expr_resolves_against_dataset(right, dataset)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_resolves_against_dataset(expr, dataset)
                && expr_resolves_against_dataset(low, dataset)
                && expr_resolves_against_dataset(high, dataset)
        }
        Expr::InList { expr, items, .. } => {
            expr_resolves_against_dataset(expr, dataset)
                && items
                    .iter()
                    .all(|item| expr_resolves_against_dataset(item, dataset))
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_resolves_against_dataset(expr, dataset)
                && expr_resolves_against_dataset(pattern, dataset)
                && escape
                    .as_ref()
                    .is_none_or(|expr| expr_resolves_against_dataset(expr, dataset))
        }
        Expr::Function { args, .. } => args
            .iter()
            .all(|arg| expr_resolves_against_dataset(arg, dataset)),
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_none_or(|expr| expr_resolves_against_dataset(expr, dataset))
                && branches.iter().all(|(condition, value)| {
                    expr_resolves_against_dataset(condition, dataset)
                        && expr_resolves_against_dataset(value, dataset)
                })
                && else_expr
                    .as_ref()
                    .is_none_or(|expr| expr_resolves_against_dataset(expr, dataset))
        }
        Expr::Row(items) => items
            .iter()
            .all(|item| expr_resolves_against_dataset(item, dataset)),
        Expr::Aggregate { .. }
        | Expr::RowNumber { .. }
        | Expr::WindowFunction { .. }
        | Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_) => false,
    }
}

fn grouped_projection_expr_matches_group_expr(
    projection_expr: &Expr,
    group_expr: &Expr,
    binding: TableBindingRef<'_>,
) -> bool {
    if projection_expr == group_expr {
        return true;
    }
    matches!(group_expr, Expr::Column { column, .. } if expr_matches_binding_column(projection_expr, binding, column))
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

#[derive(Clone, Debug)]
pub(crate) struct SimpleOrderByPlan {
    projection_index: usize,
    descending: bool,
    collation: Option<Collation>,
}

#[derive(Clone, Copy, Debug)]
enum SimpleExpressionProjectionSource<'a> {
    Column(usize),
    Expr(&'a Expr),
}

#[derive(Clone, Debug)]
pub(crate) struct SimpleExpressionProjectionPlan<'a> {
    sources: Vec<SimpleExpressionProjectionSource<'a>>,
    column_names: Vec<String>,
}

#[derive(Clone, Debug)]
pub(crate) enum SimpleJoinProjectionSource {
    Left(usize),
    Right(usize),
    Expr(Expr),
}

#[derive(Clone, Debug)]
pub(crate) struct SimpleRangeProjectionFilter<'a> {
    table: Option<&'a str>,
    column: &'a str,
    lower: Option<SimpleRangeBound<'a>>,
    upper: Option<SimpleRangeBound<'a>>,
    residual: Vec<SimpleResidualFilterTerm<'a>>,
}

#[derive(Clone, Copy, Debug)]
struct SimpleResidualFilterTerm<'a> {
    table: Option<&'a str>,
    column: &'a str,
    op: BinaryOp,
    value_expr: &'a Expr,
}

#[derive(Clone, Debug)]
pub(crate) struct SimpleResidualPlan {
    column_index: usize,
    op: BinaryOp,
    value: Value,
}

fn residual_like_filter_can_use_direct_scan(filter: &Expr) -> bool {
    simple_contains_like_projection_filter(filter).is_some()
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SimpleRangeFilterState<'a> {
    table: Option<&'a str>,
    column: Option<&'a str>,
    lower: Option<SimpleRangeBound<'a>>,
    upper: Option<SimpleRangeBound<'a>>,
    residual: Vec<SimpleResidualFilterTerm<'a>>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum SimpleRangeBoundKind {
    Lower(bool),
    Upper(bool),
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

#[derive(Clone, Copy, Debug)]
pub(crate) struct QualifiedColumnRef<'a> {
    table: Option<&'a str>,
    column: &'a str,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct TableBindingRef<'a> {
    name: &'a str,
    alias: &'a Option<String>,
}

impl<'a> TableBindingRef<'a> {
    fn binding_name(self) -> &'a str {
        self.alias.as_deref().unwrap_or(self.name)
    }
}

#[derive(Clone, Copy, Debug)]
struct SpatialJoinOrientation<'a> {
    indexed_table: TableBindingRef<'a>,
    indexed_ref: QualifiedColumnRef<'a>,
    probe_table: TableBindingRef<'a>,
    probe_ref: QualifiedColumnRef<'a>,
    indexed_on_left: bool,
    left_alias: &'a Option<String>,
    right_alias: &'a Option<String>,
    constraint: &'a JoinConstraint,
    radius_expr: Option<&'a Expr>,
    params: &'a [Value],
    ctes: &'a BTreeMap<String, Dataset>,
}

#[derive(Clone, Debug)]
pub(crate) struct IndexedJoinPlan<'a> {
    filtered_table: TableBindingRef<'a>,
    filtered_dataset: &'a Dataset,
    filtered_join_columns: Vec<&'a str>,
    probe_table: TableBindingRef<'a>,
    probe_join_columns: Vec<&'a str>,
    filtered_on_left: bool,
}

#[derive(Clone, Copy, Debug)]
struct DeferredViewProjection {
    table_index: usize,
    column_index: usize,
    is_rowid_alias: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct DeferredViewJoinStep {
    previous_table_index: usize,
    previous_column_index: usize,
    current_table_index: usize,
    current_index_name: String,
    /// True when `previous_column_index` is the previous table's row-id alias
    /// (single-column `INTEGER PRIMARY KEY` auto-increment column). In that case
    /// the join key value is exactly the previous row's `row_id`, so it never
    /// needs to be decoded from the projected row and is omitted from the
    /// projection entirely.
    previous_is_rowid_alias: bool,
}

#[derive(Clone)]
pub(crate) enum DeferredViewTableRowReader<'a> {
    Source(VisibleTableRowSource<'a>),
    Deferred {
        state: PersistedTableState,
        schema: &'a TableSchema,
        paged_locator_cache: Option<&'a DeferredPagedRowLocatorCache>,
    },
}

impl<'a> DeferredViewTableRowReader<'a> {
    /// Reads from data already owned by the observed-current runtime.
    ///
    /// The outer `Option` reports whether the lookup can be completed without
    /// storage; the inner `Option` distinguishes an absent row from a row that
    /// was decoded successfully. Verified paged payloads were checksummed when
    /// the immutable locator cache was built.
    fn read_projected_from_observed_cache(
        &self,
        row_id: i64,
        projection_indexes: &[usize],
    ) -> Result<Option<Option<StoredRow>>> {
        match *self {
            Self::Source(source) => source
                .projected_values_by_id(row_id, projection_indexes)
                .map(|values| Some(values.map(|values| StoredRow { row_id, values }))),
            Self::Deferred {
                state,
                paged_locator_cache,
                ..
            } => {
                let Some(cache) = paged_locator_cache.filter(|cache| cache.matches_state(state))
                else {
                    return Ok(None);
                };
                let Some(cached) = cache.locators.get(row_id) else {
                    return Ok(Some(None));
                };
                let Some(payload) = cache.verified_payload(cached.pointer, cached.checksum) else {
                    return Ok(None);
                };
                decode_projected_values_by_locator_from_payload::<page::InMemoryPageStore>(
                    None,
                    payload,
                    cached.locator,
                    projection_indexes,
                )
                .map(|values| Some(Some(StoredRow { row_id, values })))
            }
        }
    }

    fn read_projected_with_chunk_cache<S: PageStore>(
        &self,
        store: &S,
        row_id: i64,
        use_persistent_pk_index: bool,
        projection_indexes: &[usize],
        chunk_payload_cache: &mut HashMap<CachedPagedChunkPayloadKey, Arc<Vec<u8>>>,
    ) -> Result<Option<StoredRow>> {
        match *self {
            Self::Source(source) => source
                .projected_values_by_id(
                    row_id,
                    if projection_indexes.is_empty() {
                        &[]
                    } else {
                        projection_indexes
                    },
                )
                .map(|values| values.map(|values| StoredRow { row_id, values })),
            Self::Deferred {
                state,
                schema,
                paged_locator_cache,
            } => {
                if !projection_indexes.is_empty() && state.pointer.is_table_paged_manifest() {
                    if let Some(cache) =
                        paged_locator_cache.filter(|cache| cache.matches_state(state))
                    {
                        return cache
                            .locators
                            .get(row_id)
                            .map(|cached| {
                                read_deferred_projected_values_by_cached_paged_locator_with_query_cache(
                                    store,
                                    cached,
                                    cache.verified_payload_arc(cached.pointer, cached.checksum),
                                    chunk_payload_cache,
                                    projection_indexes,
                                )
                            })
                            .transpose()
                            .map(|values| values.map(|values| StoredRow { row_id, values }));
                    }
                }
                read_deferred_projected_values_by_id(
                    store,
                    state,
                    schema,
                    row_id,
                    use_persistent_pk_index,
                    paged_locator_cache,
                    projection_indexes,
                )
                .map(|values| values.map(|values| StoredRow { row_id, values }))
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DeferredViewTableProjection {
    projection_indexes: Vec<usize>,
    projected_positions: Vec<Option<usize>>,
}

impl DeferredViewTableProjection {
    fn new(schema: &TableSchema, required_columns: BTreeSet<usize>) -> Self {
        let projection_indexes = required_columns.into_iter().collect::<Vec<_>>();
        let mut projected_positions = vec![None; schema.columns.len()];
        for (position, column_index) in projection_indexes.iter().copied().enumerate() {
            projected_positions[column_index] = Some(position);
        }
        Self {
            projection_indexes,
            projected_positions,
        }
    }

    fn position(&self, column_index: usize) -> Option<usize> {
        self.projected_positions
            .get(column_index)
            .copied()
            .flatten()
    }
}

fn build_deferred_view_table_projections(
    table_schemas: &[&TableSchema],
    projections: &[DeferredViewProjection],
    join_steps: &[DeferredViewJoinStep],
) -> Vec<DeferredViewTableProjection> {
    let mut required_columns = table_schemas
        .iter()
        .map(|_| BTreeSet::new())
        .collect::<Vec<_>>();
    for projection in projections {
        if projection.is_rowid_alias {
            continue;
        }
        required_columns[projection.table_index].insert(projection.column_index);
    }
    for step in join_steps {
        if step.previous_is_rowid_alias {
            continue;
        }
        required_columns[step.previous_table_index].insert(step.previous_column_index);
    }
    table_schemas
        .iter()
        .zip(required_columns)
        .map(|(schema, columns)| DeferredViewTableProjection::new(schema, columns))
        .collect()
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum DeferredViewProjectionSource {
    RowId {
        table_index: usize,
    },
    Projected {
        table_index: usize,
        projected_index: usize,
    },
}

fn build_deferred_view_projection_indexes(
    projections: &[DeferredViewProjection],
    table_projections: &[DeferredViewTableProjection],
    context: &str,
) -> Result<Vec<DeferredViewProjectionSource>> {
    let mut projection_indexes = Vec::with_capacity(projections.len());
    for projection in projections {
        if projection.is_rowid_alias {
            projection_indexes.push(DeferredViewProjectionSource::RowId {
                table_index: projection.table_index,
            });
            continue;
        }
        let projected_index = table_projections[projection.table_index]
            .position(projection.column_index)
            .ok_or_else(|| {
                DbError::internal(format!(
                    "{context} projection is missing required column index {}",
                    projection.column_index
                ))
            })?;
        projection_indexes.push(DeferredViewProjectionSource::Projected {
            table_index: projection.table_index,
            projected_index,
        });
    }
    Ok(projection_indexes)
}

fn collect_deferred_view_projection_values(
    partial_rows: &[StoredRow],
    projection_indexes: &[DeferredViewProjectionSource],
    context: &str,
) -> Result<Vec<Value>> {
    let mut values = Vec::with_capacity(projection_indexes.len());
    if projection_indexes.is_empty() {
        return Ok(values);
    }
    for source in projection_indexes.iter().copied() {
        let (table_index, projected_index) = match source {
            DeferredViewProjectionSource::RowId { table_index } => {
                let Some(row) = partial_rows.get(table_index) else {
                    return Err(DbError::internal(format!(
                        "{context} row is shorter than planned schema",
                    )));
                };
                values.push(Value::Int64(row.row_id));
                continue;
            }
            DeferredViewProjectionSource::Projected {
                table_index,
                projected_index,
            } => (table_index, projected_index),
        };
        let Some(row) = partial_rows.get(table_index) else {
            return Err(DbError::internal(format!(
                "{context} row is shorter than planned schema",
            )));
        };
        let Some(value) = row.values.get(projected_index) else {
            return Err(DbError::internal(format!(
                "{context} projection row is shorter than planned schema",
            )));
        };
        values.push(value.clone());
    }
    Ok(values)
}

fn deferred_view_linear_tail_projection_can_move(
    projection_indexes: &[DeferredViewProjectionSource],
) -> bool {
    let mut previous = None;
    for source in projection_indexes {
        let DeferredViewProjectionSource::Projected {
            table_index: 2,
            projected_index,
        } = *source
        else {
            continue;
        };
        if previous.is_some_and(|previous| projected_index <= previous) {
            return false;
        }
        previous = Some(projected_index);
    }
    true
}

fn collect_deferred_view_query_row_from_linear_tail(
    root_row: &StoredRow,
    row1: &StoredRow,
    row2: StoredRow,
    projection_indexes: &[DeferredViewProjectionSource],
    context: &str,
    row2_can_move: bool,
) -> Result<QueryRow> {
    if row2_can_move && projection_indexes.len() == 4 {
        if let [DeferredViewProjectionSource::RowId { table_index: 0 }, DeferredViewProjectionSource::Projected {
            table_index: 0,
            projected_index: 0,
        }, DeferredViewProjectionSource::Projected {
            table_index: 1,
            projected_index: 0,
        }, DeferredViewProjectionSource::Projected {
            table_index: 2,
            projected_index: 0,
        }] = projection_indexes
        {
            let Some(root_value) = root_row.values.first() else {
                return Err(DbError::internal(format!(
                    "{context} projection row is shorter than planned schema",
                )));
            };
            let Some(row1_value) = row1.values.first() else {
                return Err(DbError::internal(format!(
                    "{context} projection row is shorter than planned schema",
                )));
            };
            let mut row2_values = row2.values.into_iter();
            let Some(row2_value) = row2_values.next() else {
                return Err(DbError::internal(format!(
                    "{context} projection row is shorter than planned schema",
                )));
            };
            return Ok(QueryRow::from_small_values(smallvec![
                Value::Int64(root_row.row_id),
                root_value.clone(),
                row1_value.clone(),
                row2_value,
            ]));
        }
    }

    if row2_can_move && projection_indexes.len() == 3 {
        if let [DeferredViewProjectionSource::Projected {
            table_index: 1,
            projected_index: 0,
        }, DeferredViewProjectionSource::Projected {
            table_index: 2,
            projected_index: 0,
        }, DeferredViewProjectionSource::Projected {
            table_index: 2,
            projected_index: 1,
        }] = projection_indexes
        {
            let Some(row1_value) = row1.values.first() else {
                return Err(DbError::internal(format!(
                    "{context} projection row is shorter than planned schema",
                )));
            };
            let mut row2_values = row2.values.into_iter();
            let Some(row2_first) = row2_values.next() else {
                return Err(DbError::internal(format!(
                    "{context} projection row is shorter than planned schema",
                )));
            };
            let Some(row2_second) = row2_values.next() else {
                return Err(DbError::internal(format!(
                    "{context} projection row is shorter than planned schema",
                )));
            };
            return Ok(QueryRow::from_small_values(smallvec![
                row1_value.clone(),
                row2_first,
                row2_second,
            ]));
        }
    }

    collect_deferred_view_projection_values_from_linear_tail(
        root_row,
        row1,
        row2,
        projection_indexes,
        context,
        row2_can_move,
    )
    .map(QueryRow::new)
}

fn collect_deferred_view_projection_values_from_linear_tail(
    root_row: &StoredRow,
    row1: &StoredRow,
    row2: StoredRow,
    projection_indexes: &[DeferredViewProjectionSource],
    context: &str,
    row2_can_move: bool,
) -> Result<Vec<Value>> {
    if row2_can_move && projection_indexes.len() == 4 {
        if let [DeferredViewProjectionSource::RowId { table_index: 0 }, DeferredViewProjectionSource::Projected {
            table_index: 0,
            projected_index: 0,
        }, DeferredViewProjectionSource::Projected {
            table_index: 1,
            projected_index: 0,
        }, DeferredViewProjectionSource::Projected {
            table_index: 2,
            projected_index: 0,
        }] = projection_indexes
        {
            let Some(root_value) = root_row.values.first() else {
                return Err(DbError::internal(format!(
                    "{context} projection row is shorter than planned schema",
                )));
            };
            let Some(row1_value) = row1.values.first() else {
                return Err(DbError::internal(format!(
                    "{context} projection row is shorter than planned schema",
                )));
            };
            let mut row2_values = row2.values.into_iter();
            let Some(row2_value) = row2_values.next() else {
                return Err(DbError::internal(format!(
                    "{context} projection row is shorter than planned schema",
                )));
            };
            return Ok(vec![
                Value::Int64(root_row.row_id),
                root_value.clone(),
                row1_value.clone(),
                row2_value,
            ]);
        }
    }

    if row2_can_move && projection_indexes.len() == 3 {
        if let [DeferredViewProjectionSource::Projected {
            table_index: 1,
            projected_index: 0,
        }, DeferredViewProjectionSource::Projected {
            table_index: 2,
            projected_index: 0,
        }, DeferredViewProjectionSource::Projected {
            table_index: 2,
            projected_index: 1,
        }] = projection_indexes
        {
            let Some(row1_value) = row1.values.first() else {
                return Err(DbError::internal(format!(
                    "{context} projection row is shorter than planned schema",
                )));
            };
            let mut row2_values = row2.values.into_iter();
            let Some(row2_first) = row2_values.next() else {
                return Err(DbError::internal(format!(
                    "{context} projection row is shorter than planned schema",
                )));
            };
            let Some(row2_second) = row2_values.next() else {
                return Err(DbError::internal(format!(
                    "{context} projection row is shorter than planned schema",
                )));
            };
            return Ok(vec![row1_value.clone(), row2_first, row2_second]);
        }
    }

    let row2_row_id = row2.row_id;
    let mut row2_values = row2.values;
    let mut row2_removed = 0usize;
    let mut values = Vec::with_capacity(projection_indexes.len());
    for source in projection_indexes.iter().copied() {
        match source {
            DeferredViewProjectionSource::RowId { table_index } => {
                let row_id = match table_index {
                    0 => root_row.row_id,
                    1 => row1.row_id,
                    2 => row2_row_id,
                    _ => {
                        return Err(DbError::internal(format!(
                            "{context} row is shorter than planned schema",
                        )));
                    }
                };
                values.push(Value::Int64(row_id));
            }
            DeferredViewProjectionSource::Projected {
                table_index,
                projected_index,
            } => match table_index {
                0 => {
                    let Some(value) = root_row.values.get(projected_index) else {
                        return Err(DbError::internal(format!(
                            "{context} projection row is shorter than planned schema",
                        )));
                    };
                    values.push(value.clone());
                }
                1 => {
                    let Some(value) = row1.values.get(projected_index) else {
                        return Err(DbError::internal(format!(
                            "{context} projection row is shorter than planned schema",
                        )));
                    };
                    values.push(value.clone());
                }
                2 if row2_can_move => {
                    let Some(adjusted_index) = projected_index.checked_sub(row2_removed) else {
                        return Err(DbError::internal(format!(
                            "{context} projection row is shorter than planned schema",
                        )));
                    };
                    if adjusted_index >= row2_values.len() {
                        return Err(DbError::internal(format!(
                            "{context} projection row is shorter than planned schema",
                        )));
                    }
                    values.push(row2_values.remove(adjusted_index));
                    row2_removed = row2_removed.saturating_add(1);
                }
                2 => {
                    let Some(value) = row2_values.get(projected_index) else {
                        return Err(DbError::internal(format!(
                            "{context} projection row is shorter than planned schema",
                        )));
                    };
                    values.push(value.clone());
                }
                _ => {
                    return Err(DbError::internal(format!(
                        "{context} row is shorter than planned schema",
                    )));
                }
            },
        }
    }
    Ok(values)
}

fn table_output_columns(table: &TableSchema, alias: &Option<String>) -> Vec<ColumnBinding> {
    let table_name = alias.clone().unwrap_or_else(|| table.name.clone());
    table
        .columns
        .iter()
        .map(|column| {
            ColumnBinding::visible(Some(table_name.clone()), column.name.clone())
                .with_type(column.column_type)
        })
        .collect()
}

fn flatten_inner_join_chain<'a>(
    item: &'a FromItem,
    tables: &mut Vec<TableBindingRef<'a>>,
    constraints: &mut Vec<&'a Expr>,
) -> bool {
    match item {
        FromItem::Table { name, alias } => {
            tables.push(TableBindingRef { name, alias });
            true
        }
        FromItem::Join {
            left,
            right,
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(on),
        } => {
            flatten_inner_join_chain(left, tables, constraints)
                && flatten_inner_join_chain(right, tables, constraints)
                && {
                    constraints.push(on);
                    true
                }
        }
        _ => false,
    }
}

fn spatial_join_argument_orientation<'a>(
    left_binding: TableBindingRef<'a>,
    right_binding: TableBindingRef<'a>,
    indexed_ref: QualifiedColumnRef<'a>,
    probe_ref: QualifiedColumnRef<'a>,
) -> Option<(TableBindingRef<'a>, TableBindingRef<'a>, bool)> {
    let indexed_on_left = matches_table_binding(left_binding, indexed_ref.table);
    let indexed_on_right = matches_table_binding(right_binding, indexed_ref.table);
    let probe_on_left = matches_table_binding(left_binding, probe_ref.table);
    let probe_on_right = matches_table_binding(right_binding, probe_ref.table);
    match (
        indexed_on_left,
        indexed_on_right,
        probe_on_left,
        probe_on_right,
    ) {
        (true, false, false, true) => Some((left_binding, right_binding, true)),
        (false, true, true, false) => Some((right_binding, left_binding, false)),
        _ => None,
    }
}

pub(crate) fn orient_join_equalities<'a>(
    equalities: &[(QualifiedColumnRef<'a>, QualifiedColumnRef<'a>)],
    filtered_table: TableBindingRef<'a>,
    probe_table: TableBindingRef<'a>,
) -> Option<(Vec<&'a str>, Vec<&'a str>)> {
    let mut filtered_columns = Vec::with_capacity(equalities.len());
    let mut probe_columns = Vec::with_capacity(equalities.len());
    for (left_ref, right_ref) in equalities {
        if matches_table_binding(filtered_table, left_ref.table)
            && matches_table_binding(probe_table, right_ref.table)
        {
            filtered_columns.push(left_ref.column);
            probe_columns.push(right_ref.column);
        } else if matches_table_binding(filtered_table, right_ref.table)
            && matches_table_binding(probe_table, left_ref.table)
        {
            filtered_columns.push(right_ref.column);
            probe_columns.push(left_ref.column);
        } else {
            return None;
        }
    }
    Some((filtered_columns, probe_columns))
}

fn flatten_left_deep_inner_join_tables<'a>(
    item: &'a FromItem,
    tables: &mut Vec<IndexedJoinLimitTablePlan<'a>>,
    constraints: &mut Vec<&'a JoinConstraint>,
) -> bool {
    match item {
        FromItem::Table { name, alias } => {
            if !tables.is_empty() {
                return false;
            }
            tables.push(IndexedJoinLimitTablePlan {
                name: name.as_str(),
                alias,
            });
            true
        }
        FromItem::Join {
            left,
            right,
            kind: JoinKind::Inner,
            constraint,
        } => {
            if !flatten_left_deep_inner_join_tables(left, tables, constraints) {
                return false;
            }
            let FromItem::Table { name, alias } = &**right else {
                return false;
            };
            tables.push(IndexedJoinLimitTablePlan {
                name: name.as_str(),
                alias,
            });
            constraints.push(constraint);
            true
        }
        _ => false,
    }
}

fn indexed_join_group_column_indexes(
    group_by: &[Expr],
    binding: TableBindingRef<'_>,
    schema: &TableSchema,
) -> Option<Vec<usize>> {
    group_by
        .iter()
        .map(|expr| {
            let Expr::Column { table, column } = expr else {
                return None;
            };
            if !matches_table_binding(binding, table.as_deref()) {
                return None;
            }
            schema
                .columns
                .iter()
                .position(|candidate| identifiers_equal(&candidate.name, column))
        })
        .collect()
}

fn indexed_join_grouped_count_is_safe(
    expr: &Expr,
    child_binding: TableBindingRef<'_>,
    child_schema: &TableSchema,
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
    if !name.eq_ignore_ascii_case("count") || *distinct || !order_by.is_empty() || *within_group {
        return false;
    }
    if *star {
        return args.is_empty();
    }
    if args.len() != 1 {
        return false;
    }
    let Expr::Column { table, column } = &args[0] else {
        return false;
    };
    if !matches_table_binding(child_binding, table.as_deref()) {
        return false;
    }
    child_schema
        .columns
        .iter()
        .find(|candidate| identifiers_equal(&candidate.name, column))
        .is_some_and(|column| column.primary_key || !column.nullable)
}

fn indexed_join_limit_projection_column(
    expr: &Expr,
    tables: &[IndexedJoinLimitTablePlan<'_>],
    runtime: &EngineRuntime,
) -> Option<(usize, usize)> {
    let Expr::Column {
        table: qualifier,
        column,
    } = expr
    else {
        return None;
    };
    let mut matched = None;
    let unqualified_unique = qualifier.is_none()
        && tables
            .iter()
            .filter(|candidate| {
                runtime
                    .table_schema(candidate.name)
                    .is_some_and(|schema| schema_column_index(schema, column).is_some())
            })
            .count()
            == 1;
    for (table_index, table_plan) in tables.iter().enumerate() {
        let binding = TableBindingRef {
            name: table_plan.name,
            alias: table_plan.alias,
        };
        let qualifier_matches = qualifier
            .as_deref()
            .is_some_and(|qualifier| matches_table_binding(binding, Some(qualifier)));
        let schema = runtime.table_schema(table_plan.name)?;
        let Some(column_index) = schema_column_index(schema, column) else {
            continue;
        };
        if (qualifier_matches || unqualified_unique)
            && matched.replace((table_index, column_index)).is_some()
        {
            return None;
        }
    }
    matched
}

fn pushed_view_projection_for_outer_projection(
    outer_projection: &[SelectItem],
    view_select: &Select,
    view_name: &str,
    view_binding: &str,
    view_column_names: &[String],
) -> Option<Vec<SelectItem>> {
    let mut pushed = Vec::new();
    for item in outer_projection {
        match item {
            SelectItem::Wildcard => {
                append_all_view_projection_items(&mut pushed, view_select, view_column_names)?;
            }
            SelectItem::QualifiedWildcard(qualifier)
                if identifiers_equal(qualifier, view_binding)
                    || identifiers_equal(qualifier, view_name) =>
            {
                append_all_view_projection_items(&mut pushed, view_select, view_column_names)?;
            }
            SelectItem::QualifiedWildcard(_) => return None,
            SelectItem::Expr { expr, alias } => {
                let Expr::Column { table, column } = expr else {
                    return None;
                };
                if table.as_deref().is_some_and(|qualifier| {
                    !identifiers_equal(qualifier, view_binding)
                        && !identifiers_equal(qualifier, view_name)
                }) {
                    return None;
                }
                let view_expr = view_projection_expr_for_output_column_with_names(
                    &view_select.projection,
                    view_column_names,
                    column,
                )?;
                pushed.push(SelectItem::Expr {
                    expr: view_expr,
                    alias: Some(alias.clone().unwrap_or_else(|| infer_expr_name(expr, 1))),
                });
            }
        }
    }
    Some(pushed)
}

fn append_all_view_projection_items(
    pushed: &mut Vec<SelectItem>,
    view_select: &Select,
    view_column_names: &[String],
) -> Option<()> {
    for (index, item) in view_select.projection.iter().enumerate() {
        let SelectItem::Expr { expr, .. } = item else {
            return None;
        };
        pushed.push(SelectItem::Expr {
            expr: expr.clone(),
            alias: Some(view_output_column_name(
                &view_select.projection,
                view_column_names,
                index,
            )?),
        });
    }
    Some(())
}

fn view_projection_expr_for_output_column_with_names(
    items: &[SelectItem],
    view_column_names: &[String],
    column: &str,
) -> Option<Expr> {
    for (index, item) in items.iter().enumerate() {
        if identifiers_equal(
            &view_output_column_name(items, view_column_names, index)?,
            column,
        ) {
            let SelectItem::Expr { expr, .. } = item else {
                return None;
            };
            return Some(expr.clone());
        }
    }
    None
}

fn view_output_column_name(
    items: &[SelectItem],
    view_column_names: &[String],
    index: usize,
) -> Option<String> {
    if let Some(name) = view_column_names.get(index) {
        return Some(name.clone());
    }
    let SelectItem::Expr { expr, alias } = items.get(index)? else {
        return None;
    };
    Some(
        alias
            .clone()
            .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
    )
}

fn indexed_join_table_eval_columns(
    tables: &[IndexedJoinLimitTablePlan<'_>],
    runtime: &EngineRuntime,
) -> Option<Vec<ColumnBinding>> {
    let mut columns = Vec::new();
    for table in tables {
        let schema = runtime.table_schema(table.name)?;
        let binding_name = table.alias.as_deref().unwrap_or(table.name);
        columns.extend(schema.columns.iter().map(|column| {
            ColumnBinding::visible_source(
                Some(binding_name.to_string()),
                Some(schema.name.clone()),
                column.name.clone(),
            )
            .with_type(column.column_type)
        }));
    }
    Some(columns)
}

fn single_plain_btree_index_matches_column(
    index: &IndexSchema,
    table_name: &str,
    column_name: &str,
) -> bool {
    identifiers_equal(&index.table_name, table_name)
        && index.fresh
        && index.kind == IndexKind::Btree
        && index.columns.len() == 1
        && index.columns[0].expression_sql.is_none()
        && index.columns[0]
            .column_name
            .as_deref()
            .is_some_and(|index_column| identifiers_equal(index_column, column_name))
}

fn filter_contains_partial_index_predicate(
    filter: &Expr,
    predicate: &Expr,
    root_binding: &str,
) -> bool {
    match filter {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            filter_contains_partial_index_predicate(left, predicate, root_binding)
                || filter_contains_partial_index_predicate(right, predicate, root_binding)
        }
        _ => partial_index_predicate_expr_matches(filter, predicate, root_binding),
    }
}

fn partial_index_predicate_expr_matches(left: &Expr, right: &Expr, root_binding: &str) -> bool {
    match (left, right) {
        (
            Expr::Column {
                table: left_table,
                column: left_column,
            },
            Expr::Column {
                table: right_table,
                column: right_column,
            },
        ) => {
            identifiers_equal(left_column, right_column)
                && partial_index_predicate_qualifier_matches(left_table.as_deref(), root_binding)
                && partial_index_predicate_qualifier_matches(right_table.as_deref(), root_binding)
        }
        (Expr::Literal(left), Expr::Literal(right)) => left == right,
        (
            Expr::Binary {
                left: left_left,
                op: left_op,
                right: left_right,
            },
            Expr::Binary {
                left: right_left,
                op: right_op,
                right: right_right,
            },
        ) if left_op == right_op => {
            let direct = partial_index_predicate_expr_matches(left_left, right_left, root_binding)
                && partial_index_predicate_expr_matches(left_right, right_right, root_binding);
            direct
                || binary_op_is_commutative_for_partial_predicate(*left_op)
                    && partial_index_predicate_expr_matches(left_left, right_right, root_binding)
                    && partial_index_predicate_expr_matches(left_right, right_left, root_binding)
        }
        (
            Expr::Unary {
                op: left_op,
                expr: left_expr,
            },
            Expr::Unary {
                op: right_op,
                expr: right_expr,
            },
        ) if left_op == right_op => {
            partial_index_predicate_expr_matches(left_expr, right_expr, root_binding)
        }
        (
            Expr::Cast {
                expr: left_expr,
                target_type: left_type,
            },
            Expr::Cast {
                expr: right_expr,
                target_type: right_type,
            },
        ) if left_type == right_type => {
            partial_index_predicate_expr_matches(left_expr, right_expr, root_binding)
        }
        (
            Expr::IsNull {
                expr: left_expr,
                negated: left_not,
            },
            Expr::IsNull {
                expr: right_expr,
                negated: right_not,
            },
        ) if left_not == right_not => {
            partial_index_predicate_expr_matches(left_expr, right_expr, root_binding)
        }
        _ => left == right,
    }
}

fn partial_index_predicate_qualifier_matches(qualifier: Option<&str>, root_binding: &str) -> bool {
    qualifier.is_none_or(|qualifier| identifiers_equal(qualifier, root_binding))
}

fn binary_op_is_commutative_for_partial_predicate(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::And
            | BinaryOp::Or
            | BinaryOp::Add
            | BinaryOp::Mul
            | BinaryOp::IsDistinctFrom
            | BinaryOp::IsNotDistinctFrom
    )
}

fn visit_runtime_btree_row_ids_in_order<F>(
    keys: &RuntimeBtreeKeys,
    descending: bool,
    mut visitor: F,
) -> Result<bool>
where
    F: FnMut(i64) -> Result<bool>,
{
    match keys {
        RuntimeBtreeKeys::UniqueEncoded(entries, deleted) => {
            if descending {
                for row_id in entries.values().rev() {
                    if !deleted.contains(row_id) && visitor(*row_id)? {
                        return Ok(true);
                    }
                }
            } else {
                for row_id in entries.values() {
                    if !deleted.contains(row_id) && visitor(*row_id)? {
                        return Ok(true);
                    }
                }
            }
        }
        RuntimeBtreeKeys::NonUniqueEncoded(entries, deleted) => {
            if descending {
                for row_ids in entries.values().rev() {
                    for row_id in row_ids {
                        if !deleted.contains(row_id) && visitor(*row_id)? {
                            return Ok(true);
                        }
                    }
                }
            } else {
                for row_ids in entries.values() {
                    for row_id in row_ids {
                        if !deleted.contains(row_id) && visitor(*row_id)? {
                            return Ok(true);
                        }
                    }
                }
            }
        }
        RuntimeBtreeKeys::UniqueUuid(entries, deleted) => {
            if descending {
                for row_id in entries.values().rev() {
                    if !deleted.contains(row_id) && visitor(*row_id)? {
                        return Ok(true);
                    }
                }
            } else {
                for row_id in entries.values() {
                    if !deleted.contains(row_id) && visitor(*row_id)? {
                        return Ok(true);
                    }
                }
            }
        }
        RuntimeBtreeKeys::NonUniqueUuid(entries, deleted) => {
            if descending {
                for row_ids in entries.values().rev() {
                    for row_id in row_ids {
                        if !deleted.contains(row_id) && visitor(*row_id)? {
                            return Ok(true);
                        }
                    }
                }
            } else {
                for row_ids in entries.values() {
                    for row_id in row_ids {
                        if !deleted.contains(row_id) && visitor(*row_id)? {
                            return Ok(true);
                        }
                    }
                }
            }
        }
        RuntimeBtreeKeys::UniqueInt64(entries, deleted) => {
            let mut ordered = entries
                .iter()
                .filter(|(_, row_id)| !deleted.contains(row_id))
                .collect::<Vec<_>>();
            ordered.sort_unstable_by_key(|(key, _)| *key);
            if descending {
                ordered.reverse();
            }
            for (_, row_id) in ordered {
                if visitor(row_id)? {
                    return Ok(true);
                }
            }
        }
        RuntimeBtreeKeys::NonUniqueInt64(entries, deleted) => {
            let mut ordered = entries
                .iter()
                .map(|(key, row_ids)| (key, row_ids.to_vec()))
                .collect::<Vec<_>>();
            ordered.sort_unstable_by_key(|(key, _)| *key);
            if descending {
                ordered.reverse();
            }
            for (_, mut row_ids) in ordered {
                row_ids.sort_unstable();
                for row_id in row_ids {
                    if !deleted.contains(&row_id) && visitor(row_id)? {
                        return Ok(true);
                    }
                }
            }
        }
    }
    Ok(false)
}

fn indexed_join_limit_rows_for_value(
    source: VisibleTableRowSource<'_>,
    keys: Option<&RuntimeBtreeKeys>,
    value: &Value,
) -> Result<Vec<Vec<Value>>> {
    if matches!(value, Value::Null) {
        return Ok(Vec::new());
    }
    let Some(keys) = keys else {
        let Value::Int64(row_id) = value else {
            return Ok(Vec::new());
        };
        return Ok(source
            .row_by_id(*row_id)?
            .map(|row| vec![row.values().to_vec()])
            .unwrap_or_default());
    };
    let row_ids = keys.row_ids_for_value_set(value)?;
    let mut rows = Vec::with_capacity(row_ids.len());
    let mut row_error = None;
    row_ids.for_each(|row_id| {
        if row_error.is_some() {
            return;
        }
        match source.row_by_id(row_id) {
            Ok(Some(row)) => rows.push(row.values().to_vec()),
            Ok(None) => {}
            Err(error) => row_error = Some(error),
        }
    });
    if let Some(error) = row_error {
        return Err(error);
    }
    Ok(rows)
}

fn indexed_join_row_ids_for_value(
    keys: Option<&RuntimeBtreeKeys>,
    value: &Value,
) -> Result<Vec<i64>> {
    if matches!(value, Value::Null) {
        return Ok(Vec::new());
    }
    let Some(keys) = keys else {
        return Ok(match value {
            Value::Int64(row_id) => vec![*row_id],
            _ => Vec::new(),
        });
    };
    let row_ids = keys.row_ids_for_value_set(value)?;
    let mut values = Vec::with_capacity(row_ids.len());
    row_ids.for_each(|row_id| values.push(row_id));
    Ok(values)
}

fn sort_join_row_ids_by_column(
    source: VisibleTableRowSource<'_>,
    row_ids: Vec<i64>,
    column_index: usize,
) -> Result<Vec<i64>> {
    let mut keyed = Vec::with_capacity(row_ids.len());
    for row_id in row_ids {
        let Some(row) = source.row_by_id(row_id)? else {
            continue;
        };
        let Some(value) = row.values().get(column_index) else {
            return Err(DbError::internal(
                "indexed join order row is shorter than schema",
            ));
        };
        keyed.push((row_id, value.clone()));
    }
    let mut sort_error = None;
    keyed.sort_by(|(_, left), (_, right)| match compare_values(left, right) {
        Ok(ordering) => ordering,
        Err(error) => {
            if sort_error.is_none() {
                sort_error = Some(error);
            }
            std::cmp::Ordering::Equal
        }
    });
    if let Some(error) = sort_error {
        return Err(error);
    }
    Ok(keyed.into_iter().map(|(row_id, _)| row_id).collect())
}

fn project_indexed_join_row(
    current_rows: &[&[Value]],
    projections: &[IndexedJoinLimitProjection],
) -> Result<QueryRow> {
    let mut output = Vec::with_capacity(projections.len());
    for projection in projections {
        let Some(row) = current_rows.get(projection.table_index) else {
            return Err(DbError::internal(
                "indexed join projection table index is out of range",
            ));
        };
        let Some(value) = row.get(projection.column_index) else {
            return Err(DbError::internal(
                "indexed join projection row is shorter than schema",
            ));
        };
        output.push(value.clone());
    }
    Ok(QueryRow::new(output))
}

fn push_indexed_join_limit_projection(
    current_rows: &[&[Value]],
    projections: &[IndexedJoinLimitProjection],
    offset_remaining: &mut usize,
    limit_remaining: &mut usize,
    rows: &mut Vec<QueryRow>,
) -> bool {
    if *offset_remaining > 0 {
        *offset_remaining -= 1;
        return false;
    }
    if *limit_remaining == 0 {
        return true;
    }
    let mut output = Vec::with_capacity(projections.len());
    for projection in projections {
        let Some(row) = current_rows.get(projection.table_index) else {
            return true;
        };
        let Some(value) = row.get(projection.column_index) else {
            return true;
        };
        output.push(value.clone());
    }
    rows.push(QueryRow::new(output));
    *limit_remaining = (*limit_remaining).saturating_sub(1);
    *limit_remaining == 0
}

fn indexed_join_limit_result(plan: &IndexedJoinLimitPlan<'_>, rows: Vec<QueryRow>) -> QueryResult {
    QueryResult::with_rows(
        plan.projections
            .iter()
            .map(|projection| projection.column_name.clone())
            .collect(),
        rows,
    )
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

/// Returns the schema column index of the table's row-id alias column, when the
/// table has a single-column auto-increment `INTEGER PRIMARY KEY`. The stored
/// `row_id` of every row equals the value of this column.
fn rowid_alias_column_index(table: &TableSchema) -> Option<usize> {
    let alias = row_id_alias_column_name(table)?;
    table
        .columns
        .iter()
        .position(|column| identifiers_equal(&column.name, alias))
}

fn push_projection_index(indexes: &mut Vec<usize>, index: usize) -> usize {
    if let Some(position) = indexes.iter().position(|candidate| *candidate == index) {
        position
    } else {
        indexes.push(index);
        indexes.len() - 1
    }
}

fn project_resolved_simple_join_row(
    projections: &[ResolvedSimpleJoinProjection],
    left_values: &[Value],
    right_values: &[Value],
) -> Result<QueryRow> {
    let mut projected = Vec::with_capacity(projections.len());
    for projection in projections {
        let values = match projection.side {
            SimpleJoinProjectionSide::Left => left_values,
            SimpleJoinProjectionSide::Right => right_values,
        };
        let value = values
            .get(projection.index)
            .ok_or_else(|| DbError::internal("prepared join projection index out of bounds"))?;
        projected.push(value.clone());
    }
    Ok(QueryRow::new(projected))
}

fn project_resolved_simple_join_row_from_full_values(
    projections: &[ResolvedSimpleJoinProjection],
    left_projection_indexes: &[usize],
    right_projection_indexes: &[usize],
    left_values: &[Value],
    right_values: &[Value],
) -> Result<QueryRow> {
    let mut projected = Vec::with_capacity(projections.len());
    for projection in projections {
        let (projection_indexes, values) = match projection.side {
            SimpleJoinProjectionSide::Left => (left_projection_indexes, left_values),
            SimpleJoinProjectionSide::Right => (right_projection_indexes, right_values),
        };
        let original_index = projection_indexes
            .get(projection.index)
            .ok_or_else(|| DbError::internal("prepared join projection index out of bounds"))?;
        let value = values
            .get(*original_index)
            .ok_or_else(|| DbError::internal("prepared join projection index out of bounds"))?;
        projected.push(value.clone());
    }
    Ok(QueryRow::new(projected))
}

fn covering_projection_offsets(
    covering: &RuntimeCoveringPayloads,
    table_schema: &TableSchema,
    projection_indexes: &[usize],
) -> Option<Vec<usize>> {
    projection_indexes
        .iter()
        .map(|projection_index| {
            table_schema
                .columns
                .get(*projection_index)
                .and_then(|column| covering.column_position(&column.name))
        })
        .collect()
}

fn apply_simple_projection_postprocessing_with_order(
    runtime: Option<&EngineRuntime>,
    mut rows: Vec<QueryRow>,
    column_names: Vec<String>,
    order_by: Option<&[SimpleOrderByPlan]>,
    limit: Option<usize>,
    offset: usize,
) -> Result<QueryResult> {
    if let Some(order_by) = order_by {
        if let Some(limit) = limit {
            let bounded_row_count = offset.saturating_add(limit);
            if bounded_row_count == 0 {
                return Ok(QueryResult::with_rows(column_names, Vec::new()));
            }
            let mut bounded_rows = Vec::with_capacity(rows.len().min(bounded_row_count));
            for row in rows {
                push_bounded_projection_ordered_query_row(
                    runtime,
                    &mut bounded_rows,
                    row,
                    order_by,
                    bounded_row_count,
                )?;
            }
            sort_query_rows_by_projection_order(runtime, &mut bounded_rows, order_by)?;
            let rows = bounded_rows.into_iter().skip(offset).take(limit).collect();
            return Ok(QueryResult::with_rows(column_names, rows));
        }
        sort_query_rows_by_projection_order(runtime, &mut rows, order_by)?;
    }
    let rows = rows
        .into_iter()
        .skip(offset)
        .take(limit.unwrap_or(usize::MAX))
        .collect();
    Ok(QueryResult::with_rows(column_names, rows))
}

fn dedup_query_rows(rows: Vec<QueryRow>) -> Result<Vec<QueryRow>> {
    let mut seen = BTreeSet::new();
    let mut distinct_rows = Vec::with_capacity(rows.len());
    for row in rows {
        if seen.insert(row_identity(row.values())?) {
            distinct_rows.push(row);
        }
    }
    Ok(distinct_rows)
}

fn compare_query_row_order_values(
    runtime: Option<&EngineRuntime>,
    left_order: &[Value],
    right_order: &[Value],
    order_by: &[crate::sql::ast::OrderBy],
) -> Result<std::cmp::Ordering> {
    for (index, order) in order_by.iter().enumerate() {
        let ordering = compare_values_with_runtime_collation(
            runtime,
            &left_order[index],
            &right_order[index],
            order.collation.clone(),
        )?;
        if ordering == std::cmp::Ordering::Equal {
            continue;
        }
        return Ok(if order.descending {
            ordering.reverse()
        } else {
            ordering
        });
    }
    Ok(std::cmp::Ordering::Equal)
}

fn sort_query_rows_by_order_values(
    runtime: Option<&EngineRuntime>,
    rows: &mut [(QueryRow, Vec<Value>)],
    order_by: &[crate::sql::ast::OrderBy],
) -> Result<()> {
    let mut sort_error = None;
    rows.sort_by(|(_, left_order), (_, right_order)| {
        match compare_query_row_order_values(runtime, left_order, right_order, order_by) {
            Ok(ordering) => ordering,
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
    Ok(())
}

fn push_bounded_ordered_query_row(
    runtime: Option<&EngineRuntime>,
    rows: &mut Vec<(QueryRow, Vec<Value>)>,
    row: (QueryRow, Vec<Value>),
    order_by: &[crate::sql::ast::OrderBy],
    bounded_row_count: usize,
) -> Result<()> {
    if bounded_row_count == 0 {
        return Ok(());
    }
    if rows.len() < bounded_row_count {
        rows.push(row);
        return Ok(());
    }
    let mut worst_index = 0;
    for index in 1..rows.len() {
        if compare_query_row_order_values(runtime, &rows[index].1, &rows[worst_index].1, order_by)?
            == std::cmp::Ordering::Greater
        {
            worst_index = index;
        }
    }
    if compare_query_row_order_values(runtime, &row.1, &rows[worst_index].1, order_by)?
        == std::cmp::Ordering::Less
    {
        rows[worst_index] = row;
    }
    Ok(())
}

pub(crate) fn sort_query_rows_by_projection_order(
    runtime: Option<&EngineRuntime>,
    rows: &mut [QueryRow],
    order_by: &[SimpleOrderByPlan],
) -> Result<()> {
    let mut sort_error = None;
    rows.sort_by(|left, right| {
        for order in order_by {
            let ordering = compare_values_with_runtime_collation(
                runtime,
                &left.values()[order.projection_index],
                &right.values()[order.projection_index],
                order.collation.clone(),
            );
            match ordering {
                Ok(std::cmp::Ordering::Equal) => continue,
                Ok(ordering) => {
                    return if order.descending {
                        ordering.reverse()
                    } else {
                        ordering
                    };
                }
                Err(error) => {
                    if sort_error.is_none() {
                        sort_error = Some(error);
                    }
                    return std::cmp::Ordering::Equal;
                }
            }
        }
        std::cmp::Ordering::Equal
    });
    if let Some(error) = sort_error {
        return Err(error);
    }
    Ok(())
}

fn compare_query_rows_by_projection_order(
    runtime: Option<&EngineRuntime>,
    left: &QueryRow,
    right: &QueryRow,
    order_by: &[SimpleOrderByPlan],
) -> Result<std::cmp::Ordering> {
    for order in order_by {
        let ordering = compare_values_with_runtime_collation(
            runtime,
            &left.values()[order.projection_index],
            &right.values()[order.projection_index],
            order.collation.clone(),
        )?;
        if ordering == std::cmp::Ordering::Equal {
            continue;
        }
        return Ok(if order.descending {
            ordering.reverse()
        } else {
            ordering
        });
    }
    Ok(std::cmp::Ordering::Equal)
}

fn push_bounded_projection_ordered_query_row(
    runtime: Option<&EngineRuntime>,
    rows: &mut Vec<QueryRow>,
    row: QueryRow,
    order_by: &[SimpleOrderByPlan],
    bounded_row_count: usize,
) -> Result<()> {
    if bounded_row_count == 0 {
        return Ok(());
    }
    if rows.len() < bounded_row_count {
        rows.push(row);
        return Ok(());
    }
    let mut worst_index = 0;
    for index in 1..rows.len() {
        if compare_query_rows_by_projection_order(
            runtime,
            &rows[index],
            &rows[worst_index],
            order_by,
        )? == std::cmp::Ordering::Greater
        {
            worst_index = index;
        }
    }
    if compare_query_rows_by_projection_order(runtime, &row, &rows[worst_index], order_by)?
        == std::cmp::Ordering::Less
    {
        rows[worst_index] = row;
    }
    Ok(())
}

fn unique_grouped_having_column_index(column_names: &[String], column: &str) -> Option<usize> {
    let mut matched = None;
    for (index, candidate) in column_names.iter().enumerate() {
        if !candidate.eq_ignore_ascii_case(column) {
            continue;
        }
        if matched.replace(index).is_some() {
            return None;
        }
    }
    matched
}

fn unique_grouped_having_group_index(select: &Select, column: &str) -> Option<usize> {
    let mut matched = None;
    for (index, expr) in select.group_by.iter().enumerate() {
        let Expr::Column {
            column: group_column,
            ..
        } = expr
        else {
            continue;
        };
        if !identifiers_equal(group_column, column) {
            continue;
        }
        if matched.replace(index).is_some() {
            return None;
        }
    }
    matched
}

fn first_persistent_pk_row_id<S: PageStore>(
    store: &S,
    table_schema: &TableSchema,
) -> Result<Option<i64>> {
    let Some(pk_index_root) = table_schema.pk_index_root else {
        return Ok(None);
    };
    let Some(position) = btree_first_position(store, Some(pk_index_root))? else {
        return Ok(None);
    };
    let (key, _) = btree_materialize_current(store, &position)?;
    Ok(Some(decode_row_id_locator_key(key)))
}

#[allow(clippy::too_many_arguments)]
fn try_persistent_pk_ordered_projection_result<S: PageStore>(
    store: &S,
    state: PersistedTableState,
    table_schema: &TableSchema,
    projection_indexes: &[usize],
    column_names: Vec<String>,
    limit: Option<usize>,
    offset: usize,
    descending: bool,
) -> Result<Option<QueryResult>> {
    let Some(pk_index_root) = table_schema.pk_index_root else {
        return Ok(None);
    };
    let take = limit.unwrap_or(usize::MAX);
    if take == 0 {
        return Ok(Some(QueryResult::with_rows(column_names, Vec::new())));
    }

    let mut cursor = if descending {
        BtreeCursor::from_end(store, Some(pk_index_root))?
    } else {
        BtreeCursor::from_start(store, Some(pk_index_root))?
    };
    let mut skipped = 0usize;
    let mut rows = Vec::with_capacity(take.min(64));
    while let Some((_, payload)) = if descending {
        cursor.prev()?
    } else {
        cursor.next()?
    } {
        if skipped < offset {
            skipped += 1;
            continue;
        }
        let locator = decode_row_locator(&payload)?;
        if let Some(values) =
            read_deferred_projected_values_by_locator(store, state, locator, projection_indexes)?
        {
            rows.push(QueryRow::new(values));
            if rows.len() == take {
                break;
            }
        }
    }

    Ok(Some(QueryResult::with_rows(column_names, rows)))
}

fn append_paged_row_locator_entries(
    entries: &mut BTreeMap<u64, Vec<u8>>,
    payload: &[u8],
    chunk_index: u32,
    is_overlay: bool,
    skip: &BTreeSet<i64>,
) -> Result<()> {
    if payload.is_empty() {
        return Ok(());
    }
    if !payload.starts_with(TABLE_PAYLOAD_MAGIC) {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    let mut cursor = Cursor::new(payload);
    let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
    if magic != TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    let row_count = cursor.read_u32()? as usize;
    for _ in 0..row_count {
        let row_id = cursor.read_i64()?;
        let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
        let row_bytes_offset = cursor.offset;
        cursor.read_slice(row_bytes_len)?;
        if is_tombstone || skip.contains(&row_id) {
            continue;
        }
        let locator = RowLocatorV2 {
            chunk_index,
            byte_offset: u32::try_from(row_bytes_offset)
                .map_err(|_| DbError::constraint("row locator offset exceeds u32"))?,
            byte_len: u32::try_from(row_bytes_len)
                .map_err(|_| DbError::constraint("row locator length exceeds u32"))?,
            is_overlay,
        };
        entries.insert(
            encode_row_id_locator_key(row_id),
            encode_paged_row_locator(locator),
        );
    }
    Ok(())
}

fn append_cached_paged_row_locators(
    locators: &mut Int64Map<CachedPagedRowLocator>,
    payload: &[u8],
    pointer: OverflowPointer,
    checksum: u32,
    skip: &BTreeSet<i64>,
) -> Result<()> {
    if payload.is_empty() {
        return Ok(());
    }
    let mut cursor = Cursor::new(payload);
    let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
    if magic != TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    let row_count = cursor.read_u32()? as usize;
    for _ in 0..row_count {
        let row_id = cursor.read_i64()?;
        let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
        let row_bytes_offset = cursor.offset;
        cursor.read_slice(row_bytes_len)?;
        if is_tombstone || skip.contains(&row_id) {
            continue;
        }
        locators.insert(
            row_id,
            CachedPagedRowLocator {
                pointer,
                checksum,
                locator: RowLocatorV1 {
                    byte_offset: u32::try_from(row_bytes_offset)
                        .map_err(|_| DbError::constraint("row locator offset exceeds u32"))?,
                    byte_len: u32::try_from(row_bytes_len)
                        .map_err(|_| DbError::constraint("row locator length exceeds u32"))?,
                },
            },
        );
    }
    Ok(())
}

fn maybe_cache_verified_paged_chunk_payload(
    payloads: &mut HashMap<CachedPagedChunkPayloadKey, Arc<Vec<u8>>>,
    cached_payload_bytes: &mut usize,
    pointer: OverflowPointer,
    checksum: u32,
    payload: &Arc<Vec<u8>>,
) {
    let payload_len = payload.len();
    if payload_len == 0 || payload_len > DEFERRED_PAGED_ROW_PAYLOAD_CACHE_LIMIT_BYTES {
        return;
    }
    if cached_payload_bytes.saturating_add(payload_len)
        > DEFERRED_PAGED_ROW_PAYLOAD_CACHE_LIMIT_BYTES
    {
        return;
    }
    let key = CachedPagedChunkPayloadKey::new(pointer, checksum);
    if payloads.contains_key(&key) {
        return;
    }
    *cached_payload_bytes = cached_payload_bytes.saturating_add(payload_len);
    payloads.insert(key, Arc::clone(payload));
}

fn build_deferred_paged_row_locator_cache(
    state: PersistedTableState,
    chunks: &[TablePageManifestChunk],
) -> Result<DeferredPagedRowLocatorCache> {
    let mut locators = if let Some(directory) = try_build_dense_paged_row_directory(chunks)? {
        let mut sources = Vec::new();
        try_reserve_paged_directory(&mut sources, chunks.len(), "deferred paged chunk sources")?;
        sources.extend(chunks.iter().map(|chunk| CachedPagedChunkSource {
            pointer: chunk.pointer,
            checksum: chunk.checksum,
        }));
        DeferredPagedRowLocators::Dense {
            directory,
            chunks: sources,
        }
    } else {
        let expected_locators = chunks.iter().try_fold(0usize, |total, chunk| {
            total
                .checked_add(chunk.row_count)
                .ok_or_else(|| DbError::constraint("paged table row count overflow"))
        })?;
        let mut sparse = Int64Map::with_hasher(Int64HashBuilder::default());
        sparse.try_reserve(expected_locators).map_err(|error| {
            DbError::internal(format!(
                "failed to reserve {expected_locators} deferred paged row locators: {error}"
            ))
        })?;
        DeferredPagedRowLocators::Sparse(sparse)
    };
    let mut verified_payloads = HashMap::new();
    let mut cached_payload_bytes = 0usize;
    for chunk in chunks {
        maybe_cache_verified_paged_chunk_payload(
            &mut verified_payloads,
            &mut cached_payload_bytes,
            chunk.pointer,
            chunk.checksum,
            &chunk.payload,
        );
        if let DeferredPagedRowLocators::Sparse(sparse) = &mut locators {
            append_cached_paged_row_locators(
                sparse,
                chunk.payload.as_slice(),
                chunk.pointer,
                chunk.checksum,
                &chunk.tombstoned_row_ids,
            )?;
        }
        if let (Some(overlay_pointer), Some(overlay_checksum), Some(overlay_payload)) = (
            chunk.overlay_pointer,
            chunk.overlay_checksum,
            chunk.overlay_payload.as_ref(),
        ) {
            maybe_cache_verified_paged_chunk_payload(
                &mut verified_payloads,
                &mut cached_payload_bytes,
                overlay_pointer,
                overlay_checksum,
                overlay_payload,
            );
            if let DeferredPagedRowLocators::Sparse(sparse) = &mut locators {
                append_cached_paged_row_locators(
                    sparse,
                    overlay_payload.as_slice(),
                    overlay_pointer,
                    overlay_checksum,
                    &BTreeSet::new(),
                )?;
            }
        }
    }
    Ok(DeferredPagedRowLocatorCache {
        manifest_pointer: state.pointer,
        manifest_checksum: state.checksum,
        locators,
        verified_payloads,
    })
}

fn build_row_locator_entries(payload: &[u8]) -> Result<BTreeMap<u64, Vec<u8>>> {
    if payload.is_empty() {
        return Ok(BTreeMap::new());
    }
    if !payload.starts_with(TABLE_PAYLOAD_MAGIC) {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    let mut cursor = Cursor::new(payload);
    let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
    if magic != TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    let row_count = cursor.read_u32()? as usize;
    let mut entries = BTreeMap::new();
    for _ in 0..row_count {
        let row_id = cursor.read_i64()?;
        let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
        let row_bytes_offset = cursor.offset;
        cursor.read_slice(row_bytes_len)?;
        if is_tombstone {
            continue;
        }
        let locator = RowLocatorV1 {
            byte_offset: u32::try_from(row_bytes_offset)
                .map_err(|_| DbError::constraint("row locator offset exceeds u32"))?,
            byte_len: u32::try_from(row_bytes_len)
                .map_err(|_| DbError::constraint("row locator length exceeds u32"))?,
        };
        entries.insert(
            encode_row_id_locator_key(row_id),
            encode_row_locator(locator),
        );
    }
    Ok(entries)
}

fn build_paged_row_locator_entries_from_chunk_payloads(
    chunk_payloads: &[TablePageManifestChunk],
) -> Result<BTreeMap<u64, Vec<u8>>> {
    let mut entries = BTreeMap::new();
    for (chunk_index, chunk) in chunk_payloads.iter().enumerate() {
        let skip = chunk.tombstoned_row_ids.iter().copied().collect();
        append_paged_row_locator_entries(
            &mut entries,
            chunk.payload.as_slice(),
            u32::try_from(chunk_index)
                .map_err(|_| DbError::constraint("paged table chunk index exceeds u32"))?,
            false,
            &skip,
        )?;
        if let Some(overlay) = &chunk.overlay_payload {
            append_paged_row_locator_entries(
                &mut entries,
                overlay.as_slice(),
                u32::try_from(chunk_index)
                    .map_err(|_| DbError::constraint("paged table chunk index exceeds u32"))?,
                true,
                &BTreeSet::new(),
            )?;
        }
    }
    Ok(entries)
}

fn deferred_compressed_lookup_cache() -> &'static Mutex<DeferredCompressedLookupCache> {
    DEFERRED_COMPRESSED_LOOKUP_CACHE
        .get_or_init(|| Mutex::new(DeferredCompressedLookupCache::default()))
}

fn deferred_runtime_btree_index_cache() -> &'static Mutex<DeferredRuntimeBtreeIndexCache> {
    DEFERRED_RUNTIME_BTREE_INDEX_CACHE
        .get_or_init(|| Mutex::new(DeferredRuntimeBtreeIndexCache::default()))
}

fn cached_deferred_runtime_btree_index(
    key: &DeferredRuntimeBtreeIndexCacheKey,
) -> Result<Option<Arc<DeferredRuntimeBtreeIndexCacheEntry>>> {
    let cache = deferred_runtime_btree_index_cache()
        .lock()
        .map_err(|_| DbError::internal("deferred runtime index cache lock poisoned"))?;
    Ok(cache.entries.get(key).cloned())
}

fn cache_deferred_runtime_btree_index(
    key: DeferredRuntimeBtreeIndexCacheKey,
    entry: DeferredRuntimeBtreeIndexCacheEntry,
) -> Result<()> {
    let mut cache = deferred_runtime_btree_index_cache()
        .lock()
        .map_err(|_| DbError::internal("deferred runtime index cache lock poisoned"))?;
    if !cache.entries.contains_key(&key) {
        if cache.entries.len() >= DEFERRED_RUNTIME_BTREE_INDEX_CACHE_LIMIT {
            if let Some(evicted) = cache.insertion_order.pop_front() {
                cache.entries.remove(&evicted);
            }
        }
        cache.insertion_order.push_back(key.clone());
    }
    cache.entries.insert(key, Arc::new(entry));
    Ok(())
}

fn deferred_compressed_lookup_cache_key(
    state: PersistedTableState,
) -> DeferredCompressedLookupCacheKey {
    DeferredCompressedLookupCacheKey {
        head_page_id: state.pointer.head_page_id,
        logical_len: state.pointer.logical_len,
        flags: state.pointer.flags,
        checksum: state.checksum,
    }
}

fn read_deferred_compressed_table_lookup_entry<S: PageStore>(
    store: &S,
    state: PersistedTableState,
) -> Result<Arc<DeferredCompressedLookupCacheEntry>> {
    let cache_key = deferred_compressed_lookup_cache_key(state);
    {
        let cache = deferred_compressed_lookup_cache()
            .lock()
            .map_err(|_| DbError::internal("deferred compressed lookup cache lock poisoned"))?;
        if let Some(entry) = cache.entries.get(&cache_key) {
            return Ok(entry.clone());
        }
    }

    let payload = Arc::new(read_overflow(store, state.pointer)?);
    if crc32c_parts(&[payload.as_slice()]) != state.checksum {
        return Err(DbError::corruption(
            "deferred table payload checksum mismatch",
        ));
    }
    let entry = Arc::new(decode_compressed_table_payload_lookup_entry(payload)?);

    let mut cache = deferred_compressed_lookup_cache()
        .lock()
        .map_err(|_| DbError::internal("deferred compressed lookup cache lock poisoned"))?;
    if let Some(existing) = cache.entries.get(&cache_key) {
        return Ok(existing.clone());
    }
    if cache.entries.len() >= DEFERRED_COMPRESSED_LOOKUP_CACHE_LIMIT {
        if let Some(evicted) = cache.insertion_order.pop_front() {
            cache.entries.remove(&evicted);
        }
    }
    cache.insertion_order.push_back(cache_key);
    cache.entries.insert(cache_key, entry.clone());
    Ok(entry)
}

fn read_deferred_row_by_locator_from_table_payload<S: PageStore>(
    store: &S,
    state: PersistedTableState,
    row_id: i64,
    locator: RowLocatorV1,
) -> Result<Option<StoredRow>> {
    let pointer = state.pointer;
    if pointer.head_page_id == 0 {
        return Ok(None);
    }
    if pointer.is_compressed() {
        let entry = read_deferred_compressed_table_lookup_entry(store, state)?;
        return decode_row_by_locator_from_payload(entry.payload.as_slice(), row_id, locator)
            .map(Some);
    }
    let mut cursor = OverflowPayloadCursor::new(store, pointer);
    cursor.skip(locator.byte_offset as usize)?;
    let row_bytes = cursor.read_vec(locator.byte_len as usize)?;
    let row = Row::decode(&row_bytes)?;
    Ok(Some(StoredRow {
        row_id,
        values: row.into_values(),
    }))
}

fn read_deferred_row_by_locator_from_paged_table_payload<S: PageStore>(
    store: &S,
    state: PersistedTableState,
    row_id: i64,
    locator: RowLocatorV2,
) -> Result<Option<StoredRow>> {
    let manifest_payload = read_overflow(store, state.pointer)?;
    if crc32c_parts(&[manifest_payload.as_slice()]) != state.checksum {
        return Err(DbError::corruption(
            "paged table manifest checksum mismatch",
        ));
    }
    let manifest = decode_paged_table_manifest_payload(&manifest_payload)?;
    let chunk = manifest
        .chunks
        .get(locator.chunk_index as usize)
        .ok_or_else(|| DbError::corruption("paged table locator chunk index is invalid"))?;
    let pointer = if locator.is_overlay {
        chunk.overlay_pointer.ok_or_else(|| {
            DbError::corruption("paged table overlay pointer missing for overlay locator")
        })?
    } else {
        chunk.pointer
    };
    let payload = read_overflow(store, pointer)?;
    let start = locator.byte_offset as usize;
    let end = start.saturating_add(locator.byte_len as usize);
    let row_bytes = payload
        .get(start..end)
        .ok_or_else(|| DbError::corruption("paged row locator exceeded payload length"))?;
    let row = Row::decode(row_bytes)?;
    Ok(Some(StoredRow {
        row_id,
        values: row.into_values(),
    }))
}

fn read_deferred_row_by_cached_paged_locator<S: PageStore>(
    store: &S,
    row_id: i64,
    cached: CachedPagedRowLocator,
    verified_payload: Option<&[u8]>,
) -> Result<Option<StoredRow>> {
    let owned_payload;
    let payload = if let Some(payload) = verified_payload {
        payload
    } else {
        owned_payload = read_overflow(store, cached.pointer)?;
        if crc32c_parts(&[owned_payload.as_slice()]) != cached.checksum {
            return Err(DbError::corruption("paged table chunk checksum mismatch"));
        }
        owned_payload.as_slice()
    };
    decode_row_by_locator_from_payload(payload, row_id, cached.locator).map(Some)
}

fn read_deferred_projected_values_by_cached_paged_locator<S: PageStore>(
    store: &S,
    cached: CachedPagedRowLocator,
    verified_payload: Option<&[u8]>,
    projection_indexes: &[usize],
) -> Result<Vec<Value>> {
    let owned_payload;
    let payload = if let Some(payload) = verified_payload {
        payload
    } else {
        owned_payload = read_overflow(store, cached.pointer)?;
        if crc32c_parts(&[owned_payload.as_slice()]) != cached.checksum {
            return Err(DbError::corruption("paged table chunk checksum mismatch"));
        }
        owned_payload.as_slice()
    };
    decode_projected_values_by_locator_from_payload(
        Some(store),
        payload,
        cached.locator,
        projection_indexes,
    )
}

fn read_deferred_projected_values_by_cached_paged_locator_with_query_cache<S: PageStore>(
    store: &S,
    cached: CachedPagedRowLocator,
    verified_payload: Option<&Arc<Vec<u8>>>,
    chunk_payload_cache: &mut HashMap<CachedPagedChunkPayloadKey, Arc<Vec<u8>>>,
    projection_indexes: &[usize],
) -> Result<Vec<Value>> {
    if let Some(payload) = verified_payload {
        return decode_projected_values_by_locator_from_payload(
            Some(store),
            payload.as_slice(),
            cached.locator,
            projection_indexes,
        );
    }
    let key = CachedPagedChunkPayloadKey::new(cached.pointer, cached.checksum);
    if let Some(payload) = chunk_payload_cache.get(&key) {
        return decode_projected_values_by_locator_from_payload(
            Some(store),
            payload.as_slice(),
            cached.locator,
            projection_indexes,
        );
    }

    let payload = Arc::new(read_overflow(store, cached.pointer)?);
    if crc32c_parts(&[payload.as_slice()]) != cached.checksum {
        return Err(DbError::corruption("paged table chunk checksum mismatch"));
    }
    let values = decode_projected_values_by_locator_from_payload(
        Some(store),
        payload.as_slice(),
        cached.locator,
        projection_indexes,
    )?;
    chunk_payload_cache.insert(key, payload);
    Ok(values)
}

fn read_deferred_projected_values_by_locator_from_table_payload<S: PageStore>(
    store: &S,
    state: PersistedTableState,
    locator: RowLocatorV1,
    projection_indexes: &[usize],
) -> Result<Option<Vec<Value>>> {
    let pointer = state.pointer;
    if pointer.head_page_id == 0 {
        return Ok(None);
    }
    if pointer.is_compressed() {
        let entry = read_deferred_compressed_table_lookup_entry(store, state)?;
        return decode_projected_values_by_locator_from_payload(
            Some(store),
            entry.payload.as_slice(),
            locator,
            projection_indexes,
        )
        .map(Some);
    }

    let mut cursor = OverflowPayloadCursor::new(store, pointer);
    cursor.skip(locator.byte_offset as usize)?;
    let row_bytes = cursor.read_vec(locator.byte_len as usize)?;
    Row::decode_projection_sorted_unique_with_overflow(
        row_bytes.as_slice(),
        Some(store),
        projection_indexes,
    )
    .map(Some)
}

fn read_deferred_projected_values_by_locator_from_paged_table_payload<S: PageStore>(
    store: &S,
    state: PersistedTableState,
    locator: RowLocatorV2,
    projection_indexes: &[usize],
) -> Result<Option<Vec<Value>>> {
    let manifest_payload = read_overflow(store, state.pointer)?;
    if crc32c_parts(&[manifest_payload.as_slice()]) != state.checksum {
        return Err(DbError::corruption(
            "paged table manifest checksum mismatch",
        ));
    }
    let manifest = decode_paged_table_manifest_payload(&manifest_payload)?;
    let chunk = manifest
        .chunks
        .get(locator.chunk_index as usize)
        .ok_or_else(|| DbError::corruption("paged table locator chunk index is invalid"))?;
    let pointer = if locator.is_overlay {
        chunk.overlay_pointer.ok_or_else(|| {
            DbError::corruption("paged table overlay pointer missing for overlay locator")
        })?
    } else {
        chunk.pointer
    };
    let payload = read_overflow(store, pointer)?;
    decode_projected_values_by_locator_from_payload(
        Some(store),
        payload.as_slice(),
        RowLocatorV1 {
            byte_offset: locator.byte_offset,
            byte_len: locator.byte_len,
        },
        projection_indexes,
    )
    .map(Some)
}

fn read_deferred_projected_values_by_locator<S: PageStore>(
    store: &S,
    state: PersistedTableState,
    locator: DecodedRowLocator,
    projection_indexes: &[usize],
) -> Result<Option<Vec<Value>>> {
    if state.pointer.is_table_paged_manifest() {
        return match locator {
            DecodedRowLocator::V2(locator) => {
                read_deferred_projected_values_by_locator_from_paged_table_payload(
                    store,
                    state,
                    locator,
                    projection_indexes,
                )
            }
            DecodedRowLocator::V1(_) => Err(DbError::corruption(
                "paged table persistent pk locator payload is invalid",
            )),
        };
    }

    match locator {
        DecodedRowLocator::V1(locator) => {
            read_deferred_projected_values_by_locator_from_table_payload(
                store,
                state,
                locator,
                projection_indexes,
            )
        }
        DecodedRowLocator::V2(locator) => {
            read_deferred_projected_values_by_locator_from_table_payload(
                store,
                state,
                RowLocatorV1 {
                    byte_offset: locator.byte_offset,
                    byte_len: locator.byte_len,
                },
                projection_indexes,
            )
        }
    }
}

fn read_deferred_row_by_id_from_paged_chunk<S: PageStore>(
    store: &S,
    chunk: &PersistedTableChunkState,
    row_id: i64,
) -> Result<Option<StoredRow>> {
    if let Some(overlay_pointer) = chunk.overlay_pointer {
        let overlay_payload = read_overflow(store, overlay_pointer)?;
        if Some(crc32c_parts(&[overlay_payload.as_slice()])) != chunk.overlay_checksum {
            return Err(DbError::corruption(
                "paged table overlay chunk checksum mismatch",
            ));
        }
        if let Some(row) = read_row_from_table_payload_by_id(overlay_payload.as_slice(), row_id)? {
            return Ok(Some(row));
        }
    }

    if !chunk.tombstoned_row_ids.is_empty() && chunk.tombstoned_row_ids.contains(&row_id) {
        return Ok(None);
    }

    let payload = read_overflow(store, chunk.pointer)?;
    if crc32c_parts(&[payload.as_slice()]) != chunk.checksum {
        return Err(DbError::corruption("paged table chunk checksum mismatch"));
    }
    read_row_from_table_payload_by_id(payload.as_slice(), row_id)
}

fn read_row_from_table_payload_by_id(payload: &[u8], row_id: i64) -> Result<Option<StoredRow>> {
    if payload.is_empty() {
        return Ok(None);
    }
    let mut cursor = Cursor::new(payload);
    let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
    if magic != TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    let row_count = cursor.read_u32()? as usize;
    for _ in 0..row_count {
        let candidate_row_id = cursor.read_i64()?;
        let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
        let row_bytes = cursor.read_slice(row_bytes_len)?;
        if is_tombstone {
            continue;
        }
        if candidate_row_id == row_id {
            let row = Row::decode(row_bytes)?;
            return Ok(Some(StoredRow {
                row_id: candidate_row_id,
                values: row.into_values(),
            }));
        }
    }
    Ok(None)
}

fn read_deferred_row_by_id_from_paged_table_manifest<S: PageStore>(
    store: &S,
    state: PersistedTableState,
    row_id: i64,
) -> Result<Option<StoredRow>> {
    let manifest_payload = read_overflow(store, state.pointer)?;
    if crc32c_parts(&[manifest_payload.as_slice()]) != state.checksum {
        return Err(DbError::corruption(
            "paged table manifest checksum mismatch",
        ));
    }
    let manifest = decode_paged_table_manifest_payload(&manifest_payload)?;
    let total_row_count = manifest
        .chunks
        .iter()
        .fold(0usize, |total, chunk| total.saturating_add(chunk.row_count));
    if state.row_count != 0 && total_row_count != state.row_count {
        return Err(DbError::corruption(
            "paged table manifest row count mismatch",
        ));
    }

    let mut checked_chunks = BTreeSet::new();
    for position in [
        row_id
            .checked_sub(1)
            .and_then(|position| usize::try_from(position).ok()),
        usize::try_from(row_id).ok(),
    ]
    .into_iter()
    .flatten()
    {
        let Some(chunk_index) = manifest_chunk_index_for_row_position(&manifest.chunks, position)
        else {
            continue;
        };
        if !checked_chunks.insert(chunk_index) {
            continue;
        }
        if let Some(row) =
            read_deferred_row_by_id_from_paged_chunk(store, &manifest.chunks[chunk_index], row_id)?
        {
            return Ok(Some(row));
        }
    }

    for (chunk_index, chunk) in manifest.chunks.iter().enumerate() {
        if checked_chunks.contains(&chunk_index) {
            continue;
        }
        if let Some(row) = read_deferred_row_by_id_from_paged_chunk(store, chunk, row_id)? {
            return Ok(Some(row));
        }
    }
    Ok(None)
}

fn read_deferred_stored_row_by_id<S: PageStore>(
    store: &S,
    state: PersistedTableState,
    table_schema: &TableSchema,
    row_id: i64,
    use_persistent_pk_index: bool,
    paged_locator_cache: Option<&DeferredPagedRowLocatorCache>,
) -> Result<Option<StoredRow>> {
    let locator = if use_persistent_pk_index {
        if let Some(pk_index_root) = table_schema.pk_index_root {
            btree_find_exact(
                store,
                Some(pk_index_root),
                encode_row_id_locator_key(row_id),
            )?
            .map(|payload| decode_row_locator(&payload))
            .transpose()?
        } else {
            None
        }
    } else {
        None
    };

    if state.pointer.is_table_paged_manifest() {
        if let Some(cache) = paged_locator_cache.filter(|cache| cache.matches_state(state)) {
            return cache
                .locators
                .get(row_id)
                .map(|cached| {
                    read_deferred_row_by_cached_paged_locator(
                        store,
                        row_id,
                        cached,
                        cache.verified_payload(cached.pointer, cached.checksum),
                    )
                })
                .unwrap_or(Ok(None));
        }
        return match locator {
            Some(DecodedRowLocator::V2(locator)) => {
                read_deferred_row_by_locator_from_paged_table_payload(store, state, row_id, locator)
            }
            _ => read_deferred_row_by_id_from_paged_table_manifest(store, state, row_id),
        };
    }

    if let Some(locator) = locator {
        return match locator {
            DecodedRowLocator::V1(locator) => {
                read_deferred_row_by_locator_from_table_payload(store, state, row_id, locator)
            }
            DecodedRowLocator::V2(locator) => read_deferred_row_by_locator_from_table_payload(
                store,
                state,
                row_id,
                RowLocatorV1 {
                    byte_offset: locator.byte_offset,
                    byte_len: locator.byte_len,
                },
            ),
        };
    }

    read_deferred_row_by_id_from_table_payload(store, state, row_id)
}

fn read_deferred_projected_values_by_id<S: PageStore>(
    store: &S,
    state: PersistedTableState,
    table_schema: &TableSchema,
    row_id: i64,
    use_persistent_pk_index: bool,
    paged_locator_cache: Option<&DeferredPagedRowLocatorCache>,
    projection_indexes: &[usize],
) -> Result<Option<Vec<Value>>> {
    if projection_indexes.is_empty() {
        if state.pointer.is_compressed() {
            let entry = read_deferred_compressed_table_lookup_entry(store, state)?;
            if entry.row_locators.contains_key(&row_id) {
                return Ok(Some(Vec::new()));
            }
        }
        if use_persistent_pk_index {
            if let Some(pk_index_root) = table_schema.pk_index_root {
                if btree_find_exact(
                    store,
                    Some(pk_index_root),
                    encode_row_id_locator_key(row_id),
                )?
                .is_some()
                {
                    return Ok(Some(Vec::new()));
                }
            }
        }
        if state.pointer.is_table_paged_manifest()
            && paged_locator_cache
                .filter(|cache| cache.matches_state(state))
                .and_then(|cache| cache.locators.get(row_id))
                .is_some()
        {
            return Ok(Some(Vec::new()));
        }

        return read_deferred_stored_row_by_id(
            store,
            state,
            table_schema,
            row_id,
            use_persistent_pk_index,
            paged_locator_cache,
        )
        .map(|row| row.map(|_| Vec::new()));
    }

    if state.pointer.is_table_paged_manifest() {
        if let Some(cache) = paged_locator_cache.filter(|cache| cache.matches_state(state)) {
            return cache
                .locators
                .get(row_id)
                .map(|cached| {
                    read_deferred_projected_values_by_cached_paged_locator(
                        store,
                        cached,
                        cache.verified_payload(cached.pointer, cached.checksum),
                        projection_indexes,
                    )
                })
                .transpose();
        }
    }

    read_deferred_stored_row_by_id(
        store,
        state,
        table_schema,
        row_id,
        use_persistent_pk_index,
        paged_locator_cache,
    )
    .map(|row| row.map(|row| project_simple_projection_value_vec(&row.values, projection_indexes)))
}

fn deferred_rowid_lookup_available(
    state: PersistedTableState,
    table_schema: &TableSchema,
    use_persistent_pk_index: bool,
    paged_locator_cache: Option<&DeferredPagedRowLocatorCache>,
) -> bool {
    state.pointer.is_compressed()
        || (use_persistent_pk_index && table_schema.pk_index_root.is_some())
        || paged_locator_cache.is_some_and(|cache| cache.matches_state(state))
}

fn read_table_payload_row_count<S: PageStore>(
    store: &S,
    pointer: OverflowPointer,
) -> Result<usize> {
    if pointer.head_page_id == 0 || pointer.logical_len == 0 {
        return Ok(0);
    }
    if pointer.is_compressed() {
        let payload = read_overflow(store, pointer)?;
        let mut cursor = Cursor::new(&payload);
        let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
        if magic != TABLE_PAYLOAD_MAGIC {
            return Err(DbError::corruption("table payload magic is invalid"));
        }
        return Ok(cursor.read_u32()? as usize);
    }

    let mut cursor = OverflowPayloadCursor::new(store, pointer);
    let mut magic = [0_u8; TABLE_PAYLOAD_MAGIC.len()];
    cursor.read_exact(&mut magic)?;
    if magic != *TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    Ok(cursor.read_u32()? as usize)
}

pub(crate) fn read_persisted_table_row_count<S: PageStore>(
    store: &S,
    state: PersistedTableState,
) -> Result<usize> {
    if state.pointer.head_page_id == 0 || state.pointer.logical_len == 0 {
        return Ok(0);
    }
    if !state.pointer.is_table_paged_manifest() {
        let payload = read_overflow(store, state.pointer)?;
        return read_table_payload_live_row_count_from_bytes(&payload);
    }

    let manifest_payload = read_overflow(store, state.pointer)?;
    if crc32c_parts(&[manifest_payload.as_slice()]) != state.checksum {
        return Err(DbError::corruption(
            "paged table manifest checksum mismatch",
        ));
    }
    let manifest = decode_paged_table_manifest_payload(&manifest_payload)?;
    let mut row_count = 0usize;
    for chunk in manifest.chunks {
        if chunk.tombstoned_row_ids.is_empty() && chunk.overlay_pointer.is_none() {
            row_count = row_count.saturating_add(chunk.row_count);
            continue;
        }
        let base_count = read_table_payload_row_count(store, chunk.pointer)?;
        let overlay_count = match chunk.overlay_pointer {
            Some(pointer) => read_table_payload_row_count(store, pointer)?,
            None => 0,
        };
        row_count = row_count.saturating_add(
            base_count
                .saturating_sub(chunk.tombstoned_row_ids.len())
                .saturating_add(overlay_count),
        );
    }
    Ok(row_count)
}

fn read_deferred_row_by_id_from_table_payload<S: PageStore>(
    store: &S,
    state: PersistedTableState,
    row_id: i64,
) -> Result<Option<StoredRow>> {
    let pointer = state.pointer;
    if pointer.head_page_id == 0 {
        return Ok(None);
    }
    if pointer.is_compressed() {
        let entry = read_deferred_compressed_table_lookup_entry(store, state)?;
        let Some(locator) = entry.row_locators.get(&row_id).copied() else {
            return Ok(None);
        };
        return decode_row_by_locator_from_payload(entry.payload.as_slice(), row_id, locator)
            .map(Some);
    }
    let mut cursor = OverflowPayloadCursor::new(store, pointer);
    let mut magic = [0_u8; TABLE_PAYLOAD_MAGIC.len()];
    cursor.read_exact(&mut magic)?;
    if magic != *TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }

    let row_count = cursor.read_u32()? as usize;
    for _ in 0..row_count {
        let candidate_row_id = cursor.read_i64()?;
        let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
        if candidate_row_id == row_id && !is_tombstone {
            let row_bytes = cursor.read_vec(row_bytes_len)?;
            let row = Row::decode(&row_bytes)?;
            return Ok(Some(StoredRow {
                row_id: candidate_row_id,
                values: row.into_values(),
            }));
        }
        cursor.skip(row_bytes_len)?;
    }
    Ok(None)
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

fn projected_dataset_order_column_index(dataset: &Dataset, expr: &Expr) -> Option<usize> {
    let Expr::Column { table, column } = expr else {
        return None;
    };
    if let Some(index) = dataset_column_index(dataset, table.as_deref(), column) {
        return Some(index);
    }
    if table.is_none() {
        return None;
    }
    let matches = dataset
        .columns
        .iter()
        .enumerate()
        .filter(|(_, binding)| !binding.hidden && identifiers_equal(&binding.name, column))
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

fn append_showdown_review_ranking_group(
    group: &mut Vec<ReviewRankingFastRow>,
    rows: &mut Vec<QueryRow>,
) {
    let mut buckets: [Vec<ReviewRankingFastRow>; 11] = std::array::from_fn(|_| Vec::new());
    for item in group.drain(..) {
        buckets[item.score as usize].push(item);
    }
    let mut current_rank = 1_i64;
    let mut current_dense_rank = 1_i64;
    let mut ordinal = 0_usize;
    let mut seen_score = false;
    for score in (1..buckets.len()).rev() {
        let bucket = &mut buckets[score];
        if bucket.is_empty() {
            continue;
        }
        if seen_score {
            current_rank = (ordinal + 1) as i64;
            current_dense_rank += 1;
        } else {
            seen_score = true;
        }
        for item in bucket.drain(..) {
            rows.push(QueryRow::new(vec![
                Value::Int64(item.movie_id),
                Value::Int64(item.score),
                item.author,
                Value::Int64(current_rank),
                Value::Int64(current_dense_rank),
            ]));
            ordinal += 1;
        }
    }
}

fn table_has_single_column_foreign_key(
    child_schema: &TableSchema,
    child_column: &str,
    parent_schema: &TableSchema,
    parent_column: &str,
) -> bool {
    child_schema.foreign_keys.iter().any(|foreign_key| {
        if foreign_key.columns.len() != 1
            || !identifiers_equal(&foreign_key.columns[0], child_column)
            || !identifiers_equal(&foreign_key.referenced_table, &parent_schema.name)
        {
            return false;
        }
        let referenced_columns = if foreign_key.referenced_columns.is_empty() {
            parent_schema.primary_key_columns.as_slice()
        } else {
            foreign_key.referenced_columns.as_slice()
        };
        referenced_columns.len() == 1 && identifiers_equal(&referenced_columns[0], parent_column)
    })
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

#[allow(clippy::too_many_arguments)]
fn accumulate_genre_popularity_movie(
    movie_source: &VisibleTableRowSource<'_>,
    movie_index_keys: Option<&RuntimeBtreeKeys>,
    movie_id_is_rowid_alias: bool,
    movie_id_value: Option<&Value>,
    movie_rating_index: usize,
    movie_count: &mut i64,
    rating_sum: &mut f64,
    rating_count: &mut i64,
) -> Result<()> {
    let Some(movie_id_value) = movie_id_value else {
        return Ok(());
    };
    if matches!(movie_id_value, Value::Null) {
        return Ok(());
    }

    if movie_id_is_rowid_alias {
        if let Some(row_id) = value_as_int64(movie_id_value) {
            if let Some(movie_row) = movie_source.row_by_id(row_id)? {
                accumulate_genre_popularity_rating(
                    movie_row.values(),
                    movie_rating_index,
                    movie_count,
                    rating_sum,
                    rating_count,
                );
                return Ok(());
            }
        }
    }

    let Some(keys) = movie_index_keys else {
        return Ok(());
    };
    match keys.row_ids_for_value_set(movie_id_value)? {
        RuntimeRowIdSet::Empty => {}
        RuntimeRowIdSet::Single(row_id) => {
            if let Some(movie_row) = movie_source.row_by_id(row_id)? {
                accumulate_genre_popularity_rating(
                    movie_row.values(),
                    movie_rating_index,
                    movie_count,
                    rating_sum,
                    rating_count,
                );
            }
        }
        RuntimeRowIdSet::Contiguous { start, len } => {
            for row_id in contiguous_row_ids(start, len) {
                if let Some(movie_row) = movie_source.row_by_id(row_id)? {
                    accumulate_genre_popularity_rating(
                        movie_row.values(),
                        movie_rating_index,
                        movie_count,
                        rating_sum,
                        rating_count,
                    );
                }
            }
        }
        RuntimeRowIdSet::Many(row_ids) => {
            for row_id in row_ids {
                if let Some(movie_row) = movie_source.row_by_id(*row_id)? {
                    accumulate_genre_popularity_rating(
                        movie_row.values(),
                        movie_rating_index,
                        movie_count,
                        rating_sum,
                        rating_count,
                    );
                }
            }
        }
        RuntimeRowIdSet::Owned(row_ids) => {
            for row_id in row_ids {
                if let Some(movie_row) = movie_source.row_by_id(row_id)? {
                    accumulate_genre_popularity_rating(
                        movie_row.values(),
                        movie_rating_index,
                        movie_count,
                        rating_sum,
                        rating_count,
                    );
                }
            }
        }
    }
    Ok(())
}

fn accumulate_genre_popularity_rating(
    movie_values: &[Value],
    movie_rating_index: usize,
    movie_count: &mut i64,
    rating_sum: &mut f64,
    rating_count: &mut i64,
) {
    *movie_count = movie_count.saturating_add(1);
    if let Some(value) = movie_values.get(movie_rating_index) {
        if let Some(rating) = indexed_join_aggregate_as_f64(value) {
            *rating_sum += rating;
            *rating_count = rating_count.saturating_add(1);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn push_movie_tag_search_movie_rows(
    runtime: &EngineRuntime,
    movie_source: &VisibleTableRowSource<'_>,
    movie_index_keys: Option<&RuntimeBtreeKeys>,
    movie_id_is_rowid_alias: bool,
    movie_id_value: Option<&Value>,
    projection_indexes: &[usize],
    bounded_order: Option<(&[SimpleOrderByPlan], usize)>,
    rows: &mut Vec<QueryRow>,
) -> Result<()> {
    let Some(movie_id_value) = movie_id_value else {
        return Ok(());
    };
    if matches!(movie_id_value, Value::Null) {
        return Ok(());
    }

    if movie_id_is_rowid_alias {
        if let Some(row_id) = value_as_int64(movie_id_value) {
            if let Some(movie_row) = movie_source.row_by_id(row_id)? {
                push_movie_tag_search_projected_row(
                    runtime,
                    movie_row.values(),
                    projection_indexes,
                    bounded_order,
                    rows,
                )?;
                return Ok(());
            }
        }
    }

    let Some(keys) = movie_index_keys else {
        return Ok(());
    };
    match keys.row_ids_for_value_set(movie_id_value)? {
        RuntimeRowIdSet::Empty => {}
        RuntimeRowIdSet::Single(row_id) => {
            if let Some(movie_row) = movie_source.row_by_id(row_id)? {
                push_movie_tag_search_projected_row(
                    runtime,
                    movie_row.values(),
                    projection_indexes,
                    bounded_order,
                    rows,
                )?;
            }
        }
        RuntimeRowIdSet::Contiguous { start, len } => {
            for row_id in contiguous_row_ids(start, len) {
                if let Some(movie_row) = movie_source.row_by_id(row_id)? {
                    push_movie_tag_search_projected_row(
                        runtime,
                        movie_row.values(),
                        projection_indexes,
                        bounded_order,
                        rows,
                    )?;
                }
            }
        }
        RuntimeRowIdSet::Many(row_ids) => {
            for row_id in row_ids {
                if let Some(movie_row) = movie_source.row_by_id(*row_id)? {
                    push_movie_tag_search_projected_row(
                        runtime,
                        movie_row.values(),
                        projection_indexes,
                        bounded_order,
                        rows,
                    )?;
                }
            }
        }
        RuntimeRowIdSet::Owned(row_ids) => {
            for row_id in row_ids {
                if let Some(movie_row) = movie_source.row_by_id(row_id)? {
                    push_movie_tag_search_projected_row(
                        runtime,
                        movie_row.values(),
                        projection_indexes,
                        bounded_order,
                        rows,
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn push_movie_tag_search_projected_row(
    runtime: &EngineRuntime,
    movie_values: &[Value],
    projection_indexes: &[usize],
    bounded_order: Option<(&[SimpleOrderByPlan], usize)>,
    rows: &mut Vec<QueryRow>,
) -> Result<()> {
    let row = project_simple_projection_values(movie_values, projection_indexes);
    if let Some((order_by, limit)) = bounded_order {
        push_bounded_projection_ordered_query_row(Some(runtime), rows, row, order_by, limit)
    } else {
        rows.push(row);
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn insert_movie_watchlist_group_rows(
    movie_source: &VisibleTableRowSource<'_>,
    movie_index_keys: Option<&RuntimeBtreeKeys>,
    movie_id_is_rowid_alias: bool,
    movie_id_value: &Value,
    priority: &Value,
    review_source: &VisibleTableRowSource<'_>,
    review_movie_keys: &RuntimeBtreeKeys,
    movie_id_index: usize,
    movie_title_index: usize,
    review_score_index: usize,
    groups: &mut BTreeMap<Vec<u8>, QueryRow>,
) -> Result<()> {
    if movie_id_is_rowid_alias {
        if let Some(row_id) = value_as_int64(movie_id_value) {
            if let Some(movie_row) = movie_source.row_by_id(row_id)? {
                insert_movie_watchlist_group_row(
                    movie_row.values(),
                    priority,
                    review_source,
                    review_movie_keys,
                    movie_id_index,
                    movie_title_index,
                    review_score_index,
                    groups,
                )?;
                return Ok(());
            }
        }
    }

    let Some(keys) = movie_index_keys else {
        return Ok(());
    };
    match keys.row_ids_for_value_set(movie_id_value)? {
        RuntimeRowIdSet::Empty => {}
        RuntimeRowIdSet::Single(row_id) => {
            if let Some(movie_row) = movie_source.row_by_id(row_id)? {
                insert_movie_watchlist_group_row(
                    movie_row.values(),
                    priority,
                    review_source,
                    review_movie_keys,
                    movie_id_index,
                    movie_title_index,
                    review_score_index,
                    groups,
                )?;
            }
        }
        RuntimeRowIdSet::Contiguous { start, len } => {
            for row_id in contiguous_row_ids(start, len) {
                if let Some(movie_row) = movie_source.row_by_id(row_id)? {
                    insert_movie_watchlist_group_row(
                        movie_row.values(),
                        priority,
                        review_source,
                        review_movie_keys,
                        movie_id_index,
                        movie_title_index,
                        review_score_index,
                        groups,
                    )?;
                }
            }
        }
        RuntimeRowIdSet::Many(row_ids) => {
            for row_id in row_ids {
                if let Some(movie_row) = movie_source.row_by_id(*row_id)? {
                    insert_movie_watchlist_group_row(
                        movie_row.values(),
                        priority,
                        review_source,
                        review_movie_keys,
                        movie_id_index,
                        movie_title_index,
                        review_score_index,
                        groups,
                    )?;
                }
            }
        }
        RuntimeRowIdSet::Owned(row_ids) => {
            for row_id in row_ids {
                if let Some(movie_row) = movie_source.row_by_id(row_id)? {
                    insert_movie_watchlist_group_row(
                        movie_row.values(),
                        priority,
                        review_source,
                        review_movie_keys,
                        movie_id_index,
                        movie_title_index,
                        review_score_index,
                        groups,
                    )?;
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_movie_watchlist_group_row(
    movie_values: &[Value],
    priority: &Value,
    review_source: &VisibleTableRowSource<'_>,
    review_movie_keys: &RuntimeBtreeKeys,
    movie_id_index: usize,
    movie_title_index: usize,
    review_score_index: usize,
    groups: &mut BTreeMap<Vec<u8>, QueryRow>,
) -> Result<()> {
    let Some(movie_id) = movie_values.get(movie_id_index) else {
        return Err(DbError::internal(
            "movie watchlist id column missing from movie row",
        ));
    };
    let group_key = row_identity(std::slice::from_ref(movie_id))?;
    if groups.contains_key(&group_key) {
        return Ok(());
    }
    let Some(title) = movie_values.get(movie_title_index) else {
        return Err(DbError::internal(
            "movie watchlist title column missing from movie row",
        ));
    };
    let avg = movie_watchlist_review_avg(
        review_source,
        review_movie_keys,
        movie_id,
        review_score_index,
    )?;
    groups.insert(
        group_key,
        QueryRow::new(vec![movie_id.clone(), title.clone(), priority.clone(), avg]),
    );
    Ok(())
}

fn push_bounded_movie_busiest_people_count(
    counts: &mut Vec<MovieBusiestPeopleCount>,
    candidate: MovieBusiestPeopleCount,
    bounded_count: usize,
) {
    if bounded_count == 0 {
        return;
    }
    if counts.len() < bounded_count {
        counts.push(candidate);
        return;
    }
    let mut worst_index = 0;
    for index in 1..counts.len() {
        if compare_movie_busiest_people_counts(&counts[index], &counts[worst_index])
            == std::cmp::Ordering::Greater
        {
            worst_index = index;
        }
    }
    if compare_movie_busiest_people_counts(&candidate, &counts[worst_index])
        == std::cmp::Ordering::Less
    {
        counts[worst_index] = candidate;
    }
}

fn sort_movie_busiest_people_counts(counts: &mut [MovieBusiestPeopleCount]) {
    counts.sort_by(compare_movie_busiest_people_counts);
}

fn compare_movie_busiest_people_counts(
    left: &MovieBusiestPeopleCount,
    right: &MovieBusiestPeopleCount,
) -> std::cmp::Ordering {
    right
        .role_count
        .cmp(&left.role_count)
        .then_with(|| compare_runtime_btree_keys(&left.person_key, &right.person_key))
}

fn compare_runtime_btree_keys(
    left: &RuntimeBtreeKey,
    right: &RuntimeBtreeKey,
) -> std::cmp::Ordering {
    match (left, right) {
        (RuntimeBtreeKey::Encoded(left), RuntimeBtreeKey::Encoded(right)) => left.cmp(right),
        (RuntimeBtreeKey::Int64(left), RuntimeBtreeKey::Int64(right)) => left.cmp(right),
        (RuntimeBtreeKey::Uuid(left), RuntimeBtreeKey::Uuid(right)) => left.cmp(right),
        (RuntimeBtreeKey::Encoded(_), RuntimeBtreeKey::Int64(_)) => std::cmp::Ordering::Less,
        (RuntimeBtreeKey::Encoded(_), RuntimeBtreeKey::Uuid(_)) => std::cmp::Ordering::Less,
        (RuntimeBtreeKey::Int64(_), RuntimeBtreeKey::Encoded(_)) => std::cmp::Ordering::Greater,
        (RuntimeBtreeKey::Int64(_), RuntimeBtreeKey::Uuid(_)) => std::cmp::Ordering::Less,
        (RuntimeBtreeKey::Uuid(_), RuntimeBtreeKey::Encoded(_)) => std::cmp::Ordering::Greater,
        (RuntimeBtreeKey::Uuid(_), RuntimeBtreeKey::Int64(_)) => std::cmp::Ordering::Greater,
    }
}

fn push_movie_busiest_people_row(
    people_source: &VisibleTableRowSource<'_>,
    people_index_keys: Option<&RuntimeBtreeKeys>,
    people_id_is_rowid_alias: bool,
    candidate: &MovieBusiestPeopleCount,
    projection_indexes: &[usize],
    rows: &mut Vec<QueryRow>,
) -> Result<()> {
    if people_id_is_rowid_alias {
        if let RuntimeBtreeKey::Int64(row_id) = &candidate.person_key {
            if let Some(people_row) = people_source.row_by_id(*row_id)? {
                push_movie_busiest_people_projected_row(
                    people_row.values(),
                    candidate.role_count,
                    projection_indexes,
                    rows,
                );
            }
            return Ok(());
        }
    }

    let Some(keys) = people_index_keys else {
        return Ok(());
    };
    match keys.row_id_set_for_key(&candidate.person_key) {
        RuntimeRowIdSet::Empty => {}
        RuntimeRowIdSet::Single(row_id) => {
            if let Some(people_row) = people_source.row_by_id(row_id)? {
                push_movie_busiest_people_projected_row(
                    people_row.values(),
                    candidate.role_count,
                    projection_indexes,
                    rows,
                );
            }
        }
        RuntimeRowIdSet::Contiguous { start, len } => {
            for row_id in contiguous_row_ids(start, len) {
                if let Some(people_row) = people_source.row_by_id(row_id)? {
                    push_movie_busiest_people_projected_row(
                        people_row.values(),
                        candidate.role_count,
                        projection_indexes,
                        rows,
                    );
                }
            }
        }
        RuntimeRowIdSet::Many(row_ids) => {
            for row_id in row_ids {
                if let Some(people_row) = people_source.row_by_id(*row_id)? {
                    push_movie_busiest_people_projected_row(
                        people_row.values(),
                        candidate.role_count,
                        projection_indexes,
                        rows,
                    );
                }
            }
        }
        RuntimeRowIdSet::Owned(row_ids) => {
            for row_id in row_ids {
                if let Some(people_row) = people_source.row_by_id(row_id)? {
                    push_movie_busiest_people_projected_row(
                        people_row.values(),
                        candidate.role_count,
                        projection_indexes,
                        rows,
                    );
                }
            }
        }
    }
    Ok(())
}

fn push_movie_busiest_people_projected_row(
    people_values: &[Value],
    role_count: i64,
    projection_indexes: &[usize],
    rows: &mut Vec<QueryRow>,
) {
    let projected = project_simple_projection_values(people_values, projection_indexes);
    let mut values = projected.values().to_vec();
    values.push(Value::Int64(role_count));
    rows.push(QueryRow::new(values));
}

fn accumulate_directors_cte_movie(
    movie_source: &VisibleTableRowSource<'_>,
    movie_index_keys: Option<&RuntimeBtreeKeys>,
    movie_id_is_rowid_alias: bool,
    movie_id_value: &Value,
    movie_title_index: usize,
    movie_rating_index: usize,
    accumulator: &mut DirectorsCteAccumulator,
) -> Result<()> {
    if movie_id_is_rowid_alias {
        if let Some(row_id) = value_as_int64(movie_id_value) {
            if let Some(movie_row) = movie_source.row_by_id(row_id)? {
                accumulator.add_movie(movie_row.values(), movie_title_index, movie_rating_index);
                return Ok(());
            }
        }
    }

    let Some(keys) = movie_index_keys else {
        return Ok(());
    };
    match keys.row_ids_for_value_set(movie_id_value)? {
        RuntimeRowIdSet::Empty => {}
        RuntimeRowIdSet::Single(row_id) => {
            if let Some(movie_row) = movie_source.row_by_id(row_id)? {
                accumulator.add_movie(movie_row.values(), movie_title_index, movie_rating_index);
            }
        }
        RuntimeRowIdSet::Contiguous { start, len } => {
            for row_id in contiguous_row_ids(start, len) {
                if let Some(movie_row) = movie_source.row_by_id(row_id)? {
                    accumulator.add_movie(
                        movie_row.values(),
                        movie_title_index,
                        movie_rating_index,
                    );
                }
            }
        }
        RuntimeRowIdSet::Many(row_ids) => {
            for row_id in row_ids {
                if let Some(movie_row) = movie_source.row_by_id(*row_id)? {
                    accumulator.add_movie(
                        movie_row.values(),
                        movie_title_index,
                        movie_rating_index,
                    );
                }
            }
        }
        RuntimeRowIdSet::Owned(row_ids) => {
            for row_id in row_ids {
                if let Some(movie_row) = movie_source.row_by_id(row_id)? {
                    accumulator.add_movie(
                        movie_row.values(),
                        movie_title_index,
                        movie_rating_index,
                    );
                }
            }
        }
    }
    Ok(())
}

fn projection_expr_matches_binding_column(
    item: &SelectItem,
    binding: TableBindingRef<'_>,
    column: &str,
) -> bool {
    matches!(
        item,
        SelectItem::Expr { expr, .. } if expr_matches_binding_column_or_unqualified(expr, binding, column)
    )
}

fn group_exprs_match_binding_columns(
    group_by: &[Expr],
    binding: TableBindingRef<'_>,
    columns: &[&str],
) -> bool {
    group_by.len() == columns.len()
        && group_by
            .iter()
            .zip(columns)
            .all(|(expr, column)| expr_matches_binding_column_or_unqualified(expr, binding, column))
}

pub(crate) fn expr_matches_binding_column_or_unqualified(
    expr: &Expr,
    binding: TableBindingRef<'_>,
    column: &str,
) -> bool {
    match expr {
        Expr::Column {
            table: None,
            column: expr_column,
        } => identifiers_equal(expr_column, column),
        _ => expr_matches_binding_column(expr, binding, column),
    }
}

fn equality_filter_text_literal<'a>(
    expr: &'a Expr,
    binding: TableBindingRef<'_>,
    column: &str,
) -> Option<&'a str> {
    let Expr::Binary {
        left,
        op: BinaryOp::Eq,
        right,
    } = expr
    else {
        return None;
    };
    if expr_matches_binding_column(left, binding, column) {
        return text_literal_value(right);
    }
    if expr_matches_binding_column(right, binding, column) {
        return text_literal_value(left);
    }
    None
}

fn text_literal_value(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(Value::Text(value)) => Some(value.as_str()),
        _ => None,
    }
}

fn projection_expr_string_agg_separator<'a>(
    item: &'a SelectItem,
    binding: TableBindingRef<'_>,
    column: &str,
) -> Option<&'a str> {
    let SelectItem::Expr { expr, .. } = item else {
        return None;
    };
    let Expr::Aggregate {
        name,
        args,
        distinct,
        star,
        order_by,
        within_group,
    } = expr
    else {
        return None;
    };
    if !(name.eq_ignore_ascii_case("string_agg") || name.eq_ignore_ascii_case("group_concat"))
        || *distinct
        || *star
        || !order_by.is_empty()
        || *within_group
        || args.len() != 2
        || !expr_matches_binding_column(&args[0], binding, column)
    {
        return None;
    }
    text_literal_value(&args[1])
}

fn status_case_condition_matches(
    expr: &Expr,
    binding: TableBindingRef<'_>,
    status_column: &str,
    status_value: &str,
) -> bool {
    let Expr::Binary { left, op, right } = expr else {
        return false;
    };
    if *op != BinaryOp::Eq {
        return false;
    }
    (expr_matches_binding_column(left, binding, status_column)
        && matches!(&**right, Expr::Literal(Value::Text(value)) if value == status_value))
        || (expr_matches_binding_column(right, binding, status_column)
            && matches!(&**left, Expr::Literal(Value::Text(value)) if value == status_value))
}

fn classify_indexed_join_aggregate(
    expr: &Expr,
    binding: TableBindingRef<'_>,
    schema: &TableSchema,
) -> Option<IndexedJoinAggregateKind> {
    let Expr::Aggregate {
        name,
        args,
        distinct,
        star,
        order_by,
        within_group,
    } = expr
    else {
        return None;
    };
    if !order_by.is_empty() || *within_group {
        return None;
    }
    if *star && name.eq_ignore_ascii_case("count") && args.is_empty() && !*distinct {
        return Some(IndexedJoinAggregateKind::CountRows);
    }
    if args.len() != 1 {
        return None;
    }
    let col = resolved_child_column_index(args.first()?, binding, schema)?;
    let name_lower = name.to_lowercase();
    match name_lower.as_str() {
        "count" if !*distinct => Some(IndexedJoinAggregateKind::CountNonNull(col)),
        "count" if *distinct => Some(IndexedJoinAggregateKind::CountDistinct(col)),
        "sum" if !*distinct => Some(IndexedJoinAggregateKind::Sum(col)),
        "avg" if !*distinct => Some(IndexedJoinAggregateKind::Avg(col)),
        "min" if !*distinct => Some(IndexedJoinAggregateKind::Min(col)),
        "max" if !*distinct => Some(IndexedJoinAggregateKind::Max(col)),
        _ => None,
    }
}

fn resolved_child_column_index(
    expr: &Expr,
    binding: TableBindingRef<'_>,
    schema: &TableSchema,
) -> Option<usize> {
    let Expr::Column { table, column } = expr else {
        return None;
    };
    if let Some(table_ref) = table {
        if !identifiers_equal(table_ref, binding.name)
            && !binding
                .alias
                .as_ref()
                .is_some_and(|alias| identifiers_equal(table_ref, alias))
        {
            return None;
        }
    }
    schema
        .columns
        .iter()
        .position(|col| identifiers_equal(&col.name, column))
        .or_else(|| {
            let lowered = column.to_lowercase();
            schema
                .columns
                .iter()
                .position(|col| col.name.to_lowercase() == lowered)
        })
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

fn projection_order_by_plan(
    order_by: &[crate::sql::ast::OrderBy],
    projection: &[SelectItem],
) -> Option<Vec<SimpleOrderByPlan>> {
    if order_by.is_empty() {
        return None;
    }
    order_by
        .iter()
        .map(|entry| {
            order_by_projection_index(entry, projection).map(|projection_index| SimpleOrderByPlan {
                projection_index,
                descending: entry.descending,
                collation: entry.collation.clone(),
            })
        })
        .collect()
}

fn order_by_projection_index(
    order_by: &crate::sql::ast::OrderBy,
    projection: &[SelectItem],
) -> Option<usize> {
    let mut matched = None;
    for (index, item) in projection.iter().enumerate() {
        let item_matches = match item {
            SelectItem::Expr { expr, alias } => {
                let column_match = if let Expr::Column { table, column } = &order_by.expr {
                    if table.is_none()
                        && alias
                            .as_deref()
                            .is_some_and(|alias| identifiers_equal(column, alias))
                    {
                        true
                    } else if let Expr::Column {
                        table: projection_table,
                        column: projection_column,
                    } = expr
                    {
                        let qualifier_matches =
                            match (table.as_deref(), projection_table.as_deref()) {
                                (Some(order_table), Some(projection_table)) => {
                                    identifiers_equal(order_table, projection_table)
                                }
                                (Some(_), None) | (None, _) => true,
                            };
                        qualifier_matches && identifiers_equal(column, projection_column)
                    } else {
                        false
                    }
                } else {
                    false
                };
                column_match || &order_by.expr == expr
            }
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
        };
        if item_matches && matched.replace(index).is_some() {
            return None;
        }
    }
    matched
}

fn matching_simple_grouped_aggregate_binding<'a>(
    expr: &Expr,
    table_name: &str,
    binding_name: &str,
    aggregate_bindings: &'a [SimpleGroupedNumericAggregateBinding],
) -> Option<&'a SimpleGroupedNumericAggregateBinding> {
    let Expr::Aggregate {
        name,
        args,
        distinct,
        star,
        order_by,
        within_group,
    } = expr
    else {
        return None;
    };
    if !order_by.is_empty() || *within_group {
        return None;
    }
    aggregate_bindings
        .iter()
        .find(|binding| match binding.kind {
            SimpleGroupedNumericAggregateKind::CountRows => {
                name.eq_ignore_ascii_case("count") && !*distinct && args.is_empty() && *star
            }
            SimpleGroupedNumericAggregateKind::CountNonNull => {
                if !name.eq_ignore_ascii_case("count") || *distinct || *star || args.len() != 1 {
                    return false;
                }
                if let Some(expected) = binding.source_expr.as_ref() {
                    expected == &args[0]
                        && expr_references_binding_names(&args[0], table_name, binding_name)
                } else {
                    let Expr::Column { table, column } = &args[0] else {
                        return false;
                    };
                    if let Some(table) = table.as_deref() {
                        if !identifiers_equal(table, table_name)
                            && !identifiers_equal(table, binding_name)
                        {
                            return false;
                        }
                    }
                    binding
                        .source_column_name
                        .as_deref()
                        .is_some_and(|expected| identifiers_equal(column, expected))
                }
            }
            SimpleGroupedNumericAggregateKind::CountDistinct => {
                if !name.eq_ignore_ascii_case("count") || !*distinct || *star || args.len() != 1 {
                    return false;
                }
                if let Some(expected) = binding.source_expr.as_ref() {
                    expected == &args[0]
                        && expr_references_binding_names(&args[0], table_name, binding_name)
                } else {
                    let Expr::Column { table, column } = &args[0] else {
                        return false;
                    };
                    if let Some(table) = table.as_deref() {
                        if !identifiers_equal(table, table_name)
                            && !identifiers_equal(table, binding_name)
                        {
                            return false;
                        }
                    }
                    binding
                        .source_column_name
                        .as_deref()
                        .is_some_and(|expected| identifiers_equal(column, expected))
                }
            }
            SimpleGroupedNumericAggregateKind::Sum
            | SimpleGroupedNumericAggregateKind::SumDistinct
            | SimpleGroupedNumericAggregateKind::Avg
            | SimpleGroupedNumericAggregateKind::AvgDistinct
            | SimpleGroupedNumericAggregateKind::Total
            | SimpleGroupedNumericAggregateKind::TotalDistinct
            | SimpleGroupedNumericAggregateKind::StddevSamp
            | SimpleGroupedNumericAggregateKind::StddevSampDistinct
            | SimpleGroupedNumericAggregateKind::StddevPop
            | SimpleGroupedNumericAggregateKind::StddevPopDistinct
            | SimpleGroupedNumericAggregateKind::VarSamp
            | SimpleGroupedNumericAggregateKind::VarSampDistinct
            | SimpleGroupedNumericAggregateKind::VarPop
            | SimpleGroupedNumericAggregateKind::VarPopDistinct
            | SimpleGroupedNumericAggregateKind::BoolAnd
            | SimpleGroupedNumericAggregateKind::BoolAndDistinct
            | SimpleGroupedNumericAggregateKind::BoolOr
            | SimpleGroupedNumericAggregateKind::BoolOrDistinct => {
                let expected_distinct = binding.kind.uses_distinct();
                if !binding.kind.matches_aggregate_name(name)
                    || *distinct != expected_distinct
                    || *star
                    || args.len() != 1
                {
                    return false;
                }
                if let Some(expected) = binding.source_expr.as_ref() {
                    expected == &args[0]
                        && expr_references_binding_names(&args[0], table_name, binding_name)
                } else {
                    let Expr::Column { table, column } = &args[0] else {
                        return false;
                    };
                    if let Some(table) = table.as_deref() {
                        if !identifiers_equal(table, table_name)
                            && !identifiers_equal(table, binding_name)
                        {
                            return false;
                        }
                    }
                    binding
                        .source_column_name
                        .as_deref()
                        .is_some_and(|expected| identifiers_equal(column, expected))
                }
            }
            SimpleGroupedNumericAggregateKind::Min | SimpleGroupedNumericAggregateKind::Max => {
                if !binding.kind.matches_aggregate_name(name) || *star || args.len() != 1 {
                    return false;
                }
                binding.source_expr.as_ref().is_some_and(|expected| {
                    expected == &args[0]
                        && expr_references_binding_names(&args[0], table_name, binding_name)
                })
            }
        })
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

fn merged_single_column_using_expr(left_binding: &str, right_binding: &str, column: &str) -> Expr {
    Expr::Function {
        name: "coalesce".to_string(),
        args: vec![
            Expr::Column {
                table: Some(left_binding.to_string()),
                column: column.to_string(),
            },
            Expr::Column {
                table: Some(right_binding.to_string()),
                column: column.to_string(),
            },
        ],
    }
}

type SimpleJoinHashRows = BTreeMap<Vec<u8>, Vec<(i64, Vec<Value>)>>;

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
                let mut binding = ColumnBinding::visible(
                    None,
                    alias
                        .clone()
                        .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
                );
                binding.column_type = dataset.columns[source_index].column_type;
                output_columns.push(binding);
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
                    if binding.hidden || binding.table.as_deref() != Some(table.as_str()) {
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
    for row in dataset.rows.iter() {
        let mut output_row = Vec::with_capacity(projection_plan.len());
        for source_index in &projection_plan {
            let value = row
                .get(*source_index)
                .ok_or_else(|| DbError::internal("projection source index exceeds row width"))?;
            output_row.push(value.clone());
        }
        output_rows.push(output_row);
    }
    Ok(Some(Dataset::with_rows(output_columns, output_rows)))
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SimpleTrigramLookup<'a> {
    table_qualifier: Option<&'a str>,
    column_name: &'a str,
    pattern_expr: &'a Expr,
    has_additional_filter: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SimpleFullTextLookup<'a> {
    index_name_expr: &'a Expr,
    query_expr: &'a Expr,
}

pub(crate) fn exact_fulltext_lookup(filter: Option<&Expr>) -> Option<SimpleFullTextLookup<'_>> {
    let Some(Expr::Function { name, args }) = filter else {
        return None;
    };
    if !name.eq_ignore_ascii_case("fulltext_match") || args.len() != 2 {
        return None;
    }
    Some(SimpleFullTextLookup {
        index_name_expr: &args[0],
        query_expr: &args[1],
    })
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SimpleSpatialLookup<'a> {
    table_qualifier: Option<&'a str>,
    column_name: &'a str,
    value_expr: &'a Expr,
    radius_expr: Option<&'a Expr>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SimpleSpatialJoinPredicate<'a> {
    left: QualifiedColumnRef<'a>,
    right: QualifiedColumnRef<'a>,
    radius_expr: Option<&'a Expr>,
}

fn qualified_column_ref_expr(expr: &Expr) -> Option<QualifiedColumnRef<'_>> {
    let Expr::Column { table, column } = expr else {
        return None;
    };
    Some(QualifiedColumnRef {
        table: table.as_deref(),
        column,
    })
}

fn expr_has_column_ref(expr: &Expr) -> bool {
    match expr {
        Expr::Default => false,
        Expr::Column { .. } => true,
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Collate { expr, .. } => expr_has_column_ref(expr),
        Expr::Binary { left, right, .. } => expr_has_column_ref(left) || expr_has_column_ref(right),
        Expr::Between {
            expr, low, high, ..
        } => expr_has_column_ref(expr) || expr_has_column_ref(low) || expr_has_column_ref(high),
        Expr::InList { expr, items, .. } => {
            expr_has_column_ref(expr) || items.iter().any(expr_has_column_ref)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_has_column_ref(expr)
                || expr_has_column_ref(pattern)
                || escape.as_deref().is_some_and(expr_has_column_ref)
        }
        Expr::Function { args, .. } => args.iter().any(expr_has_column_ref),
        Expr::Aggregate { args, order_by, .. } => {
            args.iter().any(expr_has_column_ref)
                || order_by
                    .iter()
                    .any(|order| expr_has_column_ref(&order.expr))
        }
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand.as_deref().is_some_and(expr_has_column_ref)
                || branches.iter().any(|(condition, value)| {
                    expr_has_column_ref(condition) || expr_has_column_ref(value)
                })
                || else_expr.as_deref().is_some_and(expr_has_column_ref)
        }
        Expr::Row(items) => items.iter().any(expr_has_column_ref),
        Expr::InSubquery { expr, .. } | Expr::CompareSubquery { expr, .. } => {
            expr_has_column_ref(expr)
        }
        Expr::Literal(_)
        | Expr::Parameter(_)
        | Expr::RowNumber { .. }
        | Expr::WindowFunction { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_) => false,
    }
}

fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Default => false,
        Expr::Aggregate { .. } => true,
        Expr::Unary { expr, .. } | Expr::Collate { expr, .. } => expr_contains_aggregate(expr),
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

fn expr_contains_runtime_extension_aggregate(runtime: &EngineRuntime, expr: &Expr) -> Result<bool> {
    Ok(match expr {
        Expr::Default => false,
        Expr::Aggregate { .. } => true,
        Expr::Function { name, args } => {
            crate::extensions::runtime_has_aggregate_function(runtime, name)?
                || args.iter().try_fold(false, |found, arg| {
                    if found {
                        Ok(true)
                    } else {
                        expr_contains_runtime_extension_aggregate(runtime, arg)
                    }
                })?
        }
        Expr::Unary { expr, .. } | Expr::Collate { expr, .. } | Expr::Cast { expr, .. } => {
            expr_contains_runtime_extension_aggregate(runtime, expr)?
        }
        Expr::Binary { left, right, .. } => {
            expr_contains_runtime_extension_aggregate(runtime, left)?
                || expr_contains_runtime_extension_aggregate(runtime, right)?
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_runtime_extension_aggregate(runtime, expr)?
                || expr_contains_runtime_extension_aggregate(runtime, low)?
                || expr_contains_runtime_extension_aggregate(runtime, high)?
        }
        Expr::InList { expr, items, .. } => {
            expr_contains_runtime_extension_aggregate(runtime, expr)?
                || items.iter().try_fold(false, |found, item| {
                    if found {
                        Ok(true)
                    } else {
                        expr_contains_runtime_extension_aggregate(runtime, item)
                    }
                })?
        }
        Expr::InSubquery { expr, .. } | Expr::CompareSubquery { expr, .. } => {
            expr_contains_runtime_extension_aggregate(runtime, expr)?
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_runtime_extension_aggregate(runtime, expr)?
                || expr_contains_runtime_extension_aggregate(runtime, pattern)?
                || escape
                    .as_deref()
                    .map(|expr| expr_contains_runtime_extension_aggregate(runtime, expr))
                    .transpose()?
                    .unwrap_or(false)
        }
        Expr::IsNull { expr, .. } => expr_contains_runtime_extension_aggregate(runtime, expr)?,
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand
                .as_deref()
                .map(|expr| expr_contains_runtime_extension_aggregate(runtime, expr))
                .transpose()?
                .unwrap_or(false)
                || branches.iter().try_fold(false, |found, (left, right)| {
                    if found {
                        Ok(true)
                    } else {
                        Ok(expr_contains_runtime_extension_aggregate(runtime, left)?
                            || expr_contains_runtime_extension_aggregate(runtime, right)?)
                    }
                })?
                || else_expr
                    .as_deref()
                    .map(|expr| expr_contains_runtime_extension_aggregate(runtime, expr))
                    .transpose()?
                    .unwrap_or(false)
        }
        Expr::Row(items) => items.iter().try_fold(false, |found, item| {
            if found {
                Ok(true)
            } else {
                expr_contains_runtime_extension_aggregate(runtime, item)
            }
        })?,
        Expr::ScalarSubquery(_) | Expr::Exists(_) => false,
        Expr::RowNumber { .. }
        | Expr::WindowFunction { .. }
        | Expr::Literal(_)
        | Expr::Column { .. }
        | Expr::Parameter(_) => false,
    })
}

fn expr_contains_window(expr: &Expr) -> bool {
    match expr {
        Expr::Default => false,
        Expr::RowNumber { .. } | Expr::WindowFunction { .. } => true,
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Collate { expr, .. } => expr_contains_window(expr),
        Expr::Binary { left, right, .. } => {
            expr_contains_window(left) || expr_contains_window(right)
        }
        Expr::Between {
            expr, low, high, ..
        } => expr_contains_window(expr) || expr_contains_window(low) || expr_contains_window(high),
        Expr::InList { expr, items, .. } => {
            expr_contains_window(expr) || items.iter().any(expr_contains_window)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_window(expr)
                || expr_contains_window(pattern)
                || escape.as_ref().is_some_and(|e| expr_contains_window(e))
        }
        Expr::Function { args, .. } => args.iter().any(expr_contains_window),
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand.as_ref().is_some_and(|e| expr_contains_window(e))
                || branches
                    .iter()
                    .any(|(c, v)| expr_contains_window(c) || expr_contains_window(v))
                || else_expr.as_ref().is_some_and(|e| expr_contains_window(e))
        }
        Expr::Row(items) => items.iter().any(expr_contains_window),
        Expr::Aggregate { args, .. } => args.iter().any(expr_contains_window),
        Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_) => false,
        Expr::Literal(_) | Expr::Column { .. } | Expr::Parameter(_) => false,
    }
}

#[derive(Clone, Debug)]
pub(crate) struct JoinUsingColumn {
    name: String,
    left_index: usize,
    right_index: usize,
}

pub(crate) struct JoinEvalContext<'a> {
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

fn resolve_join_using_columns_for_schemas(
    left_columns: &[ColumnBinding],
    right_columns: &[ColumnBinding],
    constraint: &JoinConstraint,
    _left_table: &TableSchema,
    _right_table: &TableSchema,
) -> Result<Vec<JoinUsingColumn>> {
    match constraint {
        JoinConstraint::Using(names) => {
            let mut result = Vec::new();
            for name in names {
                let left_idx = left_columns
                    .iter()
                    .position(|c| identifiers_equal(&c.name, name))
                    .ok_or_else(|| {
                        DbError::sql(format!("column \"{name}\" not found in left table"))
                    })?;
                let right_idx = right_columns
                    .iter()
                    .position(|c| identifiers_equal(&c.name, name))
                    .ok_or_else(|| {
                        DbError::sql(format!("column \"{name}\" not found in right table"))
                    })?;
                result.push(JoinUsingColumn {
                    name: left_columns[left_idx].name.clone(),
                    left_index: left_idx,
                    right_index: right_idx,
                });
            }
            Ok(result)
        }
        JoinConstraint::Natural => {
            let mut result = Vec::new();
            for lc in left_columns.iter() {
                if let Some(right_idx) = right_columns
                    .iter()
                    .position(|rc| identifiers_equal(&rc.name, &lc.name))
                {
                    let left_idx = left_columns
                        .iter()
                        .position(|c| identifiers_equal(&c.name, &lc.name))
                        .ok_or_else(|| {
                            DbError::internal(
                                "internal: NATURAL join column is missing on left table",
                            )
                        })?;
                    result.push(JoinUsingColumn {
                        name: left_columns[left_idx].name.clone(),
                        left_index: left_idx,
                        right_index: right_idx,
                    });
                }
            }
            Ok(result)
        }
        JoinConstraint::On(_) => Ok(Vec::new()),
    }
}

impl EngineRuntime {}

pub(crate) fn statement_is_read_only(statement: &Statement) -> bool {
    matches!(statement, Statement::Query(_) | Statement::Explain(_))
}

#[cfg(test)]
mod tests;

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
                spatial_type: None,
                enum_type: None,
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
            pk_index_root: None,
        };
        assert!(generated_columns_are_stored(&table));

        let table2 = TableSchema {
            name: "u".to_string(),
            temporary: false,
            columns: vec![crate::catalog::ColumnSchema {
                name: "g".to_string(),
                column_type: crate::catalog::ColumnType::Int64,
                spatial_type: None,
                enum_type: None,
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
            pk_index_root: None,
        };
        assert!(generated_columns_are_stored(&table2));

        let table3 = TableSchema {
            name: "v".to_string(),
            temporary: false,
            columns: vec![crate::catalog::ColumnSchema {
                name: "g".to_string(),
                column_type: crate::catalog::ColumnType::Int64,
                spatial_type: None,
                enum_type: None,
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
            pk_index_root: None,
        };
        assert!(!generated_columns_are_stored(&table3));
    }
}

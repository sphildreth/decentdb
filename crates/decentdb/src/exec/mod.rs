//! Query execution operators, SQL result values, and runtime state.

pub(crate) mod bulk_load;
pub(crate) mod constraints;
pub(crate) mod ddl;
pub(crate) mod dml;
pub(crate) mod operators;
pub(crate) mod row;
pub(crate) mod triggers;
pub(crate) mod txn;
pub(crate) mod views;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Instant;

use crate::catalog::{
    identifiers_equal, CatalogState, ColumnSchema, IndexKind, IndexSchema, TableSchema,
};
use crate::error::{DbError, Result};
use crate::json::{parse_json, parse_json_path, JsonValue};
use crate::planner;
use crate::record::compression::CompressionMode;
use crate::record::key::encode_index_key;
use crate::record::overflow::{read_overflow, write_overflow, OverflowPointer};
use crate::record::row::Row;
use crate::record::value::{parse_decimal_text, Value};
use crate::search::{TrigramIndex, TrigramQueryResult};
use crate::sql::ast::{
    BinaryOp, Expr, FromItem, JoinKind, Query, QueryBody, Select, SelectItem, Statement, UnaryOp,
};
use crate::sql::parser::parse_sql_statement;
use crate::storage::checksum::crc32c_parts;
use crate::storage::page::{self, PageId, PageStore};
use crate::storage::PagerHandle;
use crate::wal::WalHandle;

use self::row::{ColumnBinding, Dataset};

pub use row::{QueryResult, QueryRow};

const ENGINE_ROOT_MAGIC: [u8; 8] = *b"DDBSQL1\0";
const ENGINE_ROOT_VERSION: u32 = 1;
const ENGINE_ROOT_HEADER_SIZE: usize = 32;
const LEGACY_RUNTIME_PAYLOAD_MAGIC: &[u8; 9] = b"DDBSTATE1";
const MANIFEST_PAYLOAD_MAGIC: &[u8; 8] = b"DDBMANF1";
const TABLE_PAYLOAD_MAGIC: &[u8; 8] = b"DDBTBL01";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StoredRow {
    pub(crate) row_id: i64,
    pub(crate) values: Vec<Value>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct TableData {
    pub(crate) rows: Vec<StoredRow>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PersistedTableState {
    pub(crate) pointer: OverflowPointer,
    pub(crate) checksum: u32,
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
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum RuntimeBtreeKey {
    Encoded(Vec<u8>),
    Int64(i64),
}

#[derive(Clone, Debug)]
pub(crate) enum RuntimeBtreeKeys {
    UniqueEncoded(BTreeMap<Vec<u8>, i64>),
    NonUniqueEncoded(BTreeMap<Vec<u8>, Vec<i64>>),
    UniqueInt64(HashMap<i64, i64>),
    NonUniqueInt64(HashMap<i64, Vec<i64>>),
}

impl RuntimeBtreeKeys {
    pub(super) fn row_ids_for_key(&self, key: &RuntimeBtreeKey) -> Vec<i64> {
        match (self, key) {
            (Self::UniqueEncoded(keys), RuntimeBtreeKey::Encoded(key)) => {
                keys.get(key).copied().into_iter().collect()
            }
            (Self::NonUniqueEncoded(keys), RuntimeBtreeKey::Encoded(key)) => {
                keys.get(key).cloned().unwrap_or_default()
            }
            (Self::UniqueInt64(keys), RuntimeBtreeKey::Int64(key)) => {
                keys.get(key).copied().into_iter().collect()
            }
            (Self::NonUniqueInt64(keys), RuntimeBtreeKey::Int64(key)) => {
                keys.get(key).cloned().unwrap_or_default()
            }
            _ => Vec::new(),
        }
    }

    pub(super) fn row_ids_for_value(&self, value: &Value) -> Result<Vec<i64>> {
        match self {
            Self::UniqueEncoded(_) | Self::NonUniqueEncoded(_) => {
                Ok(self.row_ids_for_key(&RuntimeBtreeKey::Encoded(encode_index_key(value)?)))
            }
            Self::UniqueInt64(_) | Self::NonUniqueInt64(_) => match value {
                Value::Int64(value) => Ok(self.row_ids_for_key(&RuntimeBtreeKey::Int64(*value))),
                _ => Ok(Vec::new()),
            },
        }
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

    pub(crate) fn total_row_id_count(&self) -> usize {
        match self {
            Self::UniqueEncoded(keys) => keys.len(),
            Self::NonUniqueEncoded(keys) => keys.values().map(Vec::len).sum(),
            Self::UniqueInt64(keys) => keys.len(),
            Self::NonUniqueInt64(keys) => keys.values().map(Vec::len).sum(),
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

#[derive(Clone, Debug)]
pub(crate) struct EngineRuntime {
    pub(crate) catalog: CatalogState,
    pub(crate) tables: BTreeMap<String, TableData>,
    pub(crate) indexes: BTreeMap<String, RuntimeIndex>,
    pub(crate) persisted_tables: BTreeMap<String, PersistedTableState>,
    pub(crate) dirty_tables: BTreeSet<String>,
    pub(crate) index_state_epoch: u64,
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

impl EngineRuntime {
    #[must_use]
    pub(crate) fn empty(schema_cookie: u32) -> Self {
        Self {
            catalog: CatalogState::empty(schema_cookie),
            tables: BTreeMap::new(),
            indexes: BTreeMap::new(),
            persisted_tables: BTreeMap::new(),
            dirty_tables: BTreeSet::new(),
            index_state_epoch: 0,
        }
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
        let Some(root) = decode_root_header(&root_page)? else {
            return Ok((Self::empty(schema_cookie), snapshot_lsn));
        };
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
        runtime.catalog.schema_cookie = schema_cookie;
        runtime.rebuild_indexes(pager.page_size())?;
        drop(reader);
        Ok((runtime, snapshot_lsn))
    }

    pub(crate) fn persist_to_db(&mut self, db: &crate::db::Db) -> Result<()> {
        let old_root_page = db.read_page(page::CATALOG_ROOT_PAGE_ID)?;
        let old_root = decode_root_header(&old_root_page)?;
        let dirty_tables = if self.persisted_tables.is_empty() {
            self.catalog.tables.keys().cloned().collect::<Vec<_>>()
        } else {
            self.dirty_tables.iter().cloned().collect::<Vec<_>>()
        };
        let mut table_states = self.persisted_tables.clone();
        let mut rewritten_old_heads = Vec::new();

        {
            let mut store = DbTxnPageStore { db };
            for table_name in dirty_tables {
                let Some(_table) = self.catalog.tables.get(&table_name) else {
                    continue;
                };
                let data = self.tables.get(&table_name).ok_or_else(|| {
                    DbError::internal(format!("table data for {table_name} is missing"))
                })?;
                let payload = encode_table_payload(data)?;
                let checksum = crc32c_parts(&[payload.as_slice()]);
                let pointer = if payload.is_empty() {
                    OverflowPointer {
                        head_page_id: 0,
                        logical_len: 0,
                        flags: 0,
                    }
                } else {
                    write_overflow(&mut store, &payload, CompressionMode::Auto)?
                };
                if let Some(previous) = table_states.insert(
                    table_name.clone(),
                    PersistedTableState { pointer, checksum },
                ) {
                    if previous.pointer.head_page_id != 0
                        && previous.pointer.head_page_id != pointer.head_page_id
                    {
                        rewritten_old_heads.push(previous.pointer.head_page_id);
                    }
                }
            }
        }
        table_states.retain(|table_name, _| self.catalog.tables.contains_key(table_name));

        let manifest = encode_manifest_payload(self, &table_states)?;
        let checksum = crc32c_parts(&[manifest.as_slice()]);
        let pointer = if manifest.is_empty() {
            OverflowPointer {
                head_page_id: 0,
                logical_len: 0,
                flags: 0,
            }
        } else {
            let mut store = DbTxnPageStore { db };
            write_overflow(&mut store, &manifest, CompressionMode::Never)?
        };

        let root_page = encode_root_header(
            db.config().page_size,
            RootHeader {
                schema_cookie: self.catalog.schema_cookie,
                payload_checksum: checksum,
                pointer,
            },
        );
        db.write_page(page::CATALOG_ROOT_PAGE_ID, &root_page)?;

        {
            let mut store = DbTxnPageStore { db };
            if let Some(old_root) = old_root {
                if old_root.pointer.head_page_id != 0
                    && old_root.pointer.head_page_id != pointer.head_page_id
                {
                    crate::record::overflow::free_overflow(
                        &mut store,
                        old_root.pointer.head_page_id,
                    )?;
                }
            }
            for old_head in rewritten_old_heads {
                crate::record::overflow::free_overflow(&mut store, old_head)?;
            }
            for (table_name, state) in &self.persisted_tables {
                if self.catalog.tables.contains_key(table_name) {
                    continue;
                }
                if state.pointer.head_page_id != 0 {
                    crate::record::overflow::free_overflow(&mut store, state.pointer.head_page_id)?;
                }
            }
        }

        self.persisted_tables = table_states;
        self.dirty_tables.clear();
        db.set_schema_cookie(self.catalog.schema_cookie)?;
        Ok(())
    }

    pub(super) fn table_schema(&self, name: &str) -> Option<&TableSchema> {
        self.catalog.table(name)
    }

    pub(super) fn table_data(&self, name: &str) -> Option<&TableData> {
        let table_name = self.table_schema(name)?.name.clone();
        self.tables.get(&table_name)
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
        if self.dirty_tables.contains(table_name) {
            return;
        }
        self.dirty_tables.insert(table_name.to_string());
    }

    pub(super) fn mark_all_tables_dirty(&mut self) {
        self.dirty_tables
            .extend(self.catalog.tables.keys().cloned());
    }

    pub(super) fn prepare_insert_index_updates(
        &mut self,
        table_name: &str,
        row: &StoredRow,
        page_size: u32,
    ) -> Result<Vec<PendingIndexInsert>> {
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
                    let Some(key) = compute_index_key(self, &index, &table, row)? else {
                        continue;
                    };
                    updates.push(PendingIndexInsert::Btree {
                        name: index.name.clone(),
                        key,
                        row_id: row.row_id,
                    });
                }
                IndexKind::Trigram => {
                    if !row_satisfies_index_predicate(self, &index, &table, row)? {
                        continue;
                    }
                    let text = compute_index_values(self, &index, &table, row)?
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
            Statement::CreateTable(statement) => {
                self.execute_create_table(statement)?;
                self.rebuild_indexes(page_size)?;
                Ok(QueryResult::with_affected_rows(0))
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
        }
    }

    pub(crate) fn execute_read_statement(
        &self,
        statement: &Statement,
        params: &[Value],
        _page_size: u32,
    ) -> Result<QueryResult> {
        match statement {
            Statement::Query(query) => self
                .evaluate_query(query, params, &BTreeMap::new())
                .map(dataset_to_result),
            Statement::Explain(explain) => {
                let mut lines =
                    planner::plan_statement(&Statement::Explain(explain.clone()), &self.catalog)?
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

    pub(crate) fn evaluate_query(
        &self,
        query: &Query,
        params: &[Value],
        inherited_ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Dataset> {
        let mut ctes = inherited_ctes.clone();
        for cte in &query.ctes {
            let mut dataset = self.evaluate_query(&cte.query, params, &ctes)?;
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

        let mut ctes = inherited_ctes.clone();
        for cte in &query.ctes {
            let mut dataset =
                self.evaluate_query_with_outer(&cte.query, params, &ctes, outer_dataset, outer_row)?;
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
                let mut source =
                    self.build_select_dataset_with_outer(select, params, &ctes, outer_dataset, outer_row)?;
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
            _ => self.evaluate_query_body_with_outer(&query.body, params, &ctes, outer_dataset, outer_row)?,
        };
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
            QueryBody::SetOperation {
                op,
                all,
                left,
                right,
            } => {
                let left =
                    self.evaluate_query_body_with_outer(left, params, ctes, outer_dataset, outer_row)?;
                let right =
                    self.evaluate_query_body_with_outer(right, params, ctes, outer_dataset, outer_row)?;
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

    fn build_select_dataset(
        &self,
        select: &Select,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Dataset> {
        let mut dataset = if let Some(dataset) = self.try_indexed_scan(select, params, ctes)? {
            dataset
        } else if let Some(dataset) = self.try_indexed_join(select, params, ctes)? {
            dataset
        } else if select.from.is_empty() {
            Dataset {
                columns: Vec::new(),
                rows: vec![Vec::new()],
            }
        } else {
            let mut iter = select.from.iter();
            let mut current =
                self.evaluate_from_item(iter.next().expect("first FROM item"), params, ctes)?;
            for item in iter {
                let right = self.evaluate_from_item(item, params, ctes)?;
                current = nested_loop_join(
                    current,
                    right,
                    JoinKind::Inner,
                    &Expr::Literal(Value::Bool(true)),
                    self,
                    params,
                    ctes,
                )?;
            }
            current
        };

        if let Some(filter) = &select.filter {
            let bindings = dataset.columns.clone();
            dataset.rows.retain(|row| {
                let temp = Dataset {
                    columns: bindings.clone(),
                    rows: Vec::new(),
                };
                matches!(
                    self.eval_expr(filter, &temp, row, params, ctes, None),
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
        let mut dataset = if select.from.is_empty() {
            Dataset {
                columns: Vec::new(),
                rows: vec![Vec::new()],
            }
        } else {
            let mut iter = select.from.iter();
            let mut current =
                self.evaluate_from_item(iter.next().expect("first FROM item"), params, ctes)?;
            for item in iter {
                let right = self.evaluate_from_item(item, params, ctes)?;
                current = nested_loop_join(
                    current,
                    right,
                    JoinKind::Inner,
                    &Expr::Literal(Value::Bool(true)),
                    self,
                    params,
                    ctes,
                )?;
            }
            current
        };

        dataset = augment_dataset_with_outer_scope(dataset, outer_dataset, outer_row);
        if let Some(filter) = &select.filter {
            let bindings = dataset.columns.clone();
            dataset.rows.retain(|row| {
                let temp = Dataset {
                    columns: bindings.clone(),
                    rows: Vec::new(),
                };
                matches!(
                    self.eval_expr(filter, &temp, row, params, ctes, None),
                    Ok(Value::Bool(true))
                )
            });
        }
        Ok(dataset)
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
        if ctes.contains_key(name) || self.catalog.view(name).is_some() {
            return Ok(None);
        }
        let table = self
            .table_schema(name)
            .ok_or_else(|| DbError::sql(format!("unknown table or view {name}")))?;
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
                    let row_ids = keys.row_ids_for_value(&value)?;
                    return self
                        .dataset_from_row_ids(table, data, alias, &row_ids)
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
                            .dataset_from_row_ids(table, data, alias, &row_ids)
                            .map(Some);
                    }
                }
            }
        }

        Ok(None)
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
            on,
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
            || self.catalog.views.contains_key(left_name)
            || self.catalog.views.contains_key(right_name)
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
            return self.indexed_inner_join_filtered(
                IndexedJoinPlan {
                    filtered_table: left_binding,
                    filtered_dataset: &left_dataset,
                    filtered_join_column: left_join.column,
                    probe_table: right_binding,
                    probe_join_column: right_join.column,
                    filtered_on_left: true,
                    on,
                },
                params,
                ctes,
            );
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
            return self.indexed_inner_join_filtered(
                IndexedJoinPlan {
                    filtered_table: right_binding,
                    filtered_dataset: &right_dataset,
                    filtered_join_column: right_join.column,
                    probe_table: left_binding,
                    probe_join_column: left_join.column,
                    filtered_on_left: false,
                    on,
                },
                params,
                ctes,
            );
        }

        Ok(None)
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
        let row_ids = keys.row_ids_for_value(&value)?;
        self.dataset_from_row_ids(table, data, alias, &row_ids)
            .map(Some)
    }

    fn indexed_inner_join_filtered(
        &self,
        plan: IndexedJoinPlan<'_>,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Option<Dataset>> {
        let filtered_table = self
            .table_schema(plan.filtered_table.name)
            .ok_or_else(|| {
                DbError::sql(format!(
                    "unknown table or view {}",
                    plan.filtered_table.name
                ))
            })?;
        let probe_table = self
            .table_schema(plan.probe_table.name)
            .ok_or_else(|| {
                DbError::sql(format!("unknown table or view {}", plan.probe_table.name))
            })?;
        let probe_data = self
            .table_data(plan.probe_table.name)
            .cloned()
            .unwrap_or_default();
        let filtered_join_index = filtered_table
            .columns
            .iter()
            .position(|column| identifiers_equal(&column.name, plan.filtered_join_column))
            .ok_or_else(|| DbError::sql(format!("unknown column {}", plan.filtered_join_column)))?;
        let Some(probe_index) = self.catalog.indexes.values().find(|index| {
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
        }) else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Btree { keys }) = self.indexes.get(&probe_index.name) else {
            return Ok(None);
        };

        let probe_columns = probe_table
            .columns
            .iter()
            .map(|column| ColumnBinding {
                table: Some(plan.probe_table.binding_name().to_string()),
                name: column.name.clone(),
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
            let row_ids = keys.row_ids_for_value(join_value)?;
            if row_ids.is_empty() {
                continue;
            }
            let probe_dataset = self.dataset_from_row_ids(
                probe_table,
                &probe_data,
                plan.probe_table.alias,
                &row_ids,
            )?;
            for probe_row in &probe_dataset.rows {
                let mut row = if plan.filtered_on_left {
                    filtered_row.clone()
                } else {
                    probe_row.clone()
                };
                if plan.filtered_on_left {
                    row.extend(probe_row.clone());
                } else {
                    row.extend(filtered_row.clone());
                }
                let dataset = Dataset {
                    columns: columns.clone(),
                    rows: Vec::new(),
                };
                if matches!(
                    self.eval_expr(plan.on, &dataset, &row, params, ctes, None)?,
                    Value::Bool(true)
                ) {
                    rows.push(row);
                }
            }
        }
        Ok(Some(Dataset { columns, rows }))
    }

    fn evaluate_from_item(
        &self,
        item: &FromItem,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
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
                if let Some(view) = self.catalog.view(name) {
                    let view_statement = parse_sql_statement(&view.sql_text)?;
                    let Statement::Query(query) = view_statement else {
                        return Err(DbError::corruption(format!(
                            "view {} does not contain a SELECT statement",
                            view.name
                        )));
                    };
                    let mut dataset = self.evaluate_query(&query, params, ctes)?;
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
                let data = self.table_data(name).cloned().unwrap_or_default();
                Ok(Dataset {
                    columns: table
                        .columns
                        .iter()
                        .map(|column| ColumnBinding {
                            table: Some(alias.clone().unwrap_or_else(|| name.clone())),
                            name: column.name.clone(),
                        })
                        .collect(),
                    rows: data.rows.into_iter().map(|row| row.values).collect(),
                })
            }
            FromItem::Subquery { query, alias } => {
                let mut dataset = self.evaluate_query(query, params, ctes)?;
                for column in &mut dataset.columns {
                    column.table = Some(alias.clone());
                }
                Ok(dataset)
            }
            FromItem::Join {
                left,
                right,
                kind,
                on,
            } => {
                let left = self.evaluate_from_item(left, params, ctes)?;
                let right = self.evaluate_from_item(right, params, ctes)?;
                nested_loop_join(left, right, *kind, on, self, params, ctes)
            }
        }
    }

    fn dataset_from_row_ids(
        &self,
        table: &TableSchema,
        data: &TableData,
        alias: &Option<String>,
        row_ids: &[i64],
    ) -> Result<Dataset> {
        let table_name = alias.clone().unwrap_or_else(|| table.name.clone());
        let rows = row_ids
            .iter()
            .filter_map(|row_id| {
                data.rows
                    .iter()
                    .find(|row| row.row_id == *row_id)
                    .map(|row| row.values.clone())
            })
            .collect::<Vec<_>>();
        Ok(Dataset {
            columns: table
                .columns
                .iter()
                .map(|column| ColumnBinding {
                    table: Some(table_name.clone()),
                    name: column.name.clone(),
                })
                .collect(),
            rows,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct RootHeader {
    schema_cookie: u32,
    payload_checksum: u32,
    pointer: OverflowPointer,
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

    fn read_page(&self, page_id: PageId) -> Result<Vec<u8>> {
        if let Some(page) = self.wal.read_page_at_snapshot(page_id, self.snapshot_lsn)? {
            Ok(page)
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

    fn read_page(&self, page_id: PageId) -> Result<Vec<u8>> {
        self.db.read_page(page_id)
    }

    fn write_page(&mut self, page_id: PageId, data: &[u8]) -> Result<()> {
        self.db.write_page(page_id, data)
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
        FromItem::Subquery { alias, .. } => {
            names.insert(alias.clone());
        }
        FromItem::Join { left, right, .. } => {
            collect_from_item_table_names(left, names);
            collect_from_item_table_names(right, names);
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
                let mut keys = HashMap::<i64, i64>::new();
                for row in &data.rows {
                    let Some(key) = compute_index_key(runtime, index, table, row)? else {
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
                    let Some(key) = compute_index_key(runtime, index, table, row)? else {
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
                let mut keys = HashMap::<i64, Vec<i64>>::new();
                for row in &data.rows {
                    let Some(key) = compute_index_key(runtime, index, table, row)? else {
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
                    let Some(key) = compute_index_key(runtime, index, table, row)? else {
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
                if !row_satisfies_index_predicate(runtime, index, table, row)? {
                    continue;
                }
                if let Value::Text(text) = compute_index_values(runtime, index, table, row)?
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

fn compute_index_key(
    runtime: &EngineRuntime,
    index: &IndexSchema,
    table: &TableSchema,
    row: &StoredRow,
) -> Result<Option<RuntimeBtreeKey>> {
    if !row_satisfies_index_predicate(runtime, index, table, row)? {
        return Ok(None);
    }
    if btree_uses_typed_int64_keys(index, table) {
        let [column] = index.columns.as_slice() else {
            return Err(DbError::internal(
                "typed INT64 runtime indexes require exactly one indexed column",
            ));
        };
        if let Some(column_name) = &column.column_name {
            let position = column_position(table, column_name)
                .ok_or_else(|| {
                    DbError::constraint(format!("index column {} does not exist", column_name))
                })?;
            let Value::Int64(value) = row
                .values
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
    let values = compute_index_values(runtime, index, table, row)?;
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
    column_schema(table, column_name)
        .is_some_and(|column| {
            column.column_type == crate::catalog::ColumnType::Int64 && !column.nullable
        })
}

fn compute_index_values(
    runtime: &EngineRuntime,
    index: &IndexSchema,
    table: &TableSchema,
    row: &StoredRow,
) -> Result<Vec<Value>> {
    let dataset = table_row_dataset(table, &row.values, &table.name);
    let bindings = dataset.rows.first().map(Vec::as_slice).unwrap_or(&[]);
    index
        .columns
        .iter()
        .map(|column| {
            if let Some(column_name) = &column.column_name {
                let position = column_position(table, column_name)
                    .ok_or_else(|| {
                        DbError::constraint(format!("index column {} does not exist", column_name))
                    })?;
                Ok(row.values[position].clone())
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
    row: &StoredRow,
) -> Result<bool> {
    let Some(predicate_sql) = &index.predicate_sql else {
        return Ok(true);
    };
    let expr = crate::sql::parser::parse_expression_sql(predicate_sql)?;
    let dataset = table_row_dataset(table, &row.values, &table.name);
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
            .map(|column| ColumnBinding {
                table: Some(table_name.to_string()),
                name: column.name.clone(),
            })
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
    Ok(output)
}

fn decode_runtime_payload(bytes: &[u8]) -> Result<EngineRuntime> {
    let mut cursor = Cursor::new(bytes);
    let magic = cursor.read_bytes(9)?;
    if magic.as_slice() != LEGACY_RUNTIME_PAYLOAD_MAGIC {
        return Err(DbError::corruption("catalog state magic is invalid"));
    }
    let mut runtime = EngineRuntime::empty(cursor.read_u32()?);
    let table_count = cursor.read_u32()?;
    for _ in 0..table_count {
        let table_name = cursor.read_string()?;
        let column_count = cursor.read_u32()?;
        let mut table = TableSchema {
            name: table_name.clone(),
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
            let row = Row::decode(&cursor.read_vec()?)?;
            data.rows.push(StoredRow {
                row_id,
                values: row.values().to_vec(),
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
                predicate_sql,
                fresh,
            },
        );
    }

    let view_count = cursor.read_u32()?;
    for _ in 0..view_count {
        let view = crate::catalog::ViewSchema {
            name: cursor.read_string()?,
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
    Ok(runtime)
}

fn encode_manifest_payload(
    runtime: &EngineRuntime,
    table_states: &BTreeMap<String, PersistedTableState>,
) -> Result<Vec<u8>> {
    let mut output = Vec::new();
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
        encode_i64(&mut output, table.next_row_id);
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
    Ok(output)
}

fn decode_manifest_payload<S: PageStore>(store: &S, bytes: &[u8]) -> Result<EngineRuntime> {
    let mut cursor = Cursor::new(bytes);
    let magic = cursor.read_bytes(MANIFEST_PAYLOAD_MAGIC.len())?;
    if magic.as_slice() != MANIFEST_PAYLOAD_MAGIC {
        return Err(DbError::corruption("catalog manifest magic is invalid"));
    }
    let mut runtime = EngineRuntime::empty(cursor.read_u32()?);
    let table_count = cursor.read_u32()?;
    for _ in 0..table_count {
        let table_name = cursor.read_string()?;
        let column_count = cursor.read_u32()?;
        let mut table = TableSchema {
            name: table_name.clone(),
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
        };
        runtime.catalog.tables.insert(table_name.clone(), table);
        runtime.persisted_tables.insert(table_name.clone(), state);
        let data = if state.pointer.head_page_id == 0 || state.pointer.logical_len == 0 {
            TableData::default()
        } else {
            let payload = read_overflow(store, state.pointer)?;
            if crc32c_parts(&[payload.as_slice()]) != state.checksum {
                return Err(DbError::corruption(format!(
                    "table payload checksum mismatch for {table_name}"
                )));
            }
            decode_table_payload(&payload)?
        };
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
                predicate_sql,
                fresh,
            },
        );
    }

    let view_count = cursor.read_u32()?;
    for _ in 0..view_count {
        let view = crate::catalog::ViewSchema {
            name: cursor.read_string()?,
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

fn decode_table_payload(bytes: &[u8]) -> Result<TableData> {
    if bytes.is_empty() {
        return Ok(TableData::default());
    }
    let mut cursor = Cursor::new(bytes);
    let magic = cursor.read_bytes(TABLE_PAYLOAD_MAGIC.len())?;
    if magic.as_slice() != TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    let row_count = cursor.read_u32()?;
    let mut data = TableData::default();
    for _ in 0..row_count {
        let row_id = cursor.read_i64()?;
        let row = Row::decode(&cursor.read_vec()?)?;
        data.rows.push(StoredRow {
            row_id,
            values: row.values().to_vec(),
        });
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

    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| DbError::corruption("cursor overflow"))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| DbError::corruption("truncated catalog state"))?;
        self.offset = end;
        Ok(bytes.to_vec())
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
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes(
            bytes.as_slice().try_into().expect("u32"),
        ))
    }

    fn read_i64(&mut self) -> Result<i64> {
        let bytes = self.read_bytes(8)?;
        Ok(i64::from_le_bytes(
            bytes.as_slice().try_into().expect("i64"),
        ))
    }

    fn read_string(&mut self) -> Result<String> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_bytes(len)?;
        String::from_utf8(bytes).map_err(|error| {
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

    fn read_vec(&mut self) -> Result<Vec<u8>> {
        let len = self.read_u32()? as usize;
        self.read_bytes(len)
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
    on: &'a Expr,
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
        Expr::Cast { expr, .. } => expr_contains_aggregate(expr),
        Expr::Literal(_) | Expr::Column { .. } | Expr::Parameter(_) => false,
    }
}

fn nested_loop_join(
    left: Dataset,
    right: Dataset,
    kind: JoinKind,
    on: &Expr,
    runtime: &EngineRuntime,
    params: &[Value],
    ctes: &BTreeMap<String, Dataset>,
) -> Result<Dataset> {
    let mut columns = left.columns.clone();
    columns.extend(right.columns.clone());
    let mut rows = Vec::new();
    for left_row in &left.rows {
        let mut matched = false;
        for right_row in &right.rows {
            let mut row = left_row.clone();
            row.extend(right_row.clone());
            let dataset = Dataset {
                columns: columns.clone(),
                rows: Vec::new(),
            };
            if matches!(
                runtime.eval_expr(on, &dataset, &row, params, ctes, None)?,
                Value::Bool(true)
            ) {
                matched = true;
                rows.push(row);
            }
        }
        if !matched && kind == JoinKind::Left {
            let mut row = left_row.clone();
            row.extend(std::iter::repeat_n(Value::Null, right.columns.len()));
            rows.push(row);
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
                let right_keys = right
                    .rows
                    .iter()
                    .map(|row| row_identity(row))
                    .collect::<Result<Vec<_>>>()?;
                let mut rows = left
                    .rows
                    .into_iter()
                    .filter(|row| {
                        row_identity(row)
                            .map(|identity| right_keys.contains(&identity))
                            .unwrap_or(false)
                    })
                    .collect::<Vec<_>>();
                if !all {
                    rows = deduplicate_rows(rows)?;
                }
                rows
            }
            crate::sql::ast::SetOperation::Except => {
                let right_keys = right
                    .rows
                    .iter()
                    .map(|row| row_identity(row))
                    .collect::<Result<Vec<_>>>()?;
                let mut rows = left
                    .rows
                    .into_iter()
                    .filter(|row| {
                        row_identity(row)
                            .map(|identity| !right_keys.contains(&identity))
                            .unwrap_or(false)
                    })
                    .collect::<Vec<_>>();
                if !all {
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
        let window_values = items
            .iter()
            .map(|item| match item {
                SelectItem::Expr {
                    expr:
                        Expr::RowNumber {
                            partition_by,
                            order_by,
                        },
                    ..
                } => self
                    .compute_row_number_values(dataset, partition_by, order_by, params, ctes)
                    .map(Some),
                SelectItem::Expr {
                    expr:
                        Expr::WindowFunction {
                            name,
                            args,
                            partition_by,
                            order_by,
                        },
                    ..
                } => self
                    .compute_window_function_values(
                        dataset,
                        name,
                        args,
                        partition_by,
                        order_by,
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
                SelectItem::Expr { expr, alias } => columns.push(ColumnBinding {
                    table: None,
                    name: alias
                        .clone()
                        .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
                }),
                SelectItem::Wildcard => columns.extend(dataset.columns.clone()),
                SelectItem::QualifiedWildcard(table) => columns.extend(
                    dataset
                        .columns
                        .iter()
                        .filter(|column| column.table.as_deref() == Some(table.as_str()))
                        .cloned(),
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
                    SelectItem::Wildcard => output.extend(row.clone()),
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

    fn compute_window_function_values(
        &self,
        dataset: &Dataset,
        name: &str,
        args: &[Expr],
        partition_by: &[Expr],
        order_by: &[crate::sql::ast::OrderBy],
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

            match name {
                "rank" => {
                    let mut current_rank = 1_i64;
                    for (ordinal, row_index) in sorted.iter().enumerate() {
                        if ordinal > 0
                            && !window_order_keys_equal(&order_keys[ordinal - 1], &order_keys[ordinal])?
                        {
                            current_rank = (ordinal + 1) as i64;
                        }
                        results[*row_index] = Value::Int64(current_rank);
                    }
                }
                "dense_rank" => {
                    let mut current_rank = 1_i64;
                    for (ordinal, row_index) in sorted.iter().enumerate() {
                        if ordinal > 0
                            && !window_order_keys_equal(&order_keys[ordinal - 1], &order_keys[ordinal])?
                        {
                            current_rank += 1;
                        }
                        results[*row_index] = Value::Int64(current_rank);
                    }
                }
                "lag" | "lead" => {
                    if args.is_empty() || args.len() > 3 {
                        return Err(DbError::sql(format!(
                            "{} expects 1 to 3 arguments",
                            name.to_ascii_uppercase()
                        )));
                    }
                    let offset = match args.get(1) {
                        Some(expr) => match self.eval_expr(expr, dataset, &[], params, ctes, None)? {
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
                        },
                        None => 1,
                    };
                    let ordered_values = sorted
                        .iter()
                        .map(|row_index| {
                            self.eval_expr(&args[0], dataset, &dataset.rows[*row_index], params, ctes, None)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    for (ordinal, row_index) in sorted.iter().enumerate() {
                        let target_ordinal = if name == "lag" {
                            ordinal.checked_sub(offset)
                        } else {
                            ordinal.checked_add(offset).filter(|target| *target < sorted.len())
                        };
                        results[*row_index] = if let Some(target_ordinal) = target_ordinal {
                            ordered_values[target_ordinal].clone()
                        } else if let Some(default_expr) = args.get(2) {
                            self.eval_expr(default_expr, dataset, &dataset.rows[*row_index], params, ctes, None)?
                        } else {
                            Value::Null
                        };
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

    fn evaluate_grouped_select(
        &self,
        select: &Select,
        dataset: Dataset,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Dataset> {
        let mut groups = BTreeMap::<Vec<u8>, Vec<Vec<Value>>>::new();
        if dataset.rows.is_empty() && select.group_by.is_empty() {
            groups.insert(Vec::new(), Vec::new());
        } else {
            for row in &dataset.rows {
                let key_values = select
                    .group_by
                    .iter()
                    .map(|expr| self.eval_expr(expr, &dataset, row, params, ctes, None))
                    .collect::<Result<Vec<_>>>()?;
                groups
                    .entry(row_identity(&key_values)?)
                    .or_default()
                    .push(row.clone());
            }
        }
        let columns = select
            .projection
            .iter()
            .enumerate()
            .map(|(index, item)| match item {
                SelectItem::Expr { expr, alias } => ColumnBinding {
                    table: None,
                    name: alias
                        .clone()
                        .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
                },
                SelectItem::Wildcard => ColumnBinding {
                    table: None,
                    name: format!("col{}", index + 1),
                },
                SelectItem::QualifiedWildcard(_) => ColumnBinding {
                    table: None,
                    name: format!("col{}", index + 1),
                },
            })
            .collect::<Vec<_>>();
        let mut rows = Vec::new();
        for group_rows in groups.into_values() {
            if let Some(having) = &select.having {
                if !matches!(
                    self.eval_group_expr(having, &dataset, &group_rows, params, ctes)?,
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
                        &group_rows,
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
        let bindings = dataset.columns.clone();
        dataset.rows.sort_by(|left, right| {
            for order in order_by {
                let temp = Dataset {
                    columns: bindings.clone(),
                    rows: Vec::new(),
                };
                let left_value = self
                    .eval_expr(&order.expr, &temp, left, params, ctes, None)
                    .unwrap_or(Value::Null);
                let right_value = self
                    .eval_expr(&order.expr, &temp, right, params, ctes, None)
                    .unwrap_or(Value::Null);
                let ordering =
                    compare_values(&left_value, &right_value).unwrap_or(std::cmp::Ordering::Equal);
                if ordering != std::cmp::Ordering::Equal {
                    return if order.descending {
                        ordering.reverse()
                    } else {
                        ordering
                    };
                }
            }
            std::cmp::Ordering::Equal
        });
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

    fn eval_group_expr(
        &self,
        expr: &Expr,
        dataset: &Dataset,
        group_rows: &[Vec<Value>],
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
            } => match name.as_str() {
                "count" => {
                    if *star {
                        Ok(Value::Int64(group_rows.len() as i64))
                    } else if *distinct {
                        let mut vals = Vec::new();
                        for row in group_rows {
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
                        for row in group_rows {
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
                    group_rows,
                    &args[0],
                    NumericAgg::Sum,
                    *distinct,
                ),
                "avg" => aggregate_numeric(
                    &aggregate_ctx,
                    group_rows,
                    &args[0],
                    NumericAgg::Avg,
                    *distinct,
                ),
                "min" => aggregate_extreme(self, dataset, group_rows, &args[0], params, ctes, true),
                "max" => {
                    aggregate_extreme(self, dataset, group_rows, &args[0], params, ctes, false)
                }
                "group_concat" => {
                    aggregate_group_concat(self, dataset, group_rows, args, params, ctes)
                }
                other => Err(DbError::sql(format!(
                    "unsupported aggregate function {other}"
                ))),
            },
            Expr::Unary { op, expr } => {
                let value = self.eval_group_expr(expr, dataset, group_rows, params, ctes)?;
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
                self.eval_group_expr(left, dataset, group_rows, params, ctes)?,
                self.eval_group_expr(right, dataset, group_rows, params, ctes)?,
            ),
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let value = self.eval_group_expr(expr, dataset, group_rows, params, ctes)?;
                let low = self.eval_group_expr(low, dataset, group_rows, params, ctes)?;
                let high = self.eval_group_expr(high, dataset, group_rows, params, ctes)?;
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
                let value = self.eval_group_expr(expr, dataset, group_rows, params, ctes)?;
                if matches!(value, Value::Null) {
                    return Ok(Value::Null);
                }
                let mut saw_null = false;
                for item in items {
                    let item = self.eval_group_expr(item, dataset, group_rows, params, ctes)?;
                    if matches!(item, Value::Null) {
                        saw_null = true;
                        continue;
                    }
                    if compare_values(&value, &item)? == std::cmp::Ordering::Equal {
                        return Ok(Value::Bool(!*negated));
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
                let left = self.eval_group_expr(expr, dataset, group_rows, params, ctes)?;
                let right = self.eval_group_expr(pattern, dataset, group_rows, params, ctes)?;
                let escape = escape
                    .as_ref()
                    .map(|expr| self.eval_group_expr(expr, dataset, group_rows, params, ctes))
                    .transpose()?;
                eval_like(left, right, escape, *case_insensitive, *negated)
            }
            Expr::IsNull { expr, negated } => {
                let is_null = matches!(
                    self.eval_group_expr(expr, dataset, group_rows, params, ctes)?,
                    Value::Null
                );
                Ok(Value::Bool(if *negated { !is_null } else { is_null }))
            }
            Expr::Function { name, args } => {
                let row = group_rows.first().map(Vec::as_slice).unwrap_or(&[]);
                let values = args
                    .iter()
                    .map(|arg| self.eval_group_expr(arg, dataset, group_rows, params, ctes))
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
                    .map(|expr| self.eval_group_expr(expr, dataset, group_rows, params, ctes))
                    .transpose()?;
                for (condition, result) in branches {
                    let matches = if let Some(operand_value) = &operand_value {
                        compare_values(
                            operand_value,
                            &self.eval_group_expr(condition, dataset, group_rows, params, ctes)?,
                        )? == std::cmp::Ordering::Equal
                    } else {
                        matches!(
                            self.eval_group_expr(condition, dataset, group_rows, params, ctes)?,
                            Value::Bool(true)
                        )
                    };
                    if matches {
                        return self.eval_group_expr(result, dataset, group_rows, params, ctes);
                    }
                }
                else_expr
                    .as_deref()
                    .map(|expr| self.eval_group_expr(expr, dataset, group_rows, params, ctes))
                    .transpose()?
                    .map_or(Ok(Value::Null), Ok)
            }
            Expr::Cast { expr, target_type } => cast_value(
                self.eval_group_expr(expr, dataset, group_rows, params, ctes)?,
                *target_type,
            ),
            Expr::RowNumber { .. } | Expr::WindowFunction { .. } => Err(DbError::sql(
                "window functions cannot be nested inside grouped expressions",
            )),
            _ => {
                let row = group_rows.first().map(Vec::as_slice).unwrap_or(&[]);
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
                let value = self.eval_expr(expr, dataset, row, params, ctes, excluded)?;
                if matches!(value, Value::Null) {
                    return Ok(Value::Null);
                }
                let mut saw_null = false;
                for item in items {
                    let item = self.eval_expr(item, dataset, row, params, ctes, excluded)?;
                    if matches!(item, Value::Null) {
                        saw_null = true;
                        continue;
                    }
                    if compare_values(&value, &item)? == std::cmp::Ordering::Equal {
                        return Ok(Value::Bool(!*negated));
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
                let value = self.eval_expr(expr, dataset, row, params, ctes, excluded)?;
                if matches!(value, Value::Null) {
                    return Ok(Value::Null);
                }
                let subquery = self.evaluate_query_with_outer(query, params, ctes, dataset, row)?;
                if subquery.columns.len() != 1 {
                    return Err(DbError::sql("IN subquery must return exactly one column"));
                }
                let mut saw_null = false;
                for subquery_row in &subquery.rows {
                    let candidate = subquery_row.first().cloned().unwrap_or(Value::Null);
                    if matches!(candidate, Value::Null) {
                        saw_null = true;
                        continue;
                    }
                    if compare_values(&value, &candidate)? == std::cmp::Ordering::Equal {
                        return Ok(Value::Bool(!*negated));
                    }
                }
                if saw_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Bool(*negated))
                }
            }
            Expr::ScalarSubquery(query) => {
                let subquery = self.evaluate_query_with_outer(query, params, ctes, dataset, row)?;
                if subquery.columns.len() != 1 {
                    return Err(DbError::sql("scalar subquery must return exactly one column"));
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
            Expr::RowNumber { .. } | Expr::WindowFunction { .. } => {
                Err(DbError::sql("window-function execution is not yet implemented"))
            }
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
        let matches = dataset
            .columns
            .iter()
            .enumerate()
            .filter(|(_, binding)| {
                identifiers_equal(&binding.name, column)
                    && table.is_none_or(|table| {
                        binding
                            .table
                            .as_deref()
                            .is_some_and(|binding_table| identifiers_equal(binding_table, table))
                    })
            })
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [single] => row
                .get(single.0)
                .cloned()
                .ok_or_else(|| DbError::internal("row is shorter than its bindings")),
            [] => Err(DbError::sql(format!("unknown column {column}"))),
            _ => Err(DbError::sql(format!("ambiguous column reference {column}"))),
        }
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
    rows: &[Vec<Value>],
    expr: &Expr,
    kind: NumericAgg,
    distinct: bool,
) -> Result<Value> {
    let mut total_int = 0_i64;
    let mut total_float = 0_f64;
    let mut saw_float = false;
    let mut count = 0_i64;

    let mut vals = Vec::new();
    for row in rows {
        let val = ctx.eval_row(row, expr)?;
        if !matches!(val, Value::Null) {
            vals.push(val);
        }
    }

    if distinct {
        vals.sort_by(|a, b| compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal));
        vals.dedup_by(|a, b| {
            compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal) == std::cmp::Ordering::Equal
        });
    }

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
            other => {
                return Err(DbError::sql(format!(
                    "numeric aggregate does not support {other:?}"
                )))
            }
        }
    }
    if count == 0 {
        return Ok(Value::Null);
    }
    Ok(match kind {
        NumericAgg::Sum if saw_float => Value::Float64(total_float),
        NumericAgg::Sum => Value::Int64(total_int),
        NumericAgg::Avg => Value::Float64(total_float / count as f64),
    })
}

fn aggregate_extreme(
    runtime: &EngineRuntime,
    dataset: &Dataset,
    rows: &[Vec<Value>],
    expr: &Expr,
    params: &[Value],
    ctes: &BTreeMap<String, Dataset>,
    want_min: bool,
) -> Result<Value> {
    let mut current: Option<Value> = None;
    for row in rows {
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
        "lower" => unary_text_fn(values, |value| value.to_ascii_lowercase()),
        "upper" => unary_text_fn(values, |value| value.to_ascii_uppercase()),
        "trim" | "pg_catalog.btrim" => unary_text_fn(values, |value| value.trim().to_string()),
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
        "substr" => {
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
        "json_array_length" => eval_json_array_length(values),
        "json_extract" => eval_json_extract(values),
        other => Err(DbError::sql(format!("unsupported scalar function {other}"))),
    }
}

fn unary_text_fn(values: Vec<Value>, f: impl FnOnce(String) -> String) -> Result<Value> {
    if values.len() != 1 {
        return Err(DbError::sql("function expects one argument"));
    }
    match values.into_iter().next().expect("one arg") {
        Value::Text(value) => Ok(Value::Text(f(value))),
        Value::Null => Ok(Value::Null),
        other => Err(DbError::sql(format!(
            "function expects text, got {other:?}"
        ))),
    }
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
    runtime: &EngineRuntime,
    dataset: &Dataset,
    rows: &[Vec<Value>],
    args: &[Expr],
    params: &[Value],
    ctes: &BTreeMap<String, Dataset>,
) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(DbError::sql("GROUP_CONCAT expects 1 or 2 arguments"));
    }
    let mut parts = Vec::new();
    let mut separator = ",".to_string();
    for row in rows {
        let value = runtime.eval_expr(&args[0], dataset, row, params, ctes, None)?;
        if matches!(value, Value::Null) {
            continue;
        }
        if let Some(separator_expr) = args.get(1) {
            separator = match runtime.eval_expr(separator_expr, dataset, row, params, ctes, None)? {
                Value::Text(value) => value,
                Value::Null => String::new(),
                other => {
                    return Err(DbError::sql(format!(
                        "GROUP_CONCAT separator must be text, got {other:?}"
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
                Ok(Value::Float64((scaled as f64) / 10_f64.powi(i32::from(scale))))
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
            other => Err(DbError::sql(format!("cannot cast {other:?} to TIMESTAMP"))),
        },
    }
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
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
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

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use crate::record::compression::CompressionMode;
    use crate::record::overflow::write_overflow;
    use crate::search::TrigramQueryResult;
    use crate::sql::parser::parse_sql_statement;
    use crate::storage::page::InMemoryPageStore;
    use crate::Value;

    use super::{
        decode_manifest_payload, decode_runtime_payload, encode_manifest_payload,
        encode_runtime_payload, encode_table_payload, EngineRuntime, PersistedTableState,
        RuntimeBtreeKeys, RuntimeIndex,
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
        let pointer = write_overflow(&mut store, &table_payload, CompressionMode::Auto)
            .expect("write table payload");
        let mut table_states = runtime.persisted_tables.clone();
        table_states.insert(
            "docs".to_string(),
            PersistedTableState {
                pointer,
                checksum: crate::storage::checksum::crc32c_parts(&[table_payload.as_slice()]),
            },
        );

        let manifest =
            encode_manifest_payload(&runtime, &table_states).expect("encode manifest payload");
        let decoded = decode_manifest_payload(&store, &manifest).expect("decode manifest payload");

        assert_eq!(decoded.catalog.schema_cookie, runtime.catalog.schema_cookie);
        assert_eq!(decoded.tables["docs"].rows, runtime.tables["docs"].rows);
        assert_eq!(decoded.persisted_tables["docs"], table_states["docs"]);
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
        execute_sql(&mut runtime, "INSERT INTO del_artists VALUES (1, 10), (2, 20)");
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

        let count = parse_sql_statement("SELECT COUNT(*) FROM del_contributors")
            .expect("parse count");
        let result = runtime
            .execute_statement(&count, &[], PAGE_SIZE)
            .expect("execute count");
        assert_eq!(result.rows()[0].values(), &[Value::Int64(1)]);
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
    match (left, right) {
        (Value::Int64(left), Value::Int64(right)) => {
            if matches!(op, BinaryOp::Div) && right == 0 {
                return Ok(Value::Null);
            }
            Ok(match op {
                BinaryOp::Add => Value::Int64(left + right),
                BinaryOp::Sub => Value::Int64(left - right),
                BinaryOp::Mul => Value::Int64(left * right),
                BinaryOp::Div => Value::Int64(left / right),
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
            if matches!(op, BinaryOp::Div) && right == 0.0 {
                return Ok(Value::Null);
            }
            Ok(match op {
                BinaryOp::Add => Value::Float64(left + right),
                BinaryOp::Sub => Value::Float64(left - right),
                BinaryOp::Mul => Value::Float64(left * right),
                BinaryOp::Div => Value::Float64(left / right),
                _ => unreachable!(),
            })
        }
        other => Err(DbError::sql(format!(
            "arithmetic is not defined for {other:?}"
        ))),
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
        (Value::Bool(left), Value::Bool(right)) => Ok(left.cmp(right)),
        (Value::Text(left), Value::Text(right)) => Ok(left.cmp(right)),
        (Value::Blob(left), Value::Blob(right)) => Ok(left.cmp(right)),
        (Value::Uuid(left), Value::Uuid(right)) => Ok(left.cmp(right)),
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
        '%' => (0..=input.len()).any(|offset| like_match_chars(&input[offset..], &pattern[1..], escape)),
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

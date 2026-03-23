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

use std::collections::BTreeMap;

use crate::catalog::{CatalogState, IndexKind, IndexSchema, TableSchema};
use crate::error::{DbError, Result};
use crate::planner;
use crate::record::compression::CompressionMode;
use crate::record::key::encode_index_key;
use crate::record::overflow::{read_overflow, write_overflow, OverflowPointer};
use crate::record::row::Row;
use crate::record::value::Value;
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

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StoredRow {
    pub(crate) row_id: i64,
    pub(crate) values: Vec<Value>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct TableData {
    pub(crate) rows: Vec<StoredRow>,
}

#[derive(Debug)]
pub(crate) enum RuntimeIndex {
    Btree { keys: BTreeMap<Vec<u8>, Vec<i64>> },
    Trigram { index: TrigramIndex },
}

#[derive(Debug)]
pub(crate) struct EngineRuntime {
    pub(crate) catalog: CatalogState,
    pub(crate) tables: BTreeMap<String, TableData>,
    pub(crate) indexes: BTreeMap<String, RuntimeIndex>,
}

impl Clone for EngineRuntime {
    fn clone(&self) -> Self {
        Self {
            catalog: self.catalog.clone(),
            tables: self.tables.clone(),
            indexes: BTreeMap::new(),
        }
    }
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
        }
    }

    pub(crate) fn load_from_storage(
        pager: &PagerHandle,
        wal: &WalHandle,
        schema_cookie: u32,
    ) -> Result<Self> {
        let snapshot_lsn = wal.latest_snapshot();
        let store = SnapshotPageStore {
            pager,
            wal,
            snapshot_lsn,
        };
        let root_page = store.read_page(page::CATALOG_ROOT_PAGE_ID)?;
        let Some(root) = decode_root_header(&root_page)? else {
            return Ok(Self::empty(schema_cookie));
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
        } else {
            decode_runtime_payload(&payload)?
        };
        runtime.catalog.schema_cookie = schema_cookie;
        runtime.rebuild_indexes(pager.page_size())?;
        Ok(runtime)
    }

    pub(crate) fn persist_to_db(&self, db: &crate::db::Db) -> Result<()> {
        let payload = encode_runtime_payload(self)?;
        let checksum = crc32c_parts(&[payload.as_slice()]);
        let old_root_page = db.read_page(page::CATALOG_ROOT_PAGE_ID)?;
        let old_root = decode_root_header(&old_root_page)?;

        let pointer = if payload.is_empty() {
            OverflowPointer {
                head_page_id: 0,
                logical_len: 0,
                flags: 0,
            }
        } else {
            let mut store = DbTxnPageStore { db };
            write_overflow(&mut store, &payload, CompressionMode::Auto)?
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

        if let Some(old_root) = old_root {
            if old_root.pointer.head_page_id != 0
                && old_root.pointer.head_page_id != pointer.head_page_id
            {
                let mut store = DbTxnPageStore { db };
                crate::record::overflow::free_overflow(&mut store, old_root.pointer.head_page_id)?;
            }
        }

        db.set_schema_cookie(self.catalog.schema_cookie)?;
        Ok(())
    }

    pub(crate) fn rebuild_indexes(&mut self, page_size: u32) -> Result<()> {
        let mut rebuilt = BTreeMap::new();
        for index in self.catalog.indexes.values() {
            rebuilt.insert(
                index.name.clone(),
                build_runtime_index(index, self, page_size)?,
            );
        }
        self.indexes = rebuilt;
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
                self.rebuild_indexes(page_size)?;
                Ok(result)
            }
            Statement::Update(statement) => {
                let result = self.execute_update(statement, params, page_size)?;
                self.rebuild_indexes(page_size)?;
                Ok(result)
            }
            Statement::Delete(statement) => {
                let result = self.execute_delete(statement, params, page_size)?;
                self.rebuild_indexes(page_size)?;
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

        let mut dataset = self.evaluate_query_body(&query.body, params, &ctes)?;
        if !query.order_by.is_empty() {
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

    fn evaluate_select(
        &self,
        select: &Select,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Dataset> {
        let mut dataset = if let Some(dataset) = self.try_indexed_scan(select, params, ctes)? {
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

        if !select.group_by.is_empty() || projection_has_aggregate_items(&select.projection) {
            self.evaluate_grouped_select(select, dataset, params, ctes)
        } else {
            self.project_dataset(&dataset, &select.projection, params, ctes, None)
        }
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
        if ctes.contains_key(name) || self.catalog.views.contains_key(name) {
            return Ok(None);
        }
        let table = self
            .catalog
            .tables
            .get(name)
            .ok_or_else(|| DbError::sql(format!("unknown table or view {name}")))?;
        let Some(data) = self.tables.get(name) else {
            return Ok(None);
        };

        if let Some((column_name, value_expr)) = simple_btree_lookup(filter) {
            if let Some(index) = self.catalog.indexes.values().find(|index| {
                index.table_name == *name
                    && index.fresh
                    && index.kind == IndexKind::Btree
                    && index.columns.len() == 1
                    && index.columns[0].column_name.as_deref() == Some(column_name)
                    && index.columns[0].expression_sql.is_none()
            }) {
                let value =
                    self.eval_expr(value_expr, &Dataset::empty(), &[], params, ctes, None)?;
                let key = encode_index_key(&value)?;
                if let Some(RuntimeIndex::Btree { keys }) = self.indexes.get(&index.name) {
                    let row_ids = keys.get(&key).cloned().unwrap_or_default();
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
                index.table_name == *name
                    && index.fresh
                    && index.kind == IndexKind::Trigram
                    && index.columns.len() == 1
                    && index.columns[0].column_name.as_deref() == Some(column_name)
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
                if let Some(view) = self.catalog.views.get(name) {
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
                    .catalog
                    .tables
                    .get(name)
                    .ok_or_else(|| DbError::sql(format!("unknown table or view {name}")))?;
                let data = self.tables.get(name).cloned().unwrap_or_default();
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

fn build_runtime_index(
    index: &IndexSchema,
    runtime: &EngineRuntime,
    page_size: u32,
) -> Result<RuntimeIndex> {
    let table = runtime
        .catalog
        .tables
        .get(&index.table_name)
        .ok_or_else(|| {
            DbError::corruption(format!(
                "index {} references missing table {}",
                index.name, index.table_name
            ))
        })?;
    let data = runtime.tables.get(&index.table_name).ok_or_else(|| {
        DbError::corruption(format!("table data for {} is missing", index.table_name))
    })?;

    match index.kind {
        IndexKind::Btree => {
            let mut keys = BTreeMap::<Vec<u8>, Vec<i64>>::new();
            for row in &data.rows {
                let Some(key) = compute_index_key(runtime, index, table, row)? else {
                    continue;
                };
                keys.entry(key).or_default().push(row.row_id);
            }
            Ok(RuntimeIndex::Btree { keys })
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
) -> Result<Option<Vec<u8>>> {
    if !row_satisfies_index_predicate(runtime, index, table, row)? {
        return Ok(None);
    }
    let values = compute_index_values(runtime, index, table, row)?;
    let key = if values.len() == 1 {
        encode_index_key(&values[0])?
    } else {
        Row::new(values).encode()?
    };
    Ok(Some(key))
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
                let position = table
                    .columns
                    .iter()
                    .position(|entry| entry.name == *column_name)
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

fn row_satisfies_index_predicate(
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

fn encode_runtime_payload(runtime: &EngineRuntime) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    output.extend_from_slice(b"DDBSTATE1");
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
    if magic.as_slice() != b"DDBSTATE1" {
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

fn simple_btree_lookup(filter: &Expr) -> Option<(&str, &Expr)> {
    match filter {
        Expr::Binary { left, op, right } if *op == BinaryOp::Eq => match (&**left, &**right) {
            (Expr::Column { column, .. }, value)
                if matches!(value, Expr::Literal(_) | Expr::Parameter(_)) =>
            {
                Some((column.as_str(), value))
            }
            (value, Expr::Column { column, .. })
                if matches!(value, Expr::Literal(_) | Expr::Parameter(_)) =>
            {
                Some((column.as_str(), value))
            }
            _ => None,
        },
        _ => None,
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
        Expr::Exists(_) => false,
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
        Expr::RowNumber { .. } => false,
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
        let row_number_values = items
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
                        Expr::RowNumber { .. } => output.push(
                            row_number_values[item_index]
                                .as_ref()
                                .and_then(|values| values.get(row_index))
                                .cloned()
                                .ok_or_else(|| {
                                    DbError::internal("ROW_NUMBER() values were not precomputed")
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
        match expr {
            Expr::Aggregate { name, args, star } => match name.as_str() {
                "count" => {
                    if *star {
                        Ok(Value::Int64(group_rows.len() as i64))
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
                    self,
                    dataset,
                    group_rows,
                    &args[0],
                    params,
                    ctes,
                    NumericAgg::Sum,
                ),
                "avg" => aggregate_numeric(
                    self,
                    dataset,
                    group_rows,
                    &args[0],
                    params,
                    ctes,
                    NumericAgg::Avg,
                ),
                "min" => aggregate_extreme(self, dataset, group_rows, &args[0], params, ctes, true),
                "max" => {
                    aggregate_extreme(self, dataset, group_rows, &args[0], params, ctes, false)
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
                case_insensitive,
                negated,
                ..
            } => {
                let left = self.eval_group_expr(expr, dataset, group_rows, params, ctes)?;
                let right = self.eval_group_expr(pattern, dataset, group_rows, params, ctes)?;
                match (left, right) {
                    (Value::Text(left), Value::Text(right)) => {
                        let matches = like_match(&left, &right, *case_insensitive);
                        Ok(Value::Bool(if *negated { !matches } else { matches }))
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    other => Err(DbError::sql(format!(
                        "LIKE expects text values, got {other:?}"
                    ))),
                }
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
            Expr::RowNumber { .. } => Err(DbError::sql(
                "ROW_NUMBER() cannot be nested inside grouped expressions",
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
            Expr::Exists(query) => Ok(Value::Bool(
                !self.evaluate_query(query, params, ctes)?.rows.is_empty(),
            )),
            Expr::Like {
                expr,
                pattern,
                case_insensitive,
                negated,
                ..
            } => {
                let left = self.eval_expr(expr, dataset, row, params, ctes, excluded)?;
                let right = self.eval_expr(pattern, dataset, row, params, ctes, excluded)?;
                match (left, right) {
                    (Value::Text(left), Value::Text(right)) => {
                        let matches = like_match(&left, &right, *case_insensitive);
                        Ok(Value::Bool(if *negated { !matches } else { matches }))
                    }
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    other => Err(DbError::sql(format!(
                        "LIKE expects text values, got {other:?}"
                    ))),
                }
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
            Expr::RowNumber { .. } => {
                Err(DbError::sql("ROW_NUMBER execution is not yet implemented"))
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
            if table_name == "excluded" {
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
                binding.name == column
                    && table.is_none_or(|table| binding.table.as_deref() == Some(table))
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
        _ => format!("col{ordinal}"),
    }
}

enum NumericAgg {
    Sum,
    Avg,
}

fn aggregate_numeric(
    runtime: &EngineRuntime,
    dataset: &Dataset,
    rows: &[Vec<Value>],
    expr: &Expr,
    params: &[Value],
    ctes: &BTreeMap<String, Dataset>,
    kind: NumericAgg,
) -> Result<Value> {
    let mut total_int = 0_i64;
    let mut total_float = 0_f64;
    let mut saw_float = false;
    let mut count = 0_i64;
    for row in rows {
        match runtime.eval_expr(expr, dataset, row, params, ctes, None)? {
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
        "trim" => unary_text_fn(values, |value| value.trim().to_string()),
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

fn cast_value(value: Value, target_type: crate::catalog::ColumnType) -> Result<Value> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    match target_type {
        crate::catalog::ColumnType::Int64 => match value {
            Value::Int64(value) => Ok(Value::Int64(value)),
            Value::Float64(value) => Ok(Value::Int64(value as i64)),
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
        other => Err(DbError::sql(format!(
            "CAST to {} is not yet implemented",
            other.as_str()
        ))),
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

fn arithmetic(op: &BinaryOp, left: Value, right: Value) -> Result<Value> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (left, right) {
        (Value::Int64(left), Value::Int64(right)) => Ok(match op {
            BinaryOp::Add => Value::Int64(left + right),
            BinaryOp::Sub => Value::Int64(left - right),
            BinaryOp::Mul => Value::Int64(left * right),
            BinaryOp::Div => Value::Float64(left as f64 / right as f64),
            _ => unreachable!(),
        }),
        (Value::Int64(left), Value::Float64(right)) => {
            arithmetic(op, Value::Float64(left as f64), Value::Float64(right))
        }
        (Value::Float64(left), Value::Int64(right)) => {
            arithmetic(op, Value::Float64(left), Value::Float64(right as f64))
        }
        (Value::Float64(left), Value::Float64(right)) => Ok(match op {
            BinaryOp::Add => Value::Float64(left + right),
            BinaryOp::Sub => Value::Float64(left - right),
            BinaryOp::Mul => Value::Float64(left * right),
            BinaryOp::Div => Value::Float64(left / right),
            _ => unreachable!(),
        }),
        other => Err(DbError::sql(format!(
            "arithmetic is not defined for {other:?}"
        ))),
    }
}

fn compare_values(left: &Value, right: &Value) -> Result<std::cmp::Ordering> {
    use std::cmp::Ordering;
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
        _ => Err(DbError::sql(format!(
            "cannot compare values {left:?} and {right:?}"
        ))),
    }
}

fn like_match(input: &str, pattern: &str, case_insensitive: bool) -> bool {
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
    like_match_bytes(input.as_bytes(), pattern.as_bytes())
}

fn like_match_bytes(input: &[u8], pattern: &[u8]) -> bool {
    if pattern.is_empty() {
        return input.is_empty();
    }
    match pattern[0] {
        b'%' => (0..=input.len()).any(|offset| like_match_bytes(&input[offset..], &pattern[1..])),
        b'_' => !input.is_empty() && like_match_bytes(&input[1..], &pattern[1..]),
        byte => {
            !input.is_empty() && input[0] == byte && like_match_bytes(&input[1..], &pattern[1..])
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

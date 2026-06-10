#![allow(dead_code)]

use std::cell::RefCell;
use std::sync::Mutex;

use crate::record::value::Value;
use crate::tracing::config::RuntimeTracingConfig;

/// How an index was used in a traced operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexUsageKind {
    Read,
    Write,
}

thread_local! {
    static LOCAL_INDEX_USAGE: RefCell<Vec<(String, String, String, IndexUsageKind)>> = const { RefCell::new(Vec::new()) };
}

/// Push a usage event into the thread-local accumulator.
pub fn record_index_usage_local(
    table_name: &str,
    index_name: &str,
    index_kind: &str,
    kind: IndexUsageKind,
) {
    LOCAL_INDEX_USAGE.with(|buf| {
        buf.borrow_mut().push((
            table_name.to_string(),
            index_name.to_string(),
            index_kind.to_string(),
            kind,
        ));
    });
}

/// Drain the thread-local buffer and return its contents.
pub fn drain_local_index_usage() -> Vec<(String, String, String, IndexUsageKind)> {
    LOCAL_INDEX_USAGE.with(|buf| std::mem::take(&mut *buf.borrow_mut()))
}

/// Per-index usage aggregate maintained by `IndexUsageStore`.
#[derive(Clone, Debug)]
pub struct IndexUsageRow {
    pub table_name: String,
    pub index_name: String,
    pub index_kind: String,
    pub read_count: u64,
    pub write_count: u64,
}

impl IndexUsageRow {
    pub fn to_query_row(&self) -> Vec<Value> {
        vec![
            Value::Text(self.table_name.clone()),
            Value::Text(self.index_name.clone()),
            Value::Text(self.index_kind.clone()),
            Value::Int64(i64::try_from(self.read_count).unwrap_or(-1)),
            Value::Int64(i64::try_from(self.write_count).unwrap_or(-1)),
        ]
    }
}

#[derive(Debug)]
pub(crate) struct IndexUsageStore {
    config: RuntimeTracingConfig,
    rows: Mutex<Vec<IndexUsageRow>>,
}

impl IndexUsageStore {
    pub(crate) fn new(config: &RuntimeTracingConfig) -> Self {
        let capacity = config.index_usage.max_rows.clamp(1, 65_536);
        Self {
            config: config.clone(),
            rows: Mutex::new(Vec::with_capacity(capacity)),
        }
    }

    #[inline]
    pub(crate) fn record(
        &self,
        table_name: &str,
        index_name: &str,
        index_kind: &str,
        kind: IndexUsageKind,
    ) {
        if !self.config.enabled || !self.config.index_usage.enabled {
            return;
        }
        if let Ok(mut rows) = self.rows.lock() {
            if let Some(existing) = rows.iter_mut().find(|r| {
                r.table_name == table_name && r.index_name == index_name
            }) {
                match kind {
                    IndexUsageKind::Read => existing.read_count += 1,
                    IndexUsageKind::Write => existing.write_count += 1,
                }
            } else if rows.len() < self.config.index_usage.max_rows {
                rows.push(IndexUsageRow {
                    table_name: table_name.to_string(),
                    index_name: index_name.to_string(),
                    index_kind: index_kind.to_string(),
                    read_count: match kind {
                        IndexUsageKind::Read => 1,
                        IndexUsageKind::Write => 0,
                    },
                    write_count: match kind {
                        IndexUsageKind::Read => 0,
                        IndexUsageKind::Write => 1,
                    },
                });
            }
        }
    }

    pub(crate) fn snapshot(&self) -> Vec<IndexUsageRow> {
        self.rows
            .lock()
            .map(|rows| rows.clone())
            .unwrap_or_default()
    }

    pub(crate) fn reset(&mut self) {
        if let Ok(mut rows) = self.rows.lock() {
            rows.clear();
        }
    }
}

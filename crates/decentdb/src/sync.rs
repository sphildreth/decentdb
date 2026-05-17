use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::catalog::TableSchema;
use crate::error::{DbError, Result};
use crate::record::value::Value;
use crate::vfs::{self, FileKind, OpenMode, VfsFile, VfsHandle};

const METADATA_TABLE: &str = "__decentdb_sync_metadata";

pub(crate) const METADATA_TABLE_DDL: &str =
    "CREATE TABLE IF NOT EXISTS __decentdb_sync_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)";

pub(crate) fn is_internal_table_name(name: &str) -> bool {
    name.starts_with("__decentdb_")
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncJournalRecord {
    #[serde(rename = "schema_version")]
    pub schema_version: u32,
    pub sequence: u64,
    pub replica_id: String,
    pub transaction_lsn: u64,
    pub table: String,
    pub operation: String,
    pub primary_key: serde_json::Value,
    pub after: Option<serde_json::Value>,
    pub schema_cookie: u32,
    pub committed_at_micros: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct SyncStatus {
    pub enabled: bool,
    pub replica_id: Option<String>,
    pub next_sequence: u64,
    pub journal_path: Option<String>,
    pub journal_size_bytes: u64,
}

pub(crate) struct SyncMutation {
    pub table: String,
    pub operation: SyncOperation,
    pub primary_key: serde_json::Value,
    pub after: Option<serde_json::Value>,
    pub schema_cookie: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SyncOperation {
    Insert,
    Update,
    Delete,
}

impl SyncOperation {
    fn as_str(&self) -> &'static str {
        match self {
            SyncOperation::Insert => "insert",
            SyncOperation::Update => "update",
            SyncOperation::Delete => "delete",
        }
    }
}

pub(crate) struct SyncContext {
    enabled: AtomicBool,
    replica_id: Mutex<Option<String>>,
    next_sequence: AtomicU64,
    journal_file: Mutex<Option<Arc<dyn VfsFile>>>,
    journal_write_offset: Mutex<u64>,
    journal_path: PathBuf,
    pub(crate) pending_mutations: Mutex<Vec<SyncMutation>>,
}

impl SyncContext {
    pub(crate) fn new(db_path: &Path) -> Self {
        let journal_path = journal_path_for(db_path);
        Self {
            enabled: AtomicBool::new(false),
            replica_id: Mutex::new(None),
            next_sequence: AtomicU64::new(1),
            journal_file: Mutex::new(None),
            journal_write_offset: Mutex::new(0),
            journal_path,
            pending_mutations: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    pub(crate) fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Release);
        if !enabled {
            if let Ok(mut guard) = self.journal_file.lock() {
                *guard = None;
            }
            if let Ok(mut off) = self.journal_write_offset.lock() {
                *off = 0;
            }
        }
    }

    pub(crate) fn ensure_journal_open(&self, vfs: &VfsHandle) -> Result<()> {
        let mut guard = self
            .journal_file
            .lock()
            .map_err(|_| DbError::internal("sync journal lock poisoned"))?;
        if guard.is_some() {
            return Ok(());
        }
        let file = vfs.open(&self.journal_path, OpenMode::OpenOrCreate, FileKind::SyncJournal)?;
        let file_size = file.file_size()?;
        if let Ok(mut off) = self.journal_write_offset.lock() {
            *off = file_size;
        }
        *guard = Some(file);
        Ok(())
    }

    pub(crate) fn set_replica_id(&self, replica_id: &str) {
        if let Ok(mut guard) = self.replica_id.lock() {
            *guard = Some(replica_id.to_string());
        }
    }

    pub(crate) fn replica_id(&self) -> Option<String> {
        self.replica_id.lock().ok()?.clone()
    }

    pub(crate) fn next_sequence(&self) -> u64 {
        self.next_sequence.load(Ordering::Acquire)
    }

    pub(crate) fn set_next_sequence(&self, seq: u64) {
        self.next_sequence.store(seq, Ordering::Release);
    }

    pub(crate) fn journal_path(&self) -> &Path {
        &self.journal_path
    }

    pub(crate) fn journal_size_bytes(&self) -> u64 {
        *self.journal_write_offset.lock().unwrap_or(std::sync::MutexGuard::new(&mut 0))
    }

    pub(crate) fn flush_journal(&self, vfs: &VfsHandle, transaction_lsn: u64) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }
        let replica_id = match self.replica_id() {
            Some(id) => id,
            None => return Ok(()),
        };
        let mutations: Vec<SyncMutation> = {
            let mut guard = self
                .pending_mutations
                .lock()
                .map_err(|_| DbError::internal("sync pending mutations lock poisoned"))?;
            std::mem::take(&mut *guard)
        };
        if mutations.is_empty() {
            return Ok(());
        }

        self.ensure_journal_open(vfs)?;
        let now_micros = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);

        let mut buffer = Vec::new();
        let mut seq = self.next_sequence();

        for mutation in &mutations {
            let record = SyncJournalRecord {
                schema_version: 1,
                sequence: seq,
                replica_id: replica_id.clone(),
                transaction_lsn,
                table: mutation.table.clone(),
                operation: mutation.operation.as_str().to_string(),
                primary_key: std::mem::take(&mut mutation.primary_key),
                after: std::mem::take(&mut mutation.after),
                schema_cookie: mutation.schema_cookie,
                committed_at_micros: now_micros,
            };
            serde_json::to_writer(&mut buffer, &record).map_err(|e| {
                DbError::internal(format!("failed to serialize sync journal record: {e}"))
            })?;
            buffer.push(b'\n');
            seq += 1;
        }

        let guard = self
            .journal_file
            .lock()
            .map_err(|_| DbError::internal("sync journal lock poisoned"))?;
        let journal_file = guard
            .as_ref()
            .ok_or_else(|| DbError::internal("sync journal file not open"))?;
        let mut offset = *self
            .journal_write_offset
            .lock()
            .map_err(|_| DbError::internal("sync journal offset lock poisoned"))?;

        vfs::write_all_at(journal_file.as_ref(), offset, &buffer)?;
        journal_file.sync_data()?;

        offset += buffer.len() as u64;
        *self
            .journal_write_offset
            .lock()
            .map_err(|_| DbError::internal("sync journal offset lock poisoned"))? = offset;
        self.set_next_sequence(seq);

        Ok(())
    }
}

fn journal_path_for(db_path: &Path) -> PathBuf {
    let mut name = db_path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "decentdb.sync-journal".to_string());
    name.push_str(".sync-journal");
    if let Some(parent) = db_path.parent() {
        parent.join(&name)
    } else {
        PathBuf::from(name)
    }
}

pub(crate) fn build_after_json(schema: &TableSchema, values: &[Value]) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (col, val) in schema.columns.iter().zip(values) {
        map.insert(col.name.clone(), value_to_json(val));
    }
    serde_json::Value::Object(map)
}

pub(crate) fn build_primary_key_json(schema: &TableSchema, values: &[Value]) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for pk_col in &schema.primary_key_columns {
        if let Some(pos) = schema.columns.iter().position(|c| &c.name == pk_col) {
            if let Some(val) = values.get(pos) {
                map.insert(pk_col.clone(), value_to_json(val));
            }
        }
    }
    serde_json::Value::Object(map)
}

fn value_to_json(val: &Value) -> serde_json::Value {
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Int64(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
        Value::Float64(f) => {
            serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Blob(b) => {
            let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
            serde_json::Value::String(hex)
        }
        Value::Decimal { scaled, scale } => {
            serde_json::Value::String(format!("{scaled}e-{scale}"))
        }
        Value::Uuid(u) => {
            let s = format!(
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                u[0], u[1], u[2], u[3], u[4], u[5], u[6], u[7],
                u[8], u[9], u[10], u[11], u[12], u[13], u[14], u[15]
            );
            serde_json::Value::String(s)
        }
        Value::TimestampMicros(ts) => serde_json::Value::Number(serde_json::Number::from(*ts)),
    }
}

pub(crate) fn read_journal_records(
    journal_path: &Path,
    vfs: &VfsHandle,
    since_seq: u64,
    limit: usize,
) -> Result<Vec<SyncJournalRecord>> {
    let file = vfs.open(journal_path, OpenMode::OpenExisting, FileKind::SyncJournal)?;
    let file_size = file.file_size()?;
    if file_size == 0 {
        return Ok(Vec::new());
    }

    let mut buf = vec![0u8; file_size as usize];
    let mut offset: u64 = 0;
    while (offset as usize) < buf.len() {
        let read = file.read_at(offset, &mut buf[offset as usize..])?;
        if read == 0 {
            break;
        }
        offset += read as u64;
    }
    let content = String::from_utf8_lossy(&buf[..offset as usize]);

    let mut records = Vec::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let record: SyncJournalRecord = serde_json::from_str(trimmed).map_err(|e| {
            DbError::corruption(format!("failed to parse sync journal record: {e}"))
        })?;
        if record.sequence > since_seq {
            records.push(record);
        }
    }
    if records.len() > limit {
        records.truncate(limit);
    }
    Ok(records)
}

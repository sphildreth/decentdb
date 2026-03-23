//! Active-reader tracking for snapshot retention and checkpoint coordination.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use crate::error::{DbError, Result};

#[derive(Clone, Debug, Default)]
pub(crate) struct ReaderRegistry {
    inner: Arc<ReaderRegistryInner>,
}

#[derive(Debug, Default)]
struct ReaderRegistryInner {
    next_id: AtomicU64,
    readers: Mutex<HashMap<u64, ReaderInfo>>,
    warnings: Mutex<Vec<String>>,
}

#[derive(Clone, Debug)]
struct ReaderInfo {
    snapshot_lsn: u64,
    started_at: SystemTime,
}

#[derive(Debug)]
pub(crate) struct ReaderGuard {
    inner: Arc<ReaderRegistryInner>,
    reader_id: u64,
    snapshot_lsn: u64,
}

impl ReaderRegistry {
    pub(crate) fn register(&self, snapshot_lsn: u64) -> Result<ReaderGuard> {
        let reader_id = self.inner.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        self.inner
            .readers
            .lock()
            .map_err(|_| DbError::internal("reader registry lock poisoned"))?
            .insert(
                reader_id,
                ReaderInfo {
                    snapshot_lsn,
                    started_at: SystemTime::now(),
                },
            );
        Ok(ReaderGuard {
            inner: Arc::clone(&self.inner),
            reader_id,
            snapshot_lsn,
        })
    }

    pub(crate) fn active_reader_count(&self) -> Result<usize> {
        self.inner
            .readers
            .lock()
            .map(|readers| readers.len())
            .map_err(|_| DbError::internal("reader registry lock poisoned"))
    }

    pub(crate) fn min_snapshot_lsn(&self) -> Result<Option<u64>> {
        self.inner
            .readers
            .lock()
            .map(|readers| readers.values().map(|info| info.snapshot_lsn).min())
            .map_err(|_| DbError::internal("reader registry lock poisoned"))
    }

    pub(crate) fn capture_long_reader_warnings(&self, timeout_sec: u64) -> Result<Vec<String>> {
        let now = SystemTime::now();
        let threshold = Duration::from_secs(timeout_sec);
        let readers = self
            .inner
            .readers
            .lock()
            .map_err(|_| DbError::internal("reader registry lock poisoned"))?;
        let mut warnings = Vec::new();
        for (reader_id, reader) in readers.iter() {
            let age = now
                .duration_since(reader.started_at)
                .unwrap_or_else(|_| Duration::from_secs(0));
            if age >= threshold {
                warnings.push(format!(
                    "reader {reader_id} has held snapshot {} for {}s",
                    reader.snapshot_lsn,
                    age.as_secs()
                ));
            }
        }
        drop(readers);
        if !warnings.is_empty() {
            self.inner
                .warnings
                .lock()
                .map_err(|_| DbError::internal("reader warning log poisoned"))?
                .extend(warnings.clone());
        }
        Ok(warnings)
    }

    pub(crate) fn warnings(&self) -> Result<Vec<String>> {
        self.inner
            .warnings
            .lock()
            .map(|warnings| warnings.clone())
            .map_err(|_| DbError::internal("reader warning log poisoned"))
    }
}

impl ReaderGuard {
    #[must_use]
    pub(crate) fn id(&self) -> u64 {
        self.reader_id
    }

    #[must_use]
    pub(crate) fn snapshot_lsn(&self) -> u64 {
        self.snapshot_lsn
    }
}

impl Drop for ReaderGuard {
    fn drop(&mut self) {
        if let Ok(mut readers) = self.inner.readers.lock() {
            readers.remove(&self.reader_id);
        }
    }
}

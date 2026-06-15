//! Active-reader tracking for snapshot retention and checkpoint coordination.
//!
//! Implements:
//! - design/adr/0018-checkpointing-reader-count-mechanism.md
//! - design/adr/0019-wal-retention-for-active-readers.md
//! - design/adr/0024-wal-growth-prevention-long-readers.md

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use std::time::Instant;

use crate::error::{DbError, Result};
use crate::wal::coordination::ProcessReaderGuard;

#[derive(Clone, Debug, Default)]
pub(crate) struct ReaderRegistry {
    inner: Arc<ReaderRegistryInner>,
}

#[derive(Debug, Default)]
struct ReaderRegistryInner {
    next_id: AtomicU64,
    active_count: AtomicU64,
    readers: Mutex<ReaderSlots>,
    warnings: Mutex<Vec<String>>,
}

#[derive(Debug, Default)]
struct ReaderSlots {
    slots: Vec<Option<ReaderInfo>>,
    free: Vec<usize>,
}

#[derive(Clone, Debug)]
struct ReaderInfo {
    reader_id: u64,
    snapshot_lsn: u64,
    started_at: ReaderStartedAt,
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
type ReaderStartedAt = Instant;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
type ReaderStartedAt = ();

#[derive(Debug)]
pub(crate) struct ReaderGuard {
    inner: Arc<ReaderRegistryInner>,
    reader_id: u64,
    slot_index: usize,
    snapshot_lsn: u64,
    _process_guard: Option<ProcessReaderGuard>,
}

impl ReaderRegistry {
    #[cfg(test)]
    pub(crate) fn register(&self, snapshot_lsn: u64) -> Result<ReaderGuard> {
        self.register_with_process_guard(snapshot_lsn, None)
    }

    pub(crate) fn next_reader_id(&self) -> u64 {
        self.inner.next_id.load(Ordering::Relaxed) + 1
    }

    pub(crate) fn register_with_process_guard(
        &self,
        snapshot_lsn: u64,
        process_guard: Option<ProcessReaderGuard>,
    ) -> Result<ReaderGuard> {
        let reader_id = self.inner.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let mut readers = self
            .inner
            .readers
            .lock()
            .map_err(|_| DbError::internal("reader registry lock poisoned"))?;
        let info = ReaderInfo {
            reader_id,
            snapshot_lsn,
            started_at: reader_started_at(),
        };
        let slot_index = if let Some(slot_index) = readers.free.pop() {
            readers.slots[slot_index] = Some(info);
            slot_index
        } else {
            readers.slots.push(Some(info));
            readers.slots.len() - 1
        };
        drop(readers);
        self.inner.active_count.fetch_add(1, Ordering::Release);
        Ok(ReaderGuard {
            inner: Arc::clone(&self.inner),
            reader_id,
            slot_index,
            snapshot_lsn,
            _process_guard: process_guard,
        })
    }

    pub(crate) fn active_reader_count(&self) -> Result<usize> {
        let count = self.inner.active_count.load(Ordering::Acquire);
        usize::try_from(count).map_err(|_| DbError::internal("reader count overflowed usize"))
    }

    pub(crate) fn min_snapshot_lsn(&self) -> Result<Option<u64>> {
        self.inner
            .readers
            .lock()
            .map(|readers| {
                readers
                    .slots
                    .iter()
                    .filter_map(|slot| slot.as_ref().map(|info| info.snapshot_lsn))
                    .min()
            })
            .map_err(|_| DbError::internal("reader registry lock poisoned"))
    }

    pub(crate) fn capture_long_reader_warnings(&self, timeout_sec: u64) -> Result<Vec<String>> {
        let threshold = Duration::from_secs(timeout_sec);
        let readers = self
            .inner
            .readers
            .lock()
            .map_err(|_| DbError::internal("reader registry lock poisoned"))?;
        let mut warnings = Vec::new();
        for reader in readers.slots.iter().filter_map(Option::as_ref) {
            if let Some(age) = reader_age(&reader.started_at).filter(|age| *age >= threshold) {
                warnings.push(format!(
                    "reader {} has held snapshot {} for {}s",
                    reader.reader_id,
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

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn reader_started_at() -> ReaderStartedAt {
    Instant::now()
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn reader_started_at() -> ReaderStartedAt {}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn reader_age(started_at: &ReaderStartedAt) -> Option<Duration> {
    Some(started_at.elapsed())
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn reader_age(_started_at: &ReaderStartedAt) -> Option<Duration> {
    None
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
            let removed = if let Some(slot) = readers.slots.get_mut(self.slot_index) {
                if slot
                    .as_ref()
                    .is_some_and(|info| info.reader_id == self.reader_id)
                {
                    *slot = None;
                    true
                } else {
                    false
                }
            } else {
                false
            };
            if removed {
                readers.free.push(self.slot_index);
                self.inner.active_count.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }
}

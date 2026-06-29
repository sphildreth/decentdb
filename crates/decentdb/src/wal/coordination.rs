//! Cross-process WAL coordination sidecar.
//!
//! Implements the first native-file slice of
//! `design/_archive/WIN_CROSS_PROCESS_WAL_COORDINATION_SPEC.md`.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

use crate::config::ProcessCoordinationMode;
use crate::error::{DbError, Result};
use crate::storage::checksum;
use crate::storage::DatabaseHeader;
use crate::vfs::{
    lock_range_with_timeout, read_exact_at, write_all_at, FileKind, OpenMode, VfsFile, VfsFileLock,
    VfsHandle,
};

pub(crate) const COORDINATION_SIDECAR_VERSION: u16 = 1;
pub(crate) const READER_SLOT_COUNT: u16 = 64;

const MAGIC: &[u8; 8] = b"DDBCRD01";
const HEADER_LEN: u64 = 256;
const HEADER_CHECKSUM_OFFSET: usize = HEADER_LEN as usize - 4;
const READER_SLOT_LEN: u64 = 128;
const READER_SLOT_CHECKSUM_OFFSET: usize = READER_SLOT_LEN as usize - 4;

const INIT_LOCK_OFFSET: u64 = 0;
const WRITER_LOCK_OFFSET: u64 = 1;
const META_LOCK_OFFSET: u64 = 2;
const READER_LOCK_BASE: u64 = 4096;

const READER_STATE_EMPTY: u8 = 0;
const READER_STATE_ACTIVE: u8 = 1;

thread_local! {
    static HELD_WRITER_LOCKS: RefCell<HashMap<PathBuf, usize>> = RefCell::new(HashMap::new());
}

#[derive(Clone, Debug)]
pub(crate) struct ProcessCoordinator {
    inner: Arc<ProcessCoordinatorInner>,
}

struct ProcessCoordinatorInner {
    file: Arc<dyn VfsFile>,
    coord_path: PathBuf,
    mode: ProcessCoordinationMode,
    timeout: Option<Duration>,
    process_id: u64,
    process_token: u64,
    database_id: [u8; 16],
    db_format_version: u32,
    page_size: u32,
    fingerprint: [u8; 32],
    metrics: ProcessCoordinationMetrics,
    #[allow(clippy::type_complexity)]
    lock_wait_callback: Mutex<Option<Arc<dyn Fn(bool, Duration, &str) + Send + Sync>>>,
}

impl std::fmt::Debug for ProcessCoordinatorInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessCoordinatorInner")
            .field("coord_path", &self.coord_path)
            .field("mode", &self.mode)
            .field("timeout", &self.timeout)
            .field("process_id", &self.process_id)
            .field("process_token", &self.process_token)
            .field("database_id", &self.database_id)
            .field("db_format_version", &self.db_format_version)
            .field("page_size", &self.page_size)
            .field("fingerprint", &self.fingerprint)
            .field("metrics", &self.metrics)
            .field("lock_wait_callback", &"...")
            .finish()
    }
}

#[derive(Debug, Default)]
struct ProcessCoordinationMetrics {
    writer_lock_waits: AtomicU64,
    writer_lock_timeouts: AtomicU64,
    checkpoint_lock_waits: AtomicU64,
    checkpoint_lock_timeouts: AtomicU64,
    reader_slot_allocations: AtomicU64,
    reader_slot_reclaims: AtomicU64,
    wal_refreshes: AtomicU64,
    wal_refresh_failures: AtomicU64,
    last_refresh_unix_ms: AtomicU64,
    current_writer_pid: AtomicU64,
    current_writer_lock_started_ms: AtomicU64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CoordinationHeaderSnapshot {
    pub(crate) version: u16,
    pub(crate) slot_count: u16,
    pub(crate) database_id: [u8; 16],
    pub(crate) db_format_version: u32,
    pub(crate) page_size: u32,
    pub(crate) fingerprint: [u8; 32],
    pub(crate) coordinator_generation: u64,
    pub(crate) wal_generation: u64,
    pub(crate) wal_end_lsn: u64,
    pub(crate) checkpoint_generation: u64,
    pub(crate) checkpoint_lsn: u64,
    pub(crate) writer_owner_pid: u64,
    pub(crate) writer_owner_token: u64,
    pub(crate) writer_owner_started_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProcessCoordinationSnapshot {
    pub(crate) mode: ProcessCoordinationMode,
    pub(crate) enabled: bool,
    pub(crate) supported: bool,
    pub(crate) coord_path: Option<PathBuf>,
    pub(crate) coord_version: u16,
    pub(crate) coordinator_generation: u64,
    pub(crate) wal_end_lsn: u64,
    pub(crate) checkpoint_generation: u64,
    pub(crate) active_reader_slots: u64,
    pub(crate) last_refresh_age_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProcessLockMetricsSnapshot {
    pub(crate) current_writer_pid: Option<u64>,
    pub(crate) current_writer_lock_age_ms: Option<u64>,
    pub(crate) current_checkpoint_pid: Option<u64>,
    pub(crate) current_checkpoint_lock_age_ms: Option<u64>,
    pub(crate) writer_lock_waits: u64,
    pub(crate) writer_lock_timeouts: u64,
    pub(crate) checkpoint_lock_waits: u64,
    pub(crate) checkpoint_lock_timeouts: u64,
    pub(crate) reader_slot_allocations: u64,
    pub(crate) reader_slot_reclaims: u64,
    pub(crate) wal_refreshes: u64,
    pub(crate) wal_refresh_failures: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProcessReaderSlotSnapshot {
    pub(crate) slot_id: u16,
    pub(crate) pid: u64,
    pub(crate) connection_id: String,
    pub(crate) snapshot_lsn: u64,
    pub(crate) age_ms: u64,
    pub(crate) heartbeat_age_ms: u64,
    pub(crate) state: String,
    pub(crate) retention_blocking: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReaderRetentionSnapshot {
    pub(crate) min_snapshot_lsn: Option<u64>,
    pub(crate) active_count: usize,
    pub(crate) truncation_blocked: bool,
}

#[derive(Debug)]
pub(crate) struct ProcessWriterGuard {
    coordinator: ProcessCoordinator,
    owned_lock: bool,
    _lock: Option<Box<dyn VfsFileLock>>,
}

#[derive(Debug)]
pub(crate) struct ProcessReaderGuard {
    coordinator: ProcessCoordinator,
    slot: u16,
    _lock: Option<Box<dyn VfsFileLock>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ReaderSlotKey {
    coord_path: PathBuf,
    slot: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReaderSlotRecord {
    state: u8,
    generation: u64,
    process_id: u64,
    process_token: u64,
    reader_id: u64,
    snapshot_lsn: u64,
    started_unix_ms: u64,
}

impl ProcessCoordinator {
    pub(crate) fn open(
        vfs: &VfsHandle,
        db_path: &Path,
        header: &DatabaseHeader,
        mode: ProcessCoordinationMode,
        timeout_ms: u64,
    ) -> Result<Option<Self>> {
        if mode == ProcessCoordinationMode::SingleProcessUnsafe {
            return Ok(None);
        }
        if vfs.is_memory() {
            if mode == ProcessCoordinationMode::Required {
                return Err(DbError::transaction(
                    "process_coordination=required is not supported for in-memory databases",
                ));
            }
            return Ok(None);
        }
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        {
            if mode == ProcessCoordinationMode::Required {
                return Err(DbError::transaction(
                    "process_coordination=required is not supported by the wasm OPFS runtime",
                ));
            }
            return Ok(None);
        }
        if !vfs.supports_file_locks() {
            return Err(DbError::transaction(format!(
                "process coordination requires native local file locks for {}",
                db_path.display()
            )));
        }
        if header.database_id == [0_u8; 16] {
            return Err(DbError::corruption(
                "database header has an empty coordination identity",
            ));
        }

        let coord_path = coord_path_for_db(db_path);
        let file = vfs.open(&coord_path, OpenMode::OpenOrCreate, FileKind::Coordination)?;
        let coordinator = Self {
            inner: Arc::new(ProcessCoordinatorInner {
                file,
                coord_path,
                mode,
                timeout: Some(Duration::from_millis(timeout_ms)),
                process_id: current_process_id(),
                process_token: random_process_token(),
                database_id: header.database_id,
                db_format_version: header.format_version,
                page_size: header.page_size,
                fingerprint: coordination_fingerprint(
                    &header.database_id,
                    header.format_version,
                    header.page_size,
                ),
                metrics: ProcessCoordinationMetrics::default(),
                lock_wait_callback: Mutex::new(None),
            }),
        };
        coordinator.initialize_or_rebuild()?;
        Ok(Some(coordinator))
    }

    pub(crate) fn snapshot(&self) -> Result<CoordinationHeaderSnapshot> {
        let header = self.read_header()?;
        self.validate_header_identity(&header)?;
        Ok(header)
    }

    pub(crate) fn coordination_snapshot(&self) -> Result<ProcessCoordinationSnapshot> {
        let header = self.snapshot()?;
        let readers = self.scan_reader_retention()?;
        let last_refresh_ms = self
            .inner
            .metrics
            .last_refresh_unix_ms
            .load(Ordering::Acquire);
        let last_refresh_age_ms = if last_refresh_ms == 0 {
            None
        } else {
            Some(now_unix_ms().saturating_sub(last_refresh_ms))
        };
        Ok(ProcessCoordinationSnapshot {
            mode: self.inner.mode,
            enabled: true,
            supported: true,
            coord_path: Some(self.inner.coord_path.clone()),
            coord_version: header.version,
            coordinator_generation: header.coordinator_generation,
            wal_end_lsn: header.wal_end_lsn,
            checkpoint_generation: header.checkpoint_generation,
            active_reader_slots: readers.active_count as u64,
            last_refresh_age_ms,
        })
    }

    pub(crate) fn lock_metrics_snapshot(&self) -> Result<ProcessLockMetricsSnapshot> {
        let header = self.snapshot()?;
        let now = now_unix_ms();
        let current_writer_pid = nonzero_u64(header.writer_owner_pid);
        let current_writer_lock_age_ms =
            nonzero_u64(header.writer_owner_started_ms).map(|started| now.saturating_sub(started));
        let current_local_pid = nonzero_u64(
            self.inner
                .metrics
                .current_writer_pid
                .load(Ordering::Acquire),
        );
        let current_local_age = nonzero_u64(
            self.inner
                .metrics
                .current_writer_lock_started_ms
                .load(Ordering::Acquire),
        )
        .map(|started| now.saturating_sub(started));
        Ok(ProcessLockMetricsSnapshot {
            current_writer_pid: current_writer_pid.or(current_local_pid),
            current_writer_lock_age_ms: current_writer_lock_age_ms.or(current_local_age),
            current_checkpoint_pid: current_writer_pid.or(current_local_pid),
            current_checkpoint_lock_age_ms: current_writer_lock_age_ms.or(current_local_age),
            writer_lock_waits: self.inner.metrics.writer_lock_waits.load(Ordering::Relaxed),
            writer_lock_timeouts: self
                .inner
                .metrics
                .writer_lock_timeouts
                .load(Ordering::Relaxed),
            checkpoint_lock_waits: self
                .inner
                .metrics
                .checkpoint_lock_waits
                .load(Ordering::Relaxed),
            checkpoint_lock_timeouts: self
                .inner
                .metrics
                .checkpoint_lock_timeouts
                .load(Ordering::Relaxed),
            reader_slot_allocations: self
                .inner
                .metrics
                .reader_slot_allocations
                .load(Ordering::Relaxed),
            reader_slot_reclaims: self
                .inner
                .metrics
                .reader_slot_reclaims
                .load(Ordering::Relaxed),
            wal_refreshes: self.inner.metrics.wal_refreshes.load(Ordering::Relaxed),
            wal_refresh_failures: self
                .inner
                .metrics
                .wal_refresh_failures
                .load(Ordering::Relaxed),
        })
    }

    pub(crate) fn mark_refresh_result(&self, result: &Result<()>) {
        match result {
            Ok(()) => {
                self.inner
                    .metrics
                    .wal_refreshes
                    .fetch_add(1, Ordering::Relaxed);
                self.inner
                    .metrics
                    .last_refresh_unix_ms
                    .store(now_unix_ms(), Ordering::Release);
            }
            Err(_) => {
                self.inner
                    .metrics
                    .wal_refresh_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn lock_writer(&self) -> Result<ProcessWriterGuard> {
        self.lock_writer_inner(false)
    }

    pub(crate) fn lock_checkpoint(&self) -> Result<ProcessWriterGuard> {
        self.lock_writer_inner(true)
    }

    #[allow(dead_code)]
    #[allow(clippy::type_complexity)]
    pub(crate) fn set_lock_wait_callback(
        &self,
        callback: Option<Arc<dyn Fn(bool, Duration, &str) + Send + Sync>>,
    ) {
        if let Ok(mut guard) = self.inner.lock_wait_callback.lock() {
            *guard = callback;
        }
    }

    pub(crate) fn begin_reader(
        &self,
        reader_id: u64,
        snapshot_lsn: u64,
    ) -> Result<ProcessReaderGuard> {
        for slot in 0..READER_SLOT_COUNT {
            let Some(lock) = self
                .inner
                .file
                .try_lock_range(reader_lock_offset(slot), 1, true)?
            else {
                continue;
            };
            if self.is_local_active_slot(slot)? {
                continue;
            }
            let record = ReaderSlotRecord {
                state: READER_STATE_ACTIVE,
                generation: now_unix_ms(),
                process_id: self.inner.process_id,
                process_token: self.inner.process_token,
                reader_id,
                snapshot_lsn,
                started_unix_ms: now_unix_ms(),
            };
            self.write_reader_slot(slot, record)?;
            active_reader_slots()
                .lock()
                .map_err(|_| DbError::internal("process reader slot registry poisoned"))?
                .insert(ReaderSlotKey {
                    coord_path: self.inner.coord_path.clone(),
                    slot,
                });
            self.inner
                .metrics
                .reader_slot_allocations
                .fetch_add(1, Ordering::Relaxed);
            return Ok(ProcessReaderGuard {
                coordinator: self.clone(),
                slot,
                _lock: Some(lock),
            });
        }
        Err(DbError::busy(format!(
            "process reader slots exhausted for {} (limit {})",
            self.inner.coord_path.display(),
            READER_SLOT_COUNT
        )))
    }

    pub(crate) fn scan_reader_retention(&self) -> Result<ReaderRetentionSnapshot> {
        let mut min_snapshot_lsn = None::<u64>;
        let mut active_count = 0usize;
        let mut truncation_blocked = false;
        for slot in 0..READER_SLOT_COUNT {
            let record = match self.read_reader_slot(slot) {
                Ok(record) if record.state == READER_STATE_ACTIVE => record,
                Ok(_) => continue,
                Err(_) => {
                    truncation_blocked = true;
                    continue;
                }
            };
            if self.is_local_active_slot(slot)? {
                active_count += 1;
                min_snapshot_lsn = Some(min_snapshot_lsn.map_or(record.snapshot_lsn, |current| {
                    current.min(record.snapshot_lsn)
                }));
                continue;
            }
            match self
                .inner
                .file
                .try_lock_range(reader_lock_offset(slot), 1, true)?
            {
                Some(_stale_lock) => {
                    self.clear_reader_slot(slot)?;
                    self.inner
                        .metrics
                        .reader_slot_reclaims
                        .fetch_add(1, Ordering::Relaxed);
                }
                None => {
                    active_count += 1;
                    min_snapshot_lsn =
                        Some(min_snapshot_lsn.map_or(record.snapshot_lsn, |current| {
                            current.min(record.snapshot_lsn)
                        }));
                }
            }
        }
        Ok(ReaderRetentionSnapshot {
            min_snapshot_lsn,
            active_count,
            truncation_blocked,
        })
    }

    pub(crate) fn reader_slot_snapshots(&self) -> Result<Vec<ProcessReaderSlotSnapshot>> {
        let mut rows = Vec::new();
        let now = now_unix_ms();
        for slot in 0..READER_SLOT_COUNT {
            let record = match self.read_reader_slot(slot) {
                Ok(record) if record.state == READER_STATE_ACTIVE => record,
                Ok(_) => continue,
                Err(_) => {
                    rows.push(ProcessReaderSlotSnapshot {
                        slot_id: slot,
                        pid: 0,
                        connection_id: String::new(),
                        snapshot_lsn: 0,
                        age_ms: 0,
                        heartbeat_age_ms: 0,
                        state: "stale".to_string(),
                        retention_blocking: true,
                    });
                    continue;
                }
            };
            let local_active = self.is_local_active_slot(slot)?;
            let externally_locked = if local_active {
                true
            } else {
                self.inner
                    .file
                    .try_lock_range(reader_lock_offset(slot), 1, true)?
                    .is_none()
            };
            let active = local_active || externally_locked;
            let age_ms = now.saturating_sub(record.started_unix_ms);
            rows.push(ProcessReaderSlotSnapshot {
                slot_id: slot,
                pid: record.process_id,
                connection_id: format!("{}:{:016x}", record.process_id, record.process_token),
                snapshot_lsn: record.snapshot_lsn,
                age_ms,
                heartbeat_age_ms: age_ms,
                state: if active { "active" } else { "stale" }.to_string(),
                retention_blocking: active,
            });
        }
        Ok(rows)
    }

    pub(crate) fn publish_recovered_wal(
        &self,
        wal_end_lsn: u64,
        checkpoint_lsn: u64,
    ) -> Result<CoordinationHeaderSnapshot> {
        let _meta = lock_range_with_timeout(
            self.inner.file.as_ref(),
            META_LOCK_OFFSET,
            1,
            true,
            self.inner.timeout,
        )?;
        let mut header = self.read_header().unwrap_or_else(|_| self.initial_header());
        self.validate_header_identity(&header)?;
        let changed = header.wal_end_lsn != wal_end_lsn || header.checkpoint_lsn != checkpoint_lsn;
        header.wal_end_lsn = wal_end_lsn;
        header.checkpoint_lsn = checkpoint_lsn;
        if changed {
            header.coordinator_generation = header.coordinator_generation.saturating_add(1);
            header.wal_generation = header.wal_generation.saturating_add(1);
        }
        self.write_header(&header)?;
        Ok(header)
    }

    pub(crate) fn publish_commit(&self, wal_end_lsn: u64) -> Result<CoordinationHeaderSnapshot> {
        // The caller holds the process writer lock. That lock is already the
        // cross-process serialization point for committed WAL publication, so
        // taking the metadata byte lock again only adds a syscall to every
        // durable commit.
        let mut header = self.read_header()?;
        self.validate_header_identity(&header)?;
        header.coordinator_generation = header.coordinator_generation.saturating_add(1);
        header.wal_generation = header.wal_generation.saturating_add(1);
        header.wal_end_lsn = wal_end_lsn;
        self.write_header(&header)?;
        Ok(header)
    }

    pub(crate) fn publish_checkpoint(
        &self,
        checkpoint_lsn: u64,
        wal_end_lsn: u64,
    ) -> Result<CoordinationHeaderSnapshot> {
        // The caller holds the process checkpoint lock (same underlying
        // writer byte-range lock), which serializes checkpoint publication.
        let mut header = self.read_header()?;
        self.validate_header_identity(&header)?;
        header.coordinator_generation = header.coordinator_generation.saturating_add(1);
        header.checkpoint_generation = header.checkpoint_generation.saturating_add(1);
        header.checkpoint_lsn = checkpoint_lsn;
        header.wal_end_lsn = wal_end_lsn;
        self.write_header(&header)?;
        Ok(header)
    }

    fn lock_writer_inner(&self, checkpoint: bool) -> Result<ProcessWriterGuard> {
        let key = self.inner.coord_path.clone();
        let already_held = HELD_WRITER_LOCKS.with(|held| {
            let mut held = held.borrow_mut();
            if let Some(count) = held.get_mut(&key) {
                *count += 1;
                true
            } else {
                false
            }
        });
        if already_held {
            return Ok(ProcessWriterGuard {
                coordinator: self.clone(),
                owned_lock: false,
                _lock: None,
            });
        }

        let start = std::time::Instant::now();
        let guard = match lock_range_with_timeout(
            self.inner.file.as_ref(),
            WRITER_LOCK_OFFSET,
            1,
            true,
            self.inner.timeout,
        ) {
            Ok(guard) => guard,
            Err(DbError::Busy { .. }) => {
                let elapsed = start.elapsed();
                self.record_lock_timeout(checkpoint);
                self.maybe_notify_lock_wait_callback(checkpoint, elapsed, "busy");
                return Err(DbError::busy("process writer lock is busy"));
            }
            Err(DbError::Timeout { .. }) => {
                let elapsed = start.elapsed();
                self.record_lock_timeout(checkpoint);
                self.maybe_notify_lock_wait_callback(checkpoint, elapsed, "timeout");
                return Err(DbError::timeout("process writer lock wait timed out"));
            }
            Err(error) => return Err(error),
        };
        let elapsed = start.elapsed();
        self.record_lock_wait(checkpoint);
        self.maybe_notify_lock_wait_callback(checkpoint, elapsed, "ok");
        HELD_WRITER_LOCKS.with(|held| {
            held.borrow_mut().insert(key.clone(), 1);
        });
        if let Err(error) = self.publish_writer_owner_if_required(true) {
            HELD_WRITER_LOCKS.with(|held| {
                held.borrow_mut().remove(&key);
            });
            return Err(error);
        }
        Ok(ProcessWriterGuard {
            coordinator: self.clone(),
            owned_lock: true,
            _lock: Some(guard),
        })
    }

    fn initialize_or_rebuild(&self) -> Result<()> {
        let _init = lock_range_with_timeout(
            self.inner.file.as_ref(),
            INIT_LOCK_OFFSET,
            1,
            true,
            self.inner.timeout,
        )?;
        let min_len = HEADER_LEN + u64::from(READER_SLOT_COUNT) * READER_SLOT_LEN;
        let file_len = self.inner.file.file_size()?;
        let header = self.initial_header();
        if file_len == 0 {
            self.inner.file.set_len(min_len)?;
            self.write_header(&header)?;
            self.clear_all_reader_slots_bulk()?;
            return Ok(());
        }

        let rebuild = match self.read_header() {
            Ok(header) => self.validate_header_identity(&header).is_err(),
            Err(_) => true,
        };
        if rebuild {
            self.inner.file.set_len(min_len)?;
            self.write_header(&header)?;
            self.clear_all_reader_slots_bulk()?;
        } else {
            if file_len < min_len {
                self.inner.file.set_len(min_len)?;
            }
        }
        // The coordination sidecar contains live-process coordination state,
        // not authoritative database content. If a crash loses a create or
        // rebuild here, the next opener reconstructs it from the durable
        // database header and WAL.
        Ok(())
    }

    fn initial_header(&self) -> CoordinationHeaderSnapshot {
        CoordinationHeaderSnapshot {
            version: COORDINATION_SIDECAR_VERSION,
            slot_count: READER_SLOT_COUNT,
            database_id: self.inner.database_id,
            db_format_version: self.inner.db_format_version,
            page_size: self.inner.page_size,
            fingerprint: self.inner.fingerprint,
            coordinator_generation: 1,
            wal_generation: 0,
            wal_end_lsn: 0,
            checkpoint_generation: 0,
            checkpoint_lsn: 0,
            writer_owner_pid: 0,
            writer_owner_token: 0,
            writer_owner_started_ms: 0,
        }
    }

    fn validate_header_identity(&self, header: &CoordinationHeaderSnapshot) -> Result<()> {
        if header.version != COORDINATION_SIDECAR_VERSION {
            return Err(DbError::unsupported_format_version(u32::from(
                header.version,
            )));
        }
        if header.slot_count != READER_SLOT_COUNT
            || header.database_id != self.inner.database_id
            || header.db_format_version != self.inner.db_format_version
            || header.page_size != self.inner.page_size
            || header.fingerprint != self.inner.fingerprint
        {
            return Err(DbError::corruption(format!(
                "coordination sidecar {} does not match database identity",
                self.inner.coord_path.display()
            )));
        }
        Ok(())
    }

    fn read_header(&self) -> Result<CoordinationHeaderSnapshot> {
        let mut bytes = [0_u8; HEADER_LEN as usize];
        read_exact_at(self.inner.file.as_ref(), 0, &mut bytes)?;
        decode_header(&bytes)
    }

    fn write_header(&self, header: &CoordinationHeaderSnapshot) -> Result<()> {
        write_all_at(self.inner.file.as_ref(), 0, &encode_header(header))
    }

    fn read_reader_slot(&self, slot: u16) -> Result<ReaderSlotRecord> {
        let mut bytes = [0_u8; READER_SLOT_LEN as usize];
        read_exact_at(
            self.inner.file.as_ref(),
            reader_record_offset(slot),
            &mut bytes,
        )?;
        decode_reader_slot(&bytes)
    }

    fn write_reader_slot(&self, slot: u16, record: ReaderSlotRecord) -> Result<()> {
        write_all_at(
            self.inner.file.as_ref(),
            reader_record_offset(slot),
            &encode_reader_slot(record),
        )
    }

    fn clear_reader_slot(&self, slot: u16) -> Result<()> {
        self.write_reader_slot(slot, empty_reader_slot_record())
    }

    fn clear_all_reader_slots_bulk(&self) -> Result<()> {
        const SLOT_BYTES: usize = READER_SLOT_COUNT as usize * READER_SLOT_LEN as usize;
        let encoded = encode_reader_slot(empty_reader_slot_record());
        let mut bytes = [0_u8; SLOT_BYTES];
        for chunk in bytes.chunks_exact_mut(READER_SLOT_LEN as usize) {
            chunk.copy_from_slice(&encoded);
        }
        write_all_at(self.inner.file.as_ref(), HEADER_LEN, &bytes)
    }

    fn publish_writer_owner_if_required(&self, active: bool) -> Result<()> {
        self.record_local_writer_owner(active);
        if matches!(self.inner.mode, ProcessCoordinationMode::Required) {
            self.publish_writer_owner(active)
        } else {
            Ok(())
        }
    }

    fn record_local_writer_owner(&self, active: bool) {
        if active {
            self.inner
                .metrics
                .current_writer_pid
                .store(self.inner.process_id, Ordering::Release);
            self.inner
                .metrics
                .current_writer_lock_started_ms
                .store(now_unix_ms(), Ordering::Release);
        } else {
            self.inner
                .metrics
                .current_writer_pid
                .store(0, Ordering::Release);
            self.inner
                .metrics
                .current_writer_lock_started_ms
                .store(0, Ordering::Release);
        }
    }

    fn publish_writer_owner(&self, active: bool) -> Result<()> {
        let _meta = lock_range_with_timeout(
            self.inner.file.as_ref(),
            META_LOCK_OFFSET,
            1,
            true,
            self.inner.timeout,
        )?;
        let mut header = self.read_header()?;
        self.validate_header_identity(&header)?;
        if active {
            let now = now_unix_ms();
            header.writer_owner_pid = self.inner.process_id;
            header.writer_owner_token = self.inner.process_token;
            header.writer_owner_started_ms = now;
            self.inner
                .metrics
                .current_writer_pid
                .store(self.inner.process_id, Ordering::Release);
            self.inner
                .metrics
                .current_writer_lock_started_ms
                .store(now, Ordering::Release);
        } else if header.writer_owner_pid == self.inner.process_id
            && header.writer_owner_token == self.inner.process_token
        {
            header.writer_owner_pid = 0;
            header.writer_owner_token = 0;
            header.writer_owner_started_ms = 0;
            self.inner
                .metrics
                .current_writer_pid
                .store(0, Ordering::Release);
            self.inner
                .metrics
                .current_writer_lock_started_ms
                .store(0, Ordering::Release);
        }
        self.write_header(&header)
    }

    fn record_lock_wait(&self, checkpoint: bool) {
        if checkpoint {
            self.inner
                .metrics
                .checkpoint_lock_waits
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner
                .metrics
                .writer_lock_waits
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_lock_timeout(&self, checkpoint: bool) {
        if checkpoint {
            self.inner
                .metrics
                .checkpoint_lock_timeouts
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner
                .metrics
                .writer_lock_timeouts
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn maybe_notify_lock_wait_callback(
        &self,
        checkpoint: bool,
        elapsed: std::time::Duration,
        status: &str,
    ) {
        if let Ok(callback) = self.inner.lock_wait_callback.lock() {
            if let Some(ref cb) = *callback {
                cb(checkpoint, elapsed, status);
            }
        }
    }

    fn is_local_active_slot(&self, slot: u16) -> Result<bool> {
        active_reader_slots()
            .lock()
            .map(|slots| {
                slots.contains(&ReaderSlotKey {
                    coord_path: self.inner.coord_path.clone(),
                    slot,
                })
            })
            .map_err(|_| DbError::internal("process reader slot registry poisoned"))
    }
}

impl Drop for ProcessWriterGuard {
    fn drop(&mut self) {
        let key = self.coordinator.inner.coord_path.clone();
        let should_clear = HELD_WRITER_LOCKS.with(|held| {
            let mut held = held.borrow_mut();
            let Some(count) = held.get_mut(&key) else {
                return false;
            };
            if *count > 1 {
                *count -= 1;
                false
            } else {
                held.remove(&key);
                self.owned_lock
            }
        });
        if should_clear {
            let _ = self.coordinator.publish_writer_owner_if_required(false);
        }
    }
}

impl Drop for ProcessReaderGuard {
    fn drop(&mut self) {
        if let Ok(mut slots) = active_reader_slots().lock() {
            slots.remove(&ReaderSlotKey {
                coord_path: self.coordinator.inner.coord_path.clone(),
                slot: self.slot,
            });
            if slots.is_empty() {
                slots.shrink_to_fit();
            }
        }
        let _ = self.coordinator.clear_reader_slot(self.slot);
    }
}

fn coord_path_for_db(db_path: &Path) -> PathBuf {
    let mut path = db_path.as_os_str().to_os_string();
    path.push(".coord");
    PathBuf::from(path)
}

fn empty_reader_slot_record() -> ReaderSlotRecord {
    ReaderSlotRecord {
        state: READER_STATE_EMPTY,
        generation: 0,
        process_id: 0,
        process_token: 0,
        reader_id: 0,
        snapshot_lsn: 0,
        started_unix_ms: 0,
    }
}

fn active_reader_slots() -> &'static Mutex<HashSet<ReaderSlotKey>> {
    static ACTIVE: OnceLock<Mutex<HashSet<ReaderSlotKey>>> = OnceLock::new();
    ACTIVE.get_or_init(|| Mutex::new(HashSet::new()))
}

fn coordination_fingerprint(
    database_id: &[u8; 16],
    db_format_version: u32,
    page_size: u32,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"DECENTDB_COORD_ID_V1");
    hasher.update(database_id);
    hasher.update(db_format_version.to_le_bytes());
    hasher.update(page_size.to_le_bytes());
    hasher.finalize().into()
}

fn encode_header(header: &CoordinationHeaderSnapshot) -> [u8; HEADER_LEN as usize] {
    let mut bytes = [0_u8; HEADER_LEN as usize];
    bytes[0..8].copy_from_slice(MAGIC);
    write_u16(&mut bytes, 8, header.version);
    write_u16(&mut bytes, 10, HEADER_LEN as u16);
    write_u16(&mut bytes, 12, header.slot_count);
    write_u16(&mut bytes, 14, READER_SLOT_LEN as u16);
    bytes[16..32].copy_from_slice(&header.database_id);
    write_u32(&mut bytes, 32, header.db_format_version);
    write_u32(&mut bytes, 36, header.page_size);
    bytes[40..72].copy_from_slice(&header.fingerprint);
    write_u64(&mut bytes, 72, header.coordinator_generation);
    write_u64(&mut bytes, 80, header.wal_generation);
    write_u64(&mut bytes, 88, header.wal_end_lsn);
    write_u64(&mut bytes, 96, header.checkpoint_generation);
    write_u64(&mut bytes, 104, header.checkpoint_lsn);
    write_u64(&mut bytes, 112, header.writer_owner_pid);
    write_u64(&mut bytes, 120, header.writer_owner_token);
    write_u64(&mut bytes, 128, header.writer_owner_started_ms);
    let checksum = checksum::crc32c_parts(&[&bytes[..HEADER_CHECKSUM_OFFSET]]);
    write_u32(&mut bytes, HEADER_CHECKSUM_OFFSET, checksum);
    bytes
}

fn decode_header(bytes: &[u8; HEADER_LEN as usize]) -> Result<CoordinationHeaderSnapshot> {
    if &bytes[0..8] != MAGIC {
        return Err(DbError::corruption(
            "invalid process coordination sidecar magic",
        ));
    }
    let stored_checksum = read_u32(bytes, HEADER_CHECKSUM_OFFSET);
    let expected_checksum = checksum::crc32c_parts(&[&bytes[..HEADER_CHECKSUM_OFFSET]]);
    if stored_checksum != expected_checksum {
        return Err(DbError::corruption(
            "process coordination sidecar header checksum mismatch",
        ));
    }
    let version = read_u16(bytes, 8);
    let header_len = read_u16(bytes, 10);
    let slot_count = read_u16(bytes, 12);
    let slot_len = read_u16(bytes, 14);
    if header_len != HEADER_LEN as u16 || slot_len != READER_SLOT_LEN as u16 {
        return Err(DbError::corruption(
            "process coordination sidecar layout is unsupported",
        ));
    }
    Ok(CoordinationHeaderSnapshot {
        version,
        slot_count,
        database_id: read_array::<16>(bytes, 16),
        db_format_version: read_u32(bytes, 32),
        page_size: read_u32(bytes, 36),
        fingerprint: read_array::<32>(bytes, 40),
        coordinator_generation: read_u64(bytes, 72),
        wal_generation: read_u64(bytes, 80),
        wal_end_lsn: read_u64(bytes, 88),
        checkpoint_generation: read_u64(bytes, 96),
        checkpoint_lsn: read_u64(bytes, 104),
        writer_owner_pid: read_u64(bytes, 112),
        writer_owner_token: read_u64(bytes, 120),
        writer_owner_started_ms: read_u64(bytes, 128),
    })
}

fn encode_reader_slot(record: ReaderSlotRecord) -> [u8; READER_SLOT_LEN as usize] {
    let mut bytes = [0_u8; READER_SLOT_LEN as usize];
    bytes[0] = record.state;
    write_u64(&mut bytes, 8, record.generation);
    write_u64(&mut bytes, 16, record.process_id);
    write_u64(&mut bytes, 24, record.process_token);
    write_u64(&mut bytes, 32, record.reader_id);
    write_u64(&mut bytes, 40, record.snapshot_lsn);
    write_u64(&mut bytes, 48, record.started_unix_ms);
    let checksum = checksum::crc32c_parts(&[&bytes[..READER_SLOT_CHECKSUM_OFFSET]]);
    write_u32(&mut bytes, READER_SLOT_CHECKSUM_OFFSET, checksum);
    bytes
}

fn decode_reader_slot(bytes: &[u8; READER_SLOT_LEN as usize]) -> Result<ReaderSlotRecord> {
    let stored_checksum = read_u32(bytes, READER_SLOT_CHECKSUM_OFFSET);
    let expected_checksum = checksum::crc32c_parts(&[&bytes[..READER_SLOT_CHECKSUM_OFFSET]]);
    if stored_checksum != expected_checksum {
        return Err(DbError::corruption("process reader slot checksum mismatch"));
    }
    let state = bytes[0];
    if state != READER_STATE_EMPTY && state != READER_STATE_ACTIVE {
        return Err(DbError::corruption("invalid process reader slot state"));
    }
    Ok(ReaderSlotRecord {
        state,
        generation: read_u64(bytes, 8),
        process_id: read_u64(bytes, 16),
        process_token: read_u64(bytes, 24),
        reader_id: read_u64(bytes, 32),
        snapshot_lsn: read_u64(bytes, 40),
        started_unix_ms: read_u64(bytes, 48),
    })
}

fn reader_record_offset(slot: u16) -> u64 {
    HEADER_LEN + u64::from(slot) * READER_SLOT_LEN
}

fn reader_lock_offset(slot: u16) -> u64 {
    READER_LOCK_BASE + u64::from(slot)
}

fn current_process_id() -> u64 {
    u64::from(std::process::id())
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn random_process_token() -> u64 {
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    {
        let mut bytes = [0_u8; 8];
        if getrandom::fill(&mut bytes).is_ok() {
            let token = u64::from_le_bytes(bytes);
            if token != 0 {
                return token;
            }
        }
    }
    now_unix_ms().max(1)
}

fn nonzero_u64(value: u64) -> Option<u64> {
    if value == 0 {
        None
    } else {
        Some(value)
    }
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> [u8; N] {
    bytes[offset..offset + N]
        .try_into()
        .expect("fixed sidecar slice")
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(read_array::<2>(bytes, offset))
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(read_array::<4>(bytes, offset))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(read_array::<8>(bytes, offset))
}

fn write_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn write_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_header() -> CoordinationHeaderSnapshot {
        CoordinationHeaderSnapshot {
            version: COORDINATION_SIDECAR_VERSION,
            slot_count: READER_SLOT_COUNT,
            database_id: [7; 16],
            db_format_version: 13,
            page_size: 4096,
            fingerprint: coordination_fingerprint(&[7; 16], 13, 4096),
            coordinator_generation: 3,
            wal_generation: 4,
            wal_end_lsn: 1024,
            checkpoint_generation: 5,
            checkpoint_lsn: 512,
            writer_owner_pid: 123,
            writer_owner_token: 456,
            writer_owner_started_ms: 789,
        }
    }

    #[test]
    fn coordination_header_round_trips() {
        let header = sample_header();
        let encoded = encode_header(&header);
        let decoded = decode_header(&encoded).expect("decode header");
        assert_eq!(decoded, header);
    }

    #[test]
    fn coordination_header_checksum_covers_identity() {
        let header = sample_header();
        let mut encoded = encode_header(&header);
        encoded[16] ^= 0x55;
        let error = decode_header(&encoded).expect_err("checksum should fail");
        assert!(matches!(error, DbError::Corruption { .. }));
    }

    #[test]
    fn reader_slot_round_trips() {
        let record = ReaderSlotRecord {
            state: READER_STATE_ACTIVE,
            generation: 11,
            process_id: 22,
            process_token: 33,
            reader_id: 44,
            snapshot_lsn: 55,
            started_unix_ms: 66,
        };
        let encoded = encode_reader_slot(record);
        let decoded = decode_reader_slot(&encoded).expect("decode slot");
        assert_eq!(decoded, record);
    }
}

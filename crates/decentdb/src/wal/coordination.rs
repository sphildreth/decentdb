//! Cross-process WAL coordination sidecar.
//!
//! Implements the first native-file slice of
//! `design/_archive/WIN_CROSS_PROCESS_WAL_COORDINATION_SPEC.md`.

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use sha2::{Digest, Sha256};

use crate::config::ProcessCoordinationMode;
use crate::error::{DbError, Result};
use crate::storage::checksum;
use crate::storage::DatabaseHeader;
use crate::vfs::{
    lock_range_with_timeout, read_exact_at, write_all_at, VfsFile, VfsFileLock, VfsHandle,
};

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use crate::vfs::{FileKind, OpenMode};

pub(crate) const COORDINATION_SIDECAR_VERSION: u16 = 1;
pub(crate) const READER_SLOT_COUNT: u16 = 64;

const MAGIC: &[u8; 8] = b"DDBCRD01";
const HEADER_LEN: u64 = 256;
const HEADER_CHECKSUM_OFFSET: usize = HEADER_LEN as usize - 4;
const READER_SLOT_LEN: u64 = 128;
const READER_SLOT_CHECKSUM_OFFSET: usize = READER_SLOT_LEN as usize - 4;
const READER_SLOTS_BYTES: usize = READER_SLOT_COUNT as usize * READER_SLOT_LEN as usize;

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
const INIT_LOCK_OFFSET: u64 = 0;
const WRITER_LOCK_OFFSET: u64 = 1;
const META_LOCK_OFFSET: u64 = 2;
const READER_ADMISSION_LOCK_OFFSET: u64 = 3;
const READER_LOCK_BASE: u64 = 4096;

const READER_STATE_EMPTY: u8 = 0;
const READER_STATE_ACTIVE: u8 = 1;
const READER_STATE_INITIALIZING: u8 = 2;

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
    admission_gate: Arc<LocalAdmissionGate>,
    metrics: ProcessCoordinationMetrics,
    #[cfg(test)]
    #[allow(clippy::type_complexity)]
    stale_probe_callback: Mutex<Option<Arc<dyn Fn(u16) + Send + Sync>>>,
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
    owner_thread: std::thread::ThreadId,
    generation: u64,
}

#[derive(Debug)]
pub(crate) struct ProcessCheckpointGuard {
    _writer: ProcessWriterGuard,
    _admission: ProcessAdmissionGateGuard,
}

#[derive(Debug)]
pub(crate) struct ProcessAdmissionGateGuard {
    gate: Arc<LocalAdmissionGate>,
    exclusive: bool,
}

#[derive(Debug)]
struct LocalAdmissionGate {
    writer_state: Mutex<LocalProcessWriterState>,
    writer_wake: Condvar,
    state: Mutex<LocalAdmissionGateState>,
    wake: Condvar,
    // Classic POSIX record locks are released when the process closes any
    // descriptor for the locked inode. All same-process coordinators therefore
    // share this one descriptor, so dropping one handle cannot silently release
    // another handle's process locks.
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    file: Arc<dyn VfsFile>,
}

#[derive(Debug, Default)]
struct LocalProcessWriterState {
    owner_thread: Option<std::thread::ThreadId>,
    generation: u64,
    next_generation: u64,
    owner_coordinator: Option<Weak<ProcessCoordinatorInner>>,
    owner_process_id: u64,
    owner_process_token: u64,
    publish_owner: bool,
    recursion: usize,
    waiting: usize,
    pending_os_attempt: Option<u64>,
    next_os_attempt: u64,
    os_lock: Option<Box<dyn VfsFileLock>>,
}

#[derive(Debug, Default)]
struct LocalAdmissionGateState {
    readers: usize,
    writer: bool,
    waiting_readers: usize,
    waiting_writers: usize,
    pending_os_attempt: Option<AdmissionOsAttempt>,
    next_os_attempt: u64,
    os_lock: Option<Box<dyn VfsFileLock>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AdmissionOsAttempt {
    generation: u64,
    exclusive: bool,
}

#[derive(Debug)]
pub(crate) struct ProcessReaderGuard {
    coordinator: ProcessCoordinator,
    slot: u16,
    _lock: Option<Box<dyn VfsFileLock>>,
    reservation: LocalReaderSlotReservation,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ReaderSlotKey {
    coord_path: PathBuf,
    slot: u16,
}

#[derive(Debug)]
struct LocalReaderSlotReservation {
    key: ReaderSlotKey,
    active: bool,
}

#[derive(Debug)]
enum ReaderSlotOwnershipProbe {
    LocallyReserved,
    ExternallyLocked,
    Reclaimable(ReclaimableReaderSlotProbe),
}

#[derive(Debug)]
struct ReclaimableReaderSlotProbe {
    lock: Option<Box<dyn VfsFileLock>>,
    reservation: LocalReaderSlotReservation,
}

impl Drop for ReclaimableReaderSlotProbe {
    fn drop(&mut self) {
        drop(self.lock.take());
        self.reservation.release();
    }
}

impl LocalReaderSlotReservation {
    fn try_acquire(key: ReaderSlotKey) -> Result<Option<Self>> {
        let inserted = active_reader_slots()
            .lock()
            .map_err(|_| DbError::internal("process reader slot registry poisoned"))?
            .insert(key.clone());
        Ok(if inserted {
            Some(Self { key, active: true })
        } else {
            None
        })
    }

    fn release(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut slots) = active_reader_slots().lock() {
            slots.remove(&self.key);
        }
        self.active = false;
    }
}

impl Drop for LocalReaderSlotReservation {
    fn drop(&mut self) {
        self.release();
    }
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
            let _ = (db_path, header, timeout_ms);
            Ok(None)
        }
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        {
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

            // The database already exists by the time process coordination opens.
            // Deriving the gate key from its canonical path lets all local openers
            // find the shared coordination descriptor before opening another one.
            let canonical_db_path = vfs.canonicalize_path(db_path)?;
            let coord_path = coord_path_for_db(&canonical_db_path);
            let admission_gate = local_admission_gate(vfs, &coord_path)?;
            let file = Arc::clone(&admission_gate.file);
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
                    admission_gate,
                    metrics: ProcessCoordinationMetrics::default(),
                    #[cfg(test)]
                    stale_probe_callback: Mutex::new(None),
                    lock_wait_callback: Mutex::new(None),
                }),
            };
            coordinator.initialize_or_rebuild()?;
            Ok(Some(coordinator))
        }
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

    pub(crate) fn lock_checkpoint(&self) -> Result<ProcessCheckpointGuard> {
        // Lock order is fixed: writer/checkpoint serialization first, then
        // the reader-admission gate. Reader admission never takes the writer
        // lock, so a reader holding the shared gate can finish refresh and
        // registration while a checkpoint waits without forming a cycle.
        let writer = self.lock_writer_inner(true)?;
        let admission = self.lock_reader_admission_exclusive()?;
        Ok(ProcessCheckpointGuard {
            _writer: writer,
            _admission: admission,
        })
    }

    pub(crate) fn lock_reader_admission(&self) -> Result<ProcessAdmissionGateGuard> {
        self.lock_admission_gate(false)
    }

    pub(crate) fn reader_registration_timed_out(&self, elapsed: Duration) -> bool {
        self.inner.timeout.is_some_and(|timeout| elapsed >= timeout)
    }

    pub(crate) fn reader_registration_timeout_is_zero(&self) -> bool {
        self.inner.timeout.is_some_and(|timeout| timeout.is_zero())
    }

    fn lock_reader_admission_exclusive(&self) -> Result<ProcessAdmissionGateGuard> {
        self.lock_admission_gate(true)
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
            let slot_key = ReaderSlotKey {
                coord_path: self.inner.coord_path.clone(),
                slot,
            };
            // Classic POSIX range locks do not conflict with another lock in
            // this process. Atomically reserve the slot locally before its
            // first record write so another local allocator or retention scan
            // cannot acquire/clear the same locked-but-initializing slot.
            let Some(reservation) = LocalReaderSlotReservation::try_acquire(slot_key)? else {
                continue;
            };
            let lock = match self
                .inner
                .file
                .try_lock_range(reader_lock_offset(slot), 1, true)
            {
                Ok(Some(lock)) => lock,
                Ok(None) => continue,
                Err(error) => return Err(error),
            };
            let generation = now_unix_ms();
            let initializing = ReaderSlotRecord {
                state: READER_STATE_INITIALIZING,
                generation,
                process_id: self.inner.process_id,
                process_token: self.inner.process_token,
                reader_id,
                snapshot_lsn: 0,
                started_unix_ms: generation,
            };
            // A locked slot is marked before its snapshot becomes active. The
            // admission gate is the primary checkpoint fence; the initializing
            // state remains a conservative crash/fault diagnostic if active
            // publication fails after slot ownership is acquired.
            let record = ReaderSlotRecord {
                state: READER_STATE_ACTIVE,
                snapshot_lsn,
                ..initializing
            };
            if let Err(error) = self
                .write_reader_slot(slot, initializing)
                .and_then(|()| self.write_reader_slot(slot, record))
            {
                drop(lock);
                return Err(error);
            }
            self.inner
                .metrics
                .reader_slot_allocations
                .fetch_add(1, Ordering::Relaxed);
            return Ok(ProcessReaderGuard {
                coordinator: self.clone(),
                slot,
                _lock: Some(lock),
                reservation,
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
        // The normal path takes one coherent-sized sidecar read. If that read
        // fails, fall back to the prior per-slot behavior so a transient or
        // partial-file error retains the same active-reader information and
        // conservative truncation decision as before this optimization.
        let slot_bytes = self.read_all_reader_slots().ok();
        for slot in 0..READER_SLOT_COUNT {
            let decoded = match slot_bytes.as_ref() {
                Some(bytes) => decode_reader_slot_from_bulk(bytes, slot),
                None => self.read_reader_slot(slot),
            };
            let record = match decoded {
                Ok(record) => record,
                Err(_) => {
                    truncation_blocked = true;
                    continue;
                }
            };
            self.notify_before_reader_slot_probe(slot);
            if record.state == READER_STATE_EMPTY {
                match self.reserve_reader_slot(slot)? {
                    Some(reservation) => drop(reservation),
                    None => truncation_blocked = true,
                }
                continue;
            }
            let ownership = self.probe_reader_slot_ownership(slot)?;
            match record.state {
                READER_STATE_ACTIVE => match ownership {
                    ReaderSlotOwnershipProbe::LocallyReserved
                    | ReaderSlotOwnershipProbe::ExternallyLocked => {
                        active_count += 1;
                        min_snapshot_lsn =
                            Some(min_snapshot_lsn.map_or(record.snapshot_lsn, |current| {
                                current.min(record.snapshot_lsn)
                            }));
                    }
                    ReaderSlotOwnershipProbe::Reclaimable(probe) => {
                        self.clear_reader_slot(slot)?;
                        drop(probe);
                        self.inner
                            .metrics
                            .reader_slot_reclaims
                            .fetch_add(1, Ordering::Relaxed);
                    }
                },
                READER_STATE_INITIALIZING => match ownership {
                    ReaderSlotOwnershipProbe::LocallyReserved
                    | ReaderSlotOwnershipProbe::ExternallyLocked => {
                        truncation_blocked = true;
                    }
                    ReaderSlotOwnershipProbe::Reclaimable(probe) => {
                        self.clear_reader_slot(slot)?;
                        drop(probe);
                        self.inner
                            .metrics
                            .reader_slot_reclaims
                            .fetch_add(1, Ordering::Relaxed);
                    }
                },
                _ => truncation_blocked = true,
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
            self.notify_before_reader_slot_probe(slot);
            let active = match self.probe_reader_slot_ownership(slot)? {
                ReaderSlotOwnershipProbe::LocallyReserved
                | ReaderSlotOwnershipProbe::ExternallyLocked => true,
                ReaderSlotOwnershipProbe::Reclaimable(probe) => {
                    drop(probe);
                    false
                }
            };
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
        if !changed {
            return Ok(header);
        }
        header.wal_end_lsn = wal_end_lsn;
        header.checkpoint_lsn = checkpoint_lsn;
        header.coordinator_generation = header.coordinator_generation.saturating_add(1);
        header.wal_generation = header.wal_generation.saturating_add(1);
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

    fn lock_admission_gate(&self, exclusive: bool) -> Result<ProcessAdmissionGateGuard> {
        let start = Instant::now();
        let gate = Arc::clone(&self.inner.admission_gate);
        let mut state = gate
            .state
            .lock()
            .map_err(|_| DbError::internal("process reader-admission gate poisoned"))?;
        if exclusive {
            state.waiting_writers = state.waiting_writers.saturating_add(1);
        } else {
            state.waiting_readers = state.waiting_readers.saturating_add(1);
        }

        loop {
            let can_join_shared = !exclusive
                && !state.writer
                && state.waiting_writers == 0
                && state.pending_os_attempt.is_none()
                && state.readers > 0;
            if can_join_shared {
                state.waiting_readers = state.waiting_readers.saturating_sub(1);
                state.readers = state.readers.saturating_add(1);
                drop(state);
                return Ok(ProcessAdmissionGateGuard { gate, exclusive });
            }

            let can_lead_os_attempt = state.pending_os_attempt.is_none()
                && !state.writer
                && state.readers == 0
                && (exclusive || state.waiting_writers == 0);
            if !can_lead_os_attempt {
                let label = if exclusive {
                    "process checkpoint reader-admission gate"
                } else {
                    "process reader-admission gate"
                };
                state = match wait_for_local_admission_gate(
                    &gate,
                    state,
                    self.inner.timeout,
                    start,
                    label,
                ) {
                    Ok(state) => state,
                    Err(error) => {
                        if let Ok(mut state) = gate.state.lock() {
                            decrement_admission_waiter(&mut state, exclusive);
                            gate.wake.notify_all();
                        }
                        return Err(error);
                    }
                };
                continue;
            }

            let timeout = match remaining_lock_timeout(self.inner.timeout, start) {
                Ok(timeout) => timeout,
                Err(error) => {
                    decrement_admission_waiter(&mut state, exclusive);
                    gate.wake.notify_all();
                    return Err(error);
                }
            };
            state.next_os_attempt = state.next_os_attempt.saturating_add(1).max(1);
            let attempt = AdmissionOsAttempt {
                generation: state.next_os_attempt,
                exclusive,
            };
            state.pending_os_attempt = Some(attempt);
            drop(state);

            let lock_result = lock_range_with_timeout(
                self.inner.file.as_ref(),
                READER_ADMISSION_LOCK_OFFSET,
                1,
                exclusive,
                timeout,
            );
            state = gate
                .state
                .lock()
                .map_err(|_| DbError::internal("process reader-admission gate poisoned"))?;
            if state.pending_os_attempt != Some(attempt) {
                return Err(DbError::internal(
                    "process reader-admission OS attempt ownership changed",
                ));
            }
            state.pending_os_attempt = None;
            match lock_result {
                Ok(os_lock) => {
                    decrement_admission_waiter(&mut state, exclusive);
                    if exclusive {
                        state.writer = true;
                    } else {
                        state.readers = 1;
                    }
                    state.os_lock = Some(os_lock);
                    gate.wake.notify_all();
                    drop(state);
                    return Ok(ProcessAdmissionGateGuard { gate, exclusive });
                }
                Err(error) => {
                    decrement_admission_waiter(&mut state, exclusive);
                    gate.wake.notify_all();
                    return Err(error);
                }
            }
        }
    }

    fn lock_writer_inner(&self, checkpoint: bool) -> Result<ProcessWriterGuard> {
        let start = Instant::now();
        let owner_thread = std::thread::current().id();
        let gate = Arc::clone(&self.inner.admission_gate);
        let mut state = gate
            .writer_state
            .lock()
            .map_err(|_| DbError::internal("process-local writer lock poisoned"))?;
        if state.owner_thread == Some(owner_thread) {
            state.recursion = state
                .recursion
                .checked_add(1)
                .ok_or_else(|| DbError::internal("process writer lock recursion overflow"))?;
            return Ok(ProcessWriterGuard {
                coordinator: self.clone(),
                owner_thread,
                generation: state.generation,
            });
        }

        state.waiting = state.waiting.saturating_add(1);
        let os_lock = loop {
            if state.owner_thread.is_some() || state.pending_os_attempt.is_some() {
                state = match wait_for_local_writer(
                    &gate,
                    state,
                    self.inner.timeout,
                    start,
                    "process writer lock",
                ) {
                    Ok(state) => state,
                    Err(error) => {
                        if let Ok(mut state) = gate.writer_state.lock() {
                            state.waiting = state.waiting.saturating_sub(1);
                            gate.writer_wake.notify_all();
                        }
                        self.record_writer_acquire_failure(checkpoint, start, &error);
                        return Err(error);
                    }
                };
                continue;
            }
            let timeout = match remaining_writer_timeout(self.inner.timeout, start) {
                Ok(timeout) => timeout,
                Err(error) => {
                    state.waiting = state.waiting.saturating_sub(1);
                    self.record_writer_acquire_failure(checkpoint, start, &error);
                    return Err(error);
                }
            };
            state.next_os_attempt = state.next_os_attempt.saturating_add(1).max(1);
            let attempt = state.next_os_attempt;
            state.pending_os_attempt = Some(attempt);
            drop(state);

            let lock_result = lock_range_with_timeout(
                self.inner.file.as_ref(),
                WRITER_LOCK_OFFSET,
                1,
                true,
                timeout,
            );
            state = gate
                .writer_state
                .lock()
                .map_err(|_| DbError::internal("process-local writer lock poisoned"))?;
            if state.pending_os_attempt != Some(attempt) {
                return Err(DbError::internal(
                    "process writer OS attempt ownership changed",
                ));
            }
            state.pending_os_attempt = None;
            gate.writer_wake.notify_all();
            match lock_result {
                Ok(os_lock) => break os_lock,
                Err(error) => {
                    state.waiting = state.waiting.saturating_sub(1);
                    self.record_writer_acquire_failure(checkpoint, start, &error);
                    return match error {
                        DbError::Busy { .. } => Err(DbError::busy("process writer lock is busy")),
                        DbError::Timeout { .. } => {
                            Err(DbError::timeout("process writer lock wait timed out"))
                        }
                        error => Err(error),
                    };
                }
            }
        };
        state.waiting = state.waiting.saturating_sub(1);
        state.owner_thread = Some(owner_thread);
        state.next_generation = state.next_generation.saturating_add(1).max(1);
        state.generation = state.next_generation;
        state.owner_coordinator = Some(Arc::downgrade(&self.inner));
        state.owner_process_id = self.inner.process_id;
        state.owner_process_token = self.inner.process_token;
        state.publish_owner = matches!(self.inner.mode, ProcessCoordinationMode::Required);
        state.recursion = 1;
        state.os_lock = Some(os_lock);
        let generation = state.generation;
        drop(state);

        let elapsed = start.elapsed();
        self.record_lock_wait(checkpoint);
        self.maybe_notify_lock_wait_callback(checkpoint, elapsed, "ok");
        let guard = ProcessWriterGuard {
            coordinator: self.clone(),
            owner_thread,
            generation,
        };
        if let Err(error) = self.publish_writer_owner_if_required(true) {
            drop(guard);
            return Err(error);
        }
        Ok(guard)
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
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

    fn read_all_reader_slots(&self) -> Result<[u8; READER_SLOTS_BYTES]> {
        let mut bytes = [0_u8; READER_SLOTS_BYTES];
        read_exact_at(self.inner.file.as_ref(), HEADER_LEN, &mut bytes)?;
        Ok(bytes)
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

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    fn clear_all_reader_slots_bulk(&self) -> Result<()> {
        const SLOT_BYTES: usize = READER_SLOT_COUNT as usize * READER_SLOT_LEN as usize;
        let encoded = encode_reader_slot(empty_reader_slot_record());
        let mut bytes = [0_u8; SLOT_BYTES];
        for chunk in bytes.as_chunks_mut::<{ READER_SLOT_LEN as usize }>().0 {
            chunk.copy_from_slice(&encoded);
        }
        write_all_at(self.inner.file.as_ref(), HEADER_LEN, &bytes)
    }

    fn publish_writer_owner_if_required(&self, active: bool) -> Result<()> {
        self.record_local_writer_owner(active);
        if matches!(self.inner.mode, ProcessCoordinationMode::Required) {
            self.publish_writer_owner(active, self.inner.process_id, self.inner.process_token)
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

    fn publish_writer_owner(
        &self,
        active: bool,
        owner_process_id: u64,
        owner_process_token: u64,
    ) -> Result<()> {
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
            header.writer_owner_pid = owner_process_id;
            header.writer_owner_token = owner_process_token;
            header.writer_owner_started_ms = now;
            self.inner
                .metrics
                .current_writer_pid
                .store(owner_process_id, Ordering::Release);
            self.inner
                .metrics
                .current_writer_lock_started_ms
                .store(now, Ordering::Release);
        } else if header.writer_owner_pid == owner_process_id
            && header.writer_owner_token == owner_process_token
        {
            header.writer_owner_pid = 0;
            header.writer_owner_token = 0;
            header.writer_owner_started_ms = 0;
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

    fn record_writer_acquire_failure(&self, checkpoint: bool, start: Instant, error: &DbError) {
        let status = match error {
            DbError::Busy { .. } => "busy",
            DbError::Timeout { .. } => "timeout",
            _ => return,
        };
        self.record_lock_timeout(checkpoint);
        self.maybe_notify_lock_wait_callback(checkpoint, start.elapsed(), status);
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

    fn reserve_reader_slot(&self, slot: u16) -> Result<Option<LocalReaderSlotReservation>> {
        let key = ReaderSlotKey {
            coord_path: self.inner.coord_path.clone(),
            slot,
        };
        LocalReaderSlotReservation::try_acquire(key)
    }

    fn probe_reader_slot_ownership(&self, slot: u16) -> Result<ReaderSlotOwnershipProbe> {
        let Some(reservation) = self.reserve_reader_slot(slot)? else {
            return Ok(ReaderSlotOwnershipProbe::LocallyReserved);
        };
        match self
            .inner
            .file
            .try_lock_range(reader_lock_offset(slot), 1, true)
        {
            Ok(Some(lock)) => Ok(ReaderSlotOwnershipProbe::Reclaimable(
                ReclaimableReaderSlotProbe {
                    lock: Some(lock),
                    reservation,
                },
            )),
            Ok(None) => Ok(ReaderSlotOwnershipProbe::ExternallyLocked),
            Err(error) => Err(error),
        }
    }

    #[cfg(test)]
    fn notify_before_reader_slot_probe(&self, slot: u16) {
        if let Ok(callback) = self.inner.stale_probe_callback.lock() {
            if let Some(callback) = callback.as_ref() {
                callback(slot);
            }
        }
    }

    #[cfg(not(test))]
    fn notify_before_reader_slot_probe(&self, _slot: u16) {}

    #[cfg(test)]
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
        let gate = Arc::clone(&self.coordinator.inner.admission_gate);
        let Ok(mut state) = gate.writer_state.lock() else {
            return;
        };
        if state.owner_thread != Some(self.owner_thread)
            || state.generation != self.generation
            || state.recursion == 0
        {
            return;
        }
        state.recursion -= 1;
        if state.recursion > 0 {
            return;
        }

        let owner = state
            .owner_coordinator
            .take()
            .and_then(|owner| owner.upgrade());
        let owner_process_id = state.owner_process_id;
        let owner_process_token = state.owner_process_token;
        let publish_owner = state.publish_owner;
        if let Some(owner) = owner {
            let owner = ProcessCoordinator { inner: owner };
            let _ = owner.publish_writer_owner_if_required(false);
        } else {
            self.coordinator.record_local_writer_owner(false);
            if publish_owner {
                let _ = self.coordinator.publish_writer_owner(
                    false,
                    owner_process_id,
                    owner_process_token,
                );
            }
        }
        state.owner_thread = None;
        state.generation = 0;
        state.owner_process_id = 0;
        state.owner_process_token = 0;
        state.publish_owner = false;
        drop(state.os_lock.take());
        gate.writer_wake.notify_all();
    }
}

impl Drop for ProcessAdmissionGateGuard {
    fn drop(&mut self) {
        let Ok(mut state) = self.gate.state.lock() else {
            return;
        };
        let release_os_lock = if self.exclusive {
            state.writer = false;
            true
        } else {
            state.readers = state.readers.saturating_sub(1);
            state.readers == 0
        };
        if release_os_lock {
            // POSIX record locks are process-associated rather than tied to an
            // individual file descriptor. Keep exactly one OS lock per local
            // mode and release it only after the last local shared holder (or
            // the sole local exclusive holder) exits.
            drop(state.os_lock.take());
        }
        self.gate.wake.notify_all();
    }
}

impl Drop for ProcessReaderGuard {
    fn drop(&mut self) {
        // Keep the local reservation through record clearing and OS unlock.
        // On classic POSIX locks, a second local lock does not conflict and
        // this guard's later F_UNLCK would otherwise release that replacement
        // reader's ownership too.
        let _ = self.coordinator.clear_reader_slot(self.slot);
        drop(self._lock.take());
        self.reservation.release();
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn coord_path_for_db(db_path: &Path) -> PathBuf {
    let mut path = db_path.as_os_str().to_os_string();
    path.push(".coord");
    PathBuf::from(path)
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn local_admission_gate(vfs: &VfsHandle, coord_path: &Path) -> Result<Arc<LocalAdmissionGate>> {
    static GATES: OnceLock<Mutex<HashMap<PathBuf, Weak<LocalAdmissionGate>>>> = OnceLock::new();
    let gates = GATES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut gates = gates
        .lock()
        .map_err(|_| DbError::internal("process reader-admission gate registry poisoned"))?;
    gates.retain(|_, gate| gate.strong_count() > 0);
    if let Some(gate) = gates.get(coord_path).and_then(Weak::upgrade) {
        return Ok(gate);
    }
    let file = vfs.open(coord_path, OpenMode::OpenOrCreate, FileKind::Coordination)?;
    let gate = Arc::new(LocalAdmissionGate {
        writer_state: Mutex::new(LocalProcessWriterState::default()),
        writer_wake: Condvar::new(),
        state: Mutex::new(LocalAdmissionGateState::default()),
        wake: Condvar::new(),
        file,
    });
    gates.insert(coord_path.to_path_buf(), Arc::downgrade(&gate));
    Ok(gate)
}

fn wait_for_local_admission_gate<'a>(
    gate: &'a LocalAdmissionGate,
    state: std::sync::MutexGuard<'a, LocalAdmissionGateState>,
    timeout: Option<Duration>,
    start: Instant,
    label: &str,
) -> Result<std::sync::MutexGuard<'a, LocalAdmissionGateState>> {
    match timeout {
        Some(timeout) if timeout.is_zero() => Err(DbError::busy(format!("{label} is busy"))),
        Some(timeout) => {
            let remaining = timeout
                .checked_sub(start.elapsed())
                .ok_or_else(|| DbError::timeout(format!("timed out waiting for {label}")))?;
            let (state, _) = gate
                .wake
                .wait_timeout(state, remaining)
                .map_err(|_| DbError::internal("process reader-admission gate poisoned"))?;
            Ok(state)
        }
        None => gate
            .wake
            .wait(state)
            .map_err(|_| DbError::internal("process reader-admission gate poisoned")),
    }
}

fn decrement_admission_waiter(state: &mut LocalAdmissionGateState, exclusive: bool) {
    if exclusive {
        state.waiting_writers = state.waiting_writers.saturating_sub(1);
    } else {
        state.waiting_readers = state.waiting_readers.saturating_sub(1);
    }
}

fn wait_for_local_writer<'a>(
    gate: &'a LocalAdmissionGate,
    state: std::sync::MutexGuard<'a, LocalProcessWriterState>,
    timeout: Option<Duration>,
    start: Instant,
    label: &str,
) -> Result<std::sync::MutexGuard<'a, LocalProcessWriterState>> {
    match timeout {
        Some(timeout) if timeout.is_zero() => Err(DbError::busy(format!("{label} is busy"))),
        Some(timeout) => {
            let remaining = timeout
                .checked_sub(start.elapsed())
                .ok_or_else(|| DbError::timeout(format!("timed out waiting for {label}")))?;
            let (state, _) = gate
                .writer_wake
                .wait_timeout(state, remaining)
                .map_err(|_| DbError::internal("process-local writer lock poisoned"))?;
            Ok(state)
        }
        None => gate
            .writer_wake
            .wait(state)
            .map_err(|_| DbError::internal("process-local writer lock poisoned")),
    }
}

fn remaining_writer_timeout(timeout: Option<Duration>, start: Instant) -> Result<Option<Duration>> {
    match timeout {
        Some(timeout) if timeout.is_zero() => Ok(Some(Duration::ZERO)),
        Some(timeout) => timeout
            .checked_sub(start.elapsed())
            .map(Some)
            .ok_or_else(|| DbError::timeout("process writer lock wait timed out")),
        None => Ok(None),
    }
}

fn remaining_lock_timeout(timeout: Option<Duration>, start: Instant) -> Result<Option<Duration>> {
    match timeout {
        Some(timeout) if timeout.is_zero() => Ok(Some(Duration::ZERO)),
        Some(timeout) => timeout
            .checked_sub(start.elapsed())
            .map(Some)
            .ok_or_else(|| DbError::timeout("process reader-admission gate wait timed out")),
        None => Ok(None),
    }
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

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
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
    if state != READER_STATE_EMPTY
        && state != READER_STATE_ACTIVE
        && state != READER_STATE_INITIALIZING
    {
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

fn decode_reader_slot_from_bulk(
    bytes: &[u8; READER_SLOTS_BYTES],
    slot: u16,
) -> Result<ReaderSlotRecord> {
    let start = usize::from(slot)
        .checked_mul(READER_SLOT_LEN as usize)
        .ok_or_else(|| DbError::corruption("process reader slot offset overflows"))?;
    let end = start
        .checked_add(READER_SLOT_LEN as usize)
        .ok_or_else(|| DbError::corruption("process reader slot end overflows"))?;
    let encoded: &[u8; READER_SLOT_LEN as usize] = bytes
        .get(start..end)
        .ok_or_else(|| DbError::corruption(format!("process reader slot {slot} is out of range")))?
        .try_into()
        .map_err(|_| DbError::corruption("process reader slot has an invalid encoded length"))?;
    decode_reader_slot(encoded)
}

fn reader_record_offset(slot: u16) -> u64 {
    HEADER_LEN + u64::from(slot) * READER_SLOT_LEN
}

fn reader_lock_offset(slot: u16) -> u64 {
    READER_LOCK_BASE + u64::from(slot)
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn current_process_id() -> u64 {
    u64::from(std::process::id())
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
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
    use std::process::Command;
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc;
    use std::thread;

    #[derive(Debug, Default)]
    struct CoordinationIoCounts {
        opens: AtomicU64,
        reads: AtomicU64,
        writes: AtomicU64,
        locks: AtomicU64,
    }

    impl CoordinationIoCounts {
        fn reset(&self) {
            self.opens.store(0, Ordering::Release);
            self.reads.store(0, Ordering::Release);
            self.writes.store(0, Ordering::Release);
            self.locks.store(0, Ordering::Release);
        }
    }

    #[derive(Debug)]
    struct CountingCoordinationVfs {
        inner: crate::vfs::os::OsVfs,
        counts: Arc<CoordinationIoCounts>,
    }

    impl CountingCoordinationVfs {
        fn new(counts: Arc<CoordinationIoCounts>) -> Self {
            Self {
                inner: crate::vfs::os::OsVfs,
                counts,
            }
        }
    }

    impl crate::vfs::Vfs for CountingCoordinationVfs {
        fn open(&self, path: &Path, mode: OpenMode, kind: FileKind) -> Result<Arc<dyn VfsFile>> {
            if kind == FileKind::Coordination {
                self.counts.opens.fetch_add(1, Ordering::Relaxed);
            }
            let inner = crate::vfs::Vfs::open(&self.inner, path, mode, kind)?;
            Ok(Arc::new(CountingCoordinationFile {
                inner,
                counts: Arc::clone(&self.counts),
            }))
        }

        fn file_exists(&self, path: &Path) -> Result<bool> {
            crate::vfs::Vfs::file_exists(&self.inner, path)
        }

        fn remove_file(&self, path: &Path) -> Result<()> {
            crate::vfs::Vfs::remove_file(&self.inner, path)
        }

        fn canonicalize_path(&self, path: &Path) -> Result<PathBuf> {
            crate::vfs::Vfs::canonicalize_path(&self.inner, path)
        }

        fn supports_file_locks(&self) -> bool {
            true
        }
    }

    #[derive(Debug)]
    struct CountingCoordinationFile {
        inner: Arc<dyn VfsFile>,
        counts: Arc<CoordinationIoCounts>,
    }

    impl VfsFile for CountingCoordinationFile {
        fn kind(&self) -> FileKind {
            self.inner.kind()
        }

        fn path(&self) -> &Path {
            self.inner.path()
        }

        fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
            if self.inner.kind() == FileKind::Coordination {
                self.counts.reads.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.read_at(offset, buf)
        }

        fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize> {
            if self.inner.kind() == FileKind::Coordination {
                self.counts.writes.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.write_at(offset, buf)
        }

        fn write_all_at_many(&self, writes: &[(u64, &[u8])]) -> Result<()> {
            if self.inner.kind() == FileKind::Coordination {
                self.counts.writes.fetch_add(
                    u64::try_from(writes.len()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
            }
            self.inner.write_all_at_many(writes)
        }

        fn advise_sequential(&self) -> Result<()> {
            self.inner.advise_sequential()
        }

        fn sync_data(&self) -> Result<()> {
            self.inner.sync_data()
        }

        fn sync_metadata(&self) -> Result<()> {
            self.inner.sync_metadata()
        }

        fn file_size(&self) -> Result<u64> {
            self.inner.file_size()
        }

        fn set_len(&self, len: u64) -> Result<()> {
            self.inner.set_len(len)
        }

        fn try_lock_range(
            &self,
            offset: u64,
            len: u64,
            exclusive: bool,
        ) -> Result<Option<Box<dyn VfsFileLock>>> {
            if self.inner.kind() == FileKind::Coordination {
                self.counts.locks.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.try_lock_range(offset, len, exclusive)
        }
    }

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

    fn open_test_coordinator(
        vfs: &VfsHandle,
        db_path: &Path,
        header: &DatabaseHeader,
        timeout_ms: u64,
    ) -> ProcessCoordinator {
        ProcessCoordinator::open(
            vfs,
            db_path,
            header,
            ProcessCoordinationMode::Required,
            timeout_ms,
        )
        .expect("open coordinator")
        .expect("required coordinator")
    }

    fn wait_for_admission_counts(
        coordinator: &ProcessCoordinator,
        expected_readers: usize,
        expected_writers: usize,
    ) {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let state = coordinator
                .inner
                .admission_gate
                .state
                .lock()
                .expect("admission state");
            if state.waiting_readers == expected_readers
                && state.waiting_writers == expected_writers
            {
                return;
            }
            drop(state);
            assert!(
                Instant::now() < deadline,
                "timed out waiting for admission counts readers={expected_readers}, writers={expected_writers}"
            );
            thread::yield_now();
        }
    }

    fn wait_for_test_path(path: &Path, label: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !path.exists() {
            assert!(Instant::now() < deadline, "timed out waiting for {label}");
            thread::yield_now();
        }
    }

    fn run_external_writer_probe(db_path: &Path, header_path: &Path, result_path: &Path) -> String {
        let status = Command::new(std::env::current_exe().expect("current test executable"))
            .arg("--exact")
            .arg("wal::coordination::tests::cross_process_writer_probe_helper")
            .arg("--nocapture")
            .env("DDB_WRITER_PROBE_DB", db_path)
            .env("DDB_WRITER_PROBE_HEADER", header_path)
            .env("DDB_WRITER_PROBE_RESULT", result_path)
            .status()
            .expect("run external writer probe");
        assert!(status.success(), "external writer probe failed: {status}");
        std::fs::read_to_string(result_path).expect("read external writer probe result")
    }

    fn spawn_external_range_holder(
        db_path: &Path,
        header_path: &Path,
        offset: u64,
        held_path: &Path,
        release_path: &Path,
    ) -> std::process::Child {
        Command::new(std::env::current_exe().expect("current test executable"))
            .arg("--exact")
            .arg("wal::coordination::tests::cross_process_range_lock_holder_helper")
            .arg("--nocapture")
            .env("DDB_RANGE_HOLDER_DB", db_path)
            .env("DDB_RANGE_HOLDER_HEADER", header_path)
            .env("DDB_RANGE_HOLDER_OFFSET", offset.to_string())
            .env("DDB_RANGE_HOLDER_HELD", held_path)
            .env("DDB_RANGE_HOLDER_RELEASE", release_path)
            .spawn()
            .expect("spawn external range-lock holder")
    }

    fn stale_active_reader_record(snapshot_lsn: u64) -> ReaderSlotRecord {
        ReaderSlotRecord {
            state: READER_STATE_ACTIVE,
            generation: 1,
            process_id: 99_001,
            process_token: 99_002,
            reader_id: 99_003,
            snapshot_lsn,
            started_unix_ms: 1,
        }
    }

    fn pause_before_reader_slot_probe(
        coordinator: &ProcessCoordinator,
        target_slot: u16,
    ) -> (mpsc::Receiver<()>, mpsc::SyncSender<()>) {
        let (decoded_tx, decoded_rx) = mpsc::sync_channel(1);
        let (resume_tx, resume_rx) = mpsc::sync_channel(1);
        let resume_rx = Arc::new(Mutex::new(resume_rx));
        let fired = Arc::new(AtomicBool::new(false));
        *coordinator
            .inner
            .stale_probe_callback
            .lock()
            .expect("stale probe callback") = Some(Arc::new(move |slot| {
            if slot != target_slot || fired.swap(true, Ordering::AcqRel) {
                return;
            }
            decoded_tx.send(()).expect("publish stale slot decode");
            resume_rx
                .lock()
                .expect("stale probe resume receiver")
                .recv()
                .expect("resume stale slot probe");
        }));
        (decoded_rx, resume_tx)
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

    #[test]
    fn local_reader_slot_registry_retains_capacity_after_release() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let key = ReaderSlotKey {
            coord_path: temp.path().join("retained-reader-registry-capacity.coord"),
            slot: 0,
        };
        let reservation = LocalReaderSlotReservation::try_acquire(key.clone())
            .expect("reserve local reader slot")
            .expect("unique local reader slot");
        let occupied_capacity = active_reader_slots()
            .lock()
            .expect("active reader slots")
            .capacity();
        drop(reservation);
        let slots = active_reader_slots().lock().expect("active reader slots");
        assert!(!slots.contains(&key));
        assert!(
            slots.capacity() >= occupied_capacity,
            "empty-slot scans should reuse registry allocation capacity"
        );
    }

    #[test]
    fn same_process_coordinators_share_one_custom_vfs_file_descriptor() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let db_path = temp.path().join("shared-coordination-file.ddb");
        let first_counts = Arc::new(CoordinationIoCounts::default());
        let second_counts = Arc::new(CoordinationIoCounts::default());
        let first_vfs = VfsHandle::from_vfs(Arc::new(CountingCoordinationVfs::new(Arc::clone(
            &first_counts,
        ))));
        let second_vfs = VfsHandle::from_vfs(Arc::new(CountingCoordinationVfs::new(Arc::clone(
            &second_counts,
        ))));
        let database_header = DatabaseHeader::new(4096);

        let first = open_test_coordinator(&first_vfs, &db_path, &database_header, 1_000);
        let second = open_test_coordinator(&second_vfs, &db_path, &database_header, 1_000);
        assert!(Arc::ptr_eq(&first.inner.file, &second.inner.file));
        assert_eq!(first_counts.opens.load(Ordering::Acquire), 1);
        assert_eq!(second_counts.opens.load(Ordering::Acquire), 0);

        let admission = first
            .lock_reader_admission()
            .expect("lock admission through shared file");
        drop(second);
        assert_eq!(
            Arc::strong_count(&first.inner.file),
            2,
            "only the coordinator and path-local gate should retain the descriptor"
        );
        drop(admission);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_and_canonical_database_paths_share_the_canonical_sidecar_both_directions() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::TempDir::new().expect("tempdir");
        for alias_first in [false, true] {
            let suffix = if alias_first {
                "alias-first"
            } else {
                "real-first"
            };
            let real_path = temp.path().join(format!("{suffix}.ddb"));
            let alias_path = temp.path().join(format!("{suffix}-alias.ddb"));
            std::fs::write(&real_path, []).expect("create canonical database path");
            symlink(&real_path, &alias_path).expect("create database symlink");
            let database_header = DatabaseHeader::new(4096);
            let first_path = if alias_first { &alias_path } else { &real_path };
            let second_path = if alias_first { &real_path } else { &alias_path };
            let first_vfs = VfsHandle::for_path(first_path);
            let second_vfs = VfsHandle::for_path(second_path);
            let first = open_test_coordinator(&first_vfs, first_path, &database_header, 1_000);
            let second = open_test_coordinator(&second_vfs, second_path, &database_header, 1_000);

            let canonical_coord = coord_path_for_db(
                &std::fs::canonicalize(&real_path).expect("canonical database path"),
            );
            assert_eq!(first.inner.coord_path, canonical_coord);
            assert_eq!(second.inner.coord_path, canonical_coord);
            assert!(Arc::ptr_eq(&first.inner.file, &second.inner.file));
            assert!(canonical_coord.exists());
            assert!(
                !coord_path_for_db(&alias_path).exists(),
                "opening through a symlink must not create an alias sidecar"
            );
        }
    }

    #[test]
    fn same_process_readers_reserve_distinct_slots_and_reuse_only_after_unlock() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let db_path = temp.path().join("local-reader-slot-reservation.ddb");
        let header_path = temp.path().join("database-header.bin");
        let attempted_path = temp.path().join("external-reader-attempted");
        let registered_path = temp.path().join("external-reader-registered");
        let release_path = temp.path().join("external-reader-release");
        let vfs = VfsHandle::for_path(&db_path);
        let database_header = DatabaseHeader::new(4096);
        std::fs::write(&header_path, database_header.encode()).expect("write database header");
        let first_coordinator = open_test_coordinator(&vfs, &db_path, &database_header, 1_000);
        let second_coordinator = open_test_coordinator(&vfs, &db_path, &database_header, 1_000);

        let first = first_coordinator
            .begin_reader(1, 100)
            .expect("register first reader");
        let second = second_coordinator
            .begin_reader(2, 200)
            .expect("register second reader");
        assert_ne!(first.slot, second.slot);
        assert!(first_coordinator
            .is_local_active_slot(first.slot)
            .expect("first local reservation"));
        assert!(first_coordinator
            .is_local_active_slot(second.slot)
            .expect("second local reservation"));

        let mut child = Command::new(std::env::current_exe().expect("current test executable"))
            .arg("--exact")
            .arg("wal::coordination::tests::cross_process_reader_admission_helper")
            .arg("--nocapture")
            .env("DDB_ADMISSION_CHILD_DB", &db_path)
            .env("DDB_ADMISSION_CHILD_HEADER", &header_path)
            .env("DDB_ADMISSION_CHILD_ATTEMPTED", &attempted_path)
            .env("DDB_ADMISSION_CHILD_REGISTERED", &registered_path)
            .env("DDB_ADMISSION_CHILD_RELEASE", &release_path)
            .spawn()
            .expect("spawn external reader probe");
        wait_for_test_path(&attempted_path, "external reader attempt");
        wait_for_test_path(&registered_path, "external reader registration");
        let external_slot: u16 = std::fs::read_to_string(&registered_path)
            .expect("read external reader slot")
            .parse()
            .expect("parse external reader slot");
        assert_ne!(external_slot, first.slot);
        assert_ne!(external_slot, second.slot);
        assert!(first_coordinator
            .is_local_active_slot(first.slot)
            .expect("first local reservation after child"));
        let retention = first_coordinator
            .scan_reader_retention()
            .expect("scan local and external readers");
        assert_eq!(
            retention.active_count,
            3,
            "external_slot={external_slot}, records={:?}",
            (0..3)
                .map(|slot| first_coordinator.read_reader_slot(slot))
                .collect::<Vec<_>>()
        );
        assert_eq!(retention.min_snapshot_lsn, Some(100));
        std::fs::write(&release_path, []).expect("release external reader");
        assert!(
            child.wait().expect("wait for external reader").success(),
            "external reader probe failed"
        );

        let released_slot = first.slot;
        drop(first);
        let replacement = first_coordinator
            .begin_reader(3, 300)
            .expect("register replacement reader");
        assert_eq!(replacement.slot, released_slot);
        let retention = first_coordinator
            .scan_reader_retention()
            .expect("scan local readers");
        assert_eq!(retention.active_count, 2);
        assert_eq!(retention.min_snapshot_lsn, Some(200));
        drop(second);
        drop(replacement);
    }

    #[test]
    fn nested_writer_guards_keep_external_writer_excluded_when_dropped_out_of_order() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let db_path = temp.path().join("nested-writer.ddb");
        let header_path = temp.path().join("database-header.bin");
        let probe_path = temp.path().join("writer-probe");
        let vfs = VfsHandle::for_path(&db_path);
        let database_header = DatabaseHeader::new(4096);
        std::fs::write(&header_path, database_header.encode()).expect("write database header");
        let coordinator = open_test_coordinator(&vfs, &db_path, &database_header, 1_000);

        let outer = coordinator.lock_writer().expect("lock outer writer");
        let inner = coordinator.lock_writer().expect("lock nested writer");
        drop(outer);
        {
            let state = coordinator
                .inner
                .admission_gate
                .writer_state
                .lock()
                .expect("writer state");
            assert_eq!(state.recursion, 1);
            assert!(state.os_lock.is_some());
        }
        assert_eq!(
            run_external_writer_probe(&db_path, &header_path, &probe_path),
            "busy"
        );
        drop(inner);
        let state = coordinator
            .inner
            .admission_gate
            .writer_state
            .lock()
            .expect("writer state");
        assert_eq!(state.recursion, 0);
        assert!(state.owner_thread.is_none());
        assert!(state.os_lock.is_none());
        drop(state);
        assert_eq!(coordinator.snapshot().expect("header").writer_owner_pid, 0);

        let moved = coordinator.lock_writer().expect("lock movable writer");
        thread::spawn(move || drop(moved))
            .join()
            .expect("drop writer on another thread");
        let state = coordinator
            .inner
            .admission_gate
            .writer_state
            .lock()
            .expect("writer state after moved drop");
        assert!(state.owner_thread.is_none());
        assert!(state.os_lock.is_none());
        drop(state);
        assert_eq!(coordinator.snapshot().expect("header").writer_owner_pid, 0);
    }

    #[test]
    fn second_local_writer_waits_without_weakening_external_exclusion() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let db_path = temp.path().join("multithread-writer.ddb");
        let header_path = temp.path().join("database-header.bin");
        let probe_path = temp.path().join("writer-probe");
        let vfs = VfsHandle::for_path(&db_path);
        let database_header = DatabaseHeader::new(4096);
        std::fs::write(&header_path, database_header.encode()).expect("write database header");
        let coordinator = open_test_coordinator(&vfs, &db_path, &database_header, 2_000);
        let first = coordinator.lock_writer().expect("lock first writer");

        let waiting_coordinator = coordinator.clone();
        let (acquired_tx, acquired_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let waiter = thread::spawn(move || {
            let writer = waiting_coordinator
                .lock_writer()
                .expect("lock second local writer");
            acquired_tx.send(()).expect("publish local acquisition");
            release_rx.recv().expect("wait for local writer release");
            drop(writer);
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let waiting = coordinator
                .inner
                .admission_gate
                .writer_state
                .lock()
                .expect("writer state")
                .waiting;
            if waiting == 1 {
                break;
            }
            assert!(Instant::now() < deadline, "local writer did not wait");
            thread::yield_now();
        }
        assert!(matches!(
            acquired_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        assert_eq!(
            run_external_writer_probe(&db_path, &header_path, &probe_path),
            "busy"
        );

        drop(first);
        acquired_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("second local writer acquired");
        std::fs::remove_file(&probe_path).expect("remove first probe result");
        assert_eq!(
            run_external_writer_probe(&db_path, &header_path, &probe_path),
            "busy"
        );
        release_tx.send(()).expect("release second local writer");
        waiter.join().expect("writer waiter");
        assert_eq!(coordinator.snapshot().expect("header").writer_owner_pid, 0);
    }

    #[test]
    fn local_writer_contention_preserves_busy_timeout_metrics_and_callbacks() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        for timeout_ms in [0, 20] {
            let db_path = temp.path().join(format!("writer-timeout-{timeout_ms}.ddb"));
            let vfs = VfsHandle::for_path(&db_path);
            let database_header = DatabaseHeader::new(4096);
            let owner = open_test_coordinator(&vfs, &db_path, &database_header, timeout_ms);
            let waiter = open_test_coordinator(&vfs, &db_path, &database_header, timeout_ms);
            let writer = owner.lock_writer().expect("lock writer owner");
            let callbacks = Arc::new(Mutex::new(Vec::<(bool, String)>::new()));
            let callback_rows = Arc::clone(&callbacks);
            waiter.set_lock_wait_callback(Some(Arc::new(move |checkpoint, _elapsed, status| {
                callback_rows
                    .lock()
                    .expect("callback rows")
                    .push((checkpoint, status.to_string()));
            })));

            let wait_thread = thread::spawn(move || {
                let started = Instant::now();
                let error = waiter
                    .lock_writer()
                    .expect_err("local writer contention must not recurse across threads");
                let metrics = waiter.lock_metrics_snapshot().expect("waiter metrics");
                (error, started.elapsed(), metrics.writer_lock_timeouts)
            });
            let (error, elapsed, timeouts) = wait_thread.join().expect("writer timeout thread");
            drop(writer);
            if timeout_ms == 0 {
                assert!(matches!(error, DbError::Busy { .. }));
            } else {
                assert!(matches!(error, DbError::Timeout { .. }));
                assert!(elapsed >= Duration::from_millis(10));
            }
            assert_eq!(timeouts, 1);
            assert_eq!(
                callbacks.lock().expect("callback rows").as_slice(),
                &[(
                    false,
                    if timeout_ms == 0 { "busy" } else { "timeout" }.to_string()
                )]
            );
        }
    }

    #[test]
    fn cross_process_writer_probe_helper() {
        let Some(db_path) = std::env::var_os("DDB_WRITER_PROBE_DB").map(PathBuf::from) else {
            return;
        };
        let header_path = PathBuf::from(
            std::env::var_os("DDB_WRITER_PROBE_HEADER").expect("writer probe header"),
        );
        let result_path = PathBuf::from(
            std::env::var_os("DDB_WRITER_PROBE_RESULT").expect("writer probe result"),
        );
        let encoded = std::fs::read(header_path).expect("read writer probe header");
        let encoded: &[u8; crate::storage::header::DB_HEADER_SIZE] = encoded
            .as_slice()
            .try_into()
            .expect("database header length");
        let database_header = DatabaseHeader::decode(encoded).expect("decode database header");
        let vfs = VfsHandle::for_path(&db_path);
        let coordinator = open_test_coordinator(&vfs, &db_path, &database_header, 0);
        let result = match coordinator.lock_writer() {
            Err(DbError::Busy { .. }) | Err(DbError::Timeout { .. }) => "busy",
            Ok(writer) => {
                drop(writer);
                "acquired"
            }
            Err(error) => panic!("unexpected external writer probe error: {error}"),
        };
        std::fs::write(result_path, result).expect("write writer probe result");
    }

    #[test]
    fn cross_process_range_lock_holder_helper() {
        let Some(db_path) = std::env::var_os("DDB_RANGE_HOLDER_DB").map(PathBuf::from) else {
            return;
        };
        let header_path = PathBuf::from(
            std::env::var_os("DDB_RANGE_HOLDER_HEADER").expect("range holder header"),
        );
        let offset: u64 = std::env::var("DDB_RANGE_HOLDER_OFFSET")
            .expect("range holder offset")
            .parse()
            .expect("parse range holder offset");
        let held_path = PathBuf::from(
            std::env::var_os("DDB_RANGE_HOLDER_HELD").expect("range holder held path"),
        );
        let release_path = PathBuf::from(
            std::env::var_os("DDB_RANGE_HOLDER_RELEASE").expect("range holder release path"),
        );
        let encoded = std::fs::read(header_path).expect("read range holder header");
        let encoded: &[u8; crate::storage::header::DB_HEADER_SIZE] = encoded
            .as_slice()
            .try_into()
            .expect("database header length");
        let database_header = DatabaseHeader::decode(encoded).expect("decode database header");
        let vfs = VfsHandle::for_path(&db_path);
        let coordinator = open_test_coordinator(&vfs, &db_path, &database_header, 5_000);
        let lock = lock_range_with_timeout(
            coordinator.inner.file.as_ref(),
            offset,
            1,
            true,
            Some(Duration::from_secs(5)),
        )
        .expect("hold external coordination range");
        std::fs::write(held_path, []).expect("publish held range");
        wait_for_test_path(&release_path, "range holder release");
        drop(lock);
    }

    #[test]
    fn checkpoint_admission_gate_covers_empty_scan_through_publication() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let db_path = temp.path().join("checkpoint-admission-race.ddb");
        let vfs = VfsHandle::for_path(&db_path);
        let database_header = DatabaseHeader::new(4096);
        let checkpoint_coordinator = open_test_coordinator(&vfs, &db_path, &database_header, 2_000);
        let reader_coordinator = open_test_coordinator(&vfs, &db_path, &database_header, 2_000);
        assert!(Arc::ptr_eq(
            &checkpoint_coordinator.inner.file,
            &reader_coordinator.inner.file
        ));

        let checkpoint = checkpoint_coordinator
            .lock_checkpoint()
            .expect("lock checkpoint");
        let retention = checkpoint_coordinator
            .scan_reader_retention()
            .expect("scan before publication");
        assert_eq!(retention.active_count, 0);
        assert!(!retention.truncation_blocked);

        let (registered_tx, registered_rx) = mpsc::sync_channel(1);
        let reader_thread = thread::spawn(move || {
            let admission = reader_coordinator
                .lock_reader_admission()
                .expect("admit reader");
            let reader = reader_coordinator
                .begin_reader(91, 777)
                .expect("register reader");
            drop(admission);
            registered_tx.send(reader).expect("send reader guard");
        });
        wait_for_admission_counts(&checkpoint_coordinator, 1, 0);
        assert!(matches!(
            registered_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        checkpoint_coordinator
            .publish_checkpoint(777, 0)
            .expect("publish checkpoint while admission remains closed");
        drop(checkpoint);

        let reader = registered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("reader admitted after checkpoint publication");
        reader_thread.join().expect("reader thread");
        let retention = checkpoint_coordinator
            .scan_reader_retention()
            .expect("scan registered reader");
        assert_eq!(retention.active_count, 1);
        assert_eq!(retention.min_snapshot_lsn, Some(777));
        drop(reader);
    }

    #[test]
    fn cross_process_reader_cannot_register_during_checkpoint_publication() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let db_path = temp.path().join("cross-process-admission-race.ddb");
        let header_path = temp.path().join("database-header.bin");
        let attempted_path = temp.path().join("reader-attempted");
        let registered_path = temp.path().join("reader-registered");
        let release_path = temp.path().join("release-reader");
        let vfs = VfsHandle::for_path(&db_path);
        let database_header = DatabaseHeader::new(4096);
        std::fs::write(&header_path, database_header.encode()).expect("write database header");
        let coordinator = open_test_coordinator(&vfs, &db_path, &database_header, 5_000);

        let checkpoint = coordinator.lock_checkpoint().expect("lock checkpoint");
        let retention = coordinator
            .scan_reader_retention()
            .expect("scan before child admission");
        assert_eq!(retention.active_count, 0);
        assert!(!retention.truncation_blocked);

        let mut child = Command::new(std::env::current_exe().expect("current test executable"))
            .arg("--exact")
            .arg("wal::coordination::tests::cross_process_reader_admission_helper")
            .arg("--nocapture")
            .env("DDB_ADMISSION_CHILD_DB", &db_path)
            .env("DDB_ADMISSION_CHILD_HEADER", &header_path)
            .env("DDB_ADMISSION_CHILD_ATTEMPTED", &attempted_path)
            .env("DDB_ADMISSION_CHILD_REGISTERED", &registered_path)
            .env("DDB_ADMISSION_CHILD_RELEASE", &release_path)
            .spawn()
            .expect("spawn reader process");
        wait_for_test_path(&attempted_path, "child reader admission attempt");
        thread::sleep(Duration::from_millis(100));
        assert!(
            !registered_path.exists(),
            "external reader registered while checkpoint admission was closed"
        );
        assert!(
            child.try_wait().expect("poll child").is_none(),
            "external reader process exited while checkpoint admission was closed"
        );

        coordinator
            .publish_checkpoint(777, 0)
            .expect("publish checkpoint before reopening admission");
        drop(checkpoint);
        wait_for_test_path(&registered_path, "child reader registration");
        let retention = coordinator
            .scan_reader_retention()
            .expect("scan external reader");
        assert_eq!(retention.active_count, 1);
        assert_eq!(retention.min_snapshot_lsn, Some(777));

        std::fs::write(&release_path, []).expect("release child reader");
        let status = child.wait().expect("wait for reader process");
        assert!(status.success(), "reader process failed: {status}");
    }

    #[test]
    fn cross_process_reader_admission_helper() {
        let Some(db_path) = std::env::var_os("DDB_ADMISSION_CHILD_DB").map(PathBuf::from) else {
            return;
        };
        let header_path = PathBuf::from(
            std::env::var_os("DDB_ADMISSION_CHILD_HEADER").expect("child header path"),
        );
        let attempted_path = PathBuf::from(
            std::env::var_os("DDB_ADMISSION_CHILD_ATTEMPTED").expect("child attempted path"),
        );
        let registered_path = PathBuf::from(
            std::env::var_os("DDB_ADMISSION_CHILD_REGISTERED").expect("child registered path"),
        );
        let release_path = PathBuf::from(
            std::env::var_os("DDB_ADMISSION_CHILD_RELEASE").expect("child release path"),
        );
        let encoded = std::fs::read(header_path).expect("read child database header");
        let encoded: &[u8; crate::storage::header::DB_HEADER_SIZE] = encoded
            .as_slice()
            .try_into()
            .expect("database header length");
        let database_header = DatabaseHeader::decode(encoded).expect("decode database header");
        let vfs = VfsHandle::for_path(&db_path);
        let coordinator = open_test_coordinator(&vfs, &db_path, &database_header, 5_000);

        std::fs::write(attempted_path, []).expect("publish child attempt");
        let admission = coordinator
            .lock_reader_admission()
            .expect("admit child reader");
        let reader = coordinator
            .begin_reader(92, 777)
            .expect("register child reader");
        drop(admission);
        // The parent treats the registration path's existence as a complete value.
        let pending_registration_path = registered_path.with_extension("pending");
        std::fs::write(&pending_registration_path, reader.slot.to_string())
            .expect("stage child registration");
        std::fs::rename(pending_registration_path, registered_path)
            .expect("publish child registration");
        wait_for_test_path(&release_path, "parent reader release");
        drop(reader);
    }

    #[test]
    fn locked_empty_reader_window_blocks_checkpoint_until_registration_can_finish() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let db_path = temp.path().join("locked-empty-reader.ddb");
        let vfs = VfsHandle::for_path(&db_path);
        let database_header = DatabaseHeader::new(4096);
        let reader_coordinator = open_test_coordinator(&vfs, &db_path, &database_header, 2_000);
        let checkpoint_coordinator = open_test_coordinator(&vfs, &db_path, &database_header, 2_000);

        // This is the intentional refresh-to-slot interval: the reader owns
        // admission but has not written any slot record yet.
        let admission = reader_coordinator
            .lock_reader_admission()
            .expect("lock reader admission");
        let (checkpoint_tx, checkpoint_rx) = mpsc::sync_channel(1);
        let checkpoint_thread = thread::spawn(move || {
            checkpoint_tx
                .send(checkpoint_coordinator.lock_checkpoint())
                .expect("send checkpoint result");
        });
        wait_for_admission_counts(&reader_coordinator, 0, 1);
        assert!(matches!(
            checkpoint_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        drop(admission);
        let checkpoint = checkpoint_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("checkpoint result")
            .expect("checkpoint proceeds after reader window closes");
        drop(checkpoint);
        checkpoint_thread.join().expect("checkpoint thread");
    }

    #[test]
    fn reader_admission_gate_honors_zero_and_nonzero_timeouts() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let database_header = DatabaseHeader::new(4096);

        let zero_path = temp.path().join("zero-timeout.ddb");
        let vfs = VfsHandle::for_path(&zero_path);
        let zero = open_test_coordinator(&vfs, &zero_path, &database_header, 0);
        let zero_peer = open_test_coordinator(&vfs, &zero_path, &database_header, 0);
        let shared = zero.lock_reader_admission().expect("lock shared gate");
        let error = zero_peer
            .lock_checkpoint()
            .expect_err("zero timeout must report a busy gate");
        assert!(matches!(error, DbError::Busy { .. }));
        drop(shared);

        let timed_path = temp.path().join("nonzero-timeout.ddb");
        let timed = open_test_coordinator(&vfs, &timed_path, &database_header, 20);
        let timed_peer = open_test_coordinator(&vfs, &timed_path, &database_header, 20);
        let shared = timed.lock_reader_admission().expect("lock shared gate");
        let started = Instant::now();
        let error = timed_peer
            .lock_checkpoint()
            .expect_err("nonzero timeout must expire while the gate is held");
        assert!(matches!(error, DbError::Timeout { .. }));
        assert!(started.elapsed() >= Duration::from_millis(10));
        drop(shared);
    }

    #[test]
    fn admission_waiters_keep_independent_deadlines_while_os_leader_waits() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let db_path = temp.path().join("admission-os-leader.ddb");
        let header_path = temp.path().join("database-header.bin");
        let held_path = temp.path().join("range-held");
        let external_release_path = temp.path().join("external-release");
        let vfs = VfsHandle::for_path(&db_path);
        let database_header = DatabaseHeader::new(4096);
        std::fs::write(&header_path, database_header.encode()).expect("write database header");
        let long = open_test_coordinator(&vfs, &db_path, &database_header, 2_000);
        let zero = open_test_coordinator(&vfs, &db_path, &database_header, 0);
        let short = open_test_coordinator(&vfs, &db_path, &database_header, 25);
        let mut external = spawn_external_range_holder(
            &db_path,
            &header_path,
            READER_ADMISSION_LOCK_OFFSET,
            &held_path,
            &external_release_path,
        );
        wait_for_test_path(&held_path, "external admission lock");

        let (acquired_tx, acquired_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let long_waiter = thread::spawn(move || {
            let guard = long.lock_reader_admission().expect("long admission waiter");
            acquired_tx.send(()).expect("publish admission acquisition");
            release_rx.recv().expect("wait to release admission");
            drop(guard);
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if zero
                .inner
                .admission_gate
                .state
                .lock()
                .expect("admission state")
                .pending_os_attempt
                .is_some()
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "admission OS leader did not start"
            );
            thread::yield_now();
        }

        let started = Instant::now();
        let error = zero
            .lock_reader_admission()
            .expect_err("zero-timeout admission waiter must not block behind OS leader");
        assert!(matches!(error, DbError::Busy { .. }));
        assert!(started.elapsed() < Duration::from_millis(250));

        let started = Instant::now();
        let error = short
            .lock_reader_admission()
            .expect_err("short admission waiter must retain its own deadline");
        assert!(matches!(error, DbError::Timeout { .. }));
        assert!(started.elapsed() >= Duration::from_millis(10));
        assert!(started.elapsed() < Duration::from_millis(500));

        std::fs::write(&external_release_path, []).expect("release external admission lock");
        assert!(external.wait().expect("wait external holder").success());
        acquired_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("long admission waiter acquired");
        release_tx.send(()).expect("release long admission waiter");
        long_waiter.join().expect("long admission waiter thread");
    }

    #[test]
    fn writer_waiters_keep_independent_deadlines_while_os_leader_waits() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let db_path = temp.path().join("writer-os-leader.ddb");
        let header_path = temp.path().join("database-header.bin");
        let held_path = temp.path().join("range-held");
        let external_release_path = temp.path().join("external-release");
        let vfs = VfsHandle::for_path(&db_path);
        let database_header = DatabaseHeader::new(4096);
        std::fs::write(&header_path, database_header.encode()).expect("write database header");
        let long = open_test_coordinator(&vfs, &db_path, &database_header, 2_000);
        let zero = open_test_coordinator(&vfs, &db_path, &database_header, 0);
        let short = open_test_coordinator(&vfs, &db_path, &database_header, 25);
        let mut external = spawn_external_range_holder(
            &db_path,
            &header_path,
            WRITER_LOCK_OFFSET,
            &held_path,
            &external_release_path,
        );
        wait_for_test_path(&held_path, "external writer lock");

        let (acquired_tx, acquired_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let long_waiter = thread::spawn(move || {
            let guard = long.lock_writer().expect("long writer waiter");
            acquired_tx.send(()).expect("publish writer acquisition");
            release_rx.recv().expect("wait to release writer");
            drop(guard);
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if zero
                .inner
                .admission_gate
                .writer_state
                .lock()
                .expect("writer state")
                .pending_os_attempt
                .is_some()
            {
                break;
            }
            assert!(Instant::now() < deadline, "writer OS leader did not start");
            thread::yield_now();
        }

        let started = Instant::now();
        let error = zero
            .lock_writer()
            .expect_err("zero-timeout writer must not block behind OS leader");
        assert!(matches!(error, DbError::Busy { .. }));
        assert!(started.elapsed() < Duration::from_millis(250));

        let started = Instant::now();
        let error = short
            .lock_writer()
            .expect_err("short writer must retain its own deadline");
        assert!(matches!(error, DbError::Timeout { .. }));
        assert!(started.elapsed() >= Duration::from_millis(10));
        assert!(started.elapsed() < Duration::from_millis(500));

        std::fs::write(&external_release_path, []).expect("release external writer lock");
        assert!(external.wait().expect("wait external holder").success());
        acquired_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("long writer waiter acquired");
        release_tx.send(()).expect("release long writer waiter");
        long_waiter.join().expect("long writer waiter thread");
    }

    #[test]
    fn retention_probe_does_not_clear_reader_registered_after_stale_decode() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let db_path = temp.path().join("retention-stale-decode-race.ddb");
        let vfs = VfsHandle::for_path(&db_path);
        let database_header = DatabaseHeader::new(4096);
        let scanner = open_test_coordinator(&vfs, &db_path, &database_header, 1_000);
        let registrar = open_test_coordinator(&vfs, &db_path, &database_header, 1_000);
        scanner
            .write_reader_slot(0, stale_active_reader_record(9_999))
            .expect("write stale active reader");
        let (decoded_rx, resume_tx) = pause_before_reader_slot_probe(&scanner, 0);

        let scan_coordinator = scanner.clone();
        let scan_thread = thread::spawn(move || scan_coordinator.scan_reader_retention());
        decoded_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("retention scan decoded stale slot");
        let reader = registrar
            .begin_reader(100, 123)
            .expect("register reader after stale decode");
        assert_eq!(reader.slot, 0);
        resume_tx
            .send(())
            .expect("resume retention ownership probe");
        let retention = scan_thread
            .join()
            .expect("retention scan thread")
            .expect("retention scan result");
        assert_eq!(retention.active_count, 1);
        let record = scanner.read_reader_slot(0).expect("read live slot");
        assert_eq!(record.state, READER_STATE_ACTIVE);
        assert_eq!(record.reader_id, 100);
        assert_eq!(record.snapshot_lsn, 123);
        assert!(scanner.is_local_active_slot(0).expect("live reservation"));
        *scanner
            .inner
            .stale_probe_callback
            .lock()
            .expect("clear stale probe callback") = None;
        drop(reader);
    }

    #[test]
    fn diagnostic_probe_reports_reader_registered_after_stale_decode_as_active() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let db_path = temp.path().join("diagnostic-stale-decode-race.ddb");
        let vfs = VfsHandle::for_path(&db_path);
        let database_header = DatabaseHeader::new(4096);
        let scanner = open_test_coordinator(&vfs, &db_path, &database_header, 1_000);
        let registrar = open_test_coordinator(&vfs, &db_path, &database_header, 1_000);
        scanner
            .write_reader_slot(0, stale_active_reader_record(8_888))
            .expect("write stale active reader");
        let (decoded_rx, resume_tx) = pause_before_reader_slot_probe(&scanner, 0);

        let scan_coordinator = scanner.clone();
        let scan_thread = thread::spawn(move || scan_coordinator.reader_slot_snapshots());
        decoded_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("diagnostic scan decoded stale slot");
        let reader = registrar
            .begin_reader(101, 321)
            .expect("register reader after stale diagnostic decode");
        assert_eq!(reader.slot, 0);
        resume_tx
            .send(())
            .expect("resume diagnostic ownership probe");
        let rows = scan_thread
            .join()
            .expect("diagnostic scan thread")
            .expect("diagnostic scan result");
        let row = rows
            .iter()
            .find(|row| row.slot_id == 0)
            .expect("slot zero diagnostic row");
        assert_eq!(row.state, "active");
        assert!(row.retention_blocking);
        let record = scanner.read_reader_slot(0).expect("read live slot");
        assert_eq!(record.state, READER_STATE_ACTIVE);
        assert_eq!(record.reader_id, 101);
        assert_eq!(record.snapshot_lsn, 321);
        assert!(scanner.is_local_active_slot(0).expect("live reservation"));
        *scanner
            .inner
            .stale_probe_callback
            .lock()
            .expect("clear stale probe callback") = None;
        drop(reader);
    }

    #[test]
    fn failed_stale_slot_clear_releases_probe_lock_and_local_reservation() {
        let _failpoint_guard = crate::vfs::faulty::test_failpoint_lock()
            .lock()
            .expect("failpoint test lock");
        crate::vfs::faulty::clear_failpoints().expect("clear failpoints");
        let temp = tempfile::TempDir::new().expect("tempdir");
        let db_path = temp.path().join("stale-probe-clear-failure.ddb");
        let os_vfs: Arc<dyn crate::vfs::Vfs> = Arc::new(crate::vfs::os::OsVfs);
        let vfs = VfsHandle::from_vfs(Arc::new(crate::vfs::faulty::FaultyVfs::wrap(os_vfs)));
        let database_header = DatabaseHeader::new(4096);
        let coordinator = open_test_coordinator(&vfs, &db_path, &database_header, 1_000);
        coordinator
            .write_reader_slot(0, stale_active_reader_record(777))
            .expect("write stale reader before failpoint");

        crate::vfs::faulty::install_failpoint(crate::vfs::faulty::Failpoint {
            label: "coord.write".to_string(),
            trigger_on: 1,
            action: crate::vfs::faulty::FailAction::Error,
        })
        .expect("install stale clear failpoint");
        let error = coordinator
            .scan_reader_retention()
            .expect_err("stale reader clear must fail");
        assert!(matches!(error, DbError::Io { .. }));
        assert!(!coordinator
            .is_local_active_slot(0)
            .expect("probe reservation released"));

        crate::vfs::faulty::clear_failpoints().expect("clear stale clear failpoint");
        let reader = coordinator
            .begin_reader(102, 456)
            .expect("probe OS lock and reservation must be reusable");
        assert_eq!(reader.slot, 0);
        drop(reader);
        crate::vfs::faulty::clear_failpoints().expect("final failpoint cleanup");
    }

    #[test]
    fn reader_write_and_writer_owner_publish_failures_release_local_ownership() {
        let _failpoint_guard = crate::vfs::faulty::test_failpoint_lock()
            .lock()
            .expect("failpoint test lock");
        crate::vfs::faulty::clear_failpoints().expect("clear failpoints");
        let temp = tempfile::TempDir::new().expect("tempdir");
        let db_path = temp.path().join("coordination-ownership-failures.ddb");
        let os_vfs: Arc<dyn crate::vfs::Vfs> = Arc::new(crate::vfs::os::OsVfs);
        let vfs = VfsHandle::from_vfs(Arc::new(crate::vfs::faulty::FaultyVfs::wrap(os_vfs)));
        let database_header = DatabaseHeader::new(4096);
        let coordinator = open_test_coordinator(&vfs, &db_path, &database_header, 1_000);

        crate::vfs::faulty::install_failpoint(crate::vfs::faulty::Failpoint {
            label: "coord.write".to_string(),
            trigger_on: 1,
            action: crate::vfs::faulty::FailAction::Error,
        })
        .expect("install reader write failpoint");
        let error = coordinator
            .begin_reader(1, 100)
            .expect_err("initializing write must fail");
        assert!(matches!(error, DbError::Io { .. }));
        assert!(!coordinator.is_local_active_slot(0).expect("slot state"));
        crate::vfs::faulty::clear_failpoints().expect("clear reader failpoint");
        let reader = coordinator
            .begin_reader(2, 200)
            .expect("failed reservation must be reusable");
        assert_eq!(reader.slot, 0);
        drop(reader);

        crate::vfs::faulty::install_failpoint(crate::vfs::faulty::Failpoint {
            label: "coord.write".to_string(),
            trigger_on: 1,
            action: crate::vfs::faulty::FailAction::Error,
        })
        .expect("install writer owner failpoint");
        let error = coordinator
            .lock_writer()
            .expect_err("writer owner publication must fail");
        assert!(matches!(error, DbError::Io { .. }));
        crate::vfs::faulty::clear_failpoints().expect("clear writer failpoint");
        {
            let state = coordinator
                .inner
                .admission_gate
                .writer_state
                .lock()
                .expect("writer state");
            assert!(state.owner_thread.is_none());
            assert_eq!(state.recursion, 0);
            assert!(state.os_lock.is_none());
        }
        assert_eq!(coordinator.snapshot().expect("header").writer_owner_pid, 0);
        let writer = coordinator
            .lock_writer()
            .expect("writer lock reusable after publish failure");
        drop(writer);
        crate::vfs::faulty::clear_failpoints().expect("final failpoint cleanup");
    }

    #[test]
    fn initializing_reader_slot_blocks_conservatively_until_ownership_is_reclaimable() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let db_path = temp.path().join("initializing-reader.ddb");
        let vfs = VfsHandle::for_path(&db_path);
        let database_header = DatabaseHeader::new(4096);
        let coordinator = open_test_coordinator(&vfs, &db_path, &database_header, 1_000);
        let slot = 0;
        let key = ReaderSlotKey {
            coord_path: coordinator.inner.coord_path.clone(),
            slot,
        };
        let mut empty_reservation = LocalReaderSlotReservation::try_acquire(key.clone())
            .expect("reserve empty slot")
            .expect("empty slot available");
        let retention = coordinator
            .scan_reader_retention()
            .expect("scan locally reserved empty slot");
        assert_eq!(retention.active_count, 0);
        assert!(retention.truncation_blocked);
        empty_reservation.release();

        coordinator
            .write_reader_slot(
                slot,
                ReaderSlotRecord {
                    state: READER_STATE_INITIALIZING,
                    generation: 1,
                    process_id: coordinator.inner.process_id,
                    process_token: coordinator.inner.process_token,
                    reader_id: 99,
                    snapshot_lsn: 0,
                    started_unix_ms: now_unix_ms(),
                },
            )
            .expect("write initializing slot");
        active_reader_slots()
            .lock()
            .expect("active slot registry")
            .insert(key.clone());

        let retention = coordinator
            .scan_reader_retention()
            .expect("scan live initializer");
        assert_eq!(retention.active_count, 0);
        assert!(retention.truncation_blocked);

        active_reader_slots()
            .lock()
            .expect("active slot registry")
            .remove(&key);
        let retention = coordinator
            .scan_reader_retention()
            .expect("reclaim abandoned initializer");
        assert_eq!(retention.active_count, 0);
        assert!(!retention.truncation_blocked);
        assert_eq!(
            coordinator
                .read_reader_slot(slot)
                .expect("read reclaimed slot")
                .state,
            READER_STATE_EMPTY
        );
    }

    #[test]
    fn reader_retention_bulk_scan_uses_one_read_and_preserves_corruption_blocking() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let db_path = temp.path().join("bulk-reader-scan.ddb");
        let counts = Arc::new(CoordinationIoCounts::default());
        let vfs = VfsHandle::from_vfs(Arc::new(CountingCoordinationVfs::new(Arc::clone(&counts))));
        let database_header = DatabaseHeader::new(4096);
        let coordinator = ProcessCoordinator::open(
            &vfs,
            &db_path,
            &database_header,
            ProcessCoordinationMode::Required,
            1_000,
        )
        .expect("open coordinator")
        .expect("required coordinator");
        let reader = coordinator
            .begin_reader(44, 1_234)
            .expect("register local reader");

        counts.reset();
        let active = coordinator
            .scan_reader_retention()
            .expect("scan active readers");
        assert_eq!(active.active_count, 1);
        assert_eq!(active.min_snapshot_lsn, Some(1_234));
        assert!(!active.truncation_blocked);
        assert_eq!(counts.reads.load(Ordering::Acquire), 1);
        assert_eq!(counts.locks.load(Ordering::Acquire), 0);

        let corrupt_slot = if reader.slot == 0 { 1 } else { 0 };
        let mut corrupt = encode_reader_slot(empty_reader_slot_record());
        corrupt[8] ^= 0xA5;
        write_all_at(
            coordinator.inner.file.as_ref(),
            reader_record_offset(corrupt_slot),
            &corrupt,
        )
        .expect("write corrupt reader slot");

        counts.reset();
        let conservative = coordinator
            .scan_reader_retention()
            .expect("scan with corrupt slot");
        assert_eq!(conservative.active_count, 1);
        assert_eq!(conservative.min_snapshot_lsn, Some(1_234));
        assert!(conservative.truncation_blocked);
        assert_eq!(counts.reads.load(Ordering::Acquire), 1);

        write_all_at(
            coordinator.inner.file.as_ref(),
            reader_record_offset(corrupt_slot),
            &encode_reader_slot(empty_reader_slot_record()),
        )
        .expect("restore empty reader slot");
        drop(reader);
        counts.reset();
        let empty = coordinator
            .scan_reader_retention()
            .expect("scan empty readers");
        assert_eq!(empty.active_count, 0);
        assert_eq!(empty.min_snapshot_lsn, None);
        assert!(!empty.truncation_blocked);
        assert_eq!(counts.reads.load(Ordering::Acquire), 1);
    }

    #[test]
    fn recovered_wal_publish_skips_only_the_unchanged_header_write() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let db_path = temp.path().join("publish-recovered.ddb");
        let counts = Arc::new(CoordinationIoCounts::default());
        let vfs = VfsHandle::from_vfs(Arc::new(CountingCoordinationVfs::new(Arc::clone(&counts))));
        let database_header = DatabaseHeader::new(4096);
        let coordinator = ProcessCoordinator::open(
            &vfs,
            &db_path,
            &database_header,
            ProcessCoordinationMode::Required,
            1_000,
        )
        .expect("open coordinator")
        .expect("required coordinator");
        let initial = coordinator.snapshot().expect("initial snapshot");
        assert_eq!(initial.database_id, database_header.database_id);

        counts.reset();
        let unchanged = coordinator
            .publish_recovered_wal(initial.wal_end_lsn, initial.checkpoint_lsn)
            .expect("publish unchanged recovery state");
        assert_eq!(unchanged, initial);
        assert_eq!(counts.locks.load(Ordering::Acquire), 1);
        assert_eq!(counts.reads.load(Ordering::Acquire), 1);
        assert_eq!(counts.writes.load(Ordering::Acquire), 0);

        counts.reset();
        let changed = coordinator
            .publish_recovered_wal(512, initial.checkpoint_lsn)
            .expect("publish changed recovery state");
        assert_eq!(changed.database_id, database_header.database_id);
        assert_eq!(changed.wal_end_lsn, 512);
        assert_eq!(
            changed.coordinator_generation,
            initial.coordinator_generation + 1
        );
        assert_eq!(changed.wal_generation, initial.wal_generation + 1);
        assert_eq!(counts.locks.load(Ordering::Acquire), 1);
        assert_eq!(counts.reads.load(Ordering::Acquire), 1);
        assert_eq!(counts.writes.load(Ordering::Acquire), 1);
        drop(coordinator);

        let recovered = ProcessCoordinator::open(
            &vfs,
            &db_path,
            &database_header,
            ProcessCoordinationMode::Required,
            1_000,
        )
        .expect("reopen coordinator")
        .expect("required coordinator");
        let recovered = recovered.snapshot().expect("recovered snapshot");
        assert_eq!(recovered.database_id, database_header.database_id);
        assert_eq!(recovered.wal_end_lsn, 512);
    }

    #[test]
    fn recovered_wal_publish_failpoint_distinguishes_unchanged_and_changed_state() {
        let _failpoint_guard = crate::vfs::faulty::test_failpoint_lock()
            .lock()
            .expect("failpoint test lock");
        crate::vfs::faulty::clear_failpoints().expect("clear failpoints");

        let temp = tempfile::TempDir::new().expect("tempdir");
        let db_path = temp.path().join("publish-recovered-failpoint.ddb");
        let counts = Arc::new(CoordinationIoCounts::default());
        let counting: Arc<dyn crate::vfs::Vfs> = Arc::new(CountingCoordinationVfs::new(counts));
        let vfs = VfsHandle::from_vfs(Arc::new(crate::vfs::faulty::FaultyVfs::wrap(counting)));
        let database_header = DatabaseHeader::new(4096);
        let coordinator = ProcessCoordinator::open(
            &vfs,
            &db_path,
            &database_header,
            ProcessCoordinationMode::Required,
            1_000,
        )
        .expect("open coordinator")
        .expect("required coordinator");

        crate::vfs::faulty::install_failpoint(crate::vfs::faulty::Failpoint {
            label: "coord.write".to_string(),
            trigger_on: 1,
            action: crate::vfs::faulty::FailAction::Error,
        })
        .expect("install coordination write failpoint");
        coordinator
            .publish_recovered_wal(0, 0)
            .expect("unchanged publication should not attempt a write");
        assert!(crate::vfs::faulty::failpoint_logs()
            .expect("read failpoint log")
            .iter()
            .all(|entry| entry.label != "coord.write"));

        let error = coordinator
            .publish_recovered_wal(512, 0)
            .expect_err("changed publication must attempt the header write");
        assert!(matches!(error, DbError::Io { .. }));
        assert!(crate::vfs::faulty::failpoint_logs()
            .expect("read failpoint log")
            .iter()
            .any(|entry| entry.label == "coord.write" && entry.outcome == "error"));

        crate::vfs::faulty::clear_failpoints().expect("clear failpoints");
    }
}

//! Shared WAL acquisition keyed by canonical database path.
//!
//! Implements:
//! - design/adr/0117-shared-wal-registry.md

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::thread;

use crate::alloc::EngineAllocHandle;
use crate::config::DbConfig;
use crate::error::Result;
use crate::storage::PagerHandle;
use crate::vfs::{FileKind, OpenMode, VfsHandle};

use super::index_sidecar::{WalIndexBackendKind, WalIndexSidecar};
use super::reader_registry::ReaderRegistry;
use super::recovery;
use super::{AutoCheckpointConfig, SharedWalInner, WalHandle, WalWriteState};

pub(crate) fn acquire(
    vfs: &VfsHandle,
    db_path: &Path,
    config: &DbConfig,
    pager: &PagerHandle,
) -> Result<WalHandle> {
    if vfs.is_memory() {
        return build_handle(vfs, None, db_path, config, pager);
    }

    let canonical_path = vfs.canonicalize_path(db_path)?;
    let registry = registry();
    {
        let registry_guard = registry
            .lock()
            .expect("shared wal registry lock should not be poisoned");
        if let Some(existing) = registry_guard.get(&canonical_path).and_then(Weak::upgrade) {
            while existing.checkpoint_pending.load(Ordering::SeqCst) {
                thread::yield_now();
            }
            return Ok(WalHandle { inner: existing });
        }
    }

    let handle = build_handle(
        vfs,
        Some(canonical_path.clone()),
        &canonical_path,
        config,
        pager,
    )?;
    registry
        .lock()
        .expect("shared wal registry lock should not be poisoned")
        .insert(canonical_path, Arc::downgrade(&handle.inner));
    Ok(handle)
}

fn build_handle(
    vfs: &VfsHandle,
    canonical_path: Option<PathBuf>,
    db_path: &Path,
    config: &DbConfig,
    pager: &PagerHandle,
) -> Result<WalHandle> {
    let wal_path = wal_path_for_db(db_path);
    let mode = if vfs.file_exists(&wal_path)? {
        OpenMode::OpenExisting
    } else {
        OpenMode::OpenOrCreate
    };
    let file = vfs.open(&wal_path, mode, FileKind::Wal)?;
    let backend_kind = WalIndexBackendKind::for_hot_set_pages(config.wal_index_hot_set_pages);
    let mut index_sidecar = match backend_kind {
        WalIndexBackendKind::InMemory => None,
        WalIndexBackendKind::PagedSidecar => Some(WalIndexSidecar::open(vfs, db_path)?),
    };
    let (index, end_lsn, recovered_max_page_id) = recovery::initialize_or_recover(
        &file,
        pager,
        config.page_size,
        config.wal_index_hot_set_pages,
        index_sidecar.as_mut(),
    )?;
    let allocated_len = file.file_size()?;

    let async_commit = match config.wal_sync_mode {
        crate::config::WalSyncMode::AsyncCommit { interval_ms } => Some(
            super::async_commit::AsyncCommitState::new(Arc::clone(&file), end_lsn, interval_ms),
        ),
        _ => None,
    };
    let alloc = EngineAllocHandle::default();

    let inner = Arc::new(SharedWalInner {
        canonical_path,
        file,
        page_size: config.page_size,
        sync_mode: config.wal_sync_mode,
        index: Mutex::new(index),
        index_sidecar: index_sidecar.take().map(Mutex::new),
        wal_index_hot_set_pages: config.wal_index_hot_set_pages,
        wal_end_lsn: AtomicU64::new(end_lsn),
        max_page_count: AtomicU32::new(recovered_max_page_id),
        allocated_len: AtomicU64::new(allocated_len),
        write_lock: Mutex::new(WalWriteState::new(alloc)),
        reader_registry: ReaderRegistry::default(),
        checkpoint_pending: AtomicBool::new(false),
        checkpoint_epoch: AtomicU64::new(0),
        async_commit,
        resident_versions_per_page: config.wal_resident_versions_per_page,
        auto_checkpoint: AutoCheckpointConfig::from_db_config(config),
        pages_since_checkpoint: AtomicU32::new(0),
        checkpoint_scratch: Mutex::new(Vec::new()),
        materialize_scratch: Mutex::new(Vec::with_capacity(config.page_size as usize)),
        bg_checkpointer: std::sync::OnceLock::new(),
    });

    if let Some(sidecar) = &inner.index_sidecar {
        let mut index = inner
            .index
            .lock()
            .expect("wal index lock should not be poisoned");
        let mut sidecar = sidecar
            .lock()
            .expect("wal index sidecar lock should not be poisoned");
        let wal = WalHandle {
            inner: Arc::clone(&inner),
        };
        wal.spill_excess_hot_pages_locked(&mut index, &mut sidecar)?;
    }

    // Background checkpoint worker (ADR 0058). Only spawn when at least one
    // size-based threshold is enabled; otherwise the worker would be idle
    // forever and just consume an OS thread.
    let cfg = AutoCheckpointConfig::from_db_config(config);
    let any_threshold_enabled = cfg.threshold_pages != 0 || cfg.threshold_bytes != 0;
    if config.background_checkpoint_worker && any_threshold_enabled {
        let bg = super::background::BgCheckpointer::start(Arc::downgrade(&inner), pager.clone());
        // `set` only fails if the cell was already initialized; build_handle
        // is the sole writer so this is unreachable in practice. Drop the
        // result rather than `expect`-ing to keep the cdylib boundary clean.
        let _ = inner.bg_checkpointer.set(bg);
    }

    Ok(WalHandle { inner })
}

fn wal_path_for_db(db_path: &Path) -> PathBuf {
    let mut path = db_path.as_os_str().to_os_string();
    path.push(".wal");
    PathBuf::from(path)
}

fn registry() -> &'static Mutex<HashMap<PathBuf, Weak<SharedWalInner>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<PathBuf, Weak<SharedWalInner>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
pub(crate) fn has_registry_entry_for_tests(path: &Path) -> bool {
    registry()
        .lock()
        .expect("shared wal registry lock should not be poisoned")
        .contains_key(path)
}

impl Drop for SharedWalInner {
    fn drop(&mut self) {
        // Stop the background checkpoint worker (ADR 0058) before tearing
        // down the rest of the WAL state. The worker holds a `Weak` to us,
        // so it cannot itself prolong our lifetime, but joining here makes
        // teardown ordering observable and avoids leaving an orphan thread
        // on processes that load and unload many `Db` instances.
        if let Some(bg) = self.bg_checkpointer.take() {
            drop(bg);
        }
        let Some(canonical_path) = self.canonical_path.as_ref() else {
            return;
        };
        let mut registry = registry()
            .lock()
            .expect("shared wal registry lock should not be poisoned");
        let should_remove = registry
            .get(canonical_path)
            .is_some_and(|entry| entry.upgrade().is_none());
        if should_remove {
            registry.remove(canonical_path);
            if registry.is_empty() {
                registry.shrink_to_fit();
            }
        }
    }
}

pub(crate) fn evict(vfs: &VfsHandle, db_path: &Path) -> Result<()> {
    if vfs.is_memory() {
        return Ok(());
    }

    let canonical_path = vfs.canonicalize_path(db_path)?;
    let mut registry = registry()
        .lock()
        .expect("shared wal registry lock should not be poisoned");
    registry.remove(&canonical_path);
    if registry.is_empty() {
        registry.shrink_to_fit();
    }
    Ok(())
}

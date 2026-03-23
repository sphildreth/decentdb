//! Shared WAL acquisition keyed by canonical database path.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use crate::config::WalSyncMode;
use crate::error::Result;
use crate::vfs::{FileKind, OpenMode, VfsHandle};

use super::reader_registry::ReaderRegistry;
use super::recovery;
use super::{SharedWalInner, WalHandle};

pub(crate) fn acquire(
    vfs: &VfsHandle,
    db_path: &Path,
    page_size: u32,
    sync_mode: WalSyncMode,
) -> Result<WalHandle> {
    if vfs.is_memory() {
        return build_handle(vfs, None, db_path, page_size, sync_mode);
    }

    let canonical_path = vfs.canonicalize_path(db_path)?;
    let registry = registry();
    {
        let registry_guard = registry
            .lock()
            .expect("shared wal registry lock should not be poisoned");
        if let Some(existing) = registry_guard.get(&canonical_path).and_then(Weak::upgrade) {
            return Ok(WalHandle { inner: existing });
        }
    }

    let handle = build_handle(
        vfs,
        Some(canonical_path.clone()),
        &canonical_path,
        page_size,
        sync_mode,
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
    page_size: u32,
    sync_mode: WalSyncMode,
) -> Result<WalHandle> {
    let wal_path = wal_path_for_db(db_path);
    let mode = if vfs.file_exists(&wal_path)? {
        OpenMode::OpenExisting
    } else {
        OpenMode::OpenOrCreate
    };
    let file = vfs.open(&wal_path, mode, FileKind::Wal)?;
    let (index, end_lsn, recovered_max_page_id) =
        recovery::initialize_or_recover(&file, page_size)?;

    Ok(WalHandle {
        inner: Arc::new(SharedWalInner {
            canonical_path,
            file,
            page_size,
            sync_mode,
            index: Mutex::new(index),
            wal_end_lsn: AtomicU64::new(end_lsn),
            max_page_count: AtomicU32::new(recovered_max_page_id),
            write_lock: Mutex::new(()),
            reader_registry: ReaderRegistry::default(),
            checkpoint_pending: AtomicBool::new(false),
        }),
    })
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

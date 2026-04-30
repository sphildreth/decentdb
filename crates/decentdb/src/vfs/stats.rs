//! Benchmark-focused VFS statistics wrapper.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use crate::error::Result;

use super::{FileKind, OpenMode, Vfs, VfsFile};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct BenchVfsFileStats {
    pub(crate) open_calls: u64,
    pub(crate) read_calls: u64,
    pub(crate) write_calls: u64,
    pub(crate) bytes_read: u64,
    pub(crate) bytes_written: u64,
    pub(crate) sync_data_calls: u64,
    pub(crate) sync_metadata_calls: u64,
    pub(crate) set_len_calls: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct BenchVfsStats {
    pub(crate) db: BenchVfsFileStats,
    pub(crate) wal: BenchVfsFileStats,
    pub(crate) open_create_like_calls: u64,
    pub(crate) file_exists_calls: u64,
    pub(crate) remove_file_calls: u64,
    pub(crate) canonicalize_calls: u64,
}

#[derive(Debug, Default)]
struct AtomicFileStats {
    open_calls: AtomicU64,
    read_calls: AtomicU64,
    write_calls: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    sync_data_calls: AtomicU64,
    sync_metadata_calls: AtomicU64,
    set_len_calls: AtomicU64,
}

impl AtomicFileStats {
    fn snapshot(&self) -> BenchVfsFileStats {
        BenchVfsFileStats {
            open_calls: self.open_calls.load(Ordering::Acquire),
            read_calls: self.read_calls.load(Ordering::Acquire),
            write_calls: self.write_calls.load(Ordering::Acquire),
            bytes_read: self.bytes_read.load(Ordering::Acquire),
            bytes_written: self.bytes_written.load(Ordering::Acquire),
            sync_data_calls: self.sync_data_calls.load(Ordering::Acquire),
            sync_metadata_calls: self.sync_metadata_calls.load(Ordering::Acquire),
            set_len_calls: self.set_len_calls.load(Ordering::Acquire),
        }
    }

    fn reset(&self) {
        self.open_calls.store(0, Ordering::Release);
        self.read_calls.store(0, Ordering::Release);
        self.write_calls.store(0, Ordering::Release);
        self.bytes_read.store(0, Ordering::Release);
        self.bytes_written.store(0, Ordering::Release);
        self.sync_data_calls.store(0, Ordering::Release);
        self.sync_metadata_calls.store(0, Ordering::Release);
        self.set_len_calls.store(0, Ordering::Release);
    }
}

#[derive(Debug, Default)]
struct StatsState {
    enabled: AtomicBool,
    db: AtomicFileStats,
    wal: AtomicFileStats,
    open_create_like_calls: AtomicU64,
    file_exists_calls: AtomicU64,
    remove_file_calls: AtomicU64,
    canonicalize_calls: AtomicU64,
}

impl StatsState {
    fn file_stats(&self, kind: FileKind) -> &AtomicFileStats {
        match kind {
            FileKind::Database => &self.db,
            FileKind::Wal => &self.wal,
        }
    }

    fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    fn snapshot(&self) -> BenchVfsStats {
        BenchVfsStats {
            db: self.db.snapshot(),
            wal: self.wal.snapshot(),
            open_create_like_calls: self.open_create_like_calls.load(Ordering::Acquire),
            file_exists_calls: self.file_exists_calls.load(Ordering::Acquire),
            remove_file_calls: self.remove_file_calls.load(Ordering::Acquire),
            canonicalize_calls: self.canonicalize_calls.load(Ordering::Acquire),
        }
    }

    fn reset(&self) {
        self.db.reset();
        self.wal.reset();
        self.open_create_like_calls.store(0, Ordering::Release);
        self.file_exists_calls.store(0, Ordering::Release);
        self.remove_file_calls.store(0, Ordering::Release);
        self.canonicalize_calls.store(0, Ordering::Release);
    }
}

fn global_stats_state() -> &'static StatsState {
    static STATE: OnceLock<StatsState> = OnceLock::new();
    STATE.get_or_init(StatsState::default)
}

pub(crate) fn set_enabled(enabled: bool) {
    global_stats_state()
        .enabled
        .store(enabled, Ordering::Release);
}

pub(crate) fn reset() {
    global_stats_state().reset();
}

pub(crate) fn snapshot() -> BenchVfsStats {
    global_stats_state().snapshot()
}

#[derive(Debug)]
pub(crate) struct StatsVfs {
    inner: Arc<dyn Vfs>,
}

impl StatsVfs {
    pub(crate) fn wrap(inner: Arc<dyn Vfs>) -> Self {
        Self { inner }
    }
}

impl Vfs for StatsVfs {
    fn open(&self, path: &Path, mode: OpenMode, kind: FileKind) -> Result<Arc<dyn VfsFile>> {
        let state = global_stats_state();
        if state.enabled() {
            state
                .file_stats(kind)
                .open_calls
                .fetch_add(1, Ordering::Relaxed);
            if matches!(mode, OpenMode::CreateNew | OpenMode::OpenOrCreate) {
                state.open_create_like_calls.fetch_add(1, Ordering::Relaxed);
            }
        }
        let inner = self.inner.open(path, mode, kind)?;
        Ok(Arc::new(StatsVfsFile { inner }))
    }

    fn file_exists(&self, path: &Path) -> Result<bool> {
        let state = global_stats_state();
        if state.enabled() {
            state.file_exists_calls.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.file_exists(path)
    }

    fn remove_file(&self, path: &Path) -> Result<()> {
        let state = global_stats_state();
        if state.enabled() {
            state.remove_file_calls.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.remove_file(path)
    }

    fn canonicalize_path(&self, path: &Path) -> Result<PathBuf> {
        let state = global_stats_state();
        if state.enabled() {
            state.canonicalize_calls.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.canonicalize_path(path)
    }

    fn is_memory(&self) -> bool {
        self.inner.is_memory()
    }
}

#[derive(Debug)]
struct StatsVfsFile {
    inner: Arc<dyn VfsFile>,
}

impl VfsFile for StatsVfsFile {
    fn kind(&self) -> FileKind {
        self.inner.kind()
    }

    fn path(&self) -> &Path {
        self.inner.path()
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let read = self.inner.read_at(offset, buf)?;
        let state = global_stats_state();
        if state.enabled() {
            let stats = state.file_stats(self.inner.kind());
            stats.read_calls.fetch_add(1, Ordering::Relaxed);
            stats
                .bytes_read
                .fetch_add(u64::try_from(read).unwrap_or(u64::MAX), Ordering::Relaxed);
        }
        Ok(read)
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize> {
        let written = self.inner.write_at(offset, buf)?;
        let state = global_stats_state();
        if state.enabled() {
            let stats = state.file_stats(self.inner.kind());
            stats.write_calls.fetch_add(1, Ordering::Relaxed);
            stats.bytes_written.fetch_add(
                u64::try_from(written).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
        Ok(written)
    }

    fn advise_sequential(&self) -> Result<()> {
        self.inner.advise_sequential()
    }

    fn sync_data(&self) -> Result<()> {
        let state = global_stats_state();
        if state.enabled() {
            state
                .file_stats(self.inner.kind())
                .sync_data_calls
                .fetch_add(1, Ordering::Relaxed);
        }
        self.inner.sync_data()
    }

    fn sync_metadata(&self) -> Result<()> {
        let state = global_stats_state();
        if state.enabled() {
            state
                .file_stats(self.inner.kind())
                .sync_metadata_calls
                .fetch_add(1, Ordering::Relaxed);
        }
        self.inner.sync_metadata()
    }

    fn file_size(&self) -> Result<u64> {
        self.inner.file_size()
    }

    fn set_len(&self, len: u64) -> Result<()> {
        let state = global_stats_state();
        if state.enabled() {
            state
                .file_stats(self.inner.kind())
                .set_len_calls
                .fetch_add(1, Ordering::Relaxed);
        }
        self.inner.set_len(len)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use crate::vfs::{write_all_at, FileKind, OpenMode, Vfs};

    use super::{reset, set_enabled, snapshot, StatsVfs};

    #[test]
    fn stats_wrapper_collects_db_and_wal_writes() {
        reset();
        set_enabled(true);
        let vfs = StatsVfs::wrap(Arc::new(crate::vfs::mem::MemVfs::default()));

        let db = vfs
            .open(
                Path::new(":memory:"),
                OpenMode::CreateNew,
                FileKind::Database,
            )
            .expect("open db");
        write_all_at(db.as_ref(), 0, &[1, 2, 3, 4]).expect("write db");
        db.sync_data().expect("db sync");

        let wal = vfs
            .open(
                Path::new(":memory:.wal"),
                OpenMode::CreateNew,
                FileKind::Wal,
            )
            .expect("open wal");
        write_all_at(wal.as_ref(), 0, &[9, 8]).expect("write wal");
        wal.sync_metadata().expect("wal sync metadata");

        set_enabled(false);

        let snap = snapshot();
        assert!(snap.db.write_calls >= 1);
        assert!(snap.db.bytes_written >= 4);
        assert!(snap.db.sync_data_calls >= 1);
        assert!(snap.wal.write_calls >= 1);
        assert!(snap.wal.bytes_written >= 2);
        assert!(snap.wal.sync_metadata_calls >= 1);
    }
}

//! Virtual filesystem abstractions for database and WAL I/O.
//!
//! Implements:
//! - design/adr/0119-rust-vfs-pread-pwrite.md
//! - design/adr/0105-in-memory-vfs.md

pub(crate) mod encrypted;
pub(crate) mod faulty;
pub(crate) mod mem;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) mod opfs;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod os;
#[cfg(any(test, feature = "bench-internals"))]
pub(crate) mod stats;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::DbConfig;
use crate::error::{DbError, Result};

use self::encrypted::EncryptedVfs;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use self::faulty::FaultyVfs;
use self::mem::MemVfs;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use self::os::OsVfs;
#[cfg(any(test, feature = "bench-internals"))]
use self::stats::StatsVfs;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FileKind {
    Database,
    Wal,
    SyncJournal,
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    Coordination,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OpenMode {
    CreateNew,
    OpenExisting,
    OpenOrCreate,
}

pub(crate) trait Vfs: Send + Sync + std::fmt::Debug {
    fn open(&self, path: &Path, mode: OpenMode, kind: FileKind) -> Result<Arc<dyn VfsFile>>;
    fn file_exists(&self, path: &Path) -> Result<bool>;
    #[allow(dead_code)]
    fn remove_file(&self, path: &Path) -> Result<()>;
    fn canonicalize_path(&self, path: &Path) -> Result<PathBuf>;

    fn is_memory(&self) -> bool {
        false
    }

    fn supports_file_locks(&self) -> bool {
        false
    }

    /// Whether a fresh database bootstrap's `sync_data` call may run on a
    /// short-lived worker while the caller constructs database state that
    /// does not access the new main-database file.
    ///
    /// This capability is deliberately opt-in. Custom and browser VFS
    /// implementations retain the conservative inline durability barrier
    /// unless they explicitly establish the same cross-thread contract.
    fn concurrent_bootstrap_sync_reservation(
        &self,
    ) -> Option<Box<dyn BootstrapSyncReservation + '_>> {
        None
    }
}

/// Scoped proof that a VFS permits fresh-bootstrap `sync_data` on a worker.
/// Wrappers may retain synchronization guards inside the reservation so the
/// capability cannot be invalidated between selection and worker completion.
pub(crate) trait BootstrapSyncReservation {}

impl<T> BootstrapSyncReservation for T {}

pub(crate) trait VfsFileLock: Send + Sync + std::fmt::Debug {}

pub(crate) trait VfsFile: Send + Sync + std::fmt::Debug {
    fn kind(&self) -> FileKind;
    fn path(&self) -> &Path;
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize>;
    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize>;
    fn write_all_at_many(&self, writes: &[(u64, &[u8])]) -> Result<()> {
        for (offset, buf) in writes {
            let mut cursor = 0;
            while cursor < buf.len() {
                let written = self.write_at(*offset + cursor as u64, &buf[cursor..])?;
                if written == 0 {
                    return Err(DbError::io(
                        format!(
                            "short write on {} at offset {}: expected {} bytes, got {cursor}",
                            self.path().display(),
                            *offset + cursor as u64,
                            buf.len()
                        ),
                        std::io::Error::new(std::io::ErrorKind::WriteZero, "short write"),
                    ));
                }
                cursor += written;
            }
        }
        Ok(())
    }
    fn advise_sequential(&self) -> Result<()>;
    /// Makes all preceding file-content writes durable, including any file
    /// length change required to address those writes. Implementations need
    /// not persist unrelated metadata such as timestamps or ownership.
    ///
    /// The WAL checkpoint protocol relies on this barrier before it truncates
    /// the only other durable copy of checkpointed pages.
    fn sync_data(&self) -> Result<()>;
    /// Makes preceding file-content writes and associated metadata durable.
    fn sync_metadata(&self) -> Result<()>;
    fn file_size(&self) -> Result<u64>;
    fn set_len(&self, len: u64) -> Result<()>;
    fn try_lock_range(
        &self,
        _offset: u64,
        _len: u64,
        _exclusive: bool,
    ) -> Result<Option<Box<dyn VfsFileLock>>> {
        Err(DbError::transaction(format!(
            "{} VFS file does not support process coordination locks",
            self.path().display()
        )))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct VfsHandle {
    inner: Arc<dyn Vfs>,
    canonical_path_hint: Option<Arc<(PathBuf, PathBuf)>>,
}

impl VfsHandle {
    pub(crate) fn for_path(path: &Path) -> Self {
        if is_memory_path(path) {
            Self {
                inner: Arc::new(MemVfs::default()),
                canonical_path_hint: None,
            }
        } else {
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            {
                Self {
                    inner: Arc::new(MemVfs::default()),
                    canonical_path_hint: None,
                }
            }
            #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
            {
                let os_vfs: Arc<dyn Vfs> = Arc::new(OsVfs);
                #[cfg(any(test, feature = "bench-internals"))]
                let os_vfs: Arc<dyn Vfs> = Arc::new(StatsVfs::wrap(os_vfs));
                Self {
                    inner: Arc::new(FaultyVfs::wrap(os_vfs)),
                    canonical_path_hint: None,
                }
            }
        }
    }

    pub(crate) fn with_config(self, config: &DbConfig) -> Self {
        if let Some(encryption) = &config.encryption {
            Self {
                inner: Arc::new(EncryptedVfs::wrap(self.inner, encryption.clone())),
                canonical_path_hint: self.canonical_path_hint,
            }
        } else {
            self
        }
    }

    #[cfg(any(test, all(target_arch = "wasm32", target_os = "unknown")))]
    pub(crate) fn from_vfs(inner: Arc<dyn Vfs>) -> Self {
        Self {
            inner,
            canonical_path_hint: None,
        }
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(crate) fn with_canonical_path_hint(mut self, source: PathBuf, canonical: PathBuf) -> Self {
        self.canonical_path_hint = Some(Arc::new((source, canonical)));
        self
    }

    pub(crate) fn open(
        &self,
        path: &Path,
        mode: OpenMode,
        kind: FileKind,
    ) -> Result<Arc<dyn VfsFile>> {
        self.inner.open(path, mode, kind)
    }

    pub(crate) fn file_exists(&self, path: &Path) -> Result<bool> {
        self.inner.file_exists(path)
    }

    pub(crate) fn canonicalize_path(&self, path: &Path) -> Result<PathBuf> {
        if let Some(hint) = &self.canonical_path_hint {
            if path == hint.0 {
                return Ok(hint.1.clone());
            }
        }
        self.inner.canonicalize_path(path)
    }

    pub(crate) fn is_memory(&self) -> bool {
        self.inner.is_memory()
    }

    pub(crate) fn supports_file_locks(&self) -> bool {
        self.inner.supports_file_locks()
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(crate) fn concurrent_bootstrap_sync_reservation(
        &self,
    ) -> Option<Box<dyn BootstrapSyncReservation + '_>> {
        self.inner.concurrent_bootstrap_sync_reservation()
    }
}

pub(crate) fn lock_range_with_timeout(
    file: &dyn VfsFile,
    offset: u64,
    len: u64,
    exclusive: bool,
    timeout: Option<Duration>,
) -> Result<Box<dyn VfsFileLock>> {
    let start = Instant::now();
    let mut delay = Duration::from_micros(100);
    loop {
        if let Some(guard) = file.try_lock_range(offset, len, exclusive)? {
            return Ok(guard);
        }
        match timeout {
            Some(timeout) if timeout.is_zero() => {
                return Err(DbError::busy(format!(
                    "process coordination lock at {} offset {offset} length {len} is busy",
                    file.path().display()
                )));
            }
            Some(timeout) if start.elapsed() >= timeout => {
                return Err(DbError::timeout(format!(
                    "timed out waiting for process coordination lock at {} offset {offset} length {len}",
                    file.path().display()
                )));
            }
            _ => {}
        }
        std::thread::sleep(delay);
        delay = (delay * 2).min(Duration::from_millis(5));
    }
}

pub(crate) fn is_memory_path(path: &Path) -> bool {
    path.as_os_str()
        .to_string_lossy()
        .eq_ignore_ascii_case(":memory:")
}

pub(crate) fn read_exact_at(file: &dyn VfsFile, offset: u64, buf: &mut [u8]) -> Result<()> {
    let mut cursor = 0;
    while cursor < buf.len() {
        let read = file.read_at(offset + cursor as u64, &mut buf[cursor..])?;
        if read == 0 {
            return Err(DbError::io(
                format!(
                    "short read on {} at offset {}: expected {} bytes, got {cursor}",
                    file.path().display(),
                    offset + cursor as u64,
                    buf.len()
                ),
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "short read"),
            ));
        }
        if read > buf.len() - cursor {
            return Err(DbError::internal(format!(
                "VFS read for {} returned {read} bytes into a {} byte buffer",
                file.path().display(),
                buf.len() - cursor
            )));
        }
        cursor += read;
    }
    Ok(())
}

pub(crate) fn write_all_at(file: &dyn VfsFile, offset: u64, buf: &[u8]) -> Result<()> {
    let mut cursor = 0;
    while cursor < buf.len() {
        let written = file.write_at(offset + cursor as u64, &buf[cursor..])?;
        if written == 0 {
            return Err(DbError::io(
                format!(
                    "short write on {} at offset {}: expected {} bytes, got {cursor}",
                    file.path().display(),
                    offset + cursor as u64,
                    buf.len()
                ),
                std::io::Error::new(std::io::ErrorKind::WriteZero, "short write"),
            ));
        }
        if written > buf.len() - cursor {
            return Err(DbError::internal(format!(
                "VFS write for {} accepted {written} bytes from a {} byte buffer",
                file.path().display(),
                buf.len() - cursor
            )));
        }
        cursor += written;
    }
    Ok(())
}

pub(crate) fn write_all_at_many(file: &dyn VfsFile, writes: &[(u64, &[u8])]) -> Result<()> {
    file.write_all_at_many(writes)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    use super::{read_exact_at, write_all_at, FileKind, VfsFile};
    use crate::error::{DbError, Result};

    #[derive(Debug)]
    struct ChunkedFile {
        path: PathBuf,
        data: Mutex<Vec<u8>>,
        max_chunk: usize,
    }

    impl ChunkedFile {
        fn new(max_chunk: usize) -> Self {
            Self {
                path: PathBuf::from("chunked.ddb"),
                data: Mutex::new(Vec::new()),
                max_chunk,
            }
        }

        fn bytes(&self) -> Vec<u8> {
            self.data.lock().expect("chunked file lock").clone()
        }
    }

    impl VfsFile for ChunkedFile {
        fn kind(&self) -> FileKind {
            FileKind::Database
        }

        fn path(&self) -> &Path {
            &self.path
        }

        fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
            let data = self
                .data
                .lock()
                .map_err(|_| DbError::internal("chunked file lock poisoned"))?;
            let offset = offset as usize;
            if offset >= data.len() {
                return Ok(0);
            }
            let len = self.max_chunk.min(buf.len()).min(data.len() - offset);
            buf[..len].copy_from_slice(&data[offset..offset + len]);
            Ok(len)
        }

        fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize> {
            let mut data = self
                .data
                .lock()
                .map_err(|_| DbError::internal("chunked file lock poisoned"))?;
            let offset = offset as usize;
            let len = self.max_chunk.min(buf.len());
            let end = offset + len;
            if data.len() < end {
                data.resize(end, 0);
            }
            data[offset..end].copy_from_slice(&buf[..len]);
            Ok(len)
        }

        fn advise_sequential(&self) -> Result<()> {
            Ok(())
        }

        fn sync_data(&self) -> Result<()> {
            Ok(())
        }

        fn sync_metadata(&self) -> Result<()> {
            Ok(())
        }

        fn file_size(&self) -> Result<u64> {
            self.data
                .lock()
                .map(|data| data.len() as u64)
                .map_err(|_| DbError::internal("chunked file lock poisoned"))
        }

        fn set_len(&self, len: u64) -> Result<()> {
            self.data
                .lock()
                .map_err(|_| DbError::internal("chunked file lock poisoned"))?
                .resize(len as usize, 0);
            Ok(())
        }
    }

    #[test]
    fn write_all_at_retries_partial_writes() {
        let file = ChunkedFile::new(3);
        write_all_at(&file, 2, b"abcdefghij").expect("write all");

        assert_eq!(file.bytes(), b"\0\0abcdefghij");
    }

    #[test]
    fn read_exact_at_retries_partial_reads() {
        let file = ChunkedFile::new(2);
        write_all_at(&file, 0, b"abcdefghij").expect("seed bytes");
        let mut out = [0_u8; 7];

        read_exact_at(&file, 2, &mut out).expect("read exact");

        assert_eq!(&out, b"cdefghi");
    }
}

//! Virtual filesystem abstractions for database and WAL I/O.
//!
//! Implements:
//! - design/adr/0119-rust-vfs-pread-pwrite.md
//! - design/adr/0105-in-memory-vfs.md

pub(crate) mod faulty;
pub(crate) mod mem;
pub(crate) mod os;
#[cfg(feature = "bench-internals")]
pub(crate) mod stats;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{DbError, Result};

use self::faulty::FaultyVfs;
use self::mem::MemVfs;
use self::os::OsVfs;
#[cfg(feature = "bench-internals")]
use self::stats::StatsVfs;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FileKind {
    Database,
    Wal,
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
}

pub(crate) trait VfsFile: Send + Sync + std::fmt::Debug {
    fn kind(&self) -> FileKind;
    fn path(&self) -> &Path;
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize>;
    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize>;
    fn sync_data(&self) -> Result<()>;
    fn sync_metadata(&self) -> Result<()>;
    fn file_size(&self) -> Result<u64>;
    fn set_len(&self, len: u64) -> Result<()>;
}

#[derive(Clone, Debug)]
pub(crate) struct VfsHandle {
    inner: Arc<dyn Vfs>,
}

impl VfsHandle {
    pub(crate) fn for_path(path: &Path) -> Self {
        if is_memory_path(path) {
            Self {
                inner: Arc::new(MemVfs::default()),
            }
        } else {
            let os_vfs: Arc<dyn Vfs> = Arc::new(OsVfs);
            #[cfg(feature = "bench-internals")]
            let os_vfs: Arc<dyn Vfs> = Arc::new(StatsVfs::wrap(os_vfs));
            Self {
                inner: Arc::new(FaultyVfs::wrap(os_vfs)),
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn from_vfs(inner: Arc<dyn Vfs>) -> Self {
        Self { inner }
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
        self.inner.canonicalize_path(path)
    }

    pub(crate) fn is_memory(&self) -> bool {
        self.inner.is_memory()
    }
}

pub(crate) fn is_memory_path(path: &Path) -> bool {
    path.as_os_str()
        .to_string_lossy()
        .eq_ignore_ascii_case(":memory:")
}

pub(crate) fn read_exact_at(file: &dyn VfsFile, offset: u64, buf: &mut [u8]) -> Result<()> {
    let read = file.read_at(offset, buf)?;
    if read == buf.len() {
        Ok(())
    } else {
        Err(DbError::io(
            format!(
                "short read on {} at offset {offset}: expected {} bytes, got {read}",
                file.path().display(),
                buf.len()
            ),
            std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "short read"),
        ))
    }
}

pub(crate) fn write_all_at(file: &dyn VfsFile, offset: u64, buf: &[u8]) -> Result<()> {
    let written = file.write_at(offset, buf)?;
    if written == buf.len() {
        Ok(())
    } else {
        Err(DbError::io(
            format!(
                "short write on {} at offset {offset}: expected {} bytes, got {written}",
                file.path().display(),
                buf.len()
            ),
            std::io::Error::new(std::io::ErrorKind::WriteZero, "short write"),
        ))
    }
}

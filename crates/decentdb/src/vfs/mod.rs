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
    SyncJournal,
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

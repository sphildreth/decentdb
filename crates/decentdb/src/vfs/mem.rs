//! In-memory VFS used for `:memory:` databases and tests.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use crate::error::{DbError, Result};

use super::{FileKind, OpenMode, Vfs, VfsFile};

#[derive(Debug, Default)]
pub(crate) struct MemVfs {
    files: Mutex<HashMap<PathBuf, Arc<MemFileState>>>,
}

impl Vfs for MemVfs {
    fn open(&self, path: &Path, mode: OpenMode, kind: FileKind) -> Result<Arc<dyn VfsFile>> {
        let mut files = self
            .files
            .lock()
            .map_err(|_| DbError::internal("mem vfs lock poisoned"))?;
        let path_buf = path.to_path_buf();

        match mode {
            OpenMode::CreateNew => {
                if files.contains_key(&path_buf) {
                    return Err(DbError::io(
                        format!("create in-memory file {}", path.display()),
                        std::io::Error::new(std::io::ErrorKind::AlreadyExists, "file exists"),
                    ));
                }
                let state = Arc::new(MemFileState::new(kind, path_buf.clone()));
                files.insert(path_buf.clone(), Arc::clone(&state));
                Ok(Arc::new(MemVfsFile { state }))
            }
            OpenMode::OpenExisting => {
                let state = files.get(&path_buf).cloned().ok_or_else(|| {
                    DbError::io(
                        format!("open in-memory file {}", path.display()),
                        std::io::Error::new(std::io::ErrorKind::NotFound, "file not found"),
                    )
                })?;
                Ok(Arc::new(MemVfsFile { state }))
            }
            OpenMode::OpenOrCreate => {
                let state = files
                    .entry(path_buf.clone())
                    .or_insert_with(|| Arc::new(MemFileState::new(kind, path_buf.clone())))
                    .clone();
                Ok(Arc::new(MemVfsFile { state }))
            }
        }
    }

    fn file_exists(&self, path: &Path) -> Result<bool> {
        let files = self
            .files
            .lock()
            .map_err(|_| DbError::internal("mem vfs lock poisoned"))?;
        Ok(files.contains_key(path))
    }

    fn remove_file(&self, path: &Path) -> Result<()> {
        let mut files = self
            .files
            .lock()
            .map_err(|_| DbError::internal("mem vfs lock poisoned"))?;
        files.remove(path);
        Ok(())
    }

    fn canonicalize_path(&self, path: &Path) -> Result<PathBuf> {
        Ok(path.to_path_buf())
    }

    fn is_memory(&self) -> bool {
        true
    }
}

#[derive(Debug)]
struct MemFileState {
    kind: FileKind,
    path: PathBuf,
    data: RwLock<Vec<u8>>,
}

impl MemFileState {
    fn new(kind: FileKind, path: PathBuf) -> Self {
        Self {
            kind,
            path,
            data: RwLock::new(Vec::new()),
        }
    }
}

#[derive(Debug)]
struct MemVfsFile {
    state: Arc<MemFileState>,
}

impl VfsFile for MemVfsFile {
    fn kind(&self) -> FileKind {
        self.state.kind
    }

    fn path(&self) -> &Path {
        &self.state.path
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let data = self
            .state
            .data
            .read()
            .map_err(|_| DbError::internal("mem file read lock poisoned"))?;
        let offset = offset as usize;
        if offset >= data.len() {
            return Ok(0);
        }
        let available = &data[offset..];
        let copied = available.len().min(buf.len());
        buf[..copied].copy_from_slice(&available[..copied]);
        Ok(copied)
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize> {
        let mut data = self
            .state
            .data
            .write()
            .map_err(|_| DbError::internal("mem file write lock poisoned"))?;
        let offset = offset as usize;
        let end = offset + buf.len();
        if data.len() < end {
            data.resize(end, 0);
        }
        data[offset..end].copy_from_slice(buf);
        Ok(buf.len())
    }

    fn sync_data(&self) -> Result<()> {
        Ok(())
    }

    fn sync_metadata(&self) -> Result<()> {
        Ok(())
    }

    fn file_size(&self) -> Result<u64> {
        let data = self
            .state
            .data
            .read()
            .map_err(|_| DbError::internal("mem file read lock poisoned"))?;
        Ok(data.len() as u64)
    }

    fn set_len(&self, len: u64) -> Result<()> {
        let mut data = self
            .state
            .data
            .write()
            .map_err(|_| DbError::internal("mem file write lock poisoned"))?;
        data.resize(len as usize, 0);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::vfs::{read_exact_at, write_all_at, FileKind, OpenMode, Vfs};

    use super::MemVfs;

    #[test]
    fn create_open_remove_round_trip() {
        let vfs = MemVfs::default();
        let path = std::path::Path::new(":memory:");

        assert!(!vfs.file_exists(path).expect("query file existence"));
        let file = vfs
            .open(path, OpenMode::CreateNew, FileKind::Database)
            .expect("create file");
        write_all_at(file.as_ref(), 0, &[1, 2, 3, 4]).expect("write bytes");
        assert!(vfs.file_exists(path).expect("query file existence"));

        let reopened = vfs
            .open(path, OpenMode::OpenExisting, FileKind::Database)
            .expect("open existing");
        let mut buf = [0_u8; 4];
        read_exact_at(reopened.as_ref(), 0, &mut buf).expect("read bytes");
        assert_eq!(buf, [1, 2, 3, 4]);

        vfs.remove_file(path).expect("remove file");
        assert!(!vfs.file_exists(path).expect("query file existence"));
    }

    #[test]
    fn mem_vfs_is_memory_returns_true() {
        let vfs = MemVfs::default();
        assert!(vfs.is_memory());
    }

    #[test]
    fn create_new_fails_if_file_exists() {
        let vfs = MemVfs::default();
        let path = std::path::Path::new("test.db");

        let _file1 = vfs
            .open(path, OpenMode::CreateNew, FileKind::Database)
            .expect("create file");
        
        let result = vfs.open(path, OpenMode::CreateNew, FileKind::Database);
        assert!(result.is_err());
    }

    #[test]
    fn open_existing_fails_if_file_not_found() {
        let vfs = MemVfs::default();
        let path = std::path::Path::new("nonexistent.db");

        let result = vfs.open(path, OpenMode::OpenExisting, FileKind::Database);
        assert!(result.is_err());
    }

    #[test]
    fn open_or_creates_file_if_not_exists() {
        let vfs = MemVfs::default();
        let path = std::path::Path::new("test.db");

        assert!(!vfs.file_exists(path).expect("query file existence"));
        let _file = vfs
            .open(path, OpenMode::OpenOrCreate, FileKind::Database)
            .expect("open or create");
        assert!(vfs.file_exists(path).expect("query file existence"));
    }

    #[test]
    fn canonicalize_path_returns_input_path() {
        let vfs = MemVfs::default();
        let path = std::path::Path::new("/some/path.db");
        let canonical = vfs.canonicalize_path(path).expect("canonicalize");
        assert_eq!(canonical, path);
    }

    #[test]
    fn file_kind_is_preserved() {
        let vfs = MemVfs::default();
        let path = std::path::Path::new("test.db");

        let db_file = vfs
            .open(path, OpenMode::CreateNew, FileKind::Database)
            .expect("create db file");
        assert_eq!(db_file.kind(), FileKind::Database);

        let wal_path = std::path::Path::new("test.db.wal");
        let wal_file = vfs
            .open(wal_path, OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        assert_eq!(wal_file.kind(), FileKind::Wal);
    }

    #[test]
    fn read_at_returns_zero_when_offset_beyond_eof() {
        let vfs = MemVfs::default();
        let path = std::path::Path::new("test.db");
        let file = vfs
            .open(path, OpenMode::CreateNew, FileKind::Database)
            .expect("create file");
        
        let mut buf = [0_u8; 10];
        let bytes_read = file.read_at(100, &mut buf).expect("read");
        assert_eq!(bytes_read, 0);
    }

    #[test]
    fn read_at_partial_when_buffer_larger_than_data() {
        let vfs = MemVfs::default();
        let path = std::path::Path::new("test.db");
        let file = vfs
            .open(path, OpenMode::CreateNew, FileKind::Database)
            .expect("create file");
        write_all_at(file.as_ref(), 0, &[1, 2, 3]).expect("write");

        let mut buf = [0_u8; 10];
        let bytes_read = file.read_at(0, &mut buf).expect("read");
        assert_eq!(bytes_read, 3);
        assert_eq!(&buf[..3], &[1, 2, 3]);
    }

    #[test]
    fn write_at_expands_file() {
        let vfs = MemVfs::default();
        let path = std::path::Path::new("test.db");
        let file = vfs
            .open(path, OpenMode::CreateNew, FileKind::Database)
            .expect("create file");

        assert_eq!(file.file_size().expect("get size"), 0);
        write_all_at(file.as_ref(), 0, &[1, 2, 3]).expect("write");
        assert_eq!(file.file_size().expect("get size"), 3);

        write_all_at(file.as_ref(), 10, &[4, 5, 6]).expect("write at offset");
        assert_eq!(file.file_size().expect("get size"), 13);
    }

    #[test]
    fn set_len_truncates_and_expands() {
        let vfs = MemVfs::default();
        let path = std::path::Path::new("test.db");
        let file = vfs
            .open(path, OpenMode::CreateNew, FileKind::Database)
            .expect("create file");
        
        write_all_at(file.as_ref(), 0, &[1, 2, 3, 4, 5]).expect("write");
        assert_eq!(file.file_size().expect("get size"), 5);

        file.set_len(3).expect("truncate");
        assert_eq!(file.file_size().expect("get size"), 3);

        file.set_len(10).expect("expand");
        assert_eq!(file.file_size().expect("get size"), 10);
    }

    #[test]
    fn sync_operations_are_noop() {
        let vfs = MemVfs::default();
        let path = std::path::Path::new("test.db");
        let file = vfs
            .open(path, OpenMode::CreateNew, FileKind::Database)
            .expect("create file");

        assert!(file.sync_data().is_ok());
        assert!(file.sync_metadata().is_ok());
    }

    #[test]
    fn multiple_files_can_coexist() {
        let vfs = MemVfs::default();
        let path1 = std::path::Path::new("db1.db");
        let path2 = std::path::Path::new("db2.db");

        let file1 = vfs.open(path1, OpenMode::CreateNew, FileKind::Database).expect("create 1");
        let file2 = vfs.open(path2, OpenMode::CreateNew, FileKind::Database).expect("create 2");

        write_all_at(file1.as_ref(), 0, &[1, 1, 1]).expect("write 1");
        write_all_at(file2.as_ref(), 0, &[2, 2, 2]).expect("write 2");

        let mut buf1 = [0_u8; 3];
        let mut buf2 = [0_u8; 3];
        file1.read_at(0, &mut buf1).expect("read 1");
        file2.read_at(0, &mut buf2).expect("read 2");

        assert_eq!(buf1, [1, 1, 1]);
        assert_eq!(buf2, [2, 2, 2]);
    }

    #[test]
    fn read_write_at_various_offsets() {
        let vfs = MemVfs::default();
        let path = std::path::Path::new("test.db");
        let file = vfs
            .open(path, OpenMode::CreateNew, FileKind::Database)
            .expect("create file");

        // Write at various offsets
        write_all_at(file.as_ref(), 0, &[0, 1, 2]).expect("write 0");
        write_all_at(file.as_ref(), 5, &[5, 6, 7]).expect("write 5");
        write_all_at(file.as_ref(), 10, &[10, 11, 12]).expect("write 10");

        // Read back
        let mut buf = [0_u8; 13];
        file.read_at(0, &mut buf).expect("read all");
        assert_eq!(buf, [0, 1, 2, 0, 0, 5, 6, 7, 0, 0, 10, 11, 12]);
    }
}

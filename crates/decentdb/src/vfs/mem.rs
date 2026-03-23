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
}

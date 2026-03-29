//! Operating-system backed VFS using positional I/O only.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{DbError, Result};

use super::{FileKind, OpenMode, Vfs, VfsFile};

#[derive(Debug, Default)]
pub(crate) struct OsVfs;

impl Vfs for OsVfs {
    fn open(&self, path: &Path, mode: OpenMode, kind: FileKind) -> Result<Arc<dyn VfsFile>> {
        let mut options = OpenOptions::new();
        options.read(true).write(true);
        match mode {
            OpenMode::CreateNew => {
                options.create_new(true);
            }
            OpenMode::OpenExisting => {}
            OpenMode::OpenOrCreate => {
                options.create(true);
            }
        }

        let file = options.open(path).map_err(|source| {
            DbError::io(
                format!("open {} file at {}", kind.label(), path.display()),
                source,
            )
        })?;

        Ok(Arc::new(OsVfsFile {
            kind,
            path: path.to_path_buf(),
            file,
        }))
    }

    fn file_exists(&self, path: &Path) -> Result<bool> {
        Ok(path.exists())
    }

    fn remove_file(&self, path: &Path) -> Result<()> {
        std::fs::remove_file(path)
            .map_err(|source| DbError::io(format!("remove file {}", path.display()), source))
    }

    fn canonicalize_path(&self, path: &Path) -> Result<PathBuf> {
        if path.exists() {
            std::fs::canonicalize(path).map_err(|source| {
                DbError::io(format!("canonicalize path {}", path.display()), source)
            })
        } else if path.is_absolute() {
            Ok(path.to_path_buf())
        } else {
            std::env::current_dir()
                .map(|cwd| cwd.join(path))
                .map_err(|source| DbError::io("resolve current working directory", source))
        }
    }
}

#[derive(Debug)]
struct OsVfsFile {
    kind: FileKind,
    path: PathBuf,
    file: File,
}

impl VfsFile for OsVfsFile {
    fn kind(&self) -> FileKind {
        self.kind
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        read_at(&self.file, offset, buf).map_err(|source| {
            DbError::io(
                format!("read {} file at {}", self.kind.label(), self.path.display()),
                source,
            )
        })
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize> {
        write_at(&self.file, offset, buf).map_err(|source| {
            DbError::io(
                format!(
                    "write {} file at {}",
                    self.kind.label(),
                    self.path.display()
                ),
                source,
            )
        })
    }

    fn sync_data(&self) -> Result<()> {
        self.file
            .sync_data()
            .map_err(|source| DbError::io(format!("sync data for {}", self.path.display()), source))
    }

    fn sync_metadata(&self) -> Result<()> {
        self.file.sync_all().map_err(|source| {
            DbError::io(format!("sync metadata for {}", self.path.display()), source)
        })
    }

    fn file_size(&self) -> Result<u64> {
        self.file
            .metadata()
            .map(|metadata| metadata.len())
            .map_err(|source| DbError::io(format!("stat file {}", self.path.display()), source))
    }

    fn set_len(&self, len: u64) -> Result<()> {
        self.file
            .set_len(len)
            .map_err(|source| DbError::io(format!("resize file {}", self.path.display()), source))
    }
}

#[cfg(unix)]
fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buf, offset)
}

#[cfg(unix)]
fn write_at(file: &File, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
    std::os::unix::fs::FileExt::write_at(file, buf, offset)
}

#[cfg(windows)]
fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buf, offset)
}

#[cfg(windows)]
fn write_at(file: &File, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
    std::os::windows::fs::FileExt::seek_write(file, buf, offset)
}

impl FileKind {
    fn label(self) -> &'static str {
        match self {
            Self::Database => "database",
            Self::Wal => "wal",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::vfs::{read_exact_at, write_all_at, FileKind, OpenMode, Vfs};

    use super::OsVfs;

    static NEXT_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn concurrent_read_at_reads_expected_bytes() {
        let vfs = OsVfs;
        let path = unique_path("concurrent-read");
        let file = vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database file");
        let payload = vec![0xAB; 8192];
        write_all_at(file.as_ref(), 0, &payload).expect("seed file");

        let shared = Arc::clone(&file);
        let mut threads = Vec::new();
        for _ in 0..4 {
            let file = Arc::clone(&shared);
            threads.push(thread::spawn(move || {
                let mut buf = vec![0_u8; 4096];
                read_exact_at(file.as_ref(), 1024, &mut buf).expect("read slice");
                assert!(buf.iter().all(|byte| *byte == 0xAB));
            }));
        }

        for thread in threads {
            thread.join().expect("thread should succeed");
        }

        std::fs::remove_file(path).expect("cleanup file");
    }

    fn unique_path(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("monotonic wall clock")
            .as_nanos();
        let ordinal = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "decentdb-vfs-os-{label}-{}-{stamp}-{ordinal}.bin",
            std::process::id()
        ))
    }

    #[test]
    fn file_exists_and_remove_file_works() {
        let vfs = OsVfs;
        let path = unique_path("exists-remove");
        let _file = vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database file");
        assert!(vfs.file_exists(&path).expect("file_exists"));
        vfs.remove_file(&path).expect("remove file");
        assert!(!vfs.file_exists(&path).expect("file_exists after remove"));
    }

    #[test]
    fn canonicalize_path_variants() {
        let vfs = OsVfs;
        let path = unique_path("canonicalize1");
        let _file = vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database file");
        let canon = vfs.canonicalize_path(&path).expect("canonicalize existing");
        let expect = std::fs::canonicalize(&path).expect("std canonicalize");
        assert_eq!(canon, expect);

        // Absolute non-existent path should return the same absolute path
        let abs = std::env::temp_dir().join(format!(
            "abs-nonexistent-{}",
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let abs_ret = vfs
            .canonicalize_path(&abs)
            .expect("canonicalize absolute non-exist");
        assert_eq!(abs_ret, abs);

        // Relative path should be resolved against current_dir
        let rel = PathBuf::from("some-relative-path-for-test");
        let rel_ret = vfs.canonicalize_path(&rel).expect("canonicalize relative");
        let expected_rel = std::env::current_dir().expect("cwd").join(&rel);
        assert_eq!(rel_ret, expected_rel);
    }

    #[test]
    fn open_modes_behaviour() {
        let vfs = OsVfs;
        let path = unique_path("open-modes");

        // OpenExisting should fail on missing
        assert!(vfs
            .open(&path, OpenMode::OpenExisting, FileKind::Database)
            .is_err());

        // OpenOrCreate should succeed
        let f = vfs
            .open(&path, OpenMode::OpenOrCreate, FileKind::Database)
            .expect("open or create");
        drop(f);

        // CreateNew should now fail because file exists
        assert!(vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .is_err());

        // Cleanup
        vfs.remove_file(&path).expect("remove file");
    }

    #[test]
    fn file_size_and_set_len_work() {
        let vfs = OsVfs;
        let path = unique_path("set-len");
        let file = vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create file");

        let data = vec![0x11u8; 1024];
        write_all_at(file.as_ref(), 0, &data).expect("write data");
        assert_eq!(file.file_size().expect("size1"), 1024);

        file.set_len(2048).expect("extend");
        assert_eq!(file.file_size().expect("size2"), 2048);

        file.set_len(512).expect("shrink");
        assert_eq!(file.file_size().expect("size3"), 512);

        vfs.remove_file(&path).expect("cleanup");
    }
}

//! Stable database owner and bootstrap lifecycle entry points.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use crate::catalog::CatalogHandle;
use crate::config::DbConfig;
use crate::error::{DbError, Result};
use crate::storage::{self, DatabaseHeader, PagerHandle};
use crate::vfs::VfsHandle;
use crate::wal::WalHandle;

/// Stable engine owner used across later storage, SQL, and FFI slices.
#[derive(Debug)]
pub struct Db {
    path: PathBuf,
    config: DbConfig,
    _vfs: VfsHandle,
    _pager: PagerHandle,
    _wal: WalHandle,
    _catalog: CatalogHandle,
}

impl Db {
    /// Creates a brand new database file with an initialized page-1 header and
    /// reserved catalog root page.
    pub fn create(path: impl AsRef<Path>, config: DbConfig) -> Result<Self> {
        let path = path.as_ref();
        config.validate_for_create()?;

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|source| {
                DbError::io(format!("create database at {}", path.display()), source)
            })?;

        let header = DatabaseHeader::new(config.page_size);
        storage::write_database_bootstrap(&mut file, &header)?;
        file.sync_all().map_err(|source| {
            DbError::io(format!("sync database at {}", path.display()), source)
        })?;

        Ok(Self::new(path.to_path_buf(), config))
    }

    /// Opens an existing database file and validates its fixed header.
    pub fn open(path: impl AsRef<Path>, config: DbConfig) -> Result<Self> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|source| {
                DbError::io(format!("open database at {}", path.display()), source)
            })?;

        let header = storage::read_database_header(&mut file)?;
        let mut effective_config = config;
        effective_config.page_size = header.page_size;

        Ok(Self::new(path.to_path_buf(), effective_config))
    }

    #[must_use]
    pub fn config(&self) -> &DbConfig {
        &self.config
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn new(path: PathBuf, config: DbConfig) -> Self {
        Self {
            path,
            config,
            _vfs: VfsHandle::placeholder(),
            _pager: PagerHandle::placeholder(),
            _wal: WalHandle::placeholder(),
            _catalog: CatalogHandle::placeholder(),
        }
    }
}

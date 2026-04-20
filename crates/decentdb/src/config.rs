//! Engine configuration surface for DecentDB.

use std::path::PathBuf;

use crate::error::{DbError, Result};
use crate::storage::page;

/// WAL sync policy used by the engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalSyncMode {
    /// Durability-first mode. Commits must sync the WAL fully.
    Full,
    /// Reduced sync overhead for environments that can tolerate weaker flush
    /// behavior.
    Normal,
    /// Test-only mode with no durability guarantees.
    TestingOnlyUnsafeNoSync,
}

/// Engine configuration applied at database create/open time.
///
/// ```
/// use decentdb::{DbConfig, WalSyncMode};
///
/// let config = DbConfig::default();
/// assert_eq!(config.page_size, 4096);
/// assert_eq!(config.wal_sync_mode, WalSyncMode::Full);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DbConfig {
    pub page_size: u32,
    pub cache_size_mb: usize,
    pub cached_payloads_max_entries: usize,
    pub wal_sync_mode: WalSyncMode,
    pub checkpoint_timeout_sec: u64,
    pub trigram_postings_threshold: usize,
    pub temp_dir: PathBuf,
}

impl DbConfig {
    pub(crate) fn validate_for_create(&self) -> Result<()> {
        if page::is_supported_page_size(self.page_size) {
            Ok(())
        } else {
            Err(DbError::internal(format!(
                "unsupported page size {}; supported sizes are 4096, 8192, 16384",
                self.page_size
            )))
        }
    }

    #[doc(hidden)]
    pub fn set_cached_payloads_max_entries_for_tests(&mut self, entries: usize) {
        self.cached_payloads_max_entries = entries;
    }
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            page_size: page::DEFAULT_PAGE_SIZE,
            cache_size_mb: 4,
            cached_payloads_max_entries: 1024,
            wal_sync_mode: WalSyncMode::Full,
            checkpoint_timeout_sec: 30,
            trigram_postings_threshold: 100_000,
            temp_dir: std::env::temp_dir(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DbConfig, WalSyncMode};
    use crate::storage::page;

    #[test]
    fn default_config_matches_spec_requirements() {
        let config = DbConfig::default();

        assert_eq!(config.page_size, page::DEFAULT_PAGE_SIZE);
        assert_eq!(config.cache_size_mb, 4);
        assert_eq!(config.cached_payloads_max_entries, 1024);
        assert_eq!(config.wal_sync_mode, WalSyncMode::Full);
        assert_eq!(config.checkpoint_timeout_sec, 30);
        assert_eq!(config.trigram_postings_threshold, 100_000);
        assert!(!config.temp_dir.as_os_str().is_empty());
    }
}

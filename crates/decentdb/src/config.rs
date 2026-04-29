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
    /// Group-commit mode: commits are acknowledged as soon as the WAL frame is
    /// written; a background flusher thread fsyncs the WAL on the configured
    /// interval. Trades up to `interval_ms` of post-crash durability for
    /// dramatically lower commit latency. Atomicity, consistency, and
    /// isolation are unaffected. Use [`crate::Db::sync`] for an explicit
    /// durability barrier.
    ///
    /// See `design/adr/0135-async-commit-wal-group-commit.md`.
    AsyncCommit {
        /// Interval between background fsync ticks, in milliseconds. Smaller
        /// values reduce the durability window at the cost of more wakeups.
        /// Must be at least 1 ms.
        interval_ms: u32,
    },
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

    /// Trigger an automatic checkpoint when the in-memory WAL has accumulated
    /// at least this many distinct dirty page versions since the last
    /// checkpoint. `0` disables the page-count trigger. The trigger only
    /// fires when there are no active readers; otherwise the WAL is allowed
    /// to keep growing until readers complete (per ADR 0019).
    ///
    /// Default: `4096` (≈ 16 MB at the default 4 KB page size).
    ///
    /// See `design/2026-04-25.ENGINE-MEMORY-WORK.md` §6 Appendix (slice M1).
    pub wal_checkpoint_threshold_pages: u32,

    /// Trigger an automatic checkpoint when the WAL has grown by at least
    /// this many bytes since the last checkpoint. `0` disables the
    /// byte-count trigger. Bounds writer-side memory growth on workloads
    /// that touch many distinct pages without exceeding the page-count
    /// threshold (e.g. wide-row inserts on large pages).
    ///
    /// Default: `64 * 1024 * 1024` (64 MB).
    ///
    /// See `design/2026-04-25.ENGINE-MEMORY-WORK.md` §6 Appendix (slice M1).
    pub wal_checkpoint_threshold_bytes: u64,

    /// On Linux/glibc, call `malloc_trim(0)` after a successful checkpoint
    /// to return freed heap arenas to the operating system. No-op on other
    /// platforms regardless of value.
    ///
    /// Defaults to `true` on Linux/glibc and `false` elsewhere. On
    /// long-lived embedders the default removes the dominant cause of
    /// observed RSS growth (allocator fragmentation amplifying transient
    /// per-commit allocations).
    ///
    /// See `design/2026-04-25.ENGINE-MEMORY-WORK.md` §6 Appendix (slice M2).
    pub release_freed_memory_after_checkpoint: bool,

    /// Run auto-checkpoints on a dedicated background thread instead of
    /// blocking the writer's commit hot path.
    ///
    /// When `true` (default), `Db::open` / `Db::create` spawn a per-WAL
    /// worker thread that wakes when `wal_checkpoint_threshold_pages` or
    /// `wal_checkpoint_threshold_bytes` is exceeded. The writer signals
    /// the worker via a condvar and returns immediately. When `false`,
    /// the writer falls back to running `checkpoint::checkpoint()`
    /// synchronously on the commit thread (the Phase 1 / ADR 0137 default).
    ///
    /// See ADR 0058 — Background / Incremental Checkpoint Worker.
    pub background_checkpoint_worker: bool,

    /// Maximum number of page-version chains kept resident in the WAL
    /// index. `0` (default) preserves the historical behavior of an
    /// unbounded in-memory index. Non-zero values request that the
    /// engine spill cold chains to a `<db>.wal-idx` sidecar file.
    ///
    /// The current ADR 0141 slice spills reader-free latest full-page
    /// versions into the sidecar and promotes those pages back into the
    /// in-memory hot set on demand. Multi-version reader history and
    /// delta-dependent latest versions still stay resident, and recovery
    /// still rebuilds the index in memory before post-open spill.
    ///
    /// See ADR 0141 — Paged On-Disk WAL Index.
    pub wal_index_hot_set_pages: u32,

    /// Number of most-recent WAL versions to keep resident per page before
    /// the M4 demotion pass is allowed to convert older cold versions to
    /// `WalVersionPayload::OnDisk`.
    ///
    /// Default: `16`.
    ///
    /// See ADR 0140 — `WalVersion` Discriminated Payload.
    pub wal_resident_versions_per_page: u32,

    /// When `true`, `Db::open` skips the eager materialization of all
    /// table row data into memory, leaving each table in the
    /// `deferred_tables` set. Tables are then materialized on first
    /// access — when ADR 0143 Phase B can prove the active statement's
    /// table set is conservatively exhaustive, only those tables are
    /// loaded; otherwise all deferred tables are loaded as a fallback.
    ///
    /// Default: `true`. Deferred materialization now pins a single WAL
    /// snapshot across the runtime refresh and the first-use overflow
    /// payload read, avoiding the old `overflow payload length mismatch`
    /// race under concurrent checkpoints. Set this to `false` to restore
    /// the old eager-at-open behavior for callers that prefer immediate
    /// full materialization.
    ///
    /// See ADR 0143 — On-Disk Row-Scan Executor.
    pub defer_table_materialization: bool,

    /// When `true`, the engine persists a row-id -> table-payload-byte-range
    /// locator B+Tree for base tables and consults it for deferred
    /// `WHERE id = ?` reads before falling back to full table materialization.
    ///
    /// Default: `false` for one release of soak time. Existing databases and
    /// callers keep the Phase B behavior until they opt in.
    ///
    /// See ADR 0144 — Persistent Primary-Key Locator Index.
    pub persistent_pk_index: bool,

    /// When `true`, persisted base tables are stored behind a table-level
    /// paged-row manifest instead of a single table payload pointer. The
    /// current Phase D persistence slice writes one manifest chunk per table
    /// and backfills legacy tables on open; later slices will extend that
    /// manifest to true append-only multi-chunk storage.
    ///
    /// Default: `true`. Set to `false` to keep the legacy single-payload
    /// resident table materialization path for compatibility testing.
    ///
    /// See ADR 0145 — Paged Table Row Source.
    pub paged_row_storage: bool,

    /// If non-zero, `Db::open` will run a synchronous checkpoint when the
    /// existing WAL file size on disk exceeds this threshold (in MiB).
    /// This drops the in-memory WAL page-version index and lets steady-
    /// state RSS approach the configured `cache_size_mb` rather than
    /// growing with the size of an uncheckpointed WAL.
    ///
    /// Default: `16` MiB. Set to `0` to disable.
    ///
    /// See ADR 0143 — engine-memory plan, open-time checkpoint heuristic.
    pub auto_checkpoint_on_open_mb: u32,
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
            wal_checkpoint_threshold_pages: 4096,
            wal_checkpoint_threshold_bytes: 64 * 1024 * 1024,
            release_freed_memory_after_checkpoint: cfg!(all(
                target_os = "linux",
                target_env = "gnu"
            )),
            background_checkpoint_worker: true,
            wal_index_hot_set_pages: 0,
            wal_resident_versions_per_page: 16,
            defer_table_materialization: true,
            persistent_pk_index: false,
            paged_row_storage: true,
            auto_checkpoint_on_open_mb: 16,
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
        assert_eq!(config.wal_checkpoint_threshold_pages, 4096);
        assert_eq!(config.wal_checkpoint_threshold_bytes, 64 * 1024 * 1024);
        assert_eq!(config.wal_resident_versions_per_page, 16);
        assert!(config.defer_table_materialization);
        assert!(!config.persistent_pk_index);
        assert!(config.paged_row_storage);
        // Default depends on platform; just assert the field is reachable.
        let _ = config.release_freed_memory_after_checkpoint;
    }
}

//! Engine configuration surface for DecentDB.

use std::fmt;
use std::path::PathBuf;

use crate::error::{DbError, Result};
use crate::extensions::ExtensionTrustAnchor;
use crate::storage::page;
use zeroize::Zeroize;

/// Cross-process WAL coordination mode for native on-disk databases.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessCoordinationMode {
    /// Enable process coordination when the VFS can provide the required
    /// local-file locks. In-memory databases remain process-local.
    Auto,
    /// Require process coordination. Open fails if the selected VFS cannot
    /// provide the coordination contract.
    Required,
    /// Bypass process coordination. Safe only when one OS process can access
    /// the database file.
    SingleProcessUnsafe,
}

impl ProcessCoordinationMode {
    #[must_use]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Required => "required",
            Self::SingleProcessUnsafe => "single_process_unsafe",
        }
    }
}

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

/// Application-supplied encryption key material for local TDE.
///
/// DecentDB owns a copy of these bytes so it can derive per-file encryption
/// keys while the database handle is open. Debug output is intentionally
/// redacted.
#[derive(Clone, Eq, PartialEq)]
pub struct EncryptionKey {
    bytes: Vec<u8>,
}

impl EncryptionKey {
    /// Copies raw application key bytes into an engine-owned key buffer.
    ///
    /// The key must not be empty. Callers should use high-entropy key bytes
    /// from their KMS or platform secret store.
    pub fn from_bytes(bytes: impl AsRef<[u8]>) -> Result<Self> {
        let bytes = bytes.as_ref();
        if bytes.is_empty() {
            return Err(DbError::internal("encryption key must not be empty"));
        }
        Ok(Self {
            bytes: bytes.to_vec(),
        })
    }

    pub(crate) fn expose_secret(&self) -> &[u8] {
        &self.bytes
    }
}

impl fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncryptionKey")
            .field("len", &self.bytes.len())
            .field("redacted", &true)
            .finish()
    }
}

impl Drop for EncryptionKey {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

/// Local transparent data encryption configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DbEncryptionConfig {
    pub key: EncryptionKey,
}

impl DbEncryptionConfig {
    /// Creates a TDE configuration from application-owned key bytes.
    pub fn from_key_bytes(bytes: impl AsRef<[u8]>) -> Result<Self> {
        Ok(Self {
            key: EncryptionKey::from_bytes(bytes)?,
        })
    }
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
    /// Maximum number of reusable page-sized buffers retained by each pager.
    ///
    /// The pool only recycles buffers already created by pager hot paths and
    /// never allocates during recycle. `0` disables pooling.
    ///
    /// Default: `256`.
    pub page_pool_max: usize,
    pub wal_sync_mode: WalSyncMode,
    /// Cross-process WAL coordination mode for native on-disk databases.
    ///
    /// Default: `ProcessCoordinationMode::Auto`.
    pub process_coordination: ProcessCoordinationMode,
    /// Maximum time a process-coordination writer/checkpoint lock acquisition
    /// waits before returning `DbError::Timeout`. `0` means fail immediately
    /// with `DbError::Busy`.
    ///
    /// Default: `30_000`.
    pub process_coordination_timeout_ms: u64,
    pub checkpoint_timeout_sec: u64,
    pub trigram_postings_threshold: usize,
    pub temp_dir: PathBuf,

    /// Optional local transparent data encryption for database, WAL, and sync
    /// journal files.
    ///
    /// When set, DecentDB wraps the configured VFS and encrypts all logical
    /// bytes after a small plaintext per-file TDE prefix. Existing plaintext
    /// databases cannot be opened with encryption enabled without an explicit
    /// migration/export step.
    ///
    /// Default: `None`.
    pub encryption: Option<DbEncryptionConfig>,

    /// Trigger an automatic checkpoint when the in-memory WAL has accumulated
    /// at least this many distinct dirty page versions since the last
    /// checkpoint. `0` disables the page-count trigger. The trigger only
    /// fires when there are no active readers; otherwise the WAL is allowed
    /// to keep growing until readers complete (per ADR 0019).
    ///
    /// Default: `4096` (≈ 16 MB at the default 4 KB page size).
    ///
    /// See ADR 0137 — Size-Based Auto-Checkpoint Trigger.
    pub wal_checkpoint_threshold_pages: u32,

    /// Trigger an automatic checkpoint when the WAL has grown by at least
    /// this many bytes since the last checkpoint. `0` disables the
    /// byte-count trigger. Bounds writer-side memory growth on workloads
    /// that touch many distinct pages without exceeding the page-count
    /// threshold (e.g. wide-row inserts on large pages).
    ///
    /// Default: `64 * 1024 * 1024` (64 MB).
    ///
    /// See ADR 0137 — Size-Based Auto-Checkpoint Trigger.
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
    /// See ADR 0138 — Post-Checkpoint Heap Release.
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

    /// Keep paged row sources resident on the current handle after successful
    /// write commits instead of immediately returning them to the deferred set.
    ///
    /// Default: `false`. The default preserves the low-memory profile where
    /// committed paged tables can be dropped and reloaded on demand. Set this
    /// to `true` for hot-read workloads that bulk load data and then run many
    /// reads on the same handle, accepting higher process memory in exchange
    /// for avoiding repeated row-source reloads.
    pub retain_paged_row_sources_after_commit: bool,

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

    /// Advertises that high-level bindings may use the engine-owned write
    /// queue for normal execution paths.
    ///
    /// The native Rust direct APIs remain direct unless callers explicitly use
    /// the queued execution APIs. This preserves low-contention direct-path
    /// behavior while letting bindings opt into a consistent queue contract.
    ///
    /// Default: `false`.
    pub write_queue_enabled: bool,

    /// Maximum number of admitted queued write requests waiting for execution.
    /// Must be at least `1`; `Db::open` and `Db::create` clamp `0` to `1`.
    ///
    /// Default: `1024`.
    pub write_queue_capacity: usize,

    /// Default queued-write timeout in milliseconds. `0` means no default
    /// timeout; callers can still pass an explicit per-call timeout.
    ///
    /// Default: `0`.
    pub write_queue_default_timeout_ms: u64,

    /// When `true`, queued commits under synchronous WAL modes may share one
    /// physical WAL sync, and callers receive success only after the covering
    /// sync completes.
    ///
    /// Default: `true`.
    pub write_queue_strict_group_commit: bool,

    /// Maximum number of ready queued requests drained by one queue executor
    /// pass before yielding to waiters.
    ///
    /// Default: `64`.
    pub write_queue_max_batch: usize,

    /// Optional delay used to collect more ready queued writes before a strict
    /// group-commit sync. The default `0` avoids sleeping on the single-writer
    /// path and only batches work already ready in the queue.
    ///
    /// Default: `0`.
    pub write_queue_max_group_delay_us: u64,

    /// Default per-watch event queue capacity for reactive subscriptions.
    ///
    /// Default: `1024`.
    pub reactive_watch_queue_capacity: usize,

    /// Maximum per-watch event queue capacity accepted from callers.
    ///
    /// Default: `8192`.
    pub reactive_watch_queue_max_capacity: usize,

    /// Maximum row-level changes retained in one reactive commit event before
    /// degrading to table-level invalidation. `0` disables row-change capture
    /// limits.
    ///
    /// Default: `4096`.
    pub reactive_max_row_changes_per_event: usize,

    /// Connection-level allowlist for enabled Lua extension packages.
    ///
    /// Installed packages are inert until enabled in the database and allowed
    /// by the connection. Each entry must match the extension name and exact
    /// `sha256:...` package content hash.
    ///
    /// Default: empty, so no Lua extension code executes.
    pub extension_trust_anchors: Vec<ExtensionTrustAnchor>,

    /// Development-only override that allows unsigned extension installation
    /// and execution without a name/hash allowlist.
    ///
    /// This is intentionally off by default. Production callers should prefer
    /// exact `extension_trust_anchors`.
    pub extension_unsigned_development_mode: bool,
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
            page_pool_max: 256,
            wal_sync_mode: WalSyncMode::Full,
            process_coordination: ProcessCoordinationMode::Auto,
            process_coordination_timeout_ms: 30_000,
            checkpoint_timeout_sec: 30,
            trigram_postings_threshold: 100_000,
            temp_dir: default_temp_dir(),
            encryption: None,
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
            retain_paged_row_sources_after_commit: false,
            auto_checkpoint_on_open_mb: 16,
            write_queue_enabled: false,
            write_queue_capacity: 1024,
            write_queue_default_timeout_ms: 0,
            write_queue_strict_group_commit: true,
            write_queue_max_batch: 64,
            write_queue_max_group_delay_us: 0,
            reactive_watch_queue_capacity: 1024,
            reactive_watch_queue_max_capacity: 8192,
            reactive_max_row_changes_per_event: 4096,
            extension_trust_anchors: Vec::new(),
            extension_unsigned_development_mode: false,
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn default_temp_dir() -> PathBuf {
    PathBuf::from("/tmp")
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn default_temp_dir() -> PathBuf {
    std::env::temp_dir()
}

#[cfg(test)]
mod tests {
    use super::{DbConfig, ProcessCoordinationMode, WalSyncMode};
    use crate::storage::page;

    #[test]
    fn default_config_matches_spec_requirements() {
        let config = DbConfig::default();

        assert_eq!(config.page_size, page::DEFAULT_PAGE_SIZE);
        assert_eq!(config.cache_size_mb, 4);
        assert_eq!(config.cached_payloads_max_entries, 1024);
        assert_eq!(config.page_pool_max, 256);
        assert_eq!(config.wal_sync_mode, WalSyncMode::Full);
        assert_eq!(config.process_coordination, ProcessCoordinationMode::Auto);
        assert_eq!(config.process_coordination_timeout_ms, 30_000);
        assert_eq!(config.checkpoint_timeout_sec, 30);
        assert_eq!(config.trigram_postings_threshold, 100_000);
        assert!(!config.temp_dir.as_os_str().is_empty());
        assert!(config.encryption.is_none());
        assert_eq!(config.wal_checkpoint_threshold_pages, 4096);
        assert_eq!(config.wal_checkpoint_threshold_bytes, 64 * 1024 * 1024);
        assert_eq!(config.wal_resident_versions_per_page, 16);
        assert!(config.defer_table_materialization);
        assert!(!config.persistent_pk_index);
        assert!(config.paged_row_storage);
        assert!(!config.retain_paged_row_sources_after_commit);
        assert!(!config.write_queue_enabled);
        assert_eq!(config.write_queue_capacity, 1024);
        assert_eq!(config.write_queue_default_timeout_ms, 0);
        assert!(config.write_queue_strict_group_commit);
        assert_eq!(config.write_queue_max_batch, 64);
        assert_eq!(config.write_queue_max_group_delay_us, 0);
        assert_eq!(config.reactive_watch_queue_capacity, 1024);
        assert_eq!(config.reactive_watch_queue_max_capacity, 8192);
        assert_eq!(config.reactive_max_row_changes_per_event, 4096);
        assert!(config.extension_trust_anchors.is_empty());
        assert!(!config.extension_unsigned_development_mode);
        // Default depends on platform; just assert the field is reachable.
        let _ = config.release_freed_memory_after_checkpoint;
    }
}

//! Paged on-disk WAL index sidecar — initial scaffold for ADR 0141.
//!
//! The current `WalIndex` is fully resident in RAM. On databases with
//! millions of dirty pages the `HashMap<PageId, Vec<WalVersion>>` and its
//! per-page version chains dominate engine memory after `Db::open`.
//!
//! ADR 0141 proposes a paged sidecar file (`<db>.wal-idx`) so that only a
//! bounded "hot set" of recently-touched page chains lives in memory; the
//! cold tail spills to disk and is paged back on demand.
//!
//! Scope of this scaffold:
//! - Reserve the sidecar file extension and on-disk magic bytes so future
//!   versions can detect format mismatches without a flag day.
//! - Add `DbConfig::wal_index_hot_set_pages` so embedders can opt in once
//!   the full implementation lands. Default `0` means "unbounded /
//!   in-memory" — exactly today's behavior.
//! - Provide `WalIndexBackend` so the in-memory implementation and the
//!   future sidecar implementation share a type. Today only the in-memory
//!   variant is wired in.
//!
//! Out of scope (tracked as ADR 0141 follow-up work):
//! - Sidecar file format (chain page layout, free-list, checksums).
//! - Spill / refill policy and the LRU governing the hot set.
//! - Recovery / truncation interactions with checkpoint.
//! - Metrics surface.

#![allow(dead_code)]

/// File extension for the WAL index sidecar (e.g. `mydb.ddb.wal-idx`).
pub(crate) const WAL_INDEX_SIDECAR_EXT: &str = "wal-idx";

/// Magic bytes prefixing the sidecar header so a future format-version
/// check can detect mis-matched files.
///
/// The literal value is `DDB-WIDX` (8 bytes) and is intentionally
/// reserved now even though no writer emits it yet.
pub(crate) const WAL_INDEX_SIDECAR_MAGIC: &[u8; 8] = b"DDB-WIDX";

/// On-disk format version stamped into the sidecar header. Bumping this
/// requires an ADR-tracked migration path (see AGENTS.md §7).
pub(crate) const WAL_INDEX_SIDECAR_VERSION: u16 = 0;

/// Tag identifying the active WAL-index implementation. Used by tests and
/// future telemetry to assert that the sidecar code path is or is not in
/// effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalIndexBackendKind {
    /// Fully-resident `HashMap<PageId, Vec<WalVersion>>` — current
    /// behavior. Selected when `wal_index_hot_set_pages == 0`.
    InMemory,
    /// Paged sidecar (ADR 0141 follow-up). Reserved; not yet wired up.
    PagedSidecar,
}

impl WalIndexBackendKind {
    /// Choose the backend based on the embedder's configuration. Today
    /// the function always returns `InMemory`; once the sidecar is
    /// implemented this will switch to `PagedSidecar` when the embedder
    /// opts in.
    pub(crate) fn for_hot_set_pages(hot_set_pages: u32) -> Self {
        if hot_set_pages == 0 {
            Self::InMemory
        } else {
            // Sidecar implementation is not yet wired in. Until it is, we
            // honor the configured hot-set hint by ignoring it rather
            // than panicking — embedders may set the field early so they
            // can roll out the config change ahead of the engine update.
            Self::InMemory
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn magic_bytes_are_eight_ascii_chars() {
        assert_eq!(WAL_INDEX_SIDECAR_MAGIC.len(), 8);
        assert!(WAL_INDEX_SIDECAR_MAGIC.iter().all(|b| b.is_ascii_graphic()));
    }

    #[test]
    fn zero_hot_set_picks_in_memory_backend() {
        assert_eq!(
            WalIndexBackendKind::for_hot_set_pages(0),
            WalIndexBackendKind::InMemory
        );
    }

    #[test]
    fn nonzero_hot_set_falls_back_to_in_memory_until_sidecar_lands() {
        // Until ADR 0141's full sidecar implementation lands, opting in
        // is a no-op rather than a hard error.
        assert_eq!(
            WalIndexBackendKind::for_hot_set_pages(4096),
            WalIndexBackendKind::InMemory
        );
    }
}

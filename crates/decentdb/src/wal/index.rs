//! In-memory WAL page-version index.

use std::collections::HashMap;
use std::sync::Arc;

use smallvec::SmallVec;

use crate::storage::page::PageId;

/// Payload backing a single WAL page version (slice M4).
///
/// Today every version is `Resident` — exactly the historical behavior of
/// holding an `Arc<[u8]>` in the index. The `OnDisk` variant is reserved
/// for the follow-up that demotes cold versions to a `(wal_offset,
/// frame_len)` reference and re-reads them through the WAL mmap on
/// demand. Defining both variants now lets the rest of the codebase
/// adopt the `payload_bytes()` accessor before the demotion path lands,
/// avoiding a large flag-day diff later.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WalVersionPayload {
    /// Page bytes held directly in heap. Same representation as the
    /// pre-M4 `WalVersion::data: Arc<[u8]>` field.
    Resident(Arc<[u8]>),
    /// Reserved for future use (ADR 0140 / slice M4 demotion path). Not
    /// emitted by any current writer; kept here so the `materialize`
    /// path's contract is fixed at the type level.
    #[allow(dead_code)]
    OnDisk { wal_offset: u64, frame_len: u32 },
}

impl WalVersionPayload {
    /// Borrow the page bytes. Today this is a cheap `Arc` deref; once
    /// `OnDisk` lands, the demoted variant will require a frame re-read
    /// and this signature will change to `Cow<[u8]>` or move behind an
    /// explicit `materialize()` call. The current `&[u8]` return is safe
    /// because only the `Resident` variant is ever constructed.
    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Resident(arc) => arc,
            Self::OnDisk { .. } => unreachable!(
                "WalVersionPayload::OnDisk is reserved for slice M4 follow-up; not yet emitted"
            ),
        }
    }

    /// Clone the underlying `Arc<[u8]>` (cheap reference-count bump).
    /// Mirrors the pre-M4 `Arc::clone(&version.data)` idiom.
    pub(crate) fn arc(&self) -> Arc<[u8]> {
        match self {
            Self::Resident(arc) => Arc::clone(arc),
            Self::OnDisk { .. } => unreachable!(
                "WalVersionPayload::OnDisk is reserved for slice M4 follow-up; not yet emitted"
            ),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalVersion {
    pub(crate) lsn: u64,
    pub(crate) payload: WalVersionPayload,
}

impl WalVersion {
    /// Construct a resident version from owned page bytes. Convenience
    /// wrapper over `WalVersionPayload::Resident(Arc::from(data))`.
    pub(crate) fn resident(lsn: u64, data: Arc<[u8]>) -> Self {
        Self {
            lsn,
            payload: WalVersionPayload::Resident(data),
        }
    }
}

/// Inline storage for the common single-version-per-page case (slice M7).
/// Most pages have exactly one live WAL version between checkpoints, so
/// the inline slot eliminates a separate heap allocation and pointer
/// indirection per page on workloads that touch millions of distinct
/// pages.
type VersionVec = SmallVec<[WalVersion; 1]>;

#[derive(Clone, Debug, Default)]
pub(crate) struct WalIndex {
    pages: HashMap<PageId, VersionVec>,
}

impl WalIndex {
    pub(crate) fn add_version(
        &mut self,
        page_id: PageId,
        version: WalVersion,
        retain_history: bool,
    ) {
        let versions = self.pages.entry(page_id).or_default();
        if retain_history {
            debug_assert!(
                versions.last().is_none_or(|entry| entry.lsn <= version.lsn),
                "WAL page versions should be appended in nondecreasing LSN order",
            );
            versions.push(version);
        } else {
            versions.clear();
            versions.push(version);
        }
    }

    pub(crate) fn latest_visible(&self, page_id: PageId, snapshot_lsn: u64) -> Option<&WalVersion> {
        self.pages.get(&page_id).and_then(|versions| {
            versions
                .iter()
                .rev()
                .find(|version| version.lsn <= snapshot_lsn)
        })
    }

    #[cfg(test)]
    pub(crate) fn latest_versions_at_or_before(&self, safe_lsn: u64) -> Vec<(PageId, WalVersion)> {
        let mut out = Vec::with_capacity(self.pages.len());
        self.populate_latest_versions_at_or_before(safe_lsn, &mut out);
        out
    }

    /// Same as `latest_versions_at_or_before` but reuses caller-provided
    /// storage. Used by the checkpoint path (slice M5) to avoid a fresh
    /// `Vec` allocation on every checkpoint.
    pub(crate) fn populate_latest_versions_at_or_before(
        &self,
        safe_lsn: u64,
        out: &mut Vec<(PageId, WalVersion)>,
    ) {
        out.clear();
        if out.capacity() < self.pages.len() {
            out.reserve(self.pages.len() - out.capacity());
        }
        for (page_id, entries) in &self.pages {
            if let Some(entry) = entries.iter().rev().find(|entry| entry.lsn <= safe_lsn) {
                out.push((*page_id, entry.clone()));
            }
        }
        out.sort_by_key(|(page_id, _)| *page_id);
    }

    pub(crate) fn prune_at_or_below(&mut self, page_ids: &[PageId], safe_lsn: u64) {
        for page_id in page_ids {
            if let Some(entries) = self.pages.get_mut(page_id) {
                let original_len = entries.len();
                entries.retain(|entry| entry.lsn > safe_lsn);
                if entries.is_empty() {
                    self.pages.remove(page_id);
                } else if entries.len() != original_len {
                    entries.shrink_to_fit();
                }
            }
        }
        self.pages.shrink_to_fit();
    }

    pub(crate) fn clear(&mut self) {
        self.pages = HashMap::new();
    }

    #[must_use]
    pub(crate) fn version_count(&self) -> usize {
        self.pages.values().map(SmallVec::len).sum()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{WalVersion, WalVersionPayload};

    #[test]
    fn wal_version_clone_shares_arc_backing_storage() {
        let data = Arc::<[u8]>::from(vec![0xAB; 16]);
        let first = WalVersion::resident(42, Arc::clone(&data));
        let second = first.clone();

        let WalVersionPayload::Resident(first_data) = &first.payload else {
            panic!("expected resident payload");
        };
        let WalVersionPayload::Resident(second_data) = &second.payload else {
            panic!("expected resident payload");
        };
        assert!(Arc::ptr_eq(first_data, second_data));
    }
}

//! In-memory WAL page-version hot set.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use smallvec::SmallVec;

use crate::storage::page::PageId;

use super::format::FrameEncoding;

/// Payload backing a single WAL page version (slice M4).
///
/// Hot versions stay `Resident`, while colder versions can be demoted to
/// `OnDisk` and later rematerialized from the WAL file by the read path
/// in `wal/mod.rs`. The index keeps the WAL offset, encoded frame length,
/// and frame encoding so rematerialization can reconstruct either a full
/// page frame or a delta frame on demand.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WalVersionPayload {
    /// Page bytes held directly in heap. Same representation as the
    /// pre-M4 `WalVersion::data: Arc<[u8]>` field.
    Resident {
        data: Arc<[u8]>,
        wal_offset: u64,
        frame_len: u32,
        encoding: FrameEncoding,
    },
    /// Cold page version whose bytes live only in the WAL file.
    OnDisk {
        wal_offset: u64,
        frame_len: u32,
        encoding: FrameEncoding,
    },
}

impl WalVersionPayload {
    /// Borrow the resident page bytes.
    ///
    /// Demoted `OnDisk` payloads must be rematerialized by the WAL read
    /// path before a direct borrow is possible.
    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Resident { data, .. } => data,
            Self::OnDisk { .. } => unreachable!("demoted WAL payload must be materialized first"),
        }
    }

    #[must_use]
    pub(crate) fn wal_metadata(&self) -> (u64, u32, FrameEncoding) {
        match self {
            Self::Resident {
                wal_offset,
                frame_len,
                encoding,
                ..
            }
            | Self::OnDisk {
                wal_offset,
                frame_len,
                encoding,
            } => (*wal_offset, *frame_len, *encoding),
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
    pub(crate) fn resident(
        lsn: u64,
        wal_offset: u64,
        frame_len: u32,
        encoding: FrameEncoding,
        data: Arc<[u8]>,
    ) -> Self {
        Self {
            lsn,
            payload: WalVersionPayload::Resident {
                data,
                wal_offset,
                frame_len,
                encoding,
            },
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
    access_order: VecDeque<(PageId, u64)>,
    page_touch_epochs: HashMap<PageId, u64>,
    next_touch_epoch: u64,
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
        self.touch(page_id);
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

    #[cfg(test)]
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
        self.access_order.clear();
        self.page_touch_epochs.clear();
        self.next_touch_epoch = 0;
    }

    pub(crate) fn demote_cold(
        &mut self,
        min_reader_snapshot: Option<u64>,
        retain_recent_per_page: u32,
    ) {
        let retain_recent_per_page = retain_recent_per_page as usize;
        for versions in self.pages.values_mut() {
            let demotable_prefix_len = versions.len().saturating_sub(retain_recent_per_page);
            for version in versions.iter_mut().take(demotable_prefix_len) {
                if min_reader_snapshot.is_some_and(|snapshot| version.lsn <= snapshot) {
                    continue;
                }
                let WalVersionPayload::Resident {
                    wal_offset,
                    frame_len,
                    encoding,
                    ..
                } = version.payload
                else {
                    continue;
                };
                version.payload = WalVersionPayload::OnDisk {
                    wal_offset,
                    frame_len,
                    encoding,
                };
            }
        }
    }

    #[must_use]
    pub(crate) fn version_count(&self) -> usize {
        self.pages.values().map(SmallVec::len).sum()
    }

    #[must_use]
    pub(crate) fn version_counts_by_payload(&self) -> (usize, usize) {
        let mut resident = 0usize;
        let mut on_disk = 0usize;
        for versions in self.pages.values() {
            for version in versions {
                match version.payload {
                    WalVersionPayload::Resident { .. } => resident += 1,
                    WalVersionPayload::OnDisk { .. } => on_disk += 1,
                }
            }
        }
        (resident, on_disk)
    }

    pub(crate) fn contains_page(&self, page_id: PageId) -> bool {
        self.pages.contains_key(&page_id)
    }

    pub(crate) fn seed_latest(&mut self, page_id: PageId, version: WalVersion) {
        let mut versions = VersionVec::new();
        versions.push(version);
        self.pages.insert(page_id, versions);
        self.touch(page_id);
    }

    pub(crate) fn touch(&mut self, page_id: PageId) {
        self.next_touch_epoch = self.next_touch_epoch.wrapping_add(1);
        let epoch = self.next_touch_epoch;
        self.page_touch_epochs.insert(page_id, epoch);
        self.access_order.push_back((page_id, epoch));
    }

    pub(crate) fn spill_one_cold_latest(
        &mut self,
        hot_set_pages: usize,
    ) -> Option<(PageId, WalVersion)> {
        while self.pages.len() > hot_set_pages {
            let (page_id, epoch) = self.access_order.pop_front()?;
            if self.page_touch_epochs.get(&page_id).copied() != Some(epoch) {
                continue;
            }
            let should_spill = self
                .pages
                .get(&page_id)
                .is_some_and(|versions| versions.len() == 1);
            if !should_spill {
                continue;
            }
            self.page_touch_epochs.remove(&page_id);
            let versions = self.pages.remove(&page_id).expect("page exists");
            return versions
                .into_iter()
                .next()
                .map(|version| (page_id, version));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::wal::format::FrameEncoding;

    use super::{WalIndex, WalVersion, WalVersionPayload};

    #[test]
    fn wal_version_clone_shares_arc_backing_storage() {
        let data = Arc::<[u8]>::from(vec![0xAB; 16]);
        let first = WalVersion::resident(42, 38, 4, FrameEncoding::Page, Arc::clone(&data));
        let second = first.clone();

        let WalVersionPayload::Resident {
            data: first_data, ..
        } = &first.payload
        else {
            panic!("expected resident payload");
        };
        let WalVersionPayload::Resident {
            data: second_data, ..
        } = &second.payload
        else {
            panic!("expected resident payload");
        };
        assert!(Arc::ptr_eq(first_data, second_data));
    }

    #[test]
    fn payload_breakdown_counts_resident_and_on_disk_versions() {
        let mut index = WalIndex::default();
        index.add_version(
            1,
            WalVersion {
                lsn: 10,
                payload: WalVersionPayload::Resident {
                    data: Arc::<[u8]>::from(vec![0xAA; 16]),
                    wal_offset: 32,
                    frame_len: 29,
                    encoding: FrameEncoding::Page,
                },
            },
            true,
        );
        index.add_version(
            1,
            WalVersion {
                lsn: 20,
                payload: WalVersionPayload::OnDisk {
                    wal_offset: 128,
                    frame_len: 64,
                    encoding: FrameEncoding::PageDelta,
                },
            },
            true,
        );

        assert_eq!(index.version_counts_by_payload(), (1, 1));
    }

    #[test]
    fn demote_cold_keeps_recent_and_reader_visible_versions_resident() {
        let mut index = WalIndex::default();
        for lsn in [10_u64, 20, 30] {
            index.add_version(
                1,
                WalVersion::resident(
                    lsn,
                    lsn - 4,
                    4,
                    FrameEncoding::Page,
                    Arc::<[u8]>::from(vec![lsn as u8; 16]),
                ),
                true,
            );
        }

        index.demote_cold(Some(15), 1);

        assert!(matches!(
            index.latest_visible(1, 10).unwrap().payload,
            WalVersionPayload::Resident { .. }
        ));
        assert!(matches!(
            index.latest_visible(1, 20).unwrap().payload,
            WalVersionPayload::OnDisk { .. }
        ));
        assert!(matches!(
            index.latest_visible(1, 30).unwrap().payload,
            WalVersionPayload::Resident { .. }
        ));
    }

    #[test]
    fn spill_one_cold_latest_spills_single_delta_version() {
        let mut index = WalIndex::default();
        index.seed_latest(
            7,
            WalVersion {
                lsn: 11,
                payload: WalVersionPayload::OnDisk {
                    wal_offset: 128,
                    frame_len: 64,
                    encoding: FrameEncoding::PageDelta,
                },
            },
        );
        index.seed_latest(
            8,
            WalVersion::resident(
                12,
                256,
                128,
                FrameEncoding::Page,
                Arc::<[u8]>::from(vec![0x44; 16]),
            ),
        );

        let (page_id, version) = index
            .spill_one_cold_latest(1)
            .expect("one cold page should spill");
        assert_eq!(page_id, 7);
        assert!(matches!(
            version.payload,
            WalVersionPayload::OnDisk {
                encoding: FrameEncoding::PageDelta,
                ..
            }
        ));
        assert!(index.latest_visible(8, u64::MAX).is_some());
    }
}

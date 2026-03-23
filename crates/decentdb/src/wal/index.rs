//! In-memory WAL page-version index.

use std::collections::HashMap;

use crate::storage::page::PageId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalVersion {
    pub(crate) lsn: u64,
    pub(crate) data: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct WalIndex {
    pages: HashMap<PageId, Vec<WalVersion>>,
}

impl WalIndex {
    pub(crate) fn add_version(&mut self, page_id: PageId, version: WalVersion) {
        let versions = self.pages.entry(page_id).or_default();
        versions.push(version);
        versions.sort_by_key(|entry| entry.lsn);
    }

    pub(crate) fn latest_visible(&self, page_id: PageId, snapshot_lsn: u64) -> Option<&WalVersion> {
        self.pages.get(&page_id).and_then(|versions| {
            versions
                .iter()
                .rev()
                .find(|version| version.lsn <= snapshot_lsn)
        })
    }

    pub(crate) fn latest_versions_at_or_before(&self, safe_lsn: u64) -> Vec<(PageId, WalVersion)> {
        let mut versions = Vec::new();
        for (page_id, entries) in &self.pages {
            if let Some(entry) = entries.iter().rev().find(|entry| entry.lsn <= safe_lsn) {
                versions.push((*page_id, entry.clone()));
            }
        }
        versions.sort_by_key(|(page_id, _)| *page_id);
        versions
    }

    pub(crate) fn prune_at_or_below(&mut self, page_ids: &[PageId], safe_lsn: u64) {
        for page_id in page_ids {
            if let Some(entries) = self.pages.get_mut(page_id) {
                entries.retain(|entry| entry.lsn > safe_lsn);
                if entries.is_empty() {
                    self.pages.remove(page_id);
                }
            }
        }
    }

    pub(crate) fn clear(&mut self) {
        self.pages.clear();
    }

    #[must_use]
    pub(crate) fn version_count(&self) -> usize {
        self.pages.values().map(std::vec::Vec::len).sum()
    }
}

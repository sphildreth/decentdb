//! Fixed-size page helpers used by bootstrap and later pager slices.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::{DbError, Result};

pub(crate) type PageId = u32;
pub(crate) const DEFAULT_PAGE_SIZE: u32 = 4096;
pub(crate) const SUPPORTED_PAGE_SIZES: [u32; 3] = [4096, 8192, 16384];
pub(crate) const HEADER_PAGE_ID: PageId = 1;
pub(crate) const CATALOG_ROOT_PAGE_ID: PageId = 2;

#[must_use]
pub(crate) fn is_supported_page_size(page_size: u32) -> bool {
    SUPPORTED_PAGE_SIZES.contains(&page_size)
}

#[must_use]
pub(crate) fn zeroed_page(page_size: u32) -> Vec<u8> {
    vec![0_u8; page_size as usize]
}

#[must_use]
pub(crate) fn page_offset(page_id: PageId, page_size: u32) -> u64 {
    u64::from(page_id.saturating_sub(1)) * u64::from(page_size)
}

#[must_use]
pub(crate) fn page_count_for_len(len: u64, page_size: u32) -> PageId {
    (len / u64::from(page_size)) as PageId
}

pub(crate) fn validate_page_id(page_id: PageId) -> Result<()> {
    if page_id == 0 {
        Err(DbError::corruption("page id 0 is invalid"))
    } else {
        Ok(())
    }
}

#[allow(dead_code)]
pub(crate) trait PageStore: std::fmt::Debug {
    fn page_size(&self) -> u32;
    fn allocate_page(&mut self) -> Result<PageId>;
    fn free_page(&mut self, page_id: PageId) -> Result<()>;
    fn read_page(&self, page_id: PageId) -> Result<Arc<[u8]>>;
    fn write_page(&mut self, page_id: PageId, data: &[u8]) -> Result<()>;

    fn write_page_owned(&mut self, page_id: PageId, data: Vec<u8>) -> Result<()> {
        self.write_page(page_id, &data)
    }
}

/// Simple in-memory page store used by the Phase 2 record, B+Tree, and search
/// unit tests before the structures are wired into the full pager lifecycle.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct InMemoryPageStore {
    page_size: u32,
    next_page_id: PageId,
    free_pages: Vec<PageId>,
    pages: BTreeMap<PageId, Vec<u8>>,
}

impl InMemoryPageStore {
    pub(crate) fn new(page_size: u32) -> Self {
        Self {
            page_size,
            next_page_id: CATALOG_ROOT_PAGE_ID + 1,
            free_pages: Vec::new(),
            pages: BTreeMap::new(),
        }
    }

    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn contains_page(&self, page_id: PageId) -> bool {
        self.pages.contains_key(&page_id)
    }

    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn allocated_page_count(&self) -> usize {
        self.pages.len()
    }
}

impl Default for InMemoryPageStore {
    fn default() -> Self {
        Self::new(DEFAULT_PAGE_SIZE)
    }
}

impl PageStore for InMemoryPageStore {
    fn page_size(&self) -> u32 {
        self.page_size
    }

    fn allocate_page(&mut self) -> Result<PageId> {
        if let Some(page_id) = self.free_pages.pop() {
            self.pages.insert(page_id, zeroed_page(self.page_size));
            return Ok(page_id);
        }

        let page_id = self.next_page_id;
        self.next_page_id = self.next_page_id.saturating_add(1);
        self.pages.insert(page_id, zeroed_page(self.page_size));
        Ok(page_id)
    }

    fn free_page(&mut self, page_id: PageId) -> Result<()> {
        validate_page_id(page_id)?;
        self.pages.remove(&page_id);
        if !self.free_pages.contains(&page_id) {
            self.free_pages.push(page_id);
        }
        Ok(())
    }

    fn read_page(&self, page_id: PageId) -> Result<Arc<[u8]>> {
        validate_page_id(page_id)?;
        Ok(Arc::from(
            self.pages
                .get(&page_id)
                .cloned()
                .unwrap_or_else(|| zeroed_page(self.page_size)),
        ))
    }

    fn write_page(&mut self, page_id: PageId, data: &[u8]) -> Result<()> {
        validate_page_id(page_id)?;
        if data.len() != self.page_size as usize {
            return Err(DbError::internal(format!(
                "page {page_id} write length {} does not match configured page size {}",
                data.len(),
                self.page_size
            )));
        }
        self.pages.insert(page_id, data.to_vec());
        Ok(())
    }

    fn write_page_owned(&mut self, page_id: PageId, data: Vec<u8>) -> Result<()> {
        validate_page_id(page_id)?;
        if data.len() != self.page_size as usize {
            return Err(DbError::internal(format!(
                "page {page_id} write length {} does not match configured page size {}",
                data.len(),
                self.page_size
            )));
        }
        self.pages.insert(page_id, data);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zeroed_page_and_page_count() {
        assert!(is_supported_page_size(DEFAULT_PAGE_SIZE));
        assert!(!is_supported_page_size(123));
        let z = zeroed_page(4096);
        assert_eq!(z.len(), 4096);
        assert_eq!(page_offset(1, 4096), 0);
        assert_eq!(page_offset(2, 4096), 4096);
        assert_eq!(page_count_for_len(8192, 4096), 2);
    }

    #[test]
    fn in_memory_store_allocate_free_read_write() {
        let mut store = InMemoryPageStore::new(4096);
        assert_eq!(store.page_size(), 4096);
        let pid = store.allocate_page().unwrap();
        assert!(store.contains_page(pid));
        assert_eq!(store.allocated_page_count(), 1);

        // write page with wrong size -> error
        let small = vec![0u8; 100];
        assert!(store.write_page(pid, &small).is_err());

        // write with correct size
        let data = vec![7u8; 4096];
        store.write_page(pid, &data).unwrap();
        let read = store.read_page(pid).unwrap();
        assert_eq!(read.to_vec(), data);

        // free page
        store.free_page(pid).unwrap();
        assert!(!store.contains_page(pid));

        // allocate uses freed page
        let pid2 = store.allocate_page().unwrap();
        assert_eq!(pid2, pid);
    }

    #[test]
    fn read_missing_page_returns_zeroed() {
        let store = InMemoryPageStore::new(4096);
        let read = store.read_page(100).unwrap();
        assert_eq!(read.len(), 4096);
        assert!(read.iter().all(|&b| b == 0));
    }

    #[test]
    fn validate_page_id_rejects_zero() {
        assert!(validate_page_id(0).is_err());
    }
}

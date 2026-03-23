//! Fixed-size page helpers used by bootstrap and later pager slices.

use std::collections::BTreeMap;

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
    fn read_page(&self, page_id: PageId) -> Result<Vec<u8>>;
    fn write_page(&mut self, page_id: PageId, data: &[u8]) -> Result<()>;
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

    fn read_page(&self, page_id: PageId) -> Result<Vec<u8>> {
        validate_page_id(page_id)?;
        Ok(self
            .pages
            .get(&page_id)
            .cloned()
            .unwrap_or_else(|| zeroed_page(self.page_size)))
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
}

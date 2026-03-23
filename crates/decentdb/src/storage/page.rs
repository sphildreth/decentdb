//! Fixed-size page helpers used by bootstrap and later pager slices.

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

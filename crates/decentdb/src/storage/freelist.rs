//! Freelist bootstrap metadata.

use crate::error::{DbError, Result};

use super::page::{self, PageId};

/// Freelist pointers stored in the fixed database header.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FreelistState {
    pub(crate) root_page_id: u32,
    pub(crate) head_page_id: u32,
    pub(crate) page_count: u32,
}

pub(crate) const FREELIST_NEXT_OFFSET: usize = 0;

#[must_use]
pub(crate) fn encode_freelist_page(page_size: u32, next_page_id: PageId) -> Vec<u8> {
    let mut page = page::zeroed_page(page_size);
    page[FREELIST_NEXT_OFFSET..FREELIST_NEXT_OFFSET + 4]
        .copy_from_slice(&next_page_id.to_le_bytes());
    page
}

pub(crate) fn decode_freelist_next(page: &[u8]) -> Result<PageId> {
    if page.len() < 4 {
        return Err(DbError::corruption(
            "freelist page is shorter than next-page pointer",
        ));
    }

    let mut raw = [0_u8; 4];
    raw.copy_from_slice(&page[FREELIST_NEXT_OFFSET..FREELIST_NEXT_OFFSET + 4]);
    Ok(u32::from_le_bytes(raw))
}

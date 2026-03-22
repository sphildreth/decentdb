//! Fixed-size page helpers used by bootstrap and later pager slices.

pub(crate) const DEFAULT_PAGE_SIZE: u32 = 4096;
pub(crate) const SUPPORTED_PAGE_SIZES: [u32; 3] = [4096, 8192, 16384];

#[must_use]
pub(crate) fn is_supported_page_size(page_size: u32) -> bool {
    SUPPORTED_PAGE_SIZES.contains(&page_size)
}

#[must_use]
pub(crate) fn zeroed_page(page_size: u32) -> Vec<u8> {
    vec![0_u8; page_size as usize]
}

//! Freelist bootstrap metadata.

/// Freelist pointers stored in the fixed database header.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FreelistState {
    pub(crate) root_page_id: u32,
    pub(crate) head_page_id: u32,
    pub(crate) page_count: u32,
}

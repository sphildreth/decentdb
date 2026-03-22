//! Fixed-page storage primitives and bootstrap helpers.

pub(crate) mod checksum;
pub(crate) mod freelist;
pub(crate) mod header;
pub(crate) mod page;

pub(crate) use header::{read_database_header, write_database_bootstrap, DatabaseHeader};

/// Placeholder pager owner until the Phase 1 pager/cache implementation lands.
#[derive(Debug, Default)]
pub(crate) struct PagerHandle;

impl PagerHandle {
    pub(crate) const fn placeholder() -> Self {
        Self
    }
}

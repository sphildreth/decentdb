//! Fixed-page storage primitives and bootstrap helpers.

pub(crate) mod cache;
pub(crate) mod checksum;
pub(crate) mod freelist;
pub(crate) mod header;
pub(crate) mod page;
pub(crate) mod pager;

pub use header::DB_FORMAT_VERSION;
pub(crate) use header::{
    read_database_header_vfs, read_database_header_vfs_loose, write_database_bootstrap_vfs,
    DatabaseHeader,
};
pub(crate) use pager::PagerHandle;

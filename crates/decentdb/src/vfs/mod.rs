//! Virtual filesystem abstractions for database and WAL I/O.

/// Placeholder VFS owner until the Phase 1 VFS implementation lands.
#[derive(Debug, Default)]
pub(crate) struct VfsHandle;

impl VfsHandle {
    pub(crate) const fn placeholder() -> Self {
        Self
    }
}

//! System catalog ownership and schema bootstrap scaffolding.

/// Placeholder catalog owner until catalog bootstrap lands.
#[derive(Debug, Default)]
pub(crate) struct CatalogHandle;

impl CatalogHandle {
    pub(crate) const fn placeholder() -> Self {
        Self
    }
}

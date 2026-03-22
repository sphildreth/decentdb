//! Write-ahead log ownership and recovery scaffolding.

/// Placeholder WAL owner until the Phase 1 WAL implementation lands.
#[derive(Debug, Default)]
pub(crate) struct WalHandle;

impl WalHandle {
    pub(crate) const fn placeholder() -> Self {
        Self
    }
}

//! Statement-level savepoint markers used by the SQL executor.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StatementSavepoint {
    pub(crate) snapshot_lsn: u64,
}

impl StatementSavepoint {
    #[must_use]
    pub(crate) fn new(snapshot_lsn: u64) -> Self {
        Self { snapshot_lsn }
    }
}

//! Trigram freshness tracking and lazy rebuild hooks.

use crate::error::Result;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Freshness {
    Fresh,
    Stale,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RebuildState {
    stale: bool,
}

impl RebuildState {
    #[must_use]
    pub(crate) fn freshness(&self) -> Freshness {
        if self.stale {
            Freshness::Stale
        } else {
            Freshness::Fresh
        }
    }

    pub(crate) fn mark_stale(&mut self) {
        self.stale = true;
    }

    pub(crate) fn mark_rebuilt(&mut self) {
        self.stale = false;
    }

    pub(crate) fn ensure_fresh<F>(&mut self, rebuild: F) -> Result<()>
    where
        F: FnOnce() -> Result<()>,
    {
        if self.stale {
            rebuild()?;
            self.stale = false;
        }
        Ok(())
    }
}

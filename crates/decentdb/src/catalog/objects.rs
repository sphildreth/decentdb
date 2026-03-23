//! Thread-safe catalog lookup helpers.

use std::sync::{Arc, RwLock};

use crate::error::{DbError, Result};

use super::schema::CatalogState;

#[derive(Clone, Debug)]
pub(crate) struct CatalogHandle {
    inner: Arc<RwLock<CatalogState>>,
}

impl CatalogHandle {
    pub(crate) fn new(state: CatalogState) -> Self {
        Self {
            inner: Arc::new(RwLock::new(state)),
        }
    }

    pub(crate) fn replace(&self, state: CatalogState) -> Result<()> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| DbError::internal("catalog lock poisoned"))?;
        *guard = state;
        Ok(())
    }

    pub(crate) fn schema_cookie(&self) -> Result<u32> {
        self.inner
            .read()
            .map(|state| state.schema_cookie)
            .map_err(|_| DbError::internal("catalog lock poisoned"))
    }
}

//! Thread-safe catalog lookup helpers.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use crate::error::{DbError, Result};

use super::schema::CatalogState;

#[derive(Clone, Debug)]
pub(crate) struct CatalogHandle {
    inner: Arc<CatalogHandleInner>,
}

#[derive(Debug)]
struct CatalogHandleInner {
    state: RwLock<CatalogState>,
    schema_cookie: AtomicU32,
}

impl CatalogHandle {
    pub(crate) fn new(state: CatalogState) -> Self {
        Self {
            inner: Arc::new(CatalogHandleInner {
                schema_cookie: AtomicU32::new(state.schema_cookie),
                state: RwLock::new(state),
            }),
        }
    }

    pub(crate) fn replace(&self, state: CatalogState) -> Result<()> {
        let mut guard = self
            .inner
            .state
            .write()
            .map_err(|_| DbError::internal("catalog lock poisoned"))?;
        let schema_cookie = state.schema_cookie;
        *guard = state;
        self.inner
            .schema_cookie
            .store(schema_cookie, Ordering::Release);
        Ok(())
    }

    pub(crate) fn schema_cookie(&self) -> Result<u32> {
        Ok(self.inner.schema_cookie.load(Ordering::Acquire))
    }
}

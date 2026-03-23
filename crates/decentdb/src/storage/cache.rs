//! Page-cache implementation with explicit pin/unpin tracking.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::error::{DbError, Result};

use super::page::PageId;

#[derive(Debug)]
pub(crate) struct PageCache {
    capacity_pages: usize,
    page_size: usize,
    state: Mutex<PageCacheState>,
}

#[derive(Debug, Default)]
struct PageCacheState {
    access_counter: u64,
    pages: HashMap<PageId, Arc<CachedPage>>,
}

#[derive(Debug)]
struct CachedPage {
    inner: Mutex<CachedPageInner>,
}

#[derive(Debug)]
struct CachedPageInner {
    data: Vec<u8>,
    dirty: bool,
    pin_count: usize,
    last_access: u64,
}

#[derive(Debug)]
pub(crate) struct PageHandle {
    page: Arc<CachedPage>,
}

impl PageCache {
    pub(crate) fn new(capacity_pages: usize, page_size: usize) -> Self {
        Self {
            capacity_pages: capacity_pages.max(1),
            page_size,
            state: Mutex::new(PageCacheState::default()),
        }
    }

    pub(crate) fn pin_or_load<F>(&self, page_id: PageId, loader: F) -> Result<PageHandle>
    where
        F: FnOnce() -> Result<Vec<u8>>,
    {
        if let Some(handle) = self.try_pin_existing(page_id)? {
            return Ok(handle);
        }

        let mut loaded = loader()?;
        if loaded.len() != self.page_size {
            return Err(DbError::internal(format!(
                "page {page_id} loaded with {} bytes; expected {}",
                loaded.len(),
                self.page_size
            )));
        }
        if loaded.is_empty() {
            loaded.resize(self.page_size, 0);
        }

        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("page cache lock poisoned"))?;
        if let Some(page) = state.pages.get(&page_id).cloned() {
            {
                let mut inner = page
                    .inner
                    .lock()
                    .map_err(|_| DbError::internal("cached page lock poisoned"))?;
                state.access_counter += 1;
                inner.pin_count += 1;
                inner.last_access = state.access_counter;
            }
            return Ok(PageHandle { page });
        }

        self.evict_one_if_needed(&mut state)?;
        state.access_counter += 1;
        let page = Arc::new(CachedPage {
            inner: Mutex::new(CachedPageInner {
                data: loaded,
                dirty: false,
                pin_count: 1,
                last_access: state.access_counter,
            }),
        });
        state.pages.insert(page_id, Arc::clone(&page));
        Ok(PageHandle { page })
    }

    pub(crate) fn insert_clean_page(&self, page_id: PageId, data: Vec<u8>) -> Result<()> {
        self.insert_page(page_id, data, false)
    }

    pub(crate) fn discard(&self, page_id: PageId) -> Result<()> {
        self.state
            .lock()
            .map_err(|_| DbError::internal("page cache lock poisoned"))?
            .pages
            .remove(&page_id);
        Ok(())
    }

    fn try_pin_existing(&self, page_id: PageId) -> Result<Option<PageHandle>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("page cache lock poisoned"))?;
        let Some(page) = state.pages.get(&page_id).cloned() else {
            return Ok(None);
        };

        {
            let mut inner = page
                .inner
                .lock()
                .map_err(|_| DbError::internal("cached page lock poisoned"))?;
            state.access_counter += 1;
            inner.pin_count += 1;
            inner.last_access = state.access_counter;
        }
        Ok(Some(PageHandle { page }))
    }

    fn insert_page(&self, page_id: PageId, data: Vec<u8>, dirty: bool) -> Result<()> {
        if data.len() != self.page_size {
            return Err(DbError::internal(format!(
                "page {page_id} inserted with {} bytes; expected {}",
                data.len(),
                self.page_size
            )));
        }

        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("page cache lock poisoned"))?;
        if let Some(page) = state.pages.get(&page_id).cloned() {
            let mut inner = page
                .inner
                .lock()
                .map_err(|_| DbError::internal("cached page lock poisoned"))?;
            state.access_counter += 1;
            inner.data = data;
            inner.dirty = dirty;
            inner.last_access = state.access_counter;
            return Ok(());
        }

        self.evict_one_if_needed(&mut state)?;
        state.access_counter += 1;
        let last_access = state.access_counter;
        state.pages.insert(
            page_id,
            Arc::new(CachedPage {
                inner: Mutex::new(CachedPageInner {
                    data,
                    dirty,
                    pin_count: 0,
                    last_access,
                }),
            }),
        );
        Ok(())
    }

    fn evict_one_if_needed(&self, state: &mut PageCacheState) -> Result<()> {
        if state.pages.len() < self.capacity_pages {
            return Ok(());
        }

        let mut candidate: Option<(PageId, u64)> = None;
        for (page_id, page) in &state.pages {
            let inner = page
                .inner
                .lock()
                .map_err(|_| DbError::internal("cached page lock poisoned"))?;
            if inner.pin_count == 0 {
                match candidate {
                    Some((_, oldest_access)) if oldest_access <= inner.last_access => {}
                    _ => candidate = Some((*page_id, inner.last_access)),
                }
            }
        }

        let Some((victim, _)) = candidate else {
            return Err(DbError::transaction(
                "page cache is full and every cached page is pinned",
            ));
        };

        state.pages.remove(&victim);
        Ok(())
    }
}

impl PageHandle {
    pub(crate) fn read(&self) -> Result<Vec<u8>> {
        self.page
            .inner
            .lock()
            .map(|inner| inner.data.clone())
            .map_err(|_| DbError::internal("cached page lock poisoned"))
    }
}

impl Drop for PageHandle {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.page.inner.lock() {
            inner.pin_count = inner.pin_count.saturating_sub(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PageCache;

    #[test]
    fn all_pages_pinned_returns_transaction_error() {
        let cache = PageCache::new(2, 4);
        let first = cache
            .pin_or_load(1, || Ok(vec![1, 1, 1, 1]))
            .expect("load first page");
        let second = cache
            .pin_or_load(2, || Ok(vec![2, 2, 2, 2]))
            .expect("load second page");
        let error = cache
            .pin_or_load(3, || Ok(vec![3, 3, 3, 3]))
            .expect_err("all pages are pinned");

        assert_eq!(first.read().expect("read first page"), vec![1, 1, 1, 1]);
        assert_eq!(second.read().expect("read second page"), vec![2, 2, 2, 2]);
        assert!(matches!(error, crate::error::DbError::Transaction { .. }));
    }
}

//! Page-cache implementation with explicit pin/unpin tracking.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::error::{DbError, Result};

use super::page::PageId;

#[derive(Debug)]
pub(crate) struct PageCache {
    capacity_pages: usize,
    page_size: usize,
    access_counter: AtomicU64,
    state: RwLock<PageCacheState>,
}

#[derive(Debug, Default)]
struct PageCacheState {
    pages: HashMap<PageId, Arc<CachedPage>>,
}

#[derive(Debug)]
struct CachedPage {
    inner: Mutex<CachedPageInner>,
}

#[derive(Debug)]
struct CachedPageInner {
    data: Arc<[u8]>,
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
            access_counter: AtomicU64::new(0),
            state: RwLock::new(PageCacheState::default()),
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
        let loaded: Arc<[u8]> = Arc::from(loaded);

        let mut state = self
            .state
            .write()
            .map_err(|_| DbError::internal("page cache lock poisoned"))?;
        if let Some(page) = state.pages.get(&page_id).cloned() {
            {
                let mut inner = page
                    .inner
                    .lock()
                    .map_err(|_| DbError::internal("cached page lock poisoned"))?;
                let ts = self.access_counter.fetch_add(1, Ordering::Relaxed);
                inner.pin_count += 1;
                inner.last_access = ts;
            }
            return Ok(PageHandle { page });
        }

        self.evict_one_if_needed(&mut state)?;
        let ts = self.access_counter.fetch_add(1, Ordering::Relaxed);
        let page = Arc::new(CachedPage {
            inner: Mutex::new(CachedPageInner {
                data: loaded,
                dirty: false,
                pin_count: 1,
                last_access: ts,
            }),
        });
        state.pages.insert(page_id, Arc::clone(&page));
        Ok(PageHandle { page })
    }

    pub(crate) fn insert_clean_page(&self, page_id: PageId, data: Vec<u8>) -> Result<()> {
        self.insert_page(page_id, data, false)
    }

    #[cfg(test)]
    pub(crate) fn discard(&self, page_id: PageId) -> Result<()> {
        self.state
            .write()
            .map_err(|_| DbError::internal("page cache lock poisoned"))?
            .pages
            .remove(&page_id);
        Ok(())
    }

    pub(crate) fn clear(&self) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| DbError::internal("page cache lock poisoned"))?;
        state.pages.clear();
        self.access_counter.store(0, Ordering::Relaxed);
        Ok(())
    }

    fn try_pin_existing(&self, page_id: PageId) -> Result<Option<PageHandle>> {
        let state = self
            .state
            .read()
            .map_err(|_| DbError::internal("page cache lock poisoned"))?;
        let Some(page) = state.pages.get(&page_id).cloned() else {
            return Ok(None);
        };
        drop(state);

        {
            let mut inner = page
                .inner
                .lock()
                .map_err(|_| DbError::internal("cached page lock poisoned"))?;
            let ts = self.access_counter.fetch_add(1, Ordering::Relaxed);
            inner.pin_count += 1;
            inner.last_access = ts;
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
            .write()
            .map_err(|_| DbError::internal("page cache lock poisoned"))?;
        if let Some(page) = state.pages.get(&page_id).cloned() {
            let mut inner = page
                .inner
                .lock()
                .map_err(|_| DbError::internal("cached page lock poisoned"))?;
            let ts = self.access_counter.fetch_add(1, Ordering::Relaxed);
            inner.data = Arc::from(data);
            inner.dirty = dirty;
            inner.last_access = ts;
            return Ok(());
        }

        self.evict_one_if_needed(&mut state)?;
        let ts = self.access_counter.fetch_add(1, Ordering::Relaxed);
        state.pages.insert(
            page_id,
            Arc::new(CachedPage {
                inner: Mutex::new(CachedPageInner {
                    data: Arc::from(data),
                    dirty,
                    pin_count: 0,
                    last_access: ts,
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
    pub(crate) fn read(&self) -> Result<Arc<[u8]>> {
        self.page
            .inner
            .lock()
            .map(|inner| Arc::clone(&inner.data))
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
    use std::cell::Cell;

    fn dirty_flag(cache: &PageCache, page_id: u32) -> bool {
        let state = cache.state.read().expect("cache state");
        let page = state.pages.get(&page_id).expect("page exists");
        let dirty = page.inner.lock().expect("page inner").dirty;
        dirty
    }

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

        assert_eq!(
            first.read().expect("read first page").to_vec(),
            vec![1, 1, 1, 1]
        );
        assert_eq!(
            second.read().expect("read second page").to_vec(),
            vec![2, 2, 2, 2]
        );
        assert!(matches!(error, crate::error::DbError::Transaction { .. }));
    }

    #[test]
    fn lru_eviction_reloads_oldest_unpinned_page() {
        let cache = PageCache::new(2, 4);
        drop(
            cache
                .pin_or_load(1, || Ok(vec![1, 1, 1, 1]))
                .expect("load page1"),
        );
        drop(
            cache
                .pin_or_load(2, || Ok(vec![2, 2, 2, 2]))
                .expect("load page2"),
        );

        drop(
            cache
                .pin_or_load(1, || panic!("page1 should still be cached"))
                .expect("repin page1"),
        );

        drop(
            cache
                .pin_or_load(3, || Ok(vec![3, 3, 3, 3]))
                .expect("load page3"),
        );

        drop(
            cache
                .pin_or_load(1, || panic!("page1 should remain cached"))
                .expect("repin page1 again"),
        );

        let page2_loads = Cell::new(0);
        let page2 = cache
            .pin_or_load(2, || {
                page2_loads.set(page2_loads.get() + 1);
                Ok(vec![2, 2, 2, 2])
            })
            .expect("reload evicted page2");
        assert_eq!(page2.read().expect("read page2").to_vec(), vec![2, 2, 2, 2]);
        assert_eq!(page2_loads.get(), 1, "page2 should be loaded again");
    }

    #[test]
    fn dirty_tracking_clear_and_discard_work() {
        let cache = PageCache::new(2, 4);
        cache
            .insert_clean_page(1, vec![1, 1, 1, 1])
            .expect("insert clean page");
        cache
            .insert_page(2, vec![2, 2, 2, 2], true)
            .expect("insert dirty page");

        assert!(!dirty_flag(&cache, 1));
        assert!(dirty_flag(&cache, 2));

        cache.discard(2).expect("discard page");
        let page2_loads = Cell::new(0);
        let page2 = cache
            .pin_or_load(2, || {
                page2_loads.set(page2_loads.get() + 1);
                Ok(vec![2, 2, 2, 2])
            })
            .expect("reload discarded page");
        assert_eq!(page2.read().expect("read page2").to_vec(), vec![2, 2, 2, 2]);
        assert_eq!(page2_loads.get(), 1);
        drop(page2);

        cache.clear().expect("clear cache");
        let page1_loads = Cell::new(0);
        let page1 = cache
            .pin_or_load(1, || {
                page1_loads.set(page1_loads.get() + 1);
                Ok(vec![1, 1, 1, 1])
            })
            .expect("reload page1");
        assert_eq!(page1.read().expect("read page1").to_vec(), vec![1, 1, 1, 1]);
        assert_eq!(page1_loads.get(), 1);
    }

    #[test]
    fn pin_unpin_allows_reloading_after_drop() {
        let cache = PageCache::new(1, 4);
        let first = cache
            .pin_or_load(1, || Ok(vec![1, 1, 1, 1]))
            .expect("load page1");
        let error = cache
            .pin_or_load(2, || Ok(vec![2, 2, 2, 2]))
            .expect_err("cache should be full while page1 is pinned");
        assert!(matches!(error, crate::error::DbError::Transaction { .. }));

        drop(first);

        let second = cache
            .pin_or_load(2, || Ok(vec![2, 2, 2, 2]))
            .expect("load page2 after unpin");
        assert_eq!(
            second.read().expect("read page2").to_vec(),
            vec![2, 2, 2, 2]
        );
        drop(second);

        let first_reload = cache
            .pin_or_load(1, || Ok(vec![1, 1, 1, 1]))
            .expect("reload page1 after dropping page2");
        assert_eq!(
            first_reload.read().expect("read page1").to_vec(),
            vec![1, 1, 1, 1]
        );
    }

    #[test]
    fn insert_page_size_mismatch_errors() {
        let cache = PageCache::new(1, 4);
        let err = cache
            .insert_page(1, vec![1, 2, 3], false)
            .expect_err("size mismatch");
        assert!(matches!(err, crate::error::DbError::Internal { .. }));
    }

    #[test]
    fn pin_or_load_loader_size_mismatch_errors() {
        let cache = PageCache::new(1, 4);
        let err = cache
            .pin_or_load(1, || Ok(vec![1, 2]))
            .expect_err("loader size mismatch");
        assert!(matches!(err, crate::error::DbError::Internal { .. }));
    }
}

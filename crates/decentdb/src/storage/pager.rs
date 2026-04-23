//! Pager and direct main-database page access.
//!
//! Implements:
//! - design/adr/0001-page-size.md

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::error::{DbError, Result};
use crate::vfs::{read_exact_at, write_all_at, VfsFile};

use super::cache::PageCache;
use super::freelist::{decode_freelist_next, encode_freelist_page};
use super::header::{DatabaseHeader, DB_HEADER_SIZE};
use super::page::{self, PageId};

#[derive(Clone, Debug)]
pub(crate) struct PagerHandle {
    inner: Arc<Pager>,
}

#[derive(Debug)]
struct Pager {
    file: Arc<dyn VfsFile>,
    page_size: u32,
    cache: PageCache,
    header: Mutex<DatabaseHeader>,
}

impl PagerHandle {
    pub(crate) fn open(
        file: Arc<dyn VfsFile>,
        header: DatabaseHeader,
        cache_size_mb: usize,
    ) -> Result<Self> {
        let page_size = header.page_size;
        let bytes = cache_size_mb.saturating_mul(1024 * 1024);
        let capacity_pages = (bytes / page_size as usize).max(1);
        Ok(Self {
            inner: Arc::new(Pager {
                file,
                page_size,
                cache: PageCache::new(capacity_pages, page_size as usize),
                header: Mutex::new(header),
            }),
        })
    }

    pub(crate) fn read_page(&self, page_id: PageId) -> Result<Arc<[u8]>> {
        page::validate_page_id(page_id)?;
        let handle = self
            .inner
            .cache
            .pin_or_load(page_id, || self.inner.load_page_from_disk(page_id))?;
        handle.read()
    }

    pub(crate) fn read_page_from_disk(&self, page_id: PageId) -> Result<Arc<[u8]>> {
        page::validate_page_id(page_id)?;
        Ok(Arc::from(self.inner.load_page_from_disk(page_id)?))
    }

    pub(crate) fn write_page_direct(&self, page_id: PageId, data: &[u8]) -> Result<()> {
        page::validate_page_id(page_id)?;
        if data.len() != self.inner.page_size as usize {
            return Err(DbError::internal(format!(
                "page {page_id} write length {} does not match page size {}",
                data.len(),
                self.inner.page_size
            )));
        }

        write_all_at(
            self.inner.file.as_ref(),
            page::page_offset(page_id, self.inner.page_size),
            data,
        )?;
        let required_len =
            page::page_offset(page_id, self.inner.page_size) + u64::from(self.inner.page_size);
        if self.inner.file.file_size()? < required_len {
            self.inner.file.set_len(required_len)?;
        }
        self.inner.cache.insert_clean_page(page_id, data.to_vec())
    }

    pub(crate) fn on_disk_page_count(&self) -> Result<PageId> {
        self.inner
            .file
            .file_size()
            .map(|size| page::page_count_for_len(size, self.inner.page_size))
    }

    #[cfg(test)]
    pub(crate) fn allocate_page(&self) -> Result<PageId> {
        let mut header = self
            .inner
            .header
            .lock()
            .map_err(|_| DbError::internal("pager header lock poisoned"))?;
        if header.freelist.head_page_id != 0 {
            let page_id = header.freelist.head_page_id;
            let next = self.read_freelist_next(page_id)?;
            header.freelist.head_page_id = next;
            header.freelist.page_count = header.freelist.page_count.saturating_sub(1);
            self.persist_header(&header)?;
            self.inner.cache.discard(page_id)?;
            return Ok(page_id);
        }

        let page_id = self.on_disk_page_count()? + 1;
        let empty = page::zeroed_page(self.inner.page_size);
        write_all_at(
            self.inner.file.as_ref(),
            page::page_offset(page_id, self.inner.page_size),
            &empty,
        )?;
        self.inner.file.set_len(
            page::page_offset(page_id, self.inner.page_size) + u64::from(self.inner.page_size),
        )?;
        Ok(page_id)
    }

    #[cfg(test)]
    pub(crate) fn free_page(&self, page_id: PageId) -> Result<()> {
        if page_id <= page::CATALOG_ROOT_PAGE_ID {
            return Err(DbError::transaction(format!(
                "page {page_id} is reserved and cannot be freed"
            )));
        }
        let mut header = self
            .inner
            .header
            .lock()
            .map_err(|_| DbError::internal("pager header lock poisoned"))?;
        let page_bytes = encode_freelist_page(self.inner.page_size, header.freelist.head_page_id);
        write_all_at(
            self.inner.file.as_ref(),
            page::page_offset(page_id, self.inner.page_size),
            &page_bytes,
        )?;
        header.freelist.head_page_id = page_id;
        header.freelist.page_count += 1;
        self.persist_header(&header)?;
        self.inner.cache.discard(page_id)
    }

    pub(crate) fn set_last_checkpoint_lsn(&self, lsn: u64) -> Result<()> {
        let mut header = self
            .inner
            .header
            .lock()
            .map_err(|_| DbError::internal("pager header lock poisoned"))?;
        header.last_checkpoint_lsn = lsn;
        self.persist_header(&header)
    }

    pub(crate) fn set_schema_cookie(&self, schema_cookie: u32) -> Result<()> {
        let mut header = self
            .inner
            .header
            .lock()
            .map_err(|_| DbError::internal("pager header lock poisoned"))?;
        header.schema_cookie = schema_cookie;
        self.persist_header(&header)
    }

    pub(crate) fn header_snapshot(&self) -> Result<DatabaseHeader> {
        self.inner
            .header
            .lock()
            .map(|header| header.clone())
            .map_err(|_| DbError::internal("pager header lock poisoned"))
    }

    pub(crate) fn header_from_disk(&self) -> Result<DatabaseHeader> {
        let mut bytes = [0_u8; DB_HEADER_SIZE];
        read_exact_at(self.inner.file.as_ref(), 0, &mut bytes)?;
        DatabaseHeader::decode(&bytes)
    }

    pub(crate) fn refresh_from_disk(&self, header: DatabaseHeader) -> Result<()> {
        if header.page_size != self.inner.page_size {
            return Err(DbError::corruption(format!(
                "database page size changed from {} to {}",
                self.inner.page_size, header.page_size
            )));
        }
        self.inner.cache.clear()?;
        *self
            .inner
            .header
            .lock()
            .map_err(|_| DbError::internal("pager header lock poisoned"))? = header;
        Ok(())
    }

    pub(crate) fn truncate_freelist_tail(&self) -> Result<Option<PageId>> {
        let mut header = self.header_snapshot()?;
        if header.freelist.head_page_id == 0 || header.freelist.page_count == 0 {
            return Ok(None);
        }

        let current_page_count = self.on_disk_page_count()?;
        if current_page_count <= page::CATALOG_ROOT_PAGE_ID {
            return Ok(None);
        }

        let mut ordered_freelist_pages = Vec::with_capacity(header.freelist.page_count as usize);
        let mut page_id = header.freelist.head_page_id;
        while page_id != 0 {
            ordered_freelist_pages.push(page_id);
            page_id = self.read_freelist_next(page_id)?;
        }

        let freelist_pages = ordered_freelist_pages
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut new_page_count = current_page_count;
        let mut trimmed_pages = HashSet::new();
        while new_page_count > page::CATALOG_ROOT_PAGE_ID
            && freelist_pages.contains(&new_page_count)
        {
            trimmed_pages.insert(new_page_count);
            new_page_count = new_page_count.saturating_sub(1);
        }
        if trimmed_pages.is_empty() {
            return Ok(None);
        }

        let remaining_pages = ordered_freelist_pages
            .into_iter()
            .filter(|page_id| !trimmed_pages.contains(page_id))
            .collect::<Vec<_>>();
        for (index, page_id) in remaining_pages.iter().enumerate() {
            let next_page_id = remaining_pages.get(index + 1).copied().unwrap_or(0);
            write_all_at(
                self.inner.file.as_ref(),
                page::page_offset(*page_id, self.inner.page_size),
                &encode_freelist_page(self.inner.page_size, next_page_id),
            )?;
        }

        header.freelist.head_page_id = remaining_pages.first().copied().unwrap_or(0);
        header.freelist.page_count = header
            .freelist
            .page_count
            .saturating_sub(trimmed_pages.len() as u32);
        self.persist_header(&header)?;
        self.inner.file.set_len(page::page_offset(
            new_page_count.saturating_add(1),
            self.inner.page_size,
        ))?;
        self.inner.cache.clear()?;
        *self
            .inner
            .header
            .lock()
            .map_err(|_| DbError::internal("pager header lock poisoned"))? = header;
        Ok(Some(new_page_count))
    }

    #[must_use]
    pub(crate) fn page_size(&self) -> u32 {
        self.inner.page_size
    }

    fn read_freelist_next(&self, page_id: PageId) -> Result<PageId> {
        let mut page = page::zeroed_page(self.inner.page_size);
        read_exact_at(
            self.inner.file.as_ref(),
            page::page_offset(page_id, self.inner.page_size),
            &mut page,
        )?;
        decode_freelist_next(&page)
    }

    fn persist_header(&self, header: &DatabaseHeader) -> Result<()> {
        let bytes = header.encode();
        write_all_at(self.inner.file.as_ref(), 0, &bytes)?;
        self.inner
            .cache
            .insert_clean_page(page::HEADER_PAGE_ID, self.header_page(header))
    }

    fn header_page(&self, header: &DatabaseHeader) -> Vec<u8> {
        let mut page = page::zeroed_page(self.inner.page_size);
        page[..DB_HEADER_SIZE].copy_from_slice(&header.encode());
        page
    }
}

impl Pager {
    fn load_page_from_disk(&self, page_id: PageId) -> Result<Vec<u8>> {
        let page_count = self
            .file
            .file_size()
            .map(|size| page::page_count_for_len(size, self.page_size))?;
        if page_id > page_count {
            return Ok(page::zeroed_page(self.page_size));
        }

        let mut data = page::zeroed_page(self.page_size);
        read_exact_at(
            self.file.as_ref(),
            page::page_offset(page_id, self.page_size),
            &mut data,
        )?;
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::storage::{write_database_bootstrap_vfs, DatabaseHeader};
    use crate::vfs::mem::MemVfs;
    use crate::vfs::{write_all_at, FileKind, OpenMode, Vfs, VfsFile};

    use super::PagerHandle;
    use crate::storage::page;

    static NEXT_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn repeated_reads_hit_vfs_once_when_page_is_cached() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("cache-hit");
        let inner = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(inner.as_ref(), &header).expect("bootstrap database");
        let payload = vec![0x7A; page::DEFAULT_PAGE_SIZE as usize];
        write_all_at(
            inner.as_ref(),
            page::page_offset(3, page::DEFAULT_PAGE_SIZE),
            &payload,
        )
        .expect("write page 3");
        inner
            .set_len(page::page_offset(4, page::DEFAULT_PAGE_SIZE))
            .expect("extend file");

        let counter = Arc::new(AtomicUsize::new(0));
        let file = Arc::new(CountingFile {
            inner,
            read_count: Arc::clone(&counter),
        });

        let pager = PagerHandle::open(file, header, 1).expect("open pager");
        let first = pager.read_page(3).expect("first read");
        let second = pager.read_page(3).expect("second read");

        assert_eq!(first.to_vec(), payload);
        assert_eq!(second.to_vec(), payload);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn read_page_from_disk_bypasses_stale_cache() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("disk-bypass");
        let inner = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(inner.as_ref(), &header).expect("bootstrap database");
        let original = vec![0x11; page::DEFAULT_PAGE_SIZE as usize];
        write_all_at(
            inner.as_ref(),
            page::page_offset(3, page::DEFAULT_PAGE_SIZE),
            &original,
        )
        .expect("write original page");
        inner
            .set_len(page::page_offset(4, page::DEFAULT_PAGE_SIZE))
            .expect("extend file");

        let pager = PagerHandle::open(Arc::clone(&inner), header, 1).expect("open pager");
        assert_eq!(pager.read_page(3).expect("cached read").to_vec(), original);

        let updated = vec![0x22; page::DEFAULT_PAGE_SIZE as usize];
        write_all_at(
            inner.as_ref(),
            page::page_offset(3, page::DEFAULT_PAGE_SIZE),
            &updated,
        )
        .expect("overwrite page");

        assert_eq!(
            pager
                .read_page(3)
                .expect("cached page remains stale")
                .to_vec(),
            original
        );
        assert_eq!(
            pager
                .read_page_from_disk(3)
                .expect("disk bypass read")
                .to_vec(),
            updated
        );
    }

    #[test]
    fn file_growth_and_freelist_reuse_are_deterministic() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("freelist");
        let file = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap database");
        let pager = PagerHandle::open(file, header, 1).expect("open pager");

        let first_allocated = pager.allocate_page().expect("allocate page");
        assert_eq!(first_allocated, 3);
        assert_eq!(pager.on_disk_page_count().expect("page count"), 3);

        pager.free_page(first_allocated).expect("free page");
        let reused = pager.allocate_page().expect("reuse freelist page");
        assert_eq!(reused, 3);
    }

    #[test]
    fn write_page_direct_does_not_shrink_existing_file() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("write-page-direct-no-shrink");
        let file = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap database");
        file.set_len(page::page_offset(5, page::DEFAULT_PAGE_SIZE))
            .expect("extend file to four pages");

        let pager = PagerHandle::open(file, header, 4).expect("open pager");
        let page_four = vec![0x4A; page::DEFAULT_PAGE_SIZE as usize];
        pager
            .write_page_direct(4, &page_four)
            .expect("seed page four");
        let original_len = pager
            .inner
            .file
            .file_size()
            .expect("file size after seeding page four");

        let page_two = vec![0x2B; page::DEFAULT_PAGE_SIZE as usize];
        pager
            .write_page_direct(2, &page_two)
            .expect("rewrite smaller page id");

        assert_eq!(
            pager
                .inner
                .file
                .file_size()
                .expect("file size after rewrite"),
            original_len
        );
        assert_eq!(
            pager.read_page(4).expect("read page four").to_vec(),
            page_four
        );
    }

    #[derive(Debug)]
    struct CountingFile {
        inner: Arc<dyn VfsFile>,
        read_count: Arc<AtomicUsize>,
    }

    impl VfsFile for CountingFile {
        fn kind(&self) -> FileKind {
            self.inner.kind()
        }

        fn path(&self) -> &Path {
            self.inner.path()
        }

        fn read_at(&self, offset: u64, buf: &mut [u8]) -> crate::Result<usize> {
            self.read_count.fetch_add(1, Ordering::Relaxed);
            self.inner.read_at(offset, buf)
        }

        fn write_at(&self, offset: u64, buf: &[u8]) -> crate::Result<usize> {
            self.inner.write_at(offset, buf)
        }

        fn sync_data(&self) -> crate::Result<()> {
            self.inner.sync_data()
        }

        fn sync_metadata(&self) -> crate::Result<()> {
            self.inner.sync_metadata()
        }

        fn file_size(&self) -> crate::Result<u64> {
            self.inner.file_size()
        }

        fn set_len(&self, len: u64) -> crate::Result<()> {
            self.inner.set_len(len)
        }
    }

    fn unique_path(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("monotonic wall clock")
            .as_nanos();
        let ordinal = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        PathBuf::from(format!(
            ":memory:{label}:{}:{stamp}:{ordinal}",
            std::process::id()
        ))
    }

    #[test]
    fn header_from_disk_reads_header_written_to_vfs() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("header-from-disk");
        let file = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap database");
        let pager = PagerHandle::open(file, header.clone(), 1).expect("open pager");
        let on_disk = pager.header_from_disk().expect("read header from disk");
        assert_eq!(on_disk, header);
    }

    #[test]
    fn set_last_checkpoint_and_schema_cookie_persist_to_disk() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("persist-header");
        let file = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap database");
        let pager = PagerHandle::open(file, header.clone(), 1).expect("open pager");

        pager.set_last_checkpoint_lsn(0xDEADBEEF).expect("set lsn");
        pager.set_schema_cookie(0xBEEF).expect("set schema cookie");

        let on_disk = pager.header_from_disk().expect("read header from disk");
        assert_eq!(on_disk.last_checkpoint_lsn, 0xDEADBEEF);
        assert_eq!(on_disk.schema_cookie, 0xBEEF);
    }

    #[test]
    fn refresh_from_disk_detects_page_size_change() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("refresh-page-size");
        let file = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap database");
        let pager = PagerHandle::open(file, header.clone(), 1).expect("open pager");

        let bad_header = DatabaseHeader::new(header.page_size * 2);
        let res = pager.refresh_from_disk(bad_header);
        assert!(res.is_err());
    }

    #[test]
    fn free_reserved_page_returns_error() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("free-reserved");
        let file = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap database");
        let pager = PagerHandle::open(file, header, 1).expect("open pager");

        let res = pager.free_page(page::CATALOG_ROOT_PAGE_ID);
        assert!(res.is_err());
    }

    #[test]
    fn read_page_beyond_count_returns_zeroed_page() {
        let mem_vfs = MemVfs::default();
        let path = unique_path("read-beyond");
        let file = mem_vfs
            .open(&path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap database");
        let pager = PagerHandle::open(file, header.clone(), 1).expect("open pager");

        let data = pager.read_page(100).expect("read beyond");
        assert_eq!(data.to_vec(), page::zeroed_page(page::DEFAULT_PAGE_SIZE));
    }
}

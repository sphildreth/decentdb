//! WAL recovery and index rebuild logic.

use std::sync::Arc;

use crate::error::{DbError, Result};
use crate::storage::page::PageId;
use crate::storage::PagerHandle;
use crate::vfs::{read_exact_at, write_all_at, VfsFile};

use super::delta::apply_page_delta;
use super::format::{FrameType, WalFrame, WalHeader, WAL_HEADER_SIZE, WAL_HEADER_SIZE_USIZE};
use super::index::{WalIndex, WalVersion};

pub(crate) fn initialize_or_recover(
    file: &Arc<dyn VfsFile>,
    pager: &PagerHandle,
    page_size: u32,
) -> Result<(WalIndex, u64, u32)> {
    let size = file.file_size()?;
    if size == 0 {
        let header = WalHeader::new(page_size, 0);
        write_all_at(file.as_ref(), 0, &header.encode())?;
        file.set_len(WAL_HEADER_SIZE)?;
        return Ok((WalIndex::default(), 0, 0));
    }

    if size < WAL_HEADER_SIZE {
        return Err(DbError::corruption(format!(
            "WAL file {} is shorter than the fixed header",
            file.path().display()
        )));
    }

    let mut header_bytes = [0_u8; WAL_HEADER_SIZE_USIZE];
    read_exact_at(file.as_ref(), 0, &mut header_bytes)?;
    let header = WalHeader::decode(&header_bytes)?;
    if header.page_size != page_size {
        return Err(DbError::corruption(format!(
            "WAL page size {} does not match database page size {}",
            header.page_size, page_size
        )));
    }
    if header.wal_end_offset > size {
        return Err(DbError::corruption(format!(
            "WAL logical end offset {} exceeds file size {}",
            header.wal_end_offset, size
        )));
    }

    let mut index = WalIndex::default();
    let mut max_page_id = 0;
    let mut offset = WAL_HEADER_SIZE;
    let mut pending = Vec::<(PageId, Vec<u8>, u64)>::new();
    while offset < header.wal_end_offset {
        let Some(frame) =
            WalFrame::decode_from_file(file.as_ref(), offset, page_size, header.wal_end_offset)?
        else {
            break;
        };
        let next_offset = offset + frame.encoded_len(page_size) as u64;
        match frame.frame_type {
            FrameType::Page => {
                max_page_id = max_page_id.max(frame.page_id);
                pending.push((frame.page_id, frame.payload, next_offset));
            }
            FrameType::PageDelta => {
                max_page_id = max_page_id.max(frame.page_id);
                let base = if let Some(version) = index.latest_visible(frame.page_id, u64::MAX) {
                    version.data.clone()
                } else {
                    pager.read_page(frame.page_id)?.to_vec()
                };
                let data = apply_page_delta(&base, &frame.payload)?;
                pending.push((frame.page_id, data, next_offset));
            }
            FrameType::Commit => {
                for (page_id, data, lsn) in pending.drain(..) {
                    // Recovery runs before any readers can hold snapshots, so we only need
                    // the latest recovered version per page. Retaining full historical
                    // versions from a large WAL can consume substantial memory on open.
                    index.add_version(page_id, WalVersion { lsn, data }, false);
                }
            }
            FrameType::Checkpoint => {
                pending.clear();
            }
        }
        offset = next_offset;
    }

    Ok((index, header.wal_end_offset, max_page_id))
}

pub(crate) fn persist_header(
    file: &Arc<dyn VfsFile>,
    page_size: u32,
    wal_end_offset: u64,
) -> Result<()> {
    let header = WalHeader::new(page_size, wal_end_offset);
    write_all_at(file.as_ref(), 0, &header.encode())
}

pub(crate) fn truncate_to_header(file: &Arc<dyn VfsFile>, page_size: u32) -> Result<()> {
    persist_header(file, page_size, 0)?;
    file.set_len(WAL_HEADER_SIZE)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use crate::storage::page;
    use crate::storage::{write_database_bootstrap_vfs, DatabaseHeader, PagerHandle};
    use crate::vfs::{write_all_at, FileKind, OpenMode, Vfs};

    use super::initialize_or_recover;
    use crate::wal::format::{WalHeader, WAL_HEADER_SIZE};

    fn test_pager(vfs: &crate::vfs::mem::MemVfs, path: &Path) -> PagerHandle {
        let file = vfs
            .open(path, OpenMode::CreateNew, FileKind::Database)
            .expect("create database file");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap db");
        PagerHandle::open(Arc::clone(&file), header, 1).expect("open pager")
    }

    #[test]
    fn corrupt_wal_header_is_reported() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));
        let bad_header = [0_u8; 32];
        write_all_at(file.as_ref(), 0, &bad_header).expect("write corrupt header");
        file.set_len(WAL_HEADER_SIZE).expect("size wal header");

        let error = initialize_or_recover(&file, &pager, page::DEFAULT_PAGE_SIZE)
            .expect_err("header is corrupt");
        assert!(matches!(error, crate::error::DbError::Corruption { .. }));
    }

    #[test]
    fn empty_wal_is_initialized_header_only() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));
        let (index, end, max_page_id) =
            initialize_or_recover(&file, &pager, page::DEFAULT_PAGE_SIZE).expect("initialize wal");

        assert_eq!(index.version_count(), 0);
        assert_eq!(end, 0);
        assert_eq!(max_page_id, 0);

        let mut header_bytes = [0_u8; 32];
        crate::vfs::read_exact_at(file.as_ref(), 0, &mut header_bytes).expect("read header");
        let header = WalHeader::decode(&header_bytes).expect("decode wal header");
        assert_eq!(header.page_size, page::DEFAULT_PAGE_SIZE);
    }

    #[test]
    fn wal_header_page_size_mismatch() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));
        // Write a header with a different page size
        let header = WalHeader::new(page::DEFAULT_PAGE_SIZE * 2, 0);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write header");
        file.set_len(WAL_HEADER_SIZE).expect("size wal header");

        let error =
            initialize_or_recover(&file, &pager, page::DEFAULT_PAGE_SIZE).expect_err("mismatch");
        assert!(matches!(error, crate::error::DbError::Corruption { .. }));
    }

    #[test]
    fn wal_header_end_offset_exceeds_file_size() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));
        // Set wal_end_offset to be larger than the actual file size
        let header = WalHeader::new(page::DEFAULT_PAGE_SIZE, WAL_HEADER_SIZE + 100);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write header");
        file.set_len(WAL_HEADER_SIZE).expect("size wal header");

        let error =
            initialize_or_recover(&file, &pager, page::DEFAULT_PAGE_SIZE).expect_err("end_exceeds");
        assert!(matches!(error, crate::error::DbError::Corruption { .. }));
    }

    #[test]
    fn replay_committed_frames_populates_index() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));

        let ps = page::DEFAULT_PAGE_SIZE;
        let frames = vec![
            crate::wal::format::WalFrame::page(3, vec![0xAA; ps as usize]),
            crate::wal::format::WalFrame::commit(),
        ];
        let mut data = Vec::new();
        for f in &frames {
            data.extend_from_slice(&f.encode(ps).unwrap());
        }
        let logical_end = WAL_HEADER_SIZE + data.len() as u64;
        let header = WalHeader::new(ps, logical_end);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write header");
        write_all_at(file.as_ref(), WAL_HEADER_SIZE, &data).expect("write frames");
        file.set_len(logical_end).expect("set len");

        let (index, end, max_page_id) = initialize_or_recover(&file, &pager, ps).expect("recover");
        assert_eq!(index.version_count(), 1);
        assert_eq!(end, logical_end);
        assert_eq!(max_page_id, 3);
    }

    #[test]
    fn recovery_retains_only_latest_version_per_page() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));

        let ps = page::DEFAULT_PAGE_SIZE;
        let first = vec![0x11; ps as usize];
        let second = vec![0x22; ps as usize];
        let frames = vec![
            crate::wal::format::WalFrame::page(7, first),
            crate::wal::format::WalFrame::commit(),
            crate::wal::format::WalFrame::page(7, second.clone()),
            crate::wal::format::WalFrame::commit(),
        ];
        let mut data = Vec::new();
        for frame in &frames {
            data.extend_from_slice(&frame.encode(ps).expect("encode frame"));
        }
        let logical_end = WAL_HEADER_SIZE + data.len() as u64;
        let header = WalHeader::new(ps, logical_end);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write header");
        write_all_at(file.as_ref(), WAL_HEADER_SIZE, &data).expect("write frames");
        file.set_len(logical_end).expect("set len");

        let (index, _end, _max_page_id) =
            initialize_or_recover(&file, &pager, ps).expect("recover");
        assert_eq!(index.version_count(), 1);
        let latest = index
            .latest_visible(7, u64::MAX)
            .expect("latest version for page 7");
        assert_eq!(latest.data, second);
    }

    #[test]
    fn recovery_large_wal_replay_keeps_single_version_per_page() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));

        let ps = page::DEFAULT_PAGE_SIZE;
        let update_count = 1024_u32;
        let page_id = 42_u32;
        let mut frames = Vec::with_capacity((update_count as usize) * 2);
        for update in 0..update_count {
            frames.push(crate::wal::format::WalFrame::page(
                page_id,
                vec![(update % 251) as u8; ps as usize],
            ));
            frames.push(crate::wal::format::WalFrame::commit());
        }

        let mut data = Vec::new();
        for frame in &frames {
            data.extend_from_slice(&frame.encode(ps).expect("encode frame"));
        }
        let logical_end = WAL_HEADER_SIZE + data.len() as u64;
        let header = WalHeader::new(ps, logical_end);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write header");
        write_all_at(file.as_ref(), WAL_HEADER_SIZE, &data).expect("write frames");
        file.set_len(logical_end).expect("set len");

        let (index, _end, _max_page_id) =
            initialize_or_recover(&file, &pager, ps).expect("recover");
        assert_eq!(
            index.version_count(),
            1,
            "recovery should not retain full historical page versions",
        );
        let latest = index
            .latest_visible(page_id, u64::MAX)
            .expect("latest version for page");
        assert_eq!(latest.data[0], ((update_count - 1) % 251) as u8);
    }

    #[test]
    fn partial_frames_at_end_are_ignored() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));

        let ps = page::DEFAULT_PAGE_SIZE;
        let frame = crate::wal::format::WalFrame::page(1, vec![0xBB; ps as usize]);
        let mut data = frame.encode(ps).unwrap();
        // Truncate the last byte to simulate a torn/partial write
        data.truncate(data.len() - 1);

        let logical_end = WAL_HEADER_SIZE + data.len() as u64;
        let header = WalHeader::new(ps, logical_end);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write header");
        write_all_at(file.as_ref(), WAL_HEADER_SIZE, &data).expect("write partial frame");
        file.set_len(logical_end).expect("set len");

        let (index, _end, _max_page_id) =
            initialize_or_recover(&file, &pager, ps).expect("recover");
        // No committed frames -> index should be empty
        assert_eq!(index.version_count(), 0);
    }

    #[test]
    fn replay_page_delta_frames_populates_index_with_rebuilt_page() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));

        let ps = page::DEFAULT_PAGE_SIZE;
        let mut base = vec![0_u8; ps as usize];
        base[24..28].copy_from_slice(&11_u32.to_le_bytes());
        pager.write_page_direct(3, &base).expect("write base page");

        let mut updated = base.clone();
        updated[24..28].copy_from_slice(&27_u32.to_le_bytes());
        updated[96..104].copy_from_slice(b"manifest");
        let delta = crate::wal::delta::encode_page_delta(&base, &updated).expect("encode delta");

        let frames = vec![
            crate::wal::format::WalFrame::page_delta(3, delta),
            crate::wal::format::WalFrame::commit(),
        ];
        let mut data = Vec::new();
        for frame in &frames {
            data.extend_from_slice(&frame.encode(ps).expect("encode frame"));
        }
        let logical_end = WAL_HEADER_SIZE + data.len() as u64;
        let header = WalHeader::new(ps, logical_end);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write header");
        write_all_at(file.as_ref(), WAL_HEADER_SIZE, &data).expect("write frames");
        file.set_len(logical_end).expect("set len");

        let (index, _end, _max_page_id) =
            initialize_or_recover(&file, &pager, ps).expect("recover");
        let version = index
            .latest_visible(3, u64::MAX)
            .expect("recovered page version");
        assert_eq!(version.data, updated);
    }
}

//! WAL recovery and index rebuild logic.

use std::sync::Arc;

use crate::error::{DbError, Result};
use crate::storage::page::PageId;
use crate::vfs::{read_exact_at, write_all_at, VfsFile};

use super::format::{FrameType, WalFrame, WalHeader, WAL_HEADER_SIZE, WAL_HEADER_SIZE_USIZE};
use super::index::{WalIndex, WalVersion};

pub(crate) fn initialize_or_recover(
    file: &Arc<dyn VfsFile>,
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
            FrameType::Commit => {
                for (page_id, data, lsn) in pending.drain(..) {
                    index.add_version(page_id, WalVersion { lsn, data }, true);
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

    use crate::storage::page;
    use crate::vfs::{write_all_at, FileKind, OpenMode, Vfs};

    use super::initialize_or_recover;
    use crate::wal::format::{WalHeader, WAL_HEADER_SIZE};

    #[test]
    fn corrupt_wal_header_is_reported() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let bad_header = [0_u8; 32];
        write_all_at(file.as_ref(), 0, &bad_header).expect("write corrupt header");
        file.set_len(WAL_HEADER_SIZE).expect("size wal header");

        let error =
            initialize_or_recover(&file, page::DEFAULT_PAGE_SIZE).expect_err("header is corrupt");
        assert!(matches!(error, crate::error::DbError::Corruption { .. }));
    }

    #[test]
    fn empty_wal_is_initialized_header_only() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let (index, end, max_page_id) =
            initialize_or_recover(&file, page::DEFAULT_PAGE_SIZE).expect("initialize wal");

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
        // Write a header with a different page size
        let header = WalHeader::new(page::DEFAULT_PAGE_SIZE * 2, 0);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write header");
        file.set_len(WAL_HEADER_SIZE).expect("size wal header");

        let error = initialize_or_recover(&file, page::DEFAULT_PAGE_SIZE).expect_err("mismatch");
        assert!(matches!(error, crate::error::DbError::Corruption { .. }));
    }

    #[test]
    fn wal_header_end_offset_exceeds_file_size() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        // Set wal_end_offset to be larger than the actual file size
        let header = WalHeader::new(page::DEFAULT_PAGE_SIZE, WAL_HEADER_SIZE + 100);
        write_all_at(file.as_ref(), 0, &header.encode()).expect("write header");
        file.set_len(WAL_HEADER_SIZE).expect("size wal header");

        let error = initialize_or_recover(&file, page::DEFAULT_PAGE_SIZE).expect_err("end_exceeds");
        assert!(matches!(error, crate::error::DbError::Corruption { .. }));
    }

    #[test]
    fn replay_committed_frames_populates_index() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");

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

        let (index, end, max_page_id) = initialize_or_recover(&file, ps).expect("recover");
        assert_eq!(index.version_count(), 1);
        assert_eq!(end, logical_end);
        assert_eq!(max_page_id, 3);
    }

    #[test]
    fn partial_frames_at_end_are_ignored() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");

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

        let (index, _end, _max_page_id) = initialize_or_recover(&file, ps).expect("recover");
        // No committed frames -> index should be empty
        assert_eq!(index.version_count(), 0);
    }
}

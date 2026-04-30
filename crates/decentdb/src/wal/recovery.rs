//! WAL recovery and index rebuild logic.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{DbError, Result};
use crate::storage::page::PageId;
use crate::storage::PagerHandle;
use crate::vfs::{read_exact_at, write_all_at, VfsFile};

use super::delta::apply_page_delta_in_place;
use super::format::{
    FrameEncoding, FrameType, WalFrame, WalHeader, WAL_HEADER_SIZE, WAL_HEADER_SIZE_USIZE,
};
use super::index::{WalIndex, WalVersion};
use super::index_sidecar::WalIndexSidecar;

const MAX_PENDING_RECOVERY_FRAMES: usize = 1_000_000;
const RECOVERY_PENDING_OVERFLOW_MESSAGE: &str =
    "WAL recovery aborted: more than 1,000,000 uncommitted page frames before commit";

#[derive(Debug)]
struct PendingRecoveryPage {
    data: Vec<u8>,
    lsn: u64,
    wal_offset: u64,
    frame_len: u32,
    encoding: FrameEncoding,
}

pub(crate) fn initialize_or_recover(
    file: &Arc<dyn VfsFile>,
    pager: &PagerHandle,
    page_size: u32,
    hot_set_pages: u32,
    mut sidecar: Option<&mut WalIndexSidecar>,
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
    let mut pending = Vec::<PageId>::new();
    let mut pending_pages = HashMap::<PageId, PendingRecoveryPage>::new();
    while offset < header.wal_end_offset {
        let Some(frame) =
            WalFrame::decode_from_file(file.as_ref(), offset, page_size, header.wal_end_offset)?
        else {
            break;
        };
        let frame_len = frame.encoded_len(page_size) as u32;
        let next_offset = offset + frame_len as u64;
        match frame.frame_type {
            FrameType::Page => {
                max_page_id = max_page_id.max(frame.page_id);
                push_pending_frame(&mut pending, frame.page_id)?;
                store_pending_page(
                    &mut pending_pages,
                    frame.page_id,
                    frame.payload,
                    next_offset,
                    offset,
                    frame_len,
                    FrameEncoding::Page,
                )?;
            }
            FrameType::PageDelta => {
                max_page_id = max_page_id.max(frame.page_id);
                push_pending_frame(&mut pending, frame.page_id)?;
                if let Some(pending_page) = pending_pages.get_mut(&frame.page_id) {
                    apply_page_delta_in_place(&mut pending_page.data, &frame.payload)?;
                    pending_page.lsn = next_offset;
                    pending_page.wal_offset = offset;
                    pending_page.frame_len = frame_len;
                    pending_page.encoding = FrameEncoding::PageDelta;
                } else {
                    let mut data =
                        if let Some(version) = index.latest_visible(frame.page_id, u64::MAX) {
                            version.payload.as_slice().to_vec()
                        } else {
                            pager.read_page(frame.page_id)?.to_vec()
                        };
                    apply_page_delta_in_place(&mut data, &frame.payload)?;
                    store_pending_page(
                        &mut pending_pages,
                        frame.page_id,
                        data,
                        next_offset,
                        offset,
                        frame_len,
                        FrameEncoding::PageDelta,
                    )?;
                }
            }
            FrameType::Commit => {
                pending.clear();
                for (page_id, pending_page) in pending_pages.drain() {
                    // Recovery runs before any readers can hold snapshots, so we only need
                    // the latest recovered version per page. Retaining full historical
                    // versions from a large WAL can consume substantial memory on open.
                    index.add_version(
                        page_id,
                        WalVersion::resident(
                            pending_page.lsn,
                            pending_page.wal_offset,
                            pending_page.frame_len,
                            pending_page.encoding,
                            Arc::from(pending_page.data),
                        ),
                        false,
                    );
                }
                spill_recovered_latest_versions(&mut index, hot_set_pages, sidecar.as_deref_mut())?;
            }
            FrameType::Checkpoint => {
                pending.clear();
                pending_pages.clear();
            }
        }
        offset = next_offset;
    }

    Ok((index, header.wal_end_offset, max_page_id))
}

fn spill_recovered_latest_versions(
    index: &mut WalIndex,
    hot_set_pages: u32,
    mut sidecar: Option<&mut WalIndexSidecar>,
) -> Result<()> {
    let Some(sidecar) = sidecar.as_mut() else {
        return Ok(());
    };
    let hot_set_pages = usize::try_from(hot_set_pages).unwrap_or(usize::MAX);
    while let Some((page_id, version)) = index.spill_one_cold_latest(hot_set_pages) {
        sidecar.write_latest(page_id, &version)?;
    }
    Ok(())
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

fn push_pending_frame(pending: &mut Vec<PageId>, page_id: PageId) -> Result<()> {
    if pending.len() >= MAX_PENDING_RECOVERY_FRAMES {
        return Err(recovery_pending_overflow_error());
    }
    pending.push(page_id);
    Ok(())
}

fn store_pending_page(
    pending_pages: &mut HashMap<PageId, PendingRecoveryPage>,
    page_id: PageId,
    data: Vec<u8>,
    lsn: u64,
    wal_offset: u64,
    frame_len: u32,
    encoding: FrameEncoding,
) -> Result<()> {
    let at_capacity = pending_pages.len() >= MAX_PENDING_RECOVERY_FRAMES;
    match pending_pages.entry(page_id) {
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            let pending_page = entry.get_mut();
            pending_page.data = data;
            pending_page.lsn = lsn;
            pending_page.wal_offset = wal_offset;
            pending_page.frame_len = frame_len;
            pending_page.encoding = encoding;
            Ok(())
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
            if at_capacity {
                return Err(recovery_pending_overflow_error());
            }
            entry.insert(PendingRecoveryPage {
                data,
                lsn,
                wal_offset,
                frame_len,
                encoding,
            });
            Ok(())
        }
    }
}

fn recovery_pending_overflow_error() -> DbError {
    DbError::corruption(RECOVERY_PENDING_OVERFLOW_MESSAGE)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use tempfile::TempDir;

    use crate::storage::page;
    use crate::storage::{write_database_bootstrap_vfs, DatabaseHeader, PagerHandle};
    use crate::vfs::{write_all_at, FileKind, OpenMode, Vfs, VfsHandle};

    use super::{initialize_or_recover, persist_header, MAX_PENDING_RECOVERY_FRAMES};
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

        let error = initialize_or_recover(&file, &pager, page::DEFAULT_PAGE_SIZE, 0, None)
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
            initialize_or_recover(&file, &pager, page::DEFAULT_PAGE_SIZE, 0, None)
                .expect("initialize wal");

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

        let error = initialize_or_recover(&file, &pager, page::DEFAULT_PAGE_SIZE, 0, None)
            .expect_err("mismatch");
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

        let error = initialize_or_recover(&file, &pager, page::DEFAULT_PAGE_SIZE, 0, None)
            .expect_err("end_exceeds");
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

        let (index, end, max_page_id) =
            initialize_or_recover(&file, &pager, ps, 0, None).expect("recover");
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
            initialize_or_recover(&file, &pager, ps, 0, None).expect("recover");
        assert_eq!(index.version_count(), 1);
        let latest = index
            .latest_visible(7, u64::MAX)
            .expect("latest version for page 7");
        assert_eq!(latest.payload.as_slice(), second.as_slice());
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
            initialize_or_recover(&file, &pager, ps, 0, None).expect("recover");
        assert_eq!(
            index.version_count(),
            1,
            "recovery should not retain full historical page versions",
        );
        let latest = index
            .latest_visible(page_id, u64::MAX)
            .expect("latest version for page");
        assert_eq!(
            latest.payload.as_slice()[0],
            ((update_count - 1) % 251) as u8
        );
    }

    #[test]
    fn recovery_spills_excess_full_page_versions_into_sidecar() {
        let mem_vfs = Arc::new(crate::vfs::mem::MemVfs::default());
        let vfs: Arc<dyn Vfs> = mem_vfs.clone();
        let handle = VfsHandle::from_vfs(vfs);
        let db_path = Path::new("spill-recovery.ddb");
        let wal_path = Path::new("spill-recovery.ddb.wal");
        let file = mem_vfs
            .open(wal_path, OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&mem_vfs, db_path);

        let ps = page::DEFAULT_PAGE_SIZE;
        let frames = vec![
            crate::wal::format::WalFrame::page(7, vec![0x11; ps as usize]),
            crate::wal::format::WalFrame::commit(),
            crate::wal::format::WalFrame::page(8, vec![0x22; ps as usize]),
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

        let mut sidecar = crate::wal::index_sidecar::WalIndexSidecar::open(&handle, db_path)
            .expect("open sidecar");
        let (index, _end, _max_page_id) =
            initialize_or_recover(&file, &pager, ps, 1, Some(&mut sidecar)).expect("recover");

        assert_eq!(index.version_count(), 1);
        assert_eq!(sidecar.version_count(), 1);
        assert!(index.latest_visible(8, u64::MAX).is_some());
        let spilled = sidecar
            .read_latest(7)
            .expect("read spilled latest")
            .expect("page 7 should spill during recovery");
        assert!(matches!(
            spilled.payload,
            crate::wal::index::WalVersionPayload::OnDisk {
                encoding: crate::wal::format::FrameEncoding::Page,
                ..
            }
        ));
    }

    #[test]
    fn recovery_spills_excess_delta_versions_into_sidecar() {
        let mem_vfs = Arc::new(crate::vfs::mem::MemVfs::default());
        let vfs: Arc<dyn Vfs> = mem_vfs.clone();
        let handle = VfsHandle::from_vfs(vfs);
        let db_path = Path::new("spill-recovery-delta.ddb");
        let wal_path = Path::new("spill-recovery-delta.ddb.wal");
        let file = mem_vfs
            .open(wal_path, OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&mem_vfs, db_path);

        let ps = page::DEFAULT_PAGE_SIZE;
        let base_page = vec![0x21; ps as usize];
        pager
            .write_page_direct(7, &base_page)
            .expect("seed base page for delta recovery");
        let mut updated_page = base_page.clone();
        updated_page[0] = 0x99;
        updated_page[11] = 0x42;
        let delta_payload =
            crate::wal::delta::encode_page_delta(&base_page, &updated_page).expect("delta payload");
        let frames = vec![
            crate::wal::format::WalFrame::page_delta(7, delta_payload),
            crate::wal::format::WalFrame::commit(),
            crate::wal::format::WalFrame::page(8, vec![0x22; ps as usize]),
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

        let mut sidecar = crate::wal::index_sidecar::WalIndexSidecar::open(&handle, db_path)
            .expect("open sidecar");
        let (index, _end, _max_page_id) =
            initialize_or_recover(&file, &pager, ps, 1, Some(&mut sidecar)).expect("recover");

        assert_eq!(index.version_count(), 1);
        assert_eq!(sidecar.version_count(), 1);
        assert!(index.latest_visible(8, u64::MAX).is_some());
        let spilled = sidecar
            .read_latest(7)
            .expect("read spilled latest")
            .expect("page 7 should spill during recovery");
        assert!(matches!(
            spilled.payload,
            crate::wal::index::WalVersionPayload::OnDisk {
                encoding: crate::wal::format::FrameEncoding::PageDelta,
                ..
            }
        ));
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
            initialize_or_recover(&file, &pager, ps, 0, None).expect("recover");
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
            initialize_or_recover(&file, &pager, ps, 0, None).expect("recover");
        let version = index
            .latest_visible(3, u64::MAX)
            .expect("recovered page version");
        assert_eq!(version.payload.as_slice(), updated.as_slice());
    }

    #[test]
    fn recovery_many_deltas_to_same_page_does_not_clone_base_per_delta() {
        let vfs = crate::vfs::mem::MemVfs::default();
        let file = vfs
            .open(Path::new(":memory:"), OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager = test_pager(&vfs, Path::new(":memory-db:"));

        let ps = page::DEFAULT_PAGE_SIZE;
        let page_id = 9_u32;
        let mut current = vec![0_u8; ps as usize];
        current[0..4].copy_from_slice(&1_u32.to_le_bytes());

        let mut frames = Vec::with_capacity(1026);
        frames.push(crate::wal::format::WalFrame::page(page_id, current.clone()));
        for update in 0_u32..1024_u32 {
            let mut next = current.clone();
            let byte_index = ((update as usize) * 31) % next.len();
            next[byte_index] = next[byte_index]
                .wrapping_add((update % 251) as u8)
                .wrapping_add(1);
            let delta =
                crate::wal::delta::encode_page_delta(&current, &next).expect("encode delta");
            frames.push(crate::wal::format::WalFrame::page_delta(page_id, delta));
            current = next;
        }
        let expected = current;
        frames.push(crate::wal::format::WalFrame::commit());

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
            initialize_or_recover(&file, &pager, ps, 0, None).expect("recover");
        assert_eq!(index.version_count(), 1);
        let version = index
            .latest_visible(page_id, u64::MAX)
            .expect("latest version for page");
        assert_eq!(version.payload.as_slice(), expected.as_slice());
    }

    #[test]
    #[ignore = "requires generating a large synthetic WAL to exercise the hard overflow bound"]
    fn recovery_rejects_pending_overflow() {
        let tempdir = TempDir::new().expect("tempdir");
        let wal_path = tempdir.path().join("overflow.wal");
        let vfs = crate::vfs::VfsHandle::for_path(&wal_path);
        let file = vfs
            .open(&wal_path, OpenMode::CreateNew, FileKind::Wal)
            .expect("create wal file");
        let pager_vfs = crate::vfs::mem::MemVfs::default();
        let pager = test_pager(&pager_vfs, Path::new(":memory-db:"));

        let ps = page::DEFAULT_PAGE_SIZE;
        let page_id = 13_u32;
        let base = vec![0_u8; ps as usize];
        let mut updated = base.clone();
        updated[0] = 1;
        let delta = crate::wal::delta::encode_page_delta(&base, &updated).expect("encode delta");
        let page_bytes = crate::wal::format::WalFrame::page(page_id, base)
            .encode(ps)
            .expect("encode page frame");
        let delta_bytes = crate::wal::format::WalFrame::page_delta(page_id, delta)
            .encode(ps)
            .expect("encode delta frame");

        write_all_at(file.as_ref(), 0, &WalHeader::new(ps, 0).encode()).expect("write header");
        let mut offset = WAL_HEADER_SIZE;
        write_all_at(file.as_ref(), offset, &page_bytes).expect("write page frame");
        offset += page_bytes.len() as u64;
        for _ in 0..MAX_PENDING_RECOVERY_FRAMES {
            write_all_at(file.as_ref(), offset, &delta_bytes).expect("write delta frame");
            offset += delta_bytes.len() as u64;
        }
        persist_header(&file, ps, offset).expect("persist header");
        file.set_len(offset).expect("set wal len");

        let error =
            initialize_or_recover(&file, &pager, ps, 0, None).expect_err("pending overflow");
        assert!(matches!(error, crate::error::DbError::Corruption { .. }));
        assert!(error
            .to_string()
            .contains("more than 1,000,000 uncommitted page frames"));
    }
}

//! WAL index spill sidecar for ADR 0141.
//!
//! The sidecar is a bounded-memory cache of *latest* cold page versions.
//! Each spilled page stores only the WAL metadata needed to rematerialize
//! its payload from the WAL file (`lsn`, `wal_offset`, `frame_len`,
//! `encoding`). Historical multi-version chains stay resident whenever
//! readers are active; only the reader-free latest-version case spills.

/// File extension for the WAL index sidecar (e.g. `mydb.ddb.wal-idx`).
pub(crate) const WAL_INDEX_SIDECAR_EXT: &str = "wal-idx";

/// Magic bytes prefixing the sidecar header so a future format-version
/// check can detect mis-matched files.
///
/// The literal value is `DDB-WIDX` (8 bytes) and is intentionally
/// reserved now even though no writer emits it yet.
pub(crate) const WAL_INDEX_SIDECAR_MAGIC: &[u8; 8] = b"DDB-WIDX";

/// On-disk format version stamped into the sidecar header. Bumping this
/// requires an ADR-tracked migration path (see AGENTS.md §7).
pub(crate) const WAL_INDEX_SIDECAR_VERSION: u16 = 0;

const WAL_INDEX_SIDECAR_HEADER_LEN: u64 = 16;
const WAL_INDEX_SIDECAR_RECORD_LEN: u64 = 32;
const RECORD_STATE_EMPTY: u8 = 0;
const RECORD_STATE_PRESENT: u8 = 1;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{DbError, Result};
use crate::storage::page::PageId;
use crate::vfs::{read_exact_at, write_all_at, FileKind, OpenMode, VfsFile, VfsHandle};

use super::format::FrameEncoding;
use super::index::{WalVersion, WalVersionPayload};

#[derive(Debug)]
pub(crate) struct WalIndexSidecar {
    file: Arc<dyn VfsFile>,
    entry_count: usize,
}

impl WalIndexSidecar {
    pub(crate) fn open(vfs: &VfsHandle, db_path: &Path) -> Result<Self> {
        let path = sidecar_path_for_db(db_path);
        let mode = if vfs.file_exists(&path)? {
            OpenMode::OpenExisting
        } else {
            OpenMode::OpenOrCreate
        };
        let file = vfs.open(&path, mode, FileKind::Wal)?;
        let mut sidecar = Self {
            file,
            entry_count: 0,
        };
        // The sidecar is a rebuildable cache derived from the WAL. Reset it
        // on every open so stale pre-ADR-0141 files cannot affect recovery.
        sidecar.clear()?;
        Ok(sidecar)
    }

    pub(crate) fn clear(&mut self) -> Result<()> {
        let mut header = [0_u8; WAL_INDEX_SIDECAR_HEADER_LEN as usize];
        header[..WAL_INDEX_SIDECAR_MAGIC.len()].copy_from_slice(WAL_INDEX_SIDECAR_MAGIC);
        header[8..10].copy_from_slice(&WAL_INDEX_SIDECAR_VERSION.to_le_bytes());
        header[10..12].copy_from_slice(&(WAL_INDEX_SIDECAR_RECORD_LEN as u16).to_le_bytes());
        write_all_at(self.file.as_ref(), 0, &header)?;
        self.file.set_len(WAL_INDEX_SIDECAR_HEADER_LEN)?;
        self.entry_count = 0;
        Ok(())
    }

    pub(crate) fn read_latest(&self, page_id: PageId) -> Result<Option<WalVersion>> {
        if page_id == 0 {
            return Ok(None);
        }
        let Some(record) = self.read_record(page_id)? else {
            return Ok(None);
        };
        Ok(Some(WalVersion {
            lsn: record.lsn,
            payload: WalVersionPayload::OnDisk {
                wal_offset: record.wal_offset,
                frame_len: record.frame_len,
                encoding: record.encoding,
            },
        }))
    }

    pub(crate) fn write_latest(&mut self, page_id: PageId, version: &WalVersion) -> Result<()> {
        if page_id == 0 {
            return Err(DbError::corruption(
                "WAL index sidecar cannot store page id 0",
            ));
        }
        let (wal_offset, frame_len, encoding) = version.payload.wal_metadata();
        let offset = record_offset(page_id);
        let existed = self.read_record(page_id)?.is_some();
        if self.file.file_size()? < offset + WAL_INDEX_SIDECAR_RECORD_LEN {
            self.file.set_len(offset + WAL_INDEX_SIDECAR_RECORD_LEN)?;
        }
        let mut record = [0_u8; WAL_INDEX_SIDECAR_RECORD_LEN as usize];
        record[0] = RECORD_STATE_PRESENT;
        record[1] = encode_encoding(encoding);
        record[4..8].copy_from_slice(&frame_len.to_le_bytes());
        record[8..16].copy_from_slice(&version.lsn.to_le_bytes());
        record[16..24].copy_from_slice(&wal_offset.to_le_bytes());
        write_all_at(self.file.as_ref(), offset, &record)?;
        if !existed {
            self.entry_count += 1;
        }
        Ok(())
    }

    pub(crate) fn clear_latest(&mut self, page_id: PageId) -> Result<()> {
        if page_id == 0 {
            return Ok(());
        }
        let offset = record_offset(page_id);
        if self.file.file_size()? < offset + WAL_INDEX_SIDECAR_RECORD_LEN {
            return Ok(());
        }
        if self.read_record(page_id)?.is_none() {
            return Ok(());
        }
        let record = [0_u8; WAL_INDEX_SIDECAR_RECORD_LEN as usize];
        write_all_at(self.file.as_ref(), offset, &record)?;
        self.entry_count = self.entry_count.saturating_sub(1);
        Ok(())
    }

    pub(crate) fn populate_latest_versions_at_or_before(
        &self,
        safe_lsn: u64,
        out: &mut Vec<(PageId, WalVersion)>,
    ) -> Result<()> {
        let file_size = self.file.file_size()?;
        if file_size <= WAL_INDEX_SIDECAR_HEADER_LEN {
            return Ok(());
        }
        let slot_count =
            ((file_size - WAL_INDEX_SIDECAR_HEADER_LEN) / WAL_INDEX_SIDECAR_RECORD_LEN) as u32;
        for page_id in 1..=slot_count {
            if let Some(version) = self.read_latest(page_id)? {
                if version.lsn <= safe_lsn {
                    out.push((page_id, version));
                }
            }
        }
        Ok(())
    }

    #[must_use]
    pub(crate) fn version_count(&self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub(crate) fn version_counts_by_payload(&self) -> (usize, usize) {
        (0, self.entry_count)
    }

    fn read_record(&self, page_id: PageId) -> Result<Option<SidecarRecord>> {
        let offset = record_offset(page_id);
        if self.file.file_size()? < offset + WAL_INDEX_SIDECAR_RECORD_LEN {
            return Ok(None);
        }
        let mut record = [0_u8; WAL_INDEX_SIDECAR_RECORD_LEN as usize];
        read_exact_at(self.file.as_ref(), offset, &mut record)?;
        match record[0] {
            RECORD_STATE_EMPTY => Ok(None),
            RECORD_STATE_PRESENT => Ok(Some(SidecarRecord {
                encoding: decode_encoding(record[1])?,
                frame_len: u32::from_le_bytes(record[4..8].try_into().expect("u32 slice")),
                lsn: u64::from_le_bytes(record[8..16].try_into().expect("u64 slice")),
                wal_offset: u64::from_le_bytes(record[16..24].try_into().expect("u64 slice")),
            })),
            state => Err(DbError::corruption(format!(
                "WAL index sidecar {} has invalid record state {state} for page {page_id}",
                self.file.path().display()
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct SidecarRecord {
    lsn: u64,
    wal_offset: u64,
    frame_len: u32,
    encoding: FrameEncoding,
}

fn sidecar_path_for_db(db_path: &Path) -> PathBuf {
    let mut path = db_path.as_os_str().to_os_string();
    path.push(".");
    path.push(WAL_INDEX_SIDECAR_EXT);
    PathBuf::from(path)
}

fn record_offset(page_id: PageId) -> u64 {
    WAL_INDEX_SIDECAR_HEADER_LEN + (u64::from(page_id) - 1) * WAL_INDEX_SIDECAR_RECORD_LEN
}

fn encode_encoding(encoding: FrameEncoding) -> u8 {
    match encoding {
        FrameEncoding::Page => 1,
        FrameEncoding::PageDelta => 2,
    }
}

fn decode_encoding(tag: u8) -> Result<FrameEncoding> {
    match tag {
        1 => Ok(FrameEncoding::Page),
        2 => Ok(FrameEncoding::PageDelta),
        _ => Err(DbError::corruption(format!(
            "WAL index sidecar has invalid frame encoding tag {tag}"
        ))),
    }
}

/// Tag identifying the active WAL-index implementation. Used by tests and
/// future telemetry to assert that the sidecar code path is or is not in
/// effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalIndexBackendKind {
    /// Fully-resident `HashMap<PageId, Vec<WalVersion>>` — current
    /// behavior. Selected when `wal_index_hot_set_pages == 0`.
    InMemory,
    /// Paged sidecar (ADR 0141 follow-up). Reserved; not yet wired up.
    PagedSidecar,
}

impl WalIndexBackendKind {
    /// Choose the backend based on the embedder's configuration. Today
    /// the function always returns `InMemory`; once the sidecar is
    /// implemented this will switch to `PagedSidecar` when the embedder
    /// opts in.
    pub(crate) fn for_hot_set_pages(hot_set_pages: u32) -> Self {
        if hot_set_pages == 0 {
            Self::InMemory
        } else {
            Self::PagedSidecar
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::storage::page;
    use crate::vfs::mem::MemVfs;

    use super::*;
    use crate::wal::index::WalVersionPayload;

    #[test]
    fn magic_bytes_are_eight_ascii_chars() {
        assert_eq!(WAL_INDEX_SIDECAR_MAGIC.len(), 8);
        assert!(WAL_INDEX_SIDECAR_MAGIC.iter().all(|b| b.is_ascii_graphic()));
    }

    #[test]
    fn zero_hot_set_picks_in_memory_backend() {
        assert_eq!(
            WalIndexBackendKind::for_hot_set_pages(0),
            WalIndexBackendKind::InMemory
        );
    }

    #[test]
    fn nonzero_hot_set_picks_paged_sidecar() {
        assert_eq!(
            WalIndexBackendKind::for_hot_set_pages(4096),
            WalIndexBackendKind::PagedSidecar
        );
    }

    #[test]
    fn sidecar_round_trips_latest_metadata() {
        let vfs = VfsHandle::from_vfs(Arc::new(MemVfs::default()));
        let mut sidecar = WalIndexSidecar::open(&vfs, Path::new("demo.ddb")).expect("open sidecar");
        sidecar
            .write_latest(
                7,
                &WalVersion::resident(
                    88,
                    44,
                    24,
                    FrameEncoding::PageDelta,
                    Arc::from(vec![0xAA; page::DEFAULT_PAGE_SIZE as usize]),
                ),
            )
            .expect("write sidecar entry");
        let version = sidecar.read_latest(7).expect("read sidecar entry").unwrap();
        assert_eq!(version.lsn, 88);
        assert!(matches!(
            version.payload,
            WalVersionPayload::OnDisk {
                wal_offset: 44,
                frame_len: 24,
                encoding: FrameEncoding::PageDelta,
            }
        ));
    }

    #[test]
    fn sidecar_populates_latest_versions_at_or_before() {
        let vfs = VfsHandle::from_vfs(Arc::new(MemVfs::default()));
        let mut sidecar = WalIndexSidecar::open(&vfs, Path::new("demo.ddb")).expect("open sidecar");
        sidecar
            .write_latest(
                2,
                &WalVersion {
                    lsn: 10,
                    payload: WalVersionPayload::OnDisk {
                        wal_offset: 100,
                        frame_len: 40,
                        encoding: FrameEncoding::Page,
                    },
                },
            )
            .expect("write page 2");
        sidecar
            .write_latest(
                4,
                &WalVersion {
                    lsn: 20,
                    payload: WalVersionPayload::OnDisk {
                        wal_offset: 200,
                        frame_len: 44,
                        encoding: FrameEncoding::PageDelta,
                    },
                },
            )
            .expect("write page 4");
        let mut out = Vec::new();
        sidecar
            .populate_latest_versions_at_or_before(15, &mut out)
            .expect("collect latest versions");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, 2);
        assert_eq!(out[0].1.lsn, 10);
    }
}

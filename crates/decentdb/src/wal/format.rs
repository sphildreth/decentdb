//! WAL header and frame encoding for the v8 layout.
//!
//! Implements:
//! - design/adr/0064-wal-frame-checksum-removal.md
//! - design/adr/0065-wal-frame-lsn-removal.md
//! - design/adr/0066-wal-frame-payload-length-removal.md
//! - design/adr/0068-wal-header-end-offset.md

use crate::error::{DbError, Result};
use crate::storage::page::PageId;
use crate::vfs::{read_exact_at, VfsFile};

pub(crate) const WAL_HEADER_SIZE: u64 = 32;
pub(crate) const WAL_HEADER_SIZE_USIZE: usize = WAL_HEADER_SIZE as usize;
pub(crate) const WAL_HEADER_VERSION: u32 = 1;
pub(crate) const WAL_MAGIC: [u8; 8] = *b"DDBWAL01";
pub(crate) const FRAME_HEADER_SIZE: usize = 5;
pub(crate) const FRAME_TRAILER_SIZE: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FrameType {
    Page = 0,
    Commit = 1,
    Checkpoint = 2,
}

impl FrameType {
    pub(crate) fn payload_size(self, page_size: u32) -> usize {
        match self {
            Self::Page => page_size as usize,
            Self::Commit => 0,
            Self::Checkpoint => 8,
        }
    }
}

impl TryFrom<u8> for FrameType {
    type Error = DbError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Page),
            1 => Ok(Self::Commit),
            2 => Ok(Self::Checkpoint),
            _ => Err(DbError::corruption(format!(
                "unknown WAL frame type {value}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalHeader {
    pub(crate) page_size: u32,
    pub(crate) wal_end_offset: u64,
}

impl WalHeader {
    #[must_use]
    pub(crate) fn new(page_size: u32, wal_end_offset: u64) -> Self {
        Self {
            page_size,
            wal_end_offset,
        }
    }

    #[must_use]
    pub(crate) fn encode(&self) -> [u8; WAL_HEADER_SIZE_USIZE] {
        let mut bytes = [0_u8; WAL_HEADER_SIZE_USIZE];
        bytes[0..WAL_MAGIC.len()].copy_from_slice(&WAL_MAGIC);
        bytes[8..12].copy_from_slice(&WAL_HEADER_VERSION.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.page_size.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.wal_end_offset.to_le_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8; WAL_HEADER_SIZE_USIZE]) -> Result<Self> {
        if bytes[0..WAL_MAGIC.len()] != WAL_MAGIC {
            return Err(DbError::corruption("invalid WAL header magic"));
        }
        let header_version = read_u32(bytes, 8);
        if header_version != WAL_HEADER_VERSION {
            return Err(DbError::corruption(format!(
                "unsupported WAL header version {header_version}; expected {WAL_HEADER_VERSION}"
            )));
        }
        let page_size = read_u32(bytes, 12);
        let wal_end_offset = read_u64(bytes, 16);
        if wal_end_offset != 0 && wal_end_offset < WAL_HEADER_SIZE {
            return Err(DbError::corruption(format!(
                "invalid WAL logical end offset {wal_end_offset}"
            )));
        }
        Ok(Self {
            page_size,
            wal_end_offset,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalFrame {
    pub(crate) frame_type: FrameType,
    pub(crate) page_id: PageId,
    pub(crate) payload: Vec<u8>,
}

impl WalFrame {
    #[must_use]
    pub(crate) fn page(page_id: PageId, payload: Vec<u8>) -> Self {
        Self {
            frame_type: FrameType::Page,
            page_id,
            payload,
        }
    }

    #[must_use]
    pub(crate) fn commit() -> Self {
        Self {
            frame_type: FrameType::Commit,
            page_id: 0,
            payload: Vec::new(),
        }
    }

    #[must_use]
    pub(crate) fn checkpoint(checkpoint_lsn: u64) -> Self {
        Self {
            frame_type: FrameType::Checkpoint,
            page_id: 0,
            payload: checkpoint_lsn.to_le_bytes().to_vec(),
        }
    }

    pub(crate) fn encode(&self, page_size: u32) -> Result<Vec<u8>> {
        let expected_payload = self.frame_type.payload_size(page_size);
        if self.payload.len() != expected_payload {
            return Err(DbError::internal(format!(
                "WAL frame payload length {} does not match expected payload length {}",
                self.payload.len(),
                expected_payload
            )));
        }
        if matches!(self.frame_type, FrameType::Page) && self.page_id == 0 {
            return Err(DbError::corruption(
                "page WAL frames must have a non-zero page id",
            ));
        }
        if !matches!(self.frame_type, FrameType::Page) && self.page_id != 0 {
            return Err(DbError::corruption(
                "non-page WAL frames must use page id 0",
            ));
        }

        let mut bytes = Vec::with_capacity(self.encoded_len(page_size));
        bytes.push(self.frame_type as u8);
        bytes.extend_from_slice(&self.page_id.to_le_bytes());
        bytes.extend_from_slice(&self.payload);
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        Ok(bytes)
    }

    #[must_use]
    pub(crate) fn encoded_len(&self, page_size: u32) -> usize {
        FRAME_HEADER_SIZE + self.frame_type.payload_size(page_size) + FRAME_TRAILER_SIZE
    }

    pub(crate) fn decode_from_file(
        file: &dyn VfsFile,
        offset: u64,
        page_size: u32,
        logical_end: u64,
    ) -> Result<Option<Self>> {
        if offset >= logical_end {
            return Ok(None);
        }
        if offset + FRAME_HEADER_SIZE as u64 > logical_end {
            return Ok(None);
        }

        let mut header = [0_u8; FRAME_HEADER_SIZE];
        read_exact_at(file, offset, &mut header)?;
        let frame_type = FrameType::try_from(header[0])?;
        let page_id = u32::from_le_bytes([header[1], header[2], header[3], header[4]]);
        let payload_len = frame_type.payload_size(page_size);
        let total_len = FRAME_HEADER_SIZE + payload_len + FRAME_TRAILER_SIZE;
        if offset + total_len as u64 > logical_end {
            return Ok(None);
        }

        let mut body = vec![0_u8; payload_len + FRAME_TRAILER_SIZE];
        read_exact_at(file, offset + FRAME_HEADER_SIZE as u64, &mut body)?;
        let payload = body[..payload_len].to_vec();

        if matches!(frame_type, FrameType::Page) && page_id == 0 {
            return Err(DbError::corruption("page WAL frame used page id 0"));
        }
        if !matches!(frame_type, FrameType::Page) && page_id != 0 {
            return Err(DbError::corruption(
                "non-page WAL frame used a non-zero page id",
            ));
        }

        Ok(Some(Self {
            frame_type,
            page_id,
            payload,
        }))
    }
}

fn read_u32(bytes: &[u8; WAL_HEADER_SIZE_USIZE], offset: usize) -> u32 {
    let mut raw = [0_u8; 4];
    raw.copy_from_slice(&bytes[offset..offset + 4]);
    u32::from_le_bytes(raw)
}

fn read_u64(bytes: &[u8; WAL_HEADER_SIZE_USIZE], offset: usize) -> u64 {
    let mut raw = [0_u8; 8];
    raw.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(raw)
}

#[cfg(test)]
mod tests {
    use crate::storage::page;

    use super::{FrameType, WalFrame, WalHeader};

    #[test]
    fn wal_header_roundtrip_preserves_fields() {
        let header = WalHeader::new(page::DEFAULT_PAGE_SIZE, 4096);
        let encoded = header.encode();
        let decoded = WalHeader::decode(&encoded).expect("decode WAL header");

        assert_eq!(decoded, header);
    }

    #[test]
    fn wal_frame_encode_preserves_current_format_sizes() {
        let page_frame = WalFrame::page(3, vec![0_u8; page::DEFAULT_PAGE_SIZE as usize]);
        let commit = WalFrame::commit();
        let checkpoint = WalFrame::checkpoint(123);

        assert_eq!(
            page_frame.encoded_len(page::DEFAULT_PAGE_SIZE),
            5 + page::DEFAULT_PAGE_SIZE as usize + 8
        );
        assert_eq!(commit.encoded_len(page::DEFAULT_PAGE_SIZE), 13);
        assert_eq!(checkpoint.encoded_len(page::DEFAULT_PAGE_SIZE), 21);
        assert_eq!(commit.frame_type, FrameType::Commit);
    }
}

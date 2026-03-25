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
    #[cfg(test)]
    #[must_use]
    pub(crate) fn page(page_id: PageId, payload: Vec<u8>) -> Self {
        Self {
            frame_type: FrameType::Page,
            page_id,
            payload,
        }
    }

    #[cfg(test)]
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
    use std::sync::RwLock;

    use crate::error::{DbError, Result};
    use crate::storage::page;
    use crate::vfs::{FileKind, VfsFile};

    use super::{FrameType, WalFrame, WalHeader};

    /// A minimal in-memory VfsFile for unit-testing frame decode.
    #[derive(Debug)]
    struct TestFile {
        data: RwLock<Vec<u8>>,
    }

    impl TestFile {
        fn new(bytes: Vec<u8>) -> Self {
            Self {
                data: RwLock::new(bytes),
            }
        }
    }

    impl VfsFile for TestFile {
        fn kind(&self) -> FileKind {
            FileKind::Wal
        }
        fn path(&self) -> &std::path::Path {
            std::path::Path::new(":test:")
        }
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
            let data = self.data.read().map_err(|_| DbError::internal("lock"))?;
            let off = offset as usize;
            if off >= data.len() {
                return Ok(0);
            }
            let avail = &data[off..];
            let n = avail.len().min(buf.len());
            buf[..n].copy_from_slice(&avail[..n]);
            Ok(n)
        }
        fn write_at(&self, _: u64, _: &[u8]) -> Result<usize> {
            unimplemented!()
        }
        fn sync_data(&self) -> Result<()> {
            Ok(())
        }
        fn sync_metadata(&self) -> Result<()> {
            Ok(())
        }
        fn file_size(&self) -> Result<u64> {
            Ok(self.data.read().unwrap().len() as u64)
        }
        fn set_len(&self, _: u64) -> Result<()> {
            Ok(())
        }
    }

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

    // ------- New decode / roundtrip tests -------

    #[test]
    fn frame_type_try_from_valid() {
        assert_eq!(FrameType::try_from(0).unwrap(), FrameType::Page);
        assert_eq!(FrameType::try_from(1).unwrap(), FrameType::Commit);
        assert_eq!(FrameType::try_from(2).unwrap(), FrameType::Checkpoint);
    }

    #[test]
    fn frame_type_try_from_invalid() {
        assert!(FrameType::try_from(3).is_err());
        assert!(FrameType::try_from(255).is_err());
    }

    #[test]
    fn wal_header_decode_bad_magic() {
        let mut bytes = [0_u8; super::WAL_HEADER_SIZE_USIZE];
        bytes[0..8].copy_from_slice(b"BADMAGIC");
        assert!(WalHeader::decode(&bytes).is_err());
    }

    #[test]
    fn wal_header_decode_bad_version() {
        let header = WalHeader::new(page::DEFAULT_PAGE_SIZE, 4096);
        let mut encoded = header.encode();
        // Corrupt the version field (bytes 8..12)
        encoded[8..12].copy_from_slice(&99_u32.to_le_bytes());
        assert!(WalHeader::decode(&encoded).is_err());
    }

    #[test]
    fn wal_header_decode_bad_end_offset() {
        let header = WalHeader::new(page::DEFAULT_PAGE_SIZE, 4096);
        let mut encoded = header.encode();
        // Set wal_end_offset to something < WAL_HEADER_SIZE but non-zero
        encoded[16..24].copy_from_slice(&1_u64.to_le_bytes());
        assert!(WalHeader::decode(&encoded).is_err());
    }

    #[test]
    fn wal_header_zero_end_offset_ok() {
        let header = WalHeader::new(page::DEFAULT_PAGE_SIZE, 0);
        let encoded = header.encode();
        let decoded = WalHeader::decode(&encoded).unwrap();
        assert_eq!(decoded.wal_end_offset, 0);
    }

    #[test]
    fn page_frame_encode_decode_roundtrip() {
        let ps = page::DEFAULT_PAGE_SIZE;
        let payload = vec![42_u8; ps as usize];
        let frame = WalFrame::page(7, payload.clone());
        let encoded = frame.encode(ps).unwrap();

        let file = TestFile::new(encoded.to_vec());
        let total_len = encoded.len() as u64;
        let decoded = WalFrame::decode_from_file(&file, 0, ps, total_len)
            .unwrap()
            .unwrap();

        assert_eq!(decoded.frame_type, FrameType::Page);
        assert_eq!(decoded.page_id, 7);
        assert_eq!(decoded.payload, payload);
    }

    #[test]
    fn commit_frame_encode_decode_roundtrip() {
        let ps = page::DEFAULT_PAGE_SIZE;
        let frame = WalFrame::commit();
        let encoded = frame.encode(ps).unwrap();

        let file = TestFile::new(encoded.to_vec());
        let total_len = encoded.len() as u64;
        let decoded = WalFrame::decode_from_file(&file, 0, ps, total_len)
            .unwrap()
            .unwrap();

        assert_eq!(decoded.frame_type, FrameType::Commit);
        assert_eq!(decoded.page_id, 0);
    }

    #[test]
    fn checkpoint_frame_encode_decode_roundtrip() {
        let ps = page::DEFAULT_PAGE_SIZE;
        let frame = WalFrame::checkpoint(456);
        let encoded = frame.encode(ps).unwrap();

        let file = TestFile::new(encoded.to_vec());
        let total_len = encoded.len() as u64;
        let decoded = WalFrame::decode_from_file(&file, 0, ps, total_len)
            .unwrap()
            .unwrap();

        assert_eq!(decoded.frame_type, FrameType::Checkpoint);
        assert_eq!(decoded.page_id, 0);
        assert_eq!(decoded.payload, 456_u64.to_le_bytes());
    }

    #[test]
    fn decode_returns_none_beyond_logical_end() {
        let ps = page::DEFAULT_PAGE_SIZE;
        let file = TestFile::new([].to_vec());
        assert!(WalFrame::decode_from_file(&file, 0, ps, 0).unwrap().is_none());
        assert!(WalFrame::decode_from_file(&file, 100, ps, 50).unwrap().is_none());
    }

    #[test]
    fn decode_returns_none_for_partial_header() {
        let ps = page::DEFAULT_PAGE_SIZE;
        let file = TestFile::new([0, 0, 0].to_vec());
        // logical_end is 3 which is less than FRAME_HEADER_SIZE(5), so header overflows
        assert!(WalFrame::decode_from_file(&file, 0, ps, 3).unwrap().is_none());
    }

    #[test]
    fn decode_returns_none_for_partial_frame() {
        let ps = page::DEFAULT_PAGE_SIZE;
        let frame = WalFrame::commit();
        let encoded = frame.encode(ps).unwrap();
        let file = TestFile::new(encoded.to_vec());
        // Set logical end to less than the full frame size
        let short = (encoded.len() - 1) as u64;
        assert!(WalFrame::decode_from_file(&file, 0, ps, short).unwrap().is_none());
    }

    #[test]
    fn encode_error_wrong_payload_size() {
        let ps = page::DEFAULT_PAGE_SIZE;
        // Page frame with wrong payload size
        let frame = WalFrame {
            frame_type: FrameType::Page,
            page_id: 1,
            payload: vec![0; 10], // wrong size
        };
        assert!(frame.encode(ps).is_err());
    }

    #[test]
    fn encode_error_page_with_zero_id() {
        let ps = page::DEFAULT_PAGE_SIZE;
        let frame = WalFrame {
            frame_type: FrameType::Page,
            page_id: 0, // invalid
            payload: vec![0; ps as usize],
        };
        assert!(frame.encode(ps).is_err());
    }

    #[test]
    fn encode_error_commit_with_nonzero_id() {
        let ps = page::DEFAULT_PAGE_SIZE;
        let frame = WalFrame {
            frame_type: FrameType::Commit,
            page_id: 5, // should be 0
            payload: vec![],
        };
        assert!(frame.encode(ps).is_err());
    }

    #[test]
    fn multi_frame_decode_from_file() {
        let ps = page::DEFAULT_PAGE_SIZE;
        let frames = vec![
            WalFrame::page(1, vec![0xAA; ps as usize]),
            WalFrame::page(2, vec![0xBB; ps as usize]),
            WalFrame::commit(),
        ];

        let mut data = Vec::new();
        for f in &frames {
            data.extend_from_slice(&f.encode(ps).unwrap());
        }
        let logical_end = data.len() as u64;
        let file = TestFile::new(data.to_vec());

        let mut offset = 0_u64;
        let mut decoded = Vec::new();
        while let Some(frame) = WalFrame::decode_from_file(&file, offset, ps, logical_end).unwrap()
        {
            offset += frame.encoded_len(ps) as u64;
            decoded.push(frame);
        }

        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].page_id, 1);
        assert_eq!(decoded[1].page_id, 2);
        assert_eq!(decoded[2].frame_type, FrameType::Commit);
    }

    #[test]
    fn decode_corrupt_page_id_zero_for_page_frame() {
        let ps = page::DEFAULT_PAGE_SIZE;
        let frame = WalFrame::page(1, vec![0; ps as usize]);
        let mut encoded = frame.encode(ps).unwrap();
        // Corrupt page_id to 0 (bytes 1..5)
        encoded[1..5].copy_from_slice(&0_u32.to_le_bytes());
        let file = TestFile::new(encoded.to_vec());
        let logical_end = encoded.len() as u64;
        assert!(WalFrame::decode_from_file(&file, 0, ps, logical_end).is_err());
    }

    #[test]
    fn decode_corrupt_nonzero_page_id_for_commit() {
        let ps = page::DEFAULT_PAGE_SIZE;
        let frame = WalFrame::commit();
        let mut encoded = frame.encode(ps).unwrap();
        // Corrupt page_id to non-zero (bytes 1..5)
        encoded[1..5].copy_from_slice(&42_u32.to_le_bytes());
        let file = TestFile::new(encoded.to_vec());
        let logical_end = encoded.len() as u64;
        assert!(WalFrame::decode_from_file(&file, 0, ps, logical_end).is_err());
    }
}

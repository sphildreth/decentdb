//! Fixed main-database header encoding and validation.

use crate::error::{DbError, Result};
use crate::vfs::{read_exact_at, write_all_at, VfsFile};

use super::checksum;
use super::freelist::FreelistState;
use super::page;

pub(crate) const DB_HEADER_SIZE: usize = 128;
pub const DB_FORMAT_VERSION: u32 = 9;
#[allow(dead_code)]
pub(crate) const WAL_HEADER_VERSION: u32 = 1;
pub(crate) const HEADER_PAGE_ID: u32 = 1;
pub(crate) const CATALOG_ROOT_PAGE_ID: u32 = 2;
pub(crate) const DB_MAGIC: [u8; 16] = *b"DECENTDB\0\0\0\0\0\0\0\0";

const FORMAT_VERSION_OFFSET: usize = 16;
const PAGE_SIZE_OFFSET: usize = 20;
const CHECKSUM_OFFSET: usize = 24;
const SCHEMA_COOKIE_OFFSET: usize = 28;
const CATALOG_ROOT_OFFSET: usize = 32;
const FREELIST_ROOT_OFFSET: usize = 36;
const FREELIST_HEAD_OFFSET: usize = 40;
const FREELIST_COUNT_OFFSET: usize = 44;
const LAST_CHECKPOINT_LSN_OFFSET: usize = 48;
const RESERVED_OFFSET: usize = 56;
const RESERVED_LEN: usize = 72;

/// Parsed representation of the fixed page-1 database header.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DatabaseHeader {
    pub(crate) magic: [u8; 16],
    pub(crate) format_version: u32,
    pub(crate) page_size: u32,
    pub(crate) header_checksum: u32,
    pub(crate) schema_cookie: u32,
    pub(crate) catalog_root_page_id: u32,
    pub(crate) freelist: FreelistState,
    pub(crate) last_checkpoint_lsn: u64,
    pub(crate) reserved: [u8; RESERVED_LEN],
}

impl DatabaseHeader {
    #[must_use]
    pub(crate) fn new(page_size: u32) -> Self {
        let mut header = Self {
            magic: DB_MAGIC,
            format_version: DB_FORMAT_VERSION,
            page_size,
            header_checksum: 0,
            schema_cookie: 0,
            catalog_root_page_id: CATALOG_ROOT_PAGE_ID,
            freelist: FreelistState::default(),
            last_checkpoint_lsn: 0,
            reserved: [0; RESERVED_LEN],
        };
        header.header_checksum = header.compute_checksum();
        header
    }

    #[must_use]
    pub(crate) fn encode(&self) -> [u8; DB_HEADER_SIZE] {
        let mut bytes = self.encode_with_checksum(0);
        let checksum =
            checksum::crc32c_parts(&[&bytes[0..CHECKSUM_OFFSET], &bytes[CHECKSUM_OFFSET + 4..]]);
        write_u32(&mut bytes, CHECKSUM_OFFSET, checksum);
        bytes
    }

    pub(crate) fn decode(bytes: &[u8; DB_HEADER_SIZE]) -> Result<Self> {
        let header = Self::decode_loose(bytes)?;
        if header.format_version != DB_FORMAT_VERSION {
            return Err(DbError::unsupported_format_version(header.format_version));
        }

        let stored_checksum = read_u32(bytes, CHECKSUM_OFFSET);
        let expected_checksum =
            checksum::crc32c_parts(&[&bytes[0..CHECKSUM_OFFSET], &bytes[CHECKSUM_OFFSET + 4..]]);
        if stored_checksum != expected_checksum {
            return Err(DbError::corruption(format!(
                "database header checksum mismatch on page {}",
                HEADER_PAGE_ID
            )));
        }

        Ok(header)
    }

    pub(crate) fn decode_loose(bytes: &[u8; DB_HEADER_SIZE]) -> Result<Self> {
        let magic = read_array::<16>(bytes, 0);
        if magic != DB_MAGIC {
            return Err(DbError::corruption(format!(
                "invalid database header magic on page {}",
                HEADER_PAGE_ID
            )));
        }

        let format_version = read_u32(bytes, FORMAT_VERSION_OFFSET);

        let page_size = read_u32(bytes, PAGE_SIZE_OFFSET);
        if !page::is_supported_page_size(page_size) {
            return Err(DbError::corruption(format!(
                "invalid database page size {} on page {}",
                page_size, HEADER_PAGE_ID
            )));
        }

        let stored_checksum = read_u32(bytes, CHECKSUM_OFFSET);

        Ok(Self {
            magic,
            format_version,
            page_size,
            header_checksum: stored_checksum,
            schema_cookie: read_u32(bytes, SCHEMA_COOKIE_OFFSET),
            catalog_root_page_id: read_u32(bytes, CATALOG_ROOT_OFFSET),
            freelist: FreelistState {
                root_page_id: read_u32(bytes, FREELIST_ROOT_OFFSET),
                head_page_id: read_u32(bytes, FREELIST_HEAD_OFFSET),
                page_count: read_u32(bytes, FREELIST_COUNT_OFFSET),
            },
            last_checkpoint_lsn: read_u64(bytes, LAST_CHECKPOINT_LSN_OFFSET),
            reserved: read_array::<RESERVED_LEN>(bytes, RESERVED_OFFSET),
        })
    }

    #[must_use]
    fn compute_checksum(&self) -> u32 {
        let bytes = self.encode_with_checksum(0);
        checksum::crc32c_parts(&[&bytes[0..CHECKSUM_OFFSET], &bytes[CHECKSUM_OFFSET + 4..]])
    }

    #[must_use]
    fn encode_with_checksum(&self, header_checksum: u32) -> [u8; DB_HEADER_SIZE] {
        let mut bytes = [0_u8; DB_HEADER_SIZE];
        bytes[0..DB_MAGIC.len()].copy_from_slice(&self.magic);
        write_u32(&mut bytes, FORMAT_VERSION_OFFSET, self.format_version);
        write_u32(&mut bytes, PAGE_SIZE_OFFSET, self.page_size);
        write_u32(&mut bytes, CHECKSUM_OFFSET, header_checksum);
        write_u32(&mut bytes, SCHEMA_COOKIE_OFFSET, self.schema_cookie);
        write_u32(&mut bytes, CATALOG_ROOT_OFFSET, self.catalog_root_page_id);
        write_u32(&mut bytes, FREELIST_ROOT_OFFSET, self.freelist.root_page_id);
        write_u32(&mut bytes, FREELIST_HEAD_OFFSET, self.freelist.head_page_id);
        write_u32(&mut bytes, FREELIST_COUNT_OFFSET, self.freelist.page_count);
        write_u64(
            &mut bytes,
            LAST_CHECKPOINT_LSN_OFFSET,
            self.last_checkpoint_lsn,
        );
        bytes[RESERVED_OFFSET..RESERVED_OFFSET + RESERVED_LEN].copy_from_slice(&self.reserved);
        bytes
    }
}

pub(crate) fn write_database_bootstrap_vfs(
    file: &dyn VfsFile,
    header: &DatabaseHeader,
) -> Result<()> {
    let page_size = header.page_size as usize;
    let mut first_page = page::zeroed_page(header.page_size);
    first_page[..DB_HEADER_SIZE].copy_from_slice(&header.encode());
    let second_page = page::zeroed_page(header.page_size);

    write_all_at(file, 0, &first_page)?;
    write_all_at(file, page_size as u64, &second_page)?;
    file.set_len((page_size * 2) as u64)?;
    Ok(())
}

pub(crate) fn read_database_header_vfs(file: &dyn VfsFile) -> Result<DatabaseHeader> {
    let mut bytes = [0_u8; DB_HEADER_SIZE];
    read_exact_at(file, 0, &mut bytes)?;
    DatabaseHeader::decode(&bytes)
}

pub(crate) fn read_database_header_vfs_loose(file: &dyn VfsFile) -> Result<DatabaseHeader> {
    let mut bytes = [0_u8; DB_HEADER_SIZE];
    read_exact_at(file, 0, &mut bytes)?;
    DatabaseHeader::decode_loose(&bytes)
}

fn read_u32(bytes: &[u8; DB_HEADER_SIZE], offset: usize) -> u32 {
    let mut raw = [0_u8; 4];
    raw.copy_from_slice(&bytes[offset..offset + 4]);
    u32::from_le_bytes(raw)
}

fn read_u64(bytes: &[u8; DB_HEADER_SIZE], offset: usize) -> u64 {
    let mut raw = [0_u8; 8];
    raw.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(raw)
}

fn read_array<const N: usize>(bytes: &[u8; DB_HEADER_SIZE], offset: usize) -> [u8; N] {
    let mut raw = [0_u8; N];
    raw.copy_from_slice(&bytes[offset..offset + N]);
    raw
}

fn write_u32(bytes: &mut [u8; DB_HEADER_SIZE], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_u64(bytes: &mut [u8; DB_HEADER_SIZE], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::{DatabaseHeader, DB_FORMAT_VERSION, DB_HEADER_SIZE};
    use crate::error::DbError;
    use crate::storage::page;

    #[test]
    fn header_roundtrip_preserves_fields() {
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        let encoded = header.encode();
        let decoded = DatabaseHeader::decode(&encoded).expect("header should decode");

        assert_eq!(decoded, header);
        assert_eq!(decoded.catalog_root_page_id, super::CATALOG_ROOT_PAGE_ID);
        assert_eq!(decoded.freelist.page_count, 0);
    }

    #[test]
    fn invalid_magic_is_reported_as_corruption() {
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        let mut encoded = header.encode();
        encoded[0] = b'X';

        assert!(matches!(
            DatabaseHeader::decode(&encoded),
            Err(DbError::Corruption { .. })
        ));
    }

    #[test]
    fn invalid_format_version_is_reported() {
        let mut header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        header.format_version = DB_FORMAT_VERSION + 1;
        let encoded = header.encode();

        assert!(matches!(
            DatabaseHeader::decode(&encoded),
            Err(DbError::UnsupportedFormatVersion { .. })
        ));
    }

    #[test]
    fn invalid_page_size_is_reported_as_corruption() {
        let mut header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        header.page_size = 2048;
        let encoded = header.encode();

        assert!(matches!(
            DatabaseHeader::decode(&encoded),
            Err(DbError::Corruption { .. })
        ));
    }

    #[test]
    fn checksum_mismatch_is_reported_as_corruption() {
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        let mut encoded = header.encode();
        encoded[24] ^= 0xFF;

        assert!(matches!(
            DatabaseHeader::decode(&encoded),
            Err(DbError::Corruption { .. })
        ));
    }

    #[test]
    fn encoded_header_is_exactly_128_bytes() {
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        let encoded = header.encode();

        assert_eq!(encoded.len(), DB_HEADER_SIZE);
    }
}

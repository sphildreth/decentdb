//! Overflow-page storage for large record payloads.
//!
//! Implements:
//! - design/adr/0020-overflow-pages-for-blobs.md
//! - design/adr/0031-overflow-page-format.md

use crate::error::{DbError, Result};
use crate::record::compression::{decompress, maybe_compress, CompressionMode};
use crate::storage::page::{PageId, PageStore};

const OVERFLOW_HEADER_SIZE: usize = 8;
const FLAG_COMPRESSED: u8 = 0x01;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OverflowPointer {
    pub(crate) head_page_id: PageId,
    pub(crate) logical_len: u32,
    pub(crate) flags: u8,
}

impl OverflowPointer {
    #[must_use]
    pub(crate) fn is_compressed(self) -> bool {
        self.flags & FLAG_COMPRESSED != 0
    }
}

pub(crate) fn write_overflow<S: PageStore>(
    store: &mut S,
    bytes: &[u8],
    compression: CompressionMode,
) -> Result<OverflowPointer> {
    let compressed = maybe_compress(bytes, compression);
    let flags = if compressed.compressed {
        FLAG_COMPRESSED
    } else {
        0
    };
    let head_page_id = write_chain(store, &compressed.bytes)?;
    Ok(OverflowPointer {
        head_page_id,
        logical_len: u32::try_from(bytes.len())
            .map_err(|_| DbError::constraint("overflow payload exceeds u32 logical length"))?,
        flags,
    })
}

pub(crate) fn rewrite_overflow<S: PageStore>(
    store: &mut S,
    previous: OverflowPointer,
    bytes: &[u8],
    compression: CompressionMode,
) -> Result<OverflowPointer> {
    let compressed = maybe_compress(bytes, compression);
    let flags = if compressed.compressed {
        FLAG_COMPRESSED
    } else {
        0
    };
    let logical_len = u32::try_from(bytes.len())
        .map_err(|_| DbError::constraint("overflow payload exceeds u32 logical length"))?;

    if compressed.bytes.is_empty() {
        if previous.head_page_id != 0 {
            free_overflow(store, previous.head_page_id)?;
        }
        return Ok(OverflowPointer {
            head_page_id: 0,
            logical_len,
            flags,
        });
    }

    if previous.head_page_id == 0 {
        let head_page_id = write_chain(store, &compressed.bytes)?;
        return Ok(OverflowPointer {
            head_page_id,
            logical_len,
            flags,
        });
    }

    let page_size = store.page_size() as usize;
    if page_size <= OVERFLOW_HEADER_SIZE {
        return Err(DbError::internal("page size too small for overflow pages"));
    }
    let chunk_capacity = page_size - OVERFLOW_HEADER_SIZE;
    let needed_pages = compressed.bytes.len().div_ceil(chunk_capacity);
    let mut page_ids = collect_chain_page_ids(store, previous.head_page_id)?;
    if page_ids.len() < needed_pages {
        for _ in 0..(needed_pages - page_ids.len()) {
            page_ids.push(store.allocate_page()?);
        }
    }
    let head_page_id = page_ids[0];
    write_chain_pages(
        store,
        &page_ids[..needed_pages],
        &compressed.bytes,
        page_size,
        chunk_capacity,
        true,
    )?;
    for page_id in page_ids.into_iter().skip(needed_pages) {
        store.free_page(page_id)?;
    }

    Ok(OverflowPointer {
        head_page_id,
        logical_len,
        flags,
    })
}

pub(crate) fn read_overflow<S: PageStore>(store: &S, pointer: OverflowPointer) -> Result<Vec<u8>> {
    let bytes = read_chain(store, pointer.head_page_id)?;
    let decoded = if pointer.is_compressed() {
        decompress(&bytes)?
    } else {
        bytes
    };

    if decoded.len() != pointer.logical_len as usize {
        return Err(DbError::corruption(format!(
            "overflow payload length mismatch: expected {}, decoded {}",
            pointer.logical_len,
            decoded.len()
        )));
    }
    Ok(decoded)
}

pub(crate) fn read_chain<S: PageStore>(store: &S, head_page_id: PageId) -> Result<Vec<u8>> {
    let mut page_id = head_page_id;
    let mut output = Vec::new();

    while page_id != 0 {
        let page = store.read_page(page_id)?;
        if page.len() < OVERFLOW_HEADER_SIZE {
            return Err(DbError::corruption("overflow page shorter than header"));
        }
        let next_page_id = u32::from_le_bytes(page[0..4].try_into().expect("header next page"));
        let chunk_len = u32::from_le_bytes(page[4..8].try_into().expect("header chunk len"));
        let chunk_end = OVERFLOW_HEADER_SIZE + chunk_len as usize;
        if chunk_end > page.len() {
            return Err(DbError::corruption(
                "overflow chunk length exceeds page payload",
            ));
        }
        output.extend_from_slice(&page[OVERFLOW_HEADER_SIZE..chunk_end]);
        page_id = next_page_id;
    }

    Ok(output)
}

pub(crate) fn free_overflow<S: PageStore>(
    store: &mut S,
    head_page_id: PageId,
) -> Result<Vec<PageId>> {
    let mut page_id = head_page_id;
    let mut freed = Vec::new();

    while page_id != 0 {
        let page = store.read_page(page_id)?;
        if page.len() < OVERFLOW_HEADER_SIZE {
            return Err(DbError::corruption("overflow page shorter than header"));
        }
        let next_page_id = u32::from_le_bytes(page[0..4].try_into().expect("header next page"));
        store.free_page(page_id)?;
        freed.push(page_id);
        page_id = next_page_id;
    }

    Ok(freed)
}

fn write_chain<S: PageStore>(store: &mut S, bytes: &[u8]) -> Result<PageId> {
    if bytes.is_empty() {
        return Ok(0);
    }

    let page_size = store.page_size() as usize;
    if page_size <= OVERFLOW_HEADER_SIZE {
        return Err(DbError::internal("page size too small for overflow pages"));
    }
    let chunk_capacity = page_size - OVERFLOW_HEADER_SIZE;
    let chunk_count = bytes.len().div_ceil(chunk_capacity);

    let mut page_ids = Vec::with_capacity(chunk_count);
    for _ in 0..chunk_count {
        page_ids.push(store.allocate_page()?);
    }
    write_chain_pages(store, &page_ids, bytes, page_size, chunk_capacity, false)?;

    Ok(page_ids[0])
}

fn collect_chain_page_ids<S: PageStore>(store: &S, head_page_id: PageId) -> Result<Vec<PageId>> {
    let mut page_id = head_page_id;
    let mut page_ids = Vec::new();

    while page_id != 0 {
        let page = store.read_page(page_id)?;
        if page.len() < OVERFLOW_HEADER_SIZE {
            return Err(DbError::corruption("overflow page shorter than header"));
        }
        page_ids.push(page_id);
        page_id = u32::from_le_bytes(page[0..4].try_into().expect("header next page"));
    }

    Ok(page_ids)
}

fn write_chain_pages<S: PageStore>(
    store: &mut S,
    page_ids: &[PageId],
    bytes: &[u8],
    page_size: usize,
    chunk_capacity: usize,
    skip_unchanged: bool,
) -> Result<()> {
    let mut offset = 0_usize;
    for (index, page_id) in page_ids.iter().copied().enumerate() {
        let next = page_ids.get(index + 1).copied().unwrap_or(0);
        let remaining = bytes.len().saturating_sub(offset);
        let chunk_len = remaining.min(chunk_capacity);
        let mut page = vec![0_u8; page_size];
        page[0..4].copy_from_slice(&next.to_le_bytes());
        page[4..8].copy_from_slice(
            &(u32::try_from(chunk_len)
                .map_err(|_| DbError::constraint("overflow chunk length exceeds u32"))?)
            .to_le_bytes(),
        );
        if chunk_len > 0 {
            let end = offset + chunk_len;
            page[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + chunk_len]
                .copy_from_slice(&bytes[offset..end]);
            offset = end;
        }
        if skip_unchanged {
            let existing = store.read_page(page_id)?;
            if existing == page {
                continue;
            }
        }
        store.write_page(page_id, &page)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::record::compression::CompressionMode;
    use crate::storage::page::InMemoryPageStore;

    use super::{free_overflow, read_overflow, rewrite_overflow, write_overflow};

    #[test]
    fn overflow_roundtrip_and_release_are_deterministic() {
        let mut store = InMemoryPageStore::default();
        let payload = b"large-text".repeat(900);
        let pointer = write_overflow(&mut store, &payload, CompressionMode::Auto).expect("write");
        assert!(store.contains_page(pointer.head_page_id));

        let decoded = read_overflow(&store, pointer).expect("read");
        assert_eq!(decoded, payload);

        let freed = free_overflow(&mut store, pointer.head_page_id).expect("free");
        assert!(!freed.is_empty());
        for page_id in freed {
            assert!(!store.contains_page(page_id));
        }
    }

    #[test]
    fn rewrite_overflow_reuses_chain_head_when_possible() {
        let mut store = InMemoryPageStore::default();
        let payload = (0_u32..20_000_u32)
            .map(|value| (value % 251) as u8)
            .collect::<Vec<_>>();
        let pointer = write_overflow(&mut store, &payload, CompressionMode::Never).expect("write");
        let initial_allocated = store.allocated_page_count();

        let updated = (0_u32..18_000_u32)
            .map(|value| (value % 239) as u8)
            .collect::<Vec<_>>();
        let rewritten = rewrite_overflow(&mut store, pointer, &updated, CompressionMode::Never)
            .expect("rewrite");
        assert_eq!(rewritten.head_page_id, pointer.head_page_id);
        assert_eq!(read_overflow(&store, rewritten).expect("read rewrite"), updated);

        let shrink = vec![42_u8; 1_024];
        let shrunk =
            rewrite_overflow(&mut store, rewritten, &shrink, CompressionMode::Never).expect("shrink");
        assert_eq!(shrunk.head_page_id, pointer.head_page_id);
        assert_eq!(read_overflow(&store, shrunk).expect("read shrink"), shrink);
        assert!(store.allocated_page_count() < initial_allocated);
    }
}

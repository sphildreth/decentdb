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

    let mut chunks = bytes
        .chunks(chunk_capacity)
        .map(|chunk| chunk.to_vec())
        .collect::<Vec<_>>();
    let mut page_ids = Vec::with_capacity(chunks.len());
    for _ in 0..chunks.len() {
        page_ids.push(store.allocate_page()?);
    }

    for (index, chunk) in chunks.drain(..).enumerate() {
        let next = page_ids.get(index + 1).copied().unwrap_or(0);
        let mut page = vec![0_u8; page_size];
        page[0..4].copy_from_slice(&next.to_le_bytes());
        page[4..8].copy_from_slice(
            &(u32::try_from(chunk.len())
                .map_err(|_| DbError::constraint("overflow chunk length exceeds u32"))?)
            .to_le_bytes(),
        );
        page[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + chunk.len()].copy_from_slice(&chunk);
        store.write_page(page_ids[index], &page)?;
    }

    Ok(page_ids[0])
}

#[cfg(test)]
mod tests {
    use crate::record::compression::CompressionMode;
    use crate::storage::page::InMemoryPageStore;

    use super::{free_overflow, read_overflow, write_overflow};

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
}

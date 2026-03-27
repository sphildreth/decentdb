//! Overflow-page storage for large record payloads.
//!
//! Implements:
//! - design/adr/0020-overflow-pages-for-blobs.md
//! - design/adr/0031-overflow-page-format.md

use crate::error::{DbError, Result};
use crate::record::compression::{decompress, maybe_compress, CompressionMode};
use crate::storage::checksum::crc32c_parts;
use crate::storage::page::{PageId, PageStore};

pub(crate) const OVERFLOW_HEADER_SIZE: usize = 8;
const FLAG_COMPRESSED: u8 = 0x01;

/// Cached page IDs for an overflow chain.  Used by
/// [`rewrite_overflow_cached`] to skip the chain walk and compare pages
/// lazily via memcmp instead of reading the entire chain upfront.
#[derive(Clone, Debug, Default)]
pub(crate) struct OverflowChainCache {
    pub(crate) page_ids: Vec<PageId>,
}

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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct OverflowTailInfo {
    pub(crate) page_id: PageId,
    pub(crate) chunk_len: usize,
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
    let existing_chain_pages = collect_chain_pages(store, previous.head_page_id)?;
    let mut page_ids = existing_chain_pages
        .iter()
        .map(|(page_id, _)| *page_id)
        .collect::<Vec<_>>();
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
        Some(&existing_chain_pages),
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

/// Like [`rewrite_overflow`] but uses a cached chain of page IDs and per-page
/// Rewrite an overflow chain using cached page IDs, comparing pages lazily
/// via vectorized memcmp instead of hashing.  Returns the updated pointer
/// together with a fresh [`OverflowChainCache`] for the next call.
///
/// Callers must pass `bytes` that are **uncompressed** (this function always
/// stores uncompressed data and sets flags to 0).
pub(crate) fn rewrite_overflow_cached<S: PageStore>(
    store: &mut S,
    previous: OverflowPointer,
    bytes: &[u8],
    cached_page_ids: &[PageId],
    skip_first_n_pages: usize,
) -> Result<(OverflowPointer, OverflowChainCache, OverflowTailInfo)> {
    let logical_len = u32::try_from(bytes.len())
        .map_err(|_| DbError::constraint("overflow payload exceeds u32 logical length"))?;

    if bytes.is_empty() {
        if previous.head_page_id != 0 {
            free_overflow(store, previous.head_page_id)?;
        }
        return Ok((
            OverflowPointer {
                head_page_id: 0,
                logical_len,
                flags: 0,
            },
            OverflowChainCache::default(),
            OverflowTailInfo::default(),
        ));
    }

    let page_size = store.page_size() as usize;
    if page_size <= OVERFLOW_HEADER_SIZE {
        return Err(DbError::internal("page size too small for overflow pages"));
    }
    let chunk_capacity = page_size - OVERFLOW_HEADER_SIZE;
    let needed_pages = bytes.len().div_ceil(chunk_capacity);

    // Reuse cached page IDs; allocate extras only if the payload grew.
    let mut page_ids = Vec::with_capacity(needed_pages);
    for i in 0..needed_pages {
        if i < cached_page_ids.len() {
            page_ids.push(cached_page_ids[i]);
        } else {
            page_ids.push(store.allocate_page()?);
        }
    }

    // Determine how many leading pages can be safely skipped.
    // Pages are skippable when: same page count as before (no chain
    // restructure), the page was in the previous cache, and its byte
    // range falls entirely within the unchanged prefix.
    let effective_skip = if needed_pages == cached_page_ids.len() {
        skip_first_n_pages.min(needed_pages)
    } else {
        // Chain length changed — next pointers may differ; rewrite all.
        0
    };

    // Single reusable buffer avoids per-page allocation.
    let mut page_buf = vec![0u8; page_size];
    let mut offset = effective_skip * chunk_capacity;

    for (index, &page_id) in page_ids.iter().enumerate() {
        if index < effective_skip {
            continue;
        }

        let next = page_ids.get(index + 1).copied().unwrap_or(0);
        let remaining = bytes.len().saturating_sub(offset);
        let chunk_len = remaining.min(chunk_capacity);

        page_buf.fill(0);
        page_buf[0..4].copy_from_slice(&next.to_le_bytes());
        page_buf[4..8].copy_from_slice(
            &u32::try_from(chunk_len)
                .map_err(|_| DbError::constraint("overflow chunk length exceeds u32"))?
                .to_le_bytes(),
        );
        if chunk_len > 0 {
            page_buf[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + chunk_len]
                .copy_from_slice(&bytes[offset..offset + chunk_len]);
            offset += chunk_len;
        }

        // Lazy comparison: read the existing page from the page cache
        // (vectorized memcmp is ~4× faster than byte-by-byte CRC-32C).
        if index < cached_page_ids.len() {
            if let Ok(existing) = store.read_page(page_id) {
                if existing.as_slice() == page_buf.as_slice() {
                    continue;
                }
            }
        }

        store.write_page(page_id, &page_buf)?;
    }

    // Free excess pages if the payload shrank.
    for &old_page_id in cached_page_ids.iter().skip(needed_pages) {
        store.free_page(old_page_id)?;
    }

    // Compute tail info from the chain structure.
    let last_page_id = page_ids[needed_pages - 1];
    let total_full_pages = needed_pages.saturating_sub(1);
    let last_chunk_len = bytes
        .len()
        .saturating_sub(total_full_pages * chunk_capacity);
    let tail = OverflowTailInfo {
        page_id: last_page_id,
        chunk_len: last_chunk_len,
    };

    Ok((
        OverflowPointer {
            head_page_id: page_ids[0],
            logical_len,
            flags: 0,
        },
        OverflowChainCache { page_ids },
        tail,
    ))
}

/// Build an [`OverflowChainCache`] by collecting page IDs and hashing each
/// Collect page IDs for an existing overflow chain to seed an
/// [`OverflowChainCache`].  Only walks the chain to extract IDs — no
/// hashing is performed since [`rewrite_overflow_cached`] uses lazy
/// memcmp for comparison.
pub(crate) fn build_overflow_chain_cache<S: PageStore>(
    store: &S,
    head_page_id: PageId,
) -> Result<OverflowChainCache> {
    if head_page_id == 0 {
        return Ok(OverflowChainCache::default());
    }
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
    Ok(OverflowChainCache { page_ids })
}

pub(crate) fn read_overflow_prefix<S: PageStore>(
    store: &S,
    pointer: OverflowPointer,
    prefix_len: usize,
) -> Result<Option<Vec<u8>>> {
    if pointer.head_page_id == 0 || pointer.is_compressed() || prefix_len == 0 {
        return Ok(None);
    }

    let mut page_id = pointer.head_page_id;
    let mut prefix = Vec::with_capacity(prefix_len.min(pointer.logical_len as usize));
    while page_id != 0 && prefix.len() < prefix_len {
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
        let remaining = prefix_len.saturating_sub(prefix.len());
        let take = remaining.min(chunk_len as usize);
        prefix.extend_from_slice(&page[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + take]);
        page_id = next_page_id;
    }

    Ok(Some(prefix))
}

pub(crate) fn read_uncompressed_overflow_tail<S: PageStore>(
    store: &S,
    pointer: OverflowPointer,
) -> Result<Option<OverflowTailInfo>> {
    if pointer.head_page_id == 0 || pointer.is_compressed() {
        return Ok(None);
    }

    let mut page_id = pointer.head_page_id;
    let mut logical_len = 0_usize;
    let mut tail = OverflowTailInfo::default();
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
        logical_len = logical_len.saturating_add(chunk_len as usize);
        tail = OverflowTailInfo {
            page_id,
            chunk_len: chunk_len as usize,
        };
        page_id = next_page_id;
    }

    if logical_len != pointer.logical_len as usize {
        return Err(DbError::corruption(format!(
            "overflow logical length mismatch: expected {}, read {}",
            pointer.logical_len, logical_len
        )));
    }

    Ok(Some(tail))
}

pub(crate) fn append_uncompressed_with_tail<S: PageStore>(
    store: &mut S,
    previous: OverflowPointer,
    previous_tail: OverflowTailInfo,
    appended: &[u8],
) -> Result<(OverflowPointer, OverflowTailInfo)> {
    if previous.head_page_id == 0 {
        return Err(DbError::internal(
            "append_uncompressed_with_tail requires an existing overflow chain",
        ));
    }
    if previous.is_compressed() {
        return Err(DbError::internal(
            "append_uncompressed_with_tail does not support compressed payloads",
        ));
    }
    if previous_tail.page_id == 0 {
        return Err(DbError::corruption(
            "append overflow tail page id is invalid",
        ));
    }

    let page_size = store.page_size() as usize;
    if page_size <= OVERFLOW_HEADER_SIZE {
        return Err(DbError::internal("page size too small for overflow pages"));
    }
    let chunk_capacity = page_size - OVERFLOW_HEADER_SIZE;

    let mut tail_page = store.read_page(previous_tail.page_id)?;
    if tail_page.len() < OVERFLOW_HEADER_SIZE {
        return Err(DbError::corruption("overflow page shorter than header"));
    }
    let tail_chunk_len = u32::from_le_bytes(tail_page[4..8].try_into().expect("header chunk len"));
    if tail_chunk_len as usize != previous_tail.chunk_len {
        return Err(DbError::corruption(
            "append overflow tail chunk length did not match cached state",
        ));
    }
    if tail_chunk_len as usize > chunk_capacity {
        return Err(DbError::corruption(
            "append overflow tail chunk length exceeds capacity",
        ));
    }

    let mut remaining = appended;
    let available = chunk_capacity.saturating_sub(tail_chunk_len as usize);
    let mut updated_tail_len = tail_chunk_len as usize;
    if available > 0 && !remaining.is_empty() {
        let take = available.min(remaining.len());
        let start = OVERFLOW_HEADER_SIZE + tail_chunk_len as usize;
        tail_page[start..start + take].copy_from_slice(&remaining[..take]);
        updated_tail_len += take;
        tail_page[4..8].copy_from_slice(
            &(u32::try_from(updated_tail_len)
                .map_err(|_| DbError::constraint("overflow chunk length exceeds u32"))?)
            .to_le_bytes(),
        );
        remaining = &remaining[take..];
    }

    let mut final_tail = OverflowTailInfo {
        page_id: previous_tail.page_id,
        chunk_len: updated_tail_len,
    };
    if !remaining.is_empty() {
        let mut new_page_ids = Vec::new();
        let mut new_pages = Vec::new();
        let mut source = remaining;
        while !source.is_empty() {
            let new_page_id = store.allocate_page()?;
            let take = source.len().min(chunk_capacity);
            let mut page = vec![0_u8; page_size];
            page[4..8].copy_from_slice(
                &(u32::try_from(take)
                    .map_err(|_| DbError::constraint("overflow chunk length exceeds u32"))?)
                .to_le_bytes(),
            );
            page[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + take]
                .copy_from_slice(&source[..take]);
            source = &source[take..];
            new_page_ids.push(new_page_id);
            new_pages.push(page);
            final_tail = OverflowTailInfo {
                page_id: new_page_id,
                chunk_len: take,
            };
        }

        tail_page[0..4].copy_from_slice(&new_page_ids[0].to_le_bytes());
        for index in 0..new_pages.len().saturating_sub(1) {
            new_pages[index][0..4].copy_from_slice(&new_page_ids[index + 1].to_le_bytes());
        }

        store.write_page(previous_tail.page_id, &tail_page)?;
        for (new_page_id, new_page) in new_page_ids.iter().copied().zip(new_pages.iter()) {
            store.write_page(new_page_id, new_page)?;
        }
    } else {
        store.write_page(previous_tail.page_id, &tail_page)?;
    }

    Ok((
        OverflowPointer {
            head_page_id: previous.head_page_id,
            logical_len: previous.logical_len.saturating_add(
                u32::try_from(appended.len()).map_err(|_| {
                    DbError::constraint("overflow payload exceeds u32 logical length")
                })?,
            ),
            flags: previous.flags,
        },
        final_tail,
    ))
}

pub(crate) fn append_uncompressed_with_first_page_patch<S: PageStore>(
    store: &mut S,
    previous: OverflowPointer,
    patch_offset: usize,
    patch_bytes: &[u8],
    appended: &[u8],
) -> Result<(OverflowPointer, u32)> {
    if previous.head_page_id == 0 {
        return Err(DbError::internal(
            "append_uncompressed_with_first_page_patch requires an existing overflow chain",
        ));
    }
    if previous.is_compressed() {
        return Err(DbError::internal(
            "append_uncompressed_with_first_page_patch does not support compressed payloads",
        ));
    }

    let page_size = store.page_size() as usize;
    if page_size <= OVERFLOW_HEADER_SIZE {
        return Err(DbError::internal("page size too small for overflow pages"));
    }
    let chunk_capacity = page_size - OVERFLOW_HEADER_SIZE;

    let mut page_ids = Vec::new();
    let mut pages = Vec::new();
    let mut chunk_lens = Vec::new();
    let mut page_id = previous.head_page_id;
    let mut logical_len = 0_usize;
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
        logical_len = logical_len.saturating_add(chunk_len as usize);
        page_ids.push(page_id);
        pages.push(page);
        chunk_lens.push(chunk_len as usize);
        page_id = next_page_id;
    }

    if logical_len != previous.logical_len as usize {
        return Err(DbError::corruption(format!(
            "overflow logical length mismatch: expected {}, read {}",
            previous.logical_len, logical_len
        )));
    }
    if page_ids.is_empty() {
        return Err(DbError::corruption(
            "overflow pointer referenced an empty page chain",
        ));
    }

    let first_chunk_len = chunk_lens[0];
    let patch_end = patch_offset.saturating_add(patch_bytes.len());
    if patch_end > first_chunk_len {
        return Err(DbError::corruption(
            "first-page patch exceeded available overflow payload bytes",
        ));
    }
    let patch_start = OVERFLOW_HEADER_SIZE + patch_offset;
    pages[0][patch_start..patch_start + patch_bytes.len()].copy_from_slice(patch_bytes);

    let mut remaining = appended;
    let tail_index = page_ids.len() - 1;
    if !remaining.is_empty() {
        let tail_chunk_len = chunk_lens[tail_index];
        let available = chunk_capacity.saturating_sub(tail_chunk_len);
        if available > 0 {
            let take = available.min(remaining.len());
            let start = OVERFLOW_HEADER_SIZE + tail_chunk_len;
            pages[tail_index][start..start + take].copy_from_slice(&remaining[..take]);
            chunk_lens[tail_index] += take;
            pages[tail_index][4..8].copy_from_slice(
                &(u32::try_from(chunk_lens[tail_index])
                    .map_err(|_| DbError::constraint("overflow chunk length exceeds u32"))?)
                .to_le_bytes(),
            );
            remaining = &remaining[take..];
        }
    }

    let mut new_page_ids = Vec::new();
    let mut new_pages = Vec::new();
    if !remaining.is_empty() {
        let mut source = remaining;
        while !source.is_empty() {
            let new_page_id = store.allocate_page()?;
            let take = source.len().min(chunk_capacity);
            let mut page = vec![0_u8; page_size];
            page[4..8].copy_from_slice(
                &(u32::try_from(take)
                    .map_err(|_| DbError::constraint("overflow chunk length exceeds u32"))?)
                .to_le_bytes(),
            );
            page[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + take]
                .copy_from_slice(&source[..take]);
            source = &source[take..];
            new_page_ids.push(new_page_id);
            new_pages.push(page);
        }

        pages[tail_index][0..4].copy_from_slice(&new_page_ids[0].to_le_bytes());
        for index in 0..new_pages.len().saturating_sub(1) {
            new_pages[index][0..4].copy_from_slice(&new_page_ids[index + 1].to_le_bytes());
        }
    }

    let mut checksum_parts = Vec::with_capacity(pages.len() + new_pages.len());
    for (page, chunk_len) in pages.iter().zip(chunk_lens.iter().copied()) {
        checksum_parts.push(&page[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + chunk_len]);
    }
    for page in &new_pages {
        let chunk_len =
            u32::from_le_bytes(page[4..8].try_into().expect("header chunk len")) as usize;
        checksum_parts.push(&page[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + chunk_len]);
    }
    let checksum = crc32c_parts(&checksum_parts);

    if tail_index == 0 {
        store.write_page(page_ids[0], &pages[0])?;
    } else {
        store.write_page(page_ids[0], &pages[0])?;
        store.write_page(page_ids[tail_index], &pages[tail_index])?;
    }
    for (new_page_id, new_page) in new_page_ids.iter().copied().zip(new_pages.iter()) {
        store.write_page(new_page_id, new_page)?;
    }

    Ok((
        OverflowPointer {
            head_page_id: previous.head_page_id,
            logical_len: previous.logical_len.saturating_add(
                u32::try_from(appended.len()).map_err(|_| {
                    DbError::constraint("overflow payload exceeds u32 logical length")
                })?,
            ),
            flags: previous.flags,
        },
        checksum,
    ))
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
    write_chain_pages(
        store,
        &page_ids,
        bytes,
        page_size,
        chunk_capacity,
        false,
        None,
    )?;

    Ok(page_ids[0])
}

fn collect_chain_pages<S: PageStore>(
    store: &S,
    head_page_id: PageId,
) -> Result<Vec<(PageId, Vec<u8>)>> {
    let mut page_id = head_page_id;
    let mut pages = Vec::new();

    while page_id != 0 {
        let page = store.read_page(page_id)?;
        if page.len() < OVERFLOW_HEADER_SIZE {
            return Err(DbError::corruption("overflow page shorter than header"));
        }
        pages.push((page_id, page.clone()));
        page_id = u32::from_le_bytes(page[0..4].try_into().expect("header next page"));
    }

    Ok(pages)
}

fn write_chain_pages<S: PageStore>(
    store: &mut S,
    page_ids: &[PageId],
    bytes: &[u8],
    page_size: usize,
    chunk_capacity: usize,
    skip_unchanged: bool,
    existing_pages: Option<&[(PageId, Vec<u8>)]>,
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
            let unchanged = if let Some(existing_pages) = existing_pages {
                if let Some((existing_page_id, existing_page)) = existing_pages.get(index) {
                    if *existing_page_id == page_id {
                        existing_page.as_slice() == page.as_slice()
                    } else {
                        store.read_page(page_id)? == page
                    }
                } else {
                    false
                }
            } else {
                store.read_page(page_id)? == page
            };
            if unchanged {
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

    use super::{
        append_uncompressed_with_first_page_patch, free_overflow, read_overflow,
        read_overflow_prefix, rewrite_overflow, write_overflow,
    };

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
        assert_eq!(
            read_overflow(&store, rewritten).expect("read rewrite"),
            updated
        );

        let shrink = vec![42_u8; 1_024];
        let shrunk = rewrite_overflow(&mut store, rewritten, &shrink, CompressionMode::Never)
            .expect("shrink");
        assert_eq!(shrunk.head_page_id, pointer.head_page_id);
        assert_eq!(read_overflow(&store, shrunk).expect("read shrink"), shrink);
        assert!(store.allocated_page_count() < initial_allocated);
    }

    #[test]
    fn append_uncompressed_with_patch_updates_first_page_and_tail() {
        let mut store = InMemoryPageStore::default();
        let mut payload = b"DDBTBL01".to_vec();
        payload.extend_from_slice(&1_u32.to_le_bytes());
        payload.extend_from_slice(b"seed-row");
        let pointer =
            write_overflow(&mut store, &payload, CompressionMode::Never).expect("write payload");

        let prefix = read_overflow_prefix(&store, pointer, 12)
            .expect("read prefix")
            .expect("uncompressed prefix");
        assert_eq!(&prefix[..8], b"DDBTBL01");
        assert_eq!(
            u32::from_le_bytes(prefix[8..12].try_into().expect("count")),
            1
        );

        let (updated, checksum) = append_uncompressed_with_first_page_patch(
            &mut store,
            pointer,
            8,
            &2_u32.to_le_bytes(),
            b"tail-row",
        )
        .expect("append and patch");
        let decoded = read_overflow(&store, updated).expect("read updated payload");
        assert_eq!(&decoded[..8], b"DDBTBL01");
        assert_eq!(
            u32::from_le_bytes(decoded[8..12].try_into().expect("count")),
            2
        );
        assert!(decoded.ends_with(b"tail-row"));
        assert_eq!(
            crate::storage::checksum::crc32c_parts(&[decoded.as_slice()]),
            checksum
        );
    }

    // ── Compression mode tests ──────────────────────────────────────

    #[test]
    fn overflow_never_compression() {
        let mut store = InMemoryPageStore::default();
        let payload = vec![0_u8; 10_000]; // highly compressible
        let pointer = write_overflow(&mut store, &payload, CompressionMode::Never).expect("write");
        assert!(!pointer.is_compressed());
        let decoded = read_overflow(&store, pointer).expect("read");
        assert_eq!(decoded, payload);
    }

    #[test]
    fn overflow_auto_compression_highly_compressible() {
        let mut store = InMemoryPageStore::default();
        let payload = vec![0_u8; 10_000]; // highly compressible
        let pointer = write_overflow(&mut store, &payload, CompressionMode::Auto).expect("write");
        // Auto may or may not compress; just verify roundtrip
        let decoded = read_overflow(&store, pointer).expect("read");
        assert_eq!(decoded, payload);
    }

    #[test]
    fn overflow_auto_compression_large_compressible() {
        let mut store = InMemoryPageStore::default();
        let payload = vec![42_u8; 20_000]; // large and compressible
        let pointer = write_overflow(&mut store, &payload, CompressionMode::Auto).expect("write");
        // Auto should compress this
        let decoded = read_overflow(&store, pointer).expect("read");
        assert_eq!(decoded, payload);
    }

    #[test]
    fn overflow_auto_compression_random_data() {
        let mut store = InMemoryPageStore::default();
        // Random-ish data that doesn't compress well
        let payload: Vec<u8> = (0..10_000_u32)
            .map(|i| ((i * 7 + 13) % 256) as u8)
            .collect();
        let pointer = write_overflow(&mut store, &payload, CompressionMode::Auto).expect("write");
        let decoded = read_overflow(&store, pointer).expect("read");
        assert_eq!(decoded, payload);
    }

    // ── Prefix reading ──────────────────────────────────────────────

    #[test]
    fn read_prefix_zero_page_returns_none() {
        use super::OverflowPointer;
        let store = InMemoryPageStore::default();
        let pointer = OverflowPointer {
            head_page_id: 0,
            logical_len: 100,
            flags: 0,
        };
        assert!(read_overflow_prefix(&store, pointer, 10)
            .expect("ok")
            .is_none());
    }

    #[test]
    fn read_prefix_compressed_returns_none() {
        use super::{OverflowPointer, FLAG_COMPRESSED};
        // Simulate a compressed pointer (can't actually write one without Always mode)
        let store = InMemoryPageStore::default();
        let pointer = OverflowPointer {
            head_page_id: 1,
            logical_len: 100,
            flags: FLAG_COMPRESSED,
        };
        assert!(read_overflow_prefix(&store, pointer, 10)
            .expect("ok")
            .is_none());
    }

    #[test]
    fn read_prefix_zero_len_returns_none() {
        use super::OverflowPointer;
        let store = InMemoryPageStore::default();
        let pointer = OverflowPointer {
            head_page_id: 1,
            logical_len: 100,
            flags: 0,
        };
        assert!(read_overflow_prefix(&store, pointer, 0)
            .expect("ok")
            .is_none());
    }

    // ── Tail reading ────────────────────────────────────────────────

    #[test]
    fn read_tail_of_uncompressed_chain() {
        use super::read_uncompressed_overflow_tail;
        let mut store = InMemoryPageStore::default();
        let payload = b"hello world overflow tail test data".repeat(500);
        let pointer = write_overflow(&mut store, &payload, CompressionMode::Never).expect("write");
        let tail = read_uncompressed_overflow_tail(&store, pointer)
            .expect("read tail")
            .expect("should be some");
        assert!(tail.page_id > 0);
        assert!(tail.chunk_len > 0);
    }

    #[test]
    fn read_tail_compressed_returns_none() {
        use super::{read_uncompressed_overflow_tail, OverflowPointer, FLAG_COMPRESSED};
        let store = InMemoryPageStore::default();
        let pointer = OverflowPointer {
            head_page_id: 1,
            logical_len: 100,
            flags: FLAG_COMPRESSED,
        };
        assert!(read_uncompressed_overflow_tail(&store, pointer)
            .expect("ok")
            .is_none());
    }

    #[test]
    fn read_tail_zero_head_returns_none() {
        use super::{read_uncompressed_overflow_tail, OverflowPointer};
        let store = InMemoryPageStore::default();
        let pointer = OverflowPointer {
            head_page_id: 0,
            logical_len: 0,
            flags: 0,
        };
        assert!(read_uncompressed_overflow_tail(&store, pointer)
            .expect("ok")
            .is_none());
    }

    // ── Append with tail ────────────────────────────────────────────

    #[test]
    fn append_uncompressed_with_tail_extends_chain() {
        use super::{append_uncompressed_with_tail, read_uncompressed_overflow_tail};
        let mut store = InMemoryPageStore::default();
        let payload = b"initial data".to_vec();
        let pointer = write_overflow(&mut store, &payload, CompressionMode::Never).expect("write");
        let tail = read_uncompressed_overflow_tail(&store, pointer)
            .expect("read tail")
            .expect("some");
        let (updated, new_tail) =
            append_uncompressed_with_tail(&mut store, pointer, tail, b"appended").expect("append");
        let decoded = read_overflow(&store, updated).expect("read");
        assert!(decoded.starts_with(b"initial data"));
        assert!(decoded.ends_with(b"appended"));
        assert!(new_tail.page_id > 0);
    }

    // ── Rewrite with growth ─────────────────────────────────────────

    #[test]
    fn rewrite_overflow_with_growth() {
        let mut store = InMemoryPageStore::default();
        let small = vec![1_u8; 100];
        let pointer =
            write_overflow(&mut store, &small, CompressionMode::Never).expect("write small");
        let big = vec![2_u8; 50_000];
        let rewritten =
            rewrite_overflow(&mut store, pointer, &big, CompressionMode::Never).expect("rewrite");
        assert_eq!(rewritten.head_page_id, pointer.head_page_id);
        let decoded = read_overflow(&store, rewritten).expect("read");
        assert_eq!(decoded, big);
    }

    // ── Free overflow ───────────────────────────────────────────────

    #[test]
    fn free_overflow_multi_page_chain() {
        let mut store = InMemoryPageStore::default();
        let payload = vec![42_u8; 50_000];
        let pointer = write_overflow(&mut store, &payload, CompressionMode::Never).expect("write");
        let pages_before = store.allocated_page_count();
        assert!(pages_before > 1);
        let freed = free_overflow(&mut store, pointer.head_page_id).expect("free");
        assert_eq!(freed.len(), pages_before);
        assert_eq!(store.allocated_page_count(), 0);
    }

    // ── OverflowPointer tests ───────────────────────────────────────

    #[test]
    fn overflow_pointer_is_compressed_flag() {
        use super::{OverflowPointer, FLAG_COMPRESSED};
        let uncompressed = OverflowPointer {
            head_page_id: 1,
            logical_len: 100,
            flags: 0,
        };
        assert!(!uncompressed.is_compressed());
        let compressed = OverflowPointer {
            head_page_id: 1,
            logical_len: 100,
            flags: FLAG_COMPRESSED,
        };
        assert!(compressed.is_compressed());
    }
}

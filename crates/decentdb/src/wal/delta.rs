//! Fixed-size WAL delta payloads for small page edits.

use crate::error::{DbError, Result};

pub(crate) const DELTA_FRAME_PAYLOAD_SIZE: usize = 512;

const DELTA_PATCH_COUNT_SIZE: usize = 2;
const DELTA_PATCH_HEADER_SIZE: usize = 4;

#[allow(dead_code)]
pub(crate) fn encode_page_delta(base: &[u8], updated: &[u8]) -> Option<Vec<u8>> {
    let mut payload = Vec::new();
    if encode_page_delta_into(&mut payload, base, updated) {
        Some(payload.as_slice().to_vec())
    } else {
        None
    }
}

/// Reusable variant of `encode_page_delta` (slice M6). Reuses caller
/// storage so the writer does not allocate a fresh `Vec<u8>` per page on
/// the commit hot path. Returns `true` if a delta was produced (and
/// `payload` now contains it); `false` if the page is incompatible with
/// delta encoding (in which case `payload` is left empty and the writer
/// falls back to a full-page frame).
pub(crate) fn encode_page_delta_into(payload: &mut Vec<u8>, base: &[u8], updated: &[u8]) -> bool {
    payload.clear();
    if base.len() != updated.len() {
        return false;
    }
    payload.reserve(DELTA_FRAME_PAYLOAD_SIZE);
    payload.extend_from_slice(&0_u16.to_le_bytes());
    let mut patch_count = 0_u16;
    let mut index = 0_usize;
    while index < updated.len() {
        if base[index] == updated[index] {
            index += 1;
            continue;
        }

        let start = index;
        index += 1;
        while index < updated.len() && base[index] != updated[index] {
            index += 1;
        }
        let len = index - start;
        if payload.len() + DELTA_PATCH_HEADER_SIZE + len > DELTA_FRAME_PAYLOAD_SIZE {
            payload.clear();
            return false;
        }

        let Ok(offset) = u16::try_from(start) else {
            payload.clear();
            return false;
        };
        let Ok(patch_len) = u16::try_from(len) else {
            payload.clear();
            return false;
        };
        payload.extend_from_slice(&offset.to_le_bytes());
        payload.extend_from_slice(&patch_len.to_le_bytes());
        payload.extend_from_slice(&updated[start..index]);
        let Some(next) = patch_count.checked_add(1) else {
            payload.clear();
            return false;
        };
        patch_count = next;
    }

    if patch_count == 0 || payload.len() >= updated.len() {
        payload.clear();
        return false;
    }

    payload[..DELTA_PATCH_COUNT_SIZE].copy_from_slice(&patch_count.to_le_bytes());
    payload.resize(DELTA_FRAME_PAYLOAD_SIZE, 0);
    true
}

#[cfg(test)]
pub(crate) fn apply_page_delta(base: &[u8], payload: &[u8]) -> Result<Vec<u8>> {
    let mut page = base.to_vec();
    apply_page_delta_in_place(&mut page, payload)?;
    Ok(page)
}

pub(crate) fn apply_page_delta_in_place(page: &mut [u8], payload: &[u8]) -> Result<()> {
    if payload.len() != DELTA_FRAME_PAYLOAD_SIZE {
        return Err(DbError::corruption(format!(
            "invalid WAL delta payload length {}; expected {}",
            payload.len(),
            DELTA_FRAME_PAYLOAD_SIZE
        )));
    }

    let patch_count = u16::from_le_bytes(
        payload[..DELTA_PATCH_COUNT_SIZE]
            .try_into()
            .expect("delta patch count bytes"),
    ) as usize;

    let mut cursor = DELTA_PATCH_COUNT_SIZE;
    for _ in 0..patch_count {
        let (offset, len, next_cursor) = decode_delta_patch_header(payload, cursor)?;
        if next_cursor + len > payload.len() {
            return Err(DbError::corruption("WAL delta patch bytes overrun payload"));
        }
        if offset + len > page.len() {
            return Err(DbError::corruption("WAL delta patch writes past page end"));
        }
        cursor = next_cursor + len;
    }

    let mut cursor = DELTA_PATCH_COUNT_SIZE;
    for _ in 0..patch_count {
        let (offset, len, next_cursor) = decode_delta_patch_header(payload, cursor)?;
        let bytes_end = next_cursor + len;
        page[offset..offset + len].copy_from_slice(&payload[next_cursor..bytes_end]);
        cursor = bytes_end;
    }
    Ok(())
}

fn decode_delta_patch_header(payload: &[u8], cursor: usize) -> Result<(usize, usize, usize)> {
    if cursor + DELTA_PATCH_HEADER_SIZE > payload.len() {
        return Err(DbError::corruption(
            "WAL delta patch header overruns payload",
        ));
    }
    let offset = u16::from_le_bytes(
        payload[cursor..cursor + 2]
            .try_into()
            .expect("delta patch offset bytes"),
    ) as usize;
    let len = u16::from_le_bytes(
        payload[cursor + 2..cursor + 4]
            .try_into()
            .expect("delta patch length bytes"),
    ) as usize;
    Ok((offset, len, cursor + DELTA_PATCH_HEADER_SIZE))
}

#[cfg(test)]
mod tests {
    use super::{apply_page_delta, encode_page_delta, DELTA_FRAME_PAYLOAD_SIZE};

    #[test]
    fn encode_and_apply_page_delta_round_trip() {
        let mut base = vec![0_u8; 64];
        let mut updated = base.clone();
        updated[4..8].copy_from_slice(b"root");
        updated[32..39].copy_from_slice(b"payload");

        let delta = encode_page_delta(&base, &updated).expect("delta should encode");
        assert_eq!(delta.len(), DELTA_FRAME_PAYLOAD_SIZE);
        let rebuilt = apply_page_delta(&base, &delta).expect("apply delta");
        assert_eq!(rebuilt, updated);

        base[4..8].copy_from_slice(b"root");
        let second = encode_page_delta(&base, &updated).expect("smaller delta");
        let rebuilt = apply_page_delta(&base, &second).expect("apply second delta");
        assert_eq!(rebuilt, updated);
    }

    #[test]
    fn encode_page_delta_rejects_large_diff() {
        let base = vec![0_u8; 4096];
        let updated = vec![0xFF_u8; 4096];
        assert!(encode_page_delta(&base, &updated).is_none());
    }

    #[test]
    fn apply_page_delta_rejects_out_of_bounds_patch() {
        let base = vec![0_u8; 16];
        let mut delta = vec![0_u8; DELTA_FRAME_PAYLOAD_SIZE];
        delta[..2].copy_from_slice(&1_u16.to_le_bytes());
        delta[2..4].copy_from_slice(&15_u16.to_le_bytes());
        delta[4..6].copy_from_slice(&4_u16.to_le_bytes());
        let error = apply_page_delta(&base, &delta).expect_err("delta should fail");
        assert!(error.to_string().contains("page end"));
    }
}

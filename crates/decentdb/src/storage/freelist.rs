//! Freelist bootstrap metadata.

use crate::error::{DbError, Result};

use super::page::{self, PageId};

/// Freelist pointers stored in the fixed database header.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FreelistState {
    pub(crate) root_page_id: u32,
    pub(crate) head_page_id: u32,
    pub(crate) page_count: u32,
}

pub(crate) const FREELIST_NEXT_OFFSET: usize = 0;

#[must_use]
pub(crate) fn encode_freelist_page(page_size: u32, next_page_id: PageId) -> Vec<u8> {
    let mut page = page::zeroed_page(page_size);
    page[FREELIST_NEXT_OFFSET..FREELIST_NEXT_OFFSET + 4]
        .copy_from_slice(&next_page_id.to_le_bytes());
    page
}

pub(crate) fn decode_freelist_next(page: &[u8]) -> Result<PageId> {
    if page.len() < 4 {
        return Err(DbError::corruption(
            "freelist page is shorter than next-page pointer",
        ));
    }

    let mut raw = [0_u8; 4];
    raw.copy_from_slice(&page[FREELIST_NEXT_OFFSET..FREELIST_NEXT_OFFSET + 4]);
    Ok(u32::from_le_bytes(raw))
}

#[cfg(test)]
mod tests {
    use super::{decode_freelist_next, encode_freelist_page, FreelistState, FREELIST_NEXT_OFFSET};

    #[test]
    fn freelist_state_default_has_zero_values() {
        let state = FreelistState::default();
        assert_eq!(state.root_page_id, 0);
        assert_eq!(state.head_page_id, 0);
        assert_eq!(state.page_count, 0);
    }

    #[test]
    fn freelist_state_copy_and_debug() {
        let state = FreelistState {
            root_page_id: 1,
            head_page_id: 5,
            page_count: 10,
        };
        let copied = state;
        assert_eq!(copied.root_page_id, 1);
        assert_eq!(copied.head_page_id, 5);
        assert_eq!(copied.page_count, 10);
    }

    #[test]
    fn encode_freelist_page_produces_correct_size() {
        let page_size = 4096;
        let encoded = encode_freelist_page(page_size, 42);
        assert_eq!(encoded.len(), page_size as usize);
    }

    #[test]
    fn encode_freelist_page_stores_next_page_id_correctly() {
        let page_size = 4096;
        let test_page_ids = [0, 1, 42, 256, 1000, u32::MAX];

        for page_id in test_page_ids {
            let encoded = encode_freelist_page(page_size, page_id);
            let decoded = decode_freelist_next(&encoded).expect("decode should succeed");
            assert_eq!(decoded, page_id, "page ID mismatch for {page_id}");
        }
    }

    #[test]
    fn encode_freelist_page_zeroes_rest_of_page() {
        let page_size = 4096;
        let encoded = encode_freelist_page(page_size, 12345);

        // First 4 bytes should be the page ID
        assert_eq!(&encoded[0..4], &12345u32.to_le_bytes());

        // Rest should be zeros
        for byte in &encoded[4..] {
            assert_eq!(*byte, 0, "freelist page should be zeroed after header");
        }
    }

    #[test]
    fn decode_freelist_next_rejects_short_pages() {
        let short_pages: Vec<Vec<u8>> = vec![vec![], vec![0], vec![0, 0], vec![0, 0, 0]];

        for page in short_pages {
            assert!(
                decode_freelist_next(&page).is_err(),
                "should reject page with {} bytes",
                page.len()
            );
        }
    }

    #[test]
    fn decode_freelist_next_handles_boundary_cases() {
        // Exactly 4 bytes
        let page = vec![0x2A, 0x00, 0x00, 0x00];
        assert_eq!(decode_freelist_next(&page).unwrap(), 42);

        // More than 4 bytes (should only read first 4)
        let page = vec![0x2A, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF];
        assert_eq!(decode_freelist_next(&page).unwrap(), 42);
    }

    #[test]
    fn freelist_next_offset_is_zero() {
        // This is a compile-time constant, but test it for documentation
        assert_eq!(FREELIST_NEXT_OFFSET, 0);
    }

    #[test]
    fn encode_decode_roundtrip_various_page_sizes() {
        let page_sizes = [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536];
        let page_ids = [0, 1, 100, 1000, u32::MAX];

        for page_size in page_sizes {
            for page_id in page_ids {
                let encoded = encode_freelist_page(page_size, page_id);
                let decoded = decode_freelist_next(&encoded).unwrap();
                assert_eq!(
                    decoded, page_id,
                    "roundtrip failed for page_size={page_size}, page_id={page_id}"
                );
                assert_eq!(encoded.len(), page_size as usize);
            }
        }
    }
}

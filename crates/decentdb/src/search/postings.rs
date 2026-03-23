//! Delta-encoded postings-list serialization.

use crate::error::{DbError, Result};
use crate::record::{decode_varint_u64, encode_varint_u64};

pub(crate) fn encode_postings(row_ids: &[u64]) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    let mut previous = 0_u64;
    for &row_id in row_ids {
        if row_id < previous {
            return Err(DbError::constraint(
                "postings must be sorted in ascending row-id order",
            ));
        }
        let delta = row_id - previous;
        encoded.extend_from_slice(&encode_varint_u64(delta));
        previous = row_id;
    }
    Ok(encoded)
}

pub(crate) fn decode_postings(bytes: &[u8]) -> Result<Vec<u64>> {
    let mut offset = 0_usize;
    let mut previous = 0_u64;
    let mut decoded = Vec::new();

    while offset < bytes.len() {
        let (delta, used) = decode_varint_u64(&bytes[offset..])?;
        offset += used;
        previous = previous
            .checked_add(delta)
            .ok_or_else(|| DbError::corruption("postings delta overflow"))?;
        decoded.push(previous);
    }

    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::{decode_postings, encode_postings};

    #[test]
    fn postings_roundtrip_preserves_sorted_row_ids() {
        let row_ids = vec![1, 2, 8, 1024, 8_192];
        let encoded = encode_postings(&row_ids).expect("encode");
        let decoded = decode_postings(&encoded).expect("decode");
        assert_eq!(decoded, row_ids);
    }
}

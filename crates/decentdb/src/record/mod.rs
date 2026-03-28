#![allow(dead_code)]
//! Record and index-key encoding for DecentDB storage structures.
//!
//! Row encoding is intentionally separate from index-key encoding:
//! - row encoding optimizes for compact field serialization
//! - index-key encoding optimizes for stable comparison semantics

pub(crate) mod compression;
pub(crate) mod key;
pub(crate) mod overflow;
pub(crate) mod row;
pub(crate) mod value;

use crate::error::{DbError, Result};

#[must_use]
pub(crate) fn zigzag_encode_i64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

#[must_use]
pub(crate) fn zigzag_decode_u64(value: u64) -> i64 {
    ((value >> 1) as i64) ^ (-((value & 1) as i64))
}

#[must_use]
pub(crate) fn encode_varint_u64(value: u64) -> Vec<u8> {
    let mut output = Vec::new();
    encode_varint_u64_into(value, &mut output);
    output
}

pub(crate) fn encode_varint_u64_into(mut value: u64, output: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if value == 0 {
            break;
        }
    }
}

pub(crate) fn decode_varint_u64(input: &[u8]) -> Result<(u64, usize)> {
    let mut shift = 0_u32;
    let mut value = 0_u64;

    for (index, byte) in input.iter().copied().enumerate() {
        let payload = u64::from(byte & 0x7F);
        value |= payload << shift;
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
        shift += 7;
        if shift >= 64 {
            return Err(DbError::corruption("varint exceeds 64-bit range"));
        }
    }

    Err(DbError::corruption("truncated varint"))
}

#[cfg(test)]
mod tests {
    use super::{decode_varint_u64, encode_varint_u64, encode_varint_u64_into, zigzag_decode_u64, zigzag_encode_i64};

    #[test]
    fn zigzag_encode_decode_roundtrip() {
        let test_values = [
            0,
            1,
            -1,
            2,
            -2,
            i64::MIN,
            i64::MAX,
            100,
            -100,
            12345,
            -12345,
        ];

        for value in test_values {
            let encoded = zigzag_encode_i64(value);
            let decoded = zigzag_decode_u64(encoded);
            assert_eq!(decoded, value, "zigzag roundtrip failed for {value}");
        }
    }

    #[test]
    fn zigzag_encode_produces_expected_values() {
        assert_eq!(zigzag_encode_i64(0), 0);
        assert_eq!(zigzag_encode_i64(-1), 1);
        assert_eq!(zigzag_encode_i64(1), 2);
        assert_eq!(zigzag_encode_i64(-2), 3);
        assert_eq!(zigzag_encode_i64(2), 4);
    }

    #[test]
    fn varint_u64_encode_decode_roundtrip() {
        let test_values = [
            0,
            1,
            127,
            128,
            255,
            256,
            16383,
            16384,
            u64::MAX,
            100,
            1000,
            10000,
            100000,
            1_000_000,
            u64::MAX / 2,
        ];

        for value in test_values {
            let encoded = encode_varint_u64(value);
            let (decoded, bytes_read) = decode_varint_u64(&encoded).expect("decode should succeed");
            assert_eq!(decoded, value, "varint roundtrip failed for {value}");
            assert_eq!(bytes_read, encoded.len(), "bytes read should match encoded length");
        }
    }

    #[test]
    fn varint_u64_encoding_length_varies_by_magnitude() {
        assert_eq!(encode_varint_u64(0).len(), 1);
        assert_eq!(encode_varint_u64(127).len(), 1);
        assert_eq!(encode_varint_u64(128).len(), 2);
        assert_eq!(encode_varint_u64(16383).len(), 2);
        assert_eq!(encode_varint_u64(16384).len(), 3);
        assert_eq!(encode_varint_u64(u64::MAX).len(), 10);
    }

    #[test]
    fn varint_u64_decode_rejects_truncated_input() {
        assert!(decode_varint_u64(&[0x80]).is_err());
        assert!(decode_varint_u64(&[0x80, 0x80]).is_err());
        assert!(decode_varint_u64(&[0x80, 0x80, 0x80]).is_err());
    }

    #[test]
    fn varint_u64_decode_handles_empty_input() {
        let empty: Vec<u8> = vec![];
        assert!(decode_varint_u64(&empty).is_err());
    }

    #[test]
    fn varint_u64_encode_into_accumulates_correctly() {
        let mut output = Vec::new();
        encode_varint_u64_into(127, &mut output);
        assert_eq!(output, vec![0x7F]);

        output.clear();
        encode_varint_u64_into(128, &mut output);
        assert_eq!(output, vec![0x80, 0x01]);

        output.clear();
        encode_varint_u64_into(1, &mut output);
        encode_varint_u64_into(2, &mut output);
        assert_eq!(output, vec![0x01, 0x02]);
    }

    #[test]
    fn varint_u64_decode_stops_at_correct_boundary() {
        let mut buffer = Vec::new();
        encode_varint_u64_into(300, &mut buffer);
        encode_varint_u64_into(400, &mut buffer);

        let (first, bytes_read) = decode_varint_u64(&buffer).expect("decode should succeed");
        assert_eq!(first, 300);
        assert!(bytes_read < buffer.len(), "should not consume entire buffer");

        let remaining = &buffer[bytes_read..];
        let (second, _) = decode_varint_u64(remaining).expect("decode second should succeed");
        assert_eq!(second, 400);
    }
}

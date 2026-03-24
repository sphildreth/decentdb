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

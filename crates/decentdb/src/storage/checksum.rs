//! CRC-32C checksum support for fixed database metadata.
//!
//! Uses hardware SSE4.2 CRC-32C instructions on x86-64 when available,
//! falling back to a table-based software implementation otherwise.

const CRC32C_POLYNOMIAL: u32 = 0x82F6_3B78;
const CRC32C_TABLE: [u32; 256] = build_crc32c_table();

const fn build_crc32c_table() -> [u32; 256] {
    let mut table = [0_u32; 256];
    let mut index = 0_usize;
    while index < 256 {
        let mut crc = index as u32;
        let mut bit = 0_u8;
        while bit < 8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ CRC32C_POLYNOMIAL;
            } else {
                crc >>= 1;
            }
            bit += 1;
        }
        table[index] = crc;
        index += 1;
    }
    table
}

/// Software CRC-32C: processes one byte at a time using a lookup table.
fn crc32c_software(mut crc: u32, bytes: &[u8]) -> u32 {
    for byte in bytes {
        let table_index = ((crc ^ u32::from(*byte)) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC32C_TABLE[table_index];
    }
    crc
}

/// Hardware CRC-32C using x86-64 SSE4.2 `crc32` instructions.
///
/// Processes 8 bytes per iteration via `_mm_crc32_u64`, then handles
/// any trailing bytes one at a time via `_mm_crc32_u8`.
///
/// # Safety
///
/// The SSE4.2 feature check is performed at runtime via `is_x86_feature_detected!`.
/// The intrinsics `_mm_crc32_u64` and `_mm_crc32_u8` operate on plain integer
/// values with no memory-safety implications beyond reading from the input slice,
/// which is bounds-checked by the loop structure.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn crc32c_hw(mut crc: u32, bytes: &[u8]) -> u32 {
    use std::arch::x86_64::{_mm_crc32_u64, _mm_crc32_u8};

    let mut i = 0usize;
    let mut crc64 = u64::from(crc);

    // Process 8 bytes at a time.
    let chunks_end = bytes.len() & !7;
    while i < chunks_end {
        let word = u64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
        crc64 = _mm_crc32_u64(crc64, word);
        i += 8;
    }
    crc = crc64 as u32;

    // Handle trailing bytes.
    while i < bytes.len() {
        crc = _mm_crc32_u8(crc, bytes[i]);
        i += 1;
    }
    crc
}

/// Dispatches to hardware or software CRC-32C based on CPU support.
fn crc32c_update(crc: u32, bytes: &[u8]) -> u32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("sse4.2") {
            // SAFETY: feature check above guarantees SSE4.2 is available.
            return unsafe { crc32c_hw(crc, bytes) };
        }
    }
    crc32c_software(crc, bytes)
}

#[cfg(test)]
#[must_use]
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    crc32c_parts(&[bytes])
}

#[must_use]
pub(crate) fn crc32c_parts(parts: &[&[u8]]) -> u32 {
    let mut crc = u32::MAX;
    for part in parts {
        crc = crc32c_update(crc, part);
    }
    !crc
}

#[must_use]
pub(crate) fn crc32c_extend(initial_crc: u32, parts: &[&[u8]]) -> u32 {
    let mut crc = !initial_crc;
    for part in parts {
        crc = crc32c_update(crc, part);
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::{crc32c, crc32c_extend, crc32c_parts};

    #[test]
    fn crc32c_matches_known_castagnoli_vector() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn crc32c_parts_matches_single_slice_crc() {
        let whole = crc32c(b"decentdb");
        let split = crc32c_parts(&[b"dec", b"ent", b"db"]);

        assert_eq!(whole, split);
    }

    #[test]
    fn crc32c_extend_matches_rehashing_with_appended_bytes() {
        let prefix = b"decent";
        let suffix = b"db";
        let full = crc32c_parts(&[prefix, suffix]);
        let extended = crc32c_extend(crc32c(prefix), &[suffix]);
        assert_eq!(full, extended);
    }

    /// Ensures hardware and software paths produce identical results.
    #[test]
    fn crc32c_hw_matches_software_on_varied_sizes() {
        for size in [0, 1, 3, 7, 8, 15, 16, 63, 64, 1000, 4096, 72000] {
            let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
            let sw = {
                let mut c = u32::MAX;
                c = super::crc32c_software(c, &data);
                !c
            };
            let combined = crc32c_parts(&[data.as_slice()]);
            assert_eq!(
                sw, combined,
                "mismatch at size {size}: sw={sw:#010x} combined={combined:#010x}"
            );
        }
    }
}

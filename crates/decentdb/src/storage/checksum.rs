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

#[must_use]
pub(crate) fn crc32c_patch_bytes(
    existing_crc: u32,
    total_len: usize,
    offset: usize,
    old_bytes: &[u8],
    new_bytes: &[u8],
) -> Option<u32> {
    if old_bytes.len() != new_bytes.len() {
        return None;
    }
    let end = offset.checked_add(old_bytes.len())?;
    if end > total_len {
        return None;
    }

    let mut delta = Vec::with_capacity(old_bytes.len());
    let mut changed = false;
    for (old, new) in old_bytes.iter().zip(new_bytes) {
        let byte = old ^ new;
        changed |= byte != 0;
        delta.push(byte);
    }
    if !changed {
        return Some(existing_crc);
    }

    let mut delta_crc = crc32c_update(0, &delta);
    delta_crc = crc32c_shift_zeroes(delta_crc, total_len - end);
    Some(existing_crc ^ delta_crc)
}

fn crc32c_shift_zeroes(mut crc: u32, mut zero_bytes: usize) -> u32 {
    if zero_bytes == 0 {
        return crc;
    }

    let mut odd = [0_u32; 32];
    let mut even = [0_u32; 32];

    odd[0] = CRC32C_POLYNOMIAL;
    let mut row = 1_u32;
    for slot in odd.iter_mut().skip(1) {
        *slot = row;
        row <<= 1;
    }

    gf2_matrix_square(&mut even, &odd);
    gf2_matrix_square(&mut odd, &even);

    loop {
        gf2_matrix_square(&mut even, &odd);
        if zero_bytes & 1 != 0 {
            crc = gf2_matrix_times(&even, crc);
        }
        zero_bytes >>= 1;
        if zero_bytes == 0 {
            break;
        }

        gf2_matrix_square(&mut odd, &even);
        if zero_bytes & 1 != 0 {
            crc = gf2_matrix_times(&odd, crc);
        }
        zero_bytes >>= 1;
        if zero_bytes == 0 {
            break;
        }
    }

    crc
}

fn gf2_matrix_times(matrix: &[u32; 32], mut value: u32) -> u32 {
    let mut sum = 0_u32;
    let mut index = 0_usize;
    while value != 0 {
        if value & 1 != 0 {
            sum ^= matrix[index];
        }
        value >>= 1;
        index += 1;
    }
    sum
}

fn gf2_matrix_square(square: &mut [u32; 32], matrix: &[u32; 32]) {
    for index in 0..32 {
        square[index] = gf2_matrix_times(matrix, matrix[index]);
    }
}

#[cfg(test)]
mod tests {
    use super::{crc32c, crc32c_extend, crc32c_parts, crc32c_patch_bytes};

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

    #[test]
    fn crc32c_empty_and_empty_parts() {
        assert_eq!(crc32c(b""), 0);
        assert_eq!(crc32c_parts(&[b"", b""]), 0);
    }

    #[test]
    fn crc32c_extend_multiple_parts_matches_concat() {
        let a = b"hello";
        let b = b"_";
        let c = b"world";
        let full = crc32c_parts(&[a, b, c]);
        let ext = crc32c_extend(crc32c_parts(&[a]), &[b, c]);
        assert_eq!(full, ext);
    }

    #[test]
    fn crc32c_patch_bytes_matches_full_rehash() {
        let mut original = Vec::new();
        for index in 0..16_384 {
            original.push((index * 31 % 251) as u8);
        }
        let original_crc = crc32c_parts(&[original.as_slice()]);

        for offset in [0_usize, 1, 7, 255, 4093, 8192, original.len() - 4] {
            let mut updated = original.clone();
            let old = updated[offset..offset + 4].to_vec();
            let new = [0x80, 0x00, 0x00, 0x00];
            updated[offset..offset + 4].copy_from_slice(&new);

            let patched =
                crc32c_patch_bytes(original_crc, original.len(), offset, &old, &new).unwrap();
            assert_eq!(patched, crc32c_parts(&[updated.as_slice()]));
        }
    }

    #[test]
    fn crc32c_patch_bytes_composes_multiple_patches() {
        let mut original = vec![0_u8; 65_537];
        for (index, byte) in original.iter_mut().enumerate() {
            *byte = (index * 17 % 239) as u8;
        }
        let mut patched_crc = crc32c_parts(&[original.as_slice()]);
        let mut updated = original.clone();

        for (offset, new) in [
            (3_usize, [1, 2, 3, 4]),
            (4095, [5, 6, 7, 8]),
            (40_000, [9, 10, 11, 12]),
            (65_533, [13, 14, 15, 16]),
        ] {
            let old = updated[offset..offset + 4].to_vec();
            updated[offset..offset + 4].copy_from_slice(&new);
            patched_crc =
                crc32c_patch_bytes(patched_crc, updated.len(), offset, &old, &new).unwrap();
        }

        assert_eq!(patched_crc, crc32c_parts(&[updated.as_slice()]));
    }

    #[test]
    fn crc32c_explicit_hw_call_matches_software_when_supported() {
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("sse4.2") {
                for size in [0, 1, 3, 8, 15, 16, 100, 4096] {
                    let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
                    let sw = {
                        let mut c = u32::MAX;
                        c = super::crc32c_software(c, &data);
                        !c
                    };
                    let hw = unsafe { super::crc32c_hw(u32::MAX, &data) };
                    let hw_inv = !hw;
                    assert_eq!(hw_inv, sw);
                }
            }
        }
    }

    #[test]
    fn build_crc32c_table_runtime_call() {
        let table = super::build_crc32c_table();
        assert_eq!(table.len(), 256);
        assert_eq!(table[0], 0);
        assert_ne!(table[1], 0);
    }
}

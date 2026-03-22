//! CRC-32C checksum support for fixed database metadata.

const CRC32C_POLYNOMIAL: u32 = 0x82F6_3B78;

#[cfg(test)]
#[must_use]
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    crc32c_parts(&[bytes])
}

#[must_use]
pub(crate) fn crc32c_parts(parts: &[&[u8]]) -> u32 {
    let mut crc = u32::MAX;

    for part in parts {
        for byte in *part {
            crc ^= u32::from(*byte);
            for _ in 0..8 {
                if crc & 1 == 1 {
                    crc = (crc >> 1) ^ CRC32C_POLYNOMIAL;
                } else {
                    crc >>= 1;
                }
            }
        }
    }

    !crc
}

#[cfg(test)]
mod tests {
    use super::{crc32c, crc32c_parts};

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
}

//! CRC-32C checksum support for fixed database metadata.

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
            let table_index = ((crc ^ u32::from(*byte)) & 0xFF) as usize;
            crc = (crc >> 8) ^ CRC32C_TABLE[table_index];
        }
    }

    !crc
}

#[must_use]
pub(crate) fn crc32c_extend(initial_crc: u32, parts: &[&[u8]]) -> u32 {
    let mut crc = !initial_crc;

    for part in parts {
        for byte in *part {
            let table_index = ((crc ^ u32::from(*byte)) & 0xFF) as usize;
            crc = (crc >> 8) ^ CRC32C_TABLE[table_index];
        }
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
}

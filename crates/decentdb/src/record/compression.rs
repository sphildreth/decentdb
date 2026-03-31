//! Pure-Rust overflow compression wrapper used by record-layer spill paths.

use miniz_oxide::deflate::compress_to_vec_zlib;
use miniz_oxide::inflate::decompress_to_vec_zlib;

use crate::error::{DbError, Result};

const AUTO_COMPRESSION_LEVEL: u8 = 1;
pub(crate) const AUTO_MIN_PAYLOAD_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompressionMode {
    Never,
    Auto,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompressedPayload {
    pub(crate) bytes: Vec<u8>,
    pub(crate) compressed: bool,
}

pub(crate) fn maybe_compress(bytes: &[u8], mode: CompressionMode) -> CompressedPayload {
    if mode == CompressionMode::Never || bytes.is_empty() {
        return CompressedPayload {
            bytes: bytes.to_vec(),
            compressed: false,
        };
    }
    if bytes.len() < AUTO_MIN_PAYLOAD_BYTES {
        return CompressedPayload {
            bytes: bytes.to_vec(),
            compressed: false,
        };
    }

    let compressed = compress_to_vec_zlib(bytes, AUTO_COMPRESSION_LEVEL);
    if compressed.len() + 8 < bytes.len() {
        CompressedPayload {
            bytes: compressed,
            compressed: true,
        }
    } else {
        CompressedPayload {
            bytes: bytes.to_vec(),
            compressed: false,
        }
    }
}

pub(crate) fn decompress(bytes: &[u8]) -> Result<Vec<u8>> {
    decompress_to_vec_zlib(bytes).map_err(|error| {
        DbError::corruption(format!("invalid compressed overflow payload: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use super::{decompress, maybe_compress, CompressionMode};

    #[test]
    fn compression_roundtrip_preserves_bytes() {
        let payload = b"repeat-me repeat-me repeat-me repeat-me repeat-me".repeat(2_048);
        let compressed = maybe_compress(&payload, CompressionMode::Auto);
        assert!(compressed.compressed);
        assert_eq!(decompress(&compressed.bytes).expect("decompress"), payload);
    }

    #[test]
    fn maybe_compress_never_mode_leaves_bytes_alone() {
        let data = b"short data";
        let out = maybe_compress(data, CompressionMode::Never);
        assert!(!out.compressed);
        assert_eq!(out.bytes, data);
    }

    #[test]
    fn maybe_compress_small_payload_not_compressed() {
        let data = vec![0u8; 8]; // much smaller than AUTO_MIN_PAYLOAD_BYTES
        let out = maybe_compress(&data, CompressionMode::Auto);
        assert!(!out.compressed);
        assert_eq!(out.bytes, data);
    }

    #[test]
    fn decompress_invalid_blob_returns_corruption_error() {
        let bad = b"not-a-zlib-stream";
        let res = decompress(bad);
        assert!(res.is_err());
        // Verify error category is corruption by matching message contains 'invalid compressed'
        let err = res.unwrap_err();
        assert!(format!("{err}").contains("invalid compressed"));
    }
}

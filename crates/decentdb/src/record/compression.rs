//! Pure-Rust overflow compression wrapper used by record-layer spill paths.

use miniz_oxide::deflate::compress_to_vec_zlib;
use miniz_oxide::inflate::decompress_to_vec_zlib;

use crate::error::{DbError, Result};

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

    let compressed = compress_to_vec_zlib(bytes, 6);
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
        let payload = b"repeat-me repeat-me repeat-me repeat-me repeat-me".repeat(64);
        let compressed = maybe_compress(&payload, CompressionMode::Auto);
        assert!(compressed.compressed);
        assert_eq!(decompress(&compressed.bytes).expect("decompress"), payload);
    }
}

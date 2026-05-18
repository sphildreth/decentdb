#[cfg(test)]
mod tests {
    use std::sync::RwLock;

    use proptest::prelude::*;

    use crate::error::{DbError, Result};
    use crate::storage::page;
    use crate::vfs::{FileKind, VfsFile};
    use crate::wal::delta::DELTA_FRAME_PAYLOAD_SIZE;
    use crate::wal::format::{
        FrameType, WalFrame, WalHeader, FRAME_HEADER_SIZE, FRAME_TRAILER_SIZE, WAL_HEADER_SIZE,
    };

    #[derive(Debug)]
    struct TestFile {
        data: RwLock<Vec<u8>>,
    }

    impl TestFile {
        fn new(bytes: Vec<u8>) -> Self {
            Self {
                data: RwLock::new(bytes),
            }
        }
    }

    impl VfsFile for TestFile {
        fn kind(&self) -> FileKind {
            FileKind::Wal
        }
        fn path(&self) -> &std::path::Path {
            std::path::Path::new(":test:")
        }
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
            let data = self.data.read().map_err(|_| DbError::internal("lock"))?;
            let off = offset as usize;
            if off >= data.len() {
                return Ok(0);
            }
            let avail = &data[off..];
            let n = avail.len().min(buf.len());
            buf[..n].copy_from_slice(&avail[..n]);
            Ok(n)
        }
        fn write_at(&self, _: u64, _: &[u8]) -> Result<usize> {
            unimplemented!()
        }
        fn advise_sequential(&self) -> Result<()> {
            Ok(())
        }
        fn sync_data(&self) -> Result<()> {
            Ok(())
        }
        fn sync_metadata(&self) -> Result<()> {
            Ok(())
        }
        fn file_size(&self) -> Result<u64> {
            Ok(self.data.read().unwrap().len() as u64)
        }
        fn set_len(&self, _: u64) -> Result<()> {
            Ok(())
        }
    }

    proptest! {
        /// Frame roundtrip: a valid page frame survives encode then decode
        /// identity, preserving frame_type, page_id, and payload.
        #[test]
        fn page_frame_encode_decode_roundtrip_is_identity(
            page_id in 1u32..=u32::MAX,
            payload in proptest::collection::vec(any::<u8>(), page::DEFAULT_PAGE_SIZE as usize..=page::DEFAULT_PAGE_SIZE as usize),
        ) {
            let frame = WalFrame::page(page_id, payload.clone());
            let encoded = frame.encode(page::DEFAULT_PAGE_SIZE).expect("encode frame");
            let logical_end = encoded.len() as u64;
            let file = TestFile::new(encoded.to_vec());
            let decoded = WalFrame::decode_from_file(&file, 0, page::DEFAULT_PAGE_SIZE, logical_end)
                .expect("decode frame")
                .expect("frame decoded");
            prop_assert_eq!(decoded.frame_type, FrameType::Page);
            prop_assert_eq!(decoded.page_id, page_id);
            prop_assert_eq!(&decoded.payload, &payload);
        }

        /// Header roundtrip: WalHeader encode/decode is a bijection.
        #[test]
        fn wal_header_roundtrip_is_identity(
            page_size in 128u32..=65536u32,
            wal_end_offset in (0u64..=1_000_000u64).prop_filter("not between 0 and 32 exclusive", |v| *v == 0 || *v >= WAL_HEADER_SIZE),
        ) {
            let header = WalHeader::new(page_size, wal_end_offset);
            let encoded = header.encode();
            let decoded = WalHeader::decode(&encoded).expect("decode header");
            prop_assert_eq!(decoded, header);
        }

        /// Invalid frame types: any u8 outside 0-3 produces DbError::Corruption.
        #[test]
        fn invalid_frame_types_are_rejected(
            bad_type in (4u8..=255u8)
        ) {
            let result = FrameType::try_from(bad_type);
            prop_assert!(result.is_err());
        }

        /// Frame size consistency: payload size + header + trailer equals
        /// encoded_len for valid page/delta frames.
        #[test]
        fn page_frame_encoded_len_matches_payload_size(
            (page_size, page_id, payload) in (512u32..=16384u32).prop_flat_map(|ps| {
                let ps_usize = ps as usize;
                (
                    Just(ps),
                    (1u32..=u32::MAX),
                    proptest::collection::vec(any::<u8>(), ps_usize..=ps_usize),
                )
            }),
        ) {
            let frame = WalFrame::page(page_id, payload);
            let expected = FRAME_HEADER_SIZE + page_size as usize + FRAME_TRAILER_SIZE;
            prop_assert_eq!(frame.encoded_len(page_size), expected);
        }

        #[test]
        fn delta_frame_encoded_len_matches_payload_size(
            page_size in 512u32..=16384u32,
            page_id in 1u32..=u32::MAX,
            payload in proptest::collection::vec(any::<u8>(), DELTA_FRAME_PAYLOAD_SIZE..=DELTA_FRAME_PAYLOAD_SIZE),
        ) {
            let frame = WalFrame::page_delta(page_id, payload);
            let expected = FRAME_HEADER_SIZE + DELTA_FRAME_PAYLOAD_SIZE + FRAME_TRAILER_SIZE;
            prop_assert_eq!(frame.encoded_len(page_size), expected);
        }

        /// Commit frame roundtrip: a commit frame survives encode then decode.
        #[test]
        fn commit_frame_encode_decode_roundtrip_is_identity(
            page_size in 512u32..=16384u32,
        ) {
            let frame = WalFrame::commit();
            let encoded = frame.encode(page_size).expect("encode commit");
            let logical_end = encoded.len() as u64;
            let file = TestFile::new(encoded.to_vec());
            let decoded = WalFrame::decode_from_file(&file, 0, page_size, logical_end)
                .expect("decode commit")
                .expect("commit frame decoded");
            prop_assert_eq!(decoded.frame_type, FrameType::Commit);
            prop_assert_eq!(decoded.page_id, 0);
        }

        /// Checkpoint frame roundtrip: checkpoint frame survives encode then
        /// decode, preserving the checkpoint LSN.
        #[test]
        fn checkpoint_frame_encode_decode_roundtrip_is_identity(
            page_size in 512u32..=16384u32,
            checkpoint_lsn in 1u64..=u64::MAX,
        ) {
            let frame = WalFrame::checkpoint(checkpoint_lsn);
            let encoded = frame.encode(page_size).expect("encode checkpoint");
            let logical_end = encoded.len() as u64;
            let file = TestFile::new(encoded.to_vec());
            let decoded = WalFrame::decode_from_file(&file, 0, page_size, logical_end)
                .expect("decode checkpoint")
                .expect("checkpoint frame decoded");
            prop_assert_eq!(decoded.frame_type, FrameType::Checkpoint);
            prop_assert_eq!(decoded.page_id, 0);
            prop_assert_eq!(&decoded.payload, &checkpoint_lsn.to_le_bytes());
        }
    }
}

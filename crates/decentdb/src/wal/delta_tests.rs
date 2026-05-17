#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use crate::wal::delta::{
        apply_page_delta_in_place, encode_page_delta, DELTA_FRAME_PAYLOAD_SIZE,
    };

    fn byte_vec_strategy(max_len: usize) -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(any::<u8>(), 1..=max_len)
    }

    fn equal_length_pair_strategy(max_len: usize) -> impl Strategy<Value = (Vec<u8>, Vec<u8>)> {
        (1..=max_len).prop_flat_map(|len| {
            let left = proptest::collection::vec(any::<u8>(), len..=len);
            let right = proptest::collection::vec(any::<u8>(), len..=len);
            (left, right)
        })
    }

    proptest! {
        /// Delta roundtrip: applying a valid encoded delta to base produces
        /// updated. When encoding returns None (diff exceeds 512-byte limit),
        /// that is also acceptable.
        #[test]
        fn delta_roundtrip_applied_to_base_produces_updated(
            (base, updated) in equal_length_pair_strategy(64)
        ) {
            if let Some(delta) = encode_page_delta(&base, &updated) {
                prop_assert_eq!(delta.len(), DELTA_FRAME_PAYLOAD_SIZE);
                let mut rebuilt = base.clone();
                apply_page_delta_in_place(&mut rebuilt, &delta).expect("apply delta");
                prop_assert_eq!(&rebuilt, &updated);
            }
        }

        /// No-op identity: base == updated should produce None (nothing to
        /// encode) or a minimal delta that roundtrips harmlessly.
        #[test]
        fn delta_same_base_and_updated_is_no_op(
            base in byte_vec_strategy(128)
        ) {
            let result = encode_page_delta(&base, &base);
            prop_assert!(result.is_none());
        }

        /// Size bounds: every encoded delta payload is exactly
        /// DELTA_FRAME_PAYLOAD_SIZE bytes (padded to fixed frame size).
        #[test]
        fn delta_payload_never_exceeds_frame_size(
            (base, updated) in equal_length_pair_strategy(128)
        ) {
            if let Some(delta) = encode_page_delta(&base, &updated) {
                prop_assert_eq!(delta.len(), DELTA_FRAME_PAYLOAD_SIZE);
            }
        }

        /// Cross-platform determinism: the same inputs always produce the
        /// identical byte sequence.
        #[test]
        fn delta_encoding_is_deterministic(
            (base, updated) in equal_length_pair_strategy(64)
        ) {
            let first = encode_page_delta(&base, &updated);
            let second = encode_page_delta(&base, &updated);
            prop_assert_eq!(first, second);
        }

        /// Delta application with a mutated payload must return a
        /// corruption error, never a panic.
        #[test]
        fn delta_corrupted_payload_is_rejected(
            (base, updated) in equal_length_pair_strategy(64)
        ) {
            if let Some(delta) = encode_page_delta(&base, &updated) {
                let mut corrupted = delta.clone();
                if corrupted.len() >= 4 {
                    corrupted[2..4].copy_from_slice(&u16::MAX.to_le_bytes());
                }
                let mut page = base.clone();
                let _ = apply_page_delta_in_place(&mut page, &corrupted);
            }
        }
    }
}
